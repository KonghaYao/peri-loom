//! Tests for the maintenance scheduling rules.
//!
//! Four of the five properties W4 pins down are testable without a native engine, so they run
//! against a fake target that counts what it was asked to run. The long-reader property needs
//! the real database and lives in `database/connections.rs`, next to the preflight it
//! exercises.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

use super::budget::{BudgetDenied, MaintenanceBudget};
use super::policy::{MaintenanceDecision, MaintenancePolicy, MaintenanceSkip, Watermark};
use super::{
    BlockReason, MaintenanceLane, MaintenanceLedger, MaintenanceScheduler, MaintenanceSignals,
    MaintenanceTarget, PreSuspendOutcome, PreSuspendReason,
};

/// A target that records what it was asked to run and does nothing else. `signals` returns the
/// figures the test wants the scheduler to decide on.
struct FakeTarget {
    name: &'static str,
    ledger: MaintenanceLedger,
    signals: Mutex<MaintenanceSignals>,
    runs: AtomicUsize,
    fail: bool,
    delay: Duration,
}

impl FakeTarget {
    fn new(name: &'static str, wal_bytes: u64) -> Arc<Self> {
        Self::build(name, wal_bytes, false, Duration::ZERO)
    }

    fn failing(name: &'static str, wal_bytes: u64) -> Arc<Self> {
        Self::build(name, wal_bytes, true, Duration::ZERO)
    }

    fn slow(name: &'static str, wal_bytes: u64, delay: Duration) -> Arc<Self> {
        Self::build(name, wal_bytes, false, delay)
    }

    fn slow_failing(name: &'static str, wal_bytes: u64, delay: Duration) -> Arc<Self> {
        Self::build(name, wal_bytes, true, delay)
    }

    fn build(name: &'static str, wal_bytes: u64, fail: bool, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            name,
            ledger: MaintenanceLedger::default(),
            signals: Mutex::new(MaintenanceSignals {
                wal_bytes,
                ..MaintenanceSignals::default()
            }),
            runs: AtomicUsize::new(0),
            fail,
            delay,
        })
    }

    fn runs(&self) -> usize {
        self.runs.load(Ordering::SeqCst)
    }
}

impl MaintenanceTarget for FakeTarget {
    fn identity(&self) -> String {
        self.name.to_owned()
    }

    fn ledger(&self) -> &MaintenanceLedger {
        &self.ledger
    }

    fn signals(&self) -> Result<MaintenanceSignals> {
        Ok(*self
            .signals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()))
    }

    fn run_maintenance(&self, lane: MaintenanceLane) -> Result<()> {
        // The real entry points record execution before doing any work; mirroring that here
        // is what makes "started but unconfirmed" observable in the scheduler tests.
        self.ledger.record_execution(lane);
        self.runs.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            std::thread::sleep(self.delay);
        }
        if self.fail {
            anyhow::bail!("synthetic maintenance failure");
        }
        Ok(())
    }
}

/// The smallest policy that is still a real policy: one byte of WAL arms both lanes, a
/// five-second cooldown, and lanes whose cadences differ so their slots do not coincide.
fn policy(enabled: bool) -> MaintenancePolicy {
    MaintenancePolicy::for_tests(
        enabled,
        Watermark {
            low_bytes: 0,
            high_bytes: 1,
            period: Duration::from_secs(1),
            phase: Duration::ZERO,
        },
        Watermark {
            low_bytes: 0,
            high_bytes: 1,
            period: Duration::from_secs(2),
            phase: Duration::from_secs(1),
        },
        Duration::from_secs(5),
        Duration::from_secs(60),
    )
}

fn budget(per_database: usize, global: usize, requests: u64, bytes: u64) -> MaintenanceBudget {
    MaintenanceBudget::new(
        per_database,
        global,
        requests,
        bytes,
        Duration::from_secs(60),
    )
}

fn scheduler(policy: MaintenancePolicy, budget: MaintenanceBudget) -> Arc<MaintenanceScheduler> {
    Arc::new(MaintenanceScheduler::new(policy, budget))
}

/// Past the phase plus period of the slower lane, so the first pass finds both lanes due.
fn armed_now(ledger: &MaintenanceLedger) -> Instant {
    ledger.opened() + Duration::from_secs(4)
}

fn signals(wal_bytes: u64) -> MaintenanceSignals {
    MaintenanceSignals {
        wal_bytes,
        ..MaintenanceSignals::default()
    }
}

fn observe(scheduler: &MaintenanceScheduler, target: &Arc<FakeTarget>, now: Instant) {
    let target: Arc<dyn MaintenanceTarget> = target.clone();
    scheduler.observe(&target, now).expect("maintenance probe");
}

#[test]
fn cooldown_does_not_repeat_maintenance() {
    let scheduler = scheduler(policy(true), budget(1, 1, 100, u64::MAX));
    let target = FakeTarget::new("cooldown", 4096);
    let now = armed_now(&target.ledger);

    observe(&scheduler, &target, now);
    // One unit per drain, so an armed database with two due lanes still serializes them.
    assert_eq!(scheduler.drain(1, now).completed, 1);
    assert_eq!(target.runs(), 1, "the second lane waits for the next drain");
    assert_eq!(scheduler.drain(1, now).completed, 1);
    let after_first = target.ledger.counters();
    assert_eq!(after_first.completed, 2);
    assert_eq!(target.runs(), 2);

    // The WAL never shrinks in this window, so the watermark alone would pick both lanes on
    // every pass. The cooldown is what has to hold them back.
    for step in 1..=20 {
        let at = now + Duration::from_millis(step * 100);
        observe(&scheduler, &target, at);
        assert_eq!(
            scheduler.drain(8, at).completed,
            0,
            "a lane inside its cooldown must not run again"
        );
    }
    let after_cooldown = target.ledger.counters();
    assert_eq!(
        after_cooldown.attempts, after_first.attempts,
        "an ineligible lane must not even be picked"
    );
    assert_eq!(target.runs(), 2, "no unit ran inside the cooldown");

    // Past the cooldown and past each lane's own slot: both lanes are eligible again.
    let later = now + Duration::from_secs(6);
    observe(&scheduler, &target, later);
    assert_eq!(scheduler.drain(8, later).completed, 2);
    assert_eq!(target.runs(), 4);
}

#[test]
fn the_cooldown_opens_when_the_lane_is_picked_not_when_it_finishes() {
    let policy = policy(true);
    let scheduler = scheduler(policy.clone(), budget(2, 2, 100, u64::MAX));
    let target = FakeTarget::new("pick", 4096);
    let now = armed_now(&target.ledger);

    observe(&scheduler, &target, now);
    let queued = target.ledger.counters();
    assert_eq!(queued.attempts, 2, "both due lanes were picked");
    assert_eq!(queued.executions, 0, "observing only queues");
    // The window opens at the pick. A unit that outlives its cooldown must not be picked a
    // second time while it is still running, and a refused unit must not be retried on the
    // next tick; both require the ledger to record the deadline before `drain` runs.
    for lane in MaintenanceLane::ALL {
        assert_eq!(
            target.ledger.next_allowed(lane),
            Some(now + policy.cooldown()),
            "lane {} must be cooling down before its unit starts",
            lane.name()
        );
    }

    assert_eq!(scheduler.drain(2, now).completed, 2);
    for lane in MaintenanceLane::ALL {
        assert_eq!(
            target.ledger.next_allowed(lane),
            Some(now + policy.cooldown()),
            "completion must not move the cooldown window",
        );
    }
}

#[test]
fn watermarks_are_hysteresis_not_a_threshold() {
    let policy = MaintenancePolicy::for_tests(
        true,
        Watermark {
            low_bytes: 10,
            high_bytes: 100,
            period: Duration::from_secs(1),
            phase: Duration::ZERO,
        },
        Watermark {
            low_bytes: 10,
            high_bytes: 100,
            period: Duration::from_secs(2),
            phase: Duration::from_secs(1),
        },
        Duration::from_secs(1),
        Duration::from_secs(1),
    );
    let lane = MaintenanceLane::Checkpoint;
    let ledger = MaintenanceLedger::default();
    // Crossing the high mark arms; dropping to the low mark disarms; anything in between
    // leaves the lane exactly as it was, which is the band a run "just above the threshold"
    // would otherwise re-arm on.
    ledger.set_armed(lane, policy.arming(lane, 100).expect("armed"));
    assert!(ledger.armed(lane));
    assert_eq!(policy.arming(lane, 50), None);
    assert!(
        ledger.armed(lane),
        "a value inside the band must not change the armed state"
    );
    ledger.set_armed(lane, policy.arming(lane, 10).expect("disarmed"));
    assert!(!ledger.armed(lane));

    // Disarmed means ineligible no matter how many slots have passed.
    let now = ledger.opened() + Duration::from_secs(60);
    assert_eq!(
        policy.decide(lane, &signals(100), &ledger, now),
        MaintenanceDecision::Skip(MaintenanceSkip::BelowLow)
    );
}

#[test]
fn maintenance_is_not_on_the_request_path() {
    // Every commit here crosses a watermark and passes every slot, and a unit is queued
    // before they run. A commit that executed maintenance — or that waited for the unit the
    // scheduler had already picked — would show a completion here.
    let scheduler = scheduler(policy(true), budget(1, 1, 100, u64::MAX));
    let directory = tempfile::tempdir().expect("cache dir");
    let database = Arc::new(
        crate::database::RemoteDatabase::open(
            Arc::new(crate::object_store::MemoryObjectStore::default()),
            crate::namespace::Namespace::new("maintenance", "request_path").expect("namespace"),
            directory.path(),
            0,
        )
        .expect("open database"),
    );
    database
        .execute("CREATE TABLE ledger(value INTEGER)")
        .expect("create table");

    assert_eq!(
        database.maintenance_ledger().counters().attempts,
        0,
        "no commit, query or session open may pick a maintenance lane"
    );
    assert_eq!(
        database.maintenance_ledger().counters().executions,
        0,
        "no commit, query or session open may run a maintenance unit"
    );

    let target: Arc<dyn MaintenanceTarget> = database.clone();
    scheduler
        .observe(&target, armed_now(database.maintenance_ledger()))
        .expect("maintenance probe");
    let queued = database.maintenance_ledger().counters();
    assert!(queued.attempts >= 1, "the pass picked at least one lane");
    assert_eq!(
        queued.executions, 0,
        "a scheduling pass must only queue, never run"
    );

    // Work queued but not yet executed, while the request path keeps serving.
    for value in 0..16 {
        database
            .execute(&format!("INSERT INTO ledger VALUES({value})"))
            .expect("insert");
    }
    let _ = database
        .query("SELECT count(*) FROM ledger")
        .expect("query");
    assert_eq!(
        database.maintenance_ledger().counters().executions,
        0,
        "a commit must not execute, or wait for, queued maintenance"
    );

    // Only the drain path runs it, which no request path calls.
    assert!(
        scheduler
            .drain(8, armed_now(database.maintenance_ledger()))
            .completed
            >= 1
    );
    assert!(
        database.maintenance_ledger().counters().executions >= 1,
        "the drain path is what runs a unit"
    );
    assert_eq!(
        database
            .query("SELECT count(*) FROM ledger")
            .expect("query")[0][0]
            .to_string(),
        "16"
    );
}

#[test]
fn a_long_reader_block_is_recorded_and_backs_the_database_off() {
    let policy = policy(true);
    let ledger = MaintenanceLedger::default();
    let lane = MaintenanceLane::IndexCompact;
    let now = armed_now(&ledger);
    ledger.set_armed(lane, true);
    assert!(policy.decide(lane, &signals(4096), &ledger, now).is_due());

    ledger.record_block(BlockReason::LongReader, Some(policy.blocked_until(now)));
    assert_eq!(
        policy.decide(lane, &signals(4096), &ledger, now + Duration::from_secs(1)),
        MaintenanceDecision::Skip(MaintenanceSkip::Cooling)
    );
    let counters = ledger.counters();
    assert_eq!(counters.blocks.get("long_reader"), Some(&1));
    assert_eq!(
        counters.attempts, 0,
        "nothing ran against the blocked reader"
    );
    assert_eq!(counters.completed, 0, "nothing was reclaimed");
    // The block holds the whole database back, not only the lane that was blocked: while the
    // reader still holds its snapshot, the other lane would spend a backfill against it too.
    ledger.set_armed(MaintenanceLane::Checkpoint, true);
    assert_eq!(
        policy.decide(
            MaintenanceLane::Checkpoint,
            &signals(4096),
            &ledger,
            now + Duration::from_secs(2)
        ),
        MaintenanceDecision::Skip(MaintenanceSkip::Cooling)
    );
}

#[test]
fn an_unconfirmed_wal_skips_the_compaction_lane_before_any_watermark() {
    let policy = policy(true);
    let ledger = MaintenanceLedger::default();
    ledger.set_armed(MaintenanceLane::IndexCompact, true);
    ledger.set_armed(MaintenanceLane::Checkpoint, true);
    let unconfirmed = MaintenanceSignals {
        wal_bytes: 4096,
        unconfirmed_wal: true,
        ..MaintenanceSignals::default()
    };
    assert_eq!(
        policy.decide(
            MaintenanceLane::IndexCompact,
            &unconfirmed,
            &ledger,
            armed_now(&ledger)
        ),
        MaintenanceDecision::Skip(MaintenanceSkip::UnconfirmedWal)
    );
    // The checkpoint lane is not gated on it: a Full checkpoint is what confirms a tail.
    assert!(policy
        .decide(
            MaintenanceLane::Checkpoint,
            &unconfirmed,
            &ledger,
            armed_now(&ledger)
        )
        .is_due());
}

#[test]
fn a_spent_budget_skips_the_unit_instead_of_running_it() {
    // One request per window admits the first lane and refuses the second.
    let scheduler = scheduler(policy(true), budget(2, 2, 1, u64::MAX));
    let target = FakeTarget::new("spent", 4096);
    let now = armed_now(&target.ledger);

    observe(&scheduler, &target, now);
    let drained = scheduler.drain(8, now);
    assert_eq!(drained.completed, 1);
    assert_eq!(drained.refused, 1);
    assert_eq!(target.runs(), 1, "a refused unit must not reach the target");
    let counters = target.ledger.counters();
    assert_eq!(counters.blocks.get("budget"), Some(&1));
    assert_eq!(counters.refused, 1);
    assert_eq!(counters.failed, 0, "a refusal is a skip, not a failure");
    assert_eq!(
        counters.completed, 1,
        "the refused lane did not run partially"
    );
}

#[test]
fn the_per_database_cap_holds_while_another_database_is_admitted() {
    let budget = budget(1, 4, 100, u64::MAX);
    let now = Instant::now();
    let first = budget.acquire("database-a", 0, now).expect("first lease");
    let second = budget
        .acquire("database-b", 0, now)
        .expect("other database");
    assert_eq!(
        budget.acquire("database-a", 0, now).unwrap_err(),
        BudgetDenied::PerDatabase
    );
    drop(first);
    let reused = budget
        .acquire("database-a", 0, now)
        .expect("the slot is reusable once released");
    drop(second);
    assert_eq!(budget.in_flight(), 1, "only the reused lease is still held");
    drop(reused);
    assert_eq!(budget.in_flight(), 0);
}

#[test]
fn the_global_cap_admits_one_unit_at_a_time() {
    let budget = budget(4, 1, 100, u64::MAX);
    let now = Instant::now();
    let held = budget.acquire("database-a", 0, now).expect("first lease");
    assert_eq!(
        budget.acquire("database-b", 0, now).unwrap_err(),
        BudgetDenied::GlobalConcurrency
    );
    drop(held);
    assert_eq!(budget.in_flight(), 0);
}

#[test]
fn the_request_budget_admits_a_rate_and_the_byte_budget_a_ceiling() {
    let now = Instant::now();
    let requests = budget(4, 4, 1, u64::MAX);
    let first = requests.acquire("a", 0, now).expect("first request");
    assert_eq!(
        requests.acquire("b", 0, now).unwrap_err(),
        BudgetDenied::Requests
    );
    drop(first);

    let bytes = budget(4, 4, 4, 100);
    let first = bytes.acquire("a", 60, now).expect("within the ceiling");
    let second = bytes.acquire("b", 30, now).expect("still within");
    assert_eq!(
        bytes.acquire("c", 20, now).unwrap_err(),
        BudgetDenied::Bytes
    );
    drop(first);
    drop(second);
    // The window sum is a rate, not a level: releasing a lease does not return its bytes,
    // so the ceiling still refuses inside the same window.
    assert_eq!(
        bytes.acquire("c", 20, now).unwrap_err(),
        BudgetDenied::Bytes
    );
}

#[test]
fn a_full_window_rolls_over_instead_of_refusing_forever() {
    // Both counters are window sums: a scheduler that never reaches `window` would refuse
    // every later unit even after the rate should have recovered. The window opens when the
    // budget is built, so the clock is taken after both budgets exist.
    let window = Duration::from_secs(60);
    let requests = MaintenanceBudget::new(4, 4, 1, u64::MAX, window);
    let bytes = MaintenanceBudget::new(4, 4, 4, 100, window);
    let now = Instant::now();
    drop(requests.acquire("a", 0, now).expect("first request"));
    assert_eq!(
        requests
            .acquire("a", 0, now + window - Duration::from_millis(1))
            .unwrap_err(),
        BudgetDenied::Requests,
        "the window must still be closed just before it rolls"
    );
    drop(
        requests
            .acquire("a", 0, now + window)
            .expect("the next window admits the same rate again"),
    );
    assert_eq!(
        requests.acquire("a", 0, now + window).unwrap_err(),
        BudgetDenied::Requests,
        "the roll admits one request, not an unbounded burst"
    );

    drop(bytes.acquire("a", 60, now).expect("first batch"));
    drop(bytes.acquire("b", 40, now).expect("fills the byte window"));
    assert_eq!(
        bytes
            .acquire("c", 1, now + window - Duration::from_millis(1))
            .unwrap_err(),
        BudgetDenied::Bytes
    );
    drop(
        bytes
            .acquire("c", 1, now + window)
            .expect("the byte sum resets with the window"),
    );
}

#[test]
fn a_disabled_scheduler_never_picks_a_lane() {
    let scheduler = scheduler(policy(false), budget(1, 1, 100, u64::MAX));
    let target = FakeTarget::new("disabled", 1 << 30);
    let now = armed_now(&target.ledger);
    observe(&scheduler, &target, now);
    assert_eq!(scheduler.drain(8, now).completed, 0);
    assert_eq!(target.runs(), 0);
    assert_eq!(target.ledger.counters().attempts, 0);
    assert_eq!(
        scheduler.policy().decide(
            MaintenanceLane::Checkpoint,
            &signals(1 << 30),
            &target.ledger,
            now
        ),
        MaintenanceDecision::Skip(MaintenanceSkip::Disabled)
    );
}

#[test]
fn a_failing_unit_is_counted_as_a_failure_not_as_a_completion() {
    let scheduler = scheduler(policy(true), budget(2, 2, 100, u64::MAX));
    let target = FakeTarget::failing("failing", 4096);
    let now = armed_now(&target.ledger);
    observe(&scheduler, &target, now);
    let drained = scheduler.drain(8, now);
    assert_eq!(drained.failed, 2);
    assert_eq!(drained.completed, 0);
    assert_eq!(drained.refused, 0);
    let counters = target.ledger.counters();
    assert_eq!(counters.failed, 2);
    assert_eq!(counters.completed, 0);
}

#[test]
fn lanes_are_de_aligned() {
    let policy = policy(true);
    let origin = Instant::now();
    // Different cadences and different offsets inside them, so the two lanes do not become
    // eligible in lock-step and their backfills cannot stack into one spike.
    assert_ne!(
        policy.next_slot(MaintenanceLane::Checkpoint, origin, None),
        policy.next_slot(MaintenanceLane::IndexCompact, origin, None)
    );
    assert_ne!(
        policy.watermark(MaintenanceLane::Checkpoint).period,
        policy.watermark(MaintenanceLane::IndexCompact).period
    );
    assert_ne!(
        policy.watermark(MaintenanceLane::Checkpoint).phase,
        policy.watermark(MaintenanceLane::IndexCompact).phase
    );
}

#[test]
fn the_enums_and_the_counters_agree_on_positions() {
    // The export indexes the counters by the variant's discriminant, so a variant moved out
    // of order would silently relabel every count. This pins that assumption.
    for (index, reason) in BlockReason::ALL.iter().enumerate() {
        assert_eq!(*reason as usize, index, "{} moved", reason.name());
    }
    for (index, denied) in super::BudgetDenied::ALL.iter().enumerate() {
        assert_eq!(*denied as usize, index, "{} moved", denied.name());
    }
    for (index, lane) in MaintenanceLane::ALL.iter().enumerate() {
        assert_eq!(lane.index(), index, "{} moved", lane.name());
    }
}

#[test]
fn the_reader_age_is_an_upper_bound_that_survives_the_reader() {
    let ledger = MaintenanceLedger::default();
    assert_eq!(ledger.counters().oldest_reader_peak_ms, 0);
    ledger.observe_reader_age(Duration::from_millis(4200));
    ledger.observe_reader_age(Duration::from_millis(10));
    assert_eq!(ledger.counters().oldest_reader_peak_ms, 4200);
    assert_eq!(ledger.readers().peak(), Duration::from_millis(4200));
}

fn pre_suspend_scheduler(
    enabled: bool,
    bytes: u64,
    timeout: Duration,
) -> Arc<MaintenanceScheduler> {
    Arc::new(MaintenanceScheduler::new_for_tests(
        policy(enabled),
        budget(1, 1, 4, bytes),
        timeout,
    ))
}

fn pre_suspend(scheduler: &MaintenanceScheduler, target: &Arc<FakeTarget>) -> PreSuspendOutcome {
    let target: Arc<dyn MaintenanceTarget> = target.clone();
    scheduler.pre_suspend(&target)
}

#[test]
fn a_suspend_pre_tidy_does_nothing_while_the_schedule_is_off() {
    let scheduler = pre_suspend_scheduler(false, u64::MAX, Duration::from_secs(1));
    let target = FakeTarget::new("suspend_disabled", 4096);
    let outcome = pre_suspend(&scheduler, &target);
    assert_eq!(outcome.reason, PreSuspendReason::Disabled);
    assert_eq!(outcome.lane, None);
    assert!(!outcome.started);
    assert_eq!(target.runs(), 0);
    assert_eq!(target.ledger.counters().executions, 0);
}

#[test]
fn a_suspend_pre_tidy_runs_one_unit_inside_the_budget() {
    let scheduler = pre_suspend_scheduler(true, u64::MAX, Duration::from_secs(1));
    let target = FakeTarget::new("suspend_ok", 4096);
    let outcome = pre_suspend(&scheduler, &target);
    assert_eq!(outcome.reason, PreSuspendReason::Completed, "{outcome:?}");
    assert_eq!(outcome.lane, Some(MaintenanceLane::IndexCompact));
    assert!(outcome.started);
    assert_eq!(target.runs(), 1);
    let counters = target.ledger.counters();
    assert_eq!(counters.executions, 1);
    assert_eq!(counters.completed, 1);
    assert_eq!(counters.failed, 0);
    assert_eq!(counters.refused, 0);
}

#[test]
fn a_suspend_pre_tidy_refused_by_the_budget_does_not_start_or_claim_completion() {
    let scheduler = pre_suspend_scheduler(true, 0, Duration::from_secs(1));
    let target = FakeTarget::new("suspend_refused", 4096);
    let outcome = pre_suspend(&scheduler, &target);
    assert_eq!(
        outcome.reason,
        PreSuspendReason::Refused(BudgetDenied::Bytes)
    );
    assert!(!outcome.started);
    assert_eq!(target.runs(), 0, "a refused unit must not reach the target");
    let counters = target.ledger.counters();
    assert_eq!(counters.executions, 0);
    assert_eq!(counters.completed, 0);
    assert_eq!(counters.refused, 1);
    assert_eq!(counters.blocks.get("budget"), Some(&1));
}

#[test]
fn a_timed_out_pre_tidy_is_unconfirmed_and_is_not_retried() {
    let scheduler = pre_suspend_scheduler(true, u64::MAX, Duration::from_millis(5));
    let target = FakeTarget::slow_failing("suspend_timeout", 4096, Duration::from_millis(30));
    let outcome = pre_suspend(&scheduler, &target);
    assert_eq!(outcome.reason, PreSuspendReason::Timeout, "{outcome:?}");
    assert!(
        outcome.started,
        "the entry point was reached, so the work is unconfirmed rather than absent"
    );
    assert!(outcome.elapsed >= Duration::from_millis(5));
    let counters = target.ledger.counters();
    assert_eq!(counters.executions, 1);
    assert_eq!(counters.failed, 1);
    assert_eq!(counters.completed, 0);

    // The pick opened the cooldown, so a second suspend does not silently retry the unit.
    let again = pre_suspend(&scheduler, &target);
    assert_eq!(again.reason, PreSuspendReason::Cooling, "{again:?}");
    assert_eq!(target.runs(), 1, "a timeout must not be retried in place");
}

#[test]
fn only_one_pre_tidy_per_database_is_in_flight() {
    let scheduler = pre_suspend_scheduler(true, u64::MAX, Duration::from_secs(2));
    let target = FakeTarget::slow("suspend_single", 4096, Duration::from_millis(500));
    let first = {
        let scheduler = scheduler.clone();
        let target = target.clone();
        std::thread::spawn(move || pre_suspend(&scheduler, &target))
    };
    // `run_maintenance` increments this before it sleeps, so the first call holds the
    // per-database slot by the time the second call starts.
    while target.runs() == 0 {
        std::thread::yield_now();
    }
    let second = pre_suspend(&scheduler, &target);
    assert_eq!(second.reason, PreSuspendReason::InFlight, "{second:?}");
    assert_eq!(first.join().unwrap().reason, PreSuspendReason::Completed);
    assert_eq!(target.runs(), 1);
}
