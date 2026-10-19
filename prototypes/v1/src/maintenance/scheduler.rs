//! The scheduler: probe, decide, charge, then run — in that order, and never on the request
//! path.
//!
//! `observe` is what a periodic driver calls. It is cheap when the switch is off (one static
//! read), and when it is on it only reads in-process figures and appends to a bounded queue;
//! it never runs a unit. `drain` is what executes queued units, on the scheduler's own thread
//! and under the budget.
//!
//! The split is the whole point of the module: a commit, a query or a session open cannot
//! reach `drain`, so no request ever waits for a checkpoint or a compaction. The tests assert
//! this by doing commits against a real database whose scheduler is armed and then finding
//! the ledger's attempt counter at zero.
//!
//! `drain` is also where a budget refusal becomes a *skip*: the unit is dropped from the
//! queue with a recorded reason and its backoff, the WAL is left alone, and nothing is
//! partially applied.
//!
//! `pre_suspend` is the one call that runs a unit on its caller's thread. It exists for the
//! unload path, where the caller has to know whether the single pre-tidy finished; it still
//! charges the same budget, opens the same cooldown and treats every refusal as a skip. The
//! ownership gate has already moved the lease to `Draining` by then, so the call carries a
//! thread-local retirement authorization that admits only its `validate_transaction(false)`
//! and leaves every epoch, role, deadline and quarantine check in place.

use std::cell::Cell;
use std::collections::{BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;

use super::budget::{BudgetDenied, MaintenanceBudget};
use super::policy::{MaintenanceDecision, MaintenanceLane, MaintenancePolicy, MaintenanceSkip};
use super::{BlockReason, MaintenanceTarget};

/// How often the driver thread looks at its queue. The queue is only ever filled by `observe`
/// on the watchdog, so this is a wake-up cadence, not a maintenance rate.
const DRIVER_INTERVAL: Duration = Duration::from_millis(50);

/// How many units a drain may run. One per drain keeps a slow unit from monopolising the
/// driver thread and makes the global concurrency budget the only way to have two in flight.
const UNITS_PER_DRAIN: usize = 1;

/// How long a suspend may spend on its pre-tidy. This is an experiment input, not a measured
/// value: it bounds the wait for the maintenance admission and the operation gate, and it
/// keeps a control-plane unload from inheriting an unbounded statement window. The switch is
/// off by default, so the default only applies when `PERI_MAINTENANCE_SCHEDULE=1`.
const DEFAULT_PRE_SUSPEND_BUDGET: Duration = Duration::from_secs(2);
const MAX_PRE_SUSPEND_BUDGET_MS: u64 = 30_000;
const PRE_SUSPEND_BUDGET_ENV: &str = "PERI_MAINTENANCE_SUSPEND_BUDGET_MS";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MaintenanceDrain {
    pub completed: usize,
    pub failed: usize,
    pub refused: usize,
}

/// What a suspend-time pre-tidy did, reported so the unload path can log the real outcome
/// instead of inferring one. `Timeout`, `BudgetExceeded` and `Failed` are deliberately
/// distinct from the skipped reasons: they mean the unit may have started, so they must never
/// be described as "nothing ran". `started` is the ledger's answer to whether the lane's entry
/// point was reached.
///
/// The two budget outcomes are not a hard cancellation: a synchronous native checkpoint
/// cannot be interrupted safely. They say what the caller actually observed:
///
/// - `CompletedOverBudget` means the unit returned `Ok` after the budget elapsed. The
///   completion is real; only the wait bound was missed.
/// - `BudgetExceeded` means the unit returned an error after the budget elapsed without the
///   admission deadline being the cause, so its completion is unconfirmed.
/// - `Timeout` means the admission/operation-gate deadline stopped the unit before a native
///   completion was observed; completion is likewise unconfirmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreSuspendReason {
    Disabled,
    InFlight,
    NothingToDo,
    Cooling,
    Refused(BudgetDenied),
    Timeout,
    BudgetExceeded,
    Failed,
    ProbeFailed,
    Completed,
    CompletedOverBudget,
}

impl PreSuspendReason {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::InFlight => "in_flight",
            Self::NothingToDo => "nothing_to_do",
            Self::Cooling => "cooldown",
            Self::Refused(_) => "budget_refused",
            Self::Timeout => "timeout_unconfirmed",
            Self::BudgetExceeded => "budget_exceeded_unconfirmed",
            Self::Failed => "failed_unconfirmed",
            Self::ProbeFailed => "probe_failed",
            Self::Completed => "completed",
            Self::CompletedOverBudget => "completed_over_budget",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreSuspendOutcome {
    pub(crate) reason: PreSuspendReason,
    pub(crate) lane: Option<MaintenanceLane>,
    pub(crate) started: bool,
    pub(crate) elapsed: Duration,
    pub(crate) detail: Option<String>,
}

impl PreSuspendOutcome {
    fn skipped(reason: PreSuspendReason, lane: Option<MaintenanceLane>, elapsed: Duration) -> Self {
        Self {
            reason,
            lane,
            started: false,
            elapsed,
            detail: None,
        }
    }
}

/// One pre-tidy per database, independent of the configured per-database concurrency. The
/// budget may admit two units to one database when an operator raises its limit, but a
/// suspend must still not race itself.
struct PreSuspendSlot<'scheduler> {
    active: &'scheduler Mutex<BTreeSet<String>>,
    identity: String,
}

impl Drop for PreSuspendSlot<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.identity);
    }
}

thread_local! {
    /// Set only while a suspend-time unit is running on this thread. The ownership gate
    /// consults it to allow the retirement pre-tidy's `validate_transaction(false)` while
    /// the lease is `Draining`. It never allows a transaction (`true`), and it does not
    /// bypass any epoch, generation, role, deadline, quarantine or operation-gate check.
    static RETIRING_MAINTENANCE: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn retiring_maintenance_authorized() -> bool {
    RETIRING_MAINTENANCE.get()
}

pub(crate) struct RetiringMaintenanceAuthorization {
    previous: bool,
    /// The authorization is a slot in the *current thread's* TLS. Dropping the guard on a
    /// different thread would restore that thread's previous value and leave the originating
    /// thread authorized, so the guard must not be movable across threads. `Rc` is the same
    /// non-`Send` marker `session::DeadlineScope` uses for its thread-local scope.
    thread: PhantomData<Rc<()>>,
}

impl RetiringMaintenanceAuthorization {
    pub(crate) fn enter() -> Self {
        Self {
            previous: RETIRING_MAINTENANCE.replace(true),
            thread: PhantomData,
        }
    }
}

impl Drop for RetiringMaintenanceAuthorization {
    fn drop(&mut self) {
        RETIRING_MAINTENANCE.set(self.previous);
    }
}

struct Queued {
    lane: MaintenanceLane,
    target: Arc<dyn MaintenanceTarget>,
    /// The backfill volume the decision was taken on. It is the charge against the byte
    /// budget, so a unit is never charged for a volume it was not planned around.
    bytes: u64,
}

pub(crate) struct MaintenanceScheduler {
    policy: MaintenancePolicy,
    budget: MaintenanceBudget,
    pre_suspend_budget: Duration,
    pre_suspend_active: Mutex<BTreeSet<String>>,
    queue: Mutex<VecDeque<Queued>>,
    started: AtomicBool,
    /// Shutdown handshake. `stopping` refuses new units; `active` counts units that have
    /// already crossed the start boundary, including one native unit that cannot be
    /// cancelled. `idle` wakes a quiesce once the count reaches zero.
    stopping: AtomicBool,
    active: Mutex<usize>,
    idle: Condvar,
}

/// Keeps an in-flight unit visible to `quiesce`. It is created while holding `active`, after
/// checking `stopping`, so a unit cannot start behind a quiesce that already observed zero.
struct ActiveUnit<'scheduler> {
    scheduler: &'scheduler MaintenanceScheduler,
}

impl Drop for ActiveUnit<'_> {
    fn drop(&mut self) {
        let mut active = self
            .scheduler
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = active.saturating_sub(1);
        self.scheduler.idle.notify_all();
    }
}

impl MaintenanceScheduler {
    pub(crate) fn configured() -> Result<Self> {
        let policy = MaintenancePolicy::configured()?;
        let budget = MaintenanceBudget::configured()?;
        // With the schedule off the unload path never calls `pre_suspend`, so the knob is not
        // consulted at all: a default deployment keeps exactly the old behaviour even if the
        // variable is present or malformed.
        let pre_suspend_budget = if policy.enabled() {
            pre_suspend_budget_setting()?
        } else {
            DEFAULT_PRE_SUSPEND_BUDGET
        };
        Ok(Self::with_pre_suspend_budget(
            policy,
            budget,
            pre_suspend_budget,
        ))
    }

    /// Only tests construct a scheduler from a policy directly; the worker uses `configured`.
    #[cfg(test)]
    pub(crate) fn new(policy: MaintenancePolicy, budget: MaintenanceBudget) -> Self {
        Self::with_pre_suspend_budget(policy, budget, DEFAULT_PRE_SUSPEND_BUDGET)
    }

    fn with_pre_suspend_budget(
        policy: MaintenancePolicy,
        budget: MaintenanceBudget,
        pre_suspend_budget: Duration,
    ) -> Self {
        Self {
            policy,
            budget,
            pre_suspend_budget,
            pre_suspend_active: Mutex::new(BTreeSet::new()),
            queue: Mutex::new(VecDeque::new()),
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            active: Mutex::new(0),
            idle: Condvar::new(),
        }
    }

    /// The production timeout is read from the environment; tests need a constructor that can
    /// arm a timeout short enough to observe without sleeping for the default.
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        policy: MaintenancePolicy,
        budget: MaintenanceBudget,
        pre_suspend_budget: Duration,
    ) -> Self {
        Self::with_pre_suspend_budget(policy, budget, pre_suspend_budget)
    }

    /// A scheduler whose checkpoint lane is due `period` after the database opened and whose
    /// compaction lane never arms. This is what lets a test drive the watchdog wiring — tick,
    /// queue, driver thread, real entry point — without waiting for the production cadence.
    #[cfg(test)]
    pub(crate) fn for_tests(period: Duration) -> Self {
        use super::policy::Watermark;

        Self::new(
            MaintenancePolicy::for_tests(
                true,
                Watermark {
                    low_bytes: 0,
                    high_bytes: 1,
                    period,
                    phase: Duration::ZERO,
                },
                Watermark {
                    low_bytes: 0,
                    high_bytes: u64::MAX,
                    period: Duration::from_secs(3600),
                    phase: Duration::ZERO,
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            MaintenanceBudget::new(1, 1, 64, 64 * 1024 * 1024, Duration::from_secs(60)),
        )
    }

    /// A scheduler for the unload-path tests: the switch and both lanes are real, and the
    /// pre-tidy timeout is short enough to observe without waiting for the production value.
    #[cfg(test)]
    pub(crate) fn for_suspend_tests(
        enabled: bool,
        budget: MaintenanceBudget,
        pre_suspend_budget: Duration,
    ) -> Self {
        use super::policy::Watermark;

        Self::with_pre_suspend_budget(
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
                Duration::from_millis(1),
                Duration::from_millis(1),
            ),
            budget,
            pre_suspend_budget,
        )
    }

    pub(crate) fn enabled(&self) -> bool {
        self.policy.enabled()
    }

    #[cfg(test)]
    pub(crate) fn policy(&self) -> &MaintenancePolicy {
        &self.policy
    }

    /// Refuses new units and waits, until `deadline`, for the one already running to reach
    /// its safe boundary. The native unit itself cannot be cancelled: the scheduler only
    /// stops admitting work and reports whether the last unit finished inside the budget.
    pub(crate) fn quiesce(&self, deadline: Instant) -> Result<()> {
        let started = Instant::now();
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Set the flag while holding the same lock `begin_unit` uses: a unit that has not
        // observed `stopping` yet either has already incremented `active` (and is waited
        // for) or will refuse to start.
        self.stopping.store(true, Ordering::SeqCst);
        while *active > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                anyhow::bail!(
                    "maintenance_in_flight units={} waited_ms={}",
                    *active,
                    started.elapsed().as_millis()
                );
            }
            let (guard, timeout) = self
                .idle
                .wait_timeout(active, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active = guard;
            if timeout.timed_out() && *active > 0 {
                anyhow::bail!(
                    "maintenance_in_flight units={} waited_ms={}",
                    *active,
                    started.elapsed().as_millis()
                );
            }
        }
        Ok(())
    }

    fn begin_unit(&self) -> Option<ActiveUnit<'_>> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.stopping.load(Ordering::SeqCst) {
            return None;
        }
        *active += 1;
        Some(ActiveUnit { scheduler: self })
    }

    /// One scheduling pass over one database. Callers pass every loaded database from a
    /// periodic driver that is not serving requests.
    ///
    /// This only decides and queues: it never runs a unit, and it never starts the driver
    /// thread. A test can therefore drain the same queue deterministically, and a worker that
    /// schedules without an executor cannot have one appear underneath it.
    pub(crate) fn observe(&self, target: &Arc<dyn MaintenanceTarget>, now: Instant) -> Result<()> {
        if !self.policy.enabled() || self.stopping.load(Ordering::SeqCst) {
            return Ok(());
        }
        let ledger = target.ledger();
        let signals = target.signals()?;
        ledger.observe_reader_age(Duration::from_millis(signals.oldest_reader_ms));
        for lane in MaintenanceLane::ALL {
            if let Some(armed) = self.policy.arming(lane, signals.wal_bytes) {
                ledger.set_armed(lane, armed);
            }
        }
        for lane in MaintenanceLane::ALL {
            match self.policy.decide(lane, &signals, ledger, now) {
                MaintenanceDecision::Due => {
                    // The cooldown window opens at the *pick*, not at completion: a unit that
                    // outlives its cooldown must not be picked twice, and a unit refused by
                    // the budget must not be retried on the next tick.
                    ledger.record_start(lane, now, self.policy.cooldown());
                    self.lock().push_back(Queued {
                        lane,
                        target: target.clone(),
                        bytes: signals.backfill_bytes(),
                    });
                }
                MaintenanceDecision::Skip(skip) => {
                    if skip == MaintenanceSkip::UnconfirmedWal {
                        ledger.record_block(BlockReason::UnconfirmedWal, None);
                    }
                }
            }
        }
        Ok(())
    }

    /// Runs at most `max_units` queued units. Public so a test can drive the same code path
    /// the driver thread drives, without owning a thread.
    pub(crate) fn drain(&self, max_units: usize, now: Instant) -> MaintenanceDrain {
        let mut summary = MaintenanceDrain::default();
        for _ in 0..max_units {
            if self.stopping.load(Ordering::SeqCst) {
                break;
            }
            let Some(queued) = self.lock().pop_front() else {
                break;
            };
            let Some(_active) = self.begin_unit() else {
                // Shutdown started between the pop and the start boundary. The unit is
                // dropped rather than run behind a quiesce that already observed zero;
                // nothing was executed, so there is nothing to prove.
                break;
            };
            let ledger = queued.target.ledger();
            let lease = match self
                .budget
                .acquire(&queued.target.identity(), queued.bytes, now)
            {
                Ok(lease) => lease,
                Err(denied) => {
                    // Budget exhausted: limit new work. The unit is skipped as a whole, the
                    // backoff is recorded, and the WAL it would have reclaimed stays put.
                    ledger.record_block(BlockReason::Budget, Some(self.policy.blocked_until(now)));
                    ledger.record_refused(queued.lane);
                    crate::work_metrics::observe_maintenance_refused(denied);
                    summary.refused += 1;
                    continue;
                }
            };
            let started = Instant::now();
            match queued.target.run_maintenance(queued.lane) {
                // The unit records its own duration through `work_metrics`, so the admin
                // entry points and the scheduled ones are measured on one caliber and no
                // unit is counted twice.
                Ok(()) => {
                    ledger.record_completion(queued.lane);
                    summary.completed += 1;
                }
                Err(error) => {
                    ledger.record_failure(queued.lane);
                    summary.failed += 1;
                    // A blocked unit is not a silent one: the reason is in the ledger and on
                    // stderr, and the cooldown keeps the next attempt from being immediate.
                    eprintln!(
                        "maintenance outcome=blocked lane={} elapsed_ms={} reason={error:#}",
                        queued.lane.name(),
                        started.elapsed().as_millis(),
                    );
                }
            }
            drop(lease);
        }
        summary
    }

    /// One pre-tidy immediately before a database is retired.
    ///
    /// This runs on the unload caller's thread, unlike `observe`/`drain`, because a suspend
    /// has to know whether the one unit it asked for finished. `pre_suspend_budget` is
    /// installed as the ambient request deadline. It bounds the admission and operation-gate
    /// checks that consult that deadline; it is **not** a hard upper bound on the whole call.
    /// A native checkpoint or compaction that has already been dispatched, ordinary lock
    /// acquisition, and `finish_maintenance` cleanup retries (which can add up to their own
    /// 20-second cleanup deadline) are not all interruptible by it. The unit is never
    /// detached, so its native dispatch keeps the fencing and deadline semantics it already
    /// had instead of running on after the instance has been retired; the outcome reports
    /// whether it completed inside or after the budget and whether a completion is
    /// unconfirmed.
    ///
    /// Nothing here is on the correctness path: a refusal, a timeout or a failure leaves the
    /// WAL where it is and returns an outcome the caller only logs. The caller must still
    /// proceed with the unload. The lane's cooldown is opened at the pick, so neither this
    /// path nor the periodic driver retries immediately, and a lane that the driver already
    /// queued makes this call a no-op instead of stacking a second backfill.
    pub(crate) fn pre_suspend(&self, target: &Arc<dyn MaintenanceTarget>) -> PreSuspendOutcome {
        if !self.policy.enabled() {
            return PreSuspendOutcome::skipped(PreSuspendReason::Disabled, None, Duration::ZERO);
        }
        let started = Instant::now();
        let identity = target.identity();
        let Some(_slot) = self.begin_pre_suspend(identity.clone()) else {
            return PreSuspendOutcome::skipped(PreSuspendReason::InFlight, None, started.elapsed());
        };
        let signals = match target.signals() {
            Ok(signals) => signals,
            Err(error) => {
                return PreSuspendOutcome {
                    reason: PreSuspendReason::ProbeFailed,
                    lane: None,
                    started: false,
                    elapsed: started.elapsed(),
                    detail: Some(format!("{error:#}")),
                };
            }
        };
        // A settled WAL is compacted; an unsettled one still allows a Full checkpoint,
        // which is the unit that makes the append and its publication current. The
        // compaction precondition is left to the unit itself as well, so a race between the
        // probe and dispatch is still refused before the WAL index is touched.
        let lane = if signals.unconfirmed_wal {
            MaintenanceLane::Checkpoint
        } else {
            MaintenanceLane::IndexCompact
        };
        if signals.wal_bytes == 0 {
            return PreSuspendOutcome::skipped(
                PreSuspendReason::NothingToDo,
                Some(lane),
                started.elapsed(),
            );
        }
        let ledger = target.ledger();
        // The driver's pick already opened this window, and a pick that was refused by the
        // budget did too. Skipping here is what keeps a suspend from stacking on top of a
        // queued or in-flight periodic unit.
        if ledger
            .next_allowed(lane)
            .is_some_and(|allowed| started < allowed)
        {
            return PreSuspendOutcome::skipped(
                PreSuspendReason::Cooling,
                Some(lane),
                started.elapsed(),
            );
        }
        ledger.record_start(lane, started, self.policy.cooldown());
        let lease = match self
            .budget
            .acquire(&identity, signals.backfill_bytes(), started)
        {
            Ok(lease) => lease,
            Err(denied) => {
                ledger.record_block(
                    BlockReason::Budget,
                    Some(self.policy.blocked_until(started)),
                );
                ledger.record_refused(lane);
                crate::work_metrics::observe_maintenance_refused(denied);
                return PreSuspendOutcome {
                    reason: PreSuspendReason::Refused(denied),
                    lane: Some(lane),
                    started: false,
                    elapsed: started.elapsed(),
                    detail: Some(denied.name().to_owned()),
                };
            }
        };
        let executions_before = ledger.executions(lane);
        let timeout = self.pre_suspend_budget;
        let deadline = started + timeout;
        let result = {
            let _deadline = crate::session::RequestDeadline::scope(deadline);
            // The unload path has already moved the lease to `Draining` when this unit runs.
            // A database transaction was refused there, but this unit is not a new
            // transaction: it is the retirement pre-tidy, and it still goes through the same
            // epoch, Writer-role, generation, quarantine and operation-gate checks. The
            // authorization is thread-local and lives only for this call.
            let _retiring = RetiringMaintenanceAuthorization::enter();
            target.run_maintenance(lane)
        };
        let elapsed = started.elapsed();
        let over_budget = elapsed >= timeout;
        // The entry point records its own execution before it takes a deadline. Reading the
        // counter here is what lets the caller say "started, completion unconfirmed" instead
        // of the false claim that a timed-out unit never touched the database.
        let unit_started = ledger.executions(lane) > executions_before;
        drop(lease);
        match result {
            Ok(()) => {
                ledger.record_completion(lane);
                PreSuspendOutcome {
                    // A native completion after the budget is still a completion. Reporting
                    // it as `Completed` would hide that the caller waited past its bound;
                    // reporting it as `Timeout` would deny work that actually happened.
                    reason: if over_budget {
                        PreSuspendReason::CompletedOverBudget
                    } else {
                        PreSuspendReason::Completed
                    },
                    lane: Some(lane),
                    started: unit_started,
                    elapsed,
                    detail: None,
                }
            }
            Err(error) => {
                let deadline_limited = error.downcast_ref::<crate::session::AdmissionError>()
                    == Some(&crate::session::AdmissionError::Deadline);
                ledger.record_failure(lane);
                PreSuspendOutcome {
                    reason: if deadline_limited {
                        PreSuspendReason::Timeout
                    } else if over_budget {
                        PreSuspendReason::BudgetExceeded
                    } else {
                        PreSuspendReason::Failed
                    },
                    lane: Some(lane),
                    started: unit_started,
                    elapsed,
                    detail: Some(format!("{error:#}")),
                }
            }
        }
    }

    fn begin_pre_suspend(&self, identity: String) -> Option<PreSuspendSlot<'_>> {
        let mut active = self
            .pre_suspend_active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(identity.clone()) {
            return None;
        }
        drop(active);
        Some(PreSuspendSlot {
            active: &self.pre_suspend_active,
            identity,
        })
    }

    /// Starts the driver thread once. The owner calls this; `observe` deliberately does not,
    /// so a caller always knows whether an executor exists.
    ///
    /// The thread holds a weak reference, so it exits by itself when the runtime that owns the
    /// scheduler is dropped and never keeps a database alive.
    pub(crate) fn start(self: &Arc<Self>) -> Result<()> {
        if !self.policy.enabled() || self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("peri-maintenance".to_owned())
            .spawn(move || {
                while let Some(scheduler) = weak.upgrade() {
                    if scheduler.stopping.load(Ordering::SeqCst) {
                        return;
                    }
                    scheduler.drain(UNITS_PER_DRAIN, Instant::now());
                    std::thread::sleep(DRIVER_INTERVAL);
                }
            })
            .map(|_| ())
            .map_err(|error| {
                self.started.store(false, Ordering::SeqCst);
                error.into()
            })
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<Queued>> {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn pre_suspend_budget_setting() -> Result<Duration> {
    use anyhow::{ensure, Context};

    match std::env::var(PRE_SUSPEND_BUDGET_ENV) {
        Ok(value) => {
            let millis = value
                .parse::<u64>()
                .with_context(|| format!("invalid maintenance {PRE_SUSPEND_BUDGET_ENV}"))?;
            ensure!(
                millis > 0 && millis <= MAX_PRE_SUSPEND_BUDGET_MS,
                "invalid maintenance {PRE_SUSPEND_BUDGET_ENV}"
            );
            Ok(Duration::from_millis(millis))
        }
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_PRE_SUSPEND_BUDGET),
        Err(error) => Err(error.into()),
    }
}
