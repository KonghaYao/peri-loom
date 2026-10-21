use super::{WorkerLimits, WorkerRuntime};
use crate::maintenance::{MaintenanceBudget, MaintenanceScheduler};
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use crate::provider::LocalAuthorityProvider;
use crate::runtime::{ActivationPhase, DatabaseId};
use crate::session::{AdmissionError, Budget, RequestDeadline, SqlEvidence, UnsettledSqlOutcome};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Holds reads only once the load has entered `RemoteDatabase::open`. Holding every read
/// would make the load overrun at the last boundary *before* the native open, which is a
/// different failure than the one these tests measure: the authority reads before the
/// boundary must stay fast, so the overrun is paid by the opener inside the native call.
fn hold_during_native_open(
    store: &Arc<SlowStore>,
    runtime: &Arc<WorkerRuntime>,
    database: &DatabaseId,
) {
    let weak_store = Arc::downgrade(store);
    let weak_runtime = Arc::downgrade(runtime);
    let database = database.clone();
    store.on_get(move |_| {
        let Some(runtime) = weak_runtime.upgrade() else {
            return;
        };
        let phase = runtime
            .activation_snapshot(&database)
            .ok()
            .flatten()
            .map(|activation| activation.phase);
        if phase == Some(ActivationPhase::OpeningDatabase) {
            if let Some(store) = weak_store.upgrade() {
                store.hold(true);
            }
        }
    });
}

/// A store whose reads can be held. A cold load pays one remote round trip per WAL chunk,
/// so this is what makes a load outlast any statement budget. A held read announces itself
/// before it sleeps, which is what lets a test open a second session mid-load.
type ReadHook = Box<dyn Fn(&str) + Send>;

struct SlowStore {
    inner: MemoryObjectStore,
    delay: Duration,
    held: AtomicBool,
    started: Mutex<usize>,
    progress: Condvar,
    /// Runs on every read. The drain-during-open test uses it to retire the activation from
    /// the read that happens inside `RemoteDatabase::open`, which is the phase no boundary
    /// check can reach.
    hook: Mutex<Option<ReadHook>>,
}

impl SlowStore {
    fn new(delay: Duration) -> Self {
        Self {
            inner: MemoryObjectStore::default(),
            delay,
            held: AtomicBool::new(false),
            started: Mutex::new(0),
            progress: Condvar::new(),
            hook: Mutex::new(None),
        }
    }

    fn hold(&self, held: bool) {
        self.held.store(held, Ordering::SeqCst);
    }

    fn on_get(&self, hook: impl Fn(&str) + Send + 'static) {
        *self.hook.lock().unwrap() = Some(Box::new(hook));
    }

    fn off_get(&self) {
        *self.hook.lock().unwrap() = None;
    }

    /// Waits for a held read to start. The load registers its `Loading` entry before it reads
    /// anything, so a load observed here is one that a second open has to coalesce with. The
    /// timeout keeps a load that never reaches the store from hanging the test.
    fn wait_for_read(&self) {
        let started = self.started.lock().unwrap();
        let (started, timeout) = self
            .progress
            .wait_timeout_while(started, Duration::from_secs(10), |started| *started == 0)
            .unwrap();
        assert!(*started > 0 && !timeout.timed_out(), "the load never read");
    }
}

impl ObjectStore for SlowStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if let Some(hook) = self.hook.lock().unwrap().as_ref() {
            hook(key);
        }
        if self.held.load(Ordering::SeqCst) {
            *self.started.lock().unwrap() += 1;
            self.progress.notify_all();
            std::thread::sleep(self.delay);
        }
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn identifier(database: &str) -> Result<DatabaseId> {
    DatabaseId::new(Namespace::new("load_budget", database)?, "main")
}

/// Joins a thread with a deadline. A regression that leaves a coalesced open waiting for a verdict
/// its loader never publishes would otherwise hang the test instead of failing it. The deadline is
/// generous because a held store sleeps on every read, so this test's load legitimately takes tens
/// of seconds; only a load that never returns at all is meant to trip it.
fn join_within<T>(handle: std::thread::JoinHandle<T>, what: &str) -> T {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !handle.is_finished() {
        assert!(Instant::now() < deadline, "{what} never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
    handle.join().unwrap()
}

fn fixture(delay: Duration) -> Result<(tempfile::TempDir, Arc<SlowStore>, Arc<WorkerRuntime>)> {
    let cache = tempfile::tempdir()?;
    let store = Arc::new(SlowStore::new(delay));
    let runtime = WorkerRuntime::new(
        Arc::new(LocalAuthorityProvider::new(store.clone())),
        cache.path(),
        WorkerLimits::default(),
    )?;
    Ok((cache, store, runtime))
}

/// The bit the RPC reply turns into `execution_completed = false`. Without it the platform
/// can only report an unknown outcome, which a caller cannot tell from a write that may
/// have committed.
fn proven_not_executed(error: &anyhow::Error) -> bool {
    error.downcast_ref::<UnsettledSqlOutcome>().is_none()
        && error.downcast_ref::<SqlEvidence>() == Some(&SqlEvidence::NotExecuted)
}

#[test]
fn the_session_budget_is_anchored_after_the_load() -> Result<()> {
    let (_cache, _store, runtime) = fixture(Duration::from_millis(1))?;
    let database = identifier("anchor")?;
    // The request deadline the RPC server installs, with a load already spent against it:
    // every deadline taken inside the open was clamped to it, so the session could not be
    // created once the load had outlived the statement budget.
    let deadline = RequestDeadline::scope(Instant::now());
    let opened = runtime.open(database, 1);
    drop(deadline);
    let handle = opened?;
    assert!(!handle.in_transaction()?);
    Ok(())
}

#[test]
fn a_load_that_overruns_its_budget_fails_honestly_and_keeps_the_instance() -> Result<()> {
    let (_cache, store, mut runtime) = fixture(Duration::from_millis(60))?;
    let database = identifier("overrun")?;
    let handle = runtime.open(database.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(7)")?;
    drop(handle);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?, vec![database.clone()]);
    Arc::get_mut(&mut runtime).unwrap().load_budget = Some(Duration::from_millis(20));
    hold_during_native_open(&store, &runtime, &database);
    let error = match runtime.open(database.clone(), 1) {
        Ok(_) => panic!("a load over the budget must not open a session"),
        Err(error) => error,
    };
    assert!(
        format!("{error:#}").contains("database load budget exceeded"),
        "{error:#}"
    );
    assert!(proven_not_executed(&error), "{error:#}");
    let status = runtime.status()?;
    assert_eq!(
        status.loaded_databases, 1,
        "the loaded instance was dropped"
    );
    assert_eq!(status.active_sessions, 0);
    // The load is not paid twice: the same budget against the same held store would fail
    // again if this open had to load the database.
    let handle = runtime.open(database, 1)?;
    assert_eq!(
        handle.query("SELECT value FROM data")?[0][0].to_string(),
        "7"
    );
    Ok(())
}

#[test]
fn the_default_load_budget_covers_a_cold_load() -> Result<()> {
    let (_cache, _store, runtime) = fixture(Duration::from_millis(1))?;
    // The measured cold load that made this budget necessary took 16.6 s.
    assert!(runtime.load_budget() >= Duration::from_secs(60));
    assert!(runtime.load_budget() >= Budget::compatibility().wall_duration());
    Ok(())
}

/// The load budget judges the open that paid for the load. An open that coalesces with it runs
/// no load of its own, so it must not be failed for the overrun, and it must not be left waiting
/// for a verdict the overrun path never publishes.
#[test]
fn concurrent_openers_split_on_the_load_budget() -> Result<()> {
    let (_cache, store, mut runtime) = fixture(Duration::from_millis(250))?;
    // Wide enough that the fast authority reads before `RemoteDatabase::open` cannot trip a
    // boundary, far below the delay the held reads put inside the native open.
    Arc::get_mut(&mut runtime).unwrap().load_budget = Some(Duration::from_millis(50));
    let database = identifier("split")?;
    hold_during_native_open(&store, &runtime, &database);

    let loader = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };
    store.wait_for_read();
    let waiter = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };

    let error = match join_within(loader, "the overrunning load") {
        Ok(_) => panic!("the open that paid the overrunning load must report it"),
        Err(error) => error,
    };
    assert!(
        format!("{error:#}").contains("database load budget exceeded"),
        "{error:#}"
    );
    assert!(proven_not_executed(&error), "{error:#}");
    let handle = join_within(waiter, "the coalesced open")
        .expect("an open that coalesced with the load reuses the resident instance");
    assert!(!handle.in_transaction()?);
    drop(handle);

    let status = runtime.status()?;
    assert_eq!(
        status.loaded_databases, 1,
        "the loaded instance was dropped"
    );
    assert_eq!(status.active_sessions, 0);
    // The overrun is not paid twice: the next open finds the retained instance.
    let reused = runtime.open(database, 1)?;
    assert!(!reused.in_transaction()?);
    Ok(())
}

#[test]
fn the_configured_load_budget_is_validated_and_absent_by_default() {
    assert_eq!(super::configured_load_budget(None).unwrap(), None);
    assert_eq!(
        super::configured_load_budget(Some("1500")).unwrap(),
        Some(Duration::from_millis(1500))
    );
    for invalid in ["0", "86400001", "-1", "1s", ""] {
        assert!(
            super::configured_load_budget(Some(invalid)).is_err(),
            "{invalid}"
        );
    }
}

/// The watchdog path, end to end: a tick sees the loaded database, queues the checkpoint
/// lane, starts the scheduler's own thread once, and the unit runs through the real
/// `RemoteDatabase` entry point without the test ever calling `drain`. The repeated ticks
/// also pin that a tick only decides: the cooldown keeps a second unit from starting, so
/// the execution counter stays at one while the test keeps sweeping.
#[test]
fn the_watchdog_starts_one_driver_that_runs_the_queued_unit() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance = Arc::new(
        crate::maintenance::MaintenanceScheduler::for_tests(Duration::from_millis(10)),
    );
    let database = identifier("scheduled")?;
    let handle = runtime.open(database, 1)?;
    handle.execute("CREATE TABLE data(value)")?;
    handle.execute("INSERT INTO data VALUES(1)")?;

    let deadline = Instant::now() + Duration::from_secs(10);
    let counters = loop {
        runtime.maintenance_tick()?;
        let counters = handle.diagnostics()?["maintenance"].clone();
        let finished =
            counters["completed"].as_u64().unwrap_or(0) + counters["failed"].as_u64().unwrap_or(0);
        if finished >= 1 {
            break counters;
        }
        assert!(
            Instant::now() < deadline,
            "the scheduler thread never ran the queued checkpoint: {counters}"
        );
        std::thread::sleep(Duration::from_millis(10));
    };

    assert_eq!(counters["executions"], 1);
    assert_eq!(counters["completed"], 1, "{counters}");
    assert_eq!(counters["failed"], 0, "{counters}");
    // Let the driver thread perform several more drains; the cooldown is the only thing
    // that may keep the next checkpoint from starting, and it is still active.
    for _ in 0..3 {
        std::thread::sleep(Duration::from_millis(60));
        runtime.maintenance_tick()?;
    }
    let settled = handle.diagnostics()?["maintenance"].clone();
    assert_eq!(settled["executions"], 1, "{settled}");
    assert_eq!(
        handle.query("SELECT value FROM data")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

fn suspend_scheduler(
    enabled: bool,
    bytes_per_window: u64,
    timeout: Duration,
) -> Arc<MaintenanceScheduler> {
    Arc::new(MaintenanceScheduler::for_suspend_tests(
        enabled,
        MaintenanceBudget::new(1, 1, 16, bytes_per_window, Duration::from_secs(60)),
        timeout,
    ))
}

fn loaded_database(
    runtime: &WorkerRuntime,
    identifier: &DatabaseId,
) -> Arc<crate::database::RemoteDatabase> {
    runtime
        .native_instances
        .lock()
        .unwrap()
        .iter()
        .find(|(id, _)| id == identifier)
        .and_then(|(_, database)| database.upgrade())
        .expect("the database must be resident")
}

#[test]
fn a_suspend_with_the_schedule_off_does_not_run_maintenance() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance =
        suspend_scheduler(false, u64::MAX, Duration::from_secs(1));
    let identifier = identifier("suspend_off")?;
    let handle = runtime.open(identifier.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    let database = loaded_database(&runtime, &identifier);
    drop(handle);
    assert!(runtime.drain(&identifier)?);
    let counters = database.maintenance_ledger().counters();
    assert_eq!(counters.attempts, 0);
    assert_eq!(counters.executions, 0);
    assert_eq!(counters.completed, 0);
    Ok(())
}

#[test]
fn a_suspend_runs_one_pre_tidy_inside_the_budget() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance =
        suspend_scheduler(true, u64::MAX, Duration::from_secs(2));
    let identifier = identifier("suspend_ok")?;
    let handle = runtime.open(identifier.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    let database = loaded_database(&runtime, &identifier);
    assert!(database.io().maintenance_signals()?.wal_bytes > 0);
    drop(handle);
    assert!(runtime.drain(&identifier)?);
    let counters = database.maintenance_ledger().counters();
    assert_eq!(counters.executions, 1);
    assert_eq!(counters.completed, 1);
    assert_eq!(counters.failed, 0);
    drop(database);

    // The unload still completed, and the checkpoint/compaction did not change the rows.
    let reopened = runtime.open(identifier.clone(), 1)?;
    assert_eq!(
        reopened.query("SELECT value FROM data")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

#[test]
fn a_budget_refusal_does_not_block_the_suspend() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance =
        suspend_scheduler(true, 0, Duration::from_secs(1));
    let identifier = identifier("suspend_refused")?;
    let handle = runtime.open(identifier.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    let database = loaded_database(&runtime, &identifier);
    assert!(database.io().maintenance_signals()?.wal_bytes > 0);
    drop(handle);
    assert!(runtime.drain(&identifier)?);
    let counters = database.maintenance_ledger().counters();
    assert_eq!(counters.executions, 0);
    assert_eq!(counters.completed, 0);
    assert_eq!(counters.refused, 1);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    Ok(())
}

#[test]
fn a_timed_out_pre_tidy_does_not_block_the_suspend_or_claim_completion() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance =
        suspend_scheduler(true, u64::MAX, Duration::ZERO);
    let identifier = identifier("suspend_timeout")?;
    let handle = runtime.open(identifier.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    let database = loaded_database(&runtime, &identifier);
    drop(handle);
    assert!(runtime.drain(&identifier)?);
    let counters = database.maintenance_ledger().counters();
    assert_eq!(
        counters.executions, 1,
        "the unit reached its entry point before the deadline"
    );
    assert_eq!(counters.completed, 0, "a timeout is not a completion");
    assert_eq!(counters.failed, 1);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    Ok(())
}

#[test]
fn a_draining_database_runs_the_pre_tidy_when_the_last_session_leaves() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance =
        suspend_scheduler(true, u64::MAX, Duration::from_secs(2));
    let identifier = identifier("suspend_release")?;
    let handle = runtime.open(identifier.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    let database = loaded_database(&runtime, &identifier);

    assert!(runtime.drain(&identifier)?);
    assert_eq!(
        database.maintenance_ledger().counters().executions,
        0,
        "a database with a session is only marked draining"
    );
    drop(handle);
    let counters = database.maintenance_ledger().counters();
    assert_eq!(counters.executions, 1);
    assert_eq!(counters.completed, 1);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    Ok(())
}

#[test]
fn repeated_drain_runs_the_pre_tidy_once() -> Result<()> {
    let (_cache, _store, mut runtime) = fixture(Duration::from_millis(1))?;
    Arc::get_mut(&mut runtime).unwrap().maintenance =
        suspend_scheduler(true, u64::MAX, Duration::from_secs(2));
    let identifier = identifier("suspend_repeat")?;
    let handle = runtime.open(identifier.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    let database = loaded_database(&runtime, &identifier);
    drop(handle);

    assert!(runtime.drain(&identifier)?);
    assert!(
        !runtime.drain(&identifier)?,
        "the retired instance is gone, so a repeated suspend is a no-op"
    );
    let counters = database.maintenance_ledger().counters();
    assert_eq!(counters.executions, 1, "a repeated suspend must not re-run");
    assert_eq!(counters.completed, 1);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    Ok(())
}

fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "{what} never happened");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The waiter deadline is the caller's, not the load's. A caller that leaves at its own
/// deadline must not cancel the load the other callers are waiting on, and its timeout must
/// stay distinguishable from a load failure.
#[test]
fn a_timed_out_waiter_leaves_the_queue_and_another_waiter_gets_the_same_load() -> Result<()> {
    let (_cache, store, runtime) = fixture(Duration::from_millis(120))?;
    let database = identifier("waiter_timeout")?;
    store.hold(true);

    let loader = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };
    store.wait_for_read();

    let timed_out = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || {
            runtime.open_until(
                database,
                1,
                Some(Instant::now() + Duration::from_millis(25)),
            )
        })
    };
    let error = match join_within(timed_out, "the timed-out waiter") {
        Ok(_) => panic!("a waiter that reaches its deadline must not open a session"),
        Err(error) => error,
    };
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline),
        "{error:#}"
    );
    assert!(
        format!("{error:#}").contains("timed out waiting for database activation"),
        "{error:#}"
    );

    // The timeout is not a load verdict: the shared load is still running, and the wait
    // queue no longer holds the caller that left.
    let status = runtime.status()?;
    assert_eq!(status.loading_databases, 1, "{status:?}");
    assert_eq!(
        status.active_sessions, 1,
        "the timed-out waiter leaked a session slot"
    );
    let activation = runtime
        .activation_snapshot(&database)?
        .expect("the loading entry publishes its activation");
    assert_eq!(activation.epoch, 1);
    assert_eq!(activation.waiters, 0, "the timed-out waiter stayed queued");

    let second = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || runtime.open_until(database, 1, None))
    };
    let handle = join_within(second, "the second waiter")
        .expect("a second waiter must receive the same load result");
    assert!(!handle.in_transaction()?);
    drop(handle);
    let loaded = join_within(loader, "the shared load")
        .expect("a waiter deadline must not cancel the shared load");
    assert!(!loaded.in_transaction()?);
    drop(loaded);

    let status = runtime.status()?;
    assert_eq!(status.loaded_databases, 1);
    assert_eq!(status.loading_databases, 0);
    assert_eq!(status.active_sessions, 0);
    let activation = runtime
        .activation_snapshot(&database)?
        .expect("the retained instance carries its activation");
    assert_eq!(activation.phase, ActivationPhase::Ready);
    assert_eq!(activation.waiters, 0);
    assert!(
        activation.replayed_bytes.is_some(),
        "a completed activation reports what the open replayed"
    );
    Ok(())
}

/// A load budget missed at a phase boundary fails before a native open publishes anything:
/// the entry is removed, no waiter is left behind, and the failure is a proven non-execution.
#[test]
fn a_load_that_misses_its_budget_at_a_boundary_fails_without_publishing() -> Result<()> {
    let (_cache, store, mut runtime) = fixture(Duration::from_millis(150))?;
    Arc::get_mut(&mut runtime).unwrap().load_budget = Some(Duration::from_millis(40));
    let database = identifier("boundary_budget")?;
    store.hold(true);

    let error = match runtime.open(database.clone(), 1) {
        Ok(_) => panic!("a load over the budget must not open a session"),
        Err(error) => error,
    };
    assert!(
        format!("{error:#}").contains("database load budget exceeded"),
        "{error:#}"
    );
    assert!(proven_not_executed(&error), "{error:#}");

    let status = runtime.status()?;
    assert_eq!(
        status.loading_databases, 0,
        "the failed load stayed Loading"
    );
    assert_eq!(status.loaded_databases, 0);
    assert_eq!(status.active_sessions, 0);
    assert!(runtime.activation_snapshot(&database)?.is_none());
    Ok(())
}

/// Cold loads run under an explicit process-wide bound. A queued caller that reaches its
/// own deadline leaves the queue instead of occupying a slot; the load it queued behind is
/// untouched.
#[test]
fn cold_loads_are_bounded_and_a_queued_waiter_leaves_at_its_deadline() -> Result<()> {
    let (_cache, store, mut runtime) = fixture(Duration::from_millis(120))?;
    Arc::get_mut(&mut runtime).unwrap().load_permits = super::LoadPermits::configured(1, 8);
    let first = identifier("cold_slot_a")?;
    let second = identifier("cold_slot_b")?;
    store.hold(true);

    let loader = {
        let runtime = runtime.clone();
        let database = first.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };
    store.wait_for_read();
    let queued = {
        let runtime = runtime.clone();
        let database = second.clone();
        std::thread::spawn(move || {
            runtime.open_until(
                database,
                1,
                Some(Instant::now() + Duration::from_millis(30)),
            )
        })
    };
    let error = match join_within(queued, "the queued load") {
        Ok(_) => panic!("a queued load that misses its deadline must not start"),
        Err(error) => error,
    };
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline),
        "{error:#}"
    );
    assert!(
        format!("{error:#}").contains("timed out waiting for a cold load slot"),
        "{error:#}"
    );
    assert_eq!(runtime.load_permits.peak(), 1, "two loads ran at once");
    assert_eq!(
        runtime.load_permits.waiting(),
        0,
        "the timed-out caller stayed queued"
    );

    let handle = join_within(loader, "the first cold load")
        .expect("the admitted load must not be affected by a queued timeout");
    assert!(!handle.in_transaction()?);
    drop(handle);
    let status = runtime.status()?;
    assert_eq!(status.loaded_databases, 1);
    assert_eq!(status.loading_databases, 0);
    assert_eq!(status.active_sessions, 0);
    assert!(runtime.activation_snapshot(&second)?.is_none());
    Ok(())
}

/// The wait queue itself is bounded: once the load limit and the queue are full, a caller is
/// rejected immediately instead of extending the queue without limit.
#[test]
fn a_full_cold_load_queue_rejects_without_starting_a_load() -> Result<()> {
    let (_cache, store, mut runtime) = fixture(Duration::from_millis(120))?;
    Arc::get_mut(&mut runtime).unwrap().load_permits = super::LoadPermits::configured(1, 1);
    let first = identifier("queue_a")?;
    let second = identifier("queue_b")?;
    let third = identifier("queue_c")?;
    store.hold(true);

    let loader = {
        let runtime = runtime.clone();
        let database = first.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };
    store.wait_for_read();
    let queued = {
        let runtime = runtime.clone();
        let database = second.clone();
        std::thread::spawn(move || runtime.open_until(database, 1, None))
    };
    wait_until("the queued load", || runtime.load_permits.waiting() == 1);

    let error = match runtime.open_until(
        third.clone(),
        1,
        Some(Instant::now() + Duration::from_secs(5)),
    ) {
        Ok(_) => panic!("a full load queue must reject before the deadline"),
        Err(error) => error,
    };
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::QueueFull),
        "{error:#}"
    );
    assert!(
        format!("{error:#}").contains("worker cold load queue full"),
        "{error:#}"
    );
    assert!(runtime.activation_snapshot(&third)?.is_none());

    let handle = join_within(loader, "the admitted load").expect("the admitted load must finish");
    drop(handle);
    let handle =
        join_within(queued, "the queued load").expect("a queued load must start once a slot frees");
    drop(handle);
    assert_eq!(runtime.load_permits.peak(), 1);
    assert_eq!(runtime.status()?.loaded_databases, 2);
    Ok(())
}

/// A drain that lands while the native open is running must retire the activation at the
/// first finalizer instead of leaving a permanent tombstone (or a permanent Loading entry).
#[test]
fn a_drain_during_a_cold_load_finalizes_the_activation() -> Result<()> {
    let (_cache, store, runtime) = fixture(Duration::from_millis(1))?;
    let database = identifier("drain_loading")?;
    // A fresh database opens without replaying anything, so the drain below would meet a
    // boundary check instead of running inside the native open. The WAL forces a replay.
    let handle = runtime.open(database.clone(), 1)?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    drop(handle);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?, vec![database.clone()]);

    // The drain runs on the replay read itself. That is the only point inside
    // `RemoteDatabase::open`, which cannot be interrupted from the outside and is therefore
    // the phase the finalizer has to handle without an `Instance` to drop.
    let drained = Arc::new(AtomicBool::new(false));
    {
        let runtime = runtime.clone();
        let database = database.clone();
        let drained = drained.clone();
        store.on_get(move |_key| {
            let phase = runtime
                .activation_snapshot(&database)
                .ok()
                .flatten()
                .map(|activation| activation.phase);
            if phase == Some(ActivationPhase::OpeningDatabase) {
                drained.store(true, Ordering::SeqCst);
                let _ = runtime.drain(&database);
            }
        });
    }

    let loader = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };
    let error = match join_within(loader, "the drained load") {
        Ok(_) => panic!("a load retired by a drain must not publish an instance"),
        Err(error) => error,
    };
    assert!(drained.load(Ordering::SeqCst), "the drain never ran");
    assert!(
        format!("{error:#}").contains("database is draining"),
        "{error:#}"
    );
    let status = runtime.status()?;
    assert_eq!(status.loading_databases, 0);
    assert_eq!(status.loaded_databases, 0);
    assert_eq!(status.active_sessions, 0);
    assert!(
        status.databases.is_empty(),
        "the drained load left a tombstone: {status:?}"
    );

    // A retired activation is gone, not a permanent "unloading" answer.
    store.off_get();
    store.hold(false);
    let reopened = runtime.open(database.clone(), 1)?;
    assert!(!reopened.in_transaction()?);
    drop(reopened);
    assert_eq!(runtime.status()?.loaded_databases, 1);
    Ok(())
}

/// The activation snapshot is the runtime's own answer, and the worker status carries the
/// same state inside `resources` so the controller's strict decode keeps working.
#[test]
fn activation_status_reports_phase_waiters_and_replayed_bytes() -> Result<()> {
    let (_cache, store, runtime) = fixture(Duration::from_millis(150))?;
    let database = identifier("activation_status")?;
    store.hold(true);

    let loader = {
        let runtime = runtime.clone();
        let database = database.clone();
        std::thread::spawn(move || runtime.open(database, 1))
    };
    store.wait_for_read();

    let loading = runtime
        .activation_snapshot(&database)?
        .expect("a loading activation is queryable");
    assert_eq!(loading.epoch, 1);
    assert_ne!(
        loading.phase,
        ActivationPhase::Ready,
        "the load reported Ready before it finished"
    );
    assert_eq!(loading.waiters, 0);
    assert!(
        loading.replayed_bytes.is_none(),
        "replayed bytes are only known once the native open returned"
    );
    let status = runtime.status()?;
    assert_eq!(status.loading_databases, 1);
    let resources = status.databases[0]
        .resources
        .as_ref()
        .expect("a loading activation reports resources");
    assert!(
        resources.usage.is_none(),
        "a loading entry has no native database to sample"
    );
    let reported = resources
        .activation
        .as_ref()
        .expect("the status carries the activation");
    assert_eq!(reported.epoch, 1);
    assert_ne!(reported.phase, ActivationPhase::Ready);

    // The hold exists to stretch the load. Once it returns, the follow-up statement runs
    // under the real 5s budget, so an artificial 150ms per read would fail it for no reason.
    store.hold(false);
    let handle = join_within(loader, "the cold load")?;
    handle.execute("CREATE TABLE data(value); INSERT INTO data VALUES(1)")?;
    drop(handle);

    let ready = runtime
        .activation_snapshot(&database)?
        .expect("the retained instance carries its activation");
    assert_eq!(ready.phase, ActivationPhase::Ready);
    assert_eq!(ready.epoch, 1);
    assert!(ready.replayed_bytes.is_some());
    let status = runtime.status()?;
    let resources = status.databases[0]
        .resources
        .as_ref()
        .expect("a loaded activation reports resources");
    assert!(resources.usage.is_some());
    let value = serde_json::to_value(resources)?;
    assert_eq!(
        value["activation"]["phase"].as_str(),
        Some("ready"),
        "the activation travels inside resources: {value}"
    );
    assert!(
        value.get("wal_bytes").is_some(),
        "resource usage stays flattened for the controller: {value}"
    );
    Ok(())
}

#[test]
fn the_cold_load_concurrency_is_validated() {
    assert_eq!(super::configured_max_concurrent_loads(None).unwrap(), 2);
    assert_eq!(
        super::configured_max_concurrent_loads(Some("1")).unwrap(),
        1
    );
    assert_eq!(
        super::configured_max_concurrent_loads(Some("64")).unwrap(),
        64
    );
    for invalid in ["0", "65", "-1", "abc", ""] {
        assert!(
            super::configured_max_concurrent_loads(Some(invalid)).is_err(),
            "{invalid}"
        );
    }
}
