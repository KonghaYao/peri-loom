use super::{WorkerLimits, WorkerRuntime};
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use crate::provider::LocalAuthorityProvider;
use crate::runtime::DatabaseId;
use crate::session::{Budget, RequestDeadline, SqlEvidence, UnsettledSqlOutcome};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// A store whose reads can be held. A cold load pays one remote round trip per WAL chunk,
/// so this is what makes a load outlast any statement budget. A held read announces itself
/// before it sleeps, which is what lets a test open a second session mid-load.
struct SlowStore {
    inner: MemoryObjectStore,
    delay: Duration,
    held: AtomicBool,
    started: Mutex<usize>,
    progress: Condvar,
}

impl SlowStore {
    fn new(delay: Duration) -> Self {
        Self {
            inner: MemoryObjectStore::default(),
            delay,
            held: AtomicBool::new(false),
            started: Mutex::new(0),
            progress: Condvar::new(),
        }
    }

    fn hold(&self, held: bool) {
        self.held.store(held, Ordering::SeqCst);
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
    Arc::get_mut(&mut runtime).unwrap().load_budget = Some(Duration::from_millis(1));
    let database = identifier("split")?;
    store.hold(true);

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
