use super::{WorkerLimits, WorkerRuntime};
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use crate::provider::LocalAuthorityProvider;
use crate::runtime::DatabaseId;
use crate::session::{Budget, RequestDeadline, SqlEvidence, UnsettledSqlOutcome};
use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A store whose reads can be held. A cold load pays one remote round trip per WAL chunk,
/// so this is what makes a load outlast any statement budget.
struct SlowStore {
    inner: MemoryObjectStore,
    delay: Duration,
    held: AtomicBool,
}

impl SlowStore {
    fn new(delay: Duration) -> Self {
        Self {
            inner: MemoryObjectStore::default(),
            delay,
            held: AtomicBool::new(false),
        }
    }

    fn hold(&self, held: bool) {
        self.held.store(held, Ordering::SeqCst);
    }
}

impl ObjectStore for SlowStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if self.held.load(Ordering::SeqCst) {
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
