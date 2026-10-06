use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::session::Budget;

fn open() -> Result<(tempfile::TempDir, tempfile::TempDir, Arc<RemoteDatabase>)> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(LocalObjectStore::open(objects.path())?),
        Namespace::new("p6", "budget")?,
        cache.path(),
        0,
    )?);
    Ok((objects, cache, database))
}

const RECURSIVE: &str = "WITH RECURSIVE forever(value) AS (VALUES(1) UNION ALL SELECT value+1 FROM forever) SELECT sum(value) FROM forever";

#[test]
fn recursive_vm_abort_and_handler_cleanup() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let session = database.session()?;
    let budget = Budget {
        max_vm_steps: 10_000,
        ..Budget::default()
    };
    let started = Instant::now();
    assert!(session.query_with_budget(RECURSIVE, &budget).is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(session.query("SELECT 42")?[0][0].to_string(), "42");
    assert!(session.execute_with_budget(RECURSIVE, &budget).is_err());
    session.execute("CREATE TABLE items(value INTEGER)")?;
    assert_eq!(database.query("SELECT 7")?[0][0].to_string(), "7");
    Ok(())
}

#[test]
fn result_caps_before_collection_and_reuse() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let session = database.session()?;
    let budget = Budget {
        max_rows: 3,
        ..Budget::default()
    };
    assert!(session
        .query_with_budget(
            "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4",
            &budget
        )
        .is_err());
    assert_eq!(
        session
            .query_with_budget("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3", &budget)?
            .len(),
        3
    );
    let budget = Budget {
        max_result_bytes: 512,
        ..Budget::default()
    };
    assert!(session
        .query_with_budget("SELECT zeroblob(1024)", &budget)
        .is_err());
    assert!(session
        .query_with_budget(
            "SELECT zeroblob(256) UNION ALL SELECT zeroblob(256)",
            &budget
        )
        .is_err());
    assert_eq!(session.query("SELECT 1")?.len(), 1);
    assert!(session.query("SELECT zeroblob(1000000001)").is_err());
    assert!(session.query("SELECT randomblob(1000000001)").is_err());
    assert_eq!(session.query("SELECT 3")?[0][0].to_string(), "3");
    Ok(())
}

#[test]
fn sql_cap_both_database_and_session_ingress() -> Result<()> {
    Budget::set_default(Budget::default())?;
    let (_objects, _cache, database) = open()?;
    let sql = format!("SELECT 1{}", " ".repeat(64 * 1024));
    assert!(database.query(&sql).is_err());
    assert!(database.execute(&sql).is_err());
    let session = database.session()?;
    assert!(session.execute(&sql).is_err());
    assert!(session.query(&sql).is_err());
    assert_eq!(session.query("SELECT 1")?.len(), 1);
    Ok(())
}

#[test]
fn duration_budget_interrupts_and_releases_handler() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let session = database.session()?;
    let budget = Budget {
        max_query_duration: Duration::from_millis(5),
        max_vm_steps: u64::MAX,
        ..Budget::default()
    };
    let started = Instant::now();
    assert!(session.query_with_budget(RECURSIVE, &budget).is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(session.query("SELECT 2")?[0][0].to_string(), "2");
    Ok(())
}

#[test]
fn batch_error_does_not_claim_rollback_of_acknowledged_prefix() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let session = database.session()?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    let budget = Budget {
        max_vm_steps: 10_000,
        ..Budget::default()
    };
    assert!(session
        .execute_with_budget(
            &format!("INSERT INTO items VALUES(1); {RECURSIVE}"),
            &budget
        )
        .is_err());
    assert_eq!(
        session.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    session.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    assert!(session.execute("INSERT INTO absent VALUES(3)").is_err());
    assert!(session.in_transaction()?);
    session.execute("ROLLBACK")?;
    assert_eq!(
        session.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

#[test]
fn default_budget_is_explicit_and_attachment_remains_disabled() -> Result<()> {
    let budget = Budget::default();
    assert_eq!(budget.max_sql_bytes, 65_536);
    assert_eq!(budget.max_rows, 10_000);
    assert_eq!(budget.max_result_bytes, 4 * 1024 * 1024);
    assert_eq!(budget.max_vm_steps, 10_000_000);
    assert_eq!(budget.max_query_duration, Duration::from_secs(5));
    let (_objects, _cache, database) = open()?;
    assert!(database
        .execute("ATTACH DATABASE ':memory:' AS other")
        .is_err());
    assert_eq!(database.query("SELECT 1")?.len(), 1);
    Ok(())
}

struct ProgressFenceStore {
    inner: Arc<dyn ObjectStore>,
    armed: AtomicBool,
    checks: AtomicUsize,
}

impl ObjectStore for ProgressFenceStore {
    fn validate_local(&self) -> Result<()> {
        if self.armed.load(Ordering::Relaxed) && self.checks.fetch_add(1, Ordering::Relaxed) >= 100
        {
            anyhow::bail!("local native owner fence expired");
        }
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
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

#[test]
fn native_progress_checks_local_owner_without_waiting_for_a_row() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(ProgressFenceStore {
        inner: Arc::new(LocalObjectStore::open(objects.path())?),
        armed: AtomicBool::new(false),
        checks: AtomicUsize::new(0),
    });
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        Namespace::new("p6", "progress")?,
        cache.path(),
        0,
    )?);
    let session = database.session()?;
    session.query("SELECT 1")?;
    store.armed.store(true, Ordering::Relaxed);
    let started = Instant::now();
    assert!(session
        .query_with_budget(
            RECURSIVE,
            &Budget {
                max_vm_steps: u64::MAX,
                ..Budget::default()
            }
        )
        .is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(store.checks.load(Ordering::Relaxed) >= 100);
    store.armed.store(false, Ordering::Relaxed);
    assert_eq!(session.query("SELECT 42")?[0][0].to_string(), "42");
    Ok(())
}
