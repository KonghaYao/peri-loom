use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Result};
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::metrics::Metrics;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::metrics::MetricsObjectStore;
use peri_loom_v1::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::ownership::types::AccessRole;

#[test]
fn real_store_operations_report_counts_bytes_errors_and_latency() -> Result<()> {
    let metrics = Arc::new(Metrics::default());
    let store = MetricsObjectStore::new(Arc::new(MemoryObjectStore::default()), metrics.clone());
    assert_eq!(metrics.snapshot().get.total, 0);
    store.put("one", b"actual payload", PutCondition::Absent)?;
    assert_eq!(store.get("one")?.unwrap().bytes, b"actual payload");
    assert!(store.get("missing")?.is_none());
    assert!(store
        .put("one", b"not written", PutCondition::Absent)
        .is_err());
    assert_eq!(store.list("")?, vec!["one"]);
    store.delete("one", None)?;
    let report = metrics.snapshot();
    assert_eq!(report.put.total, 2);
    assert_eq!(report.put.errors, 1);
    assert_eq!(report.put.bytes, 14);
    assert_eq!(report.get.total, 2);
    assert_eq!(report.get.bytes, 14);
    assert_eq!(report.list.total, 1);
    assert_eq!(report.list.bytes, 3);
    assert_eq!(report.delete.total, 1);
    assert!(report.get.latency_total_ns > 0);
    assert_eq!(report.get.latency_buckets[6], 2);
    assert_eq!(report.checkpoint.total, 0);
    let serialized = serde_json::to_value(report)?;
    assert!(serialized.get("cache_hits").is_none());
    Ok(())
}

struct GuardedStore {
    validations: AtomicUsize,
}

impl ObjectStore for GuardedStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }
    fn access_role(&self) -> AccessRole {
        AccessRole::ReadOnly
    }
    fn validate_session(&self) -> Result<()> {
        self.validations.fetch_add(1, Ordering::Relaxed);
        anyhow::bail!("session denied")
    }
    fn validate_local(&self) -> Result<()> {
        self.validations.fetch_add(1, Ordering::Relaxed);
        anyhow::bail!("local fence denied")
    }
    fn validate_transaction(&self, in_transaction: bool) -> Result<()> {
        self.validations.fetch_add(1, Ordering::Relaxed);
        ensure!(!in_transaction, "transaction denied");
        Ok(())
    }
    fn get(&self, _key: &str) -> Result<Option<StoredObject>> {
        anyhow::bail!("denied")
    }
    fn put(&self, _key: &str, _bytes: &[u8], _condition: PutCondition) -> Result<String> {
        anyhow::bail!("denied")
    }
    fn delete(&self, _key: &str, _expected: Option<&str>) -> Result<()> {
        anyhow::bail!("denied")
    }
    fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        anyhow::bail!("denied")
    }
}

#[test]
fn wrapper_preserves_role_and_all_fences_without_fabricated_io() -> Result<()> {
    let inner = Arc::new(GuardedStore {
        validations: AtomicUsize::new(0),
    });
    let metrics = Arc::new(Metrics::default());
    let store = MetricsObjectStore::new(inner.clone(), metrics.clone());
    assert!(store.supports_strict_fencing());
    assert_eq!(store.access_role(), AccessRole::ReadOnly);
    assert!(store.validate_session().is_err());
    assert!(store.validate_local().is_err());
    assert!(store.validate_transaction(true).is_err());
    store.validate_transaction(false)?;
    assert_eq!(inner.validations.load(Ordering::Relaxed), 4);
    assert_eq!(metrics.snapshot().get.total, 0);
    assert!(store.get("one").is_err());
    assert_eq!(metrics.snapshot().get.errors, 1);
    Ok(())
}

#[test]
fn native_query_and_execute_are_observed_without_fake_cache_metrics() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let metrics = Metrics::global();
    let store = Arc::new(MetricsObjectStore::new(
        Arc::new(peri_loom_v1::object_store::LocalObjectStore::open(
            objects.path(),
        )?),
        metrics.clone(),
    ));
    let database = RemoteDatabase::open(store, Namespace::new("p6", "metrics")?, cache.path(), 0)?;
    let before = metrics.snapshot();
    database.execute("CREATE TABLE items(value); INSERT INTO items VALUES('real')")?;
    assert_eq!(database.query("SELECT value FROM items")?.len(), 1);
    assert!(database.query("SELECT missing FROM items").is_err());
    let after = metrics.snapshot();
    assert_eq!(after.execute.total - before.execute.total, 1);
    assert_eq!(after.query.total - before.query.total, 2);
    assert_eq!(after.query.errors - before.query.errors, 1);
    assert!(after.query.latency_total_ns > before.query.latency_total_ns);
    assert!(after.put.total > before.put.total);
    assert!(after.put.bytes > before.put.bytes);
    Ok(())
}
