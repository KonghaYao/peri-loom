#[allow(dead_code)]
#[path = "p4_remote_helpers/harness.rs"]
mod harness;

use anyhow::{ensure, Result};
use peri_loom_v1::{
    authority_rpc::RemoteAuthorityProvider,
    database::RemoteDatabase,
    io::RemoteIO,
    manifest::{WalPosition, SEGMENT_SIZE},
    metrics::{Metrics, Operation, OperationMetrics},
    namespace::Namespace,
    object_store::{
        metrics::MetricsObjectStore, MemoryObjectStore, ObjectStore, PutCondition, StoredObject,
    },
    ownership::types::AccessRole,
    runtime::{DatabaseId, DatabaseProvider, WorkerLimits, WorkerRuntime},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};

static GLOBAL_TESTS: Mutex<()> = Mutex::new(());

fn histogram(metric: &OperationMetrics) {
    assert_eq!(metric.latency_buckets[6], metric.total);
    assert!(metric
        .latency_buckets
        .windows(2)
        .all(|pair| pair[0] <= pair[1]));
    assert!(metric.latency_max_ns <= metric.latency_total_ns);
}

#[test]
fn operations_count_actual_success_errors_bytes_and_monotonic_histograms() -> Result<()> {
    let metrics = Metrics::default();
    for operation in [
        Operation::SegmentGet,
        Operation::SegmentPut,
        Operation::WalDurability,
        Operation::NativeCommitDurability,
        Operation::ColdOpen,
    ] {
        metrics.observe(operation, || Ok(73u64), |bytes| *bytes)?;
        assert!(metrics
            .observe(
                operation,
                || anyhow::bail!("observed failure"),
                |_: &u64| 999
            )
            .is_err());
    }
    let snapshot = metrics.snapshot();
    for metric in [
        &snapshot.segment_get,
        &snapshot.segment_put,
        &snapshot.wal_durability,
        &snapshot.native_commit_durability,
        &snapshot.cold_open,
    ] {
        assert_eq!(metric.total, 2);
        assert_eq!(metric.errors, 1);
        assert_eq!(metric.bytes, 73);
        histogram(metric);
    }
    assert!(snapshot
        .measurement_scopes
        .native_commit_durability
        .contains("not full SQL COMMIT"));
    assert!(snapshot
        .measurement_scopes
        .segment_io
        .contains("authority fencing"));
    Ok(())
}

#[test]
fn worker_cold_load_and_native_commit_are_real_not_reused_sessions_or_execute_regex() -> Result<()>
{
    let _serial = GLOBAL_TESTS.lock().unwrap();
    let service = harness::Harness::new()?;
    let id = harness::identifier("observed-cold")?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(
        service.grant(id.clone(), "worker-a", 1, 0)?,
        AccessRole::Writer,
    )?;
    let cache = tempfile::tempdir()?;
    let worker = WorkerRuntime::new(remote.clone(), cache.path(), WorkerLimits::default())?;
    let metrics = Metrics::global();
    let before = metrics.snapshot();
    let first = worker.open(id.clone(), 1)?;
    let second = worker.open(id.clone(), 1)?;
    assert_eq!(
        metrics.snapshot().cold_open.total - before.cold_open.total,
        1
    );
    first.execute("CREATE TABLE observations(value TEXT)")?;
    let prepared = metrics.snapshot();
    first.execute("BEGIN; INSERT INTO observations VALUES('COMMIT is data, not syntax')")?;
    assert_eq!(
        metrics.snapshot().native_commit_durability.total,
        prepared.native_commit_durability.total
    );
    first.execute("COMMIT")?;
    let committed = metrics.snapshot();
    assert_eq!(
        committed.native_commit_durability.total - prepared.native_commit_durability.total,
        1
    );
    assert!(committed.wal_durability.total > prepared.wal_durability.total);
    assert!(committed.native_commit_durability.bytes > prepared.native_commit_durability.bytes);
    assert!(
        committed.native_commit_durability.latency_total_ns
            > prepared.native_commit_durability.latency_total_ns
    );
    for _ in 0..4 {
        second.query("SELECT value FROM observations")?;
    }
    let queried = metrics.snapshot();
    assert_eq!(queried.query.total - committed.query.total, 4);
    assert_eq!(
        queried.native_commit_durability.total,
        committed.native_commit_durability.total
    );
    assert_eq!(queried.wal_durability.total, committed.wal_durability.total);
    first.execute("BEGIN; INSERT INTO observations VALUES('rolled back'); ROLLBACK")?;
    assert_eq!(
        metrics.snapshot().native_commit_durability.total,
        committed.native_commit_durability.total
    );
    first.checkpoint()?;
    first.compact_wal()?;
    assert!(metrics.snapshot().segment_put.bytes > before.segment_put.bytes);
    drop(first);
    drop(second);
    worker.close(&id)?;
    let cold = tempfile::tempdir()?;
    let migrated = WorkerRuntime::new(remote, cold.path(), WorkerLimits::default())?;
    let before_reopen = metrics.snapshot();
    let recovered = migrated.open(id.clone(), 1)?;
    assert_eq!(recovered.query("SELECT value FROM observations")?.len(), 1);
    let reopened = metrics.snapshot();
    assert_eq!(reopened.cold_open.total - before_reopen.cold_open.total, 1);
    assert!(reopened.segment_get.bytes > before_reopen.segment_get.bytes);
    let status = serde_json::to_value(migrated.status()?)?;
    let usage = &status["databases"][0]["resources"];
    assert!(usage["segment_cache_lookups"].as_u64().unwrap() > 0);
    assert!(usage.get("pager_hit_ratio").is_none());
    assert!(reopened.cold_open.latency_total_ns > before_reopen.cold_open.latency_total_ns);
    histogram(&reopened.cold_open);
    histogram(&reopened.native_commit_durability);
    Ok(())
}

#[test]
fn segment_cache_resources_count_ram_ssd_misses_evictions_and_exact_lookup_ratio() -> Result<()> {
    let _serial = GLOBAL_TESTS.lock().unwrap();
    let store = Arc::new(MemoryObjectStore::default());
    let namespace = Namespace::new("observability", "cache")?;
    let cache = tempfile::tempdir()?;
    let io = RemoteIO::open(
        store.clone(),
        namespace.clone(),
        cache.path(),
        0,
        SEGMENT_SIZE,
        SEGMENT_SIZE as u64,
    )?;
    assert_eq!(io.resources()?.segment_cache_hit_ratio, None);
    let storage = io.storage();
    storage.write_at(0, &vec![0x41; SEGMENT_SIZE])?;
    storage.write_at(SEGMENT_SIZE as u64, &vec![0x42; SEGMENT_SIZE])?;
    assert_eq!(
        io.resources()?.dirty_segment_bytes,
        (SEGMENT_SIZE * 2) as u64
    );
    storage.publish(WalPosition::default(), 0)?;
    let mut bytes = [0; 1];
    storage.read_at(0, &mut bytes)?;
    storage.read_at(0, &mut bytes)?;
    storage.read_at(SEGMENT_SIZE as u64, &mut bytes)?;
    let ram = io.resources()?;
    assert_eq!(ram.segment_cache_misses, 2);
    assert_eq!(ram.segment_cache_hits_ram, 1);
    assert_eq!(ram.segment_cache_hits_disk, 0);
    assert_eq!(ram.segment_cache_lookups, 3);
    assert_eq!(ram.segment_cache_hit_ratio, Some(1.0 / 3.0));
    assert!(ram.segment_cache_evictions >= 2);
    assert_eq!(ram.segment_loads, 2);
    assert_eq!(ram.dirty_segment_bytes, 0);
    drop(storage);
    drop(io);
    let io = RemoteIO::open(store, namespace, cache.path(), 0, 0, SEGMENT_SIZE as u64)?;
    io.storage().read_at(SEGMENT_SIZE as u64, &mut bytes)?;
    io.storage().read_at(SEGMENT_SIZE as u64, &mut bytes)?;
    let ssd = io.resources()?;
    assert_eq!(ssd.segment_cache_hits_disk, 2);
    assert_eq!(ssd.segment_cache_hits_ram, 0);
    assert_eq!(ssd.segment_cache_misses, 0);
    assert_eq!(ssd.segment_cache_lookups, 2);
    assert_eq!(ssd.segment_cache_hit_ratio, Some(1.0));
    assert_eq!(ssd.segment_loads, 0);
    assert!(serde_json::to_value(ssd)?.get("pager_hit_ratio").is_none());
    Ok(())
}

#[test]
fn logical_segment_rpc_counts_payloads_denials_and_keeps_existing_role_and_lease_fences(
) -> Result<()> {
    let _serial = GLOBAL_TESTS.lock().unwrap();
    let service = harness::Harness::new()?;
    let id = harness::identifier("segments")?;
    let lease = service.grant(id.clone(), "worker-a", 1, 0)?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(lease.clone(), AccessRole::Writer)?;
    let store = remote.open_store(&id, 1)?;
    let payload = vec![0x63; SEGMENT_SIZE];
    let key = id
        .namespace
        .key(&format!("segments/{}", blake3::hash(&payload).to_hex()))?;
    let metrics = Metrics::global();
    let before = metrics.snapshot();
    store.put(&key, &payload, PutCondition::Absent)?;
    assert_eq!(store.get(&key)?.unwrap().bytes, payload);
    assert!(store.get(&id.namespace.key("wal/index")?)?.is_none());
    let completed = metrics.snapshot();
    assert_eq!(completed.segment_put.total - before.segment_put.total, 1);
    assert_eq!(completed.segment_get.total - before.segment_get.total, 1);
    assert_eq!(
        completed.segment_put.bytes - before.segment_put.bytes,
        SEGMENT_SIZE as u64
    );
    assert_eq!(
        completed.segment_get.bytes - before.segment_get.bytes,
        SEGMENT_SIZE as u64
    );
    remote.install_lease(lease, AccessRole::ReadOnly)?;
    let reader = remote.open_store(&id, 1)?;
    assert_eq!(reader.access_role(), AccessRole::ReadOnly);
    assert!(reader.put(&key, &payload, PutCondition::Any).is_err());
    assert!(reader
        .get(&id.namespace.key("segments/invalid-hash")?)
        .is_err());
    service.clock.0.store(10101, Ordering::SeqCst);
    assert!(store.get(&key).is_err());
    let denied = metrics.snapshot();
    assert_eq!(denied.segment_put.errors - completed.segment_put.errors, 1);
    assert_eq!(denied.segment_get.errors - completed.segment_get.errors, 2);
    assert_eq!(denied.segment_put.bytes, completed.segment_put.bytes);
    assert_eq!(denied.segment_get.bytes, completed.segment_get.bytes);
    assert!(denied.segment_get.latency_total_ns > completed.segment_get.latency_total_ns);
    histogram(&denied.segment_get);
    histogram(&denied.segment_put);
    Ok(())
}

struct FailingStore {
    inner: MemoryObjectStore,
    fail_index: AtomicBool,
}

impl ObjectStore for FailingStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        ensure!(
            !(key.ends_with("/wal/index") && self.fail_index.load(Ordering::SeqCst)),
            "injected unconfirmed WAL publication"
        );
        self.inner.put(key, bytes, condition)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
}

#[test]
fn failed_native_wal_append_records_errors_without_discarding_pending_batch_or_acknowledging_commit(
) -> Result<()> {
    let _serial = GLOBAL_TESTS.lock().unwrap();
    let store = Arc::new(FailingStore {
        inner: MemoryObjectStore::default(),
        fail_index: AtomicBool::new(false),
    });
    let namespace = Namespace::new("observability", "durability-errors")?;
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(store.clone(), namespace.clone(), cache.path(), 0)?;
    database.execute("CREATE TABLE observations(value)")?;
    let metrics = Metrics::global();
    let before = metrics.snapshot();
    store.fail_index.store(true, Ordering::SeqCst);
    assert!(database
        .execute("INSERT INTO observations VALUES('unconfirmed')")
        .is_err());
    let failed = metrics.snapshot();
    assert_eq!(
        failed.wal_durability.errors - before.wal_durability.errors,
        1
    );
    assert_eq!(
        failed.native_commit_durability.errors - before.native_commit_durability.errors,
        1
    );
    assert_eq!(
        failed.native_commit_durability.latency_total_ns
            - before.native_commit_durability.latency_total_ns,
        failed.wal_durability.latency_total_ns - before.wal_durability.latency_total_ns
    );
    assert_eq!(failed.wal_durability.bytes, before.wal_durability.bytes);
    assert!(database.resources()?.pending_wal_bytes > 0);
    assert!(database.checkpoint().is_err());
    drop(database);
    store.fail_index.store(false, Ordering::SeqCst);
    let cold = tempfile::tempdir()?;
    let recovered = RemoteDatabase::open(store, namespace, cold.path(), 0)?;
    assert!(recovered
        .query("SELECT value FROM observations")?
        .is_empty());
    Ok(())
}

struct DeniedProvider;
impl DatabaseProvider for DeniedProvider {
    fn open_store(&self, _: &DatabaseId, _: u64) -> Result<Arc<dyn ObjectStore>> {
        anyhow::bail!("cold admission denied")
    }
}

#[test]
fn cold_load_failure_is_observed_and_does_not_fabricate_native_commit_or_io() -> Result<()> {
    let _serial = GLOBAL_TESTS.lock().unwrap();
    let cache = tempfile::tempdir()?;
    let worker = WorkerRuntime::new(
        Arc::new(DeniedProvider),
        cache.path(),
        WorkerLimits::default(),
    )?;
    let metrics = Metrics::global();
    let before = metrics.snapshot();
    assert!(worker.open(harness::identifier("cold-denied")?, 1).is_err());
    let after = metrics.snapshot();
    assert_eq!(after.cold_open.total - before.cold_open.total, 1);
    assert_eq!(after.cold_open.errors - before.cold_open.errors, 1);
    assert_eq!(after.segment_get.total, before.segment_get.total);
    assert_eq!(after.wal_durability.total, before.wal_durability.total);
    assert_eq!(
        after.native_commit_durability.total,
        before.native_commit_durability.total
    );
    assert_eq!(worker.status()?.loaded_databases, 0);
    Ok(())
}

#[test]
fn raw_object_store_metrics_remain_separate_from_logical_segment_rpc() -> Result<()> {
    let metrics = Arc::new(Metrics::default());
    let raw = MetricsObjectStore::new(Arc::new(MemoryObjectStore::default()), metrics.clone());
    assert!(raw.supports_strict_fencing());
    raw.put(
        "authority/blobs/physical",
        b"physical-bytes",
        PutCondition::Absent,
    )?;
    raw.get("authority/blobs/physical")?;
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.get.total, 1);
    assert_eq!(snapshot.put.total, 1);
    assert_eq!(snapshot.get.bytes, 14);
    assert_eq!(snapshot.put.bytes, 14);
    assert_eq!(snapshot.segment_get.total, 0);
    assert_eq!(snapshot.segment_put.total, 0);
    Ok(())
}

#[test]
fn raw_wal_sync_without_native_commit_marker_is_not_reported_as_sql_commit() -> Result<()> {
    let _serial = GLOBAL_TESTS.lock().unwrap();
    let cache = tempfile::tempdir()?;
    let io = RemoteIO::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("observability", "unmarked-wal")?,
        cache.path(),
        0,
        SEGMENT_SIZE,
        SEGMENT_SIZE as u64,
    )?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    let payload = Arc::new(Buffer::new_temporary(128));
    payload.as_mut_slice().fill(0x75);
    assert!(wal
        .pwrite(0, payload, Completion::new_write(|_| {}))?
        .finished());
    assert_eq!(io.resources()?.pending_wal_bytes, 128);
    let metrics = Metrics::global();
    let before = metrics.snapshot();
    assert!(wal
        .sync(
            Completion::new_sync(|result| assert!(result.is_ok())),
            FileSyncType::Fsync
        )?
        .finished());
    let after = metrics.snapshot();
    assert_eq!(after.wal_durability.total - before.wal_durability.total, 1);
    assert_eq!(
        after.wal_durability.bytes - before.wal_durability.bytes,
        128
    );
    assert_eq!(
        after.native_commit_durability.total,
        before.native_commit_durability.total
    );
    assert_eq!(io.resources()?.pending_wal_bytes, 0);
    assert!(wal
        .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?
        .finished());
    assert_eq!(
        metrics.snapshot().wal_durability.total,
        after.wal_durability.total
    );
    Ok(())
}
