use super::{enabled, TestBucket};
use anyhow::{ensure, Result};
use peri_loom_v1::{
    namespace::Namespace,
    provider::LocalAuthorityProvider,
    runtime::{DatabaseId, WorkerLimits, WorkerRuntime},
};
use std::{sync::Arc, time::Duration};

#[test]
#[ignore = "requires PERI_S3_TEST=1 or PERI_MINIO_TEST=1 and explicit test S3 configuration"]
fn network_worker_multi_db_unload_and_fresh_cache_recovery() -> Result<()> {
    ensure!(enabled(), "real S3 tests require explicit opt-in");
    let fixture = TestBucket::new()?;
    let cache = tempfile::tempdir()?;
    let provider = Arc::new(LocalAuthorityProvider::new(fixture.store.clone()));
    let runtime = WorkerRuntime::new(provider.clone(), cache.path(), WorkerLimits::default())?;
    let first = DatabaseId::new(Namespace::new("tenant", "first")?, "main")?;
    let second = DatabaseId::new(Namespace::new("tenant", "second")?, "main")?;
    let writer = runtime.open(first.clone(), 0)?;
    let isolated = runtime.open(second.clone(), 0)?;
    writer.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    isolated.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(2)")?;
    assert!(writer.query("SELECT * FROM items")?[0][0]
        .to_string()
        .contains('1'));
    writer.execute("BEGIN; INSERT INTO items VALUES(3)")?;
    drop(writer);
    let reader = runtime.open(first.clone(), 0)?;
    assert_eq!(reader.query("SELECT * FROM items")?.len(), 1);
    reader.compact_wal()?;
    drop(reader);
    drop(isolated);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?.len(), 2);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    drop(runtime);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    let worker = WorkerRuntime::new(provider, fresh.path(), WorkerLimits::default())?;
    let first_handle = worker.open(first, 0)?;
    let second_handle = worker.open(second, 0)?;
    assert_eq!(
        first_handle.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    assert_eq!(
        second_handle.query("SELECT id FROM items")?[0][0].to_string(),
        "2"
    );
    assert_eq!(
        first_handle.query("PRAGMA integrity_check")?[0][0].to_string(),
        "ok"
    );
    ensure!(
        first_handle.resources()?.wal_bytes <= 64 * 1024,
        "unbounded compacted current WAL"
    );
    drop(first_handle);
    drop(second_handle);
    assert_eq!(worker.evict_idle(Duration::ZERO)?.len(), 2);
    eprintln!("PASS real S3 P3: multi-DB isolation, session-drop rollback, compaction, scale-to-zero, whole-cache deletion and fresh worker recovery");
    Ok(())
}
