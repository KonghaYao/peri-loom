#[path = "stateless_acceptance/catalog.rs"]
mod catalog;
#[path = "stateless_acceptance/counting.rs"]
mod counting;
#[path = "stateless_acceptance/process.rs"]
mod process;

use anyhow::{ensure, Context, Result};
use catalog::{
    assert_authority_layout, assert_cache_only, authority, database, manifest, metric, resources,
};
use peri_loom_v1::capabilities::Capabilities;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore};
use peri_loom_v1::runtime::WorkerLimits;
use process::Worker;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;

fn assert_empty(status: &Value) -> Result<()> {
    ensure!(
        status["loaded_databases"] == 0
            && status["loading_databases"] == 0
            && status["active_sessions"] == 0,
        "worker did not unload: {status}"
    );
    ensure!(
        status["databases"]
            .as_array()
            .context("registry missing")?
            .is_empty(),
        "unloaded registry retained entries: {status}"
    );
    Ok(())
}

fn assert_cache_limits(usage: &Value) -> Result<()> {
    let limits = WorkerLimits::default();
    ensure!(
        metric(usage, "segment_ram_bytes")? <= limits.segment_ram_bytes as u64,
        "RAM cache cap exceeded"
    );
    ensure!(
        metric(usage, "segment_disk_bytes")? <= limits.segment_disk_bytes,
        "disk cache cap exceeded"
    );
    Ok(())
}

#[test]
fn standalone_worker_dynamic_databases_and_idle_scale_to_zero() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("objects");
    let cache = root.path().join("worker");
    let mut worker = Worker::spawn(&store, &cache)?;
    assert_empty(&worker.status()?)?;
    ensure!(
        LocalObjectStore::open(&store)?.list("")?.is_empty(),
        "startup eagerly initialized a database"
    );
    let alpha = database("alpha", "main");
    let beta = database("beta", "main");
    let first = worker.open(&alpha)?;
    let second = worker.open(&beta)?;
    worker.exec(
        &first,
        "CREATE TABLE items(value TEXT); INSERT INTO items VALUES('alpha')",
    )?;
    worker.exec(
        &second,
        "CREATE TABLE items(value TEXT); INSERT INTO items VALUES('beta')",
    )?;
    assert_eq!(
        worker.query(&first, "SELECT value FROM items")?,
        json!([["alpha"]])
    );
    assert_eq!(
        worker.query(&second, "SELECT value FROM items")?,
        json!([["beta"]])
    );
    let status = worker.status()?;
    assert_eq!(status["loaded_databases"], 2);
    assert_eq!(status["active_sessions"], 2);
    assert_cache_limits(&resources(&status, &alpha)?)?;
    assert_cache_limits(&resources(&status, &beta)?)?;
    worker.rejected(json!({"op":"open", "database":{"namespace":{"tenant":"../escape", "database":"bad"}, "branch":"main"}, "epoch":0}))?;
    worker.rejected(json!({"op":"open", "database":database("failed", "missing"), "epoch":0}))?;
    assert_eq!(worker.status()?["loading_databases"], 0);
    worker.close(&first)?;
    worker.rejected(json!({"op":"query", "session":first, "sql":"SELECT 1"}))?;
    worker.success(json!({"op":"unload", "database":alpha}))?;
    assert_eq!(worker.status()?["loaded_databases"], 1);
    worker.close(&second)?;
    worker.success(json!({"op":"evict", "idle_ms":0}))?;
    assert_empty(&worker.status()?)?;
    assert_cache_only(&cache)?;
    assert_authority_layout(&store, "alpha")?;
    assert_authority_layout(&store, "beta")?;
    let reopened = worker.open(&beta)?;
    assert_eq!(
        worker.query(&reopened, "SELECT value FROM items")?,
        json!([["beta"]])
    );
    worker.finish()?;
    Ok(())
}

#[test]
fn separate_protocol_sessions_rollback_and_drain_admission() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("objects");
    let cache = root.path().join("worker");
    let mut worker = Worker::spawn(&store, &cache)?;
    let identifier = database("sessions", "main");
    let writer = worker.open(&identifier)?;
    let reader = worker.open(&identifier)?;
    ensure!(writer != reader, "two opens reused the same session");
    worker.exec(&writer, "CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    worker.exec(&writer, "BEGIN; INSERT INTO items VALUES(1)")?;
    assert_eq!(
        worker.query(&reader, "SELECT count(*) FROM items")?,
        json!([[0]])
    );
    worker.success(json!({"op":"evict", "idle_ms":0}))?;
    assert_eq!(worker.status()?["loaded_databases"], 1);
    worker.rejected(json!({"op":"unload", "database":identifier}))?;
    worker.exec(&reader, "BEGIN; ROLLBACK")?;
    worker.exec(&writer, "COMMIT")?;
    assert_eq!(
        worker.query(&reader, "SELECT count(*) FROM items")?,
        json!([[1]])
    );
    worker.exec(&writer, "BEGIN; INSERT INTO items VALUES(2)")?;
    worker.close(&writer)?;
    assert_eq!(
        worker.query(&reader, "SELECT count(*) FROM items")?,
        json!([[1]])
    );
    worker.exec(&reader, "INSERT INTO items VALUES(3)")?;
    worker.success(json!({"op":"drain", "database":identifier}))?;
    let status = worker.status()?;
    assert_eq!(status["databases"][0]["lifecycle"], "draining");
    worker.rejected(json!({"op":"open", "database":identifier, "epoch":0}))?;
    assert_eq!(
        worker.query(&reader, "SELECT count(*) FROM items")?,
        json!([[2]])
    );
    worker.close(&reader)?;
    assert_empty(&worker.status()?)?;
    assert!(authority(&store, "sessions")?
        .snapshot()?
        .root
        .pins
        .is_empty());
    worker.finish()?;
    Ok(())
}

#[test]
fn default_session_resource_admission_releases_capacity_after_close() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("objects");
    let mut worker = Worker::spawn(&store, &root.path().join("worker"))?;
    let identifier = database("admission", "main");
    let limit = WorkerLimits::default().max_sessions_per_database;
    let mut sessions = Vec::new();
    let mut distinct = BTreeSet::new();
    for _ in 0..limit {
        let session = worker.open(&identifier)?;
        ensure!(distinct.insert(session.clone()), "active session ID reused");
        sessions.push(session);
    }
    worker.rejected(json!({"op":"open", "database":identifier, "epoch":0}))?;
    let status = worker.status()?;
    assert_eq!(status["loaded_databases"], 1);
    assert_eq!(status["active_sessions"], limit);
    assert_cache_limits(&resources(&status, &identifier)?)?;
    worker.close(&sessions.remove(0))?;
    sessions.push(worker.open(&identifier)?);
    assert_eq!(worker.status()?["active_sessions"], limit);
    for session in sessions {
        worker.close(&session)?;
    }
    worker.success(json!({"op":"evict", "idle_ms":0}))?;
    assert_empty(&worker.status()?)?;
    worker.finish()?;
    Ok(())
}

#[test]
fn stdin_eof_aborts_uncommitted_sessions_and_releases_database_handles() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("objects");
    let cache = root.path().join("reusable-worker-cache");
    let identifier = database("eof", "main");
    let mut first = Worker::spawn(&store, &cache)?;
    let session = first.open(&identifier)?;
    first.exec(
        &session,
        "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)",
    )?;
    first.exec(&session, "BEGIN; INSERT INTO items VALUES(2)")?;
    first.finish()?;
    assert!(authority(&store, "eof")?.snapshot()?.root.pins.is_empty());
    let mut second = Worker::spawn(&store, &cache)?;
    let session = second.open(&identifier)?;
    assert_eq!(
        second.query(&session, "SELECT count(*) FROM items")?,
        json!([[1]])
    );
    second.exec(&session, "INSERT INTO items VALUES(3)")?;
    assert_eq!(
        second.query(&session, "SELECT count(*) FROM items")?,
        json!([[2]])
    );
    second.finish()?;
    assert_cache_only(&cache)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn acknowledged_sql_tail_survives_sigkill_and_entire_cache_removal() -> Result<()> {
    use std::os::unix::process::ExitStatusExt;

    let root = tempfile::tempdir()?;
    let store = root.path().join("objects");
    let first_cache = root.path().join("worker-a");
    let second_cache = root.path().join("worker-b-empty");
    let identifier = database("disposal", "main");
    let mut first = Worker::spawn(&store, &first_cache)?;
    let session = first.open(&identifier)?;
    first.exec(&session, "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO items VALUES(1, 'base')")?;
    first.operation(&session, "checkpoint")?;
    let control = authority(&store, "disposal")?;
    let checkpoint = control.snapshot()?.root.branches["main"].current_sequence;
    first.exec(&session, "BEGIN; INSERT INTO items VALUES(2, 'acked-tail'); INSERT INTO items VALUES(3, 'acked-tail'); COMMIT")?;
    ensure!(
        control.snapshot()?.root.branches["main"].current_sequence > checkpoint,
        "no durable native commit point recorded for acknowledged tail"
    );
    ensure!(
        metric(&resources(&first.status()?, &identifier)?, "wal_bytes")? > 32,
        "test did not leave a WAL tail"
    );
    let status = first.kill()?;
    assert_eq!(status.signal(), Some(9));
    fs::remove_dir_all(&first_cache)?;
    ensure!(
        !first_cache.exists() && !second_cache.exists(),
        "cache disposal/cold setup failed"
    );
    let mut second = Worker::spawn(&store, &second_cache)?;
    assert_empty(&second.status()?)?;
    let recovered = second.open(&identifier)?;
    assert_eq!(
        second.query(&recovered, "SELECT id,value FROM items ORDER BY id")?,
        json!([[1, "base"], [2, "acked-tail"], [3, "acked-tail"]])
    );
    assert_eq!(
        second.query(&recovered, "PRAGMA integrity_check")?,
        json!([["ok"]])
    );
    second.finish()?;
    assert_cache_only(&second_cache)?;
    assert_authority_layout(&store, "disposal")?;
    Ok(())
}

#[test]
fn large_checkpointed_migration_is_lazy_and_old_pitr_survives_compaction() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("objects");
    let first_cache = root.path().join("worker-a");
    let second_cache = root.path().join("worker-b-empty");
    let identifier = database("migration", "main");
    let mut first = Worker::spawn(&store, &first_cache)?;
    let session = first.open(&identifier)?;
    let mut sql =
        String::from("BEGIN; CREATE TABLE records(id INTEGER PRIMARY KEY, payload TEXT);");
    let payload = "x".repeat(2048);
    for record in 0..1000 {
        sql.push_str(&format!(
            "INSERT INTO records VALUES({record}, '{payload}');"
        ));
    }
    sql.push_str("COMMIT;");
    first.exec(&session, &sql)?;
    let control = authority(&store, "migration")?;
    let capabilities = Capabilities::new(control.clone());
    let snapshot = capabilities.snapshot("main", "before-compact")?;
    first.exec(
        &session,
        "INSERT INTO records VALUES(1000, 'post-snapshot')",
    )?;
    first.operation(&session, "checkpoint")?;
    first.operation(&session, "compact")?;
    let current = manifest(&control, "main")?;
    ensure!(
        current.segments.len() > 16,
        "fixture is not a large segmented DB: {}",
        current.segments.len()
    );
    let usage = resources(&first.status()?, &identifier)?;
    ensure!(
        metric(&usage, "wal_bytes")? <= 64 * 1024,
        "compaction retained a full current WAL image: {usage}"
    );
    ensure!(
        metric(&usage, "pending_wal_bytes")? == 0,
        "compaction left pending WAL payload: {usage}"
    );
    first.close(&session)?;
    first.success(json!({"op":"unload", "database":identifier}))?;
    assert_empty(&first.status()?)?;
    first.finish()?;
    fs::remove_dir_all(&first_cache)?;
    ensure!(!second_cache.exists(), "destination cache is not empty");
    let mut second = Worker::spawn(&store, &second_cache)?;
    let session = second.open(&identifier)?;
    let opened = resources(&second.status()?, &identifier)?;
    let opening_loads = metric(&opened, "segment_loads")?;
    ensure!(
        opening_loads <= 4,
        "cold open fetched too many segments: {opened}"
    );
    ensure!(
        metric(&opened, "wal_bytes")? <= 64 * 1024,
        "cold open replayed archived WAL history: {opened}"
    );
    assert_eq!(
        second.query(
            &session,
            "SELECT id,length(payload) FROM records WHERE id=999"
        )?,
        json!([[999, 2048]])
    );
    let queried = resources(&second.status()?, &identifier)?;
    let point_loads = metric(&queried, "segment_loads")?;
    ensure!(
        point_loads <= 8 && point_loads < current.segments.len() as u64,
        "point query fetched the complete database: {queried}"
    );
    assert_cache_limits(&queried)?;
    eprintln!("P3 process migration: rows=1000 payload_bytes=2048 manifest_segments={} cold_open_segment_loads={opening_loads} point_query_segment_loads={point_loads} current_wal_bytes={}", current.segments.len(), metric(&opened, "wal_bytes")?);
    assert_cache_only(&second_cache)?;
    let point = capabilities.pitr("main", snapshot.sequence)?;
    capabilities.branch("main", "past")?;
    capabilities.restore_point("past", &point)?;
    let past = second.open(&database("migration", "past"))?;
    assert_eq!(
        second.query(&past, "SELECT count(*) FROM records")?,
        json!([[1000]])
    );
    capabilities.clone("before-compact", "snapshot-copy")?;
    let cloned = second.open(&database("migration", "snapshot-copy"))?;
    assert_eq!(
        second.query(&cloned, "SELECT count(*) FROM records")?,
        json!([[1000]])
    );
    assert_eq!(
        second.query(&session, "SELECT count(*) FROM records")?,
        json!([[1001]])
    );
    second.finish()?;
    assert_cache_only(&second_cache)?;
    assert_authority_layout(&store, "migration")?;
    counting::assert_lazy_current_stream(
        &store,
        &root.path().join("worker-c-counted-empty"),
        "migration",
    )?;
    Ok(())
}
