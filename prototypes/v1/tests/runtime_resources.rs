use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use peri_loom_v1::authority::Authority;
use peri_loom_v1::capabilities::Capabilities;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::io::RemoteIO;
use peri_loom_v1::manifest::SEGMENT_SIZE;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::recovery::validate_index_commit;
use peri_loom_v1::wal::{DurableWal, RemoteWal};
use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};

fn namespace() -> Result<Namespace> {
    Namespace::new("resources", "database")
}

struct ObservedStore {
    inner: Arc<dyn ObjectStore>,
    chunk_bytes: AtomicUsize,
    chunk_gets: AtomicUsize,
    fail_index_countdown: AtomicUsize,
}

impl ObservedStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            chunk_bytes: AtomicUsize::new(0),
            chunk_gets: AtomicUsize::new(0),
            fail_index_countdown: AtomicUsize::new(0),
        }
    }

    fn reset_reads(&self) {
        self.chunk_bytes.store(0, Ordering::SeqCst);
        self.chunk_gets.store(0, Ordering::SeqCst);
    }
}

impl ObjectStore for ObservedStore {
    fn validate_session(&self) -> Result<()> {
        self.inner.validate_session()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let object = self.inner.get(key)?;
        if key.contains("/wal/chunks/") {
            if let Some(object) = &object {
                self.chunk_bytes
                    .fetch_add(object.bytes.len(), Ordering::SeqCst);
                self.chunk_gets.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(object)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.ends_with("/wal/index") {
            let countdown = self.fail_index_countdown.load(Ordering::SeqCst);
            if countdown != 0 {
                self.fail_index_countdown.fetch_sub(1, Ordering::SeqCst);
                if countdown == 1 {
                    bail!("injected durable WAL index failure");
                }
            }
        }
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
fn resource_fields_count_payload_not_logical_file_sizes() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let io = RemoteIO::open(
        store,
        namespace()?,
        cache.path(),
        0,
        SEGMENT_SIZE,
        SEGMENT_SIZE as u64,
    )?;
    let empty = io.resources()?;
    assert_eq!(empty.segment_ram_bytes, 0);
    assert_eq!(empty.segment_disk_bytes, 0);
    assert_eq!(empty.dirty_segment_bytes, 0);
    assert_eq!(empty.wal_bytes, 0);
    assert_eq!(empty.pending_wal_bytes, 0);
    assert_eq!(empty.segment_loads, 0);
    let main = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    for offset in [0, 64] {
        let payload = Arc::new(Buffer::new_temporary(64));
        payload.as_mut_slice().fill(0x7f);
        let completion = wal.pwrite(
            offset,
            payload,
            Completion::new_write(|result| assert_eq!(result.unwrap(), 64)),
        )?;
        assert!(completion.finished());
    }
    let payload = Arc::new(Buffer::new_temporary(7));
    payload.as_mut_slice().fill(0x55);
    let completion = main.pwrite(
        SEGMENT_SIZE as u64 - 3,
        payload,
        Completion::new_write(|result| assert_eq!(result.unwrap(), 7)),
    )?;
    assert!(completion.finished());
    let dirty = io.resources()?;
    assert_eq!(dirty.dirty_segment_bytes, (2 * SEGMENT_SIZE) as u64);
    assert_eq!(dirty.segment_ram_bytes, 0);
    assert_eq!(dirty.segment_disk_bytes, 0);
    assert_eq!(dirty.segment_loads, 0);
    let pending = io.resources()?;
    assert_eq!(pending.wal_bytes, 128);
    assert_eq!(pending.pending_wal_bytes, 128);
    let completion = wal.sync(
        Completion::new_sync(|result| assert!(result.is_ok())),
        FileSyncType::Fsync,
    )?;
    assert!(completion.finished());
    assert_eq!(io.resources()?.pending_wal_bytes, 0);
    main.checkpoint_wal_position([1, 2], 1)?;
    let completion = main.sync(
        Completion::new_sync(|result| assert!(result.is_ok())),
        FileSyncType::Fsync,
    )?;
    assert!(completion.finished());
    let published = io.resources()?;
    assert_eq!(published.dirty_segment_bytes, 0);
    assert_eq!(published.segment_ram_bytes, 0);
    assert_eq!(published.segment_disk_bytes, 0);
    assert_eq!(published.wal_bytes, 128);
    assert_eq!(published.segment_loads, 0);
    let mut payload = [0; 7];
    io.storage()
        .read_at(SEGMENT_SIZE as u64 - 3, &mut payload)?;
    assert_eq!(payload, [0x55; 7]);
    let loaded = io.resources()?;
    assert_eq!(loaded.segment_ram_bytes, SEGMENT_SIZE);
    assert_eq!(loaded.segment_disk_bytes, SEGMENT_SIZE as u64);
    assert_eq!(loaded.segment_loads, 2);
    let completion = wal.truncate(0, Completion::new_trunc(|result| assert!(result.is_ok())))?;
    assert!(completion.finished());
    assert_eq!(io.resources()?.wal_bytes, 0);
    assert_eq!(io.resources()?.pending_wal_bytes, 0);
    Ok(())
}

#[test]
fn native_resource_accounting_tracks_cache_caps_and_compacted_wal() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = RemoteDatabase::open_with_limits(
        store,
        namespace()?,
        cache.path(),
        0,
        SEGMENT_SIZE,
        SEGMENT_SIZE as u64,
    )?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload BLOB); INSERT INTO items VALUES(1, zeroblob(400000))")?;
    let committed = database.resources()?;
    assert!(committed.wal_bytes > 400000);
    assert_eq!(committed.pending_wal_bytes, 0);
    database.compact_wal()?;
    let compacted = database.resources()?;
    assert_eq!(compacted.wal_bytes, 0);
    assert_eq!(compacted.pending_wal_bytes, 0);
    assert_eq!(compacted.dirty_segment_bytes, 0);
    assert!(compacted.segment_ram_bytes <= SEGMENT_SIZE);
    assert!(compacted.segment_disk_bytes <= SEGMENT_SIZE as u64);
    let cache_stats = database.storage().stats();
    assert_eq!(compacted.segment_ram_bytes, cache_stats.cache.ram_bytes);
    assert_eq!(compacted.segment_disk_bytes, cache_stats.cache.disk_bytes);
    assert_eq!(compacted.segment_loads, cache_stats.segment_loads);
    assert_eq!(
        database.query("SELECT length(payload) FROM items WHERE id=1")?[0][0].to_string(),
        "400000"
    );
    Ok(())
}

#[test]
fn compacted_cold_open_is_lazy_and_old_immutable_recovery_remains_valid() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let authority = Authority::open(
        Arc::new(LocalObjectStore::open(objects.path())?),
        namespace()?,
    )?;
    let capabilities = Capabilities::new(authority.clone());
    let store = Arc::new(ObservedStore::new(authority.branch_store("main")?));
    let database = RemoteDatabase::open_with_limits(
        store.clone(),
        namespace()?,
        cache.path(),
        0,
        SEGMENT_SIZE,
        (2 * SEGMENT_SIZE) as u64,
    )?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload BLOB)")?;
    database.execute("INSERT INTO items VALUES(1, zeroblob(1024))")?;
    let point = capabilities.snapshot("main", "early")?;
    let old_index = store
        .get(&namespace()?.key("wal/index")?)?
        .context("old index missing")?
        .bytes;
    for identifier in 2..=21 {
        database.execute(&format!(
            "INSERT INTO items VALUES({identifier}, zeroblob(200000))"
        ))?;
    }
    assert!(database.resources()?.wal_bytes > 4_000_000);
    database.compact_wal()?;
    let segment_count = database.storage().manifest().segments.len();
    assert!(segment_count > 25);
    let compacted_index = store
        .get(&namespace()?.key("wal/index")?)?
        .context("compacted index missing")?
        .bytes;
    let metadata: serde_json::Value = serde_json::from_slice(&compacted_index)?;
    assert_eq!(metadata["entries"].as_array().unwrap().len(), 1);
    assert!(metadata["commit"].is_null());
    assert!(
        validate_index_commit(&old_index, &namespace()?, |key| Ok(store
            .get(key)?
            .context("old chunk missing")?
            .bytes))?
        .is_some()
    );
    let records = RemoteWal::new(store.clone()).read(&namespace()?, 0)?;
    assert_eq!(records.len(), 1);
    drop(database);
    std::fs::remove_dir_all(cache.path())?;
    store.reset_reads();
    let cold = tempfile::tempdir()?;
    let database = RemoteDatabase::open_with_limits(
        store.clone(),
        namespace()?,
        cold.path(),
        0,
        SEGMENT_SIZE,
        (2 * SEGMENT_SIZE) as u64,
    )?;
    assert!(store.chunk_bytes.load(Ordering::SeqCst) < 4096);
    assert!(store.chunk_gets.load(Ordering::SeqCst) <= 2);
    assert_eq!(database.resources()?.wal_bytes, 0);
    assert!(database.resources()?.segment_loads < segment_count as u64 / 2);
    assert_eq!(
        database.query("SELECT length(payload) FROM items WHERE id=21")?[0][0].to_string(),
        "200000"
    );
    assert!(database.resources()?.segment_loads < segment_count as u64 / 2);
    assert!(!cold.path().join("database").exists());
    assert!(!cold.path().join("database-wal").exists());
    database.execute("INSERT INTO items VALUES(22, zeroblob(1))")?;
    database.compact_wal()?;
    drop(database);
    capabilities.restore_point("main", &point)?;
    let old_cache = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(
        authority.branch_store("main")?,
        namespace()?,
        old_cache.path(),
        0,
    )?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    assert_eq!(
        restored.query("SELECT length(payload) FROM items WHERE id=1")?[0][0].to_string(),
        "1024"
    );
    Ok(())
}

#[test]
fn failed_commit_prevents_compaction_and_cold_recovery_excludes_unacked_write() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(ObservedStore::new(Arc::new(LocalObjectStore::open(
        objects.path(),
    )?)));
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        namespace()?,
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let session = database.session()?;
    session.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    store.fail_index_countdown.store(1, Ordering::SeqCst);
    assert!(session.execute("COMMIT").is_err());
    assert!(database.resources()?.pending_wal_bytes > 0);
    assert!(database.compact_wal().is_err());
    drop(session);
    drop(database);
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(store, namespace()?, cold.path(), 0)?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

#[test]
fn failed_compaction_cas_is_closed_and_durable_database_still_recovers() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(ObservedStore::new(Arc::new(LocalObjectStore::open(
        objects.path(),
    )?)));
    let database = RemoteDatabase::open(store.clone(), namespace()?, cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    store.fail_index_countdown.store(2, Ordering::SeqCst);
    assert!(database.compact_wal().is_err());
    assert_eq!(store.fail_index_countdown.load(Ordering::SeqCst), 0);
    assert!(database.compact_wal().is_err());
    assert!(database.execute("INSERT INTO items VALUES(2)").is_err());
    drop(database);
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(store, namespace()?, cold.path(), 0)?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    restored.compact_wal()?;
    Ok(())
}
