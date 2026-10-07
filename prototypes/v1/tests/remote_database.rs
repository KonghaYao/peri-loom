use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use anyhow::{bail, Result};
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{
    LocalObjectStore, ObjectStore, PreconditionFailed, PutCondition, StoredObject,
};
use peri_loom_v1::wal::{DurableWal, RemoteWal, WalBatch, WalOperation};
use turso_core::Value;

fn namespace() -> Namespace {
    Namespace::new("tenant", "database").unwrap()
}

fn open(store: Arc<dyn ObjectStore>, cache: &Path) -> Result<RemoteDatabase> {
    RemoteDatabase::open(store, namespace(), cache, 0)
}

fn count(database: &RemoteDatabase) -> Result<i64> {
    let rows = database.query("SELECT count(*) FROM items")?;
    match &rows[0][0] {
        Value::Numeric(turso_core::Numeric::Integer(value)) => Ok(*value),
        other => bail!("unexpected count {other:?}"),
    }
}

#[test]
fn cold_cache_restores_committed_wal_tail_and_checkpoint() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = open(store.clone(), cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    database.checkpoint()?;
    let checkpoint_lsn = database.storage().manifest().applied_lsn;
    database.execute("BEGIN; INSERT INTO items VALUES(1, 'remote'); INSERT INTO items VALUES(2, 'tail'); COMMIT;")?;
    assert!(database.io().wal_lsn() > checkpoint_lsn);
    assert_eq!(count(&database)?, 2);
    assert!(!cache.path().join("database").exists());
    assert!(!cache.path().join("database-wal").exists());
    drop(database);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    let database = open(store.clone(), fresh.path())?;
    assert_eq!(count(&database)?, 2);
    database.checkpoint()?;
    assert!(
        database
            .storage()
            .manifest()
            .applied_wal_position
            .frame_count
            > 0
    );
    drop(database);
    fresh.close()?;
    let third = tempfile::tempdir()?;
    assert_eq!(count(&open(store, third.path())?)?, 2);
    Ok(())
}

struct FaultStore {
    inner: LocalObjectStore,
    mode: AtomicU8,
}

impl ObjectStore for FaultStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let mode = self.mode.load(Ordering::SeqCst);
        if key.ends_with("wal/index") && mode == 1 {
            bail!(PreconditionFailed);
        }
        if key.contains("wal/chunks/") && mode == 2 {
            bail!("injected WAL chunk upload failure");
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if key.ends_with("wal/index") && mode == 3 {
            self.mode.store(0, Ordering::SeqCst);
            bail!("lost index acknowledgement");
        }
        Ok(etag)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn sql_commit_requires_remote_index_and_chunk_durability() -> Result<()> {
    for mode in [1, 2] {
        let objects = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let store = Arc::new(FaultStore {
            inner: LocalObjectStore::open(objects.path())?,
            mode: AtomicU8::new(0),
        });
        let database = open(store.clone(), cache.path())?;
        database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
        database.checkpoint()?;
        database.execute("BEGIN; INSERT INTO items VALUES(1);")?;
        store.mode.store(mode, Ordering::SeqCst);
        assert!(database.execute("COMMIT").is_err());
        drop(database);
        store.mode.store(0, Ordering::SeqCst);
        cache.close()?;
        let fresh = tempfile::tempdir()?;
        assert_eq!(count(&open(store, fresh.path())?)?, 0);
    }
    Ok(())
}

#[test]
fn rollback_and_torn_remote_wal_do_not_recover_uncommitted_rows() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = open(store.clone(), cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    database.execute("INSERT INTO items VALUES(1)")?;
    database.execute("BEGIN; INSERT INTO items VALUES(2); ROLLBACK;")?;
    let wal = RemoteWal::new(store.clone());
    let records = wal.read(&namespace(), 0)?;
    let mut wal_size = 0;
    for record in records {
        for operation in record.batch.operations {
            match operation {
                WalOperation::Write { offset, bytes } => {
                    wal_size = wal_size.max(offset + bytes.len() as u64)
                }
                WalOperation::Truncate { size } => wal_size = size,
            }
        }
    }
    wal.append(
        &namespace(),
        0,
        WalBatch {
            operations: vec![WalOperation::Write {
                offset: wal_size,
                bytes: vec![0x37; 127],
            }],
        },
    )?;
    drop(database);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    assert_eq!(count(&open(store, fresh.path())?)?, 1);
    Ok(())
}

#[test]
fn wal_history_baseline_preserves_header_and_lost_ack_is_not_duplicated() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let store = Arc::new(FaultStore {
        inner: LocalObjectStore::open(objects.path())?,
        mode: AtomicU8::new(3),
    });
    let wal = RemoteWal::new(store.clone());
    let batch = |offset, bytes: &[u8]| WalBatch {
        operations: vec![WalOperation::Write {
            offset,
            bytes: bytes.to_vec(),
        }],
    };
    assert_eq!(wal.append(&namespace(), 0, batch(0, b"header"))?, 1);
    assert_eq!(wal.read(&namespace(), 0)?.len(), 1);
    assert_eq!(wal.append(&namespace(), 0, batch(6, b"first"))?, 2);
    assert_eq!(wal.append(&namespace(), 0, batch(11, b"tail"))?, 3);
    assert_eq!(wal.read(&namespace(), 1)?.len(), 3);
    wal.truncate(&namespace(), 3)?;
    let records = wal.read(&namespace(), 0)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].lsn, 2);
    assert_eq!(
        records[0].batch.operations[1],
        WalOperation::Write {
            offset: 0,
            bytes: b"headerfirst".to_vec()
        }
    );
    assert!(wal.read(&namespace(), 1).is_err());
    assert_eq!(wal.metadata(&namespace())?.replay_baseline_lsn, 2);
    assert_eq!(wal.append(&namespace(), 0, batch(15, b"next"))?, 4);
    Ok(())
}

#[test]
fn cache_lock_covers_retained_native_file_handles() -> Result<()> {
    use turso_core::io::{OpenFlags, IO};
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = open(store.clone(), cache.path())?;
    let io = database.io();
    let file = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    assert!(open(store.clone(), cache.path()).is_err());
    drop(database);
    assert!(open(store.clone(), cache.path()).is_err());
    drop(io);
    assert!(open(store.clone(), cache.path()).is_err());
    drop(file);
    let reopened = open(store, cache.path())?;
    drop(reopened);
    Ok(())
}

#[test]
fn truncated_history_still_replays_real_sql_wal_header() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = open(store.clone(), cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    database.execute("INSERT INTO items VALUES(1)")?;
    database.execute("INSERT INTO items VALUES(2)")?;
    let lsn = database.io().wal_lsn();
    assert!(lsn > 2);
    let wal = RemoteWal::new(store.clone());
    wal.truncate(&namespace(), lsn)?;
    drop(database);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    let database = open(store, fresh.path())?;
    assert_eq!(count(&database)?, 2);
    database.execute("INSERT INTO items VALUES(3)")?;
    assert_eq!(count(&database)?, 3);
    Ok(())
}

#[test]
fn native_wal_reset_is_guarded_and_remote_only() -> Result<()> {
    use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let io = peri_loom_v1::io::RemoteIO::open(store, namespace(), cache.path(), 0, 1024, 4096)?;
    let main = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    let buffer = Arc::new(Buffer::new_temporary(64));
    buffer.as_mut_slice().copy_from_slice(&[0x7f; 64]);
    let completion = wal.pwrite(
        0,
        buffer,
        Completion::new_write(|result| assert_eq!(result.unwrap(), 64)),
    )?;
    assert!(completion.finished());
    let blocked = Completion::new_trunc(|result| assert!(result.is_err()));
    assert!(wal.truncate(0, blocked.clone()).is_err());
    assert!(blocked.finished());
    let completion = wal.sync(
        Completion::new_sync(|result| assert!(result.is_ok())),
        FileSyncType::Fsync,
    )?;
    assert!(completion.finished());
    assert!(wal
        .truncate(0, Completion::new_trunc(|result| assert!(result.is_err())))
        .is_err());
    let buffer = Arc::new(Buffer::new_temporary(3));
    buffer.as_mut_slice().copy_from_slice(b"abc");
    let completion = main.pwrite(
        0,
        buffer,
        Completion::new_write(|result| assert_eq!(result.unwrap(), 3)),
    )?;
    assert!(completion.finished());
    assert!(main
        .sync(
            Completion::new_sync(|result| assert!(result.is_err())),
            FileSyncType::Fsync
        )
        .is_err());
    assert!(io
        .remove_file(&format!("{}-wal", io.database_path()))
        .is_err());
    main.checkpoint_wal_position([1, 2], 1)?;
    let completion = main.sync(
        Completion::new_sync(|result| assert!(result.is_ok())),
        FileSyncType::Fsync,
    )?;
    assert!(completion.finished());
    assert_eq!(io.storage().manifest().applied_lsn, 1);
    let completion = wal.truncate(0, Completion::new_trunc(|result| assert!(result.is_ok())))?;
    assert!(completion.finished());
    assert_eq!(wal.size()?, 0);
    assert_eq!(io.wal_lsn(), 2);
    assert!(!cache.path().join("database-wal").exists());
    Ok(())
}

struct ContendedStore {
    inner: LocalObjectStore,
    barrier: std::sync::Barrier,
    reads: std::sync::atomic::AtomicUsize,
    conflicts: std::sync::atomic::AtomicUsize,
}

impl ObjectStore for ContendedStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let object = self.inner.get(key)?;
        if key.ends_with("wal/index") && self.reads.fetch_add(1, Ordering::SeqCst) < 8 {
            self.barrier.wait();
        }
        Ok(object)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let result = self.inner.put(key, bytes, condition);
        if result
            .as_ref()
            .is_err_and(|error| error.downcast_ref::<PreconditionFailed>().is_some())
        {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn concurrent_cas_appends_assign_unique_lsns_even_for_identical_batches() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let store = Arc::new(ContendedStore {
        inner: LocalObjectStore::open(objects.path())?,
        barrier: std::sync::Barrier::new(8),
        reads: std::sync::atomic::AtomicUsize::new(0),
        conflicts: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut threads = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        threads.push(std::thread::spawn(move || {
            RemoteWal::new(store).append(
                &namespace(),
                0,
                WalBatch {
                    operations: vec![WalOperation::Write {
                        offset: 0,
                        bytes: b"same".to_vec(),
                    }],
                },
            )
        }));
    }
    let mut lsns = Vec::new();
    for thread in threads {
        lsns.push(thread.join().unwrap()?);
    }
    lsns.sort();
    assert_eq!(lsns, (1..=8).collect::<Vec<_>>());
    assert!(store.conflicts.load(Ordering::SeqCst) >= 7);
    let wal = RemoteWal::new(store);
    assert_eq!(wal.read(&namespace(), 0)?.len(), 8);
    let other = Namespace::new("other", "database")?;
    assert!(wal.read(&other, 0)?.is_empty());
    wal.fence(&namespace(), 2)?;
    assert!(wal.append(&namespace(), 0, WalBatch::default()).is_err());
    assert_eq!(wal.append(&namespace(), 2, WalBatch::default())?, 9);
    Ok(())
}

#[test]
fn missing_committed_chunk_fails_cold_open_instead_of_silent_data_loss() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = open(store.clone(), cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    database.execute("INSERT INTO items VALUES(1)")?;
    drop(database);
    let chunks = store.list(&namespace().key("wal/chunks")?)?;
    assert!(!chunks.is_empty());
    for key in chunks {
        store.delete(&key, None)?;
    }
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    assert!(open(store, fresh.path()).is_err());
    Ok(())
}

#[test]
fn same_batch_synchronous_off_cannot_bypass_remote_commit_ack() -> Result<()> {
    for mode in [0, 1] {
        let objects = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let store = Arc::new(FaultStore {
            inner: LocalObjectStore::open(objects.path())?,
            mode: AtomicU8::new(0),
        });
        let database = open(store.clone(), cache.path())?;
        database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
        database.checkpoint()?;
        let previous = database.io().wal_lsn();
        store.mode.store(mode, Ordering::SeqCst);
        let result =
            database.execute("PRAGMA synchronous=OFF; BEGIN; INSERT INTO items VALUES(1); COMMIT;");
        if mode == 0 {
            result?;
            assert!(database.io().wal_lsn() > previous);
        } else {
            assert!(result.is_err());
        }
        drop(database);
        store.mode.store(0, Ordering::SeqCst);
        cache.close()?;
        let fresh = tempfile::tempdir()?;
        assert_eq!(
            count(&open(store, fresh.path())?)?,
            if mode == 0 { 1 } else { 0 }
        );
    }
    Ok(())
}

#[test]
fn query_pragma_cannot_disable_next_commit_and_host_files_are_rejected() -> Result<()> {
    use turso_core::io::{OpenFlags, TempFile, IO};
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(FaultStore {
        inner: LocalObjectStore::open(objects.path())?,
        mode: AtomicU8::new(0),
    });
    let database = open(store.clone(), cache.path())?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    database.query("PRAGMA synchronous=OFF")?;
    let io = database.io();
    let path = cache.path().join("attached.db");
    assert!(io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .is_err());
    assert!(!path.exists());
    let dynamic: Arc<dyn IO> = io.clone();
    let temporary = TempFile::new(&dynamic)?;
    drop(temporary);
    store.mode.store(1, Ordering::SeqCst);
    assert!(database.execute("INSERT INTO items VALUES(1)").is_err());
    assert!(io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .is_err());
    assert!(!path.exists());
    let dynamic: Arc<dyn IO> = io;
    assert!(TempFile::new(&dynamic).is_err());
    assert!(database.writer_admission_status()?.quarantined);
    Ok(())
}

#[test]
fn cache_directory_lock_is_cross_process_even_without_native_locks() -> Result<()> {
    if let Some(cache) = std::env::var_os("PERI_P1_CACHE_LOCK_PROBE") {
        let store = Arc::new(LocalObjectStore::open(
            std::env::var_os("PERI_P1_LOCK_STORE").unwrap(),
        )?);
        assert!(open(store, Path::new(&cache)).is_err());
        return Ok(());
    }
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let _database = open(store, cache.path())?;
    let status = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "cache_directory_lock_is_cross_process_even_without_native_locks",
            "--nocapture",
        ])
        .env("PERI_P1_CACHE_LOCK_PROBE", cache.path())
        .env("PERI_P1_LOCK_STORE", objects.path())
        .env("LIMBO_DISABLE_FILE_LOCK", "1")
        .status()?;
    assert!(status.success());
    Ok(())
}
