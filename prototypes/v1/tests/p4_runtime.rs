#[path = "p4_runtime_helpers/clock_gate.rs"]
mod clock_gate;
#[path = "p4_runtime_helpers/native.rs"]
mod native;
#[path = "p4_runtime_helpers/remote.rs"]
mod remote;
#[path = "p4_runtime_helpers/scenarios.rs"]
mod scenarios;

use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use anyhow::Result;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::io::RemoteIO;
use peri_loom_v1::manifest::WalPosition;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::ownership::types::{AccessRole, LeaseClock};
use peri_loom_v1::ownership::SystemLeaseClock;
use peri_loom_v1::wal::{DurableWal, RemoteWal, WalBatch};
use turso_core::io::{Buffer, Completion, FileSyncType, OpenFlags, IO};

use clock_gate::Gate;
use native::PolicyStore;

fn namespace() -> Result<Namespace> {
    Namespace::new("p4", "native")
}

#[test]
fn cached_page_io_and_dirty_ram_writes_do_not_fetch_authority_root() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = PolicyStore::new(
        Arc::new(LocalObjectStore::open(objects.path())?),
        AccessRole::Writer,
    );
    let authority = peri_loom_v1::authority::Authority::open(backing.clone(), namespace()?)?;
    let scoped = PolicyStore::new(authority.branch_store("main")?, AccessRole::Writer);
    let database = RemoteDatabase::open(scoped.clone(), namespace()?, cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    database.checkpoint()?;
    let io = database.io();
    let main = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    let header = Arc::new(Buffer::new_temporary(64));
    assert!(main
        .pread(0, Completion::new_read(header.clone(), |_| None))?
        .finished());
    let roots = backing.root_gets.load(Ordering::SeqCst);
    let validations = scoped.session_checks.load(Ordering::SeqCst);
    for _ in 0..100 {
        assert!(main
            .pread(0, Completion::new_read(header.clone(), |_| None))?
            .finished());
        let buffer = Arc::new(Buffer::new_temporary(32));
        assert!(wal
            .pread(0, Completion::new_read(buffer, |_| None))?
            .finished());
        main.size()?;
        wal.size()?;
    }
    let completion = main.pwrite(
        0,
        header.clone(),
        Completion::new_write(|result| {
            assert_eq!(result.unwrap(), 64);
        }),
    )?;
    assert!(completion.finished());
    database.storage().write_at(0, header.as_slice())?;
    database.storage().truncate(database.storage().size())?;
    let position = database.storage().manifest().applied_wal_position;
    main.checkpoint_wal_position(position.salts, position.frame_count)?;
    assert_eq!(backing.root_gets.load(Ordering::SeqCst), roots);
    assert_eq!(scoped.session_checks.load(Ordering::SeqCst), validations);
    let completion = main.sync(
        Completion::new_sync(|result| assert!(result.is_ok())),
        FileSyncType::Fsync,
    )?;
    assert!(completion.finished());
    assert!(scoped.session_checks.load(Ordering::SeqCst) > validations);
    assert!(backing.root_gets.load(Ordering::SeqCst) > roots);
    scoped.mode.store(2, Ordering::SeqCst);
    assert!(main
        .pread(0, Completion::new_read(header, |_| None))
        .is_err());
    assert!(wal.size().is_err());
    Ok(())
}

#[test]
fn readonly_is_native_on_every_connection_and_cannot_mutate_remote_io() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    let writer = RemoteDatabase::open(backing.clone(), namespace()?, cache.path(), 0)?;
    writer.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(7)")?;
    writer.compact_wal()?;
    drop(writer);
    let cold = tempfile::tempdir()?;
    let store = PolicyStore::new(backing, AccessRole::ReadOnly);
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        namespace()?,
        cold.path(),
        0,
    )?);
    assert!(database.is_readonly()?);
    let session = database.session()?;
    assert!(session.is_readonly()?);
    assert_eq!(
        session.query("SELECT id FROM items")?[0][0].to_string(),
        "7"
    );
    for sql in [
        "INSERT INTO items VALUES(8) RETURNING id",
        "CREATE TABLE denied(id INTEGER)",
        "PRAGMA user_version=5",
        "ATTACH ':memory:' AS escape",
        "SELECT load_extension('/tmp/escape')",
    ] {
        assert!(
            session.query(sql).is_err(),
            "read-only query accepted {sql}"
        );
    }
    assert!(session
        .execute("PRAGMA synchronous=OFF; INSERT INTO items VALUES(9)")
        .is_err());
    assert!(database.checkpoint().is_err());
    assert!(database.compact_wal().is_err());
    assert!(database.storage().write_at(0, b"denied").is_err());
    assert!(database.storage().truncate(0).is_err());
    assert!(database
        .storage()
        .publish(WalPosition::default(), 0)
        .is_err());
    let wal = RemoteWal::new(store.clone());
    assert!(wal.append(&namespace()?, 0, WalBatch::default()).is_err());
    assert!(wal.truncate(&namespace()?, u64::MAX).is_err());
    let io = database.io();
    let file = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    let result = file.pwrite(
        0,
        Arc::new(Buffer::new_temporary(32)),
        Completion::new_write(|result| assert!(result.is_err())),
    );
    assert!(result.is_err());
    assert!(file
        .sync(
            Completion::new_sync(|result| assert!(result.is_err())),
            FileSyncType::Fsync
        )
        .is_err());
    assert!(file
        .truncate(0, Completion::new_trunc(|result| assert!(result.is_err())))
        .is_err());
    assert!(io
        .remove_file(&format!("{}-wal", io.database_path()))
        .is_err());
    assert_eq!(store.mutations.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn draining_checks_each_statement_without_rejecting_successful_commit_ack() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = PolicyStore::new(
        Arc::new(LocalObjectStore::open(objects.path())?),
        AccessRole::Writer,
    );
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        namespace()?,
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    let session = database.session()?;
    session.execute("BEGIN; INSERT INTO items VALUES(1)")?;
    store.mode.store(1, Ordering::SeqCst);
    let before = store.transaction_checks.load(Ordering::SeqCst);
    session.execute("COMMIT")?;
    assert_eq!(store.transaction_checks.load(Ordering::SeqCst), before + 1);
    assert!(!session.in_transaction()?);
    assert!(session.execute("BEGIN").is_err());
    assert!(session.query("SELECT count(*) FROM items").is_err());
    assert!(database.session().is_err());
    store.mode.store(0, Ordering::SeqCst);
    session.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    store.mode.store(1, Ordering::SeqCst);
    assert!(session
        .execute("COMMIT; BEGIN; INSERT INTO items VALUES(3)")
        .is_err());
    assert!(!session.in_transaction()?);
    drop(session);
    drop(database);
    store.mode.store(0, Ordering::SeqCst);
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(store, namespace()?, cold.path(), 0)?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    Ok(())
}

#[test]
fn revocation_blocks_cached_sql_and_drop_does_not_flush_unfinished_writer() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = PolicyStore::new(
        Arc::new(LocalObjectStore::open(objects.path())?),
        AccessRole::Writer,
    );
    let database = Arc::new(RemoteDatabase::open(
        store.clone(),
        namespace()?,
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let session = database.session()?;
    assert_eq!(
        session.query("SELECT id FROM items")?[0][0].to_string(),
        "1"
    );
    session.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    store.mode.store(2, Ordering::SeqCst);
    let mutations = store.mutations.load(Ordering::SeqCst);
    assert!(session.query("SELECT id FROM items").is_err());
    assert!(session.execute("COMMIT").is_err());
    assert!(database.checkpoint().is_err());
    assert!(database.storage().read_at(0, &mut [0; 16]).is_err());
    drop(session);
    drop(database);
    assert_eq!(store.mutations.load(Ordering::SeqCst), mutations);
    store.mode.store(0, Ordering::SeqCst);
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(store, namespace()?, cold.path(), 0)?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    restored.execute("INSERT INTO items VALUES(3)")?;
    Ok(())
}

#[test]
fn system_clock_is_suspend_inclusive_and_monotonic() -> Result<()> {
    let clock = SystemLeaseClock;
    let before = clock.now_ms()?;
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(clock.now_ms()? > before);
    Ok(())
}

#[test]
fn native_readonly_cold_recovery_uses_acknowledged_wal_without_writing() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    let writer = RemoteDatabase::open(backing.clone(), namespace()?, cache.path(), 0)?;
    writer.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(19)")?;
    assert!(writer.resources()?.wal_bytes > 32);
    drop(writer);
    std::fs::remove_dir_all(cache.path())?;
    let cold = tempfile::tempdir()?;
    let store = PolicyStore::new(backing, AccessRole::ReadOnly);
    let reader = RemoteDatabase::open(store.clone(), namespace()?, cold.path(), 0)?;
    assert!(reader.is_readonly()?);
    assert_eq!(
        reader.query("SELECT id FROM items")?[0][0].to_string(),
        "19"
    );
    assert_eq!(store.mutations.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn readonly_missing_database_fails_without_initializing_head() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = PolicyStore::new(
        Arc::new(LocalObjectStore::open(objects.path())?),
        AccessRole::ReadOnly,
    );
    assert!(RemoteDatabase::open(store.clone(), namespace()?, cache.path(), 0).is_err());
    assert_eq!(store.mutations.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn stale_native_file_handles_cannot_sync_publish_reset_or_append_after_revoke() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = PolicyStore::new(
        Arc::new(LocalObjectStore::open(objects.path())?),
        AccessRole::Writer,
    );
    let database = RemoteDatabase::open(store.clone(), namespace()?, cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let io = database.io();
    let main = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    let wal_file = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    store.mode.store(2, Ordering::SeqCst);
    let mutations = store.mutations.load(Ordering::SeqCst);
    assert!(main.checkpoint_wal_position([1, 2], 1).is_err());
    for file in [&main, &wal_file] {
        assert!(file
            .sync(
                Completion::new_sync(|result| assert!(result.is_err())),
                FileSyncType::Fsync
            )
            .is_err());
        assert!(file
            .truncate(0, Completion::new_trunc(|result| assert!(result.is_err())))
            .is_err());
        assert!(file
            .pwrite(
                0,
                Arc::new(Buffer::new_temporary(32)),
                Completion::new_write(|result| assert!(result.is_err()))
            )
            .is_err());
        assert!(file.size().is_err());
    }
    assert!(io
        .remove_file(&format!("{}-wal", io.database_path()))
        .is_err());
    assert!(RemoteWal::new(store.clone())
        .fence(&namespace()?, 1)
        .is_err());
    assert_eq!(store.mutations.load(Ordering::SeqCst), mutations);
    Ok(())
}

#[test]
fn dropping_large_unfinished_transaction_does_not_poison_next_writer() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = Arc::new(RemoteDatabase::open(
        backing.clone(),
        namespace()?,
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, payload BLOB); INSERT INTO items VALUES(1, zeroblob(1))")?;
    let writer = database.session()?;
    writer.execute("PRAGMA cache_size=5; BEGIN; INSERT INTO items VALUES(2, zeroblob(500000))")?;
    drop(writer);
    database
        .session()?
        .execute("INSERT INTO items VALUES(3, zeroblob(1))")?;
    let index = backing.get(&namespace()?.key("wal/index")?)?.unwrap().bytes;
    assert!(
        peri_loom_v1::recovery::validate_index_commit(&index, &namespace()?, |key| Ok(backing
            .get(key)?
            .unwrap()
            .bytes))?
        .is_some()
    );
    drop(database);
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(backing, namespace()?, cold.path(), 0)?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    assert_eq!(
        restored.query("SELECT max(id) FROM items")?[0][0].to_string(),
        "3"
    );
    Ok(())
}

/// A parked segment fetch for the main database must not stall WAL readers: both files share
/// the remote I/O state, but only the WAL branch needs the state lock.
#[test]
fn slow_main_segment_fetch_does_not_block_wal_reads() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    {
        let database = RemoteDatabase::open(backing.clone(), namespace()?, cache.path(), 0)?;
        database
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
        database.checkpoint()?;
    }
    // A fresh cache root makes the first main-database page read miss the segment cache and
    // reach the object store, where the gate parks it. Opening the I/O directly (rather than a
    // database) keeps the engine's own header read from warming that segment first.
    let cold = tempfile::tempdir()?;
    let gate = Arc::new(Gate::default());
    let store: Arc<dyn ObjectStore> = Arc::new(SegmentGateStore {
        inner: backing,
        gate: gate.clone(),
    });
    let io = RemoteIO::open(
        store,
        namespace()?,
        cold.path(),
        0,
        8 * 1024 * 1024,
        64 * 1024 * 1024,
    )?;
    let main = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;

    gate.arm(1);
    let parked = main.clone();
    let slow = std::thread::spawn(move || -> Result<()> {
        let header = Arc::new(Buffer::new_temporary(64));
        assert!(parked
            .pread(0, Completion::new_read(header, |_| None))?
            .finished());
        Ok(())
    });
    gate.wait_entered();

    // The WAL read touches only the in-memory image; it must complete while the segment GET
    // is still parked. The timeout keeps a regression from hanging the suite.
    let (sender, receiver) = mpsc::channel();
    let wal_reader = std::thread::spawn(move || {
        let buffer = Arc::new(Buffer::new_temporary(32));
        let outcome = wal
            .pread(0, Completion::new_read(buffer, |_| None))
            .map(|completion| completion.finished())
            .map_err(|error| format!("{error:?}"));
        let _ = sender.send(outcome);
    });
    let outcome = receiver.recv_timeout(Duration::from_secs(5));
    gate.release();
    let _ = wal_reader.join();
    match outcome {
        Ok(Ok(true)) => {}
        other => panic!("WAL read advanced only after the segment fetch: {other:?}"),
    }
    slow.join().unwrap()?;
    Ok(())
}

struct SegmentGateStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<Gate>,
}

impl ObjectStore for SegmentGateStore {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.contains("segments/") {
            self.gate.pause()?;
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

/// A parked WAL chunk upload must not stall readers of the WAL image: the append is
/// frozen under the state lock and then runs outside it.
#[test]
fn slow_wal_append_does_not_block_wal_reads() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    {
        let database = RemoteDatabase::open(backing.clone(), namespace()?, cache.path(), 0)?;
        database
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
        database.checkpoint()?;
    }
    let cold = tempfile::tempdir()?;
    let gate = Arc::new(Gate::default());
    let store: Arc<dyn ObjectStore> = Arc::new(WalChunkGateStore {
        inner: backing,
        gate: gate.clone(),
        fail: false,
    });
    let io = RemoteIO::open(
        store,
        namespace()?,
        cold.path(),
        0,
        8 * 1024 * 1024,
        64 * 1024 * 1024,
    )?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    let frame = Arc::new(Buffer::new_temporary(64));
    frame.as_mut_slice().fill(0);
    assert!(wal
        .pwrite(
            0,
            frame,
            Completion::new_write(|result| { assert_eq!(result.unwrap(), 64) })
        )?
        .finished());

    gate.arm(1);
    let parked = wal.clone();
    let appender = std::thread::spawn(move || -> Result<()> {
        drop(parked.sync(
            Completion::new_sync(|result| {
                result.expect("WAL sync");
            }),
            FileSyncType::Fsync,
        )?);
        Ok(())
    });
    gate.wait_entered();

    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let buffer = Arc::new(Buffer::new_temporary(32));
        let outcome = wal
            .pread(0, Completion::new_read(buffer, |_| None))
            .map(|completion| completion.finished())
            .map_err(|error| format!("{error:?}"));
        let _ = sender.send(outcome);
    });
    let outcome = receiver.recv_timeout(Duration::from_secs(5));
    gate.release();
    let _ = reader.join();
    match outcome {
        Ok(Ok(true)) => {}
        other => panic!("WAL read advanced only after the chunk upload: {other:?}"),
    }
    appender.join().unwrap()?;
    Ok(())
}

/// A failed remote append still closes every write path and leaves the mirror readable;
/// the frozen batch is not retried because durability is unknown.
#[test]
fn failed_wal_append_requires_cold_recovery() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    {
        let database = RemoteDatabase::open(backing.clone(), namespace()?, cache.path(), 0)?;
        database
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
        database.checkpoint()?;
    }
    let cold = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(WalChunkGateStore {
        inner: backing,
        gate: Arc::new(Gate::default()),
        fail: true,
    });
    let io = RemoteIO::open(
        store,
        namespace()?,
        cold.path(),
        0,
        8 * 1024 * 1024,
        64 * 1024 * 1024,
    )?;
    let wal = io.open_file(
        &format!("{}-wal", io.database_path()),
        OpenFlags::Create,
        false,
    )?;
    let frame = Arc::new(Buffer::new_temporary(64));
    frame.as_mut_slice().fill(0);
    assert!(wal
        .pwrite(
            0,
            frame,
            Completion::new_write(|result| { assert_eq!(result.unwrap(), 64) })
        )?
        .finished());
    let error = wal
        .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("WAL chunk upload failed"),
        "{error:?}"
    );
    // The failed batch remains accounted for; discarding it would under-report the
    // unconfirmed bytes while cold recovery is still owed.
    assert!(io.resources()?.pending_wal_bytes > 0);
    let buffer = Arc::new(Buffer::new_temporary(16));
    assert!(wal
        .pread(0, Completion::new_read(buffer, |_| None))?
        .finished());
    let frame = Arc::new(Buffer::new_temporary(64));
    let error = wal
        .pwrite(0, frame, Completion::new_write(|_| {}))
        .unwrap_err();
    assert!(
        format!("{error:?}").contains("WAL durability failure requires cold recovery"),
        "{error:?}"
    );
    Ok(())
}

struct WalChunkGateStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<Gate>,
    fail: bool,
}

impl ObjectStore for WalChunkGateStore {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.contains("wal/chunks/") {
            self.gate.pause()?;
            if self.fail {
                anyhow::bail!("injected WAL chunk failure");
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
