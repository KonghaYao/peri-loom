use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Barrier};

use anyhow::{bail, Context, Result};
use peri_loom_v1::cache::SegmentCache;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::io::RemoteIO;
use peri_loom_v1::manifest::{WalPosition, SEGMENT_SIZE};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::storage::RemoteStorage;
use peri_loom_v1::wal::{DurableWal, RemoteWal, WalBatch, WalOperation};
use turso_core::io::{OpenFlags, IO};
use turso_core::Value;

fn namespace() -> Result<Namespace> {
    Namespace::new("verify", "database")
}

fn cache(root: &std::path::Path) -> Result<Arc<SegmentCache>> {
    Ok(Arc::new(SegmentCache::new(root, 0, SEGMENT_SIZE as u64)?))
}

fn write_batch(offset: u64, bytes: &[u8]) -> WalBatch {
    WalBatch {
        operations: vec![WalOperation::Write {
            offset,
            bytes: bytes.to_vec(),
        }],
    }
}

fn replay(wal: &RemoteWal) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    for record in wal.read(&namespace()?, 0)? {
        for operation in record.batch.operations {
            match operation {
                WalOperation::Write {
                    offset,
                    bytes: data,
                } => {
                    let offset = usize::try_from(offset)?;
                    bytes.resize(bytes.len().max(offset + data.len()), 0);
                    bytes[offset..offset + data.len()].copy_from_slice(&data);
                }
                WalOperation::Truncate { size } => bytes.resize(usize::try_from(size)?, 0),
            }
        }
    }
    Ok(bytes)
}

#[test]
fn forged_public_namespace_cannot_touch_remote_state() -> Result<()> {
    for invalid in ["../other", "", "tenant/other", ".", "a%2fb"] {
        let store = Arc::new(MemoryObjectStore::default());
        let root = tempfile::tempdir()?;
        let forged = Namespace {
            tenant: invalid.into(),
            database: "database".into(),
        };
        let wal = RemoteWal::new(store.clone());
        assert!(wal.append(&forged, 0, write_batch(0, b"bad")).is_err());
        assert!(wal.read(&forged, 0).is_err());
        assert!(wal.truncate(&forged, 1).is_err());
        assert!(wal.fence(&forged, 1).is_err());
        assert!(RemoteStorage::open(store.clone(), forged.clone(), cache(root.path())?).is_err());
        assert!(RemoteDatabase::open(store.clone(), forged, root.path(), 0).is_err());
        assert!(store.list("")?.is_empty());
    }
    Ok(())
}

#[test]
fn disappeared_or_obstructed_ssd_cache_is_not_authoritative() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let root = tempfile::tempdir()?;
    let cache_root = root.path().join("segments");
    let storage = RemoteStorage::open(store.clone(), namespace()?, cache(&cache_root)?)?;
    storage.write_at(0, &vec![73; SEGMENT_SIZE])?;
    storage.publish(WalPosition::default(), 0)?;
    let storage = RemoteStorage::open(store, namespace()?, cache(&cache_root)?)?;
    let mut bytes = [0; 8];
    assert_eq!(storage.read_at(0, &mut bytes)?, 8);
    assert_eq!(bytes, [73; 8]);
    std::fs::remove_dir_all(&cache_root)?;
    std::fs::write(&cache_root, b"not a directory")?;
    assert_eq!(storage.read_at(0, &mut bytes)?, 8);
    assert_eq!(bytes, [73; 8]);
    assert_eq!(storage.read_at(0, &mut bytes)?, 8);
    assert_eq!(storage.stats().segment_loads, 3);
    assert_eq!(storage.stats().cache.disk_bytes, 0);
    Ok(())
}

#[test]
fn simultaneous_head_publication_has_exactly_one_authoritative_winner() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let first_root = tempfile::tempdir()?;
    let second_root = tempfile::tempdir()?;
    let first = RemoteStorage::open(store.clone(), namespace()?, cache(first_root.path())?)?;
    let second = RemoteStorage::open(store.clone(), namespace()?, cache(second_root.path())?)?;
    first.write_at(0, b"first")?;
    second.write_at(0, b"other")?;
    let barrier = Arc::new(Barrier::new(2));
    let workers = [first, second].map(|storage| {
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let result = storage.publish(WalPosition::default(), 0);
            (storage, result)
        })
    });
    let outcomes = workers.map(|worker| worker.join().expect("publisher panicked"));
    assert_eq!(
        outcomes.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    for (storage, result) in &outcomes {
        assert_eq!(storage.stats().dirty_segments, usize::from(result.is_err()));
    }
    let fresh = tempfile::tempdir()?;
    let reader = RemoteStorage::open(store, namespace()?, cache(fresh.path())?)?;
    let mut bytes = [0; 5];
    assert_eq!(reader.read_at(0, &mut bytes)?, 5);
    assert!(bytes == *b"first" || bytes == *b"other");
    assert_eq!(reader.manifest().generation, 1);
    Ok(())
}

#[test]
fn repeated_wal_compaction_preserves_sparse_bytes_and_monotonic_lsns() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let wal = RemoteWal::new(store.clone());
    assert_eq!(wal.append(&namespace()?, 0, write_batch(4, b"abc"))?, 1);
    assert_eq!(wal.append(&namespace()?, 0, write_batch(0, b"head"))?, 2);
    wal.truncate(&namespace()?, 3)?;
    assert_eq!(replay(&RemoteWal::new(store.clone()))?, b"headabc");
    assert_eq!(
        wal.append(
            &namespace()?,
            0,
            WalBatch {
                operations: vec![
                    WalOperation::Truncate { size: 5 },
                    WalOperation::Write {
                        offset: 8,
                        bytes: b"z".to_vec()
                    }
                ],
            }
        )?,
        3
    );
    wal.truncate(&namespace()?, 4)?;
    assert_eq!(replay(&RemoteWal::new(store.clone()))?, b"heada\0\0\0z");
    assert!(wal.read(&namespace()?, 1).is_err());
    assert_eq!(wal.append(&namespace()?, 0, write_batch(0, b"new"))?, 4);
    assert_eq!(replay(&RemoteWal::new(store))?, b"newda\0\0\0z");
    Ok(())
}

struct AckFaultStore {
    inner: MemoryObjectStore,
    mode: AtomicU8,
}

impl ObjectStore for AckFaultStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let mode = self.mode.load(Ordering::SeqCst);
        if mode == 3 && key.ends_with("wal/index") {
            bail!("index unavailable before publication");
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if (mode == 1 && key.contains("wal/chunks/")) || (mode == 2 && key.ends_with("wal/index")) {
            self.mode.store(0, Ordering::SeqCst);
            bail!("acknowledgement lost after persistence");
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
fn lost_chunk_and_index_acks_resolve_by_remote_readback_without_duplicate_lsn() -> Result<()> {
    let store = Arc::new(AckFaultStore {
        inner: MemoryObjectStore::default(),
        mode: AtomicU8::new(1),
    });
    let wal = RemoteWal::new(store.clone());
    assert_eq!(wal.append(&namespace()?, 0, write_batch(0, b"a"))?, 1);
    store.mode.store(2, Ordering::SeqCst);
    assert_eq!(wal.append(&namespace()?, 0, write_batch(1, b"b"))?, 2);
    assert_eq!(wal.read(&namespace()?, 0)?.len(), 2);
    assert_eq!(replay(&RemoteWal::new(store))?, b"ab");
    Ok(())
}

#[test]
fn unacknowledged_index_failure_leaves_only_unreachable_orphan_chunk() -> Result<()> {
    let store = Arc::new(AckFaultStore {
        inner: MemoryObjectStore::default(),
        mode: AtomicU8::new(3),
    });
    let wal = RemoteWal::new(store.clone());
    assert!(wal
        .append(&namespace()?, 0, write_batch(0, b"orphan"))
        .is_err());
    assert!(!store.list(&namespace()?.key("wal/chunks")?)?.is_empty());
    assert!(wal.read(&namespace()?, 0)?.is_empty());
    assert_eq!(wal.metadata(&namespace()?)?.last_lsn, 0);
    store.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        wal.append(&namespace()?, 0, write_batch(0, b"committed"))?,
        1
    );
    assert_eq!(replay(&RemoteWal::new(store))?, b"committed");
    Ok(())
}

#[test]
fn cold_sql_recovery_keeps_tenant_namespaces_separate_without_local_db_or_wal() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    for (tenant, number) in [("tenant-a", 11), ("tenant-b", 22)] {
        let root = tempfile::tempdir()?;
        let database = RemoteDatabase::open(
            store.clone(),
            Namespace::new(tenant, "same-db")?,
            root.path(),
            0,
        )?;
        database.execute(&format!(
            "CREATE TABLE items(value INTEGER); INSERT INTO items VALUES({number});"
        ))?;
        drop(database);
        root.close()?;
    }
    for (tenant, number) in [("tenant-a", 11), ("tenant-b", 22)] {
        let root = tempfile::tempdir()?;
        let database = RemoteDatabase::open(
            store.clone(),
            Namespace::new(tenant, "same-db")?,
            root.path(),
            0,
        )?;
        assert_eq!(
            database.query("SELECT value FROM items")?,
            vec![vec![Value::from_i64(number)]]
        );
        let names = std::fs::read_dir(root.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert!(names
            .iter()
            .all(|name| name == ".peri-loom.lock" || name == "segments"));
    }
    Ok(())
}

#[test]
fn corrupted_committed_wal_chunk_rejects_fresh_sql_open() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let root = tempfile::tempdir()?;
    let database = RemoteDatabase::open(store.clone(), namespace()?, root.path(), 0)?;
    database.execute("CREATE TABLE items(value INTEGER); INSERT INTO items VALUES(42)")?;
    drop(database);
    root.close()?;
    let chunks = store.list(&namespace()?.key("wal/chunks")?)?;
    assert!(!chunks.is_empty());
    store.put(&chunks[0], b"corrupted", PutCondition::Any)?;
    let fresh = tempfile::tempdir()?;
    assert!(RemoteDatabase::open(store, namespace()?, fresh.path(), 0).is_err());
    Ok(())
}

#[test]
fn sql_sync_override_cannot_acknowledge_a_non_durable_commit() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let root = tempfile::tempdir()?;
    let database = RemoteDatabase::open(store.clone(), namespace()?, root.path(), 0)?;
    database.execute("CREATE TABLE items(value INTEGER)")?;
    database.checkpoint()?;
    let lsn = database.io().wal_lsn();
    if let Err(error) = database.execute("PRAGMA synchronous=OFF") {
        eprintln!("sync override rejected: {error:#}");
        return Ok(());
    }
    if let Err(error) = database.execute("INSERT INTO items VALUES(42)") {
        eprintln!("non-durable write rejected: {error:#}");
        return Ok(());
    }
    assert!(
        database.io().wal_lsn() > lsn,
        "SQL acknowledged without remote WAL durability confirmation"
    );
    drop(database);
    root.close()?;
    let fresh = tempfile::tempdir()?;
    let database = RemoteDatabase::open(store, namespace()?, fresh.path(), 0)?;
    assert_eq!(
        database.query("SELECT value FROM items")?,
        vec![vec![Value::from_i64(42)]]
    );
    Ok(())
}

#[test]
fn released_io_lock_is_not_retained_by_concurrent_process_creation() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let root = tempfile::tempdir()?;
    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = barrier.clone();
    let process_worker = std::thread::spawn(move || -> Result<()> {
        worker_barrier.wait();
        for _ in 0..64 {
            let result = std::process::Command::new(std::env::current_exe()?)
                .arg("--list")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()?;
            anyhow::ensure!(result.success(), "test-list subprocess failed");
        }
        Ok(())
    });
    let result = (|| -> Result<()> {
        barrier.wait();
        for round in 0..256 {
            let io = RemoteIO::open(store.clone(), namespace()?, root.path(), 0, 0, 0)
                .with_context(|| format!("initial IO open in round {round}"))?;
            assert!(RemoteIO::open(store.clone(), namespace()?, root.path(), 0, 0, 0).is_err());
            let retained_file = io.open_file(&io.database_path(), OpenFlags::Create, false)?;
            drop(io);
            assert!(RemoteIO::open(store.clone(), namespace()?, root.path(), 0, 0, 0).is_err());
            drop(retained_file);
            let reopened = RemoteIO::open(store.clone(), namespace()?, root.path(), 0, 0, 0)
                .with_context(|| format!("IO reopen after last owner drop in round {round}"))?;
            drop(reopened);
        }
        Ok(())
    })();
    process_worker.join().expect("process worker panicked")?;
    result
}
