use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use peri_loom_v1::authority::Authority;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::manifest::WalPosition;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::recovery::{committed_position, validate_index_commit};
use peri_loom_v1::wal::{
    DurableWal, RemoteWal, WalBatch, WalCommit, WalMetadata, WalOperation, WalRecord,
};
use turso_core::storage::sqlite3_ondisk::{checksum_wal, WalHeader};
use turso_core::Value;

fn namespace() -> Namespace {
    Namespace::new("recovery", "database").unwrap()
}

fn index(store: &dyn ObjectStore) -> Result<Vec<u8>> {
    Ok(store
        .get(&namespace().key("wal/index")?)?
        .context("missing WAL index")?
        .bytes)
}

fn validate(store: &dyn ObjectStore, bytes: &[u8]) -> Result<Option<WalCommit>> {
    validate_index_commit(bytes, &namespace(), |key| {
        assert!(key.starts_with(&namespace().key("wal/chunks")?));
        Ok(store.get(key)?.context("missing committed chunk")?.bytes)
    })
}

fn batch(bytes: Vec<u8>) -> WalBatch {
    WalBatch {
        operations: vec![
            WalOperation::Truncate { size: 0 },
            WalOperation::Write { offset: 0, bytes },
        ],
    }
}

fn put_word(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

fn wal_image(big_endian: bool, db_sizes: &[u32]) -> Vec<u8> {
    let mut header = WalHeader::new();
    header.magic = if big_endian { 0x377f0683 } else { 0x377f0682 };
    header.file_format = 3007000;
    header.page_size = 512;
    header.salt_1 = 17;
    header.salt_2 = 23;
    let native = cfg!(target_endian = "big") == big_endian;
    let mut bytes = vec![0; 32];
    for (offset, value) in [
        (0, header.magic),
        (4, 3007000),
        (8, 512),
        (16, 17),
        (20, 23),
    ] {
        put_word(&mut bytes, offset, value);
    }
    let mut rolling = checksum_wal(&bytes[..24], &header, (0, 0), native);
    put_word(&mut bytes, 24, rolling.0);
    put_word(&mut bytes, 28, rolling.1);
    for db_size in db_sizes {
        let mut frame = vec![0x41; 536];
        put_word(&mut frame, 0, 1);
        put_word(&mut frame, 4, *db_size);
        put_word(&mut frame, 8, 17);
        put_word(&mut frame, 12, 23);
        rolling = checksum_wal(&frame[..8], &header, rolling, native);
        rolling = checksum_wal(&frame[24..], &header, rolling, native);
        put_word(&mut frame, 16, rolling.0);
        put_word(&mut frame, 20, rolling.1);
        bytes.extend(frame);
    }
    bytes
}

#[test]
fn native_sql_multiple_commits_and_rollback_mark_actual_durable_frames() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let backing = Arc::new(LocalObjectStore::open(objects.path())?);
    let authority = Authority::open(backing, namespace())?;
    let store = authority.branch_store("main")?;
    let database = RemoteDatabase::open(store.clone(), namespace(), cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    let first = validate(store.as_ref(), &index(store.as_ref())?)?
        .context("CREATE commit marker missing")?;
    database.execute("BEGIN; INSERT INTO items VALUES(10); ROLLBACK;")?;
    assert_eq!(
        validate(store.as_ref(), &index(store.as_ref())?)?,
        Some(first)
    );
    database.execute("BEGIN; INSERT INTO items VALUES(1); INSERT INTO items VALUES(2); COMMIT;")?;
    let second = validate(store.as_ref(), &index(store.as_ref())?)?
        .context("INSERT commit marker missing")?;
    assert!(second.lsn > first.lsn);
    assert_eq!(second.position.salts, first.position.salts);
    assert!(second.position.frame_count > first.position.frame_count);
    database.execute("INSERT INTO items VALUES(3)")?;
    let third =
        validate(store.as_ref(), &index(store.as_ref())?)?.context("third marker missing")?;
    assert!(third.lsn > second.lsn && third.position.frame_count > second.position.frame_count);
    assert_eq!(third.lsn, database.io().wal_lsn());
    let records = RemoteWal::new(store.clone()).read(&namespace(), 0)?;
    assert!(
        records
            .iter()
            .filter(|record| record.commit.is_some())
            .count()
            >= 3
    );
    assert_eq!(records.last().unwrap().commit, Some(third));
    let root = authority.snapshot()?;
    let history = &root.root.branches["main"].history;
    assert_eq!(history.len(), 3);
    assert!(history
        .windows(2)
        .all(|pair| pair[0].sequence < pair[1].sequence
            && pair[0].timestamp_ms <= pair[1].timestamp_ms));
    assert_eq!(
        validate(
            store.as_ref(),
            &authority.read_blob(history.last().unwrap().wal.as_ref().unwrap())?
        )?,
        Some(third)
    );
    drop(database);
    cache.close()?;
    let fresh = tempfile::tempdir()?;
    let recovered = RemoteDatabase::open(
        authority.branch_store("main")?,
        namespace(),
        fresh.path(),
        0,
    )?;
    assert_eq!(
        recovered.query("SELECT count(*) FROM items")?,
        vec![vec![Value::from_i64(3)]]
    );
    Ok(())
}

#[test]
fn native_checkpoint_reset_and_compaction_preserve_old_index_cold_replay() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = RemoteDatabase::open(store.clone(), namespace(), cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    let old_index = index(store.as_ref())?;
    let old_head = store
        .get(&namespace().key("HEAD")?)?
        .context("missing HEAD")?
        .bytes;
    let old_commit = validate(store.as_ref(), &old_index)?.context("old marker missing")?;
    database.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert!(validate(store.as_ref(), &index(store.as_ref())?)?.is_none());
    database.execute("INSERT INTO items VALUES(2)")?;
    let reset_commit =
        validate(store.as_ref(), &index(store.as_ref())?)?.context("reset marker missing")?;
    assert!(reset_commit.lsn > old_commit.lsn);
    assert_ne!(reset_commit.position.salts, old_commit.position.salts);
    let wal = RemoteWal::new(store.clone());
    wal.truncate(&namespace(), reset_commit.lsn + 1)?;
    assert_eq!(
        validate(store.as_ref(), &index(store.as_ref())?)?,
        Some(reset_commit)
    );
    assert_eq!(validate(store.as_ref(), &old_index)?, Some(old_commit));
    let archived = tempfile::tempdir()?;
    let snapshot = Arc::new(LocalObjectStore::open(archived.path())?);
    for key in store.list(&namespace().prefix())? {
        let bytes = if key == namespace().key("wal/index")? {
            old_index.clone()
        } else if key == namespace().key("HEAD")? {
            old_head.clone()
        } else {
            store.get(&key)?.context("missing old dependency")?.bytes
        };
        snapshot.put(&key, &bytes, PutCondition::Absent)?;
    }
    let old_cache = tempfile::tempdir()?;
    let old_database = RemoteDatabase::open(snapshot, namespace(), old_cache.path(), 0)?;
    assert_eq!(
        old_database.query("SELECT id FROM items ORDER BY id")?,
        vec![vec![Value::from_i64(1)]]
    );
    drop(database);
    cache.close()?;
    let new_cache = tempfile::tempdir()?;
    let current = RemoteDatabase::open(store, namespace(), new_cache.path(), 0)?;
    assert_eq!(
        current.query("SELECT count(*) FROM items")?,
        vec![vec![Value::from_i64(2)]]
    );
    Ok(())
}

#[test]
fn native_checksum_both_orders_and_last_frame_commit_boundary_are_required() -> Result<()> {
    for big_endian in [false, true] {
        assert_eq!(
            committed_position(&wal_image(big_endian, &[0, 2]))?,
            Some(WalPosition {
                salts: [17, 23],
                frame_count: 2
            })
        );
        assert_eq!(committed_position(&wal_image(big_endian, &[1, 0]))?, None);
        assert_eq!(committed_position(&wal_image(big_endian, &[]))?, None);
        let mut truncated = wal_image(big_endian, &[1]);
        truncated.pop();
        assert_eq!(committed_position(&truncated)?, None);
        let mut corrupt = wal_image(big_endian, &[1]);
        corrupt[100] ^= 1;
        assert_eq!(committed_position(&corrupt)?, None);
        let mut salt_mismatch = wal_image(big_endian, &[1]);
        salt_mismatch[40] ^= 1;
        assert_eq!(committed_position(&salt_mismatch)?, None);
        for offset in [32 + 536, 32 + 536 + 8, 32 + 536 + 24] {
            let mut invalid_tail = wal_image(big_endian, &[1, 2]);
            if offset == 32 + 536 {
                put_word(&mut invalid_tail, offset, 0);
            } else {
                invalid_tail[offset] ^= 1;
            }
            assert_eq!(committed_position(&invalid_tail)?, None);
        }
    }
    for (offset, value) in [(0, 0), (4, 1), (8, 513), (8, 0), (8, 131072)] {
        let mut invalid = wal_image(false, &[1]);
        put_word(&mut invalid, offset, value);
        assert!(committed_position(&invalid).is_err());
    }
    let mut header_corrupt = wal_image(false, &[1]);
    header_corrupt[24] ^= 1;
    assert!(committed_position(&header_corrupt).is_err());
    assert_eq!(committed_position(&[0; 31])?, None);
    Ok(())
}

#[test]
fn append_none_clears_index_marker_and_legacy_json_defaults_to_none() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let wal = RemoteWal::new(store.clone());
    let bytes = wal_image(false, &[1]);
    let position = committed_position(&bytes)?.unwrap();
    assert_eq!(
        wal.append_commit(&namespace(), 0, batch(bytes), Some(position))?,
        1
    );
    assert_eq!(
        validate(store.as_ref(), &index(store.as_ref())?)?,
        Some(WalCommit { lsn: 1, position })
    );
    assert_eq!(wal.append(&namespace(), 0, WalBatch::default())?, 2);
    assert_eq!(wal.metadata(&namespace())?.commit, None);
    assert_eq!(validate(store.as_ref(), &index(store.as_ref())?)?, None);
    assert!(wal.read(&namespace(), 0)?[0].commit.is_some());
    let mut legacy_record = serde_json::to_value(wal.read(&namespace(), 0)?[1].clone())?;
    legacy_record.as_object_mut().unwrap().remove("commit");
    assert!(serde_json::from_value::<WalRecord>(legacy_record)?
        .commit
        .is_none());
    let mut legacy_index = serde_json::to_value(wal.metadata(&namespace())?)?;
    legacy_index.as_object_mut().unwrap().remove("commit");
    assert!(serde_json::from_value::<WalMetadata>(legacy_index)?
        .commit
        .is_none());
    Ok(())
}

#[test]
fn validator_rejects_forged_lsn_namespace_hash_and_replay_position() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let wal = RemoteWal::new(store.clone());
    let bytes = wal_image(false, &[1]);
    let position = committed_position(&bytes)?.unwrap();
    wal.append_commit(&namespace(), 0, batch(bytes), Some(position))?;
    let original = index(store.as_ref())?;
    for value in [0, 2] {
        let mut altered: serde_json::Value = serde_json::from_slice(&original)?;
        altered["commit"]["lsn"] = value.into();
        assert!(validate(store.as_ref(), &serde_json::to_vec(&altered)?).is_err());
    }
    let mut altered: serde_json::Value = serde_json::from_slice(&original)?;
    altered["commit"]["position"]["frame_count"] = 7.into();
    assert!(validate(store.as_ref(), &serde_json::to_vec(&altered)?).is_err());
    assert!(
        validate_index_commit(&original, &Namespace::new("other", "database")?, |key| Ok(
            store.get(key)?.context("no foreign chunk")?.bytes
        ))
        .is_err()
    );
    assert!(validate_index_commit(&original, &namespace(), |key| {
        let mut bytes = store.get(key)?.unwrap().bytes;
        bytes[0] ^= 1;
        Ok(bytes)
    })
    .is_err());
    let bad_position = WalPosition {
        salts: [99, 23],
        frame_count: 1,
    };
    wal.append_commit(
        &namespace(),
        0,
        batch(wal_image(false, &[1])),
        Some(bad_position),
    )?;
    assert!(validate(store.as_ref(), &index(store.as_ref())?).is_err());
    Ok(())
}

#[test]
fn uncommitted_and_partial_frames_cannot_be_promoted_to_index_commits() -> Result<()> {
    for bytes in [
        wal_image(false, &[0]),
        wal_image(false, &[1, 0]),
        {
            let mut bytes = wal_image(false, &[1]);
            bytes.push(0);
            bytes
        },
        {
            let mut bytes = wal_image(false, &[1, 2]);
            bytes[32 + 536 + 16] ^= 1;
            bytes
        },
    ] {
        let objects = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(objects.path())?);
        let wal = RemoteWal::new(store.clone());
        wal.append_commit(
            &namespace(),
            0,
            batch(bytes),
            Some(WalPosition {
                salts: [17, 23],
                frame_count: 1,
            }),
        )?;
        assert!(validate(store.as_ref(), &index(store.as_ref())?).is_err());
    }
    Ok(())
}

#[test]
fn commit_requires_frame_write_proof_and_accepts_split_frame_writes() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let wal = RemoteWal::new(store.clone());
    let bytes = wal_image(false, &[1]);
    let position = committed_position(&bytes)?.unwrap();
    wal.append(&namespace(), 0, batch(bytes[..32].to_vec()))?;
    wal.append_commit(
        &namespace(),
        0,
        WalBatch {
            operations: vec![
                WalOperation::Write {
                    offset: 32,
                    bytes: bytes[32..200].to_vec(),
                },
                WalOperation::Write {
                    offset: 200,
                    bytes: bytes[200..].to_vec(),
                },
            ],
        },
        Some(position),
    )?;
    assert_eq!(
        validate(store.as_ref(), &index(store.as_ref())?)?,
        Some(WalCommit { lsn: 2, position })
    );
    wal.append_commit(&namespace(), 0, WalBatch::default(), Some(position))?;
    assert!(validate(store.as_ref(), &index(store.as_ref())?).is_err());
    Ok(())
}

struct IndexFault {
    inner: LocalObjectStore,
    mode: AtomicU8,
}

impl ObjectStore for IndexFault {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.ends_with("wal/index") && self.mode.load(Ordering::SeqCst) == 1 {
            anyhow::bail!("injected index CAS failure");
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if key.ends_with("wal/index") && self.mode.swap(0, Ordering::SeqCst) == 2 {
            anyhow::bail!("lost commit index ACK");
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
fn native_commit_marker_is_atomic_with_index_and_failed_commit_is_not_recoverable() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(IndexFault {
        inner: LocalObjectStore::open(objects.path())?,
        mode: AtomicU8::new(0),
    });
    let database = RemoteDatabase::open(store.clone(), namespace(), cache.path(), 0)?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    let previous = index(store.as_ref())?;
    database.execute("BEGIN; INSERT INTO items VALUES(1)")?;
    store.mode.store(1, Ordering::SeqCst);
    assert!(database.execute("COMMIT").is_err());
    assert_eq!(index(store.as_ref())?, previous);
    drop(database);
    cache.close()?;
    store.mode.store(0, Ordering::SeqCst);
    let fresh = tempfile::tempdir()?;
    let recovered = RemoteDatabase::open(store, namespace(), fresh.path(), 0)?;
    assert_eq!(
        recovered.query("SELECT count(*) FROM items")?,
        vec![vec![Value::from_i64(0)]]
    );
    Ok(())
}

#[test]
fn lost_commit_ack_returns_one_marker_and_one_lsn() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let store = Arc::new(IndexFault {
        inner: LocalObjectStore::open(objects.path())?,
        mode: AtomicU8::new(2),
    });
    let wal = RemoteWal::new(store.clone());
    let bytes = wal_image(false, &[1]);
    let position = committed_position(&bytes)?.unwrap();
    assert_eq!(
        wal.append_commit(&namespace(), 0, batch(bytes), Some(position))?,
        1
    );
    assert_eq!(wal.read(&namespace(), 0)?.len(), 1);
    assert_eq!(
        validate(store.as_ref(), &index(store.as_ref())?)?,
        Some(WalCommit { lsn: 1, position })
    );
    Ok(())
}

#[test]
fn native_65536_page_wal_header_uses_full_size_not_sqlite_db_header_encoding() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = RemoteDatabase::open(store.clone(), namespace(), cache.path(), 0)?;
    database.execute("PRAGMA page_size=65536; CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    assert_eq!(
        database.query("PRAGMA page_size")?,
        vec![vec![Value::from_i64(65536)]]
    );
    let mut header = None;
    for record in RemoteWal::new(store.clone()).read(&namespace(), 0)? {
        for operation in record.batch.operations {
            if let WalOperation::Write { offset: 0, bytes } = operation {
                if bytes.len() >= 32 {
                    header = Some(u32::from_be_bytes(bytes[8..12].try_into().unwrap()));
                }
            }
        }
    }
    assert_eq!(header, Some(65536));
    assert!(validate(store.as_ref(), &index(store.as_ref())?)?.is_some());
    Ok(())
}
