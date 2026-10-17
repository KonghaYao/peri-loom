use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use peri_loom_v1::authority::{ownership::OwnershipAuthority, Authority};
use peri_loom_v1::capabilities::Capabilities;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::gc::GarbageCollector;
use peri_loom_v1::manifest::WalPosition;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::ownership::types::{
    AccessRole, GrantIntent, GrantRequest, LeaseClock, LeaseIntent,
};
use peri_loom_v1::recovery::{
    committed_position, validate_index_append, validate_index_commit, validate_index_state,
};
use peri_loom_v1::runtime::DatabaseId;
use peri_loom_v1::wal::{
    DurableWal, RemoteWal, WalAppendRequest, WalBatch, WalCommit, WalMetadata, WalOperation,
    WalRecord,
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
        assert!(
            key.starts_with(&namespace().key("wal/chunks")?)
                || key.starts_with(&namespace().key("wal/index-segments")?)
                || key.starts_with(&namespace().key("wal/receipt-pages")?)
        );
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
fn incremental_validation_reads_only_new_chunk_and_rejects_forged_history() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(directory.path())?);
    let wal = RemoteWal::new(store.clone());
    wal.append(&namespace(), 1, batch(wal_image(false, &[1])))?;
    let original = index(store.as_ref())?;
    // Extending consumes the accepted state, so every rejected candidate below starts from a
    // freshly derived one: that is the fail-closed contract the authority's cache relies on.
    let cold = || {
        validate_index_state(
            &original,
            &namespace(),
            |_| anyhow::bail!("unexpected segment in inline fixture"),
            |key| Ok(store.get(key)?.context("missing WAL chunk")?.bytes),
            |_| anyhow::bail!("unexpected receipt page in inline fixture"),
        )
    };
    let validated = cold()?;
    wal.append(&namespace(), 1, WalBatch::default())?;
    let next = index(store.as_ref())?;
    let mut loaded = Vec::new();
    let incremented = validate_index_append(
        validated,
        &next,
        &namespace(),
        |_| anyhow::bail!("unexpected segment in inline fixture"),
        |key| {
            loaded.push(key.to_owned());
            Ok(store.get(key)?.context("missing WAL chunk")?.bytes)
        },
        |_| anyhow::bail!("unexpected receipt page in inline fixture"),
    )?;
    assert_eq!(loaded.len(), 1);
    assert_eq!(incremented.commit(), validate(store.as_ref(), &next)?);

    for field in ["hash", "lsn"] {
        let mut forged: serde_json::Value = serde_json::from_slice(&next)?;
        forged["entries"][0][field] = if field == "hash" {
            serde_json::json!("wrong-history")
        } else {
            serde_json::json!(99)
        };
        assert!(validate_index_append(
            cold()?,
            &serde_json::to_vec(&forged)?,
            &namespace(),
            |_| anyhow::bail!("unexpected segment in inline fixture"),
            |key| { Ok(store.get(key)?.context("missing WAL chunk")?.bytes) },
            |_| anyhow::bail!("unexpected receipt page in inline fixture"),
        )
        .is_err());
    }
    for field in ["receipts", "receipt_digests"] {
        let mut forged: serde_json::Value = serde_json::from_slice(&next)?;
        let key = forged[field]
            .as_object()
            .unwrap()
            .keys()
            .next()
            .unwrap()
            .clone();
        forged[field].as_object_mut().unwrap().remove(&key);
        assert!(validate_index_append(
            cold()?,
            &serde_json::to_vec(&forged)?,
            &namespace(),
            |_| anyhow::bail!("unexpected segment in inline fixture"),
            |key| { Ok(store.get(key)?.context("missing WAL chunk")?.bytes) },
            |_| anyhow::bail!("unexpected receipt page in inline fixture"),
        )
        .is_err());
    }
    let new_id = wal.read(&namespace(), 2)?[0].append_id.clone();
    let mut forged: serde_json::Value = serde_json::from_slice(&next)?;
    forged["receipt_digests"][&new_id] = serde_json::json!("wrong-digest");
    assert!(validate_index_append(
        cold()?,
        &serde_json::to_vec(&forged)?,
        &namespace(),
        |_| anyhow::bail!("unexpected segment in inline fixture"),
        |key| { Ok(store.get(key)?.context("missing WAL chunk")?.bytes) },
        |_| anyhow::bail!("unexpected receipt page in inline fixture"),
    )
    .is_err());
    let mut forged: serde_json::Value = serde_json::from_slice(&next)?;
    forged["commit"] =
        serde_json::json!({"lsn": 2, "position": {"salts": [17, 23], "frame_count": 1}});
    assert!(validate_index_append(
        cold()?,
        &serde_json::to_vec(&forged)?,
        &namespace(),
        |_| anyhow::bail!("unexpected segment in inline fixture"),
        |key| { Ok(store.get(key)?.context("missing WAL chunk")?.bytes) },
        |_| anyhow::bail!("unexpected receipt page in inline fixture"),
    )
    .is_err());

    assert!(validate_index_state(
        &next,
        &namespace(),
        |_| anyhow::bail!("unexpected segment in inline fixture"),
        |key| {
            let mut bytes = store.get(key)?.context("missing WAL chunk")?.bytes;
            if key == loaded[0] {
                bytes[0] ^= 1;
            }
            Ok(bytes)
        },
        |_| anyhow::bail!("unexpected receipt page in inline fixture"),
    )
    .is_err());
    assert!(validate_index_append(
        cold()?,
        &next,
        &namespace(),
        |_| anyhow::bail!("unexpected segment in inline fixture"),
        |key| {
            let mut bytes = store.get(key)?.context("missing WAL chunk")?.bytes;
            bytes[0] ^= 1;
            Ok(bytes)
        },
        |_| anyhow::bail!("unexpected receipt page in inline fixture"),
    )
    .is_err());
    Ok(())
}

struct FrozenClock;

impl LeaseClock for FrozenClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(0)
    }
}

#[derive(Default)]
struct TrackedAuthorityBackend {
    inner: peri_loom_v1::object_store::MemoryObjectStore,
    watched: Mutex<Option<String>>,
    reads: AtomicUsize,
    fail_changed_wal: Mutex<Option<String>>,
}

impl ObjectStore for TrackedAuthorityBackend {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if self.watched.lock().unwrap().as_deref() == Some(key) {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.ends_with("authority/root") {
            let mut old_hash = self.fail_changed_wal.lock().unwrap();
            if let Some(previous) = old_hash.as_ref() {
                let value: serde_json::Value = serde_json::from_slice(bytes)?;
                if value["root"]["branches"]["main"]["wal"]["hash"]
                    .as_str()
                    .is_some_and(|hash| hash != previous)
                {
                    old_hash.take();
                    return Err(peri_loom_v1::object_store::PreconditionFailed.into());
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
fn authority_append_replays_on_cas_conflict_and_cold_session() -> Result<()> {
    let namespace = Namespace::new("recovery", "authority-append")?;
    let database = DatabaseId::new(namespace.clone(), "main")?;
    let backend = Arc::new(TrackedAuthorityBackend::default());
    let authority = OwnershipAuthority::new(backend.clone(), Arc::new(FrozenClock));
    let intent = GrantIntent {
        database,
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 10000,
        role: AccessRole::Writer,
    };
    let nonce = authority
        .challenge(LeaseIntent::Grant(intent.clone()))?
        .nonce;
    let grant = authority.grant(GrantRequest {
        request_id: "grant".into(),
        nonce,
        intent,
    })?;
    let session = authority.open_session(&grant.binding, AccessRole::Writer)?;
    let root = Authority::open(backend.clone(), namespace.clone())?;
    let pin_len = |root: &Authority| -> Result<usize> { root.pinned_entry_count() };
    let initial_pin = pin_len(&root)?;
    let request = |id: &str| WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        append_id: id.into(),
        batch: WalBatch::default(),
        position: None,
        timestamp_millis: 1,
    };
    assert_eq!(session.append_wal(&request("first"))?, Some(1));
    assert_eq!(pin_len(&root)?, initial_pin + 2);
    let snapshot = root.snapshot()?;
    let first_chunk = snapshot
        .root
        .objects
        .iter()
        .find(|(key, _)| key.starts_with("wal/chunks/"))
        .context("first WAL chunk missing")?
        .1;
    let first_key = namespace.key(&format!("authority/blobs/{}", first_chunk.blob))?;
    *backend.watched.lock().unwrap() = Some(first_key.clone());

    assert_eq!(session.append_wal(&request("second"))?, Some(2));
    assert_eq!(pin_len(&root)?, initial_pin + 4);
    assert_eq!(backend.reads.load(Ordering::SeqCst), 0);

    let previous = root.snapshot()?.root.branches["main"].wal.clone().unwrap();
    *backend.fail_changed_wal.lock().unwrap() = Some(previous.hash);
    assert_eq!(session.append_wal(&request("third"))?, Some(3));
    assert_eq!(pin_len(&root)?, initial_pin + 6);
    assert!(backend.fail_changed_wal.lock().unwrap().is_none());
    assert!(backend.reads.load(Ordering::SeqCst) > 0);

    backend.reads.store(0, Ordering::SeqCst);
    let cold = authority.open_session(&grant.binding, AccessRole::Writer)?;
    assert_eq!(cold.append_wal(&request("fourth"))?, Some(4));
    assert!(backend.reads.load(Ordering::SeqCst) > 0);

    backend
        .inner
        .put(&first_key, b"corrupted old chunk", PutCondition::Any)?;
    let fresh = authority.open_session(&grant.binding, AccessRole::Writer)?;
    let before = root.snapshot()?.root.branches["main"].wal.clone();
    assert!(fresh.append_wal(&request("fifth")).is_err());
    assert_eq!(root.snapshot()?.root.branches["main"].wal, before);
    Ok(())
}

#[test]
fn branch_session_warm_cache_accepts_legacy_receipts_and_rolls_them() -> Result<()> {
    let namespace = Namespace::new("recovery", "branch-legacy")?;
    let database = DatabaseId::new(namespace.clone(), "main")?;
    let backend = Arc::new(peri_loom_v1::object_store::MemoryObjectStore::default());
    let authority = OwnershipAuthority::new(backend, Arc::new(FrozenClock));
    let intent = GrantIntent {
        database,
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 10000,
        role: AccessRole::Writer,
    };
    let nonce = authority
        .challenge(LeaseIntent::Grant(intent.clone()))?
        .nonce;
    let grant = authority.grant(GrantRequest {
        request_id: "grant".into(),
        nonce,
        intent,
    })?;
    let session = authority.open_session(&grant.binding, AccessRole::Writer)?;
    let request = |identity: &str| WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        append_id: identity.into(),
        batch: WalBatch::default(),
        position: None,
        timestamp_millis: 1,
    };
    for ordinal in 1..=256 {
        assert_eq!(
            session.append_wal(&request(&format!("legacy-{ordinal}")))?,
            Some(ordinal)
        );
    }
    let key = namespace.key("wal/index")?;
    let current = session.get(&key)?.context("missing WAL index")?;
    let mut index: serde_json::Value = serde_json::from_slice(&current.bytes)?;
    index["receipt_digests"] = serde_json::json!({});
    session.put(
        &key,
        &serde_json::to_vec(&index)?,
        PutCondition::Match(current.etag),
    )?;

    assert_eq!(session.append_wal(&request("ordinary-1"))?, Some(257));
    assert_eq!(session.append_wal(&request("ordinary-2"))?, Some(258));
    assert_eq!(session.append_wal(&request("ordinary-2"))?, Some(258));

    let current = session.get(&key)?.context("missing WAL index")?;
    let index: serde_json::Value = serde_json::from_slice(&current.bytes)?;
    assert_eq!(index["last_lsn"], 258);
    assert!(index["receipt_index"].is_string());
    assert!(index["receipts"]["ordinary-1"].is_null());
    assert_eq!(index["receipts"]["ordinary-2"], 258);

    let loads = session.clone();
    let namespace_for_load = namespace.clone();
    peri_loom_v1::recovery::validate_index_state(
        &current.bytes,
        &namespace,
        |hash| {
            Ok(loads
                .get(&namespace_for_load.key(&format!("wal/index-segments/{hash}"))?)?
                .context("missing WAL index segment")?
                .bytes)
        },
        |key| Ok(loads.get(key)?.context("missing WAL chunk")?.bytes),
        |hash| {
            Ok(loads
                .get(&namespace_for_load.key(&format!("wal/receipt-pages/{hash}"))?)?
                .context("missing WAL receipt page")?
                .bytes)
        },
    )?;
    Ok(())
}

#[test]
fn pinned_recovery_dependencies_survive_gc_without_pinning_unrelated_objects() -> Result<()> {
    let backend = Arc::new(peri_loom_v1::object_store::MemoryObjectStore::default());
    let authority = Authority::open(backend, namespace())?;
    let capabilities = Capabilities::new(authority.clone());
    let writer = authority.branch_store("main")?;
    let wal = RemoteWal::new(writer.clone());
    wal.append(&namespace(), 1, WalBatch::default())?;
    let reader = authority.branch_store("main")?;
    let old_index = reader
        .get(&namespace().key("wal/index")?)?
        .context("old index missing")?
        .bytes;
    let first_chunk = serde_json::from_slice::<serde_json::Value>(&old_index)?["entries"][0]
        ["hash"]
        .as_str()
        .context("first chunk hash missing")?
        .to_owned();
    let first_key = namespace().key(&format!("wal/chunks/{first_chunk}"))?;
    let first_bytes = reader
        .get(&first_key)?
        .context("first chunk missing")?
        .bytes;
    let first_blob = authority.snapshot()?.root.objects[&format!("wal/chunks/{first_chunk}")]
        .blob
        .clone();

    let segment = vec![7; peri_loom_v1::manifest::SEGMENT_SIZE];
    let unrelated = format!("segments/{}", blake3::hash(&segment).to_hex());
    writer.put(
        &namespace().key(&unrelated)?,
        &segment,
        PutCondition::Absent,
    )?;
    let snapshot = authority.snapshot()?;
    let unrelated_blob = &snapshot.root.objects[&unrelated].blob;
    let pinned = reader.pinned_entries()?;
    assert!(
        pinned.len() == 2
            && pinned.iter().any(|entry| entry.blob == first_blob)
            && pinned.iter().all(|entry| &entry.blob != unrelated_blob)
    );

    capabilities.snapshot("main", "early")?;
    wal.append(&namespace(), 1, WalBatch::default())?;
    capabilities.branch("main", "restored")?;
    capabilities.restore("restored", "early")?;
    capabilities.delete_branch("restored")?;
    capabilities.delete_snapshot("early")?;
    writer.delete(&namespace().key("wal/index")?, None)?;
    drop(wal);
    drop(writer);

    let gc = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert!(gc.mark()? > 0);
    let root = authority.snapshot()?;
    assert!(root.root.blobs.contains_key(&first_blob));
    assert!(!root.root.objects.contains_key(&unrelated));
    assert_eq!(
        reader
            .get(&first_key)?
            .context("pinned chunk missing")?
            .bytes,
        first_bytes
    );
    assert_eq!(
        reader
            .get(&namespace().key("wal/index")?)?
            .context("pinned index missing")?
            .bytes,
        old_index
    );
    drop(reader);
    assert!(gc.mark()? > 0);
    assert!(!authority
        .snapshot()?
        .root
        .objects
        .contains_key(&format!("wal/chunks/{first_chunk}")));
    Ok(())
}

#[test]
fn externally_discovered_manifest_and_listing_pin_dependencies_before_gc() -> Result<()> {
    let authority = Authority::open(
        Arc::new(peri_loom_v1::object_store::MemoryObjectStore::default()),
        namespace(),
    )?;
    let reader = authority.branch_store("main")?;
    let writer = authority.branch_store("main")?;
    let segment = vec![9; peri_loom_v1::manifest::SEGMENT_SIZE];
    let hash = blake3::hash(&segment).to_hex().to_string();
    let segment_key = namespace().key(&format!("segments/{hash}"))?;
    writer.put(&segment_key, &segment, PutCondition::Absent)?;
    let manifest = peri_loom_v1::manifest::Manifest {
        logical_db_size: segment.len() as u64,
        segments: std::collections::BTreeMap::from([(0, hash)]),
        ..Default::default()
    };
    let bytes = serde_json::to_vec(&manifest)?;
    let manifest_key = namespace().key(&format!("manifests/{}", blake3::hash(&bytes).to_hex()))?;
    writer.put(&manifest_key, &bytes, PutCondition::Absent)?;
    assert_eq!(
        reader
            .get(&manifest_key)?
            .context("external manifest missing")?
            .bytes,
        bytes
    );

    let listed_segment = vec![3; peri_loom_v1::manifest::SEGMENT_SIZE];
    let listed_key = namespace().key(&format!(
        "segments/{}",
        blake3::hash(&listed_segment).to_hex()
    ))?;
    writer.put(&listed_key, &listed_segment, PutCondition::Absent)?;
    assert!(reader
        .list(&format!("{}segments/", namespace().prefix()))?
        .contains(&listed_key));
    drop(writer);

    let gc = GarbageCollector::new(authority.clone(), Duration::ZERO);
    gc.mark()?;
    assert_eq!(
        reader
            .get(&segment_key)?
            .context("manifest dependency collected")?
            .bytes,
        segment
    );
    assert_eq!(
        reader
            .get(&listed_key)?
            .context("listed segment collected")?
            .bytes,
        listed_segment
    );
    drop(reader);
    assert!(gc.mark()? > 0);
    assert!(!authority
        .snapshot()?
        .root
        .objects
        .contains_key(&format!("segments/{}", blake3::hash(&segment).to_hex())));
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
    assert_eq!(
        database.query("PRAGMA cache_size")?,
        vec![vec![Value::from_i64(256)]]
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
