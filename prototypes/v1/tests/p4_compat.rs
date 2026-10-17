use std::{fs, sync::Arc};

use anyhow::Result;
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    database::RemoteDatabase,
    namespace::Namespace,
    object_store::{LocalObjectStore, ObjectStore, PutCondition},
    ownership::types::{AccessRole, GrantIntent, GrantRequest, LeaseClock, LeaseIntent},
    runtime::DatabaseId,
};

struct Clock;
impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1000)
    }
}

fn fixture() -> Result<(tempfile::TempDir, Arc<dyn ObjectStore>, DatabaseId)> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("store");
    fs::create_dir(&destination)?;
    let source =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/p4/p3-store");
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        fs::copy(entry.path(), destination.join(entry.file_name()))?;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::open(destination)?);
    let identifier = DatabaseId::new(Namespace::new("legacy", "compat")?, "main")?;
    Ok((temporary, store, identifier))
}

#[test]
fn exact_old_root_checksum_is_verified_before_schema_extension() -> Result<()> {
    let (_temporary, store, identifier) = fixture()?;
    let before = store
        .get(&identifier.namespace.key("authority/root")?)?
        .unwrap();
    let envelope: serde_json::Value = serde_json::from_slice(&before.bytes)?;
    assert!(envelope["root"].get("ownership").is_none());
    assert_eq!(envelope["root"]["version"], 1);
    let authority = Authority::open(store.clone(), identifier.namespace.clone())?;
    let snapshot = authority.snapshot()?;
    assert_eq!(snapshot.root.version, 1);
    assert!(snapshot.root.ownership.is_none());
    assert_eq!(snapshot.root.branches["main"].sequence, 3);
    assert_eq!(snapshot.root.branches["main"].current_sequence, 2);
    assert_eq!(
        store
            .get(&identifier.namespace.key("authority/root")?)?
            .unwrap()
            .bytes,
        before.bytes
    );
    Ok(())
}

#[test]
fn migration_retains_restored_state_refs_blobs_and_native_wal() -> Result<()> {
    let (temporary, store, identifier) = fixture()?;
    let base = Authority::open(store.clone(), identifier.namespace.clone())?;
    let before = base.snapshot()?.root;
    let ownership = OwnershipAuthority::new(store, Arc::new(Clock));
    let initial = ownership.state(&identifier)?;
    assert!(initial.binding.is_none());
    let migrated = base.snapshot()?.root;
    assert_eq!(migrated.version, 5);
    assert_eq!(migrated.incarnation, before.incarnation);
    assert_eq!(migrated.branches, before.branches);
    assert_eq!(migrated.snapshots, before.snapshots);
    assert_eq!(migrated.objects, before.objects);
    assert_eq!(migrated.blobs, before.blobs);
    assert_eq!(migrated.uploads, before.uploads);
    assert_eq!(migrated.pins, before.pins);
    assert_eq!(migrated.tombstones, before.tombstones);
    assert_eq!(migrated.receipts.len(), 1);
    assert!(migrated.receipt_index.is_some());
    for (operation, revision) in before.receipts {
        assert_eq!(base.receipt(&operation)?, Some(revision));
    }
    let intent = GrantIntent {
        database: identifier.clone(),
        worker_id: "new-worker".into(),
        epoch: 11,
        expected_epoch: initial.epoch,
        lease_id: "new-lease".into(),
        ttl_ms: 10000,
        role: AccessRole::Writer,
    };
    let nonce = ownership.challenge(LeaseIntent::Grant(intent.clone()))?;
    let grant = ownership.grant(GrantRequest {
        request_id: "compat-grant".into(),
        nonce: nonce.nonce,
        intent,
    })?;
    let owned = ownership.open_session(&grant.binding, AccessRole::Writer)?;
    let database = RemoteDatabase::open(
        owned,
        identifier.namespace.clone(),
        temporary.path().join("cache"),
        11,
    )?;
    assert_eq!(
        database.query("SELECT id, value FROM items")?[0][0].to_string(),
        "1"
    );
    assert_eq!(
        database.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    database.execute("INSERT INTO items VALUES(3, 'after-upgrade')")?;
    database.compact_wal()?;
    assert_eq!(
        database.query("PRAGMA integrity_check")?[0][0].to_string(),
        "ok"
    );
    drop(database);
    assert_eq!(base.snapshot()?.root.snapshots, migrated.snapshots);
    Ok(())
}

#[test]
fn corrupt_legacy_envelope_fails_without_reinitialization() -> Result<()> {
    let (_temporary, store, identifier) = fixture()?;
    let key = identifier.namespace.key("authority/root")?;
    let object = store.get(&key)?.unwrap();
    let mut envelope: serde_json::Value = serde_json::from_slice(&object.bytes)?;
    envelope["root"]["revision"] = serde_json::json!(999999);
    let corrupted = serde_json::to_vec(&envelope)?;
    store.put(&key, &corrupted, PutCondition::Match(object.etag))?;
    assert!(Authority::open(store.clone(), identifier.namespace).is_err());
    assert_eq!(store.get(&key)?.unwrap().bytes, corrupted);
    Ok(())
}

#[test]
fn paged_receipts_keep_owned_root_bounded_and_corruption_fails_closed() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::open(temporary.path())?);
    let namespace = Namespace::new("receipt", "bounded")?;
    let database = DatabaseId::new(namespace.clone(), "main")?;
    let authority = Authority::open(store.clone(), namespace.clone())?;
    let ownership = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    ownership.state(&database)?;
    let root_key = namespace.key("authority/root")?;
    let before = authority.snapshot()?.root;
    let first = authority.transact(None, |_| Ok(()))?.root;
    let first_receipt = first
        .receipts
        .iter()
        .find(|(id, _)| !before.receipts.contains_key(*id))
        .map(|(id, revision)| (id.clone(), *revision))
        .unwrap();
    for _ in 1..256 {
        authority.transact(None, |_| Ok(()))?;
    }
    let snapshot = authority.snapshot()?;
    assert_eq!(snapshot.root.version, 5);
    assert!(snapshot.root.receipts.len() <= 128);
    assert!(!snapshot.root.receipts.contains_key(&first_receipt.0));
    assert_eq!(authority.receipt(&first_receipt.0)?, Some(first_receipt.1));
    assert!(store.get(&root_key)?.unwrap().bytes.len() <= 32 * 1024);
    let page_key = namespace.key(&format!(
        "authority/index-pages/{}",
        snapshot.root.receipt_index.as_deref().unwrap()
    ))?;
    let page = store.get(&page_key)?.unwrap();
    let mut corrupt = page.bytes.clone();
    corrupt[0] ^= 1;
    store.put(&page_key, &corrupt, PutCondition::Match(page.etag))?;
    assert!(authority.snapshot().is_err());
    assert!(Authority::open(store.clone(), namespace).is_err());
    Ok(())
}

#[test]
fn legacy_receipt_migration_shrinks_root_without_losing_history() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::open(temporary.path())?);
    let namespace = Namespace::new("receipt", "migration")?;
    let database = DatabaseId::new(namespace.clone(), "main")?;
    let authority = Authority::open(store.clone(), namespace.clone())?;
    for _ in 0..100 {
        authority.transact(None, |_| Ok(()))?;
    }
    let before = authority.snapshot()?.root;
    let key = namespace.key("authority/root")?;
    let old_size = store.get(&key)?.unwrap().bytes.len();
    OwnershipAuthority::new(store.clone(), Arc::new(Clock)).state(&database)?;
    let after = authority.snapshot()?.root;
    assert_eq!(after.version, 5);
    assert_eq!(after.receipts.len(), 1);
    assert!(store.get(&key)?.unwrap().bytes.len() < old_size / 2);
    for (operation, revision) in before.receipts {
        assert_eq!(authority.receipt(&operation)?, Some(revision));
    }
    Ok(())
}
