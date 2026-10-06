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
    assert_eq!(migrated.version, 2);
    assert_eq!(migrated.incarnation, before.incarnation);
    assert_eq!(migrated.branches, before.branches);
    assert_eq!(migrated.snapshots, before.snapshots);
    assert_eq!(migrated.objects, before.objects);
    assert_eq!(migrated.blobs, before.blobs);
    assert_eq!(migrated.uploads, before.uploads);
    assert_eq!(migrated.pins, before.pins);
    assert_eq!(migrated.tombstones, before.tombstones);
    for (operation, revision) in before.receipts {
        assert_eq!(migrated.receipts[&operation], revision);
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
