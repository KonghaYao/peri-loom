use super::{grant, identifier, support::Clock};
use anyhow::{Context, Result};
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    authority_rpc::protocol::MaintenanceOperation,
    namespace::Namespace,
    object_store::{LocalObjectStore, MemoryObjectStore, ObjectStore, PutCondition},
    ownership::types::{AccessRole, GrantState},
    runtime::DatabaseId,
};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{fs, sync::Arc};

#[test]
fn restore_delete_recreate_revoke_same_cas_and_keep_the_epoch_tombstone() -> Result<()> {
    let store = Arc::new(MemoryObjectStore::default());
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    let id = identifier("refs");
    let first = grant(&authority, id.clone(), 10, 1000)?;
    let scoped = authority.open_session(&first.binding, AccessRole::Writer)?;
    authority.maintenance(
        &id,
        "snapshot-before",
        MaintenanceOperation::Snapshot {
            name: "before".into(),
        },
    )?;
    let base = Authority::open(store, id.namespace.clone())?;
    let before = base.snapshot()?.root;
    authority.maintenance(
        &id,
        "restore-once",
        MaintenanceOperation::Restore {
            snapshot: "before".into(),
        },
    )?;
    let restored = base.snapshot()?.root;
    assert_eq!(restored.revision, before.revision + 1);
    assert!(restored.branches["main"].incarnation > before.branches["main"].incarnation);
    let owner = &restored.ownership.as_ref().unwrap().branches["main"];
    assert_eq!(owner.epoch, 10);
    assert_eq!(owner.state, GrantState::Revoked);
    assert!(scoped.validate_session().is_err());
    assert!(authority
        .validate_lease(&first.binding, AccessRole::Writer)
        .is_err());
    let revision = restored.revision;
    authority.maintenance(
        &id,
        "restore-once",
        MaintenanceOperation::Restore {
            snapshot: "before".into(),
        },
    )?;
    assert_eq!(
        base.snapshot()?.root.revision,
        revision,
        "same request replay changed incarnation again"
    );
    assert!(authority
        .maintenance(
            &id,
            "restore-once",
            MaintenanceOperation::Snapshot {
                name: "collision".into()
            }
        )
        .is_err());
    authority.maintenance(
        &id,
        "keep-branch",
        MaintenanceOperation::Branch {
            source: "main".into(),
            name: "survivor".into(),
        },
    )?;
    let before_delete = base.snapshot()?.root;
    authority.maintenance(&id, "delete-main", MaintenanceOperation::DropBranch)?;
    let deleted = base.snapshot()?.root;
    assert_eq!(deleted.revision, before_delete.revision + 1);
    assert!(!deleted.branches.contains_key("main"));
    assert_eq!(
        deleted.ownership.as_ref().unwrap().branches["main"].epoch,
        10
    );
    assert_eq!(
        deleted.ownership.as_ref().unwrap().branches["main"].state,
        GrantState::Revoked
    );
    let mut survivor = id.clone();
    survivor.branch = "survivor".into();
    authority.maintenance(
        &survivor,
        "recreate-main",
        MaintenanceOperation::Branch {
            source: "survivor".into(),
            name: "main".into(),
        },
    )?;
    assert_eq!(authority.state(&id)?.epoch, 10);
    assert!(super::grant_request(&authority, id.clone(), 10, 10, 1000, "epoch-reuse").is_err());
    let fresh = grant(&authority, id, 11, 1000)?;
    assert_ne!(
        fresh.binding.branch_incarnation,
        first.binding.branch_incarnation
    );
    assert!(authority
        .open_session(&first.binding, AccessRole::Writer)
        .is_err());
    Ok(())
}

#[derive(Deserialize)]
struct Envelope<'input> {
    #[serde(borrow)]
    root: &'input RawValue,
    checksum: String,
}

#[test]
fn frozen_legacy_checksum_uses_embedded_bytes_and_boot_preserves_physical_objects() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let directory = temporary.path().join("objects");
    fs::create_dir(&directory)?;
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/p4/p3-store");
    let mut count = 0;
    for entry in fs::read_dir(fixture)? {
        let entry = entry?;
        fs::copy(entry.path(), directory.join(entry.file_name()))?;
        count += 1;
    }
    assert_eq!(count, 14, "actual old fixture was replaced or changed size");
    let store = Arc::new(LocalObjectStore::open(directory)?);
    let id = DatabaseId::new(Namespace::new("legacy", "compat")?, "main")?;
    let key = id.namespace.key("authority/root")?;
    let original = store.get(&key)?.context("frozen legacy root missing")?;
    let envelope: Envelope<'_> = serde_json::from_slice(&original.bytes)?;
    assert_eq!(
        blake3::hash(envelope.root.get().as_bytes())
            .to_hex()
            .as_str(),
        envelope.checksum
    );
    let root: serde_json::Value = serde_json::from_str(envelope.root.get())?;
    assert!(root.get("ownership").is_none());
    assert_eq!(root["branches"]["main"]["current_sequence"], 2);
    assert_eq!(root["branches"]["main"]["sequence"], 3);
    let changed_raw = envelope.root.get().replacen('{', "{ ", 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&changed_raw)?,
        root
    );
    let changed = format!(
        "{{\"root\":{changed_raw},\"checksum\":\"{}\"}}",
        envelope.checksum
    );
    store.put(&key, changed.as_bytes(), PutCondition::Match(original.etag))?;
    assert!(Authority::open(store.clone(), id.namespace.clone()).is_err());
    assert_eq!(store.get(&key)?.unwrap().bytes, changed.as_bytes());
    let changed_etag = store.get(&key)?.unwrap().etag;
    store.put(&key, &original.bytes, PutCondition::Match(changed_etag))?;
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let before = base.snapshot()?.root;
    let physical_before: Vec<_> = store
        .list(&id.namespace.key("authority/blobs")?)?
        .into_iter()
        .map(|key| {
            let bytes = store.get(&key).unwrap().unwrap().bytes;
            (key, bytes)
        })
        .collect();
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    let initial = authority.state(&id)?;
    let after = base.snapshot()?.root;
    assert_eq!(after.version, 2);
    assert_eq!(after.branches, before.branches);
    assert_eq!(after.snapshots, before.snapshots);
    assert_eq!(after.incarnation, before.incarnation);
    for (key, bytes) in physical_before {
        assert_eq!(store.get(&key)?.unwrap().bytes, bytes);
    }
    let result = authority.maintenance(
        &id,
        "old-pitr-3",
        MaintenanceOperation::Pitr { sequence: 3 },
    )?;
    assert_eq!(result["sequence"], 3);
    assert!(result["wal"].is_object());
    assert!(initial.binding.is_none());
    assert_eq!(
        after.ownership.as_ref().unwrap().branches["main"].state,
        GrantState::Revoked
    );
    let floor = after.ownership.unwrap().branches["main"].epoch;
    assert!(super::grant_request(&authority, id, floor, floor, 1000, "migration-reuse").is_err());
    Ok(())
}
