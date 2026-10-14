use super::support::*;
use anyhow::Result;
use peri_loom_v1::{
    authority_rpc::protocol::MaintenanceOperation, object_store::PutCondition, ownership::types::*,
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
};

#[test]
fn restore_revoke_and_receipt_are_atomic_and_epoch_tombstone_survives_recreate() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    initialize(store.clone())?;
    authority.maintenance(
        &identifier("main"),
        "snapshot",
        MaintenanceOperation::Snapshot {
            name: "saved".into(),
        },
    )?;
    let before = root(&raw)?.snapshot()?.root;
    let operation = MaintenanceOperation::Restore {
        snapshot: "saved".into(),
    };
    let first = authority.maintenance(&identifier("main"), "restore", operation.clone())?;
    let restored = root(&raw)?.snapshot()?.root;
    assert_eq!(restored.revision, before.revision + 1);
    assert_eq!(
        restored.ownership.as_ref().unwrap().branches["main"].state,
        GrantState::Revoked
    );
    assert_eq!(
        restored.ownership.as_ref().unwrap().branches["main"].epoch,
        10
    );
    assert!(restored
        .ownership
        .as_ref()
        .unwrap()
        .commands
        .contains_key("restore"));
    assert!(store.validate_session().is_err());
    assert_eq!(
        authority.maintenance(&identifier("main"), "restore", operation)?,
        first
    );
    assert_eq!(root(&raw)?.snapshot()?.root.revision, restored.revision);
    authority.maintenance(
        &identifier("main"),
        "fork",
        MaintenanceOperation::Branch {
            source: "main".into(),
            name: "other".into(),
        },
    )?;
    authority.maintenance(
        &identifier("main"),
        "drop",
        MaintenanceOperation::DropBranch,
    )?;
    authority.maintenance(
        &identifier("other"),
        "recreate",
        MaintenanceOperation::Branch {
            source: "other".into(),
            name: "main".into(),
        },
    )?;
    assert!(grant_request(&authority, "main", 10, 10, "reuse", 1000).is_err());
    let fresh = grant(&authority, 11, 10)?.binding;
    assert!(fresh.branch_incarnation > binding.branch_incarnation);
    Ok(())
}

#[test]
fn branches_have_independent_leases_and_clones_do_not_copy_grants() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let first = authority.open_session(&binding, AccessRole::Writer)?;
    initialize(first.clone())?;
    authority.maintenance(
        &identifier("main"),
        "fork",
        MaintenanceOperation::Branch {
            source: "main".into(),
            name: "other".into(),
        },
    )?;
    assert_eq!(
        authority.state(&identifier("other"))?.mode,
        OwnershipMode::Revoked
    );
    let other = authority
        .grant(grant_request(
            &authority,
            "other",
            1,
            0,
            "grant-other",
            10000,
        )?)?
        .binding;
    let second = authority.open_session(&other, AccessRole::Writer)?;
    first.validate_session()?;
    second.validate_session()?;
    authority.revoke(RevokeRequest {
        request_id: "revoke-main".into(),
        database: identifier("main"),
        expected_epoch: 10,
    })?;
    assert!(first.validate_session().is_err());
    second.validate_session()?;
    assert_eq!(root(&raw)?.snapshot()?.root.branches.len(), 2);
    Ok(())
}

#[test]
fn gc_starts_durable_ledger_before_physical_delete_and_replay_is_side_effect_free() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    store.put(&segment(b"orphan"), b"orphan", PutCondition::Absent)?;
    drop(store);
    let inspected = Arc::new(AtomicBool::new(false));
    let observed = inspected.clone();
    let base = root(&raw)?;
    *raw.before_delete.lock().unwrap() = Some(Hook {
        needle: "authority/blobs/".into(),
        skip: 0,
        action: Box::new(move || {
            let root = base.snapshot().unwrap().root;
            assert_eq!(
                root.ownership.unwrap().commands["gc"].result["status"],
                json!("pending_gc")
            );
            observed.store(true, Ordering::SeqCst);
        }),
    });
    let operation = MaintenanceOperation::GcCollect {
        safety_window_ms: 0,
    };
    let result = authority.maintenance(&identifier("main"), "gc", operation.clone())?;
    assert!(inspected.load(Ordering::SeqCst));
    assert_eq!(result["marked"], json!(1));
    assert_eq!(result["deleted"], json!(1));
    assert_eq!(result["status"], json!("completed_gc"));
    let count = raw.mutations.lock().unwrap().len();
    assert_eq!(
        authority.maintenance(&identifier("main"), "gc", operation)?,
        result
    );
    assert_eq!(raw.mutations.lock().unwrap().len(), count);
    assert!(root(&raw)?.snapshot()?.root.blobs.is_empty());
    Ok(())
}

#[test]
fn ownership_tombstones_and_request_digest_cannot_be_forged() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    store.put(&segment(b"orphan"), b"orphan", PutCondition::Absent)?;
    drop(store);
    let base = root(&raw)?;
    base.transact(None, |root| {
        root.ownership.as_mut().unwrap().commands.insert(
            "illegal".into(),
            CommandReceipt {
                digest: blake3::hash(b"x").to_hex().to_string(),
                result: json!(1),
            },
        );
        Ok(())
    })?;
    assert!(base
        .transact(None, |root| {
            root.ownership.as_mut().unwrap().branches.remove("main");
            Ok(())
        })
        .is_err());
    assert!(authority
        .maintenance(
            &identifier("main"),
            "illegal",
            MaintenanceOperation::GcCollect {
                safety_window_ms: 0
            }
        )
        .is_err());
    assert!(authority.quiescent());
    Ok(())
}

#[test]
fn unknown_delete_keeps_namespace_blocked_and_command_pending() -> Result<()> {
    for apply in [false, true] {
        let (raw, _, authority) = fixture()?;
        let binding = grant(&authority, 10, 0)?.binding;
        let store = authority.open_session(&binding, AccessRole::Writer)?;
        store.put(&segment(b"orphan"), b"orphan", PutCondition::Absent)?;
        drop(store);
        *raw.failure.lock().unwrap() = Some(("authority/blobs/".into(), apply));
        assert!(authority
            .maintenance(
                &identifier("main"),
                "gc",
                MaintenanceOperation::GcCollect {
                    safety_window_ms: 0
                }
            )
            .is_err());
        assert!(!authority.quiescent());
        let root = root(&raw)?.snapshot()?.root;
        assert_eq!(
            root.ownership.unwrap().commands["gc"].result["status"],
            json!("pending_gc")
        );
        assert!(!root.tombstones.values().next().unwrap().deleted);
        assert!(authority
            .revoke(RevokeRequest {
                request_id: "revoke".into(),
                database: identifier("main"),
                expected_epoch: 10
            })
            .is_err());
    }
    Ok(())
}

#[test]
fn physical_gc_delete_holds_transition_barrier_until_root_ack() -> Result<()> {
    let (raw, _, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    store.put(&segment(b"orphan"), b"orphan", PutCondition::Absent)?;
    drop(store);
    let request = grant_request(&authority, "main", 11, 10, "takeover", 10000)?;
    let gate = Arc::new(Gate::default());
    let paused = gate.clone();
    *raw.before_delete.lock().unwrap() = Some(Hook {
        needle: "authority/blobs/".into(),
        skip: 0,
        action: Box::new(move || paused.enter()),
    });
    let service = authority.clone();
    let gc = thread::spawn(move || {
        service.maintenance(
            &identifier("main"),
            "gc",
            MaintenanceOperation::GcCollect {
                safety_window_ms: 0,
            },
        )
    });
    gate.entered();
    assert!(!authority.quiescent());
    let service = authority.clone();
    let (sent, received) = mpsc::channel();
    let competing = request.clone();
    let takeover = thread::spawn(move || sent.send(service.grant(competing)).unwrap());
    assert!(received
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .is_err());
    takeover.join().unwrap();
    gate.release();
    gc.join().unwrap()?;
    authority.grant(request)?;
    assert_eq!(
        root(&raw)?.snapshot()?.root.ownership.unwrap().branches["main"].epoch,
        11
    );
    Ok(())
}

#[test]
fn gc_safety_window_starts_at_each_tombstone_not_namespace_boot() -> Result<()> {
    let (raw, clock, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    store.put(
        &segment(b"recent orphan"),
        b"recent orphan",
        PutCondition::Absent,
    )?;
    drop(store);
    clock.set(9000);
    let operation = MaintenanceOperation::GcCollect {
        safety_window_ms: 1,
    };
    let first = authority.maintenance(&identifier("main"), "mark", operation.clone())?;
    assert_eq!(first["marked"], json!(1));
    assert_eq!(first["deleted"], json!(0));
    let second = authority.maintenance(&identifier("main"), "not-aged", operation.clone())?;
    assert_eq!(second["deleted"], json!(0));
    assert_eq!(root(&raw)?.snapshot()?.root.blobs.len(), 1);
    thread::sleep(Duration::from_millis(2));
    clock.set(9001);
    let third = authority.maintenance(&identifier("main"), "aged", operation)?;
    assert_eq!(third["deleted"], json!(1));
    assert!(root(&raw)?.snapshot()?.root.blobs.is_empty());
    Ok(())
}
