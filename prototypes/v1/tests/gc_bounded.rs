#[path = "../src/authority/p4_test_support.rs"]
mod support;

use anyhow::Result;
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    authority_rpc::protocol::MaintenanceOperation,
    database::RemoteDatabase,
    object_store::{ObjectStore, PutCondition},
    ownership::types::*,
};
use serde_json::json;
use std::{sync::Arc, thread};
use support::*;

fn upload_garbage(
    authority: &Arc<OwnershipAuthority>,
    binding: &LeaseBinding,
    count: usize,
) -> Result<Vec<String>> {
    let session = authority.open_session(binding, AccessRole::Writer)?;
    let mut blobs = Vec::new();
    for index in 0..count {
        let bytes = vec![index as u8 + 1; 4096];
        session.put(&segment(&bytes), &bytes, PutCondition::Absent)?;
        blobs.push(segment(&bytes));
    }
    // Dropping the session releases its reader pin; only then do the segments become
    // collection candidates.
    drop(session);
    Ok(blobs)
}

fn pause_before_root_put(raw: &Arc<Store>, gate: &Arc<Gate>) {
    let paused = gate.clone();
    *raw.before_put.lock().unwrap() = Some(Hook {
        needle: "authority/root".into(),
        skip: 0,
        action: Box::new(move || paused.enter()),
    });
}

/// A collection cycle no longer owns the namespace exclusive: mark planning and its
/// publication run without a transition, so reads, SQL, and a lease renewal keep
/// progressing while the cycle is mid-flight.
#[test]
fn gc_mark_does_not_block_reads_sql_or_lease_renewal() -> Result<()> {
    let (raw, clock, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    initialize(store.clone())?;
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(
        store.clone(),
        identifier("main").namespace,
        cache.path(),
        binding.epoch,
    )?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    let garbage = upload_garbage(&authority, &binding, 4)?;

    let gate = Arc::new(Gate::default());
    pause_before_root_put(&raw, &gate);
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

    // Read-only maintenance is served from the immutable published snapshot.
    let refs = authority.maintenance(
        &identifier("main"),
        "refs-during-mark",
        MaintenanceOperation::Refs,
    )?;
    assert!(refs["revision"].as_u64().unwrap() >= 1);
    let inspect = authority.maintenance(
        &identifier("main"),
        "inspect-during-mark",
        MaintenanceOperation::GcInspect,
    )?;
    assert!(inspect["root_revision"].as_u64().unwrap() >= 1);

    // A live session keeps committing while collection is paused.
    database.execute("INSERT INTO items VALUES (1, 'live')")?;

    // Renewal takes the namespace write transition; it must not see collection holding it.
    clock.set(200);
    let intent = RenewIntent {
        binding: binding.clone(),
        sequence: 1,
        ttl_ms: 20_000,
    };
    let request = RenewRequest {
        request_id: "renew-during-gc".into(),
        nonce: authority
            .challenge(LeaseIntent::Renew(intent.clone()))?
            .nonce,
        intent,
    };
    assert!(authority.renew(request)?.remaining_ttl_ms > 0);

    gate.release();
    let result = gc.join().unwrap()?;
    assert_eq!(result["status"], json!("completed_gc"));
    // Retiring the branch pin's superseded generation makes the initial HEAD/manifest and
    // the intermediate WAL index collectable as well, so the exact count is not the point:
    // every committed recovery point must keep resolving and its bodies must still exist.
    assert!(
        result["deleted"].as_u64().unwrap() >= garbage.len() as u64,
        "collection did not delete every garbage segment: {result}"
    );
    let namespace = identifier("main").namespace;
    for sequence in [1, 2] {
        let point = authority.maintenance(
            &identifier("main"),
            &format!("pitr-{sequence}"),
            MaintenanceOperation::Pitr { sequence },
        )?;
        for entry in [&point["head"], &point["wal"]] {
            let blob = entry["blob"].as_str().expect("recovery entry blob");
            let key = namespace.key(&format!("authority/blobs/{blob}"))?;
            assert!(
                raw.get(&key)?.is_some(),
                "collection removed the body of recovery point {sequence}: {blob}"
            );
        }
    }
    assert_eq!(
        database.query("SELECT value FROM items WHERE id=1")?.len(),
        1
    );
    Ok(())
}

/// A restore publication that lands between a mark plan and its publication must win:
/// the stale attempt loses the CAS, the cycle re-plans on the restored head, and no
/// object reachable from the restored branch is removed.
#[test]
fn restore_between_mark_and_publish_rewinds_the_plan() -> Result<()> {
    let (raw, _clock, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    initialize(store.clone())?;
    let cache = tempfile::tempdir()?;
    let database = RemoteDatabase::open(
        store.clone(),
        identifier("main").namespace,
        cache.path(),
        binding.epoch,
    )?;
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    database.execute("INSERT INTO items VALUES (1, 'kept')")?;
    database.checkpoint()?;
    authority.maintenance(
        &identifier("main"),
        "snapshot",
        MaintenanceOperation::Snapshot {
            name: "saved".into(),
        },
    )?;
    database.execute("INSERT INTO items VALUES (2, 'discarded')")?;
    database.checkpoint()?;
    let garbage = upload_garbage(&authority, &binding, 2)?;

    let service = authority.clone();
    let target = identifier("main");
    let gate = Arc::new(Gate::default());
    let paused = gate.clone();
    *raw.before_put.lock().unwrap() = Some(Hook {
        needle: "authority/root".into(),
        skip: 0,
        action: Box::new(move || {
            service
                .maintenance(
                    &target,
                    "restore-during-mark",
                    MaintenanceOperation::Restore {
                        snapshot: "saved".into(),
                    },
                )
                .expect("restore during collection must publish");
            paused.enter();
        }),
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
    gate.release();
    let result = gc.join().unwrap()?;
    assert_eq!(result["status"], json!("completed_gc"));
    assert!(
        result["deleted"].as_u64().unwrap() >= garbage.len() as u64,
        "collection did not advance after the restore conflict"
    );

    // The restored head still reads its one retained row through an unfenced handle.
    let restored_cache = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(
        Authority::open(raw.clone(), identifier("main").namespace)?.branch_store("main")?,
        identifier("main").namespace,
        restored_cache.path(),
        binding.epoch,
    )?;
    assert_eq!(
        restored.query("SELECT value FROM items WHERE id=1")?.len(),
        1
    );
    assert_eq!(
        restored.query("SELECT value FROM items WHERE id=2")?.len(),
        0
    );
    let root = root(&raw)?.snapshot()?.root;
    assert_eq!(
        root.branches["main"].current_sequence,
        root.snapshots["saved"].sequence
    );
    assert!(root
        .ownership
        .as_ref()
        .unwrap()
        .commands
        .contains_key("restore-during-mark"));
    Ok(())
}
