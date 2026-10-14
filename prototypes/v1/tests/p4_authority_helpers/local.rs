use super::support::*;
use anyhow::Result;
use peri_loom_v1::{
    authority_rpc::protocol::MaintenanceOperation,
    object_store::{ObjectStore, PutCondition},
    ownership::types::*,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn observe_root(raw: &Store, count: &Arc<AtomicUsize>) {
    let observed = count.clone();
    *raw.before_get.lock().unwrap() = Some(Hook {
        needle: "authority/root".into(),
        skip: 0,
        action: Box::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    });
}

#[test]
fn local_cache_fence_never_reads_root_but_ingress_and_durable_io_do() -> Result<()> {
    let (raw, clock, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    let count = Arc::new(AtomicUsize::new(0));
    observe_root(&raw, &count);
    for _page in 0..1000 {
        store.validate_local()?;
    }
    assert_eq!(count.load(Ordering::SeqCst), 0);
    store.validate_session()?;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    observe_root(&raw, &count);
    store.put(&segment(b"durable"), b"durable", PutCondition::Absent)?;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    observe_root(&raw, &count);
    assert!(store.get(&segment(b"durable"))?.is_some());
    assert_eq!(count.load(Ordering::SeqCst), 3);
    observe_root(&raw, &count);
    clock.set(10100);
    assert!(store.validate_local().is_err());
    assert_eq!(count.load(Ordering::SeqCst), 3);
    Ok(())
}

#[test]
fn trusted_compat_branch_local_cache_validation_has_no_root_io() -> Result<()> {
    let (raw, _, _) = fixture()?;
    let authority = root(&raw)?;
    let branch = authority.branch_store("main")?;
    let count = Arc::new(AtomicUsize::new(0));
    observe_root(&raw, &count);
    for _page in 0..1000 {
        branch.validate_local()?;
    }
    assert_eq!(count.load(Ordering::SeqCst), 0);
    branch.validate_session()?;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn known_takeover_and_uncertainty_reject_local_cache_without_root_io() -> Result<()> {
    for uncertain in [false, true] {
        let (raw, _, authority) = fixture()?;
        let binding = grant(&authority, 10, 0)?.binding;
        let store = authority.open_session(&binding, AccessRole::Writer)?;
        if uncertain {
            *raw.failure.lock().unwrap() = Some(("authority/blobs/".into(), false));
            assert!(store
                .put(&segment(b"unknown"), b"unknown", PutCondition::Absent)
                .is_err());
        } else {
            grant(&authority, 11, 10)?;
        }
        let count = Arc::new(AtomicUsize::new(0));
        observe_root(&raw, &count);
        assert!(store.validate_local().is_err());
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[test]
fn restore_and_drop_remove_known_live_memory_binding_before_reopening_admission() -> Result<()> {
    for restore in [false, true] {
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
        let operation = if restore {
            MaintenanceOperation::Restore {
                snapshot: "saved".into(),
            }
        } else {
            MaintenanceOperation::DropBranch
        };
        authority.maintenance(&identifier("main"), "revoke", operation.clone())?;
        let count = Arc::new(AtomicUsize::new(0));
        observe_root(&raw, &count);
        assert!(store.validate_local().is_err());
        assert_eq!(count.load(Ordering::SeqCst), 0);
        authority.maintenance(&identifier("main"), "revoke", operation.clone())?;
        observe_root(&raw, &count);
        assert!(store.validate_local().is_err());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        if restore {
            let fresh_binding = grant(&authority, 11, 10)?.binding;
            let fresh = authority.open_session(&fresh_binding, AccessRole::Writer)?;
            authority.maintenance(&identifier("main"), "revoke", operation)?;
            let count = Arc::new(AtomicUsize::new(0));
            observe_root(&raw, &count);
            fresh.validate_local()?;
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}
