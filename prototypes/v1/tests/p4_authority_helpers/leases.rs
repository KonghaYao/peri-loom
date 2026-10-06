use super::support::*;
use anyhow::Result;
use peri_loom_v1::{
    authority::ownership::OwnershipAuthority, object_store::PutCondition, ownership::types::*,
};
use std::{
    sync::{atomic::Ordering, Arc, Barrier},
    thread,
};

#[test]
fn grant_receipt_is_idempotent_and_reuse_with_other_payload_fails() -> Result<()> {
    let (_, clock, authority) = fixture()?;
    let request = grant_request(&authority, "main", 10, 0, "grant-10", 1000)?;
    let first = authority.grant(request.clone())?;
    clock.set(200);
    let replay = authority.grant(request.clone())?;
    assert_eq!(first.binding, replay.binding);
    assert_eq!(replay.remaining_ttl_ms, 900);
    let mut changed = request.clone();
    changed.intent.ttl_ms += 1;
    assert!(authority.grant(changed).is_err());
    clock.set(1100);
    assert!(authority.grant(request).is_err());
    assert_eq!(
        authority.state(&identifier("main"))?.mode,
        OwnershipMode::Revoked
    );
    Ok(())
}

#[test]
fn nonce_is_single_use_bounded_and_intent_bound() -> Result<()> {
    let (_, clock, authority) = fixture()?;
    let request = grant_request(&authority, "main", 10, 0, "first", 10000)?;
    let mut changed = request.clone();
    changed.intent.worker_id = "attacker".into();
    assert!(authority.grant(changed).is_err());
    authority.grant(request.clone())?;
    let mut reused = request;
    reused.request_id = "second".into();
    assert!(authority.grant(reused).is_err());
    let late = grant_request(&authority, "main", 11, 10, "late", 10000)?;
    clock.set(5100);
    assert!(authority.grant(late).is_err());
    for ttl in [0, 99, 60001, u64::MAX] {
        assert!(grant_request(&authority, "main", 11, 10, "bad", ttl).is_err());
    }
    assert_eq!(authority.state(&identifier("main"))?.epoch, 10);
    Ok(())
}

#[test]
fn renew_requires_current_sequence_and_keeps_existing_session_valid() -> Result<()> {
    let (_, clock, authority) = fixture()?;
    let binding = grant(&authority, 10, 0)?.binding;
    let store = authority.open_session(&binding, AccessRole::Writer)?;
    clock.set(200);
    let intent = RenewIntent {
        binding: binding.clone(),
        sequence: 1,
        ttl_ms: 20000,
    };
    let request = RenewRequest {
        request_id: "renew-1".into(),
        nonce: authority
            .challenge(LeaseIntent::Renew(intent.clone()))?
            .nonce,
        intent,
    };
    let reply = authority.renew(request.clone())?;
    assert_eq!(reply.binding.renewal_sequence, 1);
    assert_eq!(
        authority
            .validate_lease(&binding, AccessRole::Writer)?
            .binding,
        reply.binding
    );
    store.put(
        &segment(b"new segment"),
        b"new segment",
        PutCondition::Absent,
    )?;
    clock.set(300);
    assert_eq!(authority.renew(request)?.remaining_ttl_ms, 19900);
    assert!(authority
        .challenge(LeaseIntent::Renew(RenewIntent {
            binding: binding.clone(),
            sequence: 2,
            ttl_ms: 1000
        }))
        .is_err());
    let mut forged = reply.binding;
    forged.renewal_sequence = 100;
    assert!(authority
        .validate_lease(&forged, AccessRole::Writer)
        .is_err());
    Ok(())
}

#[test]
fn late_renew_cannot_resurrect_expired_writer() -> Result<()> {
    let (_, clock, authority) = fixture()?;
    let binding = authority
        .grant(grant_request(&authority, "main", 10, 0, "grant", 1000)?)?
        .binding;
    let intent = RenewIntent {
        binding,
        sequence: 1,
        ttl_ms: 60000,
    };
    let request = RenewRequest {
        request_id: "renew".into(),
        nonce: authority
            .challenge(LeaseIntent::Renew(intent.clone()))?
            .nonce,
        intent,
    };
    clock.set(1100);
    assert!(authority.renew(request).is_err());
    assert_eq!(
        authority.state(&identifier("main"))?.mode,
        OwnershipMode::Revoked
    );
    Ok(())
}

#[test]
fn clock_regression_or_unavailability_closes_namespace() -> Result<()> {
    for unavailable in [false, true] {
        let (_, clock, authority) = fixture()?;
        let binding = grant(&authority, 10, 0)?.binding;
        if unavailable {
            clock.failed.store(true, Ordering::SeqCst);
        } else {
            clock.set(99);
        }
        assert!(authority
            .validate_lease(&binding, AccessRole::Writer)
            .is_err());
        clock.failed.store(false, Ordering::SeqCst);
        clock.set(200);
        assert!(authority.state(&identifier("main")).is_err());
        assert!(!authority.quiescent());
    }
    Ok(())
}

#[test]
fn restart_term_revokes_old_binding_but_keeps_epoch_floor() -> Result<()> {
    let (store, clock, authority) = fixture()?;
    let old = grant(&authority, 10, 0)?.binding;
    drop(authority);
    let restarted = OwnershipAuthority::new(store, clock);
    let state = restarted.state(&identifier("main"))?;
    assert_eq!(state.epoch, 10);
    assert_eq!(state.mode, OwnershipMode::Revoked);
    assert!(restarted.validate_lease(&old, AccessRole::Writer).is_err());
    let fresh = grant(&restarted, 11, 10)?.binding;
    assert!(fresh.authority_term > old.authority_term);
    assert_eq!(fresh.root_incarnation, old.root_incarnation);
    Ok(())
}

#[test]
fn concurrent_namespace_load_boots_once() -> Result<()> {
    let store = Arc::new(Store::default());
    let clock = Arc::new(Clock::default());
    let authority = OwnershipAuthority::new(store.clone(), clock);
    let start = Arc::new(Barrier::new(9));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let authority = authority.clone();
        let start = start.clone();
        tasks.push(thread::spawn(move || {
            start.wait();
            authority.state(&identifier("main")).unwrap();
        }));
    }
    start.wait();
    for task in tasks {
        task.join().unwrap();
    }
    assert_eq!(
        root(&store)?
            .snapshot()?
            .root
            .ownership
            .unwrap()
            .authority_term,
        1
    );
    assert!(authority.quiescent());
    Ok(())
}

#[test]
fn delayed_grant_nonce_cannot_cross_restore_at_same_epoch() -> Result<()> {
    use peri_loom_v1::authority_rpc::protocol::MaintenanceOperation;
    let (_, _, authority) = fixture()?;
    let request = grant_request(&authority, "main", 10, 0, "delayed", 10000)?;
    authority.maintenance(
        &identifier("main"),
        "save",
        MaintenanceOperation::Snapshot {
            name: "empty".into(),
        },
    )?;
    authority.maintenance(
        &identifier("main"),
        "restore",
        MaintenanceOperation::Restore {
            snapshot: "empty".into(),
        },
    )?;
    assert!(authority.grant(request).is_err());
    assert_eq!(authority.state(&identifier("main"))?.epoch, 0);
    grant(&authority, 10, 0)?;
    Ok(())
}

#[test]
fn epoch_exhaustion_and_clock_overflow_fail_closed() -> Result<()> {
    let (_, _, authority) = fixture()?;
    authority.grant(grant_request(
        &authority,
        "main",
        u64::MAX,
        0,
        "last-epoch",
        1000,
    )?)?;
    assert!(grant_request(&authority, "main", u64::MAX, u64::MAX, "overflow", 1000).is_err());
    let (_, clock, authority) = fixture()?;
    clock.set(u64::MAX - 4000);
    assert!(grant_request(&authority, "main", 10, 0, "clock-overflow", 1000).is_err());
    assert!(!authority.quiescent());
    Ok(())
}

#[test]
fn backend_without_explicit_terminal_contract_is_rejected_before_boot() -> Result<()> {
    let store = Arc::new(Store::default());
    store.unsupported.store(true, Ordering::SeqCst);
    let authority = OwnershipAuthority::new(store.clone(), Arc::new(Clock::default()));
    let error = authority.state(&identifier("main")).unwrap_err();
    assert!(format!("{error:#}").contains("UncertainUnsupported"));
    assert!(store.mutations.lock().unwrap().is_empty());
    assert!(!authority.quiescent());
    Ok(())
}

#[test]
fn initial_root_and_boot_lost_ack_both_keep_startup_closed() -> Result<()> {
    for initialized in [false, true] {
        let store = Arc::new(Store::default());
        if initialized {
            root(&store)?;
        }
        *store.failure.lock().unwrap() = Some(("authority/root".into(), true));
        let authority = OwnershipAuthority::new(store.clone(), Arc::new(Clock::default()));
        assert!(authority.state(&identifier("main")).is_err());
        assert!(!authority.quiescent());
        let count = store.mutations.lock().unwrap().len();
        assert!(authority.state(&identifier("main")).is_err());
        assert_eq!(store.mutations.lock().unwrap().len(), count);
    }
    Ok(())
}
