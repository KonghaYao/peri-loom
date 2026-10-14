use super::{
    grant, grant_request, identifier,
    support::{Clock, ProbeStore},
};
use anyhow::Result;
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    ownership::types::{
        AccessRole, GrantIntent, GrantState, LeaseIntent, OwnershipMode, RenewIntent, RenewRequest,
        RevokeRequest,
    },
};

#[test]
fn nonce_binding_exact_deadline_and_receipt_payload_conflict_are_fail_closed() -> Result<()> {
    let clock = Clock::new(1000);
    let store = ProbeStore::new(clock.clone());
    let authority = OwnershipAuthority::new(store.clone(), clock.clone());
    let id = identifier("nonce");
    for ttl_ms in [0, 99, 60001, u64::MAX] {
        let intent = GrantIntent {
            database: id.clone(),
            worker_id: "worker".into(),
            epoch: 10,
            expected_epoch: 0,
            lease_id: "lease".into(),
            ttl_ms,
            role: AccessRole::Writer,
        };
        assert!(authority.challenge(LeaseIntent::Grant(intent)).is_err());
    }
    let request = grant_request(&authority, id.clone(), 10, 0, 1000, "nonce-bound")?;
    let mut mismatched = request.clone();
    mismatched.intent.worker_id = "forged-worker".into();
    assert!(authority.grant(mismatched).is_err());
    let mut mismatched_role = request.clone();
    mismatched_role.intent.role = AccessRole::ReadOnly;
    assert!(authority.grant(mismatched_role).is_err());
    let lease = authority.grant(request.clone())?;
    clock.set(1100);
    let replay = authority.grant(request.clone())?;
    assert_eq!(replay.binding, lease.binding);
    assert_eq!(replay.remaining_ttl_ms, 900);
    let mut conflict = request.clone();
    conflict.intent.ttl_ms = 2000;
    assert!(authority.grant(conflict).is_err());
    clock.set(2000);
    let dispatches = store.put_count();
    assert!(authority.grant(request).is_err());
    assert!(authority
        .open_session(&lease.binding, AccessRole::Writer)
        .is_err());
    assert_eq!(store.put_count(), dispatches);
    assert_eq!(authority.state(&id)?.mode, OwnershipMode::Revoked);
    let fresh = grant_request(&authority, id.clone(), 11, 10, 1000, "late-intent")?;
    clock.set(7000);
    let dispatches = store.put_count();
    assert!(authority.grant(fresh).is_err());
    assert_eq!(store.put_count(), dispatches);
    assert_eq!(authority.state(&id)?.epoch, 10);
    Ok(())
}

#[test]
fn renew_replay_after_revoke_never_resurrects_and_epoch_floor_survives_term_boot() -> Result<()> {
    let clock = Clock::new(1000);
    let store = ProbeStore::new(clock.clone());
    let authority = OwnershipAuthority::new(store.clone(), clock.clone());
    let id = identifier("renew");
    let original = grant(&authority, id.clone(), 10, 1000)?;
    let intent = RenewIntent {
        binding: original.binding.clone(),
        sequence: 1,
        ttl_ms: 2000,
    };
    let nonce = authority.challenge(LeaseIntent::Renew(intent.clone()))?;
    let request = RenewRequest {
        request_id: "renew-1".into(),
        nonce: nonce.nonce,
        intent,
    };
    clock.set(1500);
    let renewed = authority.renew(request.clone())?;
    assert_eq!(renewed.binding.renewal_sequence, 1);
    clock.set(1600);
    assert_eq!(authority.renew(request.clone())?.remaining_ttl_ms, 1900);
    let mut wrong_sequence = RenewIntent {
        binding: renewed.binding.clone(),
        sequence: 3,
        ttl_ms: 1000,
    };
    assert!(authority
        .challenge(LeaseIntent::Renew(wrong_sequence.clone()))
        .is_err());
    wrong_sequence.sequence = 2;
    let nonce = authority.challenge(LeaseIntent::Renew(wrong_sequence.clone()))?;
    let pending = RenewRequest {
        request_id: "renew-delayed".into(),
        nonce: nonce.nonce,
        intent: wrong_sequence,
    };
    authority.revoke(RevokeRequest {
        request_id: "revoke-10".into(),
        database: id.clone(),
        expected_epoch: 10,
    })?;
    let dispatches = store.put_count();
    assert!(authority.renew(pending).is_err());
    assert!(authority.renew(request).is_err());
    assert_eq!(store.put_count(), dispatches);
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let old_root = base.snapshot()?.root;
    let old_term = old_root.ownership.unwrap().authority_term;
    drop(authority);
    let restarted = OwnershipAuthority::new(store, clock);
    assert_eq!(restarted.state(&id)?.epoch, 10);
    let catalog = base.snapshot()?.root.ownership.unwrap();
    assert_eq!(catalog.authority_term, old_term + 1);
    assert_eq!(catalog.branches["main"].state, GrantState::Revoked);
    assert!(restarted
        .validate_lease(&renewed.binding, AccessRole::Writer)
        .is_err());
    assert!(grant_request(&restarted, id.clone(), 10, 10, 1000, "reuse-epoch").is_err());
    assert_eq!(
        grant(&restarted, id, 11, 1000)?.binding.authority_term,
        old_term + 1
    );
    Ok(())
}

#[test]
fn clock_regression_blocks_namespace_without_affecting_a_distinct_namespace() -> Result<()> {
    let clock = Clock::new(1000);
    let store = ProbeStore::new(clock.clone());
    let authority = OwnershipAuthority::new(store.clone(), clock.clone());
    let id = identifier("regression");
    let lease = grant(&authority, id.clone(), 10, 1000)?;
    clock.set(999);
    assert!(authority
        .validate_lease(&lease.binding, AccessRole::Writer)
        .is_err());
    clock.set(1001);
    let dispatches = store.put_count();
    assert!(authority.state(&id).is_err());
    assert!(authority
        .revoke(RevokeRequest {
            request_id: "revoke-blocked".into(),
            database: id,
            expected_epoch: 10
        })
        .is_err());
    assert_eq!(store.put_count(), dispatches);
    let other = identifier("unaffected");
    assert_eq!(grant(&authority, other, 10, 1000)?.binding.epoch, 10);
    Ok(())
}
