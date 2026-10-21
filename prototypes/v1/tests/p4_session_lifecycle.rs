#[allow(dead_code)]
#[path = "p4_remote_helpers/harness.rs"]
mod harness;

use anyhow::{ensure, Context, Result};
use harness::{identifier, Harness};
use peri_loom_v1::authority::CatalogRoot;
use peri_loom_v1::authority_rpc::protocol::{AuthorityCommand, SessionOperation};
use peri_loom_v1::object_store::ObjectStore;
use peri_loom_v1::ownership::types::{
    AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseBinding, LeaseIntent, NonceReply,
};
use peri_loom_v1::runtime::DatabaseId;
use serde_json::Value;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

fn session_id(reply: Value) -> Result<String> {
    Ok(reply["session"]
        .as_str()
        .context("opaque authority session missing")?
        .to_owned())
}

fn open(service: &Harness, binding: &LeaseBinding, role: AccessRole) -> Result<String> {
    session_id(service.worker.call(AuthorityCommand::OpenSession {
        binding: binding.clone(),
        role,
    })?)
}

fn grant_ttl(
    service: &Harness,
    database: DatabaseId,
    worker: &str,
    epoch: u64,
    expected: u64,
    ttl_ms: u64,
) -> Result<GrantReply> {
    let intent = GrantIntent {
        role: AccessRole::Writer,
        database,
        worker_id: worker.to_owned(),
        epoch,
        expected_epoch: expected,
        lease_id: harness::token()?,
        ttl_ms,
    };
    let nonce: NonceReply =
        serde_json::from_value(service.control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Grant(intent.clone()),
        })?)?;
    Ok(serde_json::from_value(service.control.call(
        AuthorityCommand::Grant {
            request: GrantRequest {
                request_id: harness::token()?,
                nonce: nonce.nonce,
                intent,
            },
        },
    )?)?)
}

fn pins(service: &Harness, database: &DatabaseId) -> Result<Vec<String>> {
    let object = service
        .raw
        .get(&database.namespace.key("authority/root")?)?
        .context("authority root missing")?;
    let envelope: Value = serde_json::from_slice(&object.bytes)?;
    let root: CatalogRoot = serde_json::from_value(envelope["root"].clone())?;
    Ok(root.pins.into_keys().collect())
}

fn wait_for(mut check: impl FnMut() -> Result<bool>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if check()? {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "authority session condition not reached before deadline"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn session_state(service: &Harness, session: &str) -> Result<Value> {
    service.worker.call(AuthorityCommand::SessionState {
        session: session.to_owned(),
    })
}

#[test]
fn duplicate_open_and_close_release_references_once() -> Result<()> {
    let mut service = Harness::new()?;
    service.verify_preconditions()?;
    let database = identifier("refcount")?;
    let lease = service.grant(database.clone(), "worker-a", 1, 0)?;

    let first = open(&service, &lease.binding, AccessRole::Writer)?;
    let second = service.worker.call(AuthorityCommand::OpenSession {
        binding: lease.binding.clone(),
        role: AccessRole::Writer,
    })?;
    assert_eq!(second["session"].as_str(), Some(first.as_str()));
    assert_eq!(
        second["reused"], true,
        "duplicate OPEN allocated a new scope"
    );
    let state = session_state(&service, &first)?;
    assert_eq!(state["state"], "live");
    assert_eq!(state["references"], 2);

    let proof = service.worker.call(AuthorityCommand::CloseSession {
        session: first.clone(),
    })?;
    assert_eq!(proof["closed"], true);
    assert_eq!(proof["drained"], false);
    assert_eq!(proof["references"], 1);
    service.worker.call(AuthorityCommand::Session {
        session: first.clone(),
        operation: SessionOperation::Validate,
    })?;

    let proof = service.worker.call(AuthorityCommand::CloseSession {
        session: first.clone(),
    })?;
    assert_eq!(proof["drained"], true);
    assert!(
        pins(&service, &database)?.is_empty(),
        "drained proof returned before the reader pin was released"
    );
    let proof = service.worker.call(AuthorityCommand::CloseSession {
        session: first.clone(),
    })?;
    assert_eq!(
        proof["already_closed"], true,
        "a repeated CLOSE must return a closed proof, not release again"
    );
    assert!(
        service
            .other
            .call(AuthorityCommand::CloseSession {
                session: first.clone()
            })
            .is_err(),
        "another worker must not read or retire this proof"
    );
    assert!(service
        .other
        .call(AuthorityCommand::SessionState {
            session: first.clone()
        })
        .is_err());
    assert!(
        pins(&service, &database)?.is_empty(),
        "drained session left a reader pin"
    );

    // A new OPEN after the final CLOSE is a new scope, not a resurrected tombstone.
    let reopened = open(&service, &lease.binding, AccessRole::Writer)?;
    assert_ne!(reopened, first);
    assert_eq!(session_state(&service, &reopened)?["state"], "live");
    service
        .worker
        .call(AuthorityCommand::RetireSession { session: reopened })?;
    service.shutdown()
}

#[test]
fn expired_lease_reaps_orphans_and_releases_pins() -> Result<()> {
    let mut service = Harness::new()?;
    service.verify_preconditions()?;
    let orphan = identifier("orphan")?;
    let healthy = identifier("healthy")?;
    let orphan_lease = service.grant(orphan.clone(), "worker-a", 1, 0)?;
    // The healthy lease must outlive the fake clock advance below.
    let healthy_lease = grant_ttl(&service, healthy.clone(), "worker-a", 1, 0, 60_000)?;
    let first = open(&service, &orphan_lease.binding, AccessRole::Writer)?;
    let second = open(&service, &orphan_lease.binding, AccessRole::ReadOnly)?;
    assert_ne!(first, second, "roles must own distinct remote scopes");
    let kept = open(&service, &healthy_lease.binding, AccessRole::Writer)?;
    assert!(
        !pins(&service, &orphan)?.is_empty(),
        "open session did not create a reader pin"
    );

    service.clock.0.fetch_add(20_000, Ordering::SeqCst);
    wait_for(|| {
        let metrics =
            service.control.call(AuthorityCommand::GetMetrics)?["authority_sessions"].clone();
        Ok(metrics["live"] == 1 && metrics["orphan"] == 0 && metrics["retiring"] == 0)
    })?;

    for session in [&first, &second] {
        let state = session_state(&service, session)?;
        assert_eq!(state["state"], "closed");
        assert_eq!(state["retire_reason"], "lease_expired");
    }
    assert!(
        pins(&service, &orphan)?.is_empty(),
        "expired orphan sessions kept their reader pins"
    );
    // Reaping one namespace must not disturb another database's live lease.
    service.worker.call(AuthorityCommand::Session {
        session: kept,
        operation: SessionOperation::Validate,
    })?;
    let metrics = service.control.call(AuthorityCommand::GetMetrics)?["authority_sessions"].clone();
    assert_eq!(metrics["reaped_total"], 2);
    service.shutdown()
}

#[test]
fn takeover_fences_the_old_binding_without_reuse() -> Result<()> {
    let mut service = Harness::new()?;
    service.verify_preconditions()?;
    let database = identifier("fenced")?;
    let old = service.grant(database.clone(), "worker-a", 1, 0)?;
    let old_session = open(&service, &old.binding, AccessRole::Writer)?;
    service.worker.call(AuthorityCommand::Session {
        session: old_session.clone(),
        operation: SessionOperation::Validate,
    })?;

    let replacement = service.grant(database.clone(), "worker-a", 2, 1)?;
    assert!(service
        .worker
        .call(AuthorityCommand::OpenSession {
            binding: old.binding.clone(),
            role: AccessRole::Writer
        })
        .is_err());
    assert!(
        service
            .worker
            .call(AuthorityCommand::Session {
                session: old_session.clone(),
                operation: SessionOperation::Validate
            })
            .is_err(),
        "fenced binding still dispatched after takeover"
    );
    wait_for(|| {
        let state = session_state(&service, &old_session)?;
        Ok(state["state"] == "closed")
    })?;
    let closed = session_state(&service, &old_session)?;
    assert_eq!(closed["retire_reason"], "lease_fenced");

    let new_session = open(&service, &replacement.binding, AccessRole::Writer)?;
    assert_ne!(new_session, old_session);
    service.worker.call(AuthorityCommand::Session {
        session: new_session,
        operation: SessionOperation::Validate,
    })?;
    service.shutdown()
}
