#[path = "p4_remote_helpers/harness.rs"]
mod harness;

use anyhow::{Context, Result};
use harness::{identifier, Harness};
use peri_loom_v1::authority_rpc::protocol::{AuthorityCommand, SessionOperation};
use peri_loom_v1::authority_rpc::RemoteAuthorityProvider;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::object_store::{ObjectStore, PutCondition};
use peri_loom_v1::ownership::types::{
    AccessRole, GrantReply, LeaseIntent, NonceReply, RenewIntent, RenewRequest,
};
use peri_loom_v1::runtime::{DatabaseProvider, WorkerLimits, WorkerRuntime};
use std::sync::atomic::Ordering;

/// 从 harness 搬来：本目标是该助手的唯一使用者。
fn session(reply: serde_json::Value) -> Result<String> {
    Ok(reply["session"]
        .as_str()
        .context("opaque session missing")?
        .to_owned())
}

#[test]
fn remote_native_sql_cold_reopen_and_readonly() -> Result<()> {
    let mut service = Harness::new()?;
    service.verify_preconditions()?;
    let database = identifier("native")?;
    let lease = service.grant(database.clone(), "worker-a", 1, 0)?;
    let provider = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    provider.install_lease(lease.clone(), AccessRole::Writer)?;
    let cache = tempfile::tempdir()?;
    let worker = WorkerRuntime::new(provider.clone(), cache.path(), WorkerLimits::default())?;
    {
        let native = worker.open(database.clone(), 1)?;
        native.execute("CREATE TABLE records(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO records VALUES(1,'remote');")?;
        assert_eq!(
            native.query("SELECT value FROM records WHERE id=1")?.len(),
            1
        );
    }
    assert!(worker.close(&database)?);
    assert_eq!(worker.status()?.loaded_databases, 0);
    provider.install_lease(lease, AccessRole::ReadOnly)?;
    let cold = tempfile::tempdir()?;
    let reopened = WorkerRuntime::new(provider.clone(), cold.path(), WorkerLimits::default())?;
    {
        let store = provider.open_store(&database, 1)?;
        assert_eq!(store.access_role(), AccessRole::ReadOnly);
        drop(store);
        let native = reopened.open(database.clone(), 1)?;
        assert!(native.is_readonly()?);
        assert_eq!(
            native.query("SELECT value FROM records WHERE id=1")?.len(),
            1
        );
        assert!(native
            .execute("INSERT INTO records VALUES(2,'denied')")
            .is_err());
    }
    assert!(reopened.close(&database)?);
    assert!(service.owner.quiescent());
    service.shutdown()
}

#[test]
fn principals_sessions_and_physical_keys_are_isolated() -> Result<()> {
    let mut service = Harness::new()?;
    let database = identifier("isolation")?;
    let lease = service.grant(database.clone(), "worker-a", 1, 0)?;
    assert!(service.worker.call(AuthorityCommand::Shutdown).is_err());
    assert!(service
        .other
        .call(AuthorityCommand::GetDatabaseState {
            database: database.clone()
        })
        .is_err());
    assert!(service
        .other
        .call(AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer
        })
        .is_err());
    let opaque = session(service.worker.call(AuthorityCommand::OpenSession {
        binding: lease.binding.clone(),
        role: AccessRole::Writer,
    })?)?;
    assert!(service
        .other
        .call(AuthorityCommand::Session {
            session: opaque.clone(),
            operation: SessionOperation::Validate
        })
        .is_err());
    assert!(service
        .worker
        .call(AuthorityCommand::Session {
            session: "00000000000000000000000000000000".into(),
            operation: SessionOperation::Validate
        })
        .is_err());
    for key in [
        "ROOT".to_owned(),
        database.namespace.key("authority/root")?,
        "namespaces/other/db/HEAD".to_owned(),
    ] {
        assert!(service
            .worker
            .call(AuthorityCommand::Session {
                session: opaque.clone(),
                operation: SessionOperation::Get { key }
            })
            .is_err());
    }
    assert!(service
        .worker
        .call(AuthorityCommand::Session {
            session: opaque.clone(),
            operation: SessionOperation::Delete {
                key: database.namespace.key("HEAD")?,
                expected: None
            }
        })
        .is_err());
    assert!(service
        .worker
        .call(AuthorityCommand::Session {
            session: opaque.clone(),
            operation: SessionOperation::Put {
                key: database.namespace.key("HEAD")?,
                bytes: b"{}".to_vec(),
                condition: PutCondition::Any
            }
        })
        .is_err());
    service
        .worker
        .call(AuthorityCommand::CloseSession { session: opaque })?;
    service.shutdown()
}

#[test]
fn renewal_fresh_ttl_and_takeover_fence_cached_native_queries() -> Result<()> {
    let mut service = Harness::new()?;
    let database = identifier("takeover")?;
    let lease = service.grant(database.clone(), "worker-a", 1, 0)?;
    let provider = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    provider.install_lease(lease.clone(), AccessRole::Writer)?;
    let cache = tempfile::tempdir()?;
    let native = RemoteDatabase::open(
        provider.open_store(&database, 1)?,
        database.namespace.clone(),
        cache.path(),
        1,
    )?;
    native.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1);")?;
    let intent = RenewIntent {
        binding: lease.binding.clone(),
        sequence: 1,
        ttl_ms: 10_000,
    };
    let nonce: NonceReply =
        serde_json::from_value(service.control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Renew(intent.clone()),
        })?)?;
    let renewed: GrantReply =
        serde_json::from_value(service.control.call(AuthorityCommand::Renew {
            request: RenewRequest {
                request_id: harness::token()?,
                nonce: nonce.nonce,
                intent,
            },
        })?)?;
    provider.install_lease(renewed.clone(), AccessRole::Writer)?;
    service.clock.0.fetch_add(100, Ordering::SeqCst);
    assert!(
        provider
            .validate_lease(&lease.binding, AccessRole::Writer)?
            .remaining_ttl_ms
            < renewed.remaining_ttl_ms
    );
    assert_eq!(native.query("SELECT * FROM items")?.len(), 1);
    let replacement = service.grant(database.clone(), "worker-a", 2, 1)?;
    provider.install_lease(replacement, AccessRole::Writer)?;
    provider.remove_lease(&database, 1)?;
    assert!(provider.open_store(&database, 2).is_ok());
    assert!(native.query("SELECT * FROM items").is_err());
    assert!(native.execute("INSERT INTO items VALUES(2)").is_err());
    assert!(native.checkpoint().is_err());
    drop(native);
    service.shutdown()
}

#[test]
fn grant_replay_is_idempotent_and_worker_cannot_grant() -> Result<()> {
    let mut service = Harness::new()?;
    let request = service.grant_request(identifier("receipts")?, "worker-a", 1, 0)?;
    assert!(service
        .worker
        .call(AuthorityCommand::Grant {
            request: request.clone()
        })
        .is_err());
    let first = service.control.call(AuthorityCommand::Grant {
        request: request.clone(),
    })?;
    let root_key = request.intent.database.namespace.key("authority/root")?;
    let committed_etag = service.raw.get(&root_key)?.unwrap().etag;
    let second = service.control.call(AuthorityCommand::Grant {
        request: request.clone(),
    })?;
    assert_eq!(first, second);
    assert_eq!(committed_etag, service.raw.get(&root_key)?.unwrap().etag);
    let mut altered = request;
    altered.intent.lease_id = harness::token()?;
    assert!(service
        .control
        .call(AuthorityCommand::Grant { request: altered })
        .is_err());
    service.shutdown()
}

#[test]
fn readonly_grant_cannot_escalate_via_provider_or_wire() -> Result<()> {
    let mut service = Harness::new()?;
    let database = identifier("readonly-grant")?;
    let mut request = service.grant_request(database, "worker-a", 1, 0)?;
    request.intent.role = AccessRole::ReadOnly;
    let nonce: NonceReply =
        serde_json::from_value(service.control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Grant(request.intent.clone()),
        })?)?;
    request.nonce = nonce.nonce;
    let lease: GrantReply =
        serde_json::from_value(service.control.call(AuthorityCommand::Grant { request })?)?;
    let provider = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    assert!(provider
        .install_lease(lease.clone(), AccessRole::Writer)
        .is_err());
    assert!(provider
        .validate_lease(&lease.binding, AccessRole::Writer)
        .is_err());
    assert!(service
        .worker
        .call(AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer
        })
        .is_err());
    assert!(service
        .worker
        .call(AuthorityCommand::ValidateLease {
            binding: lease.binding.clone(),
            role: AccessRole::Writer
        })
        .is_err());
    provider.install_lease(lease.clone(), AccessRole::ReadOnly)?;
    provider.validate_lease(&lease.binding, AccessRole::ReadOnly)?;
    let mut forged = lease.binding.clone();
    forged.role = AccessRole::Writer;
    assert!(service
        .worker
        .call(AuthorityCommand::OpenSession {
            binding: forged,
            role: AccessRole::Writer,
        })
        .is_err());
    service.shutdown()
}

#[path = "p4_remote_helpers/wire.rs"]
mod wire;
