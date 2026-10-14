use super::{
    identifier,
    process_support::{Cluster, WORKER_A, WORKER_B},
};
use anyhow::Result;
use peri_loom_v1::{
    authority_rpc::{
        client::AuthorityClient,
        protocol::{AuthorityCommand, SessionOperation},
    },
    manifest::SEGMENT_SIZE,
    object_store::PutCondition,
    ownership::types::{AccessRole, OwnershipCommand},
    worker_rpc::protocol::WorkerCommand,
};
use serde_json::json;

#[test]
fn readonly_grant_cannot_be_amplified_by_remote_worker_or_forged_binding() -> Result<()> {
    let mut cluster = Cluster::new()?;
    let mut writer = cluster.worker("verify-a", WORKER_A)?;
    let mut reader = cluster.worker("verify-b", WORKER_B)?;
    let id = identifier("role-security");
    let original = cluster.grant(id.clone(), "verify-a", 10, 0, 60000)?;
    writer.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::AcquireDatabase {
                lease: original,
                role: AccessRole::Writer,
            },
        },
    )?;
    let opened = writer.worker(
        false,
        WorkerCommand::OpenSession {
            database: id.clone(),
        },
    )?;
    let session = opened["session"].as_str().unwrap().to_owned();
    writer.worker(
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(7)".into(),
        },
    )?;
    writer.worker(
        false,
        WorkerCommand::Compact {
            session: session.clone(),
        },
    )?;
    writer.worker(false, WorkerCommand::CloseSession { session })?;
    let lease = cluster.grant_role(id.clone(), "verify-b", 11, 10, 60000, AccessRole::ReadOnly)?;
    assert_eq!(lease.binding.role, AccessRole::ReadOnly);
    let remote = AuthorityClient::new(
        &format!("http://{}/", cluster.authority.address),
        WORKER_B.into(),
    )?;
    for command in [
        AuthorityCommand::ValidateLease {
            binding: lease.binding.clone(),
            role: AccessRole::Writer,
        },
        AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer,
        },
    ] {
        assert!(
            remote.call(command).is_err(),
            "ReadOnly grant was amplified to Writer"
        );
    }
    let mut forged = lease.binding.clone();
    forged.role = AccessRole::Writer;
    assert!(remote
        .call(AuthorityCommand::ValidateLease {
            binding: forged.clone(),
            role: AccessRole::Writer
        })
        .is_err());
    assert!(remote
        .call(AuthorityCommand::OpenSession {
            binding: forged,
            role: AccessRole::Writer
        })
        .is_err());
    let scope = remote.call(AuthorityCommand::OpenSession {
        binding: lease.binding.clone(),
        role: AccessRole::ReadOnly,
    })?;
    let scope = scope["session"].as_str().unwrap().to_owned();
    let bytes = vec![4; SEGMENT_SIZE];
    let key = id
        .namespace
        .key(&format!("segments/{}", blake3::hash(&bytes).to_hex()))?;
    assert!(remote
        .call(AuthorityCommand::Session {
            session: scope.clone(),
            operation: SessionOperation::Put {
                key,
                bytes,
                condition: PutCondition::Absent,
            }
        })
        .is_err());
    reader.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::AcquireDatabase {
                lease,
                role: AccessRole::ReadOnly,
            },
        },
    )?;
    let opened = reader.worker(false, WorkerCommand::OpenSession { database: id })?;
    assert_eq!(opened["readonly"], true);
    let session = opened["session"].as_str().unwrap().to_owned();
    assert_eq!(
        reader.worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT id FROM items".into()
            }
        )?,
        json!([[7]])
    );
    assert!(reader
        .worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "/* malicious */ INSERT INTO items VALUES(8) RETURNING id".into()
            }
        )
        .is_err());
    assert!(reader
        .worker(
            false,
            WorkerCommand::Execute {
                session,
                sql: "PRAGMA query_only=OFF; INSERT INTO items VALUES(9)".into()
            }
        )
        .is_err());
    remote.call(AuthorityCommand::CloseSession { session: scope })?;
    writer.worker(true, WorkerCommand::Shutdown)?;
    reader.worker(true, WorkerCommand::Shutdown)?;
    assert!(writer.wait()?.success());
    assert!(reader.wait()?.success());
    cluster.control.call(AuthorityCommand::Shutdown)?;
    assert!(cluster.authority.wait()?.success());
    eprintln!("P4 independent actual ReadOnly grant: remote Writer OpenSession/ValidateLease, forged role, scoped PUT and native query/pragma writes all denied");
    Ok(())
}
