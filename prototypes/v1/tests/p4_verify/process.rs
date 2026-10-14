use super::{
    identifier,
    process_support::{
        authority_arguments, Cluster, Process, AUTHORITY_CONTROL, WORKER_A, WORKER_B,
    },
};
use anyhow::{ensure, Context, Result};
use peri_loom_v1::{
    authority_rpc::{
        client::AuthorityClient,
        protocol::{AuthorityCommand, MaintenanceOperation, SessionOperation},
    },
    object_store::{PutCondition, StoredObject},
    ownership::types::{
        AccessRole, GrantReply, LeaseIntent, NonceReply, OwnershipCommand, RenewIntent,
        RenewRequest,
    },
    worker_rpc::protocol::WorkerCommand,
};
use serde_json::json;
use std::{os::unix::process::ExitStatusExt, thread, time::Duration};

#[test]
fn actual_two_workers_fence_cached_sql_commit_head_and_worker_maintenance() -> Result<()> {
    let mut cluster = Cluster::new()?;
    let mut first = cluster.worker("verify-a", WORKER_A)?;
    let mut second = cluster.worker("verify-b", WORKER_B)?;
    let id = identifier("process-fence");
    let lease = cluster.grant(id.clone(), "verify-a", 10, 0, 60000)?;
    first.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::AcquireDatabase {
                lease: lease.clone(),
                role: AccessRole::Writer,
            },
        },
    )?;
    let opened = first.worker(
        false,
        WorkerCommand::OpenSession {
            database: id.clone(),
        },
    )?;
    let session = opened["session"].as_str().unwrap().to_owned();
    let heartbeat = first.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::GetWorkerHeartbeat,
        },
    )?;
    assert_eq!(heartbeat["resources"]["loaded_databases"], 1);
    assert!(heartbeat["resources"]["databases"][0]["resources"].is_null());
    let full = first.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::GetWorkerState,
        },
    )?;
    assert!(full["resources"]["databases"][0]["resources"].is_object());
    first.worker(
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)".into(),
        },
    )?;
    first.worker(
        false,
        WorkerCommand::Checkpoint {
            session: session.clone(),
        },
    )?;
    assert_eq!(
        first.worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT count(*) FROM items".into()
            }
        )?,
        json!([[1]])
    );
    let remote_a = AuthorityClient::new(
        &format!("http://{}/", cluster.authority.address),
        WORKER_A.into(),
    )?;
    let remote_b = AuthorityClient::new(
        &format!("http://{}/", cluster.authority.address),
        WORKER_B.into(),
    )?;
    let raw_scope = remote_a.call(AuthorityCommand::OpenSession {
        binding: lease.binding.clone(),
        role: AccessRole::Writer,
    })?;
    let raw_session = raw_scope["session"].as_str().unwrap().to_owned();
    let head_key = id.namespace.key("HEAD")?;
    let head: Option<StoredObject> =
        serde_json::from_value(remote_a.call(AuthorityCommand::Session {
            session: raw_session.clone(),
            operation: SessionOperation::Get {
                key: head_key.clone(),
            },
        })?)?;
    let head = head.unwrap();
    for command in [
        AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer,
        },
        AuthorityCommand::Session {
            session: raw_session.clone(),
            operation: SessionOperation::Validate,
        },
    ] {
        assert!(
            remote_b.call(command).is_err(),
            "other worker principal used worker A's scope"
        );
    }
    assert!(first
        .worker(
            false,
            WorkerCommand::Ownership {
                action: OwnershipCommand::GetWorkerState
            }
        )
        .is_err());
    assert!(first
        .worker(
            true,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT 1".into()
            }
        )
        .is_err());
    assert!(remote_a
        .call(AuthorityCommand::Maintenance {
            request_id: "worker-gc-denied".into(),
            database: id.clone(),
            operation: MaintenanceOperation::GcCollect {
                safety_window_ms: 0
            }
        })
        .is_err());
    assert!(remote_a
        .call(AuthorityCommand::Session {
            session: raw_session.clone(),
            operation: SessionOperation::Put {
                key: id.namespace.key("authority/root")?,
                bytes: b"replacement".to_vec(),
                condition: PutCondition::Any,
            }
        })
        .is_err());
    assert!(remote_a
        .call(AuthorityCommand::Session {
            session: raw_session.clone(),
            operation: SessionOperation::Delete {
                key: head_key.clone(),
                expected: None
            }
        })
        .is_err());
    first.worker(
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "BEGIN; INSERT INTO items VALUES(999)".into(),
        },
    )?;
    let next = cluster.grant(id.clone(), "verify-b", 11, 10, 60000)?;
    second.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::AcquireDatabase {
                lease: next,
                role: AccessRole::Writer,
            },
        },
    )?;
    for command in [
        WorkerCommand::Query {
            session: session.clone(),
            sql: "SELECT count(*) FROM items".into(),
        },
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "COMMIT".into(),
        },
        WorkerCommand::Checkpoint {
            session: session.clone(),
        },
        WorkerCommand::Compact {
            session: session.clone(),
        },
    ] {
        assert!(
            first.worker(false, command).is_err(),
            "stale worker operation succeeded"
        );
    }
    assert!(remote_a
        .call(AuthorityCommand::Session {
            session: raw_session.clone(),
            operation: SessionOperation::Put {
                key: head_key,
                bytes: head.bytes,
                condition: PutCondition::Match(head.etag),
            }
        })
        .is_err());
    assert!(remote_a
        .call(AuthorityCommand::CloseSession {
            session: raw_session
        })
        .is_ok());
    let opened = second.worker(false, WorkerCommand::OpenSession { database: id })?;
    let current = opened["session"].as_str().unwrap().to_owned();
    assert_eq!(
        second.worker(
            false,
            WorkerCommand::Query {
                session: current.clone(),
                sql: "SELECT id FROM items".into()
            }
        )?,
        json!([[1]])
    );
    second.worker(
        false,
        WorkerCommand::Execute {
            session: current.clone(),
            sql: "INSERT INTO items VALUES(2)".into(),
        },
    )?;
    assert_eq!(
        second.worker(
            false,
            WorkerCommand::Query {
                session: current,
                sql: "PRAGMA integrity_check".into()
            }
        )?,
        json!([["ok"]])
    );
    first.worker(true, WorkerCommand::Shutdown)?;
    second.worker(true, WorkerCommand::Shutdown)?;
    assert!(first.wait()?.success());
    assert!(second.wait()?.success());
    cluster.control.call(AuthorityCommand::Shutdown)?;
    assert!(cluster.authority.wait()?.success());
    eprintln!("P4 independent actual workers A=10 B=11: cached SQL/COMMIT/checkpoint/compact/HEAD denied; raw-backend-credential-free worker args/env; principal/scope/GC/root/delete abuse denied");
    Ok(())
}

#[test]
fn authority_sigkill_causes_partition_refusal_and_default_dirty_restart_refusal() -> Result<()> {
    use peri_loom_v1::ownership::{clock::SystemLeaseClock, types::LeaseClock};
    let clock = SystemLeaseClock;
    let started = clock.now_ms()?;
    let mut cluster = Cluster::new()?;
    let mut worker = cluster.worker("verify-a", WORKER_A)?;
    let id = identifier("partition");
    let mut lease = cluster.grant(id.clone(), "verify-a", 10, 0, 2000)?;
    eprintln!(
        "P4_KILL_SETUP stage=grant_ack elapsed_ms={} binding={}",
        clock.now_ms()? - started,
        serde_json::to_string(&lease)?
    );
    worker.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::AcquireDatabase {
                lease: lease.clone(),
                role: AccessRole::Writer,
            },
        },
    )?;
    eprintln!(
        "P4_KILL_SETUP stage=acquire_ack elapsed_ms={}",
        clock.now_ms()? - started
    );
    let opened = worker.worker(
        false,
        WorkerCommand::OpenSession {
            database: id.clone(),
        },
    )?;
    eprintln!(
        "P4_KILL_SETUP stage=open_ack elapsed_ms={}",
        clock.now_ms()? - started
    );
    let session = opened["session"].as_str().unwrap().to_owned();
    lease = renew_partition_setup(&cluster, &worker, &lease)?;
    eprintln!(
        "P4_KILL_SETUP stage=pre_sql_renew_ack elapsed_ms={} binding={}",
        clock.now_ms()? - started,
        serde_json::to_string(&lease)?
    );
    worker.worker(
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)".into(),
        },
    )?;
    eprintln!(
        "P4_KILL_SETUP stage=execute_ack elapsed_ms={}",
        clock.now_ms()? - started
    );
    lease = renew_partition_setup(&cluster, &worker, &lease)?;
    eprintln!(
        "P4_KILL_SETUP stage=pre_kill_renew_ack elapsed_ms={} binding={}",
        clock.now_ms()? - started,
        serde_json::to_string(&lease)?
    );
    let live = worker.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::GetDatabaseState {
                database: id.clone(),
            },
        },
    )?;
    ensure!(
        live["mode"] == "writer"
            && live["remaining_ttl_ms"]
                .as_u64()
                .is_some_and(|remaining| remaining > 0),
        "partition fault setup lease is not live: {live}"
    );
    eprintln!(
        "P4_KILL_SETUP stage=live_before_kill elapsed_ms={} state={live}",
        clock.now_ms()? - started
    );
    eprintln!(
        "P4_KILL_SETUP stage=kill_begin elapsed_ms={}",
        clock.now_ms()? - started
    );
    assert_eq!(cluster.authority.kill()?.signal(), Some(9));
    eprintln!(
        "P4_KILL_SETUP stage=kill_ack signal=9 elapsed_ms={}",
        clock.now_ms()? - started
    );
    assert!(worker
        .worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT id FROM items".into()
            }
        )
        .is_err());
    assert!(worker
        .worker(
            false,
            WorkerCommand::Execute {
                session,
                sql: "INSERT INTO items VALUES(2)".into()
            }
        )
        .is_err());
    let mut restarted = Process::spawn(
        &authority_arguments(cluster.temporary.path()),
        &[
            ("PERI_AUTHORITY_CONTROL_TOKEN", AUTHORITY_CONTROL),
            ("PERI_AUTHORITY_WORKERS_FILE", &cluster.credential_path),
        ],
    )?;
    let status = restarted.wait()?;
    ensure!(!status.success(), "unclean authority restart was admitted");
    let diagnostics = restarted.diagnostics();
    ensure!(
        diagnostics.to_ascii_lowercase().contains("unclean")
            || diagnostics.to_ascii_lowercase().contains("dirty"),
        "restart failed for an unrelated reason: {diagnostics}"
    );
    thread::sleep(Duration::from_millis(2200));
    assert!(worker
        .worker(
            true,
            WorkerCommand::Ownership {
                action: OwnershipCommand::RenewLease { lease }
            }
        )
        .is_err());
    let state = worker.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::GetDatabaseState { database: id },
        },
    )?;
    assert_eq!(state["mode"], "revoked");
    worker.worker(true, WorkerCommand::Shutdown)?;
    assert!(worker.wait()?.success());
    eprintln!("P4 independent actual authority SIGKILL signal=9: cached SQL partition refused, default dirty restart exited nonzero, expired renewal did not resurrect");
    Ok(())
}

fn renew_partition_setup(
    cluster: &Cluster,
    worker: &Process,
    lease: &GrantReply,
) -> Result<GrantReply> {
    let sequence = lease
        .binding
        .renewal_sequence
        .checked_add(1)
        .context("renewal sequence exhausted")?;
    let intent = RenewIntent {
        binding: lease.binding.clone(),
        sequence,
        ttl_ms: 2000,
    };
    let nonce: NonceReply =
        serde_json::from_value(cluster.control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Renew(intent.clone()),
        })?)?;
    let renewed: GrantReply =
        serde_json::from_value(cluster.control.call(AuthorityCommand::Renew {
            request: RenewRequest {
                request_id: format!("partition-setup-renew-{sequence}"),
                nonce: nonce.nonce,
                intent,
            },
        })?)?;
    let mut expected = lease.binding.clone();
    expected.renewal_sequence = sequence;
    ensure!(
        renewed.binding == expected && renewed.remaining_ttl_ms > 0,
        "setup renewal full identity mismatch"
    );
    worker.worker(
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::RenewLease {
                lease: renewed.clone(),
            },
        },
    )?;
    Ok(renewed)
}
