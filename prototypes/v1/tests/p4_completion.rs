#![cfg(unix)]

#[path = "p4_completion/http.rs"]
mod http;
#[allow(dead_code)]
#[path = "p4_verify/process_support.rs"]
mod process_support;
#[allow(dead_code)]
#[path = "p4_verify/trace.rs"]
mod trace;

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use peri_loom_v1::authority::CatalogRoot;
use peri_loom_v1::authority_rpc::{
    protocol::{AuthorityCommand, RpcRequest, RpcResponse, SessionOperation},
    AuthorityClient, RemoteAuthorityProvider,
};
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PutCondition};
use peri_loom_v1::ownership::types::{
    AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseBinding, LeaseIntent, OwnershipCommand,
};
use peri_loom_v1::runtime::{DatabaseId, DatabaseProvider};
use peri_loom_v1::wal::{RemoteWal, WalAppendRequest, WalBatch, WalOperation};
use peri_loom_v1::worker_rpc::protocol::{WorkerCommand, WorkerRequest, WorkerResponse};
use serde_json::{json, Value};

use http::{Backend, Edge, Target};
use process_support::{Process, AUTHORITY_CONTROL, WORKER_A, WORKER_B, WORKER_CONTROL, WORKER_SQL};

struct Fixture {
    authority: Process,
    control: Arc<AuthorityClient>,
    worker_client: Arc<AuthorityClient>,
    first: Process,
    second: Process,
    rpc: Edge,
    backend: Edge,
    raw: Arc<dyn ObjectStore>,
    directory: tempfile::TempDir,
    environment: Vec<(String, String)>,
    database: DatabaseId,
}

fn identifier(name: &str) -> DatabaseId {
    DatabaseId::new(Namespace::new("p4-completion", name).unwrap(), "main").unwrap()
}

fn private_evidence_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && metadata.permissions().mode() & 0o777 == 0o700,
        "P4 S3 evidence directory must be a private, non-symlink directory (0700)"
    );
    Ok(())
}

fn retained_directory(parent: &Path) -> Result<tempfile::TempDir> {
    private_evidence_directory(parent)?;
    let mut directory = tempfile::Builder::new()
        .prefix("p4-s3-")
        .tempdir_in(parent)?;
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    directory.disable_cleanup(true);
    eprintln!(
        "retained P4 S3 local evidence: {}",
        directory.path().display()
    );
    Ok(directory)
}

#[test]
fn s3_evidence_directory_is_private_and_survives_fixture_drop() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))?;
    let retained = retained_directory(root.path())?;
    let path = retained.path().to_owned();
    drop(retained);
    assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o700);
    Ok(())
}

#[test]
fn s3_evidence_directory_rejects_non_private_and_symlink_paths() -> Result<()> {
    let root = tempfile::tempdir()?;
    let evidence = root.path().join("evidence");
    fs::create_dir(&evidence)?;
    fs::set_permissions(&evidence, fs::Permissions::from_mode(0o700))?;
    private_evidence_directory(&evidence)?;
    fs::set_permissions(&evidence, fs::Permissions::from_mode(0o755))?;
    assert!(private_evidence_directory(&evidence).is_err());
    fs::set_permissions(&evidence, fs::Permissions::from_mode(0o700))?;
    let linked = root.path().join("linked");
    symlink(&evidence, &linked)?;
    assert!(private_evidence_directory(&linked).is_err());
    Ok(())
}

#[test]
fn s3_evidence_retains_owned_process_stderr_with_private_mode() -> Result<()> {
    let evidence = tempfile::tempdir()?;
    let path = evidence
        .path()
        .to_str()
        .context("evidence path is not Unicode")?;
    let mut process = Process::spawn(&[], &[("PERI_TEST_PROCESS_LOG_DIR", path)])?;
    assert!(!process.wait()?.success());
    drop(process);
    let logs = fs::read_dir(evidence.path())?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(logs.len(), 1);
    let log = &logs[0].path();
    assert_eq!(fs::metadata(log)?.permissions().mode() & 0o777, 0o600);
    assert!(fs::read_to_string(log)?.contains("usage: peri-loom-v1"));
    Ok(())
}

impl Fixture {
    fn new(real_s3: bool) -> Result<Self> {
        let directory = if real_s3 {
            ensure!(
                std::env::var("PERI_P4_S3_TEST").as_deref() == Ok("1"),
                "requires Main's independent bucket and PERI_P4_S3_TEST=1"
            );
            let evidence = std::env::var("PERI_P4_S3_EVIDENCE_DIR")?;
            retained_directory(Path::new(&evidence))?
        } else {
            tempfile::tempdir()?
        };
        let (backend, raw, config) = if real_s3 {
            let raw = peri_loom_v1::config::authority_backend("s3")?;
            let endpoint = std::env::var("S3_ENDPOINT")?;
            let backend = Edge::start(Backend::Forward(endpoint))?;
            let mut config = Vec::new();
            for name in [
                "S3_REGION",
                "S3_BUCKET",
                "S3_ACCESS_KEY_ID",
                "S3_SECRET_ACCESS_KEY",
                "S3_SESSION_TOKEN",
            ] {
                if let Ok(value) = std::env::var(name) {
                    config.push((name.to_owned(), value));
                }
            }
            (backend, raw, config)
        } else {
            let local = Arc::new(LocalObjectStore::open(directory.path().join("objects"))?);
            let backend = Edge::start(Backend::Local(local.clone()))?;
            let config = vec![
                ("S3_REGION".into(), "us-east-1".into()),
                ("S3_BUCKET".into(), "independent-p4".into()),
                ("S3_ACCESS_KEY_ID".into(), "fixtureaccess".into()),
                (
                    "S3_SECRET_ACCESS_KEY".into(),
                    "fixture-secret-not-a-credential".into(),
                ),
            ];
            (backend, local as Arc<dyn ObjectStore>, config)
        };
        let mut environment = config;
        environment.push(("PERI_REQUEST_READ_TRACE".into(), "1".into()));
        environment.push(("S3_ENDPOINT".into(), backend.endpoint.clone()));
        if real_s3 {
            environment.push((
                "PERI_TEST_PROCESS_LOG_DIR".into(),
                directory.path().to_string_lossy().into_owned(),
            ));
        }
        let credentials = directory.path().join("workers.json");
        fs::write(
            &credentials,
            serde_json::to_vec(&json!({"verify-a": WORKER_A, "verify-b": WORKER_B}))?,
        )?;
        fs::set_permissions(&credentials, fs::Permissions::from_mode(0o600))?;
        environment.push((
            "PERI_AUTHORITY_CONTROL_TOKEN".into(),
            AUTHORITY_CONTROL.into(),
        ));
        environment.push((
            "PERI_AUTHORITY_WORKERS_FILE".into(),
            credentials.to_string_lossy().into_owned(),
        ));
        let authority = Process::start(
            &[
                "authority".into(),
                "s3".into(),
                "127.0.0.1:0".into(),
                directory
                    .path()
                    .join("state")
                    .to_string_lossy()
                    .into_owned(),
            ],
            &environment
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
        )?;
        let rpc = Edge::start(Backend::Forward(format!("http://{}", authority.address)))?;
        let endpoint = format!("{}/", rpc.endpoint);
        let control = AuthorityClient::new(&endpoint, AUTHORITY_CONTROL.into())?;
        let worker_client = AuthorityClient::new(&endpoint, WORKER_A.into())?;
        let first = worker(&directory, &endpoint, "verify-a", WORKER_A, real_s3)?;
        let second = worker(&directory, &endpoint, "verify-b", WORKER_B, real_s3)?;
        let name = format!(
            "scope-{}",
            directory
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .replace('.', "-")
        );
        let database = identifier(&name);
        Ok(Self {
            authority,
            control,
            worker_client,
            first,
            second,
            rpc,
            backend,
            raw,
            directory,
            environment,
            database,
        })
    }

    fn request(&self, epoch: u64, previous: u64) -> Result<GrantRequest> {
        let intent = GrantIntent {
            database: self.database.clone(),
            worker_id: if epoch == 10 { "verify-a" } else { "verify-b" }.into(),
            epoch,
            expected_epoch: previous,
            lease_id: format!("completion-lease-{epoch}"),
            ttl_ms: 60000,
            role: AccessRole::Writer,
        };
        let nonce = self.control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Grant(intent.clone()),
        })?;
        Ok(GrantRequest {
            request_id: format!("completion-grant-{epoch}"),
            nonce: nonce["nonce"].as_str().context("nonce missing")?.into(),
            intent,
        })
    }

    fn ledger(&self, phase: &str) -> Result<CatalogRoot> {
        let object = self
            .raw
            .get(&self.database.namespace.key("authority/root")?)?
            .context("ledger missing")?;
        let envelope: Value = serde_json::from_slice(&object.bytes)?;
        let root: CatalogRoot = serde_json::from_value(envelope["root"].clone())?;
        eprintln!(
            "P4_COMPLETION_LEDGER phase={phase} root_incarnation={} ownership={}",
            root.incarnation,
            serde_json::to_string(&root.ownership)?
        );
        eprintln!("P4_COMPLETION_PUBLICATIONS phase={phase} revision={} objects={} branches={} uploads={} pins={}", root.revision, serde_json::to_string(&root.objects)?, serde_json::to_string(&root.branches)?, serde_json::to_string(&root.uploads)?, serde_json::to_string(&root.pins)?);
        Ok(root)
    }

    fn baseline(&self) -> Result<(GrantReply, String)> {
        let lease: GrantReply =
            serde_json::from_value(self.control.call(AuthorityCommand::Grant {
                request: self.request(10, 0)?,
            })?)?;
        acquire(&self.first, &lease)?;
        let session = open(&self.first, &self.database)?;
        observed_worker(
            &self.first,
            false,
            WorkerCommand::Execute {
                session: session.clone(),
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)"
                    .into(),
            },
        )?;
        observed_worker(
            &self.first,
            false,
            WorkerCommand::Checkpoint {
                session: session.clone(),
            },
        )?;
        assert_eq!(
            observed_worker(
                &self.first,
                false,
                WorkerCommand::Query {
                    session: session.clone(),
                    sql: "SELECT id FROM items".into()
                }
            )?,
            json!([[1]])
        );
        identity(&self.ledger("before")?, &lease.binding)?;
        Ok((lease, session))
    }

    fn raw_session(&self, lease: &GrantReply) -> Result<String> {
        Ok(self.worker_client.call(AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer,
        })?["session"]
            .as_str()
            .context("scope missing")?
            .into())
    }

    fn replacement(&self, lease: &GrantReply, old_session: &str, count: u64) -> Result<()> {
        identity(&self.ledger("after-grant")?, &lease.binding)?;
        assert!(self
            .first
            .worker(
                false,
                WorkerCommand::Query {
                    session: old_session.into(),
                    sql: "SELECT count(*) FROM items".into()
                }
            )
            .is_err());
        acquire(&self.second, lease)?;
        let session = open(&self.second, &self.database)?;
        assert_eq!(
            self.second.worker(
                false,
                WorkerCommand::Query {
                    session: session.clone(),
                    sql: "SELECT count(*) FROM items".into()
                }
            )?,
            json!([[count]])
        );
        self.second.worker(
            false,
            WorkerCommand::Execute {
                session: session.clone(),
                sql: "INSERT INTO items VALUES(3)".into(),
            },
        )?;
        self.second.worker(
            false,
            WorkerCommand::Checkpoint {
                session: session.clone(),
            },
        )?;
        assert_eq!(
            self.second.worker(
                false,
                WorkerCommand::Query {
                    session: session.clone(),
                    sql: "SELECT count(*) FROM items".into()
                }
            )?,
            json!([[count + 1]])
        );
        self.second
            .worker(false, WorkerCommand::CloseSession { session })?;
        identity(&self.ledger("after-new-write")?, &lease.binding)?;
        let metrics = self.second.worker(true, WorkerCommand::GetMetrics)?;
        assert_eq!(metrics["retirement"]["available"], true);
        assert_eq!(metrics["retirement"]["quarantined_live_databases"], 0);
        assert_eq!(metrics["retirement"]["retiring_databases"], 0);
        for name in ["quarantined_databases", "pending_statements", "pending_io"] {
            assert_eq!(
                metrics["retirement"][name], 0,
                "idle after known Close: {name}"
            );
        }
        for sample in metrics["retirement"]["instances"].as_array().unwrap() {
            assert_eq!(sample["io"]["pending_native_operations"], 0);
            assert_eq!(sample["pending_retained_statements"], 0);
        }
        Ok(())
    }

    fn shutdown(&mut self, clean: bool) -> Result<()> {
        let _ = self.first.worker(true, WorkerCommand::Shutdown);
        let _ = self.second.worker(true, WorkerCommand::Shutdown);
        self.first.wait()?;
        self.second.wait()?;
        let outcome = wire_call(
            &self.rpc.endpoint,
            AUTHORITY_CONTROL,
            AuthorityCommand::Shutdown,
        );
        eprintln!("P4_COMPLETION_SHUTDOWN expected_clean={clean} outcome={outcome:?}");
        assert_eq!(self.authority.wait()?.success(), clean);
        let marker = fs::read(self.directory.path().join("state/authority.lock"))?;
        assert_eq!(
            marker,
            if clean {
                b"clean\n".to_vec()
            } else {
                b"dirty\n".to_vec()
            }
        );
        self.rpc.stop()?;
        Ok(())
    }
}

fn worker(
    directory: &tempfile::TempDir,
    endpoint: &str,
    name: &str,
    token: &str,
    retain_evidence: bool,
) -> Result<Process> {
    let mut environment = vec![
        ("PERI_WORKER_AUTHORITY_TOKEN", token),
        ("PERI_WORKER_CONTROL_TOKEN", WORKER_CONTROL),
        ("PERI_WORKER_SQL_TOKEN", WORKER_SQL),
    ];
    if retain_evidence {
        environment.push((
            "PERI_TEST_PROCESS_LOG_DIR",
            directory
                .path()
                .to_str()
                .context("evidence path is not Unicode")?,
        ));
    }
    Process::start(
        &[
            "owned-worker".into(),
            endpoint.into(),
            directory.path().join(name).to_string_lossy().into_owned(),
            name.into(),
            "127.0.0.1:0".into(),
        ],
        &environment,
    )
}

fn acquire(worker: &Process, lease: &GrantReply) -> Result<()> {
    observed_worker(
        worker,
        true,
        WorkerCommand::Ownership {
            action: OwnershipCommand::AcquireDatabase {
                lease: lease.clone(),
                role: AccessRole::Writer,
            },
        },
    )?;
    Ok(())
}

fn open(worker: &Process, database: &DatabaseId) -> Result<String> {
    Ok(observed_worker(
        worker,
        false,
        WorkerCommand::OpenSession {
            database: database.clone(),
        },
    )?["session"]
        .as_str()
        .context("worker session missing")?
        .into())
}

fn observed_worker(worker: &Process, control: bool, command: WorkerCommand) -> Result<Value> {
    let label = serde_json::to_value(&command)?["command"]
        .as_str()
        .context("worker command missing")?
        .to_owned();
    let started = Instant::now();
    eprintln!("P4_WORKER_BEGIN command={label} control={control}");
    let result = worker.worker(control, command);
    eprintln!("P4_WORKER_TERMINAL command={label} control={control} elapsed_ms={} transport_or_operation_ok={}", started.elapsed().as_millis(), result.is_ok());
    result.with_context(|| {
        format!(
            "worker phase={label} elapsed_ms={} child_stderr={}",
            started.elapsed().as_millis(),
            worker.diagnostics()
        )
    })
}

fn identity(root: &CatalogRoot, binding: &LeaseBinding) -> Result<()> {
    let catalog = root.ownership.as_ref().context("catalog missing")?;
    let owner = &catalog.branches[&binding.database.branch];
    assert_eq!(root.incarnation, binding.root_incarnation);
    assert_eq!(catalog.authority_term, binding.authority_term);
    assert_eq!(owner.branch_incarnation, binding.branch_incarnation);
    assert_eq!(owner.epoch, binding.epoch);
    assert_eq!(owner.worker_id, binding.worker_id);
    assert_eq!(owner.lease_id, binding.lease_id);
    assert_eq!(owner.grant_id, binding.grant_id);
    assert_eq!(owner.renewal_sequence, binding.renewal_sequence);
    assert_eq!(owner.role, binding.role);
    assert_eq!(
        serde_json::from_value::<GrantReply>(catalog.commands[&binding.grant_id].result.clone())?
            .binding,
        *binding
    );
    Ok(())
}

fn transition_started(fixture: &Fixture, request: &GrantRequest) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let outcome = wire_call(
            &fixture.rpc.endpoint,
            AUTHORITY_CONTROL,
            AuthorityCommand::Challenge {
                intent: LeaseIntent::Grant(request.intent.clone()),
            },
        );
        if outcome
            .as_ref()
            .is_err_and(|error| error.to_string().contains("Retryable admission closed"))
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "Grant did not enter the production admission transition: {outcome:?}"
        );
        thread::yield_now();
    }
}

fn put(
    client: &AuthorityClient,
    database: &DatabaseId,
    session: &str,
    bytes: Vec<u8>,
) -> Result<Value> {
    let key = database
        .namespace
        .key(&format!("segments/{}", blake3::hash(&bytes).to_hex()))?;
    client.call(AuthorityCommand::Session {
        session: session.into(),
        operation: SessionOperation::Put {
            key,
            bytes,
            condition: PutCondition::Absent,
        },
    })
}

#[test]
fn close_scope_and_grant_wait_for_real_backend_response_terminal() -> Result<()> {
    completion_gate(false, false, false)
}

#[test]
fn lost_backend_response_quarantines_grant_and_dirty_restart() -> Result<()> {
    completion_gate(false, true, false)
}

#[test]
fn close_scope_timeout_quarantines_even_after_backend_later_finishes() -> Result<()> {
    completion_gate(false, false, true)
}

#[test]
#[ignore = "Main supplies PERI_P4_S3_TEST=1 and an independent S3_BUCKET; no bucket creation/deletion"]
fn actual_s3_close_scope_and_grant_completion_gate() -> Result<()> {
    completion_gate(true, false, false)
}

#[test]
#[ignore = "Main supplies PERI_P4_S3_TEST=1 and an independent S3_BUCKET; no bucket creation/deletion"]
fn actual_s3_lost_backend_response_and_dirty_restart_gate() -> Result<()> {
    completion_gate(true, true, false)
}

fn completion_gate(real_s3: bool, lost: bool, timeout: bool) -> Result<()> {
    let mut fixture = Fixture::new(real_s3)?;
    let (lease, native_session) = fixture.baseline()?;
    let scope = fixture.raw_session(&lease)?;
    let request = fixture.request(11, 10)?;
    let entered = fixture.backend.arm(Target::Blob);
    thread::scope(|threads| -> Result<()> {
        let writing = threads.spawn(|| {
            put(
                &fixture.worker_client,
                &fixture.database,
                &scope,
                vec![71; 4096],
            )
        });
        let held = entered.recv_timeout(Duration::from_secs(3))?;
        let key = held.path.trim_start_matches('/').split_once('/').unwrap().1;
        assert_eq!(fixture.raw.get(key)?.unwrap().bytes, vec![71; 4096]);
        eprintln!("P4_COMPLETION_IO phase=backend-terminal-response-held key={key} bytes=4096");
        identity(
            &fixture.ledger("during-backend-response-held")?,
            &lease.binding,
        )?;
        let (grant_sent, grant_result) = mpsc::channel();
        let control = fixture.control.clone();
        let next_request = request.clone();
        let granting = threads.spawn(move || {
            let result = control.call(AuthorityCommand::Grant {
                request: next_request,
            });
            let _ = grant_sent.send(result);
        });
        transition_started(&fixture, &request)?;
        assert!(matches!(
            grant_result.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        identity(&fixture.ledger("during-grant-barrier")?, &lease.binding)?;
        let (close_sent, close_result) = mpsc::channel();
        let endpoint = fixture.rpc.endpoint.clone();
        let closing_scope = scope.clone();
        let closing = threads.spawn(move || {
            // The worker provider may still hold a reference to this scope; terminal
            // retirement is the operation that must drain it regardless of references.
            let _ = close_sent.send(wire_call(
                &endpoint,
                WORKER_A,
                AuthorityCommand::RetireSession {
                    session: closing_scope,
                },
            ));
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let outcome = wire_call(
                &fixture.rpc.endpoint,
                WORKER_A,
                AuthorityCommand::Session {
                    session: scope.clone(),
                    operation: SessionOperation::Validate,
                },
            );
            if outcome
                .as_ref()
                .is_err_and(|error| error.to_string().contains("authority scope retired"))
            {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "Close did not stop dispatch: {outcome:?}"
            );
            thread::yield_now();
        }
        assert!(matches!(
            close_result.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(matches!(
            grant_result.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        if timeout {
            let error = close_result
                .recv_timeout(Duration::from_secs(6))?
                .unwrap_err();
            ensure!(
                error.to_string().contains("retirement deadline"),
                "unexpected close timeout {error}"
            );
        }
        held.release(!lost)?;
        let write_result = writing.join().unwrap();
        assert_eq!(write_result.is_err(), lost || timeout);
        let grant_result = grant_result.recv_timeout(Duration::from_secs(3))?;
        granting.join().unwrap();
        closing.join().unwrap();
        if !timeout {
            let proof = close_result.recv_timeout(Duration::from_secs(3))?;
            if lost {
                // The write outcome is unknown, so the namespace is isolated and the
                // pin cannot be published away. The scope stays retiring for a retry.
                let error = proof.unwrap_err();
                ensure!(
                    error.to_string().contains("session release deferred"),
                    "unexpected deferred close error {error}"
                );
            } else {
                let proof = proof?;
                assert_eq!(proof["drained"], true);
                assert_eq!(
                    serde_json::from_value::<LeaseBinding>(proof["binding"].clone())?,
                    lease.binding
                );
            }
        }
        assert!(put(
            &fixture.worker_client,
            &fixture.database,
            &scope,
            vec![99; 32]
        )
        .is_err());
        if lost || timeout {
            let error = grant_result.unwrap_err();
            ensure!(
                error.to_string().contains("authority operation rejected"),
                "unexpected Grant error {error}"
            );
            let blocked = wire_call(
                &fixture.rpc.endpoint,
                AUTHORITY_CONTROL,
                AuthorityCommand::Grant {
                    request: request.clone(),
                },
            )
            .unwrap_err();
            ensure!(
                blocked.to_string().contains("UnknownOutcome"),
                "Grant not quarantined: {blocked}"
            );
            let root = fixture.ledger("after-quarantine")?;
            identity(&root, &lease.binding)?;
            assert!(!root.objects.contains_key(&format!(
                "segments/{}",
                blake3::hash(&vec![71; 4096]).to_hex()
            )));
            assert!(fixture
                .control
                .call(AuthorityCommand::Grant {
                    request: request.clone()
                })
                .is_err());
            assert!(fixture
                .first
                .worker(
                    false,
                    WorkerCommand::Execute {
                        session: native_session.clone(),
                        sql: "INSERT INTO items VALUES(99)".into()
                    }
                )
                .is_err());
            isolated_namespace(&fixture)?;
        } else {
            let next: GrantReply = serde_json::from_value(grant_result?)?;
            fixture.replacement(&next, &native_session, 1)?;
        }
        Ok(())
    })?;
    fixture.shutdown(!lost && !timeout)?;
    if lost || timeout {
        let mut restarted = Process::spawn(
            &[
                "authority".into(),
                "s3".into(),
                "127.0.0.1:0".into(),
                fixture
                    .directory
                    .path()
                    .join("state")
                    .to_string_lossy()
                    .into_owned(),
            ],
            &fixture
                .environment
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
        )?;
        assert!(!restarted.wait()?.success());
        ensure!(
            restarted.diagnostics().contains("quiescent"),
            "dirty restart did not reject: {}",
            restarted.diagnostics()
        );
        for (name, reconciled) in [
            ("state", true),
            ("new-state", false),
            ("new-reconciled-state", true),
        ] {
            let state = fixture.directory.path().join(name);
            let mut arguments = vec![
                "authority".into(),
                "s3".into(),
                "127.0.0.1:0".into(),
                state.to_string_lossy().into_owned(),
            ];
            if reconciled {
                arguments.push("--reconciled".into());
            }
            let mut restarted = Process::spawn(
                &arguments,
                &fixture
                    .environment
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect::<Vec<_>>(),
            )?;
            assert!(!restarted.wait()?.success());
            ensure!(
                restarted
                    .diagnostics()
                    .contains("dirty backend authority seal"),
                "backend seal bypass: {}",
                restarted.diagnostics()
            );
        }
    } else {
        clean_restart(&fixture)?;
    }
    Ok(())
}

#[test]
fn grant_reply_loss_is_unknown_with_durable_full_identity_not_new_id_retry() -> Result<()> {
    grant_reply_loss(false)
}

#[test]
#[ignore = "Main supplies PERI_P4_S3_TEST=1 and an independent S3_BUCKET; no bucket creation/deletion"]
fn actual_s3_grant_reply_loss_full_identity_gate() -> Result<()> {
    grant_reply_loss(true)
}

fn grant_reply_loss(real_s3: bool) -> Result<()> {
    let mut fixture = Fixture::new(real_s3)?;
    let (old, session) = fixture.baseline()?;
    let request = fixture.request(11, 10)?;
    let entered = fixture.rpc.arm(Target::Grant);
    thread::scope(|threads| -> Result<()> {
        let granting = threads.spawn(|| {
            fixture.control.call(AuthorityCommand::Grant {
                request: request.clone(),
            })
        });
        let held = entered.recv_timeout(Duration::from_secs(3))?;
        let receipt: RpcResponse = serde_json::from_slice(&held.response)?;
        assert!(receipt.ok);
        let committed: GrantReply = serde_json::from_value(receipt.result.unwrap())?;
        identity(
            &fixture.ledger("grant-catalog-committed-reply-held")?,
            &committed.binding,
        )?;
        assert!(fixture
            .worker_client
            .call(AuthorityCommand::ValidateLease {
                binding: old.binding.clone(),
                role: AccessRole::Writer
            })
            .is_err());
        held.release(false)?;
        let error = granting.join().unwrap().unwrap_err();
        ensure!(
            error.to_string().contains("outcome may be unknown"),
            "lost Grant receipt classification: {error}"
        );
        let new_client =
            AuthorityClient::new(&format!("{}/", fixture.rpc.endpoint), WORKER_B.into())?;
        let valid: GrantReply =
            serde_json::from_value(new_client.call(AuthorityCommand::ValidateLease {
                binding: committed.binding.clone(),
                role: AccessRole::Writer,
            })?)?;
        assert_eq!(valid.binding, committed.binding);
        for field in [
            "root_incarnation",
            "authority_term",
            "branch_incarnation",
            "epoch",
            "worker_id",
            "lease_id",
            "grant_id",
            "renewal_sequence",
        ] {
            let mut changed = serde_json::to_value(&committed.binding)?;
            if changed[field].is_string() {
                changed[field] = json!("different-identity");
            } else {
                changed[field] = json!(999);
            }
            assert!(
                new_client
                    .call(AuthorityCommand::ValidateLease {
                        binding: serde_json::from_value(changed)?,
                        role: AccessRole::Writer
                    })
                    .is_err(),
                "accepted mismatched {field}"
            );
        }
        fixture.replacement(&valid, &session, 1)?;
        Ok(())
    })?;
    fixture.shutdown(true)
}

#[test]
fn native_checkpoint_grant_fence_preserves_acknowledged_prefix() -> Result<()> {
    native_checkpoint_gate(false)
}

#[test]
fn delayed_backend_sql_setup_preserves_original_budget() -> Result<()> {
    delayed_backend_sql_setup(40)
}

#[test]
fn delayed_backend_setup_can_exceed_execution_budget_without_timeout() -> Result<()> {
    delayed_backend_sql_setup(60)
}

fn delayed_backend_sql_setup(delay_ms: u64) -> Result<()> {
    let mut fixture = Fixture::new(false)?;
    let lease: GrantReply =
        serde_json::from_value(fixture.control.call(AuthorityCommand::Grant {
            request: fixture.request(10, 0)?,
        })?)?;
    acquire(&fixture.first, &lease)?;
    let session = open(&fixture.first, &fixture.database)?;
    fixture.rpc.reset_statistics();
    fixture.backend.reset_statistics();
    fixture.backend.delay(delay_ms);
    let started = Instant::now();
    let executed = observed_worker(
        &fixture.first,
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)".into(),
        },
    );
    fixture.backend.delay(0);
    eprintln!(
        "P4_DELAYED_SQL execution_cpu_budget_ms=5000 wall_budget_ms=10000 delay_ms={delay_ms} elapsed_ms={} rpc={} physical={} cache={}",
        started.elapsed().as_millis(),
        fixture.rpc.statistics(),
        fixture.backend.statistics(),
        fixture.authority.diagnostics()
    );
    executed?;
    assert_eq!(
        fixture.first.worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT id FROM items".into()
            }
        )?,
        json!([[1]])
    );
    fixture
        .first
        .worker(false, WorkerCommand::CloseSession { session })?;
    fixture.shutdown(true)
}

fn append_request(fixture: &Fixture, epoch: u64, append_id: &str) -> WalAppendRequest {
    WalAppendRequest {
        namespace: fixture.database.namespace.clone(),
        epoch,
        batch: WalBatch::default(),
        position: None,
        append_id: append_id.into(),
        timestamp_millis: 1,
    }
}

fn append_store(fixture: &Fixture, lease: &GrantReply) -> Result<Arc<dyn ObjectStore>> {
    let provider = RemoteAuthorityProvider::new(fixture.worker_client.clone(), "verify-a".into())?;
    provider.install_lease(lease.clone(), AccessRole::Writer)?;
    provider.open_store(&fixture.database, lease.binding.epoch)
}

#[test]
fn append_wal_receipt_binds_complete_request_over_real_rpc() -> Result<()> {
    let mut fixture = Fixture::new(false)?;
    let lease: GrantReply =
        serde_json::from_value(fixture.control.call(AuthorityCommand::Grant {
            request: fixture.request(10, 0)?,
        })?)?;
    let store = append_store(&fixture, &lease)?;
    let request = append_request(&fixture, 10, "bound-receipt");
    assert_eq!(
        store.append_wal(&request).context("initial bound append")?,
        Some(1)
    );
    fixture.backend.reset_statistics();
    assert_eq!(
        store
            .append_wal(&request)
            .context("exact bound receipt replay")?,
        Some(1)
    );
    for field in ["payload", "position", "epoch", "timestamp", "namespace"] {
        let mut conflicting = request.clone();
        match field {
            "payload" => conflicting.batch.operations.push(WalOperation::Write {
                offset: 0,
                bytes: vec![42],
            }),
            "position" => conflicting.position = Some(Default::default()),
            "epoch" => conflicting.epoch = 9,
            "timestamp" => conflicting.timestamp_millis = 2,
            "namespace" => conflicting.namespace = Namespace::new("other", "database")?,
            _ => unreachable!(),
        }
        assert!(
            store.append_wal(&conflicting).is_err(),
            "accepted conflicting {field}"
        );
    }
    let physical = fixture.backend.statistics();
    assert!(physical["PUT/root"].is_null());
    assert!(physical["PUT/blob"].is_null());
    eprintln!("P4_APPEND_BOUND_RECEIPT conflicts=5 mutations=0 physical={physical}");
    store.retire_scope(Instant::now() + Duration::from_secs(5))?;
    drop(store);
    fixture.shutdown(true)
}

#[test]
fn append_wal_one_rpc_and_lost_receipt_never_falls_back_or_replays() -> Result<()> {
    for deliver in [true, false] {
        let mut fixture = Fixture::new(false)?;
        let lease: GrantReply =
            serde_json::from_value(fixture.control.call(AuthorityCommand::Grant {
                request: fixture.request(10, 0)?,
            })?)?;
        let store = append_store(&fixture, &lease)?;
        fixture.rpc.reset_statistics();
        fixture.backend.reset_statistics();
        let entered = fixture.rpc.arm(Target::AppendWal);
        thread::scope(|threads| -> Result<()> {
            let appended = threads.spawn(|| {
                RemoteWal::new(store.clone()).append_commit(
                    &fixture.database.namespace,
                    10,
                    WalBatch::default(),
                    None,
                )
            });
            let held = entered.recv_timeout(Duration::from_secs(5))?;
            let reply: RpcResponse = serde_json::from_slice(&held.response)?;
            assert!(reply.ok);
            assert_eq!(reply.result, Some(json!(1)));
            let ledger = fixture.ledger("append-backend-terminal-receipt-held")?;
            assert!(ledger.branches["main"].wal.is_some());
            assert!(ledger.uploads.is_empty());
            let physical = fixture.backend.statistics();
            held.release(deliver)?;
            let result = appended.join().unwrap();
            if deliver {
                assert_eq!(result?, 1);
            } else {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("outcome may be unknown"));
                assert!(store
                    .append_wal(&append_request(&fixture, 10, "no-replay"))
                    .is_err());
                assert_eq!(fixture.backend.statistics(), physical);
            }
            let rpc = fixture.rpc.statistics();
            assert_eq!(rpc["session/append_wal/other"]["started"], 1);
            assert_eq!(rpc["session/append_wal/other"]["backend_terminal"], 1);
            assert!(rpc["session/get/wal"].is_null());
            assert!(rpc["session/put/wal"].is_null());
            assert!(rpc["session/validate/other"].is_null());
            eprintln!("P4_APPEND_RECEIPT deliver={deliver} rpc={rpc} physical={physical} replay=false fallback=false");
            Ok(())
        })?;
        if deliver {
            store.retire_scope(Instant::now() + Duration::from_secs(5))?;
        }
        drop(store);
        fixture.shutdown(true)?;
    }
    Ok(())
}

#[test]
fn append_wal_blob_and_root_holds_block_scope_retirement_and_next_grant() -> Result<()> {
    // The append publishes the pair, its registrations and the head CAS as one group,
    // so a commit has two body writes and one root write. There is no reservation
    // revision to hold, which is what these phases pin down.
    for (target, ordinal, stage) in [
        (Target::Blob, 1, "chunk-blob"),
        (Target::Blob, 2, "index-blob"),
        (Target::Root, 1, "head-cas"),
    ] {
        let mut fixture = Fixture::new(false)?;
        let (lease, old_session) = fixture.baseline()?;
        let store = append_store(&fixture, &lease)?;
        let before = fixture.ledger("append-before-ordinal-hold")?;
        let request = fixture.request(11, 10)?;
        fixture.backend.reset_statistics();
        let entered = fixture.backend.arm_nth(target, ordinal);
        thread::scope(|threads| -> Result<()> {
            let appended = threads.spawn(|| {
                RemoteWal::new(store.clone()).append_commit(
                    &fixture.database.namespace,
                    10,
                    WalBatch::default(),
                    None,
                )
            });
            let held = entered.recv_timeout(Duration::from_secs(5))?;
            assert_eq!(held.ordinal, ordinal);
            let during = fixture.ledger("append-physical-response-held")?;
            identity(&during, &lease.binding)?;
            let physical = fixture.backend.statistics();
            let category = match target {
                Target::Blob => "PUT/blob",
                Target::Root => "PUT/root",
                _ => unreachable!(),
            };
            assert_eq!(physical[category]["backend_terminal"], ordinal);
            // An append reserves nothing in a revision that outlives it: the intent that
            // `prepare_blobs` demands is inserted and discharged inside the group.
            assert!(during.uploads.is_empty());
            if stage == "head-cas" {
                // The CAS that ends the group is the commit, and both bodies were written
                // before it, so the durable head already names them.
                assert_eq!(physical["PUT/blob"]["backend_terminal"], 2);
                assert_ne!(
                    during.branches["main"].wal.as_ref().unwrap().hash,
                    before.branches["main"].wal.as_ref().unwrap().hash
                );
            } else {
                // A held body is still in the group buffer and the group is written in
                // order, so nothing of this commit is durable and the CAS has not started.
                assert!(physical["PUT/root"].is_null());
                assert_eq!(
                    during.branches["main"].wal.as_ref().unwrap().hash,
                    before.branches["main"].wal.as_ref().unwrap().hash
                );
            }
            let rpc_before = fixture.rpc.statistics();
            let close_started_before = rpc_before["retire_session/none"]["started"]
                .as_u64()
                .unwrap_or(0);
            let grant_started_before = rpc_before["grant/none"]["started"].as_u64().unwrap_or(0);
            let (closed_sender, closed_receiver) = mpsc::channel();
            let closing_store = store.clone();
            let closing = threads.spawn(move || {
                let result = closing_store.retire_scope(Instant::now() + Duration::from_secs(5));
                closed_sender.send(result).unwrap();
            });
            let (grant_sender, grant_receiver) = mpsc::channel();
            let granting_control = fixture.control.clone();
            let granting = threads.spawn(move || {
                grant_sender
                    .send(granting_control.call(AuthorityCommand::Grant { request }))
                    .unwrap();
            });
            let dispatch_deadline = Instant::now() + Duration::from_secs(2);
            let dispatched = loop {
                let rpc = fixture.rpc.statistics();
                let close_started = rpc["retire_session/none"]["started"].as_u64().unwrap_or(0);
                let grant_started = rpc["grant/none"]["started"].as_u64().unwrap_or(0);
                if close_started > close_started_before && grant_started > grant_started_before {
                    assert_eq!(close_started, close_started_before + 1);
                    assert_eq!(grant_started, grant_started_before + 1);
                    break rpc;
                }
                ensure!(
                    Instant::now() < dispatch_deadline,
                    "RetireSession and Grant were not both dispatched while backend response remained held: {rpc}"
                );
                thread::sleep(Duration::from_millis(1));
            };
            assert!(closed_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err());
            assert!(grant_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err());
            held.release(true)?;
            let append_result = appended.join().unwrap();
            assert!(
                append_result.is_ok()
                    || append_result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.to_string().contains("retired or unsettled"))
            );
            closed_receiver.recv_timeout(Duration::from_secs(5))??;
            closing.join().unwrap();
            let next: GrantReply =
                serde_json::from_value(grant_receiver.recv_timeout(Duration::from_secs(5))??)?;
            granting.join().unwrap();
            assert!(store
                .append_wal(&append_request(&fixture, 10, "old-after-grant"))
                .is_err());
            fixture.replacement(&next, &old_session, 1)?;
            eprintln!("P4_APPEND_TERMINAL stage={stage} ordinal={ordinal} triggered=true physical={physical} dispatched={dispatched} close_proven=true next_grant=true old_rejected=true next_native_write=true");
            Ok(())
        })?;
        drop(store);
        fixture.shutdown(true)?;
    }
    Ok(())
}

#[test]
fn append_wal_forged_scope_identity_and_payload_dispatch_zero_mutations() -> Result<()> {
    let mut fixture = Fixture::new(false)?;
    let lease: GrantReply =
        serde_json::from_value(fixture.control.call(AuthorityCommand::Grant {
            request: fixture.request(10, 0)?,
        })?)?;
    let store = append_store(&fixture, &lease)?;
    let before = fixture.ledger("append-before-forgeries")?;
    fixture.backend.reset_statistics();
    for field in [
        "root_incarnation",
        "authority_term",
        "branch_incarnation",
        "epoch",
        "worker_id",
        "lease_id",
        "grant_id",
        "renewal_sequence",
        "role",
    ] {
        let mut forged = serde_json::to_value(&lease.binding)?;
        forged[field] = if field == "role" {
            json!("read_only")
        } else if forged[field].is_string() {
            json!("forged")
        } else {
            json!(999)
        };
        let decoded = serde_json::from_value(forged);
        if let Ok(binding) = decoded {
            assert!(
                fixture
                    .worker_client
                    .call(AuthorityCommand::OpenSession {
                        binding,
                        role: AccessRole::Writer
                    })
                    .is_err(),
                "accepted forged {field}"
            );
        } else {
            assert_eq!(field, "role");
        }
    }
    let readonly = fixture.worker_client.call(AuthorityCommand::OpenSession {
        binding: lease.binding.clone(),
        role: AccessRole::ReadOnly,
    })?["session"]
        .as_str()
        .unwrap()
        .to_owned();
    fixture.backend.reset_statistics();
    assert!(fixture
        .worker_client
        .call(AuthorityCommand::Session {
            session: readonly.clone(),
            operation: SessionOperation::AppendWal {
                request: append_request(&fixture, 10, "readonly")
            }
        })
        .is_err());
    for epoch in [9, 11] {
        assert!(store
            .append_wal(&append_request(&fixture, epoch, "wrong-epoch"))
            .is_err());
    }
    let counts = fixture.backend.statistics();
    assert!(counts["PUT/root"].is_null());
    assert!(counts["PUT/blob"].is_null());
    let after = fixture.ledger("append-after-forgeries")?;
    assert_eq!(after.branches["main"].wal, before.branches["main"].wal);
    fixture
        .worker_client
        .call(AuthorityCommand::CloseSession { session: readonly })?;
    store.retire_scope(Instant::now() + Duration::from_secs(5))?;
    drop(store);
    fixture.shutdown(true)
}

#[test]
#[ignore = "Main supplies PERI_P4_S3_TEST=1 and an independent S3_BUCKET; no bucket creation/deletion"]
fn actual_s3_native_checkpoint_acknowledged_prefix_gate() -> Result<()> {
    native_checkpoint_gate(true)
}

fn native_checkpoint_gate(real_s3: bool) -> Result<()> {
    let mut fixture = Fixture::new(real_s3)?;
    let (lease, session) = fixture.baseline()?;
    observed_worker(
        &fixture.first,
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "INSERT INTO items VALUES(2)".into(),
        },
    )?;
    let request = fixture.request(11, 10)?;
    let entered = fixture.backend.arm(Target::Blob);
    let address = fixture.first.address.clone();
    thread::scope(|threads| -> Result<()> {
        let checkpoint = threads.spawn(|| {
            worker_call(
                &address,
                WorkerCommand::Checkpoint {
                    session: session.clone(),
                },
            )
        });
        let held = entered.recv_timeout(Duration::from_secs(3))?;
        let key = held.path.trim_start_matches('/').split_once('/').unwrap().1;
        let object = fixture
            .raw
            .get(key)?
            .context("native checkpoint upload did not reach backend")?;
        eprintln!("P4_COMPLETION_IO phase=native-checkpoint-backend-terminal key={key} bytes={} checksum={}", object.bytes.len(), blake3::hash(&object.bytes).to_hex());
        identity(
            &fixture.ledger("native-checkpoint-backend-held")?,
            &lease.binding,
        )?;
        let (sent, received) = mpsc::channel();
        let client = fixture.control.clone();
        let next_request = request.clone();
        let granting = threads.spawn(move || {
            let _ = sent.send(client.call(AuthorityCommand::Grant {
                request: next_request,
            }));
        });
        transition_started(&fixture, &request)?;
        assert!(matches!(
            received.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        held.release(true)?;
        let error = checkpoint.join().unwrap().unwrap_err();
        ensure!(
            error.downcast_ref::<WorkerRefusal>().is_some(),
            "checkpoint transport failure is not a fencing receipt: {error:#}"
        );
        eprintln!("P4_CHECKPOINT_GRANT_FENCE checkpoint_response_delivered=true backend_response_delivered=true receipt={error:#}");
        let next: GrantReply =
            serde_json::from_value(received.recv_timeout(Duration::from_secs(3))??)?;
        granting.join().unwrap();
        fixture.replacement(&next, &session, 2)?;
        Ok(())
    })?;
    fixture.shutdown(true)
}

fn worker_call(address: &str, command: WorkerCommand) -> Result<Value> {
    let request = WorkerRequest {
        version: 1,
        request_id: "completion-worker-checkpoint".into(),
        command,
    };
    let response: WorkerResponse = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(8))
        .build()?
        .post(format!("http://{address}/sql"))
        .bearer_auth(WORKER_SQL)
        .json(&request)
        .send()?
        .json()?;
    ensure!(
        response.version == 1 && response.request_id == request.request_id,
        "worker response identity mismatch"
    );
    if !response.ok {
        return Err(WorkerRefusal {
            message: response.error.context("worker refusal message missing")?,
            execution_completed: response.execution_completed,
        }
        .into());
    }
    response.result.context("worker result missing")
}

#[derive(Debug)]
struct WorkerRefusal {
    message: String,
    execution_completed: Option<bool>,
}

impl std::fmt::Display for WorkerRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "worker refused: {}; execution_completed={:?}",
            self.message, self.execution_completed
        )
    }
}

impl std::error::Error for WorkerRefusal {}

#[test]
fn grant_drains_backend_not_delivery_of_worker_rpc_receipt() -> Result<()> {
    let mut fixture = Fixture::new(false)?;
    let (lease, native_session) = fixture.baseline()?;
    let scope = fixture.raw_session(&lease)?;
    let request = fixture.request(11, 10)?;
    let entered = fixture.rpc.arm(Target::Put);
    thread::scope(|threads| -> Result<()> {
        let writing = threads.spawn(|| {
            put(
                &fixture.worker_client,
                &fixture.database,
                &scope,
                vec![73; 4096],
            )
        });
        let held = entered.recv_timeout(Duration::from_secs(3))?;
        let receipt: RpcResponse = serde_json::from_slice(&held.response)?;
        assert!(receipt.ok);
        identity(&fixture.ledger("worker-rpc-receipt-held")?, &lease.binding)?;
        let proof = wire_call(
            &fixture.rpc.endpoint,
            WORKER_A,
            AuthorityCommand::RetireSession {
                session: scope.clone(),
            },
        )?;
        assert_eq!(proof["drained"], true);
        let next: GrantReply =
            serde_json::from_value(known_grant(&fixture.rpc.endpoint, request)?)?;
        assert!(!writing.is_finished());
        assert!(put(
            &fixture.worker_client,
            &fixture.database,
            &scope,
            vec![99; 32]
        )
        .is_err());
        fixture.replacement(&next, &native_session, 1)?;
        held.release(true)?;
        writing.join().unwrap()?;
        Ok(())
    })?;
    fixture.shutdown(true)
}

fn wire_call(endpoint: &str, token: &str, command: AuthorityCommand) -> Result<Value> {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let request_id = match &command {
        AuthorityCommand::Grant { request } => request.request_id.clone(),
        _ => format!(
            "completion-wire-{}",
            SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ),
    };
    let request = RpcRequest {
        version: 1,
        request_id: request_id.clone(),
        command,
    };
    let response: RpcResponse = reqwest::blocking::Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .timeout(Duration::from_secs(10))
        .build()?
        .post(format!("{endpoint}/"))
        .bearer_auth(token)
        .json(&request)
        .send()?
        .json()?;
    ensure!(
        response.version == 1 && response.request_id == request_id,
        "wire response identity mismatch"
    );
    ensure!(response.ok, "{}", response.error.unwrap_or_default());
    response.result.context("wire result missing")
}

fn known_grant(endpoint: &str, request: GrantRequest) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = wire_call(
            endpoint,
            AUTHORITY_CONTROL,
            AuthorityCommand::Grant {
                request: request.clone(),
            },
        );
        match result {
            Err(error)
                if error.to_string().starts_with("Retryable admission")
                    && Instant::now() < deadline =>
            {
                thread::yield_now()
            }
            result => return result,
        }
    }
}

fn clean_restart(fixture: &Fixture) -> Result<()> {
    let state = fixture.directory.path().join("known-terminal-restart");
    let mut authority = Process::start(
        &[
            "authority".into(),
            "s3".into(),
            "127.0.0.1:0".into(),
            state.to_string_lossy().into_owned(),
        ],
        &fixture
            .environment
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>(),
    )?;
    let endpoint = format!("http://{}/", authority.address);
    let control = AuthorityClient::new(&endpoint, AUTHORITY_CONTROL.into())?;
    let intent = GrantIntent {
        database: fixture.database.clone(),
        worker_id: "verify-b".into(),
        epoch: 12,
        expected_epoch: 11,
        lease_id: "completion-recovered-lease-12".into(),
        ttl_ms: 60000,
        role: AccessRole::Writer,
    };
    let nonce = control.call(AuthorityCommand::Challenge {
        intent: LeaseIntent::Grant(intent.clone()),
    })?;
    let lease: GrantReply = serde_json::from_value(control.call(AuthorityCommand::Grant {
        request: GrantRequest {
            request_id: "completion-recovered-grant-12".into(),
            nonce: nonce["nonce"].as_str().unwrap().into(),
            intent,
        },
    })?)?;
    assert_eq!(lease.binding.authority_term, 2);
    identity(
        &fixture.ledger("known-terminal-restart-new-grant")?,
        &lease.binding,
    )?;
    let mut recovered = worker(
        &fixture.directory,
        &endpoint,
        "verify-b",
        WORKER_B,
        fixture
            .environment
            .iter()
            .any(|(name, _)| name == "PERI_TEST_PROCESS_LOG_DIR"),
    )?;
    acquire(&recovered, &lease)?;
    let session = open(&recovered, &fixture.database)?;
    assert_eq!(
        recovered.worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT id FROM items ORDER BY id".into()
            }
        )?,
        json!([[1], [3]])
    );
    recovered.worker(
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "INSERT INTO items VALUES(4)".into(),
        },
    )?;
    assert_eq!(
        recovered.worker(
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT count(*) FROM items".into()
            }
        )?,
        json!([[3]])
    );
    recovered.worker(false, WorkerCommand::CloseSession { session })?;
    recovered.worker(true, WorkerCommand::Shutdown)?;
    assert!(recovered.wait()?.success());
    let shutdown = control.call(AuthorityCommand::Shutdown);
    eprintln!("P4_COMPLETION_RECOVERY_SHUTDOWN outcome={shutdown:?}");
    assert!(authority.wait()?.success());
    assert_eq!(fs::read(state.join("authority.lock"))?, b"clean\n");
    Ok(())
}

fn isolated_namespace(fixture: &Fixture) -> Result<()> {
    let mut database = fixture.database.clone();
    database.namespace.database.push_str("-unaffected");
    let intent = GrantIntent {
        database: database.clone(),
        worker_id: "verify-b".into(),
        epoch: 10,
        expected_epoch: 0,
        lease_id: "unaffected-lease".into(),
        ttl_ms: 60000,
        role: AccessRole::Writer,
    };
    let nonce = fixture.control.call(AuthorityCommand::Challenge {
        intent: LeaseIntent::Grant(intent.clone()),
    })?;
    let lease: GrantReply =
        serde_json::from_value(fixture.control.call(AuthorityCommand::Grant {
            request: GrantRequest {
                request_id: "unaffected-grant".into(),
                nonce: nonce["nonce"].as_str().unwrap().into(),
                intent,
            },
        })?)?;
    acquire(&fixture.second, &lease)?;
    let session = open(&fixture.second, &database)?;
    observed_worker(
        &fixture.second,
        false,
        WorkerCommand::Execute {
            session: session.clone(),
            sql: "CREATE TABLE healthy(id INTEGER); INSERT INTO healthy VALUES(7)".into(),
        },
    )?;
    assert_eq!(
        observed_worker(
            &fixture.second,
            false,
            WorkerCommand::Query {
                session: session.clone(),
                sql: "SELECT id FROM healthy".into()
            }
        )?,
        json!([[7]])
    );
    observed_worker(
        &fixture.second,
        false,
        WorkerCommand::CloseSession { session },
    )?;
    eprintln!(
        "P4_COMPLETION_NAMESPACE_ISOLATION unaffected={}",
        database.namespace.prefix()
    );
    Ok(())
}

#[test]
fn live_backend_seal_rejects_second_state_directory_and_reconciled_takeover() -> Result<()> {
    let mut fixture = Fixture::new(false)?;
    let state = fixture.directory.path().join("unsupported-takeover");
    let mut takeover = Process::spawn(
        &[
            "authority".into(),
            "s3".into(),
            "127.0.0.1:0".into(),
            state.to_string_lossy().into_owned(),
            "--reconciled".into(),
        ],
        &fixture
            .environment
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>(),
    )?;
    assert!(!takeover.wait()?.success());
    ensure!(
        takeover
            .diagnostics()
            .contains("dirty backend authority seal"),
        "second authority admitted: {}",
        takeover.diagnostics()
    );
    fixture.baseline()?;
    fixture.shutdown(true)
}

#[test]
fn explicit_remote_retirement_receipt_loss_is_sticky_without_drop_retry() -> Result<()> {
    let mut fixture = Fixture::new(false)?;
    let (lease, session) = fixture.baseline()?;
    let provider = RemoteAuthorityProvider::new(fixture.worker_client.clone(), "verify-a".into())?;
    provider.install_lease(lease.clone(), AccessRole::Writer)?;
    let store = provider.open_store(&fixture.database, 10)?;
    let entered = fixture.rpc.arm(Target::Close);
    thread::scope(|threads| -> Result<()> {
        let retiring =
            threads.spawn(|| store.retire_scope(Instant::now() + Duration::from_secs(3)));
        let held = entered.recv_timeout(Duration::from_secs(2))?;
        let reply: RpcResponse = serde_json::from_slice(&held.response)?;
        assert!(reply.ok);
        let proof = reply.result.unwrap();
        assert_eq!(proof["drained"], true);
        assert_eq!(
            serde_json::from_value::<LeaseBinding>(proof["binding"].clone())?,
            lease.binding
        );
        held.release(false)?;
        assert!(retiring.join().unwrap().is_err());
        let unexpected = fixture.rpc.arm(Target::Close);
        let error = store
            .retire_scope(Instant::now() + Duration::from_secs(1))
            .unwrap_err();
        ensure!(
            error.to_string().contains("refusing retry"),
            "lost receipt was silently recovered: {error}"
        );
        assert!(store.validate_session().is_err());
        assert!(matches!(
            unexpected.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        Ok(())
    })?;
    drop(store);
    fixture.rpc.disarm();
    let next: GrantReply =
        serde_json::from_value(fixture.control.call(AuthorityCommand::Grant {
            request: fixture.request(11, 10)?,
        })?)?;
    fixture.replacement(&next, &session, 1)?;
    fixture.shutdown(true)
}
