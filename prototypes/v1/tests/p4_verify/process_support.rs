use anyhow::{ensure, Context, Result};
use peri_loom_v1::{
    authority_rpc::{client::AuthorityClient, protocol::AuthorityCommand},
    ownership::types::{
        AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseIntent, NonceReply,
    },
    runtime::DatabaseId,
    worker_rpc::protocol::{WorkerCommand, WorkerRequest, WorkerResponse},
};
use serde_json::{json, Value};
use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

pub const AUTHORITY_CONTROL: &str = "fixture-authority-control-token-not-a-secret";
pub const WORKER_A: &str = "fixture-worker-a-authority-token-not-a-secret";
pub const WORKER_B: &str = "fixture-worker-b-authority-token-not-a-secret";
pub const WORKER_CONTROL: &str = "fixture-worker-control-token-not-a-secret";
pub const WORKER_SQL: &str = "fixture-worker-sql-only-token-not-a-secret";

pub struct Process {
    child: Child,
    ready: mpsc::Receiver<std::io::Result<String>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    logger: Option<thread::JoinHandle<()>>,
    evidence: Option<PathBuf>,
    pub address: String,
}

impl Process {
    pub fn spawn(arguments: &[String], environment: &[(&str, &str)]) -> Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_peri-loom-v1"));
        command
            .env_clear()
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in environment {
            command.env(key, value);
        }
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            command.env("PERI_NATIVE_GATE_TRACE", "1");
        }
        let mut child = command.spawn()?;
        static LOG_SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let evidence = environment
            .iter()
            .find(|(name, _)| *name == "PERI_TEST_PROCESS_LOG_DIR")
            .map(|(_, value)| {
                Path::new(value).join(format!(
                    "process-{}-{}.stderr.log",
                    child.id(),
                    LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                ))
            });
        let output = child.stdout.take().context("service stdout missing")?;
        let errors = child.stderr.take().context("service stderr missing")?;
        let (sender, ready) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let captured = stderr.clone();
        let logger = thread::spawn(move || {
            let mut errors = errors;
            let mut buffer = [0; 4096];
            while let Ok(length) = errors.read(&mut buffer) {
                if length == 0 {
                    break;
                }
                let mut captured = captured.lock().unwrap();
                captured.extend_from_slice(&buffer[..length]);
                let excess = captured.len().saturating_sub(64 * 1024);
                captured.drain(..excess);
            }
        });
        Ok(Self {
            child,
            ready,
            stderr,
            logger: Some(logger),
            evidence,
            address: String::new(),
        })
    }

    pub fn start(arguments: &[String], environment: &[(&str, &str)]) -> Result<Self> {
        let mut process = Self::spawn(arguments, environment)?;
        let line = process
            .ready
            .recv_timeout(Duration::from_secs(8))
            .with_context(|| {
                format!(
                    "service startup timeout/disconnect: {}",
                    process.diagnostics()
                )
            })??;
        let ready: Value = serde_json::from_str(&line)?;
        ensure!(
            ready["ready"] == true,
            "service did not report ready: {ready}"
        );
        process.address = ready["address"]
            .as_str()
            .context("ready address missing")?
            .into();
        Ok(process)
    }

    pub fn diagnostics(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }

    pub fn wait(&mut self) -> Result<ExitStatus> {
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait()? {
                if let Some(logger) = self.logger.take() {
                    logger.join().unwrap();
                }
                return Ok(status);
            }
            ensure!(
                start.elapsed() < Duration::from_secs(8),
                "service exit timeout: {}",
                self.diagnostics()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn kill(&mut self) -> Result<ExitStatus> {
        self.child.kill()?;
        self.wait()
    }

    pub fn worker(&self, control: bool, command: WorkerCommand) -> Result<Value> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let request_id = format!("verify-{}", SEQUENCE.fetch_add(1, Ordering::SeqCst));
        let request = WorkerRequest {
            version: 1,
            request_id: request_id.clone(),
            command,
        };
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()?;
        let response = client
            .post(format!(
                "http://{}/{}",
                self.address,
                if control { "control" } else { "sql" }
            ))
            .bearer_auth(if control { WORKER_CONTROL } else { WORKER_SQL })
            .json(&request)
            .send()?;
        ensure!(response.status().is_success(), "worker HTTP refusal");
        let response: WorkerResponse = response.json()?;
        ensure!(
            response.version == 1 && response.request_id == request_id,
            "worker response identity mismatch"
        );
        ensure!(
            response.ok,
            "worker rejected {:?}: {:?}; child stderr: {}",
            request.command,
            response.error,
            self.diagnostics()
        );
        response.result.context("worker result missing")
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(logger) = self.logger.take() {
            let _ = logger.join();
        }
        if let Some(path) = &self.evidence {
            let result = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .and_then(|mut file| file.write_all(self.stderr.lock().unwrap().as_slice()));
            if let Err(error) = result {
                eprintln!(
                    "could not retain P4 S3 child stderr at {}: {error}",
                    path.display()
                );
            }
        }
    }
}

pub struct Cluster {
    pub authority: Process,
    pub control: Arc<AuthorityClient>,
    pub credential_path: String,
    trace: Option<super::trace::Trace>,
    pub temporary: tempfile::TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.authority.child.kill();
        let status = self.authority.wait();
        let diagnostics = self.authority.diagnostics();
        if !diagnostics.is_empty() {
            eprintln!("P4_AUTHORITY_DIAGNOSTICS exit={status:?} stderr={diagnostics}");
        }
    }
}

impl Cluster {
    pub fn new() -> Result<Self> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("workers.json");
        fs::write(
            &path,
            serde_json::to_vec(&json!({"verify-a":WORKER_A,"verify-b":WORKER_B}))?,
        )?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        let credential_path = path.to_string_lossy().into_owned();
        let authority = Process::start(
            &authority_arguments(temporary.path()),
            &[
                ("PERI_AUTHORITY_CONTROL_TOKEN", AUTHORITY_CONTROL),
                ("PERI_AUTHORITY_WORKERS_FILE", &credential_path),
            ],
        )?;
        let trace = if std::env::var("PERI_P4_VERIFY_TRACE").as_deref() == Ok("1") {
            Some(super::trace::Trace::start(authority.address.clone())?)
        } else {
            None
        };
        let endpoint = trace
            .as_ref()
            .map_or(authority.address.as_str(), |trace| trace.address.as_str());
        let control =
            AuthorityClient::new(&format!("http://{endpoint}/"), AUTHORITY_CONTROL.into())?;
        Ok(Self {
            temporary,
            authority,
            control,
            credential_path,
            trace,
        })
    }

    pub fn worker(&self, identifier: &str, token: &str) -> Result<Process> {
        let endpoint = self
            .trace
            .as_ref()
            .map_or(self.authority.address.as_str(), |trace| {
                trace.address.as_str()
            });
        let arguments = vec![
            "owned-worker".into(),
            format!("http://{endpoint}/"),
            self.temporary
                .path()
                .join(identifier)
                .to_string_lossy()
                .into_owned(),
            identifier.into(),
            "127.0.0.1:0".into(),
        ];
        Process::start(
            &arguments,
            &[
                ("PERI_WORKER_AUTHORITY_TOKEN", token),
                ("PERI_WORKER_CONTROL_TOKEN", WORKER_CONTROL),
                ("PERI_WORKER_SQL_TOKEN", WORKER_SQL),
            ],
        )
    }

    pub fn grant(
        &self,
        id: DatabaseId,
        worker: &str,
        epoch: u64,
        previous: u64,
        ttl_ms: u64,
    ) -> Result<GrantReply> {
        self.grant_role(id, worker, epoch, previous, ttl_ms, AccessRole::Writer)
    }

    pub fn grant_role(
        &self,
        id: DatabaseId,
        worker: &str,
        epoch: u64,
        previous: u64,
        ttl_ms: u64,
        role: AccessRole,
    ) -> Result<GrantReply> {
        let intent = GrantIntent {
            database: id,
            worker_id: worker.into(),
            epoch,
            expected_epoch: previous,
            lease_id: format!("process-lease-{epoch}"),
            ttl_ms,
            role,
        };
        let nonce: NonceReply =
            serde_json::from_value(self.control.call(AuthorityCommand::Challenge {
                intent: LeaseIntent::Grant(intent.clone()),
            })?)?;
        let request = GrantRequest {
            request_id: format!("process-grant-{epoch}"),
            nonce: nonce.nonce,
            intent,
        };
        Ok(serde_json::from_value(
            self.control.call(AuthorityCommand::Grant { request })?,
        )?)
    }
}

pub fn authority_arguments(root: &Path) -> Vec<String> {
    vec![
        "authority".into(),
        root.join("objects").to_string_lossy().into_owned(),
        "127.0.0.1:0".into(),
        root.join("state").to_string_lossy().into_owned(),
    ]
}
