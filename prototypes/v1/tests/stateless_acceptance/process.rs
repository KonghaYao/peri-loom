use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

pub struct Worker {
    child: Child,
    input: Option<mpsc::SyncSender<Vec<u8>>>,
    output: mpsc::Receiver<Result<Value>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    threads: Vec<JoinHandle<()>>,
    sequence: u64,
    terminated: bool,
}

impl Worker {
    pub fn spawn(store: &Path, cache: &Path) -> Result<Self> {
        Self::spawn_binary(Path::new(env!("CARGO_BIN_EXE_peri-loom-v1")), store, cache)
    }

    /// Spawn a specific worker binary. The upgrade tests use this to drive the previous
    /// release against the same store, so a durable-format change is verified against the
    /// binary it actually breaks instead of a copied whitelist.
    pub fn spawn_binary(binary: &Path, store: &Path, cache: &Path) -> Result<Self> {
        let mut child = Command::new(binary)
            .arg("worker")
            .arg(store)
            .arg(cache)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawn standalone P3 worker")?;
        let mut stdin = child.stdin.take().context("worker stdin missing")?;
        let stdout = child.stdout.take().context("worker stdout missing")?;
        let mut errors = child.stderr.take().context("worker stderr missing")?;
        let (input, requests) = mpsc::sync_channel::<Vec<u8>>(1);
        let (responses, output) = mpsc::channel();
        let write_errors = responses.clone();
        let writer = thread::spawn(move || {
            for bytes in requests {
                if let Err(error) = stdin.write_all(&bytes).and_then(|_| stdin.flush()) {
                    let _ = write_errors.send(Err(error.into()));
                    break;
                }
            }
        });
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let response = line.map_err(anyhow::Error::from).and_then(|line| {
                    serde_json::from_str(&line).context("invalid worker JSON response")
                });
                if responses.send(response).is_err() {
                    return;
                }
            }
            let _ = responses.send(Err(anyhow::anyhow!("worker stdout reached EOF")));
        });
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let capture = stderr.clone();
        let logger = thread::spawn(move || {
            let mut buffer = [0; 4096];
            while let Ok(count) = errors.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                let mut captured = capture.lock().unwrap();
                captured.extend_from_slice(&buffer[..count]);
                let discard = captured.len().saturating_sub(64 * 1024);
                captured.drain(..discard);
            }
        });
        Ok(Self {
            child,
            input: Some(input),
            output,
            stderr,
            threads: vec![writer, reader, logger],
            sequence: 0,
            terminated: false,
        })
    }

    pub fn diagnostics(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }

    pub fn request(&mut self, request: Value) -> Result<Value> {
        let identifier = self.send(request)?;
        self.receive(&identifier)
    }

    /// Send a request without waiting for its response. Process-level concurrency tests use
    /// this to put two workers into the same store operation before either reads an answer.
    pub fn send(&mut self, mut request: Value) -> Result<String> {
        self.sequence += 1;
        let identifier = format!("request-{}", self.sequence);
        request["id"] = json!(identifier);
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        self.input
            .as_ref()
            .context("worker stdin is closed")?
            .send(bytes)
            .context("worker input writer stopped")?;
        Ok(identifier)
    }

    pub fn receive(&mut self, identifier: &str) -> Result<Value> {
        let response = self
            .output
            .recv_timeout(REQUEST_TIMEOUT)
            .with_context(|| format!("worker response timeout/EOF: {}", self.diagnostics()))?
            .with_context(|| format!("worker response failed: {}", self.diagnostics()))?;
        ensure!(
            response["id"] == identifier,
            "worker response ID mismatch: {response}"
        );
        Ok(response)
    }

    pub fn success(&mut self, request: Value) -> Result<Value> {
        let response = self.request(request)?;
        ensure!(
            response["ok"] == true,
            "worker request rejected: {response}"
        );
        Ok(response)
    }

    pub fn rejected(&mut self, request: Value) -> Result<Value> {
        let response = self.request(request)?;
        ensure!(
            response["ok"] == false,
            "request unexpectedly succeeded: {response}"
        );
        ensure!(
            response["error"]
                .as_str()
                .is_some_and(|message| !message.is_empty()),
            "missing worker error detail: {response}"
        );
        Ok(response)
    }

    pub fn open(&mut self, database: &Value) -> Result<String> {
        let response = self.success(json!({"op":"open", "database":database, "epoch":0}))?;
        Ok(response["session"]
            .as_str()
            .context("missing session ID")?
            .to_owned())
    }

    pub fn exec(&mut self, session: &str, sql: &str) -> Result<()> {
        self.success(json!({"op":"exec", "session":session, "sql":sql}))?;
        Ok(())
    }

    pub fn query(&mut self, session: &str, sql: &str) -> Result<Value> {
        Ok(self.success(json!({"op":"query", "session":session, "sql":sql}))?["result"].clone())
    }

    pub fn operation(&mut self, session: &str, operation: &str) -> Result<()> {
        let response = self.success(json!({"op":operation, "session":session}))?;
        ensure!(
            response["result"]["ok"] == true,
            "missing operation acknowledgment: {response}"
        );
        Ok(())
    }

    pub fn close(&mut self, session: &str) -> Result<()> {
        self.success(json!({"op":"close", "session":session}))?;
        Ok(())
    }

    pub fn status(&mut self) -> Result<Value> {
        Ok(self.success(json!({"op":"status"}))?["result"].clone())
    }

    pub fn finish(&mut self) -> Result<()> {
        self.input.take();
        let deadline = Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(status) = self.child.try_wait()? {
                break status;
            }
            ensure!(
                Instant::now() < deadline,
                "worker did not exit after stdin EOF: {}",
                self.diagnostics()
            );
            thread::sleep(Duration::from_millis(10));
        };
        self.terminated = true;
        self.join_threads();
        ensure!(
            status.success(),
            "worker EOF exit failed: {status}: {}",
            self.diagnostics()
        );
        Ok(())
    }

    pub fn kill(&mut self) -> Result<ExitStatus> {
        self.child.kill()?;
        let status = self.child.wait()?;
        self.terminated = true;
        self.input.take();
        self.join_threads();
        Ok(status)
    }

    fn join_threads(&mut self) {
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.input.take();
        if !self.terminated {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.join_threads();
    }
}
