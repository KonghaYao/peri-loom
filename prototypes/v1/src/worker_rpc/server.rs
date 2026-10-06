use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Result};
use serde_json::{json, Value};

use crate::authority_rpc::http;
use crate::metrics::{Metrics, Operation};
use crate::ownership::types::{validate_label, OwnershipCommand, OwnershipMode};
use crate::ownership::OwnershipRuntime;
use crate::session::Budget;

use super::protocol::{WorkerCommand, WorkerRequest, WorkerResponse};
use super::sessions::{rows, Sessions};

#[derive(Clone, Debug)]
pub struct WorkerLimits {
    pub max_inflight: usize,
    pub control_reserve: usize,
    pub max_sessions: usize,
    pub max_request_bytes: usize,
}

impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            max_inflight: 128,
            control_reserve: 16,
            max_sessions: 128,
            max_request_bytes: 512 * 1024,
        }
    }
}

pub struct WorkerServer {
    listener: TcpListener,
    state: Arc<State>,
}

struct State {
    runtime: Arc<OwnershipRuntime>,
    sessions: Sessions,
    control_token: String,
    sql_token: String,
    stopped: AtomicBool,
    inflight: AtomicUsize,
    sql_inflight: AtomicUsize,
    limits: WorkerLimits,
}

struct Ticket(Arc<State>);
impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

struct SqlTicket<'a>(&'a AtomicUsize);
impl Drop for SqlTicket<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl WorkerServer {
    pub fn bind(
        address: SocketAddr,
        runtime: Arc<OwnershipRuntime>,
        control_token: String,
        sql_token: String,
    ) -> Result<Self> {
        Self::bind_with_limits(
            address,
            runtime,
            control_token,
            sql_token,
            WorkerLimits::default(),
        )
    }

    pub fn bind_with_limits(
        address: SocketAddr,
        runtime: Arc<OwnershipRuntime>,
        control_token: String,
        sql_token: String,
        limits: WorkerLimits,
    ) -> Result<Self> {
        ensure!(
            limits.max_inflight > limits.control_reserve && limits.control_reserve > 0,
            "worker requires a positive reserved control capacity"
        );
        ensure!(
            limits.max_sessions > 0 && limits.max_request_bytes > 0,
            "worker request/session limits must be positive"
        );
        ensure!(
            limits.max_request_bytes <= crate::authority_rpc::protocol::MAX_RPC_BYTES,
            "worker payload limit exceeds transport limit"
        );
        ensure!(
            address.ip().is_loopback(),
            "P4 worker listener must be loopback-only"
        );
        ensure!(
            control_token.len() >= 32 && sql_token.len() >= 32 && control_token != sql_token,
            "worker requires distinct nonempty control and SQL credentials of at least 32 bytes"
        );
        let listener = TcpListener::bind(address)?;
        crate::session::Budget::set_default(crate::session::Budget::default())?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            state: Arc::new(State {
                sessions: Sessions::new(runtime.clone(), limits.max_sessions),
                runtime,
                control_token,
                sql_token,
                stopped: AtomicBool::new(false),
                inflight: AtomicUsize::new(0),
                sql_inflight: AtomicUsize::new(0),
                limits,
            }),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    pub fn run(self) -> Result<()> {
        let watchdog_state = self.state.clone();
        let watchdog = thread::spawn(move || {
            while !watchdog_state.stopped.load(Ordering::SeqCst) {
                if let Ok(status) = watchdog_state.runtime.status() {
                    for database in status.databases {
                        if matches!(
                            database.mode,
                            OwnershipMode::Revoked | OwnershipMode::Detached
                        ) {
                            let _ = watchdog_state.sessions.clear_database(&database.database);
                        }
                    }
                }
                thread::sleep(Duration::from_millis(100));
            }
        });
        let mut workers = Vec::new();
        let result = (|| {
            while !self.state.stopped.load(Ordering::SeqCst) {
                workers.retain(|worker: &thread::JoinHandle<()>| !worker.is_finished());
                let (mut connection, _) = match self.listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                if self.state.inflight.load(Ordering::SeqCst) >= self.state.limits.max_inflight {
                    let _ = http::reply(&mut connection, 429, b"{\"error\":\"worker saturated\"}");
                    continue;
                }
                self.state.inflight.fetch_add(1, Ordering::SeqCst);
                let state = self.state.clone();
                let ticket = Ticket(state.clone());
                match thread::Builder::new().spawn(move || {
                    let _ticket = ticket;
                    let _ = state.serve(&mut connection);
                }) {
                    Ok(worker) => workers.push(worker),
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        })();
        self.state.stopped.store(true, Ordering::SeqCst);
        drop(self.listener);
        for worker in workers {
            let _ = worker.join();
        }
        let _ = watchdog.join();
        self.state.sessions.clear()?;
        result
    }
}

impl State {
    fn serve(&self, stream: &mut TcpStream) -> Result<()> {
        let request = match read_request(stream, self.limits.max_request_bytes) {
            Ok(request) => request,
            Err(_) => return http::reply(stream, 400, b"{\"error\":\"invalid HTTP request\"}"),
        };
        let supplied = request.authorization.strip_prefix("Bearer ").unwrap_or("");
        let control = request.path == "/control" && token_matches(supplied, &self.control_token);
        let sql = request.path == "/sql" && token_matches(supplied, &self.sql_token);
        if !control && !sql {
            return http::reply(stream, 403, b"{\"error\":\"unauthorized\"}");
        }
        let _sql_ticket = if sql {
            let capacity = self.limits.max_inflight - self.limits.control_reserve;
            if self
                .sql_inflight
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    (current < capacity).then_some(current + 1)
                })
                .is_err()
            {
                return http::reply(stream, 429, b"{\"error\":\"SQL lane saturated\"}");
            }
            Some(SqlTicket(&self.sql_inflight))
        } else {
            None
        };
        let parsed = match serde_json::from_slice::<WorkerRequest>(&request.body) {
            Ok(parsed) => parsed,
            Err(_) => return http::reply(stream, 400, b"{\"error\":\"invalid worker request\"}"),
        };
        let identifier = parsed.request_id.clone();
        let result = (|| {
            ensure!(parsed.version == 1, "unsupported worker protocol version");
            validate_label(&parsed.request_id)?;
            ensure!(!self.stopped.load(Ordering::SeqCst), "worker stopping");
            self.dispatch(parsed.command, control)
        })();
        let response = match result {
            Ok(result) => WorkerResponse {
                version: 1,
                request_id: identifier,
                ok: true,
                result: Some(result),
                error: None,
                execution_completed: None,
            },
            Err(error) => WorkerResponse {
                version: 1,
                request_id: identifier,
                ok: false,
                result: None,
                error: Some(format!("{error:#}")),
                execution_completed: (!control).then_some(true),
            },
        };
        http::reply(stream, 200, &serde_json::to_vec(&response)?)
    }

    fn dispatch(&self, command: WorkerCommand, control: bool) -> Result<Value> {
        match command {
            WorkerCommand::GetMetrics => {
                ensure!(control, "control authorization required");
                Ok(json!({
                    "metrics": Metrics::global().snapshot(),
                    "resources": self.runtime.status()?.resources,
                    "http_inflight": self.inflight.load(Ordering::SeqCst),
                    "sql_inflight": self.sql_inflight.load(Ordering::SeqCst),
                    "sessions": self.sessions.len()?,
                }))
            }
            WorkerCommand::Ownership { action } => {
                ensure!(control, "control authorization required");
                let revoked = match &action {
                    OwnershipCommand::RevokeDatabase { database, .. } => Some(database.clone()),
                    _ => None,
                };
                let result = self.runtime.command(action)?;
                if let Some(identifier) = revoked {
                    self.sessions.clear_database(&identifier)?;
                }
                Ok(result)
            }
            WorkerCommand::Shutdown => {
                ensure!(control, "control authorization required");
                self.stopped.store(true, Ordering::SeqCst);
                Ok(json!({"stopping": true}))
            }
            _ if control => bail!("SQL authorization required"),
            WorkerCommand::OpenSession { database } => self.sessions.open(&database),
            WorkerCommand::CloseSession { session } => {
                self.sessions.close(&session)?;
                Ok(json!({"closed": true}))
            }
            WorkerCommand::Execute { session, sql } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                self.sessions.handle(&session)?.execute(&sql)?;
                Ok(json!({"executed": true}))
            }
            WorkerCommand::Query { session, sql } => {
                ensure!(
                    sql.len() <= Budget::trusted_default().max_sql_bytes,
                    "SQL byte budget exceeded"
                );
                Ok(rows(self.sessions.handle(&session)?.query(&sql)?))
            }
            WorkerCommand::Checkpoint { session } => {
                Metrics::global().observe(
                    Operation::Checkpoint,
                    || self.sessions.handle(&session)?.checkpoint(),
                    |_| 0,
                )?;
                Ok(json!({"checkpointed": true}))
            }
            WorkerCommand::Compact { session } => {
                self.sessions.handle(&session)?.compact_wal()?;
                Ok(json!({"compacted": true}))
            }
            WorkerCommand::Diagnostics { session } => self.sessions.handle(&session)?.diagnostics(),
        }
    }
}

fn token_matches(supplied: &str, expected: &str) -> bool {
    let supplied = blake3::hash(supplied.as_bytes());
    let expected = blake3::hash(expected.as_bytes());
    supplied
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn read_request(stream: &mut TcpStream, max_body_bytes: usize) -> Result<http::Request> {
    stream.set_nonblocking(false)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream);
    let mut headers = BTreeMap::new();
    let mut consumed = 0;
    let first = header_line(&mut reader, &mut consumed, deadline)?;
    let parts = first.split(' ').collect::<Vec<_>>();
    ensure!(
        parts.len() == 3
            && parts[0] == "POST"
            && parts[2] == "HTTP/1.1"
            && parts[1].starts_with('/')
            && parts[1].bytes().all(|byte| byte.is_ascii_graphic()),
        "invalid HTTP request line"
    );
    let path = parts[1].to_owned();
    loop {
        let line = header_line(&mut reader, &mut consumed, deadline)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid HTTP header"))?;
        ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid HTTP header name"
        );
        ensure!(
            headers
                .insert(name.to_ascii_lowercase(), value.trim().to_owned())
                .is_none(),
            "duplicate HTTP header"
        );
    }
    ensure!(
        !headers.contains_key("transfer-encoding"),
        "chunked requests unsupported"
    );
    ensure!(
        headers.get("host").is_some_and(|host| !host.is_empty()),
        "HTTP Host required"
    );
    let length: usize = headers
        .get("content-length")
        .ok_or_else(|| anyhow::anyhow!("HTTP Content-Length required"))?
        .parse()?;
    ensure!(
        length > 0 && length <= max_body_bytes,
        "worker HTTP body budget exceeded"
    );
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("HTTP request deadline elapsed"))?;
    reader.get_mut().set_read_timeout(Some(remaining))?;
    let mut body = Vec::with_capacity(length);
    reader.take(length as u64).read_to_end(&mut body)?;
    ensure!(
        body.len() == length && Instant::now() <= deadline,
        "incomplete or expired HTTP body"
    );
    Ok(http::Request {
        path,
        authorization: headers.remove("authorization").unwrap_or_default(),
        body,
    })
}

fn header_line(
    reader: &mut BufReader<&mut TcpStream>,
    consumed: &mut usize,
    deadline: Instant,
) -> Result<String> {
    const MAX_HEADERS: usize = 32 * 1024;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("HTTP header deadline elapsed"))?;
    reader.get_mut().set_read_timeout(Some(remaining))?;
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take((MAX_HEADERS - *consumed + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    *consumed += bytes.len();
    ensure!(
        *consumed <= MAX_HEADERS && bytes.ends_with(b"\r\n"),
        "invalid or oversized HTTP headers"
    );
    bytes.truncate(bytes.len() - 2);
    let text = String::from_utf8(bytes)?;
    ensure!(
        text.bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ' || byte == b'\t'),
        "invalid HTTP header bytes"
    );
    Ok(text)
}
