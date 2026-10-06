use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, ensure, Result};
use serde_json::{json, Value};

use crate::authority_rpc::http;
use crate::ownership::types::{validate_label, OwnershipCommand, OwnershipMode};
use crate::ownership::OwnershipRuntime;

use super::protocol::{WorkerCommand, WorkerRequest, WorkerResponse};
use super::sessions::{rows, Sessions};

const MAX_INFLIGHT: usize = 128;

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
}

struct Ticket(Arc<State>);
impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl WorkerServer {
    pub fn bind(
        address: SocketAddr,
        runtime: Arc<OwnershipRuntime>,
        control_token: String,
        sql_token: String,
    ) -> Result<Self> {
        ensure!(
            address.ip().is_loopback(),
            "P4 worker listener must be loopback-only"
        );
        ensure!(
            control_token.len() >= 32 && sql_token.len() >= 32 && control_token != sql_token,
            "worker requires distinct nonempty control and SQL credentials of at least 32 bytes"
        );
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            listener,
            state: Arc::new(State {
                sessions: Sessions::new(runtime.clone()),
                runtime,
                control_token,
                sql_token,
                stopped: AtomicBool::new(false),
                inflight: AtomicUsize::new(0),
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
                if self.state.inflight.load(Ordering::SeqCst) >= MAX_INFLIGHT {
                    let _ = http::reply(&mut connection, 503, b"{\"error\":\"worker saturated\"}");
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
        let request = match http::read_request(stream) {
            Ok(request) => request,
            Err(_) => return http::reply(stream, 400, b"{\"error\":\"invalid HTTP request\"}"),
        };
        let supplied = request.authorization.strip_prefix("Bearer ").unwrap_or("");
        let control = request.path == "/control" && token_matches(supplied, &self.control_token);
        let sql = request.path == "/sql" && token_matches(supplied, &self.sql_token);
        if !control && !sql {
            return http::reply(stream, 403, b"{\"error\":\"unauthorized\"}");
        }
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
            },
            Err(error) => WorkerResponse {
                version: 1,
                request_id: identifier,
                ok: false,
                result: None,
                error: Some(format!("{error:#}")),
            },
        };
        http::reply(stream, 200, &serde_json::to_vec(&response)?)
    }

    fn dispatch(&self, command: WorkerCommand, control: bool) -> Result<Value> {
        match command {
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
                self.sessions.handle(&session)?.execute(&sql)?;
                Ok(json!({"executed": true}))
            }
            WorkerCommand::Query { session, sql } => {
                Ok(rows(self.sessions.handle(&session)?.query(&sql)?))
            }
            WorkerCommand::Checkpoint { session } => {
                self.sessions.handle(&session)?.checkpoint()?;
                Ok(json!({"checkpointed": true}))
            }
            WorkerCommand::Compact { session } => {
                self.sessions.handle(&session)?.compact_wal()?;
                Ok(json!({"compacted": true}))
            }
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
