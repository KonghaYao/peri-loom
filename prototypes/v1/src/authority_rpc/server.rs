use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use base64::Engine;
use serde_json::{json, Value};

use crate::authority::ownership::OwnershipAuthority;
use crate::object_store::{ObjectStore, PreconditionFailed};
use crate::ownership::types::{validate_label, AccessRole, LeaseBinding};
use crate::runtime::DatabaseId;

use super::auth::{AuthConfig, Principal};
use super::http;
use super::ingress::{Acceptor, Quota, Wakeup};
use super::protocol::{
    AuthorityCommand, RpcRequest, RpcResponse, SessionOperation, PROTOCOL_VERSION,
};
use super::startup::AuthorityStartup;

#[cfg(test)]
#[path = "batch_tests.rs"]
mod batch_tests;
#[cfg(test)]
#[path = "import_segment_tests.rs"]
mod import_segment_tests;

/// Requests the authority dispatches at once. This is the execution budget: it
/// gates work, which is why a parked connection must not hold one.
const MAX_INFLIGHT: usize = 128;

/// Connections the listener holds at once, whether they are mid-request or
/// parked. Kept well above the socket budget of a single worker
/// (`authority_rpc::client`) so reuse reduces connection churn without turning
/// an idle pool into a refusal to serve.
pub(crate) const MAX_CONNECTIONS: usize = 256;

pub struct Server {
    acceptor: Acceptor,
    state: Arc<State>,
    startup: AuthorityStartup,
    seal: super::seal::Seal,
}

struct State {
    authority: Arc<OwnershipAuthority>,
    auth: AuthConfig,
    sessions: Mutex<BTreeMap<String, Arc<Session>>>,
    authorized: Mutex<BTreeSet<(DatabaseId, String)>>,
    stopping: AtomicBool,
    connections: Arc<Quota>,
    inflight: Arc<Quota>,
    cleanup: Mutex<Vec<thread::JoinHandle<()>>>,
    wakeup: Wakeup,
}

struct Session {
    worker_id: String,
    binding: LeaseBinding,
    store: Arc<dyn ObjectStore>,
    scope: super::scope::Scope,
}

impl Server {
    pub fn bind(
        address: SocketAddr,
        authority: Arc<OwnershipAuthority>,
        auth: AuthConfig,
        startup: AuthorityStartup,
    ) -> Result<Self> {
        ensure!(
            address.ip().is_loopback(),
            "P4 authority listener must be loopback-only"
        );
        let acceptor = Acceptor::bind(address)?;
        let acceptor_wakeup = acceptor.wakeup();
        let seal = super::seal::Seal::acquire(authority.backend())?;
        Ok(Self {
            acceptor,
            startup,
            seal,
            state: Arc::new(State {
                authority,
                auth,
                sessions: Mutex::new(BTreeMap::new()),
                authorized: Mutex::new(BTreeSet::new()),
                stopping: AtomicBool::new(false),
                connections: Quota::new(MAX_CONNECTIONS),
                inflight: Quota::new(MAX_INFLIGHT),
                cleanup: Mutex::new(Vec::new()),
                wakeup: acceptor_wakeup,
            }),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.acceptor
            .local_addr()
            .expect("bound authority listener")
    }

    pub fn run(mut self) -> Result<()> {
        let mut workers = Vec::new();
        let result = (|| {
            while !self.state.stopping.load(Ordering::SeqCst) {
                workers.retain(|worker: &thread::JoinHandle<()>| !worker.is_finished());
                self.acceptor.park()?;
                while !self.state.stopping.load(Ordering::SeqCst) {
                    let Some(mut connection) = self.acceptor.take()? else {
                        break;
                    };
                    // The connection budget is separate from `inflight`: a
                    // pooled connection is not work, so it must not spend the
                    // execution budget that gates dispatch.
                    let Some(connection_ticket) = self.state.connections.try_acquire() else {
                        let _ = http::reply(
                            &mut connection,
                            503,
                            b"{\"error\":\"authority connection limit reached\"}",
                        );
                        continue;
                    };
                    let state = self.state.clone();
                    workers.push(thread::Builder::new().spawn(move || {
                        let _ticket = connection_ticket;
                        let _ = state.serve(&mut connection);
                    })?);
                }
            }
            Ok(())
        })();
        self.state.stopping.store(true, Ordering::SeqCst);
        self.state.wakeup.wake();
        for worker in workers {
            let _ = worker.join();
        }
        for cleanup in std::mem::take(
            &mut *self
                .state
                .cleanup
                .lock()
                .map_err(|_| anyhow::anyhow!("scope cleanup poisoned"))?,
        ) {
            let _ = cleanup.join();
        }
        let sessions = std::mem::take(
            &mut *self
                .state
                .sessions
                .lock()
                .map_err(|_| anyhow::anyhow!("session registry poisoned"))?,
        );
        drop(sessions);
        if result.is_ok() && self.state.authority.quiescent() {
            self.seal.mark_clean()?;
            self.startup.mark_clean()?;
        } else {
            return Err(result.err().unwrap_or_else(|| {
                anyhow::anyhow!("authority has unsettled backend operations; startup remains dirty")
            }));
        }
        Ok(())
    }
}

impl State {
    /// Serve one pooled connection until the peer closes it, it goes idle, or a
    /// request must not be replayed on the same socket.
    ///
    /// Requests are strictly serialized: the next request is only read after the
    /// current response has been flushed, so a lost response can never be
    /// followed by an implicitly replayed one on the same connection.
    ///
    /// The execution budget is taken per request, not per connection: a socket
    /// parked between requests is not work and must not hold a dispatch slot.
    fn serve(&self, stream: &mut TcpStream) -> Result<()> {
        let mut first = true;
        loop {
            if self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
            let request = match http::read_request_optional(
                stream,
                if first {
                    None
                } else {
                    Some(http::KEEP_ALIVE_IDLE)
                },
                &self.stopping,
            ) {
                Ok(Some(request)) => request,
                Ok(None) => return Ok(()),
                Err(error) => {
                    eprintln!("authority HTTP parse failure: {error}");
                    let _ = http::reply(stream, 400, b"{\"error\":\"invalid HTTP request\"}");
                    return Ok(());
                }
            };
            first = false;
            let keep_alive = request.keep_alive && !self.stopping.load(Ordering::SeqCst);
            let Some(_execution_ticket) = self.inflight.try_acquire() else {
                let _ = http::reply(stream, 503, b"{\"error\":\"authority saturated\"}");
                return Ok(());
            };
            if !self.handle(stream, request, keep_alive)? {
                return Ok(());
            }
        }
    }

    /// Returns whether the connection may serve another request.
    fn handle(
        &self,
        stream: &mut TcpStream,
        request: http::Request,
        keep_alive: bool,
    ) -> Result<bool> {
        if request.path != "/" {
            let _ = http::reply(stream, 400, b"{\"error\":\"invalid authority path\"}");
            return Ok(false);
        }
        let principal = match self.auth.authenticate(&request.authorization) {
            Ok(principal) => principal,
            Err(_) => {
                let _ = http::reply(stream, 403, b"{\"error\":\"Unauthorized\"}");
                return Ok(false);
            }
        };
        let parsed = match serde_json::from_slice::<RpcRequest>(&request.body) {
            Ok(parsed) => parsed,
            Err(error) => {
                eprintln!(
                    "authority JSON parse failure category={:?} line={} column={}",
                    error.classify(),
                    error.line(),
                    error.column()
                );
                let _ = http::reply(stream, 400, b"{\"error\":\"invalid authority request\"}");
                return Ok(false);
            }
        };
        let identifier = parsed.request_id.clone();
        let result = (|| {
            ensure!(
                parsed.version == PROTOCOL_VERSION,
                "unsupported authority protocol version"
            );
            validate_label(&parsed.request_id)?;
            ensure!(!self.stopping.load(Ordering::SeqCst), "authority stopping");
            let _reads = crate::authority::request_reads::Scope::for_rpc(&identifier);
            self.dispatch(parsed.command, &principal, &identifier)
        })();
        let response = match result {
            Ok(result) => RpcResponse {
                version: PROTOCOL_VERSION,
                request_id: identifier,
                ok: true,
                result: Some(result),
                error: None,
                precondition_failed: false,
            },
            Err(error) => RpcResponse {
                version: PROTOCOL_VERSION,
                request_id: identifier,
                ok: false,
                result: None,
                error: Some(format!("{error:#}")),
                precondition_failed: error.is::<PreconditionFailed>(),
            },
        };
        let result = http::reply_with(stream, 200, &serde_json::to_vec(&response)?, keep_alive);
        // Reuse only after a fully written response; a partial write may have
        // left the peer without the outcome, so the socket is retired.
        Ok(result.is_ok() && keep_alive)
    }

    fn control(principal: &Principal) -> Result<()> {
        ensure!(
            *principal == Principal::Control,
            "Unauthorized control operation"
        );
        Ok(())
    }

    fn worker<'principal>(
        principal: &'principal Principal,
        binding: &LeaseBinding,
    ) -> Result<&'principal str> {
        let Principal::Worker(worker) = principal else {
            anyhow::bail!("Unauthorized worker operation");
        };
        ensure!(worker == &binding.worker_id, "Unauthorized worker binding");
        Ok(worker)
    }

    fn allowed(&self, database: &DatabaseId, worker: &str) -> Result<()> {
        ensure!(
            self.authorized
                .lock()
                .map_err(|_| anyhow::anyhow!("authorization registry poisoned"))?
                .contains(&(database.clone(), worker.to_owned())),
            "Unauthorized database; no grant issued to worker in this authority term"
        );
        Ok(())
    }

    fn dispatch(
        &self,
        command: AuthorityCommand,
        principal: &Principal,
        identifier: &str,
    ) -> Result<Value> {
        match command {
            AuthorityCommand::GetMetrics => {
                Self::control(principal)?;
                let mut result =
                    serde_json::to_value(crate::metrics::Metrics::global().snapshot())?;
                result["capacity_trace"] = crate::capacity_trace::snapshot();
                Ok(result)
            }
            AuthorityCommand::GetCapacityTrace {
                stream_id,
                after_sequence,
                max_events,
            } => {
                Self::control(principal)?;
                crate::capacity_trace::page(&stream_id, after_sequence, max_events)
            }
            AuthorityCommand::Challenge { intent } => {
                Self::control(principal)?;
                Ok(serde_json::to_value(self.authority.challenge(intent)?)?)
            }
            AuthorityCommand::Grant { request } => {
                Self::control(principal)?;
                ensure!(
                    request.request_id == identifier,
                    "command identity mismatch"
                );
                let grant = self.authority.grant(request)?;
                self.authorized
                    .lock()
                    .map_err(|_| anyhow::anyhow!("authorization registry poisoned"))?
                    .insert((
                        grant.binding.database.clone(),
                        grant.binding.worker_id.clone(),
                    ));
                Ok(serde_json::to_value(grant)?)
            }
            AuthorityCommand::Renew { request } => {
                Self::control(principal)?;
                ensure!(
                    request.request_id == identifier,
                    "command identity mismatch"
                );
                Ok(serde_json::to_value(self.authority.renew(request)?)?)
            }
            AuthorityCommand::Revoke { request } => {
                Self::control(principal)?;
                ensure!(
                    request.request_id == identifier,
                    "command identity mismatch"
                );
                Ok(serde_json::to_value(self.authority.revoke(request)?)?)
            }
            AuthorityCommand::Maintenance {
                request_id,
                database,
                operation,
            } => {
                Self::control(principal)?;
                ensure!(request_id == identifier, "command identity mismatch");
                self.authority
                    .maintenance(&database, &request_id, operation)
            }
            AuthorityCommand::ImportSegments {
                database,
                expected_head,
                segments,
            } => {
                Self::control(principal)?;
                ensure!(
                    !segments.is_empty()
                        && segments.len() <= super::protocol::MAX_IMPORT_SEGMENT_BATCH,
                    "invalid import segment batch"
                );
                ensure!(
                    crate::manifest::valid_hash(&expected_head),
                    "invalid expected HEAD"
                );
                let encoded_size = crate::manifest::SEGMENT_SIZE.div_ceil(3) * 4;
                let mut seen = BTreeSet::new();
                let mut decoded = Vec::with_capacity(segments.len());
                for segment in segments {
                    ensure!(
                        crate::manifest::valid_hash(&segment.hash)
                            && seen.insert(segment.hash.clone())
                            && segment.data_base64.len() == encoded_size,
                        "invalid import segment"
                    );
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(&segment.data_base64)
                        .context("invalid import segment encoding")?;
                    crate::manifest::verify_segment(&segment.hash, &bytes)?;
                    decoded.push(bytes);
                }
                Ok(serde_json::to_value(self.authority.import_segments(
                    &database,
                    &expected_head,
                    &decoded,
                )?)?)
            }
            AuthorityCommand::GetDatabaseState { database } => {
                if let Principal::Worker(worker) = principal {
                    self.allowed(&database, worker)?;
                }
                Ok(serde_json::to_value(self.authority.state(&database)?)?)
            }
            AuthorityCommand::ValidateLease { binding, role } => {
                let worker = Self::worker(principal, &binding)?;
                self.allowed(&binding.database, worker)?;
                Ok(serde_json::to_value(
                    self.authority.validate_lease(&binding, role)?,
                )?)
            }
            AuthorityCommand::OpenSession { binding, role } => {
                let worker = Self::worker(principal, &binding)?;
                self.allowed(&binding.database, worker)?;
                let store = self.authority.open_session(&binding, role)?;
                let mut random = [0u8; 32];
                std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
                let identifier = random
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                let session = Arc::new(Session {
                    worker_id: worker.to_owned(),
                    binding,
                    store,
                    scope: super::scope::Scope::default(),
                });
                let mut sessions = self
                    .sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
                ensure!(sessions.len() < 4096, "authority session capacity exceeded");
                let std::collections::btree_map::Entry::Vacant(entry) =
                    sessions.entry(identifier.clone())
                else {
                    anyhow::bail!("session identity collision");
                };
                entry.insert(session);
                Ok(json!({"session": identifier}))
            }
            AuthorityCommand::CloseSession { session } => {
                let Principal::Worker(worker) = principal else {
                    anyhow::bail!("Unauthorized worker operation");
                };
                let selected = {
                    let sessions = self
                        .sessions
                        .lock()
                        .map_err(|_| anyhow::anyhow!("session registry poisoned"))?;
                    ensure!(
                        sessions
                            .get(&session)
                            .is_some_and(|session| &session.worker_id == worker),
                        "Unauthorized session"
                    );
                    sessions.get(&session).cloned().unwrap()
                };
                let deadline = Instant::now() + Duration::from_secs(5);
                if let Err(error) = selected.scope.retire(deadline) {
                    self.authority
                        .quarantine_database(&selected.binding.database)?;
                    return Err(error);
                }
                let removed = self
                    .sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session registry poisoned"))?
                    .remove(&session);
                let proof = json!({"closed": true, "drained": true, "binding": selected.binding});
                let (completed, completion) = std::sync::mpsc::channel();
                let mut cleanup = self
                    .cleanup
                    .lock()
                    .map_err(|_| anyhow::anyhow!("scope cleanup poisoned"))?;
                cleanup.retain(|task| !task.is_finished());
                cleanup.push(thread::Builder::new().spawn(move || {
                    drop(removed);
                    drop(selected);
                    let _ = completed.send(());
                })?);
                drop(cleanup);
                if completion
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    .is_err()
                {
                    let binding: LeaseBinding = serde_json::from_value(proof["binding"].clone())?;
                    self.authority.quarantine_database(&binding.database)?;
                    anyhow::bail!("scope retirement deadline; namespace quarantined");
                }
                Ok(proof)
            }
            AuthorityCommand::Session { session, operation } => {
                let Principal::Worker(worker) = principal else {
                    anyhow::bail!("Unauthorized worker operation");
                };
                let selected = self
                    .sessions
                    .lock()
                    .map_err(|_| anyhow::anyhow!("session registry poisoned"))?
                    .get(&session)
                    .cloned()
                    .context("unknown authority session")?;
                ensure!(&selected.worker_id == worker, "Unauthorized session");
                let _dispatch = selected.scope.dispatch()?;
                match operation {
                    SessionOperation::UploadSegments { segments } => {
                        super::protocol::validate_segment_batch(&segments)?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer,
                            "ReadOnly"
                        );
                        Ok(serde_json::to_value(
                            selected.store.upload_segments(&segments)?,
                        )?)
                    }
                    SessionOperation::RegisterSegments { entries } => {
                        super::protocol::validate_registration_batch(&entries)?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer,
                            "ReadOnly"
                        );
                        selected.store.register_segments(&entries)?;
                        Ok(Value::Null)
                    }
                    SessionOperation::AppendWal { request } => {
                        request.validate()?;
                        ensure!(
                            selected.store.access_role() == AccessRole::Writer
                                && request.namespace == selected.binding.database.namespace
                                && request.epoch == selected.binding.epoch,
                            "Fenced WAL append identity"
                        );
                        Ok(json!(selected
                            .store
                            .append_wal(&request)?
                            .context("authority WAL append capability unavailable")?))
                    }
                    SessionOperation::Validate => Ok(serde_json::to_value(
                        self.authority
                            .validate_lease(&selected.binding, selected.store.access_role())?,
                    )?),
                    SessionOperation::Get { key } => {
                        Ok(serde_json::to_value(selected.store.get(&key)?)?)
                    }
                    SessionOperation::Put {
                        key,
                        bytes,
                        condition,
                    } => Ok(json!(selected.store.put(&key, &bytes, condition)?)),
                    SessionOperation::Delete { key, expected } => {
                        selected.store.delete(&key, expected.as_deref())?;
                        Ok(Value::Null)
                    }
                    SessionOperation::List { prefix } => Ok(json!(selected.store.list(&prefix)?)),
                }
            }
            AuthorityCommand::Shutdown => {
                Self::control(principal)?;
                self.stopping.store(true, Ordering::SeqCst);
                // The acceptor is parked on `poll`, so it has to be told; waiting
                // out the supervision period would delay every clean shutdown.
                self.wakeup.wake();
                ensure!(
                    self.authority.quiescent(),
                    "authority has unsettled operations; shutdown cannot acknowledge a clean fence"
                );
                Ok(json!({"stopping": true}))
            }
        }
    }
}
