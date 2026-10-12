use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{ensure, Result};

use crate::authority_rpc::http;
use crate::ownership::types::{validate_label, OwnershipMode};
use crate::ownership::OwnershipRuntime;
use crate::session::Budget;

use super::admission::{Admission, Lane};
use super::diagnostics::Diagnostics;
use super::outcome::failure_response;
use super::protocol::{WorkerCommand, WorkerRequest, WorkerResponse};
use super::sessions::Sessions;
use super::transport::{read_request, token_matches};

mod shutdown;

use shutdown::WorkerThreads;

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

pub(super) struct State {
    pub runtime: Arc<OwnershipRuntime>,
    pub sessions: Sessions,
    control_token: String,
    sql_token: String,
    pub stopped: AtomicBool,
    pub inflight: AtomicUsize,
    pub admission: Arc<Admission>,
    pub diagnostics: Diagnostics,
    pub limits: WorkerLimits,
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
        Self::bind_with_limits_and_writer_profile(
            address,
            runtime,
            control_token,
            sql_token,
            limits,
            false,
        )
    }

    pub fn bind_with_limits_and_writer_profile(
        address: SocketAddr,
        runtime: Arc<OwnershipRuntime>,
        control_token: String,
        sql_token: String,
        limits: WorkerLimits,
        enabled: bool,
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
        let admission = Admission::new(limits.max_inflight, limits.control_reserve, enabled)?;
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
                admission,
                diagnostics: Diagnostics::default(),
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
            let mut failure = None;
            while !watchdog_state.stopped.load(Ordering::SeqCst) {
                match watchdog_state.runtime.status() {
                    Ok(status) => {
                        for database in status.databases {
                            if matches!(
                                database.mode,
                                OwnershipMode::Revoked | OwnershipMode::Detached
                            ) {
                                if let Err(error) =
                                    watchdog_state.sessions.clear_database(&database.database)
                                {
                                    failure.get_or_insert(error);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
                thread::sleep(Duration::from_millis(100));
            }
            match failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        });
        let mut workers = WorkerThreads::default();
        let result = (|| {
            while !self.state.stopped.load(Ordering::SeqCst) {
                if workers.reap_finished() || watchdog.is_finished() {
                    break;
                }
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
        workers.finish(watchdog, result, || self.state.sessions.clear())
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
        let parsed = match serde_json::from_slice::<WorkerRequest>(&request.body) {
            Ok(parsed) => parsed,
            Err(_) => return http::reply(stream, 400, b"{\"error\":\"invalid worker request\"}"),
        };
        let version = parsed.version;
        let identifier = parsed.request_id.clone();
        let write_cursor_request = matches!(
            &parsed.command,
            WorkerCommand::OpenWriteCursor { .. } | WorkerCommand::FetchWriteCursor { .. }
        );
        let stateful = matches!(
            &parsed.command,
            WorkerCommand::OpenSession { .. }
                | WorkerCommand::Execute { .. }
                | WorkerCommand::Query { .. }
                | WorkerCommand::Describe { .. }
                | WorkerCommand::Statement { .. }
                | WorkerCommand::OpenReadCursor { .. }
                | WorkerCommand::FetchReadCursor { .. }
                | WorkerCommand::CloseReadCursor { .. }
                | WorkerCommand::OpenWriteCursor { .. }
                | WorkerCommand::FetchWriteCursor { .. }
                | WorkerCommand::CloseWriteCursor { .. }
                | WorkerCommand::CloseSession { .. }
        );
        if version == 2
            && (control
                || !self.admission.enabled()
                || !stateful && !matches!(&parsed.command, WorkerCommand::WriterProfileStatus))
        {
            return http::reply(stream, 400, b"{\"error\":\"writer profile unavailable\"}");
        }
        let mut call = None;
        let prepared = (|| {
            ensure!(
                version == 1 || version == 2,
                "unsupported worker protocol version"
            );
            validate_label(&identifier)?;
            ensure!(!self.stopped.load(Ordering::SeqCst), "worker stopping");
            let administrative = matches!(
                &parsed.command,
                WorkerCommand::Ownership { .. }
                    | WorkerCommand::GetMetrics
                    | WorkerCommand::GetCapacityTrace { .. }
                    | WorkerCommand::Shutdown
            );
            ensure!(
                administrative == control,
                "worker command authorization required"
            );
            ensure!(
                version == 2 || !matches!(&parsed.command, WorkerCommand::WriterProfileStatus),
                "writer profile requires protocol v2"
            );
            match &parsed.command {
                WorkerCommand::Execute { session, sql }
                | WorkerCommand::Query { session, sql }
                | WorkerCommand::Describe { session, sql }
                | WorkerCommand::Statement { session, sql, .. }
                | WorkerCommand::OpenReadCursor { session, sql, .. }
                | WorkerCommand::OpenWriteCursor { session, sql, .. } => {
                    ensure!(
                        sql.len() <= Budget::trusted_default().max_sql_bytes,
                        "SQL byte budget exceeded"
                    );
                    call = Some(self.sessions.begin(session)?);
                }
                WorkerCommand::Checkpoint { session }
                | WorkerCommand::Compact { session }
                | WorkerCommand::Diagnostics { session }
                | WorkerCommand::FetchReadCursor { session, .. }
                | WorkerCommand::CloseReadCursor { session }
                | WorkerCommand::FetchWriteCursor { session, .. }
                | WorkerCommand::CloseWriteCursor { session } => {
                    call = Some(self.sessions.begin(session)?);
                }
                _ => {}
            }
            Ok(())
        })();
        let mut terminal = None;
        let lane = if control {
            Lane::Control
        } else if prepared.is_ok()
            && self.admission.enabled()
            && matches!(&parsed.command, WorkerCommand::CloseSession { session } if self.sessions.known(session))
        {
            Lane::Cleanup
        } else if prepared.is_ok() && version == 2 && call.as_ref().is_some_and(|call| call.owner) {
            Lane::Owner
        } else {
            Lane::General
        };
        let lane_ticket = self.admission.try_acquire(lane)?;
        if lane_ticket.is_none() {
            return http::reply(
                stream,
                429,
                b"{\"error\":\"worker execution lane saturated\"}",
            );
        }
        let trace_guard = if crate::capacity_trace::enabled() {
            call.as_ref()
                .and_then(|call| call.handle().lease_binding_snapshot().ok())
                .map(|binding| {
                    let reserved_lane = match lane {
                        Lane::Owner => Some("owner_sql"),
                        Lane::Cleanup => Some("cleanup_sql"),
                        _ => None,
                    };
                    let cleanup = if reserved_lane.is_some() {
                        match &parsed.command {
                            WorkerCommand::Execute { sql, .. }
                            | WorkerCommand::Statement { sql, .. } => {
                                let normalized = sql.trim().trim_end_matches(';').trim();
                                if ["COMMIT", "COMMIT TRANSACTION", "END", "END TRANSACTION"]
                                    .iter()
                                    .any(|statement| normalized.eq_ignore_ascii_case(statement))
                                {
                                    Some("COMMIT")
                                } else if ["ROLLBACK", "ROLLBACK TRANSACTION"]
                                    .iter()
                                    .any(|statement| normalized.eq_ignore_ascii_case(statement))
                                {
                                    Some("ROLLBACK")
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    crate::capacity_trace::enter(
                        binding.database,
                        binding.epoch,
                        identifier.clone(),
                        cleanup,
                        reserved_lane,
                    )
                })
        } else {
            None
        };
        let result = prepared.and_then(|()| {
            self.dispatch(
                parsed.command,
                control,
                &identifier,
                version,
                call.as_ref(),
                &mut terminal,
            )
        });
        let result = if version == 2 && stateful && terminal.is_none() && result.is_ok() {
            Err(anyhow::anyhow!("worker session terminal state unavailable"))
        } else {
            result
        };
        if trace_guard.is_some() {
            if let Some(context) = crate::capacity_trace::context() {
                if let (Some(trace), Some(operation)) = (context.ring, context.cleanup) {
                    trace.record(crate::capacity_trace::Event {
                        kind: "cleanup_finish",
                        operation: Some(operation),
                        reserved_lane: context.reserved_lane,
                        control_id: context.control_id,
                        request_id: Some(identifier.clone()),
                        success: Some(
                            result.is_ok()
                                && terminal.as_ref().is_some_and(|terminal| !terminal.owner),
                        ),
                        terminal_sequence: terminal.as_ref().map(|terminal| terminal.sequence),
                        ..Default::default()
                    });
                }
            }
        }
        drop(trace_guard);
        let response = match result {
            Ok(result) => WorkerResponse {
                version,
                request_id: identifier,
                ok: true,
                result: Some(result),
                error: None,
                execution_completed: None,
                session_state: terminal,
            },
            Err(error) => failure_response(version, identifier, control, error, terminal),
        };
        let bytes = serde_json::to_vec(&response)?;
        let result = http::reply(stream, 200, &bytes);
        if result.is_err() && write_cursor_request && response.ok {
            if let Some(call) = call.as_ref() {
                if response
                    .result
                    .as_ref()
                    .is_some_and(|result| result["done"] == true)
                    || call.handle().close_write_cursor().is_err()
                {
                    let _ = call.handle().quarantine_unknown_write_result();
                }
            }
        }
        drop(call);
        drop(lane_ticket);
        result
    }
}
