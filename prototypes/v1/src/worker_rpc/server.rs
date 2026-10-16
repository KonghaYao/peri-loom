use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};

use crate::authority_rpc::http;
use crate::authority_rpc::ingress::{Acceptor, Quota, Wakeup};
use crate::ownership::types::{validate_label, OwnershipMode};
use crate::ownership::OwnershipRuntime;
use crate::session::{AdmissionError, Budget};

use super::admission::{Admission, Lane};
use super::diagnostics::Diagnostics;
use super::outcome::failure_response;
use super::protocol::{WorkerCommand, WorkerRequest, WorkerResponse};
use super::sessions::Sessions;
use super::transport::{self, read_request, token_matches};

mod shutdown;

use shutdown::WorkerThreads;

/// Connections the listener holds at once, whether they are mid-request or
/// parked between requests. Deliberately not `max_inflight`: once a connection
/// can be reused, a parked one is not work, and letting it spend the execution
/// budget is what would let idle clients starve real requests.
const MAX_CONNECTIONS: usize = 256;

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
    acceptor: Acceptor,
    state: Arc<State>,
}

pub(super) struct State {
    pub runtime: Arc<OwnershipRuntime>,
    pub sessions: Sessions,
    control_token: String,
    sql_token: String,
    pub stopped: AtomicBool,
    /// Execution budget, taken per request. `dispatch` reports it as
    /// `http_inflight`, which now counts work rather than parked sockets.
    pub inflight: Arc<Quota>,
    pub connections: Arc<Quota>,
    pub admission: Arc<Admission>,
    pub diagnostics: Diagnostics,
    pub limits: WorkerLimits,
    wakeup: Wakeup,
    pub(super) shutdown_budget: crate::shutdown_budget::ShutdownBudget,
}

impl State {
    /// Stops the worker and releases the parked acceptor so shutdown does not
    /// wait out the supervision period.
    pub(super) fn stop(&self) {
        self.shutdown_budget.start();
        self.stopped.store(true, Ordering::SeqCst);
        self.wakeup.wake();
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
        let acceptor = Acceptor::bind(address)?;
        let acceptor_wakeup = acceptor.wakeup();
        crate::session::Budget::set_default(crate::session::Budget::default())?;
        // A connection cap below the request cap would make the request cap
        // unreachable, so an oversized `PERI_WORKER_MAX_INFLIGHT` lifts it.
        let connections = MAX_CONNECTIONS.max(limits.max_inflight);
        Ok(Self {
            acceptor,
            state: Arc::new(State {
                sessions: Sessions::new(runtime.clone(), limits.max_sessions)?,
                runtime,
                control_token,
                sql_token,
                stopped: AtomicBool::new(false),
                inflight: Quota::new(limits.max_inflight),
                connections: Quota::new(connections),
                admission,
                diagnostics: Diagnostics::default(),
                limits,
                wakeup: acceptor_wakeup,
                shutdown_budget: crate::shutdown_budget::ShutdownBudget::configured()?,
            }),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.acceptor.local_addr()?)
    }

    pub fn run(self) -> Result<()> {
        let budget = self.state.shutdown_budget.clone();
        let _shutdown_return = budget.cancel_on_return();
        let result = self.run_inner();
        drop(self);
        budget.finish();
        result
    }

    fn run_inner(&self) -> Result<()> {
        let watchdog_state = self.state.clone();
        let watchdog = thread::spawn(move || {
            let mut failure = None;
            while !watchdog_state.stopped.load(Ordering::SeqCst) {
                match watchdog_state.runtime.status() {
                    Ok(status) => {
                        let mut revoked: Vec<_> = status
                            .databases
                            .into_iter()
                            .filter(|database| {
                                matches!(
                                    database.mode,
                                    OwnershipMode::Revoked | OwnershipMode::Detached
                                )
                            })
                            .map(|database| database.database)
                            .collect();
                        revoked.extend(status.failures.into_iter().map(|failure| failure.database));
                        if let Err(error) = watchdog_state.sessions.sweep(&revoked) {
                            failure.get_or_insert(error);
                        }
                        // Failure detail is not dropped here: the sweep records and
                        // prints each database once, and the same ledger is what
                        // `get_metrics` reports as `ownership_failures`.
                        if let Err(error) = watchdog_state.runtime.sweep_revocations() {
                            failure.get_or_insert(error);
                        }
                        // Maintenance is scheduled from this thread and executed on the
                        // scheduler's own, so a checkpoint cannot delay the sweep above. A
                        // failed pass is reported but never takes the worker down: a blocked
                        // unit is recorded against its database and retried after its backoff.
                        if let Err(error) = watchdog_state.runtime.maintenance_tick() {
                            eprintln!("maintenance outcome=pass_failed reason={error:#}");
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
                self.acceptor.park()?;
                while !self.state.stopped.load(Ordering::SeqCst) {
                    let Some(mut connection) = self.acceptor.take()? else {
                        break;
                    };
                    // Connection budget, not execution budget: a connection that
                    // will sit parked between requests must not consume a request
                    // slot, or idle pooled clients would starve real work.
                    let Some(connection_ticket) = self.state.connections.try_acquire() else {
                        let _ = http::reply(
                            &mut connection,
                            429,
                            &super::protocol::AdmissionRejection::encode(
                                "worker connection limit reached",
                            ),
                        );
                        continue;
                    };
                    let state = self.state.clone();
                    let accepted = Instant::now();
                    match thread::Builder::new().spawn(move || {
                        let _ticket = connection_ticket;
                        let _ = state.serve(&mut connection, accepted);
                    }) {
                        Ok(worker) => workers.push(worker),
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Ok(())
        })();
        self.state.stop();
        let shutdown = workers.finish(watchdog, result, || self.state.sessions.clear());
        // Sample retirement unconditionally: a failed session cleanup must not hide
        // why the worker is unclean, which is exactly what an early `?` did.
        let health = self.state.runtime.retirement_health();
        let mut failures = Vec::new();
        if let Err(error) = shutdown {
            failures.push(format!("{error:#}"));
        }
        if let Some(outstanding) = shutdown::retirement_unproven(&health) {
            failures.push(format!("retirement unproven: {outstanding}"));
        }
        if let Err(error) = self.state.shutdown_budget.check() {
            failures.push(format!("{error:#}"));
        }
        if !failures.is_empty() {
            let reason = failures.join("; ");
            eprintln!("{}", shutdown::unclean_log_line(&reason, &health));
            anyhow::bail!("worker shutdown unproven; not clean: {reason}");
        }
        Ok(())
    }
}

impl State {
    /// Serve one pooled connection until the peer closes it, it goes idle, or
    /// the request pipeline decides the connection must not be reused.
    ///
    /// Requests are strictly serialized: the next request is only read after the
    /// current response has been flushed, so a lost response can never be
    /// followed by an implicitly replayed one on the same socket.
    fn serve(&self, stream: &mut TcpStream, _accepted: Instant) -> Result<()> {
        let mut first = true;
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return Ok(());
            }
            let request = match read_request(
                stream,
                self.limits.max_request_bytes,
                if first {
                    None
                } else {
                    Some(transport::KEEP_ALIVE_IDLE)
                },
                &self.stopped,
            ) {
                Ok(Some(request)) => request,
                Ok(None) => return Ok(()),
                Err(_) => {
                    let _ = transport::reply(
                        stream,
                        400,
                        b"{\"error\":\"invalid HTTP request\"}",
                        false,
                    );
                    return Ok(());
                }
            };
            first = false;
            let keep_alive = request.keep_alive && !self.stopped.load(Ordering::SeqCst);
            // One lease window per request: the first full validation reaches
            // the authority, later ones in the same request are local.
            let _lease_window = crate::session::lease_window::open();
            // Re-anchor the request budget per request: a pooled connection must
            // not inherit a deadline measured from when it was accepted.
            let accepted = Instant::now();
            // The execution budget is taken per request. Charging it per
            // connection is what let a parked socket hold a slot indefinitely.
            let Some(_execution_ticket) = self.inflight.try_acquire() else {
                let _ = transport::reply(
                    stream,
                    429,
                    &super::protocol::AdmissionRejection::encode("worker saturated"),
                    false,
                );
                return Ok(());
            };
            if !self.handle(stream, request, accepted, keep_alive)? {
                return Ok(());
            }
        }
    }

    /// Returns whether the connection may serve another request.
    fn handle(
        &self,
        stream: &mut TcpStream,
        request: transport::Request,
        accepted: Instant,
        keep_alive: bool,
    ) -> Result<bool> {
        let supplied = request.authorization.strip_prefix("Bearer ").unwrap_or("");
        let control = request.path == "/control" && token_matches(supplied, &self.control_token);
        let sql = request.path == "/sql" && token_matches(supplied, &self.sql_token);
        if !control && !sql {
            let _ = transport::reply(stream, 403, b"{\"error\":\"unauthorized\"}", false);
            return Ok(false);
        }
        let parsed = match serde_json::from_slice::<WorkerRequest>(&request.body) {
            Ok(parsed) => parsed,
            Err(_) => {
                let _ = transport::reply(
                    stream,
                    400,
                    b"{\"error\":\"invalid worker request\"}",
                    false,
                );
                return Ok(false);
            }
        };
        let version = parsed.version;
        let identifier = parsed.request_id.clone();
        let deadline = accepted + Budget::trusted_default().wall_duration();
        let _deadline_scope = crate::session::RequestDeadline::scope(deadline);
        let write_cursor_request = matches!(
            &parsed.command,
            WorkerCommand::OpenWriteCursor { .. } | WorkerCommand::FetchWriteCursor { .. }
        );
        let stateful = matches!(
            &parsed.command,
            WorkerCommand::OpenSession { .. }
                | WorkerCommand::OpenOwnedSession { .. }
                | WorkerCommand::Execute { .. }
                | WorkerCommand::Query { .. }
                | WorkerCommand::Describe { .. }
                | WorkerCommand::Statement { .. }
                | WorkerCommand::SessionAutocommit { .. }
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
            let _ = transport::reply(
                stream,
                400,
                b"{\"error\":\"writer profile unavailable\"}",
                false,
            );
            return Ok(false);
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
                    | WorkerCommand::CloseOwnedSessions { .. }
                    | WorkerCommand::GetCapacityTrace { .. }
                    | WorkerCommand::Shutdown
                    | WorkerCommand::ShutdownWithin { .. }
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
                    call = Some(self.sessions.begin(session, deadline)?);
                }
                WorkerCommand::Checkpoint { session }
                | WorkerCommand::Compact { session }
                | WorkerCommand::Diagnostics { session }
                | WorkerCommand::SessionAutocommit { session }
                | WorkerCommand::FetchReadCursor { session, .. }
                | WorkerCommand::CloseReadCursor { session }
                | WorkerCommand::FetchWriteCursor { session, .. }
                | WorkerCommand::CloseWriteCursor { session } => {
                    call = Some(self.sessions.begin(session, deadline)?);
                }
                _ => {}
            }
            Ok(())
        })();
        let prepared =
            prepared.map_err(|error| error.context(crate::session::SqlEvidence::NotExecuted));
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
            let _ = transport::reply(
                stream,
                429,
                &super::protocol::AdmissionRejection::encode("worker execution lane saturated"),
                false,
            );
            return Ok(false);
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
            if stateful && Instant::now() >= deadline {
                return Err(anyhow::Error::new(AdmissionError::Deadline)
                    .context(crate::session::SqlEvidence::NotExecuted));
            }
            self.dispatch(
                parsed.command,
                control,
                &identifier,
                version,
                call.as_ref(),
                &mut terminal,
                deadline,
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
        // A client may send its next request as soon as it receives the receipt.
        // Only write cursors still need the guard to classify a lost reply.
        if !write_cursor_request {
            drop(call.take());
        }
        let result = transport::reply(stream, 200, &bytes, keep_alive);
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
        // Reuse only after a response that was fully written. A failed write may
        // have left the peer without the outcome, so the socket is retired.
        Ok(result.is_ok() && keep_alive)
    }
}
