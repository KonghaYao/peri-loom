//! Local parent/child RPC evidence. This never changes wire identities or delivery semantics.
//!
//! Unlike slow spans, the opt-in link stream includes fast calls and early failures. It is
//! still best-effort and incomplete: the shared bounded sink has no normal-shutdown drain
//! proof, and a process can exit before this guard drops. No event asserts remote receipt.
use super::{enqueue, nanos, safe_id, Detail, Event, Runtime, CONTEXT};
use crate::authority_rpc::protocol::{AuthorityCommand, SessionOperation};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
pub(crate) enum RpcPhase {
    RequestId,
    ValidateRequest,
    EncodeRequest,
    Deadline,
    AcquireSlot,
    Send,
    ResponseHeaders,
    ReadBody,
    DecodeResponse,
    ValidateResponse,
    Complete,
}

impl RpcPhase {
    fn name(self) -> &'static str {
        match self {
            Self::RequestId => "request_id",
            Self::ValidateRequest => "validate_request",
            Self::EncodeRequest => "encode_request",
            Self::Deadline => "deadline",
            Self::AcquireSlot => "acquire_slot",
            Self::Send => "send",
            Self::ResponseHeaders => "response_headers",
            Self::ReadBody => "read_body",
            Self::DecodeResponse => "decode_response",
            Self::ValidateResponse => "validate_response",
            Self::Complete => "complete",
        }
    }
}

struct State {
    runtime: &'static Runtime,
    started: Instant,
    parent: Option<String>,
    parent_id_status: &'static str,
    child: Option<String>,
    child_id_status: &'static str,
    command: &'static str,
    phase: RpcPhase,
    result: &'static str,
    send_invoked: bool,
    response_headers_received: bool,
}

/// The parent is captured at call entry, so nested scopes cannot relabel a returning call.
/// !Send keeps this synchronous call guard in its originating request thread.
pub(crate) struct AuthorityRpcLink(Option<State>, PhantomData<Rc<()>>);

impl AuthorityRpcLink {
    pub(crate) fn begin(command: &AuthorityCommand) -> Self {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        let enabled =
            *ENABLED.get_or_init(|| std::env::var("PERI_DIAG_RPC_LINK").as_deref() == Ok("1"));
        Self::start(
            if enabled { super::runtime() } else { None },
            || command_name(command),
            Instant::now,
        )
    }

    fn start(
        runtime: Option<&'static Runtime>,
        command: impl FnOnce() -> &'static str,
        now: impl FnOnce() -> Instant,
    ) -> Self {
        let Some(runtime) = runtime else {
            return Self(None, PhantomData);
        };
        let (parent, parent_id_status) = CONTEXT.with(|slot| {
            let context = slot.borrow();
            sanitized(context.request_id.as_deref())
        });
        Self(
            Some(State {
                runtime,
                started: now(),
                parent,
                parent_id_status,
                child: None,
                child_id_status: "unassigned",
                command: command(),
                phase: RpcPhase::RequestId,
                result: "error_or_early_return",
                send_invoked: false,
                response_headers_received: false,
            }),
            PhantomData,
        )
    }

    pub(crate) fn child(&mut self, identifier: &str) {
        if let Some(state) = &mut self.0 {
            (state.child, state.child_id_status) = sanitized(Some(identifier));
            state.phase = RpcPhase::ValidateRequest;
        }
    }

    pub(crate) fn phase(&mut self, phase: RpcPhase) {
        if let Some(state) = &mut self.0 {
            state.phase = phase;
            if matches!(phase, RpcPhase::Send) {
                state.send_invoked = true;
            }
            if matches!(phase, RpcPhase::ResponseHeaders) {
                state.response_headers_received = true;
            }
        }
    }

    pub(crate) fn complete(&mut self, result: &'static str) {
        if let Some(state) = &mut self.0 {
            state.phase = RpcPhase::Complete;
            state.result = result;
        }
    }

    fn finish_at(&mut self, ended: Instant, unwinding: bool) {
        let Some(state) = self.0.take() else {
            return;
        };
        let link_available = state.parent.is_some() && state.child.is_some();
        let event = Event {
            schema: "peri-authority-rpc-link-v1",
            evidence_class: "non_harness",
            pid: std::process::id(),
            // request_id is the existing child Authority envelope identity; its parent is
            // the current Worker/request context captured before starting that child.
            request_id: state.child,
            parent_request_id: state.parent,
            op_id: None,
            start_mono_ns: nanos(state.started.duration_since(state.runtime.origin)),
            end_mono_ns: nanos(ended.duration_since(state.runtime.origin)),
            ts_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .map(|time| time.as_millis().min(u64::MAX as u128) as u64),
            duration_ns: nanos(ended.duration_since(state.started)),
            result: if unwinding { "unwind" } else { state.result },
            retry_count: None,
            retry_scope: "not_observed_by_rpc_link",
            detail: Detail::AuthorityRpcLink {
                scope: "local_authority_client_call",
                command: state.command,
                phase: state.phase.name(),
                parent_id_status: state.parent_id_status,
                child_id_status: state.child_id_status,
                link_available,
                send_invoked: state.send_invoked,
                response_headers_received: state.response_headers_received,
                capture: "all_call_completions_when_enabled",
                complete: false,
            },
        };
        enqueue(&state.runtime.sender, &state.runtime.counts, event);
    }
}

impl Drop for AuthorityRpcLink {
    fn drop(&mut self) {
        // A disabled guard must not read a clock, allocate or touch the sink.
        if self.0.is_some() {
            self.finish_at(Instant::now(), std::thread::panicking());
        }
    }
}

fn sanitized(identifier: Option<&str>) -> (Option<String>, &'static str) {
    match identifier {
        None => (None, "absent"),
        Some(value) if value.contains("plk1_") || value.contains("pldb1.") => (None, "rejected"),
        Some(value) => match safe_id(value) {
            Some(value) => (Some(value), "present"),
            None => (None, "rejected"),
        },
    }
}

// Only static variant names cross the diagnostic boundary. Never serialize a command,
// session, namespace, object key, SQL, credentials, payload or error text for a link.
fn command_name(command: &AuthorityCommand) -> &'static str {
    match command {
        AuthorityCommand::Challenge { .. } => "challenge",
        AuthorityCommand::Grant { .. } => "grant",
        AuthorityCommand::Renew { .. } => "renew",
        AuthorityCommand::Revoke { .. } => "revoke",
        AuthorityCommand::GetDatabaseState { .. } => "get_database_state",
        AuthorityCommand::Maintenance { .. } => "maintenance",
        AuthorityCommand::ImportSegments { .. } => "import_segments",
        AuthorityCommand::ValidateLease { .. } => "validate_lease",
        AuthorityCommand::OpenSession { .. } => "open_session",
        AuthorityCommand::CloseSession { .. } => "close_session",
        AuthorityCommand::RetireSession { .. } => "retire_session",
        AuthorityCommand::SessionState { .. } => "session_state",
        AuthorityCommand::Shutdown => "shutdown",
        AuthorityCommand::ShutdownWithin { .. } => "shutdown_within",
        AuthorityCommand::GetMetrics => "get_metrics",
        AuthorityCommand::GetCapacityTrace { .. } => "get_capacity_trace",
        AuthorityCommand::Session { operation, .. } => match operation {
            SessionOperation::UploadSegments { .. } => "upload_segments",
            SessionOperation::RegisterSegments { .. } => "register_segments",
            SessionOperation::AppendWal { .. } => "append_wal",
            SessionOperation::Validate => "validate_session",
            SessionOperation::Get { .. } => "get",
            SessionOperation::Put { .. } => "put",
            SessionOperation::PutGroup { .. } => "put_group",
            SessionOperation::Delete { .. } => "delete",
            SessionOperation::List { .. } => "list",
        },
    }
}

#[cfg(test)]
mod tests;
