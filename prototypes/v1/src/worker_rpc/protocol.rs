use serde::{Deserialize, Serialize};

use crate::ownership::types::{LeaseBinding, OwnershipCommand};
use crate::runtime::DatabaseId;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRequest {
    pub version: u32,
    pub request_id: String,
    pub command: WorkerCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerCommand {
    WriterProfileStatus,
    Ownership {
        action: OwnershipCommand,
    },
    OpenSession {
        database: DatabaseId,
    },
    OpenOwnedSession {
        database: DatabaseId,
        owner_incarnation: String,
    },
    CloseOwnedSessions {
        binding: LeaseBinding,
        owner_incarnation: String,
        open_request_id: String,
    },
    Execute {
        session: String,
        sql: String,
    },
    Query {
        session: String,
        sql: String,
    },
    Describe {
        session: String,
        sql: String,
    },
    // Reads the connection state a protocol condition asks about, on the session
    // it belongs to, without preparing or stepping SQL.
    SessionAutocommit {
        session: String,
    },
    Statement {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        // Absent means the caller expects the rows, so a caller that predates
        // this field keeps the full result.
        #[serde(default = "want_rows_default")]
        want_rows: bool,
    },
    // One statement prepared, bound and stepped with the engine's read-only
    // verdict required before every step. A statement the engine classifies as
    // writing before it runs answers `requires_intent` with an engine verdict;
    // transaction/context refusals have distinct reasons and cannot seed a
    // cross-session write hint. No writing instruction runs on this path.
    GuardedReadonlyStatement {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        #[serde(default = "want_rows_default")]
        want_rows: bool,
    },
    // One implicit session open fused with the first guarded read-only statement.
    // The worker opens exactly as `open_session` and then runs the statement in the
    // same request's lease window, so the authority validation is paid once. A
    // statement the engine classifies as writing is refused before it steps, which
    // is what makes the whole command unable to commit.
    OpenGuardedStatement {
        database: DatabaseId,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        #[serde(default = "want_rows_default")]
        want_rows: bool,
    },
    // The same guarded statement, repeated for a caller-known contiguous block of
    // read-only work. Each statement keeps its own budget, deadline check and
    // engine verdict; the block only removes round trips, never evidence.
    GuardedReadonlyBatch {
        session: String,
        statements: Vec<BatchStatement>,
        // Remaining pipeline result budget in JSON bytes. The worker stops after
        // the statement that crosses it and reports how many it decided.
        max_result_bytes: usize,
    },
    // Feature discovery for a caller that must know whether this worker parses
    // the batch command before it sends one. Answered before any session exists;
    // an older worker rejects the request as invalid before dispatch.
    Capabilities,
    OpenReadCursor {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
    },
    OpenReadCursorFirst {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        max_rows: usize,
    },
    FetchReadCursor {
        session: String,
        max_rows: usize,
    },
    CloseReadCursor {
        session: String,
    },
    OpenWriteCursor {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
    },
    OpenWriteCursorFirst {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
        max_rows: usize,
    },
    FetchWriteCursor {
        session: String,
        max_rows: usize,
    },
    CloseWriteCursor {
        session: String,
    },
    Checkpoint {
        session: String,
    },
    Compact {
        session: String,
    },
    Diagnostics {
        session: String,
    },
    CloseSession {
        session: String,
    },
    GetMetrics,
    GetCapacityTrace {
        stream_id: String,
        after_sequence: u64,
        max_events: usize,
    },
    Shutdown,
    ShutdownWithin {
        remaining_ms: u64,
    },
}

/// Only emitted before dispatch; absence never establishes non-execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRejection {
    pub error: String,
    pub execution_completed: bool,
    pub admission: AdmissionOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionOutcome {
    NotAdmitted,
}

impl AdmissionRejection {
    pub fn encode(reason: &str) -> Vec<u8> {
        serde_json::to_vec(&Self {
            error: reason.into(),
            execution_completed: false,
            admission: AdmissionOutcome::NotAdmitted,
        })
        .expect("admission response serializes")
    }
}

fn want_rows_default() -> bool {
    true
}

/// One entry of `guarded_readonly_batch`. The defaults mirror
/// `GuardedReadonlyStatement`, so a statement that names only its SQL behaves the
/// same on both commands.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchStatement {
    pub sql: String,
    #[serde(default)]
    pub args: Vec<StatementArg>,
    #[serde(default)]
    pub named_args: Vec<NamedStatementArg>,
    #[serde(default = "want_rows_default")]
    pub want_rows: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum StatementArg {
    Null,
    Integer { value: String },
    Float { value: f64 },
    Text { value: String },
    Blob { base64: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedStatementArg {
    pub name: String,
    pub value: StatementArg,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionState {
    pub session: String,
    pub binding: LeaseBinding,
    pub owner: bool,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResponse {
    pub version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    // 失败时 true 表示执行已收束（不保证回滚），false 表示未进入执行；缺失表示无证据。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_completed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_state: Option<SessionState>,
}
