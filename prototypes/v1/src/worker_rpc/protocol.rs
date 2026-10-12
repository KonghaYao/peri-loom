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
    Statement {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
    },
    OpenReadCursor {
        session: String,
        sql: String,
        #[serde(default)]
        args: Vec<StatementArg>,
        #[serde(default)]
        named_args: Vec<NamedStatementArg>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_completed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_state: Option<SessionState>,
}
