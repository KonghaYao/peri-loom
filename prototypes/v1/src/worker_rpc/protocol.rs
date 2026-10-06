use serde::{Deserialize, Serialize};

use crate::ownership::types::OwnershipCommand;
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
    Ownership { action: OwnershipCommand },
    OpenSession { database: DatabaseId },
    Execute { session: String, sql: String },
    Query { session: String, sql: String },
    Checkpoint { session: String },
    Compact { session: String },
    Diagnostics { session: String },
    CloseSession { session: String },
    GetMetrics,
    Shutdown,
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
}
