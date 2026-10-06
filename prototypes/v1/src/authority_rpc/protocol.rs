use serde::{Deserialize, Serialize};

use crate::object_store::PutCondition;
use crate::ownership::types::{
    AccessRole, GrantRequest, LeaseBinding, LeaseIntent, RenewRequest, RevokeRequest,
};
use crate::runtime::DatabaseId;

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_RPC_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcRequest {
    pub version: u32,
    pub request_id: String,
    pub command: AuthorityCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthorityCommand {
    Challenge {
        intent: LeaseIntent,
    },
    Grant {
        request: GrantRequest,
    },
    Renew {
        request: RenewRequest,
    },
    Revoke {
        request: RevokeRequest,
    },
    GetDatabaseState {
        database: DatabaseId,
    },
    Maintenance {
        request_id: String,
        database: DatabaseId,
        operation: MaintenanceOperation,
    },
    ValidateLease {
        binding: LeaseBinding,
        role: AccessRole,
    },
    OpenSession {
        binding: LeaseBinding,
        role: AccessRole,
    },
    CloseSession {
        session: String,
    },
    Session {
        session: String,
        operation: SessionOperation,
    },
    Shutdown,
    GetMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaintenanceOperation {
    Refs,
    Snapshot { name: String },
    Branch { source: String, name: String },
    Clone { snapshot: String, name: String },
    Restore { snapshot: String },
    RestorePitr { sequence: u64 },
    Pitr { sequence: u64 },
    PitrTime { timestamp_ms: u64 },
    DropBranch,
    DropSnapshot { name: String },
    RetainAfter { sequence: u64 },
    GcInspect,
    GcCollect { safety_window_ms: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionOperation {
    Validate,
    Get {
        key: String,
    },
    Put {
        key: String,
        bytes: Vec<u8>,
        condition: PutCondition,
    },
    Delete {
        key: String,
        expected: Option<String>,
    },
    List {
        prefix: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcResponse {
    pub version: u32,
    pub request_id: String,
    pub ok: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub precondition_failed: bool,
}
