use serde::{Deserialize, Serialize};

use crate::object_store::PutCondition;
use crate::ownership::types::{
    AccessRole, GrantRequest, LeaseBinding, LeaseIntent, RenewRequest, RevokeRequest,
};
use crate::runtime::DatabaseId;

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_RPC_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SEGMENT_BATCH: usize = 16;
pub const MAX_IMPORT_SEGMENT_BATCH: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportSegment {
    pub hash: String,
    pub data_base64: String,
}

pub fn validate_segment_batch(segments: &[Vec<u8>]) -> anyhow::Result<()> {
    anyhow::ensure!(
        !segments.is_empty()
            && segments.len() <= MAX_SEGMENT_BATCH
            && segments
                .iter()
                .all(|segment| segment.len() == crate::manifest::SEGMENT_SIZE),
        "invalid segment upload batch"
    );
    Ok(())
}

pub fn validate_registration_batch(
    entries: &[crate::authority::ObjectEntry],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        !entries.is_empty() && entries.len() <= MAX_SEGMENT_BATCH,
        "invalid segment registration batch"
    );
    Ok(())
}

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
    ImportSegments {
        database: DatabaseId,
        expected_head: String,
        segments: Vec<ImportSegment>,
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
    ShutdownWithin {
        remaining_ms: u64,
    },
    GetMetrics,
    GetCapacityTrace {
        stream_id: String,
        after_sequence: u64,
        max_events: usize,
    },
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
    UploadSegments {
        segments: Vec<Vec<u8>>,
    },
    RegisterSegments {
        entries: Vec<crate::authority::ObjectEntry>,
    },
    AppendWal {
        request: crate::wal::WalAppendRequest,
    },
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
