use serde::{Deserialize, Serialize};

use anyhow::Context;

use crate::object_store::PutCondition;
use crate::ownership::types::{
    AccessRole, GrantRequest, LeaseBinding, LeaseIntent, RenewRequest, RevokeRequest,
};
use crate::runtime::DatabaseId;

#[cfg(test)]
#[path = "payload_tests.rs"]
mod payload_tests;

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_RPC_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_SEGMENT_BATCH: usize = 16;
pub const MAX_IMPORT_SEGMENT_BATCH: usize = 64;
/// A checkpoint backfill publishes hundreds of dirty segments. Each body used to cost a
/// conditional put and a media barrier on its own, which made one backfill tens of seconds
/// and blocked foreground SQL for the whole truncate checkpoint. One group carries the
/// same bodies with one authority publication and one barrier per batch; the bound keeps a
/// single RPC far below `MAX_RPC_BYTES` after base64 expansion.
pub const MAX_GROUP_WRITES: usize = 32;
pub const MAX_GROUP_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentGroupWrite {
    pub key: String,
    #[serde(with = "crate::object_store::payload")]
    pub bytes: Vec<u8>,
}

pub fn validate_segment_group(writes: &[SegmentGroupWrite]) -> anyhow::Result<()> {
    anyhow::ensure!(
        !writes.is_empty() && writes.len() <= MAX_GROUP_WRITES,
        "invalid segment group batch"
    );
    let mut total = 0usize;
    for write in writes {
        // The fenced store proves the key belongs to this namespace; here the last path
        // component must be the content digest, which is what makes retries idempotent.
        let hash = write.key.rsplit('/').next().unwrap_or_default();
        anyhow::ensure!(
            crate::manifest::valid_hash(hash)
                && write.bytes.len() == crate::manifest::SEGMENT_SIZE
                && crate::manifest::digest(&write.bytes) == hash,
            "invalid segment group body"
        );
        total = total
            .checked_add(write.bytes.len())
            .context("segment group size overflow")?;
    }
    anyhow::ensure!(
        total <= MAX_GROUP_BYTES,
        "segment group exceeds byte budget"
    );
    Ok(())
}

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
    /// Terminal close used by worker database retirement. It drains in-flight work and
    /// releases the reader pin even when repeated opens left extra references; a lost
    /// OPEN response must not pin a namespace until its lease happens to expire.
    RetireSession {
        session: String,
    },
    SessionState {
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
        #[serde(with = "crate::object_store::payload::batch")]
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
        #[serde(with = "crate::object_store::payload")]
        bytes: Vec<u8>,
        condition: PutCondition,
    },
    /// Durability group for immutable content-addressed segment bodies. It never carries a
    /// mutable key or a conditional head: the caller still publishes the manifest and the
    /// HEAD CAS as separate writes after this group is durable.
    PutGroup {
        writes: Vec<SegmentGroupWrite>,
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
