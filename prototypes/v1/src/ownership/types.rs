use std::collections::BTreeMap;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use crate::authority::CatalogRoot;
use crate::runtime::{DatabaseId, WorkerStatus};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipMode {
    Detached,
    Acquiring,
    Writer,
    ReadOnly,
    Draining,
    Revoked,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccessRole {
    Writer,
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrantState {
    Active,
    Revoked,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecord {
    pub epoch: u64,
    pub branch_incarnation: u64,
    pub worker_id: String,
    pub lease_id: String,
    pub grant_id: String,
    pub renewal_sequence: u64,
    pub state: GrantState,
    pub role: AccessRole,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CommandReceipt {
    pub digest: String,
    pub result: serde_json::Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OwnershipCatalog {
    pub authority_term: u64,
    pub branches: BTreeMap<String, OwnerRecord>,
    pub commands: BTreeMap<String, CommandReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseBinding {
    pub database: DatabaseId,
    pub root_incarnation: String,
    pub authority_term: u64,
    pub branch_incarnation: u64,
    pub epoch: u64,
    pub worker_id: String,
    pub lease_id: String,
    pub grant_id: String,
    pub renewal_sequence: u64,
    pub role: AccessRole,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GrantIntent {
    pub database: DatabaseId,
    pub worker_id: String,
    pub epoch: u64,
    pub expected_epoch: u64,
    pub lease_id: String,
    pub ttl_ms: u64,
    pub role: AccessRole,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RenewIntent {
    pub binding: LeaseBinding,
    pub sequence: u64,
    pub ttl_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    content = "intent",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum LeaseIntent {
    Grant(GrantIntent),
    Renew(RenewIntent),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NonceReply {
    pub nonce: String,
    pub expires_in_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantRequest {
    pub request_id: String,
    pub nonce: String,
    pub intent: GrantIntent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewRequest {
    pub request_id: String,
    pub nonce: String,
    pub intent: RenewIntent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeRequest {
    pub request_id: String,
    pub database: DatabaseId,
    pub expected_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantReply {
    pub binding: LeaseBinding,
    pub remaining_ttl_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseOwnership {
    pub database: DatabaseId,
    pub mode: OwnershipMode,
    pub binding: Option<LeaseBinding>,
    pub epoch: u64,
    pub remaining_ttl_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationStage {
    Read,
    RootPublish,
    Upload,
    Delete,
}

pub trait MutationGuard: Send + Sync {
    fn validate(&self, stage: MutationStage) -> Result<()>;
    fn validate_root(&self, current: &CatalogRoot, candidate: &CatalogRoot) -> Result<()>;
    fn uncertain(&self, stage: MutationStage);
}

pub trait LeaseClock: Send + Sync {
    fn now_ms(&self) -> Result<u64>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnershipCommand {
    AcquireDatabase {
        lease: GrantReply,
        role: AccessRole,
    },
    RenewLease {
        lease: GrantReply,
    },
    RevokeDatabase {
        database: DatabaseId,
        expected_epoch: u64,
    },
    DrainDatabase {
        database: DatabaseId,
        expected_epoch: u64,
    },
    GetDatabaseState {
        database: DatabaseId,
    },
    GetWorkerState,
    GetWorkerHeartbeat,
}

#[derive(Clone, Debug, Serialize)]
pub struct OwnershipWorkerState {
    pub worker_id: String,
    pub databases: Vec<DatabaseOwnership>,
    pub resources: WorkerStatus,
}

pub fn validate_label(label: &str) -> Result<()> {
    ensure!(
        !label.is_empty()
            && label.len() <= 128
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
        "invalid ownership identifier"
    );
    Ok(())
}
