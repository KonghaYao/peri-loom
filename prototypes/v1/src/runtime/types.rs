use crate::namespace::Namespace;
use crate::object_store::ObjectStore;
use crate::resources::ResourceUsage;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct DatabaseId {
    pub namespace: Namespace,
    pub branch: String,
}

impl DatabaseId {
    pub fn new(namespace: Namespace, branch: impl Into<String>) -> Result<Self> {
        let identifier = Self {
            namespace,
            branch: branch.into(),
        };
        identifier.validate()?;
        Ok(identifier)
    }

    pub fn validate(&self) -> Result<()> {
        Namespace::new(
            self.namespace.tenant.clone(),
            self.namespace.database.clone(),
        )?;
        ensure!(
            !self.branch.is_empty()
                && self.branch.len() <= 128
                && self
                    .branch
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
            "invalid branch name"
        );
        Ok(())
    }

    pub fn cache_key(&self) -> Result<String> {
        self.validate()?;
        Ok(blake3::hash(
            format!(
                "{}/{}/{}",
                self.namespace.tenant, self.namespace.database, self.branch
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string())
    }
}

pub trait DatabaseProvider: Send + Sync {
    fn open_store(&self, identifier: &DatabaseId, epoch: u64) -> Result<Arc<dyn ObjectStore>>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLimits {
    pub max_open_databases: usize,
    pub max_total_sessions: usize,
    pub max_sessions_per_database: usize,
    pub segment_ram_bytes: usize,
    pub segment_disk_bytes: u64,
}

impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            max_open_databases: 32,
            max_total_sessions: 128,
            max_sessions_per_database: 16,
            segment_ram_bytes: 8 * 1024 * 1024,
            segment_disk_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseLifecycle {
    Loading,
    Open,
    Draining,
}

#[derive(Clone, Debug, Serialize)]
pub struct DatabaseStatus {
    pub identifier: DatabaseId,
    pub epoch: u64,
    pub lifecycle: DatabaseLifecycle,
    pub active_sessions: usize,
    pub idle_ms: Option<u64>,
    pub resources: Option<ResourceUsage>,
    pub resource_error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WorkerStatus {
    pub loaded_databases: usize,
    pub loading_databases: usize,
    pub active_sessions: usize,
    pub resident_count: usize,
    pub oldest_resident_age_ms: u64,
    pub quarantined_count: usize,
    pub oldest_quarantined_age_ms: u64,
    pub databases: Vec<DatabaseStatus>,
}
