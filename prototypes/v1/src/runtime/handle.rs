use super::{registry::Instance, DatabaseId, WorkerRuntime};
use crate::{resources::ResourceUsage, session::RemoteSession};
use anyhow::{ensure, Result};
use std::sync::Arc;
use turso_core::Value;

pub struct DatabaseHandle {
    runtime: Arc<WorkerRuntime>,
    identifier: DatabaseId,
    database: Option<Arc<Instance>>,
    session: Option<RemoteSession>,
}

impl DatabaseHandle {
    pub(super) fn new(
        runtime: Arc<WorkerRuntime>,
        identifier: DatabaseId,
        database: Arc<Instance>,
        session: RemoteSession,
    ) -> Self {
        Self {
            runtime,
            identifier,
            database: Some(database),
            session: Some(session),
        }
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.session.as_ref().unwrap().execute(sql)
    }
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.session.as_ref().unwrap().query(sql)
    }
    pub fn in_transaction(&self) -> Result<bool> {
        self.session.as_ref().unwrap().in_transaction()
    }
    pub fn is_readonly(&self) -> Result<bool> {
        self.session.as_ref().unwrap().is_readonly()
    }
    pub fn checkpoint(&self) -> Result<()> {
        ensure!(
            !self.in_transaction()?,
            "cannot checkpoint an active transaction"
        );
        self.database.as_ref().unwrap().database().checkpoint()
    }
    pub fn compact_wal(&self) -> Result<()> {
        ensure!(
            !self.in_transaction()?,
            "cannot compact WAL in an active transaction"
        );
        self.database.as_ref().unwrap().database().compact_wal()
    }
    pub fn resources(&self) -> Result<ResourceUsage> {
        self.database.as_ref().unwrap().database().resources()
    }
    pub fn diagnostics(&self) -> Result<serde_json::Value> {
        let database = self.database.as_ref().unwrap().database();
        let storage = database.storage();
        storage.store().validate_session()?;
        let manifest = storage.manifest();
        Ok(serde_json::json!({
            "generation": manifest.generation,
            "manifest_hash": storage.manifest_id(),
            "logical_db_size": manifest.logical_db_size,
            "page_size": manifest.page_size,
            "segment_count": manifest.segments.len(),
            "applied_lsn": manifest.applied_lsn,
            "applied_wal_position": manifest.applied_wal_position,
            "resources": database.resources()?,
            "snapshot_consistency": "diagnostic_only_not_authority"
        }))
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        drop(self.session.take());
        drop(self.database.take());
        self.runtime.release(&self.identifier);
    }
}
