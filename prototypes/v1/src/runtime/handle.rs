use super::{registry::Instance, DatabaseId, WorkerRuntime};
use crate::session::{
    DescribeResult, GuardedStatementOutcome, SessionWriterState, StatementColumn, StatementResult,
    WriteCursorPage,
};
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
    pub(crate) fn writer_state(&self) -> Result<SessionWriterState> {
        self.session.as_ref().unwrap().writer_state()
    }
    pub(crate) fn begin_close(&self) -> Result<()> {
        self.session.as_ref().unwrap().begin_close()
    }
    pub(crate) fn request_close_until(&self, deadline: std::time::Instant) -> Result<()> {
        self.session.as_ref().unwrap().request_close_until(deadline)
    }
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.session.as_ref().unwrap().query(sql)
    }
    pub fn describe(&self, sql: &str) -> Result<DescribeResult> {
        self.session.as_ref().unwrap().describe(sql)
    }
    pub fn open_read_cursor(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<Vec<StatementColumn>> {
        self.session
            .as_ref()
            .unwrap()
            .open_read_cursor(sql, args, named_args)
    }
    pub fn fetch_read_cursor(&self, page_size: usize) -> Result<(Vec<Vec<Value>>, bool)> {
        self.session.as_ref().unwrap().fetch_read_cursor(page_size)
    }
    pub fn close_read_cursor(&self) -> Result<()> {
        self.session.as_ref().unwrap().close_read_cursor()
    }
    pub fn open_write_cursor(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<Vec<StatementColumn>> {
        self.session
            .as_ref()
            .unwrap()
            .open_write_cursor(sql, args, named_args)
    }
    pub fn fetch_write_cursor(&self, page_size: usize) -> Result<WriteCursorPage> {
        self.session.as_ref().unwrap().fetch_write_cursor(page_size)
    }
    pub fn close_write_cursor(&self) -> Result<bool> {
        self.session.as_ref().unwrap().close_write_cursor()
    }
    pub(crate) fn quarantine_unknown_write_result(&self) -> Result<()> {
        self.session
            .as_ref()
            .unwrap()
            .quarantine_unknown_write_result()
    }
    pub fn statement(&self, sql: &str, args: &[Value]) -> Result<StatementResult> {
        self.session.as_ref().unwrap().statement(sql, args)
    }
    pub fn statement_with_named_args(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
    ) -> Result<StatementResult> {
        self.session
            .as_ref()
            .unwrap()
            .statement_with_named_args(sql, args, named_args, want_rows)
    }
    pub fn guarded_readonly_statement(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
    ) -> Result<GuardedStatementOutcome> {
        self.session
            .as_ref()
            .unwrap()
            .guarded_readonly_statement(sql, args, named_args, want_rows)
    }
    pub fn in_transaction(&self) -> Result<bool> {
        self.session.as_ref().unwrap().in_transaction()
    }
    pub fn is_readonly(&self) -> Result<bool> {
        self.session.as_ref().unwrap().is_readonly()
    }
    pub fn autocommit(&self) -> Result<bool> {
        self.session
            .as_ref()
            .unwrap()
            .autocommit(&crate::session::Budget::trusted_default())
    }
    pub fn checkpoint(&self) -> Result<()> {
        ensure!(
            !self.in_transaction()?,
            "cannot checkpoint an active transaction"
        );
        self.database.as_ref().unwrap().database()?.checkpoint()
    }
    pub fn compact_wal(&self) -> Result<()> {
        ensure!(
            !self.in_transaction()?,
            "cannot compact WAL in an active transaction"
        );
        self.database.as_ref().unwrap().database()?.compact_wal()
    }
    pub fn resources(&self) -> Result<ResourceUsage> {
        self.database.as_ref().unwrap().database()?.resources()
    }
    pub fn diagnostics(&self) -> Result<serde_json::Value> {
        let database = self.database.as_ref().unwrap().database()?;
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
            "maintenance": database.maintenance_ledger().counters(),
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
