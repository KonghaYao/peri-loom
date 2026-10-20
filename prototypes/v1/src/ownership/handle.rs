use std::sync::Arc;

use anyhow::Result;
use turso_core::Value;

use super::types::LeaseBinding;
use crate::resources::ResourceUsage;
use crate::runtime::DatabaseHandle;
use crate::session::{
    DescribeResult, GuardedStatementOutcome, SessionWriterState, StatementColumn, StatementResult,
    WriteCursorPage,
};

use super::context::LeaseContext;
use super::runtime::OwnershipRuntime;

pub struct OwnedHandle {
    pub(super) handle: Option<DatabaseHandle>,
    pub(super) context: Arc<LeaseContext>,
    pub(super) runtime: Arc<OwnershipRuntime>,
}

impl OwnedHandle {
    pub(crate) fn writer_state(&self) -> Result<SessionWriterState> {
        self.handle.as_ref().unwrap().writer_state()
    }

    pub(crate) fn begin_close(&self) -> Result<()> {
        self.handle.as_ref().unwrap().begin_close()
    }

    pub(crate) fn request_close_until(&self, deadline: std::time::Instant) -> Result<()> {
        self.handle.as_ref().unwrap().request_close_until(deadline)
    }

    pub(crate) fn lease_binding_snapshot(&self) -> Result<LeaseBinding> {
        Ok(self.context.lock()?.binding.clone())
    }
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.handle.as_ref().unwrap().execute(sql)
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.handle.as_ref().unwrap().query(sql)
    }

    pub fn describe(&self, sql: &str) -> Result<DescribeResult> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().describe(sql)
    }
    pub fn open_read_cursor(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<Vec<StatementColumn>> {
        self.context.check(None)?;
        self.handle
            .as_ref()
            .unwrap()
            .open_read_cursor(sql, args, named_args)
    }
    pub fn fetch_read_cursor(&self, page_size: usize) -> Result<(Vec<Vec<Value>>, bool)> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().fetch_read_cursor(page_size)
    }
    pub fn close_read_cursor(&self) -> Result<()> {
        self.handle.as_ref().unwrap().close_read_cursor()
    }
    pub fn open_write_cursor(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<Vec<StatementColumn>> {
        self.context.check(Some(false))?;
        anyhow::ensure!(
            !self.handle.as_ref().unwrap().is_readonly()?,
            "write cursor requires writer access"
        );
        self.handle
            .as_ref()
            .unwrap()
            .open_write_cursor(sql, args, named_args)
    }
    pub fn fetch_write_cursor(&self, page_size: usize) -> Result<WriteCursorPage> {
        if let Err(error) = self.context.check(None) {
            if self.handle.as_ref().unwrap().close_write_cursor().is_err() {
                return Err(error.context(crate::session::UnsettledSqlOutcome));
            }
            return Err(error);
        }
        self.handle.as_ref().unwrap().fetch_write_cursor(page_size)
    }
    pub fn close_write_cursor(&self) -> Result<bool> {
        self.handle.as_ref().unwrap().close_write_cursor()
    }
    pub(crate) fn quarantine_unknown_write_result(&self) -> Result<()> {
        self.handle
            .as_ref()
            .unwrap()
            .quarantine_unknown_write_result()
    }

    pub fn statement(&self, sql: &str, args: &[Value]) -> Result<StatementResult> {
        self.handle.as_ref().unwrap().statement(sql, args)
    }
    pub fn statement_with_named_args(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
    ) -> Result<StatementResult> {
        self.handle
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
        self.context.check(None)?;
        self.handle
            .as_ref()
            .unwrap()
            .guarded_readonly_statement(sql, args, named_args, want_rows)
    }

    pub fn in_transaction(&self) -> Result<bool> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().in_transaction()
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.context.check(Some(false))?;
        self.handle.as_ref().unwrap().checkpoint()
    }

    pub fn compact_wal(&self) -> Result<()> {
        self.context.check(Some(false))?;
        self.handle.as_ref().unwrap().compact_wal()
    }

    pub fn resources(&self) -> Result<ResourceUsage> {
        self.handle.as_ref().unwrap().resources()
    }
    pub fn diagnostics(&self) -> Result<serde_json::Value> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().diagnostics()
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().is_readonly()
    }

    pub fn autocommit(&self) -> Result<bool> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().autocommit()
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        drop(self.handle.take());
        let final_handle = {
            let mut state = self
                .context
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ensure_handle_count(&mut state.handles);
            state.handles == 0
        };
        if final_handle {
            let _ = self.runtime.finish_detach(&self.context);
        }
    }
}

fn ensure_handle_count(handles: &mut usize) {
    assert!(*handles > 0, "owned session accounting underflow");
    *handles -= 1;
}
