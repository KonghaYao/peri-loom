use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use turso_core::{Connection, SyncMode, Value};

use crate::database::RemoteDatabase;

mod admission;
mod budget;
mod drive;
mod evidence;
mod execution;
mod execution_clock;
pub(crate) use evidence::SqlEvidence;
mod lane;
pub(crate) mod lease_window;
pub use admission::{AdmissionError, AdmissionStatus};
pub(crate) use admission::{SessionWriterState, UnsettledSqlOutcome, WriterAdmission};
pub use budget::Budget;
pub(crate) use budget::RequestDeadline;
pub(crate) use lane::SessionLane;

pub struct StatementColumn {
    pub name: String,
    pub decltype: Option<String>,
}

pub struct StatementResult {
    pub cols: Vec<StatementColumn>,
    pub rows: Vec<Vec<Value>>,
    pub affected_row_count: i64,
    pub last_insert_rowid: Option<i64>,
    pub autocommit: bool,
}

pub struct WriteCursorPage {
    pub rows: Vec<Vec<Value>>,
    pub done: bool,
    pub affected_row_count: Option<i64>,
    pub last_insert_rowid: Option<i64>,
}

pub struct DescribeResult {
    pub params: Vec<Option<String>>,
    pub cols: Vec<StatementColumn>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

pub struct RemoteSession {
    pub(crate) lane: Arc<SessionLane>,
}

impl RemoteSession {
    pub(crate) fn new(database: Arc<RemoteDatabase>, connection: Arc<Connection>) -> Result<Self> {
        connection.set_busy_timeout(Duration::from_secs(4));
        let lane = Arc::new(SessionLane::new(connection, database.admission())?);
        database.register_lane(&lane)?;
        lane.attach_database(database)?;
        Ok(Self { lane })
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.execute_with_budget(sql, &Budget::trusted_default())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.query_with_budget(sql, &Budget::trusted_default())
    }

    pub fn statement(&self, sql: &str, args: &[Value]) -> Result<StatementResult> {
        self.statement_with_budget(sql, args, &Budget::trusted_default())
    }

    pub fn describe(&self, sql: &str) -> Result<DescribeResult> {
        self.lane.describe(
            self.lane.database()?.as_ref(),
            sql,
            &Budget::trusted_default(),
        )
    }

    pub fn open_read_cursor(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<Vec<StatementColumn>> {
        let mut budget = Budget::trusted_default();
        budget.max_query_duration = budget.max_query_duration.max(Duration::from_secs(30));
        self.lane.open_read_cursor(
            self.lane.database()?.as_ref(),
            sql,
            args,
            named_args,
            &budget,
        )
    }

    pub fn fetch_read_cursor(&self, page_size: usize) -> Result<(Vec<Vec<Value>>, bool)> {
        self.lane
            .fetch_read_cursor(self.lane.database()?.as_ref(), page_size)
    }

    pub fn close_read_cursor(&self) -> Result<()> {
        self.lane.close_read_cursor(self.lane.database()?.as_ref())
    }

    pub fn open_write_cursor(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
    ) -> Result<Vec<StatementColumn>> {
        let mut budget = Budget::trusted_default();
        budget.max_query_duration = budget.max_query_duration.max(Duration::from_secs(30));
        self.lane.open_write_cursor(
            self.lane.database()?.as_ref(),
            sql,
            args,
            named_args,
            &budget,
        )
    }

    pub fn fetch_write_cursor(&self, page_size: usize) -> Result<WriteCursorPage> {
        self.lane
            .fetch_write_cursor(self.lane.database()?.as_ref(), page_size)
    }

    pub fn close_write_cursor(&self) -> Result<()> {
        self.lane.close_write_cursor(self.lane.database()?.as_ref())
    }

    pub(crate) fn quarantine_unknown_write_result(&self) -> Result<()> {
        self.lane.database()?.admission().quarantine();
        Ok(())
    }

    pub fn statement_with_budget(
        &self,
        sql: &str,
        args: &[Value],
        budget: &Budget,
    ) -> Result<StatementResult> {
        self.statement_with_named_args_and_budget(sql, args, &[], true, budget)
    }

    pub fn statement_with_named_args(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
    ) -> Result<StatementResult> {
        self.statement_with_named_args_and_budget(
            sql,
            args,
            named_args,
            want_rows,
            &Budget::trusted_default(),
        )
    }

    fn statement_with_named_args_and_budget(
        &self,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
        budget: &Budget,
    ) -> Result<StatementResult> {
        self.lane.statement(
            self.lane.database()?.as_ref(),
            sql,
            args,
            named_args,
            want_rows,
            budget,
        )
    }

    pub fn execute_with_budget(&self, sql: &str, budget: &Budget) -> Result<()> {
        self.lane
            .execute(self.lane.database()?.as_ref(), sql, budget)
    }

    pub fn query_with_budget(&self, sql: &str, budget: &Budget) -> Result<Vec<Vec<Value>>> {
        self.lane.query(self.lane.database()?.as_ref(), sql, budget)
    }

    pub fn in_transaction(&self) -> Result<bool> {
        self.lane.in_transaction(self.lane.database()?.as_ref())
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.lane.is_readonly(self.lane.database()?.as_ref())
    }

    pub fn autocommit(&self, budget: &Budget) -> Result<bool> {
        self.lane.autocommit(self.lane.database()?.as_ref(), budget)
    }

    #[doc(hidden)]
    pub fn admission_id(&self) -> u64 {
        self.lane.control.id
    }

    pub(crate) fn writer_state(&self) -> Result<SessionWriterState> {
        self.lane.database()?.admission().healthy()?;
        Ok(self.lane.hint())
    }

    pub(crate) fn begin_close(&self) -> Result<()> {
        self.lane.begin_close(self.lane.database()?.as_ref());
        Ok(())
    }

    pub(crate) fn request_close(&self) -> Result<()> {
        let database = self
            .lane
            .database()
            .map_err(|error| error.context(UnsettledSqlOutcome))?;
        let result = self.lane.close(&database);
        if result.is_err() {
            database.admission().quarantine();
            if database
                .cleanup_deferred_until(std::time::Instant::now() + Duration::from_secs(20))
                .is_err()
            {
                if let Some(failure) = database.cleanup_failure() {
                    return result
                        .map_err(|error| error.context(UnsettledSqlOutcome).context(failure));
                }
            }
        }
        result.map_err(|error| error.context(UnsettledSqlOutcome))
    }

    pub(crate) fn request_close_until(&self, deadline: std::time::Instant) -> Result<()> {
        let database = self
            .lane
            .database()
            .map_err(|error| error.context(UnsettledSqlOutcome))?;
        let result = self
            .lane
            .close_until(&database, &RequestDeadline::until(deadline));
        if result.is_err() {
            database.admission().quarantine();
            if database.cleanup_deferred_until(deadline).is_err() {
                if let Some(failure) = database.cleanup_failure() {
                    return result
                        .map_err(|error| error.context(UnsettledSqlOutcome).context(failure));
                }
            }
        }
        result.map_err(|error| error.context(UnsettledSqlOutcome))
    }

    #[doc(hidden)]
    pub fn close(&self) -> Result<()> {
        self.request_close()
    }
}

impl Drop for RemoteSession {
    fn drop(&mut self) {
        let _ = self.request_close();
    }
}

pub(crate) fn configure(connection: &Connection) {
    connection.set_sync_mode(SyncMode::Full);
    connection.wal_auto_actions_disable();
    connection.set_load_extension_enabled(false);
}
