use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use turso_core::{Connection, SyncMode, Value};

use crate::database::RemoteDatabase;

mod admission;
mod budget;
mod drive;
mod execution;
mod lane;
pub use admission::{AdmissionError, AdmissionStatus};
pub(crate) use admission::{SessionWriterState, UnsettledSqlOutcome, WriterAdmission};
pub use budget::Budget;
pub(crate) use budget::RequestDeadline;
pub(crate) use lane::SessionLane;

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
