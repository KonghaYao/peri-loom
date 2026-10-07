use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use turso_core::{Connection, Value};

use super::admission::{
    AdmissionError, SessionControl, SessionWriterState, UnsettledSqlOutcome, WriterAdmission,
};
use super::budget::{Budget, RequestDeadline};
use super::execution;
use crate::database::RemoteDatabase;

pub(crate) struct SessionLane {
    pub control: Arc<SessionControl>,
    pub(crate) connection: Mutex<Option<Arc<Connection>>>,
    database: Mutex<Option<Arc<RemoteDatabase>>>,
    closed: AtomicBool,
    retired_unsettled: AtomicBool,
}

impl SessionLane {
    pub fn new(connection: Arc<Connection>, admission: &WriterAdmission) -> Result<Self> {
        Ok(Self {
            connection: Mutex::new(Some(connection)),
            database: Mutex::new(None),
            control: admission.register()?,
            closed: AtomicBool::new(false),
            retired_unsettled: AtomicBool::new(false),
        })
    }

    pub fn attach_database(&self, database: Arc<RemoteDatabase>) -> Result<()> {
        *self
            .database
            .lock()
            .map_err(|_| AdmissionError::Quarantine)? = Some(database);
        Ok(())
    }

    pub fn database(&self) -> Result<Arc<RemoteDatabase>> {
        self.database
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .as_ref()
            .cloned()
            .ok_or_else(|| AdmissionError::Quarantine.into())
    }

    pub(crate) fn mark_retired_unsettled(&self) {
        self.retired_unsettled.store(true, Ordering::Release);
        self.control.closing.store(true, Ordering::Release);
    }

    pub(crate) fn detach_database(&self) -> Result<Option<Arc<RemoteDatabase>>> {
        Ok(self
            .database
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take())
    }

    fn lock<'lane>(
        &'lane self,
        database: &RemoteDatabase,
        deadline: &RequestDeadline,
        cleanup: bool,
    ) -> Result<MutexGuard<'lane, Option<Arc<Connection>>>> {
        loop {
            deadline.check()?;
            if !cleanup {
                self.check(database)?;
                database.storage().store().validate_local()?;
            }
            match self.connection.try_lock() {
                Ok(connection) => {
                    if connection.is_none() {
                        return Err(AdmissionError::Quarantine.into());
                    }
                    return Ok(connection);
                }
                Err(TryLockError::WouldBlock) => thread::sleep(Duration::from_millis(10)),
                Err(TryLockError::Poisoned(_)) => {
                    database.admission().quarantine();
                    return Err(AdmissionError::Quarantine.into());
                }
            }
        }
    }

    pub fn check(&self, database: &RemoteDatabase) -> Result<()> {
        if self.control.closing.load(Ordering::Acquire) {
            return Err(AdmissionError::Closing.into());
        }
        database.admission().healthy()
    }

    pub fn execute(&self, database: &RemoteDatabase, sql: &str, budget: &Budget) -> Result<()> {
        let deadline = RequestDeadline::new(budget)?;
        let connection = self.lock(database, &deadline, false)?;
        let result = execution::execute(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            sql,
            budget,
            &deadline,
        );
        drop(connection);
        self.finish_call(database, result)
    }

    pub fn query(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        budget: &Budget,
    ) -> Result<Vec<Vec<Value>>> {
        let deadline = RequestDeadline::new(budget)?;
        let connection = self.lock(database, &deadline, false)?;
        let result = execution::query(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            sql,
            budget,
            &deadline,
        );
        drop(connection);
        self.finish_call(database, result)
    }

    fn finish_call<Output>(
        &self,
        database: &RemoteDatabase,
        result: Result<Output>,
    ) -> Result<Output> {
        if (result
            .as_ref()
            .err()
            .is_some_and(|error| error.downcast_ref::<UnsettledSqlOutcome>().is_some())
            || database.admission().healthy().is_err())
            && database
                .cleanup_deferred_until(std::time::Instant::now() + Duration::from_secs(20))
                .is_err()
        {
            if let Some(failure) = database.cleanup_failure() {
                return result.map_err(|error| error.context(failure));
            }
        }
        result
    }

    pub fn in_transaction(&self, database: &RemoteDatabase) -> Result<bool> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        self.check(database)?;
        database.storage().store().validate_session()?;
        Ok(!self
            .lock(database, &deadline, false)?
            .as_ref()
            .ok_or(AdmissionError::Quarantine)?
            .get_auto_commit())
    }

    pub fn is_readonly(&self, database: &RemoteDatabase) -> Result<bool> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        self.check(database)?;
        database.storage().store().validate_session()?;
        Ok(self
            .lock(database, &deadline, false)?
            .as_ref()
            .ok_or(AdmissionError::Quarantine)?
            .is_readonly(0))
    }

    pub fn hint(&self) -> SessionWriterState {
        self.control.hint()
    }

    pub fn begin_close(&self, database: &RemoteDatabase) {
        database.admission().close_requested(&self.control);
    }

    pub fn close(&self, database: &RemoteDatabase) -> Result<()> {
        self.close_until(database, &RequestDeadline::new(&Budget::trusted_default())?)
    }

    pub fn close_until(&self, database: &RemoteDatabase, deadline: &RequestDeadline) -> Result<()> {
        self.begin_close(database);
        if self.retired_unsettled.load(Ordering::Acquire) {
            return Err(UnsettledSqlOutcome.into());
        }
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        database.admission().healthy()?;
        let connection = match self.lock(database, deadline, true) {
            Ok(connection) => connection,
            Err(error) => {
                database.admission().quarantine();
                return Err(error);
            }
        };
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _operation = database.operation_until(deadline, true)?;
        let native = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        super::configure(native);
        if let Err(error) = native.close() {
            database.admission().quarantine();
            return Err(error.into());
        }
        let pager = native.get_pager();
        if !native.is_closed()
            || native.is_in_write_tx()
            || pager.holds_write_lock()
            || pager.holds_read_lock()
        {
            database.admission().quarantine();
            return Err(AdmissionError::Quarantine.into());
        }
        database.admission().settle(&self.control, false)?;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    pub fn maintenance_connection(
        &self,
        deadline: &RequestDeadline,
        database: &RemoteDatabase,
    ) -> Result<MutexGuard<'_, Option<Arc<Connection>>>> {
        self.lock(database, deadline, true)
    }
}
