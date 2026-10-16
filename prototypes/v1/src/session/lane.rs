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
use super::execution::{self, ReadCursor, WriteCursor};
use super::{DescribeResult, StatementResult, WriteCursorPage};
use crate::database::RemoteDatabase;

pub(crate) struct SessionLane {
    pub control: Arc<SessionControl>,
    pub(crate) connection: Mutex<Option<Arc<Connection>>>,
    read_cursor: Mutex<Option<ReadCursor>>,
    write_cursor: Mutex<Option<WriteCursor>>,
    database: Mutex<Option<Arc<RemoteDatabase>>>,
    closed: AtomicBool,
    retired_unsettled: AtomicBool,
}

impl SessionLane {
    pub fn new(connection: Arc<Connection>, admission: &WriterAdmission) -> Result<Self> {
        Ok(Self {
            connection: Mutex::new(Some(connection)),
            read_cursor: Mutex::new(None),
            write_cursor: Mutex::new(None),
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
                    if !cleanup
                        && self
                            .read_cursor
                            .lock()
                            .map_err(|_| AdmissionError::Quarantine)?
                            .is_some()
                    {
                        anyhow::bail!("read cursor is active on this session");
                    }
                    if !cleanup
                        && self
                            .write_cursor
                            .lock()
                            .map_err(|_| AdmissionError::Quarantine)?
                            .is_some()
                    {
                        anyhow::bail!("write cursor is active on this session");
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
        let _reads = crate::authority::request_reads::Scope::enter();
        // A write must never reach the store in a cycle that has not validated
        // the lease; when the window is already satisfied this is free.
        if !crate::session::lease_window::satisfied() {
            database.storage().store().validate_session()?;
        }
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
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
            .map_err(|error| deadline.evidence.failure(error))
    }

    pub fn query(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        budget: &Budget,
    ) -> Result<Vec<Vec<Value>>> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
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
            .map_err(|error| deadline.evidence.failure(error))
    }

    pub fn describe(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        budget: &Budget,
    ) -> Result<DescribeResult> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let result = execution::describe(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            sql,
            budget,
            &deadline,
        );
        drop(connection);
        self.finish_call(database, result)
            .map_err(|error| deadline.evidence.failure(error))
    }

    pub fn statement(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
        budget: &Budget,
    ) -> Result<StatementResult> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let result = execution::statement(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            sql,
            args,
            named_args,
            want_rows,
            budget,
            &deadline,
        );
        drop(connection);
        self.finish_call(database, result)
            .map_err(|error| deadline.evidence.failure(error))
    }

    pub fn open_read_cursor(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        budget: &Budget,
    ) -> Result<Vec<super::StatementColumn>> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let result = execution::open_read_cursor(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            sql,
            args,
            named_args,
            budget,
        )
        .and_then(|(cursor, cols)| {
            database.admission().settle(
                &self.control,
                connection
                    .as_ref()
                    .ok_or(AdmissionError::Quarantine)?
                    .is_in_write_tx(),
            )?;
            let mut slot = self
                .read_cursor
                .lock()
                .map_err(|_| AdmissionError::Quarantine)?;
            *slot = Some(cursor);
            Ok(cols)
        });
        drop(connection);
        self.finish_call(database, result)
            .map_err(|error| deadline.evidence.failure(error))
    }

    pub fn fetch_read_cursor(
        &self,
        database: &RemoteDatabase,
        page_size: usize,
    ) -> Result<(Vec<Vec<Value>>, bool)> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let connection = self.lock(database, &deadline, true)?;
        self.check(database)?;
        database.storage().store().validate_local()?;
        let native = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        let mut cursor = self
            .read_cursor
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take()
            .ok_or_else(|| anyhow::anyhow!("read cursor not open"))?;
        let result =
            execution::fetch_read_cursor(database, native, &self.control, &mut cursor, page_size);
        let result = match result {
            Ok((rows, true)) => {
                drop(cursor);
                database.admission().settle(
                    &self.control,
                    native.is_in_write_tx() || native.get_pager().holds_write_lock(),
                )?;
                Ok((rows, true))
            }
            Ok((rows, false)) => {
                if let Err(error) = database.admission().settle(
                    &self.control,
                    native.is_in_write_tx() || native.get_pager().holds_write_lock(),
                ) {
                    let _ = execution::cancel_read_cursor(database, native, &self.control, cursor);
                    return Err(error.context(UnsettledSqlOutcome));
                }
                *self
                    .read_cursor
                    .lock()
                    .map_err(|_| AdmissionError::Quarantine)? = Some(cursor);
                Ok((rows, false))
            }
            Err(error) => {
                if execution::cancel_read_cursor(database, native, &self.control, cursor).is_err() {
                    Err(error.context(UnsettledSqlOutcome))
                } else {
                    // 取消已收束：失败原因不再是未决结果，证据取代猜测。
                    Err(error.context(super::SqlEvidence::Settled))
                }
            }
        };
        drop(connection);
        self.finish_call(database, result)
    }

    pub fn close_read_cursor(&self, database: &RemoteDatabase) -> Result<()> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let connection = self.lock(database, &deadline, true)?;
        let cursor = self
            .read_cursor
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take();
        // A drained read releases its slot when the read finishes, so a close
        // after `done` and a repeated close both have nothing left to release.
        // Treating them as success keeps the documented open/fetch/close cycle
        // usable; the postcondition of close is already satisfied.
        let Some(cursor) = cursor else {
            drop(connection);
            return self.finish_call(database, Ok(()));
        };
        let result = execution::cancel_read_cursor(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            cursor,
        );
        drop(connection);
        self.finish_call(database, result)
    }

    pub fn open_write_cursor(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        budget: &Budget,
    ) -> Result<Vec<super::StatementColumn>> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let result = execution::open_write_cursor(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            sql,
            args,
            named_args,
            budget,
        )
        .and_then(|(cursor, cols)| {
            database.admission().settle(
                &self.control,
                connection
                    .as_ref()
                    .ok_or(AdmissionError::Quarantine)?
                    .is_in_write_tx(),
            )?;
            *self
                .write_cursor
                .lock()
                .map_err(|_| AdmissionError::Quarantine)? = Some(cursor);
            Ok(cols)
        });
        drop(connection);
        self.finish_call(database, result)
            .map_err(|error| deadline.evidence.failure(error))
    }

    pub fn fetch_write_cursor(
        &self,
        database: &RemoteDatabase,
        page_size: usize,
    ) -> Result<WriteCursorPage> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let connection = self.lock(database, &deadline, true).map_err(|error| {
            database.admission().quarantine();
            error.context(UnsettledSqlOutcome)
        })?;
        let native = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        let mut cursor = self
            .write_cursor
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take()
            .ok_or_else(|| anyhow::anyhow!("write cursor not open"))?;
        let result = self
            .check(database)
            .and_then(|()| database.storage().store().validate_local())
            .and_then(|()| {
                execution::fetch_write_cursor(
                    database,
                    native,
                    &self.control,
                    &mut cursor,
                    page_size,
                )
            });
        let result = match result {
            Ok(page) if page.done => {
                if let Err(error) =
                    execution::settle_write_cursor(database, native, &self.control, cursor)
                {
                    database.admission().quarantine();
                    Err(error.context(UnsettledSqlOutcome))
                } else {
                    Ok(page)
                }
            }
            Ok(page) => {
                if let Err(error) = database.admission().settle(
                    &self.control,
                    native.is_in_write_tx() || native.get_pager().holds_write_lock(),
                ) {
                    let _ = execution::cancel_write_cursor(database, native, &self.control, cursor);
                    database.admission().quarantine();
                    Err(error.context(UnsettledSqlOutcome))
                } else {
                    *self
                        .write_cursor
                        .lock()
                        .map_err(|_| AdmissionError::Quarantine)? = Some(cursor);
                    Ok(page)
                }
            }
            Err(error) => {
                if execution::cancel_write_cursor(database, native, &self.control, cursor).is_err()
                {
                    Err(error.context(UnsettledSqlOutcome))
                } else {
                    // 取消已收束：失败原因不再是未决结果，证据取代猜测。
                    Err(error.context(super::SqlEvidence::Settled))
                }
            }
        };
        drop(connection);
        self.finish_call(database, result)
    }

    pub fn close_write_cursor(&self, database: &RemoteDatabase) -> Result<()> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let connection = self.lock(database, &deadline, true).map_err(|error| {
            database.admission().quarantine();
            error.context(UnsettledSqlOutcome)
        })?;
        let cursor = self
            .write_cursor
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take();
        // Closing a write cursor that already finished (or was already closed)
        // only has to reach the same postcondition: the slot is free, and the
        // write outcome was already reported by the fetch that finished it.
        let Some(cursor) = cursor else {
            drop(connection);
            return self.finish_call(database, Ok(()));
        };
        let result = execution::cancel_write_cursor(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            cursor,
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

    // A protocol condition asks whether this connection is in autocommit, and a
    // SQL probe answers it as a statement the platform then has to account for.
    // The read keeps the same session guard, lease validation, isolation and
    // deadline the statement path enforces, and prepares nothing.
    pub fn autocommit(&self, database: &RemoteDatabase, budget: &Budget) -> Result<bool> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        self.check(database)?;
        database.storage().store().validate_session()?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let result = Ok(connection
            .as_ref()
            .ok_or(AdmissionError::Quarantine)?
            .get_auto_commit());
        drop(connection);
        self.finish_call(database, result)
            .map_err(|error| deadline.evidence.failure(error))
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
        let native = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        if let Some(cursor) = self
            .read_cursor
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take()
        {
            execution::cancel_read_cursor(database, native, &self.control, cursor)?;
        }
        if let Some(cursor) = self
            .write_cursor
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take()
        {
            execution::cancel_write_cursor(database, native, &self.control, cursor)?;
        }
        let _operation = database.operation_until(deadline, true)?;
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
