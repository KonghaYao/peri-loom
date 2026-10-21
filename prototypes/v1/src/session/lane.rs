use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

use anyhow::Result;
use turso_core::{Connection, Value};

use super::admission::{
    AdmissionError, SessionControl, SessionWriterState, UnsettledSqlOutcome, WriterAdmission,
};
use super::budget::{Budget, RequestDeadline};
use super::execution::{self, ReadCursor, WriteCursor};
use super::notify::GateNotify;
use super::{DescribeResult, GuardedStatementOutcome, StatementResult, WriteCursorPage};
use crate::database::RemoteDatabase;
use crate::work_metrics::{GateDomain, LaneWait};

pub(crate) struct SessionLane {
    pub control: Arc<SessionControl>,
    pub(crate) connection: Mutex<Option<Arc<Connection>>>,
    /// Wakes lockers after a release, a close request or a quarantine. Polling the connection
    /// mutex every 10ms bounded the wait only to the next tick, and try_lock cannot enforce
    /// the request deadline while the guard is held.
    notify: GateNotify,
    read_cursor: Mutex<Option<ReadCursor>>,
    write_cursor: Mutex<Option<WriteCursor>>,
    database: Mutex<Option<Arc<RemoteDatabase>>>,
    closed: AtomicBool,
    retired_unsettled: AtomicBool,
    /// When this session opened. Used as an upper bound for the maintenance reader age.
    opened: std::time::Instant,
}

/// The lane connection guard. Releasing the connection and waking waiters are one operation:
/// the notification must be sent after the mutex is free, otherwise a woken waiter would fail
/// to acquire it and go back to sleep with no further release to wake it.
pub(crate) struct LaneConnectionGuard<'lane> {
    connection: Option<MutexGuard<'lane, Option<Arc<Connection>>>>,
    notify: &'lane GateNotify,
}

impl<'lane> LaneConnectionGuard<'lane> {
    fn new(
        connection: MutexGuard<'lane, Option<Arc<Connection>>>,
        notify: &'lane GateNotify,
    ) -> Self {
        Self {
            connection: Some(connection),
            notify,
        }
    }
}

impl Deref for LaneConnectionGuard<'_> {
    type Target = Option<Arc<Connection>>;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("lane connection guard released")
    }
}

impl DerefMut for LaneConnectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection
            .as_mut()
            .expect("lane connection guard released")
    }
}

impl Drop for LaneConnectionGuard<'_> {
    fn drop(&mut self) {
        drop(self.connection.take());
        self.notify.notify_all();
    }
}

/// How long this lane waited for the connection before its deadline expired. Recorded beside
/// the error, not in its chain: the client-visible message stays identical with observation on.
fn waited_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

impl SessionLane {
    pub fn new(connection: Arc<Connection>, admission: &WriterAdmission) -> Result<Self> {
        Ok(Self {
            connection: Mutex::new(Some(connection)),
            notify: GateNotify::new(),
            read_cursor: Mutex::new(None),
            write_cursor: Mutex::new(None),
            database: Mutex::new(None),
            control: admission.register()?,
            closed: AtomicBool::new(false),
            retired_unsettled: AtomicBool::new(false),
            opened: std::time::Instant::now(),
        })
    }

    pub(crate) fn opened(&self) -> std::time::Instant {
        self.opened
    }

    /// Whether this session currently holds a native read lock. The connection guard is
    /// released between calls, but an open transaction keeps the pager's read lock, which is
    /// exactly the reader a truncate checkpoint cannot backfill past. A busy lane is reported
    /// as not holding one: it is executing, so it cannot be the long reader a maintenance unit
    /// would be blocked by, and reporting it would make the age meaningless.
    pub(crate) fn holds_read_lock(&self) -> bool {
        self.connection.try_lock().is_ok_and(|connection| {
            connection
                .as_ref()
                .is_some_and(|connection| connection.get_pager().holds_read_lock())
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
        self.notify.notify_all();
    }

    pub(crate) fn detach_database(&self) -> Result<Option<Arc<RemoteDatabase>>> {
        Ok(self
            .database
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .take())
    }

    /// Takes the lane connection, waiting for a release instead of polling.
    ///
    /// The attempt is always a `try_lock`: the absolute deadline is enforced by
    /// [`GateNotify::wait`], and the success path stays the same uncontended mutex
    /// acquisition it was before. A newcomer defers while another thread is registered as a
    /// waiter so it cannot consume the release that thread is about to wake for; a thread
    /// that already waited always tries, otherwise two woken waiters could each treat the
    /// other as first and sleep again.
    fn lock<'lane>(
        &'lane self,
        database: &RemoteDatabase,
        deadline: &RequestDeadline,
        cleanup: bool,
    ) -> Result<LaneConnectionGuard<'lane>> {
        let mut blocked_at = None;
        loop {
            let checked = if cleanup {
                deadline.check()
            } else {
                deadline
                    .check()
                    .and_then(|()| self.check(database))
                    .and_then(|()| database.storage().store().validate_local())
            };
            if let Err(error) = checked {
                self.record_failed_wait(blocked_at);
                return Err(error);
            }
            let seen = self.notify.generation();
            let may_try = blocked_at.is_some() || self.notify.waiters() == 0;
            if may_try {
                match self.connection.try_lock() {
                    Ok(connection) => {
                        // Wrap before the cursor checks so an early return also releases the
                        // connection through Drop and wakes the next waiter.
                        let connection = LaneConnectionGuard::new(connection, &self.notify);
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
                        if let Some(started) = blocked_at {
                            crate::work_metrics::observe_gate_wait_done(
                                GateDomain::Lane,
                                started.elapsed(),
                            );
                        }
                        return Ok(connection);
                    }
                    Err(TryLockError::WouldBlock) => {}
                    Err(TryLockError::Poisoned(_)) => {
                        self.quarantine(database);
                        self.record_failed_wait(blocked_at);
                        return Err(AdmissionError::Quarantine.into());
                    }
                }
            }
            if blocked_at.is_none() {
                blocked_at = Some(Instant::now());
                crate::work_metrics::observe_gate_wait(GateDomain::Lane);
            }
            if let Err(error) = self.notify.wait(seen, deadline) {
                self.record_failed_wait(blocked_at);
                return Err(error);
            }
        }
    }

    /// A wait that ends without the connection is a timeout or a cancellation (close or
    /// quarantine); both are counted once, and the per-request diagnostic keeps the same
    /// shape it had for the polling implementation.
    fn record_failed_wait(&self, blocked_at: Option<Instant>) {
        if let Some(started) = blocked_at {
            crate::work_metrics::note_lane_wait(LaneWait {
                waited_ns: waited_ns(started),
            });
            crate::work_metrics::observe_gate_wait_timeout(GateDomain::Lane);
        }
    }

    fn quarantine(&self, database: &RemoteDatabase) {
        database.admission().quarantine();
        self.notify.notify_all();
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

    pub fn guarded_readonly_statement(
        &self,
        database: &RemoteDatabase,
        sql: &str,
        args: &[Value],
        named_args: &[(String, Value)],
        want_rows: bool,
        budget: &Budget,
    ) -> Result<GuardedStatementOutcome> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let deadline = RequestDeadline::new(budget)?;
        let connection = self
            .lock(database, &deadline, false)
            .map_err(|error| deadline.evidence.failure(error))?;
        let result = execution::guarded_readonly_statement(
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
            self.quarantine(database);
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
                    self.quarantine(database);
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
                    self.quarantine(database);
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

    /// Closes the write cursor and reports whether a live cursor was actually
    /// aborted. `true` is the native rollback proof: `cancel_write_cursor` only
    /// succeeds when `abort_noncommitting` returned `StatementStopped`. `false`
    /// means there was no cursor left to abort (it already finished or was never
    /// opened), so a caller must not read this as a rollback.
    pub fn close_write_cursor(&self, database: &RemoteDatabase) -> Result<bool> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let connection = self.lock(database, &deadline, true).map_err(|error| {
            self.quarantine(database);
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
            return self.finish_call(database, Ok(false));
        };
        let result = execution::cancel_write_cursor(
            database,
            connection.as_ref().ok_or(AdmissionError::Quarantine)?,
            &self.control,
            cursor,
        );
        drop(connection);
        self.finish_call(database, result).map(|()| true)
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
        // A lock waiter checks `closing` before every attempt; waking it here turns a session
        // close into a prompt cancellation instead of a wait for the current guard's deadline.
        self.notify.notify_all();
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
                self.quarantine(database);
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
            self.quarantine(database);
            return Err(error.into());
        }
        let pager = native.get_pager();
        if !native.is_closed()
            || native.is_in_write_tx()
            || pager.holds_write_lock()
            || pager.holds_read_lock()
        {
            self.quarantine(database);
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
    ) -> Result<LaneConnectionGuard<'_>> {
        self.lock(database, deadline, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::Namespace;
    use crate::object_store::MemoryObjectStore;
    use std::sync::mpsc;

    fn open(name: &str) -> Result<(tempfile::TempDir, Arc<RemoteDatabase>)> {
        let cache = tempfile::tempdir()?;
        let database = Arc::new(RemoteDatabase::open(
            Arc::new(MemoryObjectStore::default()),
            Namespace::new("gate", name)?,
            cache.path(),
            0,
        )?);
        Ok((cache, database))
    }

    fn deadline_after(duration: Duration) -> RequestDeadline {
        RequestDeadline::until(Instant::now() + duration)
    }

    /// Polls the registration counter rather than sleeping a fixed amount: the test only
    /// makes sense once the worker is really inside the notification wait.
    fn wait_until_registered(lane: &SessionLane, expected: usize) {
        let started = Instant::now();
        while lane.notify.waiters() < expected {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "waiter did not register: expected {expected}, saw {}",
                lane.notify.waiters()
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn a_release_wakes_the_waiter_before_its_deadline() -> Result<()> {
        let (_cache, database) = open("lane_wake")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let held = lane.lock(&database, &deadline_after(Duration::from_secs(2)), false)?;

        let (sender, receiver) = mpsc::channel();
        let worker_lane = lane.clone();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            let result = worker_lane
                .lock(
                    &worker_database,
                    &deadline_after(Duration::from_secs(2)),
                    false,
                )
                .map(|_| ());
            sender.send(result).unwrap();
        });
        wait_until_registered(&lane, 1);
        drop(held);
        receiver.recv_timeout(Duration::from_secs(1))??;
        worker.join().unwrap();
        Ok(())
    }

    #[test]
    fn a_waiter_times_out_at_its_absolute_deadline() -> Result<()> {
        let (_cache, database) = open("lane_timeout")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let held = lane.lock(&database, &deadline_after(Duration::from_secs(2)), false)?;

        let started = Instant::now();
        let error = lane
            .lock(&database, &deadline_after(Duration::from_millis(80)), false)
            .err()
            .expect("a held lane must time out");
        assert_eq!(
            error.downcast_ref::<AdmissionError>(),
            Some(&AdmissionError::Deadline)
        );
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(80));
        assert!(
            waited < Duration::from_secs(1),
            "timed out late: {waited:?}"
        );
        assert_eq!(
            lane.notify.waiters(),
            0,
            "a timed-out waiter must leave no registration"
        );
        drop(held);
        Ok(())
    }

    #[test]
    fn close_wakes_a_cancelled_waiter() -> Result<()> {
        let (_cache, database) = open("lane_close")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let held = lane.lock(&database, &deadline_after(Duration::from_secs(2)), false)?;

        let (sender, receiver) = mpsc::channel();
        let worker_lane = lane.clone();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            let result = worker_lane
                .lock(
                    &worker_database,
                    &deadline_after(Duration::from_secs(5)),
                    false,
                )
                .map(|_| ());
            sender.send(result).unwrap();
        });
        wait_until_registered(&lane, 1);
        lane.begin_close(&database);
        match receiver.recv_timeout(Duration::from_secs(1))? {
            Ok(()) => panic!("close must cancel the wait"),
            Err(error) => assert_eq!(
                error.downcast_ref::<AdmissionError>(),
                Some(&AdmissionError::Closing)
            ),
        }
        worker.join().unwrap();
        drop(held);
        Ok(())
    }

    #[test]
    fn quarantine_wakes_a_waiter_with_a_terminal_error() -> Result<()> {
        let (_cache, database) = open("lane_quarantine")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let held = lane.lock(&database, &deadline_after(Duration::from_secs(2)), false)?;

        let (sender, receiver) = mpsc::channel();
        let worker_lane = lane.clone();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            let result = worker_lane
                .lock(
                    &worker_database,
                    &deadline_after(Duration::from_secs(5)),
                    false,
                )
                .map(|_| ());
            sender.send(result).unwrap();
        });
        wait_until_registered(&lane, 1);
        lane.quarantine(&database);
        match receiver.recv_timeout(Duration::from_secs(1))? {
            Ok(()) => panic!("quarantine must cancel the wait"),
            Err(error) => assert!(matches!(
                error.downcast_ref::<AdmissionError>(),
                Some(AdmissionError::Closing | AdmissionError::Quarantine)
            )),
        }
        worker.join().unwrap();
        drop(held);
        Ok(())
    }

    #[test]
    fn waiters_advance_one_release_at_a_time() -> Result<()> {
        let (_cache, database) = open("lane_fair")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let held = lane.lock(&database, &deadline_after(Duration::from_secs(2)), false)?;

        let mut receivers = Vec::new();
        let mut workers = Vec::new();
        for _ in 0..4 {
            let (sender, receiver) = mpsc::channel();
            let worker_lane = lane.clone();
            let worker_database = database.clone();
            workers.push(std::thread::spawn(move || {
                let result = worker_lane
                    .lock(
                        &worker_database,
                        &deadline_after(Duration::from_secs(3)),
                        false,
                    )
                    .map(|_| ());
                sender.send(result).unwrap();
            }));
            receivers.push(receiver);
        }
        wait_until_registered(&lane, 4);
        drop(held);
        for receiver in receivers {
            receiver.recv_timeout(Duration::from_secs(2))??;
        }
        for worker in workers {
            worker.join().unwrap();
        }
        Ok(())
    }

    #[test]
    fn a_release_is_never_lost_across_a_thousand_cycles() -> Result<()> {
        let (_cache, database) = open("lane_no_lost")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let (wake_sender, wake_receiver) = mpsc::channel::<()>();
        let (done_sender, done_receiver) = mpsc::channel::<Result<()>>();
        let worker_lane = lane.clone();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            while wake_receiver.recv().is_ok() {
                let result = worker_lane
                    .lock(
                        &worker_database,
                        &deadline_after(Duration::from_secs(2)),
                        false,
                    )
                    .map(|_| ());
                if done_sender.send(result).is_err() {
                    return;
                }
            }
        });
        for _ in 0..1000 {
            let held = lane.lock(&database, &deadline_after(Duration::from_secs(2)), false)?;
            wake_sender.send(()).unwrap();
            wait_until_registered(&lane, 1);
            drop(held);
            done_receiver.recv_timeout(Duration::from_secs(2))??;
        }
        drop(wake_sender);
        worker.join().unwrap();
        Ok(())
    }

    #[test]
    fn an_uncontended_lane_never_enters_the_wait() -> Result<()> {
        let (_cache, database) = open("lane_fast")?;
        let session = database.session()?;
        let lane = session.lane.clone();
        let started = Instant::now();
        for _ in 0..1000 {
            drop(lane.lock(&database, &deadline_after(Duration::from_secs(1)), false)?);
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(lane.notify.waiters(), 0);
        Ok(())
    }
}
