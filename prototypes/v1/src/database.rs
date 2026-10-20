use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError, Weak};
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Context, Result};
use turso_core::io::OpenFlags;
use turso_core::{
    CheckpointMode, Connection, Database, DatabaseOpts, EncryptionOpts, OpenOptions, SqliteDialect,
    Value,
};

use crate::io::RemoteIO;
use crate::maintenance::{
    BlockReason, MaintenanceLane, MaintenanceLedger, MaintenanceSignals, MaintenanceTarget,
    MaintenanceUnitReport, UnitBudget,
};
use crate::namespace::Namespace;
use crate::object_store::ObjectStore;
use crate::ownership::types::AccessRole;
use crate::resources::ResourceUsage;
pub use crate::session::RemoteSession;
use crate::session::{
    configure, AdmissionError, AdmissionStatus, Budget, GateNotify, RequestDeadline, SessionLane,
    WriterAdmission,
};
use crate::storage::RemoteStorage;
use crate::work_metrics::GateDomain;

mod connections;
mod reclamation;
mod retirement;

pub struct RemoteDatabase {
    key: Option<crate::encryption::ResolvedDatabaseKey>,
    lane: Arc<SessionLane>,
    lanes: Mutex<Vec<Weak<SessionLane>>>,
    reclamation: reclamation::Reclamation,
    maintenance: MaintenanceLedger,
    pending: Mutex<Vec<turso_core::Statement>>,
    cleanup: AtomicBool,
    engine: Mutex<Option<Arc<Database>>>,
    operations: OperationGate,
    /// Milliseconds since the ledger's open instant at which the last foreground statement
    /// took the operations gate. The compaction lane reads it through `signals`; nothing on
    /// the request path reads it back, so it is a relaxed counter.
    last_operation_ms: AtomicU64,
    /// Serializes maintenance units with each other, not with SQL. A checkpoint runs
    /// concurrently with foreground traffic, but it must not overlap a compaction that
    /// resets the same pager and WAL state.
    maintenance_unit: Mutex<()>,
    /// Absolute frame number the last bounded checkpoint backfilled. A bounded passive
    /// checkpoint takes an inclusive frame bound, so each pass continues from this cursor
    /// instead of re-reading the frames every earlier pass already copied. It is reset when
    /// the WAL index is truncated and is meaningless after a WAL restart; the engine clamps a
    /// stale bound, so the worst case is one full pass rather than an unsafe one.
    checkpoint_cursor: AtomicU64,
    io: Arc<RemoteIO>,
    admission: Arc<WriterAdmission>,
}

/// The foreground/maintenance operations gate with event notification.
///
/// The `RwLock` still provides the mutual exclusion; the notification pair only replaces the
/// 10ms `try_lock` poll with an absolute-deadline wait. A waiter registers no permit, so a
/// timeout or cancellation leaves nothing behind.
struct OperationGate {
    operations: RwLock<()>,
    notify: GateNotify,
    /// Maintenance writers that are waiting or about to try. New readers defer while this is
    /// non-zero, so a continuous stream of short reads cannot starve a truncate checkpoint.
    writers_waiting: AtomicUsize,
}

pub(crate) struct OperationReadGuard<'gate> {
    inner: Option<RwLockReadGuard<'gate, ()>>,
    notify: &'gate GateNotify,
}

pub(crate) struct OperationWriteGuard<'gate> {
    inner: Option<RwLockWriteGuard<'gate, ()>>,
    notify: &'gate GateNotify,
}

impl OperationGate {
    fn new() -> Self {
        Self {
            operations: RwLock::new(()),
            notify: GateNotify::new(),
            writers_waiting: AtomicUsize::new(0),
        }
    }

    fn read_until<Check>(
        &self,
        deadline: &RequestDeadline,
        mut check: Check,
    ) -> Result<OperationReadGuard<'_>>
    where
        Check: FnMut() -> Result<()>,
    {
        let mut blocked_at: Option<Instant> = None;
        loop {
            if let Err(error) = check() {
                if blocked_at.is_some() {
                    crate::work_metrics::observe_gate_wait_timeout(GateDomain::Operation);
                }
                return Err(error);
            }
            let seen = self.notify.generation();
            if self.writers_waiting.load(Ordering::Acquire) == 0 {
                match self.operations.try_read() {
                    Ok(inner) => {
                        if let Some(started) = blocked_at {
                            crate::work_metrics::observe_gate_wait_done(
                                GateDomain::Operation,
                                started.elapsed(),
                            );
                        }
                        return Ok(OperationReadGuard::new(inner, &self.notify));
                    }
                    Err(TryLockError::WouldBlock) => {}
                    Err(TryLockError::Poisoned(_)) => {
                        self.notify.notify_all();
                        return Err(anyhow::Error::new(AdmissionError::Quarantine)
                            .context("database operation gate poisoned"));
                    }
                }
            }
            if blocked_at.is_none() {
                blocked_at = Some(Instant::now());
                crate::work_metrics::observe_gate_wait(GateDomain::Operation);
            }
            if let Err(error) = self.notify.wait(seen, deadline) {
                crate::work_metrics::observe_gate_wait_timeout(GateDomain::Operation);
                return Err(error);
            }
        }
    }

    fn write_until<Check>(
        &self,
        deadline: &RequestDeadline,
        mut check: Check,
    ) -> Result<OperationWriteGuard<'_>>
    where
        Check: FnMut() -> Result<()>,
    {
        // Register the intent before the first attempt: a reader that arrives between a
        // failed try_write and the wait must already see that an exclusive unit is pending.
        let _intent = WriterIntent::new(self);
        let mut blocked_at: Option<Instant> = None;
        loop {
            if let Err(error) = check() {
                if blocked_at.is_some() {
                    crate::work_metrics::observe_gate_wait_timeout(GateDomain::Operation);
                }
                return Err(error);
            }
            let seen = self.notify.generation();
            match self.operations.try_write() {
                Ok(inner) => {
                    if let Some(started) = blocked_at {
                        crate::work_metrics::observe_gate_wait_done(
                            GateDomain::Operation,
                            started.elapsed(),
                        );
                    }
                    return Ok(OperationWriteGuard::new(inner, &self.notify));
                }
                Err(TryLockError::WouldBlock) => {}
                Err(TryLockError::Poisoned(_)) => {
                    return Err(AdmissionError::Quarantine.into());
                }
            }
            if blocked_at.is_none() {
                blocked_at = Some(Instant::now());
                crate::work_metrics::observe_gate_wait(GateDomain::Operation);
            }
            if let Err(error) = self.notify.wait(seen, deadline) {
                crate::work_metrics::observe_gate_wait_timeout(GateDomain::Operation);
                return Err(error);
            }
        }
    }
}

/// Decrements the writer intent on every exit, including a timed-out or cancelled wait. The
/// notification is what lets readers that deferred for this writer try again; without it they
/// would sleep until the next unrelated release (or their deadline).
struct WriterIntent<'gate> {
    gate: &'gate OperationGate,
}

impl<'gate> WriterIntent<'gate> {
    fn new(gate: &'gate OperationGate) -> Self {
        gate.writers_waiting.fetch_add(1, Ordering::AcqRel);
        Self { gate }
    }
}

impl Drop for WriterIntent<'_> {
    fn drop(&mut self) {
        self.gate.writers_waiting.fetch_sub(1, Ordering::AcqRel);
        self.gate.notify.notify_all();
    }
}

impl<'gate> OperationReadGuard<'gate> {
    fn new(inner: RwLockReadGuard<'gate, ()>, notify: &'gate GateNotify) -> Self {
        Self {
            inner: Some(inner),
            notify,
        }
    }
}

impl<'gate> OperationWriteGuard<'gate> {
    fn new(inner: RwLockWriteGuard<'gate, ()>, notify: &'gate GateNotify) -> Self {
        Self {
            inner: Some(inner),
            notify,
        }
    }
}

impl Deref for OperationReadGuard<'_> {
    type Target = ();

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("operation read guard released")
    }
}

impl Deref for OperationWriteGuard<'_> {
    type Target = ();

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("operation write guard released")
    }
}

impl Drop for OperationReadGuard<'_> {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.notify.notify_all();
    }
}

impl Drop for OperationWriteGuard<'_> {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.notify.notify_all();
    }
}

impl RemoteDatabase {
    pub fn open(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
    ) -> Result<Self> {
        Self::open_with_limits(
            store,
            namespace,
            cache_root,
            epoch,
            8 * 1024 * 1024,
            64 * 1024 * 1024,
        )
    }

    pub fn open_with_limits(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
    ) -> Result<Self> {
        Self::open_configured(
            store, namespace, cache_root, epoch, ram_limit, disk_limit, None,
        )
    }

    pub fn open_with_encryption_and_limits(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
        key: crate::encryption::ResolvedDatabaseKey,
    ) -> Result<Self> {
        key.validate_open(&namespace, epoch, store.access_role())?;
        Self::open_configured(
            store,
            namespace,
            cache_root,
            epoch,
            ram_limit,
            disk_limit,
            Some(key),
        )
    }

    fn open_configured(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        cache_root: impl AsRef<Path>,
        epoch: u64,
        ram_limit: usize,
        disk_limit: u64,
        key: Option<crate::encryption::ResolvedDatabaseKey>,
    ) -> Result<Self> {
        let readonly = store.access_role() == AccessRole::ReadOnly;
        store.validate_transaction(false)?;
        let io = RemoteIO::open(store, namespace, cache_root, epoch, ram_limit, disk_limit)?;
        let mut options = OpenOptions::new(Arc::new(SqliteDialect)).db_opts(
            DatabaseOpts::new()
                .with_attach(false)
                .with_encryption(key.is_some()),
        );
        if let Some(key) = &key {
            options = options.encryption(EncryptionOpts {
                cipher: "aegis256".to_owned(),
                hexkey: key.hex_key().to_string(),
            });
        }
        if readonly {
            options = options.flags(OpenFlags::ReadOnly);
        }
        let database = Database::open(io.clone(), &io.database_path(), options)
            .context("open remote SQL database")?;
        io.storage().store().validate_transaction(false)?;
        let connection =
            Self::connect(&database, key.as_ref()).context("connect remote SQL database")?;
        io.storage().store().validate_transaction(false)?;
        ensure!(
            connection.is_readonly(0) == readonly,
            "native database role mismatch"
        );
        configure(&connection);
        let admission = Arc::new(WriterAdmission::default());
        let lane = Arc::new(SessionLane::new(connection, &admission)?);
        let lanes = Mutex::new(vec![Arc::downgrade(&lane)]);
        let database = Self {
            key,
            lane,
            lanes,
            reclamation: reclamation::Reclamation::default(),
            maintenance: MaintenanceLedger::default(),
            pending: Mutex::new(Vec::new()),
            cleanup: AtomicBool::new(false),
            engine: Mutex::new(Some(database)),
            operations: OperationGate::new(),
            last_operation_ms: AtomicU64::new(0),
            maintenance_unit: Mutex::new(()),
            checkpoint_cursor: AtomicU64::new(0),
            io,
            admission,
        };
        if !readonly {
            database.query("SELECT name FROM sqlite_master LIMIT 1")?;
            database.io.publish_open_header()?;
        }
        Ok(database)
    }

    fn connect(
        engine: &Arc<Database>,
        key: Option<&crate::encryption::ResolvedDatabaseKey>,
    ) -> Result<Arc<Connection>> {
        let connection = match key {
            Some(key) => engine.connect_with_encryption(Some(key.native_key()))?,
            None => engine.connect()?,
        };
        connection.set_temp_store(turso_core::TempStore::Memory)?;
        connection.set_page_cache_budget_bytes(16 * 1024 * 1024)?;
        connection.set_max_blob_bytes(8 * 1024 * 1024);
        Ok(connection)
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        self.lane.execute(self, sql, &Budget::trusted_default())
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.lane.query(self, sql, &Budget::trusted_default())
    }

    /// Passive checkpoint. This is the only entry point for the unit, so the admin RPC, the
    /// CLI and the scheduled lane all take the same path and the same admission, fencing and
    /// deadline checks. The unit records its own start, which is what makes "a commit
    /// checkpointed" observable: no request path may move that counter.
    ///
    /// Unlike compaction, a checkpoint does not take the writer admission or the operations
    /// gate. The native engine only backfills frames that no reader can still need and
    /// never blocks readers or writers itself; excluding foreground SQL here turned every
    /// scheduled backfill into a multi-second stall without adding any fence the native
    /// read marks do not already provide.
    pub fn checkpoint(&self) -> Result<()> {
        self.checkpoint_with_budget(UnitBudget::UNBOUNDED)
            .map(|_| ())
    }

    /// Bounded checkpoint entry point used by the scheduler. The admin path above keeps the
    /// unbounded budget, so an explicit operator checkpoint still drains the whole WAL.
    pub(crate) fn checkpoint_with_budget(
        &self,
        budget: UnitBudget,
    ) -> Result<MaintenanceUnitReport> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let _unit = self.maintenance_unit()?;
        self.maintenance
            .record_execution(MaintenanceLane::Checkpoint);
        let timer = crate::work_metrics::MaintenanceTimer::begin(MaintenanceLane::Checkpoint);
        let charge = self.maintenance_charge();
        let result = self.checkpoint_once(budget);
        timer.finish(charge, result.as_ref().ok().copied(), result.is_ok());
        self.finish_maintenance(result)
    }

    fn checkpoint_once(&self, budget: UnitBudget) -> Result<MaintenanceUnitReport> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        self.admission.healthy()?;
        let connection = self.lane.maintenance_connection(&deadline, self)?;
        let connection = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        self.io.check_writer()?;
        self.io.storage().store().validate_local()?;
        self.io.storage().store().validate_transaction(false)?;
        configure(connection);
        // A passive unit dispatches no client SQL and is safe to retry, so its failure is
        // recorded against the maintenance lane instead of being reported as an unsettled
        // SQL outcome. An upload that actually failed already leaves `publication_failed`
        // set, which keeps compaction and log reset closed until a later publication
        // succeeds; tagging the unit here would only schedule reclamation for work that
        // never happened.
        let upper_bound_inclusive = self.checkpoint_upper_bound(budget);
        let checkpoint = connection
            .checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive,
            })
            .context("checkpoint remote database")?;
        self.advance_checkpoint_cursor(checkpoint.wal_total_backfilled);
        let scanned_pages = checkpoint.wal_checkpoint_backfilled;
        Ok(MaintenanceUnitReport {
            scanned_pages,
            backfilled_bytes: self.frames_to_bytes(scanned_pages),
            truncated: false,
            complete: checkpoint.everything_backfilled(),
        })
    }

    /// The unit lock for maintenance work. It is a `try_lock` because a unit already in
    /// flight is a reason to retry later, not to queue more work behind it.
    fn maintenance_unit(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.maintenance_unit
            .try_lock()
            .map_err(|error| match error {
                TryLockError::WouldBlock => AdmissionError::Conflict.into(),
                TryLockError::Poisoned(_) => AdmissionError::Quarantine.into(),
            })
    }

    pub fn session(self: &Arc<Self>) -> Result<RemoteSession> {
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let _operation = self.operation_until(&deadline, false)?;
        self.admission.healthy()?;
        self.storage().store().validate_transaction(false)?;
        let engine = self
            .engine
            .lock()
            .map_err(|_| AdmissionError::Quarantine)?
            .as_ref()
            .cloned()
            .ok_or(AdmissionError::Quarantine)?;
        let connection = Self::connect(&engine, self.key.as_ref())
            .context("connect independent remote session")?;
        self.storage().store().validate_transaction(false)?;
        configure(&connection);
        deadline.check()?;
        self.admission.healthy()?;
        RemoteSession::new(self.clone(), connection)
    }

    pub fn resources(&self) -> Result<ResourceUsage> {
        self.io.resources()
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.storage().store().validate_session()?;
        self.lane.is_readonly(self)
    }

    /// Compaction entry point, shared by the admin RPC, the CLI and the scheduled lane. The
    /// preconditions are the ones this method always applied: a complete native truncate
    /// backfill and no unconfirmed WAL. A precondition that fails here records why and
    /// returns before `compact_wal` touches the WAL index, so the old references stay.
    pub fn compact_wal(&self) -> Result<()> {
        self.compact_wal_with_budget(UnitBudget::UNBOUNDED)
            .map(|_| ())
    }

    /// Bounded compaction entry point used by the scheduler. The unit is deliberately staged:
    ///
    /// 1. a passive backfill bounded by the lane's frame budget, which runs beside
    ///    foreground SQL and advances the checkpoint cursor;
    /// 2. a stop before the exclusive phase when the backfill did not cover the whole WAL or
    ///    the unit's time budget is spent, so a bounded pass never turns into a truncate;
    /// 3. the truncate checkpoint and WAL reset only after that complete backfill, under the
    ///    writer admission and operations gate as before.
    pub(crate) fn compact_wal_with_budget(
        &self,
        budget: UnitBudget,
    ) -> Result<MaintenanceUnitReport> {
        let _reads = crate::authority::request_reads::Scope::enter();
        let _unit = self.maintenance_unit()?;
        self.maintenance
            .record_execution(MaintenanceLane::IndexCompact);
        let timer = crate::work_metrics::MaintenanceTimer::begin(MaintenanceLane::IndexCompact);
        let charge = self.maintenance_charge();
        let result = self.compact_wal_once(budget);
        timer.finish(charge, result.as_ref().ok().copied(), result.is_ok());
        self.finish_maintenance(result)
    }

    /// The backfill volume a unit is acting on, read only when the counters are on. The probe
    /// is an in-memory read, but gating it keeps the switch-off path free of both the read
    /// and the clock.
    fn maintenance_charge(&self) -> u64 {
        if !crate::work_metrics::enabled() {
            return 0;
        }
        self.io
            .maintenance_signals()
            .map_or(0, |signals| signals.backfill_bytes())
    }

    fn finish_maintenance<T>(&self, result: Result<T>) -> Result<T> {
        if result.as_ref().err().is_some_and(|error| {
            error
                .downcast_ref::<crate::session::UnsettledSqlOutcome>()
                .is_some()
        }) && self
            .cleanup_deferred_until(std::time::Instant::now() + Duration::from_secs(20))
            .is_err()
        {
            if let Some(failure) = self.cleanup_failure() {
                return result.map_err(|error| error.context(failure));
            }
        }
        result
    }

    fn compact_wal_once(&self, budget: UnitBudget) -> Result<MaintenanceUnitReport> {
        let started = std::time::Instant::now();
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let upper_bound_inclusive = self.checkpoint_upper_bound(budget);
        let predrain = self.predrain_for_compaction(&deadline, upper_bound_inclusive)?;
        let mut pages = predrain.wal_checkpoint_backfilled;
        if !predrain.everything_backfilled() {
            // A pass bounded by `max_pages` is progress, not a block: the cursor moved and
            // the next slot continues from there. A pass that stopped short of its own bound
            // was stopped by a reader, which is recorded and retried only after the blocked
            // backoff.
            let budget_limited = upper_bound_inclusive.is_some_and(|bound| {
                predrain.wal_max_frame > bound && predrain.wal_total_backfilled >= bound
            });
            if budget_limited {
                return Ok(MaintenanceUnitReport {
                    scanned_pages: pages,
                    backfilled_bytes: self.frames_to_bytes(pages),
                    truncated: false,
                    complete: false,
                });
            }
            if budget.max_pages != u64::MAX {
                // A bounded unit has no more planned work. Ask the same preflight the
                // exclusive phase uses before claiming a partial backfill: a reader the
                // registry can see is `long_reader` and a retryable `Conflict`, not an
                // unexplained partial result. An invisible reader still falls through to
                // the partial-backfill block; the native truncate is never reached here, so
                // a bounded pass cannot turn a blind spot into a truncate.
                self.preflight_compaction()?;
                self.record_maintenance_block(BlockReason::PartialBackfill);
                anyhow::bail!(
                    "checkpoint did not backfill the complete WAL (backfilled {} of {} frames)",
                    predrain.wal_total_backfilled,
                    predrain.wal_max_frame
                );
            }
            // The unbounded admin path keeps its pre-existing contract: enter the exclusive
            // phase, where the preflight classifies a visible reader as `Conflict` and the
            // native truncate turns an invisible reader into `Busy` and an unsettled,
            // fail-closed outcome. Returning a classified block here would let a caller that
            // expects either signal mistake a blind spot for a clean rejection.
        }
        // The backfill is complete and no reader blocked it. Do not start the exclusive
        // phase after the unit's time budget elapsed: the truncate is bounded cheaply, but
        // a forced unit must still not hold the operations gate for an unbounded window.
        if started.elapsed() >= budget.max_time {
            return Ok(MaintenanceUnitReport {
                scanned_pages: pages,
                backfilled_bytes: self.frames_to_bytes(pages),
                truncated: false,
                complete: true,
            });
        }
        let mut permit = self.admission.maintenance()?;
        let connection = self.lane.maintenance_connection(&deadline, self)?;
        let connection = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        let _operation = self.maintenance_operation(&deadline)?;
        self.io.check_writer()?;
        self.storage().store().validate_transaction(false)?;
        ensure!(
            connection.get_auto_commit(),
            "cannot compact inside a transaction"
        );
        configure(connection);
        self.io.check_compaction()?;
        self.preflight_compaction()?;
        permit.dispatch();
        let checkpoint = connection
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .context("truncate-checkpoint remote WAL")
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))?;
        let complete = checkpoint.wal_max_frame == checkpoint.wal_total_backfilled;
        if !complete {
            // A partial backfill is the long-read case: some frame is still needed by a
            // reader, so the checkpoint cannot advance past it. Recording it before
            // `require_complete_backfill` returns is what makes the block visible; the WAL
            // index is not truncated afterwards, so the old references are kept.
            //
            // Defensive classification only: the fixed native engine reports an incomplete
            // truncate backfill as `Busy` (the error path above), never as `Ok`, so this
            // branch becomes observable only if that contract changes. A reader the preflight
            // cannot see therefore ends up quarantined instead of being mislabelled here.
            self.record_maintenance_block(BlockReason::PartialBackfill);
        }
        Self::require_complete_backfill(complete)?;
        self.io
            .compact_wal()
            .map_err(|error| error.context(crate::session::UnsettledSqlOutcome))?;
        // The WAL index was rebased; a cursor from the old index would only ask the engine
        // for a bound it must clamp. Reset it so the next pass starts at frame zero.
        self.advance_checkpoint_cursor(0);
        pages = pages.saturating_add(checkpoint.wal_checkpoint_backfilled);
        permit.complete()?;
        Ok(MaintenanceUnitReport {
            scanned_pages: pages,
            backfilled_bytes: self.frames_to_bytes(pages),
            truncated: true,
            complete: true,
        })
    }

    /// A truncate checkpoint has to copy every frame no reader needs before it may restart
    /// the log. Do that copy as a passive unit first: it runs beside foreground SQL and
    /// leaves only the restart inside the exclusive window. Without this, the whole backfill
    /// would happen while the operations gate and the writer admission hold every request.
    fn predrain_for_compaction(
        &self,
        deadline: &RequestDeadline,
        upper_bound_inclusive: Option<u64>,
    ) -> Result<turso_core::CheckpointResult> {
        self.admission.healthy()?;
        let connection = self.lane.maintenance_connection(deadline, self)?;
        let connection = connection.as_ref().ok_or(AdmissionError::Quarantine)?;
        self.io.check_writer()?;
        self.io.storage().store().validate_local()?;
        self.io.storage().store().validate_transaction(false)?;
        configure(connection);
        let checkpoint = connection
            .checkpoint(CheckpointMode::Passive {
                upper_bound_inclusive,
            })
            .context("pre-drain compaction backfill")?;
        self.advance_checkpoint_cursor(checkpoint.wal_total_backfilled);
        Ok(checkpoint)
    }

    fn checkpoint_upper_bound(&self, budget: UnitBudget) -> Option<u64> {
        if budget.max_pages == u64::MAX {
            return None;
        }
        let cursor = self.checkpoint_cursor.load(Ordering::Relaxed);
        Some(cursor.saturating_add(budget.max_pages))
    }

    fn advance_checkpoint_cursor(&self, frame: u64) {
        self.checkpoint_cursor.store(frame, Ordering::Relaxed);
    }

    /// Frames are reported as bytes for the counters. The manifest page size is the closest
    /// in-process truth; a default page size is only used before the header is published.
    fn frames_to_bytes(&self, frames: u64) -> u64 {
        let page_size = match self.storage().manifest().page_size {
            0 => 4096,
            size => u64::from(size),
        };
        frames.saturating_mul(page_size.saturating_add(24))
    }

    fn require_complete_backfill(complete: bool) -> Result<()> {
        if complete {
            Ok(())
        } else {
            Err(anyhow!("checkpoint did not backfill the complete WAL")
                .context(crate::session::UnsettledSqlOutcome))
        }
    }

    pub(crate) fn operation_until(
        &self,
        deadline: &RequestDeadline,
        cleanup: bool,
    ) -> Result<OperationReadGuard<'_>> {
        let acquired = if cleanup {
            self.operations.read_until(deadline, || Ok(()))
        } else {
            self.operations.read_until(deadline, || {
                deadline.check()?;
                self.admission.healthy()?;
                self.storage().store().validate_local()
            })
        };
        let operation = match acquired {
            Ok(operation) => operation,
            Err(error) => {
                // A poisoned gate means a holder panicked; fail closed exactly as the
                // try_lock implementation did. The notification was already sent by the gate.
                if error.downcast_ref::<AdmissionError>() == Some(&AdmissionError::Quarantine) {
                    self.admission.quarantine();
                }
                return Err(error);
            }
        };
        if !cleanup {
            // Re-check after the guard is held: a quarantine that landed while this thread
            // waited must fail the request, and dropping the guard above wakes the next
            // waiter rather than leaving it to its deadline.
            self.admission.healthy()?;
            self.last_operation_ms.store(
                crate::maintenance::millis(self.maintenance.opened().elapsed()),
                Ordering::Relaxed,
            );
        }
        Ok(operation)
    }

    fn maintenance_operation(&self, deadline: &RequestDeadline) -> Result<OperationWriteGuard<'_>> {
        self.operations.write_until(deadline, || {
            deadline.check()?;
            self.storage().store().validate_local()
        })
    }

    pub(crate) fn admission(&self) -> &Arc<WriterAdmission> {
        &self.admission
    }

    #[doc(hidden)]
    pub fn writer_admission_status(&self) -> Result<AdmissionStatus> {
        self.admission.status()
    }

    pub fn storage(&self) -> Arc<RemoteStorage> {
        self.io.storage()
    }
    pub fn io(&self) -> Arc<RemoteIO> {
        self.io.clone()
    }

    /// Records why a unit did not complete. The counter and the ledger entry are the same
    /// fact in two places: the process-wide export and this database's own history.
    pub(crate) fn record_maintenance_block(&self, reason: BlockReason) {
        self.maintenance.record_block(reason, None);
        crate::work_metrics::observe_maintenance_blocked(reason);
    }

    pub(crate) fn maintenance_ledger(&self) -> &MaintenanceLedger {
        &self.maintenance
    }
}

/// The scheduler's view of this database. Everything it needs is a cheap in-process read or
/// one of the two existing unit entry points, so the scheduling rules never reach past this
/// implementation into the native engine.
impl MaintenanceTarget for RemoteDatabase {
    fn identity(&self) -> String {
        self.io.storage().namespace().prefix()
    }

    fn ledger(&self) -> &MaintenanceLedger {
        &self.maintenance
    }

    fn signals(&self) -> Result<MaintenanceSignals> {
        let mut signals = self.io.maintenance_signals()?;
        signals.oldest_reader_ms =
            crate::maintenance::millis(self.oldest_reader_age(std::time::Instant::now()));
        let now = crate::maintenance::millis(self.maintenance.opened().elapsed());
        signals.quiet_for_ms = now.saturating_sub(self.last_operation_ms.load(Ordering::Relaxed));
        Ok(signals)
    }

    fn run_maintenance(
        &self,
        lane: MaintenanceLane,
        budget: UnitBudget,
    ) -> Result<MaintenanceUnitReport> {
        match lane {
            MaintenanceLane::Checkpoint => self.checkpoint_with_budget(budget),
            MaintenanceLane::IndexCompact => self.compact_wal_with_budget(budget),
        }
    }
}

impl Drop for RemoteDatabase {
    fn drop(&mut self) {
        let _ = self.lane.close(self);
    }
}

#[cfg(test)]
mod remote_wal_sync_failure_tests {
    use super::*;
    use crate::object_store::{MemoryObjectStore, PutCondition, StoredObject};
    use crate::wal::WalAppendRequest;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct FailingAppendStore {
        inner: MemoryObjectStore,
        armed: AtomicBool,
        failures: AtomicUsize,
    }

    impl ObjectStore for FailingAppendStore {
        fn supports_strict_fencing(&self) -> bool {
            true
        }

        fn append_wal(&self, _: &WalAppendRequest) -> Result<Option<u64>> {
            if self.armed.load(Ordering::SeqCst) {
                self.failures.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("injected remote WAL append failure");
            }
            Ok(None)
        }

        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
            self.inner.put(key, bytes, condition)
        }

        fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
            self.inner.delete(key, expected)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
    }

    fn assert_remote_append_failure_is_unsettled(retry: bool) -> Result<()> {
        let cache = tempfile::tempdir()?;
        let store = Arc::new(FailingAppendStore::default());
        let database = RemoteDatabase::open(
            store.clone(),
            Namespace::new("fault", if retry { "retry" } else { "default" })?,
            cache.path(),
            1,
        )?;
        database.execute("CREATE TABLE entries(value INTEGER)")?;
        {
            let lane = database.lane.connection.lock().unwrap();
            let connection = lane.as_ref().unwrap();
            assert!(!connection.get_data_sync_retry());
            if retry {
                connection.set_data_sync_retry(true);
            }
        }
        store.armed.store(true, Ordering::SeqCst);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            database.execute("INSERT INTO entries VALUES(1)")
        }));
        let error = result
            .expect("remote append failure must return an error")
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("injected remote WAL append failure"),
            "{message}"
        );
        assert!(message.contains("outcome is unsettled"), "{message}");
        assert_eq!(store.failures.load(Ordering::SeqCst), 1);
        assert!(database.admission.status()?.quarantined);
        Ok(())
    }

    #[test]
    fn remote_wal_append_failure_returns_unsettled_with_default_retry() -> Result<()> {
        assert_remote_append_failure_is_unsettled(false)
    }

    #[test]
    fn remote_wal_append_failure_returns_unsettled_with_retry_enabled() -> Result<()> {
        assert_remote_append_failure_is_unsettled(true)
    }
}

#[cfg(test)]
mod operation_gate_tests {
    use super::*;
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

    fn wait_until_waiters(database: &RemoteDatabase, expected: usize) {
        let started = Instant::now();
        while database.operations.notify.waiters() < expected {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "waiter did not register: expected {expected}, saw {}",
                database.operations.notify.waiters()
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn a_released_writer_wakes_the_waiting_reader() -> Result<()> {
        let (_cache, database) = open("gate_reader")?;
        let held = database.maintenance_operation(&deadline_after(Duration::from_secs(2)))?;

        let (sender, receiver) = mpsc::channel();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            let result = worker_database
                .operation_until(&deadline_after(Duration::from_secs(2)), false)
                .map(|_| ());
            sender.send(result).unwrap();
        });
        wait_until_waiters(&database, 1);
        drop(held);
        receiver.recv_timeout(Duration::from_secs(1))??;
        worker.join().unwrap();
        Ok(())
    }

    #[test]
    fn a_writer_times_out_and_releases_its_intent() -> Result<()> {
        let (_cache, database) = open("gate_timeout")?;
        let read = database.operation_until(&deadline_after(Duration::from_secs(2)), false)?;

        let started = Instant::now();
        let error = database
            .maintenance_operation(&deadline_after(Duration::from_millis(80)))
            .err()
            .expect("a held read gate must time out the writer");
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
            database.operations.writers_waiting.load(Ordering::Acquire),
            0,
            "a timed-out writer must not leave its intent behind"
        );
        assert_eq!(database.operations.notify.waiters(), 0);
        drop(read);
        Ok(())
    }

    #[test]
    fn a_waiting_writer_is_not_starved_by_a_new_reader() -> Result<()> {
        let (_cache, database) = open("gate_fair")?;
        let read = database.operation_until(&deadline_after(Duration::from_secs(2)), false)?;

        let (writer_sender, writer_receiver) = mpsc::channel();
        let writer_database = database.clone();
        let writer = std::thread::spawn(move || {
            let result = writer_database
                .maintenance_operation(&deadline_after(Duration::from_secs(3)))
                .map(|guard| {
                    // Hold the exclusive gate long enough that the reader below cannot
                    // finish by accident before the writer's release.
                    std::thread::sleep(Duration::from_millis(100));
                    drop(guard);
                });
            writer_sender.send(result).unwrap();
        });
        wait_until_waiters(&database, 1);

        let (reader_sender, reader_receiver) = mpsc::channel();
        let reader_database = database.clone();
        let reader = std::thread::spawn(move || {
            let result = reader_database
                .operation_until(&deadline_after(Duration::from_secs(3)), false)
                .map(|_| ());
            reader_sender.send(result).unwrap();
        });
        // Readers do not exclude each other, so without the writer intent this new reader
        // would acquire immediately even though a truncate checkpoint is queued.
        wait_until_waiters(&database, 2);
        assert!(
            reader_receiver
                .recv_timeout(Duration::from_millis(60))
                .is_err(),
            "a new reader barged ahead of the waiting writer"
        );

        drop(read);
        writer_receiver.recv_timeout(Duration::from_secs(1))??;
        reader_receiver.recv_timeout(Duration::from_secs(1))??;
        writer.join().unwrap();
        reader.join().unwrap();
        Ok(())
    }

    #[test]
    fn quarantine_after_release_fails_the_waiting_reader() -> Result<()> {
        let (_cache, database) = open("gate_quarantine")?;
        let held = database.maintenance_operation(&deadline_after(Duration::from_secs(2)))?;

        let (sender, receiver) = mpsc::channel();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            let result = worker_database
                .operation_until(&deadline_after(Duration::from_secs(5)), false)
                .map(|_| ());
            sender.send(result).unwrap();
        });
        wait_until_waiters(&database, 1);
        database.admission().quarantine();
        drop(held);
        match receiver.recv_timeout(Duration::from_secs(1))? {
            Ok(()) => panic!("quarantine must cancel the wait"),
            Err(error) => assert_eq!(
                error.downcast_ref::<AdmissionError>(),
                Some(&AdmissionError::Quarantine)
            ),
        }
        worker.join().unwrap();
        Ok(())
    }

    #[test]
    fn a_released_write_gate_is_never_lost_across_a_thousand_cycles() -> Result<()> {
        let (_cache, database) = open("gate_no_lost")?;
        let (wake_sender, wake_receiver) = mpsc::channel::<()>();
        let (done_sender, done_receiver) = mpsc::channel::<Result<()>>();
        let worker_database = database.clone();
        let worker = std::thread::spawn(move || {
            while wake_receiver.recv().is_ok() {
                let result = worker_database
                    .operation_until(&deadline_after(Duration::from_secs(2)), false)
                    .map(|_| ());
                if done_sender.send(result).is_err() {
                    return;
                }
            }
        });
        for _ in 0..1000 {
            let held = database.maintenance_operation(&deadline_after(Duration::from_secs(2)))?;
            wake_sender.send(()).unwrap();
            wait_until_waiters(&database, 1);
            drop(held);
            done_receiver.recv_timeout(Duration::from_secs(2))??;
        }
        drop(wake_sender);
        worker.join().unwrap();
        Ok(())
    }

    #[test]
    fn an_uncontended_operation_gate_never_enters_the_wait() -> Result<()> {
        let (_cache, database) = open("gate_fast")?;
        let started = Instant::now();
        for _ in 0..1000 {
            drop(database.operation_until(&deadline_after(Duration::from_secs(1)), false)?);
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(database.operations.notify.waiters(), 0);
        Ok(())
    }
}
