mod capacity;
mod handle;
mod health;
mod memory;
mod quarantine;
mod registry;
#[cfg(test)]
mod tests;
pub mod types;

pub use handle::DatabaseHandle;
pub(crate) use memory::result_reservation_bytes;
pub use memory::{
    BudgetLane, MemoryBudget, MemoryBudgetRejection, MemoryBudgetStatus, MemoryReservation,
};
pub use types::{
    ActivationPhase, ActivationStatus, DatabaseId, DatabaseLifecycle, DatabaseProvider,
    DatabaseResources, DatabaseStatus, WorkerLimits, WorkerStatus,
};

use crate::database::RemoteDatabase;
use crate::maintenance::MaintenanceScheduler;
use crate::object_store::ObjectStore;
use crate::session::{AdmissionError, Budget, RequestDeadline, SqlEvidence};
use anyhow::{anyhow, ensure, Context, Result};
use registry::{Activation, Completion, Entry, Instance, Loaded, Loading, Registry};
use std::{
    cell::Cell,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

/// A cold load pays one authority round trip per WAL chunk, so the time it needs is set by
/// the remote store and not by a statement: `PERI_WORKER_LOAD_MS` overrides the budget, and
/// the default is the larger of the statement wall budget and this floor.
///
/// The budget is not held below the client-visible request timeout. A load executes no
/// statement: the open path has nothing to run, and the statement path re-checks the request
/// deadline after the reload it may trigger (`worker_rpc::sessions::Sessions::begin`), so a
/// load that still finishes only makes the retry that follows it fast — which the retained
/// instance is what delivers. It must stay below the time the database is leasable: a load
/// that outlives the lease is fenced by the storage authority, which is the bound that
/// matters. The measured cold load behind this floor took 16.6 s for 100k rows.
const MIN_LOAD_BUDGET: Duration = Duration::from_secs(60);
const LOAD_BUDGET_ENV: &str = "PERI_WORKER_LOAD_MS";
/// Cold loads of different databases used to run one per caller thread, bounded only by
/// `max_open_databases`. Each replay can hold a whole fetch window, so the worker now
/// admits a conservative number at a time and queues the rest.
const MAX_CONCURRENT_LOADS_ENV: &str = "PERI_WORKER_MAX_CONCURRENT_LOADS";
const DEFAULT_MAX_CONCURRENT_LOADS: usize = 2;
const MAX_CONCURRENT_LOADS_CEILING: usize = 64;

pub struct WorkerRuntime {
    key_provider: Option<Arc<dyn crate::encryption::DatabaseKeyProvider>>,
    provider: Arc<dyn DatabaseProvider>,
    cache_root: PathBuf,
    limits: WorkerLimits,
    /// Process-wide admission ledger. Resident and session reservations are derived from the
    /// registry under its mutex; in-flight request bytes are charged atomically on the same
    /// lock, so admission and session counts cannot move independently.
    memory: Arc<MemoryBudget>,
    /// Configured override; the effective budget is resolved per open, because the
    /// statement budget it falls back to may be set after this runtime exists.
    load_budget: Option<Duration>,
    registry: Mutex<Registry>,
    load_permits: LoadPermits,
    native_instances: Mutex<Vec<(DatabaseId, std::sync::Weak<RemoteDatabase>)>>,
    /// Scheduler for the units that must not run in a request. It owns the process-wide
    /// budget and the driver thread; a worker with the switch off never starts either.
    maintenance: Arc<MaintenanceScheduler>,
}

impl WorkerRuntime {
    pub fn new(
        provider: Arc<dyn DatabaseProvider>,
        cache_root: impl AsRef<Path>,
        limits: WorkerLimits,
    ) -> Result<Arc<Self>> {
        Self::configured(provider, cache_root, limits, None)
    }

    pub fn new_with_key_provider(
        provider: Arc<dyn DatabaseProvider>,
        cache_root: impl AsRef<Path>,
        limits: WorkerLimits,
        key_provider: Arc<dyn crate::encryption::DatabaseKeyProvider>,
    ) -> Result<Arc<Self>> {
        Self::configured(provider, cache_root, limits, Some(key_provider))
    }

    fn configured(
        provider: Arc<dyn DatabaseProvider>,
        cache_root: impl AsRef<Path>,
        limits: WorkerLimits,
        key_provider: Option<Arc<dyn crate::encryption::DatabaseKeyProvider>>,
    ) -> Result<Arc<Self>> {
        ensure!(
            limits.max_open_databases > 0
                && limits.max_total_sessions > 0
                && limits.max_sessions_per_database > 0,
            "worker admission limits must be positive"
        );
        ensure!(
            limits.segment_ram_bytes > 0,
            "worker segment RAM limit must be positive"
        );
        let max_open_databases = limits.max_open_databases;
        Ok(Arc::new(Self {
            key_provider,
            provider,
            cache_root: cache_root.as_ref().to_path_buf(),
            limits,
            memory: MemoryBudget::configured()?,
            load_budget: load_budget_setting()?,
            registry: Mutex::new(Registry::default()),
            load_permits: LoadPermits::configured(
                max_concurrent_loads_setting()?,
                max_open_databases,
            ),
            native_instances: Mutex::new(Vec::new()),
            maintenance: Arc::new(MaintenanceScheduler::configured()?),
        }))
    }

    /// `PERI_WORKER_LOAD_MS` overrides the load budget. Without it the budget follows the
    /// configured statement wall budget and is resolved per open, so a budget set after this
    /// runtime exists still reaches the opens that follow it.
    fn load_budget(&self) -> Duration {
        self.load_budget.unwrap_or_else(|| {
            Budget::trusted_default()
                .wall_duration()
                .max(MIN_LOAD_BUDGET)
        })
    }

    /// Opens a session on a loaded instance.
    ///
    /// Two deadlines meet here and stay separate. The shared activation runs under its own
    /// `load_budget` (overridden by `PERI_WORKER_LOAD_MS`); a caller only waits for it
    /// until the ambient waiter deadline, which the worker RPC sets to its request deadline.
    /// A caller that times out leaves the queue; the load keeps running and its result stays
    /// resident for later opens.
    pub fn open(self: &Arc<Self>, identifier: DatabaseId, epoch: u64) -> Result<DatabaseHandle> {
        self.open_until(identifier, epoch, current_waiter_deadline())
    }

    pub(crate) fn open_until(
        self: &Arc<Self>,
        identifier: DatabaseId,
        epoch: u64,
        waiter_deadline: Option<Instant>,
    ) -> Result<DatabaseHandle> {
        identifier.validate()?;
        let activated = self.activate(identifier.clone(), epoch, waiter_deadline)?;
        // The session phase is anchored after the load returns, so neither phase inherits a
        // deadline the other one has already spent.
        let _session_budget =
            RequestDeadline::scope(Instant::now() + Budget::trusted_default().wall_duration());
        self.make_handle(identifier, activated, waiter_deadline)
    }

    /// Finds or starts the shared activation for one database.
    ///
    /// A cold load acquires one of the bounded load permits before the `Loading` entry is
    /// published, so a queued caller never appears as a load in progress. Same-database
    /// callers coalesce on the `Loading` entry instead of taking a permit.
    fn activate(
        self: &Arc<Self>,
        identifier: DatabaseId,
        epoch: u64,
        waiter_deadline: Option<Instant>,
    ) -> Result<Activated> {
        // Suspended rather than narrowed: the load's own budget is checked at phase
        // boundaries, while the waiter deadline only bounds how long this caller waits.
        let _load_budget = RequestDeadline::suspend();
        loop {
            let mut registry = self.lock()?;
            registry.check_available(&identifier)?;
            if let Some(entry) = registry.entries.get(&identifier) {
                ensure!(entry.epoch() == epoch, "database epoch mismatch");
                match entry {
                    Entry::Retiring(_) => anyhow::bail!("database is unloading"),
                    Entry::Loading(loading) => {
                        ensure!(!loading.draining, "database is draining");
                        let completion = loading.completion.clone();
                        let activation = loading.activation.clone();
                        let _waiter = activation.waiter();
                        drop(registry);
                        completion.wait(waiter_deadline)?;
                        continue;
                    }
                    Entry::Loaded(loaded) => {
                        ensure!(!loaded.draining, "database is draining");
                        ensure!(
                            loaded.sessions < self.limits.max_sessions_per_database,
                            "database session capacity exceeded"
                        );
                        ensure!(
                            registry.sessions < self.limits.max_total_sessions,
                            "worker session capacity exceeded"
                        );
                        self.check_memory_reservation(&registry, 0)?;
                        let database = loaded.database.clone();
                        let Entry::Loaded(loaded) = registry.entries.get_mut(&identifier).unwrap()
                        else {
                            unreachable!()
                        };
                        loaded.sessions += 1;
                        loaded.idle_since = None;
                        registry.admit_session();
                        drop(registry);
                        return Ok(Activated {
                            instance: database,
                            load_started: None,
                        });
                    }
                }
            }
            drop(registry);

            let permit = self.load_permits.acquire(waiter_deadline)?;

            // The permit is process-wide, so another caller may have published this
            // database's entry while this one waited. Re-check before becoming the loader.
            let mut registry = self.lock()?;
            registry.check_available(&identifier)?;
            if registry.entries.contains_key(&identifier) {
                drop(registry);
                drop(permit);
                continue;
            }
            registry.check_capacity(self.limits.max_open_databases)?;
            ensure!(
                registry.sessions < self.limits.max_total_sessions,
                "worker session capacity exceeded"
            );
            self.check_memory_reservation(&registry, 1)?;
            let load_started = Instant::now();
            let load_deadline = load_started
                .checked_add(self.load_budget())
                .context("database load deadline overflow")?;
            let loading = Loading::new(epoch);
            let completion = loading.completion.clone();
            let activation = loading.activation.clone();
            registry
                .resident_since
                .insert(identifier.clone(), Instant::now());
            registry
                .entries
                .insert(identifier.clone(), Entry::Loading(loading));
            registry.admit_session();
            drop(registry);

            return self.run_activation(
                identifier,
                epoch,
                activation,
                completion,
                load_started,
                load_deadline,
                permit,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_activation(
        self: &Arc<Self>,
        identifier: DatabaseId,
        epoch: u64,
        activation: Arc<Activation>,
        completion: Arc<Completion>,
        load_started: Instant,
        load_deadline: Instant,
        permit: LoadPermit<'_>,
    ) -> Result<Activated> {
        // A panic or an early return below must not leave a permanent Loading entry or
        // strand waiters. The explicit finalizers disarm this guard.
        let mut guard = LoadingGuard::new(
            self.clone(),
            identifier.clone(),
            completion.clone(),
            activation.clone(),
        );
        let opened = self.load(&identifier, epoch, &activation, load_deadline);
        // Sampled outside the registry lock: the resource probe takes the database and
        // storage locks, and the native-reclaim path takes this registry from under them.
        let replayed_bytes = opened
            .as_ref()
            .ok()
            .and_then(|database| database.resources().ok())
            .map(|usage| usage.wal_bytes as u64);

        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let draining = match opened.as_ref() {
            Ok(_) => match registry.entries.get(&identifier) {
                Some(Entry::Loading(loading)) if Arc::ptr_eq(&loading.activation, &activation) => {
                    loading.draining
                }
                // The entry is gone or no longer ours; never publish an instance nobody
                // admitted.
                _ => true,
            },
            Err(_) => false,
        };
        let database = match opened {
            Ok(database) if !draining => {
                drop(registry);
                database
            }
            Ok(database) => {
                registry
                    .entries
                    .insert(identifier.clone(), Entry::Retiring(epoch));
                registry.release_session();
                drop(registry);
                activation.set_phase(ActivationPhase::Draining);
                completion.finish(Err("database is draining".into()));
                guard.disarm();
                drop(database);
                // No Instance was built, so no native observer will finish the tombstone.
                // Without this the database would answer "unloading" forever.
                self.finish_retiring(&identifier);
                drop(permit);
                anyhow::bail!("database is draining");
            }
            Err(error) => {
                registry.entries.remove(&identifier);
                registry.release_session();
                if !registry.quarantined.contains_key(&identifier) {
                    registry.resident_since.remove(&identifier);
                }
                drop(registry);
                activation.set_phase(ActivationPhase::Failed);
                completion.finish(Err(format!("{error:#}")));
                guard.disarm();
                drop(permit);
                return Err(error);
            }
        };

        // Instance construction registers a native observer; keep it outside the registry
        // lock, whose order against native callbacks is the reverse.
        let instance = match Instance::new(database.clone(), self, identifier.clone(), epoch) {
            Ok(instance) => instance,
            Err(error) => {
                let mut registry = self
                    .registry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                registry.entries.remove(&identifier);
                registry.release_session();
                if !registry.quarantined.contains_key(&identifier) {
                    registry.resident_since.remove(&identifier);
                }
                drop(registry);
                activation.set_phase(ActivationPhase::Failed);
                completion.finish(Err(format!("{error:#}")));
                guard.disarm();
                drop(database);
                drop(permit);
                return Err(error);
            }
        };
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A drain or retirement may have landed while the observer was registered.
        let still_loading = matches!(
            registry.entries.get(&identifier),
            Some(Entry::Loading(loading))
                if Arc::ptr_eq(&loading.activation, &activation) && !loading.draining
        );
        if !still_loading {
            registry
                .entries
                .insert(identifier.clone(), Entry::Retiring(epoch));
            registry.release_session();
            drop(registry);
            activation.set_phase(ActivationPhase::Draining);
            completion.finish(Err("database is draining".into()));
            guard.disarm();
            drop(instance);
            drop(permit);
            anyhow::bail!("database is draining");
        }
        activation.set_replayed_bytes(replayed_bytes);
        activation.set_phase(ActivationPhase::Ready);
        registry.entries.insert(
            identifier.clone(),
            Entry::Loaded(Loaded {
                epoch,
                database: instance.clone(),
                sessions: 1,
                draining: false,
                idle_since: None,
                activation: Some(activation),
            }),
        );
        drop(registry);
        // Waiters are released before the budget is checked, so the budget only judges the
        // open that paid the load: an open that arrived while this one was loading either
        // waits here or finds the instance resident, and both paths succeed. Retaining the
        // instance for them is the same intent as retaining it below — an overrunning load
        // is never paid twice, and the overrun error belongs to the open that asked for it.
        completion.finish(Ok(()));
        guard.disarm();
        drop(permit);
        if load_started.elapsed() > self.load_budget() {
            // The load itself succeeded and only overran its budget. The instance stays
            // resident, so the next open reuses it instead of paying the load again, and
            // the failure is reported as the load overrun rather than as a statement
            // deadline the load never had.
            self.release(&identifier);
            return Err(load_budget_exceeded());
        }
        Ok(Activated {
            instance,
            load_started: Some(load_started),
        })
    }

    /// Charges the segment cache and session reserve of one admitted open. The caller holds
    /// the registry lock, so the resident figure used here is the one the admission will
    /// publish; a refusal happens before the load starts and never after it ran.
    fn check_memory_reservation(
        &self,
        registry: &Registry,
        additional_databases: usize,
    ) -> Result<()> {
        let resident = self.resident_usage(registry, additional_databases, 1)?;
        self.memory
            .check_resident(BudgetLane::Sql, resident)
            .map_err(|rejection| anyhow::Error::new(rejection).context(SqlEvidence::NotExecuted))
    }

    fn resident_usage(
        &self,
        registry: &Registry,
        additional_databases: usize,
        additional_sessions: usize,
    ) -> Result<memory::ResidentUsage> {
        memory::reservation_usage(
            registry.entries.len().saturating_add(additional_databases),
            registry.sessions.saturating_add(additional_sessions),
            self.limits.segment_ram_bytes,
        )
    }

    /// Reserves the input and output bytes of one request before it is dispatched. The
    /// registry lock is held across the reservation so a concurrent session admission sees
    /// the charge (or this caller sees the new session). The returned token releases the
    /// bytes on every path, including an error or a panic unwind.
    pub(crate) fn reserve_inflight(
        &self,
        lane: BudgetLane,
        bytes: usize,
    ) -> Result<MemoryReservation> {
        let registry = self.lock()?;
        let resident = self.resident_usage(&registry, 0, 0)?;
        self.memory
            .reserve_inflight(lane, bytes as u64, resident)
            .map_err(|rejection| anyhow::Error::new(rejection).context(SqlEvidence::NotExecuted))
    }

    fn load(
        &self,
        identifier: &DatabaseId,
        epoch: u64,
        activation: &Arc<Activation>,
        load_deadline: Instant,
    ) -> Result<Arc<RemoteDatabase>> {
        crate::metrics::Metrics::global().observe(
            crate::metrics::Operation::ColdOpen,
            || self.load_native(identifier, epoch, activation, load_deadline),
            |_| 0,
        )
    }

    /// Checks the load's own budget, drain/fence, and reports the phase at each boundary
    /// the load can safely stop at. `RemoteDatabase::open` (including WAL replay) is not
    /// interruptible: until it returns, the `Loading` entry, ownership and isolation stay
    /// in place; this never claims the native call was stopped.
    fn load_boundary(
        &self,
        identifier: &DatabaseId,
        activation: &Arc<Activation>,
        load_deadline: Instant,
        phase: ActivationPhase,
        store: Option<&Arc<dyn ObjectStore>>,
    ) -> Result<()> {
        activation.set_phase(phase);
        if Instant::now() >= load_deadline {
            return Err(load_budget_exceeded());
        }
        {
            let mut registry = self.lock()?;
            registry.check_available(identifier)?;
            match registry.entries.get(identifier) {
                // A drain that lands while the load is in flight must not abort it: the
                // native open is not interruptible, and `run_activation` retires the
                // finished instance under the same reservation. Aborting here would drop
                // the store while the entry still reports `Loading`.
                Some(Entry::Loading(loading)) if Arc::ptr_eq(&loading.activation, activation) => {}
                Some(Entry::Retiring(_)) => anyhow::bail!("database is unloading"),
                _ => anyhow::bail!("database load entry missing"),
            }
        }
        if let Some(store) = store {
            store.validate_transaction(false)?;
        }
        Ok(())
    }

    fn load_native(
        &self,
        identifier: &DatabaseId,
        epoch: u64,
        activation: &Arc<Activation>,
        load_deadline: Instant,
    ) -> Result<Arc<RemoteDatabase>> {
        self.load_boundary(
            identifier,
            activation,
            load_deadline,
            ActivationPhase::OpeningStore,
            None,
        )?;
        let store = self.provider.open_store(identifier, epoch)?;
        store.validate_transaction(false)?;
        self.load_boundary(
            identifier,
            activation,
            load_deadline,
            ActivationPhase::ResolvingKey,
            Some(&store),
        )?;
        if let Some(provider) = &self.key_provider {
            let key = provider.resolve(identifier, epoch)?;
            store.validate_transaction(false)?;
            self.load_boundary(
                identifier,
                activation,
                load_deadline,
                ActivationPhase::OpeningDatabase,
                Some(&store),
            )?;
            let database = RemoteDatabase::open_with_encryption_and_limits(
                store.clone(),
                identifier.namespace.clone(),
                self.cache_root.join(identifier.cache_key()?),
                epoch,
                self.limits.segment_ram_bytes,
                self.limits.segment_disk_bytes,
                key,
            )?;
            store.validate_transaction(false)?;
            return Ok(Arc::new(database));
        }
        self.load_boundary(
            identifier,
            activation,
            load_deadline,
            ActivationPhase::OpeningDatabase,
            Some(&store),
        )?;
        let database = RemoteDatabase::open_with_limits(
            store.clone(),
            identifier.namespace.clone(),
            self.cache_root.join(identifier.cache_key()?),
            epoch,
            self.limits.segment_ram_bytes,
            self.limits.segment_disk_bytes,
        )?;
        // The last boundary before the caller decides whether to publish this instance.
        // The budget check itself belongs to `run_activation`, which can retain an
        // overrunning but successfully opened instance for later opens.
        store.validate_transaction(false)?;
        Ok(Arc::new(database))
    }

    fn make_handle(
        self: &Arc<Self>,
        identifier: DatabaseId,
        activated: Activated,
        waiter_deadline: Option<Instant>,
    ) -> Result<DatabaseHandle> {
        let Activated {
            instance,
            load_started,
        } = activated;
        let session = (|| -> Result<crate::session::RemoteSession> {
            // Session creation is the last load phase boundary: a caller that already
            // spent its wait deadline does not create a session it cannot report, and a
            // drain or fence that landed after the load must still refuse the session.
            if waiter_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(activation_wait_deadline());
            }
            {
                let mut registry = self.lock()?;
                registry.check_available(&identifier)?;
                match registry.entries.get(&identifier) {
                    Some(Entry::Loaded(loaded)) => {
                        ensure!(!loaded.draining, "database is draining")
                    }
                    Some(Entry::Loading(loading)) => {
                        ensure!(!loading.draining, "database is draining")
                    }
                    _ => anyhow::bail!("database is unloading"),
                }
            }
            if load_started.is_some_and(|started| started.elapsed() > self.load_budget()) {
                return Err(load_budget_exceeded());
            }
            instance.database()?.session()
        })();
        match session {
            Ok(session) => Ok(DatabaseHandle::new(
                self.clone(),
                identifier,
                instance,
                session,
            )),
            Err(error) => {
                // Only the session slot is released. The instance stays resident, so an open
                // that failed after a successful load does not charge the next attempt for a
                // second cold load; the idle sweep still retires it.
                drop(instance);
                self.release(&identifier);
                Err(error)
            }
        }
    }

    pub(crate) fn initial_image_replaced(&self, identifier: &DatabaseId) -> Result<bool> {
        let loaded = {
            let mut registry = self.lock()?;
            registry.check_available(identifier)?;
            match registry.entries.get(identifier) {
                Some(Entry::Loaded(loaded)) => Some((loaded.database.database()?, loaded.epoch)),
                _ => None,
            }
        };
        match loaded {
            Some((database, epoch)) => database
                .storage()
                .initial_image_replaced(|| self.provider.open_store(identifier, epoch)),
            None => Ok(false),
        }
    }

    /// Explicit unload. This deliberately does not pre-tidy: it is also the path an image
    /// replacement uses to drop a local instance, and publishing a local checkpoint there
    /// could race the replacement. The suspend/migrate transitions (`drain` and the
    /// last-session release) run the pre-tidy instead.
    pub fn close(&self, identifier: &DatabaseId) -> Result<bool> {
        identifier.validate()?;
        let mut registry = self.lock()?;
        registry.check_available(identifier)?;
        let Some(entry) = registry.entries.get(identifier) else {
            return Ok(false);
        };
        // Idle eviction has already retired the instance. Reporting false keeps
        // close honest about this call not unloading anything, while still
        // letting finish_detach remove the lease the eviction left behind.
        if matches!(entry, Entry::Retiring(_)) {
            return Ok(false);
        }
        ensure!(
            matches!(entry, Entry::Loaded(loaded) if loaded.sessions == 0),
            "database has active or loading sessions"
        );
        let epoch = entry.epoch();
        let removed = registry
            .entries
            .insert(identifier.clone(), Entry::Retiring(epoch));
        drop(registry);
        drop(removed);
        Ok(true)
    }

    pub fn drain(&self, identifier: &DatabaseId) -> Result<bool> {
        identifier.validate()?;
        let mut registry = self.lock()?;
        registry.check_available(identifier)?;
        let Some(entry) = registry.entries.get_mut(identifier) else {
            return Ok(false);
        };
        let mut database = None;
        let remove = match entry {
            Entry::Retiring(_) => return Ok(true),
            Entry::Loading(loading) => {
                loading.draining = true;
                loading.activation.set_phase(ActivationPhase::Draining);
                false
            }
            Entry::Loaded(loaded) => {
                loaded.draining = true;
                if loaded.sessions == 0 {
                    database = Some(loaded.database.clone());
                    true
                } else {
                    false
                }
            }
        };
        let epoch = entry.epoch();
        let removed = if remove {
            registry
                .entries
                .insert(identifier.clone(), Entry::Retiring(epoch))
        } else {
            None
        };
        drop(registry);
        if let Some(database) = &database {
            self.pre_suspend(identifier, database);
        }
        drop(removed);
        drop(database);
        Ok(true)
    }

    pub fn evict_idle(&self, idle: Duration) -> Result<Vec<DatabaseId>> {
        let mut registry = self.lock()?;
        registry.observe_all_quarantines();
        let identifiers: Vec<_> = registry
            .entries
            .iter()
            .filter_map(|(identifier, entry)| match entry {
                Entry::Loaded(loaded)
                    if loaded.sessions == 0
                        && !registry.quarantined.contains_key(identifier)
                        && loaded
                            .idle_since
                            .is_some_and(|since| since.elapsed() >= idle) =>
                {
                    Some(identifier.clone())
                }
                _ => None,
            })
            .collect();
        let mut retired = Vec::with_capacity(identifiers.len());
        for identifier in &identifiers {
            let Some(epoch) = registry.entries.get(identifier).map(Entry::epoch) else {
                continue;
            };
            let removed = registry
                .entries
                .insert(identifier.clone(), Entry::Retiring(epoch));
            if let Some(Entry::Loaded(loaded)) = removed {
                retired.push((identifier.clone(), loaded.database.clone()));
            }
        }
        drop(registry);
        // The instance leaves the loaded set only after the same pre-tidy a
        // suspend runs. Without it an idle release could drop a maintenance
        // unit's boundary before it reached storage.
        for (identifier, database) in &retired {
            self.pre_suspend(identifier, database);
        }
        drop(retired);
        Ok(identifiers)
    }

    /// Whether the runtime still holds an instance, or is loading one, for this
    /// database. A retiring entry is not resident: it is already leaving and
    /// new callers must not treat it as available.
    pub(crate) fn is_resident(&self, identifier: &DatabaseId) -> Result<bool> {
        let registry = self.lock()?;
        Ok(matches!(
            registry.entries.get(identifier),
            Some(Entry::Loaded(_) | Entry::Loading(_))
        ))
    }

    pub fn status(&self) -> Result<WorkerStatus> {
        self.report(true).map(|(status, _)| status)
    }

    /// Status plus the memory ledger. The ledger is not a field of `WorkerStatus` because the
    /// controller decodes that report strictly and does not model a memory object yet; the
    /// metrics response carries it as its own top-level key instead.
    pub(crate) fn status_and_memory(&self) -> Result<(WorkerStatus, MemoryBudgetStatus)> {
        self.report(true)
    }

    pub(crate) fn status_for_control_and_memory(
        &self,
    ) -> Result<(WorkerStatus, MemoryBudgetStatus)> {
        self.report(false)
    }

    fn report(&self, include_resources: bool) -> Result<(WorkerStatus, MemoryBudgetStatus)> {
        let mut registry = self.lock()?;
        registry.observe_all_quarantines();
        let resident = self.resident_usage(&registry, 0, 0)?;
        let session_peak = registry.peak_sessions;
        let mut status = WorkerStatus {
            active_sessions: registry.sessions,
            resident_count: registry.live_resident_count(),
            oldest_resident_age_ms: registry.oldest_live_resident_age_ms(),
            quarantined_count: registry.quarantined.len(),
            oldest_quarantined_age_ms: registry.oldest_quarantined_age_ms(),
            ..WorkerStatus::default()
        };
        let mut databases = Vec::new();
        for (identifier, entry) in &registry.entries {
            let (epoch, lifecycle, sessions, idle_ms, database, activation) = match entry {
                Entry::Retiring(epoch) => {
                    status.loaded_databases += 1;
                    (*epoch, DatabaseLifecycle::Draining, 0, None, None, None)
                }
                Entry::Loading(loading) => {
                    status.loading_databases += 1;
                    (
                        loading.epoch,
                        if loading.draining {
                            DatabaseLifecycle::Draining
                        } else {
                            DatabaseLifecycle::Loading
                        },
                        1,
                        None,
                        None,
                        Some(loading.activation.clone()),
                    )
                }
                Entry::Loaded(loaded) => {
                    status.loaded_databases += 1;
                    (
                        loaded.epoch,
                        if loaded.draining {
                            DatabaseLifecycle::Draining
                        } else {
                            DatabaseLifecycle::Open
                        },
                        loaded.sessions,
                        loaded
                            .idle_since
                            .map(|since| since.elapsed().as_millis().min(u64::MAX as u128) as u64),
                        Some(loaded.database.clone()),
                        loaded.activation.clone(),
                    )
                }
            };
            databases.push((
                DatabaseStatus {
                    identifier: identifier.clone(),
                    epoch,
                    lifecycle,
                    active_sessions: sessions,
                    idle_ms,
                    resources: None,
                    resource_error: None,
                },
                database,
                activation,
            ));
        }
        drop(registry);
        let mut observed = include_resources.then(memory::ObservedUsage::default);
        for (mut information, database, activation) in databases {
            if include_resources {
                match database {
                    // A loading entry has no native database to sample; its activation
                    // snapshot is registry state and still reportable.
                    None => {
                        if let Some(activation) = activation {
                            information.resources = Some(DatabaseResources {
                                usage: None,
                                activation: Some(activation.snapshot()),
                            });
                        }
                    }
                    Some(database) => match database.resources() {
                        Ok(Some(usage)) => {
                            if let Some(observed) = &mut observed {
                                observed.segment_bytes = observed
                                    .segment_bytes
                                    .saturating_add(usage.segment_ram_bytes as u64);
                                observed.dirty_bytes = observed
                                    .dirty_bytes
                                    .saturating_add(usage.dirty_segment_bytes);
                                observed.wal_bytes = observed.wal_bytes.saturating_add(
                                    (usage.wal_bytes as u64)
                                        .saturating_add(usage.pending_wal_bytes as u64),
                                );
                                observed.wal_capacity_bytes = observed
                                    .wal_capacity_bytes
                                    .saturating_add(usage.wal_capacity_bytes);
                            }
                            information.resources = Some(DatabaseResources {
                                usage: Some(usage),
                                activation: activation.map(|activation| activation.snapshot()),
                            })
                        }
                        // The native entry is gone (quarantined); keep the partial report
                        // shape: no resources and no activation claim.
                        Ok(None) => {}
                        Err(error) => information.resource_error = Some(error.to_string()),
                    },
                }
            }
            status.databases.push(information);
        }
        let memory = self.memory.status(resident, session_peak, observed);
        Ok((status, memory))
    }

    /// The activation of one database, whether loading or already retained. Used by the
    /// runtime, tests and operator diagnostics; the worker status exposes the same snapshot
    /// under `resources`.
    pub fn activation_snapshot(&self, identifier: &DatabaseId) -> Result<Option<ActivationStatus>> {
        let registry = self.lock()?;
        Ok(match registry.entries.get(identifier) {
            Some(Entry::Loading(loading)) => Some(loading.activation.snapshot()),
            Some(Entry::Loaded(loaded)) => loaded
                .activation
                .as_ref()
                .map(|activation| activation.snapshot()),
            _ => None,
        })
    }

    pub(super) fn release(&self, identifier: &DatabaseId) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.observe_quarantine(identifier);
        let quarantined = registry.quarantined.contains_key(identifier);
        let remove = if let Some(Entry::Loaded(loaded)) = registry.entries.get_mut(identifier) {
            loaded.sessions -= 1;
            if loaded.sessions == 0 {
                loaded.idle_since = Some(Instant::now());
            }
            loaded.draining && loaded.sessions == 0 && !quarantined
        } else {
            false
        };
        registry.release_session();
        let database = if remove {
            match registry.entries.get(identifier) {
                Some(Entry::Loaded(loaded)) => Some(loaded.database.clone()),
                _ => None,
            }
        } else {
            None
        };
        let removed = if remove {
            let epoch = registry.entries.get(identifier).unwrap().epoch();
            registry
                .entries
                .insert(identifier.clone(), Entry::Retiring(epoch))
        } else {
            None
        };
        drop(registry);
        if let Some(database) = &database {
            self.pre_suspend(identifier, database);
        }
        drop(removed);
        drop(database);
    }

    /// One pre-tidy before an instance leaves the loaded set. Its budget limits the waits
    /// that consult the request deadline, not the total duration of the native operation.
    /// It is an optimisation: every failure, refusal or timeout is logged and the unload
    /// still happens. Nothing here reaches the request path, and with the schedule switch
    /// off it costs one boolean read.
    fn pre_suspend(&self, identifier: &DatabaseId, database: &Arc<Instance>) {
        if !self.maintenance.enabled() {
            return;
        }
        let database = match database.database() {
            Ok(database) => database,
            Err(error) => {
                eprintln!(
                    "维护预整理结果：database={identifier:?} lane=none \
                     outcome=database_unavailable started=false elapsed_ms=0 detail={error:#}"
                );
                return;
            }
        };
        let target: Arc<dyn crate::maintenance::MaintenanceTarget> = database;
        let outcome = self.maintenance.pre_suspend(&target);
        eprintln!(
            "维护预整理结果：database={:?} lane={} outcome={} started={} elapsed_ms={} detail={}",
            identifier,
            outcome.lane.map_or("none", |lane| lane.name()),
            outcome.reason.name(),
            outcome.started,
            outcome.elapsed.as_millis(),
            outcome.detail.as_deref().unwrap_or("-"),
        );
    }

    fn lock(&self) -> Result<MutexGuard<'_, Registry>> {
        self.registry
            .lock()
            .map_err(|_| anyhow!("worker registry mutex poisoned"))
    }

    pub(crate) fn ensure_instance_available(&self, identifier: &DatabaseId) -> Result<()> {
        self.lock()?.check_available(identifier)
    }

    pub(super) fn finish_retiring(&self, identifier: &DatabaseId) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.finish_retiring(identifier);
    }

    /// Stops admitting maintenance units and waits, until `deadline`, for the one already
    /// in flight. The unit cannot be cancelled safely; an over-budget wait is reported so
    /// shutdown stays unclean instead of claiming a boundary it did not observe.
    pub(crate) fn quiesce_maintenance(&self, deadline: Instant) -> Result<()> {
        self.maintenance.quiesce(deadline)
    }

    /// One scheduling pass over every database this worker has open.
    ///
    /// This is the only trigger for scheduled maintenance, and its caller is the worker
    /// watchdog — a thread that serves no request. Nothing here runs a unit: the pass only
    /// reads in-process figures, records decisions and queues work for the scheduler's own
    /// thread, so even a slow checkpoint cannot delay the sweep that follows it.
    ///
    /// With `PERI_MAINTENANCE_SCHEDULE` unset this is one static read per loaded database.
    pub(crate) fn maintenance_tick(&self) -> Result<()> {
        if !self.maintenance.enabled() {
            return Ok(());
        }
        // The executor exists only once the scheduler is on, and it starts here rather than
        // from a scheduling pass so a pass always runs against a known executor.
        self.maintenance.start()?;
        let instances: Vec<_> = {
            let mut observers = self
                .native_instances
                .lock()
                .map_err(|_| crate::session::AdmissionError::Quarantine)?;
            observers.retain(|(_, database)| database.strong_count() != 0);
            observers
                .iter()
                .filter_map(|(_, database)| database.upgrade())
                .collect()
        };
        let now = Instant::now();
        let mut first_error = None;
        for database in instances {
            let target: Arc<dyn crate::maintenance::MaintenanceTarget> = database;
            if let Err(error) = self.maintenance.observe(&target, now) {
                // One database that cannot be probed must not stop the sweep for the others.
                // The error is kept and returned, not swallowed.
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// The result of one activation for this caller: the retained instance and, when this
/// caller paid the cold load, the instant its own budget started. A caller that only
/// coalesced with someone else's load has no budget of its own to enforce.
struct Activated {
    instance: Arc<Instance>,
    load_started: Option<Instant>,
}

/// Finishes a `Loading` entry on any exit that did not go through an explicit finalizer,
/// including unwinding. Without it a poisoned registry lock or a panic between publishing
/// the entry and publishing the verdict would leave a permanent `Loading`.
struct LoadingGuard {
    runtime: Arc<WorkerRuntime>,
    identifier: DatabaseId,
    completion: Arc<Completion>,
    activation: Arc<Activation>,
    armed: bool,
}

impl LoadingGuard {
    fn new(
        runtime: Arc<WorkerRuntime>,
        identifier: DatabaseId,
        completion: Arc<Completion>,
        activation: Arc<Activation>,
    ) -> Self {
        Self {
            runtime,
            identifier,
            completion,
            activation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LoadingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut registry = self
            .runtime
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let ours = matches!(
            registry.entries.get(&self.identifier),
            Some(Entry::Loading(loading)) if Arc::ptr_eq(&loading.activation, &self.activation)
        );
        if ours {
            registry.entries.remove(&self.identifier);
            registry.release_session_saturating();
            if !registry.quarantined.contains_key(&self.identifier) {
                registry.resident_since.remove(&self.identifier);
            }
        }
        drop(registry);
        self.activation.set_phase(ActivationPhase::Failed);
        self.completion.finish(Err("database load aborted".into()));
    }
}

/// `PERI_WORKER_MAX_CONCURRENT_LOADS` bounds how many cold loads (and with them WAL
/// replays) run at once. The queue is bounded too, so a burst of callers cannot grow
/// without limit; a queued caller waits only until its own deadline and then leaves.
struct LoadPermits {
    limit: usize,
    queue_limit: usize,
    state: Mutex<LoadPermitState>,
    changed: Condvar,
}

#[derive(Default)]
struct LoadPermitState {
    active: usize,
    waiting: usize,
    peak: usize,
}

impl LoadPermits {
    fn configured(limit: usize, max_open_databases: usize) -> Self {
        Self {
            limit,
            // More distinct databases cannot be loading at once than the registry admits.
            queue_limit: max_open_databases.max(limit).max(1),
            state: Mutex::new(LoadPermitState::default()),
            changed: Condvar::new(),
        }
    }

    fn acquire(&self, deadline: Option<Instant>) -> Result<LoadPermit<'_>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.active < self.limit {
            state.active += 1;
            state.peak = state.peak.max(state.active);
            return Ok(LoadPermit { permits: self });
        }
        if state.waiting >= self.queue_limit {
            return Err(anyhow::Error::new(AdmissionError::QueueFull)
                .context("worker cold load queue full"));
        }
        state.waiting += 1;
        let mut queued = QueuedLoad {
            permits: self,
            armed: true,
        };
        loop {
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        // `QueuedLoad::drop` removes this caller from the queue under the
                        // same mutex; release it first instead of deadlocking on ourselves.
                        drop(state);
                        return Err(cold_load_slot_deadline());
                    }
                    let (next, timeout) = self
                        .changed
                        .wait_timeout(state, deadline.saturating_duration_since(now))
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    state = next;
                    // A slot that freed exactly at the deadline does not turn this caller
                    // into a loader past its own wait deadline.
                    if timeout.timed_out()
                        && (state.active >= self.limit || Instant::now() >= deadline)
                    {
                        drop(state);
                        return Err(cold_load_slot_deadline());
                    }
                }
                None => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
            if state.active < self.limit {
                state.active += 1;
                state.peak = state.peak.max(state.active);
                state.waiting -= 1;
                queued.armed = false;
                return Ok(LoadPermit { permits: self });
            }
        }
    }

    #[cfg(test)]
    fn peak(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .peak
    }

    #[cfg(test)]
    fn waiting(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .waiting
    }
}

struct LoadPermit<'permits> {
    permits: &'permits LoadPermits,
}

impl Drop for LoadPermit<'_> {
    fn drop(&mut self) {
        let mut state = self
            .permits
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active = state.active.saturating_sub(1);
        self.permits.changed.notify_all();
    }
}

struct QueuedLoad<'permits> {
    permits: &'permits LoadPermits,
    armed: bool,
}

impl Drop for QueuedLoad<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .permits
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.waiting = state.waiting.saturating_sub(1);
        self.permits.changed.notify_all();
    }
}

thread_local! {
    /// Installed by the worker RPC open path. It bounds only how long this caller waits on
    /// a shared activation; it is deliberately not the load budget.
    static WAITER_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Sets the calling thread's waiter deadline for the duration of the scope.
pub(crate) struct WaiterDeadlineScope {
    previous: Option<Instant>,
}

impl WaiterDeadlineScope {
    pub(crate) fn until(deadline: Instant) -> Self {
        let previous = WAITER_DEADLINE.replace(Some(deadline));
        Self { previous }
    }
}

impl Drop for WaiterDeadlineScope {
    fn drop(&mut self) {
        WAITER_DEADLINE.set(self.previous);
    }
}

fn current_waiter_deadline() -> Option<Instant> {
    WAITER_DEADLINE.get()
}

/// The caller exited its wait without a load verdict. Admission class `deadline` keeps it
/// distinguishable from a load failure in diagnostics and in the reply.
pub(super) fn activation_wait_deadline() -> anyhow::Error {
    anyhow::Error::new(AdmissionError::Deadline)
        .context("timed out waiting for database activation")
}

fn cold_load_slot_deadline() -> anyhow::Error {
    anyhow::Error::new(AdmissionError::Deadline).context("timed out waiting for a cold load slot")
}

fn max_concurrent_loads_setting() -> Result<usize> {
    match std::env::var(MAX_CONCURRENT_LOADS_ENV) {
        Ok(value) => configured_max_concurrent_loads(Some(&value)),
        Err(std::env::VarError::NotPresent) => configured_max_concurrent_loads(None),
        Err(error) => Err(error.into()),
    }
}

fn configured_max_concurrent_loads(value: Option<&str>) -> Result<usize> {
    let Some(value) = value else {
        return Ok(DEFAULT_MAX_CONCURRENT_LOADS);
    };
    let limit = value
        .parse::<usize>()
        .context("invalid worker cold load concurrency")?;
    ensure!(
        (1..=MAX_CONCURRENT_LOADS_CEILING).contains(&limit),
        "invalid worker cold load concurrency: expected 1..={MAX_CONCURRENT_LOADS_CEILING}"
    );
    Ok(limit)
}

fn load_budget_setting() -> Result<Option<Duration>> {
    match std::env::var(LOAD_BUDGET_ENV) {
        Ok(value) => configured_load_budget(Some(&value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn configured_load_budget(value: Option<&str>) -> Result<Option<Duration>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let millis = value.parse::<u64>().context("invalid worker load budget")?;
    ensure!(
        millis > 0 && millis <= 86_400_000,
        "invalid worker load budget"
    );
    Ok(Some(Duration::from_millis(millis)))
}

/// A load that overran its budget failed before any statement ran, and the reply has to say
/// so: the platform answers an unproven worker failure with an unknown outcome, which is
/// indistinguishable from a write that may have committed.
fn load_budget_exceeded() -> anyhow::Error {
    anyhow!("database load budget exceeded ({LOAD_BUDGET_ENV})").context(SqlEvidence::NotExecuted)
}
