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
pub use types::{
    DatabaseId, DatabaseLifecycle, DatabaseProvider, DatabaseStatus, WorkerLimits, WorkerStatus,
};

use crate::database::RemoteDatabase;
use crate::maintenance::MaintenanceScheduler;
use crate::session::{Budget, RequestDeadline, SqlEvidence};
use anyhow::{anyhow, ensure, Context, Result};
use registry::{Entry, Instance, Loaded, Loading, Registry};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
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

pub struct WorkerRuntime {
    key_provider: Option<Arc<dyn crate::encryption::DatabaseKeyProvider>>,
    provider: Arc<dyn DatabaseProvider>,
    cache_root: PathBuf,
    limits: WorkerLimits,
    memory_reservation_budget: Option<usize>,
    /// Configured override; the effective budget is resolved per open, because the
    /// statement budget it falls back to may be set after this runtime exists.
    load_budget: Option<Duration>,
    registry: Mutex<Registry>,
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
        Ok(Arc::new(Self {
            key_provider,
            provider,
            cache_root: cache_root.as_ref().to_path_buf(),
            limits,
            memory_reservation_budget: memory::cgroup_reservation_budget(),
            load_budget: load_budget_setting()?,
            registry: Mutex::new(Registry::default()),
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
    /// The load is not a statement: it runs with the request deadline suspended and its
    /// duration is checked against `load_budget` instead, which is what `PERI_WORKER_LOAD_MS`
    /// overrides. The session is anchored after the load returns, so neither phase inherits a
    /// deadline the other one has already spent.
    pub fn open(self: &Arc<Self>, identifier: DatabaseId, epoch: u64) -> Result<DatabaseHandle> {
        let database = self.load_instance(identifier.clone(), epoch)?;
        let _session_budget =
            RequestDeadline::scope(Instant::now() + Budget::trusted_default().wall_duration());
        self.make_handle(identifier, database)
    }

    fn load_instance(
        self: &Arc<Self>,
        identifier: DatabaseId,
        epoch: u64,
    ) -> Result<Arc<Instance>> {
        // Suspended rather than narrowed to the load budget: `RequestDeadline::new` takes the
        // smaller of the ambient deadline and its own wall budget, so a load scope that the
        // load has already overrun would make the internal call below report a statement
        // deadline again. The budget is checked where the load ends because the load cannot be
        // interrupted: its cost is the per-chunk WAL replay, which takes no deadline at all.
        let _load_budget = RequestDeadline::suspend();
        identifier.validate()?;
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
                        drop(registry);
                        completion.wait()?;
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
                        registry.sessions += 1;
                        drop(registry);
                        return Ok(database);
                    }
                }
            }
            registry.check_capacity(self.limits.max_open_databases)?;
            ensure!(
                registry.sessions < self.limits.max_total_sessions,
                "worker session capacity exceeded"
            );
            self.check_memory_reservation(&registry, 1)?;
            let loading = Loading::new(epoch);
            let completion = loading.completion.clone();
            registry
                .resident_since
                .insert(identifier.clone(), Instant::now());
            registry
                .entries
                .insert(identifier.clone(), Entry::Loading(loading));
            registry.sessions += 1;
            drop(registry);
            let load_started = Instant::now();
            let opened = self
                .load(&identifier, epoch)
                .and_then(|database| Instance::new(database, self, identifier.clone(), epoch));
            let loaded_for = load_started.elapsed();
            let mut registry = self.lock()?;
            let draining = matches!(registry.entries.get(&identifier), Some(Entry::Loading(loading)) if loading.draining);
            match opened {
                Ok(database) if !draining => {
                    registry.entries.insert(
                        identifier.clone(),
                        Entry::Loaded(Loaded {
                            epoch,
                            database: database.clone(),
                            sessions: 1,
                            draining: false,
                            idle_since: None,
                        }),
                    );
                    drop(registry);
                    // Waiters are released before the budget is checked, so the budget only
                    // judges the open that paid the load: an open that arrived while this one
                    // was loading either waits here or finds the instance resident, and both
                    // paths succeed. Retaining the instance for them is the same intent as
                    // retaining it below — an overrunning load is never paid twice, and the
                    // overrun error belongs to the open that asked for it.
                    completion.finish(Ok(()));
                    if loaded_for > self.load_budget() {
                        // The load itself succeeded and only overran its budget. The instance
                        // stays resident, so the next open reuses it instead of paying the
                        // load again, and the failure is reported as the load overrun rather
                        // than as a statement deadline the load never had.
                        self.release(&identifier);
                        return Err(load_budget_exceeded());
                    }
                    return Ok(database);
                }
                outcome => {
                    if outcome.is_ok() {
                        registry
                            .entries
                            .insert(identifier.clone(), Entry::Retiring(epoch));
                    } else {
                        registry.entries.remove(&identifier);
                        if !registry.quarantined.contains_key(&identifier) {
                            registry.resident_since.remove(&identifier);
                        }
                    }
                    registry.sessions -= 1;
                    drop(registry);
                    let error = match outcome {
                        Ok(database) => {
                            drop(database);
                            anyhow!("database is draining")
                        }
                        Err(error) => error,
                    };
                    completion.finish(Err(format!("{error:#}")));
                    return Err(error);
                }
            }
        }
    }

    fn check_memory_reservation(
        &self,
        registry: &Registry,
        additional_databases: usize,
    ) -> Result<()> {
        if let Some(budget) = self.memory_reservation_budget {
            ensure!(
                memory::fits_reservation(
                    registry.entries.len().saturating_add(additional_databases),
                    registry.sessions.saturating_add(1),
                    self.limits.segment_ram_bytes,
                    budget,
                ),
                "worker memory reservation capacity exceeded"
            );
        }
        Ok(())
    }

    fn load(&self, identifier: &DatabaseId, epoch: u64) -> Result<Arc<RemoteDatabase>> {
        crate::metrics::Metrics::global().observe(
            crate::metrics::Operation::ColdOpen,
            || self.load_native(identifier, epoch),
            |_| 0,
        )
    }

    fn load_native(&self, identifier: &DatabaseId, epoch: u64) -> Result<Arc<RemoteDatabase>> {
        let store = self.provider.open_store(identifier, epoch)?;
        store.validate_transaction(false)?;
        if let Some(provider) = &self.key_provider {
            let key = provider.resolve(identifier, epoch)?;
            store.validate_transaction(false)?;
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
        Ok(Arc::new(RemoteDatabase::open_with_limits(
            store,
            identifier.namespace.clone(),
            self.cache_root.join(identifier.cache_key()?),
            epoch,
            self.limits.segment_ram_bytes,
            self.limits.segment_disk_bytes,
        )?))
    }

    fn make_handle(
        self: &Arc<Self>,
        identifier: DatabaseId,
        database: Arc<Instance>,
    ) -> Result<DatabaseHandle> {
        match database.database().and_then(|database| database.session()) {
            Ok(session) => Ok(DatabaseHandle::new(
                self.clone(),
                identifier,
                database,
                session,
            )),
            Err(error) => {
                // Only the session slot is released. The instance stays resident, so an open
                // that failed after a successful load does not charge the next attempt for a
                // second cold load; the idle sweep still retires it.
                drop(database);
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
        let removed: Vec<_> = identifiers
            .iter()
            .filter_map(|identifier| {
                let epoch = registry.entries.get(identifier)?.epoch();
                registry
                    .entries
                    .insert(identifier.clone(), Entry::Retiring(epoch))
            })
            .collect();
        drop(registry);
        drop(removed);
        Ok(identifiers)
    }

    pub fn status(&self) -> Result<WorkerStatus> {
        self.status_with_resources(true)
    }

    pub(crate) fn status_for_control(&self) -> Result<WorkerStatus> {
        self.status_with_resources(false)
    }

    fn status_with_resources(&self, include_resources: bool) -> Result<WorkerStatus> {
        let mut registry = self.lock()?;
        registry.observe_all_quarantines();
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
            let (epoch, lifecycle, sessions, idle_ms, database) = match entry {
                Entry::Retiring(epoch) => {
                    status.loaded_databases += 1;
                    (*epoch, DatabaseLifecycle::Draining, 0, None, None)
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
            ));
        }
        drop(registry);
        for (mut information, database) in databases {
            if include_resources {
                match database.map(|database| database.resources()).transpose() {
                    Ok(resources) => information.resources = resources.flatten(),
                    Err(error) => information.resource_error = Some(error.to_string()),
                }
            }
            status.databases.push(information);
        }
        Ok(status)
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
        registry.sessions -= 1;
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
