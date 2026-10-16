mod capacity;
mod handle;
mod health;
mod memory;
mod quarantine;
mod registry;
pub mod types;

pub use handle::DatabaseHandle;
pub use types::{
    DatabaseId, DatabaseLifecycle, DatabaseProvider, DatabaseStatus, WorkerLimits, WorkerStatus,
};

use crate::database::RemoteDatabase;
use crate::maintenance::MaintenanceScheduler;
use anyhow::{anyhow, ensure, Result};
use registry::{Entry, Instance, Loaded, Loading, Registry};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

pub struct WorkerRuntime {
    key_provider: Option<Arc<dyn crate::encryption::DatabaseKeyProvider>>,
    provider: Arc<dyn DatabaseProvider>,
    cache_root: PathBuf,
    limits: WorkerLimits,
    memory_reservation_budget: Option<usize>,
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
            registry: Mutex::new(Registry::default()),
            native_instances: Mutex::new(Vec::new()),
            maintenance: Arc::new(MaintenanceScheduler::configured()?),
        }))
    }

    pub fn open(self: &Arc<Self>, identifier: DatabaseId, epoch: u64) -> Result<DatabaseHandle> {
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
                        return self.make_handle(identifier, database);
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
            let opened = self
                .load(&identifier, epoch)
                .and_then(|database| Instance::new(database, self, identifier.clone(), epoch));
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
                    completion.finish(Ok(()));
                    return self.make_handle(identifier, database);
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
        let remove = match entry {
            Entry::Retiring(_) => return Ok(true),
            Entry::Loading(loading) => {
                loading.draining = true;
                false
            }
            Entry::Loaded(loaded) => {
                loaded.draining = true;
                loaded.sessions == 0
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
        drop(removed);
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
        let removed = if remove {
            let epoch = registry.entries.get(identifier).unwrap().epoch();
            registry
                .entries
                .insert(identifier.clone(), Entry::Retiring(epoch))
        } else {
            None
        };
        drop(registry);
        drop(removed);
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
