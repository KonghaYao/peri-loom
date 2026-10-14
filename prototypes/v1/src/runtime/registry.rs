use super::{DatabaseId, WorkerRuntime};
use crate::database::RemoteDatabase;
use anyhow::{anyhow, Result};
use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex, Weak},
    time::Instant,
};

#[derive(Default)]
pub(super) struct Registry {
    pub entries: BTreeMap<DatabaseId, Entry>,
    pub sessions: usize,
    pub quarantined: BTreeMap<DatabaseId, u64>,
}

pub(super) enum Entry {
    Loading(Loading),
    Loaded(Loaded),
    Retiring(u64),
}

impl Entry {
    pub fn epoch(&self) -> u64 {
        match self {
            Self::Loading(loading) => loading.epoch,
            Self::Loaded(loaded) => loaded.epoch,
            Self::Retiring(epoch) => *epoch,
        }
    }
}

pub(super) struct Loaded {
    pub epoch: u64,
    pub database: Arc<Instance>,
    pub sessions: usize,
    pub draining: bool,
    pub idle_since: Option<Instant>,
}

pub(super) struct Loading {
    pub epoch: u64,
    pub draining: bool,
    pub completion: Arc<Completion>,
}

pub(super) struct Instance {
    database: Mutex<Option<Arc<RemoteDatabase>>>,
    runtime: Weak<WorkerRuntime>,
    identifier: DatabaseId,
    epoch: u64,
}

impl Instance {
    pub fn new(
        database: Arc<RemoteDatabase>,
        runtime: &Arc<WorkerRuntime>,
        identifier: DatabaseId,
        epoch: u64,
    ) -> Result<Arc<Self>> {
        let mut observers = runtime
            .native_instances
            .lock()
            .map_err(|_| crate::session::AdmissionError::Quarantine)?;
        observers.retain(|(_, database)| database.strong_count() != 0);
        observers.push((identifier.clone(), Arc::downgrade(&database)));
        drop(observers);
        let instance = Arc::new(Self {
            database: Mutex::new(Some(database.clone())),
            runtime: Arc::downgrade(runtime),
            identifier,
            epoch,
        });
        let weak = Arc::downgrade(&instance);
        database.on_native_reclaimed(move || {
            if let Some(instance) = weak.upgrade() {
                instance.record_quarantine();
                let native = instance
                    .database
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                drop(native);
            }
        })?;
        Ok(instance)
    }

    pub fn database(&self) -> Result<Arc<RemoteDatabase>> {
        self.database
            .lock()
            .map_err(|_| crate::session::AdmissionError::Quarantine)?
            .as_ref()
            .cloned()
            .ok_or_else(|| crate::session::AdmissionError::Quarantine.into())
    }

    pub fn resources(&self) -> Result<Option<crate::resources::ResourceUsage>> {
        let database = self
            .database
            .lock()
            .map_err(|_| crate::session::AdmissionError::Quarantine)?
            .clone();
        database.map(|database| database.resources()).transpose()
    }

    fn record_quarantine(&self) {
        if let Some(runtime) = self.runtime.upgrade() {
            let mut registry = runtime
                .registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            registry
                .quarantined
                .entry(self.identifier.clone())
                .or_insert(self.epoch);
        }
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let database = self
            .database
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if database
            .as_ref()
            .is_some_and(|database| database.admission().healthy().is_err())
        {
            self.record_quarantine();
        }
        drop(database);
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.finish_retiring(&self.identifier);
        }
    }
}

impl Loading {
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            draining: false,
            completion: Arc::new(Completion::default()),
        }
    }
}

#[derive(Default)]
pub(super) struct Completion {
    result: Mutex<Option<std::result::Result<(), String>>>,
    changed: Condvar,
}

impl Completion {
    pub fn wait(&self) -> Result<()> {
        let mut result = self
            .result
            .lock()
            .map_err(|_| anyhow!("loading completion mutex poisoned"))?;
        while result.is_none() {
            result = self
                .changed
                .wait(result)
                .map_err(|_| anyhow!("loading completion mutex poisoned"))?;
        }
        result
            .as_ref()
            .unwrap()
            .clone()
            .map_err(|error| anyhow!(error))
    }

    pub fn finish(&self, outcome: std::result::Result<(), String>) {
        *self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
        self.changed.notify_all();
    }
}
