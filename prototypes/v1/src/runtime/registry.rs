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
    database: Option<Arc<RemoteDatabase>>,
    runtime: Weak<WorkerRuntime>,
    identifier: DatabaseId,
}

impl Instance {
    pub fn new(
        database: Arc<RemoteDatabase>,
        runtime: &Arc<WorkerRuntime>,
        identifier: DatabaseId,
    ) -> Arc<Self> {
        Arc::new(Self {
            database: Some(database),
            runtime: Arc::downgrade(runtime),
            identifier,
        })
    }

    pub fn database(&self) -> &Arc<RemoteDatabase> {
        self.database.as_ref().unwrap()
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        drop(self.database.take());
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
