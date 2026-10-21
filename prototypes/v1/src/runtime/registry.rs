use super::{
    activation_wait_deadline,
    types::{ActivationPhase, ActivationStatus},
    DatabaseId, WorkerRuntime,
};
use crate::database::RemoteDatabase;
use anyhow::{anyhow, Result};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex, Weak,
    },
    time::{Duration, Instant},
};

#[derive(Default)]
pub(super) struct Registry {
    pub entries: BTreeMap<DatabaseId, Entry>,
    pub sessions: usize,
    /// High-water session count. Admission updates it while it holds this registry, so the
    /// peak is the true admitted peak and not a periodic sample.
    pub peak_sessions: usize,
    pub quarantined: BTreeMap<DatabaseId, u64>,
    pub quarantined_since: BTreeMap<DatabaseId, Instant>,
    pub resident_since: BTreeMap<DatabaseId, Instant>,
}

impl Registry {
    pub fn admit_session(&mut self) {
        self.sessions += 1;
        self.peak_sessions = self.peak_sessions.max(self.sessions);
    }

    pub fn release_session(&mut self) {
        self.sessions -= 1;
    }

    /// The loading guard can run after another error path already removed its session, so it
    /// must not panic on the decrement it may no longer own.
    pub fn release_session_saturating(&mut self) {
        self.sessions = self.sessions.saturating_sub(1);
    }
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
    pub activation: Option<Arc<Activation>>,
}

pub(super) struct Loading {
    pub epoch: u64,
    pub draining: bool,
    pub completion: Arc<Completion>,
    pub activation: Arc<Activation>,
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
            registry
                .quarantined_since
                .entry(self.identifier.clone())
                .or_insert_with(Instant::now);
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
            activation: Arc::new(Activation::new(epoch)),
        }
    }
}

#[derive(Clone, Copy)]
struct ActivationProgress {
    phase: ActivationPhase,
    replayed_bytes: Option<u64>,
}

/// The shared side of one cold load. It outlives any single caller: a waiter that reaches
/// its own deadline leaves without changing this state, and the load keeps its own budget.
pub(super) struct Activation {
    epoch: u64,
    started: Instant,
    progress: Mutex<ActivationProgress>,
    waiters: AtomicUsize,
}

impl Activation {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            started: Instant::now(),
            progress: Mutex::new(ActivationProgress {
                phase: ActivationPhase::OpeningStore,
                replayed_bytes: None,
            }),
            waiters: AtomicUsize::new(0),
        }
    }

    pub fn set_phase(&self, phase: ActivationPhase) {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase = phase;
    }

    pub fn set_replayed_bytes(&self, replayed_bytes: Option<u64>) {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replayed_bytes = replayed_bytes;
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub(super) fn waiter(&self) -> ActivationWaiter<'_> {
        self.waiters.fetch_add(1, Ordering::AcqRel);
        ActivationWaiter { activation: self }
    }

    pub fn snapshot(&self) -> ActivationStatus {
        let progress = *self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ActivationStatus {
            epoch: self.epoch,
            phase: progress.phase,
            elapsed_ms: self.elapsed().as_millis().min(u64::MAX as u128) as u64,
            waiters: self.waiters.load(Ordering::Acquire),
            replayed_bytes: progress.replayed_bytes,
        }
    }
}

pub(super) struct ActivationWaiter<'activation> {
    activation: &'activation Activation,
}

impl Drop for ActivationWaiter<'_> {
    fn drop(&mut self) {
        self.activation.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
pub(super) struct Completion {
    result: Mutex<Option<std::result::Result<(), String>>>,
    changed: Condvar,
}

impl Completion {
    /// Waits for the shared result until `deadline`. A timeout returns a deadline error and
    /// leaves the load untouched: it is not a load verdict, and the remaining callers (and
    /// the retained instance) are unaffected.
    pub fn wait(&self, deadline: Option<Instant>) -> Result<()> {
        let mut result = self
            .result
            .lock()
            .map_err(|_| anyhow!("loading completion mutex poisoned"))?;
        loop {
            if let Some(outcome) = result.as_ref() {
                return outcome.clone().map_err(|error| anyhow!(error));
            }
            match deadline {
                Some(deadline) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(activation_wait_deadline());
                    }
                    let (next, timeout) = self
                        .changed
                        .wait_timeout(result, deadline.saturating_duration_since(now))
                        .map_err(|_| anyhow!("loading completion mutex poisoned"))?;
                    result = next;
                    if timeout.timed_out() && result.is_none() {
                        return Err(activation_wait_deadline());
                    }
                }
                None => {
                    result = self
                        .changed
                        .wait(result)
                        .map_err(|_| anyhow!("loading completion mutex poisoned"))?;
                }
            }
        }
    }

    /// First verdict wins. A completion guard that runs late must not overwrite the real
    /// outcome the load already published.
    pub fn finish(&self, outcome: std::result::Result<(), String>) {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if result.is_none() {
            *result = Some(outcome);
            self.changed.notify_all();
        }
    }
}
