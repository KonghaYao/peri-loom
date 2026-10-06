use anyhow::{bail, Result};
use peri_loom_v1::{
    authority::Authority,
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
    runtime::{DatabaseId, DatabaseProvider, WorkerLimits, WorkerRuntime},
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
pub struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Gate {
    pub fn enter(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    pub fn entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |state| !state.0)
            .unwrap();
        assert!(
            state.0 && !timeout.timed_out(),
            "provider did not enter gate"
        );
    }

    pub fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
pub struct Provider {
    pub physical: Arc<MemoryObjectStore>,
    pub calls: AtomicUsize,
    pub failures: AtomicUsize,
    pub slow: Mutex<Option<(String, Arc<Gate>)>>,
    pub slow_drop: Mutex<Option<(String, Arc<Gate>)>>,
}

impl DatabaseProvider for Provider {
    fn open_store(&self, identifier: &DatabaseId, _: u64) -> Result<Arc<dyn ObjectStore>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let gate = self.slow.lock().unwrap().clone();
        if let Some((database, gate)) = gate {
            if database == identifier.namespace.database {
                gate.enter();
            }
        }
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok()
        {
            bail!("injected provider failure");
        }
        let store = Authority::open(self.physical.clone(), identifier.namespace.clone())?
            .branch_store(&identifier.branch)?;
        let slow_drop = self.slow_drop.lock().unwrap().clone();
        if let Some((database, gate)) = slow_drop {
            if database == identifier.namespace.database {
                return Ok(Arc::new(DroppingStore { inner: store, gate }));
            }
        }
        Ok(store)
    }
}

struct DroppingStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<Gate>,
}

impl ObjectStore for DroppingStore {
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
    fn validate_session(&self) -> Result<()> {
        self.inner.validate_session()
    }
}

impl Drop for DroppingStore {
    fn drop(&mut self) {
        self.gate.enter();
    }
}

pub fn identifier(database: &str) -> DatabaseId {
    DatabaseId::new(Namespace::new("tenant", database).unwrap(), "main").unwrap()
}

pub fn fixture(
    limits: WorkerLimits,
) -> Result<(tempfile::TempDir, Arc<Provider>, Arc<WorkerRuntime>)> {
    let cache = tempfile::tempdir()?;
    let provider = Arc::new(Provider::default());
    let runtime = WorkerRuntime::new(provider.clone(), cache.path(), limits)?;
    Ok((cache, provider, runtime))
}
