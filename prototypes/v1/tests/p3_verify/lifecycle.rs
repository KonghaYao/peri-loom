use super::{identifier, memory};
use anyhow::{bail, Result};
use peri_loom_v1::{
    object_store::{ObjectStore, PutCondition, StoredObject},
    provider::LocalAuthorityProvider,
    runtime::{DatabaseId, DatabaseProvider, WorkerLimits, WorkerRuntime},
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Gate {
    fn block(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.1)
            .unwrap();
        assert!(
            state.1 && !timeout.timed_out(),
            "verification gate was not released"
        );
    }

    fn entered(&self) {
        let state = self.state.lock().unwrap();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.0)
            .unwrap();
        assert!(
            state.0 && !timeout.timed_out(),
            "verification gate was not entered"
        );
    }

    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

struct Provider {
    inner: LocalAuthorityProvider,
    load: Arc<Gate>,
    retiring: Arc<Gate>,
    calls: AtomicUsize,
    fail: AtomicBool,
    block_drop: AtomicBool,
}

impl DatabaseProvider for Provider {
    fn open_store(&self, id: &DatabaseId, epoch: u64) -> Result<Arc<dyn ObjectStore>> {
        if id.namespace.database == "blocked" {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.load.block();
            if self.fail.load(Ordering::SeqCst) {
                bail!("independent injected failure");
            }
        }
        let store = self.inner.open_store(id, epoch)?;
        if id.namespace.database == "blocked" && self.block_drop.load(Ordering::SeqCst) {
            Ok(Arc::new(Dropping {
                inner: store,
                gate: self.retiring.clone(),
            }))
        } else {
            Ok(store)
        }
    }
}

struct Dropping {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<Gate>,
}

impl Drop for Dropping {
    fn drop(&mut self) {
        self.gate.block();
    }
}

impl ObjectStore for Dropping {
    fn validate_session(&self) -> Result<()> {
        self.inner.validate_session()
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

fn provider() -> Arc<Provider> {
    Arc::new(Provider {
        inner: LocalAuthorityProvider::new(memory()),
        load: Arc::new(Gate::default()),
        retiring: Arc::new(Gate::default()),
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
        block_drop: AtomicBool::new(false),
    })
}

#[test]
fn coalesced_loading_drain_keeps_native_retirement_reserved_until_drop() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let provider = provider();
    provider.block_drop.store(true, Ordering::SeqCst);
    let runtime = WorkerRuntime::new(
        provider.clone(),
        cache.path(),
        WorkerLimits {
            max_open_databases: 1,
            ..WorkerLimits::default()
        },
    )?;
    let id = identifier("verify", "blocked");
    let opening = {
        let runtime = runtime.clone();
        let id = id.clone();
        thread::spawn(move || runtime.open(id, 7))
    };
    provider.load.entered();
    let waiting = {
        let runtime = runtime.clone();
        let id = id.clone();
        thread::spawn(move || runtime.open(id, 7))
    };
    thread::sleep(Duration::from_millis(30));
    assert_eq!(runtime.status()?.loading_databases, 1);
    assert_eq!(runtime.status()?.active_sessions, 1);
    assert!(runtime.drain(&id)?);
    provider.load.release();
    provider.retiring.entered();
    let (sender, receiver) = mpsc::channel();
    let probe = {
        let runtime = runtime.clone();
        let id = id.clone();
        thread::spawn(move || {
            sender
                .send((
                    runtime.open(id, 8).is_err(),
                    runtime.open(identifier("verify", "other"), 7).is_err(),
                    runtime.status().unwrap().loaded_databases,
                ))
                .unwrap();
        })
    };
    let observed = receiver.recv_timeout(Duration::from_secs(2));
    provider.retiring.release();
    assert_eq!(observed?, (true, true, 1));
    probe.join().unwrap();
    assert!(opening.join().unwrap().is_err());
    assert!(waiting.join().unwrap().is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.status()?.active_sessions, 0);
    assert_eq!(runtime.status()?.loaded_databases, 0);
    provider.block_drop.store(false, Ordering::SeqCst);
    drop(runtime.open(id, 8)?);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?.len(), 1);
    Ok(())
}

#[test]
fn failed_coalesced_open_does_not_consume_other_tenant_capacity() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let provider = provider();
    provider.fail.store(true, Ordering::SeqCst);
    let runtime = WorkerRuntime::new(
        provider.clone(),
        cache.path(),
        WorkerLimits {
            max_open_databases: 2,
            max_total_sessions: 2,
            ..WorkerLimits::default()
        },
    )?;
    let first = {
        let runtime = runtime.clone();
        thread::spawn(move || runtime.open(identifier("verify", "blocked"), 1))
    };
    provider.load.entered();
    let second = {
        let runtime = runtime.clone();
        thread::spawn(move || runtime.open(identifier("verify", "blocked"), 1))
    };
    thread::sleep(Duration::from_millis(30));
    let other = runtime.open(identifier("neighbor", "same-name"), 1)?;
    assert_eq!(runtime.status()?.active_sessions, 2);
    provider.load.release();
    assert!(first.join().unwrap().is_err());
    assert!(second.join().unwrap().is_err());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.status()?.active_sessions, 1);
    assert_eq!(runtime.status()?.loading_databases, 0);
    provider.fail.store(false, Ordering::SeqCst);
    let retry = runtime.open(identifier("verify", "blocked"), 1)?;
    retry.execute("CREATE TABLE retry(value INTEGER)")?;
    drop(retry);
    drop(other);
    assert_eq!(runtime.evict_idle(Duration::ZERO)?.len(), 2);
    Ok(())
}
