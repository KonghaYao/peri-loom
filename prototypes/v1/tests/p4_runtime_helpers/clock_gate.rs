use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::{ensure, Result};
use peri_loom_v1::object_store::{ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::ownership::types::LeaseClock;

#[derive(Default)]
pub struct FakeClock(pub AtomicU64);

impl LeaseClock for FakeClock {
    fn now_ms(&self) -> Result<u64> {
        let now = self.0.load(Ordering::SeqCst);
        ensure!(now != u64::MAX, "injected clock failure");
        Ok(now)
    }
}

#[derive(Default)]
pub struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    remaining: usize,
    entered: bool,
    released: bool,
}

impl Gate {
    pub fn arm(&self, root_gets: usize) {
        *self.state.lock().unwrap() = GateState {
            remaining: root_gets,
            entered: false,
            released: false,
        };
    }

    pub fn wait_entered(&self) {
        let (state, timeout) = self
            .changed
            .wait_timeout_while(
                self.state.lock().unwrap(),
                Duration::from_secs(10),
                |state| !state.entered,
            )
            .unwrap();
        assert!(
            state.entered && !timeout.timed_out(),
            "authority root GET did not reach barrier"
        );
    }

    pub fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }

    fn pause(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.remaining == 0 {
            return Ok(());
        }
        state.remaining -= 1;
        if state.remaining != 0 {
            return Ok(());
        }
        state.entered = true;
        self.changed.notify_all();
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |state| !state.released)
            .unwrap();
        ensure!(
            state.released && !timeout.timed_out(),
            "authority race barrier timed out"
        );
        Ok(())
    }
}

pub struct BlockingStore {
    pub inner: Arc<dyn ObjectStore>,
    pub gate: Arc<Gate>,
}

impl ObjectStore for BlockingStore {
    fn supports_strict_fencing(&self) -> bool {
        self.inner.supports_strict_fencing()
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if key.ends_with("/authority/root") {
            self.gate.pause()?;
        }
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
