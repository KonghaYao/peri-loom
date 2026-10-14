use anyhow::{ensure, Result};
use peri_loom_v1::{
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
    ownership::types::LeaseClock,
};
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

pub struct Clock(pub AtomicU64);

impl Clock {
    pub fn new(now: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(now)))
    }
    pub fn set(&self, now: u64) {
        self.0.store(now, Ordering::SeqCst);
    }
}

impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

#[derive(Default)]
pub struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Gate {
    pub fn block(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.1)
            .unwrap();
        ensure!(state.1, "independent physical gate release timeout");
        Ok(())
    }

    pub fn entered(&self) -> Result<()> {
        let state = self.state.lock().unwrap();
        let (state, _) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.0)
            .unwrap();
        ensure!(state.0, "independent physical gate entry timeout");
        Ok(())
    }

    pub fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

struct Injection {
    prefix: String,
    gate: Arc<Gate>,
    detached: Option<mpsc::Sender<Result<String>>>,
}

pub struct ProbeStore {
    pub inner: Arc<MemoryObjectStore>,
    clock: Arc<Clock>,
    armed: Mutex<Option<Injection>>,
    pub puts: Mutex<Vec<(String, u64)>>,
    pub completed_puts: Arc<AtomicUsize>,
}

impl ProbeStore {
    pub fn new(clock: Arc<Clock>) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MemoryObjectStore::default()),
            clock,
            armed: Mutex::new(None),
            puts: Mutex::new(Vec::new()),
            completed_puts: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn pause(&self, prefix: String, gate: Arc<Gate>) {
        *self.armed.lock().unwrap() = Some(Injection {
            prefix,
            gate,
            detached: None,
        });
    }

    pub fn unknown(&self, prefix: String, gate: Arc<Gate>) -> mpsc::Receiver<Result<String>> {
        let (sender, receiver) = mpsc::channel();
        *self.armed.lock().unwrap() = Some(Injection {
            prefix,
            gate,
            detached: Some(sender),
        });
        receiver
    }

    pub fn put_count(&self) -> usize {
        self.puts.lock().unwrap().len()
    }
}

impl ObjectStore for ProbeStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.puts
            .lock()
            .unwrap()
            .push((key.into(), self.clock.now_ms()?));
        let injection = {
            let mut armed = self.armed.lock().unwrap();
            if armed
                .as_ref()
                .is_some_and(|injection| key.starts_with(&injection.prefix))
            {
                armed.take()
            } else {
                None
            }
        };
        if let Some(injection) = injection {
            if let Some(sender) = injection.detached {
                let inner = self.inner.clone();
                let completed = self.completed_puts.clone();
                let key = key.to_owned();
                let bytes = bytes.to_vec();
                thread::spawn(move || {
                    let result = injection
                        .gate
                        .block()
                        .and_then(|()| inner.put(&key, &bytes, condition));
                    completed.fetch_add(1, Ordering::SeqCst);
                    let _ = sender.send(result);
                });
                anyhow::bail!("independent injected socket close: physical outcome unknown");
            }
            injection.gate.block()?;
        }
        let result = self.inner.put(key, bytes, condition);
        self.completed_puts.fetch_add(1, Ordering::SeqCst);
        result
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}
