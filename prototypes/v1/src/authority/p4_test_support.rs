use anyhow::{bail, Result};
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
    ownership::types::*,
    runtime::DatabaseId,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::Duration;

pub struct Clock {
    pub time: AtomicU64,
    pub failed: AtomicBool,
}
impl Default for Clock {
    fn default() -> Self {
        Self {
            time: AtomicU64::new(100),
            failed: AtomicBool::new(false),
        }
    }
}
impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        if self.failed.load(Ordering::SeqCst) {
            bail!("clock unavailable");
        }
        Ok(self.time.load(Ordering::SeqCst))
    }
}
impl Clock {
    pub fn set(&self, now: u64) {
        self.time.store(now, Ordering::SeqCst);
    }
}

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
        let (state, timeout) = self
            .changed
            .wait_timeout_while(
                self.state.lock().unwrap(),
                Duration::from_secs(10),
                |state| !state.0,
            )
            .unwrap();
        assert!(
            state.0 && !timeout.timed_out(),
            "raw operation did not enter gate"
        );
    }
    pub fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

pub struct Hook {
    pub needle: String,
    pub skip: usize,
    pub action: Box<dyn FnOnce() + Send>,
}
pub fn run_hook(slot: &Mutex<Option<Hook>>, key: &str) {
    let mut hook = slot.lock().unwrap();
    if let Some(current) = hook.as_mut() {
        if key.contains(&current.needle) {
            if current.skip > 0 {
                current.skip -= 1;
                return;
            }
            let current = hook.take().unwrap();
            drop(hook);
            (current.action)();
        }
    }
}

#[derive(Default)]
pub struct Store {
    pub memory: MemoryObjectStore,
    pub mutations: Mutex<Vec<String>>,
    pub before_put: Mutex<Option<Hook>>,
    pub before_get: Mutex<Option<Hook>>,
    pub before_delete: Mutex<Option<Hook>>,
    pub failure: Mutex<Option<(String, bool)>>,
    pub late: Mutex<Option<(String, Vec<u8>)>>,
    pub unsupported: AtomicBool,
}
impl ObjectStore for Store {
    fn supports_strict_fencing(&self) -> bool {
        !self.unsupported.load(Ordering::SeqCst)
    }
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        run_hook(&self.before_get, key);
        self.memory.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        run_hook(&self.before_put, key);
        self.mutations.lock().unwrap().push(format!("put:{key}"));
        let mut failure = self.failure.lock().unwrap();
        if failure
            .as_ref()
            .is_some_and(|(needle, _)| key.contains(needle))
        {
            let (_, apply) = failure.take().unwrap();
            drop(failure);
            if apply {
                self.memory.put(key, bytes, condition)?;
            } else {
                *self.late.lock().unwrap() = Some((key.into(), bytes.to_vec()));
            }
            bail!("ambiguous backend timeout");
        }
        drop(failure);
        self.memory.put(key, bytes, condition)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        run_hook(&self.before_delete, key);
        self.mutations.lock().unwrap().push(format!("delete:{key}"));
        let mut failure = self.failure.lock().unwrap();
        if failure
            .as_ref()
            .is_some_and(|(needle, _)| key.contains(needle))
        {
            let (_, apply) = failure.take().unwrap();
            drop(failure);
            if apply {
                self.memory.delete(key, expected)?;
            }
            bail!("ambiguous delete timeout");
        }
        drop(failure);
        self.memory.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.memory.list(prefix)
    }
}

pub fn identifier(branch: &str) -> DatabaseId {
    DatabaseId::new(Namespace::new("tenant", "db").unwrap(), branch).unwrap()
}
pub fn fixture() -> Result<(Arc<Store>, Arc<Clock>, Arc<OwnershipAuthority>)> {
    let store = Arc::new(Store::default());
    let clock = Arc::new(Clock::default());
    let authority = OwnershipAuthority::new(store.clone(), clock.clone());
    authority.state(&identifier("main"))?;
    Ok((store, clock, authority))
}
pub fn grant_request(
    authority: &OwnershipAuthority,
    branch: &str,
    epoch: u64,
    expected_epoch: u64,
    request_id: &str,
    ttl_ms: u64,
) -> Result<GrantRequest> {
    let intent = GrantIntent {
        database: identifier(branch),
        worker_id: format!("worker-{epoch}"),
        epoch,
        expected_epoch,
        lease_id: format!("lease-{epoch}"),
        ttl_ms,
        role: AccessRole::Writer,
    };
    let nonce = authority
        .challenge(LeaseIntent::Grant(intent.clone()))?
        .nonce;
    Ok(GrantRequest {
        request_id: request_id.into(),
        nonce,
        intent,
    })
}
pub fn grant(authority: &OwnershipAuthority, epoch: u64, expected: u64) -> Result<GrantReply> {
    authority.grant(grant_request(
        authority,
        "main",
        epoch,
        expected,
        &format!("grant-{epoch}"),
        10000,
    )?)
}
pub fn root(store: &Arc<Store>) -> Result<Arc<Authority>> {
    Authority::open(store.clone(), identifier("main").namespace)
}
pub fn segment(bytes: &[u8]) -> String {
    identifier("main")
        .namespace
        .key(&format!("segments/{}", blake3::hash(bytes).to_hex()))
        .unwrap()
}
pub fn initialize(store: Arc<dyn ObjectStore>) -> Result<String> {
    let manifest = serde_json::to_vec(&peri_loom_v1::manifest::Manifest::default())?;
    let hash = blake3::hash(&manifest).to_hex().to_string();
    store.put(
        &identifier("main")
            .namespace
            .key(&format!("manifests/{hash}"))?,
        &manifest,
        PutCondition::Absent,
    )?;
    store.put(
        &identifier("main").namespace.key("HEAD")?,
        &serde_json::to_vec(&serde_json::json!({"manifest":hash}))?,
        PutCondition::Absent,
    )?;
    Ok(hash)
}
