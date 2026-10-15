use anyhow::{ensure, Context, Result};
use peri_loom_v1::authority::{CatalogRoot, ObjectEntry};
use peri_loom_v1::object_store::{ObjectStore, PutCondition, StoredObject};
use peri_loom_v1::ownership::types::{GrantState, LeaseBinding, LeaseClock, OwnerRecord};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{
    mpsc::{sync_channel, Receiver, SyncSender},
    Arc, Condvar, Mutex,
};
use std::time::Duration;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RootEnvelope {
    root: Box<serde_json::value::RawValue>,
    checksum: String,
}
fn decode_root(bytes: &[u8]) -> Result<CatalogRoot> {
    let envelope: RootEnvelope = serde_json::from_slice(bytes)?;
    let checksum = blake3::hash(envelope.root.get().as_bytes()).to_hex();
    ensure!(envelope.checksum == checksum.as_str(), "bad checksum");
    Ok(serde_json::from_str(envelope.root.get())?)
}
#[derive(Default)]
pub struct CleanupObserver {
    state: Arc<Mutex<CleanupState>>,
}
#[derive(Default)]
struct CleanupState {
    sequence: u64,
    active: Option<CleanupWatch>,
}
#[derive(Clone)]
struct CleanupWatch {
    token: u64,
    key: String,
    branch: String,
    incarnation: String,
    authority_term: u64,
    owner: OwnerRecord,
    remaining_pins: BTreeMap<String, Vec<ObjectEntry>>,
    pin: String,
    sender: SyncSender<CleanupEvent>,
    failed: Arc<AtomicBool>,
    settled: Arc<AtomicBool>,
}
#[derive(Clone, Copy)]
enum CleanupStatus {
    Removed,
}
#[derive(Clone, Copy)]
struct CleanupEvent {
    token: u64,
    status: CleanupStatus,
}
pub struct CleanupCapture {
    token: u64,
    receiver: Receiver<CleanupEvent>,
    failed: Arc<AtomicBool>,
    state: Arc<Mutex<CleanupState>>,
}
impl CleanupObserver {
    pub fn arm(&self, key: String, binding: &LeaseBinding, bytes: &[u8]) -> Result<CleanupCapture> {
        ensure!(
            key == binding.database.namespace.key("authority/root")?,
            "cleanup root key mismatch"
        );
        let root = decode_root(bytes)?;
        let owner = root
            .ownership
            .as_ref()
            .context("cleanup ownership missing")?
            .branches
            .get(&binding.database.branch)
            .context("cleanup owner missing")?
            .clone();
        ensure!(
            matches!(root.version, 2 | 4)
                && root.incarnation == binding.root_incarnation
                && root.ownership.as_ref().unwrap().authority_term == binding.authority_term
                && owner.epoch == binding.epoch
                && owner.branch_incarnation == binding.branch_incarnation
                && owner.worker_id == binding.worker_id
                && owner.lease_id == binding.lease_id
                && owner.grant_id == binding.grant_id
                && owner.renewal_sequence == binding.renewal_sequence
                && owner.role == binding.role
                && owner.state == GrantState::Active,
            "cleanup binding mismatch"
        );
        ensure!(root.pins.len() == 1, "cleanup pin ambiguity");
        let pin = root.pins.keys().next().unwrap().clone();
        let mut remaining_pins = root.pins;
        remaining_pins.remove(&pin);
        let (sender, receiver) = sync_channel(64);
        let failed = Arc::new(AtomicBool::new(false));
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("cleanup observer unavailable"))?;
        ensure!(state.active.is_none(), "cleanup capture already active");
        state.sequence = state
            .sequence
            .checked_add(1)
            .context("cleanup token exhausted")?;
        let token = state.sequence;
        state.active = Some(CleanupWatch {
            token,
            key,
            branch: binding.database.branch.clone(),
            incarnation: root.incarnation,
            authority_term: binding.authority_term,
            owner,
            remaining_pins,
            pin,
            sender,
            failed: failed.clone(),
            settled: Arc::new(AtomicBool::new(false)),
        });
        Ok(CleanupCapture {
            token,
            receiver,
            failed,
            state: self.state.clone(),
        })
    }
    fn before(&self, key: &str) -> Option<CleanupWatch> {
        self.state
            .lock()
            .ok()?
            .active
            .as_ref()
            .filter(|watch| watch.key == key)
            .cloned()
    }
    fn after(&self, watch: CleanupWatch, bytes: &[u8], success: bool) {
        let Ok(root) = decode_root(bytes) else {
            watch.failed.store(true, Ordering::SeqCst);
            return;
        };
        let current = self.state.lock().ok().is_some_and(|state| {
            state
                .active
                .as_ref()
                .is_some_and(|active| active.token == watch.token)
        });
        if !current {
            return;
        }
        let matched = root.ownership.as_ref().is_some_and(|catalog| {
            catalog.authority_term == watch.authority_term
                && catalog.branches.get(&watch.branch) == Some(&watch.owner)
        });
        if !success
            || !matches!(root.version, 2 | 4)
            || root.incarnation != watch.incarnation
            || !matched
        {
            watch.failed.store(true, Ordering::SeqCst);
            return;
        }
        if root.pins.contains_key(&watch.pin) {
            return;
        }
        if root.pins != watch.remaining_pins {
            watch.failed.store(true, Ordering::SeqCst);
            return;
        }
        if !watch.settled.swap(true, Ordering::SeqCst)
            && watch
                .sender
                .try_send(CleanupEvent {
                    token: watch.token,
                    status: CleanupStatus::Removed,
                })
                .is_err()
        {
            watch.failed.store(true, Ordering::SeqCst);
        }
    }
}
impl CleanupCapture {
    pub fn wait(self) -> Result<()> {
        ensure!(
            !self.failed.load(Ordering::SeqCst),
            "cleanup observation unavailable"
        );
        let event = self
            .receiver
            .recv_timeout(Duration::from_secs(5))
            .context("cleanup observation timeout")?;
        ensure!(
            event.token == self.token
                && matches!(event.status, CleanupStatus::Removed)
                && !self.failed.load(Ordering::SeqCst),
            "cleanup observation mismatch"
        );
        Ok(())
    }
}
impl Drop for CleanupCapture {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            if state
                .active
                .as_ref()
                .is_some_and(|watch| watch.token == self.token)
            {
                state.active = None;
            }
        }
    }
}
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
    pub cleanup: Option<Arc<CleanupObserver>>,
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
        let watch = self
            .cleanup
            .as_ref()
            .and_then(|observer| observer.before(key));
        let result = self.inner.put(key, bytes, condition);
        if let (Some(observer), Some(watch)) = (&self.cleanup, watch) {
            observer.after(watch, bytes, result.is_ok());
        }
        result
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}
#[cfg(test)]
mod cleanup_tests {
    use super::*;
    use peri_loom_v1::{
        authority::Authority,
        namespace::Namespace,
        object_store::{MemoryObjectStore, PreconditionFailed},
        ownership::types::{AccessRole, OwnershipCatalog},
        runtime::DatabaseId,
    };
    fn fixture() -> Result<(CleanupObserver, CatalogRoot, LeaseBinding, String)> {
        let namespace = Namespace::new("cleanup", "fixture")?;
        let mut root = Authority::open(Arc::new(MemoryObjectStore::default()), namespace.clone())?
            .snapshot()?
            .root;
        let binding: LeaseBinding = serde_json::from_value(serde_json::json!({
            "database": DatabaseId::new(namespace.clone(), "main")?,
            "root_incarnation": root.incarnation, "authority_term": 1,
            "branch_incarnation": 1, "epoch": 1, "worker_id": "worker",
            "lease_id": "lease", "grant_id": "grant", "renewal_sequence": 0,
            "role": AccessRole::Writer
        }))?;
        let owner = serde_json::from_value(serde_json::json!({
            "epoch": 1, "branch_incarnation": 1, "worker_id": "worker", "lease_id": "lease",
            "grant_id": "grant", "renewal_sequence": 0, "state": GrantState::Active,
            "role": AccessRole::Writer
        }))?;
        root.version = 2;
        root.ownership = Some(OwnershipCatalog {
            authority_term: 1,
            branches: BTreeMap::from([("main".into(), owner)]),
            commands: BTreeMap::new(),
        });
        root.pins.insert("old-session-pin".into(), Vec::new());
        let key = namespace.key("authority/root")?;
        Ok((CleanupObserver::default(), root, binding, key))
    }
    fn encode(root: &CatalogRoot) -> Result<Vec<u8>> {
        let serialized = serde_json::to_string(root)?;
        Ok(format!(
            "{{\"root\":{serialized},\"checksum\":\"{}\"}}",
            blake3::hash(serialized.as_bytes()).to_hex()
        )
        .into_bytes())
    }
    #[test]
    fn successful_identified_pin_removal_emits_only_matching_capture() -> Result<()> {
        let (observer, mut root, binding, key) = fixture()?;
        let capture = observer.arm(key.clone(), &binding, &encode(&root)?)?;
        let watch = observer.before(&key).unwrap();
        root.pins.clear();
        observer.after(watch, &encode(&root)?, true);
        capture.wait()?;
        ensure!(
            observer.state.lock().unwrap().active.is_none(),
            "capture not disarmed"
        );
        Ok(())
    }
    #[test]
    fn unrelated_invalid_or_failed_publications_never_confirm_cleanup() -> Result<()> {
        for mode in [
            "unrelated",
            "wrong-key",
            "new-owner",
            "other-pin",
            "checksum",
            "failed",
        ] {
            let (observer, mut root, binding, key) = fixture()?;
            let capture = observer.arm(key.clone(), &binding, &encode(&root)?)?;
            if mode == "wrong-key" {
                assert!(observer.before(&(key.clone() + "/suffix")).is_none());
            } else {
                let watch = observer.before(&key).unwrap();
                if mode != "unrelated" {
                    root.pins.clear();
                }
                if mode == "new-owner" {
                    root.ownership
                        .as_mut()
                        .unwrap()
                        .branches
                        .get_mut("main")
                        .unwrap()
                        .epoch = 2;
                }
                if mode == "other-pin" {
                    root.pins.insert("another-pin".into(), Vec::new());
                }
                let bytes = if mode == "checksum" {
                    b"{}".to_vec()
                } else {
                    encode(&root)?
                };
                observer.after(watch, &bytes, mode != "failed");
            }
            assert!(capture.receiver.try_recv().is_err());
            assert_eq!(
                capture.failed.load(Ordering::SeqCst),
                !matches!(mode, "unrelated" | "wrong-key")
            );
        }
        Ok(())
    }
    #[test]
    fn delayed_publication_cannot_adopt_rearmed_capture() -> Result<()> {
        let (observer, mut root, binding, key) = fixture()?;
        let previous = observer.arm(key.clone(), &binding, &encode(&root)?)?;
        let delayed = observer.before(&key).unwrap();
        drop(previous);
        let current = observer.arm(key.clone(), &binding, &encode(&root)?)?;
        root.pins.clear();
        observer.after(delayed, &encode(&root)?, true);
        assert!(current.receiver.try_recv().is_err());
        assert!(!current.failed.load(Ordering::SeqCst));
        observer.after(observer.before(&key).unwrap(), &encode(&root)?, true);
        current.wait()
    }
    #[test]
    fn full_disconnected_and_stale_channels_fail_closed() -> Result<()> {
        for mode in ["full", "disconnected", "stale"] {
            let (observer, mut root, binding, key) = fixture()?;
            let mut capture = observer.arm(key.clone(), &binding, &encode(&root)?)?;
            let watch = observer.before(&key).unwrap();
            let event = CleanupEvent {
                token: watch.token + u64::from(mode == "stale"),
                status: CleanupStatus::Removed,
            };
            if mode == "disconnected" {
                drop(std::mem::replace(&mut capture.receiver, sync_channel(1).1));
            } else {
                for _ in 0..if mode == "full" { 64 } else { 1 } {
                    watch.sender.try_send(event).unwrap();
                }
            }
            root.pins.clear();
            observer.after(watch, &encode(&root)?, true);
            assert!(capture.wait().is_err());
        }
        Ok(())
    }
    #[test]
    fn observer_never_replaces_backend_failure_or_success() -> Result<()> {
        let (observer, mut root, binding, key) = fixture()?;
        let inner = Arc::new(MemoryObjectStore::default());
        let initial = encode(&root)?;
        inner.put(&key, &initial, PutCondition::Absent)?;
        let capture = observer.arm(key.clone(), &binding, &initial)?;
        let store = BlockingStore {
            inner: inner.clone(),
            gate: Arc::new(Gate::default()),
            cleanup: Some(Arc::new(observer)),
        };
        root.pins.clear();
        let candidate = encode(&root)?;
        let absent = PutCondition::Absent;
        let original = inner.put(&key, &candidate, absent.clone()).unwrap_err();
        let observed = store.put(&key, &candidate, absent).unwrap_err();
        assert_eq!(original.to_string(), observed.to_string());
        assert_eq!(
            original.is::<PreconditionFailed>(),
            observed.is::<PreconditionFailed>()
        );
        assert!(capture.wait().is_err());
        let invalid = store
            .cleanup
            .as_ref()
            .unwrap()
            .arm(key.clone(), &binding, &initial)?;
        let result = store.put(&key, b"invalid-observed-root", PutCondition::Any);
        assert!(result.is_ok());
        assert!(invalid.wait().is_err());
        Ok(())
    }
}
