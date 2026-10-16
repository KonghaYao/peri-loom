use super::{
    grant_request, identifier,
    support::{Clock, Gate},
};
use anyhow::{ensure, Result};
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    object_store::{
        GroupWrite, MemoryObjectStore, ObjectStore, PreconditionFailed, PutCondition, StoredObject,
    },
    ownership::types::{LeaseClock, LeaseIntent, RenewIntent, RenewRequest},
    runtime::DatabaseId,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Default)]
struct FaultStore {
    inner: MemoryObjectStore,
    read_fault: Mutex<Option<(String, usize)>>,
    gate: Mutex<Option<Arc<Gate>>>,
    reads: AtomicUsize,
    puts: AtomicUsize,
    groups: AtomicUsize,
    deletes: AtomicUsize,
    group_sizes: Mutex<Vec<usize>>,
    lose_root_ack: AtomicBool,
    lose_boot_ack: AtomicBool,
    reject_boot: AtomicBool,
    fail_readback: AtomicBool,
    declares_group_atomicity: AtomicBool,
}

impl FaultStore {
    fn fail(&self, key: String, skip: usize) {
        *self.read_fault.lock().unwrap() = Some((key, skip));
    }
    fn writes(&self) -> usize {
        self.puts.load(Ordering::SeqCst)
            + self.groups.load(Ordering::SeqCst)
            + self.deletes.load(Ordering::SeqCst)
    }
    fn reset_groups(&self) {
        self.group_sizes.lock().unwrap().clear();
    }
    fn max_group(&self) -> usize {
        self.group_sizes
            .lock()
            .unwrap()
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }
    fn group_sizes(&self) -> Vec<usize> {
        self.group_sizes.lock().unwrap().clone()
    }
}

impl ObjectStore for FaultStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }
    fn group_rejection_publishes_nothing(&self) -> bool {
        self.declares_group_atomicity.load(Ordering::SeqCst)
    }
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.block()?;
        }
        let mut fault = self.read_fault.lock().unwrap();
        if let Some((target, skip)) = fault.as_mut() {
            if target == key {
                if *skip == 0 {
                    *fault = None;
                    anyhow::bail!("injected read transport failure");
                }
                *skip -= 1;
            }
        }
        drop(fault);
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let etag = self.inner.put(key, bytes, condition)?;
        if key.ends_with("/authority/root") && self.lose_root_ack.swap(false, Ordering::SeqCst) {
            if self.fail_readback.load(Ordering::SeqCst) {
                self.fail(key.into(), 0);
            }
            anyhow::bail!("injected lost root ACK");
        }
        Ok(etag)
    }
    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        self.groups.fetch_add(1, Ordering::SeqCst);
        self.group_sizes.lock().unwrap().push(writes.len());
        if self.reject_boot.load(Ordering::SeqCst) {
            return Err(PreconditionFailed.into());
        }
        // Evaluate every condition against the pre-group state first, exactly as the
        // declared `group_rejection_publishes_nothing` promise requires: a rejected
        // group must leave the store untouched.
        for write in writes {
            let current = self.inner.get(&write.key)?;
            let matches = match &write.condition {
                PutCondition::Any => true,
                PutCondition::Absent => current.is_none(),
                PutCondition::Match(etag) => current.is_some_and(|object| object.etag == *etag),
            };
            ensure!(matches, PreconditionFailed);
        }
        let mut etags = Vec::with_capacity(writes.len());
        for write in writes {
            etags.push(
                self.inner
                    .put(&write.key, &write.bytes, write.condition.clone())?,
            );
        }
        if self.lose_boot_ack.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected lost boot ACK");
        }
        Ok(etags)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn namespace_init_t1_root_and_boot_snapshot_reads_retry_without_mutation() -> Result<()> {
    for skip in [0, 1] {
        let store = Arc::new(FaultStore::default());
        let id = identifier(&format!("read-{skip}"));
        Authority::open(store.clone(), id.namespace.clone())?;
        let writes = store.writes();
        store.fail(id.namespace.key("authority/root")?, skip);
        let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
        assert!(authority.state(&id).is_err());
        assert_eq!(store.writes(), writes);
        assert!(authority.quiescent());
        authority.state(&id)?;
        assert_eq!(store.writes(), writes + 1);
        assert!(authority.quiescent());
    }
    Ok(())
}

#[test]
fn namespace_init_t2_deep_verify_manifest_read_retries() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let id = identifier("deep-read");
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let branch = base.branch_store("main")?;
    let bytes = serde_json::to_vec(&peri_loom_v1::manifest::Manifest::default())?;
    let hash = peri_loom_v1::manifest::digest(&bytes);
    branch.put(
        &id.namespace.key(&format!("manifests/{hash}"))?,
        &bytes,
        PutCondition::Absent,
    )?;
    let root = base.snapshot()?.root;
    let blob = root
        .objects
        .get(&format!("manifests/{hash}"))?
        .unwrap()
        .blob;
    store.fail(id.namespace.key(&format!("authority/blobs/{blob}"))?, 0);
    let writes = store.writes();
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    let error = authority.state(&id).unwrap_err();
    assert!(error.to_string().contains("DeepVerify"));
    assert_eq!(store.writes(), writes);
    assert!(authority.quiescent());
    authority.state(&id)?;
    assert_eq!(store.writes(), writes + 1);
    Ok(())
}

#[test]
fn namespace_init_t3_boot_wal_read_retries() -> Result<()> {
    use peri_loom_v1::wal::WalMetadata;
    let store = Arc::new(FaultStore::default());
    let id = identifier("boot-wal-read");
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let branch = base.branch_store("main")?;
    branch.put(
        &id.namespace.key("wal/index")?,
        &serde_json::to_vec(&WalMetadata::default())?,
        PutCondition::Absent,
    )?;
    let root = base.snapshot()?.root;
    let wal = root.branches["main"].wal.as_ref().unwrap();
    // Deep verification reads this WAL once; the next read is boot's epoch scan.
    store.fail(
        id.namespace.key(&format!("authority/blobs/{}", wal.blob))?,
        1,
    );
    let writes = store.writes();
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    assert!(authority
        .state(&id)
        .unwrap_err()
        .to_string()
        .contains("BootRead"));
    assert_eq!(store.writes(), writes);
    assert!(authority.quiescent());
    authority.state(&id)?;
    assert_eq!(store.writes(), writes + 1);
    Ok(())
}

#[test]
fn namespace_init_t4_invalid_root_is_definite_and_not_retried() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let id = identifier("invalid-root");
    Authority::open(store.clone(), id.namespace.clone())?;
    let key = id.namespace.key("authority/root")?;
    store.inner.put(&key, b"{}", PutCondition::Any)?;
    let writes = store.writes();
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    assert!(authority.state(&id).is_err());
    let reads = store.reads.load(Ordering::SeqCst);
    assert!(authority.state(&id).is_err());
    assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    assert_eq!(store.writes(), writes);
    assert!(authority.quiescent());
    Ok(())
}

#[test]
fn namespace_init_t5_t6_t7_unknown_is_terminal_even_with_readback() -> Result<()> {
    for (existing, readback_failure) in [(true, false), (false, true), (false, false)] {
        let store = Arc::new(FaultStore::default());
        let id = identifier("unknown-init");
        if existing {
            Authority::open(store.clone(), id.namespace.clone())?;
            store.lose_boot_ack.store(true, Ordering::SeqCst);
        } else {
            store.lose_root_ack.store(true, Ordering::SeqCst);
            store
                .fail_readback
                .store(readback_failure, Ordering::SeqCst);
        }
        let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
        assert!(authority.state(&id).is_err());
        let writes = store.writes();
        let reads = store.reads.load(Ordering::SeqCst);
        assert!(authority.state(&id).is_err());
        assert!(grant_request(&authority, id, 1, 0, 1000, "blocked").is_err());
        assert_eq!(store.writes(), writes);
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
        assert!(!authority.quiescent());
    }
    Ok(())
}

#[test]
fn namespace_init_rejected_boot_restarts_from_fresh_root() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let id = identifier("rejected");
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let before = base.snapshot()?.etag;
    store.reject_boot.store(true, Ordering::SeqCst);
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    assert!(authority.state(&id).is_err());
    assert_eq!(store.groups.load(Ordering::SeqCst), 128);
    assert_eq!(base.snapshot()?.etag, before);
    assert!(authority.quiescent());
    store.reject_boot.store(false, Ordering::SeqCst);
    authority.state(&id)?;
    assert_eq!(store.groups.load(Ordering::SeqCst), 129);
    Ok(())
}

/// A namespace whose boot publication cannot fit in a single write: the root still
/// carries inline receipts, so boot rewrites them into a paged index and the head CAS
/// joins that page in the same durability group. Rejecting such a group is the only
/// way to reach the atomicity question this fixture exists for.
fn seed_multi_write_boot(store: &Arc<FaultStore>) -> Result<DatabaseId> {
    let id = identifier("multi-write-boot");
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let branch = base.branch_store("main")?;
    let bytes = serde_json::to_vec(&peri_loom_v1::manifest::Manifest::default())?;
    let hash = peri_loom_v1::manifest::digest(&bytes);
    branch.put(
        &id.namespace.key(&format!("manifests/{hash}"))?,
        &bytes,
        PutCondition::Absent,
    )?;
    Ok(id)
}

#[test]
fn namespace_init_declared_atomic_backend_retries_rejected_multi_write_boot_group() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    store.declares_group_atomicity.store(true, Ordering::SeqCst);
    let id = seed_multi_write_boot(&store)?;
    store.reset_groups();
    store.reject_boot.store(true, Ordering::SeqCst);
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    assert!(authority.state(&id).is_err());
    assert!(
        store.max_group() > 1,
        "boot group was not multi-write; the atomicity branch is untested"
    );
    assert!(authority.quiescent());
    assert!(store.groups.load(Ordering::SeqCst) > 0);
    store.reject_boot.store(false, Ordering::SeqCst);
    authority.state(&id)?;
    assert!(authority.quiescent());
    assert_eq!(
        store.group_sizes().last(),
        Some(&store.max_group()),
        "the successful attempt republished the same multi-write group"
    );
    Ok(())
}

#[test]
fn namespace_init_undeclared_backend_keeps_rejected_multi_write_boot_group_blocked() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let id = seed_multi_write_boot(&store)?;
    store.reset_groups();
    store.reject_boot.store(true, Ordering::SeqCst);
    let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
    assert!(authority.state(&id).is_err());
    assert!(
        store.max_group() > 1,
        "boot group was not multi-write; the conservative branch is untested"
    );
    assert!(!authority.quiescent());
    let writes = store.writes();
    assert!(authority.state(&id).is_err());
    assert!(grant_request(&authority, id.clone(), 1, 0, 1000, "blocked").is_err());
    assert_eq!(store.writes(), writes);
    assert!(!authority.quiescent());
    store.reject_boot.store(false, Ordering::SeqCst);
    assert!(
        authority.state(&id).is_err(),
        "an undeclared backend must not be retried after a partly published group"
    );
    Ok(())
}

#[test]
fn namespace_init_t8_t9_concurrent_admission_is_single_flight() -> Result<()> {
    for retry in [false, true] {
        let store = Arc::new(FaultStore::default());
        let id = identifier("concurrent");
        let authority = OwnershipAuthority::new(store.clone(), Clock::new(1000));
        if retry {
            store.fail(id.namespace.key("authority/root")?, 0);
            assert!(authority.state(&id).is_err());
            assert_eq!(store.writes(), 0);
        }
        let gate = Arc::new(Gate::default());
        *store.gate.lock().unwrap() = Some(gate.clone());
        let mut threads = Vec::new();
        for _ in 0..8 {
            let authority = authority.clone();
            let id = id.clone();
            threads.push(std::thread::spawn(move || authority.state(&id)));
        }
        gate.entered()?;
        assert!(!authority.quiescent());
        gate.release();
        let mut states = Vec::new();
        for handle in threads {
            states.push(serde_json::to_value(handle.join().unwrap()?)?);
        }
        assert!(states.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(store.puts.load(Ordering::SeqCst), 1);
        assert_eq!(store.groups.load(Ordering::SeqCst), 1);
        assert!(authority.quiescent());
    }
    Ok(())
}

struct FailingClock(AtomicBool);
impl LeaseClock for FailingClock {
    fn now_ms(&self) -> Result<u64> {
        ensure!(!self.0.load(Ordering::SeqCst), "injected clock failure");
        Ok(1000)
    }
}

#[test]
fn namespace_init_t13_committed_boot_clock_failure_is_definite() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let id = identifier("clock-failure");
    let base = Authority::open(store.clone(), id.namespace.clone())?;
    let clock = Arc::new(FailingClock(AtomicBool::new(true)));
    let authority = OwnershipAuthority::new(store.clone(), clock.clone());
    assert!(authority
        .state(&id)
        .unwrap_err()
        .to_string()
        .contains("ClockAnchor"));
    assert!(authority.quiescent());
    clock.0.store(false, Ordering::SeqCst);
    assert!(authority.state(&id).is_err());
    assert_eq!(store.groups.load(Ordering::SeqCst), 1);
    assert_eq!(base.snapshot()?.root.ownership.unwrap().authority_term, 1);
    let reopened = OwnershipAuthority::new(store, clock);
    reopened.state(&id)?;
    let catalog = base.snapshot()?.root.ownership.unwrap();
    assert_eq!(catalog.authority_term, 2);
    assert!(catalog
        .branches
        .values()
        .all(|branch| branch.state == peri_loom_v1::ownership::types::GrantState::Revoked));
    Ok(())
}

#[test]
fn namespace_init_t12_unknown_namespace_does_not_block_healthy_leases() -> Result<()> {
    let store = Arc::new(FaultStore::default());
    let a = identifier("unknown-a");
    let b = identifier("healthy-b");
    Authority::open(store.clone(), a.namespace.clone())?;
    store.lose_boot_ack.store(true, Ordering::SeqCst);
    let authority = OwnershipAuthority::new(store, Clock::new(1000));
    assert!(authority.state(&a).is_err());
    let granted = authority.grant(grant_request(&authority, b, 1, 0, 1000, "healthy")?)?;
    let intent = RenewIntent {
        binding: granted.binding,
        sequence: 1,
        ttl_ms: 1000,
    };
    let nonce = authority.challenge(LeaseIntent::Renew(intent.clone()))?;
    authority.renew(RenewRequest {
        request_id: "renew-healthy".into(),
        nonce: nonce.nonce,
        intent,
    })?;
    assert!(!authority.quiescent());
    Ok(())
}

#[path = "namespace_init_rpc.rs"]
mod rpc;
