use super::*;
use crate::{
    authority::{admitted, ownership::OwnershipAuthority, RecoveryPoint},
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, StoredObject},
    ownership::types::LeaseClock,
    runtime::DatabaseId,
    wal::WalMetadata,
};
use anyhow::bail;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Default)]
struct Probe {
    inner: MemoryObjectStore,
    history_key: Mutex<Option<String>>,
    history_reads: AtomicUsize,
    armed: AtomicBool,
    /// One root read to answer with an older document, so a test can reproduce a read that
    /// began before a publication landed and decoded after it.
    stale: Mutex<Option<(String, StoredObject)>>,
    // 1 = reject one CAS, 2 = unknown before persistence, 3 = unknown after it.
    fault: AtomicU8,
    attempts: Mutex<Vec<(String, String)>>,
}

impl ObjectStore for Probe {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if self.history_key.lock().unwrap().as_deref() == Some(key) {
            self.history_reads.fetch_add(1, Ordering::SeqCst);
        }
        {
            let mut stale = self.stale.lock().unwrap();
            if stale
                .as_ref()
                .is_some_and(|(stale_key, _)| stale_key == key)
            {
                return Ok(stale.take().map(|(_, object)| object));
            }
        }
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let fault = if key.ends_with("/authority/root") && self.armed.load(Ordering::SeqCst) {
            let (root, digest) = codec::decode_with_digest(bytes)?;
            let mut attempts = self.attempts.lock().unwrap();
            for (lineage, attempted) in attempts.iter() {
                assert!(
                    !admitted::holds(key, lineage, attempted),
                    "an unacknowledged candidate entered admission before the retry"
                );
            }
            assert!(!admitted::holds(key, &root.incarnation, &digest));
            attempts.push((root.incarnation.clone(), digest));
            self.fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 1 {
            return Err(PreconditionFailed.into());
        }
        if fault == 2 {
            bail!("injected unknown boot before persistence");
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if fault == 3 {
            bail!("injected unknown boot after persistence");
        }
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

struct Clock;
impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1_000)
    }
}

fn fixture() -> Result<(Arc<Probe>, Arc<Authority>, DatabaseId)> {
    let store = Arc::new(Probe::default());
    let namespace = Namespace::new("boot-admission", unique_id()?)?;
    let authority = Authority::open(store.clone(), namespace.clone())?;
    // Only history reaches this index. A cheap current-reference validation never reads
    // it, so its physical GET count distinguishes deep verification from root decoding.
    let history = authority.upload(&serde_json::to_vec(&WalMetadata {
        epoch: 7,
        ..WalMetadata::default()
    })?)?;
    authority.transact(None, |root| {
        let branch = root.branches.get_mut("main").unwrap();
        branch.sequence = 1;
        branch.last_commit_ms = 1;
        branch.history.push(RecoveryPoint {
            id: unique_id()?,
            branch: "main".into(),
            timeline: branch.timeline,
            sequence: 1,
            timestamp_ms: 1,
            head: None,
            wal: Some(history.clone()),
        });
        Ok(())
    })?;
    *store.history_key.lock().unwrap() = Some(authority.physical_key(&history.blob)?);
    Ok((store, authority, DatabaseId::new(namespace, "main")?))
}

#[test]
fn first_state_after_boot_reuses_verified_references_and_retains_the_epoch_floor() -> Result<()> {
    let (store, authority, database) = fixture()?;
    drop(authority);
    let owner = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    assert_eq!(owner.state(&database)?.epoch, 7);
    assert_eq!(
        store.history_reads.load(Ordering::SeqCst),
        2,
        "restart should read history once in deep verification and once for the boot floor"
    );
    assert_eq!(owner.state(&database)?.epoch, 7);
    assert_eq!(store.history_reads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test]
fn an_external_root_is_deep_verified_and_invalid_history_still_fails_closed() -> Result<()> {
    let (store, authority, database) = fixture()?;
    drop(authority);
    let owner = OwnershipAuthority::new(store.clone(), Arc::new(Clock));
    owner.state(&database)?;
    let key = database.namespace.key("authority/root")?;
    let stored = store.get(&key)?.unwrap();
    let mut external = (*codec::decode(&stored.bytes)?).clone();
    external.revision += 1;
    store.put(
        &key,
        &codec::encode(&external)?,
        PutCondition::Match(stored.etag),
    )?;
    let before = store.history_reads.load(Ordering::SeqCst);
    assert_eq!(owner.state(&database)?.epoch, 7);
    assert_eq!(store.history_reads.load(Ordering::SeqCst), before + 1);

    let stored = store.get(&key)?.unwrap();
    external.revision += 1;
    external.branches.get_mut("main").unwrap().history[0]
        .wal
        .as_mut()
        .unwrap()
        .hash = blake3::hash(b"unregistered history content")
        .to_hex()
        .to_string();
    let (encoded, digest) = codec::encode_with_digest(&external)?;
    store.put(&key, &encoded, PutCondition::Match(stored.etag))?;
    assert!(owner.state(&database).is_err());
    assert!(!admitted::holds(&key, &external.incarnation, &digest));
    assert!(Authority::open(store, database.namespace).is_err());
    Ok(())
}

#[test]
fn a_stale_head_read_does_not_evict_the_newer_admission() -> Result<()> {
    let (store, authority, _database) = fixture()?;
    let key = authority.namespace().key("authority/root")?;
    let before = store.history_reads.load(Ordering::SeqCst);
    let previous = store.get(&key)?.unwrap();
    let (old_root, old_digest) = codec::decode_with_digest(&previous.bytes)?;
    assert!(admitted::holds(&key, &old_root.incarnation, &old_digest));

    authority.transact(None, |root| {
        root.branches.get_mut("main").unwrap().last_commit_ms = 2;
        Ok(())
    })?;
    let current = store.get(&key)?.unwrap();
    let (new_root, new_digest) = codec::decode_with_digest(&current.bytes)?;
    assert!(admitted::holds(&key, &new_root.incarnation, &new_digest));
    assert!(
        admitted::holds(&key, &old_root.incarnation, &old_digest),
        "a publication must not erase the admission of the head it replaces"
    );

    // The read that raced the publication decodes the older document after the publisher
    // recorded the newer one. It must stay on the cheap path, and the following read of
    // the new head must not be forced to walk the registry either.
    *store.stale.lock().unwrap() = Some((key.clone(), previous));
    authority.snapshot()?;
    authority.snapshot()?;
    assert_eq!(
        store.history_reads.load(Ordering::SeqCst),
        before,
        "a head this process verified must not trigger a registry walk after the head moves"
    );
    Ok(())
}

#[test]
fn a_head_this_process_never_verified_is_walked_once_and_then_admitted() -> Result<()> {
    let (store, authority, _database) = fixture()?;
    let key = authority.namespace().key("authority/root")?;
    let stored = store.get(&key)?.unwrap();
    let mut external = (*codec::decode(&stored.bytes)?).clone();
    external.revision += 1;
    store.put(
        &key,
        &codec::encode(&external)?,
        PutCondition::Match(stored.etag),
    )?;
    let before = store.history_reads.load(Ordering::SeqCst);
    authority.snapshot()?;
    assert_eq!(
        store.history_reads.load(Ordering::SeqCst),
        before + 1,
        "an unseen head must be resolved in full exactly once"
    );
    authority.snapshot()?;
    assert_eq!(
        store.history_reads.load(Ordering::SeqCst),
        before + 1,
        "the verified head must stay admitted"
    );
    Ok(())
}

#[derive(Default)]
struct GroupProbe {
    inner: MemoryObjectStore,
    heads_written: AtomicUsize,
}

impl ObjectStore for GroupProbe {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }

    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        for write in writes {
            if write.key.ends_with("/authority/root") {
                let (root, digest) = codec::decode_with_digest(&write.bytes)?;
                assert!(
                    admitted::holds(&write.key, &root.incarnation, &digest),
                    "a head became visible before this process admitted it"
                );
                self.heads_written.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.inner.put_group(writes)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn a_head_is_admitted_before_its_group_becomes_visible() -> Result<()> {
    let store = Arc::new(GroupProbe::default());
    let authority = Authority::open(
        store.clone(),
        Namespace::new("group-admission", unique_id()?)?,
    )?;
    authority.transact(None, |root| {
        root.branches.get_mut("main").unwrap().last_commit_ms = 2;
        Ok(())
    })?;
    assert_eq!(
        store.heads_written.load(Ordering::SeqCst),
        1,
        "the publication group did not carry the head"
    );
    Ok(())
}

#[test]
fn a_rejected_boot_candidate_is_not_admitted_before_its_successful_retry() -> Result<()> {
    let (store, authority, database) = fixture()?;
    store.fault.store(1, Ordering::SeqCst);
    store.armed.store(true, Ordering::SeqCst);
    let booted = authority
        .boot_ownership()
        .map_err(|failure| match failure {
            BootFailure::Read(error) | BootFailure::Unsettled(error) => error,
        })?;
    let key = database.namespace.key("authority/root")?;
    let attempts = store.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    let (lineage, digest) = attempts.last().unwrap();
    assert_eq!(*lineage, booted.root.incarnation);
    assert!(admitted::holds(&key, lineage, digest));
    Ok(())
}

#[test]
fn an_unknown_boot_never_admits_its_candidate_even_when_the_bytes_landed() -> Result<()> {
    for fault in [2, 3] {
        let (store, authority, database) = fixture()?;
        let key = database.namespace.key("authority/root")?;
        let previous = store.get(&key)?.unwrap();
        let (old_root, old_digest) = codec::decode_with_digest(&previous.bytes)?;
        store.fault.store(fault, Ordering::SeqCst);
        store.armed.store(true, Ordering::SeqCst);
        assert!(matches!(
            authority.boot_ownership(),
            Err(BootFailure::Unsettled(_))
        ));
        let attempts = store.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 1);
        let (lineage, digest) = &attempts[0];
        assert!(!admitted::holds(&key, lineage, digest));
        assert!(admitted::holds(&key, &old_root.incarnation, &old_digest));
        let current = store.get(&key)?.unwrap();
        assert_eq!(current.bytes == previous.bytes, fault == 2);
    }
    Ok(())
}
