//! B05: scale of the inline identity tables and root CAS contention.
//!
//! The branch/snapshot/pin identity tables are still inline. These tests measure the head
//! the CAS carries for 1/8/32/128 branches and for growing snapshot/pin populations, prove
//! that a paged registry (logical objects) no longer grows the head, and quantify concurrent
//! branch publications. The ceilings themselves are asserted separately: a head above them
//! is rejected on decode, not written.

use super::*;
use crate::capabilities::Capabilities;
use crate::gc::GarbageCollector;
use crate::namespace::Namespace;
use crate::object_store::{
    MemoryObjectStore, ObjectStore, PreconditionFailed, PutCondition, StoredObject,
};
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

#[derive(Default)]
struct CountingStore {
    inner: MemoryObjectStore,
    root_attempts: AtomicUsize,
    root_writes: AtomicUsize,
    root_conflicts: AtomicUsize,
    root_bytes: AtomicUsize,
    /// Armed by the concurrency test so every writer's first CAS is evaluated against the
    /// same head. That makes the conflict count deterministic instead of load-dependent.
    barrier: Mutex<Option<Arc<Barrier>>>,
    barrier_slots: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Default)]
struct RootCounters {
    attempts: usize,
    writes: usize,
    conflicts: usize,
    bytes: usize,
}

impl CountingStore {
    fn arm_barrier(&self, threads: usize) {
        *self.barrier.lock().unwrap() = Some(Arc::new(Barrier::new(threads)));
        self.barrier_slots.store(threads, Ordering::SeqCst);
    }

    fn reset(&self) {
        self.root_attempts.store(0, Ordering::SeqCst);
        self.root_writes.store(0, Ordering::SeqCst);
        self.root_conflicts.store(0, Ordering::SeqCst);
        self.root_bytes.store(0, Ordering::SeqCst);
    }

    fn counters(&self) -> RootCounters {
        RootCounters {
            attempts: self.root_attempts.load(Ordering::SeqCst),
            writes: self.root_writes.load(Ordering::SeqCst),
            conflicts: self.root_conflicts.load(Ordering::SeqCst),
            bytes: self.root_bytes.load(Ordering::SeqCst),
        }
    }
}

impl ObjectStore for CountingStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        if key.ends_with("/authority/root") {
            self.root_attempts.fetch_add(1, Ordering::SeqCst);
            let engage = self
                .barrier_slots
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |slots| {
                    slots.checked_sub(1)
                })
                .is_ok();
            if engage {
                let barrier = self.barrier.lock().unwrap().clone();
                if let Some(barrier) = barrier {
                    barrier.wait();
                }
            }
        }
        let result = self.inner.put(key, bytes, condition);
        if key.ends_with("/authority/root") {
            match &result {
                Ok(_) => {
                    self.root_writes.fetch_add(1, Ordering::SeqCst);
                    self.root_bytes.store(bytes.len(), Ordering::SeqCst);
                }
                Err(error) if error.is::<PreconditionFailed>() => {
                    self.root_conflicts.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {}
            }
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

fn open(database: &str) -> Result<(Arc<CountingStore>, Arc<Authority>)> {
    let store = Arc::new(CountingStore::default());
    let namespace = Namespace::new("b05", database)?;
    let authority = Authority::open(store.clone(), namespace)?;
    Ok((store, authority))
}

fn create_branches(capabilities: &Capabilities, total: usize) -> Result<()> {
    for index in 1..total {
        capabilities.branch("main", &format!("b{index}"))?;
    }
    Ok(())
}

fn create_snapshots(capabilities: &Capabilities, total: usize) -> Result<()> {
    for index in 0..total {
        capabilities.snapshot("main", &format!("s{index}"))?;
    }
    Ok(())
}

fn create_pin_identities(authority: &Arc<Authority>, total: usize) -> Result<()> {
    for index in 0..total {
        // Reader pin identities are content hashes; a padded counter is one.
        let pin = format!("{:064x}", index + 1);
        authority.transact(None, |root| {
            root.pins.insert(pin.clone(), Vec::new());
            root.pin_generations.insert(pin.clone(), 0);
            Ok(())
        })?;
    }
    Ok(())
}

/// One statement-sized mutation: exactly one branch counter moves. The clone, change-scoped
/// validation, encode and CAS are the production publication path.
fn single_write(authority: &Arc<Authority>) -> Result<()> {
    authority.transact(None, |root| {
        let branch = root.branches.get_mut("main").context("branch missing")?;
        branch.sequence += 1;
        branch.current_sequence = branch.sequence;
        branch.last_commit_ms += 1;
        branch.current_commit_ms = branch.last_commit_ms;
        Ok(())
    })?;
    Ok(())
}

fn measure_write(store: &CountingStore, authority: &Arc<Authority>) -> Result<RootCounters> {
    store.reset();
    single_write(authority)?;
    Ok(store.counters())
}

#[test]
fn identity_matrix_reports_root_bytes_and_cas_per_single_write() -> Result<()> {
    // One case per branch tier, and a second pass with snapshot/pin identities present so
    // their effect on the same single write is visible next to the branch growth.
    let cases: &[(usize, usize, usize)] = &[
        (1, 0, 0),
        (8, 0, 0),
        (32, 0, 0),
        (128, 0, 0),
        (1, 8, 8),
        (8, 8, 8),
        (32, 8, 8),
        (128, 8, 8),
        (8, 32, 32),
    ];
    let mut measured = Vec::new();
    for (branches, snapshots, pins) in cases {
        let (store, authority) = open(&format!("matrix-{branches}-{snapshots}-{pins}"))?;
        let capabilities = Capabilities::new(authority.clone());
        create_branches(&capabilities, *branches)?;
        create_snapshots(&capabilities, *snapshots)?;
        create_pin_identities(&authority, *pins)?;

        let counters = measure_write(&store, &authority)?;
        let root = authority.snapshot()?.root;
        assert_eq!(
            root.branches.len(),
            *branches,
            "branch population changed outside the setup"
        );
        assert_eq!(
            counters.attempts, 1,
            "a single write retried without contention"
        );
        assert_eq!(counters.writes, 1);
        assert_eq!(counters.conflicts, 0);
        assert!(
            counters.bytes > 0 && counters.bytes <= super::types::MAX_ROOT_BYTES,
            "root bytes {} outside the publication ceiling",
            counters.bytes
        );
        println!(
            "B05 matrix branches={branches} snapshots={snapshots} pins={pins} \
             root_bytes={} cas_attempts={} cas_conflicts={}",
            counters.bytes, counters.attempts, counters.conflicts
        );
        measured.push((*branches, *snapshots, *pins, counters.bytes));
    }

    let smallest = measured
        .iter()
        .find(|(branches, snapshots, pins, _)| (*branches, *snapshots, *pins) == (1, 0, 0))
        .map(|(_, _, _, bytes)| *bytes)
        .context("1-branch baseline missing")?;
    let largest = measured
        .iter()
        .find(|(branches, snapshots, pins, _)| (*branches, *snapshots, *pins) == (128, 8, 8))
        .map(|(_, _, _, bytes)| *bytes)
        .context("128-branch case missing")?;
    // Sanity bound: the 128-branch head stays in the same order of magnitude, i.e. it is
    // still a head document and not a monolithic catalog. The exact growth is printed above.
    assert!(
        largest <= smallest + 128 * 1024,
        "128-branch root grew by {} bytes",
        largest.saturating_sub(smallest)
    );
    Ok(())
}

#[test]
fn paged_object_aliases_do_not_grow_the_head() -> Result<()> {
    let (store, authority) = open("paged-flat")?;
    let blob = authority.upload(&vec![7u8; 4096])?;
    let baseline = measure_write(&store, &authority)?.bytes;

    authority.transact(None, |root| {
        for index in 0..512 {
            root.objects
                .insert(format!("alias/{index:04}"), blob.clone())?;
        }
        Ok(())
    })?;
    let after = measure_write(&store, &authority)?.bytes;
    let root = authority.snapshot()?.root;
    assert!(
        root.objects.pointer().is_some(),
        "512 logical aliases did not spill into pages"
    );
    println!(
        "B05 paged-set baseline_bytes={baseline} after_512_aliases={after} delta={}",
        after.saturating_sub(baseline)
    );
    // Receipt archiving moves a bounded window in and out of the head, so allow that much
    // noise; 512 paged entries themselves must not appear in the document.
    assert!(
        after <= baseline + 16 * 1024,
        "paged aliases grew the head by {} bytes",
        after.saturating_sub(baseline)
    );
    Ok(())
}

#[test]
fn concurrent_branch_writes_lose_no_commits_or_snapshots() -> Result<()> {
    const THREADS: usize = 8;
    const WRITES: usize = 8;
    let (store, authority) = open("concurrent")?;
    let capabilities = Capabilities::new(authority.clone());
    for index in 0..THREADS {
        capabilities.branch("main", &format!("w{index}"))?;
    }

    store.reset();
    store.arm_barrier(THREADS);
    let mut writers = Vec::new();
    for index in 0..THREADS {
        let authority = authority.clone();
        writers.push(std::thread::spawn(move || -> Result<()> {
            let name = format!("w{index}");
            for _ in 0..WRITES {
                authority.transact(None, |root| {
                    let branch = root.branches.get_mut(&name).context("branch missing")?;
                    branch.sequence += 1;
                    branch.current_sequence = branch.sequence;
                    branch.last_commit_ms += 1;
                    branch.current_commit_ms = branch.last_commit_ms;
                    Ok(())
                })?;
            }
            Ok(())
        }));
    }
    for writer in writers {
        writer.join().expect("writer panicked")?;
    }

    let counters = store.counters();
    println!(
        "B05 concurrency threads={THREADS} writes={} root_attempts={} root_writes={} \
         root_conflicts={} max_root_bytes={}",
        THREADS * WRITES,
        counters.attempts,
        counters.writes,
        counters.conflicts,
        counters.bytes
    );
    assert_eq!(counters.writes, THREADS * WRITES, "a commit lost its CAS");
    assert!(
        counters.conflicts >= THREADS - 1,
        "the armed barrier did not force the first-round collisions: {}",
        counters.conflicts
    );
    assert_eq!(
        counters.attempts,
        counters.writes + counters.conflicts,
        "a root write failed for a reason other than a lost CAS"
    );

    let root = authority.snapshot()?.root;
    for index in 0..THREADS {
        let name = format!("w{index}");
        let branch = root.branches.get(&name).context("branch missing")?;
        assert_eq!(branch.sequence, WRITES as u64, "lost commit on {name}");
        assert_eq!(branch.current_sequence, WRITES as u64);
    }
    // Every snapshot must describe the branch state at creation time; none may carry a
    // sequence or head from another publication.
    for index in 0..THREADS {
        let name = format!("w{index}");
        let point = capabilities.snapshot(&name, &format!("snap{index}"))?;
        let branch = authority.snapshot()?.root.branches[&name].clone();
        assert_eq!(point.sequence, branch.current_sequence);
        assert_eq!(point.timeline, branch.timeline);
        assert_eq!(point.head, branch.head);
        assert_eq!(point.branch, name);
    }
    authority.reachable(&authority.snapshot()?.root)?;
    Ok(())
}

#[test]
fn pinned_blob_survives_gc_and_identity_validation() -> Result<()> {
    let (_store, authority) = open("gc-reach")?;
    let session = authority.branch_store("main")?;
    let entry = authority.upload(&vec![9u8; 4096])?;
    let pin = session.pin_identity().to_owned();
    let key = super::types::pin_entry_key(&pin, 0, &entry.blob);
    authority.transact(None, |root| {
        root.pin_entries.insert(key.clone(), entry.clone())?;
        Ok(())
    })?;

    let root = authority.snapshot()?.root;
    assert!(root.pins.contains_key(&pin));
    assert!(authority.reachable(&root)?.contains(&entry.blob));
    let collector = GarbageCollector::new(authority.clone(), Duration::ZERO);
    assert_eq!(collector.inspect()?.reader_pins, 1);
    collector.collect()?;

    let root = authority.snapshot()?.root;
    assert!(
        root.blobs.get(&entry.blob)?.is_some(),
        "GC removed a blob protected by a reader pin"
    );
    assert!(root.pin_entries.get(&key)?.is_some());
    assert!(root.pins.contains_key(&pin));
    assert!(authority.reachable(&root)?.contains(&entry.blob));
    Ok(())
}

#[test]
fn identity_ceilings_and_unsupported_versions_fail_closed() -> Result<()> {
    let (store, authority) = open("ceiling")?;
    let namespace = authority.namespace().clone();
    let root_key = namespace.key("authority/root")?;
    // Snapshot the valid head before the store is overwritten with crafted documents.
    let original = authority.snapshot()?.root;

    let mut root = original.clone();
    for index in 0..super::types::MAX_INLINE_BRANCHES - 1 {
        root.branches
            .insert(format!("c{index}"), BranchRef::default());
    }
    assert_eq!(root.branches.len(), super::types::MAX_INLINE_BRANCHES);
    root.validate_shape()?;
    root.branches
        .insert("overflow".into(), BranchRef::default());
    let error = root.validate_shape().unwrap_err();
    assert!(
        format!("{error:#}").contains("branch identity ceiling"),
        "unexpected ceiling error: {error:#}"
    );

    let mut snapshots = original.clone();
    for index in 0..super::types::MAX_INLINE_SNAPSHOTS + 1 {
        snapshots.snapshots.insert(
            format!("z{index}"),
            RecoveryPoint {
                id: format!("z{index}"),
                branch: "main".into(),
                timeline: 1,
                sequence: 0,
                timestamp_ms: 0,
                head: None,
                wal: None,
            },
        );
    }
    let error = snapshots.validate_shape().unwrap_err();
    assert!(
        format!("{error:#}").contains("snapshot identity ceiling"),
        "unexpected snapshot ceiling error: {error:#}"
    );

    let mut pins = original.clone();
    for index in 0..super::types::MAX_INLINE_PIN_IDENTITIES + 1 {
        pins.pins.insert(format!("{index:064x}"), Vec::new());
    }
    let error = pins.validate_shape().unwrap_err();
    assert!(
        format!("{error:#}").contains("reader pin identity ceiling"),
        "unexpected pin ceiling error: {error:#}"
    );

    // The same head must be rejected on the decode path instead of becoming current.
    store.put(&root_key, &super::codec::encode(&root)?, PutCondition::Any)?;
    let error = Authority::open(store.clone(), namespace.clone())
        .err()
        .context("decode accepted an over-ceiling head")?;
    assert!(
        format!("{error:#}").contains("branch identity ceiling"),
        "decode accepted an over-ceiling head: {error:#}"
    );

    // A version this binary does not understand is rejected rather than guessed at.
    let mut unsupported = original;
    unsupported.version = 3;
    store.put(
        &root_key,
        &super::codec::encode(&unsupported)?,
        PutCondition::Any,
    )?;
    let error = Authority::open(store, namespace)
        .err()
        .context("decode accepted an unsupported version")?;
    assert!(
        format!("{error:#}").contains("unsupported authority root version"),
        "decode accepted an unsupported version: {error:#}"
    );
    Ok(())
}
