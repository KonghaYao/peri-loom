//! Local version-6 root: bounded receipts and the one-way migration.
//!
//! The migration is a normal publication with one extra allowance: it may move the durable
//! version, drop the (empty) ownership catalog and page the receipt ledger. Every test here
//! exercises that publication through the public opening path so the invariants are observed
//! where a real local worker observes them.

use super::*;
use crate::{
    namespace::Namespace,
    object_store::{
        GroupWrite, LocalObjectStore, MemoryObjectStore, ObjectStore, PutCondition, StoredObject,
    },
    ownership::types::{AccessRole, GrantState, OwnerRecord, OwnershipCatalog},
};
use anyhow::bail;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Barrier, Mutex,
};

const ROOT_SUFFIX: &str = "/authority/root";

#[derive(Default)]
struct ProbeStore {
    inner: MemoryObjectStore,
    /// Versions of every head that actually landed, in write order.
    root_versions: Mutex<Vec<u32>>,
    /// Encoded sizes of every head that actually landed.
    root_sizes: Mutex<Vec<usize>>,
    /// Write counts of groups that carried a head.
    group_sizes: Mutex<Vec<usize>>,
    /// 0 = normal, 1 = lose the group acknowledgement after persistence,
    /// 2 = persist every body but fail before the head.
    fault: AtomicU8,
}

impl ProbeStore {
    fn record_root(&self, bytes: &[u8]) {
        let envelope: serde_json::Value =
            serde_json::from_slice(bytes).expect("head envelope must be JSON");
        let version = envelope["root"]["version"]
            .as_u64()
            .expect("head version missing") as u32;
        self.root_versions.lock().unwrap().push(version);
        self.root_sizes.lock().unwrap().push(bytes.len());
    }

    fn record_group(&self, writes: &[GroupWrite]) {
        if writes.iter().any(|write| write.key.ends_with(ROOT_SUFFIX)) {
            self.group_sizes.lock().unwrap().push(writes.len());
        }
        if let Some(write) = writes.iter().find(|write| write.key.ends_with(ROOT_SUFFIX)) {
            self.record_root(&write.bytes);
        }
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
        let etag = self.inner.put(key, bytes, condition)?;
        if key.ends_with(ROOT_SUFFIX) {
            self.record_root(bytes);
        }
        Ok(etag)
    }

    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        match self.fault.swap(0, Ordering::SeqCst) {
            1 => {
                self.inner.put_group(writes)?;
                self.record_group(writes);
                bail!("injected lost group acknowledgement after persistence")
            }
            2 => {
                for write in writes {
                    if !write.key.ends_with(ROOT_SUFFIX) {
                        self.inner
                            .put(&write.key, &write.bytes, write.condition.clone())?;
                    }
                }
                bail!("injected crash before head publication")
            }
            _ => {
                let etags = self.inner.put_group(writes)?;
                self.record_group(writes);
                Ok(etags)
            }
        }
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

fn namespace(name: &str) -> Namespace {
    Namespace::new("local-v6", name).expect("namespace")
}

/// Publish a version-1 head that carries `receipts` inline receipts. Real appends are far
/// too slow for the 8 MiB boundary, and the migration only cares about the representation,
/// so the fixture writes the exact durable shape a long-lived v1 worker would have produced.
fn seed_v1_root<S>(
    store: &Arc<S>,
    namespace: &Namespace,
    receipts: usize,
    pin_schema: Option<u8>,
) -> Result<u64>
where
    S: ObjectStore + 'static,
{
    let authority = Authority::open(store.clone() as Arc<dyn ObjectStore>, namespace.clone())?;
    let current = authority.snapshot()?;
    let mut root = current.root.clone();
    root.pin_schema = pin_schema;
    root.receipts.clear();
    for index in 0..receipts {
        root.receipts
            .insert(format!("{index:064x}"), index as u64 + 1);
    }
    root.revision = receipts as u64 + 1;
    store.put(
        &namespace.key("authority/root")?,
        &codec::encode(&root)?,
        PutCondition::Match(current.etag),
    )?;
    Ok(root.revision)
}

fn stored_root(store: &ProbeStore, namespace: &Namespace) -> Result<CatalogRoot> {
    let object = store
        .get(&namespace.key("authority/root")?)?
        .context("authority root missing")?;
    Ok((*codec::decode(&object.bytes)?).clone())
}

fn stored_root_bytes(store: &dyn ObjectStore, namespace: &Namespace) -> Result<Vec<u8>> {
    Ok(store
        .get(&namespace.key("authority/root")?)?
        .context("authority root missing")?
        .bytes)
}

fn bare_owner_record() -> OwnerRecord {
    OwnerRecord {
        epoch: 1,
        branch_incarnation: 1,
        worker_id: "unowned".into(),
        lease_id: "unowned".into(),
        grant_id: "unowned".into(),
        renewal_sequence: 0,
        state: GrantState::Revoked,
        role: AccessRole::Writer,
    }
}

/// Rewrite the fresh version-1 head into a platform-like version-2/4/5 head. The migration
/// only has to observe the refusal, so the fixture writes exactly the durable shape that
/// gets past decode and deep verification.
fn seed_platform_root(
    store: &Arc<ProbeStore>,
    namespace: &Namespace,
    version: u32,
    catalog: OwnershipCatalog,
    mutate: impl FnOnce(&mut CatalogRoot),
) -> Result<()> {
    let authority = Authority::open(store.clone(), namespace.clone())?;
    let current = authority.snapshot()?;
    let mut root = current.root.clone();
    root.version = version;
    root.ownership = Some(catalog);
    mutate(&mut root);
    store.put(
        &namespace.key("authority/root")?,
        &codec::encode(&root)?,
        PutCondition::Match(current.etag),
    )?;
    Ok(())
}

fn empty_ownership() -> OwnershipCatalog {
    OwnershipCatalog {
        authority_term: 1,
        ..OwnershipCatalog::default()
    }
}

#[test]
fn v1_receipts_are_paged_and_read_back_after_restart() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("receipts");
    let revision = seed_v1_root(&store, &namespace, 1_000, Some(2))?;
    let archived = format!("{:064x}", 7);

    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    let root = authority.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert!(root.ownership.is_none());
    assert!(root.receipts.len() <= INLINE_RECEIPTS_MAX);
    assert!(root.receipt_index.is_some());
    assert!(root.revision > revision);
    assert_eq!(authority.receipt(&archived)?, Some(8));

    let encoded = store
        .get(&namespace.key("authority/root")?)?
        .context("root missing")?
        .bytes;
    assert!(
        encoded.len() <= 64 * 1024,
        "migrated head still carries {} bytes",
        encoded.len()
    );
    drop(authority);

    let reopened = Authority::open_local(store, namespace)?;
    let root = reopened.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert_eq!(reopened.receipt(&archived)?, Some(8));
    Ok(())
}

#[test]
fn local_migration_refuses_platform_ownership_records() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("owns-records");
    let catalog = OwnershipCatalog {
        authority_term: 1,
        branches: BTreeMap::from([("main".to_string(), bare_owner_record())]),
        ..OwnershipCatalog::default()
    };
    seed_platform_root(&store, &namespace, 5, catalog, |_| {})?;

    let error = Authority::open_local(store.clone(), namespace.clone())
        .map(|_| ())
        .expect_err("ownership records must not be dropped");
    let message = format!("{error:#}");
    assert!(
        message.contains("ownership catalog"),
        "unexpected refusal: {message}"
    );
    assert_eq!(
        stored_root(&store, &namespace)?.version,
        5,
        "a refused migration must leave the platform head readable"
    );
    Ok(())
}

#[test]
fn local_migration_refuses_platform_branch_state() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("branch-state");
    seed_platform_root(&store, &namespace, 5, empty_ownership(), |root| {
        let branch = root.branches.get_mut("main").unwrap();
        branch.sequence = 1;
        branch.last_commit_ms = 1;
    })?;

    let error = Authority::open_local(store.clone(), namespace.clone())
        .map(|_| ())
        .expect_err("branch state must not be dropped");
    let message = format!("{error:#}");
    assert!(
        message.contains("branch state"),
        "unexpected refusal: {message}"
    );
    assert_eq!(stored_root(&store, &namespace)?.version, 5);
    Ok(())
}

#[test]
fn local_migration_refuses_every_non_v1_root() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("only-v1");
    // Even with no ownership records and a fresh branch identity, a platform version is
    // not a local format and must not be converted.
    seed_platform_root(&store, &namespace, 5, empty_ownership(), |_| {})?;

    let error = Authority::open_local(store.clone(), namespace.clone())
        .map(|_| ())
        .expect_err("only version-1 roots may migrate");
    let message = format!("{error:#}");
    assert!(
        message.contains("only supports version-1"),
        "unexpected refusal: {message}"
    );
    assert_eq!(stored_root(&store, &namespace)?.version, 5);
    Ok(())
}

#[test]
fn migration_is_one_publication_charged_to_the_migration_origin() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("origin");
    seed_v1_root(&store, &namespace, 600, Some(1))?;
    let writes_before = store.root_versions.lock().unwrap().len();
    // Thread-local capture: the global counters are shared with every parallel test, so
    // this test observes only the publications its own thread performed.
    crate::work_metrics::begin_test_publish_capture();

    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    assert_eq!(
        authority.snapshot()?.root.version,
        types::LOCAL_ROOT_VERSION
    );

    let captured = crate::work_metrics::take_test_publish_capture()
        .expect("publication capture was not installed");
    assert_eq!(
        captured.attempts, 1,
        "migration must publish exactly one head"
    );
    assert_eq!(
        captured.origin(crate::work_metrics::PublishOrigin::Migration),
        1
    );
    assert_eq!(
        captured.origin_sum(),
        captured.attempts,
        "origin counters must reconcile"
    );
    assert_eq!(
        captured.outside_transact(crate::work_metrics::OutsideTransactKind::Migration),
        0,
        "a local migration must not bypass transact accounting"
    );

    let versions = store.root_versions.lock().unwrap().clone();
    assert_eq!(
        &versions[writes_before..],
        &[types::LOCAL_ROOT_VERSION],
        "exactly one v6 head landed"
    );
    let group_sizes = store.group_sizes.lock().unwrap().clone();
    let migration_group = group_sizes
        .last()
        .copied()
        .expect("migration group missing");
    assert!(
        migration_group > 2,
        "receipt page bodies and the head must share one group, got {migration_group} writes"
    );
    Ok(())
}

#[test]
fn migration_is_idempotent_after_the_head_is_v6() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("idempotent");
    seed_v1_root(&store, &namespace, 40, Some(2))?;
    let first = Authority::open_local(store.clone(), namespace.clone())?;
    drop(first);
    let writes = store.root_versions.lock().unwrap().len();

    let second = Authority::open_local(store.clone(), namespace.clone())?;
    assert_eq!(second.snapshot()?.root.version, types::LOCAL_ROOT_VERSION);
    assert_eq!(
        store.root_versions.lock().unwrap().len(),
        writes,
        "opening an already migrated store must not publish"
    );
    Ok(())
}

#[test]
fn lost_group_acknowledgement_is_resolved_by_the_migration_receipt() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("lost-ack");
    seed_v1_root(&store, &namespace, 300, Some(2))?;
    store.fault.store(1, Ordering::SeqCst);

    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    assert_eq!(
        authority.snapshot()?.root.version,
        types::LOCAL_ROOT_VERSION
    );
    let versions = store.root_versions.lock().unwrap().clone();
    assert_eq!(
        versions.last().copied(),
        Some(types::LOCAL_ROOT_VERSION),
        "the ambiguous group landed and must stay the only v6 head"
    );
    assert_eq!(
        versions
            .iter()
            .filter(|version| **version == types::LOCAL_ROOT_VERSION)
            .count(),
        1,
        "the retry must not publish a second migration head"
    );
    Ok(())
}

#[test]
fn a_crash_before_the_head_is_retried_on_the_next_open() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("crash");
    seed_v1_root(&store, &namespace, 300, Some(2))?;
    store.fault.store(2, Ordering::SeqCst);

    assert!(
        Authority::open_local(store.clone(), namespace.clone()).is_err(),
        "a migration whose head did not land must fail closed"
    );
    assert_eq!(
        stored_root(&store, &namespace)?.version,
        1,
        "a failed migration must leave the version-1 head readable"
    );
    assert!(stored_root(&store, &namespace)?.receipt_index.is_none());

    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    assert_eq!(
        authority.snapshot()?.root.version,
        types::LOCAL_ROOT_VERSION
    );
    Ok(())
}

#[test]
fn concurrent_open_publishes_one_migration_head_and_both_readers_see_v6() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("concurrent");
    seed_v1_root(&store, &namespace, 500, Some(2))?;
    let barrier = Arc::new(Barrier::new(2));

    let mut handles = Vec::new();
    for _ in 0..2 {
        let store = store.clone();
        let namespace = namespace.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || -> Result<CatalogRoot> {
            barrier.wait();
            let authority = Authority::open_local(store, namespace)?;
            Ok(authority.snapshot()?.root)
        }));
    }
    let roots: Vec<CatalogRoot> = handles
        .into_iter()
        .map(|handle| handle.join().expect("opener panicked"))
        .collect::<Result<_>>()?;
    for root in &roots {
        assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
        assert!(root.receipts.len() <= INLINE_RECEIPTS_MAX);
    }
    assert_eq!(roots[0].revision, roots[1].revision);
    let v6_heads = store
        .root_versions
        .lock()
        .unwrap()
        .iter()
        .filter(|version| **version == types::LOCAL_ROOT_VERSION)
        .count();
    assert_eq!(v6_heads, 1, "two openers must not publish two migrations");
    Ok(())
}

#[test]
fn v6_record_receipt_archives_instead_of_growing_inline() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("bounded");
    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    for _ in 0..200 {
        authority.transact(None, |_| Ok(()))?;
    }
    let root = authority.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert!(
        root.receipts.len() <= INLINE_RECEIPTS_MAX,
        "inline receipt window is {}",
        root.receipts.len()
    );
    let pointer = root.receipt_index.context("receipt archive missing")?;
    let archived =
        paged_index::PagedIndex::new(store.as_ref(), &namespace).entries(Some(&pointer))?;
    assert!(!archived.is_empty(), "no receipt was archived");
    let identifier = archived.keys().next().unwrap().clone();
    assert_eq!(
        authority.receipt(&identifier)?,
        archived[&identifier].as_u64()
    );
    Ok(())
}

#[test]
fn near_8mib_v1_root_migrates_with_a_bounded_head() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("ceiling");
    seed_v1_root(&store, &namespace, 103_000, Some(2))?;
    let before = store
        .get(&namespace.key("authority/root")?)?
        .context("seeded root missing")?
        .bytes
        .len();
    assert!(
        before >= 7 * 1024 * 1024,
        "the boundary fixture is only {before} bytes"
    );
    assert!(before <= types::MAX_ROOT_BYTES);

    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    let root = authority.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert!(root.receipts.len() <= INLINE_RECEIPTS_MAX);
    let after = store
        .get(&namespace.key("authority/root")?)?
        .context("migrated root missing")?
        .bytes
        .len();
    assert!(
        after <= 64 * 1024,
        "migrated head is {after} bytes; inline receipts {}",
        root.receipts.len()
    );
    assert_eq!(
        authority.receipt(&format!("{:064x}", 102_999))?,
        Some(103_000)
    );
    Ok(())
}

#[test]
fn near_8mib_v1_root_migrates_on_a_real_local_store() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(directory.path())?);
    let namespace = namespace("ceiling-local");
    seed_v1_root(&store, &namespace, 103_000, Some(2))?;
    let before = stored_root_bytes(store.as_ref(), &namespace)?;
    assert!(
        before.len() >= 7 * 1024 * 1024 && before.len() <= types::MAX_ROOT_BYTES,
        "the boundary fixture is {} bytes",
        before.len()
    );

    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    let root = authority.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert!(root.receipts.len() <= INLINE_RECEIPTS_MAX);
    let after = stored_root_bytes(store.as_ref(), &namespace)?;
    assert!(
        after.len() <= 64 * 1024,
        "real-store migrated head is {} bytes",
        after.len()
    );
    let pages = store.list(&namespace.key("authority/index-pages")?)?;
    assert!(
        !pages.is_empty(),
        "the receipt archive did not reach the real backend"
    );
    assert_eq!(
        authority.receipt(&format!("{:064x}", 102_999))?,
        Some(103_000)
    );
    drop(authority);
    let reopened = Authority::open_local(store, namespace)?;
    assert_eq!(
        reopened.receipt(&format!("{:064x}", 102_999))?,
        Some(103_000)
    );
    Ok(())
}

#[test]
fn migration_keeps_archived_and_new_inline_receipts_readable() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("mixed-receipts");
    seed_v1_root(&store, &namespace, 1_000, Some(2))?;
    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    // A fresh migration receipt stays inline while the 1,000 old ones are archived.
    let root = authority.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    let (inline_id, inline_value) = root
        .receipts
        .iter()
        .next()
        .context("inline migration receipt missing")?;
    let inline_id = inline_id.clone();
    let inline_value = *inline_value;
    assert_eq!(authority.receipt(&inline_id)?, Some(inline_value));
    assert_eq!(authority.receipt(&format!("{:064x}", 7))?, Some(8));

    // New publications append to the inline window; both representations must answer.
    for _ in 0..8 {
        authority.transact(None, |_| Ok(()))?;
    }
    let root = authority.snapshot()?.root;
    assert!(root.receipts.len() > 1 && root.receipts.len() <= INLINE_RECEIPTS_MAX);
    let (new_id, new_value) = root
        .receipts
        .iter()
        .find(|(identifier, _)| **identifier != inline_id)
        .context("new inline receipt missing")?;
    let new_id = new_id.clone();
    let new_value = *new_value;
    assert_eq!(authority.receipt(&new_id)?, Some(new_value));
    assert_eq!(authority.receipt(&inline_id)?, Some(inline_value));
    assert_eq!(authority.receipt(&format!("{:064x}", 7))?, Some(8));
    drop(authority);

    let reopened = Authority::open_local(store, namespace)?;
    assert_eq!(reopened.receipt(&new_id)?, Some(new_value));
    assert_eq!(reopened.receipt(&format!("{:064x}", 7))?, Some(8));
    Ok(())
}

#[test]
fn migration_merges_a_non_empty_schema_one_pin_layout() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("pin-merge");
    let authority = Authority::open(store.clone(), namespace.clone())?;
    let entry = authority.upload(b"pinned-segment")?;
    let pin = blake3::hash(b"reader-pin").to_hex().to_string();
    let current = authority.snapshot()?;
    let mut root = current.root.clone();
    root.pin_schema = Some(1);
    root.pins.insert(pin.clone(), Vec::new());
    root.pin_generations.clear();
    // The pre-generation key form is what schema 1 wrote; the upgrade must rewrite it.
    root.pin_entries = registry::Registry::inline_with_order(
        BTreeMap::from([(format!("{pin}/{}", entry.blob), entry.clone())]),
        paged_index::KeyOrder::Hashed,
    );
    store.put(
        &namespace.key("authority/root")?,
        &codec::encode(&root)?,
        PutCondition::Match(current.etag),
    )?;

    let migrated = Authority::open_local(store.clone(), namespace.clone())?;
    let root = migrated.snapshot()?.root;
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert_eq!(root.pin_schema, Some(types::PIN_SCHEMA_VERSION));
    assert_eq!(root.pin_generations.get(&pin), Some(&0));
    let entries = root.pin_entries.entries()?;
    assert_eq!(
        entries.get(&format!("{pin}/0/{}", entry.blob)),
        Some(&entry),
        "the pin entry was not rewritten into the generation layout"
    );
    assert!(entries
        .keys()
        .all(|key| types::pin_entry_parts(key).is_some_and(|parts| parts.generation == 0)));
    Ok(())
}

#[test]
fn a_version_6_head_with_ownership_is_rejected() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("ownership");
    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    let mut root = authority.snapshot()?.root;
    root.ownership = Some(Default::default());
    let encoded = codec::encode(&root)?;
    let error = codec::decode(&encoded).expect_err("v6 must reject ownership");
    assert!(
        error
            .to_string()
            .contains("ownership schema version mismatch"),
        "unexpected error: {error}"
    );
    Ok(())
}

#[test]
fn local_v6_cannot_be_booted_back_into_an_ownership_schema() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("no-downgrade");
    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    let error = authority
        .boot_ownership()
        .expect_err("ownership boot must reject a local v6 head");
    let message = match error {
        super::migration::BootFailure::Read(error)
        | super::migration::BootFailure::Unsettled(error) => error.to_string(),
    };
    assert!(
        message.contains("local v6"),
        "unexpected downgrade error: {message}"
    );
    assert_eq!(
        stored_root(&store, &namespace)?.version,
        types::LOCAL_ROOT_VERSION
    );
    Ok(())
}

#[test]
fn a_pre_migration_root_copy_still_opens_with_the_platform_shape() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("backup");
    seed_v1_root(&store, &namespace, 32, Some(2))?;
    let backup = store
        .get(&namespace.key("authority/root")?)?
        .context("seeded root missing")?
        .bytes;

    let _authority = Authority::open_local(store.clone(), namespace.clone())?;
    assert_eq!(
        stored_root(&store, &namespace)?.version,
        types::LOCAL_ROOT_VERSION
    );

    // The fixture's rollback material is exactly the head bytes it captured before the
    // migration; no user data is involved. A fresh store with that head must still open.
    let restored = Arc::new(ProbeStore::default());
    restored.put(
        &namespace.key("authority/root")?,
        &backup,
        PutCondition::Absent,
    )?;
    let platform = Authority::open(restored, namespace)?;
    let root = platform.snapshot()?.root;
    assert_eq!(root.version, 1);
    assert_eq!(root.receipts.len(), 32);
    Ok(())
}

/// Explicit 10k-publication experiment. Kept ignored because the required CI budget is the
/// 1k/receipt and 8 MiB boundary tests above; run it with `--ignored` and report the numbers.
#[test]
#[ignore = "10k publication experiment; run explicitly"]
fn ten_thousand_publications_keep_the_local_head_bounded() -> Result<()> {
    let store = Arc::new(ProbeStore::default());
    let namespace = namespace("ten-thousand");
    let authority = Authority::open_local(store.clone(), namespace.clone())?;
    for _ in 0..10_000 {
        authority.transact(None, |root| {
            // Emulate the steady-state branch hot window an append keeps: a bounded
            // history of recent commit positions, not an ever-growing ledger.
            let branch = root.branches.get_mut("main").unwrap();
            branch.sequence += 1;
            branch.current_sequence = branch.sequence;
            branch.last_commit_ms += 1;
            branch.current_commit_ms = branch.last_commit_ms;
            branch.history.push(RecoveryPoint {
                id: unique_id()?,
                branch: "main".into(),
                timeline: branch.timeline,
                sequence: branch.sequence,
                timestamp_ms: branch.last_commit_ms,
                head: None,
                wal: None,
            });
            if branch.history.len() > RETAINED_RECOVERY_POINTS {
                branch.history.remove(0);
            }
            Ok(())
        })?;
    }
    let root = authority.snapshot()?.root;
    let bytes = store
        .get(&namespace.key("authority/root")?)?
        .context("root missing")?
        .bytes
        .len();
    let max = *store
        .root_sizes
        .lock()
        .unwrap()
        .iter()
        .max()
        .expect("no head was published");
    println!(
        "10k publications: root_bytes={bytes} inline_receipts={} max_head={max}",
        root.receipts.len()
    );
    assert_eq!(root.version, types::LOCAL_ROOT_VERSION);
    assert!(root.receipts.len() <= INLINE_RECEIPTS_MAX);
    assert!(bytes <= 64 * 1024, "10k head is {bytes} bytes");
    assert!(max < 1024 * 1024, "single head reached {max} bytes");
    Ok(())
}
