mod branch;
mod catalog;
mod codec;
mod decode_cache;
#[cfg(test)]
mod identity_scale_tests;
#[cfg(test)]
mod local_migration_tests;
pub(crate) mod local_store;
mod migration;
pub mod ownership;
mod ownership_view;
pub(crate) mod paged_index;
mod registry;
pub(crate) mod request_reads;
#[cfg(test)]
mod scaling_probe;
mod semantic;
#[cfg(test)]
mod store_scaling_tests;
pub mod types;

pub use branch::BranchStore;
pub use types::{
    BranchRef, CatalogRoot, GcCursor, GcPhase, ObjectEntry, OutOfRetention, RecoveryIndex,
    RecoveryPoint, RecoveryPointNotFound, RecoveryRetention, RecoveryTimelineIndex,
    RetentionCapacityExceeded, RetentionMode, RootSnapshot, Tombstone, MAX_HOT_RECOVERY_POINTS,
    RECOVERY_INDEX_SCHEMA,
};

use crate::ownership::types::{CommandReceipt, MutationGuard, MutationStage};
use crate::work_metrics::{
    PublishOrigin, PublishStage, PublishTimer, SnapshotStage, SnapshotTimer,
};
use crate::{
    namespace::Namespace,
    object_store::{ObjectStore, PreconditionFailed, PutCondition},
};
use anyhow::{ensure, Context, Result};
use codec::{decode_with_digest, encode, encode_with_digest};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub struct Authority {
    store: Arc<dyn ObjectStore>,
    namespace: Namespace,
    incarnation: String,
    guard: Option<Arc<dyn MutationGuard>>,
    /// Reader pins whose owner lives in this process. Durable pins without a live owner are
    /// orphans: a crashed session or a release that could not finish. Lifecycle reporting
    /// uses this table; it never authorizes a pin mutation.
    live_pins: Arc<std::sync::Mutex<BTreeSet<String>>>,
    /// Policy applied when a branch creates its recovery index. An existing index keeps the
    /// policy it was published with: a configuration change may extend a future window, but
    /// it must never shorten one this branch already promised.
    recovery_retention: RecoveryRetention,
}

/// Which mutation rules a publication must satisfy. Routine statements keep the head's
/// version, ownership and receipt representation fixed; the one-way local migration is the
/// single path allowed to rewrite them, and it does so in one durability group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationScope {
    Statement,
    LocalMigration,
}

/// Pin-side lifecycle observed by the authority. Session lifecycle (S01) keeps its own
/// states; these answer whether a durable pin still has an owner and whether a release is
/// already in progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PinState {
    /// A live store owns the pin.
    Active,
    /// The owner is gone and the durable release marker is being worked off.
    Releasing,
    /// The pin exists durably but no live store owns it (crash or failed release).
    Orphan,
    /// No durable pin with this identity exists.
    Absent,
}

/// Outcome of one bounded physical-deletion batch. `completed` counts tombstones that moved
/// from pending to deleted; `reaped` counts completed tombstones whose physical key a late
/// writer recreated, which is reported separately because no metadata changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TombstoneDeletion {
    pub(crate) completed: usize,
    pub(crate) reaped: usize,
}

/// Heads this process verified in full, per database lineage. Change-scoped validation is
/// only sound on a head whose retained references were resolved once: a head this process
/// built or verified qualifies, a head that appeared in the store behind its back does not,
/// and the envelope checksum cannot tell the two apart because it proves only that the
/// document is self-consistent. The record is held per process rather than per handle
/// because several authorities share one store (a service and its maintenance handles,
/// fixtures that open the same store twice) and a head one of them verified is exactly as
/// sound for the other. Refusing to record a head only makes the next load verify again,
/// so a full table can never weaken the check it gates.
///
/// Each lineage keeps the last few verified digests, not one. A read can observe the
/// document that was current before a concurrent publication landed and hand it to the
/// decoder after the publisher recorded the new digest. With a single slot that stale read
/// misses, pays a full registry walk, and re-records the older digest, so the next read of
/// the *new* head misses as well: one race costs two deep walks (measured 1.1 s each at
/// this run's registry size). Keeping a short history means a document this process
/// verified is still recognized after the head moves, which is exactly the fact the record
/// exists to preserve.
mod admitted {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Mutex, OnceLock};

    const MAX_DATABASES: usize = 1024;
    const MAX_LINEAGES: usize = 64;
    /// Verified heads remembered per lineage, newest first. Four covers the concurrent
    /// publications the authority serves in one round trip; a read that lags further
    /// behind verifies again, which is sound and rare.
    const RECENT_DIGESTS: usize = 4;
    /// Digests held across every database. The per-lineage ring already bounds the steady
    /// state; this only stops a pathological namespace churn from growing the process.
    const MAX_DIGESTS: usize = 4096;

    type Lineage = Vec<String>;
    type Table = BTreeMap<String, BTreeMap<String, Lineage>>;

    fn table() -> &'static Mutex<Table> {
        static ADMITTED: OnceLock<Mutex<Table>> = OnceLock::new();
        ADMITTED.get_or_init(|| Mutex::new(Table::new()))
    }

    fn total(table: &Table) -> usize {
        table
            .values()
            .flat_map(|lineages| lineages.values())
            .map(Vec::len)
            .sum()
    }

    /// Diagnostic only: `PERI_ADMISSION_TRACE=1` classifies every deep walk, so a
    /// recurring one can be attributed to a missed race or to a head this process never
    /// recorded. Off by default; nothing below runs unless the switch is on.
    struct Seen {
        seq: u64,
        entries: BTreeMap<String, (u64, u64)>,
        order: VecDeque<String>,
    }

    fn tracing() -> bool {
        static TRACE: OnceLock<bool> = OnceLock::new();
        *TRACE.get_or_init(|| std::env::var("PERI_ADMISSION_TRACE").as_deref() == Ok("1"))
    }

    fn seen() -> &'static Mutex<Seen> {
        static SEEN: OnceLock<Mutex<Seen>> = OnceLock::new();
        SEEN.get_or_init(|| {
            Mutex::new(Seen {
                seq: 0,
                entries: BTreeMap::new(),
                order: VecDeque::new(),
            })
        })
    }

    fn short(digest: &str) -> &str {
        &digest[..digest.len().min(12)]
    }

    pub(super) fn trace_remember(_database: &str, _lineage: &str, digest: &str, revision: u64) {
        if !tracing() {
            return;
        }
        let Ok(mut seen) = seen().lock() else {
            return;
        };
        seen.seq += 1;
        let seq = seen.seq;
        if seen
            .entries
            .insert(digest.to_owned(), (seq, revision))
            .is_none()
        {
            seen.order.push_back(digest.to_owned());
            while seen.order.len() > MAX_DIGESTS {
                if let Some(old) = seen.order.pop_front() {
                    seen.entries.remove(&old);
                }
            }
        }
    }

    /// Called just before a snapshot pays the registry walk. `seq` is the number of
    /// remembers this process made; an age near zero means the missed head was published
    /// moments earlier (a race), a large age means the read saw a head this process
    /// recorded long ago and then lost from the ring.
    pub(super) fn trace_miss(database: &str, lineage: &str, digest: &str, revision: u64) {
        if !tracing() {
            return;
        }
        let ring = table()
            .lock()
            .ok()
            .and_then(|table| {
                table
                    .get(database)
                    .and_then(|lineages| lineages.get(lineage))
                    .cloned()
            })
            .unwrap_or_default();
        let Ok(seen) = seen().lock() else {
            return;
        };
        let (kind, age, prior) = match seen.entries.get(digest) {
            Some((seq, prior)) => ("evicted", seen.seq.saturating_sub(*seq), prior.to_string()),
            None => ("never", u64::MAX, "none".to_owned()),
        };
        let ring = ring
            .iter()
            .map(|held| match seen.entries.get(held) {
                Some((_, held_revision)) => format!("{}@{held_revision}", short(held)),
                None => format!("{}@?", short(held)),
            })
            .collect::<Vec<_>>()
            .join(",");
        eprintln!(
            "ADMISSION_MISS kind={kind} db={database} lineage={} digest={} rev={revision} \
             prior_rev={prior} age={age} remembers={} ring=[{ring}]",
            short(lineage),
            short(digest),
            seen.seq
        );
    }

    /// A lock that cannot be taken is not a reason to trust a head, so it reports the head
    /// as unverified and the caller re-verifies it.
    pub(super) fn holds(database: &str, lineage: &str, digest: &str) -> bool {
        table().lock().is_ok_and(|table| {
            table
                .get(database)
                .and_then(|lineages| lineages.get(lineage))
                .is_some_and(|held| held.iter().any(|recent| recent == digest))
        })
    }

    pub(super) fn remember(database: &str, lineage: &str, digest: &str) {
        let Ok(mut table) = table().lock() else {
            return;
        };
        if table
            .get(database)
            .and_then(|lineages| lineages.get(lineage))
            .is_some_and(|held| held.iter().any(|recent| recent == digest))
        {
            return;
        }
        if total(&table) >= MAX_DIGESTS {
            return;
        }
        if !table.contains_key(database) && table.len() >= MAX_DATABASES {
            return;
        }
        let lineages = table.entry(database.to_owned()).or_default();
        if !lineages.contains_key(lineage) && lineages.len() >= MAX_LINEAGES {
            return;
        }
        let held = lineages.entry(lineage.to_owned()).or_default();
        held.insert(0, digest.to_owned());
        held.truncate(RECENT_DIGESTS);
    }
}

const INLINE_RECEIPTS_MAX: usize = 128;

/// Recovery points cached inline in the branch reference. The head is decoded, validated,
/// re-encoded and conditionally published on every statement, and one point is ~661 B of
/// it, so the hot window must stay bounded. It is not the retention contract: the immutable
/// recovery index keeps older points and answers queries without reading this window. A
/// legacy branch that predates the index still uses this bound as its whole history; the
/// first publication after the upgrade migrates that history into an index.
const RETAINED_RECOVERY_POINTS: usize = 32;

/// Recent command receipts kept inline in the ownership catalog. Every statement decodes
/// and validates the whole head, and one receipt carries a full reply (~357 B), so an
/// unbounded ledger would make each lease validation follow how long the namespace has
/// been running. Older terminal receipts move to immutable pages; pending GC receipts
/// stay inline because the collector rewrites their status in place.
const INLINE_COMMANDS_MAX: usize = 32;

/// A command receipt value larger than this cannot share a page with another entry: the
/// immutable page budget is 16 KiB and a page must hold at least one value. Such values
/// are spooled to their own content-addressed object and the page tree stores only the
/// digest, so a large GC reply never wedges archiving or leaves an unbounded inline entry.
const COMMAND_ARCHIVE_VALUE_MAX: usize = 8 * 1024;

/// A receipt the archive may take. `pending_gc` is the one status the collector rewrites
/// after insertion, and pages are immutable, so it stays in the inline window until the
/// collection completes.
pub(super) fn terminal_receipt(receipt: &CommandReceipt) -> bool {
    receipt
        .result
        .get("status")
        .and_then(|status| status.as_str())
        != Some("pending_gc")
}

/// Test-only: whether a publication reaches the store as one durability group.
#[cfg(test)]
static GROUP_COMMIT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
const RECEIPT_ARCHIVE_BATCH: usize = 64;

/// A physical instance whose bytes ride in the caller's publication group instead of a
/// separate conditional put. The instance, the logical registration that makes it
/// reachable and the head CAS become one durable publication, so no intent has to be
/// reserved in an earlier revision. Instances that are published this way are never
/// referenced before the group lands, and a torn group can only leave bytes nothing
/// points at — the same cost the paged registry already accepts for page bodies.
pub(super) struct PublicationBody<'a> {
    pub entry: &'a ObjectEntry,
    pub bytes: &'a [u8],
}

/// The bytes of a body this publication has staged but not written. Validation runs
/// before the group lands, so a candidate that references one of its own bodies reads it
/// from the group buffer instead of rejecting the publication that installs it.
pub(super) fn staged_body<'b>(
    bodies: &'b [PublicationBody<'_>],
    entry: &ObjectEntry,
) -> Option<&'b [u8]> {
    bodies
        .iter()
        .find(|body| body.entry.blob == entry.blob)
        .map(|body| body.bytes)
}

/// Decode a stored head and give its registries a store handle. A hand-decoded root
/// keeps no handle and can only serve inline content, which fails closed for pages.
fn decode_head_with_digest(
    store: &Arc<dyn ObjectStore>,
    namespace: &Namespace,
    bytes: &[u8],
) -> Result<(CatalogRoot, String)> {
    let (root, digest) = decode_with_digest(bytes)?;
    // The publication path attaches its store to the registries, so it needs a head of its
    // own; readers share the decoded one.
    let mut root = (*root).clone();
    // An empty pin tree carries neither ordering nor generation state, so the head can
    // be read as current without rewriting it. The marker rides the next real
    // publication instead: opening a legacy root that has nothing to migrate must stay
    // a read (tests/p4_compat.rs verifies the exact old checksum).
    if root.pins.is_empty() && root.pin_entries.is_empty() && root.pin_generations.is_empty() {
        root.pin_schema = Some(types::PIN_SCHEMA_VERSION);
    }
    root.attach(store.clone(), namespace.clone());
    semantic::migrate_legacy_pins(&mut root)?;
    root.validate_shape()?;
    Ok((root, digest))
}

pub(super) fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

pub(super) fn unique_id() -> Result<String> {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut hash = blake3::Hasher::new();
    hash.update(&time.to_le_bytes());
    hash.update(&std::process::id().to_le_bytes());
    hash.update(&SEQUENCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    Ok(hash.finalize().to_hex().to_string())
}

impl Authority {
    pub fn open(store: Arc<dyn ObjectStore>, namespace: Namespace) -> Result<Arc<Self>> {
        Self::open_with_retention(store, namespace, RecoveryRetention::default())
    }

    /// Open an authority whose not-yet-indexed branches publish the given retention
    /// contract. Existing branches keep their durable policy.
    pub fn open_with_retention(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        recovery_retention: RecoveryRetention,
    ) -> Result<Arc<Self>> {
        recovery_retention.validate()?;
        let namespace = Namespace::new(namespace.tenant, namespace.database)?;
        let object = match Self::open_root(store.as_ref(), &namespace)? {
            Some(object) => object,
            None => Self::claim_absent_root(store.as_ref(), &namespace)?,
        };
        Self::from_loaded_with_retention(store, namespace, object, recovery_retention)
    }

    pub(crate) fn open_root(
        store: &dyn ObjectStore,
        namespace: &Namespace,
    ) -> Result<Option<crate::object_store::StoredObject>> {
        store.get(&namespace.key("authority/root")?)
    }

    pub(crate) fn claim_absent_root(
        store: &dyn ObjectStore,
        namespace: &Namespace,
    ) -> Result<crate::object_store::StoredObject> {
        let key = namespace.key("authority/root")?;
        let root = CatalogRoot::empty(namespace.clone(), unique_id()?);
        let result = store.put(&key, &encode(&root)?, PutCondition::Absent);
        store.get(&key)?.ok_or_else(|| {
            result
                .err()
                .unwrap_or_else(|| anyhow::anyhow!("authority root missing after initialization"))
        })
    }

    /// Open a local authority. Local stores are single-writer by construction and carry no
    /// ownership catalog, so they use their own root version instead of the platform schema.
    /// A version-1/2/4/5 head is migrated once, in the same publication that upgrades the
    /// reader-pin layout; there is never an intermediate pin-only publication.
    pub(crate) fn open_local(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
    ) -> Result<Arc<Self>> {
        Self::open_local_with_retention(store, namespace, RecoveryRetention::default())
    }

    pub(crate) fn open_local_with_retention(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        recovery_retention: RecoveryRetention,
    ) -> Result<Arc<Self>> {
        recovery_retention.validate()?;
        let namespace = Namespace::new(namespace.tenant, namespace.database)?;
        let object = match Self::open_root(store.as_ref(), &namespace)? {
            Some(object) => object,
            None => Self::claim_absent_local_root(store.as_ref(), &namespace)?,
        };
        Self::from_loaded_local(store, namespace, object, recovery_retention)
    }

    pub(crate) fn claim_absent_local_root(
        store: &dyn ObjectStore,
        namespace: &Namespace,
    ) -> Result<crate::object_store::StoredObject> {
        let key = namespace.key("authority/root")?;
        let root = CatalogRoot::local_empty(namespace.clone(), unique_id()?);
        let result = store.put(&key, &encode(&root)?, PutCondition::Absent);
        store.get(&key)?.ok_or_else(|| {
            result
                .err()
                .unwrap_or_else(|| anyhow::anyhow!("authority root missing after initialization"))
        })
    }

    fn from_loaded_local(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        object: crate::object_store::StoredObject,
        recovery_retention: RecoveryRetention,
    ) -> Result<Arc<Self>> {
        let key = namespace.key("authority/root")?;
        let (root, digest) = decode_head_with_digest(&store, &namespace, &object.bytes)?;
        ensure!(
            root.namespace == namespace,
            "authority root namespace mismatch"
        );
        let authority = Arc::new(Self {
            store,
            namespace,
            incarnation: root.incarnation.clone(),
            guard: None,
            live_pins: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
            recovery_retention,
        });
        authority.verify_deep(&root)?;
        authority.verify_recovery_indexes(&root)?;
        authority.remember(&key, &root, &digest);
        // `upgrade_pin_layout` is intentionally not used here: its publication is merged
        // with the schema migration so a first open can only publish one head.
        if !Self::is_local_root(&root) {
            authority.migrate_local_root()?;
        }
        Ok(authority)
    }

    /// Whether the head already has the local durable shape. The pin marker is included
    /// because a v6 head can still carry a pre-split pin tree; the migration upgrades both
    /// representations in one publication.
    fn is_local_root(root: &CatalogRoot) -> bool {
        root.version == types::LOCAL_ROOT_VERSION
            && root.ownership.is_none()
            && root.pin_schema == Some(types::PIN_SCHEMA_VERSION)
    }

    pub(crate) fn from_loaded(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        object: crate::object_store::StoredObject,
    ) -> Result<Arc<Self>> {
        Self::from_loaded_with_retention(store, namespace, object, RecoveryRetention::default())
    }

    pub(crate) fn from_loaded_with_retention(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        object: crate::object_store::StoredObject,
        recovery_retention: RecoveryRetention,
    ) -> Result<Arc<Self>> {
        let key = namespace.key("authority/root")?;
        let (root, digest) = decode_head_with_digest(&store, &namespace, &object.bytes)?;
        ensure!(
            root.namespace == namespace,
            "authority root namespace mismatch"
        );
        let authority = Arc::new(Self {
            store,
            namespace,
            incarnation: root.incarnation.clone(),
            guard: None,
            live_pins: Arc::new(std::sync::Mutex::new(BTreeSet::new())),
            recovery_retention,
        });
        authority.verify_deep(&root)?;
        authority.verify_recovery_indexes(&root)?;
        authority.remember(&key, &root, &digest);
        authority.upgrade_pin_layout(&root)?;
        Ok(authority)
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn recovery_retention(&self) -> &RecoveryRetention {
        &self.recovery_retention
    }

    pub(crate) fn scoped(&self, guard: Arc<dyn MutationGuard>) -> Arc<Self> {
        Arc::new(Self {
            store: self.store.clone(),
            namespace: self.namespace.clone(),
            incarnation: self.incarnation.clone(),
            guard: Some(guard),
            live_pins: self.live_pins.clone(),
            recovery_retention: self.recovery_retention.clone(),
        })
    }

    /// Record a head as verified. Only called once the head has passed `verify_deep`, so
    /// the change-scoped path can never be entered on a head this process has not seen the
    /// whole of.
    fn remember(&self, key: &str, root: &CatalogRoot, digest: &str) {
        admitted::remember(key, &root.incarnation, digest);
        admitted::trace_remember(key, &root.incarnation, digest, root.revision);
    }

    pub(super) fn check(&self, stage: MutationStage) -> Result<()> {
        if let Some(guard) = &self.guard {
            guard.validate(stage)?;
        }
        Ok(())
    }

    fn uncertain(&self, stage: MutationStage, error: &anyhow::Error) {
        if !error.is::<PreconditionFailed>() {
            if let Some(guard) = &self.guard {
                eprintln!("level=error event=mutation_uncertain stage={stage:?} 原因：{error:#}");
                guard.uncertain(stage);
            }
        }
    }

    pub fn snapshot(&self) -> Result<RootSnapshot> {
        #[cfg(test)]
        let _probe = scaling_probe::stage("snapshot", 0);
        let mut snapshot = SnapshotTimer::begin();
        snapshot.stage(SnapshotStage::Guard);
        self.check(MutationStage::Read)?;
        let key = self.namespace.key("authority/root")?;
        snapshot.stage(SnapshotStage::Store);
        let object = self
            .store
            .get(&key)?
            .context("authority root missing; refusing reset")?;
        snapshot.root_bytes(object.bytes.len());
        snapshot.stage(SnapshotStage::Decode);
        let (root, digest) = decode_head_with_digest(&self.store, &self.namespace, &object.bytes)?;
        snapshot.stage(SnapshotStage::Admit);
        if admitted::holds(&key, &root.incarnation, &digest) {
            snapshot.stage(SnapshotStage::Head);
            self.validate_head(&root)?;
            snapshot.stage(SnapshotStage::Refs);
            self.validate_references(&root, &[])?;
        } else {
            // The head is not one this process verified, so the references it inherits may
            // have been replaced behind the authority's back: an envelope checksum proves
            // the document is self-consistent, not that this authority wrote it, and a
            // reference that was never registered can only be caught by resolving it.
            // Verify the way boot does before any statement trusts the head.
            admitted::trace_miss(&key, &root.incarnation, &digest, root.revision);
            snapshot.stage(SnapshotStage::Deep);
            self.verify_deep(&root)?;
            self.remember(&key, &root, &digest);
        }
        Ok(RootSnapshot {
            root,
            etag: object.etag,
        })
    }

    pub fn receipt(&self, operation: &str) -> Result<Option<u64>> {
        let snapshot = self.snapshot()?;
        self.receipt_in(&snapshot.root, operation)
    }

    fn receipt_in(&self, root: &CatalogRoot, operation: &str) -> Result<Option<u64>> {
        if let Some(revision) = root.receipts.get(operation) {
            return Ok(Some(*revision));
        }
        if types::supports_paged_receipts(root.version) {
            let value = paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace)
                .get(root.receipt_index.as_deref(), operation)?;
            value
                .map(|value| {
                    value
                        .as_u64()
                        .filter(|revision| *revision > 0 && *revision <= root.revision)
                        .context("invalid paged authority receipt")
                })
                .transpose()
        } else {
            Ok(None)
        }
    }

    fn record_receipt(
        &self,
        root: &mut CatalogRoot,
        operation: String,
        index: &paged_index::PagedIndex<'_>,
    ) -> Result<()> {
        root.receipts.insert(operation, root.revision);
        if !types::supports_paged_receipts(root.version)
            || root.receipts.len() <= INLINE_RECEIPTS_MAX
        {
            return Ok(());
        }
        let mut oldest: Vec<_> = root
            .receipts
            .iter()
            .map(|(id, revision)| (id.clone(), *revision))
            .collect();
        oldest.sort_by_key(|(_, revision)| *revision);
        let archived: Vec<_> = oldest.into_iter().take(RECEIPT_ARCHIVE_BATCH).collect();
        let updates = archived
            .iter()
            .map(|(id, revision)| (id.clone(), serde_json::json!(revision)))
            .collect();
        let pointer = index.insert_many(root.receipt_index.as_deref(), updates)?;
        for (id, _) in archived {
            root.receipts.remove(&id);
        }
        root.receipt_index = Some(pointer);
        Ok(())
    }

    /// Move terminal command receipts beyond the inline window into the immutable page
    /// tree. Runs after `prepare` for the same reason as `record_receipt`: the candidate
    /// still contains every receipt the transaction validated, and only this writer may
    /// move them. A lookup consults both representations, so archiving never erases an
    /// idempotency answer.
    pub(super) fn archive_commands(
        &self,
        root: &mut CatalogRoot,
        index: &paged_index::PagedIndex<'_>,
    ) -> Result<()> {
        // Paged command receipts belong to the version-5 ownership schema. A local v6 root
        // has no ownership catalog, so there is nothing to archive.
        if root.version != 5 {
            return Ok(());
        }
        let Some(catalog) = root.ownership.as_mut() else {
            return Ok(());
        };
        let excess = catalog
            .commands
            .values()
            .filter(|receipt| terminal_receipt(receipt))
            .count()
            .saturating_sub(INLINE_COMMANDS_MAX);
        if excess == 0 {
            return Ok(());
        }
        let mut terminal: Vec<String> = catalog
            .commands
            .iter()
            .filter(|(_, receipt)| terminal_receipt(receipt))
            .map(|(identifier, _)| identifier.clone())
            .collect();
        terminal.sort();
        let archived: Vec<String> = terminal.into_iter().take(excess).collect();
        let mut updates = std::collections::BTreeMap::new();
        for identifier in &archived {
            let receipt = catalog
                .commands
                .get(identifier)
                .context("command receipt vanished before archiving")?;
            crate::ownership::types::validate_label(identifier)?;
            ensure!(
                crate::manifest::valid_hash(&receipt.digest),
                "invalid command receipt digest"
            );
            updates.insert(identifier.clone(), self.archive_value(receipt)?);
        }
        let pointer = index.insert_many(catalog.command_index.as_deref(), updates)?;
        for identifier in archived {
            catalog.commands.remove(&identifier);
        }
        catalog.command_index = Some(pointer);
        Ok(())
    }

    /// Values that fit a page stay in it. Larger values go to an immutable object first;
    /// the page entry is only a digest, and a lost head CAS leaves an unreferenced object
    /// behind rather than a pointer to bytes that were never written.
    fn archive_value(&self, receipt: &CommandReceipt) -> Result<serde_json::Value> {
        let bytes = serde_json::to_vec(receipt)?;
        if bytes.len() <= COMMAND_ARCHIVE_VALUE_MAX {
            return Ok(serde_json::from_slice(&bytes)?);
        }
        let hash = blake3::hash(&bytes).to_hex().to_string();
        self.store.put(
            &self
                .namespace
                .key(&format!("authority/command-archive/{hash}"))?,
            &bytes,
            PutCondition::Any,
        )?;
        Ok(serde_json::json!({ "archive_object": hash }))
    }

    /// Read a receipt that left the inline window. The page tree is content addressed and
    /// immutable, so a missing page or a malformed entry fails closed instead of turning
    /// an old idempotency key into a fresh command.
    pub(crate) fn archived_command(
        &self,
        pointer: Option<&str>,
        identifier: &str,
    ) -> Result<Option<CommandReceipt>> {
        let Some(value) = paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace)
            .get(pointer, identifier)?
        else {
            return Ok(None);
        };
        let Some(hash) = value.get("archive_object").and_then(|hash| hash.as_str()) else {
            return Ok(Some(serde_json::from_value(value)?));
        };
        ensure!(
            value.as_object().is_some_and(|value| value.len() == 1),
            "invalid archived command pointer"
        );
        let key = self
            .namespace
            .key(&format!("authority/command-archive/{hash}"))?;
        let object = self
            .store
            .get(&key)?
            .context("archived command payload missing")?;
        ensure!(
            blake3::hash(&object.bytes).to_hex().as_str() == hash,
            "archived command payload mismatch"
        );
        Ok(Some(serde_json::from_slice(&object.bytes)?))
    }

    pub fn transact<F>(&self, expected_revision: Option<u64>, mutation: F) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_with_origin(expected_revision, PublishOrigin::Other, mutation)
    }

    /// Publish with an explicit attribution for the origin counters. The mutation runs
    /// identically to [`Self::transact`]; only the counter charged at the head CAS differs.
    pub(crate) fn transact_with_origin<F>(
        &self,
        expected_revision: Option<u64>,
        origin: PublishOrigin,
        mutation: F,
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_inner(expected_revision, origin, mutation, &BTreeSet::new(), &[])
    }

    /// Publish a mutation whose group also carries new physical instances. The bodies
    /// are written before the head CAS, so a reader that sees the registration always
    /// sees the bytes; see `PublicationBody` for why no intent precedes them.
    pub(super) fn transact_with_bodies<F>(
        &self,
        origin: PublishOrigin,
        bodies: &[PublicationBody<'_>],
        mutation: F,
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_inner(None, origin, mutation, &BTreeSet::new(), bodies)
    }

    fn transact_inner<F>(
        &self,
        expected_revision: Option<u64>,
        origin: PublishOrigin,
        mut mutation: F,
        deletions: &BTreeSet<String>,
        bodies: &[PublicationBody<'_>],
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_inner_scoped(
            expected_revision,
            origin,
            PublicationScope::Statement,
            |root, _| mutation(root),
            deletions,
            bodies,
        )
    }

    fn transact_inner_scoped<F>(
        &self,
        expected_revision: Option<u64>,
        origin: PublishOrigin,
        scope: PublicationScope,
        mut mutation: F,
        deletions: &BTreeSet<String>,
        bodies: &[PublicationBody<'_>],
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot, &paged_index::PagedIndex<'_>) -> Result<()>,
    {
        #[cfg(test)]
        let _probe = scaling_probe::stage("transact", 0);
        let _reads = request_reads::Scope::enter();
        let operation = unique_id()?;
        for _ in 0..128 {
            let mut publish = PublishTimer::begin();
            publish.stage(PublishStage::Snapshot);
            let current = self.snapshot()?;
            if let Some(revision) = expected_revision {
                ensure!(current.root.revision == revision, PreconditionFailed);
            }
            // A concurrent local opener may have completed the migration while this
            // attempt was between its own check and this snapshot. The loser must adopt the
            // published v6 head, not publish a second migration head of its own.
            if scope == PublicationScope::LocalMigration && Self::is_local_root(&current.root) {
                return Ok(current);
            }
            // One durability group per attempt: the immutable in-flight page bodies are
            // buffered here and the head CAS joins them as the last write, so the store
            // either lands the whole publication or none of it.
            let pending = std::cell::RefCell::new(paged_index::PendingPages::default());
            let recovery_pages = Rc::new(RefCell::new(Vec::new()));
            let _recovery_scope = RecoveryStagingScope::enter(recovery_pages.clone());
            let publishing = paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace)
                .deferring(&pending);
            publish.stage(PublishStage::Clone);
            let mut candidate = {
                #[cfg(test)]
                let _probe = scaling_probe::stage("clone", scaling_probe::entries(&current.root));
                current.root.clone()
            };
            publish.stage(PublishStage::Mutate);
            mutation(&mut candidate, &publishing)?;
            publish.stage(PublishStage::Prepare);
            self.prepare_with(&current.root, &mut candidate, deletions, bodies, scope)?;
            let ownership_changed = candidate.ownership != current.root.ownership;
            candidate.revision = current
                .root
                .revision
                .checked_add(1)
                .context("authority revision overflow")?;
            publish.stage(PublishStage::Receipt);
            self.record_receipt(&mut candidate, operation.clone(), &publishing)?;
            if ownership_changed {
                self.archive_commands(&mut candidate, &publishing)?;
            }
            publish.stage(PublishStage::Validate);
            self.validate_head_with(&candidate, Some(&pending))?;
            self.validate_references(&candidate, bodies)?;
            publish.stage(PublishStage::Index);
            candidate.blobs.publish(&publishing)?;
            candidate.objects.publish(&publishing)?;
            candidate.pin_entries.publish(&publishing)?;
            candidate.uploads.publish(&publishing)?;
            candidate.tombstones.publish(&publishing)?;
            publish.stage(PublishStage::Seal);
            candidate.seal()?;
            publish.stage(PublishStage::Encode);
            let (encoded, digest) = encode_with_digest(&candidate)?;
            crate::work_metrics::observe_publish_origin(origin);
            crate::work_metrics::observe_root_publish(
                encoded.len(),
                candidate.pin_entries.window_len(),
                candidate.receipts.len(),
            );
            if let Some(guard) = &self.guard {
                guard.validate_root(&current.root, &candidate)?;
            }
            self.check(MutationStage::RootPublish)?;
            let mut writes = pending.borrow().writes().to_vec();
            writes.extend(recovery_pages.borrow_mut().drain(..));
            // Bodies are instances this group itself introduces, so they carry no
            // condition: the group stays condition-checked exactly once, by the head CAS
            // that ends it. The keys are fixed before the first attempt, so a retry
            // rewrites identical bytes and the last write stays the fence.
            for body in bodies {
                writes.push(crate::object_store::GroupWrite::new(
                    self.physical_key(&body.entry.blob)?,
                    body.bytes.to_vec(),
                    PutCondition::Any,
                ));
            }
            writes.push(crate::object_store::GroupWrite::new(
                self.namespace.key("authority/root")?,
                encoded,
                PutCondition::Match(current.etag),
            ));
            // Test-only switch: `false` reproduces the per-write behaviour this change
            // replaced, so a paired measurement can attribute the barrier difference.
            #[cfg(test)]
            let grouped = GROUP_COMMIT.load(Ordering::Relaxed);
            #[cfg(not(test))]
            let grouped = true;
            // The head becomes visible at the group's rename, but its admission could only
            // be recorded after the barrier returned. A snapshot that observed the new
            // head inside that window found it unverified and paid a full registry walk;
            // under load the barrier window is a measurable share of the timeline, which
            // is where the recurring deep walks came from. This attempt built the head
            // from a verified one and validated the change set, so recording it here
            // carries the same guarantee `remember` documents. A failed CAS leaves a
            // digest no stored head carries, so admitting early cannot vouch for a
            // document that never landed.
            self.remember(&self.namespace.key("authority/root")?, &candidate, &digest);
            publish.stage(PublishStage::Group);
            let published = if grouped {
                self.store.put_group(&writes)
            } else {
                writes
                    .iter()
                    .map(|write| {
                        self.store
                            .put(&write.key, &write.bytes, write.condition.clone())
                    })
                    .collect()
            };
            match published {
                Ok(etags) => {
                    for write in pending.borrow().writes() {
                        request_reads::remember_page(
                            Arc::as_ptr(&self.store) as *const () as usize,
                            write.key.clone(),
                            &write.bytes,
                        );
                    }
                    for body in bodies {
                        request_reads::remember(
                            Arc::as_ptr(&self.store) as *const () as usize,
                            self.physical_key(&body.entry.blob)?,
                            body.entry,
                            body.bytes,
                        );
                    }
                    // The head this attempt recorded before the group is the one the CAS
                    // landed; nothing further to admit.
                    return Ok(RootSnapshot {
                        root: candidate,
                        etag: etags
                            .last()
                            .cloned()
                            .context("authority root etag missing")?,
                    });
                }
                Err(error) => {
                    self.uncertain(MutationStage::RootPublish, &error);
                    publish.stage(PublishStage::Recover);
                    let observed = self.snapshot()?;
                    if self.receipt_in(&observed.root, &operation)? == Some(candidate.revision) {
                        return Ok(observed);
                    }
                    if error.is::<PreconditionFailed>() && expected_revision.is_none() {
                        // The head moved under this attempt, so the next loop iteration is a
                        // retry of the same publication.
                        crate::work_metrics::observe_root_publish_retry();
                        continue;
                    }
                    return Err(error);
                }
            }
        }
        Err(PreconditionFailed.into())
    }

    pub fn upload(&self, bytes: &[u8]) -> Result<ObjectEntry> {
        self.check(MutationStage::Upload)?;
        let entry = Self::upload_entry(bytes)?;
        self.transact_with_origin(None, PublishOrigin::Put, |root| {
            ensure!(
                !root.blobs.contains(&entry.blob)? && !root.tombstones.contains(&entry.blob)?,
                "physical instance collision"
            );
            root.blobs.insert(entry.blob.clone(), entry.clone())?;
            root.uploads.insert(entry.blob.clone(), entry.clone())?;
            Ok(())
        })?;
        self.finish_upload(entry, bytes)
    }

    pub(crate) fn upload_segments(&self, segments: &[Vec<u8>]) -> Result<Vec<ObjectEntry>> {
        ensure!(
            !segments.is_empty() && segments.len() <= 128,
            "invalid segment upload batch"
        );
        self.check(MutationStage::Upload)?;
        let entries = segments
            .iter()
            .map(|segment| {
                ensure!(
                    segment.len() == crate::manifest::SEGMENT_SIZE,
                    "invalid segment upload size"
                );
                Self::upload_entry(segment)
            })
            .collect::<Result<Vec<_>>>()?;
        self.transact_with_origin(None, PublishOrigin::Put, |root| {
            for entry in &entries {
                ensure!(
                    !root.blobs.contains(&entry.blob)? && !root.tombstones.contains(&entry.blob)?,
                    "physical instance collision"
                );
                root.blobs.insert(entry.blob.clone(), entry.clone())?;
                root.uploads.insert(entry.blob.clone(), entry.clone())?;
            }
            Ok(())
        })?;
        entries
            .into_iter()
            .zip(segments)
            .map(|(entry, bytes)| self.finish_upload(entry, bytes))
            .collect()
    }

    pub(crate) fn register_segments(&self, entries: &[ObjectEntry]) -> Result<()> {
        ensure!(
            !entries.is_empty() && entries.len() <= 128,
            "invalid segment registration batch"
        );
        for entry in entries {
            ensure!(
                self.read_blob(entry)?.len() == crate::manifest::SEGMENT_SIZE,
                "invalid staged segment size"
            );
        }
        self.transact_with_origin(None, PublishOrigin::Put, |root| {
            for entry in entries {
                semantic::registered(root, entry)?;
                let key = format!("segments/{}", entry.hash);
                ensure!(
                    root.objects
                        .get(&key)?
                        .is_none_or(|current| current == *entry),
                    "segment logical identity conflict"
                );
                root.objects.insert(key, entry.clone())?;
            }
            Ok(())
        })?;
        Ok(())
    }

    pub(super) fn upload_entry(bytes: &[u8]) -> Result<ObjectEntry> {
        Ok(ObjectEntry {
            blob: unique_id()?,
            hash: blake3::hash(bytes).to_hex().to_string(),
            etag: unique_id()?,
            created_ms: now_ms()?,
        })
    }

    fn finish_upload(&self, entry: ObjectEntry, bytes: &[u8]) -> Result<ObjectEntry> {
        let key = self.physical_key(&entry.blob)?;
        self.check(MutationStage::Upload)?;
        if let Err(error) = self.store.put(&key, bytes, PutCondition::Absent) {
            self.uncertain(MutationStage::Upload, &error);
            let object = self
                .store
                .get(&key)?
                .context("unresolved physical upload intent")?;
            ensure!(
                object.bytes == bytes,
                "physical upload content conflict: {error}"
            );
        }
        request_reads::remember(
            Arc::as_ptr(&self.store) as *const () as usize,
            key,
            &entry,
            bytes,
        );
        Ok(entry)
    }

    pub fn read_blob(&self, entry: &ObjectEntry) -> Result<Vec<u8>> {
        self.check(MutationStage::Read)?;
        semantic::validate_entry(entry)?;
        let object = self
            .store
            .get(&self.physical_key(&entry.blob)?)?
            .context("physical blob missing")?;
        ensure!(
            blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
            "physical blob checksum mismatch"
        );
        Ok(object.bytes)
    }

    fn read_metadata_blob(&self, entry: &ObjectEntry) -> Result<Vec<u8>> {
        #[cfg(test)]
        let _probe = scaling_probe::stage("ref_body", 0);
        self.check(MutationStage::Read)?;
        semantic::validate_entry(entry)?;
        let backend = Arc::as_ptr(&self.store) as *const () as usize;
        let key = self.physical_key(&entry.blob)?;
        if let Some(bytes) = request_reads::get(backend, &key, entry) {
            return Ok(bytes);
        }
        let bytes = self.read_blob(entry)?;
        request_reads::remember(backend, key, entry, &bytes);
        Ok(bytes)
    }

    pub(super) fn physical_key(&self, blob: &str) -> Result<String> {
        ensure!(
            crate::manifest::valid_hash(blob),
            "invalid physical blob identifier"
        );
        self.namespace.key(&format!("authority/blobs/{blob}"))
    }

    /// Physical blob identities reachable from branch, snapshot and reader-pin references.
    /// Registry page objects (including `pin_entries` pages) are logical metadata under
    /// `authority/index-pages`; they are neither members of this set nor deleted by GC.
    pub fn reachable(&self, root: &CatalogRoot) -> Result<BTreeSet<String>> {
        self.verify_deep(root)?;
        let mut reachable = self.mark(root, false)?;
        self.extend_reachable_with_recovery(root, &mut reachable)?;
        Ok(reachable)
    }

    /// Reachability for the mark phase: a blob named by any logical object alias counts as
    /// reachable until the prune phase removes that alias. Marking an aliased blob would
    /// publish a tombstone while the object entry still references it, a head that fails
    /// deep validation and cannot be reopened.
    pub(crate) fn reachable_with_aliases(&self, root: &CatalogRoot) -> Result<BTreeSet<String>> {
        self.verify_deep(root)?;
        let mut reachable = self.mark(root, true)?;
        self.extend_reachable_with_recovery(root, &mut reachable)?;
        Ok(reachable)
    }

    /// Number of reader-pin entries currently registered, across every pin. Identity count
    /// (`GcStats::reader_pins`) is intentionally separate; this is the entry population the
    /// paged pin registry holds.
    pub fn pinned_entry_count(&self) -> Result<usize> {
        Ok(self.snapshot()?.root.pin_entries.entries()?.len())
    }

    /// Lifecycle of one reader pin as this process can see it. `OutOfRetention` and the S01
    /// session states keep their existing meaning; this is the pin-side observation that
    /// tells a normal long read, a long-lived store, an orphan and a leaking release apart.
    pub fn pin_state(&self, pin: &str) -> Result<PinState> {
        let root = self.snapshot()?.root;
        let live = self
            .live_pins
            .lock()
            .map_err(|_| anyhow::anyhow!("live pin registry lock poisoned"))?
            .contains(pin);
        if root.pin_releases.contains(pin) {
            return Ok(PinState::Releasing);
        }
        if !root.pins.contains_key(pin) {
            return Ok(PinState::Absent);
        }
        Ok(if live {
            PinState::Active
        } else {
            PinState::Orphan
        })
    }

    /// Finish releases whose owner disappeared between generation batches. The marker and
    /// the advanced generation are the durable cursor; this only resumes work that is
    /// already authorized, never starts a new release.
    pub(crate) fn resume_pin_releases(&self, limit: usize) -> Result<usize> {
        let snapshot = self.snapshot()?;
        let releases: Vec<String> = snapshot
            .root
            .pin_releases
            .iter()
            .take(limit)
            .cloned()
            .collect();
        for pin in &releases {
            branch::release_pin_identity(self, pin)?;
        }
        Ok(releases.len())
    }

    /// One-way migration of a local store to the version-6 shape. It runs as a normal
    /// publication: immutable receipt/pin page bodies and the head CAS share one durability
    /// group, the retry loop resolves an unknown acknowledgement through the migration
    /// receipt, and the origin counter charges [`PublishOrigin::Migration`] at the same
    /// point as `root_publish_attempts`.
    ///
    /// The reader-pin layout upgrade is merged into this mutation. Doing it in a separate
    /// publication would expose two failure windows during a first open; the merged version
    /// is idempotent because every page is content-addressed and the operation receipt
    /// identifies an acknowledged attempt.
    fn migrate_local_root(&self) -> Result<RootSnapshot> {
        let empty = BTreeSet::new();
        self.transact_inner_scoped(
            None,
            PublishOrigin::Migration,
            PublicationScope::LocalMigration,
            |root, index| {
                ensure!(
                    matches!(root.version, 1 | 2 | 4 | 5 | types::LOCAL_ROOT_VERSION),
                    "unsupported authority root version for local migration"
                );
                if let Some(catalog) = &root.ownership {
                    // Dropping an ownership catalog loses the platform's idempotency
                    // answers. Local mode never creates one, so a non-empty ledger means
                    // this store belongs to the platform and must not be migrated.
                    ensure!(
                        catalog.commands.is_empty(),
                        "local migration cannot discard ownership command receipts"
                    );
                }
                root.ownership = None;
                if !types::supports_paged_receipts(root.version) {
                    ensure!(
                        root.receipt_index.is_none(),
                        "legacy root already carries a receipt archive"
                    );
                    let entries = std::mem::take(&mut root.receipts)
                        .into_iter()
                        .map(|(identifier, revision)| (identifier, serde_json::json!(revision)))
                        .collect();
                    root.receipt_index = index.build(entries)?;
                }
                root.version = types::LOCAL_ROOT_VERSION;
                self.apply_pin_upgrade(root)?;
                ensure!(
                    root.receipts.len() <= INLINE_RECEIPTS_MAX,
                    "inline receipt window exceeded during local migration"
                );
                Ok(())
            },
            &empty,
            &[],
        )
    }

    /// One-time move from the hash-ordered schema-1 pin tree to the raw-ordered schema-2
    /// layout. Every existing entry becomes generation zero; the generation tables are
    /// filled so a later release can address one pin's range directly.
    ///
    /// The already-verified root is passed in: taking a fresh `snapshot()` here would
    /// re-read and re-validate every branch reference at boot, which is exactly the extra
    /// remote read the load budget cannot afford.
    fn upgrade_pin_layout(&self, root: &CatalogRoot) -> Result<()> {
        if root.pin_schema == Some(types::PIN_SCHEMA_VERSION) {
            return Ok(());
        }
        self.transact_with_origin(None, PublishOrigin::PinUpgrade, |root| {
            ensure!(
                root.pin_schema != Some(types::PIN_SCHEMA_VERSION),
                "reader pin layout already upgraded"
            );
            self.apply_pin_upgrade(root)
        })?;
        Ok(())
    }

    /// Rewrite the pin registry into the bucketed generation layout. Runs inside an
    /// enclosing publication so its pages join that group; the caller decides whether the
    /// publication is the standalone pin upgrade or the merged local migration.
    fn apply_pin_upgrade(&self, root: &mut CatalogRoot) -> Result<()> {
        if root.pin_schema == Some(types::PIN_SCHEMA_VERSION) {
            return Ok(());
        }
        let mut entries = BTreeMap::new();
        let mut generations = root.pin_generations.clone();
        for (key, entry) in root.pin_entries.entries()? {
            let parts = types::pin_entry_parts(&key).context("invalid reader pin entry key")?;
            let current = generations.entry(parts.pin.to_owned()).or_insert(0);
            *current = (*current).max(parts.generation);
            let upgraded = types::pin_entry_key(parts.pin, parts.generation, parts.blob);
            ensure!(
                entries.insert(upgraded, entry).is_none(),
                "duplicate reader pin entry during upgrade"
            );
        }
        for pin in root.pins.keys() {
            generations.entry(pin.clone()).or_insert(0);
        }
        let mut registry =
            registry::Registry::inline_with_order(entries, paged_index::KeyOrder::Bucketed);
        registry.attach(self.store.clone(), self.namespace.clone());
        root.pin_entries = registry;
        root.pin_generations = generations;
        root.pin_schema = Some(types::PIN_SCHEMA_VERSION);
        Ok(())
    }

    /// Cheap late-arrival probe for sweep. A completed tombstone keeps its identity in the
    /// catalog; this only answers whether the physical key exists again, without the deep
    /// reachability walk a pending deletion needs.
    pub(crate) fn tombstone_object_present(&self, blob: &str) -> Result<bool> {
        let key = self.physical_key(blob)?;
        Ok(self.store.get(&key)?.is_some())
    }

    pub fn delete_tombstone(&self, blob: &str) -> Result<()> {
        let snapshot = self.snapshot()?;
        let tombstone = snapshot
            .root
            .tombstones
            .get(blob)?
            .context("physical deletion requires persisted tombstone")?;
        self.delete_tombstones(
            &[(blob.to_owned(), tombstone)],
            snapshot.root.revision,
            PublishOrigin::Gc,
        )?;
        Ok(())
    }

    /// Complete deletion for a batch planned from `expected_revision`.
    ///
    /// The caller must hold the namespace admission barrier. Session publications take a
    /// read ticket, so while the barrier is held the reachable set cannot gain a reference.
    /// That is what makes sharing one deep walk across the whole batch safe, instead of
    /// paying a full verification per tombstone. The batch still re-checks every identity
    /// and the frontier revision before the physical delete, and the metadata transaction is
    /// conditional on the same revision, so a plan made against an older head cannot complete.
    pub(crate) fn delete_tombstones(
        &self,
        targets: &[(String, Tombstone)],
        expected_revision: u64,
        origin: PublishOrigin,
    ) -> Result<TombstoneDeletion> {
        self.check(MutationStage::Delete)?;
        if targets.is_empty() {
            return Ok(TombstoneDeletion::default());
        }
        let snapshot = self.snapshot()?;
        ensure!(
            snapshot.root.revision == expected_revision,
            PreconditionFailed
        );
        let reachable = self.reachable(&snapshot.root)?;
        let mut reap = Vec::new();
        let mut pending = Vec::new();
        for (blob, expected) in targets {
            let current = snapshot
                .root
                .tombstones
                .get(blob)?
                .context("physical deletion requires persisted tombstone")?;
            ensure!(
                current.entry == expected.entry
                    && current.marked_ms == expected.marked_ms
                    && current.deleted == expected.deleted,
                "tombstone instance changed"
            );
            ensure!(
                !snapshot.root.uploads.contains(blob)?,
                "unresolved upload intent prevents deletion"
            );
            ensure!(
                !reachable.contains(blob),
                "reachable blob cannot be deleted"
            );
            let key = self.physical_key(blob)?;
            if expected.deleted {
                // Completed tombstones stay in the catalog as the evidence that this
                // identity must not come back. They are only touched when a late writer
                // actually recreated the physical key.
                if self.store.get(&key)?.is_some() {
                    reap.push(key);
                }
            } else {
                pending.push((blob.clone(), expected.clone(), key));
            }
        }
        self.check(MutationStage::Delete)?;
        for key in &reap {
            self.delete_physical(key)?;
        }
        for (_, _, key) in &pending {
            self.delete_physical(key)?;
        }
        if pending.is_empty() {
            return Ok(TombstoneDeletion {
                completed: 0,
                reaped: reap.len(),
            });
        }
        let deletions: BTreeSet<String> = pending.iter().map(|(blob, _, _)| blob.clone()).collect();
        self.transact_inner(
            Some(expected_revision),
            origin,
            |root| {
                for (blob, expected, _) in &pending {
                    root.tombstones.update(blob, |tombstone| {
                        ensure!(
                            tombstone.entry == expected.entry
                                && tombstone.marked_ms == expected.marked_ms
                                && !tombstone.deleted,
                            "tombstone instance changed"
                        );
                        tombstone.deleted = true;
                        Ok(())
                    })?;
                    ensure!(
                        !root.uploads.contains(blob)?,
                        "unresolved upload intent prevents deletion"
                    );
                    root.blobs.remove(blob)?;
                }
                Ok(())
            },
            &deletions,
            &[],
        )?;
        Ok(TombstoneDeletion {
            completed: pending.len(),
            reaped: reap.len(),
        })
    }

    fn delete_physical(&self, key: &str) -> Result<()> {
        if let Err(error) = self.store.delete(key, None) {
            self.uncertain(MutationStage::Delete, &error);
            if self.store.get(key)?.is_some() {
                return Err(error);
            }
        }
        Ok(())
    }
}

/// Bounded immutable pages of the recovery index. A point is stored once in its timeline
/// and every directory level carries only summaries, so resolving a commit costs the tree
/// height rather than the number of retained commits.
const RECOVERY_PAGE_BYTES: usize = 16 * 1024;
const RECOVERY_LEAF_POINTS: usize = 12;
const RECOVERY_BRANCH_CHILDREN: usize = 48;

thread_local! {
    /// Recovery pages produced by the mutation currently running on this thread. The
    /// enclosing publication takes them into its durability group, so a commit still
    /// publishes one group and the head CAS stays the last write.
    static RECOVERY_PAGE_STAGING: RefCell<Vec<Rc<RefCell<Vec<crate::object_store::GroupWrite>>>>> =
        const { RefCell::new(Vec::new()) };
}

struct RecoveryStagingScope;

impl RecoveryStagingScope {
    fn enter(buffer: Rc<RefCell<Vec<crate::object_store::GroupWrite>>>) -> Self {
        RECOVERY_PAGE_STAGING.with(|stack| stack.borrow_mut().push(buffer));
        Self
    }
}

impl Drop for RecoveryStagingScope {
    fn drop(&mut self) {
        RECOVERY_PAGE_STAGING.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

fn staged_recovery_page(hash: &str) -> Option<Vec<u8>> {
    RECOVERY_PAGE_STAGING.with(|stack| {
        let stack = stack.borrow();
        let buffer = stack.last()?;
        let bytes = buffer
            .borrow()
            .iter()
            .rev()
            .find(|write| write.key.ends_with(hash))
            .map(|write| write.bytes.clone());
        bytes
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RecoveryPage {
    Leaf {
        version: u8,
        depth: u8,
        points: Vec<RecoveryPoint>,
    },
    Branch {
        version: u8,
        depth: u8,
        children: Vec<RecoveryChild>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RecoveryChild {
    hash: String,
    count: u64,
    bytes: u64,
    oldest_sequence: u64,
    newest_sequence: u64,
    oldest_timestamp_ms: u64,
    newest_timestamp_ms: u64,
}

impl RecoveryChild {
    fn covers_sequence(&self, sequence: u64) -> bool {
        (self.oldest_sequence..=self.newest_sequence).contains(&sequence)
    }
}

enum RecoveryAppend {
    Single(RecoveryChild),
    Split(RecoveryChild, RecoveryChild),
}

fn recovery_point_bytes(point: &RecoveryPoint) -> Result<u64> {
    u64::try_from(serde_json::to_vec(point)?.len()).context("recovery point size overflow")
}

fn recovery_leaf_child(hash: String, points: &[RecoveryPoint]) -> Result<RecoveryChild> {
    ensure!(!points.is_empty(), "empty recovery leaf");
    let mut bytes = 0u64;
    let mut previous: Option<&RecoveryPoint> = None;
    for point in points {
        if let Some(previous) = previous {
            ensure!(
                point.sequence > previous.sequence,
                "recovery leaf sequence is not strictly increasing"
            );
            ensure!(
                point.timestamp_ms >= previous.timestamp_ms,
                "recovery leaf timestamp rewinds"
            );
        }
        previous = Some(point);
        bytes = bytes
            .checked_add(recovery_point_bytes(point)?)
            .context("recovery leaf byte overflow")?;
    }
    Ok(RecoveryChild {
        hash,
        count: u64::try_from(points.len()).context("recovery leaf count overflow")?,
        bytes,
        oldest_sequence: points[0].sequence,
        newest_sequence: points[points.len() - 1].sequence,
        oldest_timestamp_ms: points[0].timestamp_ms,
        newest_timestamp_ms: points[points.len() - 1].timestamp_ms,
    })
}

fn recovery_branch_child(hash: String, children: &[RecoveryChild]) -> Result<RecoveryChild> {
    ensure!(
        !children.is_empty() && children.len() <= RECOVERY_BRANCH_CHILDREN,
        "recovery branch child count is out of range"
    );
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut previous: Option<&RecoveryChild> = None;
    for child in children {
        ensure!(
            crate::manifest::valid_hash(&child.hash),
            "recovery child is not a page identity"
        );
        ensure!(child.count > 0 && child.bytes > 0, "empty recovery child");
        ensure!(
            child.oldest_sequence <= child.newest_sequence
                && child.oldest_timestamp_ms <= child.newest_timestamp_ms,
            "recovery child bounds are inverted"
        );
        if let Some(previous) = previous {
            ensure!(
                child.oldest_sequence > previous.newest_sequence,
                "recovery children overlap in sequence order"
            );
            ensure!(
                child.oldest_timestamp_ms >= previous.newest_timestamp_ms,
                "recovery children rewind timestamp order"
            );
        }
        previous = Some(child);
        count = count
            .checked_add(child.count)
            .context("recovery branch count overflow")?;
        bytes = bytes
            .checked_add(child.bytes)
            .context("recovery branch byte overflow")?;
    }
    Ok(RecoveryChild {
        hash,
        count,
        bytes,
        oldest_sequence: children[0].oldest_sequence,
        newest_sequence: children[children.len() - 1].newest_sequence,
        oldest_timestamp_ms: children[0].oldest_timestamp_ms,
        newest_timestamp_ms: children[children.len() - 1].newest_timestamp_ms,
    })
}

fn recovery_page_summary(page: &RecoveryPage, depth: u8) -> Result<RecoveryChild> {
    match page {
        RecoveryPage::Leaf {
            version,
            depth: stored,
            points,
        } => {
            ensure!(
                *version == 1 && *stored == depth && depth == 0,
                "invalid recovery leaf header"
            );
            // The hash is filled in by the writer; validation of the digest happens on read.
            recovery_leaf_child(String::new(), points)
        }
        RecoveryPage::Branch {
            version,
            depth: stored,
            children,
        } => {
            ensure!(
                *version == 1 && *stored == depth && depth > 0,
                "invalid recovery branch header"
            );
            recovery_branch_child(String::new(), children)
        }
    }
}

impl Authority {
    fn recovery_page_key(&self, hash: &str) -> Result<String> {
        ensure!(
            crate::manifest::valid_hash(hash),
            "invalid recovery index page identity"
        );
        self.namespace
            .key(&format!("authority/recovery-index/{hash}"))
    }

    fn read_recovery_page(&self, hash: &str, depth: u8) -> Result<(RecoveryPage, RecoveryChild)> {
        let key = self.recovery_page_key(hash)?;
        let bytes = match staged_recovery_page(hash) {
            Some(bytes) => bytes,
            None => {
                self.store
                    .get(&key)?
                    .context("recovery index page missing")?
                    .bytes
            }
        };
        ensure!(
            bytes.len() <= RECOVERY_PAGE_BYTES && blake3::hash(&bytes).to_hex().as_str() == hash,
            "recovery index page digest or size mismatch"
        );
        let page: RecoveryPage = serde_json::from_slice(&bytes)?;
        let mut summary = recovery_page_summary(&page, depth)?;
        summary.hash = hash.to_owned();
        Ok((page, summary))
    }

    fn write_recovery_page(
        &self,
        page: &RecoveryPage,
        depth: u8,
    ) -> Result<(String, RecoveryChild)> {
        let bytes = serde_json::to_vec(page)?;
        ensure!(
            bytes.len() <= RECOVERY_PAGE_BYTES,
            "recovery index page byte budget exceeded"
        );
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let key = self.recovery_page_key(&hash)?;
        let staged = RECOVERY_PAGE_STAGING.with(|stack| stack.borrow().last().cloned());
        if let Some(staged) = staged {
            // The enclosing publication writes this body before the head CAS. Content
            // addressing makes a retry idempotent, so no condition is needed here.
            staged
                .borrow_mut()
                .push(crate::object_store::GroupWrite::new(
                    key,
                    bytes,
                    PutCondition::Any,
                ));
        } else if self.store.put(&key, &bytes, PutCondition::Absent).is_err() {
            // Content addressing makes a lost race harmless, but the existing body must be
            // the one this attempt produced; otherwise the index would point at a page that
            // describes different points.
            let existing = self
                .store
                .get(&key)?
                .context("recovery index page upload unresolved")?;
            ensure!(
                existing.bytes == bytes,
                "immutable recovery index page conflict"
            );
        }
        let mut summary = recovery_page_summary(page, depth)?;
        summary.hash = hash.clone();
        Ok((hash, summary))
    }

    fn append_recovery_at(
        &self,
        hash: &str,
        depth: u8,
        point: &RecoveryPoint,
    ) -> Result<RecoveryAppend> {
        let (page, summary) = self.read_recovery_page(hash, depth)?;
        ensure!(
            point.sequence > summary.newest_sequence,
            "recovery append is not sequence ordered"
        );
        ensure!(
            point.timestamp_ms >= summary.newest_timestamp_ms,
            "recovery append rewinds time order"
        );
        match page {
            RecoveryPage::Leaf { points, .. } => {
                let fits_count = points.len() < RECOVERY_LEAF_POINTS;
                let mut candidate = points.clone();
                candidate.push(point.clone());
                let candidate_page = RecoveryPage::Leaf {
                    version: 1,
                    depth,
                    points: candidate,
                };
                if fits_count && serde_json::to_vec(&candidate_page)?.len() <= RECOVERY_PAGE_BYTES {
                    return Ok(RecoveryAppend::Single(
                        self.write_recovery_page(&candidate_page, depth)?.1,
                    ));
                }
                let left = RecoveryPage::Leaf {
                    version: 1,
                    depth,
                    points,
                };
                let right = RecoveryPage::Leaf {
                    version: 1,
                    depth,
                    points: vec![point.clone()],
                };
                let left = self.write_recovery_page(&left, depth)?.1;
                let right = self.write_recovery_page(&right, depth)?.1;
                Ok(RecoveryAppend::Split(left, right))
            }
            RecoveryPage::Branch { mut children, .. } => {
                let last = children.pop().context("empty recovery branch")?;
                match self.append_recovery_at(&last.hash, depth - 1, point)? {
                    RecoveryAppend::Single(child) => {
                        children.push(child);
                        self.finish_recovery_branch(children, depth)
                    }
                    RecoveryAppend::Split(left, right) => {
                        children.push(left);
                        children.push(right);
                        self.finish_recovery_branch(children, depth)
                    }
                }
            }
        }
    }

    fn finish_recovery_branch(
        &self,
        children: Vec<RecoveryChild>,
        depth: u8,
    ) -> Result<RecoveryAppend> {
        let page = RecoveryPage::Branch {
            version: 1,
            depth,
            children: children.clone(),
        };
        let fits = children.len() <= RECOVERY_BRANCH_CHILDREN
            && serde_json::to_vec(&page)?.len() <= RECOVERY_PAGE_BYTES;
        if fits {
            return Ok(RecoveryAppend::Single(
                self.write_recovery_page(&page, depth)?.1,
            ));
        }
        ensure!(
            children.len() >= 2,
            "recovery branch cannot be split into non-empty halves"
        );
        let middle = children.len() / 2;
        let (left_children, right_children) = children.split_at(middle);
        let left = RecoveryPage::Branch {
            version: 1,
            depth,
            children: left_children.to_vec(),
        };
        let right = RecoveryPage::Branch {
            version: 1,
            depth,
            children: right_children.to_vec(),
        };
        let left = self.write_recovery_page(&left, depth)?.1;
        let right = self.write_recovery_page(&right, depth)?.1;
        Ok(RecoveryAppend::Split(left, right))
    }
}

impl Authority {
    fn append_recovery_point_to_timeline(
        &self,
        state: &mut RecoveryTimelineIndex,
        point: &RecoveryPoint,
    ) -> Result<()> {
        ensure!(point.timeline > 0, "recovery point timeline is zero");
        if state.is_empty() {
            ensure!(
                state.root.is_none()
                    && state.depth == 0
                    && state.bytes == 0
                    && state.oldest_sequence == 0
                    && state.newest_sequence == 0
                    && state.oldest_timestamp_ms == 0
                    && state.newest_timestamp_ms == 0,
                "empty recovery timeline carries state"
            );
            let page = RecoveryPage::Leaf {
                version: 1,
                depth: 0,
                points: vec![point.clone()],
            };
            let (_, child) = self.write_recovery_page(&page, 0)?;
            apply_recovery_child(state, 0, &child);
            return Ok(());
        }
        ensure!(
            point.sequence > state.newest_sequence,
            "recovery point sequence is not append-only"
        );
        ensure!(
            point.timestamp_ms >= state.newest_timestamp_ms,
            "recovery point timestamp rewinds"
        );
        let root = state
            .root
            .clone()
            .context("recovery timeline root missing")?;
        let depth = state.depth;
        match self.append_recovery_at(&root, depth, point)? {
            RecoveryAppend::Single(child) => {
                apply_recovery_child(state, depth, &child);
            }
            RecoveryAppend::Split(left, right) => {
                ensure!(
                    depth < types::MAX_RECOVERY_INDEX_DEPTH,
                    "recovery index depth exhausted"
                );
                let page = RecoveryPage::Branch {
                    version: 1,
                    depth: depth + 1,
                    children: vec![left, right],
                };
                let (_, child) = self.write_recovery_page(&page, depth + 1)?;
                apply_recovery_child(state, depth + 1, &child);
            }
        }
        Ok(())
    }

    pub(crate) fn build_recovery_index(&self, history: &[RecoveryPoint]) -> Result<RecoveryIndex> {
        let mut index = RecoveryIndex::empty(self.recovery_retention.clone());
        for point in history {
            let state = index.timelines.entry(point.timeline).or_default();
            self.append_recovery_point_to_timeline(state, point)
                .with_context(|| {
                    format!(
                        "recovery history cannot be indexed at sequence {}",
                        point.sequence
                    )
                })?;
        }
        refresh_recovery_aggregate(&mut index)?;
        Ok(index)
    }

    fn trim_recovery(
        &self,
        branch_name: &str,
        branch: &mut BranchRef,
        snapshots: &BTreeMap<String, RecoveryPoint>,
        explicit_min_sequence: u64,
    ) -> Result<(u64, u64)> {
        let Some(index) = branch.recovery.as_mut() else {
            return Ok((explicit_min_sequence, 0));
        };
        index.policy.validate()?;
        let mut min_sequence = explicit_min_sequence;
        let mut min_timestamp = match index.policy.mode {
            RetentionMode::TimeWindow { retain_ms } => {
                index.newest_timestamp_ms.saturating_sub(retain_ms)
            }
            RetentionMode::SequenceWindow { .. } => 0,
        };
        if index.policy.anchor_snapshots {
            for snapshot in snapshots.values() {
                if snapshot.branch == branch_name && snapshot.sequence > 0 {
                    min_timestamp = min_timestamp.min(snapshot.timestamp_ms);
                }
            }
        }
        let timelines: Vec<u64> = index.timelines.keys().copied().collect();
        for timeline in timelines {
            let state = index
                .timelines
                .get_mut(&timeline)
                .context("recovery timeline disappeared")?;
            self.trim_recovery_timeline(state, min_sequence, min_timestamp)?;
            if state.is_empty() {
                index.timelines.remove(&timeline);
            }
        }
        if let RetentionMode::SequenceWindow { points } = index.policy.mode {
            let points = u64::from(points);
            let mut excess = index.count.saturating_sub(points);
            let mut ordered: Vec<u64> = index.timelines.keys().copied().collect();
            ordered.sort_by_key(|timeline| index.timelines[timeline].oldest_sequence);
            for timeline in ordered {
                if excess == 0 {
                    break;
                }
                let Some(state) = index.timelines.get(&timeline) else {
                    continue;
                };
                if state.count <= excess {
                    excess -= state.count;
                    min_sequence = min_sequence.max(
                        state
                            .newest_sequence
                            .checked_add(1)
                            .context("recovery sequence overflow")?,
                    );
                    index.timelines.remove(&timeline);
                    continue;
                }
                let rank = excess - 1;
                let threshold = self
                    .select_recovery_sequence(state, rank)?
                    .context("recovery rank selector missed a retained point")?;
                let keep_from = threshold
                    .checked_add(1)
                    .context("recovery sequence overflow")?;
                min_sequence = min_sequence.max(keep_from);
                let state = index
                    .timelines
                    .get_mut(&timeline)
                    .context("recovery timeline disappeared")?;
                self.trim_recovery_timeline(state, min_sequence, min_timestamp)?;
                if state.is_empty() {
                    index.timelines.remove(&timeline);
                }
                excess = 0;
            }
        }
        refresh_recovery_aggregate(index)?;
        if index.bytes > index.policy.max_bytes {
            return Err(anyhow::Error::new(types::RetentionCapacityExceeded {
                branch: branch_name.to_owned(),
                retained_bytes: index.bytes,
                max_bytes: index.policy.max_bytes,
                oldest_sequence: (index.count > 0).then_some(index.oldest_sequence),
                newest_sequence: (index.count > 0).then_some(index.newest_sequence),
                oldest_timestamp_ms: (index.count > 0).then_some(index.oldest_timestamp_ms),
                newest_timestamp_ms: (index.count > 0).then_some(index.newest_timestamp_ms),
            }));
        }
        Ok((min_sequence, min_timestamp))
    }

    fn trim_recovery_timeline(
        &self,
        state: &mut RecoveryTimelineIndex,
        min_sequence: u64,
        min_timestamp: u64,
    ) -> Result<()> {
        let Some(root) = state.root.clone() else {
            return Ok(());
        };
        match self.trim_recovery_at(&root, state.depth, min_sequence, min_timestamp)? {
            None => {
                *state = RecoveryTimelineIndex::default();
                return Ok(());
            }
            Some(child) => apply_recovery_child(state, state.depth, &child),
        }
        // Collapse a root that the trim reduced to a single child. Mid-tree branches keep
        // their shape; only the root can change the tree height without breaking the
        // uniform-depth traversal.
        loop {
            if state.depth == 0 {
                break;
            }
            let root = state.root.clone().context("recovery root disappeared")?;
            let (page, _) = self.read_recovery_page(&root, state.depth)?;
            match page {
                RecoveryPage::Branch { children, .. } if children.len() == 1 => {
                    let child = children.into_iter().next().expect("single child");
                    state.root = Some(child.hash);
                    state.depth -= 1;
                }
                _ => break,
            }
        }
        Ok(())
    }

    fn trim_recovery_at(
        &self,
        hash: &str,
        depth: u8,
        min_sequence: u64,
        min_timestamp: u64,
    ) -> Result<Option<RecoveryChild>> {
        let (page, summary) = self.read_recovery_page(hash, depth)?;
        if summary.newest_sequence < min_sequence || summary.newest_timestamp_ms < min_timestamp {
            return Ok(None);
        }
        if summary.oldest_sequence >= min_sequence && summary.oldest_timestamp_ms >= min_timestamp {
            return Ok(Some(summary));
        }
        match page {
            RecoveryPage::Leaf { mut points, .. } => {
                points.retain(|point| {
                    point.sequence >= min_sequence && point.timestamp_ms >= min_timestamp
                });
                if points.is_empty() {
                    return Ok(None);
                }
                let page = RecoveryPage::Leaf {
                    version: 1,
                    depth,
                    points,
                };
                Ok(Some(self.write_recovery_page(&page, depth)?.1))
            }
            RecoveryPage::Branch { children, .. } => {
                let mut kept = Vec::new();
                for child in children {
                    if child.newest_sequence < min_sequence
                        || child.newest_timestamp_ms < min_timestamp
                    {
                        continue;
                    }
                    if child.oldest_sequence >= min_sequence
                        && child.oldest_timestamp_ms >= min_timestamp
                    {
                        kept.push(child);
                        continue;
                    }
                    if let Some(trimmed) =
                        self.trim_recovery_at(&child.hash, depth - 1, min_sequence, min_timestamp)?
                    {
                        kept.push(trimmed);
                    }
                }
                if kept.is_empty() {
                    return Ok(None);
                }
                let page = RecoveryPage::Branch {
                    version: 1,
                    depth,
                    children: kept,
                };
                Ok(Some(self.write_recovery_page(&page, depth)?.1))
            }
        }
    }

    fn select_recovery_sequence(
        &self,
        state: &RecoveryTimelineIndex,
        rank: u64,
    ) -> Result<Option<u64>> {
        if rank >= state.count {
            return Ok(None);
        }
        let Some(root) = state.root.as_deref() else {
            return Ok(None);
        };
        self.select_recovery_sequence_at(root, state.depth, rank)
    }

    fn select_recovery_sequence_at(&self, hash: &str, depth: u8, rank: u64) -> Result<Option<u64>> {
        let (page, _) = self.read_recovery_page(hash, depth)?;
        match page {
            RecoveryPage::Leaf { points, .. } => Ok(points
                .get(usize::try_from(rank).context("recovery rank overflow")?)
                .map(|point| point.sequence)),
            RecoveryPage::Branch { children, .. } => {
                let mut remaining = rank;
                for child in children {
                    if remaining < child.count {
                        return self.select_recovery_sequence_at(&child.hash, depth - 1, remaining);
                    }
                    remaining -= child.count;
                }
                Ok(None)
            }
        }
    }

    fn lookup_recovery_in_timeline(
        &self,
        timeline: u64,
        state: &RecoveryTimelineIndex,
        sequence: u64,
    ) -> Result<Option<RecoveryPoint>> {
        if state.is_empty() || !(state.oldest_sequence..=state.newest_sequence).contains(&sequence)
        {
            return Ok(None);
        }
        let mut hash = state.root.clone().context("recovery root missing")?;
        let mut depth = state.depth;
        loop {
            let (page, _) = self.read_recovery_page(&hash, depth)?;
            match page {
                RecoveryPage::Leaf { points, .. } => {
                    return Ok(points
                        .into_iter()
                        .find(|point| point.sequence == sequence && point.timeline == timeline));
                }
                RecoveryPage::Branch { children, .. } => {
                    let Some(child) = children
                        .iter()
                        .find(|child| child.covers_sequence(sequence))
                    else {
                        return Ok(None);
                    };
                    hash = child.hash.clone();
                    ensure!(depth > 0, "recovery tree depth exhausted");
                    depth -= 1;
                }
            }
        }
    }

    fn predecessor_recovery_at(
        &self,
        hash: &str,
        depth: u8,
        timestamp_ms: u64,
    ) -> Result<Option<RecoveryPoint>> {
        let (page, _) = self.read_recovery_page(hash, depth)?;
        match page {
            RecoveryPage::Leaf { points, .. } => Ok(points
                .into_iter()
                .rev()
                .find(|point| point.timestamp_ms <= timestamp_ms)),
            RecoveryPage::Branch { children, .. } => {
                let Some(mut selected) = children
                    .iter()
                    .rposition(|child| child.oldest_timestamp_ms <= timestamp_ms)
                else {
                    return Ok(None);
                };
                loop {
                    let child = &children[selected];
                    ensure!(depth > 0, "recovery tree depth exhausted");
                    if let Some(point) =
                        self.predecessor_recovery_at(&child.hash, depth - 1, timestamp_ms)?
                    {
                        return Ok(Some(point));
                    }
                    if selected == 0 {
                        return Ok(None);
                    }
                    selected -= 1;
                }
            }
        }
    }

    fn lookup_recovery_time_in_timeline(
        &self,
        state: &RecoveryTimelineIndex,
        timestamp_ms: u64,
    ) -> Result<Option<RecoveryPoint>> {
        if state.is_empty() || state.oldest_timestamp_ms > timestamp_ms {
            return Ok(None);
        }
        let root = state.root.as_deref().context("recovery root missing")?;
        self.predecessor_recovery_at(root, state.depth, timestamp_ms)
    }

    /// All indexed and hot points that match an exact sequence. Hot entries override the
    /// indexed copy of the same timeline/sequence so an in-memory view stays authoritative.
    pub(crate) fn recovery_lookup_sequence(
        &self,
        branch: &BranchRef,
        sequence: u64,
    ) -> Result<Vec<RecoveryPoint>> {
        let mut found = BTreeMap::new();
        if let Some(index) = &branch.recovery {
            for (timeline, state) in &index.timelines {
                if let Some(point) = self.lookup_recovery_in_timeline(*timeline, state, sequence)? {
                    found.insert((*timeline, sequence), point);
                }
            }
        }
        for point in &branch.history {
            if point.sequence == sequence {
                found.insert((point.timeline, sequence), point.clone());
            }
        }
        Ok(found.into_values().collect())
    }

    /// All points that are the latest commit at or before a timestamp, one per timeline.
    pub(crate) fn recovery_lookup_time(
        &self,
        branch: &BranchRef,
        timestamp_ms: u64,
    ) -> Result<Vec<RecoveryPoint>> {
        let mut found = BTreeMap::new();
        if let Some(index) = &branch.recovery {
            for (timeline, state) in &index.timelines {
                if let Some(point) = self.lookup_recovery_time_in_timeline(state, timestamp_ms)? {
                    found.insert((*timeline, point.sequence), point);
                }
            }
        }
        for point in &branch.history {
            if point.timestamp_ms <= timestamp_ms {
                found.insert((point.timeline, point.sequence), point.clone());
            }
        }
        Ok(found.into_values().collect())
    }

    /// One bounded page of a timeline's points. Traversal skips whole subtrees that end at
    /// or before `after_sequence`, so the cost is the tree height plus the returned page,
    /// not the number of retained commits.
    pub(crate) fn recovery_range(
        &self,
        branch: &BranchRef,
        timeline: u64,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RecoveryPoint>> {
        let mut points = Vec::new();
        if let Some(index) = &branch.recovery {
            if let Some(state) = index.timelines.get(&timeline) {
                if let Some(root) = state.root.as_deref() {
                    self.collect_recovery_range(
                        root,
                        state.depth,
                        timeline,
                        after_sequence,
                        limit,
                        &mut points,
                    )?;
                }
            }
        }
        if branch.recovery.is_none() {
            points.extend(
                branch
                    .history
                    .iter()
                    .filter(|point| point.timeline == timeline && point.sequence > after_sequence)
                    .take(limit)
                    .cloned(),
            );
        }
        Ok(points)
    }

    fn collect_recovery_range(
        &self,
        hash: &str,
        depth: u8,
        timeline: u64,
        after_sequence: u64,
        limit: usize,
        points: &mut Vec<RecoveryPoint>,
    ) -> Result<()> {
        if points.len() >= limit {
            return Ok(());
        }
        let (page, summary) = self.read_recovery_page(hash, depth)?;
        if summary.newest_sequence <= after_sequence {
            return Ok(());
        }
        match page {
            RecoveryPage::Leaf { points: leaf, .. } => {
                for point in leaf {
                    if point.sequence > after_sequence && point.timeline == timeline {
                        points.push(point);
                        if points.len() >= limit {
                            break;
                        }
                    }
                }
            }
            RecoveryPage::Branch { children, .. } => {
                for child in children {
                    if child.newest_sequence <= after_sequence {
                        continue;
                    }
                    self.collect_recovery_range(
                        &child.hash,
                        depth - 1,
                        timeline,
                        after_sequence,
                        limit,
                        points,
                    )?;
                    if points.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn walk_recovery_timeline(
        &self,
        hash: &str,
        depth: u8,
        timeline: u64,
        visit: &mut impl FnMut(&RecoveryPoint) -> Result<()>,
    ) -> Result<RecoveryChild> {
        let (page, summary) = self.read_recovery_page(hash, depth)?;
        match page {
            RecoveryPage::Leaf { points, .. } => {
                for point in &points {
                    ensure!(
                        point.timeline == timeline,
                        "recovery point escaped its timeline index"
                    );
                    visit(point)?;
                }
                Ok(summary)
            }
            RecoveryPage::Branch { children, .. } => {
                for child in &children {
                    let actual =
                        self.walk_recovery_timeline(&child.hash, depth - 1, timeline, visit)?;
                    ensure!(
                        actual.count == child.count
                            && actual.bytes == child.bytes
                            && actual.oldest_sequence == child.oldest_sequence
                            && actual.newest_sequence == child.newest_sequence
                            && actual.oldest_timestamp_ms == child.oldest_timestamp_ms
                            && actual.newest_timestamp_ms == child.newest_timestamp_ms,
                        "recovery child summary disagrees with its page"
                    );
                }
                Ok(summary)
            }
        }
    }

    /// Metadata-only guard used by boot and collection: every page is digest-checked, every
    /// summary is re-derived from its page, and every point's HEAD/WAL dependencies are
    /// resolved so a corrupt index cannot become the only copy of a restorable commit.
    fn verify_recovery_indexes(&self, root: &CatalogRoot) -> Result<()> {
        for branch in root.branches.values() {
            let Some(index) = &branch.recovery else {
                continue;
            };
            index.validate_shape()?;
            for (timeline, state) in &index.timelines {
                if state.is_empty() {
                    continue;
                }
                let root_hash = state.root.as_deref().context("recovery root missing")?;
                let summary =
                    self.walk_recovery_timeline(root_hash, state.depth, *timeline, &mut |point| {
                        // Resolve every dependency so a missing manifest, chunk or receipt
                        // page fails boot instead of surfacing only when someone restores.
                        branch::referenced(self, root, point.head.as_ref(), point.wal.as_ref())?;
                        Ok(())
                    })?;
                ensure!(
                    summary.count == state.count
                        && summary.bytes == state.bytes
                        && summary.oldest_sequence == state.oldest_sequence
                        && summary.newest_sequence == state.newest_sequence
                        && summary.oldest_timestamp_ms == state.oldest_timestamp_ms
                        && summary.newest_timestamp_ms == state.newest_timestamp_ms,
                    "recovery timeline summary disagrees with its pages"
                );
            }
            // The hot window is a cache of index entries, never an independent source of
            // truth. A document whose cache names a point the index does not contain is a
            // corrupt or unsupported shape and must not be guessed at.
            for hot in &branch.history {
                let state = index
                    .timelines
                    .get(&hot.timeline)
                    .context("recovery hot point has no indexed timeline")?;
                let found = self.lookup_recovery_in_timeline(hot.timeline, state, hot.sequence)?;
                ensure!(
                    found.is_some_and(|point| point.id == hot.id && point.sequence == hot.sequence),
                    "recovery hot window is not covered by the index"
                );
            }
        }
        Ok(())
    }

    fn extend_reachable_with_recovery(
        &self,
        root: &CatalogRoot,
        reachable: &mut BTreeSet<String>,
    ) -> Result<()> {
        for branch in root.branches.values() {
            let Some(index) = &branch.recovery else {
                continue;
            };
            for (timeline, state) in &index.timelines {
                if state.is_empty() {
                    continue;
                }
                let root_hash = state.root.as_deref().context("recovery root missing")?;
                self.walk_recovery_timeline(root_hash, state.depth, *timeline, &mut |point| {
                    let entries =
                        branch::referenced(self, root, point.head.as_ref(), point.wal.as_ref())?;
                    for entry in entries.values() {
                        reachable.insert(entry.blob.clone());
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    }

    /// Publish one committed recovery point and apply the branch's committed policy. A
    /// capacity failure aborts the enclosing transaction, so the new commit is refused
    /// instead of shortening a window the branch already promised.
    pub(crate) fn record_recovery_point(
        &self,
        root: &mut CatalogRoot,
        branch_name: &str,
        point: &RecoveryPoint,
    ) -> Result<()> {
        let (branches, snapshots) = (&mut root.branches, &root.snapshots);
        let branch = branches.get_mut(branch_name).context("branch missing")?;
        if branch.recovery.is_none() {
            branch.recovery = Some(self.build_recovery_index(&branch.history)?);
        }
        let policy = {
            let index = branch
                .recovery
                .as_mut()
                .context("recovery index missing after migration")?;
            ensure!(
                index.schema == types::RECOVERY_INDEX_SCHEMA,
                "unsupported recovery index schema"
            );
            let state = index.timelines.entry(point.timeline).or_default();
            self.append_recovery_point_to_timeline(state, point)?;
            refresh_recovery_aggregate(index)?;
            index.policy.clone()
        };
        let (min_sequence, min_timestamp) =
            self.trim_recovery(branch_name, branch, snapshots, 0)?;
        branch
            .history
            .retain(|hot| hot.sequence >= min_sequence && hot.timestamp_ms >= min_timestamp);
        if !branch
            .history
            .iter()
            .any(|hot| hot.timeline == point.timeline && hot.sequence == point.sequence)
        {
            branch.history.push(point.clone());
        }
        let hot = policy.hot_points as usize;
        let excess = branch.history.len().saturating_sub(hot);
        branch.history.drain(..excess);
        Ok(())
    }

    /// Explicit operator trim. It is the only path allowed to shorten a promised window,
    /// and it trims the immutable index and the hot window together.
    pub(crate) fn retain_recovery_after(
        &self,
        root: &mut CatalogRoot,
        branch_name: &str,
        min_sequence: u64,
    ) -> Result<()> {
        let (branches, snapshots) = (&mut root.branches, &root.snapshots);
        let branch = branches.get_mut(branch_name).context("branch missing")?;
        branch
            .history
            .retain(|point| point.sequence >= min_sequence);
        if branch.recovery.is_some() {
            let (min_sequence, min_timestamp) =
                self.trim_recovery(branch_name, branch, snapshots, min_sequence)?;
            branch
                .history
                .retain(|hot| hot.sequence >= min_sequence && hot.timestamp_ms >= min_timestamp);
        }
        Ok(())
    }
}

fn apply_recovery_child(state: &mut RecoveryTimelineIndex, depth: u8, child: &RecoveryChild) {
    state.root = Some(child.hash.clone());
    state.depth = depth;
    state.count = child.count;
    state.bytes = child.bytes;
    state.oldest_sequence = child.oldest_sequence;
    state.newest_sequence = child.newest_sequence;
    state.oldest_timestamp_ms = child.oldest_timestamp_ms;
    state.newest_timestamp_ms = child.newest_timestamp_ms;
}

fn refresh_recovery_aggregate(index: &mut RecoveryIndex) -> Result<()> {
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut oldest_sequence = None;
    let mut newest_sequence = None;
    let mut oldest_timestamp = None;
    let mut newest_timestamp = None;
    for timeline in index.timelines.values() {
        if timeline.is_empty() {
            continue;
        }
        count = count
            .checked_add(timeline.count)
            .context("recovery point count overflow")?;
        bytes = bytes
            .checked_add(timeline.bytes)
            .context("recovery byte count overflow")?;
        oldest_sequence = Some(
            oldest_sequence.map_or(timeline.oldest_sequence, |value: u64| {
                value.min(timeline.oldest_sequence)
            }),
        );
        newest_sequence = Some(
            newest_sequence.map_or(timeline.newest_sequence, |value: u64| {
                value.max(timeline.newest_sequence)
            }),
        );
        oldest_timestamp = Some(
            oldest_timestamp.map_or(timeline.oldest_timestamp_ms, |value: u64| {
                value.min(timeline.oldest_timestamp_ms)
            }),
        );
        newest_timestamp = Some(
            newest_timestamp.map_or(timeline.newest_timestamp_ms, |value: u64| {
                value.max(timeline.newest_timestamp_ms)
            }),
        );
    }
    index.count = count;
    index.bytes = bytes;
    index.oldest_sequence = oldest_sequence.unwrap_or(0);
    index.newest_sequence = newest_sequence.unwrap_or(0);
    index.oldest_timestamp_ms = oldest_timestamp.unwrap_or(0);
    index.newest_timestamp_ms = newest_timestamp.unwrap_or(0);
    Ok(())
}

#[cfg(test)]
mod recovery_index_tests {
    use super::*;
    use crate::capabilities::Capabilities;
    use crate::manifest::{Head, Manifest};
    use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingStore {
        inner: MemoryObjectStore,
        index_gets: AtomicUsize,
    }

    impl ObjectStore for CountingStore {
        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            if key.contains("authority/recovery-index/") {
                self.index_gets.fetch_add(1, Ordering::SeqCst);
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

    fn authority_with(
        retention: RecoveryRetention,
    ) -> Result<(Arc<CountingStore>, Arc<Authority>)> {
        let store = Arc::new(CountingStore::default());
        let authority = Authority::open_with_retention(
            store.clone(),
            Namespace::new("recovery", "window")?,
            retention,
        )?;
        Ok((store, authority))
    }

    fn time_window(retain_ms: u64, hot_points: u32) -> RecoveryRetention {
        RecoveryRetention {
            mode: RetentionMode::TimeWindow { retain_ms },
            hot_points,
            max_bytes: 64 * 1024 * 1024,
            anchor_snapshots: true,
        }
    }

    fn commit_point(
        authority: &Arc<Authority>,
        sequence: u64,
        timestamp_ms: u64,
        head: Option<ObjectEntry>,
    ) -> Result<RecoveryPoint> {
        let mut stored = None;
        authority.transact(None, |root| {
            let branch = root.branches.get_mut("main").context("branch missing")?;
            branch.sequence = sequence;
            branch.current_sequence = sequence;
            branch.last_commit_ms = timestamp_ms;
            branch.current_commit_ms = timestamp_ms;
            let point = RecoveryPoint {
                id: blake3::hash(format!("point-{sequence}").as_bytes())
                    .to_hex()
                    .to_string(),
                branch: "main".into(),
                timeline: branch.timeline,
                sequence,
                timestamp_ms,
                head: head.clone(),
                wal: None,
            };
            authority.record_recovery_point(root, "main", &point)?;
            stored = Some(point);
            Ok(())
        })?;
        stored.context("recovery point publication missing")
    }

    #[test]
    fn time_window_keeps_commits_beyond_the_hot_window() -> Result<()> {
        let (_store, authority) = authority_with(time_window(1_000_000, 4))?;
        let capabilities = Capabilities::new(authority.clone());
        for sequence in 1..=120 {
            commit_point(&authority, sequence, sequence * 10, None)?;
        }
        let branch = authority.snapshot()?.root.branches["main"].clone();
        assert!(branch.history.len() <= 4);
        assert_eq!(branch.recovery.as_ref().unwrap().count, 120);
        for sequence in 1..=120 {
            assert_eq!(capabilities.pitr("main", sequence)?.sequence, sequence);
        }
        let report = capabilities.retention("main")?;
        assert_eq!(report.retention_mode, "time_window");
        assert!(report.indexed);
        assert_eq!(report.retained_points, 120);
        assert_eq!(report.oldest_recoverable_sequence, Some(1));
        assert_eq!(report.newest_recoverable_sequence, Some(120));
        assert_eq!(report.oldest_recoverable_timestamp_ms, Some(10));
        assert_eq!(report.newest_recoverable_timestamp_ms, Some(1200));

        let mut after = 0;
        let mut seen = Vec::new();
        loop {
            let page = capabilities.recovery_points("main", 1, after, 17)?;
            if page.points.is_empty() {
                break;
            }
            seen.extend(page.points.iter().map(|point| point.sequence));
            match page.next_after_sequence {
                Some(next) => after = next,
                None => break,
            }
        }
        assert_eq!(seen, (1..=120).collect::<Vec<_>>());

        let out = capabilities
            .pitr("main", 0)
            .err()
            .and_then(|error| error.downcast::<OutOfRetention>().ok())
            .context("sequence 0 must be out of retention")?;
        assert_eq!(out.oldest_sequence, Some(1));
        assert_eq!(out.retention_mode, "time_window");
        assert!(capabilities.pitr("main", 121).is_err());
        assert!(capabilities.resolve_time("main", 9).is_err());
        assert_eq!(capabilities.resolve_time("main", 15)?.sequence, 1);
        Ok(())
    }

    #[test]
    fn index_lookup_cost_does_not_follow_retained_history() -> Result<()> {
        let (store, authority) = authority_with(time_window(1_000_000, 4))?;
        let capabilities = Capabilities::new(authority.clone());
        for sequence in 1..=600 {
            commit_point(&authority, sequence, sequence * 10, None)?;
        }
        store.index_gets.store(0, Ordering::SeqCst);
        capabilities.pitr("main", 300)?;
        let middle = store.index_gets.swap(0, Ordering::SeqCst);
        capabilities.pitr("main", 599)?;
        let newest = store.index_gets.swap(0, Ordering::SeqCst);
        capabilities.recovery_points("main", 1, 300, 12)?;
        let page = store.index_gets.swap(0, Ordering::SeqCst);
        capabilities.resolve_time("main", 3_000)?;
        let time = store.index_gets.swap(0, Ordering::SeqCst);
        assert!(middle <= 6, "middle lookup read {middle} pages");
        assert!(newest <= 6, "newest lookup read {newest} pages");
        assert!(page <= 7, "paged lookup read {page} pages");
        assert!(time <= 6, "time lookup read {time} pages");
        assert!(
            newest.abs_diff(middle) <= 1,
            "lookup cost grew with history: {middle} -> {newest}"
        );
        Ok(())
    }

    #[test]
    fn restore_creates_a_separate_indexed_timeline() -> Result<()> {
        let (_store, authority) = authority_with(time_window(1_000_000, 4))?;
        let capabilities = Capabilities::new(authority.clone());
        for sequence in 1..=40 {
            commit_point(&authority, sequence, sequence * 10, None)?;
        }
        let old = capabilities.pitr("main", 20)?;
        capabilities.restore_point("main", &old)?;
        let timeline = authority.snapshot()?.root.branches["main"].timeline;
        assert_ne!(timeline, old.timeline);
        for sequence in 41..=50 {
            commit_point(&authority, sequence, sequence * 10, None)?;
        }
        assert_eq!(capabilities.pitr("main", 20)?.timeline, old.timeline);
        assert_eq!(capabilities.pitr("main", 45)?.timeline, timeline);
        let old_page = capabilities.recovery_points("main", old.timeline, 0, 64)?;
        assert_eq!(old_page.points.len(), 40);
        let new_page = capabilities.recovery_points("main", timeline, 0, 64)?;
        assert_eq!(
            new_page
                .points
                .iter()
                .map(|point| point.sequence)
                .collect::<Vec<_>>(),
            (41..=50).collect::<Vec<_>>()
        );
        let report = capabilities.retention("main")?;
        assert_eq!(report.oldest_recoverable_sequence, Some(1));
        assert_eq!(report.newest_recoverable_sequence, Some(50));
        Ok(())
    }

    #[test]
    fn equal_clock_is_ordered_and_a_rewind_is_rejected() -> Result<()> {
        let (_store, authority) = authority_with(time_window(1_000_000, 8))?;
        let capabilities = Capabilities::new(authority.clone());
        for sequence in 1..=3 {
            commit_point(&authority, sequence, 10, None)?;
        }
        assert_eq!(capabilities.resolve_time("main", 10)?.sequence, 3);
        let error = authority
            .transact(None, |root| {
                let point = RecoveryPoint {
                    id: blake3::hash(b"rewind").to_hex().to_string(),
                    branch: "main".into(),
                    timeline: 1,
                    sequence: 4,
                    timestamp_ms: 9,
                    head: None,
                    wal: None,
                };
                authority.record_recovery_point(root, "main", &point)
            })
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("timestamp rewinds"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    #[test]
    fn capacity_refusal_keeps_the_committed_window_intact() -> Result<()> {
        let retention = RecoveryRetention {
            mode: RetentionMode::TimeWindow {
                retain_ms: 1_000_000,
            },
            hot_points: 4,
            max_bytes: 2 * 1024,
            anchor_snapshots: true,
        };
        let (_store, authority) = authority_with(retention)?;
        let capabilities = Capabilities::new(authority.clone());
        let mut capacity = None;
        for sequence in 1..=100 {
            match commit_point(&authority, sequence, sequence * 10, None) {
                Ok(_) => {}
                Err(error) => {
                    capacity = error.downcast::<RetentionCapacityExceeded>().ok();
                    break;
                }
            }
        }
        let capacity = capacity.context("byte budget never refused a commit")?;
        assert!(capacity.retained_bytes > capacity.max_bytes);
        let report = capabilities.retention("main")?;
        assert_eq!(report.oldest_recoverable_sequence, Some(1));
        assert!(report.retained_points < 100);
        assert!(capabilities.pitr("main", 1).is_ok());
        Ok(())
    }

    #[test]
    fn snapshot_anchor_extends_the_time_floor() -> Result<()> {
        let (_store, authority) = authority_with(time_window(100, 4))?;
        let capabilities = Capabilities::new(authority.clone());
        commit_point(&authority, 1, 1_000, None)?;
        capabilities.snapshot("main", "anchor")?;
        commit_point(&authority, 2, 1_100, None)?;
        commit_point(&authority, 3, 2_000, None)?;
        assert_eq!(capabilities.pitr("main", 1)?.sequence, 1);
        let report = capabilities.retention("main")?;
        assert_eq!(report.snapshot_anchors, 1);
        assert_eq!(report.oldest_recoverable_sequence, Some(1));
        Ok(())
    }

    #[test]
    fn legacy_history_is_queryable_and_migrated_or_rejected() -> Result<()> {
        let (_store, authority) = authority_with(time_window(1_000_000, 4))?;
        let capabilities = Capabilities::new(authority.clone());
        let point = |sequence: u64, timestamp_ms: u64| RecoveryPoint {
            id: blake3::hash(&sequence.to_le_bytes()).to_hex().to_string(),
            branch: "main".into(),
            timeline: 1,
            sequence,
            timestamp_ms,
            head: None,
            wal: None,
        };
        authority.transact(None, |root| {
            let branch = root.branches.get_mut("main").unwrap();
            branch.sequence = 2;
            branch.current_sequence = 2;
            branch.last_commit_ms = 20;
            branch.current_commit_ms = 20;
            branch.history = vec![point(1, 10), point(2, 20)];
            Ok(())
        })?;
        assert!(authority.snapshot()?.root.branches["main"]
            .recovery
            .is_none());
        assert_eq!(capabilities.pitr("main", 1)?.sequence, 1);
        commit_point(&authority, 3, 30, None)?;
        assert!(authority.snapshot()?.root.branches["main"]
            .recovery
            .is_some());
        assert_eq!(capabilities.pitr("main", 1)?.sequence, 1);

        authority.transact(None, |root| {
            let branch = root.branches.get_mut("main").unwrap();
            branch.recovery = None;
            branch.history = vec![point(1, 10), point(4, 5)];
            branch.sequence = 4;
            branch.current_sequence = 4;
            branch.last_commit_ms = 40;
            branch.current_commit_ms = 40;
            Ok(())
        })?;
        let error = commit_point(&authority, 5, 50, None).unwrap_err();
        assert!(
            format!("{error:#}").contains("recovery history cannot be indexed"),
            "unexpected error: {error:#}"
        );

        let mut unsupported = RecoveryIndex::empty(RecoveryRetention::default());
        unsupported.schema = 99;
        assert!(unsupported.validate_shape().is_err());
        Ok(())
    }

    #[test]
    fn retain_after_shortens_index_and_window_together() -> Result<()> {
        let (_store, authority) = authority_with(time_window(1_000_000, 4))?;
        let capabilities = Capabilities::new(authority.clone());
        for sequence in 1..=40 {
            commit_point(&authority, sequence, sequence * 10, None)?;
        }
        capabilities.retain_after("main", 30)?;
        let report = capabilities.retention("main")?;
        assert_eq!(report.retained_points, 11);
        assert_eq!(report.oldest_recoverable_sequence, Some(30));
        assert!(capabilities.pitr("main", 29).is_err());
        assert_eq!(capabilities.pitr("main", 30)?.sequence, 30);
        assert!(capabilities.resolve_time("main", 299).is_err());
        Ok(())
    }

    #[test]
    fn garbage_collection_marks_points_outside_the_hot_window() -> Result<()> {
        let (_store, authority) = authority_with(time_window(1_000_000, 2))?;
        let manifest_bytes = serde_json::to_vec(&Manifest::default())?;
        let manifest_entry = authority.upload(&manifest_bytes)?;
        let head_entry = authority.upload(&serde_json::to_vec(&Head {
            manifest: manifest_entry.hash.clone(),
        })?)?;
        authority.transact(None, |root| {
            root.objects.insert(
                format!("manifests/{}", manifest_entry.hash),
                manifest_entry.clone(),
            )?;
            Ok(())
        })?;
        commit_point(&authority, 1, 10, Some(head_entry.clone()))?;
        for sequence in 2..=20 {
            commit_point(&authority, sequence, sequence * 10, None)?;
        }
        let root = authority.snapshot()?.root;
        assert!(!root.uploads.contains(&head_entry.blob)?);
        assert!(!root.uploads.contains(&manifest_entry.blob)?);
        let branch = root.branches["main"].clone();
        assert!(!branch.history.iter().any(|point| point.sequence == 1));
        let reachable = authority.reachable(&root)?;
        assert!(reachable.contains(&head_entry.blob));
        assert!(reachable.contains(&manifest_entry.blob));
        crate::gc::GarbageCollector::new(authority.clone(), std::time::Duration::ZERO).collect()?;
        assert!(authority
            .store
            .get(&authority.physical_key(&head_entry.blob)?)?
            .is_some());
        assert!(authority
            .store
            .get(&authority.physical_key(&manifest_entry.blob)?)?
            .is_some());
        Ok(())
    }
}

#[cfg(test)]
mod upload_batch_tests {
    use super::*;
    use crate::object_store::LocalObjectStore;

    #[test]
    fn segment_batch_reserves_intents_once_and_verifies_physical_bytes() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let authority = Authority::open(store, Namespace::new("bulk", "segments")?)?;
        let segments = vec![
            vec![0x11; crate::manifest::SEGMENT_SIZE],
            vec![0x22; crate::manifest::SEGMENT_SIZE],
        ];
        let before = authority.snapshot()?.root.revision;
        let entries = authority.upload_segments(&segments)?;
        assert_eq!(authority.snapshot()?.root.revision, before + 1);
        for (entry, expected) in entries.iter().zip(&segments) {
            assert_eq!(authority.read_blob(entry)?, *expected);
            assert_eq!(
                authority.snapshot()?.root.uploads.get(&entry.blob)?,
                Some(entry.clone())
            );
        }
        authority.register_segments(&entries)?;
        for entry in &entries {
            let key = format!("segments/{}", entry.hash);
            assert_eq!(
                authority.snapshot()?.root.objects.get(&key)?,
                Some(entry.clone())
            );
        }
        let before = authority.snapshot()?.root.revision;
        assert!(authority.upload_segments(&[]).is_err());
        assert!(authority.upload_segments(&[vec![0; 1]]).is_err());
        assert_eq!(authority.snapshot()?.root.revision, before);
        Ok(())
    }

    #[test]
    fn a_publication_reports_its_stages_when_the_switch_is_on() -> Result<()> {
        crate::work_metrics::set_enabled(true);
        let before = crate::work_metrics::snapshot().expect("enabled snapshot");
        let directory = tempfile::tempdir()?;
        let authority = Authority::open(
            Arc::new(LocalObjectStore::open(directory.path())?),
            Namespace::new("bulk", "stages")?,
        )?;
        let entry = authority.upload(&[0x5a; 64])?;
        assert_eq!(authority.read_blob(&entry)?, vec![0x5a; 64]);
        let after = crate::work_metrics::snapshot().expect("enabled snapshot");
        for stage in [
            "snapshot", "clone", "mutate", "prepare", "receipt", "validate", "index", "seal",
            "encode", "group",
        ] {
            let calls = after.metadata.publish_stage_calls[stage]
                - before.metadata.publish_stage_calls[stage];
            assert!(calls >= 1, "the publication did not charge {stage}");
        }
        assert!(after.metadata.publish_total_ns > before.metadata.publish_total_ns);
        assert!(after.metadata.publish_total_max_ns >= 1);
        Ok(())
    }

    #[test]
    fn full_segment_batch_uses_two_catalog_publications() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let authority = Authority::open(
            Arc::new(LocalObjectStore::open(directory.path())?),
            Namespace::new("bulk", "full")?,
        )?;
        let segments = (0..128)
            .map(|index| {
                let mut bytes = vec![0; crate::manifest::SEGMENT_SIZE];
                bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
                bytes
            })
            .collect::<Vec<_>>();
        let before = authority.snapshot()?.root.revision;
        let entries = authority.upload_segments(&segments)?;
        authority.register_segments(&entries)?;
        let after = authority.snapshot()?.root;
        assert_eq!(after.revision, before + 2);
        assert_eq!(after.objects.entries()?.len(), segments.len());
        Ok(())
    }
}
