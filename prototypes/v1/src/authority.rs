mod branch;
mod catalog;
mod codec;
mod decode_cache;
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
pub use types::{BranchRef, CatalogRoot, ObjectEntry, RecoveryPoint, RootSnapshot, Tombstone};

use crate::ownership::types::{MutationGuard, MutationStage};
use crate::{
    namespace::Namespace,
    object_store::{ObjectStore, PreconditionFailed, PutCondition},
};
use anyhow::{ensure, Context, Result};
use codec::{decode_with_digest, encode, encode_with_digest};
use std::{
    collections::BTreeSet,
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
mod admitted {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, OnceLock};

    const MAX_DATABASES: usize = 1024;
    const MAX_LINEAGES: usize = 64;

    type Table = BTreeMap<String, BTreeMap<String, String>>;

    fn table() -> &'static Mutex<Table> {
        static ADMITTED: OnceLock<Mutex<Table>> = OnceLock::new();
        ADMITTED.get_or_init(|| Mutex::new(Table::new()))
    }

    /// A lock that cannot be taken is not a reason to trust a head, so it reports the head
    /// as unverified and the caller re-verifies it.
    pub(super) fn holds(database: &str, lineage: &str, digest: &str) -> bool {
        table().lock().is_ok_and(|table| {
            table
                .get(database)
                .and_then(|lineages| lineages.get(lineage))
                .is_some_and(|held| held.as_str() == digest)
        })
    }

    pub(super) fn remember(database: &str, lineage: &str, digest: &str) {
        let Ok(mut table) = table().lock() else {
            return;
        };
        if !table.contains_key(database) && table.len() >= MAX_DATABASES {
            return;
        }
        let lineages = table.entry(database.to_owned()).or_default();
        if !lineages.contains_key(lineage) && lineages.len() >= MAX_LINEAGES {
            return;
        }
        lineages.insert(lineage.to_owned(), digest.to_owned());
    }
}

const INLINE_RECEIPTS_MAX: usize = 128;

/// Recovery points a branch keeps in the head document. The head is decoded, validated,
/// re-encoded and conditionally published on every statement, and one point is ~661 B of
/// it, so an unbounded window makes each later statement of a long session more expensive
/// than the one before it (measured: a 157-statement load grows from 86 ms to 1.3 s per
/// statement and from 52 KB to 155 KB of head). A branch therefore keeps a rolling window:
/// the newest points stay, older ones leave oldest-first, exactly as `retain_after` drops
/// them when an operator asks. `snapshot` is the durable anchor that outlives the window,
/// and a query for a sequence that left it keeps failing closed with "exact committed
/// sequence is not retained" instead of answering with a neighbouring point.
const RETAINED_RECOVERY_POINTS: usize = 32;

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
    root.attach(store.clone(), namespace.clone());
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
        let namespace = Namespace::new(namespace.tenant, namespace.database)?;
        let object = match Self::open_root(store.as_ref(), &namespace)? {
            Some(object) => object,
            None => Self::claim_absent_root(store.as_ref(), &namespace)?,
        };
        Self::from_loaded(store, namespace, object)
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

    pub(crate) fn from_loaded(
        store: Arc<dyn ObjectStore>,
        namespace: Namespace,
        object: crate::object_store::StoredObject,
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
        });
        authority.verify_deep(&root)?;
        authority.remember(&key, &root, &digest);
        Ok(authority)
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub(crate) fn scoped(&self, guard: Arc<dyn MutationGuard>) -> Arc<Self> {
        Arc::new(Self {
            store: self.store.clone(),
            namespace: self.namespace.clone(),
            incarnation: self.incarnation.clone(),
            guard: Some(guard),
        })
    }

    /// Record a head as verified. Only called once the head has passed `verify_deep`, so
    /// the change-scoped path can never be entered on a head this process has not seen the
    /// whole of.
    fn remember(&self, key: &str, root: &CatalogRoot, digest: &str) {
        admitted::remember(key, &root.incarnation, digest);
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
                guard.uncertain(stage);
            }
        }
    }

    pub fn snapshot(&self) -> Result<RootSnapshot> {
        #[cfg(test)]
        let _probe = scaling_probe::stage("snapshot", 0);
        self.check(MutationStage::Read)?;
        let key = self.namespace.key("authority/root")?;
        let object = self
            .store
            .get(&key)?
            .context("authority root missing; refusing reset")?;
        let (root, digest) = decode_head_with_digest(&self.store, &self.namespace, &object.bytes)?;
        if admitted::holds(&key, &root.incarnation, &digest) {
            self.validate_head(&root)?;
            self.validate_references(&root, &[])?;
        } else {
            // The head is not one this process verified, so the references it inherits may
            // have been replaced behind the authority's back: an envelope checksum proves
            // the document is self-consistent, not that this authority wrote it, and a
            // reference that was never registered can only be caught by resolving it.
            // Verify the way boot does before any statement trusts the head.
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
        if root.version == 4 {
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
        if root.version != 4 || root.receipts.len() <= INLINE_RECEIPTS_MAX {
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

    pub fn transact<F>(&self, expected_revision: Option<u64>, mutation: F) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_inner(expected_revision, mutation, None, &[])
    }

    /// Publish a mutation whose group also carries new physical instances. The bodies
    /// are written before the head CAS, so a reader that sees the registration always
    /// sees the bytes; see `PublicationBody` for why no intent precedes them.
    pub(super) fn transact_with_bodies<F>(
        &self,
        bodies: &[PublicationBody<'_>],
        mutation: F,
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        self.transact_inner(None, mutation, None, bodies)
    }

    fn transact_inner<F>(
        &self,
        expected_revision: Option<u64>,
        mut mutation: F,
        deletion: Option<&str>,
        bodies: &[PublicationBody<'_>],
    ) -> Result<RootSnapshot>
    where
        F: FnMut(&mut CatalogRoot) -> Result<()>,
    {
        #[cfg(test)]
        let _probe = scaling_probe::stage("transact", 0);
        let _reads = request_reads::Scope::enter();
        let operation = unique_id()?;
        for _ in 0..128 {
            let current = self.snapshot()?;
            if let Some(revision) = expected_revision {
                ensure!(current.root.revision == revision, PreconditionFailed);
            }
            // One durability group per attempt: the immutable in-flight page bodies are
            // buffered here and the head CAS joins them as the last write, so the store
            // either lands the whole publication or none of it.
            let pending = std::cell::RefCell::new(paged_index::PendingPages::default());
            let publishing = paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace)
                .deferring(&pending);
            let mut candidate = {
                #[cfg(test)]
                let _probe = scaling_probe::stage("clone", scaling_probe::entries(&current.root));
                current.root.clone()
            };
            mutation(&mut candidate)?;
            self.prepare(&current.root, &mut candidate, deletion, bodies)?;
            candidate.revision = current
                .root
                .revision
                .checked_add(1)
                .context("authority revision overflow")?;
            self.record_receipt(&mut candidate, operation.clone(), &publishing)?;
            self.validate_head_with(&candidate, Some(&pending))?;
            self.validate_references(&candidate, bodies)?;
            candidate.blobs.publish(&publishing)?;
            candidate.objects.publish(&publishing)?;
            candidate.uploads.publish(&publishing)?;
            candidate.tombstones.publish(&publishing)?;
            candidate.seal()?;
            let (encoded, digest) = encode_with_digest(&candidate)?;
            crate::work_metrics::observe_root_publish(
                encoded.len(),
                candidate
                    .pins
                    .values()
                    .fold(0usize, |total, entries| total.saturating_add(entries.len())),
                candidate.receipts.len(),
            );
            if let Some(guard) = &self.guard {
                guard.validate_root(&current.root, &candidate)?;
            }
            self.check(MutationStage::RootPublish)?;
            let mut writes = pending.borrow().writes().to_vec();
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
                    // This attempt built the head from a verified one and validated the
                    // change set, so the head is verified for the same reason a boot-verified
                    // one is: recording it is what keeps the next statement on the cheap
                    // path.
                    self.remember(&self.namespace.key("authority/root")?, &candidate, &digest);
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
        self.transact(None, |root| {
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
        self.transact(None, |root| {
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
        self.transact(None, |root| {
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

    pub fn reachable(&self, root: &CatalogRoot) -> Result<BTreeSet<String>> {
        self.verify_deep(root)?;
        self.mark(root)
    }

    pub fn delete_tombstone(&self, blob: &str) -> Result<()> {
        self.check(MutationStage::Delete)?;
        let snapshot = self.snapshot()?;
        let tombstone = snapshot
            .root
            .tombstones
            .get(blob)?
            .context("physical deletion requires persisted tombstone")?;
        ensure!(
            !snapshot.root.uploads.contains(blob)?,
            "unresolved upload intent prevents deletion"
        );
        ensure!(
            !self.reachable(&snapshot.root)?.contains(blob),
            "reachable blob cannot be deleted"
        );
        let entry = tombstone.entry.clone();
        let key = self.physical_key(blob)?;
        if tombstone.deleted && self.store.get(&key)?.is_none() {
            return Ok(());
        }
        self.check(MutationStage::Delete)?;
        if let Err(error) = self.store.delete(&key, None) {
            self.uncertain(MutationStage::Delete, &error);
            if self.store.get(&key)?.is_some() {
                return Err(error);
            }
        }
        if tombstone.deleted {
            return Ok(());
        }
        ensure!(
            !self.snapshot()?.root.uploads.contains(blob)?,
            "unresolved upload intent prevents deletion"
        );
        self.transact_inner(
            None,
            |root| {
                root.tombstones.update(blob, |tombstone| {
                    ensure!(tombstone.entry == entry, "tombstone instance changed");
                    tombstone.deleted = true;
                    Ok(())
                })?;
                ensure!(
                    !root.uploads.contains(blob)?,
                    "unresolved upload intent prevents deletion"
                );
                root.blobs.remove(blob)?;
                Ok(())
            },
            Some(blob),
            &[],
        )?;
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
