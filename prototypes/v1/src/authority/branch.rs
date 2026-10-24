#[path = "branch_publication.rs"]
mod publication;
#[cfg(test)]
#[path = "branch_tests.rs"]
mod tests;

use super::types::{pin_entry_key, pin_entry_parts};
use super::{
    now_ms, semantic, unique_id, Authority, BranchRef, CatalogRoot, ObjectEntry, PublicationBody,
    RecoveryPoint,
};
use crate::{
    manifest::{Head, Manifest},
    object_store::{ObjectStore, PreconditionFailed, PutCondition, StoredObject},
    ownership::types::MutationStage,
    recovery::{validate_index_append, validate_index_state, ValidatedWal},
    work_metrics::PublishOrigin,
};
use anyhow::{ensure, Context, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

struct View {
    incarnation: u64,
    /// Snapshot generation this session currently reads. A successful publication that
    /// changes the branch advances it; entries of older generations stay pinned until the
    /// last reader holding that generation is gone.
    generation: u64,
    head: Option<ObjectEntry>,
    wal: Option<ObjectEntry>,
    objects: BTreeMap<String, ObjectEntry>,
    pinned: BTreeSet<String>,
    /// Blobs owned by each live generation. Generation-qualified pin keys make the sets
    /// independent: retiring a generation removes exactly its own entries, even when the
    /// same blob is read again by a newer generation.
    generations: BTreeMap<u64, BTreeSet<String>>,
    /// Readers currently holding a generation open. Production reads hold the view lock,
    /// so this only exceeds zero for an explicit diagnostic guard or a test that keeps an
    /// old snapshot alive across a refresh.
    readers: BTreeMap<u64, usize>,
    validated_wal: Option<(ObjectEntry, ValidatedWal)>,
}

/// The parts of a branch reference a refresh installs. Kept separate from `View` so a
/// selection cannot accidentally carry generation state from another store.
struct Selected {
    incarnation: u64,
    head: Option<ObjectEntry>,
    wal: Option<ObjectEntry>,
}

impl View {
    fn admit<'a>(&mut self, entries: impl IntoIterator<Item = &'a ObjectEntry>) {
        let generation = self.generation;
        let set = self.generations.entry(generation).or_default();
        for entry in entries {
            set.insert(entry.blob.clone());
            self.pinned.insert(entry.blob.clone());
        }
    }

    fn admit_map(&mut self, entries: &BTreeMap<String, ObjectEntry>) {
        self.admit(entries.values());
    }

    /// Generations that will be dead once `next_generation` becomes current: not current,
    /// not held by a reader, and owning at least one entry. Their pin prefixes must leave in
    /// the same publication that advances the generation; a separate transaction would turn
    /// every commit into two head writes.
    fn retire_prefixes(&self, pin: &str, next_generation: u64) -> Vec<(u64, String)> {
        self.generations
            .iter()
            .filter(|(generation, _)| {
                **generation != next_generation
                    && self.readers.get(generation).copied().unwrap_or(0) == 0
            })
            .map(|(generation, _)| (*generation, format!("{pin}/{generation}/")))
            .collect()
    }

    fn finish_retirement(&mut self, retired: &[u64]) {
        for generation in retired {
            self.generations.remove(generation);
            self.readers.remove(generation);
        }
        self.pinned = self.generations.values().flatten().cloned().collect();
        self.objects
            .retain(|_, entry| self.pinned.contains(&entry.blob));
    }
}

/// Observation of one branch session's pin without changing its state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct PinViewObservation {
    pub generation: u64,
    pub live_generations: usize,
    pub held_generations: usize,
    pub pinned_entries: usize,
    pub cached_objects: usize,
}

/// Cost of releasing one pin. Generation batches are durable: the advanced generation and
/// the release marker are published first, so a crash can resume with the remaining
/// generations instead of rescanning the whole registry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PinReleaseStats {
    pub batches: usize,
    pub generations: usize,
    pub entries: usize,
    pub pages: usize,
    pub scanned: usize,
}

/// A read that holds one snapshot generation open. While this guard exists, a refresh must
/// not retire that generation: the reader may still resolve objects the newer snapshot no
/// longer references. Dropping the guard retires whatever generations are now idle.
#[cfg(test)]
pub(crate) struct GenerationRead {
    store: Arc<BranchStore>,
    generation: u64,
}

#[cfg(test)]
impl Drop for GenerationRead {
    fn drop(&mut self) {
        let mut view = match self.store.view.lock() {
            Ok(view) => view,
            Err(_) => {
                report_pin_release_failure(
                    &self.store.pin,
                    "generation-guard-poisoned",
                    &anyhow::anyhow!("branch session lock poisoned"),
                );
                return;
            }
        };
        if let Some(count) = view.readers.get_mut(&self.generation) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                view.readers.remove(&self.generation);
            }
        }
        if let Err(error) = self.store.retire_idle(&mut view) {
            report_pin_release_failure(&self.store.pin, "generation-retire", &error);
        }
    }
}

pub struct BranchStore {
    authority: Arc<Authority>,
    branch: String,
    incarnation: u64,
    pin: String,
    view: Mutex<View>,
    drop_cleanup: AtomicBool,
}

impl Authority {
    pub fn branch_store(self: &Arc<Self>, branch: &str) -> Result<Arc<BranchStore>> {
        semantic::validate_name(branch)?;
        let pin = unique_id()?;
        let mut selected = None;
        self.transact_with_origin(None, PublishOrigin::BranchOpen, |root| {
            let reference = root.branches.get(branch).context("branch missing")?;
            let incarnation = reference.incarnation;
            let head = reference.head.clone();
            let wal = reference.wal.clone();
            let entries = referenced(self, root, head.as_ref(), wal.as_ref())?;
            let mut pinned = BTreeSet::new();
            root.pins.insert(pin.clone(), Vec::new());
            root.pin_generations.insert(pin.clone(), 0);
            for entry in entries.values() {
                if pinned.insert(entry.blob.clone()) {
                    root.pin_entries
                        .insert(pin_entry_key(&pin, 0, &entry.blob), entry.clone())?;
                }
            }
            selected = Some(View {
                incarnation,
                generation: 0,
                head,
                wal,
                objects: entries
                    .into_iter()
                    .filter(|(key, _)| !matches!(key.as_str(), "HEAD" | "wal/index"))
                    .collect(),
                generations: BTreeMap::from([(0, pinned.clone())]),
                pinned,
                readers: BTreeMap::new(),
                validated_wal: None,
            });
            Ok(())
        })?;
        let view = selected.context("reader pin publication missing")?;
        self.live_pins
            .lock()
            .map_err(|_| anyhow::anyhow!("live pin registry lock poisoned"))?
            .insert(pin.clone());
        Ok(Arc::new(BranchStore {
            authority: self.clone(),
            branch: branch.to_owned(),
            incarnation: view.incarnation,
            pin,
            view: Mutex::new(view),
            drop_cleanup: AtomicBool::new(true),
        }))
    }
}

pub(crate) fn referenced(
    authority: &Authority,
    root: &CatalogRoot,
    head: Option<&ObjectEntry>,
    wal: Option<&ObjectEntry>,
) -> Result<BTreeMap<String, ObjectEntry>> {
    let mut entries = BTreeMap::new();
    if let Some(head) = head {
        semantic::registered(root, head)?;
        let metadata: Head = serde_json::from_slice(&authority.read_metadata_blob(head)?)?;
        let key = format!("manifests/{}", metadata.manifest);
        manifest_dependencies(authority, root, &key, &mut entries)?;
        entries.insert("HEAD".into(), head.clone());
    }
    if let Some(wal) = wal {
        semantic::registered(root, wal)?;
        let index = semantic::index(&authority.read_metadata_blob(wal)?)?;
        let page_root = index.receipt_index.clone();
        let chunks = crate::recovery::resolve_entries(&index, |hash| {
            let key = format!("wal/index-segments/{hash}");
            let entry = root
                .objects
                .get(&key)?
                .context("WAL index segment missing")?;
            semantic::registered(root, &entry)?;
            ensure!(entry.hash == hash, "WAL index segment digest mismatch");
            let bytes = authority.read_metadata_blob(&entry)?;
            entries.insert(key, entry);
            Ok(bytes)
        })?;
        for chunk in chunks {
            let key = format!("wal/chunks/{}", chunk.hash);
            let entry = root.objects.get(&key)?.context("WAL chunk missing")?;
            semantic::registered(root, &entry)?;
            entries.insert(key, entry.clone());
        }
        if let Some(page_root) = page_root {
            crate::authority::paged_index::walk_tree(Some(&page_root), |hash| {
                let key = format!("wal/receipt-pages/{hash}");
                let entry = root
                    .objects
                    .get(&key)?
                    .context("WAL receipt archive page missing")?;
                semantic::registered(root, &entry)?;
                let bytes = authority.read_metadata_blob(&entry)?;
                entries.insert(key, entry);
                Ok(bytes)
            })?;
        }
        entries.insert("wal/index".into(), wal.clone());
    }
    Ok(entries)
}

fn manifest_dependencies(
    authority: &Authority,
    root: &CatalogRoot,
    key: &str,
    entries: &mut BTreeMap<String, ObjectEntry>,
) -> Result<()> {
    let entry = root.objects.get(key)?.context("manifest missing")?;
    semantic::registered(root, &entry)?;
    let manifest: Manifest = serde_json::from_slice(&authority.read_metadata_blob(&entry)?)?;
    manifest.validate()?;
    entries.insert(key.to_owned(), entry.clone());
    for hash in manifest.segments.values() {
        let key = format!("segments/{hash}");
        let entry = root
            .objects
            .get(&key)?
            .context("manifest segment missing")?;
        semantic::registered(root, &entry)?;
        entries.insert(key, entry.clone());
    }
    Ok(())
}

fn accessible(
    authority: &Authority,
    root: &CatalogRoot,
    key: &str,
    entry: &ObjectEntry,
) -> Result<BTreeMap<String, ObjectEntry>> {
    let mut entries = match key {
        "HEAD" => referenced(authority, root, Some(entry), None)?,
        "wal/index" => referenced(authority, root, None, Some(entry))?,
        _ => BTreeMap::new(),
    };
    if key.starts_with("manifests/") {
        manifest_dependencies(authority, root, key, &mut entries)?;
    }
    semantic::registered(root, entry)?;
    entries.insert(key.to_owned(), entry.clone());
    Ok(entries)
}

impl BranchStore {
    pub(crate) fn suppress_drop_cleanup(&self) {
        self.drop_cleanup.store(false, Ordering::Release);
    }

    pub(crate) fn release_pin(&self, authority: &Authority) -> Result<PinReleaseStats> {
        let result = release_pin_identity(authority, &self.pin);
        // The owner is gone regardless of the outcome: a durable leftover is an orphan now,
        // not a live pin. Keeping it in the live table would make orphan detection lie.
        if let Ok(mut live) = authority.live_pins.lock() {
            live.remove(&self.pin);
        }
        result
    }

    pub(crate) fn pin_identity(&self) -> &str {
        &self.pin
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> Result<u64> {
        Ok(self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?
            .generation)
    }

    #[cfg(test)]
    pub(crate) fn observe_pin(&self) -> Result<PinViewObservation> {
        let view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        Ok(PinViewObservation {
            generation: view.generation,
            live_generations: view.generations.len(),
            held_generations: view.readers.values().filter(|count| **count > 0).count(),
            pinned_entries: view.pinned.len(),
            cached_objects: view.objects.len(),
        })
    }

    /// Hold the generation this session currently reads. A refresh skips retirement while
    /// the guard exists; dropping it retires every generation that is now idle.
    #[cfg(test)]
    pub(crate) fn hold_generation(self: &Arc<Self>) -> Result<GenerationRead> {
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let generation = view.generation;
        *view.readers.entry(generation).or_default() += 1;
        Ok(GenerationRead {
            store: self.clone(),
            generation,
        })
    }

    /// Entries this reader pin currently protects, resolved through the published pin
    /// registry. Used by diagnostics and tests to observe the split representation without
    /// reaching into the registry itself.
    pub fn pinned_entries(&self) -> Result<Vec<ObjectEntry>> {
        let root = self.authority.snapshot()?.root;
        let (entries, _) = root
            .pin_entries
            .prefix_entries_counted(&format!("{}/", self.pin))?;
        Ok(entries.into_values().collect())
    }
    fn relative<'a>(&self, key: &'a str) -> Result<&'a str> {
        let prefix = self.authority.namespace.prefix();
        let relative = key
            .strip_prefix(&prefix)
            .context("object key outside branch namespace")?;
        self.authority.namespace.key(relative)?;
        ensure!(
            !relative.starts_with("authority/"),
            "private authority path is not an engine key"
        );
        Ok(relative)
    }

    fn bound<'a>(&self, root: &'a CatalogRoot) -> Result<&'a BranchRef> {
        let branch = root
            .branches
            .get(&self.branch)
            .context("stale branch incarnation: branch missing")?;
        ensure!(
            branch.incarnation == self.incarnation,
            "stale branch incarnation"
        );
        ensure!(root.pins.contains_key(&self.pin), "reader pin disappeared");
        Ok(branch)
    }

    fn extend_pin(
        &self,
        root: &mut CatalogRoot,
        view: &View,
        generation: u64,
        entries: BTreeMap<String, ObjectEntry>,
    ) -> Result<BTreeMap<String, ObjectEntry>> {
        ensure!(root.pins.contains_key(&self.pin), "reader pin disappeared");
        ensure!(
            root.pin_generations.get(&self.pin) == Some(&generation),
            "reader pin generation changed under the session"
        );
        let mut added = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for (key, entry) in entries {
            if !view.pinned.contains(&entry.blob) && seen.insert(entry.blob.clone()) {
                let pin_key = pin_entry_key(&self.pin, generation, &entry.blob);
                if let Some(previous) = root.pin_entries.get(&pin_key)? {
                    ensure!(previous == entry, "reader pin entry identity mutation");
                } else {
                    root.pin_entries.insert(pin_key, entry.clone())?;
                }
            }
            added.insert(key, entry);
        }
        Ok(added)
    }

    /// Advance the durable generation of this pin and return it. A publication that moves
    /// the branch starts a new generation; a read that only widens the current one keeps
    /// the generation and calls `extend_pin` directly.
    fn advance_generation(&self, root: &mut CatalogRoot, view: &View) -> Result<u64> {
        let current = *root
            .pin_generations
            .get(&self.pin)
            .context("reader pin generation missing")?;
        let next = view
            .generation
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        ensure!(
            current == view.generation || current == next,
            "reader pin generation changed under the session"
        );
        root.pin_generations.insert(self.pin.clone(), next);
        Ok(next)
    }

    fn selected(root: &CatalogRoot, branch: &str) -> Result<Selected> {
        let reference = root.branches.get(branch).context("branch missing")?;
        Ok(Selected {
            incarnation: reference.incarnation,
            head: reference.head.clone(),
            wal: reference.wal.clone(),
        })
    }

    fn refresh(
        &self,
        view: &mut View,
        selected: Selected,
        added: BTreeMap<String, ObjectEntry>,
        generation: u64,
        retired: &[(u64, String)],
    ) -> Result<()> {
        if view.validated_wal.as_ref().map(|(entry, _)| entry) != selected.wal.as_ref() {
            view.validated_wal = None;
        }
        view.incarnation = selected.incarnation;
        view.generation = generation;
        view.head = selected.head;
        view.wal = selected.wal;
        view.admit_map(&added);
        view.objects.extend(
            added
                .into_iter()
                .filter(|(key, _)| !matches!(key.as_str(), "HEAD" | "wal/index")),
        );
        view.finish_retirement(
            &retired
                .iter()
                .map(|(generation, _)| *generation)
                .collect::<Vec<_>>(),
        );
        Ok(())
    }

    /// Retire idle generations outside a publication (a read guard was dropped). The
    /// commit path never reaches this: it folds the same prefixes into its own transaction
    /// so one commit still publishes one head.
    #[cfg(test)]
    fn retire_idle(&self, view: &mut View) -> Result<()> {
        let retired = view.retire_prefixes(&self.pin, view.generation);
        if retired.is_empty() {
            return Ok(());
        }
        self.authority
            .transact_with_origin(None, PublishOrigin::PinRelease, |root| {
                ensure!(root.pins.contains_key(&self.pin), "reader pin disappeared");
                for (_, prefix) in &retired {
                    root.pin_entries.remove_prefix(prefix)?;
                }
                Ok(())
            })?;
        let generations: Vec<u64> = retired
            .into_iter()
            .map(|(generation, _)| generation)
            .collect();
        view.finish_retirement(&generations);
        Ok(())
    }

    fn get_entry(&self, key: &str, view: &View) -> Option<ObjectEntry> {
        match key {
            "HEAD" => view.head.clone(),
            "wal/index" => view.wal.clone(),
            _ => view.objects.get(key).cloned(),
        }
    }

    fn condition(current: Option<&ObjectEntry>, condition: &PutCondition) -> Result<()> {
        let satisfied = match condition {
            PutCondition::Any => true,
            PutCondition::Absent => current.is_none(),
            PutCondition::Match(etag) => current.is_some_and(|entry| entry.etag == *etag),
        };
        ensure!(satisfied, PreconditionFailed);
        Ok(())
    }

    fn commit_point(
        &self,
        root: &mut CatalogRoot,
        bytes: &[u8],
        entry: &ObjectEntry,
        previous: Option<ValidatedWal>,
        bodies: &[PublicationBody<'_>],
    ) -> Result<ValidatedWal> {
        let load_segment = |hash: &str| {
            let key = format!("wal/index-segments/{hash}");
            let entry = root
                .objects
                .get(&key)?
                .context("WAL index segment missing from candidate catalog")?;
            semantic::registered(root, &entry)?;
            if let Some(bytes) = super::staged_body(bodies, &entry) {
                return Ok(bytes.to_vec());
            }
            self.authority.read_metadata_blob(&entry)
        };
        let load_chunk = |key: &str| {
            let relative = self.relative(key)?;
            let entry = root
                .objects
                .get(relative)?
                .context("commit chunk missing from candidate catalog")?;
            semantic::registered(root, &entry)?;
            // A body staged by this attempt is in the group buffer, not in the store
            // yet. This is the rule pages already follow: validation of a candidate
            // must be able to read what the candidate's own group will install,
            // otherwise the authority rejects its own in-flight publication.
            if let Some(bytes) = super::staged_body(bodies, &entry) {
                return Ok(bytes.to_vec());
            }
            self.authority.read_metadata_blob(&entry)
        };
        let load_page = |hash: &str| {
            let key = format!("wal/receipt-pages/{hash}");
            let entry = root
                .objects
                .get(&key)?
                .context("receipt archive page missing from candidate catalog")?;
            semantic::registered(root, &entry)?;
            if let Some(bytes) = super::staged_body(bodies, &entry) {
                return Ok(bytes.to_vec());
            }
            self.authority.read_metadata_blob(&entry)
        };
        let validated = match previous {
            Some(previous) => validate_index_append(
                previous,
                bytes,
                self.authority.namespace(),
                load_segment,
                load_chunk,
                load_page,
            )?,
            None => validate_index_state(
                bytes,
                self.authority.namespace(),
                load_segment,
                load_chunk,
                load_page,
            )?,
        };
        let commit = validated.commit();
        let previous = root.branches[&self.branch]
            .wal
            .as_ref()
            .map(|entry| {
                Ok::<_, anyhow::Error>(
                    semantic::index(&self.authority.read_metadata_blob(entry)?)?
                        .commit
                        .map_or(0, |commit| commit.lsn),
                )
            })
            .transpose()?
            .unwrap_or(0);
        let branch = root
            .branches
            .get_mut(&self.branch)
            .context("branch missing")?;
        branch.wal = Some(entry.clone());
        if commit.is_some_and(|commit| commit.lsn > previous) {
            branch.sequence = branch
                .sequence
                .checked_add(1)
                .context("commit sequence overflow")?;
            branch.last_commit_ms = now_ms()?.max(
                branch
                    .last_commit_ms
                    .checked_add(1)
                    .context("commit timestamp overflow")?,
            );
            branch.current_sequence = branch.sequence;
            branch.current_commit_ms = branch.last_commit_ms;
            let point = RecoveryPoint {
                id: entry.etag.clone(),
                branch: self.branch.clone(),
                timeline: branch.timeline,
                sequence: branch.sequence,
                timestamp_ms: branch.last_commit_ms,
                head: branch.head.clone(),
                wal: branch.wal.clone(),
            };
            // The index is the retention authority and the hot window is only its newest
            // cache. Publishing both in the same transaction is what lets a commit older
            // than the window stay restorable.
            self.authority
                .record_recovery_point(root, &self.branch, &point)?;
        }
        Ok(validated)
    }

    /// Publish the immutable pages produced by one receipt roll as a single durability
    /// group.
    ///
    /// The WAL head that references them is published afterwards, so a failure here leaves
    /// only unreferenced content-addressed bodies. Batching is the point: one page per
    /// `put` pays a catalogue publication and a body barrier for every archived receipt,
    /// which is exactly the tail this path must not have.
    pub(in crate::authority) fn publish_wal_pages(
        &self,
        writes: &[crate::object_store::GroupWrite],
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let snapshot = self.authority.snapshot()?;
        self.bound(&snapshot.root)?;
        self.authority.check(MutationStage::Upload)?;
        let mut pages = Vec::with_capacity(writes.len());
        for write in writes {
            let relative = self.relative(&write.key)?;
            ensure!(
                relative.starts_with(crate::wal::WAL_RECEIPT_PAGE_PREFIX),
                "batched page outside the receipt archive"
            );
            let hash = relative.rsplit('/').next().unwrap_or_default();
            ensure!(
                blake3::hash(&write.bytes).to_hex().as_str() == hash,
                "immutable receipt page checksum mismatch"
            );
            if let Some(existing) = snapshot.root.objects.get(relative)? {
                ensure!(existing.hash == hash, "immutable receipt page conflict");
                continue;
            }
            pages.push((
                relative.to_owned(),
                Authority::upload_entry(&write.bytes)?,
                write,
            ));
        }
        if pages.is_empty() {
            return Ok(());
        }
        let bodies = pages
            .iter()
            .map(|(_, entry, write)| PublicationBody {
                entry,
                bytes: &write.bytes,
            })
            .collect::<Vec<_>>();
        let mut added = BTreeMap::new();
        let mut selected = None;
        let mut generation = None;
        let next_generation = view
            .generation
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        let retired = view.retire_prefixes(&self.pin, next_generation);
        self.authority
            .transact_with_bodies(PublishOrigin::WalPages, &bodies, |root| {
                self.bound(root)?;
                let mut entries = BTreeMap::new();
                for (relative, entry, _) in &pages {
                    ensure!(
                        !root.blobs.contains(&entry.blob)?
                            && !root.tombstones.contains(&entry.blob)?,
                        "physical instance collision"
                    );
                    if let Some(existing) = root.objects.get(relative)? {
                        // A retry or another writer of the same branch may have published this
                        // content already; the key is its content address.
                        ensure!(
                            existing.hash == entry.hash,
                            "immutable receipt page conflict"
                        );
                        entries.insert(relative.clone(), existing);
                        continue;
                    }
                    root.blobs.insert(entry.blob.clone(), entry.clone())?;
                    root.uploads.insert(entry.blob.clone(), entry.clone())?;
                    root.objects.insert(relative.clone(), entry.clone())?;
                    entries.insert(relative.clone(), entry.clone());
                }
                let next = self.advance_generation(root, &view)?;
                for (_, prefix) in &retired {
                    root.pin_entries.remove_prefix(prefix)?;
                }
                added = self.extend_pin(root, &view, next, entries)?;
                generation = Some(next);
                selected = Some(Self::selected(root, &self.branch)?);
                Ok(())
            })?;
        self.refresh(
            &mut view,
            selected.context("branch page publication view missing")?,
            added,
            generation.context("branch page publication generation missing")?,
            &retired,
        )?;
        Ok(())
    }

    pub(super) fn publish_wal_append(
        &self,
        encoded: &crate::wal::EncodedWalRecord,
        metadata: &crate::wal::WalMetadata,
        expected: Option<String>,
    ) -> Result<()> {
        let record = encoded.record();
        ensure!(
            record.namespace == *self.authority.namespace()
                && metadata.epoch == record.epoch
                && metadata.last_lsn == record.lsn,
            "WAL publication identity mismatch"
        );
        let chunk_bytes = encoded.bytes();
        let hash = encoded.hash();
        let chunk_key = format!("wal/chunks/{hash}");
        let index_bytes = serde_json::to_vec(metadata)?;
        let condition = expected.map_or(PutCondition::Absent, PutCondition::Match);
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        // Both instances of the pair ride in the publication group below: the group
        // carries the bodies, the registrations that reach them and the head CAS, so a
        // commit publishes the head once instead of reserving an intent in one revision
        // and uploading the bodies between that revision and the next. The keys are
        // fixed here, before the first attempt, so a retry rewrites the same bytes.
        self.authority.check(MutationStage::Upload)?;
        let chunk = Authority::upload_entry(chunk_bytes)?;
        let index = Authority::upload_entry(&index_bytes)?;
        let bodies = [
            PublicationBody {
                entry: &chunk,
                bytes: chunk_bytes,
            },
            PublicationBody {
                entry: &index,
                bytes: &index_bytes,
            },
        ];
        let mut added = BTreeMap::new();
        let mut validated = None;
        let mut generation = None;
        let mut attempt = 0;
        // The accepted image moves into the publication that extends it and only comes back
        // when that publication confirms this exact index. A failed or retried attempt drops
        // it instead of restoring it: a cached image that described bytes no publication
        // landed is worse than a cold replay on the next append, and the ownership move is
        // what makes the failure path impossible to miss.
        let mut previous = view.validated_wal.take();
        let next_generation = view
            .generation
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        let retired = view.retire_prefixes(&self.pin, next_generation);
        let published =
            self.authority
                .transact_with_bodies(PublishOrigin::Append, &bodies, |root| {
                    attempt += 1;
                    Self::condition(self.bound(root)?.wal.as_ref(), &condition)?;
                    self.validate_publication(root, "wal/index", &index_bytes)?;
                    ensure!(!root.objects.contains(&chunk_key)?, PreconditionFailed);
                    for entry in [&chunk, &index] {
                        ensure!(
                            !root.blobs.contains(&entry.blob)?
                                && !root.tombstones.contains(&entry.blob)?,
                            "physical instance collision"
                        );
                        // The reservation lives only inside this revision: `prepare_uploads`
                        // discharges it in the transaction that makes the instance reachable,
                        // so the durable head never carries an intent whose bytes already
                        // landed with it.
                        root.blobs.insert(entry.blob.clone(), entry.clone())?;
                        root.uploads.insert(entry.blob.clone(), entry.clone())?;
                    }
                    root.objects.insert(chunk_key.clone(), chunk.clone())?;
                    let previous = if attempt == 1 {
                        previous.take().and_then(|(entry, state)| {
                            (self.bound(root).ok()?.wal.as_ref() == Some(&entry)).then_some(state)
                        })
                    } else {
                        None
                    };
                    validated =
                        Some(self.commit_point(root, &index_bytes, &index, previous, &bodies)?);
                    let next = self.advance_generation(root, &view)?;
                    for (_, prefix) in &retired {
                        root.pin_entries.remove_prefix(prefix)?;
                    }
                    added = self.extend_pin(
                        root,
                        &view,
                        next,
                        BTreeMap::from([
                            (chunk_key.clone(), chunk.clone()),
                            ("wal/index".into(), index.clone()),
                        ]),
                    )?;
                    generation = Some(next);
                    Ok(())
                })?;
        let mut selected = Self::selected(&published.root, &self.branch)?;
        if selected.head != view.head || selected.wal.as_ref() != Some(&index) {
            // A lost acknowledgement can resolve to another writer's later point. Our
            // publication pinned its own bodies, not that point's HEAD/WAL dependencies.
            // Select and pin together; even if this pin transaction itself loses its ACK,
            // refresh from its captured point, never from another unpinned returned root.
            let publication_blobs: BTreeSet<_> =
                added.values().map(|entry| entry.blob.clone()).collect();
            let mut pinned_selected = None;
            let mut pinned_entries = BTreeMap::new();
            self.authority
                .transact_with_origin(None, PublishOrigin::PinExtend, |root| {
                    self.bound(root)?;
                    let current = Self::selected(root, &self.branch)?;
                    let mut entries = referenced(
                        &self.authority,
                        root,
                        current.head.as_ref(),
                        current.wal.as_ref(),
                    )?;
                    // These entries are already durable in this session's pin, even though
                    // the in-memory view has not yet adopted the successful publication.
                    entries.retain(|_, entry| !publication_blobs.contains(&entry.blob));
                    let next = self.advance_generation(root, &view)?;
                    pinned_entries = self.extend_pin(root, &view, next, entries)?;
                    generation = Some(next);
                    pinned_selected = Some(current);
                    Ok(())
                })?;
            selected = pinned_selected.context("branch refresh pin missing")?;
            added.extend(pinned_entries);
        }
        let keeps_image = selected.wal.as_ref() == Some(&index);
        self.refresh(
            &mut view,
            selected,
            added,
            generation.context("branch refresh generation missing")?,
            &retired,
        )?;
        if keeps_image {
            view.validated_wal = Some((index, validated.context("WAL validation missing")?));
        }
        // Otherwise the observed head names a different index: the image this attempt
        // extended describes bytes the branch does not point at, so it is dropped rather
        // than cached, and the next append validates from the store.
        Ok(())
    }
}

impl ObjectStore for BranchStore {
    fn publish_pages(&self, writes: &[crate::object_store::GroupWrite]) -> Result<bool> {
        self.publish_wal_pages(writes)?;
        Ok(true)
    }

    fn validate_local(&self) -> Result<()> {
        Ok(())
    }

    fn validate_session(&self) -> Result<()> {
        self.bound(&self.authority.snapshot()?.root)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let relative = self.relative(key)?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let entry = if let Some(entry) = self.get_entry(relative, &view) {
            Some(entry)
        } else if matches!(relative, "HEAD" | "wal/index") {
            None
        } else {
            let snapshot = self.authority.snapshot()?;
            self.bound(&snapshot.root)?;
            if !snapshot.root.objects.contains(relative)? {
                return Ok(None);
            }
            let mut selected = None;
            let mut added = BTreeMap::new();
            self.authority
                .transact_with_origin(None, PublishOrigin::PinExtend, |root| {
                    self.bound(root)?;
                    selected = root.objects.get(relative)?;
                    if let Some(entry) = &selected {
                        let entries = accessible(&self.authority, root, relative, entry)?;
                        added = self.extend_pin(root, &view, view.generation, entries)?;
                    }
                    Ok(())
                })?;
            view.admit_map(&added);
            view.objects.extend(added);
            selected
        };
        entry
            .map(|entry| {
                Ok(StoredObject {
                    bytes: if matches!(relative, "HEAD" | "wal/index") {
                        self.authority.read_metadata_blob(&entry)?
                    } else {
                        self.authority.read_blob(&entry)?
                    },
                    etag: entry.etag,
                })
            })
            .transpose()
    }

    fn get_retained(&self, key: &str) -> Result<Option<StoredObject>> {
        let relative = self.relative(key)?;
        ensure!(
            relative
                .strip_prefix(crate::wal::WAL_RECEIPT_PAGE_PREFIX)
                .is_some_and(|rest| rest.starts_with('/')),
            "retained read outside the archived receipt pages"
        );
        let hash = relative.rsplit('/').next().unwrap_or_default();
        self.authority.read_retained_page(relative, hash)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let relative = self.relative(key)?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let snapshot = self.authority.snapshot()?;
        if let Some(guard) = &self.authority.guard {
            guard.validate_root(&snapshot.root, &snapshot.root)?;
        }
        let branch = self.bound(&snapshot.root)?;
        let snapshot_owned;
        let current = match relative {
            "HEAD" => branch.head.as_ref(),
            "wal/index" => branch.wal.as_ref(),
            _ => {
                snapshot_owned = snapshot.root.objects.get(relative)?;
                snapshot_owned.as_ref()
            }
        };
        Self::condition(current, &condition)?;
        self.validate_publication(&snapshot.root, relative, bytes)?;
        let entry = self.authority.upload(bytes)?;
        let mut selected = None;
        let mut added = BTreeMap::new();
        let mut generation = None;
        let next_generation = view
            .generation
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        let retired = view.retire_prefixes(&self.pin, next_generation);
        self.authority
            .transact_with_origin(None, PublishOrigin::Put, |root| {
                let branch = self.bound(root)?;
                let owned;
                let current = match relative {
                    "HEAD" => branch.head.as_ref(),
                    "wal/index" => branch.wal.as_ref(),
                    _ => {
                        owned = root.objects.get(relative)?;
                        owned.as_ref()
                    }
                };
                Self::condition(current, &condition)?;
                self.validate_publication(root, relative, bytes)?;
                match relative {
                    "HEAD" => {
                        root.branches
                            .get_mut(&self.branch)
                            .context("branch missing")?
                            .head = Some(entry.clone())
                    }
                    "wal/index" => {
                        self.commit_point(root, bytes, &entry, None, &[])?;
                    }
                    _ => {
                        root.objects.insert(relative.to_owned(), entry.clone())?;
                    }
                }
                let entries = accessible(&self.authority, root, relative, &entry)?;
                let next = self.advance_generation(root, &view)?;
                for (_, prefix) in &retired {
                    root.pin_entries.remove_prefix(prefix)?;
                }
                added = self.extend_pin(root, &view, next, entries)?;
                generation = Some(next);
                selected = Some(Self::selected(root, &self.branch)?);
                Ok(())
            })?;
        self.refresh(
            &mut view,
            selected.context("branch publication view missing")?,
            added,
            generation.context("branch publication generation missing")?,
            &retired,
        )?;
        Ok(entry.etag)
    }

    /// Publish a batch of immutable segment bodies as one authority publication. The
    /// per-body path reserves an upload intent in one revision and registers the object in
    /// the next; a checkpoint backfill would pay that twice per 128 KiB segment. Here the
    /// bodies ride in the group, every registration is discharged in the transaction that
    /// makes it reachable, and one pin extension covers the batch.
    fn put_group(&self, writes: &[crate::object_store::GroupWrite]) -> Result<Vec<String>> {
        ensure!(!writes.is_empty(), "empty segment group");
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let snapshot = self.authority.snapshot()?;
        if let Some(guard) = &self.authority.guard {
            guard.validate_root(&snapshot.root, &snapshot.root)?;
        }
        self.bound(&snapshot.root)?;
        let mut staged = Vec::with_capacity(writes.len());
        for write in writes {
            let relative = self.relative(&write.key)?.to_owned();
            ensure!(
                relative
                    .strip_prefix("segments/")
                    .is_some_and(crate::manifest::valid_hash),
                "group publication outside the segment namespace"
            );
            let current = snapshot.root.objects.get(&relative)?;
            Self::condition(current.as_ref(), &write.condition)?;
            self.validate_publication(&snapshot.root, &relative, &write.bytes)?;
            staged.push((relative, Authority::upload_entry(&write.bytes)?, write));
        }
        let bodies = staged
            .iter()
            .map(|(_, entry, write)| PublicationBody {
                entry,
                bytes: &write.bytes,
            })
            .collect::<Vec<_>>();
        let mut added = BTreeMap::new();
        let mut selected = None;
        let mut generation = None;
        let next_generation = view
            .generation
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        let retired = view.retire_prefixes(&self.pin, next_generation);
        self.authority
            .transact_with_bodies(PublishOrigin::PutGroup, &bodies, |root| {
                self.bound(root)?;
                let mut entries = BTreeMap::new();
                for (relative, entry, write) in &staged {
                    let current = root.objects.get(relative)?;
                    Self::condition(current.as_ref(), &write.condition)?;
                    self.validate_publication(root, relative, &write.bytes)?;
                    ensure!(
                        !root.blobs.contains(&entry.blob)?
                            && !root.tombstones.contains(&entry.blob)?,
                        "physical instance collision"
                    );
                    root.blobs.insert(entry.blob.clone(), entry.clone())?;
                    root.uploads.insert(entry.blob.clone(), entry.clone())?;
                    root.objects.insert(relative.clone(), entry.clone())?;
                    entries.insert(relative.clone(), entry.clone());
                }
                let next = self.advance_generation(root, &view)?;
                for (_, prefix) in &retired {
                    root.pin_entries.remove_prefix(prefix)?;
                }
                added = self.extend_pin(root, &view, next, entries)?;
                generation = Some(next);
                selected = Some(Self::selected(root, &self.branch)?);
                Ok(())
            })?;
        self.refresh(
            &mut view,
            selected.context("branch group publication view missing")?,
            added,
            generation.context("branch group publication generation missing")?,
            &retired,
        )?;
        Ok(staged.into_iter().map(|(_, entry, _)| entry.etag).collect())
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        let relative = self.relative(key)?;
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let mut selected = None;
        let mut generation = None;
        let next_generation = view
            .generation
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        let retired = view.retire_prefixes(&self.pin, next_generation);
        self.authority
            .transact_with_origin(None, PublishOrigin::Delete, |root| {
                let branch = self.bound(root)?;
                let owned;
                let current = match relative {
                    "HEAD" => branch.head.as_ref(),
                    "wal/index" => branch.wal.as_ref(),
                    _ => {
                        owned = root.objects.get(relative)?;
                        owned.as_ref()
                    }
                };
                if let (Some(expected), Some(current)) = (expected, current) {
                    ensure!(current.etag == expected, PreconditionFailed);
                }
                match relative {
                    "HEAD" => {
                        root.branches
                            .get_mut(&self.branch)
                            .context("branch missing")?
                            .head = None
                    }
                    "wal/index" => {
                        root.branches
                            .get_mut(&self.branch)
                            .context("branch missing")?
                            .wal = None
                    }
                    _ => {
                        root.objects.remove(relative)?;
                    }
                }
                generation = Some(self.advance_generation(root, &view)?);
                for (_, prefix) in &retired {
                    root.pin_entries.remove_prefix(prefix)?;
                }
                selected = Some(Self::selected(root, &self.branch)?);
                Ok(())
            })?;
        self.refresh(
            &mut view,
            selected.context("branch deletion view missing")?,
            BTreeMap::new(),
            generation.context("branch deletion generation missing")?,
            &retired,
        )?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let namespace = self.authority.namespace.prefix();
        let relative = prefix
            .strip_prefix(&namespace)
            .context("prefix outside branch namespace")?;
        if !relative.is_empty() {
            self.authority
                .namespace
                .key(relative.strip_suffix('/').unwrap_or(relative))?;
        }
        let mut view = self
            .view
            .lock()
            .map_err(|_| anyhow::anyhow!("branch session lock poisoned"))?;
        let snapshot = self.authority.snapshot()?;
        self.bound(&snapshot.root)?;
        let listed = snapshot.root.objects.entries()?;
        if listed
            .keys()
            .any(|key| key.starts_with(relative) && !view.objects.contains_key(key))
        {
            let mut added = BTreeMap::new();
            self.authority
                .transact_with_origin(None, PublishOrigin::List, |root| {
                    self.bound(root)?;
                    let mut candidates = BTreeMap::new();
                    for (key, entry) in root
                        .objects
                        .entries()?
                        .iter()
                        .filter(|(key, _)| key.starts_with(relative))
                    {
                        if !view.objects.contains_key(key) {
                            candidates.extend(accessible(&self.authority, root, key, entry)?);
                        }
                    }
                    added = self.extend_pin(root, &view, view.generation, candidates)?;
                    Ok(())
                })?;
            view.admit_map(&added);
            view.objects.extend(added);
        }
        let mut keys: Vec<String> = view
            .objects
            .keys()
            .filter(|key| key.starts_with(relative))
            .map(|key| format!("{namespace}{key}"))
            .collect();
        if view.head.is_some() && "HEAD".starts_with(relative) {
            keys.push(format!("{namespace}HEAD"));
        }
        if view.wal.is_some() && "wal/index".starts_with(relative) {
            keys.push(format!("{namespace}wal/index"));
        }
        keys.sort();
        Ok(keys)
    }
}

impl Drop for BranchStore {
    fn drop(&mut self) {
        if !self.drop_cleanup.load(Ordering::Acquire) {
            return;
        }
        if let Err(error) = self.release_pin(&self.authority) {
            report_pin_release_failure(&self.pin, "branch-drop", &error);
        }
    }
}

pub(super) fn report_pin_release_failure(pin: &str, stage: &str, error: &anyhow::Error) {
    crate::work_metrics::observe_pin_release_failure();
    eprintln!(
        "level=error event=reader_pin_release_failed pin={pin} stage={stage} 原因：{error:#}"
    );
}

/// Release every generation of one pin. The first publication is the durable work item:
/// it advances `pin_generations` past every entry the pin currently owns and records the
/// release marker. Each generation then leaves in its own publication, so a large release
/// resumes from the marker instead of keeping one transaction open for the whole set.
pub(super) fn release_pin_identity(authority: &Authority, pin: &str) -> Result<PinReleaseStats> {
    let mut stats = PinReleaseStats::default();
    let snapshot = authority.snapshot()?;
    if !snapshot.root.pins.contains_key(pin) {
        return Ok(stats);
    }
    authority.transact_with_origin(None, PublishOrigin::PinRelease, |root| {
        if !root.pins.contains_key(pin) {
            return Ok(());
        }
        let current = root
            .pin_generations
            .get(pin)
            .copied()
            .context("reader pin generation missing")?;
        let next = current
            .checked_add(1)
            .context("reader pin generation exhausted")?;
        root.pin_generations.insert(pin.to_owned(), next);
        root.pin_releases.insert(pin.to_owned());
        Ok(())
    })?;
    stats.batches = 1;

    let snapshot = authority.snapshot()?;
    if !snapshot.root.pins.contains_key(pin) {
        return Ok(stats);
    }
    let (entries, scan) = snapshot
        .root
        .pin_entries
        .prefix_entries_counted(&format!("{pin}/"))?;
    stats.pages = stats.pages.saturating_add(scan.pages);
    stats.scanned = stats.scanned.saturating_add(scan.scanned);
    let generations: BTreeSet<u64> = entries
        .keys()
        .filter_map(|key| pin_entry_parts(key).map(|parts| parts.generation))
        .collect();
    stats.generations = generations.len();
    for generation in generations {
        let prefix = format!("{pin}/{generation}/");
        let mut removed = None;
        authority.transact_with_origin(None, PublishOrigin::PinRelease, |root| {
            if !root.pins.contains_key(pin) {
                return Ok(());
            }
            let current = root
                .pin_generations
                .get(pin)
                .copied()
                .context("reader pin generation missing")?;
            ensure!(
                generation < current,
                "current reader pin generation cannot be released"
            );
            removed = Some(root.pin_entries.remove_prefix(&prefix)?);
            Ok(())
        })?;
        if let Some(batch) = removed {
            stats.pages = stats.pages.saturating_add(batch.pages);
            stats.scanned = stats.scanned.saturating_add(batch.scanned);
            stats.entries = stats.entries.saturating_add(batch.removed);
            stats.batches = stats.batches.saturating_add(1);
        }
    }
    authority.transact_with_origin(None, PublishOrigin::PinRelease, |root| {
        if !root.pins.contains_key(pin) {
            return Ok(());
        }
        let (remaining, _) = root
            .pin_entries
            .prefix_entries_counted(&format!("{pin}/"))?;
        ensure!(remaining.is_empty(), "reader pin release left entries");
        root.pins.remove(pin);
        root.pin_generations.remove(pin);
        root.pin_releases.remove(pin);
        Ok(())
    })?;
    stats.batches = stats.batches.saturating_add(1);
    Ok(stats)
}
