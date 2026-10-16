use super::{Authority, CatalogRoot, ObjectEntry, RecoveryPoint};
use crate::{
    manifest::{valid_hash, Head, Manifest},
    wal::{WalCommit, WalMetadata},
};
use anyhow::{ensure, Context, Result};
use std::collections::{BTreeSet, VecDeque};

pub(super) fn validate_entry(entry: &ObjectEntry) -> Result<()> {
    ensure!(
        valid_hash(&entry.blob) && valid_hash(&entry.hash) && valid_hash(&entry.etag),
        "invalid catalog object identity"
    );
    Ok(())
}

pub(super) fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "invalid branch or reference name"
    );
    Ok(())
}

pub(super) fn registered(root: &CatalogRoot, entry: &ObjectEntry) -> Result<()> {
    #[cfg(test)]
    let _probe = super::scaling_probe::stage("ref_registered", 0);
    validate_entry(entry)?;
    ensure!(
        root.blobs.get(&entry.blob)?.as_ref() == Some(entry),
        "unregistered physical object reference"
    );
    ensure!(
        !root.tombstones.contains(&entry.blob)?,
        "reference to tombstoned physical instance"
    );
    Ok(())
}

pub(super) fn index(bytes: &[u8]) -> Result<WalMetadata> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    let fields = value.as_object().context("WAL index must be an object")?;
    ensure!(
        fields.keys().all(|key| [
            "epoch",
            "last_lsn",
            "replay_baseline_lsn",
            "entries",
            "receipts",
            "receipt_digests",
            "commit"
        ]
        .contains(&key.as_str())),
        "unknown WAL index field"
    );
    let index: WalMetadata = serde_json::from_value(value)?;
    ensure!(
        index.entries.last().map_or(0, |entry| entry.lsn) == index.last_lsn,
        "incomplete WAL index"
    );
    ensure!(
        index
            .entries
            .windows(2)
            .all(|pair| pair[0].lsn.checked_add(1) == Some(pair[1].lsn)),
        "noncontiguous WAL index"
    );
    let first = index.entries.first().map_or(0, |entry| entry.lsn);
    ensure!(
        if index.last_lsn == 0 {
            index.entries.is_empty() && index.replay_baseline_lsn == 0
        } else if index.replay_baseline_lsn == 0 {
            first == 1
        } else {
            first == index.replay_baseline_lsn
        },
        "invalid WAL baseline"
    );
    ensure!(
        index
            .receipts
            .values()
            .all(|lsn| *lsn > 0 && *lsn <= index.last_lsn),
        "invalid WAL receipt"
    );
    ensure!(
        index
            .receipt_digests
            .iter()
            .all(|(identity, digest)| index.receipts.contains_key(identity) && valid_hash(digest)),
        "invalid WAL receipt digest"
    );
    if let Some(WalCommit { lsn, position }) = index.commit {
        ensure!(
            lsn > 0 && lsn == index.last_lsn && position.frame_count > 0,
            "invalid WAL commit marker"
        );
    }
    Ok(index)
}

impl Authority {
    /// Cheap validation for every statement: identity, receipt window and the
    /// identity-bounded head state. Volume registries are validated when they change
    /// (`catalog::prepare`) and every page read re-verifies its digest, so a statement no
    /// longer walks the whole store the way the monolithic head had to.
    pub(super) fn validate_head(&self, root: &CatalogRoot) -> Result<()> {
        self.validate_head_with(root, None)
    }

    /// Candidate validation runs before the publication group is written, so a receipt
    /// page this attempt created is only readable from the staged group buffer.
    pub(super) fn validate_head_with(
        &self,
        root: &CatalogRoot,
        pending: Option<&std::cell::RefCell<super::paged_index::PendingPages>>,
    ) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("validate", 0);
        ensure!(
            matches!(root.version, 1 | 2 | 4)
                && root.revision >= 1
                && root.namespace == self.namespace
                && root.incarnation == self.incarnation
                && valid_hash(&root.incarnation),
            "authority root identity mismatch"
        );
        super::migration::validate(root)?;
        if root.version == 4 {
            ensure!(
                root.receipts.len() <= super::INLINE_RECEIPTS_MAX,
                "inline receipt window exceeded"
            );
            let index = super::paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace);
            let index = match pending {
                Some(pending) => index.deferring(pending),
                None => index,
            };
            index.verify_root(root.receipt_index.as_deref())?;
        } else {
            ensure!(
                root.receipt_index.is_none(),
                "paged receipts in legacy root"
            );
        }
        ensure!(
            root.receipts.iter().all(|(id, revision)| valid_hash(id)
                && *revision > 0
                && *revision <= root.revision),
            "invalid authority receipt"
        );
        for (name, branch) in &root.branches {
            validate_name(name)?;
            ensure!(
                branch.incarnation > 0
                    && branch.timeline > 0
                    && branch.current_sequence <= branch.sequence
                    && branch.current_commit_ms <= branch.last_commit_ms,
                "invalid branch counters"
            );
        }
        Ok(())
    }

    /// Full walk over every registry entry and reachable reference. Boot and maintenance
    /// paths pay for this once; statements never do.
    pub(super) fn verify_deep(&self, root: &CatalogRoot) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("verify_deep", 0);
        self.validate_head(root)?;
        for (blob, entry) in root.blobs.entries()? {
            validate_entry(&entry)?;
            ensure!(blob == entry.blob, "physical registry key mismatch");
            if let Some(tombstone) = root.tombstones.get(&blob)? {
                ensure!(
                    !tombstone.deleted && tombstone.entry == entry,
                    "deleted physical instance remains registered"
                );
            }
        }
        for (blob, tombstone) in root.tombstones.entries()? {
            validate_entry(&tombstone.entry)?;
            ensure!(
                blob == tombstone.entry.blob && !root.uploads.contains(&blob)?,
                "invalid tombstone or active intent"
            );
            ensure!(
                if tombstone.deleted {
                    !root.blobs.contains(&blob)?
                } else {
                    root.blobs.get(&blob)? == Some(tombstone.entry)
                },
                "tombstone registry mismatch"
            );
        }
        for (blob, entry) in root.uploads.entries()? {
            ensure!(blob == entry.blob, "upload intent key mismatch");
            registered(root, &entry)?;
        }
        let mut walker = Walker::new(self, root, false);
        for (key, entry) in root.objects.entries()? {
            self.namespace.key(&key)?;
            ensure!(
                key != "HEAD" && key != "wal/index",
                "mutable branch key in immutable catalog"
            );
            registered(root, &entry)?;
            if let Some(hash) = key.strip_prefix("manifests/") {
                ensure!(
                    valid_hash(hash) && hash == entry.hash,
                    "manifest logical hash mismatch"
                );
                walker.manifest(&entry)?;
            } else if let Some(hash) = key
                .strip_prefix("segments/")
                .or_else(|| key.strip_prefix("wal/chunks/"))
            {
                ensure!(
                    valid_hash(hash) && hash == entry.hash,
                    "immutable logical hash mismatch"
                );
            }
        }
        for (name, branch) in &root.branches {
            validate_name(name)?;
            ensure!(
                branch.incarnation > 0
                    && branch.timeline > 0
                    && branch.current_sequence <= branch.sequence
                    && branch.current_commit_ms <= branch.last_commit_ms,
                "invalid branch counters"
            );
            walker.refs(branch.head.as_ref(), branch.wal.as_ref())?;
            let mut previous = 0;
            for point in &branch.history {
                ensure!(
                    point.sequence > previous && point.sequence <= branch.sequence,
                    "invalid branch recovery history ordering"
                );
                previous = point.sequence;
                walker.point(point)?;
            }
        }
        for (name, point) in &root.snapshots {
            validate_name(name)?;
            walker.point(point)?;
        }
        for (pin, entries) in &root.pins {
            ensure!(valid_hash(pin), "invalid reader pin identity");
            for entry in entries {
                registered(root, entry)?;
            }
        }
        Ok(())
    }

    /// Metadata integrity for the references a branch or snapshot currently publishes:
    /// the referenced HEAD, manifest and WAL index blobs are read and digest-checked and
    /// their entries must still be registered, so a corrupted or unregistered dependency
    /// fails closed on a statement and on a publication instead of being ignored until
    /// someone reads it.
    ///
    /// It deliberately never enumerates registry contents: walking a manifest's segments
    /// or a WAL index's chunks is what made a statement linear in the catalog, and that
    /// work now happens when a reference changes (`validate_changed_references`), at boot
    /// (`verify_deep`), on read and in garbage collection. It also never walks the
    /// retained recovery history: every point in it was the current reference of the
    /// publication that appended it and is validated here when it is used
    /// (`capabilities`) or swept at boot, so re-reading the whole history on every
    /// statement only re-reads immutable, digest-checked state. Whether that is even true
    /// of the head in hand is decided one level up: a head this process did not verify is
    /// resolved by `verify_deep` before a statement trusts it, so the change-scoped path
    /// never sees a head whose references were never registered.
    pub(super) fn validate_references(&self, root: &CatalogRoot) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("validate_refs", 0);
        for (name, branch) in &root.branches {
            validate_name(name)?;
            self.validate_head_ref(root, branch.head.as_ref())?;
            self.validate_wal_ref(root, branch.wal.as_ref())?;
        }
        for (name, point) in &root.snapshots {
            validate_name(name)?;
            self.validate_head_ref(root, point.head.as_ref())?;
            self.validate_wal_ref(root, point.wal.as_ref())?;
        }
        Ok(())
    }

    fn validate_head_ref(&self, root: &CatalogRoot, head: Option<&ObjectEntry>) -> Result<()> {
        let Some(head) = head else {
            return Ok(());
        };
        registered(root, head)?;
        let decoded: Head = serde_json::from_slice(&self.read_metadata_blob(head)?)?;
        ensure!(
            valid_hash(&decoded.manifest),
            "invalid HEAD manifest digest"
        );
        let manifest = root
            .objects
            .get(&format!("manifests/{}", decoded.manifest))?
            .context("HEAD manifest missing")?;
        ensure!(
            manifest.hash == decoded.manifest,
            "HEAD manifest digest mismatch"
        );
        registered(root, &manifest)?;
        let manifest: Manifest = serde_json::from_slice(&self.read_metadata_blob(&manifest)?)?;
        manifest.validate()?;
        Ok(())
    }

    fn validate_wal_ref(&self, root: &CatalogRoot, wal: Option<&ObjectEntry>) -> Result<()> {
        let Some(wal) = wal else {
            return Ok(());
        };
        registered(root, wal)?;
        index(&self.read_metadata_blob(wal)?)?;
        Ok(())
    }

    /// Change-scoped reference validation: a branch, snapshot or logical object this
    /// transaction did not touch keeps the validation that admitted it, so a statement no
    /// longer re-walks the reachable graph over manifests and chunks.
    pub(super) fn validate_changed_references(
        &self,
        previous: &CatalogRoot,
        root: &CatalogRoot,
    ) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("validate_refs", 0);
        let mut walker = Walker::new(self, root, false);
        for (name, branch) in &root.branches {
            let admitted = previous.branches.get(name);
            if admitted.is_some_and(|old| old.unchanged_refs(branch)) {
                continue;
            }
            validate_name(name)?;
            let mut prior = 0;
            for point in &branch.history {
                ensure!(
                    point.sequence > prior && point.sequence <= branch.sequence,
                    "invalid branch recovery history ordering"
                );
                prior = point.sequence;
            }
            // Only the references this publication moved are re-resolved. A retained
            // recovery point holds the head and WAL of an earlier publication, both
            // validated when that publication admitted them and immutable since, so
            // walking the history here would repeat work proportional to the session
            // length instead of to the change. `verify_deep` owns the sweep.
            let old_head = admitted.and_then(|old| old.head.as_ref());
            let old_wal = admitted.and_then(|old| old.wal.as_ref());
            if old_head != branch.head.as_ref() {
                walker.head(branch.head.as_ref())?;
            }
            if old_wal != branch.wal.as_ref() {
                walker.wal_index(branch.wal.as_ref())?;
            }
        }
        for (name, point) in &root.snapshots {
            if previous.snapshots.get(name) == Some(point) {
                continue;
            }
            validate_name(name)?;
            walker.point(point)?;
        }
        for (key, change) in root.objects.changes() {
            let super::registry::Change::Added(entry) = change else {
                continue;
            };
            self.namespace.key(key)?;
            ensure!(
                key != "HEAD" && key != "wal/index",
                "mutable branch key in immutable catalog"
            );
            // A newly registered logical object must reference a live physical instance:
            // without this, a tombstoned blob could be re-advertised as a catalog entry.
            registered(root, entry)?;
            if let Some(hash) = key.strip_prefix("manifests/") {
                ensure!(
                    valid_hash(hash) && hash == entry.hash,
                    "manifest logical hash mismatch"
                );
                walker.manifest(entry)?;
            } else if let Some(hash) = key
                .strip_prefix("segments/")
                .or_else(|| key.strip_prefix("wal/chunks/"))
            {
                ensure!(
                    valid_hash(hash) && hash == entry.hash,
                    "immutable logical hash mismatch"
                );
            }
        }
        Ok(())
    }

    pub(super) fn mark(&self, root: &CatalogRoot) -> Result<BTreeSet<String>> {
        let mut walker = Walker::new(self, root, true);
        for branch in root.branches.values() {
            walker.refs(branch.head.as_ref(), branch.wal.as_ref())?;
            for point in &branch.history {
                walker.point(point)?;
            }
        }
        for point in root.snapshots.values() {
            walker.point(point)?;
        }
        let uploads = root.uploads.entries()?;
        for entry in root.pins.values().flatten().chain(uploads.values()) {
            walker.direct(entry)?;
        }
        Ok(walker.reachable)
    }
}

struct Walker<'a> {
    authority: &'a Authority,
    root: &'a CatalogRoot,
    verified: BTreeSet<String>,
    verified_order: VecDeque<String>,
    collect_reachable: bool,
    reachable: BTreeSet<String>,
}

const VERIFIED_METADATA_IDENTITIES: usize = 128;

impl<'a> Walker<'a> {
    fn new(authority: &'a Authority, root: &'a CatalogRoot, collect_reachable: bool) -> Self {
        Self {
            authority,
            root,
            verified: BTreeSet::new(),
            verified_order: VecDeque::new(),
            collect_reachable,
            reachable: BTreeSet::new(),
        }
    }

    fn direct(&mut self, entry: &ObjectEntry) -> Result<()> {
        registered(self.root, entry)?;
        if self.collect_reachable {
            self.reachable.insert(entry.blob.clone());
        }
        Ok(())
    }

    fn bytes(&mut self, entry: &ObjectEntry) -> Result<Vec<u8>> {
        self.direct(entry)?;
        self.authority.read_metadata_blob(entry)
    }

    fn was_verified(&mut self, kind: char, entry: &ObjectEntry) -> bool {
        self.verified.contains(&format!("{kind}:{}", entry.blob))
    }

    fn remember_verified(&mut self, kind: char, entry: &ObjectEntry) {
        let key = format!("{kind}:{}", entry.blob);
        if self.verified.insert(key.clone()) {
            self.verified_order.push_back(key);
            if self.verified_order.len() > VERIFIED_METADATA_IDENTITIES {
                self.verified
                    .remove(&self.verified_order.pop_front().unwrap());
            }
        }
    }

    fn lookup(&self, key: &str) -> Result<ObjectEntry> {
        self.root
            .objects
            .get(key)?
            .context("missing logical metadata dependency")
    }

    fn manifest(&mut self, entry: &ObjectEntry) -> Result<()> {
        self.direct(entry)?;
        if self.was_verified('m', entry) {
            return Ok(());
        }
        let manifest: Manifest = serde_json::from_slice(&self.bytes(entry)?)?;
        manifest.validate()?;
        for hash in manifest.segments.values() {
            let segment = self.lookup(&format!("segments/{hash}"))?;
            ensure!(segment.hash == *hash, "segment logical digest mismatch");
            self.direct(&segment)?;
        }
        self.remember_verified('m', entry);
        Ok(())
    }

    /// Full walk of a branch reference: the HEAD manifest's segments and the WAL index's
    /// chunks are each resolved to their registered physical instance. Boot
    /// (`verify_deep`) and garbage collection (`mark`) own this sweep; a publication
    /// resolves only its change, see `head` and `wal_index`.
    fn refs(&mut self, head: Option<&ObjectEntry>, wal: Option<&ObjectEntry>) -> Result<()> {
        self.head(head)?;
        self.wal_chunks(wal)
    }

    fn head(&mut self, head: Option<&ObjectEntry>) -> Result<()> {
        if let Some(head) = head {
            self.direct(head)?;
            if !self.was_verified('h', head) {
                let decoded: Head = serde_json::from_slice(&self.bytes(head)?)?;
                ensure!(
                    valid_hash(&decoded.manifest),
                    "invalid HEAD manifest digest"
                );
                let manifest = self.lookup(&format!("manifests/{}", decoded.manifest))?;
                ensure!(
                    manifest.hash == decoded.manifest,
                    "HEAD manifest digest mismatch"
                );
                self.manifest(&manifest)?;
                self.remember_verified('h', head);
            }
        }
        Ok(())
    }

    /// The WAL index without its chunk sweep. A publication's index is validated where it
    /// is built: `recovery::validate_index_append` proves the accepted entries are the
    /// byte-identical prefix of the index it extends and resolves the entry it adds, and
    /// `validate_index_state` enumerates a rebuilt index in full. Both run before the
    /// index reaches a branch reference, so resolving every retained chunk again here
    /// would make each statement linear in the session length.
    fn wal_index(&mut self, wal: Option<&ObjectEntry>) -> Result<()> {
        let Some(wal) = wal else {
            return Ok(());
        };
        self.direct(wal)?;
        if !self.was_verified('w', wal) {
            index(&self.bytes(wal)?)?;
            self.remember_verified('w', wal);
        }
        Ok(())
    }

    fn wal_chunks(&mut self, wal: Option<&ObjectEntry>) -> Result<()> {
        if let Some(wal) = wal {
            self.direct(wal)?;
            if !self.was_verified('w', wal) {
                let index = index(&self.bytes(wal)?)?;
                for chunk in index.entries {
                    ensure!(valid_hash(&chunk.hash), "invalid WAL chunk digest");
                    let entry = self.lookup(&format!("wal/chunks/{}", chunk.hash))?;
                    ensure!(
                        entry.hash == chunk.hash,
                        "WAL chunk logical digest mismatch"
                    );
                    self.direct(&entry)?;
                }
                self.remember_verified('w', wal);
            }
        }
        Ok(())
    }

    fn point(&mut self, point: &RecoveryPoint) -> Result<()> {
        ensure!(
            valid_hash(&point.id) && point.timeline > 0,
            "invalid recovery point identity"
        );
        validate_name(&point.branch)?;
        self.refs(point.head.as_ref(), point.wal.as_ref())
    }
}
