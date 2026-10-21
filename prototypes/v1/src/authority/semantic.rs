use super::types::{pin_entry_key, pin_entry_parts};
use super::{Authority, CatalogRoot, ObjectEntry, RecoveryPoint};
use crate::{
    manifest::{valid_hash, Head, Manifest},
    wal::{WalCommit, WalMetadata},
};
use anyhow::{ensure, Context, Result};
use std::collections::{BTreeSet, VecDeque};

#[cfg(test)]
#[path = "semantic_wal_chunks_tests.rs"]
mod wal_chunk_tests;

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

/// Read-time migration for the pre-split pin representation. It runs with the store handle
/// attached and applies the same entry and registration checks as a live pin, so a corrupt
/// legacy root fails the load instead of becoming a source of reachable-looking references.
pub(super) fn migrate_legacy_pins(root: &mut CatalogRoot) -> Result<()> {
    let legacy: Vec<(String, Vec<ObjectEntry>)> = root
        .pins
        .iter()
        .filter(|(_, entries)| !entries.is_empty())
        .map(|(pin, entries)| (pin.clone(), entries.clone()))
        .collect();
    if legacy.is_empty() {
        return Ok(());
    }
    ensure!(
        root.pin_entries.is_empty(),
        "mixed reader pin representations"
    );
    for (pin, entries) in legacy {
        ensure!(valid_hash(&pin), "invalid reader pin identity");
        for entry in entries {
            validate_entry(&entry)?;
            registered(root, &entry)?;
            let key = pin_entry_key(&pin, 0, &entry.blob);
            match root.pin_entries.get(&key)? {
                Some(previous) => ensure!(
                    previous == entry,
                    "reader pin entry identity mutation during migration"
                ),
                None => {
                    root.pin_entries.insert(key, entry)?;
                }
            }
        }
        root.pin_generations.insert(pin.clone(), 0);
        root.pins.insert(pin, Vec::new());
    }
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
            "receipt_index",
            "entry_count",
            "entry_index",
            "commit"
        ]
        .contains(&key.as_str())),
        "unknown WAL index field"
    );
    let mut index: WalMetadata = serde_json::from_value(value)?;
    index.normalize_entries()?;
    ensure!(
        index.entries.last().map_or(0, |entry| entry.lsn) == index.last_lsn,
        "incomplete WAL index"
    );
    ensure!(
        index.entries.windows(2).all(|pair| {
            pair[0].lsn.checked_add(1) == Some(pair[1].lsn)
                || (index.replay_baseline_lsn > 0
                    && pair[0].lsn == pair[1].lsn
                    && pair[0].lsn == index.replay_baseline_lsn)
        }),
        "noncontiguous WAL index"
    );
    let first = index.entries.first().map_or(0, |entry| entry.lsn);
    ensure!(
        if index.last_lsn == 0 {
            index.entries.is_empty() && index.replay_baseline_lsn == 0
        } else if index.entry_index.is_some() {
            true
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
    ensure!(
        index.receipt_index.as_deref().is_none_or(valid_hash),
        "invalid WAL receipt archive root"
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
            matches!(root.version, 1 | 2 | 4 | 5)
                && root.revision >= 1
                && root.namespace == self.namespace
                && root.incarnation == self.incarnation
                && valid_hash(&root.incarnation)
                && root
                    .pin_schema
                    .is_none_or(|schema| matches!(schema, 1 | super::types::PIN_SCHEMA_VERSION)),
            "authority root identity mismatch"
        );
        super::migration::validate(root)?;
        let index = super::paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace);
        let index = match pending {
            Some(pending) => index.deferring(pending),
            None => index,
        };
        if root.version >= 4 {
            ensure!(
                root.receipts.len() <= super::INLINE_RECEIPTS_MAX,
                "inline receipt window exceeded"
            );
            index.verify_root(root.receipt_index.as_deref())?;
        } else {
            ensure!(
                root.receipt_index.is_none(),
                "paged receipts in legacy root"
            );
        }
        if root.version >= 5 {
            let catalog = root.ownership.as_ref().context("ownership missing")?;
            index.verify_root(catalog.command_index.as_deref())?;
            ensure!(
                catalog
                    .commands
                    .values()
                    .filter(|receipt| super::terminal_receipt(receipt))
                    .count()
                    <= super::INLINE_COMMANDS_MAX,
                "inline command window exceeded"
            );
        } else if let Some(catalog) = &root.ownership {
            ensure!(
                catalog.command_index.is_none(),
                "paged commands in legacy root"
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
        for (pin, inline) in &root.pins {
            ensure!(valid_hash(pin), "invalid reader pin identity");
            ensure!(
                inline.is_empty(),
                "inline reader pin entries escaped migration"
            );
            if root.pin_schema == Some(super::types::PIN_SCHEMA_VERSION) {
                ensure!(
                    root.pin_generations.contains_key(pin),
                    "reader pin without a generation"
                );
            }
        }
        for pin in &root.pin_releases {
            ensure!(
                root.pins.contains_key(pin) && root.pin_generations.contains_key(pin),
                "reader pin release without pin"
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
                .or_else(|| key.strip_prefix("wal/receipt-pages/"))
                .or_else(|| key.strip_prefix("wal/index-segments/"))
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
        for (pin, inline) in &root.pins {
            ensure!(valid_hash(pin), "invalid reader pin identity");
            ensure!(
                inline.is_empty(),
                "inline reader pin entries escaped migration"
            );
            if let Some(generation) = root.pin_generations.get(pin) {
                ensure!(*generation < u64::MAX, "reader pin generation exhausted");
            } else {
                ensure!(
                    root.pin_schema != Some(super::types::PIN_SCHEMA_VERSION),
                    "reader pin without a generation"
                );
            }
        }
        for (key, entry) in root.pin_entries.entries()? {
            let parts = pin_entry_parts(&key).context("invalid reader pin entry key")?;
            ensure!(
                valid_hash(parts.pin) && valid_hash(parts.blob) && parts.blob == entry.blob,
                "invalid reader pin entry identity"
            );
            ensure!(
                root.pins.contains_key(parts.pin),
                "reader pin entry without pin"
            );
            if let Some(generation) = root.pin_generations.get(parts.pin) {
                ensure!(
                    parts.generation <= *generation,
                    "reader pin entry from a future generation"
                );
            }
            validate_entry(&entry)?;
            registered(root, &entry)?;
        }
        for pin in &root.pin_releases {
            ensure!(
                root.pins.contains_key(pin) && root.pin_generations.contains_key(pin),
                "reader pin release without pin"
            );
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
    pub(super) fn validate_references(
        &self,
        root: &CatalogRoot,
        bodies: &[super::PublicationBody<'_>],
    ) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("validate_refs", 0);
        for (name, branch) in &root.branches {
            validate_name(name)?;
            self.validate_head_ref(root, branch.head.as_ref(), bodies)?;
            self.validate_wal_ref(root, branch.wal.as_ref(), bodies)?;
        }
        for (name, point) in &root.snapshots {
            validate_name(name)?;
            self.validate_head_ref(root, point.head.as_ref(), bodies)?;
            self.validate_wal_ref(root, point.wal.as_ref(), bodies)?;
        }
        Ok(())
    }

    fn validate_head_ref(
        &self,
        root: &CatalogRoot,
        head: Option<&ObjectEntry>,
        bodies: &[super::PublicationBody<'_>],
    ) -> Result<()> {
        let Some(head) = head else {
            return Ok(());
        };
        registered(root, head)?;
        let decoded: Head = serde_json::from_slice(&self.metadata_bytes(bodies, head)?)?;
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
        let manifest: Manifest = serde_json::from_slice(&self.metadata_bytes(bodies, &manifest)?)?;
        manifest.validate()?;
        Ok(())
    }

    fn validate_wal_ref(
        &self,
        root: &CatalogRoot,
        wal: Option<&ObjectEntry>,
        bodies: &[super::PublicationBody<'_>],
    ) -> Result<()> {
        let Some(wal) = wal else {
            return Ok(());
        };
        registered(root, wal)?;
        index(&self.metadata_bytes(bodies, wal)?)?;
        Ok(())
    }

    /// Metadata bytes for a reference of the candidate root. A body this publication has
    /// staged wins over the store, because the group that installs it has not been
    /// written yet; everything else is read normally.
    fn metadata_bytes(
        &self,
        bodies: &[super::PublicationBody<'_>],
        entry: &ObjectEntry,
    ) -> Result<Vec<u8>> {
        match super::staged_body(bodies, entry) {
            Some(bytes) => Ok(bytes.to_vec()),
            None => self.read_metadata_blob(entry),
        }
    }

    /// Change-scoped reference validation: a branch, snapshot or logical object this
    /// transaction did not touch keeps the validation that admitted it, so a statement no
    /// longer re-walks the reachable graph over manifests and chunks.
    pub(super) fn validate_changed_references(
        &self,
        previous: &CatalogRoot,
        root: &CatalogRoot,
        bodies: &[super::PublicationBody<'_>],
    ) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("validate_refs", 0);
        let mut walker = Walker::new(self, root, false).staged(bodies);
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
        for (key, change) in root.pin_entries.changes() {
            let parts = pin_entry_parts(key).context("invalid reader pin entry key")?;
            ensure!(
                valid_hash(parts.pin) && valid_hash(parts.blob),
                "invalid reader pin entry identity"
            );
            match change {
                super::registry::Change::Added(entry) => {
                    ensure!(
                        parts.blob == entry.blob,
                        "invalid reader pin entry identity"
                    );
                    ensure!(
                        root.pins.contains_key(parts.pin),
                        "reader pin entry without pin"
                    );
                    // A pin may only grow its current generation. Writing into an older
                    // generation would let a transaction keep protection that the release
                    // path is allowed to delete.
                    if let Some(generation) = root.pin_generations.get(parts.pin) {
                        ensure!(
                            parts.generation == *generation,
                            "reader pin entry outside the current generation"
                        );
                    }
                    validate_entry(entry)?;
                    registered(root, entry)?;
                }
                super::registry::Change::Removed => {
                    // The current generation may only leave with the pin itself. An older
                    // generation is exactly what a superseded snapshot retire removes.
                    if let Some(generation) = root.pin_generations.get(parts.pin) {
                        ensure!(
                            !root.pins.contains_key(parts.pin) || parts.generation < *generation,
                            "reader pin entry removal requires pin removal or a retired generation"
                        );
                    } else {
                        ensure!(
                            !root.pins.contains_key(parts.pin),
                            "reader pin entry removal requires pin removal"
                        );
                    }
                }
            }
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
                .or_else(|| key.strip_prefix("wal/receipt-pages/"))
                .or_else(|| key.strip_prefix("wal/index-segments/"))
            {
                ensure!(
                    valid_hash(hash) && hash == entry.hash,
                    "immutable logical hash mismatch"
                );
            }
        }
        Ok(())
    }

    /// Physical blob identities reachable from the durable references. Registry pages are
    /// intentionally outside this mark: they are immutable logical metadata, not members of
    /// the physical blob registry, and this walk does not drive page reclamation.
    ///
    /// `aliases` also counts blobs named by the logical object registry. The sweep path
    /// must not: prune has removed the orphan aliases by then, and a completed tombstone
    /// must stay deletable. The mark path must, because an alias still keeps the bytes
    /// reachable and a tombstone before its alias is gone would leave an unopenable head.
    pub(super) fn mark(&self, root: &CatalogRoot, aliases: bool) -> Result<BTreeSet<String>> {
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
        let pins = root.pin_entries.entries()?;
        for entry in pins.values().chain(uploads.values()) {
            walker.direct(entry)?;
        }
        if aliases {
            for entry in root.objects.entries()?.values() {
                walker.direct(entry)?;
            }
        }
        Ok(walker.reachable)
    }
}

struct Walker<'a> {
    authority: &'a Authority,
    root: &'a CatalogRoot,
    /// Bodies the candidate root references that the publication group has not written
    /// yet. Validation runs before the group is written, so without these the walker
    /// would reject the publication that is about to install them.
    staged: &'a [super::PublicationBody<'a>],
    verified: BTreeSet<String>,
    verified_order: VecDeque<String>,
    verified_chunks: BTreeSet<String>,
    verified_chunk_order: VecDeque<String>,
    collect_reachable: bool,
    reachable: BTreeSet<String>,
}

const VERIFIED_METADATA_IDENTITIES: usize = 128;
const VERIFIED_WAL_CHUNKS: usize = 4096;

impl<'a> Walker<'a> {
    fn new(authority: &'a Authority, root: &'a CatalogRoot, collect_reachable: bool) -> Self {
        Self {
            authority,
            root,
            staged: &[],
            verified: BTreeSet::new(),
            verified_order: VecDeque::new(),
            verified_chunks: BTreeSet::new(),
            verified_chunk_order: VecDeque::new(),
            collect_reachable,
            reachable: BTreeSet::new(),
        }
    }

    fn staged(mut self, bodies: &'a [super::PublicationBody<'a>]) -> Self {
        self.staged = bodies;
        self
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
        if let Some(bytes) = super::staged_body(self.staged, entry) {
            return Ok(bytes.to_vec());
        }
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
            if !self.was_verified('W', wal) {
                let index = index(&self.bytes(wal)?)?;
                let chunks = crate::recovery::resolve_entries(&index, |hash| {
                    let entry = self.lookup(&format!("wal/index-segments/{hash}"))?;
                    ensure!(entry.hash == hash, "WAL index segment digest mismatch");
                    self.bytes(&entry)
                })?;
                for chunk in &chunks {
                    self.wal_chunk(&chunk.hash)?;
                }
                self.wal_receipt_pages(&index)?;
                // A shallow index check does not prove its chunks were registered.
                self.remember_verified('W', wal);
            }
        }
        Ok(())
    }

    /// Archived receipts are reachable through the index head, so GC must mark their pages
    /// or collection could delete the only copy of an idempotency answer.
    fn wal_receipt_pages(&mut self, index: &crate::wal::WalMetadata) -> Result<()> {
        let Some(root) = index.receipt_index.as_deref() else {
            return Ok(());
        };
        let mut archived_identities = BTreeSet::new();
        crate::authority::paged_index::walk_tree_with(
            Some(root),
            |hash| {
                let entry = self.lookup(&format!("wal/receipt-pages/{hash}"))?;
                ensure!(entry.hash == hash, "WAL receipt page digest mismatch");
                self.direct(&entry)?;
                self.authority.read_metadata_blob(&entry)
            },
            |entries| {
                for (identity, value) in entries {
                    ensure!(
                        archived_identities.insert(identity.clone()),
                        "duplicate archived WAL receipt"
                    );
                    ensure!(
                        !identity.is_empty()
                            && identity.len() <= 128
                            && identity
                                .bytes()
                                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
                        "invalid archived WAL receipt identity"
                    );
                    ensure!(
                        !index.receipts.contains_key(identity),
                        "WAL receipt archived and inline"
                    );
                    let archived: crate::wal::ArchivedReceipt =
                        serde_json::from_value(value.clone())?;
                    ensure!(
                        archived.lsn > 0 && archived.lsn <= index.last_lsn,
                        "archived WAL receipt outside the WAL"
                    );
                    if let Some(digest) = archived.digest {
                        ensure!(valid_hash(&digest), "invalid archived WAL receipt digest");
                    }
                }
                Ok(())
            },
        )
    }

    fn wal_chunk(&mut self, hash: &str) -> Result<()> {
        if self.verified_chunks.contains(hash) {
            return Ok(());
        }
        ensure!(valid_hash(hash), "invalid WAL chunk digest");
        let entry = self.lookup(&format!("wal/chunks/{hash}"))?;
        ensure!(entry.hash == hash, "WAL chunk logical digest mismatch");
        self.direct(&entry)?;
        // This walker borrows one immutable root. Shared history prefixes resolve to
        // the same logical mapping and registration, and GC already collected the blob
        // on its first visit. Failed checks never populate this bounded positive cache.
        self.verified_chunks.insert(hash.to_owned());
        self.verified_chunk_order.push_back(hash.to_owned());
        if self.verified_chunk_order.len() > VERIFIED_WAL_CHUNKS {
            self.verified_chunks
                .remove(&self.verified_chunk_order.pop_front().unwrap());
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
