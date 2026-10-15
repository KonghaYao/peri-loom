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
    validate_entry(entry)?;
    ensure!(
        root.blobs.get(&entry.blob) == Some(entry),
        "unregistered physical object reference"
    );
    ensure!(
        !root.tombstones.contains_key(&entry.blob),
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
    pub(super) fn validate_root(&self, root: &CatalogRoot) -> Result<()> {
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
            super::paged_index::PagedIndex::new(self.store.as_ref(), &self.namespace)
                .verify_root(root.receipt_index.as_deref())?;
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
        for (blob, entry) in &root.blobs {
            validate_entry(entry)?;
            ensure!(blob == &entry.blob, "physical registry key mismatch");
            if let Some(tombstone) = root.tombstones.get(blob) {
                ensure!(
                    !tombstone.deleted && tombstone.entry == *entry,
                    "deleted physical instance remains registered"
                );
            }
        }
        for (blob, tombstone) in &root.tombstones {
            validate_entry(&tombstone.entry)?;
            ensure!(
                blob == &tombstone.entry.blob && !root.uploads.contains_key(blob),
                "invalid tombstone or active intent"
            );
            ensure!(
                if tombstone.deleted {
                    !root.blobs.contains_key(blob)
                } else {
                    root.blobs.get(blob) == Some(&tombstone.entry)
                },
                "tombstone registry mismatch"
            );
        }
        for (blob, entry) in &root.uploads {
            ensure!(blob == &entry.blob, "upload intent key mismatch");
            registered(root, entry)?;
        }
        let mut walker = Walker::new(self, root, false);
        for (key, entry) in &root.objects {
            self.namespace.key(key)?;
            ensure!(
                key != "HEAD" && key != "wal/index",
                "mutable branch key in immutable catalog"
            );
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
        for entry in root.pins.values().flatten().chain(root.uploads.values()) {
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
            .get(key)
            .cloned()
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

    fn refs(&mut self, head: Option<&ObjectEntry>, wal: Option<&ObjectEntry>) -> Result<()> {
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
