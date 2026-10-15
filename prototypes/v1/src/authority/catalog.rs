use super::{Authority, CatalogRoot, ObjectEntry};
use anyhow::{ensure, Result};
use std::collections::BTreeSet;

impl Authority {
    pub(super) fn prepare(
        &self,
        previous: &CatalogRoot,
        next: &mut CatalogRoot,
        deletion: Option<&str>,
    ) -> Result<()> {
        ensure!(
            next.version == previous.version
                && next.namespace == previous.namespace
                && next.incarnation == previous.incarnation
                && next.revision == previous.revision,
            "authority root identity or revision mutation forbidden"
        );
        ensure!(
            next.receipts == previous.receipts,
            "authority receipts cannot be mutated"
        );
        ensure!(
            next.receipt_index == previous.receipt_index,
            "authority receipt index cannot be mutated"
        );
        super::ownership::catalog_rules::prepare(self, previous, next)?;
        for (name, branch) in &mut next.branches {
            if let Some(old) = previous.branches.get(name) {
                ensure!(
                    branch.incarnation >= old.incarnation
                        && branch.timeline >= old.timeline
                        && branch.sequence >= old.sequence
                        && branch.last_commit_ms >= old.last_commit_ms,
                    "branch counters cannot rewind"
                );
            } else {
                branch.incarnation = previous
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("incarnation overflow"))?;
            }
        }
        for (blob, entry) in &next.blobs {
            if let Some(old) = previous.blobs.get(blob) {
                ensure!(
                    old == entry,
                    "physical registry identity mutation forbidden"
                );
            } else {
                ensure!(
                    !previous.tombstones.contains_key(blob)
                        && next.uploads.get(blob) == Some(entry),
                    "physical instance requires durable upload reservation"
                );
            }
        }
        for blob in previous
            .blobs
            .keys()
            .filter(|blob| !next.blobs.contains_key(*blob))
        {
            ensure!(
                deletion == Some(blob.as_str())
                    && previous.tombstones.contains_key(blob)
                    && next
                        .tombstones
                        .get(blob)
                        .is_some_and(|tombstone| tombstone.deleted),
                "physical registry removal requires deletion acknowledgment"
            );
        }
        for (blob, tombstone) in &previous.tombstones {
            let next_tombstone = next
                .tombstones
                .get(blob)
                .ok_or_else(|| anyhow::anyhow!("tombstone removal forbidden"))?;
            ensure!(
                next_tombstone.entry == tombstone.entry
                    && next_tombstone.marked_ms == tombstone.marked_ms
                    && (!tombstone.deleted || next_tombstone.deleted),
                "tombstone identity cannot change"
            );
            if !tombstone.deleted && next_tombstone.deleted {
                ensure!(
                    deletion == Some(blob.as_str()),
                    "tombstone completion requires narrow deletion API"
                );
            }
        }
        for (blob, tombstone) in &next.tombstones {
            if !previous.tombstones.contains_key(blob) {
                ensure!(
                    !tombstone.deleted && previous.blobs.get(blob) == Some(&tombstone.entry),
                    "tombstone requires previously registered physical instance"
                );
                ensure!(
                    !previous.uploads.contains_key(blob) && !next.uploads.contains_key(blob),
                    "unresolved intent cannot be tombstoned"
                );
            }
        }
        let published = published(next);
        for (blob, intent) in &previous.uploads {
            if !next.uploads.contains_key(blob) {
                ensure!(
                    published.contains(blob),
                    "unpublished upload intent cannot be removed"
                );
            } else {
                ensure!(
                    next.uploads.get(blob) == Some(intent),
                    "upload intent identity mutation forbidden"
                );
            }
        }
        next.uploads.retain(|blob, _| !published.contains(blob));
        Ok(())
    }
}

fn published(root: &CatalogRoot) -> BTreeSet<String> {
    let mut entries: Vec<&ObjectEntry> = root.objects.values().collect();
    for branch in root.branches.values() {
        entries.extend(branch.head.iter().chain(branch.wal.iter()));
        for point in &branch.history {
            entries.extend(point.head.iter().chain(point.wal.iter()));
        }
    }
    for point in root.snapshots.values() {
        entries.extend(point.head.iter().chain(point.wal.iter()));
    }
    entries
        .into_iter()
        .map(|entry| entry.blob.clone())
        .collect()
}
