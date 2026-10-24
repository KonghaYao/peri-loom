use super::{registry::Change, Authority, CatalogRoot, ObjectEntry};
use anyhow::{ensure, Context, Result};
use std::collections::BTreeSet;

impl Authority {
    /// Change-set validation. The durable registries are no longer compared entry by
    /// entry on every statement: each `Registry` records the keys this transaction
    /// touched, so only those keys are re-checked against the previous head while the
    /// rest of the registry stays covered by the validation that admitted it.
    pub(super) fn prepare_with(
        &self,
        previous: &CatalogRoot,
        next: &mut CatalogRoot,
        deletions: &BTreeSet<String>,
        bodies: &[super::PublicationBody<'_>],
        scope: super::PublicationScope,
    ) -> Result<()> {
        #[cfg(test)]
        let _probe = super::scaling_probe::stage("prepare", 0);
        let local_migration = scope == super::PublicationScope::LocalMigration;
        // Schema 1 is upgraded once, when the authority opens. The upgrade replaces the
        // pin registry base (a hash-ordered tree becomes a raw-ordered one), which no
        // ordinary statement may do; every other publication must keep the same base.
        let pin_upgrade = previous.pin_schema != Some(super::types::PIN_SCHEMA_VERSION)
            && next.pin_schema == Some(super::types::PIN_SCHEMA_VERSION);
        // A local migration is the only publication allowed to move the durable version.
        // It may only move forward, and only to the local schema; a statement can never
        // take this branch because it passes `Statement`.
        let version_change = local_migration
            && next.version == super::types::LOCAL_ROOT_VERSION
            && (previous.version == next.version || matches!(previous.version, 1 | 2 | 4 | 5));
        ensure!(
            (next.version == previous.version || version_change)
                && next.namespace == previous.namespace
                && next.incarnation == previous.incarnation
                && next.revision == previous.revision
                && (next.pin_schema == previous.pin_schema || pin_upgrade),
            "authority root identity or revision mutation forbidden"
        );
        ensure!(
            local_migration || next.receipts == previous.receipts,
            "authority receipts cannot be mutated"
        );
        ensure!(
            local_migration || next.receipt_index == previous.receipt_index,
            "authority receipt index cannot be mutated"
        );
        // Collection progress is durable state: a publication may advance it, but it may not
        // rewind a frontier or forget that a cycle is in flight. Without this check a
        // transaction could make the next tick rescan (or skip) arbitrary keys.
        ensure!(
            !(previous.gc.is_some() && next.gc.is_none()),
            "GC cursor removal forbidden"
        );
        if let Some(cursor) = &next.gc {
            ensure!(
                cursor.follows(previous.gc.as_ref()),
                "GC cursor cannot rewind"
            );
        }
        // A surviving pin's registry base is part of the admission state. Replacing the
        // whole registry (empty, an old snapshot, or a smaller page tree) would carry an
        // empty change set and hide a removal from change-scoped validation. Additions stay
        // legal because `extend_pin` grows a live pin; removals and replacements are handled
        // by the change-scoped checks below.
        ensure!(
            next.pin_entries.same_base(&previous.pin_entries) || pin_upgrade,
            "reader pin registry replacement forbidden"
        );
        for (pin, generation) in &next.pin_generations {
            ensure!(
                next.pins.contains_key(pin),
                "reader pin generation without pin"
            );
            if let Some(admitted) = previous.pin_generations.get(pin) {
                ensure!(
                    generation >= admitted,
                    "reader pin generation cannot rewind"
                );
            }
        }
        for pin in &next.pin_releases {
            ensure!(
                next.pins.contains_key(pin) && next.pin_generations.contains_key(pin),
                "reader pin release without pin"
            );
        }
        for pin in &previous.pin_releases {
            ensure!(
                next.pin_releases.contains(pin) || !next.pins.contains_key(pin),
                "reader pin release marker cannot be dropped while the pin survives"
            );
        }
        if !local_migration {
            super::ownership::catalog_rules::prepare(self, previous, next)?;
        }
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

        let published = self.published_blobs(previous, next)?;
        self.prepare_blobs(previous, next, deletions)?;
        self.prepare_objects(previous, next)?;
        self.prepare_tombstones(previous, next, deletions)?;
        self.prepare_uploads(previous, next, &published)?;
        self.prepare_pin_removals(previous, next)?;
        self.validate_changed_references(previous, next, bodies)?;
        // Additions are validated against the change set. A removal can invalidate a
        // reference held by state this transaction never touched, and there is no reverse
        // index to prove otherwise cheaply, so removals fall back to the full walk. That
        // path belongs to garbage collection and deletion, not to ordinary statements.
        if [&next.blobs, &next.objects].iter().any(|registry| {
            registry
                .changes()
                .any(|(_, change)| matches!(change, Change::Removed))
        }) {
            self.verify_deep(next)?;
        }
        Ok(())
    }

    /// Blobs this transaction made reachable. Reader pins are deliberately excluded:
    /// pin entries are always published through `objects` or a branch reference first,
    /// so excluding them keeps this cost proportional to the change, not to the branch.
    fn published_blobs(
        &self,
        previous: &CatalogRoot,
        next: &CatalogRoot,
    ) -> Result<BTreeSet<String>> {
        let mut published = BTreeSet::new();
        for (_, change) in next.objects.changes() {
            if let Change::Added(entry) = change {
                published.insert(entry.blob.clone());
            }
        }
        for (name, branch) in &next.branches {
            if previous
                .branches
                .get(name)
                .is_some_and(|old| old.unchanged_refs(branch))
            {
                continue;
            }
            for entry in branch.head.iter().chain(branch.wal.iter()) {
                published.insert(entry.blob.clone());
            }
            for point in &branch.history {
                for entry in point.head.iter().chain(point.wal.iter()) {
                    published.insert(entry.blob.clone());
                }
            }
        }
        for (name, point) in &next.snapshots {
            if previous.snapshots.get(name) == Some(point) {
                continue;
            }
            for entry in point.head.iter().chain(point.wal.iter()) {
                published.insert(entry.blob.clone());
            }
        }
        Ok(published)
    }

    fn prepare_blobs(
        &self,
        previous: &CatalogRoot,
        next: &mut CatalogRoot,
        deletions: &BTreeSet<String>,
    ) -> Result<()> {
        let changes: Vec<(String, Change<ObjectEntry>)> = next
            .blobs
            .changes()
            .map(|(key, change)| (key.clone(), change.clone()))
            .collect();
        for (blob, change) in changes {
            match change {
                Change::Added(entry) => {
                    if let Some(previous_entry) = previous.blobs.get(&blob)? {
                        ensure!(
                            previous_entry == entry,
                            "physical registry identity mutation forbidden"
                        );
                    } else {
                        ensure!(
                            !previous.tombstones.contains(&blob)?
                                && next.uploads.get(&blob)?.as_ref() == Some(&entry),
                            "physical instance requires durable upload reservation"
                        );
                    }
                }
                Change::Removed => {
                    ensure!(
                        deletions.contains(&blob)
                            && previous.tombstones.get(&blob)?.is_some()
                            && next
                                .tombstones
                                .get(&blob)?
                                .is_some_and(|tombstone| tombstone.deleted),
                        "physical registry removal requires deletion acknowledgment"
                    );
                }
            }
        }
        Ok(())
    }

    /// Logical registry entries are immutable: a key that already exists may only be
    /// re-published with the identical entry, exactly what the deep walk used to prove.
    fn prepare_objects(&self, previous: &CatalogRoot, next: &mut CatalogRoot) -> Result<()> {
        let changes: Vec<(String, Change<ObjectEntry>)> = next
            .objects
            .changes()
            .map(|(key, change)| (key.clone(), change.clone()))
            .collect();
        for (key, change) in changes {
            let Change::Added(entry) = change else {
                continue;
            };
            if let Some(previous_entry) = previous.objects.get(&key)? {
                ensure!(
                    previous_entry == entry,
                    "logical registry identity mutation forbidden"
                );
            }
        }
        Ok(())
    }

    fn prepare_tombstones(
        &self,
        previous: &CatalogRoot,
        next: &mut CatalogRoot,
        deletions: &BTreeSet<String>,
    ) -> Result<()> {
        let changes: Vec<(String, Change<super::Tombstone>)> = next
            .tombstones
            .changes()
            .map(|(key, change)| (key.clone(), change.clone()))
            .collect();
        for (blob, change) in changes {
            ensure!(
                matches!(change, Change::Added(_)),
                "tombstone removal forbidden"
            );
            let next_tombstone = next.tombstones.get(&blob)?;
            let next_tombstone = next_tombstone.context("tombstone removal forbidden")?;
            match previous.tombstones.get(&blob)? {
                Some(previous_tombstone) => {
                    ensure!(
                        next_tombstone.entry == previous_tombstone.entry
                            && next_tombstone.marked_ms == previous_tombstone.marked_ms
                            && (!previous_tombstone.deleted || next_tombstone.deleted),
                        "tombstone identity cannot change"
                    );
                    if !previous_tombstone.deleted && next_tombstone.deleted {
                        ensure!(
                            deletions.contains(&blob),
                            "tombstone completion requires narrow deletion API"
                        );
                    }
                }
                None => {
                    ensure!(
                        !next_tombstone.deleted
                            && previous.blobs.get(&blob)?.as_ref() == Some(&next_tombstone.entry),
                        "tombstone requires previously registered physical instance"
                    );
                    ensure!(
                        !previous.uploads.contains(&blob)? && !next.uploads.contains(&blob)?,
                        "unresolved intent cannot be tombstoned"
                    );
                }
            }
        }
        Ok(())
    }

    /// A pin's identity and its entries must disappear together. Releasing the identity
    /// while leaving entries behind would keep their blobs reachable forever, so the rare
    /// removal path pays for the scan that proves no `{pin}/` key survived.
    fn prepare_pin_removals(&self, previous: &CatalogRoot, next: &CatalogRoot) -> Result<()> {
        let removed: Vec<&str> = previous
            .pins
            .keys()
            .filter(|pin| !next.pins.contains_key(*pin))
            .map(String::as_str)
            .collect();
        if removed.is_empty() {
            return Ok(());
        }
        for pin in removed {
            // A release only looks at this pin's own page range. A hashed legacy tree has no
            // usable range and falls back to the full walk inside the registry.
            let (entries, _) = next
                .pin_entries
                .prefix_entries_counted(&format!("{pin}/"))?;
            ensure!(entries.is_empty(), "reader pin removal left entries");
            ensure!(
                !next.pin_generations.contains_key(pin),
                "reader pin removal left its generation"
            );
            ensure!(
                !next.pin_releases.contains(pin),
                "reader pin removal left a release marker"
            );
        }
        Ok(())
    }

    fn prepare_uploads(
        &self,
        previous: &CatalogRoot,
        next: &mut CatalogRoot,
        published: &BTreeSet<String>,
    ) -> Result<()> {
        let changes: Vec<(String, Change<ObjectEntry>)> = next
            .uploads
            .changes()
            .map(|(key, change)| (key.clone(), change.clone()))
            .collect();
        for (blob, change) in changes {
            match change {
                Change::Added(intent) => {
                    if let Some(previous_intent) = previous.uploads.get(&blob)? {
                        ensure!(
                            previous_intent == intent,
                            "upload intent identity mutation forbidden"
                        );
                    }
                }
                Change::Removed => ensure!(
                    published.contains(&blob),
                    "unpublished upload intent cannot be removed"
                ),
            }
        }
        // An intent is discharged as soon as its blob is published. Only this
        // transaction can publish a blob whose intent is still open, so the sweep is
        // bounded by the change set instead of by the whole registry.
        for blob in published {
            if next.uploads.contains(blob)? {
                next.uploads.remove(blob)?;
            }
        }
        Ok(())
    }
}
