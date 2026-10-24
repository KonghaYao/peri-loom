use crate::authority::{
    Authority, CatalogRoot, GcCursor, GcPhase, ObjectEntry, Tombstone, TombstoneDeletion,
};
use crate::object_store::PreconditionFailed;
use crate::ownership::types::CommandReceipt;
use crate::work_metrics::{self, PublishOrigin};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Work one bounded tick may do. The defaults are deliberately larger than every catalog a
/// test or a small deployment has, so a single RPC still finishes the job it always did; a
/// namespace that outgrows them is advanced by repeated ticks instead of one long exclusive
/// window. The time budget is checked between entries, not inside a store call.
#[derive(Clone, Debug)]
pub(crate) struct GcBudget {
    pub scan_entries: usize,
    pub scan_bytes: usize,
    pub mark_objects: usize,
    pub prune_objects: usize,
    pub delete_objects: usize,
    pub time: Duration,
}

impl Default for GcBudget {
    fn default() -> Self {
        Self {
            scan_entries: 512,
            scan_bytes: 4 * 1024 * 1024,
            mark_objects: 64,
            prune_objects: 64,
            delete_objects: 16,
            time: Duration::from_millis(100),
        }
    }
}

/// Result of one planned deletion batch. `Busy` and `Stale` carry no deletion: the caller
/// re-plans instead of treating a transient admission or revision conflict as an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GcDelete {
    Complete(TombstoneDeletion),
    Busy,
    Stale,
}

pub(crate) type GcDeleteBatch<'a> =
    dyn FnMut(&Authority, &[(String, Tombstone)], u64) -> Result<GcDelete> + 'a;

#[derive(Clone, Debug, Default, Serialize)]
pub struct GcStats {
    pub root_revision: u64,
    pub known_blobs: usize,
    pub reachable_blobs: usize,
    pub unreachable_blobs: usize,
    pub unresolved_uploads: usize,
    pub reader_pins: usize,
    pub pending_deletions: usize,
    pub completed_deletions: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GcResult {
    pub marked: usize,
    pub deleted: usize,
}

pub struct GarbageCollector {
    authority: Arc<Authority>,
    safety_window: Duration,
    budget: GcBudget,
}

struct MarkPlan {
    next_after: Option<String>,
    candidates: BTreeMap<String, ObjectEntry>,
    scanned_pages: usize,
    scanned_entries: usize,
    scanned_bytes: usize,
    complete: bool,
    interrupted: Option<&'static str>,
}

/// Logical object aliases that no durable reference reaches. Kept separate from
/// [`MarkPlan`] because the frontier addresses the object registry, a different key space
/// from the blob frontier, and finishing one does not imply the other. Pruning runs first:
/// an object entry may not reference a tombstoned blob, so the alias must be gone before
/// [`GarbageCollector::apply_mark`] publishes the tombstone.
struct PrunePlan {
    next_after: Option<String>,
    removals: Vec<String>,
    scanned_pages: usize,
    scanned_entries: usize,
    scanned_bytes: usize,
    complete: bool,
    interrupted: Option<&'static str>,
}

struct SweepPlan {
    next_after: Option<String>,
    targets: Vec<(String, Tombstone)>,
    scanned_pages: usize,
    scanned_entries: usize,
    scanned_bytes: usize,
    complete: bool,
    interrupted: Option<&'static str>,
}

impl GarbageCollector {
    pub fn new(authority: Arc<Authority>, safety_window: Duration) -> Self {
        Self {
            authority,
            safety_window,
            budget: GcBudget::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_budget(mut self, budget: GcBudget) -> Self {
        self.budget = budget;
        self
    }

    pub fn inspect(&self) -> Result<GcStats> {
        let snapshot = self.authority.snapshot()?;
        let reachable = self.authority.reachable(&snapshot.root)?;
        let blobs = snapshot.root.blobs.entries()?;
        let tombstones = snapshot.root.tombstones.entries()?;
        Ok(GcStats {
            root_revision: snapshot.root.revision,
            known_blobs: blobs.len(),
            reachable_blobs: reachable.len(),
            unreachable_blobs: blobs
                .keys()
                .filter(|blob| !reachable.contains(*blob))
                .count(),
            unresolved_uploads: snapshot.root.uploads.entries()?.len(),
            reader_pins: snapshot.root.pins.len(),
            pending_deletions: tombstones
                .values()
                .filter(|tombstone| !tombstone.deleted)
                .count(),
            completed_deletions: tombstones
                .values()
                .filter(|tombstone| tombstone.deleted)
                .count(),
        })
    }

    pub fn mark(&self) -> Result<usize> {
        let mut marked = 0;
        for _ in 0..4096 {
            let snapshot = self.authority.snapshot()?;
            let cursor = match snapshot.root.gc.as_ref() {
                Some(cursor) if matches!(cursor.phase, GcPhase::Prune | GcPhase::Mark) => {
                    cursor.clone()
                }
                // A sweep that has already started scanning owns its frontier; jumping back
                // to a fresh cycle would abandon the rest of that scan. A sweep with no
                // frontier has not examined anything yet, so the next cycle may resume
                // there and carry the counters forward.
                Some(cursor) if cursor.phase == GcPhase::Sweep => {
                    if cursor.after.is_some() {
                        return Ok(marked);
                    }
                    GcCursor {
                        phase: GcPhase::Prune,
                        after: None,
                        marked: cursor.marked,
                        deleted: cursor.deleted,
                        batches: cursor.batches,
                    }
                }
                _ => GcCursor::default(),
            };
            if cursor.phase == GcPhase::Prune {
                let plan = self.scan_prune(&snapshot.root, &cursor)?;
                record_prune_scan(&plan);
                let next = advance_prune(&cursor, &plan);
                match self.apply_prune(snapshot.root.revision, &plan, &next, None) {
                    Ok(()) => {}
                    Err(error) if error.is::<PreconditionFailed>() => continue,
                    Err(error) => return Err(error),
                }
                // A complete prune advances to `Mark`; an incomplete one stays here with
                // its frontier. Either way the next iteration is the next bounded tick.
                continue;
            }
            let plan = self.scan_mark(&snapshot.root, &cursor)?;
            let next = advance_mark(&cursor, &plan);
            record_mark_scan(&plan);
            match self.apply_mark(snapshot.root.revision, &plan, &next, None) {
                Ok(()) => {
                    work_metrics::observe_gc_marked(plan.candidates.len());
                    marked += plan.candidates.len();
                }
                Err(error) if error.is::<PreconditionFailed>() => continue,
                Err(error) => return Err(error),
            }
            if plan.complete {
                // The tombstone batch is durable and the sweep phase owns the rest.
                break;
            }
        }
        Ok(marked)
    }

    pub fn sweep(&self) -> Result<usize> {
        let mut deleted = 0;
        for _ in 0..4096 {
            let snapshot = self.authority.snapshot()?;
            let cursor = match snapshot.root.gc.as_ref() {
                Some(cursor) if cursor.phase == GcPhase::Sweep => cursor.clone(),
                // Marking and pruning must finish before a deletion plan can rely on every
                // candidate having a durable tombstone. `collect()` drives those phases
                // first; a direct `sweep()` leaves the cursor untouched instead of jumping
                // over pending work.
                Some(cursor) if matches!(cursor.phase, GcPhase::Mark | GcPhase::Prune) => {
                    return Ok(deleted)
                }
                _ => GcCursor {
                    phase: GcPhase::Sweep,
                    ..GcCursor::default()
                },
            };
            let plan = self.scan_sweep(
                &snapshot.root,
                &cursor,
                now_ms()?,
                u64::try_from(self.safety_window.as_millis())?,
                &mut |_, _| Ok(true),
            )?;
            record_sweep_scan(&plan);
            let mut outcome = TombstoneDeletion::default();
            if !plan.targets.is_empty() {
                match direct_delete(&self.authority, &plan.targets, snapshot.root.revision)? {
                    GcDelete::Complete(result) => outcome = result,
                    GcDelete::Busy => continue,
                    GcDelete::Stale => continue,
                }
            }
            deleted += outcome.completed;
            let current = self.authority.snapshot()?;
            let next = advance_sweep(&cursor, &plan, &current.root.gc, outcome);
            match self.apply_sweep(current.root.revision, &next, None) {
                Ok(()) => {}
                Err(error) if error.is::<PreconditionFailed>() => continue,
                Err(error) => return Err(error),
            }
            if plan.complete {
                break;
            }
        }
        Ok(deleted)
    }

    pub fn collect(&self) -> Result<GcResult> {
        // A release interrupted between generation batches left a durable marker. Finish a
        // bounded number before planning deletions so the pin set the mark sees is the one
        // the next release would leave behind.
        self.authority.resume_pin_releases(4)?;
        Ok(GcResult {
            marked: self.mark()?,
            deleted: self.sweep()?,
        })
    }

    /// Drive one request of the bounded collector. One call performs at most one mark tick
    /// and one sweep tick; a cycle that does not fit returns `pending_gc`, and the same
    /// request id resumes from the durable cursor without redoing the earlier work.
    pub(crate) fn collect_receipted(
        &self,
        request_id: &str,
        digest: &str,
        mut eligible: impl FnMut(&str, &Tombstone) -> Result<bool>,
        delete_batch: &mut GcDeleteBatch<'_>,
    ) -> Result<Value> {
        let mut announced = false;
        for _ in 0..4 {
            match self.receipted_tick(
                request_id,
                digest,
                &mut eligible,
                delete_batch,
                &mut announced,
            )? {
                Tick::Done(value) => return Ok(value),
                Tick::Pending(value) => return Ok(value),
                Tick::Retry => {}
            }
        }
        work_metrics::observe_gc_interruption("cas_conflict");
        Ok(pending_response(pending_skeleton(), "cas_conflict"))
    }

    fn receipted_tick(
        &self,
        request_id: &str,
        digest: &str,
        eligible: &mut impl FnMut(&str, &Tombstone) -> Result<bool>,
        delete_batch: &mut GcDeleteBatch<'_>,
        announced: &mut bool,
    ) -> Result<Tick> {
        let snapshot = self.authority.snapshot()?;
        let receipt = self.read_receipt(&snapshot.root, request_id, digest)?;
        if !*announced {
            work_metrics::observe_gc_request(receipt.is_some());
            *announced = true;
        }
        if let Some(receipt) = &receipt {
            if receipt_status(&receipt.result) == Some("completed_gc") {
                return Ok(Tick::Done(receipt.result.clone()));
            }
        }
        let mut base = receipt
            .as_ref()
            .map(|receipt| receipt.result.clone())
            .unwrap_or_else(pending_skeleton);

        let current = snapshot.root.gc.clone();
        if current
            .as_ref()
            .is_some_and(|cursor| cursor.phase == GcPhase::Done)
        {
            if let Some(receipt) = receipt.as_ref() {
                let result = completed_value(&receipt.result, current.as_ref().unwrap().deleted);
                match self.apply_sweep(
                    snapshot.root.revision,
                    current.as_ref().unwrap(),
                    Some((request_id, digest, &result)),
                ) {
                    Ok(()) => {
                        work_metrics::observe_gc_completed();
                        return Ok(Tick::Done(result));
                    }
                    Err(error) if error.is::<PreconditionFailed>() => return Ok(Tick::Retry),
                    Err(error) => return Err(error),
                }
            }
        }

        let cursor = match current {
            Some(cursor) if cursor.phase != GcPhase::Done => cursor,
            _ => GcCursor::default(),
        };

        // Prune first. Head validation forbids an object entry that references a tombstoned
        // blob, so the alias must be gone before the mark phase publishes the tombstone.
        // The phase walks its own key space with its own frontier, so a request that
        // resumes a cycle interrupted mid-prune continues exactly there.
        if cursor.phase == GcPhase::Prune {
            let plan = self.scan_prune(&snapshot.root, &cursor)?;
            record_prune_scan(&plan);
            let next = advance_prune(&cursor, &plan);
            let pending = pending_value(&base, next.marked, next.batches);
            match self.apply_prune(
                snapshot.root.revision,
                &plan,
                &next,
                Some((request_id, digest, &pending)),
            ) {
                Ok(()) => {
                    work_metrics::observe_gc_batch();
                    base = pending;
                }
                Err(error) if error.is::<PreconditionFailed>() => return Ok(Tick::Retry),
                Err(error) => return Err(error),
            }
            if !plan.complete {
                let reason = plan.interrupted.unwrap_or("scan_budget");
                work_metrics::observe_gc_interruption(reason);
                return Ok(Tick::Pending(pending_response(base, reason)));
            }
        }

        // The prune publication above moved the revision; the mark plan must be derived
        // from the head its revision check will use. A resumed mark cursor enters here
        // directly.
        let snapshot = self.authority.snapshot()?;
        let mark_cursor = snapshot
            .root
            .gc
            .clone()
            .filter(|cursor| cursor.phase == GcPhase::Mark);
        if let Some(cursor) = mark_cursor {
            let plan = self.scan_mark(&snapshot.root, &cursor)?;
            record_mark_scan(&plan);
            let next = advance_mark(&cursor, &plan);
            let pending = pending_value(&base, next.marked, next.batches);
            match self.apply_mark(
                snapshot.root.revision,
                &plan,
                &next,
                Some((request_id, digest, &pending)),
            ) {
                Ok(()) => {
                    work_metrics::observe_gc_marked(plan.candidates.len());
                    work_metrics::observe_gc_batch();
                    base = pending;
                }
                Err(error) if error.is::<PreconditionFailed>() => return Ok(Tick::Retry),
                Err(error) => return Err(error),
            }
            if !plan.complete {
                let reason = plan.interrupted.unwrap_or("scan_budget");
                work_metrics::observe_gc_interruption(reason);
                return Ok(Tick::Pending(pending_response(base, reason)));
            }
        }

        // Sweep on a fresh head: the mark publication above moved the revision, and the
        // deletion plan must be derived from the same head its revision check uses.
        let snapshot = self.authority.snapshot()?;
        let receipt = self.read_receipt(&snapshot.root, request_id, digest)?;
        let mut base = receipt
            .as_ref()
            .map(|receipt| receipt.result.clone())
            .unwrap_or(base);
        let current = snapshot.root.gc.clone();
        if current
            .as_ref()
            .is_some_and(|cursor| cursor.phase == GcPhase::Done)
        {
            if let Some(receipt) = receipt.as_ref() {
                let result = completed_value(&receipt.result, current.as_ref().unwrap().deleted);
                match self.apply_sweep(
                    snapshot.root.revision,
                    current.as_ref().unwrap(),
                    Some((request_id, digest, &result)),
                ) {
                    Ok(()) => {
                        work_metrics::observe_gc_completed();
                        return Ok(Tick::Done(result));
                    }
                    Err(error) if error.is::<PreconditionFailed>() => return Ok(Tick::Retry),
                    Err(error) => return Err(error),
                }
            }
        }
        let cursor = match current {
            Some(cursor) if cursor.phase == GcPhase::Sweep => cursor,
            // The head moved into a mark or prune phase between the two reads. A sweep plan
            // from this head would delete against work this tick did not plan, so re-read
            // and let the phase handlers above resume the cycle.
            Some(cursor) if matches!(cursor.phase, GcPhase::Mark | GcPhase::Prune) => {
                return Ok(Tick::Retry)
            }
            _ => GcCursor {
                phase: GcPhase::Sweep,
                ..GcCursor::default()
            },
        };

        // A request that starts while a sweep is already durable has no receipt yet, but the
        // deletion below must be preceded by one. Publish the position first.
        if receipt.is_none() {
            let pending = pending_value(&base, cursor.marked, cursor.batches);
            match self.apply_sweep(
                snapshot.root.revision,
                &cursor,
                Some((request_id, digest, &pending)),
            ) {
                Ok(()) => {
                    work_metrics::observe_gc_batch();
                    base = pending;
                }
                Err(error) if error.is::<PreconditionFailed>() => return Ok(Tick::Retry),
                Err(error) => return Err(error),
            }
        }

        let plan = self.scan_sweep(
            &snapshot.root,
            &cursor,
            now_ms()?,
            u64::try_from(self.safety_window.as_millis())?,
            eligible,
        )?;
        record_sweep_scan(&plan);
        let mut outcome = TombstoneDeletion::default();
        if !plan.targets.is_empty() {
            let deletion = delete_batch(&self.authority, &plan.targets, snapshot.root.revision)?;
            match deletion {
                GcDelete::Complete(result) => {
                    outcome = result;
                    work_metrics::observe_gc_deleted(
                        result.completed,
                        result.reaped,
                        plan.targets.len(),
                    );
                }
                GcDelete::Busy => {
                    work_metrics::observe_gc_interruption("admission_busy");
                    let pending = pending_value(&base, cursor.marked, cursor.batches);
                    return Ok(Tick::Pending(pending_response(pending, "admission_busy")));
                }
                GcDelete::Stale => {
                    work_metrics::observe_gc_interruption("revision_changed");
                    return Ok(Tick::Retry);
                }
            }
        }

        let current = self.authority.snapshot()?;
        let next = advance_sweep(&cursor, &plan, &current.root.gc, outcome);
        if !cursor_forward(&current.root.gc, &next) {
            return Ok(Tick::Retry);
        }
        let pending = pending_value(&base, next.marked, next.batches);
        match self.apply_sweep(
            current.root.revision,
            &next,
            Some((request_id, digest, &pending)),
        ) {
            Ok(()) => work_metrics::observe_gc_batch(),
            Err(error) if error.is::<PreconditionFailed>() => return Ok(Tick::Retry),
            Err(error) => return Err(error),
        }
        if plan.complete {
            let result = completed_value(&pending, next.deleted);
            let acknowledged = self.authority.snapshot()?.root.revision;
            match self.apply_sweep(acknowledged, &next, Some((request_id, digest, &result))) {
                Ok(()) => {
                    work_metrics::observe_gc_completed();
                    Ok(Tick::Done(result))
                }
                Err(error) if error.is::<PreconditionFailed>() => Ok(Tick::Retry),
                Err(error) => Err(error),
            }
        } else {
            let reason = plan.interrupted.unwrap_or("scan_budget");
            work_metrics::observe_gc_interruption(reason);
            Ok(Tick::Pending(pending_response(pending, reason)))
        }
    }

    fn read_receipt(
        &self,
        root: &CatalogRoot,
        request_id: &str,
        digest: &str,
    ) -> Result<Option<CommandReceipt>> {
        let catalog = root.ownership.as_ref().context("ownership missing")?;
        let receipt = match catalog.commands.get(request_id) {
            Some(receipt) => Some(receipt.clone()),
            None => self
                .authority
                .archived_command(catalog.command_index.as_deref(), request_id)?,
        };
        if let Some(receipt) = &receipt {
            ensure!(
                receipt.digest == digest,
                "Conflict request ID payload mismatch"
            );
        }
        Ok(receipt)
    }

    fn scan_mark(&self, root: &CatalogRoot, cursor: &GcCursor) -> Result<MarkPlan> {
        // Aliases count as reachability: prune has removed the orphans before this phase,
        // and a blob whose alias survived (it was pinned when prune scanned) must wait for
        // the next cycle instead of being tombstoned under a live object entry.
        let reachable = self.authority.reachable_with_aliases(root)?;
        let (blobs, blob_pages) = root.blobs.entries_counted()?;
        let (tombstones, tombstone_pages) = root.tombstones.entries_counted()?;
        let mut plan = MarkPlan {
            next_after: cursor.after.clone(),
            candidates: BTreeMap::new(),
            scanned_pages: blob_pages + tombstone_pages,
            scanned_entries: 0,
            scanned_bytes: 0,
            complete: true,
            interrupted: None,
        };
        let deadline = Instant::now() + self.budget.time;
        for (blob, entry) in &blobs {
            if cursor
                .after
                .as_ref()
                .is_some_and(|after| blob.as_str() <= after.as_str())
            {
                continue;
            }
            if plan.scanned_entries > 0
                && (plan.scanned_entries >= self.budget.scan_entries
                    || plan.scanned_bytes >= self.budget.scan_bytes
                    || Instant::now() >= deadline)
            {
                plan.complete = false;
                plan.interrupted = Some(if Instant::now() >= deadline {
                    "time_budget"
                } else {
                    "scan_budget"
                });
                break;
            }
            plan.scanned_entries += 1;
            plan.scanned_bytes += blob.len() + entry.hash.len() + entry.etag.len() + 32;
            plan.next_after = Some(blob.clone());
            if !reachable.contains(blob) && !tombstones.contains_key(blob) {
                plan.candidates.insert(blob.clone(), entry.clone());
                if plan.candidates.len() >= self.budget.mark_objects {
                    plan.complete = false;
                    plan.interrupted = Some("mark_budget");
                    break;
                }
            }
        }
        if plan.complete {
            plan.next_after = None;
        }
        Ok(plan)
    }

    /// Walk the logical object registry behind its own durable frontier and collect the
    /// aliases that no branch, snapshot, reader pin or upload intent reaches. Removing an
    /// alias is what lets the later mark phase tombstone its blob: head validation requires
    /// every object entry to reference a live, non-tombstoned blob, so the two removals
    /// cannot share a transaction.
    ///
    /// The frontier is an object key, not a blob identity, so an interrupted prune resumes
    /// exactly where it stopped. Work per tick is bounded by entries, bytes, the deadline
    /// and the removal quota; the registry page walk itself is shared with the cursor
    /// mechanism the other maintenance scans use.
    fn scan_prune(&self, root: &CatalogRoot, cursor: &GcCursor) -> Result<PrunePlan> {
        let reachable = self.authority.reachable(root)?;
        let (objects, pages) = root.objects.entries_counted()?;
        let mut plan = PrunePlan {
            next_after: cursor.after.clone(),
            removals: Vec::new(),
            scanned_pages: pages,
            scanned_entries: 0,
            scanned_bytes: 0,
            complete: true,
            interrupted: None,
        };
        let deadline = Instant::now() + self.budget.time;
        for (key, entry) in &objects {
            if cursor
                .after
                .as_ref()
                .is_some_and(|after| key.as_str() <= after.as_str())
            {
                continue;
            }
            if plan.scanned_entries > 0
                && (plan.scanned_entries >= self.budget.scan_entries
                    || plan.scanned_bytes >= self.budget.scan_bytes
                    || Instant::now() >= deadline)
            {
                plan.complete = false;
                plan.interrupted = Some(if Instant::now() >= deadline {
                    "time_budget"
                } else {
                    "scan_budget"
                });
                break;
            }
            plan.scanned_entries += 1;
            plan.scanned_bytes += key.len() + entry.hash.len() + entry.etag.len() + 32;
            plan.next_after = Some(key.clone());
            // Archived WAL receipt pages are durable metadata. A branch reader may still be
            // resolving an archived receipt through an index that has already advanced and
            // retired the pin that once protected this page path, so GC must never prune the
            // alias. Sweep never sees them: only prune creates the tombstone, and mark treats
            // an alias as reachable.
            if retained_archive_key(key) {
                continue;
            }
            if !reachable.contains(&entry.blob) || root.tombstones.contains(&entry.blob)? {
                plan.removals.push(key.clone());
                if plan.removals.len() >= self.budget.prune_objects {
                    plan.complete = false;
                    plan.interrupted = Some("prune_budget");
                    break;
                }
            }
        }
        if plan.complete {
            plan.next_after = None;
        }
        Ok(plan)
    }

    fn scan_sweep(
        &self,
        root: &CatalogRoot,
        cursor: &GcCursor,
        now: u64,
        window: u64,
        eligible: &mut impl FnMut(&str, &Tombstone) -> Result<bool>,
    ) -> Result<SweepPlan> {
        // The deep walk is the fail-closed gate for deletion: if the reachable metadata no
        // longer verifies, nothing below is planned.
        let reachable = self.authority.reachable(root)?;
        let (tombstones, pages) = root.tombstones.entries_counted()?;
        let mut plan = SweepPlan {
            next_after: cursor.after.clone(),
            targets: Vec::new(),
            scanned_pages: pages,
            scanned_entries: 0,
            scanned_bytes: 0,
            complete: true,
            interrupted: None,
        };
        let deadline = Instant::now() + self.budget.time;
        for (blob, tombstone) in &tombstones {
            if cursor
                .after
                .as_ref()
                .is_some_and(|after| blob.as_str() <= after.as_str())
            {
                continue;
            }
            if plan.scanned_entries > 0
                && (plan.scanned_entries >= self.budget.scan_entries
                    || plan.scanned_bytes >= self.budget.scan_bytes
                    || Instant::now() >= deadline)
            {
                plan.complete = false;
                plan.interrupted = Some(if Instant::now() >= deadline {
                    "time_budget"
                } else {
                    "scan_budget"
                });
                break;
            }
            plan.scanned_entries += 1;
            plan.scanned_bytes += blob.len() + 64;
            plan.next_after = Some(blob.clone());
            let target = if tombstone.deleted {
                // Completed tombstones remain the durable evidence that the identity must
                // not return. They cost one existence check, not a metadata transaction,
                // unless a late writer actually recreated the physical key.
                self.authority.tombstone_object_present(blob)?
            } else {
                now.saturating_sub(tombstone.marked_ms) >= window
                    && eligible(blob, tombstone)?
                    && !reachable.contains(blob)
                    && !root.uploads.contains(blob)?
            };
            if target {
                plan.targets.push((blob.clone(), tombstone.clone()));
                if plan.targets.len() >= self.budget.delete_objects {
                    plan.complete = false;
                    plan.interrupted = Some("delete_budget");
                    break;
                }
            }
        }
        if plan.complete {
            plan.next_after = None;
        }
        Ok(plan)
    }

    fn apply_mark(
        &self,
        revision: u64,
        plan: &MarkPlan,
        cursor: &GcCursor,
        receipt: Option<(&str, &str, &Value)>,
    ) -> Result<()> {
        let marked_ms = now_ms()?;
        self.authority
            .transact_with_origin(Some(revision), PublishOrigin::Gc, |root| {
                for (blob, entry) in &plan.candidates {
                    root.tombstones.insert(
                        blob.clone(),
                        Tombstone {
                            entry: entry.clone(),
                            marked_ms,
                            deleted: false,
                        },
                    )?;
                }
                root.gc = Some(cursor.clone());
                if let Some((request_id, digest, result)) = receipt {
                    put_receipt(root, request_id, digest, result)?;
                }
                Ok(())
            })?;
        Ok(())
    }

    fn apply_prune(
        &self,
        revision: u64,
        plan: &PrunePlan,
        cursor: &GcCursor,
        receipt: Option<(&str, &str, &Value)>,
    ) -> Result<()> {
        self.authority
            .transact_with_origin(Some(revision), PublishOrigin::Gc, |root| {
                for key in &plan.removals {
                    root.objects.remove(key)?;
                }
                root.gc = Some(cursor.clone());
                if let Some((request_id, digest, result)) = receipt {
                    put_receipt(root, request_id, digest, result)?;
                }
                Ok(())
            })?;
        Ok(())
    }

    fn apply_sweep(
        &self,
        revision: u64,
        cursor: &GcCursor,
        receipt: Option<(&str, &str, &Value)>,
    ) -> Result<()> {
        self.authority
            .transact_with_origin(Some(revision), PublishOrigin::Gc, |root| {
                root.gc = Some(cursor.clone());
                if let Some((request_id, digest, result)) = receipt {
                    put_receipt(root, request_id, digest, result)?;
                }
                Ok(())
            })?;
        Ok(())
    }
}

enum Tick {
    Done(Value),
    Pending(Value),
    Retry,
}

fn put_receipt(
    root: &mut CatalogRoot,
    request_id: &str,
    digest: &str,
    result: &Value,
) -> Result<()> {
    let catalog = root.ownership.as_mut().context("ownership missing")?;
    if let Some(existing) = catalog.commands.get(request_id) {
        ensure!(
            existing.digest == digest,
            "Conflict request ID payload mismatch"
        );
        ensure!(
            receipt_status(&existing.result) != Some("completed_gc"),
            "completed GC receipt cannot change"
        );
    }
    catalog.commands.insert(
        request_id.into(),
        CommandReceipt {
            digest: digest.into(),
            result: result.clone(),
        },
    );
    Ok(())
}

fn direct_delete(
    authority: &Authority,
    targets: &[(String, Tombstone)],
    revision: u64,
) -> Result<GcDelete> {
    match authority.delete_tombstones(targets, revision, PublishOrigin::Gc) {
        Ok(outcome) => Ok(GcDelete::Complete(outcome)),
        Err(error) if error.is::<PreconditionFailed>() => Ok(GcDelete::Stale),
        Err(error) => {
            work_metrics::observe_gc_delete_failure();
            Err(error)
        }
    }
}

fn advance_mark(cursor: &GcCursor, plan: &MarkPlan) -> GcCursor {
    GcCursor {
        phase: if plan.complete {
            GcPhase::Sweep
        } else {
            GcPhase::Mark
        },
        after: if plan.complete {
            None
        } else {
            plan.next_after.clone()
        },
        marked: cursor.marked + plan.candidates.len() as u64,
        deleted: cursor.deleted,
        batches: cursor.batches + 1,
    }
}

fn advance_prune(cursor: &GcCursor, plan: &PrunePlan) -> GcCursor {
    GcCursor {
        phase: if plan.complete {
            GcPhase::Mark
        } else {
            GcPhase::Prune
        },
        after: if plan.complete {
            None
        } else {
            plan.next_after.clone()
        },
        marked: cursor.marked,
        deleted: cursor.deleted,
        batches: cursor.batches + 1,
    }
}

fn advance_sweep(
    cursor: &GcCursor,
    plan: &SweepPlan,
    current: &Option<GcCursor>,
    outcome: TombstoneDeletion,
) -> GcCursor {
    let marked = current
        .as_ref()
        .map_or(cursor.marked, |current| cursor.marked.max(current.marked));
    let batches = current.as_ref().map_or(cursor.batches, |current| {
        cursor.batches.max(current.batches)
    }) + 1;
    let deleted = current.as_ref().map_or(cursor.deleted, |current| {
        cursor.deleted.max(current.deleted)
    }) + outcome.completed as u64;
    GcCursor {
        phase: if plan.complete {
            GcPhase::Done
        } else {
            GcPhase::Sweep
        },
        after: if plan.complete {
            None
        } else {
            plan.next_after.clone()
        },
        marked,
        deleted,
        batches,
    }
}

/// Refuse to publish a frontier that the head already moved past. The caller re-plans from
/// the newer cursor; it never rewinds the durable position.
fn cursor_forward(current: &Option<GcCursor>, next: &GcCursor) -> bool {
    let Some(current) = current else {
        return true;
    };
    match (current.phase, next.phase) {
        (GcPhase::Mark, GcPhase::Mark)
        | (GcPhase::Prune, GcPhase::Prune)
        | (GcPhase::Sweep, GcPhase::Sweep) => match (&current.after, &next.after) {
            (_, None) => true,
            (None, Some(_)) => true,
            (Some(current), Some(next)) => next >= current,
        },
        (GcPhase::Prune, GcPhase::Mark)
        | (GcPhase::Mark, GcPhase::Sweep)
        | (GcPhase::Sweep, GcPhase::Done) => next.after.is_none(),
        // A sweep without a frontier starts the next cycle; the phase handlers choose
        // whether that is a fresh prune. A sweep with a frontier keeps it, so the next
        // cycle cannot begin before the current scan is exhausted.
        (GcPhase::Sweep, GcPhase::Prune) | (GcPhase::Sweep, GcPhase::Mark) => {
            current.after.is_none()
        }
        (GcPhase::Done, GcPhase::Prune)
        | (GcPhase::Done, GcPhase::Mark)
        | (GcPhase::Done, GcPhase::Sweep) => true,
        _ => false,
    }
}

fn receipt_status(value: &Value) -> Option<&str> {
    value.get("status").and_then(Value::as_str)
}

fn pending_skeleton() -> Value {
    json!({
        "status": "pending_gc",
        "targets": [],
        "initial_pending": [],
        "marked": 0,
        "deleted": 0,
        "batches": 0,
    })
}

fn pending_value(base: &Value, marked: u64, batches: u64) -> Value {
    let mut value = base.clone();
    if let Some(object) = value.as_object_mut() {
        object.insert("status".into(), json!("pending_gc"));
        object.insert("marked".into(), json!(marked));
        object.insert("deleted".into(), json!(0));
        object.insert("batches".into(), json!(batches));
    }
    value
}

fn completed_value(base: &Value, deleted: u64) -> Value {
    let mut value = base.clone();
    if let Some(object) = value.as_object_mut() {
        object.insert("status".into(), json!("completed_gc"));
        object.insert("deleted".into(), json!(deleted));
    }
    value
}

fn pending_response(mut value: Value, interrupted: &'static str) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("interrupted".into(), json!(interrupted));
    }
    value
}

fn record_mark_scan(plan: &MarkPlan) {
    work_metrics::observe_gc_scan(plan.scanned_pages, plan.scanned_entries, plan.scanned_bytes);
}

fn record_prune_scan(plan: &PrunePlan) {
    work_metrics::observe_gc_scan(plan.scanned_pages, plan.scanned_entries, plan.scanned_bytes);
}

fn record_sweep_scan(plan: &SweepPlan) {
    work_metrics::observe_gc_scan(plan.scanned_pages, plan.scanned_entries, plan.scanned_bytes);
}

fn retained_archive_key(key: &str) -> bool {
    key.strip_prefix(crate::wal::WAL_RECEIPT_PAGE_PREFIX)
        .is_some_and(|rest| rest.starts_with('/'))
}

fn now_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("GC clock precedes Unix epoch")?
        .as_millis()
        .try_into()
        .context("GC clock overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::ownership::OwnershipAuthority;
    use crate::manifest::{digest, SEGMENT_SIZE};
    use crate::namespace::Namespace;
    use crate::object_store::{MemoryObjectStore, ObjectStore, PutCondition};
    use crate::ownership::types::LeaseClock;
    use crate::runtime::DatabaseId;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TestClock(AtomicU64);

    impl LeaseClock for TestClock {
        fn now_ms(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::Relaxed))
        }
    }

    fn setup() -> Result<(Arc<MemoryObjectStore>, Arc<Authority>, Namespace)> {
        let raw = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("tenant", "gcunit")?;
        let clock: Arc<dyn LeaseClock> = Arc::new(TestClock(AtomicU64::new(100)));
        // The receipted path stores its reply in the ownership catalog, so the namespace
        // needs the same catalog a live service installs.
        OwnershipAuthority::new(raw.clone(), clock)
            .state(&DatabaseId::new(namespace.clone(), "main")?)?;
        let authority = Authority::open(raw.clone(), namespace.clone())?;
        Ok((raw, authority, namespace))
    }

    fn upload_orphans(
        authority: &Arc<Authority>,
        namespace: &Namespace,
        count: usize,
    ) -> Result<()> {
        for index in 0..count {
            let bytes = vec![index as u8 + 1; SEGMENT_SIZE];
            let key = namespace.key(&format!("segments/{}", digest(&bytes)))?;
            // The session's reader pin is the only durable reference; dropping it leaves an
            // object alias over a blob no branch reaches, which is exactly prune's input.
            let session = authority.branch_store("main")?;
            session.put(&key, &bytes, PutCondition::Absent)?;
        }
        Ok(())
    }

    fn limited_budget() -> GcBudget {
        GcBudget {
            scan_entries: 2,
            scan_bytes: usize::MAX,
            mark_objects: 2,
            prune_objects: 2,
            delete_objects: 1,
            time: Duration::from_secs(30),
        }
    }

    fn status(value: &Value) -> &str {
        receipt_status(value).expect("GC result without status")
    }

    #[test]
    fn a_tick_stays_bounded_and_the_cycle_resumes_after_reopen() -> Result<()> {
        let (raw, authority, namespace) = setup()?;
        upload_orphans(&authority, &namespace, 6)?;
        let collector =
            GarbageCollector::new(authority.clone(), Duration::ZERO).with_budget(limited_budget());
        let request_digest = digest(b"gc-bounded");
        let mut delete = |authority: &Authority, targets: &[(String, Tombstone)], revision: u64| {
            direct_delete(authority, targets, revision)
        };
        let first = collector.collect_receipted(
            "gc-bounded",
            &request_digest,
            |_, _| Ok(true),
            &mut delete,
        )?;
        assert_eq!(status(&first), "pending_gc");
        // A call may publish at most one tick per phase. With a two-entry prune window it
        // must stop in the prune phase and must not have deleted anything yet.
        let cursor = authority
            .snapshot()?
            .root
            .gc
            .clone()
            .context("GC cursor missing")?;
        assert!(
            cursor.batches <= 3,
            "one call advanced {} batches",
            cursor.batches
        );
        assert_eq!(cursor.deleted, 0);

        // Reopening proves the persisted intermediate head is valid: a crash between prune
        // and mark must not leave a namespace that fails deep validation.
        drop(collector);
        drop(authority);
        let authority = Authority::open(raw, namespace)?;
        let collector =
            GarbageCollector::new(authority.clone(), Duration::ZERO).with_budget(limited_budget());
        let mut delete = |authority: &Authority, targets: &[(String, Tombstone)], revision: u64| {
            direct_delete(authority, targets, revision)
        };
        let mut calls = 1;
        let done = loop {
            calls += 1;
            assert!(calls <= 64, "bounded ticks did not converge");
            let value = collector.collect_receipted(
                "gc-bounded",
                &request_digest,
                |_, _| Ok(true),
                &mut delete,
            )?;
            match status(&value) {
                "completed_gc" => break value,
                "pending_gc" => {}
                other => panic!("unexpected GC status {other}"),
            }
        };
        assert!(
            calls >= 6,
            "six objects with a one-object delete quota finished in {calls} calls"
        );
        assert_eq!(done["deleted"], json!(6));

        let root = authority.snapshot()?.root;
        assert!(root.blobs.entries()?.is_empty());
        let tombstones = root.tombstones.entries()?;
        assert_eq!(tombstones.len(), 6);
        assert!(
            tombstones.values().all(|tombstone| tombstone.deleted),
            "completed tombstones must remain as the late-arrival evidence"
        );
        assert!(
            root.objects.entries()?.is_empty(),
            "prune must have removed every orphan alias"
        );
        Ok(())
    }
}
