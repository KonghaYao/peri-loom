use crate::namespace::Namespace;
use anyhow::ensure;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use super::registry::Registry;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ObjectEntry {
    pub blob: String,
    pub hash: String,
    pub etag: String,
    pub created_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPoint {
    pub id: String,
    pub branch: String,
    pub timeline: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub head: Option<ObjectEntry>,
    pub wal: Option<ObjectEntry>,
}

/// Schema of the durable recovery index. A document carrying any other value is rejected
/// rather than interpreted: the index decides which commits are still restorable, so a
/// binary that does not understand a newer layout must not guess at its bounds.
pub const RECOVERY_INDEX_SCHEMA: u8 = 1;

/// Maximum number of recovery points cached inline in the branch reference. The hot window
/// is a latency optimisation, never the retention contract: points outside it are served
/// from the immutable index.
pub const MAX_HOT_RECOVERY_POINTS: u32 = 1024;

/// Maximum tree levels the recovery index may have. Fanout is bounded by page bytes, so
/// this only guards against a corrupt or hostile pointer chain.
pub const MAX_RECOVERY_INDEX_DEPTH: u8 = 24;

/// Durable retention contract for one branch.
///
/// The window is stated in the units the operator chose. A sequence window keeps the newest
/// `points` commits; a time window keeps every commit at or after `newest - retain_ms`. The
/// window is committed here, at index creation, and is never shortened by a later process
/// configuration: changing the promised range requires an explicit `retain_after` call.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRetention {
    pub mode: RetentionMode,
    /// Points kept inline in the branch reference for commit-path latency.
    #[serde(default = "default_hot_points")]
    pub hot_points: u32,
    /// Logical recovery-point bytes the index may retain. Physical page overhead is not
    /// charged: old page versions are immutable garbage, while this budget describes the
    /// payload the retention promise covers.
    #[serde(default = "default_recovery_bytes")]
    pub max_bytes: u64,
    /// Named snapshots extend the time floor back to their point. Without this, a snapshot
    /// still restores through its own copy, but the indexed commit range may age past it.
    #[serde(default = "default_anchor_snapshots")]
    pub anchor_snapshots: bool,
}

fn default_hot_points() -> u32 {
    super::RETAINED_RECOVERY_POINTS as u32
}

fn default_recovery_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_anchor_snapshots() -> bool {
    true
}

impl Default for RecoveryRetention {
    fn default() -> Self {
        Self {
            mode: RetentionMode::SequenceWindow {
                points: super::RETAINED_RECOVERY_POINTS as u32,
            },
            hot_points: default_hot_points(),
            max_bytes: default_recovery_bytes(),
            anchor_snapshots: default_anchor_snapshots(),
        }
    }
}

impl RecoveryRetention {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            (1..=MAX_HOT_RECOVERY_POINTS).contains(&self.hot_points),
            "recovery hot window is out of range"
        );
        ensure!(self.max_bytes > 0, "recovery byte budget is zero");
        self.mode.validate()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetentionMode {
    SequenceWindow { points: u32 },
    TimeWindow { retain_ms: u64 },
}

impl RetentionMode {
    pub fn name(&self) -> &'static str {
        match self {
            Self::SequenceWindow { .. } => "sequence_window",
            Self::TimeWindow { .. } => "time_window",
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::SequenceWindow { points } => {
                ensure!(*points > 0, "recovery sequence window is zero")
            }
            Self::TimeWindow { retain_ms } => {
                ensure!(*retain_ms > 0, "recovery time window is zero")
            }
        }
        Ok(())
    }
}

/// Stable failure for a selector outside the branch's committed retention window. The
/// display form carries the code and every bound an operator needs, so an RPC that flattens
/// errors to text still reports `out_of_retention` plus the actual range instead of a
/// generic "not found".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutOfRetention {
    pub branch: String,
    pub requested_sequence: Option<u64>,
    pub requested_timestamp_ms: Option<u64>,
    pub retention_mode: String,
    pub retained_points: u64,
    pub oldest_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
    pub oldest_timestamp_ms: Option<u64>,
    pub newest_timestamp_ms: Option<u64>,
}

impl OutOfRetention {
    pub const CODE: &'static str = "out_of_retention";
}

impl std::fmt::Display for OutOfRetention {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}: branch={} mode={} retained={} requested_sequence={:?} requested_timestamp_ms={:?} oldest_sequence={:?} newest_sequence={:?} oldest_timestamp_ms={:?} newest_timestamp_ms={:?}",
            Self::CODE,
            self.branch,
            self.retention_mode,
            self.retained_points,
            self.requested_sequence,
            self.requested_timestamp_ms,
            self.oldest_sequence,
            self.newest_sequence,
            self.oldest_timestamp_ms,
            self.newest_timestamp_ms
        )
    }
}

impl std::error::Error for OutOfRetention {}

/// A selector inside the retained bounds for which no exact commit exists. This is distinct
/// from `out_of_retention`: the caller asked for a sequence the branch never committed (or a
/// time with no preceding commit), not for history that has aged out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPointNotFound {
    pub branch: String,
    pub requested_sequence: Option<u64>,
    pub requested_timestamp_ms: Option<u64>,
    pub oldest_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
}

impl RecoveryPointNotFound {
    pub const CODE: &'static str = "recovery_point_not_found";
}

impl std::fmt::Display for RecoveryPointNotFound {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}: branch={} requested_sequence={:?} requested_timestamp_ms={:?} oldest_sequence={:?} newest_sequence={:?}",
            Self::CODE,
            self.branch,
            self.requested_sequence,
            self.requested_timestamp_ms,
            self.oldest_sequence,
            self.newest_sequence
        )
    }
}

impl std::error::Error for RecoveryPointNotFound {}

/// Refusal to publish a commit that would exceed the committed byte budget without leaving
/// the promised window. The point is not silently forgotten: the publication fails and the
/// caller must extend the budget, prune with `retain_after`, or move to a larger policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionCapacityExceeded {
    pub branch: String,
    pub retained_bytes: u64,
    pub max_bytes: u64,
    pub oldest_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
    pub oldest_timestamp_ms: Option<u64>,
    pub newest_timestamp_ms: Option<u64>,
}

impl RetentionCapacityExceeded {
    pub const CODE: &'static str = "retention_capacity_exceeded";
}

impl std::fmt::Display for RetentionCapacityExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}: branch={} retained_bytes={} max_bytes={} oldest_sequence={:?} newest_sequence={:?} oldest_timestamp_ms={:?} newest_timestamp_ms={:?}",
            Self::CODE,
            self.branch,
            self.retained_bytes,
            self.max_bytes,
            self.oldest_sequence,
            self.newest_sequence,
            self.oldest_timestamp_ms,
            self.newest_timestamp_ms
        )
    }
}

impl std::error::Error for RetentionCapacityExceeded {}

/// Immutable index state for one branch timeline.
///
/// `root` addresses a content-addressed tree of bounded pages; the summary fields make
/// bounds and byte accounting readable without touching a page. `count` is the number of
/// points in the timeline, so an empty timeline is represented by zero bounds and no root.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryTimelineIndex {
    pub root: Option<String>,
    pub depth: u8,
    pub count: u64,
    pub bytes: u64,
    pub oldest_sequence: u64,
    pub newest_sequence: u64,
    pub oldest_timestamp_ms: u64,
    pub newest_timestamp_ms: u64,
}

impl RecoveryTimelineIndex {
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn validate(&self, timeline: u64) -> anyhow::Result<()> {
        ensure!(timeline > 0, "recovery timeline identity is zero");
        if self.is_empty() {
            ensure!(
                self.root.is_none()
                    && self.depth == 0
                    && self.bytes == 0
                    && self.oldest_sequence == 0
                    && self.newest_sequence == 0
                    && self.oldest_timestamp_ms == 0
                    && self.newest_timestamp_ms == 0,
                "empty recovery timeline carries state"
            );
            return Ok(());
        }
        let Some(root) = self.root.as_deref() else {
            anyhow::bail!("non-empty recovery timeline has no root");
        };
        ensure!(
            crate::manifest::valid_hash(root),
            "recovery timeline root is not a page identity"
        );
        ensure!(
            self.depth <= MAX_RECOVERY_INDEX_DEPTH,
            "recovery timeline depth is too large"
        );
        ensure!(self.bytes > 0, "recovery timeline byte count is zero");
        ensure!(
            self.oldest_sequence <= self.newest_sequence
                && self.oldest_timestamp_ms <= self.newest_timestamp_ms,
            "recovery timeline bounds are inverted"
        );
        Ok(())
    }
}

/// Per-branch recovery index. Timelines are indexed separately because a restore creates a
/// new identity with its own sequence/time boundary; points from the previous timeline stay
/// readable under their original key instead of being relabelled.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryIndex {
    pub schema: u8,
    pub policy: RecoveryRetention,
    pub count: u64,
    pub bytes: u64,
    pub oldest_sequence: u64,
    pub newest_sequence: u64,
    pub oldest_timestamp_ms: u64,
    pub newest_timestamp_ms: u64,
    pub timelines: BTreeMap<u64, RecoveryTimelineIndex>,
}

impl RecoveryIndex {
    pub fn empty(policy: RecoveryRetention) -> Self {
        Self {
            schema: RECOVERY_INDEX_SCHEMA,
            policy,
            count: 0,
            bytes: 0,
            oldest_sequence: 0,
            newest_sequence: 0,
            oldest_timestamp_ms: 0,
            newest_timestamp_ms: 0,
            timelines: BTreeMap::new(),
        }
    }

    pub fn validate_shape(&self) -> anyhow::Result<()> {
        ensure!(
            self.schema == RECOVERY_INDEX_SCHEMA,
            "unsupported recovery index schema"
        );
        self.policy.validate()?;
        let mut count = 0u64;
        let mut bytes = 0u64;
        let mut oldest_sequence = None;
        let mut newest_sequence = None;
        let mut oldest_timestamp = None;
        let mut newest_timestamp = None;
        for (timeline, entry) in &self.timelines {
            entry.validate(*timeline)?;
            if entry.is_empty() {
                continue;
            }
            count = count
                .checked_add(entry.count)
                .ok_or_else(|| anyhow::anyhow!("recovery point count overflow"))?;
            bytes = bytes
                .checked_add(entry.bytes)
                .ok_or_else(|| anyhow::anyhow!("recovery byte count overflow"))?;
            oldest_sequence = Some(oldest_sequence.map_or(entry.oldest_sequence, |value: u64| {
                value.min(entry.oldest_sequence)
            }));
            newest_sequence = Some(newest_sequence.map_or(entry.newest_sequence, |value: u64| {
                value.max(entry.newest_sequence)
            }));
            oldest_timestamp = Some(
                oldest_timestamp.map_or(entry.oldest_timestamp_ms, |value: u64| {
                    value.min(entry.oldest_timestamp_ms)
                }),
            );
            newest_timestamp = Some(
                newest_timestamp.map_or(entry.newest_timestamp_ms, |value: u64| {
                    value.max(entry.newest_timestamp_ms)
                }),
            );
        }
        ensure!(
            self.count == count && self.bytes == bytes,
            "recovery index aggregate counters disagree with timelines"
        );
        if count == 0 {
            ensure!(
                self.oldest_sequence == 0
                    && self.newest_sequence == 0
                    && self.oldest_timestamp_ms == 0
                    && self.newest_timestamp_ms == 0,
                "empty recovery index carries bounds"
            );
        } else {
            ensure!(
                self.oldest_sequence == oldest_sequence.unwrap_or(0)
                    && self.newest_sequence == newest_sequence.unwrap_or(0)
                    && self.oldest_timestamp_ms == oldest_timestamp.unwrap_or(0)
                    && self.newest_timestamp_ms == newest_timestamp.unwrap_or(0),
                "recovery index aggregate bounds disagree with timelines"
            );
        }
        ensure!(
            self.bytes <= self.policy.max_bytes,
            "recovery index exceeds its committed byte budget"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchRef {
    pub incarnation: u64,
    pub timeline: u64,
    pub sequence: u64,
    pub last_commit_ms: u64,
    #[serde(default)]
    pub current_sequence: u64,
    #[serde(default)]
    pub current_commit_ms: u64,
    pub head: Option<ObjectEntry>,
    pub wal: Option<ObjectEntry>,
    /// Bounded, newest-first cache of recovery points. The durable retention contract and
    /// the full searchable history live in `recovery`; this window only keeps the commit
    /// path from reading an index page per statement.
    pub history: Vec<RecoveryPoint>,
    /// Present once the branch has published its first indexed commit. Absent means the
    /// document predates the index; queries fall back to `history` and the next publication
    /// migrates that history into a fresh index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<RecoveryIndex>,
}

impl Default for BranchRef {
    fn default() -> Self {
        Self {
            incarnation: 1,
            timeline: 1,
            sequence: 0,
            last_commit_ms: 0,
            current_sequence: 0,
            current_commit_ms: 0,
            head: None,
            wal: None,
            history: Vec::new(),
            recovery: None,
        }
    }
}

impl BranchRef {
    /// Cheap "did this branch change?" predicate for change-scoped validation: it
    /// compares the fields a publication can move and the history length, never the
    /// history entries themselves, so per-commit cost does not grow with commit count.
    pub(super) fn unchanged_refs(&self, other: &BranchRef) -> bool {
        self.incarnation == other.incarnation
            && self.sequence == other.sequence
            && self.current_sequence == other.current_sequence
            && self.head == other.head
            && self.wal == other.wal
            && self.history.len() == other.history.len()
            && self.recovery == other.recovery
    }

    /// Structural validation that runs on decode and before every publication. Page
    /// contents are verified by the boot and collection walks; this only proves the
    /// pointers and summaries are self-consistent, so a statement never pays for history.
    pub(super) fn validate_recovery(&self) -> anyhow::Result<()> {
        match &self.recovery {
            Some(index) => {
                index.validate_shape()?;
                ensure!(
                    self.history.len() as u32 <= index.policy.hot_points,
                    "recovery hot window exceeds the committed policy"
                );
            }
            None => ensure!(
                self.history.len() <= super::RETAINED_RECOVERY_POINTS,
                "legacy recovery hot window exceeds its bound"
            ),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Tombstone {
    pub entry: ObjectEntry,
    pub marked_ms: u64,
    pub deleted: bool,
}

/// Durable progress of the namespace-wide garbage collection cycle.
///
/// One cursor is shared by every request that drives collection for a namespace. That is
/// deliberate: a request that gives up after a bounded batch must not force the next one to
/// rescan from the beginning, and a restart must continue from the last durable position
/// instead of turning the whole registry into one tick. `after` is the greatest key fully
/// examined in the current phase; keys are the same content identities the registries use,
/// so a cursor survives a page-tree rewrite even though page identities do not.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GcPhase {
    /// Remove logical object aliases that no branch, snapshot, pin or upload intent
    /// references.
    ///
    /// This is its own phase and runs before [`GcPhase::Mark`] because the two key spaces
    /// are different: the mark frontier addresses physical blob identities, while aliases
    /// live in the object registry. The order matters for validation: an object entry may
    /// not reference a tombstoned blob, so the alias has to be gone before the tombstone
    /// is published. A cycle interrupted here resumes the cleanup, not the mark.
    #[default]
    Prune,
    Mark,
    Sweep,
    Done,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GcCursor {
    pub phase: GcPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    #[serde(default)]
    pub marked: u64,
    #[serde(default)]
    pub deleted: u64,
    #[serde(default)]
    pub batches: u64,
}

impl GcCursor {
    /// A cursor may only move forward. Publication validation uses this to reject a
    /// transaction that rewinds a phase, clears a frontier or decrements a progress counter;
    /// a reset is only legal when a completed cycle starts the next one.
    pub(crate) fn follows(&self, previous: Option<&GcCursor>) -> bool {
        let Some(previous) = previous else {
            // The first publication may already carry the work of its tick, so counters are
            // not required to be zero; only the frontier shape and the phase are constrained.
            return match self.phase {
                GcPhase::Mark | GcPhase::Prune | GcPhase::Sweep => true,
                GcPhase::Done => self.after.is_none(),
            };
        };
        let counters_forward = self.marked >= previous.marked
            && self.deleted >= previous.deleted
            && self.batches >= previous.batches;
        match (previous.phase, self.phase) {
            (GcPhase::Mark, GcPhase::Mark)
            | (GcPhase::Prune, GcPhase::Prune)
            | (GcPhase::Sweep, GcPhase::Sweep) => {
                counters_forward && frontier_forward(&previous.after, &self.after)
            }
            (GcPhase::Prune, GcPhase::Mark) | (GcPhase::Sweep, GcPhase::Done) => {
                counters_forward && self.after.is_none()
            }
            // A mark from a binary that predates the prune phase jumps straight to sweep;
            // accepting it preserves the old durable state without forcing a rewrite.
            (GcPhase::Mark, GcPhase::Sweep) => counters_forward && self.after.is_none(),
            // A sweep that has not yet published a frontier may start the next cycle: the
            // tombstone registry, not the cursor, authorizes deletion, so rescanning is
            // safe. A sweep with a frontier keeps it so pending work is not silently
            // skipped.
            (GcPhase::Sweep, GcPhase::Prune) | (GcPhase::Sweep, GcPhase::Mark) => {
                previous.after.is_none()
            }
            // A completed cycle starts the next one with fresh counters. Any state at the
            // start of that cycle is legal; only the completed cycle's values are abandoned.
            (GcPhase::Done, GcPhase::Prune)
            | (GcPhase::Done, GcPhase::Mark)
            | (GcPhase::Done, GcPhase::Sweep) => self.phase != GcPhase::Done,
            // Finalizing a request against the same completed cycle does not move it.
            (GcPhase::Done, GcPhase::Done) => self.after.is_none() && counters_forward,
            _ => false,
        }
    }
}

fn frontier_forward(previous: &Option<String>, next: &Option<String>) -> bool {
    match (previous, next) {
        (Some(previous), Some(next)) => next >= previous,
        (None, _) => true,
        (Some(_), None) => false,
    }
}

/// Durable representation of the authority head. Volume registries appear twice: the
/// inline window carries the state while it is small, the `*_index` pointer addresses an
/// immutable page tree once it is not. A document never carries both, so a reader that
/// finds a pointer ignores the inline window entirely.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogDocument {
    pub(super) version: u32,
    pub(super) revision: u64,
    pub(super) incarnation: String,
    pub(super) namespace: Namespace,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) ownership: Option<crate::ownership::types::OwnershipCatalog>,
    #[serde(default)]
    pub(super) blobs: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) blob_index: Option<String>,
    #[serde(default)]
    pub(super) receipts: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) receipt_index: Option<String>,
    #[serde(default)]
    pub(super) objects: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) object_index: Option<String>,
    pub(super) branches: BTreeMap<String, BranchRef>,
    #[serde(default)]
    pub(super) snapshots: BTreeMap<String, RecoveryPoint>,
    #[serde(default)]
    pub(super) pins: BTreeMap<String, Vec<ObjectEntry>>,
    #[serde(default)]
    pub(super) pin_entries: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) pin_index: Option<String>,
    /// Current snapshot generation of each reader pin. Entries below this value belong to
    /// generations a store has stopped reading and may be removed while the pin survives;
    /// the current generation's entries may only leave with the pin itself.
    #[serde(default)]
    pub(super) pin_generations: BTreeMap<String, u64>,
    /// Pins whose owner has started releasing them. The marker is the durable work item a
    /// maintenance tick or the next open can resume after a crash; it is removed with the
    /// pin identity once every generation is gone.
    #[serde(default)]
    pub(super) pin_releases: BTreeSet<String>,
    /// Wire schema for the reader-pin representation:
    ///
    /// * absent, non-empty legacy `pins` vectors, empty `pin_entries`: original pre-split
    ///   shape; migrated on load.
    /// * absent, empty legacy vectors, any `pin_entries` state: the split-transitional
    ///   shape written before this marker existed; accepted and upgraded on open.
    /// * `1`, empty legacy vectors, `{pin}/{blob}` entries in a hash-ordered tree: the
    ///   layout published by v1.1.10; upgraded once on open to schema 2.
    /// * `2`, empty legacy vectors, `{pin}/{generation}/{blob}` entries in a bucketed
    ///   page tree: current shape.
    ///
    /// A document mixing non-empty legacy vectors and a non-empty split registry is
    /// rejected, as is an unknown schema value. A binary predating this field rejects the
    /// document as an unknown field because the document denies unknown fields, so a
    /// rollback cannot silently read the new shape. `version` remains the ownership/paging
    /// schema and is deliberately not overloaded here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) pin_schema: Option<u8>,
    pub(super) uploads: BTreeMap<String, ObjectEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) upload_index: Option<String>,
    pub(super) tombstones: BTreeMap<String, Tombstone>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tombstone_index: Option<String>,
    /// Namespace-wide collection progress. Absent when no cycle has ever run and after the
    /// document is read by a binary that predates the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) gc: Option<GcCursor>,
}

/// Reader pin entries are addressed by `{pin}/{generation}/{blob}`. The generation component
/// lets a long-lived store retire the snapshot it no longer reads without touching the
/// reader's current pin: the durable pin identity stays, only entries older than
/// `pin_generations` may be removed. Both identities are URL-safe, so the key can live in
/// the same page tree as the other registries and a release can remove every entry
/// belonging to one pin by its prefix.
pub(super) fn pin_entry_key(pin: &str, generation: u64, blob: &str) -> String {
    format!("{pin}/{generation}/{blob}")
}

/// Parsed reader-pin key. The two-component form is the pre-generation layout written by
/// schema 1; it is read as generation zero by the one-time upgrade and never written again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PinEntryParts<'a> {
    pub pin: &'a str,
    pub generation: u64,
    pub blob: &'a str,
}

pub(super) fn pin_entry_parts(key: &str) -> Option<PinEntryParts<'_>> {
    let (pin, rest) = key.split_once('/')?;
    if pin.is_empty() {
        return None;
    }
    match rest.split_once('/') {
        Some((generation, blob)) => {
            if generation.is_empty() || blob.is_empty() || blob.contains('/') {
                return None;
            }
            Some(PinEntryParts {
                pin,
                generation: generation.parse().ok()?,
                blob,
            })
        }
        None => (!rest.is_empty() && !rest.contains('/')).then_some(PinEntryParts {
            pin,
            generation: 0,
            blob: rest,
        }),
    }
}

/// Wire schema for the split reader-pin representation.
///
/// * `1`: `{pin}/{blob}` entries in a hash-ordered page tree.
/// * `2`: `{pin}/{generation}/{blob}` entries in a bucketed page tree, plus the
///   `pin_generations` and `pin_releases` identity tables.
///
/// A schema-1 document is upgraded once when the authority opens; a binary that predates
/// schema 2 rejects it through the schema check instead of reading generation-qualified keys
/// with the old parser. Absent is accepted for the original pre-split shape and the
/// split-transitional shape; `codec::parse` distinguishes them by their contents.
pub(super) const PIN_SCHEMA_VERSION: u8 = 2;

/// Hard ceilings for the identity tables that are still inline in the authority head.
///
/// These are fail-closed bounds, not pagination: the tables are small on every current
/// workload and paging them requires a new wire shape plus change-scoped validation of the
/// branch path. Until that lands, a namespace that reaches a ceiling is rejected with an
/// explicit error instead of letting one publication CAS grow without bound. The limits are
/// high enough for the measured 128-branch matrix and still bound the encoded head below
/// [`MAX_ROOT_BYTES`].
pub const MAX_INLINE_BRANCHES: usize = 256;
pub const MAX_INLINE_SNAPSHOTS: usize = 1024;
pub const MAX_INLINE_PIN_IDENTITIES: usize = 4096;
/// Branch history is already capped per branch by [`super::RETAINED_RECOVERY_POINTS`]; this
/// bounds the sum across branches so a head at the branch ceiling cannot still encode a
/// multi-megabyte hot window by multiplying 32 points by 256 branches.
pub const MAX_INLINE_BRANCH_HISTORY: usize = 8192;
/// Ceiling for the serialized head document. The identity ceilings above derive from it: the
/// worst-case branch count at full hot history must still fit. Enforced on encode, so a
/// candidate that would exceed it fails before the CAS and leaves no published state.
pub const MAX_ROOT_BYTES: usize = 8 * 1024 * 1024;

/// Local authority roots use their own durable schema. They carry no ownership catalog and
/// page their receipts exactly like version 4, but the distinct version marks the one-way
/// migration and lets a binary that predates it reject the head instead of misreading it.
///
/// Platform roots stay on versions 2/4/5: ownership is part of their fencing contract.
pub const LOCAL_ROOT_VERSION: u32 = 6;

/// Versions whose receipt ledger is bounded: `receipts` is an inline window and older
/// entries live in the page tree addressed by `receipt_index`.
pub(super) const fn supports_paged_receipts(version: u32) -> bool {
    matches!(version, 4 | 5 | LOCAL_ROOT_VERSION)
}

/// Versions that require an ownership catalog. Version 6 is deliberately absent: a local
/// root has no leases, epochs or command ledger, so accepting one with ownership would give
/// two authorities different beliefs about the same head.
pub(super) const fn requires_ownership(version: u32) -> bool {
    matches!(version, 2 | 4 | 5)
}

/// In-memory authority head. Identity state (branches, snapshots, reader pin identities,
/// receipt window) stays inline because it is bounded by distinct identities, while volume
/// state is held by `Registry` handles that page themselves once they outgrow the inline
/// budget. The inline identity tables are capped by the `MAX_INLINE_*` constants above and
/// published to metadata metrics; they are the remaining residency risk for a namespace
/// with very many branches. `pins` keeps only the reader identities; their entries live in
/// `pin_entries` and are rejected if they ever reappear inline.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "CatalogDocument", into = "CatalogDocument")]
pub struct CatalogRoot {
    pub version: u32,
    pub revision: u64,
    pub incarnation: String,
    pub namespace: Namespace,
    pub ownership: Option<crate::ownership::types::OwnershipCatalog>,
    pub blobs: Registry<ObjectEntry>,
    pub receipts: BTreeMap<String, u64>,
    pub receipt_index: Option<String>,
    pub objects: Registry<ObjectEntry>,
    pub branches: BTreeMap<String, BranchRef>,
    pub snapshots: BTreeMap<String, RecoveryPoint>,
    /// The map values are empty in the split representation; reader entries live in
    /// `pin_entries`. Pre-split documents carry entries here with no split registry;
    /// split-transitional documents carry an empty vector side and entries only in the
    /// split registry. `semantic::migrate_legacy_pins` moves the pre-split vectors on load;
    /// a document with both sides populated is rejected by `codec::parse`.
    pub pins: BTreeMap<String, Vec<ObjectEntry>>,
    pub(crate) pin_entries: Registry<ObjectEntry>,
    pub(crate) pin_generations: BTreeMap<String, u64>,
    pub(crate) pin_releases: BTreeSet<String>,
    pub(super) pin_schema: Option<u8>,
    pub uploads: Registry<ObjectEntry>,
    pub tombstones: Registry<Tombstone>,
    /// Durable position of the bounded collection state machine; see [`GcCursor`].
    pub gc: Option<GcCursor>,
}

impl CatalogRoot {
    pub(super) fn empty(namespace: Namespace, incarnation: String) -> Self {
        Self {
            version: 1,
            revision: 1,
            incarnation,
            namespace,
            ownership: None,
            blobs: Registry::inline(BTreeMap::new()),
            receipts: BTreeMap::new(),
            receipt_index: None,
            objects: Registry::inline(BTreeMap::new()),
            branches: BTreeMap::from([("main".into(), BranchRef::default())]),
            snapshots: BTreeMap::new(),
            pins: BTreeMap::new(),
            pin_entries: Registry::inline_with_order(
                BTreeMap::new(),
                super::paged_index::KeyOrder::Bucketed,
            ),
            pin_generations: BTreeMap::new(),
            pin_releases: BTreeSet::new(),
            pin_schema: Some(PIN_SCHEMA_VERSION),
            uploads: Registry::inline(BTreeMap::new()),
            tombstones: Registry::inline(BTreeMap::new()),
            gc: None,
        }
    }

    /// Absent-root claim for the local authority. `empty` stays the platform shape because
    /// `Authority::open` and the ownership boot still start from version 1; local mode pins
    /// the durable schema at creation so a fresh store never needs a migration publication.
    pub(super) fn local_empty(namespace: Namespace, incarnation: String) -> Self {
        let mut root = Self::empty(namespace, incarnation);
        root.version = LOCAL_ROOT_VERSION;
        root
    }

    pub(super) fn attach(
        &mut self,
        store: std::sync::Arc<dyn crate::object_store::ObjectStore>,
        namespace: Namespace,
    ) {
        for registry in [
            &mut self.blobs,
            &mut self.objects,
            &mut self.pin_entries,
            &mut self.uploads,
        ] {
            registry.attach(store.clone(), namespace.clone());
        }
        self.tombstones.attach(store, namespace);
    }

    /// A registry either carries its state inline or points at a page tree, never both.
    pub(super) fn validate_shape(&self) -> anyhow::Result<()> {
        for (name, pointer, inline) in [
            ("blobs", self.blobs.pointer(), self.blobs.window_len()),
            ("objects", self.objects.pointer(), self.objects.window_len()),
            (
                "pin_entries",
                self.pin_entries.pointer(),
                self.pin_entries.window_len(),
            ),
            ("uploads", self.uploads.pointer(), self.uploads.window_len()),
            (
                "tombstones",
                self.tombstones.pointer(),
                self.tombstones.window_len(),
            ),
        ] {
            anyhow::ensure!(
                pointer.is_none() || inline == 0,
                "registry {name} mixes inline state with a page pointer"
            );
            anyhow::ensure!(
                pointer.is_none_or(|pointer| pointer.len() == 64
                    && pointer
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))),
                "registry {name} pointer is not a page identity"
            );
        }
        // Identity tables are inline; refuse to decode or publish a head beyond the explicit
        // ceilings rather than letting the CAS payload grow without a bound.
        self.validate_identity_ceilings()?;
        let mut history_points = 0usize;
        for (name, branch) in &self.branches {
            anyhow::ensure!(
                !name.is_empty() && name.len() <= 128,
                "branch name is not a reference identity"
            );
            branch.validate_recovery()?;
            history_points = history_points.saturating_add(branch.history.len());
        }
        anyhow::ensure!(
            history_points <= MAX_INLINE_BRANCH_HISTORY,
            "authority branch history ceiling exceeded: {history_points} > {MAX_INLINE_BRANCH_HISTORY}"
        );
        if let Some(cursor) = &self.gc {
            anyhow::ensure!(
                cursor
                    .after
                    .as_ref()
                    .is_none_or(|key| !key.is_empty() && key.len() <= 1024),
                "GC cursor frontier is not a registry key"
            );
            anyhow::ensure!(
                cursor.phase != GcPhase::Done || cursor.after.is_none(),
                "completed GC cursor still carries a frontier"
            );
        }
        for (pin, generation) in &self.pin_generations {
            anyhow::ensure!(
                self.pins.contains_key(pin),
                "reader pin generation without pin"
            );
            anyhow::ensure!(*generation < u64::MAX, "reader pin generation exhausted");
        }
        for pin in &self.pin_releases {
            anyhow::ensure!(
                self.pins.contains_key(pin) && self.pin_generations.contains_key(pin),
                "reader pin release without pin"
            );
        }
        Ok(())
    }

    /// Guard for the publication path: no registry may still hold unreleased changes when
    /// the head is encoded, and the encoded window must respect the inline budget.
    pub(super) fn seal(&self) -> anyhow::Result<()> {
        self.validate_shape()?;
        anyhow::ensure!(
            !self.blobs.has_changes()
                && !self.objects.has_changes()
                && !self.pin_entries.has_changes()
                && !self.uploads.has_changes()
                && !self.tombstones.has_changes(),
            "registry changes were not published"
        );
        Ok(())
    }

    /// Cheap fail-closed bound for the inline identity tables. Called by `validate_shape` and
    /// by the decoder, so a document that predates these ceilings is rejected when read
    /// instead of being trusted until its next publication. O(number of identities), never
    /// O(entries in a paged registry).
    pub(super) fn validate_identity_ceilings(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.branches.len() <= MAX_INLINE_BRANCHES,
            "authority branch identity ceiling exceeded: {} > {MAX_INLINE_BRANCHES}",
            self.branches.len()
        );
        anyhow::ensure!(
            self.snapshots.len() <= MAX_INLINE_SNAPSHOTS,
            "authority snapshot identity ceiling exceeded: {} > {MAX_INLINE_SNAPSHOTS}",
            self.snapshots.len()
        );
        anyhow::ensure!(
            self.pins.len() <= MAX_INLINE_PIN_IDENTITIES,
            "authority reader pin identity ceiling exceeded: {} > {MAX_INLINE_PIN_IDENTITIES}",
            self.pins.len()
        );
        anyhow::ensure!(
            self.pin_generations.len() <= self.pins.len()
                && self.pin_releases.len() <= self.pins.len(),
            "reader pin generation or release table exceeds the pin identity ceiling"
        );
        Ok(())
    }

    /// Population of the identity tables that still live inline, plus the branch hot-window
    /// points they carry. Reported per published head so the residual head growth is visible
    /// next to `root_encoded_bytes`; a paged registry contributes zero.
    pub(crate) fn identity_counts(&self) -> IdentityCounts {
        IdentityCounts {
            branches: self.branches.len(),
            snapshots: self.snapshots.len(),
            pin_identities: self.pins.len(),
            branch_history_points: self
                .branches
                .values()
                .map(|branch| branch.history.len())
                .sum(),
        }
    }
}

/// Inline identity populations measured on one encoded authority head.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IdentityCounts {
    pub branches: usize,
    pub snapshots: usize,
    pub pin_identities: usize,
    pub branch_history_points: usize,
}

impl From<CatalogDocument> for CatalogRoot {
    fn from(document: CatalogDocument) -> Self {
        let pin_order = match document.pin_schema {
            Some(2) => super::paged_index::KeyOrder::Bucketed,
            _ => super::paged_index::KeyOrder::Hashed,
        };
        Self {
            version: document.version,
            revision: document.revision,
            incarnation: document.incarnation,
            namespace: document.namespace,
            ownership: document.ownership,
            blobs: Registry::paged(document.blob_index, document.blobs),
            receipts: document.receipts,
            receipt_index: document.receipt_index,
            objects: Registry::paged(document.object_index, document.objects),
            branches: document.branches,
            snapshots: document.snapshots,
            pins: document.pins,
            pin_entries: Registry::paged_with_order(
                document.pin_index,
                document.pin_entries,
                pin_order,
            ),
            pin_generations: document.pin_generations,
            pin_releases: document.pin_releases,
            pin_schema: document.pin_schema,
            uploads: Registry::paged(document.upload_index, document.uploads),
            tombstones: Registry::paged(document.tombstone_index, document.tombstones),
            gc: document.gc,
        }
    }
}

impl From<CatalogRoot> for CatalogDocument {
    fn from(root: CatalogRoot) -> Self {
        let (blob_index, blobs) = root.blobs.into_parts();
        let (object_index, objects) = root.objects.into_parts();
        let (upload_index, uploads) = root.uploads.into_parts();
        let (pin_index, pin_entries) = root.pin_entries.into_parts();
        let (tombstone_index, tombstones) = root.tombstones.into_parts();
        Self {
            version: root.version,
            revision: root.revision,
            incarnation: root.incarnation,
            namespace: root.namespace,
            ownership: root.ownership,
            blobs,
            blob_index,
            receipts: root.receipts,
            receipt_index: root.receipt_index,
            objects,
            object_index,
            branches: root.branches,
            snapshots: root.snapshots,
            pins: root.pins,
            pin_entries,
            pin_index,
            pin_generations: root.pin_generations,
            pin_releases: root.pin_releases,
            pin_schema: root.pin_schema,
            uploads,
            upload_index,
            tombstones,
            tombstone_index,
            gc: root.gc,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RootSnapshot {
    pub root: CatalogRoot,
    pub etag: String,
}
