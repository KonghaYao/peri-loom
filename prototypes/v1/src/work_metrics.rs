//! Optional workload counters for the T0 metric groups: WAL work, authority metadata,
//! lock/execution time and SQL result encoding. Off unless `PERI_WORK_METRICS=1`, and off
//! means the call sites skip
//! the clock read and the atomic: the counters are not a second copy of work already
//! measured, they are the same work points reporting into a process-wide sink.
//!
//! Every counter is a bare `AtomicU64` in a static: no label, no key, no allocation and no
//! lock. Database and request identity deliberately stays out of this module — a per
//! database label would grow without bound, so identity attribution belongs to the sampling
//! traces (`capacity_trace`), not to counters that are meant to be exportable as-is.
//! Each load-add-store pair would lose concurrent increments, so every update is a
//! `fetch_add`, and the snapshot reads them with `Relaxed` ordering: these counters order
//! nothing, they only report work that already happened.
//!
//! What the units mean is stated per field below; the delivery note under
//! `docs/performance-review/` repeats the same list with collection points and explicitly
//! lists what these counters do not measure. Enabling the switch never changes SQL, WAL,
//! publication or recovery behaviour: recording happens after the decision the work point
//! already made.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io::Write as _;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::MutexGuard;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;

const ENABLED_ENV: &str = "PERI_WORK_METRICS";

/// Opt-in exit snapshot channel. It is independent of `PERI_WORK_METRICS`: the counters are
/// only recorded when that switch is on, and this variable only decides where the process
/// writes them when it closes. Unset (or empty), the exit path reads no clock, allocates
/// nothing and touches no file.
pub(crate) const SNAPSHOT_PATH_ENV: &str = "PERI_METRICS_SNAPSHOT_PATH";

// The environment is consulted once per process; `UNSET` is the state before that read.
const UNSET: u8 = 0;
const OFF: u8 = 1;
const ON: u8 = 2;
static ENABLED: AtomicU8 = AtomicU8::new(UNSET);

pub(crate) fn enabled() -> bool {
    match ENABLED.load(Ordering::Relaxed) {
        ON => true,
        OFF => false,
        _ => {
            let on = std::env::var(ENABLED_ENV).as_deref() == Ok("1");
            ENABLED.store(if on { ON } else { OFF }, Ordering::Relaxed);
            on
        }
    }
}

/// Test-only: the counters are meant to be reachable in a test process, and a test cannot
/// set the environment variable without becoming the process-wide first reader.
#[cfg(test)]
pub(crate) fn set_enabled(value: bool) {
    ENABLED.store(if value { ON } else { OFF }, Ordering::Relaxed);
}

#[inline]
fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

#[inline]
fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

#[inline]
fn nanos(elapsed: Duration) -> u64 {
    elapsed.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn named(names: &[&'static str], counters: &[AtomicU64]) -> BTreeMap<&'static str, u64> {
    names
        .iter()
        .zip(counters)
        .map(|(name, counter)| (*name, load(counter)))
        .collect()
}

/// Same as [`named`], but the labels come from the enum that owns the counters, so a renamed
/// or reordered variant cannot silently mislabel a count.
fn report<T: Copy>(
    items: &[T],
    name: impl Fn(T) -> &'static str,
    counters: &[AtomicU64],
) -> BTreeMap<&'static str, u64> {
    items
        .iter()
        .zip(counters)
        .map(|(item, counter)| (name(*item), load(counter)))
        .collect()
}

fn report_origin(
    items: &[PublishOrigin],
    name: impl Fn(PublishOrigin) -> &'static str,
) -> BTreeMap<&'static str, PublishOriginCount> {
    items
        .iter()
        .map(|origin| {
            (
                name(*origin),
                PublishOriginCount {
                    calls: load(&PUBLISH_ORIGIN_CALLS[origin.index()]),
                },
            )
        })
        .collect()
}

fn report_outside_transact(
    items: &[OutsideTransactKind],
    name: impl Fn(OutsideTransactKind) -> &'static str,
) -> BTreeMap<&'static str, PublishOutsideTransactCount> {
    items
        .iter()
        .map(|kind| {
            (
                name(*kind),
                PublishOutsideTransactCount {
                    calls: load(&PUBLISH_OUTSIDE_TRANSACT_CALLS[kind.index()]),
                },
            )
        })
        .collect()
}

// --- A. WAL workload ---------------------------------------------------------------------

/// Why a verified prefix was dropped. Each reason names the mutation that made the prefix
/// describe bytes the image no longer has, so a rise in one of them tells a review which
/// WAL pattern abandoned the incremental path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanReset {
    /// The image shrank below the examined prefix without reporting a mutation.
    Shrink,
    /// The image never reached a full WAL header.
    ShortImage,
    /// The 32-byte header changed: the image was restarted, rewritten or replaced.
    HeaderChanged,
    /// A pending write starts inside the examined prefix.
    WriteInPrefix,
    /// A pending truncate cuts into the examined prefix.
    TruncateInPrefix,
    /// Test-only control arm that re-derives the checksum chain on every scan. The arm does
    /// not exist in a production build, so its counter is exported and stays zero there.
    #[cfg(test)]
    ForcedWholeImage,
}

const RESET_NAMES: [&str; 6] = [
    "shrink",
    "short_image",
    "header_changed",
    "write_in_prefix",
    "truncate_in_prefix",
    "forced_whole_image",
];
static RESETS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];

/// Why a scan stopped extending the prefix. These are checksum-chain failures of the image
/// itself, not of the caller's batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanStop {
    /// A frame carried page number 0 or salts other than the header's.
    SaltMismatch,
    /// The rolling checksum of a frame did not match the value the frame stores.
    ChecksumMismatch,
    /// The tail was shorter than one frame, so no position could be derived from it.
    IncompleteTail,
}

const STOP_NAMES: [&str; 3] = ["salt_mismatch", "checksum_mismatch", "incomplete_tail"];
static STOPS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];

/// How the WAL append decided its commit position. Only `Reused` exercises the incremental
/// authority validation (`validate_index_append`); the others send no position and make the
/// authority re-derive the whole index, which is the cost a review wants to see bounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AppendPosition {
    /// The scan produced a position the pending batch covers and the previous append had
    /// not already published.
    Reused,
    /// The scan found no committed position for the image.
    Unscanned,
    /// The scan returned exactly the position the previous append published.
    Unchanged,
    /// The pending batch does not cover the scanned position.
    NotCovered,
}

const POSITION_NAMES: [&str; 4] = ["reused", "unscanned", "unchanged", "not_covered"];
static POSITIONS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];

static WAL_APPENDS: AtomicU64 = AtomicU64::new(0);
static WAL_NATIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_DELTA_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_POSITION_FRAMES: AtomicU64 = AtomicU64::new(0);
static SCAN_CALLS: AtomicU64 = AtomicU64::new(0);
static SCANNED_BYTES: AtomicU64 = AtomicU64::new(0);
static SCANNED_FRAMES: AtomicU64 = AtomicU64::new(0);
static COPIED_BYTES: AtomicU64 = AtomicU64::new(0);
static SCAN_NANOS: AtomicU64 = AtomicU64::new(0);

/// One WAL image scan: the bytes and frames the checksum chain actually walked, and the
/// wall time inside that walk. Bytes and frames are counted where the scan advances, not by
/// a second pass over the image.
pub(crate) fn observe_scan(scanned_bytes: usize, scanned_frames: usize, elapsed: Duration) {
    if !enabled() {
        return;
    }
    add(&SCAN_CALLS, 1);
    add(&SCANNED_BYTES, scanned_bytes as u64);
    add(&SCANNED_FRAMES, scanned_frames as u64);
    add(&SCAN_NANOS, nanos(elapsed));
}

/// Bytes of a WAL image this process had to copy while validating. An append extends the
/// accepted image in place, so a steady-state append copies nothing; a batch that outgrows
/// the buffer may still reallocate, and the old capacity is the upper bound of what that
/// reallocation can copy (the allocator may also extend in place). The count is image
/// bytes, not the bytes the append itself wrote.
pub(crate) fn observe_copy(bytes: usize) {
    if !enabled() {
        return;
    }
    add(&COPIED_BYTES, bytes as u64);
}

pub(crate) fn observe_scan_reset(reason: ScanReset) {
    if !enabled() {
        return;
    }
    add(&RESETS[reason as usize], 1);
}

pub(crate) fn observe_scan_stop(reason: ScanStop) {
    if !enabled() {
        return;
    }
    add(&STOPS[reason as usize], 1);
}

/// One WAL append attempt with a pending batch: the image size it had to reason about, the
/// payload it asked the WAL to write, and how its commit position was resolved.
pub(crate) fn observe_append(
    position: AppendPosition,
    native_bytes: usize,
    delta_bytes: u64,
    position_frames: u64,
) {
    if !enabled() {
        return;
    }
    add(&WAL_APPENDS, 1);
    add(&WAL_NATIVE_BYTES, native_bytes as u64);
    add(&WAL_DELTA_BYTES, delta_bytes);
    if position == AppendPosition::Reused {
        add(&WAL_POSITION_FRAMES, position_frames);
    }
    add(&POSITIONS[position as usize], 1);
}

// --- B. authority metadata ---------------------------------------------------------------

static ROOT_ENCODED_BYTES: AtomicU64 = AtomicU64::new(0);
static ROOT_PUBLISH_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
static ROOT_PUBLISH_RETRIES: AtomicU64 = AtomicU64::new(0);
static INLINE_PIN_REFS: AtomicU64 = AtomicU64::new(0);
static PIN_RELEASE_FAILURES: AtomicU64 = AtomicU64::new(0);
static ROOT_RECEIPTS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_COMMITS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_RECEIPTS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_RETRIES: AtomicU64 = AtomicU64::new(0);
static INDEX_PAGE_READS: AtomicU64 = AtomicU64::new(0);
static INDEX_PAGE_WRITES: AtomicU64 = AtomicU64::new(0);
static INDEX_PAGE_WRITE_BYTES: AtomicU64 = AtomicU64::new(0);
static ROOT_ENCODED_MAX_BYTES: AtomicU64 = AtomicU64::new(0);
static INLINE_BRANCHES: AtomicU64 = AtomicU64::new(0);
static INLINE_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);
static INLINE_PIN_IDENTITIES: AtomicU64 = AtomicU64::new(0);
static BRANCH_HISTORY_POINTS: AtomicU64 = AtomicU64::new(0);
static MAX_INLINE_BRANCHES: AtomicU64 = AtomicU64::new(0);
static MAX_INLINE_SNAPSHOTS: AtomicU64 = AtomicU64::new(0);
static MAX_INLINE_PIN_IDENTITIES: AtomicU64 = AtomicU64::new(0);
static MAX_BRANCH_HISTORY_POINTS: AtomicU64 = AtomicU64::new(0);

/// One encoded authority candidate: the head document size the publication CAS carries,
/// the inline reader-pin entries left in that document, and its receipts. Paged pin entries
/// are deliberately not counted here; `metadata.inline_pin_refs` names the cost the head
/// itself still carries.
pub(crate) fn observe_root_publish(encoded_bytes: usize, inline_pin_refs: usize, receipts: usize) {
    if !enabled() {
        return;
    }
    add(&ROOT_PUBLISH_ATTEMPTS, 1);
    add(&ROOT_ENCODED_BYTES, encoded_bytes as u64);
    add(&INLINE_PIN_REFS, inline_pin_refs as u64);
    add(&ROOT_RECEIPTS, receipts as u64);
}

/// A reader-pin release that could not be published. Drop cannot return an error, so the
/// failure is counted and logged by the caller with the pin identity and full cause.
pub(crate) fn observe_pin_release_failure() {
    if !enabled() {
        return;
    }
    add(&PIN_RELEASE_FAILURES, 1);
}

pub(crate) fn observe_root_publish_retry() {
    if !enabled() {
        return;
    }
    add(&ROOT_PUBLISH_RETRIES, 1);
}

/// Why a publication attempt ran. One attempt charges exactly one origin at the same point
/// `observe_root_publish` charges the attempt, so the origin totals reconcile with
/// `metadata.root_publish_attempts` without a second bookkeeping path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishOrigin {
    Append,
    WalPages,
    PinExtend,
    BranchOpen,
    PinRelease,
    Put,
    PutGroup,
    Delete,
    List,
    Gc,
    Capability,
    Lease,
    Migration,
    PinUpgrade,
    Recovery,
    Other,
}

impl PublishOrigin {
    pub(crate) const ALL: [Self; 16] = [
        Self::Append,
        Self::WalPages,
        Self::PinExtend,
        Self::BranchOpen,
        Self::PinRelease,
        Self::Put,
        Self::PutGroup,
        Self::Delete,
        Self::List,
        Self::Gc,
        Self::Capability,
        Self::Lease,
        Self::Migration,
        Self::PinUpgrade,
        Self::Recovery,
        Self::Other,
    ];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::WalPages => "wal_pages",
            Self::PinExtend => "pin_extend",
            Self::BranchOpen => "branch_open",
            Self::PinRelease => "pin_release",
            Self::Put => "put",
            Self::PutGroup => "put_group",
            Self::Delete => "delete",
            Self::List => "list",
            Self::Gc => "gc",
            Self::Capability => "capability",
            Self::Lease => "lease",
            Self::Migration => "migration",
            Self::PinUpgrade => "pin_upgrade",
            Self::Recovery => "recovery",
            Self::Other => "other",
        }
    }
}

/// Publications that bypass `transact_inner` because their group is assembled by hand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutsideTransactKind {
    Migration,
    Other,
}

impl OutsideTransactKind {
    pub(crate) const ALL: [Self; 2] = [Self::Migration, Self::Other];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Migration => "migration",
            Self::Other => "other",
        }
    }
}

static PUBLISH_ORIGIN_CALLS: [AtomicU64; 16] = [const { AtomicU64::new(0) }; 16];
static PUBLISH_OUTSIDE_TRANSACT_CALLS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];

pub(crate) fn observe_publish_origin(origin: PublishOrigin) {
    if !enabled() {
        return;
    }
    add(&PUBLISH_ORIGIN_CALLS[origin.index()], 1);
}

pub(crate) fn observe_publish_outside_transact(kind: OutsideTransactKind) {
    if !enabled() {
        return;
    }
    add(&PUBLISH_OUTSIDE_TRANSACT_CALLS[kind.index()], 1);
}

/// Which half of the store a group published into. The bucket is decided from the first
/// write's relative key, exactly as the stderr diagnostic already did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum GroupClass {
    Wal,
    Receipt,
    Authority,
    #[default]
    Other,
}

impl GroupClass {
    pub(crate) const ALL: [Self; 4] = [Self::Wal, Self::Receipt, Self::Authority, Self::Other];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Wal => "wal",
            Self::Receipt => "receipt",
            Self::Authority => "authority",
            Self::Other => "other",
        }
    }
}

/// The stage timings one local group publication already measures. Passing them here keeps
/// the aggregation free of a second set of clocks.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GroupTimings {
    pub barrier_ns: u64,
    pub body_fsync_ns: u64,
    pub dir_flush_ns: u64,
    pub total_ns: u64,
}

static GROUP_CALLS: AtomicU64 = AtomicU64::new(0);
static GROUP_WRITES_ONE: AtomicU64 = AtomicU64::new(0);
static GROUP_WRITES_TWO_TO_FOUR: AtomicU64 = AtomicU64::new(0);
static GROUP_WRITES_FIVE_TO_SIXTEEN: AtomicU64 = AtomicU64::new(0);
static GROUP_WRITES_SEVENTEEN_PLUS: AtomicU64 = AtomicU64::new(0);
static GROUP_CLASS_CALLS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static GROUP_BARRIER_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_BODY_FSYNC_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_DIR_FLUSH_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_TOTAL_NS: AtomicU64 = AtomicU64::new(0);
static GROUP_TOTAL_MAX_NS: AtomicU64 = AtomicU64::new(0);

/// One successful local `put_group`. The write-count buckets exist because the same call
/// covers a one-write head publication and a several-hundred-body receipt roll; a mean would
/// hide which of the two dominates the tail.
pub(crate) fn observe_group(writes: usize, class: GroupClass, timings: GroupTimings) {
    if !enabled() {
        return;
    }
    add(&GROUP_CALLS, 1);
    let bucket = match writes {
        0 | 1 => &GROUP_WRITES_ONE,
        2..=4 => &GROUP_WRITES_TWO_TO_FOUR,
        5..=16 => &GROUP_WRITES_FIVE_TO_SIXTEEN,
        _ => &GROUP_WRITES_SEVENTEEN_PLUS,
    };
    add(bucket, 1);
    add(&GROUP_CLASS_CALLS[class.index()], 1);
    add(&GROUP_BARRIER_NS, timings.barrier_ns);
    add(&GROUP_BODY_FSYNC_NS, timings.body_fsync_ns);
    add(&GROUP_DIR_FLUSH_NS, timings.dir_flush_ns);
    add(&GROUP_TOTAL_NS, timings.total_ns);
    GROUP_TOTAL_MAX_NS.fetch_max(timings.total_ns, Ordering::Relaxed);
}

/// One encoded head document, measured at the point the publication CAS carries it. The
/// identity counts are the inline populations the head still holds; the paged registries
/// contribute nothing because they live behind a pointer. `root_encoded_bytes` is the same
/// document, summed per attempt by `observe_root_publish`; this sink adds the peak and the
/// residual inline growth, so a namespace approaching an identity ceiling is visible before
/// the fail-closed check rejects it.
pub(crate) fn observe_root_encoding(
    encoded_bytes: usize,
    identities: crate::authority::types::IdentityCounts,
) {
    if !enabled() {
        return;
    }
    let bytes = encoded_bytes as u64;
    ROOT_ENCODED_MAX_BYTES.fetch_max(bytes, Ordering::Relaxed);
    add(&INLINE_BRANCHES, identities.branches as u64);
    add(&INLINE_SNAPSHOTS, identities.snapshots as u64);
    add(&INLINE_PIN_IDENTITIES, identities.pin_identities as u64);
    add(
        &BRANCH_HISTORY_POINTS,
        identities.branch_history_points as u64,
    );
    MAX_INLINE_BRANCHES.fetch_max(identities.branches as u64, Ordering::Relaxed);
    MAX_INLINE_SNAPSHOTS.fetch_max(identities.snapshots as u64, Ordering::Relaxed);
    MAX_INLINE_PIN_IDENTITIES.fetch_max(identities.pin_identities as u64, Ordering::Relaxed);
    MAX_BRANCH_HISTORY_POINTS.fetch_max(identities.branch_history_points as u64, Ordering::Relaxed);
}

/// The stages of one authority publication. A publication is the unit a slow statement hides
/// in, so the longest single stage is what says whether the cost is the root snapshot, the
/// catalog work, the encoded head, or the store group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishStage {
    Snapshot,
    Clone,
    Mutate,
    Prepare,
    Receipt,
    Validate,
    Index,
    Seal,
    Encode,
    Group,
    Recover,
}

impl PublishStage {
    pub(crate) const ALL: [Self; 11] = [
        Self::Snapshot,
        Self::Clone,
        Self::Mutate,
        Self::Prepare,
        Self::Receipt,
        Self::Validate,
        Self::Index,
        Self::Seal,
        Self::Encode,
        Self::Group,
        Self::Recover,
    ];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Clone => "clone",
            Self::Mutate => "mutate",
            Self::Prepare => "prepare",
            Self::Receipt => "receipt",
            Self::Validate => "validate",
            Self::Index => "index",
            Self::Seal => "seal",
            Self::Encode => "encode",
            Self::Group => "group",
            Self::Recover => "recover",
        }
    }
}

static PUBLISH_STAGE_CALLS: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];
static PUBLISH_STAGE_NANOS: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];
static PUBLISH_STAGE_MAX_NANOS: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];
static PUBLISH_TOTAL_NANOS: AtomicU64 = AtomicU64::new(0);
static PUBLISH_TOTAL_MAX_NANOS: AtomicU64 = AtomicU64::new(0);

/// Timer for one authority publication attempt. Disabled, it carries no clock and every call
/// below is a branch on a missing instant.
pub(crate) struct PublishTimer {
    started: Option<Instant>,
    stage: Option<(PublishStage, Instant)>,
}

impl PublishTimer {
    pub(crate) fn begin() -> Self {
        Self {
            started: enabled().then(Instant::now),
            stage: None,
        }
    }

    /// Close the stage that just ended and start the next one. A stage is charged the wall time
    /// between its own start and the next transition. The last stage also carries whatever the
    /// attempt does after its start — write assembly, guard validation and, for `Group`, the
    /// post-commit bookkeeping — so the attempt total stays the wall time from begin to drop
    /// and the stages never exceed it.
    pub(crate) fn stage(&mut self, stage: PublishStage) {
        if self.started.is_none() {
            return;
        }
        self.close_stage();
        self.stage = Some((stage, Instant::now()));
    }

    fn close_stage(&mut self) {
        let Some((stage, started)) = self.stage.take() else {
            return;
        };
        let elapsed = nanos(started.elapsed());
        let index = stage.index();
        add(&PUBLISH_STAGE_CALLS[index], 1);
        add(&PUBLISH_STAGE_NANOS[index], elapsed);
        PUBLISH_STAGE_MAX_NANOS[index].fetch_max(elapsed, Ordering::Relaxed);
    }
}

impl Drop for PublishTimer {
    fn drop(&mut self) {
        let Some(started) = self.started else {
            return;
        };
        self.close_stage();
        let elapsed = nanos(started.elapsed());
        add(&PUBLISH_TOTAL_NANOS, elapsed);
        PUBLISH_TOTAL_MAX_NANOS.fetch_max(elapsed, Ordering::Relaxed);
    }
}

/// The steps inside one head read. `PublishStage::Snapshot` says how long a publication spent
/// reading the head; this split says whether that was the store, the decode, the referenced
/// metadata, or the full walk a head this process did not verify has to pay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SnapshotStage {
    Guard,
    Store,
    Decode,
    Admit,
    Head,
    Refs,
    Deep,
}

impl SnapshotStage {
    pub(crate) const ALL: [Self; 7] = [
        Self::Guard,
        Self::Store,
        Self::Decode,
        Self::Admit,
        Self::Head,
        Self::Refs,
        Self::Deep,
    ];

    pub(crate) fn index(self) -> usize {
        self as usize
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Guard => "guard",
            Self::Store => "store",
            Self::Decode => "decode",
            Self::Admit => "admit",
            Self::Head => "head",
            Self::Refs => "refs",
            Self::Deep => "deep",
        }
    }
}

static SNAPSHOT_STAGE_CALLS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static SNAPSHOT_STAGE_NANOS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static SNAPSHOT_STAGE_MAX_NANOS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static SNAPSHOT_TOTAL_NANOS: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_TOTAL_MAX_NANOS: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_ROOT_BYTES: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_ROOT_MAX_BYTES: AtomicU64 = AtomicU64::new(0);

/// Timer for one `Authority::snapshot` call. A `deep` stage is only charged when the head was
/// not already verified by this process, so its count is the number of full walks.
pub(crate) struct SnapshotTimer {
    started: Option<Instant>,
    stage: Option<(SnapshotStage, Instant)>,
    root_bytes: usize,
}

impl SnapshotTimer {
    pub(crate) fn begin() -> Self {
        Self {
            started: enabled().then(Instant::now),
            stage: None,
            root_bytes: 0,
        }
    }

    pub(crate) fn stage(&mut self, stage: SnapshotStage) {
        if self.started.is_none() {
            return;
        }
        self.close_stage();
        self.stage = Some((stage, Instant::now()));
    }

    pub(crate) fn root_bytes(&mut self, bytes: usize) {
        if self.started.is_some() {
            self.root_bytes = bytes;
        }
    }

    fn close_stage(&mut self) {
        let Some((stage, started)) = self.stage.take() else {
            return;
        };
        let elapsed = nanos(started.elapsed());
        let index = stage.index();
        add(&SNAPSHOT_STAGE_CALLS[index], 1);
        add(&SNAPSHOT_STAGE_NANOS[index], elapsed);
        SNAPSHOT_STAGE_MAX_NANOS[index].fetch_max(elapsed, Ordering::Relaxed);
    }
}

impl Drop for SnapshotTimer {
    fn drop(&mut self) {
        let Some(started) = self.started else {
            return;
        };
        self.close_stage();
        let elapsed = nanos(started.elapsed());
        add(&SNAPSHOT_TOTAL_NANOS, elapsed);
        SNAPSHOT_TOTAL_MAX_NANOS.fetch_max(elapsed, Ordering::Relaxed);
        add(&SNAPSHOT_ROOT_BYTES, self.root_bytes as u64);
        SNAPSHOT_ROOT_MAX_BYTES.fetch_max(self.root_bytes as u64, Ordering::Relaxed);
    }
}

/// One `wal/index` publication: the encoded index size, the idempotency receipts it has to
/// carry, and one retry when the conditional write lost its race.
pub(crate) fn observe_wal_index_commit(encoded_bytes: usize, receipts: usize) {
    if !enabled() {
        return;
    }
    add(&WAL_INDEX_COMMITS, 1);
    add(&WAL_INDEX_BYTES, encoded_bytes as u64);
    add(&WAL_INDEX_RECEIPTS, receipts as u64);
}

pub(crate) fn observe_wal_index_retry() {
    if !enabled() {
        return;
    }
    add(&WAL_INDEX_RETRIES, 1);
}

/// One paged-index page body read or written. Page bytes are counted where the body is
/// encoded, so a read that was served from the caller's staged group is not counted twice.
pub(crate) fn observe_index_page_read() {
    if !enabled() {
        return;
    }
    add(&INDEX_PAGE_READS, 1);
}

pub(crate) fn observe_index_page_write(encoded_bytes: usize) {
    if !enabled() {
        return;
    }
    add(&INDEX_PAGE_WRITES, 1);
    add(&INDEX_PAGE_WRITE_BYTES, encoded_bytes as u64);
}

// --- D. bounded garbage collection ------------------------------------------------------
//
// GC is the one maintenance path that previously held a namespace exclusive barrier for the
// whole mark and sweep. These counters make the replacement observable: how long a tick
// waited for and held the barrier, how much of the registry it actually scanned, how many
// deletions it asked the store for, and why it stopped before finishing. The interruption
// names are the contract used in the RPC result, so they are kept in one array.
pub(crate) const GC_INTERRUPTION_NAMES: [&str; 8] = [
    "scan_budget",
    "mark_budget",
    "prune_budget",
    "delete_budget",
    "time_budget",
    "admission_busy",
    "cas_conflict",
    "revision_changed",
];

static GC_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GC_RESUMED: AtomicU64 = AtomicU64::new(0);
static GC_COMPLETED: AtomicU64 = AtomicU64::new(0);
static GC_BATCHES: AtomicU64 = AtomicU64::new(0);
static GC_SCANNED_PAGES: AtomicU64 = AtomicU64::new(0);
static GC_SCANNED_ENTRIES: AtomicU64 = AtomicU64::new(0);
static GC_SCANNED_BYTES: AtomicU64 = AtomicU64::new(0);
static GC_MARKED: AtomicU64 = AtomicU64::new(0);
static GC_DELETED: AtomicU64 = AtomicU64::new(0);
static GC_DELETE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static GC_DELETE_FAILURES: AtomicU64 = AtomicU64::new(0);
static GC_WAIT_NANOS: AtomicU64 = AtomicU64::new(0);
static GC_MAX_WAIT_NANOS: AtomicU64 = AtomicU64::new(0);
static GC_EXCLUSIVE_NANOS: AtomicU64 = AtomicU64::new(0);
static GC_MAX_EXCLUSIVE_NANOS: AtomicU64 = AtomicU64::new(0);
static GC_INTERRUPTIONS: [AtomicU64; GC_INTERRUPTION_NAMES.len()] =
    [const { AtomicU64::new(0) }; GC_INTERRUPTION_NAMES.len()];
static LEASE_RENEWALS: AtomicU64 = AtomicU64::new(0);
static LEASE_REMAINING_MS: AtomicU64 = AtomicU64::new(0);
static LEASE_REMAINING_MIN_MS: AtomicU64 = AtomicU64::new(u64::MAX);

pub(crate) fn observe_gc_request(resumed: bool) {
    if !enabled() {
        return;
    }
    add(&GC_REQUESTS, 1);
    if resumed {
        add(&GC_RESUMED, 1);
    }
}

pub(crate) fn observe_gc_completed() {
    if !enabled() {
        return;
    }
    add(&GC_COMPLETED, 1);
}

pub(crate) fn observe_gc_batch() {
    if !enabled() {
        return;
    }
    add(&GC_BATCHES, 1);
}

pub(crate) fn observe_gc_scan(pages: usize, entries: usize, bytes: usize) {
    if !enabled() {
        return;
    }
    add(&GC_SCANNED_PAGES, pages as u64);
    add(&GC_SCANNED_ENTRIES, entries as u64);
    add(&GC_SCANNED_BYTES, bytes as u64);
}

pub(crate) fn observe_gc_marked(count: usize) {
    if !enabled() {
        return;
    }
    add(&GC_MARKED, count as u64);
}

pub(crate) fn observe_gc_deleted(completed: usize, reaped: usize, requests: usize) {
    if !enabled() {
        return;
    }
    add(&GC_DELETED, completed as u64);
    add(&GC_DELETE_REQUESTS, requests as u64);
    // Reaping a completed tombstone changes no metadata, so it is reported through the
    // delete-request count only; `deleted` stays the number of acknowledged tombstones.
    let _ = reaped;
}

pub(crate) fn observe_gc_delete_failure() {
    if !enabled() {
        return;
    }
    add(&GC_DELETE_FAILURES, 1);
}

pub(crate) fn observe_gc_wait(elapsed: Duration) {
    if !enabled() {
        return;
    }
    let elapsed = nanos(elapsed);
    add(&GC_WAIT_NANOS, elapsed);
    GC_MAX_WAIT_NANOS.fetch_max(elapsed, Ordering::Relaxed);
}

pub(crate) fn observe_gc_exclusive(elapsed: Duration) {
    if !enabled() {
        return;
    }
    let elapsed = nanos(elapsed);
    add(&GC_EXCLUSIVE_NANOS, elapsed);
    GC_MAX_EXCLUSIVE_NANOS.fetch_max(elapsed, Ordering::Relaxed);
}

pub(crate) fn observe_gc_interruption(reason: &'static str) {
    if !enabled() {
        return;
    }
    let Some(index) = GC_INTERRUPTION_NAMES
        .iter()
        .position(|name| *name == reason)
    else {
        debug_assert!(false, "unregistered GC interruption reason: {reason}");
        return;
    };
    add(&GC_INTERRUPTIONS[index], 1);
}

/// Headroom left on a completed lease renewal. The minimum is the number to watch: one
/// renewal that nearly missed its deadline is hidden by any average.
pub(crate) fn observe_lease_renewal(remaining_ms: u64) {
    if !enabled() {
        return;
    }
    add(&LEASE_RENEWALS, 1);
    add(&LEASE_REMAINING_MS, remaining_ms);
    LEASE_REMAINING_MIN_MS.fetch_min(remaining_ms, Ordering::Relaxed);
}

// --- C. locks and execution --------------------------------------------------------------

/// The three state locks the request path contends on. Session lanes and per-registry locks
/// are outside this set on purpose: they are bounded by a session or a process, while these
/// three are shared by every session on a database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LockDomain {
    /// `RemoteIO` state: native WAL image, pending batch, publication state.
    State,
    /// Segment cache index: RAM and disk residency plus eviction bookkeeping.
    Cache,
    /// `RemoteStorage` state: manifest, dirty segments, applied LSN.
    Storage,
}

const LOCK_NAMES: [&str; 3] = ["state", "cache", "storage"];
static LOCK_WAITS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static LOCK_WAIT_NANOS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static LOCK_HOLDS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static LOCK_HOLD_NANOS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
// A cumulative sum cannot distinguish thousands of short holds from one long hold. Keeping
// the single longest hold lets a maintenance report test that premise directly instead of
// dividing the sum by the count.
static LOCK_MAX_HOLD_NANOS: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];

thread_local! {
    /// Per-request storage wait, reset at the worker request boundary. It is `None` when the
    /// switch is off or when the thread is not serving a worker request, so a maintenance
    /// diagnostic cannot inherit a previous request's wait.
    static REQUEST_LOCK_WAITS: Cell<Option<[u64; 3]>> = const { Cell::new(None) };
    static REQUEST_ADMISSION_WAIT: Cell<Option<AdmissionWait>> = const { Cell::new(None) };
    static REQUEST_LANE_WAIT: Cell<Option<LaneWait>> = const { Cell::new(None) };
    /// Set at the worker request boundary. `note_*` refuses to store on threads that are not
    /// serving a request, so maintenance or close paths on other threads cannot populate a
    /// slot that no request will read.
    static REQUEST_OBSERVING: Cell<bool> = const { Cell::new(false) };
}

/// Recorded when a queued writer hits its deadline. Kept beside the error instead of inside
/// its chain so the client-visible message is byte-identical whether observation is on or off.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdmissionWait {
    pub(crate) waited_ns: u64,
    pub(crate) maintenance_blocked: bool,
    pub(crate) queue_depth: usize,
}

/// Recorded when a lane polling for its connection hits its deadline.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LaneWait {
    pub(crate) waited_ns: u64,
}

/// The in-process gates that replaced a `try_lock` poll with notification. Kept apart from
/// the storage lock counters: these are contention episodes, not lock acquisitions. The
/// notification hit rate is `(waits - timeouts) / waits`, and an uncontended acquisition
/// charges none of these counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GateDomain {
    /// A session's connection guard.
    Lane,
    /// The database-wide foreground/maintenance operations gate.
    Operation,
}

impl GateDomain {
    pub(crate) const ALL: [GateDomain; 2] = [GateDomain::Lane, GateDomain::Operation];

    pub(crate) fn name(self) -> &'static str {
        match self {
            GateDomain::Lane => "lane",
            GateDomain::Operation => "operation",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct GateTotals {
    /// Contention episodes that entered the notification wait.
    pub waits: u64,
    /// Time spent inside those waits, including the wake-up reacquisition.
    pub wait_ns: u64,
    /// Waits that ended at the deadline, or at a close/quarantine cancellation, instead of
    /// acquiring the gate.
    pub timeouts: u64,
}

static GATE_WAITS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static GATE_WAIT_NANOS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static GATE_TIMEOUTS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];

pub(crate) fn observe_gate_wait(domain: GateDomain) {
    if !enabled() {
        return;
    }
    add(&GATE_WAITS[domain.index()], 1);
}

pub(crate) fn observe_gate_wait_done(domain: GateDomain, waited: Duration) {
    if !enabled() {
        return;
    }
    add(&GATE_WAIT_NANOS[domain.index()], nanos(waited));
}

pub(crate) fn observe_gate_wait_timeout(domain: GateDomain) {
    if !enabled() {
        return;
    }
    add(&GATE_TIMEOUTS[domain.index()], 1);
}

pub(crate) fn begin_request_observation() {
    if enabled() {
        REQUEST_OBSERVING.with(|slot| slot.set(true));
        REQUEST_LOCK_WAITS.with(|waits| waits.set(Some([0; 3])));
        REQUEST_ADMISSION_WAIT.with(|wait| wait.set(None));
        REQUEST_LANE_WAIT.with(|wait| wait.set(None));
    }
}

fn observing() -> bool {
    enabled() && REQUEST_OBSERVING.with(Cell::get)
}

pub(crate) fn note_admission_wait(wait: AdmissionWait) {
    if observing() {
        REQUEST_ADMISSION_WAIT.with(|slot| slot.set(Some(wait)));
    }
}

pub(crate) fn admission_wait() -> Option<AdmissionWait> {
    if !enabled() {
        return None;
    }
    REQUEST_ADMISSION_WAIT.with(Cell::get)
}

pub(crate) fn note_lane_wait(wait: LaneWait) {
    if observing() {
        REQUEST_LANE_WAIT.with(|slot| slot.set(Some(wait)));
    }
}

pub(crate) fn lane_wait() -> Option<LaneWait> {
    if !enabled() {
        return None;
    }
    REQUEST_LANE_WAIT.with(Cell::get)
}

pub(crate) fn request_lock_wait_ns(domain: LockDomain) -> Option<u64> {
    if !enabled() {
        return None;
    }
    REQUEST_LOCK_WAITS.with(|waits| waits.get().map(|totals| totals[domain as usize]))
}

/// Timer started before a lock is taken. Disabled, it holds no clock and the acquisition
/// below hands back a guard that never reads one either.
pub(crate) struct LockWait {
    domain: LockDomain,
    started: Option<Instant>,
}

impl LockWait {
    pub(crate) fn begin(domain: LockDomain) -> Self {
        Self {
            domain,
            started: enabled().then(Instant::now),
        }
    }

    /// Records the wait and arms the hold timer on the acquired guard. The guard keeps the
    /// poison behaviour of the call site: the caller takes the lock with the same
    /// `expect`/`map_err` it used before, and only then hands the guard over.
    pub(crate) fn acquired<T>(self, guard: MutexGuard<'_, T>) -> ObservedLock<'_, T> {
        ObservedLock {
            guard,
            hold: self.started.map(|started| {
                let waited = nanos(started.elapsed());
                let index = self.domain as usize;
                add(&LOCK_WAITS[index], 1);
                add(&LOCK_WAIT_NANOS[index], waited);
                REQUEST_LOCK_WAITS.with(|waits| {
                    if let Some(mut totals) = waits.get() {
                        totals[index] = totals[index].saturating_add(waited);
                        waits.set(Some(totals));
                    }
                });
                (self.domain, Instant::now())
            }),
        }
    }
}

/// Guard returned by [`LockWait::acquired`]. It dereferences to the guarded state, so call
/// sites and their borrows are unchanged; the hold time is recorded when the guard drops.
pub(crate) struct ObservedLock<'a, T> {
    guard: MutexGuard<'a, T>,
    hold: Option<(LockDomain, Instant)>,
}

impl<T> Deref for ObservedLock<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for ObservedLock<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

impl<T> Drop for ObservedLock<'_, T> {
    fn drop(&mut self) {
        if let Some((domain, started)) = self.hold {
            let held = nanos(started.elapsed());
            add(&LOCK_HOLDS[domain as usize], 1);
            add(&LOCK_HOLD_NANOS[domain as usize], held);
            LOCK_MAX_HOLD_NANOS[domain as usize].fetch_max(held, Ordering::Relaxed);
        }
    }
}

static EXECUTE_CALLS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_NANOS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_MAX_NANOS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_STEPS: AtomicU64 = AtomicU64::new(0);

/// Timer for a span that has no guard to hang the measurement on. Disabled, it carries no
/// clock at all, so the timed path reads no monotonic time it would not have read anyway.
pub(crate) struct ExecuteTimer {
    started: Option<Instant>,
}

impl ExecuteTimer {
    pub(crate) fn begin() -> Self {
        Self {
            started: enabled().then(Instant::now),
        }
    }

    /// One native statement execution: the wall time this process spent inside the engine
    /// stepping a statement, with the engine's own virtual-machine step count when the
    /// execution reached the success path. It is the CPU side of a request, so a review can
    /// read it against `wal_durability` instead of attributing every millisecond to the
    /// remote store.
    pub(crate) fn finish(self, steps: Option<u64>) {
        let Some(started) = self.started else {
            return;
        };
        add(&EXECUTE_CALLS, 1);
        let elapsed = nanos(started.elapsed());
        add(&EXECUTE_NANOS, elapsed);
        EXECUTE_MAX_NANOS.fetch_max(elapsed, Ordering::Relaxed);
        if let Some(steps) = steps {
            add(&EXECUTE_STEPS, steps);
        }
    }
}

// --- F. guarded read-only statement ------------------------------------------------------

static GUARDED_STATEMENT_CALLS: AtomicU64 = AtomicU64::new(0);
static GUARDED_STATEMENT_PREPARES: AtomicU64 = AtomicU64::new(0);
static GUARDED_STATEMENT_REPREPARES: AtomicU64 = AtomicU64::new(0);
static GUARDED_STATEMENT_REQUIRES_INTENT: AtomicU64 = AtomicU64::new(0);

/// One guarded read-only RPC. `prepares` counts the calls that reached the
/// engine; a transaction control or already-transactional statement is refused
/// before preparing.
pub(crate) fn note_guarded_statement_call() {
    if enabled() {
        add(&GUARDED_STATEMENT_CALLS, 1);
    }
}

pub(crate) fn note_guarded_statement_prepare() {
    if enabled() {
        add(&GUARDED_STATEMENT_PREPARES, 1);
    }
}

/// The outcome of one guarded call: the engine's automatic reprepares and
/// whether the statement was handed back to the committing path. A refused
/// statement has no rows and no write; the caller records the intent next.
pub(crate) fn note_guarded_statement_outcome(reprepares: u64, requires_intent: bool) {
    if !enabled() {
        return;
    }
    add(&GUARDED_STATEMENT_REPREPARES, reprepares);
    if requires_intent {
        add(&GUARDED_STATEMENT_REQUIRES_INTENT, 1);
    }
}

// --- D. maintenance ----------------------------------------------------------------------

// The names and the counters come from the maintenance enums themselves, so reordering or
// renaming a reason cannot silently mislabel a counter. `MaintenanceLane::index`,
// `BlockReason as usize` and `BudgetDenied as usize` are the array positions, and the unit
// test in this module's test file pins each enum's discriminant against its position.

static UNIT_CALLS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_NANOS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_BACKFILL_BYTES: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_FAILURES: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_WAITS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_WAIT_NANOS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_SCANNED_PAGES: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_BACKFILLED_BYTES: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static UNIT_TRUNCATIONS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];

static BLOCKS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
static BACKOFFS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
static DENIED: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];

static WAL_INDEX_READS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_READ_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_READ_ENTRIES: AtomicU64 = AtomicU64::new(0);

static WAL_BUDGET_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_BUDGET_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_BUDGET_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static WAL_BUDGET_PRESSURE_EVENTS: AtomicU64 = AtomicU64::new(0);

static READER_BLOCK_EVENTS: AtomicU64 = AtomicU64::new(0);
static READER_BLOCK_NANOS: AtomicU64 = AtomicU64::new(0);

/// Timer for one maintenance unit. Disabled, it carries no clock, so a process with the
/// switch off reads no time it would not have read anyway.
pub(crate) struct MaintenanceTimer {
    lane: crate::maintenance::MaintenanceLane,
    started: Option<Instant>,
    started_wall_ms: Option<u64>,
    started_storage: Option<LockTotals>,
}

impl MaintenanceTimer {
    pub(crate) fn begin(lane: crate::maintenance::MaintenanceLane) -> Self {
        let started = enabled().then(Instant::now);
        Self {
            lane,
            started,
            started_wall_ms: started.map(|_| unix_ms()),
            started_storage: started.map(|_| lock_totals(LockDomain::Storage)),
        }
    }

    /// One maintenance unit: the planned backfill volume, the frames the unit really scanned,
    /// whether it truncated, the wall time inside it, and whether it completed. Both the admin
    /// entry points and the scheduler's units report through here, so the two triggers are
    /// measured on one caliber.
    pub(crate) fn finish(
        self,
        charge_bytes: u64,
        report: Option<crate::maintenance::MaintenanceUnitReport>,
        completed: bool,
    ) {
        let Some(started) = self.started else {
            return;
        };
        let elapsed_ns = nanos(started.elapsed());
        let index = self.lane.index();
        add(&UNIT_CALLS[index], 1);
        add(&UNIT_BACKFILL_BYTES[index], charge_bytes);
        add(&UNIT_NANOS[index], elapsed_ns);
        if let Some(report) = report {
            add(&UNIT_SCANNED_PAGES[index], report.scanned_pages);
            add(&UNIT_BACKFILLED_BYTES[index], report.backfilled_bytes);
            if report.truncated {
                add(&UNIT_TRUNCATIONS[index], 1);
            }
        }
        if !completed {
            add(&UNIT_FAILURES[index], 1);
        }
        // One line per unit, only while the work-metrics switch is on. This is what lets a
        // diagnostic run correlate a maintenance window with a request outcome without
        // turning the process-level counters into per-unit evidence.
        if let (Some(started_wall_ms), Some(started_storage)) =
            (self.started_wall_ms, self.started_storage)
        {
            let ended_storage = lock_totals(LockDomain::Storage);
            let record = serde_json::json!({
                "event": "maintenance_unit",
                "lane": self.lane.name(),
                "start_unix_ms": started_wall_ms,
                "end_unix_ms": unix_ms(),
                "duration_ns": elapsed_ns,
                "backfill_bytes": charge_bytes,
                "scanned_pages": report.map_or(0, |report| report.scanned_pages),
                "backfilled_bytes": report.map_or(0, |report| report.backfilled_bytes),
                "truncated": report.is_some_and(|report| report.truncated),
                "complete": report.is_some_and(|report| report.complete),
                "completed": completed,
                "storage_holds_delta": ended_storage.holds.saturating_sub(started_storage.holds),
                "storage_hold_ns_delta": ended_storage
                    .hold_ns
                    .saturating_sub(started_storage.hold_ns),
                "storage_waits_delta": ended_storage.waits.saturating_sub(started_storage.waits),
                "storage_wait_ns_delta": ended_storage
                    .wait_ns
                    .saturating_sub(started_storage.wait_ns),
                // Process lifetime, not this unit: the name keeps it from being read as a
                // per-window maximum next to the deltas.
                "process_storage_max_hold_ns": ended_storage.max_hold_ns,            });
            if let Ok(line) = serde_json::to_string(&record) {
                // Observation must not become a new failure mode; diagnostics.rs ignores a
                // failing writer for the same reason.
                let _ = writeln!(std::io::stderr().lock(), "{line}");
            }
        }
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

pub(crate) fn observe_maintenance_blocked(reason: crate::maintenance::BlockReason) {
    if !enabled() {
        return;
    }
    add(&BLOCKS[reason as usize], 1);
}

pub(crate) fn observe_maintenance_refused(denied: crate::maintenance::BudgetDenied) {
    if !enabled() {
        return;
    }
    add(&DENIED[denied as usize], 1);
}

/// Time a unit spent queued between the scheduling decision and its execution. Kept apart
/// from the unit's own duration so a slow drain is not mistaken for a slow checkpoint.
pub(crate) fn observe_maintenance_wait(
    lane: crate::maintenance::MaintenanceLane,
    waited: std::time::Duration,
) {
    if !enabled() {
        return;
    }
    let index = lane.index();
    add(&UNIT_WAITS[index], 1);
    add(&UNIT_WAIT_NANOS[index], nanos(waited));
}

/// One scheduler backoff after a unit observed a block. The block reason itself is counted by
/// `observe_maintenance_blocked`; this counter says a backoff was actually applied, which is a
/// different event because a refusal never starts a unit and a pre-suspend unit does not back
/// off the periodic driver.
pub(crate) fn observe_maintenance_backoff(reason: crate::maintenance::BlockReason) {
    if !enabled() {
        return;
    }
    add(&BACKOFFS[reason as usize], 1);
}

/// A reader held a native read lock long enough to block compaction. The age is the upper
/// bound described on `ReaderAge`, so the nanos are an upper bound as well.
pub(crate) fn observe_reader_block(age: std::time::Duration) {
    if !enabled() {
        return;
    }
    add(&READER_BLOCK_EVENTS, 1);
    add(&READER_BLOCK_NANOS, nanos(age));
}

/// The configured WAL capacity and the largest materialized WAL seen while it was enabled.
/// A peak is more useful than a live gauge here because the periods that matter are the ones
/// just before a maintenance pass drained the log.
pub(crate) fn observe_wal_budget_state(hard_bytes: u64, current_bytes: u64) {
    if !enabled() {
        return;
    }
    WAL_BUDGET_BYTES.store(hard_bytes, Ordering::Relaxed);
    WAL_BUDGET_PEAK_BYTES.fetch_max(current_bytes, Ordering::Relaxed);
}

/// One new WAL write refused because it would cross the hard capacity. A rejection is not a
/// maintenance skip: no unit was queued and the SQL call fails instead.
pub(crate) fn observe_wal_budget_rejection(hard_bytes: u64, projected_bytes: u64) {
    if !enabled() {
        return;
    }
    add(&WAL_BUDGET_REJECTIONS, 1);
    WAL_BUDGET_BYTES.store(hard_bytes, Ordering::Relaxed);
    WAL_BUDGET_PEAK_BYTES.fetch_max(projected_bytes, Ordering::Relaxed);
}

/// One calm-to-pressured transition across the soft mark.
pub(crate) fn observe_wal_budget_pressure() {
    if !enabled() {
        return;
    }
    add(&WAL_BUDGET_PRESSURE_EVENTS, 1);
}

/// One read of the branch's `wal/index` document. The read happens on paths that already
/// needed the index, so this counts structure that was fetched rather than fetching it to
/// measure it — which is why the index entry count is here and not in the periodic probe.
pub(crate) fn observe_wal_index_read(encoded_bytes: usize, entries: usize) {
    if !enabled() {
        return;
    }
    add(&WAL_INDEX_READS, 1);
    add(&WAL_INDEX_READ_BYTES, encoded_bytes as u64);
    add(&WAL_INDEX_READ_ENTRIES, entries as u64);
}

// --- E. SQL result encoding ---------------------------------------------------------------

// Cumulative, not a gauge: rows are materialized inside the engine call before a response
// exists, so an in-flight byte gauge would either miss that phase or require instrumenting
// the cursor lifecycle, which this pass deliberately leaves for a later decision.

static RESULT_BATCHES: AtomicU64 = AtomicU64::new(0);
static RESULT_ROWS: AtomicU64 = AtomicU64::new(0);
static RESULT_JSON_BYTES: AtomicU64 = AtomicU64::new(0);

/// One successful row-bearing SQL result batch: the rows the worker materialized and the JSON
/// bytes of its `result` payload. The envelope, session state and transport framing are not
/// included. NDJSON is a gateway materialization, not a worker result format, so it has no
/// counter here.
pub(crate) fn observe_result(rows: usize, json_bytes: usize) {
    if !enabled() {
        return;
    }
    add(&RESULT_BATCHES, 1);
    add(&RESULT_ROWS, rows as u64);
    add(&RESULT_JSON_BYTES, json_bytes as u64);
}

// --- snapshot ----------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct WalWorkload {
    /// `sync_wal` calls that carried a pending batch.
    pub append_calls: u64,
    /// Bytes in the WAL image each append had to reason about, summed over appends.
    pub native_bytes: u64,
    /// Bytes of pending WAL payload the appends asked the WAL to write.
    pub delta_bytes: u64,
    /// Commit-position frames the appends carried into the incremental authority validation.
    pub position_frames: u64,
    pub scan_calls: u64,
    pub scanned_bytes: u64,
    pub scanned_frames: u64,
    pub copied_bytes: u64,
    pub scan_ns: u64,
    pub scan_resets: BTreeMap<&'static str, u64>,
    pub scan_stops: BTreeMap<&'static str, u64>,
    pub append_position: BTreeMap<&'static str, u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PublishOriginCount {
    pub calls: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct PublishOutsideTransactCount {
    pub calls: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct MetadataWork {
    /// Encoded authority head documents built for a publication attempt.
    pub root_encoded_bytes: u64,
    /// Publication attempts, i.e. `transact` loop iterations that reached the head CAS.
    pub root_publish_attempts: u64,
    /// Attempts that lost the head CAS and retried.
    pub root_publish_retries: u64,
    /// Attempts per origin. Charged at the same point as `root_publish_attempts`, so the
    /// sum of these calls equals that counter exactly.
    pub publish_origin: BTreeMap<&'static str, PublishOriginCount>,
    /// Successful publications whose group was assembled outside `transact_inner`.
    pub publish_outside_transact: BTreeMap<&'static str, PublishOutsideTransactCount>,
    /// Inline reader-pin entries in the published documents; paged entries are excluded.
    pub inline_pin_refs: u64,
    /// Reader-pin releases that failed while a handle was being dropped.
    pub pin_release_failures: u64,
    /// Idempotency receipts in the published documents.
    pub root_receipts: u64,
    /// Encoded `wal/index` documents published.
    pub wal_index_commits: u64,
    pub wal_index_bytes: u64,
    /// Idempotency receipts the published WAL indexes carried.
    pub wal_index_receipts: u64,
    /// `wal/index` conditional writes that lost their race and retried.
    pub wal_index_retries: u64,
    pub index_page_reads: u64,
    pub index_page_writes: u64,
    pub index_page_write_bytes: u64,
    /// Largest encoded head document built for a publication attempt.
    pub root_encoded_max_bytes: u64,
    /// Inline identity populations summed over encoded heads. The identity tables are not
    /// paged yet, so these are the counters that show whether a namespace is approaching the
    /// explicit ceilings.
    pub inline_branches: u64,
    pub inline_snapshots: u64,
    pub inline_pin_identities: u64,
    /// Regression points carried inline by branch hot windows, summed over encoded heads.
    pub branch_history_points: u64,
    /// Largest single encoded head's inline identity populations.
    pub max_inline_branches: u64,
    pub max_inline_snapshots: u64,
    pub max_inline_pin_identities: u64,
    pub max_branch_history_points: u64,
    /// Per stage, in `PublishStage::ALL` order: attempts that reached the stage, the time they
    /// spent there, and the longest single stay. A dropped attempt still charges the stage it
    /// was in, so an error path does not vanish from the account.
    pub publish_stage_calls: BTreeMap<&'static str, u64>,
    pub publish_stage_ns: BTreeMap<&'static str, u64>,
    pub publish_stage_max_ns: BTreeMap<&'static str, u64>,
    /// Wall time of every publication attempt, and the longest one.
    pub publish_total_ns: u64,
    pub publish_total_max_ns: u64,
    /// Per stage, in `SnapshotStage::ALL` order: head reads that reached the stage, the time
    /// they spent there, and the longest single stay. `deep` counts the full walks paid when
    /// the head was not one this process had verified.
    pub snapshot_stage_calls: BTreeMap<&'static str, u64>,
    pub snapshot_stage_ns: BTreeMap<&'static str, u64>,
    pub snapshot_stage_max_ns: BTreeMap<&'static str, u64>,
    /// Wall time of every head read, and the longest one.
    pub snapshot_total_ns: u64,
    pub snapshot_total_max_ns: u64,
    /// Encoded head bytes read, and the largest head read.
    pub snapshot_root_bytes: u64,
    pub snapshot_root_max_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LockTotals {
    pub waits: u64,
    pub wait_ns: u64,
    pub holds: u64,
    pub hold_ns: u64,
    /// Longest single hold observed in this process for the domain.
    pub max_hold_ns: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ExecutionWork {
    /// Native statement executions observed.
    pub native_execute_calls: u64,
    pub native_execute_ns: u64,
    /// Longest single execution. A multi-second request that is not charged here happened
    /// outside the native statement, i.e. in the handler's lane or durability path.
    pub native_execute_max_ns: u64,
    /// Engine VM steps of the executions that reached the success path.
    pub native_execute_steps: u64,
    /// Guarded read-only RPCs, the calls that reached the engine, the engine's
    /// automatic reprepares during them, and the calls handed back to the
    /// committing path because the engine classified the statement as writing.
    pub guarded_statement_calls: u64,
    pub guarded_statement_prepares: u64,
    pub guarded_statement_reprepares: u64,
    pub guarded_statement_requires_intent: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct UnitTotals {
    /// Units that reached execution.
    pub calls: u64,
    pub ns: u64,
    /// Backfill volume the units were charged for, in bytes.
    pub backfill_bytes: u64,
    /// Units that executed and did not complete, including a blocked compaction.
    pub failures: u64,
    /// Units that were queued and the time they spent waiting before execution.
    pub waits: u64,
    pub wait_ns: u64,
    /// Frames the native checkpoint reports as scanned, and the bytes they represent.
    pub scanned_pages: u64,
    pub backfilled_bytes: u64,
    /// Units that reset the WAL index.
    pub truncations: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct MaintenanceWork {
    /// Per lane, in `MaintenanceLane::ALL` order.
    pub units: BTreeMap<&'static str, UnitTotals>,
    /// Why a unit did not complete, counted where the reason was decided.
    pub blocked: BTreeMap<&'static str, u64>,
    /// A block that the scheduler turned into a database-wide backoff.
    pub backoffs: BTreeMap<&'static str, u64>,
    /// Budget refusals, by the budget that ran out. A refusal is a skip.
    pub refused: BTreeMap<&'static str, u64>,
    /// Reader-block events and the peak age that caused them, process-wide.
    pub reader_block_events: u64,
    pub reader_block_ns: u64,
    /// Configured hard WAL capacity (0 when disabled), the largest materialized WAL seen,
    /// new-write rejections and calm-to-pressured transitions.
    pub wal_budget_bytes: u64,
    pub wal_budget_peak_bytes: u64,
    pub wal_budget_rejections: u64,
    pub wal_budget_pressure_events: u64,
    /// `wal/index` reads that happened on paths which already needed the index.
    pub wal_index_reads: u64,
    pub wal_index_read_bytes: u64,
    pub wal_index_read_entries: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct GcWork {
    /// Collection requests that reached the bounded state machine, including replays.
    pub requests: u64,
    /// Requests that continued a previously published cursor instead of starting a cycle.
    pub resumed: u64,
    /// Requests that observed the cycle at `done`.
    pub completed: u64,
    /// Bounded mark/sweep/cursor publications. One tick publishes at most one of these for
    /// progress, plus one conditional transaction per deletion batch.
    pub batches: u64,
    pub scanned_pages: u64,
    pub scanned_entries: u64,
    /// Bytes of scanned registry keys/values. Physical object sizes are not recorded in the
    /// catalog, so this is metadata volume, not downloaded or deleted bytes.
    pub scanned_bytes: u64,
    pub marked: u64,
    pub deleted: u64,
    /// Physical `delete` calls issued by sweep, including late-arrival reaps.
    pub delete_requests: u64,
    pub delete_failures: u64,
    /// Time spent waiting for the namespace exclusive barrier, and holding it.
    pub wait_ns: u64,
    pub max_wait_ns: u64,
    pub exclusive_ns: u64,
    pub max_exclusive_ns: u64,
    /// Bounded ticks that stopped before the cycle finished, by reason.
    pub interruptions: BTreeMap<&'static str, u64>,
    /// Completed lease renewals and the headroom they reported.
    pub lease_renewals: u64,
    pub lease_remaining_ms: u64,
    /// Smallest remaining TTL reported by a renewal; 0 when no renewal was observed.
    pub lease_remaining_min_ms: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ResultWork {
    /// Successful row-bearing SQL result batches serialized, including empty pages.
    pub batches: u64,
    /// Materialized rows carried by those batches.
    pub rows: u64,
    /// JSON bytes of the `result` payload, excluding the worker envelope and session state.
    pub json_bytes: u64,
}

/// Write-count histogram of successful local groups. The field names are the exported bucket
/// names; `1` includes an empty group even though the local store returns before publishing one.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct GroupWrites {
    #[serde(rename = "1")]
    pub one: u64,
    #[serde(rename = "2_4")]
    pub two_to_four: u64,
    #[serde(rename = "5_16")]
    pub five_to_sixteen: u64,
    #[serde(rename = "17_plus")]
    pub seventeen_plus: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct GroupWork {
    pub calls: u64,
    pub writes: GroupWrites,
    pub class: BTreeMap<&'static str, u64>,
    pub barrier_ns: u64,
    pub body_fsync_ns: u64,
    pub dir_flush_ns: u64,
    pub total_ns: u64,
    pub total_max_ns: u64,
}

fn group_work() -> GroupWork {
    GroupWork {
        calls: load(&GROUP_CALLS),
        writes: GroupWrites {
            one: load(&GROUP_WRITES_ONE),
            two_to_four: load(&GROUP_WRITES_TWO_TO_FOUR),
            five_to_sixteen: load(&GROUP_WRITES_FIVE_TO_SIXTEEN),
            seventeen_plus: load(&GROUP_WRITES_SEVENTEEN_PLUS),
        },
        class: report(&GroupClass::ALL, GroupClass::name, &GROUP_CLASS_CALLS),
        barrier_ns: load(&GROUP_BARRIER_NS),
        body_fsync_ns: load(&GROUP_BODY_FSYNC_NS),
        dir_flush_ns: load(&GROUP_DIR_FLUSH_NS),
        total_ns: load(&GROUP_TOTAL_NS),
        total_max_ns: load(&GROUP_TOTAL_MAX_NS),
    }
}

/// The `group_phases` section of the exit snapshot, or `None` while the counters are off.
pub(crate) fn group_phase_snapshot() -> Option<GroupWork> {
    if !enabled() {
        return None;
    }
    Some(group_work())
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct WorkMetricsSnapshot {
    pub wal_workload: WalWorkload,
    pub metadata: MetadataWork,
    pub locks: BTreeMap<&'static str, LockTotals>,
    pub gates: BTreeMap<&'static str, GateTotals>,
    pub execution: ExecutionWork,
    pub results: ResultWork,
    pub maintenance: MaintenanceWork,
    pub gc: GcWork,
    pub group: GroupWork,
}

fn units() -> BTreeMap<&'static str, UnitTotals> {
    crate::maintenance::MaintenanceLane::ALL
        .iter()
        .map(|lane| {
            let index = lane.index();
            (
                lane.name(),
                UnitTotals {
                    calls: load(&UNIT_CALLS[index]),
                    ns: load(&UNIT_NANOS[index]),
                    backfill_bytes: load(&UNIT_BACKFILL_BYTES[index]),
                    failures: load(&UNIT_FAILURES[index]),
                    waits: load(&UNIT_WAITS[index]),
                    wait_ns: load(&UNIT_WAIT_NANOS[index]),
                    scanned_pages: load(&UNIT_SCANNED_PAGES[index]),
                    backfilled_bytes: load(&UNIT_BACKFILLED_BYTES[index]),
                    truncations: load(&UNIT_TRUNCATIONS[index]),
                },
            )
        })
        .collect()
}

fn lock_totals(domain: LockDomain) -> LockTotals {
    let index = domain as usize;
    LockTotals {
        waits: load(&LOCK_WAITS[index]),
        wait_ns: load(&LOCK_WAIT_NANOS[index]),
        holds: load(&LOCK_HOLDS[index]),
        hold_ns: load(&LOCK_HOLD_NANOS[index]),
        max_hold_ns: load(&LOCK_MAX_HOLD_NANOS[index]),
    }
}

fn locks() -> BTreeMap<&'static str, LockTotals> {
    [LockDomain::State, LockDomain::Cache, LockDomain::Storage]
        .into_iter()
        .zip(LOCK_NAMES)
        .map(|(domain, name)| (name, lock_totals(domain)))
        .collect()
}

fn gates() -> BTreeMap<&'static str, GateTotals> {
    GateDomain::ALL
        .into_iter()
        .map(|domain| {
            let index = domain.index();
            (
                domain.name(),
                GateTotals {
                    waits: load(&GATE_WAITS[index]),
                    wait_ns: load(&GATE_WAIT_NANOS[index]),
                    timeouts: load(&GATE_TIMEOUTS[index]),
                },
            )
        })
        .collect()
}

/// Process-wide totals, or `None` while the switch is off so an export can tell "not
/// measured" from "measured, nothing happened".
pub(crate) fn snapshot() -> Option<WorkMetricsSnapshot> {
    if !enabled() {
        return None;
    }
    Some(WorkMetricsSnapshot {
        wal_workload: WalWorkload {
            append_calls: load(&WAL_APPENDS),
            native_bytes: load(&WAL_NATIVE_BYTES),
            delta_bytes: load(&WAL_DELTA_BYTES),
            position_frames: load(&WAL_POSITION_FRAMES),
            scan_calls: load(&SCAN_CALLS),
            scanned_bytes: load(&SCANNED_BYTES),
            scanned_frames: load(&SCANNED_FRAMES),
            copied_bytes: load(&COPIED_BYTES),
            scan_ns: load(&SCAN_NANOS),
            scan_resets: named(&RESET_NAMES, &RESETS),
            scan_stops: named(&STOP_NAMES, &STOPS),
            append_position: named(&POSITION_NAMES, &POSITIONS),
        },
        metadata: MetadataWork {
            root_encoded_bytes: load(&ROOT_ENCODED_BYTES),
            root_publish_attempts: load(&ROOT_PUBLISH_ATTEMPTS),
            root_publish_retries: load(&ROOT_PUBLISH_RETRIES),
            publish_origin: report_origin(&PublishOrigin::ALL, PublishOrigin::name),
            publish_outside_transact: report_outside_transact(
                &OutsideTransactKind::ALL,
                OutsideTransactKind::name,
            ),
            inline_pin_refs: load(&INLINE_PIN_REFS),
            pin_release_failures: load(&PIN_RELEASE_FAILURES),
            root_receipts: load(&ROOT_RECEIPTS),
            wal_index_commits: load(&WAL_INDEX_COMMITS),
            wal_index_bytes: load(&WAL_INDEX_BYTES),
            wal_index_receipts: load(&WAL_INDEX_RECEIPTS),
            wal_index_retries: load(&WAL_INDEX_RETRIES),
            index_page_reads: load(&INDEX_PAGE_READS),
            index_page_writes: load(&INDEX_PAGE_WRITES),
            index_page_write_bytes: load(&INDEX_PAGE_WRITE_BYTES),
            root_encoded_max_bytes: load(&ROOT_ENCODED_MAX_BYTES),
            inline_branches: load(&INLINE_BRANCHES),
            inline_snapshots: load(&INLINE_SNAPSHOTS),
            inline_pin_identities: load(&INLINE_PIN_IDENTITIES),
            branch_history_points: load(&BRANCH_HISTORY_POINTS),
            max_inline_branches: load(&MAX_INLINE_BRANCHES),
            max_inline_snapshots: load(&MAX_INLINE_SNAPSHOTS),
            max_inline_pin_identities: load(&MAX_INLINE_PIN_IDENTITIES),
            max_branch_history_points: load(&MAX_BRANCH_HISTORY_POINTS),
            publish_stage_calls: report(
                &PublishStage::ALL,
                PublishStage::name,
                &PUBLISH_STAGE_CALLS,
            ),
            publish_stage_ns: report(&PublishStage::ALL, PublishStage::name, &PUBLISH_STAGE_NANOS),
            publish_stage_max_ns: report(
                &PublishStage::ALL,
                PublishStage::name,
                &PUBLISH_STAGE_MAX_NANOS,
            ),
            publish_total_ns: load(&PUBLISH_TOTAL_NANOS),
            publish_total_max_ns: load(&PUBLISH_TOTAL_MAX_NANOS),
            snapshot_stage_calls: report(
                &SnapshotStage::ALL,
                SnapshotStage::name,
                &SNAPSHOT_STAGE_CALLS,
            ),
            snapshot_stage_ns: report(
                &SnapshotStage::ALL,
                SnapshotStage::name,
                &SNAPSHOT_STAGE_NANOS,
            ),
            snapshot_stage_max_ns: report(
                &SnapshotStage::ALL,
                SnapshotStage::name,
                &SNAPSHOT_STAGE_MAX_NANOS,
            ),
            snapshot_total_ns: load(&SNAPSHOT_TOTAL_NANOS),
            snapshot_total_max_ns: load(&SNAPSHOT_TOTAL_MAX_NANOS),
            snapshot_root_bytes: load(&SNAPSHOT_ROOT_BYTES),
            snapshot_root_max_bytes: load(&SNAPSHOT_ROOT_MAX_BYTES),
        },
        locks: locks(),
        gates: gates(),
        execution: ExecutionWork {
            native_execute_calls: load(&EXECUTE_CALLS),
            native_execute_ns: load(&EXECUTE_NANOS),
            native_execute_max_ns: load(&EXECUTE_MAX_NANOS),
            native_execute_steps: load(&EXECUTE_STEPS),
            guarded_statement_calls: load(&GUARDED_STATEMENT_CALLS),
            guarded_statement_prepares: load(&GUARDED_STATEMENT_PREPARES),
            guarded_statement_reprepares: load(&GUARDED_STATEMENT_REPREPARES),
            guarded_statement_requires_intent: load(&GUARDED_STATEMENT_REQUIRES_INTENT),
        },
        results: ResultWork {
            batches: load(&RESULT_BATCHES),
            rows: load(&RESULT_ROWS),
            json_bytes: load(&RESULT_JSON_BYTES),
        },
        maintenance: MaintenanceWork {
            units: units(),
            blocked: report(
                &crate::maintenance::BlockReason::ALL,
                crate::maintenance::BlockReason::name,
                &BLOCKS,
            ),
            backoffs: report(
                &crate::maintenance::BlockReason::ALL,
                crate::maintenance::BlockReason::name,
                &BACKOFFS,
            ),
            refused: report(
                &crate::maintenance::BudgetDenied::ALL,
                crate::maintenance::BudgetDenied::name,
                &DENIED,
            ),
            reader_block_events: load(&READER_BLOCK_EVENTS),
            reader_block_ns: load(&READER_BLOCK_NANOS),
            wal_budget_bytes: load(&WAL_BUDGET_BYTES),
            wal_budget_peak_bytes: load(&WAL_BUDGET_PEAK_BYTES),
            wal_budget_rejections: load(&WAL_BUDGET_REJECTIONS),
            wal_budget_pressure_events: load(&WAL_BUDGET_PRESSURE_EVENTS),
            wal_index_reads: load(&WAL_INDEX_READS),
            wal_index_read_bytes: load(&WAL_INDEX_READ_BYTES),
            wal_index_read_entries: load(&WAL_INDEX_READ_ENTRIES),
        },
        gc: GcWork {
            requests: load(&GC_REQUESTS),
            resumed: load(&GC_RESUMED),
            completed: load(&GC_COMPLETED),
            batches: load(&GC_BATCHES),
            scanned_pages: load(&GC_SCANNED_PAGES),
            scanned_entries: load(&GC_SCANNED_ENTRIES),
            scanned_bytes: load(&GC_SCANNED_BYTES),
            marked: load(&GC_MARKED),
            deleted: load(&GC_DELETED),
            delete_requests: load(&GC_DELETE_REQUESTS),
            delete_failures: load(&GC_DELETE_FAILURES),
            wait_ns: load(&GC_WAIT_NANOS),
            max_wait_ns: load(&GC_MAX_WAIT_NANOS),
            exclusive_ns: load(&GC_EXCLUSIVE_NANOS),
            max_exclusive_ns: load(&GC_MAX_EXCLUSIVE_NANOS),
            interruptions: named(&GC_INTERRUPTION_NAMES, &GC_INTERRUPTIONS),
            lease_renewals: load(&LEASE_RENEWALS),
            lease_remaining_ms: load(&LEASE_REMAINING_MS),
            lease_remaining_min_ms: lease_remaining_min(),
        },
        group: group_phase_snapshot().unwrap_or_default(),
    })
}

fn lease_remaining_min() -> u64 {
    if load(&LEASE_RENEWALS) == 0 {
        0
    } else {
        load(&LEASE_REMAINING_MIN_MS)
    }
}

// --- Exit snapshot -----------------------------------------------------------------------
//
// The counters above are process-wide and cheap to read, but the stdio worker has no request
// that could return them without widening its protocol. A diagnostics run instead asks the
// process to dump them once, on the way out, to `PERI_METRICS_SNAPSHOT_PATH`.
//
// The document is written as a whole and renamed into place, so a reader never sees a half
// file. It is a diagnostic artifact: it is not fsynced, because the plan forbids putting
// durable-write cost on the disk the benchmark is measuring. `schema_version` is bumped when
// a field changes meaning; adding a field is backward compatible for a reader that selects by
// name.

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;

const GROUP_PHASES_REASON: &str = "PERI_WORK_METRICS is off; no group aggregate was recorded";

#[derive(Serialize)]
struct ExitSnapshot {
    schema_version: u32,
    generated_at_unix_ms: u64,
    process: ExitProcess,
    work_metrics_enabled: bool,
    phases: Vec<ExitPhase>,
    counters: Vec<ExitCounter>,
    group_phases: ExitGroupPhases,
}

#[derive(Serialize)]
struct ExitProcess {
    pid: u32,
    version: &'static str,
}

/// One of the eleven `PublishStage` steps. `count` is the number of publication attempts that
/// reached the stage, not the number that completed it.
#[derive(Serialize)]
struct ExitPhase {
    name: &'static str,
    count: u64,
    total_ns: u64,
    max_ns: u64,
}

#[derive(Serialize)]
struct ExitCounter {
    name: String,
    value: u64,
}

/// The process aggregate of every successful local group. While the counters are off there is
/// nothing to report, so the document keeps the explicit unavailable state instead of zeros
/// that a reader could mistake for "no group ran".
#[derive(Serialize)]
struct ExitGroupPhases {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(flatten)]
    group: Option<GroupWork>,
}

/// Write the process-wide counters to `path`, atomically. Failure is returned to the caller and
/// is never retried; the worker's close result is owned by the caller and must not change
/// because a diagnostic artifact could not be written.
pub(crate) fn write_exit_snapshot(path: &Path) -> Result<()> {
    let work = snapshot();
    let document = ExitSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        generated_at_unix_ms: unix_millis(),
        process: ExitProcess {
            pid: std::process::id(),
            version: env!("CARGO_PKG_VERSION"),
        },
        work_metrics_enabled: work.is_some(),
        phases: exit_phases(work.as_ref()),
        counters: exit_counters(work.as_ref())?,
        group_phases: ExitGroupPhases {
            available: work.is_some(),
            reason: work.is_none().then_some(GROUP_PHASES_REASON),
            group: work.as_ref().map(|work| work.group.clone()),
        },
    };
    let encoded = serde_json::to_vec(&document).context("serialize metrics exit snapshot")?;
    write_atomically(path, &encoded)
}

/// The worker's exit hook. An unset or empty variable is the off state and does no work.
pub(crate) fn write_exit_snapshot_if_configured() -> Result<()> {
    let Some(path) = snapshot_path() else {
        return Ok(());
    };
    write_exit_snapshot(&path)
}

/// Whether an exit snapshot was requested. Used at startup so that enabling the counters alone
/// keeps the store stack byte-for-byte identical to a non-diagnostic run.
pub(crate) fn exit_snapshot_configured() -> bool {
    snapshot_path().is_some()
}

fn snapshot_path() -> Option<PathBuf> {
    std::env::var_os(SNAPSHOT_PATH_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn exit_phases(work: Option<&WorkMetricsSnapshot>) -> Vec<ExitPhase> {
    PublishStage::ALL
        .iter()
        .map(|stage| {
            let name = stage.name();
            let value = |map: Option<&BTreeMap<&'static str, u64>>| {
                map.and_then(|map| map.get(name).copied()).unwrap_or(0)
            };
            ExitPhase {
                name,
                count: value(work.map(|work| &work.metadata.publish_stage_calls)),
                total_ns: value(work.map(|work| &work.metadata.publish_stage_ns)),
                max_ns: value(work.map(|work| &work.metadata.publish_stage_max_ns)),
            }
        })
        .collect()
}

/// Flat `{name, value}` pairs. Counter names are the existing serialized paths, only prefixed:
/// `work.<field>` for [`WorkMetricsSnapshot`] and `object_store.<operation>.<field>` for the
/// wrapper counters in [`crate::metrics`]. Flattening instead of restating every name keeps the
/// export from drifting away from the definitions that own the counters.
fn exit_counters(work: Option<&WorkMetricsSnapshot>) -> Result<Vec<ExitCounter>> {
    let Some(work) = work else {
        return Ok(Vec::new());
    };
    let mut counters = Vec::new();
    let work = serde_json::to_value(work).context("serialize workload counters")?;
    flatten_counters("work", &work, &mut counters);
    let mut metrics = serde_json::to_value(crate::metrics::Metrics::global().snapshot())
        .context("serialize object store counters")?;
    if let serde_json::Value::Object(fields) = &mut metrics {
        // Already exported above as `work.*`; a nested copy would make two values that can be
        // read at different instants.
        fields.remove("work_metrics");
    }
    flatten_counters("object_store", &metrics, &mut counters);
    Ok(counters)
}

fn flatten_counters(prefix: &str, value: &serde_json::Value, counters: &mut Vec<ExitCounter>) {
    match value {
        serde_json::Value::Object(fields) => {
            for (field, value) in fields {
                flatten_counters(&format!("{prefix}.{field}"), value, counters);
            }
        }
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                counters.push(ExitCounter {
                    name: prefix.to_owned(),
                    value,
                });
            }
        }
        _ => {}
    }
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path.file_name().context("snapshot path has no file name")?;
    let temporary = path.with_file_name(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| -> std::io::Result<()> {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(bytes)?;
        drop(file);
        std::fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.with_context(|| format!("write metrics exit snapshot {}", path.display()))
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Serializes tests that must change the process-wide snapshot path.
#[cfg(test)]
pub(crate) static SNAPSHOT_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests;
