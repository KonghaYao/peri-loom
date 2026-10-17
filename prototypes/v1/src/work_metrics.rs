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

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::MutexGuard;
use std::time::{Duration, Instant};

use serde::Serialize;

const ENABLED_ENV: &str = "PERI_WORK_METRICS";

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
static PIN_REFS: AtomicU64 = AtomicU64::new(0);
static ROOT_RECEIPTS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_COMMITS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_RECEIPTS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_RETRIES: AtomicU64 = AtomicU64::new(0);
static INDEX_PAGE_READS: AtomicU64 = AtomicU64::new(0);
static INDEX_PAGE_WRITES: AtomicU64 = AtomicU64::new(0);
static INDEX_PAGE_WRITE_BYTES: AtomicU64 = AtomicU64::new(0);

/// One encoded authority candidate: the head document size the publication CAS carries,
/// with the reader pins and receipts the same document holds. Pin refs are counted over
/// every pin's entries, because that is what the document grows with.
pub(crate) fn observe_root_publish(encoded_bytes: usize, pin_refs: usize, receipts: usize) {
    if !enabled() {
        return;
    }
    add(&ROOT_PUBLISH_ATTEMPTS, 1);
    add(&ROOT_ENCODED_BYTES, encoded_bytes as u64);
    add(&PIN_REFS, pin_refs as u64);
    add(&ROOT_RECEIPTS, receipts as u64);
}

pub(crate) fn observe_root_publish_retry() {
    if !enabled() {
        return;
    }
    add(&ROOT_PUBLISH_RETRIES, 1);
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
                add(&LOCK_WAITS[self.domain as usize], 1);
                add(
                    &LOCK_WAIT_NANOS[self.domain as usize],
                    nanos(started.elapsed()),
                );
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
            add(&LOCK_HOLDS[domain as usize], 1);
            add(&LOCK_HOLD_NANOS[domain as usize], nanos(started.elapsed()));
        }
    }
}

static EXECUTE_CALLS: AtomicU64 = AtomicU64::new(0);
static EXECUTE_NANOS: AtomicU64 = AtomicU64::new(0);
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
        add(&EXECUTE_NANOS, nanos(started.elapsed()));
        if let Some(steps) = steps {
            add(&EXECUTE_STEPS, steps);
        }
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

static BLOCKS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];
static DENIED: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];

static WAL_INDEX_READS: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_READ_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_INDEX_READ_ENTRIES: AtomicU64 = AtomicU64::new(0);

/// Timer for one maintenance unit. Disabled, it carries no clock, so a process with the
/// switch off reads no time it would not have read anyway.
pub(crate) struct MaintenanceTimer {
    lane: crate::maintenance::MaintenanceLane,
    started: Option<Instant>,
}

impl MaintenanceTimer {
    pub(crate) fn begin(lane: crate::maintenance::MaintenanceLane) -> Self {
        Self {
            lane,
            started: enabled().then(Instant::now),
        }
    }

    /// One maintenance unit: the backfill volume it acted on, the wall time inside it, and
    /// whether it completed. Both the admin entry points and the scheduler's units report
    /// through here, so the two triggers are measured on one caliber.
    pub(crate) fn finish(self, backfill_bytes: u64, completed: bool) {
        let Some(started) = self.started else {
            return;
        };
        let index = self.lane.index();
        add(&UNIT_CALLS[index], 1);
        add(&UNIT_BACKFILL_BYTES[index], backfill_bytes);
        add(&UNIT_NANOS[index], nanos(started.elapsed()));
        if !completed {
            add(&UNIT_FAILURES[index], 1);
        }
    }
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
pub(crate) struct MetadataWork {
    /// Encoded authority head documents built for a publication attempt.
    pub root_encoded_bytes: u64,
    /// Publication attempts, i.e. `transact` loop iterations that reached the head CAS.
    pub root_publish_attempts: u64,
    /// Attempts that lost the head CAS and retried.
    pub root_publish_retries: u64,
    /// Reader-pin entries in the published documents.
    pub pin_refs: u64,
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
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct LockTotals {
    pub waits: u64,
    pub wait_ns: u64,
    pub holds: u64,
    pub hold_ns: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct ExecutionWork {
    /// Native statement executions observed.
    pub native_execute_calls: u64,
    pub native_execute_ns: u64,
    /// Engine VM steps of the executions that reached the success path.
    pub native_execute_steps: u64,
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
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct MaintenanceWork {
    /// Per lane, in `MaintenanceLane::ALL` order.
    pub units: BTreeMap<&'static str, UnitTotals>,
    /// Why a unit did not complete, counted where the reason was decided.
    pub blocked: BTreeMap<&'static str, u64>,
    /// Budget refusals, by the budget that ran out. A refusal is a skip.
    pub refused: BTreeMap<&'static str, u64>,
    /// `wal/index` reads that happened on paths which already needed the index.
    pub wal_index_reads: u64,
    pub wal_index_read_bytes: u64,
    pub wal_index_read_entries: u64,
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

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct WorkMetricsSnapshot {
    pub wal_workload: WalWorkload,
    pub metadata: MetadataWork,
    pub locks: BTreeMap<&'static str, LockTotals>,
    pub execution: ExecutionWork,
    pub results: ResultWork,
    pub maintenance: MaintenanceWork,
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
                },
            )
        })
        .collect()
}

fn locks() -> BTreeMap<&'static str, LockTotals> {
    LOCK_NAMES
        .iter()
        .enumerate()
        .map(|(index, name)| {
            (
                *name,
                LockTotals {
                    waits: load(&LOCK_WAITS[index]),
                    wait_ns: load(&LOCK_WAIT_NANOS[index]),
                    holds: load(&LOCK_HOLDS[index]),
                    hold_ns: load(&LOCK_HOLD_NANOS[index]),
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
            pin_refs: load(&PIN_REFS),
            root_receipts: load(&ROOT_RECEIPTS),
            wal_index_commits: load(&WAL_INDEX_COMMITS),
            wal_index_bytes: load(&WAL_INDEX_BYTES),
            wal_index_receipts: load(&WAL_INDEX_RECEIPTS),
            wal_index_retries: load(&WAL_INDEX_RETRIES),
            index_page_reads: load(&INDEX_PAGE_READS),
            index_page_writes: load(&INDEX_PAGE_WRITES),
            index_page_write_bytes: load(&INDEX_PAGE_WRITE_BYTES),
        },
        locks: locks(),
        execution: ExecutionWork {
            native_execute_calls: load(&EXECUTE_CALLS),
            native_execute_ns: load(&EXECUTE_NANOS),
            native_execute_steps: load(&EXECUTE_STEPS),
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
            refused: report(
                &crate::maintenance::BudgetDenied::ALL,
                crate::maintenance::BudgetDenied::name,
                &DENIED,
            ),
            wal_index_reads: load(&WAL_INDEX_READS),
            wal_index_read_bytes: load(&WAL_INDEX_READ_BYTES),
            wal_index_read_entries: load(&WAL_INDEX_READ_ENTRIES),
        },
    })
}

#[cfg(test)]
mod tests;
