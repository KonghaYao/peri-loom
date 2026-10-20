//! Per-database maintenance bookkeeping: what ran, what was blocked, and when a lane may be
//! considered again.
//!
//! This is the only place a maintenance outcome is remembered, and it is remembered per
//! database rather than per request. Two consequences matter:
//!
//! - The scheduler reads it instead of the clock alone, so a lane that just ran or was
//!   blocked cannot be picked again inside its cooldown.
//! - Tests can assert "no maintenance ran" and "this database is blocked by a long reader,
//!   for this reason" against a database object, without a process-wide counter and without
//!   the `PERI_WORK_METRICS` switch, so the assertions cannot race another test.
//!
//! Blocked reclamation is recorded alongside the error the caller already returns, never
//! instead of it: the error type and the reason it carries are the ones the call site had
//! before. Recording a block never drops, truncates or rewrites anything, and no path here
//! releases a reference a reader might still be using.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::policy::MaintenanceLane;
use super::signals::ReaderAge;

/// Why a maintenance unit did not complete. The first two are the W4 cases: a reader that
/// still needs the old WAL, and a truncate checkpoint that could not backfill all of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BlockReason {
    /// Another lane holds a native read lock, so a truncate checkpoint cannot pass it. The
    /// old WAL stays exactly where it is.
    LongReader,
    /// The truncate checkpoint returned a partial backfill. The WAL index is not truncated,
    /// so the frames the reader still needs stay published.
    PartialBackfill,
    /// The WAL carries a tail the publication does not cover, so the reset precondition is
    /// not met. Nothing is reclaimed.
    UnconfirmedWal,
    /// Another session or another maintenance unit holds the lane. Transient.
    Contended,
    /// A budget refused the unit. The unit is skipped, not run partially.
    Budget,
}

impl BlockReason {
    pub(crate) const ALL: [Self; 5] = [
        Self::LongReader,
        Self::PartialBackfill,
        Self::UnconfirmedWal,
        Self::Contended,
        Self::Budget,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::LongReader => "long_reader",
            Self::PartialBackfill => "partial_backfill",
            Self::UnconfirmedWal => "unconfirmed_wal",
            Self::Contended => "contended",
            Self::Budget => "budget",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct LaneState {
    /// Set when the WAL crossed the lane's high mark, cleared at the low mark.
    armed: bool,
    /// Picks that reached a due decision, including picks the budget later refused. A pick
    /// may be refused by the budget, so this counter is not "units executed": `executions` is.
    attempts: u64,
    /// Units that ran to completion or failed, recorded by the unit itself. This is the
    /// counter that makes "the request path did not run maintenance" observable: a commit
    /// that checkpointed would move it.
    executions: u64,
    /// Picks the budget refused before the unit started anything.
    refused: u64,
    completed: u64,
    failed: u64,
    last_due: Option<Instant>,
    next_allowed: Option<Instant>,
    /// First instant this lane was refused because the quiet window never arrived. Cleared
    /// when the lane is no longer busy or is picked, so the starvation bound measures one
    /// uninterrupted busy period rather than the lifetime of the database.
    busy_since: Option<Instant>,
}

#[derive(Debug)]
struct State {
    lanes: [LaneState; 2],
    blocks: BTreeMap<BlockReason, u64>,
    opened: Instant,
}

/// Per-database maintenance history. Exported through the worker's `diagnostics` response
/// and asserted directly by the tests, so a block reason is visible without the
/// `PERI_WORK_METRICS` switch.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct MaintenanceLedgerCounters {
    /// Picks that reached a due decision, including picks the budget later refused.
    pub attempts: u64,
    /// Units that ran, whichever trigger picked them.
    pub executions: u64,
    pub completed: u64,
    pub failed: u64,
    /// Picks the budget refused. Nothing about the database was touched.
    pub refused: u64,
    pub blocks: BTreeMap<&'static str, u64>,
    /// Highest reader age this database has seen, in ms. Kept after the reader leaves so an
    /// export taken later can still report who blocked reclamation.
    pub oldest_reader_peak_ms: u64,
}

/// Per-database ledger. Cheap to read and cheap to write, and never on the path a commit
/// takes: the request path only registers reader ages, which is one atomic and one mutex.
pub(crate) struct MaintenanceLedger {
    state: Mutex<State>,
    readers: Arc<ReaderAge>,
}

impl Default for MaintenanceLedger {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                lanes: [LaneState::default(); 2],
                blocks: BTreeMap::new(),
                opened: Instant::now(),
            }),
            readers: Arc::new(ReaderAge::default()),
        }
    }
}

impl MaintenanceLedger {
    pub(crate) fn readers(&self) -> &Arc<ReaderAge> {
        &self.readers
    }

    pub(crate) fn opened(&self) -> Instant {
        self.lock().opened
    }

    pub(crate) fn armed(&self, lane: MaintenanceLane) -> bool {
        self.lock().lanes[lane.index()].armed
    }

    pub(crate) fn set_armed(&self, lane: MaintenanceLane, armed: bool) {
        self.lock().lanes[lane.index()].armed = armed;
    }

    pub(crate) fn last_due(&self, lane: MaintenanceLane) -> Option<Instant> {
        self.lock().lanes[lane.index()].last_due
    }

    /// Units that reached the lane's entry point. A suspend-time pre-tidy reads this around
    /// its call to tell "the unit never started" apart from "the unit started and its
    /// completion is unconfirmed": a timeout must never be reported as maintenance that did
    /// not happen.
    pub(crate) fn executions(&self, lane: MaintenanceLane) -> u64 {
        self.lock().lanes[lane.index()].executions
    }

    pub(crate) fn next_allowed(&self, lane: MaintenanceLane) -> Option<Instant> {
        self.lock().lanes[lane.index()].next_allowed
    }

    /// When the lane first became ineligible because the database was never quiet. A lane
    /// that stays busy past the policy's starvation bound is admitted once with its bounded
    /// per-unit budget; without this timestamp an always-busy database would never run one.
    pub(crate) fn busy_since(&self, lane: MaintenanceLane) -> Option<Instant> {
        self.lock().lanes[lane.index()].busy_since
    }

    pub(crate) fn mark_busy(&self, lane: MaintenanceLane, now: Instant) {
        let mut state = self.lock();
        let entry = &mut state.lanes[lane.index()];
        if entry.busy_since.is_none() {
            entry.busy_since = Some(now);
        }
    }

    pub(crate) fn clear_busy(&self, lane: MaintenanceLane) {
        self.lock().lanes[lane.index()].busy_since = None;
    }

    /// A snapshot of the block counts, used by the scheduler to tell a blocked unit from an
    /// ordinary failure without changing the error type the call sites already return.
    pub(crate) fn block_counts(&self) -> [u64; BlockReason::ALL.len()] {
        let state = self.lock();
        BlockReason::ALL.map(|reason| state.blocks.get(&reason).copied().unwrap_or(0))
    }

    /// Extends every lane's eligibility by `until`. The reason was recorded by the unit that
    /// observed the block; this only applies the scheduler's cooldown, so it is combined with
    /// the existing deadline instead of replacing it.
    pub(crate) fn back_off(&self, until: Instant) {
        let mut state = self.lock();
        for entry in &mut state.lanes {
            entry.next_allowed = Some(entry.next_allowed.map_or(until, |held| held.max(until)));
        }
    }

    /// Records that a lane is about to run, and closes its cooldown window immediately: a
    /// unit that takes longer than the cooldown must not be picked a second time while the
    /// first one is still in flight.
    pub(crate) fn record_start(&self, lane: MaintenanceLane, now: Instant, cooldown: Duration) {
        let mut state = self.lock();
        let entry = &mut state.lanes[lane.index()];
        entry.attempts += 1;
        entry.last_due = Some(now);
        entry.next_allowed = Some(now + cooldown);
        entry.busy_since = None;
    }

    pub(crate) fn record_completion(&self, lane: MaintenanceLane) {
        self.lock().lanes[lane.index()].completed += 1;
    }

    /// Records that a unit started, whichever trigger picked it. Written by the unit's own
    /// entry point, so the admin path and the scheduled path are counted on one caliber.
    pub(crate) fn record_execution(&self, lane: MaintenanceLane) {
        self.lock().lanes[lane.index()].executions += 1;
    }

    /// Records a pick the budget refused. Nothing was started, so this is counted apart from
    /// `attempts`: a refused pick must never look like a unit that ran.
    pub(crate) fn record_refused(&self, lane: MaintenanceLane) {
        self.lock().lanes[lane.index()].refused += 1;
    }

    pub(crate) fn record_failure(&self, lane: MaintenanceLane) {
        self.lock().lanes[lane.index()].failed += 1;
    }

    /// Records a blocked unit. `until` pushes the *whole* database out of eligibility rather
    /// than only the lane that was blocked: while a long reader holds its snapshot, letting
    /// the other lane run would spend a second backfill against the same reader.
    pub(crate) fn record_block(&self, reason: BlockReason, until: Option<Instant>) {
        let mut state = self.lock();
        *state.blocks.entry(reason).or_insert(0) += 1;
        if let Some(until) = until {
            for entry in &mut state.lanes {
                entry.next_allowed = Some(entry.next_allowed.map_or(until, |held| held.max(until)));
            }
        }
    }

    /// Records the oldest age seen by a reader on this database.
    pub(crate) fn observe_reader_age(&self, age: Duration) {
        if !age.is_zero() {
            self.readers.observe(age);
        }
    }

    pub(crate) fn counters(&self) -> MaintenanceLedgerCounters {
        let state = self.lock();
        MaintenanceLedgerCounters {
            attempts: state.lanes.iter().map(|lane| lane.attempts).sum(),
            executions: state.lanes.iter().map(|lane| lane.executions).sum(),
            completed: state.lanes.iter().map(|lane| lane.completed).sum(),
            failed: state.lanes.iter().map(|lane| lane.failed).sum(),
            refused: state.lanes.iter().map(|lane| lane.refused).sum(),
            blocks: state
                .blocks
                .iter()
                .map(|(reason, count)| (reason.name(), *count))
                .collect(),
            oldest_reader_peak_ms: super::signals::millis(self.readers.peak()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
