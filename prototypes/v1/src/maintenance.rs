//! Maintenance scheduling for checkpoint, WAL compaction and the bookkeeping they need.
//!
//! The units here are the ones W4 asks to keep off the request path. They are not new work:
//! `RemoteDatabase::checkpoint` (Full) and `RemoteDatabase::compact_wal` (truncate checkpoint
//! plus WAL reset behind a complete-backfill precondition) already exist and keep their
//! semantics. What this module adds is *when* they may run, *how many* may run at once, and
//! what a blocked unit records.
//!
//! Three properties are deliberate and are what the tests pin down:
//!
//! 1. Nothing in the request path calls this module. The periodic driver is
//!    `WorkerRuntime::maintenance_tick`, called from the worker watchdog, and the units it
//!    decides on are executed on the scheduler's own thread. A commit never waits for a
//!    checkpoint or a compaction. The one synchronous caller is a database unload
//!    (`pre_suspend`), which is already retiring the instance; its wait is bounded and a
//!    failure there never blocks the unload.
//! 2. Every unit is decided by low/high watermarks plus a cooldown, so crossing a threshold
//!    does not turn into "do the heavy work on every commit". A unit that just ran, or that
//!    was blocked, is not eligible again until its cooldown or backoff expires.
//! 3. A unit that a budget refuses is *skipped*, never partially executed. Skipping keeps the
//!    old WAL exactly where it is; there is no path here that reclaims something a reader, a
//!    recovery or an unsettled outcome still depends on.
//!
//! Every constant below is an experiment input, not a measured threshold. The defaults come
//! from the research note's 16/64/256 MiB inputs and are only reachable with
//! `PERI_MAINTENANCE_SCHEDULE=1`; the delivery note under `docs/performance-review/` states
//! how each one is meant to be calibrated before it is trusted.

mod budget;
mod ledger;
mod policy;
mod scheduler;
mod signals;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod tests;

use anyhow::Result;

pub(crate) use budget::BudgetDenied;
#[cfg(test)]
pub(crate) use budget::MaintenanceBudget;
pub(crate) use ledger::{BlockReason, MaintenanceLedger};
pub(crate) use policy::MaintenanceLane;
pub(crate) use scheduler::MaintenanceScheduler;
#[cfg(test)]
pub(crate) use scheduler::{PreSuspendOutcome, PreSuspendReason};
pub(crate) use signals::{millis, MaintenanceSignals};

/// What the scheduler needs from a database, and nothing else.
///
/// The scheduler is deliberately not written against `RemoteDatabase`: the four operations it
/// needs are the entire contract, which keeps the scheduling rules testable without a store,
/// a native engine or a thread, and keeps the direction of the dependency one way. The real
/// implementation lives in `database.rs` next to the units it schedules.
pub(crate) trait MaintenanceTarget: Send + Sync {
    /// Stable identity of the database, used as the per-database budget key and as the
    /// ledger's scope. Two handles on the same database must produce the same string.
    fn identity(&self) -> String;

    fn ledger(&self) -> &MaintenanceLedger;

    /// Cheap in-process figures. Must not perform an object-store request: it is called from
    /// a periodic driver.
    fn signals(&self) -> Result<MaintenanceSignals>;

    /// Runs one unit. Implementations must apply the same preconditions as the admin entry
    /// point for the same unit, including the complete-backfill and no-unconfirmed-WAL rules
    /// of the compaction lane.
    fn run_maintenance(&self, lane: MaintenanceLane) -> Result<()>;
}
