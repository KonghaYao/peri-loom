//! The scheduler: probe, decide, charge, then run — in that order, and never on the request
//! path.
//!
//! `observe` is what a periodic driver calls. It is cheap when the switch is off (one static
//! read), and when it is on it only reads in-process figures and appends to a bounded queue;
//! it never runs a unit. `drain` is what executes queued units, on the scheduler's own thread
//! and under the budget.
//!
//! The split is the whole point of the module: a commit, a query or a session open cannot
//! reach `drain`, so no request ever waits for a checkpoint or a compaction. The tests assert
//! this by doing commits against a real database whose scheduler is armed and then finding
//! the ledger's attempt counter at zero.
//!
//! `drain` is also where a budget refusal becomes a *skip*: the unit is dropped from the
//! queue with a recorded reason and its backoff, the WAL is left alone, and nothing is
//! partially applied.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::Result;

use super::budget::MaintenanceBudget;
use super::policy::{MaintenanceDecision, MaintenanceLane, MaintenancePolicy, MaintenanceSkip};
use super::{BlockReason, MaintenanceTarget};

/// How often the driver thread looks at its queue. The queue is only ever filled by `observe`
/// on the watchdog, so this is a wake-up cadence, not a maintenance rate.
const DRIVER_INTERVAL: Duration = Duration::from_millis(50);

/// How many units a drain may run. One per drain keeps a slow unit from monopolising the
/// driver thread and makes the global concurrency budget the only way to have two in flight.
const UNITS_PER_DRAIN: usize = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MaintenanceDrain {
    pub completed: usize,
    pub failed: usize,
    pub refused: usize,
}

struct Queued {
    lane: MaintenanceLane,
    target: Arc<dyn MaintenanceTarget>,
    /// The backfill volume the decision was taken on. It is the charge against the byte
    /// budget, so a unit is never charged for a volume it was not planned around.
    bytes: u64,
}

pub(crate) struct MaintenanceScheduler {
    policy: MaintenancePolicy,
    budget: MaintenanceBudget,
    queue: Mutex<VecDeque<Queued>>,
    started: AtomicBool,
}

impl MaintenanceScheduler {
    pub(crate) fn configured() -> Result<Self> {
        Ok(Self::new(
            MaintenancePolicy::configured()?,
            MaintenanceBudget::configured()?,
        ))
    }

    pub(crate) fn new(policy: MaintenancePolicy, budget: MaintenanceBudget) -> Self {
        Self {
            policy,
            budget,
            queue: Mutex::new(VecDeque::new()),
            started: AtomicBool::new(false),
        }
    }

    /// A scheduler whose checkpoint lane is due `period` after the database opened and whose
    /// compaction lane never arms. This is what lets a test drive the watchdog wiring — tick,
    /// queue, driver thread, real entry point — without waiting for the production cadence.
    #[cfg(test)]
    pub(crate) fn for_tests(period: Duration) -> Self {
        use super::policy::Watermark;

        Self::new(
            MaintenancePolicy::for_tests(
                true,
                Watermark {
                    low_bytes: 0,
                    high_bytes: 1,
                    period,
                    phase: Duration::ZERO,
                },
                Watermark {
                    low_bytes: 0,
                    high_bytes: u64::MAX,
                    period: Duration::from_secs(3600),
                    phase: Duration::ZERO,
                },
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            MaintenanceBudget::new(1, 1, 64, 64 * 1024 * 1024, Duration::from_secs(60)),
        )
    }

    pub(crate) fn enabled(&self) -> bool {
        self.policy.enabled()
    }

    #[cfg(test)]
    pub(crate) fn policy(&self) -> &MaintenancePolicy {
        &self.policy
    }

    /// One scheduling pass over one database. Callers pass every loaded database from a
    /// periodic driver that is not serving requests.
    ///
    /// This only decides and queues: it never runs a unit, and it never starts the driver
    /// thread. A test can therefore drain the same queue deterministically, and a worker that
    /// schedules without an executor cannot have one appear underneath it.
    pub(crate) fn observe(&self, target: &Arc<dyn MaintenanceTarget>, now: Instant) -> Result<()> {
        if !self.policy.enabled() {
            return Ok(());
        }
        let ledger = target.ledger();
        let signals = target.signals()?;
        ledger.observe_reader_age(Duration::from_millis(signals.oldest_reader_ms));
        for lane in MaintenanceLane::ALL {
            if let Some(armed) = self.policy.arming(lane, signals.wal_bytes) {
                ledger.set_armed(lane, armed);
            }
        }
        for lane in MaintenanceLane::ALL {
            match self.policy.decide(lane, &signals, ledger, now) {
                MaintenanceDecision::Due => {
                    // The cooldown window opens at the *pick*, not at completion: a unit that
                    // outlives its cooldown must not be picked twice, and a unit refused by
                    // the budget must not be retried on the next tick.
                    ledger.record_start(lane, now, self.policy.cooldown());
                    self.lock().push_back(Queued {
                        lane,
                        target: target.clone(),
                        bytes: signals.backfill_bytes(),
                    });
                }
                MaintenanceDecision::Skip(skip) => {
                    if skip == MaintenanceSkip::UnconfirmedWal {
                        ledger.record_block(BlockReason::UnconfirmedWal, None);
                    }
                }
            }
        }
        Ok(())
    }

    /// Runs at most `max_units` queued units. Public so a test can drive the same code path
    /// the driver thread drives, without owning a thread.
    pub(crate) fn drain(&self, max_units: usize, now: Instant) -> MaintenanceDrain {
        let mut summary = MaintenanceDrain::default();
        for _ in 0..max_units {
            let Some(queued) = self.lock().pop_front() else {
                break;
            };
            let ledger = queued.target.ledger();
            let lease = match self
                .budget
                .acquire(&queued.target.identity(), queued.bytes, now)
            {
                Ok(lease) => lease,
                Err(denied) => {
                    // Budget exhausted: limit new work. The unit is skipped as a whole, the
                    // backoff is recorded, and the WAL it would have reclaimed stays put.
                    ledger.record_block(BlockReason::Budget, Some(self.policy.blocked_until(now)));
                    ledger.record_refused(queued.lane);
                    crate::work_metrics::observe_maintenance_refused(denied);
                    summary.refused += 1;
                    continue;
                }
            };
            let started = Instant::now();
            match queued.target.run_maintenance(queued.lane) {
                // The unit records its own duration through `work_metrics`, so the admin
                // entry points and the scheduled ones are measured on one caliber and no
                // unit is counted twice.
                Ok(()) => {
                    ledger.record_completion(queued.lane);
                    summary.completed += 1;
                }
                Err(error) => {
                    ledger.record_failure(queued.lane);
                    summary.failed += 1;
                    // A blocked unit is not a silent one: the reason is in the ledger and on
                    // stderr, and the cooldown keeps the next attempt from being immediate.
                    eprintln!(
                        "maintenance outcome=blocked lane={} elapsed_ms={} reason={error:#}",
                        queued.lane.name(),
                        started.elapsed().as_millis(),
                    );
                }
            }
            drop(lease);
        }
        summary
    }

    /// Starts the driver thread once. The owner calls this; `observe` deliberately does not,
    /// so a caller always knows whether an executor exists.
    ///
    /// The thread holds a weak reference, so it exits by itself when the runtime that owns the
    /// scheduler is dropped and never keeps a database alive.
    pub(crate) fn start(self: &Arc<Self>) -> Result<()> {
        if !self.policy.enabled() || self.started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("peri-maintenance".to_owned())
            .spawn(move || {
                while let Some(scheduler) = weak.upgrade() {
                    scheduler.drain(UNITS_PER_DRAIN, Instant::now());
                    std::thread::sleep(DRIVER_INTERVAL);
                }
            })
            .map(|_| ())
            .map_err(|error| {
                self.started.store(false, Ordering::SeqCst);
                error.into()
            })
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<Queued>> {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
