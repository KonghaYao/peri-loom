//! Watermarks, cooldown and the per-lane slots that keep the units apart.
//!
//! The decision is a pure function of the signals, the ledger's per-lane history and the
//! clock, so the rules can be tested without a database, a store or a thread. Nothing in this
//! file performs work; it only says whether a lane is eligible right now.
//!
//! Watermarks are a hysteresis band, not a threshold: a lane becomes *armed* when the WAL
//! crosses the high mark and stays armed until the WAL falls back to the low mark. Without
//! the band, a run that leaves the WAL just above the high mark would re-arm every commit,
//! which is the failure mode W4 names. The cooldown is a second, independent brake: a lane
//! that ran or was blocked is ineligible for a fixed period regardless of the WAL size.
//!
//! Each lane has its own period and its own phase offset inside that period. That is the
//! de-alignment: the checkpoint lane and the compaction lane never become eligible in the
//! same slot, so their backfills do not stack into one spike. `configured` refuses a policy
//! whose lanes share a period or a phase, which makes "avoid alignment" a construction-time
//! property rather than a comment.

use std::time::{Duration, Instant};

use super::ledger::MaintenanceLedger;
use super::signals::millis;
use super::signals::MaintenanceSignals;

/// The maintenance units W4 keeps apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceLane {
    /// Full checkpoint: backfills the WAL mirror into the database image and publishes.
    Checkpoint,
    /// Truncate checkpoint plus WAL reset. Heavier, rarer, and gated on a complete backfill.
    IndexCompact,
}

impl MaintenanceLane {
    pub(crate) const ALL: [Self; 2] = [Self::Checkpoint, Self::IndexCompact];

    pub(crate) fn index(self) -> usize {
        match self {
            Self::Checkpoint => 0,
            Self::IndexCompact => 1,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::IndexCompact => "index_compact",
        }
    }
}

/// One lane's hysteresis band and evaluation cadence. All three numbers are experiment
/// inputs; see the delivery note for how each is meant to be calibrated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Watermark {
    /// Below this the lane is disarmed. Above it nothing is decided on its own.
    pub low_bytes: u64,
    /// Crossing this arms the lane.
    pub high_bytes: u64,
    /// Nominal spacing between two eligible slots of this lane.
    pub period: Duration,
    /// Offset of this lane's slots inside its period, against the database's open instant.
    pub phase: Duration,
}

/// How much work one unit may plan for.
///
/// `max_pages` is the only bound the native checkpoint can be given before it starts: a
/// passive checkpoint accepts an inclusive frame bound, so the scheduler can turn it into a
/// deterministic number of frames per pass. `max_bytes` caps the charge taken from the shared
/// byte window, and `max_time` is the point after which a unit will not begin its next phase
/// (the exclusive truncate). None of the three is a hard cancellation: a native checkpoint
/// already dispatched cannot be interrupted, so a unit can exceed `max_time`; the timer reports
/// the overrun instead of hiding it. `max_bytes` is therefore also an admission charge, not a
/// promise that the engine stops at exactly that byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnitBudget {
    pub max_pages: u64,
    pub max_bytes: u64,
    pub max_time: Duration,
}

impl UnitBudget {
    /// The admin/CLI path still performs the whole unit. Only the scheduler passes a finite
    /// budget; using this value keeps the pre-existing entry points byte-for-byte compatible.
    pub(crate) const UNBOUNDED: Self = Self {
        max_pages: u64::MAX,
        max_bytes: u64::MAX,
        max_time: Duration::MAX,
    };

    /// A zero bound would make the lane either spin without progress or charge nothing, so it
    /// is rejected instead of being clamped: a silently changed budget is not a configured one.
    /// The per-page floor is SQLite's minimum page size; a byte allowance below it cannot cover
    /// the pages the lane is allowed to scan.
    pub(crate) fn validate(self, name: &str) -> anyhow::Result<()> {
        use anyhow::ensure;

        ensure!(
            self.max_pages > 0,
            "maintenance {name} max_pages must be positive"
        );
        ensure!(
            self.max_bytes > 0,
            "maintenance {name} max_bytes must be positive"
        );
        ensure!(
            !self.max_time.is_zero(),
            "maintenance {name} max_time must be positive"
        );
        ensure!(
            self.max_pages.saturating_mul(512) <= self.max_bytes,
            "maintenance {name} max_bytes must cover at least 512 bytes per page"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceSkip {
    /// The scheduler is off. This is the default.
    Disabled,
    /// An append or a database publication is still unsettled. The truncate checkpoint
    /// itself applies the WAL tail, so this lane waits for append quiescence, not for the
    /// image to catch up with every append.
    UnconfirmedWal,
    /// A foreground statement ran inside the quiet window. Both units write the main
    /// database while they run, and that write's file sync sits on the same media as the
    /// commit barrier: a checkpoint measured at 0.5–0.8 s of SQL tail every 60 s, and a
    /// compaction rewrites the whole WAL baseline.
    Busy,
    /// Past the low mark without ever crossing the high mark, or back below the low mark.
    BelowLow,
    /// This lane's slot in its own period has not come round yet.
    NotDue,
    /// The lane ran or was blocked recently.
    Cooling,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceDecision {
    Due,
    Skip(MaintenanceSkip),
}

impl MaintenanceDecision {
    #[cfg(test)]
    pub(crate) fn is_due(self) -> bool {
        matches!(self, Self::Due)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MaintenancePolicy {
    enabled: bool,
    checkpoint: Watermark,
    index_compact: Watermark,
    cooldown: Duration,
    blocked_backoff: Duration,
    /// How long the database must be without a foreground statement before either lane may
    /// run. Zero means "no quiet requirement", which is what the unit tests use.
    compact_quiet: Duration,
    /// The WAL size above which the quiet requirement is dropped for both lanes. This is a
    /// watermark, not a storage limit: crossing it never rejects a write, it only lets one
    /// bounded unit run beside load. The real capacity is `WalBudget`, which refuses a new
    /// write before it is dispatched; keeping the two values distinct is what stops a
    /// configured watermark from being mistaken for a hard bound.
    quiet_waiver_bytes: u64,
    /// How long a lane may stay busy before one bounded unit is admitted despite the quiet
    /// gate. Without it a workload that never pauses would never reach a quiet window; the
    /// unit's own pages/time budget is what keeps that waiver from becoming a large stall.
    starvation: Duration,
    checkpoint_budget: UnitBudget,
    compact_budget: UnitBudget,
}

impl MaintenancePolicy {
    /// Defaults are the research note's experimental inputs (16/64/256 MiB) placed on the
    /// two lanes, plus cadences chosen only to be obviously slower than a commit. They are
    /// **not** calibrated: the note states these sizes are inputs to an experiment, and the
    /// scheduler is off unless `PERI_MAINTENANCE_SCHEDULE=1`, so no default here runs.
    ///
    /// Supported configuration (all values are validated; zero, non-numeric or inconsistent
    /// values refuse to start rather than being clamped):
    ///
    /// - `PERI_MAINTENANCE_CHECKPOINT_{LOW,HIGH}_BYTES` — the checkpoint hysteresis band.
    /// - `PERI_MAINTENANCE_COMPACT_{LOW,HIGH}_BYTES` — the compaction hysteresis band.
    /// - `PERI_MAINTENANCE_{COOLDOWN,BLOCKED_BACKOFF}_MS` — lane brakes.
    /// - `PERI_MAINTENANCE_COMPACT_QUIET_MS` — the foreground quiet window.
    /// - `PERI_MAINTENANCE_QUIET_WAIVER_BYTES` — the WAL watermark that waives the quiet
    ///   window. It is not a capacity: writes are never rejected for crossing it.
    /// - `PERI_MAINTENANCE_STARVATION_MS` — how long load may suppress a lane before one
    ///   bounded unit is forced through.
    /// - `PERI_MAINTENANCE_CHECKPOINT_{MAX_PAGES,MAX_BYTES,MAX_MS}` and
    ///   `PERI_MAINTENANCE_COMPACT_{MAX_PAGES,MAX_BYTES,MAX_MS}` — the per-unit work budget
    ///   for each lane. `MAX_PAGES` is the frame bound handed to the native passive
    ///   checkpoint; `MAX_BYTES` is the charge against the shared byte window; `MAX_MS` is
    ///   the point after which a unit will not start its next phase.
    pub(crate) fn configured() -> anyhow::Result<Self> {
        use anyhow::{ensure, Context};

        let mib = 1024 * 1024;
        let enabled = std::env::var("PERI_MAINTENANCE_SCHEDULE").as_deref() == Ok("1");
        let mut policy = Self {
            enabled,
            checkpoint: Watermark {
                low_bytes: 64 * mib,
                high_bytes: 256 * mib,
                period: Duration::from_secs(60),
                phase: Duration::ZERO,
            },
            index_compact: Watermark {
                low_bytes: 16 * mib,
                high_bytes: 64 * mib,
                period: Duration::from_secs(300),
                phase: Duration::from_secs(150),
            },
            cooldown: Duration::from_secs(30),
            blocked_backoff: Duration::from_secs(120),
            compact_quiet: Duration::from_secs(2),
            quiet_waiver_bytes: 256 * mib,
            starvation: Duration::from_secs(60),
            checkpoint_budget: UnitBudget {
                max_pages: 4096,
                max_bytes: 16 * mib,
                max_time: Duration::from_secs(2),
            },
            compact_budget: UnitBudget {
                max_pages: 8192,
                max_bytes: 32 * mib,
                max_time: Duration::from_secs(5),
            },
        };
        let overrides = [
            (
                "PERI_MAINTENANCE_CHECKPOINT_LOW_BYTES",
                &mut policy.checkpoint.low_bytes,
            ),
            (
                "PERI_MAINTENANCE_CHECKPOINT_HIGH_BYTES",
                &mut policy.checkpoint.high_bytes,
            ),
            (
                "PERI_MAINTENANCE_COMPACT_LOW_BYTES",
                &mut policy.index_compact.low_bytes,
            ),
            (
                "PERI_MAINTENANCE_COMPACT_HIGH_BYTES",
                &mut policy.index_compact.high_bytes,
            ),
        ];
        for (name, slot) in overrides {
            if let Some(value) = env_value(name)? {
                *slot = value
                    .parse::<u64>()
                    .with_context(|| format!("invalid maintenance watermark {name}"))?;
            }
        }
        if let Some(value) = env_value("PERI_MAINTENANCE_COOLDOWN_MS")? {
            policy.cooldown =
                Duration::from_millis(value.parse::<u64>().with_context(|| {
                    "invalid maintenance cooldown PERI_MAINTENANCE_COOLDOWN_MS"
                })?);
        }
        if let Some(value) = env_value("PERI_MAINTENANCE_BLOCKED_BACKOFF_MS")? {
            policy.blocked_backoff =
                Duration::from_millis(value.parse::<u64>().with_context(|| {
                    "invalid maintenance blocked backoff PERI_MAINTENANCE_BLOCKED_BACKOFF_MS"
                })?);
        }
        if let Some(value) = env_value("PERI_MAINTENANCE_COMPACT_QUIET_MS")? {
            policy.compact_quiet =
                Duration::from_millis(value.parse::<u64>().with_context(|| {
                    "invalid maintenance compact quiet window PERI_MAINTENANCE_COMPACT_QUIET_MS"
                })?);
        }
        if let Some(value) = env_value("PERI_MAINTENANCE_QUIET_WAIVER_BYTES")? {
            policy.quiet_waiver_bytes = value
                .parse::<u64>()
                .with_context(|| "invalid maintenance quiet waiver bytes")?;
        }
        if let Some(value) = env_value("PERI_MAINTENANCE_STARVATION_MS")? {
            policy.starvation = Duration::from_millis(
                value
                    .parse::<u64>()
                    .with_context(|| "invalid maintenance starvation bound")?,
            );
        }
        policy.checkpoint_budget = unit_budget_from_env(
            "checkpoint",
            policy.checkpoint_budget,
            "PERI_MAINTENANCE_CHECKPOINT_MAX_PAGES",
            "PERI_MAINTENANCE_CHECKPOINT_MAX_BYTES",
            "PERI_MAINTENANCE_CHECKPOINT_MAX_MS",
        )?;
        policy.compact_budget = unit_budget_from_env(
            "compact",
            policy.compact_budget,
            "PERI_MAINTENANCE_COMPACT_MAX_PAGES",
            "PERI_MAINTENANCE_COMPACT_MAX_BYTES",
            "PERI_MAINTENANCE_COMPACT_MAX_MS",
        )?;
        for lane in MaintenanceLane::ALL {
            let watermark = policy.watermark(lane);
            ensure!(
                watermark.low_bytes < watermark.high_bytes,
                "maintenance low watermark must be below the high watermark"
            );
            ensure!(
                !watermark.period.is_zero() && watermark.phase < watermark.period,
                "maintenance lane phase must fall inside a non-zero period"
            );
        }
        ensure!(
            !policy.cooldown.is_zero() && !policy.blocked_backoff.is_zero(),
            "maintenance cooldown and blocked backoff must be positive"
        );
        ensure!(
            policy.compact_quiet < policy.checkpoint.period
                && policy.compact_quiet < policy.index_compact.period,
            "maintenance quiet window must be shorter than each lane's period"
        );
        ensure!(
            policy.quiet_waiver_bytes > 0,
            "maintenance quiet waiver watermark must be positive"
        );
        ensure!(
            !policy.starvation.is_zero(),
            "maintenance starvation bound must be positive"
        );
        policy.checkpoint_budget.validate("checkpoint")?;
        policy.compact_budget.validate("compact")?;
        ensure!(
            policy.checkpoint.period != policy.index_compact.period
                && policy.checkpoint.phase != policy.index_compact.phase,
            "maintenance lanes must not share a period or a phase"
        );
        Ok(policy)
    }

    #[cfg(test)]
    pub(crate) fn for_tests(
        enabled: bool,
        checkpoint: Watermark,
        index_compact: Watermark,
        cooldown: Duration,
        blocked_backoff: Duration,
    ) -> Self {
        Self {
            enabled,
            checkpoint,
            index_compact,
            cooldown,
            blocked_backoff,
            compact_quiet: Duration::ZERO,
            quiet_waiver_bytes: u64::MAX,
            starvation: Duration::from_secs(3600),
            checkpoint_budget: UnitBudget::UNBOUNDED,
            compact_budget: UnitBudget::UNBOUNDED,
        }
    }

    /// Overrides the quiet gate for a test. The default `for_tests` policy has no quiet
    /// requirement, so a test that does not call this keeps the pre-gate behaviour.
    #[cfg(test)]
    pub(crate) fn with_quiet_gate(mut self, quiet: Duration, hard_bytes: u64) -> Self {
        self.compact_quiet = quiet;
        self.quiet_waiver_bytes = hard_bytes;
        self
    }

    /// Overrides the per-unit work bounds for a test.
    #[cfg(test)]
    pub(crate) fn with_unit_budgets(mut self, checkpoint: UnitBudget, compact: UnitBudget) -> Self {
        self.checkpoint_budget = checkpoint;
        self.compact_budget = compact;
        self
    }

    /// Overrides how long sustained load may suppress a lane for a test.
    #[cfg(test)]
    pub(crate) fn with_starvation(mut self, starvation: Duration) -> Self {
        self.starvation = starvation;
        self
    }

    /// Runs the production validation against a test-built policy, so a boundary test can use
    /// the same rules `configured` enforces without touching the process environment.
    #[cfg(test)]
    pub(crate) fn validated(self) -> anyhow::Result<Self> {
        use anyhow::ensure;

        for lane in MaintenanceLane::ALL {
            let watermark = self.watermark(lane);
            ensure!(
                watermark.low_bytes < watermark.high_bytes,
                "maintenance low watermark must be below the high watermark"
            );
            ensure!(
                !watermark.period.is_zero() && watermark.phase < watermark.period,
                "maintenance lane phase must fall inside a non-zero period"
            );
        }
        ensure!(
            !self.cooldown.is_zero() && !self.blocked_backoff.is_zero(),
            "maintenance cooldown and blocked backoff must be positive"
        );
        ensure!(
            self.compact_quiet < self.checkpoint.period
                && self.compact_quiet < self.index_compact.period,
            "maintenance quiet window must be shorter than each lane's period"
        );
        ensure!(
            self.quiet_waiver_bytes > 0,
            "maintenance quiet waiver watermark must be positive"
        );
        ensure!(
            !self.starvation.is_zero(),
            "maintenance starvation bound must be positive"
        );
        self.checkpoint_budget.validate("checkpoint")?;
        self.compact_budget.validate("compact")?;
        ensure!(
            self.checkpoint.period != self.index_compact.period
                && self.checkpoint.phase != self.index_compact.phase,
            "maintenance lanes must not share a period or a phase"
        );
        Ok(self)
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn watermark(&self, lane: MaintenanceLane) -> Watermark {
        match lane {
            MaintenanceLane::Checkpoint => self.checkpoint,
            MaintenanceLane::IndexCompact => self.index_compact,
        }
    }

    pub(crate) fn cooldown(&self) -> Duration {
        self.cooldown
    }

    pub(crate) fn unit_budget(&self, lane: MaintenanceLane) -> UnitBudget {
        match lane {
            MaintenanceLane::Checkpoint => self.checkpoint_budget,
            MaintenanceLane::IndexCompact => self.compact_budget,
        }
    }

    /// The hysteresis rule, exposed so the ledger's armed flag is only ever written from the
    /// policy: `Some(true)` arms on crossing the high mark, `Some(false)` disarms at the low
    /// mark, `None` leaves the lane as it was.
    pub(crate) fn arming(&self, lane: MaintenanceLane, wal_bytes: u64) -> Option<bool> {
        let watermark = self.watermark(lane);
        if wal_bytes >= watermark.high_bytes {
            Some(true)
        } else if wal_bytes <= watermark.low_bytes {
            Some(false)
        } else {
            None
        }
    }

    /// Arming with the capacity signal. The WAL budget's soft watermark is not a hard bound;
    /// it only asks maintenance to run earlier than the configured hysteresis band would.
    pub(crate) fn arming_with(
        &self,
        lane: MaintenanceLane,
        signals: &MaintenanceSignals,
    ) -> Option<bool> {
        if signals.wal_pressure {
            Some(true)
        } else {
            self.arming(lane, signals.wal_bytes)
        }
    }

    /// When this lane may next be evaluated, given the database's open instant and the last
    /// time the lane was considered due. Pure, so a test can drive it with fixed instants.
    pub(crate) fn next_slot(
        &self,
        lane: MaintenanceLane,
        origin: Instant,
        last_due: Option<Instant>,
    ) -> Instant {
        let watermark = self.watermark(lane);
        match last_due {
            None => origin + watermark.phase + watermark.period,
            Some(last) => last + watermark.period,
        }
    }

    /// Whether this lane may run now. Order matters: an unconfirmed WAL is refused before any
    /// watermark is consulted, because that is a correctness precondition of the unit rather
    /// than a policy preference.
    pub(crate) fn decide(
        &self,
        lane: MaintenanceLane,
        signals: &MaintenanceSignals,
        ledger: &MaintenanceLedger,
        now: Instant,
    ) -> MaintenanceDecision {
        if !self.enabled {
            return MaintenanceDecision::Skip(MaintenanceSkip::Disabled);
        }
        if lane == MaintenanceLane::IndexCompact && signals.unconfirmed_wal {
            return MaintenanceDecision::Skip(MaintenanceSkip::UnconfirmedWal);
        }
        // Both units write the main database and sync it to media. That sync shares the
        // device with the commit barrier, so a unit started under load surfaces as SQL
        // latency even when it never takes the writer admission: the 60 s checkpoint
        // measured 0.5–0.8 s tails, and the first backfill after a bulk import stalls for
        // tens of seconds. Run either lane only when no foreground statement has started
        // for the quiet window, unless the WAL crossed the quiet-waiver watermark or the
        // capacity budget's soft mark: then the recovery cost of leaving it wins over one
        // blocking window. A lane that has been suppressed by continuous load for the
        // starvation bound is admitted once as well; its per-unit pages/time budget keeps
        // that waiver bounded instead of turning it into an unbounded stall.
        let quiet_waived = signals.wal_pressure || signals.wal_bytes >= self.quiet_waiver_bytes;
        if signals.quiet_for_ms < millis(self.compact_quiet) && !quiet_waived {
            let starved = ledger
                .busy_since(lane)
                .is_some_and(|since| now.saturating_duration_since(since) >= self.starvation);
            if !starved {
                return MaintenanceDecision::Skip(MaintenanceSkip::Busy);
            }
        }
        let watermark = self.watermark(lane);
        if signals.wal_bytes < watermark.low_bytes || !ledger.armed(lane) {
            return MaintenanceDecision::Skip(MaintenanceSkip::BelowLow);
        }
        let next_slot = self.next_slot(lane, ledger.opened(), ledger.last_due(lane));
        if now < next_slot {
            return MaintenanceDecision::Skip(MaintenanceSkip::NotDue);
        }
        if ledger
            .next_allowed(lane)
            .is_some_and(|allowed| now < allowed)
        {
            return MaintenanceDecision::Skip(MaintenanceSkip::Cooling);
        }
        MaintenanceDecision::Due
    }

    /// The instant a blocked lane may be retried: the longer backoff exists because the
    /// blocking reader is still holding its snapshot, and retrying at the normal cadence
    /// would spend a truncate checkpoint per period against a reader that is still there.
    pub(crate) fn blocked_until(&self, now: Instant) -> Instant {
        now + self.blocked_backoff
    }
}

fn env_value(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Reads and validates one lane's per-unit budget. Kept separate from `configured` so the
/// boundary rules can be tested without mutating process-wide environment variables.
pub(crate) fn unit_budget_from_env(
    name: &str,
    defaults: UnitBudget,
    pages_env: &str,
    bytes_env: &str,
    time_env: &str,
) -> anyhow::Result<UnitBudget> {
    unit_budget(
        name,
        defaults,
        env_value(pages_env)?.as_deref(),
        env_value(bytes_env)?.as_deref(),
        env_value(time_env)?.as_deref(),
    )
}

fn unit_budget(
    name: &str,
    defaults: UnitBudget,
    pages: Option<&str>,
    bytes: Option<&str>,
    millis: Option<&str>,
) -> anyhow::Result<UnitBudget> {
    use anyhow::{Context, Result};

    let parse = |value: Option<&str>, default: u64, variable: &str| -> Result<u64> {
        match value {
            Some(value) => value
                .parse::<u64>()
                .with_context(|| format!("invalid maintenance {name} budget {variable}")),
            None => Ok(default),
        }
    };
    let budget = UnitBudget {
        max_pages: parse(pages, defaults.max_pages, "max_pages")?,
        max_bytes: parse(bytes, defaults.max_bytes, "max_bytes")?,
        max_time: Duration::from_millis(parse(
            millis,
            defaults.max_time.as_millis().min(u128::from(u64::MAX)) as u64,
            "max_time",
        )?),
    };
    budget.validate(name)?;
    Ok(budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(pages: u64, bytes: u64, millis: u64) -> UnitBudget {
        UnitBudget {
            max_pages: pages,
            max_bytes: bytes,
            max_time: Duration::from_millis(millis),
        }
    }

    #[test]
    fn unit_budgets_reject_zero_and_inconsistent_values() {
        assert!(budget(0, 4096, 1).validate("checkpoint").is_err());
        assert!(budget(1, 0, 1).validate("checkpoint").is_err());
        assert!(budget(1, 4096, 0).validate("checkpoint").is_err());
        // 8 pages need at least 4 KiB at SQLite's 512-byte minimum page size.
        assert!(budget(8, 4095, 1).validate("checkpoint").is_err());
        assert!(budget(8, 4096, 1).validate("checkpoint").is_ok());
    }

    #[test]
    fn unit_budget_parsing_rejects_zero_and_non_numeric_values() {
        let defaults = budget(4, 4096, 10);
        assert!(unit_budget("checkpoint", defaults, Some("0"), None, None).is_err());
        assert!(unit_budget("checkpoint", defaults, None, Some("0"), None).is_err());
        assert!(unit_budget("checkpoint", defaults, None, None, Some("0")).is_err());
        assert!(unit_budget("checkpoint", defaults, Some("many"), None, None).is_err());
        assert!(unit_budget("checkpoint", defaults, None, None, Some("-1")).is_err());
        assert_eq!(
            unit_budget("checkpoint", defaults, Some("7"), Some("4096"), Some("3")).unwrap(),
            budget(7, 4096, 3)
        );
    }

    #[test]
    fn a_policy_that_disables_a_bound_is_rejected_by_validation() {
        let base = MaintenancePolicy::for_tests(
            true,
            Watermark {
                low_bytes: 1,
                high_bytes: 2,
                period: Duration::from_secs(10),
                phase: Duration::ZERO,
            },
            Watermark {
                low_bytes: 1,
                high_bytes: 2,
                period: Duration::from_secs(11),
                phase: Duration::from_millis(1),
            },
            Duration::from_millis(1),
            Duration::from_millis(1),
        );
        assert!(base.clone().validated().is_ok());
        assert!(base
            .clone()
            .with_starvation(Duration::ZERO)
            .validated()
            .is_err());
        assert!(base
            .clone()
            .with_quiet_gate(Duration::ZERO, 0)
            .validated()
            .is_err());
        let zero_pages = UnitBudget {
            max_pages: 0,
            ..UnitBudget::UNBOUNDED
        };
        assert!(base
            .with_unit_budgets(zero_pages, UnitBudget::UNBOUNDED)
            .validated()
            .is_err());
    }
}
