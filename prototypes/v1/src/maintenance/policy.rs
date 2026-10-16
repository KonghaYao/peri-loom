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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceSkip {
    /// The scheduler is off. This is the default.
    Disabled,
    /// The WAL still carries a tail the publication does not cover. Skipping here is what
    /// keeps the compaction precondition ("no unconfirmed WAL") a precondition instead of a
    /// failure discovered mid-dispatch.
    UnconfirmedWal,
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
}

impl MaintenancePolicy {
    /// Defaults are the research note's experimental inputs (16/64/256 MiB) placed on the
    /// two lanes, plus cadences chosen only to be obviously slower than a commit. They are
    /// **not** calibrated: the note states these sizes are inputs to an experiment, and the
    /// scheduler is off unless `PERI_MAINTENANCE_SCHEDULE=1`, so no default here runs.
    pub(crate) fn configured() -> anyhow::Result<Self> {
        use anyhow::{ensure, Context};

        let enabled = std::env::var("PERI_MAINTENANCE_SCHEDULE").as_deref() == Ok("1");
        let mib = 1024 * 1024;
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
            if let Ok(value) = std::env::var(name) {
                *slot = value
                    .parse::<u64>()
                    .context("invalid maintenance watermark")?;
            }
        }
        if let Ok(value) = std::env::var("PERI_MAINTENANCE_COOLDOWN_MS") {
            policy.cooldown = Duration::from_millis(
                value
                    .parse::<u64>()
                    .context("invalid maintenance cooldown")?,
            );
        }
        if let Ok(value) = std::env::var("PERI_MAINTENANCE_BLOCKED_BACKOFF_MS") {
            policy.blocked_backoff = Duration::from_millis(
                value
                    .parse::<u64>()
                    .context("invalid maintenance blocked backoff")?,
            );
        }
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
        }
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
