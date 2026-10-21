//! The WAL's real capacity budget, as opposed to the scheduling watermarks.
//!
//! The hysteresis watermarks in `policy` answer *when* a maintenance lane becomes eligible;
//! crossing them never rejects a write. This type is the other half of the contract: a hard
//! byte capacity, checked before a new WAL write is queued into the pending batch. A write
//! that would take the materialized WAL past the capacity is refused, so the caller learns
//! about the bound through the SQL outcome instead of through an append that can never fit.
//!
//! Two marks are intentional:
//!
//! - `soft_bytes` is a watermark. Reaching it arms the maintenance lanes early and is
//!   reported as pressure; it does not refuse anything.
//! - `hard_bytes` is the capacity. It is the only value that can reject a write, and the
//!   rejection happens before dispatch.
//!
//! The budget is off unless `PERI_WAL_BUDGET_BYTES` is set. An explicit hard value with a
//! value of zero, a soft value outside `(0, hard)`, or a non-numeric value refuses startup:
//! clamping a misconfigured capacity would hide the mistake until the first full WAL.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{ensure, Context, Result};

const HARD_ENV: &str = "PERI_WAL_BUDGET_BYTES";
const SOFT_ENV: &str = "PERI_WAL_BUDGET_SOFT_BYTES";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WalBudget {
    hard_bytes: u64,
    soft_bytes: u64,
}

impl WalBudget {
    pub(crate) fn configured() -> Result<Option<Self>> {
        let hard = match std::env::var(HARD_ENV) {
            Ok(value) => Some(
                value
                    .parse::<u64>()
                    .with_context(|| format!("invalid WAL budget {HARD_ENV}"))?,
            ),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(error.into()),
        };
        let Some(hard) = hard else {
            // A soft mark without a capacity has nothing to protect against; treat it as a
            // configuration error rather than silently ignoring half the setting.
            ensure!(
                std::env::var_os(SOFT_ENV).is_none(),
                "WAL budget soft mark requires {HARD_ENV}"
            );
            return Ok(None);
        };
        let soft = match std::env::var(SOFT_ENV) {
            Ok(value) => value
                .parse::<u64>()
                .with_context(|| format!("invalid WAL budget {SOFT_ENV}"))?,
            Err(std::env::VarError::NotPresent) => hard / 5 * 4,
            Err(error) => return Err(error.into()),
        };
        Self::new(hard, soft).map(Some)
    }

    pub(crate) fn new(hard_bytes: u64, soft_bytes: u64) -> Result<Self> {
        ensure!(hard_bytes > 0, "WAL budget capacity must be positive");
        ensure!(
            soft_bytes > 0 && soft_bytes < hard_bytes,
            "WAL budget soft mark must be positive and below the hard capacity"
        );
        Ok(Self {
            hard_bytes,
            soft_bytes,
        })
    }

    pub(crate) fn hard_bytes(self) -> u64 {
        self.hard_bytes
    }

    /// The only refusal rule: the already accepted image (including the pending batch) plus
    /// this write would not fit. A write that rewrites or shrinks the image is not refused,
    /// because it cannot make the log larger.
    pub(crate) fn refuses(self, projected_bytes: u64, current_bytes: u64) -> bool {
        projected_bytes > self.hard_bytes && projected_bytes > current_bytes
    }

    pub(crate) fn pressured(self, bytes: u64) -> bool {
        bytes >= self.soft_bytes
    }
}

/// Process-wide pressure latch. The WAL budget belongs to one database's IO layer, but the
/// metric is a process counter; the latch keeps a write-heavy database from emitting one
/// pressure event per frame.
#[derive(Debug, Default)]
pub(crate) struct PressureLatch {
    pressured: AtomicBool,
}

impl PressureLatch {
    /// Returns true on the transition from calm to pressured, which is the one event the
    /// export needs to count.
    pub(crate) fn observe(&self, pressured: bool) -> bool {
        let was = self.pressured.swap(pressured, Ordering::Relaxed);
        pressured && !was
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_rejects_zero_and_inverted_marks() {
        assert!(WalBudget::new(0, 0).is_err());
        assert!(WalBudget::new(100, 0).is_err());
        assert!(WalBudget::new(100, 100).is_err());
        assert!(WalBudget::new(100, 101).is_err());
        assert_eq!(WalBudget::new(100, 80).unwrap().hard_bytes(), 100);
    }

    #[test]
    fn the_soft_mark_is_pressure_and_the_hard_mark_is_the_only_refusal() {
        let budget = WalBudget::new(100, 80).unwrap();
        assert!(!budget.pressured(79));
        assert!(budget.pressured(80));
        // Growing past the capacity is refused; a shrinking rewrite is not.
        assert!(budget.refuses(101, 100));
        assert!(!budget.refuses(100, 100));
        assert!(!budget.refuses(90, 100));
        assert!(budget.refuses(101, 90));
    }
}
