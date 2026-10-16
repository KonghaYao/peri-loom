//! The budget every maintenance unit is charged against.
//!
//! W4 asks for a per-database concurrency limit plus process-wide concurrency, bandwidth and
//! request budgets. All four live here, and they are checked *before* a unit starts:
//!
//! - `per_database` is a hard cap on how many units one database may have in flight. It is
//!   the same number for every database and the map that tracks it is pruned when a count
//!   returns to zero, so it does not grow with the number of databases ever seen.
//! - `global` caps units in flight across the process.
//! - `requests_per_window` caps how many units may start inside one window.
//! - `bytes_per_window` caps the backfill volume charged inside one window.
//!
//! CPU is not metered: the global concurrency cap is what bounds it, because one unit in
//! flight is what bounds the checkpoint and compaction work a process can be doing at once.
//! A CPU-time meter would need a timer in the data path and the T0 note already rejected
//! frame-level timing for the same reason.
//!
//! A refusal is a *skip*. There is no partial path: a unit that cannot be charged in full
//! does not start, and the objects it would have reclaimed stay where they are.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BudgetDenied {
    /// The process already has the maximum number of maintenance units in flight.
    GlobalConcurrency,
    /// This database already has its maximum number of units in flight.
    PerDatabase,
    /// The window's request budget is spent.
    Requests,
    /// The window's backfill byte budget is spent.
    Bytes,
}

impl BudgetDenied {
    /// Declaration order, which is also the counter order the export uses. The test in
    /// `maintenance/tests.rs` pins each variant's discriminant against its position.
    pub(crate) const ALL: [Self; 4] = [
        Self::GlobalConcurrency,
        Self::PerDatabase,
        Self::Requests,
        Self::Bytes,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::GlobalConcurrency => "global_concurrency",
            Self::PerDatabase => "per_database",
            Self::Requests => "requests",
            Self::Bytes => "bytes",
        }
    }
}

#[derive(Debug)]
struct State {
    in_flight: usize,
    per_database: BTreeMap<String, usize>,
    window_started: Instant,
    window_requests: u64,
    window_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct MaintenanceBudget {
    per_database: usize,
    global: usize,
    requests_per_window: u64,
    bytes_per_window: u64,
    window: Duration,
    state: Arc<Mutex<State>>,
}

impl MaintenanceBudget {
    /// Defaults are uncalibrated and deliberately small: one unit in flight process-wide, one
    /// per database, and a request budget that admits a handful of units per minute. The
    /// point of the default is that enabling the scheduler cannot turn into a maintenance
    /// storm while the watermarks are still unmeasured.
    pub(crate) fn configured() -> anyhow::Result<Self> {
        use anyhow::Context;

        let read = |name: &str, default: u64| -> anyhow::Result<u64> {
            match std::env::var(name) {
                Ok(value) => value
                    .parse::<u64>()
                    .with_context(|| format!("invalid maintenance budget {name}")),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(error) => Err(error.into()),
            }
        };
        Ok(Self::new(
            read("PERI_MAINTENANCE_PER_DATABASE_CONCURRENCY", 1)? as usize,
            read("PERI_MAINTENANCE_GLOBAL_CONCURRENCY", 1)? as usize,
            read("PERI_MAINTENANCE_REQUESTS_PER_WINDOW", 4)?,
            read("PERI_MAINTENANCE_BYTES_PER_WINDOW", 1024 * 1024 * 1024)?,
            Duration::from_secs(60),
        ))
    }

    pub(crate) fn new(
        per_database: usize,
        global: usize,
        requests_per_window: u64,
        bytes_per_window: u64,
        window: Duration,
    ) -> Self {
        Self {
            per_database: per_database.max(1),
            global: global.max(1),
            requests_per_window,
            bytes_per_window,
            window,
            state: Arc::new(Mutex::new(State {
                in_flight: 0,
                per_database: BTreeMap::new(),
                window_started: Instant::now(),
                window_requests: 0,
                window_bytes: 0,
            })),
        }
    }

    /// Charges one unit. `database` is the namespace string, which is also the ledger key, so
    /// per-database accounting cannot be bypassed by opening the same database twice.
    pub(crate) fn acquire(
        &self,
        database: &str,
        bytes: u64,
        now: Instant,
    ) -> Result<MaintenanceLease, BudgetDenied> {
        let mut state = self.lock();
        if now.saturating_duration_since(state.window_started) >= self.window {
            state.window_started = now;
            state.window_requests = 0;
            state.window_bytes = 0;
        }
        if state.in_flight >= self.global {
            return Err(BudgetDenied::GlobalConcurrency);
        }
        if state.per_database.get(database).copied().unwrap_or(0) >= self.per_database {
            return Err(BudgetDenied::PerDatabase);
        }
        if state.window_requests >= self.requests_per_window {
            return Err(BudgetDenied::Requests);
        }
        if state.window_bytes.saturating_add(bytes) > self.bytes_per_window {
            return Err(BudgetDenied::Bytes);
        }
        state.in_flight += 1;
        *state.per_database.entry(database.to_owned()).or_insert(0) += 1;
        state.window_requests += 1;
        state.window_bytes = state.window_bytes.saturating_add(bytes);
        Ok(MaintenanceLease {
            budget: self.clone(),
            database: database.to_owned(),
        })
    }

    fn release(&self, database: &str) {
        let mut state = self.lock();
        state.in_flight = state.in_flight.saturating_sub(1);
        if let Some(count) = state.per_database.get_mut(database) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_database.remove(database);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.lock().in_flight
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Holds one unit's charge. Dropping it returns the concurrency slot and the per-database
/// count; the window counters are not returned, because they are a rate, not a level.
#[derive(Debug)]
pub(crate) struct MaintenanceLease {
    budget: MaintenanceBudget,
    database: String,
}

impl Drop for MaintenanceLease {
    fn drop(&mut self) {
        self.budget.release(&self.database);
    }
}
