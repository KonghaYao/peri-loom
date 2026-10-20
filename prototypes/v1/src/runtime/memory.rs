//! Worker-wide memory accounting.
//!
//! Two classes of memory live in this process and they need different handling:
//!
//! - Controllable allocations are charged before work starts: the resident segment cache
//!   reservation per database, the per-session reserve, and the input/output bytes of
//!   in-flight requests. A charge that does not fit is refused before dispatch with a typed
//!   error, and the charge is returned when the work ends. No SQL has run when the refusal
//!   is produced, so the caller can tell it apart from an execution failure.
//! - Engine-internal memory cannot be interrupted from here: dirty pages, the WAL mirror and
//!   replay window, native TEMP files, scalar copies and allocator overhead. Where a probe
//!   exists it is sampled and reported as residual risk. The enforcement for this class is
//!   process isolation (the cgroup limit) or an engine-level cap such as
//!   `PERI_WAL_BUDGET_BYTES`; it is deliberately not inferred from a SQL return value.
//!
//! The budget is an admission capacity, not an allocator cap. When neither
//! `PERI_WORKER_MEMORY_BUDGET_BYTES` nor a cgroup v2 `memory.max` exists, accounting stays
//! enabled for reporting but nothing is refused: inventing a limit would make admission
//! depend on a number no operator configured. `PERI_WORKER_MEMORY_HEADROOM_BYTES` (or a
//! derived tenth) is held back from the SQL lane so control and cleanup requests keep
//! capacity while SQL is saturated.
//!
//! The existing load, WAL and session settings keep their meaning:
//! `PERI_WORKER_LOAD_MS` and `PERI_WORKER_MAX_CONCURRENT_LOADS` bound cold loads, and
//! `PERI_WAL_BUDGET_BYTES` remains the only WAL write refusal. This module composes with
//! them instead of reimplementing either.

use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use anyhow::{ensure, Context, Result};
use serde::Serialize;

const BUDGET_ENV: &str = "PERI_WORKER_MEMORY_BUDGET_BYTES";
const HEADROOM_ENV: &str = "PERI_WORKER_MEMORY_HEADROOM_BYTES";
const CGROUP_LIMIT_PATH: &str = "/sys/fs/cgroup/memory.max";

/// The cgroup share the worker may admit when no explicit budget is configured. The other
/// half stays with the WAL, SQL work, runtime overhead and the operating system.
const CGROUP_SHARE_DIVISOR: u64 = 2;
/// Control and cleanup headroom derived from the total when no explicit value is set.
const DEFAULT_HEADROOM_DIVISOR: u64 = 10;
const MIN_HEADROOM_BYTES: u64 = 4 * 1024 * 1024;
/// A row payload is accounted by the session budget, but the RPC response is JSON and can
/// expand it (escapes, base64). Reserving half again plus an envelope keeps the in-flight
/// charge an upper bound for the encoded form instead of a lower bound.
const RESULT_ENCODING_NUMERATOR: u64 = 3;
const RESULT_ENCODING_DENOMINATOR: u64 = 2;
const RESULT_ENVELOPE_BYTES: u64 = 64 * 1024;

/// The per-session reserve covers the default pager and its minimum safe 200-page capacity
/// even when SQLite page size is 64 KiB.
pub(crate) const SESSION_RESERVATION_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetLane {
    Control,
    Cleanup,
    Sql,
}

impl BudgetLane {
    fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Cleanup => "cleanup",
            Self::Sql => "sql",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryBudgetSource {
    Environment,
    Cgroup,
    #[default]
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryRefusalReason {
    Resident,
    Inflight,
}

impl MemoryRefusalReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::Inflight => "inflight",
        }
    }
}

/// The last refusal is kept as data, not as a log line, so a status read can explain why
/// admission stopped without replaying logs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MemoryRejection {
    pub reason: MemoryRefusalReason,
    pub lane: BudgetLane,
    pub required_bytes: u64,
    pub available_bytes: u64,
}

/// Typed refusal returned before dispatch. It carries the numbers admission compared, and
/// callers may attach `SqlEvidence::NotExecuted` to make the absence of execution explicit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryBudgetRejection {
    pub reason: MemoryRefusalReason,
    pub lane: BudgetLane,
    pub required_bytes: u64,
    pub available_bytes: u64,
    pub total_bytes: Option<u64>,
}

impl fmt::Display for MemoryBudgetRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "worker memory budget exceeded: reason={} lane={} required_bytes={} available_bytes={} total_bytes={}",
            self.reason.as_str(),
            self.lane.as_str(),
            self.required_bytes,
            self.available_bytes,
            self.total_bytes
                .map_or_else(|| "unconfigured".to_owned(), |bytes| bytes.to_string()),
        )
    }
}

impl std::error::Error for MemoryBudgetRejection {}

/// A reported class of memory the admission path cannot reserve. `enforcement` names what
/// actually bounds it, so a reader never mistakes the sample for a refusal rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UncontrolledMemory {
    pub name: &'static str,
    pub enforcement: &'static str,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResidentUsage {
    pub database_bytes: u64,
    pub session_bytes: u64,
    pub sessions: usize,
}

impl ResidentUsage {
    fn total(self) -> u64 {
        self.database_bytes.saturating_add(self.session_bytes)
    }
}

/// Engine-owned memory sampled from the per-database resource probe. None of it is part of
/// the admitted reservation; it is reported so the gap between accounting and observation is
/// visible instead of silently assumed away.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ObservedUsage {
    pub segment_bytes: u64,
    pub dirty_bytes: u64,
    pub wal_bytes: u64,
    /// Sum of the per-database hard WAL capacities configured by
    /// `PERI_WAL_BUDGET_BYTES`. It is reported, not added to the worker total: each database
    /// owns its WAL capacity and Q05 is the refusal rule for it.
    pub wal_capacity_bytes: u64,
}

impl ObservedUsage {
    fn total(self) -> u64 {
        self.segment_bytes
            .saturating_add(self.dirty_bytes)
            .saturating_add(self.wal_bytes)
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MemoryBudgetStatus {
    pub source: MemoryBudgetSource,
    /// `None` means accounting is report-only: no cgroup limit and no explicit budget.
    pub total_bytes: Option<u64>,
    pub headroom_bytes: u64,
    pub resident_segment_reserved_bytes: u64,
    pub session_reserved_bytes: u64,
    pub inflight_reserved_bytes: u64,
    /// Resident plus in-flight reservations. This is the number admission compares.
    pub used_bytes: u64,
    /// Remaining SQL-lane capacity. Control and cleanup may additionally use `headroom_bytes`.
    pub available_bytes: Option<u64>,
    pub peak_resident_reserved_bytes: u64,
    pub peak_inflight_reserved_bytes: u64,
    pub peak_used_bytes: u64,
    pub peak_sessions: usize,
    pub observed_segment_bytes: Option<u64>,
    pub observed_dirty_bytes: Option<u64>,
    pub observed_wal_bytes: Option<u64>,
    pub observed_wal_capacity_bytes: Option<u64>,
    /// Observed engine bytes minus admitted bytes. Positive values are memory the accounting
    /// does not cover; negative values are reservation slack.
    pub observed_minus_reserved_bytes: Option<i64>,
    /// Observed engine bytes above the configured total. This can be non-zero even when
    /// `over_budget_peak_bytes` is zero; it is the measured residual risk, not an admission
    /// failure.
    pub observed_over_budget_bytes: Option<u64>,
    pub over_budget_peak_bytes: u64,
    pub rejections: u64,
    pub rejected_bytes: u64,
    pub last_rejection: Option<MemoryRejection>,
    pub uncontrolled: Vec<UncontrolledMemory>,
}

/// Process-wide admission ledger. Resident accounting is derived from the registry under its
/// mutex, while in-flight charges are atomics; both paths take the registry lock before they
/// compare against the budget, so a session admission cannot race an in-flight charge.
pub struct MemoryBudget {
    total_bytes: Option<u64>,
    source: MemoryBudgetSource,
    headroom_bytes: u64,
    inflight_bytes: AtomicU64,
    peak_inflight_bytes: AtomicU64,
    peak_resident_bytes: AtomicU64,
    peak_used_bytes: AtomicU64,
    peak_sessions: AtomicU64,
    rejections: AtomicU64,
    rejected_bytes: AtomicU64,
    last_rejection: Mutex<Option<MemoryRejection>>,
}

/// A successful in-flight charge. Dropping it returns the bytes, including on an error path,
/// which is the only release mechanism and therefore cannot be forgotten by a caller.
pub struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: u64,
}

impl MemoryReservation {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.budget
            .inflight_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

impl MemoryBudget {
    /// Reads the explicit budget and headroom, falling back to half of the cgroup v2 limit.
    /// An explicit value always wins; a malformed explicit value fails startup because
    /// clamping it would hide the mistake until the first refusal.
    pub(crate) fn configured() -> Result<Arc<Self>> {
        let explicit_total = env_value(BUDGET_ENV)?;
        let explicit_headroom = env_value(HEADROOM_ENV)?;
        let cgroup = std::fs::read_to_string(CGROUP_LIMIT_PATH).ok();
        Self::resolve(
            explicit_total.as_deref(),
            cgroup.as_deref(),
            explicit_headroom.as_deref(),
        )
    }

    /// Testable configuration resolution. `cgroup_limit` is the raw file content, so a
    /// `max` or malformed value falls back to no limit rather than becoming a fake one.
    pub(crate) fn resolve(
        explicit_total: Option<&str>,
        cgroup_limit: Option<&str>,
        explicit_headroom: Option<&str>,
    ) -> Result<Arc<Self>> {
        let explicit_total = explicit_total
            .map(|raw| parse_bytes(BUDGET_ENV, raw))
            .transpose()?;
        let cgroup_bytes = cgroup_limit.and_then(parse_cgroup_limit);
        let (total, source) = match (explicit_total, cgroup_bytes) {
            (Some(total), _) => (Some(total), MemoryBudgetSource::Environment),
            (None, Some(limit)) => {
                let share = limit / CGROUP_SHARE_DIVISOR;
                ((share > 0).then_some(share), MemoryBudgetSource::Cgroup)
            }
            (None, None) => (None, MemoryBudgetSource::None),
        };
        let headroom = explicit_headroom
            .map(|raw| parse_bytes(HEADROOM_ENV, raw))
            .transpose()?;
        Self::build(total, source, headroom)
    }

    /// Explicit construction for tests.
    #[cfg(test)]
    pub(crate) fn new(total: Option<u64>, headroom: Option<u64>) -> Result<Arc<Self>> {
        let source = if total.is_some() {
            MemoryBudgetSource::Environment
        } else {
            MemoryBudgetSource::None
        };
        Self::build(total, source, headroom)
    }

    fn build(
        total: Option<u64>,
        source: MemoryBudgetSource,
        explicit_headroom: Option<u64>,
    ) -> Result<Arc<Self>> {
        let headroom = match total {
            Some(total) => {
                ensure!(total > 0, "worker memory budget must be positive");
                let headroom = match explicit_headroom {
                    Some(headroom) => headroom,
                    None => (total / DEFAULT_HEADROOM_DIVISOR)
                        .max(MIN_HEADROOM_BYTES)
                        .min(total / 2)
                        .max(1),
                };
                ensure!(
                    headroom > 0 && headroom < total,
                    "worker memory headroom must be positive and below the budget"
                );
                headroom
            }
            None => {
                ensure!(
                    explicit_headroom.is_none(),
                    "worker memory headroom requires {BUDGET_ENV}"
                );
                0
            }
        };
        Ok(Arc::new(Self {
            total_bytes: total,
            source,
            headroom_bytes: headroom,
            inflight_bytes: AtomicU64::new(0),
            peak_inflight_bytes: AtomicU64::new(0),
            peak_resident_bytes: AtomicU64::new(0),
            peak_used_bytes: AtomicU64::new(0),
            peak_sessions: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
            rejected_bytes: AtomicU64::new(0),
            last_rejection: Mutex::new(None),
        }))
    }

    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    #[cfg(test)]
    pub(crate) fn source(&self) -> MemoryBudgetSource {
        self.source
    }

    pub(crate) fn inflight_bytes(&self) -> u64 {
        self.inflight_bytes.load(Ordering::Acquire)
    }

    /// The SQL lane cannot use the headroom; control and cleanup can. This is the only
    /// difference between lanes: the total is the same process-wide capacity.
    fn lane_limit(&self, lane: BudgetLane) -> Option<u64> {
        let total = self.total_bytes?;
        Some(match lane {
            BudgetLane::Control | BudgetLane::Cleanup => total,
            BudgetLane::Sql => total.saturating_sub(self.headroom_bytes),
        })
    }

    /// Checks a resident admission (database segment reserve plus session reserve). The
    /// caller holds the registry lock, so the resident figure cannot move between the
    /// comparison and the admission it authorises.
    pub(crate) fn check_resident(
        &self,
        lane: BudgetLane,
        resident: ResidentUsage,
    ) -> std::result::Result<(), MemoryBudgetRejection> {
        self.observe_resident(resident);
        let inflight = self.inflight_bytes();
        let used = resident.total().saturating_add(inflight);
        self.observe_used(used);
        if let Some(limit) = self.lane_limit(lane) {
            if used > limit {
                return Err(self.reject(
                    MemoryRefusalReason::Resident,
                    lane,
                    resident.total(),
                    limit.saturating_sub(inflight),
                ));
            }
        }
        Ok(())
    }

    /// Charges the input and output bytes of one in-flight request. Must be called while the
    /// caller holds the same registry lock that `check_resident` uses; that serialises the
    /// comparison against concurrent session admissions.
    pub(crate) fn reserve_inflight(
        self: &Arc<Self>,
        lane: BudgetLane,
        bytes: u64,
        resident: ResidentUsage,
    ) -> std::result::Result<MemoryReservation, MemoryBudgetRejection> {
        self.observe_resident(resident);
        let current = self.inflight_bytes();
        if let Some(limit) = self.lane_limit(lane) {
            let used = resident
                .total()
                .saturating_add(current)
                .saturating_add(bytes);
            if used > limit {
                let available = limit
                    .saturating_sub(resident.total())
                    .saturating_sub(current);
                return Err(self.reject(MemoryRefusalReason::Inflight, lane, bytes, available));
            }
        }
        let previous = self.inflight_bytes.fetch_add(bytes, Ordering::AcqRel);
        let next = previous.saturating_add(bytes);
        self.peak_inflight_bytes.fetch_max(next, Ordering::Relaxed);
        self.observe_used(resident.total().saturating_add(next));
        Ok(MemoryReservation {
            budget: self.clone(),
            bytes,
        })
    }

    pub(crate) fn status(
        &self,
        resident: ResidentUsage,
        session_peak: usize,
        observed: Option<ObservedUsage>,
    ) -> MemoryBudgetStatus {
        self.observe_resident(resident);
        self.peak_sessions
            .fetch_max(session_peak as u64, Ordering::Relaxed);
        let inflight = self.inflight_bytes();
        let used = resident.total().saturating_add(inflight);
        self.observe_used(used);
        let available = self
            .lane_limit(BudgetLane::Sql)
            .map(|limit| limit.saturating_sub(used));
        let observed_total = observed.map(ObservedUsage::total);
        let difference = observed_total.map(|observed| signed_difference(observed, used));
        let observed_over_budget = observed_total
            .zip(self.total_bytes)
            .map(|(observed, total)| observed.saturating_sub(total));
        MemoryBudgetStatus {
            source: self.source,
            total_bytes: self.total_bytes,
            headroom_bytes: self.headroom_bytes,
            resident_segment_reserved_bytes: resident.database_bytes,
            session_reserved_bytes: resident.session_bytes,
            inflight_reserved_bytes: inflight,
            used_bytes: used,
            available_bytes: available,
            peak_resident_reserved_bytes: self.peak_resident_bytes.load(Ordering::Relaxed),
            peak_inflight_reserved_bytes: self.peak_inflight_bytes.load(Ordering::Relaxed),
            peak_used_bytes: self.peak_used_bytes.load(Ordering::Relaxed),
            peak_sessions: self.peak_sessions.load(Ordering::Relaxed) as usize,
            observed_segment_bytes: observed.map(|usage| usage.segment_bytes),
            observed_dirty_bytes: observed.map(|usage| usage.dirty_bytes),
            observed_wal_bytes: observed.map(|usage| usage.wal_bytes),
            observed_wal_capacity_bytes: observed.map(|usage| usage.wal_capacity_bytes),
            observed_minus_reserved_bytes: difference,
            observed_over_budget_bytes: observed_over_budget,
            over_budget_peak_bytes: self.total_bytes.map_or(0, |total| {
                self.peak_used_bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(total)
            }),
            rejections: self.rejections.load(Ordering::Relaxed),
            rejected_bytes: self.rejected_bytes.load(Ordering::Relaxed),
            last_rejection: self
                .last_rejection
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            uncontrolled: uncontrolled_memory(),
        }
    }

    fn observe_resident(&self, resident: ResidentUsage) {
        self.peak_resident_bytes
            .fetch_max(resident.total(), Ordering::Relaxed);
        self.peak_sessions
            .fetch_max(resident.sessions as u64, Ordering::Relaxed);
    }

    fn observe_used(&self, used: u64) {
        self.peak_used_bytes.fetch_max(used, Ordering::Relaxed);
    }

    fn reject(
        &self,
        reason: MemoryRefusalReason,
        lane: BudgetLane,
        required_bytes: u64,
        available_bytes: u64,
    ) -> MemoryBudgetRejection {
        self.rejections.fetch_add(1, Ordering::Relaxed);
        self.rejected_bytes
            .fetch_add(required_bytes, Ordering::Relaxed);
        *self
            .last_rejection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(MemoryRejection {
            reason,
            lane,
            required_bytes,
            available_bytes,
        });
        MemoryBudgetRejection {
            reason,
            lane,
            required_bytes,
            available_bytes,
            total_bytes: self.total_bytes,
        }
    }
}

/// The resident charge for `databases` segment caches and `sessions` session reserves.
/// Checked arithmetic keeps an absurd configured limit from wrapping into a small number.
pub(crate) fn reservation_usage(
    databases: usize,
    sessions: usize,
    segment_ram_bytes: usize,
) -> Result<ResidentUsage> {
    let database_bytes = (databases as u64)
        .checked_mul(segment_ram_bytes as u64)
        .context("resident segment reservation overflow")?;
    let session_bytes = (sessions as u64)
        .checked_mul(SESSION_RESERVATION_BYTES)
        .context("resident session reservation overflow")?;
    Ok(ResidentUsage {
        database_bytes,
        session_bytes,
        sessions,
    })
}

/// Upper bound for one row-bearing response: the session result budget, the JSON expansion
/// allowance, and the envelope. Reserve this before dispatch; the actual payload is checked
/// against the session budget while rows are collected.
pub(crate) fn result_reservation_bytes(max_result_bytes: usize) -> u64 {
    (max_result_bytes as u64).saturating_mul(RESULT_ENCODING_NUMERATOR)
        / RESULT_ENCODING_DENOMINATOR
        + RESULT_ENVELOPE_BYTES
}

fn env_value(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("invalid {name}")),
    }
}

fn parse_bytes(name: &str, raw: &str) -> Result<u64> {
    let value = raw
        .trim()
        .parse::<u64>()
        .with_context(|| format!("invalid {name}"))?;
    ensure!(value > 0, "invalid {name}: value must be positive");
    Ok(value)
}

fn parse_cgroup_limit(raw: &str) -> Option<u64> {
    let value = raw.trim();
    if value == "max" {
        return None;
    }
    value.parse::<u64>().ok().filter(|limit| *limit > 0)
}

/// `observed - accounted` clamped to `i64`. The clamp only matters for a nonsensical probe;
/// it keeps the status arithmetic from wrapping into a sign flip.
fn signed_difference(observed: u64, accounted: u64) -> i64 {
    let difference = observed as i128 - accounted as i128;
    difference.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn uncontrolled_memory() -> Vec<UncontrolledMemory> {
    vec![
        UncontrolledMemory {
            name: "dirty_pages",
            enforcement: "sampled and reported; held by the storage publication, not reservable before write",
        },
        UncontrolledMemory {
            name: "wal_mirror_and_replay",
            enforcement: "PERI_WAL_BUDGET_BYTES refuses writes before dispatch when configured; otherwise process isolation",
        },
        UncontrolledMemory {
            name: "temp_workspace",
            enforcement: "native TEMP files use host disk rather than the worker heap; process isolation is the bound",
        },
        UncontrolledMemory {
            name: "large_scalars",
            enforcement: "PERI_SQL_MAX_VALUE_BYTES bounds one blob/value; copies inside the engine remain engine-level",
        },
        UncontrolledMemory {
            name: "allocator_overhead",
            enforcement: "not observable from inside the process; the cgroup OOM kill is the final bound",
        },
        UncontrolledMemory {
            name: "engine_request_workspace",
            enforcement: "input and result reservations bound the request payload; native planner and VM workspace remain engine-level",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        reservation_usage, BudgetLane, MemoryBudget, MemoryBudgetRejection, MemoryBudgetSource,
        MemoryRefusalReason, ObservedUsage, ResidentUsage, SESSION_RESERVATION_BYTES,
    };
    use crate::namespace::Namespace;
    use crate::object_store::MemoryObjectStore;
    use crate::provider::LocalAuthorityProvider;
    use crate::runtime::{DatabaseId, WorkerLimits, WorkerRuntime};
    use crate::session::SqlEvidence;
    use std::sync::Arc;

    const MIB: u64 = 1024 * 1024;

    fn resident(databases: u64, sessions: u64) -> ResidentUsage {
        ResidentUsage {
            database_bytes: databases * 8 * MIB,
            session_bytes: sessions * SESSION_RESERVATION_BYTES,
            sessions: sessions as usize,
        }
    }

    #[test]
    fn reservation_usage_checks_overflow_and_matches_the_old_half_cgroup_rule() {
        let usage = reservation_usage(10, 11, 8 * MIB as usize).unwrap();
        assert_eq!(usage.total(), 256 * MIB);
        assert!(usage.total() <= 512 * MIB / 2);
        assert!(reservation_usage(usize::MAX, 2, 8 * MIB as usize).is_err());
    }

    #[test]
    fn configuration_is_validated_and_absent_limits_are_not_invented() {
        let explicit = MemoryBudget::resolve(Some("536870912"), Some("1073741824"), None).unwrap();
        assert_eq!(explicit.total_bytes(), Some(512 * MIB));
        assert_eq!(explicit.source(), MemoryBudgetSource::Environment);

        let cgroup = MemoryBudget::resolve(None, Some("1073741824\n"), None).unwrap();
        assert_eq!(cgroup.total_bytes(), Some(512 * MIB));
        assert_eq!(cgroup.source(), MemoryBudgetSource::Cgroup);

        let none = MemoryBudget::resolve(None, Some("max\n"), None).unwrap();
        assert_eq!(none.total_bytes(), None);
        assert_eq!(none.source(), MemoryBudgetSource::None);

        for (total, headroom) in [
            (Some("0"), None),
            (Some("not-a-number"), None),
            (Some("1048576"), Some("1048576")),
            (None, Some("1048576")),
        ] {
            assert!(
                MemoryBudget::resolve(total, None, headroom).is_err(),
                "invalid configuration must fail startup: total={total:?} headroom={headroom:?}"
            );
        }
    }

    #[test]
    fn inflight_refusal_is_typed_and_releases_its_capacity() {
        let budget = MemoryBudget::new(Some(32 * MIB), Some(4 * MIB)).unwrap();
        let charge = 28 * MIB as usize;
        let first = budget
            .reserve_inflight(BudgetLane::Sql, charge as u64, ResidentUsage::default())
            .expect("the whole SQL limit fits");
        let rejection = budget
            .reserve_inflight(BudgetLane::Sql, 1, ResidentUsage::default())
            .err()
            .expect("one byte over the limit is refused");
        assert_eq!(rejection.reason, MemoryRefusalReason::Inflight);
        assert_eq!(rejection.available_bytes, 0);
        assert_eq!(budget.inflight_bytes(), charge as u64);
        drop(first);
        assert_eq!(budget.inflight_bytes(), 0);
        let replacement = budget
            .reserve_inflight(BudgetLane::Sql, charge as u64, ResidentUsage::default())
            .expect("dropping the reservation returns the capacity");
        drop(replacement);
    }

    #[test]
    fn control_and_cleanup_keep_headroom_when_sql_is_saturated() {
        let budget = MemoryBudget::new(Some(32 * MIB), Some(4 * MIB)).unwrap();
        let sql = budget
            .reserve_inflight(BudgetLane::Sql, 28 * MIB, ResidentUsage::default())
            .unwrap();
        assert!(budget
            .reserve_inflight(BudgetLane::Sql, 1, ResidentUsage::default())
            .is_err());
        let control = budget
            .reserve_inflight(BudgetLane::Control, 4 * MIB, ResidentUsage::default())
            .expect("control uses the headroom");
        assert!(budget
            .reserve_inflight(BudgetLane::Cleanup, 1, ResidentUsage::default())
            .is_err());
        drop(control);
        let cleanup = budget
            .reserve_inflight(BudgetLane::Cleanup, 4 * MIB, ResidentUsage::default())
            .expect("cleanup uses the headroom after control returns");
        drop(cleanup);
        drop(sql);
    }

    #[test]
    fn peaks_stay_inside_512m_and_2g_budgets_without_leaking_reservations() {
        for total_mib in [512, 2048] {
            let total = total_mib * MIB;
            let budget = MemoryBudget::new(Some(total), None).unwrap();
            let resident = resident(8, 16);
            let request = 6 * MIB;
            let mut held = Vec::new();
            loop {
                match budget.reserve_inflight(BudgetLane::Sql, request, resident) {
                    Ok(charge) => held.push(charge),
                    Err(rejection) => {
                        assert_eq!(rejection.reason, MemoryRefusalReason::Inflight);
                        break;
                    }
                }
            }
            let status = budget.status(
                resident,
                resident.sessions,
                Some(ObservedUsage {
                    segment_bytes: resident.database_bytes,
                    dirty_bytes: 8 * MIB,
                    wal_bytes: 16 * MIB,
                    wal_capacity_bytes: 64 * MIB,
                }),
            );
            assert!(status.used_bytes <= total);
            assert!(status.peak_used_bytes <= total);
            assert_eq!(status.over_budget_peak_bytes, 0);
            assert!(status.rejections >= 1);
            assert!(status.available_bytes.unwrap() < request);
            assert!(status.observed_minus_reserved_bytes.is_some());
            assert_eq!(status.peak_sessions, resident.sessions);
            drop(held);
            assert_eq!(budget.inflight_bytes(), 0);
            let recovered = budget.status(resident, resident.sessions, None);
            assert_eq!(recovered.inflight_reserved_bytes, 0);
            assert!(recovered.available_bytes.unwrap() >= request);
        }
    }

    #[test]
    fn an_unconfigured_budget_reports_peaks_without_refusing() {
        let budget = MemoryBudget::new(None, None).unwrap();
        let resident = resident(2, 3);
        let _first = budget
            .reserve_inflight(BudgetLane::Sql, 128 * MIB, resident)
            .expect("no configured limit means report-only");
        let _second = budget
            .reserve_inflight(BudgetLane::Sql, 128 * MIB, resident)
            .expect("report-only admission never refuses");
        let status = budget.status(resident, resident.sessions, None);
        assert_eq!(status.total_bytes, None);
        assert_eq!(status.available_bytes, None);
        assert_eq!(status.peak_used_bytes, resident.total() + 256 * MIB);
        assert_eq!(status.rejections, 0);
        assert_eq!(status.source, MemoryBudgetSource::None);
    }

    #[test]
    fn runtime_rejects_an_over_budget_open_and_recovers_after_release() -> anyhow::Result<()> {
        let cache = tempfile::tempdir()?;
        let provider = Arc::new(LocalAuthorityProvider::new(Arc::new(
            MemoryObjectStore::default(),
        )));
        let mut runtime = WorkerRuntime::new(provider, cache.path(), WorkerLimits::default())?;
        Arc::get_mut(&mut runtime).unwrap().memory =
            MemoryBudget::new(Some(40 * MIB), Some(4 * MIB))?;
        let database = DatabaseId::new(Namespace::new("memory", "admission")?, "main")?;
        let first = runtime.open(database.clone(), 0)?;
        let error = runtime
            .open(database.clone(), 0)
            .err()
            .expect("the second session exceeds the budget");
        assert!(error.downcast_ref::<MemoryBudgetRejection>().is_some());
        assert_eq!(
            error.downcast_ref::<SqlEvidence>(),
            Some(&SqlEvidence::NotExecuted)
        );
        let status = runtime.status_and_memory()?.1;
        assert_eq!(
            status.last_rejection.as_ref().unwrap().reason,
            MemoryRefusalReason::Resident
        );
        assert_eq!(status.over_budget_peak_bytes, 0);
        assert!(status.resident_segment_reserved_bytes >= 8 * MIB);
        assert!(status.session_reserved_bytes >= SESSION_RESERVATION_BYTES);
        assert!(status.observed_segment_bytes.is_some());
        assert!(!status.uncontrolled.is_empty());
        assert_eq!(status.source, MemoryBudgetSource::Environment);
        let value = serde_json::to_value(&status)?;
        assert_eq!(value["source"], "environment");
        assert_eq!(value["total_bytes"], 40 * MIB);
        assert!(value["last_rejection"]["reason"].is_string());
        assert!(value["uncontrolled"].is_array());
        drop(first);
        let replacement = runtime.open(database, 0)?;
        drop(replacement);
        Ok(())
    }

    #[test]
    fn runtime_inflight_refusal_is_typed_and_returns_capacity() -> anyhow::Result<()> {
        let cache = tempfile::tempdir()?;
        let provider = Arc::new(LocalAuthorityProvider::new(Arc::new(
            MemoryObjectStore::default(),
        )));
        let mut runtime = WorkerRuntime::new(provider, cache.path(), WorkerLimits::default())?;
        Arc::get_mut(&mut runtime).unwrap().memory =
            MemoryBudget::new(Some(32 * MIB), Some(4 * MIB))?;
        let charge = runtime.reserve_inflight(BudgetLane::Sql, 28 * MIB as usize)?;
        let error = runtime
            .reserve_inflight(BudgetLane::Sql, 1)
            .err()
            .expect("one byte over the limit is refused");
        assert!(error.downcast_ref::<MemoryBudgetRejection>().is_some());
        assert_eq!(
            error.downcast_ref::<SqlEvidence>(),
            Some(&SqlEvidence::NotExecuted)
        );
        drop(charge);
        let replacement = runtime.reserve_inflight(BudgetLane::Sql, 28 * MIB as usize)?;
        assert_eq!(replacement.bytes(), 28 * MIB);
        let status = runtime.status_and_memory()?.1;
        assert_eq!(status.total_bytes, Some(32 * MIB));
        assert_eq!(status.rejections, 1);
        assert!(status.peak_inflight_reserved_bytes >= 28 * MIB);
        Ok(())
    }
}
