use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};
use serde::Serialize;

/// One reserved owner slot keeps the published wire profile honest
/// (`owner_sql = 1`) and guarantees forward progress even while general SQL is
/// saturated. Owner slots beyond it are borrowed from idle general capacity.
const OWNER_GUARANTEED: usize = 1;
/// Cleanup keeps its own slice of `sql_total`; owner borrowing must never make
/// a cleanup request wait behind user SQL.
const CLEANUP_SLOTS: usize = 1;
/// Waiting is bounded so a parked request cannot pin a transport thread or the
/// worker-wide execution quota indefinitely.
const MAX_OWNER_WAITERS: usize = 1024;
const MAX_OWNER_WAIT: Duration = Duration::from_secs(10);
const MAX_OWNER_RESERVATION: Duration = Duration::from_secs(3600);
/// Per-database grant counters exist only for reporting; admission fairness is
/// FIFO and does not depend on them. The cap keeps metric memory bounded on a
/// long-lived worker that sees unbounded database churn.
const MAX_REPORTED_DATABASES: usize = 1024;

#[derive(Clone, Copy)]
pub(super) enum Lane {
    Control,
    General,
    Owner,
    Cleanup,
}

impl Lane {
    fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::General => 1,
            Self::Owner => 2,
            Self::Cleanup => 3,
        }
    }
}

#[derive(Clone, Copy, Serialize)]
pub(super) struct LaneUsage {
    pub control: usize,
    pub general_sql: usize,
    pub owner_sql: usize,
    pub cleanup_sql: usize,
    pub control_headroom: usize,
    pub cleanup_headroom: usize,
    pub sql_headroom: usize,
    pub owner: OwnerUsage,
}

/// Writer-profile admission is keyed by database, so the snapshot separates
/// the owner pool from the flat lane counters that the platform already reads.
#[derive(Clone, Copy, Serialize)]
pub(super) struct OwnerUsage {
    pub capacity: usize,
    pub guaranteed: usize,
    pub borrowed: usize,
    pub borrow_total: u64,
    pub waiters: usize,
    pub waits: u64,
    pub wait_max_ms: u64,
    pub wait_total_ms: u64,
    pub rejections: u64,
    pub conflicts: u64,
    pub queue_full: u64,
    pub wait_timeouts: u64,
    pub overheld: usize,
    pub long_holds: u64,
    pub databases: usize,
    pub grants_min: u64,
    pub grants_max: u64,
    pub fairness_gap: u64,
    pub wait_limit_ms: u64,
    pub waiters_limit: usize,
    pub reservation_ms: u64,
}

/// Bounded writer-profile policy. The derived defaults scale with the SQL
/// budget; tests and future callers can supply explicit values.
///
/// `owner_slots` caps concurrent owner requests worker-wide, not per database;
/// each database still holds at most one. A grant is reserved for
/// `owner_reservation` before it is reported as overheld, which makes a long
/// transaction observable without pretending a running statement can be
/// preempted. `owner_wait` bounds how long a queued database can wait, and a
/// queue beyond `owner_waiters` is refused immediately.
#[derive(Clone, Debug)]
pub(super) struct WriterProfileConfig {
    pub owner_slots: usize,
    pub owner_waiters: usize,
    pub owner_wait: Duration,
    pub owner_reservation: Duration,
}

impl WriterProfileConfig {
    fn derived(sql_total: usize) -> Self {
        let borrowable = sql_total - CLEANUP_SLOTS - OWNER_GUARANTEED;
        let owner_slots = (borrowable / 4).clamp(2, 16).min(borrowable);
        Self {
            owner_slots,
            // Waiters already hold an execution-quota slot. Keeping them out
            // of the cleanup reservation leaves both cleanup and control a
            // transport-level slot while the owner pool is saturated.
            owner_waiters: (owner_slots * 4)
                .clamp(8, 256)
                .min(sql_total - CLEANUP_SLOTS),
            owner_wait: Duration::from_millis(250),
            owner_reservation: Duration::from_secs(2),
        }
    }

    fn validate(&self, sql_total: usize) -> Result<()> {
        ensure!(
            sql_total >= CLEANUP_SLOTS + OWNER_GUARANTEED + 2,
            "writer profile requires at least four SQL slots"
        );
        let borrowable = sql_total - CLEANUP_SLOTS - OWNER_GUARANTEED;
        ensure!(
            self.owner_slots >= 2,
            "writer profile requires at least two owner slots"
        );
        ensure!(
            self.owner_slots <= borrowable,
            "writer profile owner slots exceed borrowable general capacity"
        );
        ensure!(
            self.owner_waiters > 0
                && self.owner_waiters <= MAX_OWNER_WAITERS
                && self.owner_waiters <= sql_total - CLEANUP_SLOTS,
            "invalid writer profile owner waiters"
        );
        ensure!(
            !self.owner_wait.is_zero() && self.owner_wait <= MAX_OWNER_WAIT,
            "invalid writer profile owner wait limit"
        );
        ensure!(
            !self.owner_reservation.is_zero() && self.owner_reservation <= MAX_OWNER_RESERVATION,
            "invalid writer profile owner reservation"
        );
        Ok(())
    }
}

#[derive(Default)]
struct State {
    usage: [usize; 4],
    owner: OwnerState,
}

#[derive(Default)]
struct OwnerState {
    active: BTreeMap<Arc<str>, ActiveOwner>,
    waiters: VecDeque<OwnerWaiter>,
    next_waiter: u64,
    stats: BTreeMap<Arc<str>, OwnerStats>,
    counters: OwnerCounters,
}

impl OwnerState {
    fn reject(&mut self, rejection: OwnerRejection) -> OwnerAdmission {
        self.counters.rejections += 1;
        match rejection {
            OwnerRejection::Conflict => self.counters.conflicts += 1,
            OwnerRejection::QueueFull => self.counters.queue_full += 1,
            OwnerRejection::WaitTimeout => self.counters.wait_timeouts += 1,
        }
        OwnerAdmission::Rejected(rejection)
    }
}

struct ActiveOwner {
    granted_at: Instant,
}

struct OwnerWaiter {
    id: u64,
    database: Arc<str>,
    started: Instant,
}

#[derive(Default)]
struct OwnerStats {
    grants: u64,
}

#[derive(Default)]
struct OwnerCounters {
    waits: u64,
    borrow_total: u64,
    rejections: u64,
    conflicts: u64,
    queue_full: u64,
    wait_timeouts: u64,
    long_holds: u64,
    wait_total_ns: u64,
    wait_max_ns: u64,
}

pub(super) struct Admission {
    capacities: [usize; 4],
    sql_total: usize,
    profile: Option<WriterProfileConfig>,
    owner_reservation: Duration,
    state: Mutex<State>,
    owner_changed: Condvar,
}

pub(super) struct LaneTicket {
    admission: Arc<Admission>,
    lane: Lane,
    owner: Option<OwnerTicket>,
}

struct OwnerTicket {
    database: Arc<str>,
}

pub(super) enum OwnerAdmission {
    Acquired(LaneTicket),
    Rejected(OwnerRejection),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerRejection {
    Conflict,
    QueueFull,
    WaitTimeout,
}

impl OwnerRejection {
    /// Every reason keeps the platform's closed refusal vocabulary as a prefix,
    /// so the Go gateway still classifies it as a retryable refusal instead of
    /// an unknown execution outcome.
    pub fn message(self) -> &'static str {
        match self {
            Self::Conflict => {
                "worker execution lane saturated: owner database already has a writer"
            }
            Self::QueueFull => "worker execution lane saturated: owner wait queue full",
            Self::WaitTimeout => "worker execution lane saturated: owner wait limit exceeded",
        }
    }
}

impl Admission {
    pub fn new(max_inflight: usize, control_reserve: usize, enabled: bool) -> Result<Arc<Self>> {
        ensure!(
            control_reserve > 0 && max_inflight > control_reserve,
            "invalid worker lane limits"
        );
        let sql_total = max_inflight - control_reserve;
        let profile = if enabled {
            ensure!(
                sql_total >= CLEANUP_SLOTS + OWNER_GUARANTEED + 2,
                "writer profile requires at least four SQL slots"
            );
            WriterProfileConfig::derived(sql_total)
        } else {
            return Ok(Self::build(max_inflight, control_reserve, None));
        };
        Self::with_writer_profile(max_inflight, control_reserve, profile)
    }

    pub fn with_writer_profile(
        max_inflight: usize,
        control_reserve: usize,
        profile: WriterProfileConfig,
    ) -> Result<Arc<Self>> {
        ensure!(
            control_reserve > 0 && max_inflight > control_reserve,
            "invalid worker lane limits"
        );
        let sql_total = max_inflight - control_reserve;
        profile.validate(sql_total)?;
        Ok(Self::build(max_inflight, control_reserve, Some(profile)))
    }

    fn build(
        max_inflight: usize,
        control_reserve: usize,
        profile: Option<WriterProfileConfig>,
    ) -> Arc<Self> {
        let sql_total = max_inflight - control_reserve;
        let capacities = match &profile {
            Some(_) => [
                control_reserve,
                sql_total - CLEANUP_SLOTS - OWNER_GUARANTEED,
                OWNER_GUARANTEED,
                CLEANUP_SLOTS,
            ],
            None => [max_inflight, sql_total, 0, 0],
        };
        Arc::new(Self {
            capacities,
            sql_total,
            owner_reservation: profile
                .as_ref()
                .map_or(Duration::ZERO, |profile| profile.owner_reservation),
            profile,
            state: Mutex::new(State::default()),
            owner_changed: Condvar::new(),
        })
    }

    pub fn enabled(&self) -> bool {
        self.profile.is_some()
    }

    pub fn try_acquire(self: &Arc<Self>, lane: Lane) -> Result<Option<LaneTicket>> {
        if matches!(lane, Lane::Owner) {
            // An owner permit without a database identity cannot enforce the
            // per-database single-writer rule, so callers must use
            // `acquire_owner` while the profile is enabled.
            ensure!(
                !self.enabled(),
                "owner lane requires a database-scoped acquisition"
            );
            return Ok(None);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lane state poisoned"))?;
        let index = lane.index();
        if state.usage[index] >= self.capacities[index] {
            return Ok(None);
        }
        if self.enabled() && matches!(lane, Lane::General) {
            // Owners may borrow idle general capacity, so the general lane cap
            // alone no longer bounds the aggregate. Keep the cleanup slice out
            // of every non-cleanup admission.
            let general_after = state.usage[Lane::General.index()] + 1;
            if general_after + state.usage[Lane::Owner.index()]
                > self.sql_total.saturating_sub(CLEANUP_SLOTS)
            {
                return Ok(None);
            }
        }
        state.usage[index] += 1;
        Ok(Some(LaneTicket {
            admission: self.clone(),
            lane,
            owner: None,
        }))
    }

    /// Acquires one owner permit for `database`, waiting at most the profile's
    /// owner wait limit (and never past the request deadline). A database with
    /// an owner request in flight, or already queued, is refused immediately so
    /// it cannot occupy two slots or block the fair FIFO head.
    pub fn acquire_owner(
        self: &Arc<Self>,
        database: &str,
        deadline: Instant,
    ) -> Result<OwnerAdmission> {
        let profile = self
            .profile
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("writer profile is disabled"))?;
        ensure!(!database.is_empty(), "owner database key must not be empty");
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lane state poisoned"))?;
        if state.owner.active.contains_key(database) {
            return Ok(state.owner.reject(OwnerRejection::Conflict));
        }
        let started = Instant::now();
        let wait_deadline = deadline.min(started + profile.owner_wait);
        if state.owner.waiters.is_empty() && self.owner_slot_available(&state, profile) {
            return Ok(OwnerAdmission::Acquired(self.grant_owner(
                &mut state,
                Arc::from(database),
                started,
            )));
        }
        if state
            .owner
            .waiters
            .iter()
            .any(|waiter| waiter.database.as_ref() == database)
        {
            return Ok(state.owner.reject(OwnerRejection::Conflict));
        }
        if started >= wait_deadline {
            return Ok(state.owner.reject(OwnerRejection::WaitTimeout));
        }
        if state.owner.waiters.len() >= profile.owner_waiters {
            return Ok(state.owner.reject(OwnerRejection::QueueFull));
        }
        let waiter_id = state.owner.next_waiter;
        state.owner.next_waiter = state.owner.next_waiter.wrapping_add(1);
        state.owner.waiters.push_back(OwnerWaiter {
            id: waiter_id,
            database: Arc::from(database),
            started,
        });
        state.owner.counters.waits += 1;
        loop {
            let head = state.owner.waiters.front().map(|waiter| waiter.id);
            if head == Some(waiter_id) && self.owner_slot_available(&state, profile) {
                let waiter = state
                    .owner
                    .waiters
                    .pop_front()
                    .ok_or_else(|| anyhow::anyhow!("owner wait queue lost its head"))?;
                note_owner_wait(&mut state, waiter.started.elapsed());
                return Ok(OwnerAdmission::Acquired(self.grant_owner(
                    &mut state,
                    waiter.database,
                    Instant::now(),
                )));
            }
            let remaining = wait_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let was_head = head == Some(waiter_id);
                state.owner.waiters.retain(|waiter| waiter.id != waiter_id);
                note_owner_wait(&mut state, started.elapsed());
                let admission = state.owner.reject(OwnerRejection::WaitTimeout);
                if was_head {
                    self.owner_changed.notify_all();
                }
                return Ok(admission);
            }
            let (next, _) = self
                .owner_changed
                .wait_timeout(state, remaining)
                .map_err(|_| anyhow::anyhow!("worker lane state poisoned"))?;
            state = next;
        }
    }

    pub fn snapshot(&self) -> Result<LaneUsage> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lane state poisoned"))?;
        let profile = self.profile.as_ref();
        let owner_capacity = profile.map_or(0, |profile| profile.owner_slots);
        let (grants_min, grants_max, databases) = owner_fairness(&state.owner);
        let overheld = profile.map_or(0, |profile| {
            state
                .owner
                .active
                .values()
                .filter(|active| active.granted_at.elapsed() >= profile.owner_reservation)
                .count()
        });
        let counters = &state.owner.counters;
        Ok(LaneUsage {
            control: state.usage[0],
            general_sql: state.usage[1],
            owner_sql: state.usage[2],
            cleanup_sql: state.usage[3],
            control_headroom: self.capacities[0].saturating_sub(state.usage[0]),
            cleanup_headroom: self.capacities[3].saturating_sub(state.usage[3]),
            sql_headroom: self
                .sql_total
                .saturating_sub(state.usage[1] + state.usage[2] + state.usage[3]),
            owner: OwnerUsage {
                capacity: owner_capacity,
                guaranteed: if profile.is_some() {
                    OWNER_GUARANTEED
                } else {
                    0
                },
                borrowed: state.usage[2].saturating_sub(if profile.is_some() {
                    OWNER_GUARANTEED
                } else {
                    0
                }),
                borrow_total: counters.borrow_total,
                waiters: state.owner.waiters.len(),
                waits: counters.waits,
                wait_max_ms: nanos_to_millis(counters.wait_max_ns),
                wait_total_ms: nanos_to_millis(counters.wait_total_ns),
                rejections: counters.rejections,
                conflicts: counters.conflicts,
                queue_full: counters.queue_full,
                wait_timeouts: counters.wait_timeouts,
                overheld,
                long_holds: counters.long_holds,
                databases,
                grants_min,
                grants_max,
                fairness_gap: grants_max.saturating_sub(grants_min),
                wait_limit_ms: profile.map_or(0, |profile| {
                    profile.owner_wait.as_millis().min(u64::MAX as u128) as u64
                }),
                waiters_limit: profile.map_or(0, |profile| profile.owner_waiters),
                reservation_ms: profile.map_or(0, |profile| {
                    profile.owner_reservation.as_millis().min(u64::MAX as u128) as u64
                }),
            },
        })
    }

    fn owner_slot_available(&self, state: &State, profile: &WriterProfileConfig) -> bool {
        let owner = state.usage[Lane::Owner.index()];
        let general = state.usage[Lane::General.index()];
        // The aggregate cap is `sql_total - CLEANUP_SLOTS`, so cleanup always
        // has a slot regardless of how owners and general SQL interleave.
        owner < profile.owner_slots
            && general + owner < self.sql_total.saturating_sub(CLEANUP_SLOTS)
    }

    fn grant_owner(
        self: &Arc<Self>,
        state: &mut State,
        database: Arc<str>,
        now: Instant,
    ) -> LaneTicket {
        let owner_usage = state.usage[Lane::Owner.index()];
        state.usage[Lane::Owner.index()] += 1;
        state
            .owner
            .active
            .insert(database.clone(), ActiveOwner { granted_at: now });
        if owner_usage >= OWNER_GUARANTEED {
            state.owner.counters.borrow_total += 1;
        }
        let tracked = state.owner.stats.len() < MAX_REPORTED_DATABASES;
        if let Some(stats) = state.owner.stats.get_mut(database.as_ref()) {
            stats.grants += 1;
        } else if tracked {
            state
                .owner
                .stats
                .insert(database.clone(), OwnerStats { grants: 1 });
        }
        LaneTicket {
            admission: self.clone(),
            lane: Lane::Owner,
            owner: Some(OwnerTicket { database }),
        }
    }
}

fn note_owner_wait(state: &mut State, waited: Duration) {
    let nanos = waited.as_nanos().min(u64::MAX as u128) as u64;
    let counters = &mut state.owner.counters;
    counters.wait_total_ns = counters.wait_total_ns.saturating_add(nanos);
    counters.wait_max_ns = counters.wait_max_ns.max(nanos);
}

fn nanos_to_millis(nanos: u64) -> u64 {
    nanos / 1_000_000
}

fn owner_fairness(owner: &OwnerState) -> (u64, u64, usize) {
    let mut min = u64::MAX;
    let mut max = 0;
    for stats in owner.stats.values() {
        min = min.min(stats.grants);
        max = max.max(stats.grants);
    }
    // A queued database that never received a grant is the strongest
    // starvation signal, so it counts as zero even though the ledger is capped.
    if owner
        .waiters
        .iter()
        .any(|waiter| !owner.stats.contains_key(waiter.database.as_ref()))
    {
        min = 0;
    }
    if min == u64::MAX {
        min = 0;
    }
    (min, max, owner.stats.len())
}

impl Drop for LaneTicket {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = self.lane.index();
        assert!(state.usage[index] > 0, "worker lane accounting underflow");
        state.usage[index] -= 1;
        let released_owner = self.owner.is_some();
        if let Some(owner) = self.owner.take() {
            let Some(active) = state.owner.active.remove(owner.database.as_ref()) else {
                panic!("worker owner accounting underflow");
            };
            if active.granted_at.elapsed() >= self.admission.owner_reservation {
                state.owner.counters.long_holds += 1;
            }
        }
        // General and cleanup permits can free the aggregate capacity an owner
        // waiter is borrowing, so every SQL release wakes the owner queue.
        if !state.owner.waiters.is_empty()
            && (released_owner || matches!(self.lane, Lane::General | Lane::Cleanup))
        {
            self.admission.owner_changed.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{
        Admission, Lane, OwnerAdmission, OwnerRejection, WriterProfileConfig, OWNER_GUARANTEED,
    };

    fn profile(
        owner_slots: usize,
        owner_waiters: usize,
        owner_wait: Duration,
        owner_reservation: Duration,
    ) -> WriterProfileConfig {
        WriterProfileConfig {
            owner_slots,
            owner_waiters,
            owner_wait,
            owner_reservation,
        }
    }

    fn enabled(max_inflight: usize, config: WriterProfileConfig) -> anyhow::Result<Arc<Admission>> {
        Admission::with_writer_profile(max_inflight, 1, config)
    }

    fn acquire(
        admission: &Arc<Admission>,
        database: &str,
        wait: Duration,
    ) -> anyhow::Result<OwnerAdmission> {
        admission.acquire_owner(database, Instant::now() + wait)
    }

    fn acquired(admission: &Arc<Admission>, database: &str) -> anyhow::Result<super::LaneTicket> {
        match acquire(admission, database, Duration::from_secs(1))? {
            OwnerAdmission::Acquired(ticket) => Ok(ticket),
            OwnerAdmission::Rejected(rejection) => {
                anyhow::bail!("unexpected owner rejection: {rejection:?}")
            }
        }
    }

    #[test]
    fn disabled_profile_keeps_legacy_capacities() -> anyhow::Result<()> {
        let admission = Admission::new(6, 1, false)?;
        assert!(!admission.enabled());
        let general = (0..5)
            .map(|_| admission.try_acquire(Lane::General).unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(admission.try_acquire(Lane::General)?.is_none());
        assert!(admission.try_acquire(Lane::Owner)?.is_none());
        assert!(admission.try_acquire(Lane::Cleanup)?.is_none());
        let control = admission.try_acquire(Lane::Control)?.unwrap();
        let control_second = admission.try_acquire(Lane::Control)?.unwrap();
        assert!(acquire(&admission, "tenant/db/main", Duration::from_millis(10)).is_err());
        let usage = admission.snapshot()?;
        assert_eq!(usage.general_sql, 5);
        assert_eq!(usage.control, 2);
        assert_eq!(usage.owner.capacity, 0);
        assert_eq!(usage.owner.guaranteed, 0);
        // The disabled profile keeps the legacy worker-wide control capacity,
        // not the reserved control slice used by the writer profile.
        assert_eq!(usage.control_headroom, 4);
        assert_eq!(usage.cleanup_headroom, 0);
        assert_eq!(usage.sql_headroom, 0);
        drop((general, control, control_second));
        let usage = admission.snapshot()?;
        assert_eq!(
            usage.control + usage.general_sql + usage.owner_sql + usage.cleanup_sql,
            0
        );
        Ok(())
    }

    #[test]
    fn writer_profile_configuration_is_validated() -> anyhow::Result<()> {
        assert!(Admission::new(3, 1, true).is_err());
        assert!(Admission::new(4, 1, true).is_err());
        assert!(Admission::new(5, 1, true).is_ok());
        for config in [
            profile(1, 8, Duration::from_millis(10), Duration::from_secs(1)),
            profile(4, 8, Duration::from_millis(10), Duration::from_secs(1)),
            profile(2, 0, Duration::from_millis(10), Duration::from_secs(1)),
            profile(2, 5, Duration::from_millis(10), Duration::from_secs(1)),
            profile(2, 4, Duration::ZERO, Duration::from_secs(1)),
            profile(2, 4, Duration::from_secs(11), Duration::from_secs(1)),
            profile(2, 4, Duration::from_millis(10), Duration::ZERO),
            profile(2, 4, Duration::from_millis(10), Duration::from_secs(3_601)),
            profile(
                2,
                super::MAX_OWNER_WAITERS + 1,
                Duration::from_millis(10),
                Duration::from_secs(1),
            ),
        ] {
            assert!(
                Admission::with_writer_profile(6, 1, config).is_err(),
                "invalid writer profile was accepted"
            );
        }
        let admission = Admission::with_writer_profile(
            6,
            1,
            profile(3, 4, Duration::from_millis(10), Duration::from_secs(1)),
        )?;
        assert_eq!(admission.snapshot()?.owner.capacity, 3);
        Ok(())
    }

    #[test]
    fn owner_pool_borrows_idle_general_but_stays_bounded() -> anyhow::Result<()> {
        let admission = enabled(
            6,
            profile(3, 4, Duration::from_millis(20), Duration::from_secs(1)),
        )?;
        let first = acquired(&admission, "tenant/a/main")?;
        let second = acquired(&admission, "tenant/b/main")?;
        let third = acquired(&admission, "tenant/c/main")?;
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner_sql, 3);
        assert_eq!(usage.owner.guaranteed, OWNER_GUARANTEED);
        assert_eq!(usage.owner.borrowed, 2);
        assert_eq!(usage.owner.borrow_total, 2);
        assert!(matches!(
            acquire(&admission, "tenant/d/main", Duration::from_millis(20))?,
            OwnerAdmission::Rejected(OwnerRejection::WaitTimeout)
        ));
        // The aggregate non-cleanup cap still leaves room for exactly one more
        // general request; the fourth owner request above was refused.
        let general = admission.try_acquire(Lane::General)?.unwrap();
        assert!(admission.try_acquire(Lane::General)?.is_none());
        drop((first, second, third, general));
        assert_eq!(admission.snapshot()?.owner_sql, 0);

        // With general fully occupied, only the guaranteed owner slot is
        // available; the rest of the pool cannot borrow from a busy lane.
        let general = (0..3)
            .map(|_| admission.try_acquire(Lane::General).unwrap().unwrap())
            .collect::<Vec<_>>();
        let reserved = acquired(&admission, "tenant/a/main")?;
        assert!(matches!(
            acquire(&admission, "tenant/b/main", Duration::from_millis(20))?,
            OwnerAdmission::Rejected(OwnerRejection::WaitTimeout)
        ));
        drop(reserved);
        drop(general);
        assert_eq!(admission.snapshot()?.owner.borrowed, 0);
        Ok(())
    }

    #[test]
    fn one_slow_owner_does_not_block_other_databases() -> anyhow::Result<()> {
        let admission = enabled(
            8,
            profile(3, 6, Duration::from_secs(1), Duration::from_secs(1)),
        )?;
        let slow = acquired(&admission, "tenant/slow/main")?;
        for database in ["tenant/b/main", "tenant/c/main", "tenant/d/main"] {
            let ticket = acquired(&admission, database)?;
            drop(ticket);
        }
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner_sql, 1);
        assert!(usage.owner.databases >= 3);
        drop(slow);
        assert_eq!(admission.snapshot()?.owner_sql, 0);
        Ok(())
    }

    #[test]
    fn same_database_never_gets_two_owner_tickets() -> anyhow::Result<()> {
        let admission = enabled(
            6,
            profile(2, 4, Duration::from_millis(50), Duration::from_secs(1)),
        )?;
        let first = acquired(&admission, "tenant/db/main")?;
        assert!(matches!(
            acquire(&admission, "tenant/db/main", Duration::from_secs(1))?,
            OwnerAdmission::Rejected(OwnerRejection::Conflict)
        ));
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner_sql, 1);
        assert_eq!(usage.owner.conflicts, 1);
        drop(first);
        let second = acquired(&admission, "tenant/db/main")?;
        drop(second);
        Ok(())
    }

    #[test]
    fn full_owner_and_general_lanes_preserve_cleanup_and_control() -> anyhow::Result<()> {
        let admission = enabled(
            6,
            profile(2, 4, Duration::from_millis(20), Duration::from_secs(1)),
        )?;
        let owner_a = acquired(&admission, "tenant/a/main")?;
        let owner_b = acquired(&admission, "tenant/b/main")?;
        let general_a = admission.try_acquire(Lane::General)?.unwrap();
        let general_b = admission.try_acquire(Lane::General)?.unwrap();
        assert!(admission.try_acquire(Lane::General)?.is_none());
        assert!(matches!(
            acquire(&admission, "tenant/c/main", Duration::from_millis(20))?,
            OwnerAdmission::Rejected(OwnerRejection::WaitTimeout)
        ));
        let control = admission.try_acquire(Lane::Control)?.unwrap();
        assert!(admission.try_acquire(Lane::Control)?.is_none());
        let cleanup = admission.try_acquire(Lane::Cleanup)?.unwrap();
        assert!(admission.try_acquire(Lane::Cleanup)?.is_none());
        let usage = admission.snapshot()?;
        assert_eq!(usage.control, 1);
        assert_eq!(usage.cleanup_sql, 1);
        assert_eq!(usage.owner_sql, 2);
        assert_eq!(usage.general_sql, 2);
        assert_eq!(usage.sql_headroom, 0);
        assert_eq!(usage.control_headroom, 0);
        assert_eq!(usage.cleanup_headroom, 0);
        drop(cleanup);
        let cleanup = admission.try_acquire(Lane::Cleanup)?.unwrap();
        drop((owner_a, owner_b, general_a, general_b, control, cleanup));
        assert_eq!(admission.snapshot()?.sql_headroom, 5);
        Ok(())
    }

    #[test]
    fn owner_wait_queue_is_fifo_bounded_and_explains_rejections() -> anyhow::Result<()> {
        let admission = enabled(
            6,
            profile(2, 1, Duration::from_millis(500), Duration::from_secs(1)),
        )?;
        let held_a = acquired(&admission, "tenant/a/main")?;
        let held_b = acquired(&admission, "tenant/b/main")?;
        let waiting = admission.clone();
        let waiter =
            thread::spawn(move || acquire(&waiting, "tenant/c/main", Duration::from_millis(500)));
        let deadline = Instant::now() + Duration::from_secs(1);
        while admission.snapshot()?.owner.waiters == 0 {
            assert!(Instant::now() < deadline, "owner waiter did not queue");
            thread::sleep(Duration::from_millis(2));
        }
        assert!(matches!(
            acquire(&admission, "tenant/d/main", Duration::from_secs(1))?,
            OwnerAdmission::Rejected(OwnerRejection::QueueFull)
        ));
        assert!(matches!(
            acquire(&admission, "tenant/c/main", Duration::from_secs(1))?,
            OwnerAdmission::Rejected(OwnerRejection::Conflict)
        ));
        drop(held_a);
        let outcome = waiter.join().expect("owner waiter panicked")?;
        let OwnerAdmission::Acquired(ticket) = outcome else {
            anyhow::bail!("queued owner request was not admitted");
        };
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner.waiters, 0);
        assert_eq!(usage.owner.waits, 1);
        assert_eq!(usage.owner.queue_full, 1);
        assert_eq!(usage.owner.conflicts, 1);
        assert_eq!(usage.owner_sql, 2);
        assert!(matches!(
            acquire(&admission, "tenant/e/main", Duration::from_millis(20))?,
            OwnerAdmission::Rejected(OwnerRejection::WaitTimeout)
        ));
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner.waiters, 0);
        assert_eq!(usage.owner.waits, 2);
        assert_eq!(usage.owner.wait_timeouts, 1);
        assert_eq!(usage.owner_sql, 2);
        drop((ticket, held_b));
        assert_eq!(admission.snapshot()?.owner_sql, 0);
        Ok(())
    }

    #[test]
    fn cancelled_and_timed_out_owner_waits_do_not_leak_capacity() -> anyhow::Result<()> {
        let admission = enabled(
            6,
            profile(2, 4, Duration::from_millis(20), Duration::from_secs(1)),
        )?;
        let first = acquired(&admission, "tenant/a/main")?;
        let second = acquired(&admission, "tenant/b/main")?;
        let waiters = (0..4)
            .map(|index| {
                let admission = admission.clone();
                thread::spawn(move || {
                    acquire(
                        &admission,
                        &format!("tenant/waiting{index}/main"),
                        Duration::from_millis(20),
                    )
                })
            })
            .collect::<Vec<_>>();
        for waiter in waiters {
            assert!(matches!(
                waiter.join().expect("owner waiter panicked")?,
                OwnerAdmission::Rejected(OwnerRejection::WaitTimeout)
            ));
        }
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner_sql, 2);
        assert_eq!(usage.owner.waiters, 0);
        assert_eq!(usage.owner.wait_timeouts, 4);
        drop((first, second));
        assert_eq!(admission.snapshot()?.owner_sql, 0);
        for index in 0..100 {
            let ticket = acquired(&admission, &format!("tenant/churn{index}/main"))?;
            drop(ticket);
        }
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner_sql, 0);
        assert_eq!(usage.owner.waiters, 0);
        Ok(())
    }

    #[test]
    fn owner_reservation_reports_long_holds_without_extra_blocking() -> anyhow::Result<()> {
        let admission = enabled(
            8,
            profile(3, 6, Duration::from_secs(1), Duration::from_millis(10)),
        )?;
        let slow = acquired(&admission, "tenant/slow/main")?;
        thread::sleep(Duration::from_millis(20));
        assert_eq!(admission.snapshot()?.owner.overheld, 1);
        let other = acquired(&admission, "tenant/other/main")?;
        let third = acquired(&admission, "tenant/third/main")?;
        assert_eq!(admission.snapshot()?.owner_sql, 3);
        drop((slow, other, third));
        let usage = admission.snapshot()?;
        assert_eq!(usage.owner.overheld, 0);
        assert!(usage.owner.long_holds >= 1);
        Ok(())
    }

    #[test]
    fn database_matrix_is_fair_and_strictly_bounded() -> anyhow::Result<()> {
        const ROUNDS: usize = 3;
        for databases in [1_usize, 4, 16, 32] {
            let admission = enabled(
                64,
                profile(4, 60, Duration::from_secs(2), Duration::from_secs(1)),
            )?;
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            // With at most one pool slot per database, a barrier proves the
            // bound is reachable rather than merely unbroken.
            let barrier = (databases <= 4).then(|| Arc::new(Barrier::new(databases)));
            let workers = (0..databases)
                .map(|database| {
                    let admission = admission.clone();
                    let active = active.clone();
                    let peak = peak.clone();
                    let barrier = barrier.clone();
                    thread::spawn(move || -> anyhow::Result<()> {
                        for _ in 0..ROUNDS {
                            let ticket =
                                acquired(&admission, &format!("tenant/db{database}/main"))?;
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(current, Ordering::SeqCst);
                            let usage = admission.snapshot()?;
                            assert!(usage.owner_sql <= 4);
                            assert!(usage.owner_sql + usage.general_sql <= 62);
                            if let Some(barrier) = barrier.as_ref() {
                                barrier.wait();
                            }
                            thread::sleep(Duration::from_millis(1));
                            active.fetch_sub(1, Ordering::SeqCst);
                            drop(ticket);
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            for worker in workers {
                worker.join().expect("owner matrix thread panicked")?;
            }
            let usage = admission.snapshot()?;
            assert_eq!(usage.owner_sql, 0, "databases={databases}");
            assert_eq!(usage.owner.waiters, 0, "databases={databases}");
            assert_eq!(usage.owner.databases, databases, "databases={databases}");
            assert!(usage.owner.grants_min >= 1, "databases={databases}");
            assert!(
                usage.owner.fairness_gap <= ROUNDS as u64,
                "databases={databases} gap={}",
                usage.owner.fairness_gap
            );
            let peak = peak.load(Ordering::SeqCst);
            assert!(peak <= 4, "databases={databases} peak={peak}");
            if databases <= 4 {
                assert_eq!(peak, databases, "databases={databases}");
            }
        }
        Ok(())
    }
}
