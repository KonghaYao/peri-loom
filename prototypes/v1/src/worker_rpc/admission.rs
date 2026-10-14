use std::sync::{Arc, Mutex};

use anyhow::{ensure, Result};
use serde::Serialize;

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
}

pub(super) struct Admission {
    capacities: [usize; 4],
    usage: Mutex<[usize; 4]>,
    enabled: bool,
}

pub(super) struct LaneTicket {
    admission: Arc<Admission>,
    lane: Lane,
}

impl Admission {
    pub fn new(max_inflight: usize, control_reserve: usize, enabled: bool) -> Result<Arc<Self>> {
        ensure!(
            control_reserve > 0 && max_inflight > control_reserve,
            "invalid worker lane limits"
        );
        let sql_total = max_inflight - control_reserve;
        ensure!(
            !enabled || sql_total >= 3,
            "writer profile requires three SQL slots"
        );
        let capacities = if enabled {
            [control_reserve, sql_total - 2, 1, 1]
        } else {
            [max_inflight, sql_total, 0, 0]
        };
        Ok(Arc::new(Self {
            capacities,
            usage: Mutex::new([0; 4]),
            enabled,
        }))
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn try_acquire(self: &Arc<Self>, lane: Lane) -> Result<Option<LaneTicket>> {
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lane state poisoned"))?;
        let index = lane.index();
        if usage[index] >= self.capacities[index] {
            return Ok(None);
        }
        usage[index] += 1;
        Ok(Some(LaneTicket {
            admission: self.clone(),
            lane,
        }))
    }

    pub fn snapshot(&self) -> Result<LaneUsage> {
        let usage = self
            .usage
            .lock()
            .map_err(|_| anyhow::anyhow!("worker lane state poisoned"))?;
        Ok(LaneUsage {
            control: usage[0],
            general_sql: usage[1],
            owner_sql: usage[2],
            cleanup_sql: usage[3],
        })
    }
}

impl Drop for LaneTicket {
    fn drop(&mut self) {
        let mut usage = self
            .admission
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = self.lane.index();
        assert!(usage[index] > 0, "worker lane accounting underflow");
        usage[index] -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{Admission, Lane};

    #[test]
    fn general_saturation_cannot_borrow_owner_cleanup_or_control() -> anyhow::Result<()> {
        let admission = Admission::new(6, 1, true)?;
        let general = (0..3)
            .map(|_| admission.try_acquire(Lane::General).unwrap().unwrap())
            .collect::<Vec<_>>();
        assert!(admission.try_acquire(Lane::General)?.is_none());
        let owner = admission.try_acquire(Lane::Owner)?.unwrap();
        let cleanup = admission.try_acquire(Lane::Cleanup)?.unwrap();
        let control = admission.try_acquire(Lane::Control)?.unwrap();
        assert!(admission.try_acquire(Lane::Owner)?.is_none());
        assert!(admission.try_acquire(Lane::Cleanup)?.is_none());
        assert!(admission.try_acquire(Lane::Control)?.is_none());
        let usage = admission.snapshot()?;
        assert_eq!(usage.general_sql + usage.owner_sql + usage.cleanup_sql, 5);
        assert_eq!(usage.control, 1);
        drop((general, owner, cleanup, control));
        let usage = admission.snapshot()?;
        assert_eq!(
            usage.control + usage.general_sql + usage.owner_sql + usage.cleanup_sql,
            0
        );
        Ok(())
    }

    #[test]
    fn profile_minimum_and_legacy_capacity_are_explicit() -> anyhow::Result<()> {
        assert!(Admission::new(3, 1, true).is_err());
        let admission = Admission::new(3, 1, false)?;
        assert!(!admission.enabled());
        let first = admission.try_acquire(Lane::General)?.unwrap();
        let second = admission.try_acquire(Lane::General)?.unwrap();
        assert!(admission.try_acquire(Lane::General)?.is_none());
        assert!(admission.try_acquire(Lane::Owner)?.is_none());
        assert!(admission.try_acquire(Lane::Cleanup)?.is_none());
        drop((first, second));
        Ok(())
    }
}
