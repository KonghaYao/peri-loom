use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, ensure, Result};

use super::types::{AccessRole, DatabaseOwnership, LeaseBinding, LeaseClock, OwnershipMode};

pub(super) struct LeaseState {
    pub mode: OwnershipMode,
    pub binding: LeaseBinding,
    pub role: AccessRole,
    pub deadline_ms: u64,
    pub generation: u64,
    pub handles: usize,
    pub opening: bool,
    pub renewing: bool,
    pub detaching: bool,
    pub last_now: u64,
}

pub(super) struct LeaseContext {
    pub state: Mutex<LeaseState>,
    pub clock: Arc<dyn LeaseClock>,
}

impl LeaseContext {
    pub fn lock(&self) -> Result<MutexGuard<'_, LeaseState>> {
        self.state
            .lock()
            .map_err(|_| anyhow!("lease state poisoned"))
    }

    pub fn now_ms(&self) -> Result<u64> {
        let mut state = self.lock()?;
        self.now_locked(&mut state)
    }

    pub fn now_locked(&self, state: &mut LeaseState) -> Result<u64> {
        let now = match self.clock.now_ms() {
            Ok(now) => now,
            Err(error) => {
                state.mode = OwnershipMode::Revoked;
                state.deadline_ms = 0;
                return Err(error.context("local lease clock unavailable"));
            }
        };
        if now < state.last_now {
            state.mode = OwnershipMode::Revoked;
            state.deadline_ms = 0;
            anyhow::bail!("lease clock moved backwards");
        }
        state.last_now = now;
        Ok(now)
    }

    pub fn check(&self, transaction: Option<bool>) -> Result<()> {
        let mut state = self.lock()?;
        let now = self.now_locked(&mut state)?;
        ensure!(
            state.deadline_ms != 0 || state.mode != OwnershipMode::Acquiring,
            "acquire is awaiting authoritative lease validation"
        );
        if now >= state.deadline_ms {
            state.mode = OwnershipMode::Revoked;
        }
        ensure!(
            matches!(
                state.mode,
                OwnershipMode::Acquiring
                    | OwnershipMode::Writer
                    | OwnershipMode::ReadOnly
                    | OwnershipMode::Draining
            ),
            "local ownership revoked or detached"
        );
        if state.mode == OwnershipMode::Draining {
            ensure!(
                transaction != Some(false),
                "draining rejects new transactions"
            );
        }
        Ok(())
    }

    pub fn description(&self) -> Result<DatabaseOwnership> {
        let mut state = self.lock()?;
        let now = self.now_locked(&mut state)?;
        if state.deadline_ms != 0
            && now >= state.deadline_ms
            && state.mode != OwnershipMode::Detached
        {
            state.mode = OwnershipMode::Revoked;
        }
        Ok(DatabaseOwnership {
            database: state.binding.database.clone(),
            mode: state.mode,
            binding: Some(state.binding.clone()),
            epoch: state.binding.epoch,
            remaining_ttl_ms: state.deadline_ms.saturating_sub(now),
        })
    }
}
