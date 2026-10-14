use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};

use crate::authority_rpc::provider::RemoteAuthorityProvider;
use crate::runtime::{DatabaseId, WorkerLimits, WorkerRuntime};

use super::context::{LeaseContext, LeaseState};
use super::handle::OwnedHandle;
use super::provider::{ContextProvider, Contexts};
use super::types::{
    validate_label, AccessRole, DatabaseOwnership, GrantReply, LeaseClock, OwnershipCommand,
    OwnershipMode, OwnershipWorkerState,
};

const DEADLINE_MARGIN_MS: u64 = 5;

pub struct OwnershipRuntime {
    worker_id: String,
    provider: Arc<RemoteAuthorityProvider>,
    worker: Arc<WorkerRuntime>,
    contexts: Contexts,
    clock: Arc<dyn LeaseClock>,
}

impl OwnershipRuntime {
    pub fn new(
        worker_id: String,
        provider: Arc<RemoteAuthorityProvider>,
        cache_root: impl AsRef<Path>,
        limits: WorkerLimits,
        clock: Arc<dyn LeaseClock>,
    ) -> Result<Arc<Self>> {
        Self::configured(worker_id, provider, cache_root, limits, clock, None)
    }

    pub fn new_with_key_provider(
        worker_id: String,
        provider: Arc<RemoteAuthorityProvider>,
        cache_root: impl AsRef<Path>,
        limits: WorkerLimits,
        clock: Arc<dyn LeaseClock>,
        key_provider: Arc<dyn crate::encryption::DatabaseKeyProvider>,
    ) -> Result<Arc<Self>> {
        Self::configured(
            worker_id,
            provider,
            cache_root,
            limits,
            clock,
            Some(key_provider),
        )
    }

    fn configured(
        worker_id: String,
        provider: Arc<RemoteAuthorityProvider>,
        cache_root: impl AsRef<Path>,
        limits: WorkerLimits,
        clock: Arc<dyn LeaseClock>,
        key_provider: Option<Arc<dyn crate::encryption::DatabaseKeyProvider>>,
    ) -> Result<Arc<Self>> {
        validate_label(&worker_id)?;
        let contexts = Arc::new(Mutex::new(BTreeMap::new()));
        let guarded = Arc::new(ContextProvider {
            remote: provider.clone(),
            contexts: contexts.clone(),
        });
        let worker = match key_provider {
            Some(keys) => WorkerRuntime::new_with_key_provider(guarded, cache_root, limits, keys)?,
            None => WorkerRuntime::new(guarded, cache_root, limits)?,
        };
        Ok(Arc::new(Self {
            worker_id,
            provider,
            worker,
            contexts,
            clock,
        }))
    }

    pub fn command(self: &Arc<Self>, command: OwnershipCommand) -> Result<Value> {
        match command {
            OwnershipCommand::AcquireDatabase { lease, role } => {
                self.acquire(lease, role)?;
                Ok(json!({"acquired": true}))
            }
            OwnershipCommand::RenewLease { lease } => {
                self.renew(lease)?;
                Ok(json!({"renewed": true}))
            }
            OwnershipCommand::RevokeDatabase {
                database,
                expected_epoch,
            } => {
                self.stop(&database, expected_epoch, OwnershipMode::Revoked)?;
                Ok(json!({"revoked": true}))
            }
            OwnershipCommand::DrainDatabase {
                database,
                expected_epoch,
            } => {
                self.stop(&database, expected_epoch, OwnershipMode::Draining)?;
                Ok(json!({"draining": true}))
            }
            OwnershipCommand::GetDatabaseState { database } => {
                Ok(serde_json::to_value(self.database_state(&database)?)?)
            }
            OwnershipCommand::GetWorkerState => Ok(serde_json::to_value(self.status()?)?),
            OwnershipCommand::GetWorkerHeartbeat => {
                Ok(serde_json::to_value(self.heartbeat_state()?)?)
            }
        }
    }

    pub fn status(&self) -> Result<OwnershipWorkerState> {
        self.report(true)
    }

    fn heartbeat_state(&self) -> Result<OwnershipWorkerState> {
        self.report(false)
    }

    fn report(&self, include_resources: bool) -> Result<OwnershipWorkerState> {
        let contexts: Vec<_> = self
            .contexts
            .lock()
            .map_err(|_| anyhow::anyhow!("ownership registry poisoned"))?
            .values()
            .cloned()
            .collect();
        let mut databases = Vec::new();
        for context in contexts {
            let description = context.description()?;
            if include_resources && description.mode == OwnershipMode::Revoked {
                self.finish_detach(&context)?;
            }
            databases.push(description);
        }
        Ok(OwnershipWorkerState {
            worker_id: self.worker_id.clone(),
            databases,
            resources: if include_resources {
                self.worker.status()?
            } else {
                self.worker.status_for_control()?
            },
        })
    }

    pub fn retirement_health(&self) -> serde_json::Value {
        self.worker.retirement_health()
    }

    pub fn capacity_trace_snapshot(&self) -> serde_json::Value {
        self.worker.capacity_trace_snapshot()
    }

    pub fn capacity_trace_page(
        &self,
        stream_id: &str,
        after_sequence: u64,
        max_events: usize,
    ) -> Result<serde_json::Value> {
        self.worker
            .capacity_trace_page(stream_id, after_sequence, max_events)
    }

    pub fn open_session(self: &Arc<Self>, identifier: &DatabaseId) -> Result<OwnedHandle> {
        self.worker.ensure_instance_available(identifier)?;
        let context = self.context(identifier)?;
        context.check(Some(false))?;
        let epoch = {
            let mut state = context.lock()?;
            ensure!(
                matches!(state.mode, OwnershipMode::Writer | OwnershipMode::ReadOnly),
                "database not serving sessions"
            );
            state.handles = state
                .handles
                .checked_add(1)
                .context("owned session count overflow")?;
            state.binding.epoch
        };
        let opened = self.worker.open(identifier.clone(), epoch);
        let result = opened.and_then(|handle| {
            context.check(Some(false))?;
            Ok(OwnedHandle {
                handle: Some(handle),
                context: context.clone(),
                runtime: self.clone(),
            })
        });
        if result.is_err() {
            context.lock()?.handles -= 1;
            self.finish_detach(&context)?;
        }
        result
    }

    fn context(&self, identifier: &DatabaseId) -> Result<Arc<LeaseContext>> {
        identifier.validate()?;
        self.contexts
            .lock()
            .map_err(|_| anyhow::anyhow!("ownership registry poisoned"))?
            .get(identifier)
            .cloned()
            .context("database ownership unknown")
    }

    fn database_state(&self, identifier: &DatabaseId) -> Result<DatabaseOwnership> {
        identifier.validate()?;
        let context = self
            .contexts
            .lock()
            .map_err(|_| anyhow::anyhow!("ownership registry poisoned"))?
            .get(identifier)
            .cloned();
        match context {
            Some(context) => context.description(),
            None => Ok(DatabaseOwnership {
                database: identifier.clone(),
                mode: OwnershipMode::Detached,
                binding: None,
                epoch: 0,
                remaining_ttl_ms: 0,
            }),
        }
    }

    fn fresh_deadline(
        &self,
        lease: &GrantReply,
        role: AccessRole,
        context: &LeaseContext,
    ) -> Result<u64> {
        let started = context.now_ms()?;
        let fresh = self.provider.validate_lease(&lease.binding, role)?;
        ensure!(
            fresh.binding == lease.binding,
            "authority validation binding mismatch"
        );
        let remaining = fresh
            .remaining_ttl_ms
            .checked_sub(DEADLINE_MARGIN_MS)
            .context("lease too short")?;
        let deadline = started
            .checked_add(remaining)
            .context("lease deadline overflow")?;
        let completed = context.now_ms()?;
        ensure!(
            completed >= started,
            "lease clock moved backwards during validation"
        );
        ensure!(
            completed < deadline,
            "lease validation response arrived too late"
        );
        if std::env::var("PERI_NATIVE_GATE_TRACE").as_deref() == Ok("1") {
            eprintln!("NATIVE_LEASE_VALIDATED pid={} started_ms={started} completed_ms={completed} deadline_ms={deadline} authority_remaining_ms={} binding={}", std::process::id(), fresh.remaining_ttl_ms, serde_json::to_string(&fresh.binding)?);
        }
        Ok(deadline)
    }

    fn acquire(self: &Arc<Self>, lease: GrantReply, role: AccessRole) -> Result<()> {
        lease.binding.database.validate()?;
        self.worker
            .ensure_instance_available(&lease.binding.database)?;
        ensure!(
            lease.binding.worker_id == self.worker_id,
            "lease belongs to another worker"
        );
        ensure!(
            lease.binding.role == AccessRole::Writer || role == AccessRole::ReadOnly,
            "read-only grant cannot acquire writer role"
        );
        let identifier = lease.binding.database.clone();
        let (context, generation) = {
            let mut contexts = self
                .contexts
                .lock()
                .map_err(|_| anyhow::anyhow!("ownership registry poisoned"))?;
            let context = contexts
                .entry(identifier.clone())
                .or_insert_with(|| {
                    Arc::new(LeaseContext {
                        state: Mutex::new(LeaseState {
                            mode: OwnershipMode::Detached,
                            binding: lease.binding.clone(),
                            role,
                            deadline_ms: 0,
                            generation: 0,
                            handles: 0,
                            opening: false,
                            renewing: false,
                            detaching: false,
                            last_now: 0,
                        }),
                        clock: self.clock.clone(),
                    })
                })
                .clone();
            let generation = {
                let mut state = context.lock()?;
                ensure!(
                    matches!(state.mode, OwnershipMode::Detached | OwnershipMode::Revoked)
                        && state.handles == 0
                        && !state.opening
                        && !state.renewing
                        && !state.detaching,
                    "database is already owned or still in use"
                );
                ensure!(
                    state.generation == 0 || lease.binding.epoch > state.binding.epoch,
                    "ownership epoch cannot be reused"
                );
                state.generation = state
                    .generation
                    .checked_add(1)
                    .context("ownership generation overflow")?;
                state.binding = lease.binding.clone();
                state.role = role;
                state.mode = OwnershipMode::Acquiring;
                state.deadline_ms = 0;
                state.opening = true;
                state.generation
            };
            (context, generation)
        };
        let result = (|| {
            self.provider.install_lease(lease.clone(), role)?;
            let deadline = self.fresh_deadline(&lease, role, &context)?;
            {
                let mut state = context.lock()?;
                ensure!(
                    state.generation == generation && state.mode == OwnershipMode::Acquiring,
                    "acquire revoked during validation"
                );
                state.deadline_ms = deadline;
            }
            let handle = self.worker.open(identifier.clone(), lease.binding.epoch)?;
            context.check(Some(false))?;
            drop(handle);
            let final_deadline = self.fresh_deadline(&lease, role, &context)?;
            {
                let mut state = context.lock()?;
                let now = context.now_locked(&mut state)?;
                ensure!(
                    state.generation == generation
                        && state.mode == OwnershipMode::Acquiring
                        && now < state.deadline_ms,
                    "acquire revoked or expired during native open"
                );
                state.deadline_ms = state.deadline_ms.min(final_deadline);
                ensure!(
                    now < state.deadline_ms,
                    "acquire expired before publication"
                );
                state.mode = match role {
                    AccessRole::Writer => OwnershipMode::Writer,
                    AccessRole::ReadOnly => OwnershipMode::ReadOnly,
                };
            }
            Ok(())
        })();
        if result.is_err() {
            {
                let mut state = context.lock()?;
                if state.generation == generation && state.mode == OwnershipMode::Acquiring {
                    state.mode = OwnershipMode::Revoked;
                    state.deadline_ms = 0;
                }
            }
            let _ = self.worker.drain(&identifier);
            let _ = self.provider.remove_lease(&identifier, lease.binding.epoch);
        }
        context.lock()?.opening = false;
        if result.is_err() {
            let _ = self.finish_detach(&context);
        }
        result
    }

    fn renew(&self, lease: GrantReply) -> Result<()> {
        self.worker
            .ensure_instance_available(&lease.binding.database)?;
        let context = self.context(&lease.binding.database)?;
        context.check(None)?;
        {
            let mut state = context.lock()?;
            ensure!(!state.renewing, "renewal already in progress");
            state.renewing = true;
        }
        let result = self.renew_inner(lease, &context);
        context.lock()?.renewing = false;
        if result.is_err() {
            let _ = self.finish_detach(&context);
        }
        result
    }

    fn renew_inner(&self, lease: GrantReply, context: &LeaseContext) -> Result<()> {
        let (previous, generation, role) = {
            let state = context.lock()?;
            ensure!(
                matches!(
                    state.mode,
                    OwnershipMode::Writer | OwnershipMode::ReadOnly | OwnershipMode::Draining
                ),
                "cannot renew non-serving ownership"
            );
            let mut expected = state.binding.clone();
            expected.renewal_sequence = lease.binding.renewal_sequence;
            ensure!(
                expected == lease.binding
                    && lease.binding.renewal_sequence > state.binding.renewal_sequence,
                "renewal binding or sequence mismatch"
            );
            (state.binding.clone(), state.generation, state.role)
        };
        let deadline = self.fresh_deadline(&lease, role, context)?;
        self.provider.install_lease(lease.clone(), role)?;
        let mut state = context.lock()?;
        let now = context.now_locked(&mut state)?;
        if !(state.generation == generation
            && state.binding == previous
            && now < state.deadline_ms
            && now < deadline
            && matches!(
                state.mode,
                OwnershipMode::Writer | OwnershipMode::ReadOnly | OwnershipMode::Draining
            ))
        {
            drop(state);
            let _ = self
                .provider
                .remove_lease(&lease.binding.database, lease.binding.epoch);
            anyhow::bail!("renewal arrived after revoke or expiration");
        }
        state.binding = lease.binding;
        state.deadline_ms = deadline;
        Ok(())
    }

    fn stop(&self, identifier: &DatabaseId, epoch: u64, mode: OwnershipMode) -> Result<()> {
        let context = self.context(identifier)?;
        {
            let mut state = context.lock()?;
            ensure!(
                state.binding.epoch == epoch,
                "ownership command epoch mismatch"
            );
            if mode == OwnershipMode::Draining {
                ensure!(
                    matches!(
                        state.mode,
                        OwnershipMode::Writer | OwnershipMode::ReadOnly | OwnershipMode::Draining
                    ),
                    "cannot drain detached or revoked database"
                );
            } else {
                state.generation = state
                    .generation
                    .checked_add(1)
                    .context("ownership generation overflow")?;
                state.deadline_ms = 0;
            }
            state.mode = mode;
        }
        self.finish_detach(&context)
    }

    pub(super) fn finish_detach(&self, context: &LeaseContext) -> Result<()> {
        let identifier = context.lock()?.binding.database.clone();
        if let Err(error) = self.worker.ensure_instance_available(&identifier) {
            let _ = context.quarantine();
            return Err(error);
        }
        let (identifier, epoch, generation, unload) = {
            let mut state = context.lock()?;
            if state.detaching
                || !matches!(state.mode, OwnershipMode::Draining | OwnershipMode::Revoked)
            {
                return Ok(());
            }
            state.detaching = true;
            (
                state.binding.database.clone(),
                state.binding.epoch,
                state.generation,
                state.handles == 0 && !state.opening,
            )
        };
        let result = (|| {
            self.worker.drain(&identifier)?;
            if unload {
                self.worker.close(&identifier)?;
                self.provider.remove_lease(&identifier, epoch)?;
            }
            Ok(())
        })();
        let mut state = context.lock()?;
        state.detaching = false;
        if result.is_ok()
            && unload
            && state.generation == generation
            && state.mode == OwnershipMode::Draining
        {
            state.mode = OwnershipMode::Detached;
            state.deadline_ms = 0;
        }
        let retry = result.is_ok()
            && !unload
            && state.handles == 0
            && !state.opening
            && state.generation == generation;
        drop(state);
        if retry {
            self.finish_detach(context)?;
        }
        result
    }
}
