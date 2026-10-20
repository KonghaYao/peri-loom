use std::sync::Arc;

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

use crate::object_store::{ObjectStore, PutCondition};

use super::recovery::RecoveryPlan;

const KEY: &str = "authority-control/single-authority-seal";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    owner: String,
    clean: bool,
}

pub(super) struct SealObservation {
    pub owner: String,
    pub etag: String,
    pub clean: bool,
}

pub(super) struct Seal {
    store: Arc<dyn ObjectStore>,
    record: Record,
    etag: String,
}

impl Seal {
    #[cfg(test)]
    pub fn acquire(store: Arc<dyn ObjectStore>) -> Result<Self> {
        Self::acquire_with_recovery(store, None)
    }

    #[cfg(test)]
    pub(super) fn observe(store: &dyn ObjectStore) -> Result<Option<SealObservation>> {
        let Some(previous) = store.get(KEY)? else {
            return Ok(None);
        };
        let record: Record = serde_json::from_slice(&previous.bytes)?;
        ensure!(
            record.version == 1,
            "unsupported authority seal version; refusing takeover"
        );
        Ok(Some(SealObservation {
            owner: record.owner,
            etag: previous.etag,
            clean: record.clean,
        }))
    }

    /// Acquires the single-authority seal. A clean record is the normal
    /// takeover path. A dirty record is only replaceable by a recovery plan
    /// that classified the crash after readiness, proved the backend terminal,
    /// and bound the evidence to this exact owner/etag. The replacement record
    /// is always written dirty; recovery never claims a clean window.
    pub(super) fn acquire_with_recovery(
        store: Arc<dyn ObjectStore>,
        plan: Option<&RecoveryPlan>,
    ) -> Result<Self> {
        ensure!(
            store.supports_strict_fencing(),
            "authority seal requires strict conditional backend writes"
        );
        let previous = store.get(KEY)?;
        let observation = previous
            .as_ref()
            .map(|previous| -> Result<SealObservation> {
                let record: Record = serde_json::from_slice(&previous.bytes)?;
                ensure!(
                    record.version == 1,
                    "unsupported authority seal version; refusing takeover"
                );
                Ok(SealObservation {
                    owner: record.owner,
                    etag: previous.etag.clone(),
                    clean: record.clean,
                })
            })
            .transpose()?;
        match (plan, observation.as_ref()) {
            (Some(plan), Some(observation)) => {
                if let Some((owner, etag)) = plan.seal_expectation() {
                    ensure!(
                        observation.owner == owner && observation.etag == etag,
                        "authority seal owner/etag changed after the recovery evidence was produced"
                    );
                }
                plan.authorize_seal_takeover(
                    &observation.owner,
                    &observation.etag,
                    observation.clean,
                )?;
            }
            (None, Some(observation)) if !observation.clean => {
                anyhow::bail!("dirty backend authority seal; old remote terminal is unproven; state-dir replacement and --reconciled cannot authorize takeover");
            }
            (Some(plan), None) => {
                ensure!(
                    plan.seal_expectation().is_none(),
                    "recovery evidence expects an authority seal but the backend has none"
                );
            }
            _ => {}
        }
        let record = Record {
            version: 1,
            owner: crate::authority::unique_id()?,
            clean: false,
        };
        let condition = previous.map_or(PutCondition::Absent, |previous| {
            PutCondition::Match(previous.etag)
        });
        let etag = store.put(KEY, &serde_json::to_vec(&record)?, condition)?;
        Ok(Self {
            store,
            record,
            etag,
        })
    }

    pub fn mark_clean(&mut self) -> Result<()> {
        self.record.clean = true;
        self.etag = self.store.put(
            KEY,
            &serde_json::to_vec(&self.record)?,
            PutCondition::Match(self.etag.clone()),
        )?;
        Ok(())
    }

    /// Revokes the clean record when the write raced the shutdown budget.
    /// The conditional write pins the rollback to the row this authority itself
    /// wrote, so rolling back cannot reopen a takeover window for anyone else.
    pub fn mark_dirty(&mut self) -> Result<()> {
        self.record.clean = false;
        self.etag = self.store.put(
            KEY,
            &serde_json::to_vec(&self.record)?,
            PutCondition::Match(self.etag.clone()),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority_rpc::recovery::{
        evaluate, BackendKind, MarkerState, Readiness, RecoveryDecision,
    };
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn rollback_closes_the_clean_window_a_restart_would_trust() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let mut seal = Seal::acquire(store.clone())?;
        seal.mark_clean()?;
        // The window this guards: a clean record is accepted by the next startup,
        // so an over-budget shutdown that lands it must be able to revoke it.
        let published: serde_json::Value =
            serde_json::from_slice(&store.get(KEY)?.expect("seal record").bytes)?;
        assert_eq!(published["clean"], true);
        seal.mark_dirty()?;
        assert!(Seal::acquire(store).is_err());
        Ok(())
    }

    #[test]
    fn rollback_requires_the_etag_of_the_record_this_authority_wrote() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let mut previous = Seal::acquire(store.clone())?;
        previous.mark_clean()?;
        let _takeover = Seal::acquire(store)?;
        assert!(previous.mark_dirty().is_err());
        Ok(())
    }

    #[test]
    fn dirty_seal_without_recovery_plan_stays_closed() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let _crashed = Seal::acquire(store.clone())?;
        assert!(Seal::acquire(store).is_err());
        Ok(())
    }

    #[test]
    fn clean_seal_with_mismatched_recovery_evidence_is_rejected() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let mut previous = Seal::acquire(store.clone())?;
        previous.mark_clean()?;
        let observed = Seal::observe(store.as_ref())?.expect("seal");
        let mut evidence = crate::authority_rpc::recovery::RecoveryEvidence {
            version: 1,
            generation: "generation".into(),
            backend: BackendKind::Local,
            readiness: Readiness::Announced,
            previous_authority: Some(crate::authority_rpc::recovery::PreviousAuthority {
                owner: observed.owner,
                etag: "not-the-live-etag".into(),
            }),
            local_process: None,
            remote_terminal: None,
            operations: Default::default(),
            preserved_snapshot: None,
        };
        // Make every structural step irrelevant: this test exercises the seal
        // binding, not the operation plan.
        evidence.operations.unknown.clear();
        let plan = evaluate(
            MarkerState::Dirty,
            BackendKind::Local,
            Some(&evidence),
            false,
            true,
        );
        assert!(matches!(
            plan.decision(),
            RecoveryDecision::OperatorRequired { .. }
        ));
        assert!(Seal::acquire_with_recovery(store, Some(&plan)).is_err());
        Ok(())
    }
}
