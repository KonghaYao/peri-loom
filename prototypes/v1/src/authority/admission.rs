use super::{live_binding, Service};
use crate::{
    authority::CatalogRoot,
    ownership::types::{AccessRole, LeaseBinding, MutationGuard, MutationStage},
};
use anyhow::{bail, ensure, Result};
use std::sync::Arc;

pub(super) enum Permission {
    Session(LeaseBinding, AccessRole),
    Pins,
    Control { expires: Option<u64> },
}

pub(super) struct Guard {
    pub service: Arc<Service>,
    pub permission: Permission,
}

impl MutationGuard for Guard {
    fn validate(&self, stage: MutationStage) -> Result<()> {
        if stage == MutationStage::Read {
            return Ok(());
        }
        self.service.available()?;
        let now = self.service.now()?;
        match &self.permission {
            Permission::Session(binding, role) => {
                self.service.live(binding)?;
                if *role == AccessRole::ReadOnly
                    && matches!(stage, MutationStage::Upload | MutationStage::Delete)
                {
                    bail!("ReadOnly");
                }
            }
            Permission::Pins => ensure!(stage == MutationStage::RootPublish, "Unauthorized"),
            Permission::Control { expires } => {
                if let Some(expires) = expires {
                    ensure!(now < *expires, "LeaseExpired");
                }
            }
        }
        Ok(())
    }

    fn validate_root(&self, current: &CatalogRoot, candidate: &CatalogRoot) -> Result<()> {
        self.validate(MutationStage::RootPublish)?;
        let expected = self.service.authority_term;
        ensure!(
            current.incarnation == self.service.incarnation
                && current
                    .ownership
                    .as_ref()
                    .is_some_and(|catalog| catalog.authority_term == expected),
            "Fenced authority term"
        );
        match &self.permission {
            Permission::Control { .. } => {}
            Permission::Pins => pins_only(current, candidate)?,
            Permission::Session(binding, role) => {
                live_binding(current, binding)?;
                ensure!(
                    candidate.ownership == current.ownership,
                    "Unauthorized ownership mutation"
                );
                if *role == AccessRole::ReadOnly {
                    pins_only(current, candidate)?;
                } else {
                    ensure!(
                        current.snapshots == candidate.snapshots
                            && current.tombstones == candidate.tombstones,
                        "Unauthorized reference or GC mutation"
                    );
                    ensure!(
                        current.branches.len() == candidate.branches.len(),
                        "Unauthorized branch mutation"
                    );
                    for (name, branch) in &current.branches {
                        if name != &binding.database.branch {
                            ensure!(
                                candidate.branches.get(name) == Some(branch),
                                "Unauthorized other branch mutation"
                            );
                        }
                    }
                    let branch = &candidate.branches[&binding.database.branch];
                    ensure!(
                        branch.incarnation == binding.branch_incarnation,
                        "Fenced branch incarnation"
                    );
                }
            }
        }
        Ok(())
    }

    fn uncertain(&self, _: MutationStage) {
        self.service.block();
    }
}

fn pins_only(current: &CatalogRoot, candidate: &CatalogRoot) -> Result<()> {
    let mut normalized = candidate.clone();
    normalized.pins = current.pins.clone();
    normalized.revision = current.revision;
    normalized.receipts = current.receipts.clone();
    normalized.receipt_index = current.receipt_index.clone();
    ensure!(&normalized == current, "ReadOnly permits only service pins");
    Ok(())
}
