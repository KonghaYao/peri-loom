use std::collections::BTreeMap;

use anyhow::{ensure, Context, Result};

use super::{codec, migration, Authority};
use crate::namespace::Namespace;
use crate::ownership::types::{GrantState, LeaseBinding, MutationStage, OwnerRecord};

pub(super) struct OwnershipView {
    namespace: Namespace,
    incarnation: String,
    authority_term: u64,
    branches: BTreeMap<String, (OwnerRecord, Option<u64>)>,
}

impl Authority {
    pub(super) fn ownership_view(&self) -> Result<OwnershipView> {
        self.check(MutationStage::Read)?;
        let object = self
            .store
            .get(&self.namespace.key("authority/root")?)?
            .context("authority root missing; refusing reset")?;
        let root = codec::decode(&object.bytes)?;
        Namespace::new(
            root.namespace.tenant.clone(),
            root.namespace.database.clone(),
        )?;
        ensure!(
            root.version == 2
                && root.revision >= 1
                && root.namespace == self.namespace
                && root.incarnation == self.incarnation
                && crate::manifest::valid_hash(&root.incarnation),
            "authority root identity mismatch"
        );
        migration::validate(&root)?;
        let catalog = root.ownership.context("Fenced ownership missing")?;
        Ok(OwnershipView {
            namespace: root.namespace,
            incarnation: root.incarnation,
            authority_term: catalog.authority_term,
            branches: catalog
                .branches
                .into_iter()
                .map(|(name, record)| {
                    let incarnation = root.branches.get(&name).map(|branch| branch.incarnation);
                    (name, (record, incarnation))
                })
                .collect(),
        })
    }
}

impl OwnershipView {
    pub(super) fn matches(&self, binding: &LeaseBinding) -> Result<()> {
        ensure!(
            self.namespace == binding.database.namespace
                && self.incarnation == binding.root_incarnation,
            "Fenced root identity"
        );
        let (record, incarnation) = self
            .branches
            .get(&binding.database.branch)
            .context("Fenced branch ownership missing")?;
        ensure!(
            self.authority_term == binding.authority_term
                && record.state == GrantState::Active
                && record.epoch == binding.epoch
                && record.worker_id == binding.worker_id
                && record.lease_id == binding.lease_id
                && record.grant_id == binding.grant_id
                && record.role == binding.role
                && record.branch_incarnation == binding.branch_incarnation
                && *incarnation == Some(binding.branch_incarnation),
            "Fenced ownership identity"
        );
        Ok(())
    }
}
