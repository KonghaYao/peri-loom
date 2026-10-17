use anyhow::{ensure, Context, Result};

use super::{
    codec, paged_index::PagedIndex, semantic, unique_id, Authority, CatalogRoot, RootSnapshot,
};
use crate::object_store::{PreconditionFailed, PutCondition};
use crate::ownership::types::{AccessRole, GrantState, OwnerRecord};

#[cfg(test)]
#[path = "boot_admission_tests.rs"]
mod tests;

pub(crate) enum BootFailure {
    Read(anyhow::Error),
    Unsettled(anyhow::Error),
}

impl From<anyhow::Error> for BootFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Read(error)
    }
}

impl Authority {
    pub(crate) fn boot_ownership(&self) -> Result<RootSnapshot, BootFailure> {
        if self.guard.is_some() {
            return Err(BootFailure::Read(anyhow::anyhow!(
                "ownership boot requires trusted authority"
            )));
        }
        let operation = unique_id()?;
        let key = self.namespace.key("authority/root")?;
        for _ in 0..128 {
            let previous = self.snapshot()?;
            let mut candidate = previous.root.clone();
            let mut catalog = candidate.ownership.clone().unwrap_or_default();
            catalog.authority_term = catalog
                .authority_term
                .checked_add(1)
                .context("authority term exhausted")?;
            for record in catalog.branches.values_mut() {
                record.state = GrantState::Revoked;
            }
            for (name, branch) in &candidate.branches {
                let mut floor = 0;
                for wal in branch
                    .wal
                    .iter()
                    .chain(branch.history.iter().filter_map(|point| point.wal.as_ref()))
                    .chain(
                        candidate
                            .snapshots
                            .values()
                            .filter_map(|point| point.wal.as_ref()),
                    )
                {
                    floor = floor.max(semantic::index(&self.read_blob(wal)?)?.epoch);
                }
                let record = catalog.branches.entry(name.clone()).or_insert(OwnerRecord {
                    epoch: floor,
                    branch_incarnation: branch.incarnation,
                    worker_id: "unowned".into(),
                    lease_id: "unowned".into(),
                    grant_id: unique_id()?,
                    renewal_sequence: 0,
                    state: GrantState::Revoked,
                    role: AccessRole::Writer,
                });
                record.epoch = record.epoch.max(floor);
                record.branch_incarnation = branch.incarnation;
            }
            candidate.version = 4;
            candidate.ownership = Some(catalog);
            candidate.revision = candidate
                .revision
                .checked_add(1)
                .context("authority revision exhausted")?;
            // Boot publishes the migration head through the same durability group as a
            // statement: immutable page bodies first, the head CAS last.
            let pending = std::cell::RefCell::new(super::paged_index::PendingPages::default());
            let publishing =
                PagedIndex::new(self.store.as_ref(), &self.namespace).deferring(&pending);
            if previous.root.version != 4 {
                candidate.receipt_index = publishing.build(
                    std::mem::take(&mut candidate.receipts)
                        .into_iter()
                        .map(|(id, revision)| (id, serde_json::json!(revision)))
                        .collect(),
                )?;
            }
            self.record_receipt(&mut candidate, operation.clone(), &publishing)?;
            self.validate_head_with(&candidate, Some(&pending))?;
            candidate.blobs.publish(&publishing)?;
            candidate.objects.publish(&publishing)?;
            candidate.uploads.publish(&publishing)?;
            candidate.tombstones.publish(&publishing)?;
            candidate.seal()?;
            let (encoded, digest) = codec::encode_with_digest(&candidate)?;
            let mut writes = pending.borrow().writes().to_vec();
            writes.push(crate::object_store::GroupWrite::new(
                key.clone(),
                encoded,
                PutCondition::Match(previous.etag),
            ));
            match self.store.put_group(&writes) {
                Ok(etags) => {
                    let etag = etags
                        .last()
                        .cloned()
                        .context("authority root etag missing")
                        .map_err(BootFailure::Unsettled)?;
                    // The snapshot was verified before cloning. Boot changes ownership,
                    // receipts and registry representation, while inheriting every blob,
                    // branch reference, snapshot and pin unchanged. Only the acknowledged
                    // group makes this exact encoded candidate eligible for admission.
                    self.remember(&key, &candidate, &digest);
                    return Ok(RootSnapshot {
                        root: candidate,
                        etag,
                    });
                }
                Err(error) => {
                    if !error.is::<PreconditionFailed>() {
                        return Err(BootFailure::Unsettled(
                            error.context("authority boot outcome uncertain; refusing admission"),
                        ));
                    }
                    let observed = self.snapshot()?;
                    if self.receipt_in(&observed.root, &operation)? == Some(candidate.revision) {
                        return Ok(observed);
                    }
                }
            }
        }
        Err(BootFailure::Unsettled(PreconditionFailed.into()))
    }
}

pub(super) fn validate(root: &CatalogRoot) -> Result<()> {
    ensure!(
        if root.version == 1 {
            root.ownership.is_none()
        } else {
            root.ownership.is_some()
        },
        "ownership schema mismatch"
    );
    if let Some(catalog) = &root.ownership {
        ensure!(catalog.authority_term > 0, "invalid authority term");
        for (name, record) in &catalog.branches {
            semantic::validate_name(name)?;
            crate::ownership::types::validate_label(&record.worker_id)?;
            crate::ownership::types::validate_label(&record.lease_id)?;
            crate::ownership::types::validate_label(&record.grant_id)?;
            ensure!(
                record.branch_incarnation > 0,
                "invalid owned branch incarnation"
            );
            if record.state == GrantState::Active {
                ensure!(
                    root.branches
                        .get(name)
                        .is_some_and(|branch| branch.incarnation == record.branch_incarnation),
                    "active ownership has stale incarnation"
                );
            }
        }
        for (identifier, receipt) in &catalog.commands {
            crate::ownership::types::validate_label(identifier)?;
            ensure!(
                crate::manifest::valid_hash(&receipt.digest),
                "invalid command receipt digest"
            );
        }
    }
    Ok(())
}
