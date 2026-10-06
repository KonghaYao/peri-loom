use crate::authority::{semantic, unique_id, Authority, CatalogRoot};
use crate::ownership::types::{GrantState, OwnerRecord};
use anyhow::{ensure, Context, Result};

pub(crate) fn prepare(
    authority: &Authority,
    previous: &CatalogRoot,
    next: &mut CatalogRoot,
) -> Result<()> {
    match (&previous.ownership, &mut next.ownership) {
        (None, None) => return Ok(()),
        (Some(old), Some(new)) => {
            ensure!(
                old.authority_term == new.authority_term,
                "authority term change requires exclusive boot"
            );
            for (branch, record) in &old.branches {
                let candidate = new
                    .branches
                    .get_mut(branch)
                    .context("ownership epoch tombstone removal forbidden")?;
                ensure!(
                    candidate.epoch >= record.epoch,
                    "ownership epoch cannot decrease"
                );
                if candidate.epoch == record.epoch {
                    ensure!(
                        candidate.worker_id == record.worker_id
                            && candidate.lease_id == record.lease_id
                            && candidate.grant_id == record.grant_id
                            && candidate.renewal_sequence >= record.renewal_sequence
                            && candidate.branch_incarnation == record.branch_incarnation,
                        "lease identity cannot change at same epoch"
                    );
                    ensure!(
                        candidate.epoch != record.epoch || candidate.role == record.role,
                        "lease role cannot change at same epoch"
                    );
                    if record.state == GrantState::Revoked {
                        ensure!(
                            candidate.state == GrantState::Revoked,
                            "revoked grant cannot resurrect"
                        );
                    }
                }
                if !next
                    .branches
                    .get(branch)
                    .is_some_and(|branch| branch.incarnation == candidate.branch_incarnation)
                {
                    candidate.state = GrantState::Revoked;
                }
            }
            for (identifier, receipt) in &old.commands {
                let candidate = new
                    .commands
                    .get(identifier)
                    .context("command receipt removal forbidden")?;
                let mut normalized = candidate.result.clone();
                if let Some(object) = normalized.as_object_mut() {
                    object.insert("status".into(), serde_json::json!("pending_gc"));
                    object.insert("deleted".into(), serde_json::json!(0));
                }
                let completed_gc = receipt.digest == candidate.digest
                    && receipt
                        .result
                        .get("status")
                        .and_then(|status| status.as_str())
                        == Some("pending_gc")
                    && candidate
                        .result
                        .get("status")
                        .and_then(|status| status.as_str())
                        == Some("completed_gc")
                    && receipt.result == normalized;
                ensure!(
                    candidate == receipt || completed_gc,
                    "command receipt identity mutation forbidden"
                );
            }
        }
        _ => anyhow::bail!("ownership schema mutation requires migration"),
    }
    let catalog = next.ownership.as_mut().unwrap();
    for (name, branch) in &next.branches {
        if !previous.branches.contains_key(name) && !catalog.branches.contains_key(name) {
            let mut epoch = 0;
            for wal in branch
                .wal
                .iter()
                .chain(branch.history.iter().filter_map(|point| point.wal.as_ref()))
            {
                epoch = epoch.max(semantic::index(&authority.read_blob(wal)?)?.epoch);
            }
            catalog.branches.insert(
                name.clone(),
                OwnerRecord {
                    epoch,
                    branch_incarnation: previous
                        .revision
                        .checked_add(1)
                        .context("incarnation overflow")?,
                    worker_id: "unowned".into(),
                    lease_id: "unowned".into(),
                    grant_id: unique_id()?,
                    renewal_sequence: 0,
                    state: GrantState::Revoked,
                    role: crate::ownership::types::AccessRole::Writer,
                },
            );
        }
    }
    Ok(())
}
