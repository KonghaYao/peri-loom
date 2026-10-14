use super::{commands, digest, OwnershipAuthority, Permission};
use crate::{
    authority::{BranchRef, CatalogRoot, RecoveryPoint},
    authority_rpc::protocol::MaintenanceOperation,
    capabilities::Capabilities,
    gc::GarbageCollector,
    ownership::types::validate_label,
    runtime::DatabaseId,
};
use anyhow::{anyhow, ensure, Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

pub(super) fn run(
    authority: &OwnershipAuthority,
    identifier: &DatabaseId,
    request_id: &str,
    operation: MaintenanceOperation,
) -> Result<Value> {
    validate_label(request_id)?;
    let service = authority.service(identifier)?;
    let _barrier = if matches!(
        operation,
        MaintenanceOperation::Restore { .. }
            | MaintenanceOperation::RestorePitr { .. }
            | MaintenanceOperation::DropBranch
            | MaintenanceOperation::GcCollect { .. }
    ) {
        service.gate.write()
    } else {
        service.gate.write_wait()
    }?;
    service.available()?;
    let scoped = service.scoped(Permission::Control { expires: None });
    let capabilities = Capabilities::new(scoped.clone());
    let payload = digest(&("maintenance", identifier, &operation))?;
    let revokes_branch = matches!(
        operation,
        MaintenanceOperation::Restore { .. }
            | MaintenanceOperation::RestorePitr { .. }
            | MaintenanceOperation::DropBranch
    );
    if !matches!(operation, MaintenanceOperation::GcCollect { .. }) {
        if let Some(result) = commands::receipt(&service, request_id, &payload)? {
            if revokes_branch {
                reconcile_leases(&service)?;
            }
            return Ok(result);
        }
    }
    let result = match &operation {
        MaintenanceOperation::Refs => serde_json::to_value(capabilities.list()?)?,
        MaintenanceOperation::Pitr { sequence } => {
            serde_json::to_value(capabilities.pitr(&identifier.branch, *sequence)?)?
        }
        MaintenanceOperation::PitrTime { timestamp_ms } => {
            serde_json::to_value(capabilities.resolve_time(&identifier.branch, *timestamp_ms)?)?
        }
        MaintenanceOperation::GcInspect => {
            serde_json::to_value(GarbageCollector::new(scoped, Duration::ZERO).inspect()?)?
        }
        MaintenanceOperation::GcCollect { safety_window_ms } => {
            GarbageCollector::new(scoped, Duration::from_millis(*safety_window_ms))
                .collect_receipted(request_id, &payload, |blob, _| {
                    let now = service.now()?;
                    let mut memory = service
                        .memory
                        .lock()
                        .map_err(|_| anyhow!("Unavailable GC clock"))?;
                    let marked = *memory.gc_marks.entry(blob.into()).or_insert(now);
                    Ok(now.checked_sub(marked).context("Unavailable GC clock")?
                        >= *safety_window_ms)
                })?
        }
        _ => {
            let mut result = None;
            scoped.transact(None, |root| {
                let outcome = apply(root, &identifier.branch, &operation)?;
                commands::insert_receipt(root, request_id, &payload, &outcome)?;
                result = Some(outcome);
                Ok(())
            })?;
            result.context("maintenance publication missing")?
        }
    };
    service.available()?;
    if revokes_branch {
        reconcile_leases(&service)?;
    }
    Ok(result)
}

fn reconcile_leases(service: &super::Service) -> Result<()> {
    let snapshot = service
        .authority
        .snapshot()
        .inspect_err(|_| service.block())?;
    service
        .memory
        .lock()
        .map_err(|_| anyhow!("Unavailable lease memory"))?
        .leases
        .retain(|_, live| super::live_binding(&snapshot.root, &live.binding).is_ok());
    Ok(())
}

fn apply(root: &mut CatalogRoot, branch: &str, operation: &MaintenanceOperation) -> Result<Value> {
    let fresh = root
        .revision
        .checked_add(1)
        .context("incarnation overflow")?;
    match operation {
        MaintenanceOperation::Snapshot { name } => {
            crate::authority::semantic::validate_name(name)?;
            ensure!(
                !root.snapshots.contains_key(name),
                "snapshot already exists"
            );
            let reference = root.branches.get(branch).context("branch missing")?;
            let point = RecoveryPoint {
                id: digest(&(&root.incarnation, root.revision, branch, name, reference))?,
                branch: branch.into(),
                timeline: reference.timeline,
                sequence: reference.current_sequence,
                timestamp_ms: reference.current_commit_ms,
                head: reference.head.clone(),
                wal: reference.wal.clone(),
            };
            root.snapshots.insert(name.clone(), point.clone());
            return Ok(serde_json::to_value(point)?);
        }
        MaintenanceOperation::Branch { source, name } => {
            crate::authority::semantic::validate_name(source)?;
            crate::authority::semantic::validate_name(name)?;
            ensure!(!root.branches.contains_key(name), "branch already exists");
            let mut reference = root
                .branches
                .get(source)
                .context("source branch missing")?
                .clone();
            reference.incarnation = fresh;
            reference.timeline = fresh;
            root.branches.insert(name.clone(), reference);
        }
        MaintenanceOperation::Clone { snapshot, name } => {
            crate::authority::semantic::validate_name(name)?;
            ensure!(!root.branches.contains_key(name), "branch already exists");
            let point = root
                .snapshots
                .get(snapshot)
                .context("snapshot missing")?
                .clone();
            root.branches.insert(
                name.clone(),
                BranchRef {
                    incarnation: fresh,
                    timeline: fresh,
                    sequence: point.sequence,
                    last_commit_ms: point.timestamp_ms,
                    current_sequence: point.sequence,
                    current_commit_ms: point.timestamp_ms,
                    head: point.head.clone(),
                    wal: point.wal.clone(),
                    history: if point.sequence == 0 {
                        vec![]
                    } else {
                        vec![point]
                    },
                },
            );
        }
        MaintenanceOperation::Restore { snapshot } => {
            let point = root
                .snapshots
                .get(snapshot)
                .context("snapshot missing")?
                .clone();
            crate::capabilities::restore(root, branch, &point)?;
        }
        MaintenanceOperation::RestorePitr { sequence } => {
            let point = root
                .branches
                .get(branch)
                .context("branch missing")?
                .history
                .iter()
                .filter(|point| point.sequence == *sequence)
                .max_by_key(|point| (point.timestamp_ms, point.timeline))
                .cloned()
                .context("exact committed sequence is not retained")?;
            crate::capabilities::restore(root, branch, &point)?;
        }
        MaintenanceOperation::DropBranch => {
            root.branches.remove(branch).context("branch missing")?;
        }
        MaintenanceOperation::DropSnapshot { name } => {
            root.snapshots.remove(name).context("snapshot missing")?;
        }
        MaintenanceOperation::RetainAfter { sequence } => {
            root.branches
                .get_mut(branch)
                .context("branch missing")?
                .history
                .retain(|point| point.sequence >= *sequence);
        }
        _ => anyhow::bail!("unsupported maintenance mutation"),
    }
    Ok(json!({"revision": fresh}))
}
