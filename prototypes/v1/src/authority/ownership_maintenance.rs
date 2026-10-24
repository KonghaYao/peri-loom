use super::{admission_gate::AdmissionBusy, commands, digest, OwnershipAuthority, Permission};
use crate::{
    authority::{Authority, BranchRef, CatalogRoot, RecoveryPoint, Tombstone},
    authority_rpc::protocol::MaintenanceOperation,
    capabilities::Capabilities,
    gc::{GarbageCollector, GcDelete},
    object_store::PreconditionFailed,
    ownership::types::validate_label,
    runtime::DatabaseId,
    work_metrics::PublishOrigin,
};
use anyhow::{anyhow, ensure, Context, Result};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub(super) fn run(
    authority: &OwnershipAuthority,
    identifier: &DatabaseId,
    request_id: &str,
    operation: MaintenanceOperation,
) -> Result<Value> {
    validate_label(request_id)?;
    let service = authority.service(identifier)?;
    let read_only = matches!(
        operation,
        MaintenanceOperation::Refs
            | MaintenanceOperation::Pitr { .. }
            | MaintenanceOperation::PitrTime { .. }
            | MaintenanceOperation::GcInspect
    );
    let gc_collect = matches!(operation, MaintenanceOperation::GcCollect { .. });
    // Read-only maintenance runs on the snapshot the capability code decodes from the published
    // head. It never mutates, so it takes a shared ticket instead of the exclusive queue: the
    // revision it reports is the boundary of that one immutable snapshot, and publications
    // after it are simply not part of this answer.
    let _read = if read_only {
        Some(service.gate.read()?)
    } else {
        None
    };
    // Restore and branch deletion keep the exclusive barrier for their whole transaction: they
    // rewrite branch history that live sessions must not observe half-applied. A collection
    // cycle takes no barrier here; only the deletion batches it hands to `delete_batch` below
    // acquire one, so marking and planning never stall SQL or lease renewal.
    let _barrier = if read_only || gc_collect {
        None
    } else if matches!(
        operation,
        MaintenanceOperation::Restore { .. }
            | MaintenanceOperation::RestorePitr { .. }
            | MaintenanceOperation::DropBranch
    ) {
        Some(service.gate.write()?)
    } else {
        Some(service.gate.write_wait()?)
    };
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
    if gc_collect {
        let MaintenanceOperation::GcCollect { safety_window_ms } = &operation else {
            unreachable!("collection branch requires a collection operation")
        };
        let mut delete_batch = |authority: &Authority,
                                targets: &[(String, Tombstone)],
                                revision: u64|
         -> Result<GcDelete> {
            let waiting = Instant::now();
            let _transition = match service.gate.write_wait() {
                Ok(transition) => transition,
                Err(error) if error.is::<AdmissionBusy>() => return Ok(GcDelete::Busy),
                Err(error) => return Err(error),
            };
            crate::work_metrics::observe_gc_wait(waiting.elapsed());
            service.available()?;
            let exclusive = Instant::now();
            let outcome = match authority.delete_tombstones(targets, revision, PublishOrigin::Gc) {
                Ok(outcome) => GcDelete::Complete(outcome),
                Err(error) if error.is::<PreconditionFailed>() => GcDelete::Stale,
                Err(error) => {
                    crate::work_metrics::observe_gc_delete_failure();
                    return Err(error);
                }
            };
            crate::work_metrics::observe_gc_exclusive(exclusive.elapsed());
            Ok(outcome)
        };
        let result = GarbageCollector::new(scoped, Duration::from_millis(*safety_window_ms))
            .collect_receipted(
                request_id,
                &payload,
                |blob, _| {
                    let now = service.now()?;
                    let mut memory = service
                        .memory
                        .lock()
                        .map_err(|_| anyhow!("Unavailable GC clock"))?;
                    let marked = *memory.gc_marks.entry(blob.into()).or_insert(now);
                    Ok(now.checked_sub(marked).context("Unavailable GC clock")?
                        >= *safety_window_ms)
                },
                &mut delete_batch,
            )?;
        service.available()?;
        return Ok(result);
    }
    if let Some(result) = commands::receipt(&service, request_id, &payload)? {
        if revokes_branch {
            reconcile_leases(&service)?;
        }
        return Ok(result);
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
        _ => {
            let mut result = None;
            scoped.transact_with_origin(None, PublishOrigin::Lease, |root| {
                let outcome = apply(scoped.as_ref(), root, &identifier.branch, &operation)?;
                commands::insert_receipt(&service.authority, root, request_id, &payload, &outcome)?;
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
        .inspect_err(|_| service.block("maintenance-reconcile-snapshot"))?;
    service
        .memory
        .lock()
        .map_err(|_| anyhow!("Unavailable lease memory"))?
        .leases
        .retain(|_, live| super::live_binding(&snapshot.root, &live.binding).is_ok());
    Ok(())
}

fn apply(
    authority: &Authority,
    root: &mut CatalogRoot,
    branch: &str,
    operation: &MaintenanceOperation,
) -> Result<Value> {
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
            let history = if point.sequence == 0 {
                vec![]
            } else {
                vec![point.clone()]
            };
            let recovery = Some(authority.build_recovery_index(&history)?);
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
                    history,
                    recovery,
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
            let point = {
                let reference = root.branches.get(branch).context("branch missing")?;
                crate::capabilities::resolve_sequence(authority, branch, reference, *sequence)?
            };
            crate::capabilities::restore(root, branch, &point)?;
        }
        MaintenanceOperation::DropBranch => {
            root.branches.remove(branch).context("branch missing")?;
        }
        MaintenanceOperation::DropSnapshot { name } => {
            root.snapshots.remove(name).context("snapshot missing")?;
        }
        MaintenanceOperation::RetainAfter { sequence } => {
            authority.retain_recovery_after(root, branch, *sequence)?;
        }
        _ => anyhow::bail!("unsupported maintenance mutation"),
    }
    Ok(json!({"revision": fresh}))
}
