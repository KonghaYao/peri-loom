use super::{digest, same_lease, LiveLease, OwnershipAuthority, Permission, Service};
use crate::{authority::CatalogRoot, ownership::types::*};
use anyhow::{anyhow, ensure, Context, Result};

pub(super) fn validate_intent(service: &Service, intent: &LeaseIntent) -> Result<()> {
    match intent {
        LeaseIntent::Grant(intent) => {
            intent.database.validate()?;
            validate_label(&intent.worker_id)?;
            validate_label(&intent.lease_id)?;
            ttl(intent.ttl_ms)?;
            let root = service.authority.snapshot()?.root;
            ensure!(
                root.branches.contains_key(&intent.database.branch),
                "branch missing"
            );
            let epoch = root
                .ownership
                .as_ref()
                .context("ownership missing")?
                .branches
                .get(&intent.database.branch)
                .map_or(0, |record| record.epoch);
            ensure!(
                intent.expected_epoch == epoch && intent.epoch > epoch,
                "Fenced expected epoch"
            );
        }
        LeaseIntent::Renew(intent) => {
            ttl(intent.ttl_ms)?;
            service.check_binding(&intent.binding)?;
            let live = service.live(&intent.binding)?;
            ensure!(
                live.binding == intent.binding
                    && intent.sequence
                        == intent
                            .binding
                            .renewal_sequence
                            .checked_add(1)
                            .context("renewal sequence exhausted")?,
                "Fenced renewal sequence"
            );
        }
    }
    Ok(())
}

pub(super) fn grant(authority: &OwnershipAuthority, request: GrantRequest) -> Result<GrantReply> {
    validate_label(&request.request_id)?;
    let service = authority.service(&request.intent.database)?;
    let _barrier = service.gate.write()?;
    service.available()?;
    let payload = digest(&("grant", &request))?;
    if let Some(result) = receipt(&service, &request.request_id, &payload)? {
        return replay_grant(&service, serde_json::from_value(result)?);
    }
    let intent = LeaseIntent::Grant(request.intent.clone());
    validate_intent(&service, &intent)?;
    let nonce_expiry = consume_nonce(&service, &request.nonce, &intent)?;
    let deadline = service.deadline(request.intent.ttl_ms)?;
    let root = service.authority.snapshot()?.root;
    let branch = root
        .branches
        .get(&request.intent.database.branch)
        .context("branch missing")?;
    let binding = LeaseBinding {
        database: request.intent.database.clone(),
        root_incarnation: root.incarnation,
        authority_term: service.authority_term,
        branch_incarnation: branch.incarnation,
        epoch: request.intent.epoch,
        worker_id: request.intent.worker_id.clone(),
        lease_id: request.intent.lease_id.clone(),
        grant_id: request.request_id.clone(),
        renewal_sequence: 0,
        role: request.intent.role,
    };
    let reply = GrantReply {
        binding: binding.clone(),
        remaining_ttl_ms: request.intent.ttl_ms,
    };
    let result = serde_json::to_value(&reply)?;
    let scoped = service.scoped(Permission::Control {
        expires: Some(deadline.min(nonce_expiry)),
    });
    scoped.transact(None, |root| {
        let catalog = root.ownership.as_mut().context("ownership missing")?;
        let epoch = catalog
            .branches
            .get(&binding.database.branch)
            .map_or(0, |record| record.epoch);
        ensure!(
            epoch == request.intent.expected_epoch && binding.epoch > epoch,
            "Fenced expected epoch"
        );
        ensure!(
            root.branches
                .get(&binding.database.branch)
                .is_some_and(|branch| branch.incarnation == binding.branch_incarnation),
            "Fenced branch incarnation"
        );
        catalog.branches.insert(
            binding.database.branch.clone(),
            OwnerRecord {
                epoch: binding.epoch,
                branch_incarnation: binding.branch_incarnation,
                worker_id: binding.worker_id.clone(),
                lease_id: binding.lease_id.clone(),
                grant_id: binding.grant_id.clone(),
                renewal_sequence: 0,
                state: GrantState::Active,
                role: binding.role,
            },
        );
        insert_receipt(root, &request.request_id, &payload, &result)
    })?;
    service.available()?;
    ensure!(
        service.now()? < deadline,
        "LeaseExpired grant completed too late"
    );
    service
        .memory
        .lock()
        .map_err(|_| anyhow!("Unavailable lease memory"))?
        .leases
        .insert(
            binding.database.branch.clone(),
            LiveLease {
                binding: binding.clone(),
                deadline,
            },
        );
    replay_grant(&service, reply)
}

pub(super) fn renew(authority: &OwnershipAuthority, request: RenewRequest) -> Result<GrantReply> {
    validate_label(&request.request_id)?;
    let service = authority.service(&request.intent.binding.database)?;
    let _barrier = service.gate.write_wait()?;
    service.available()?;
    let payload = digest(&("renew", &request))?;
    if let Some(result) = receipt(&service, &request.request_id, &payload)? {
        return replay_grant(&service, serde_json::from_value(result)?);
    }
    let intent = LeaseIntent::Renew(request.intent.clone());
    validate_intent(&service, &intent)?;
    let live = service.live(&request.intent.binding)?;
    let nonce_expiry = consume_nonce(&service, &request.nonce, &intent)?;
    let deadline = service.deadline(request.intent.ttl_ms)?;
    let mut binding = live.binding;
    binding.renewal_sequence = request.intent.sequence;
    let reply = GrantReply {
        binding: binding.clone(),
        remaining_ttl_ms: request.intent.ttl_ms,
    };
    let result = serde_json::to_value(&reply)?;
    let scoped = service.scoped(Permission::Control {
        expires: Some(live.deadline.min(nonce_expiry).min(deadline)),
    });
    scoped.transact(None, |root| {
        super::live_binding(root, &request.intent.binding)?;
        let record = root
            .ownership
            .as_mut()
            .context("ownership missing")?
            .branches
            .get_mut(&binding.database.branch)
            .context("ownership missing")?;
        ensure!(
            record.renewal_sequence == request.intent.binding.renewal_sequence,
            "Fenced renewal sequence"
        );
        record.renewal_sequence = binding.renewal_sequence;
        insert_receipt(root, &request.request_id, &payload, &result)
    })?;
    service.available()?;
    ensure!(
        service.now()? < live.deadline && service.now()? < deadline,
        "LeaseExpired renewal completed too late"
    );
    service
        .memory
        .lock()
        .map_err(|_| anyhow!("Unavailable lease memory"))?
        .leases
        .insert(
            binding.database.branch.clone(),
            LiveLease {
                binding: binding.clone(),
                deadline,
            },
        );
    replay_grant(&service, reply)
}

pub(super) fn revoke(
    authority: &OwnershipAuthority,
    request: RevokeRequest,
) -> Result<DatabaseOwnership> {
    validate_label(&request.request_id)?;
    let service = authority.service(&request.database)?;
    let _barrier = service.gate.write()?;
    service.available()?;
    let payload = digest(&("revoke", &request))?;
    if let Some(result) = receipt(&service, &request.request_id, &payload)? {
        return Ok(serde_json::from_value(result)?);
    }
    let reply = DatabaseOwnership {
        database: request.database.clone(),
        mode: OwnershipMode::Revoked,
        binding: None,
        epoch: request.expected_epoch,
        remaining_ttl_ms: 0,
    };
    let result = serde_json::to_value(&reply)?;
    service
        .scoped(Permission::Control { expires: None })
        .transact(None, |root| {
            let record = root
                .ownership
                .as_mut()
                .context("ownership missing")?
                .branches
                .get_mut(&request.database.branch)
                .context("ownership missing")?;
            ensure!(
                record.epoch == request.expected_epoch,
                "Fenced expected epoch"
            );
            record.state = GrantState::Revoked;
            insert_receipt(root, &request.request_id, &payload, &result)
        })?;
    service.available()?;
    service
        .memory
        .lock()
        .map_err(|_| anyhow!("Unavailable lease memory"))?
        .leases
        .remove(&request.database.branch);
    Ok(reply)
}

pub(super) fn receipt(
    service: &Service,
    identifier: &str,
    payload: &str,
) -> Result<Option<serde_json::Value>> {
    let root = service.authority.snapshot()?.root;
    let catalog = root.ownership.as_ref().context("ownership missing")?;
    if let Some(receipt) = catalog.commands.get(identifier) {
        ensure!(
            receipt.digest == payload,
            "Conflict request ID payload mismatch"
        );
        return Ok(Some(receipt.result.clone()));
    }
    Ok(None)
}

pub(super) fn insert_receipt(
    root: &mut CatalogRoot,
    identifier: &str,
    payload: &str,
    result: &serde_json::Value,
) -> Result<()> {
    let catalog = root.ownership.as_mut().context("ownership missing")?;
    ensure!(
        !catalog.commands.contains_key(identifier),
        "Conflict command already committed"
    );
    catalog.commands.insert(
        identifier.into(),
        CommandReceipt {
            digest: payload.into(),
            result: result.clone(),
        },
    );
    Ok(())
}

fn consume_nonce(service: &Service, identifier: &str, intent: &LeaseIntent) -> Result<u64> {
    let now = service.now()?;
    let database = match intent {
        LeaseIntent::Grant(intent) => &intent.database,
        LeaseIntent::Renew(intent) => &intent.binding.database,
    };
    let root = service.authority.snapshot()?.root;
    let owner = root
        .ownership
        .as_ref()
        .context("ownership missing")?
        .branches
        .get(&database.branch);
    let incarnation = root
        .branches
        .get(&database.branch)
        .context("branch missing")?
        .incarnation;
    let mut memory = service
        .memory
        .lock()
        .map_err(|_| anyhow!("Unavailable lease memory"))?;
    let nonce = memory
        .nonces
        .get_mut(identifier)
        .context("Unauthorized nonce missing")?;
    ensure!(
        !nonce.used && nonce.digest == digest(intent)? && now < nonce.expires,
        "Unauthorized stale or mismatched nonce"
    );
    ensure!(
        nonce.root_incarnation == root.incarnation
            && nonce.branch_incarnation == incarnation
            && nonce.owner.as_ref() == owner,
        "Fenced nonce expected ownership changed"
    );
    nonce.used = true;
    Ok(nonce.expires)
}

fn replay_grant(service: &Service, reply: GrantReply) -> Result<GrantReply> {
    let live = service.live(&reply.binding)?;
    ensure!(
        same_lease(&live.binding, &reply.binding),
        "Fenced grant replay"
    );
    service.check_binding(&live.binding)?;
    let remaining = live
        .deadline
        .checked_sub(service.now()?)
        .context("LeaseExpired")?;
    ensure!(remaining > 0, "LeaseExpired");
    Ok(GrantReply {
        binding: live.binding,
        remaining_ttl_ms: remaining,
    })
}

fn ttl(value: u64) -> Result<()> {
    ensure!((100..=60000).contains(&value), "invalid lease TTL");
    Ok(())
}
