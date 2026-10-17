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
        insert_receipt(
            &service.authority,
            root,
            &request.request_id,
            &payload,
            &result,
        )
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
        insert_receipt(
            &service.authority,
            root,
            &request.request_id,
            &payload,
            &result,
        )
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
            insert_receipt(
                &service.authority,
                root,
                &request.request_id,
                &payload,
                &result,
            )
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
    if let Some(receipt) = service
        .authority
        .archived_command(catalog.command_index.as_deref(), identifier)?
    {
        ensure!(
            receipt.digest == payload,
            "Conflict request ID payload mismatch"
        );
        return Ok(Some(receipt.result));
    }
    Ok(None)
}

pub(super) fn insert_receipt(
    authority: &crate::authority::Authority,
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
    // An archived id is still a committed id: inserting it again would shadow the page
    // the old reply lives in and turn a replay into a second execution.
    ensure!(
        authority
            .archived_command(catalog.command_index.as_deref(), identifier)?
            .is_none(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authority::{paged_index::PagedIndex, Authority};
    use crate::namespace::Namespace;
    use crate::object_store::{MemoryObjectStore, ObjectStore};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn fixture() -> Result<(Arc<Authority>, Arc<dyn ObjectStore>, Namespace)> {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryObjectStore::default());
        let namespace = Namespace::new("archive", "commands")?;
        let authority = Authority::open(store.clone(), namespace.clone())?;
        Ok((authority, store, namespace))
    }

    /// Grow a version-5 catalog with `terminal` terminal receipts and `pending` GC
    /// receipts, then move the overflow to the archive.
    fn archived_catalog(
        authority: &Authority,
        store: &Arc<dyn ObjectStore>,
        namespace: &Namespace,
        terminal: usize,
        pending: usize,
    ) -> Result<(CatalogRoot, Vec<(String, CommandReceipt)>)> {
        let mut root = authority.snapshot()?.root;
        root.version = 5;
        let mut catalog = OwnershipCatalog {
            authority_term: 1,
            ..OwnershipCatalog::default()
        };
        let mut receipts = Vec::new();
        for index in 0..terminal {
            let identifier = format!("terminal-{index:04}");
            let receipt = CommandReceipt {
                digest: blake3::hash(identifier.as_bytes()).to_hex().to_string(),
                result: serde_json::json!({"kind": "terminal", "index": index}),
            };
            catalog.commands.insert(identifier.clone(), receipt.clone());
            receipts.push((identifier, receipt));
        }
        for index in 0..pending {
            let identifier = format!("pending-{index:04}");
            catalog.commands.insert(
                identifier,
                CommandReceipt {
                    digest: blake3::hash(format!("pending-{index}").as_bytes())
                        .to_hex()
                        .to_string(),
                    result: serde_json::json!({"status": "pending_gc"}),
                },
            );
        }
        root.ownership = Some(catalog);
        let index = PagedIndex::new(store.as_ref(), namespace);
        authority.archive_commands(&mut root, &index)?;
        Ok((root, receipts))
    }

    #[test]
    fn archive_bounds_terminal_receipts_but_keeps_pending_gc_inline() -> Result<()> {
        let (authority, store, namespace) = fixture()?;
        let overflow = 7;
        let (root, receipts) = archived_catalog(
            &authority,
            &store,
            &namespace,
            super::super::super::INLINE_COMMANDS_MAX + overflow,
            3,
        )?;
        let catalog = root.ownership.as_ref().context("ownership missing")?;
        assert_eq!(
            catalog.commands.len(),
            super::super::super::INLINE_COMMANDS_MAX + 3,
            "the window kept the wrong number of receipts"
        );
        assert_eq!(
            catalog
                .commands
                .values()
                .filter(|receipt| receipt
                    .result
                    .get("status")
                    .and_then(|status| status.as_str())
                    == Some("pending_gc"))
                .count(),
            3,
            "pending GC receipts left the inline window"
        );
        let pointer = catalog
            .command_index
            .as_deref()
            .context("commands were not archived")?;
        // Eviction takes the lexicographically smallest identifiers first.
        for (identifier, expected) in receipts.iter().take(overflow) {
            assert!(!catalog.commands.contains_key(identifier));
            assert_eq!(
                authority
                    .archived_command(Some(pointer), identifier)?
                    .as_ref(),
                Some(expected)
            );
        }
        for (identifier, expected) in receipts.iter().skip(overflow) {
            assert_eq!(
                catalog.commands.get(identifier),
                Some(expected),
                "a receipt inside the window was archived"
            );
        }
        Ok(())
    }

    #[test]
    fn archived_identifier_cannot_be_inserted_again() -> Result<()> {
        let (authority, store, namespace) = fixture()?;
        let (mut root, receipts) = archived_catalog(
            &authority,
            &store,
            &namespace,
            super::super::super::INLINE_COMMANDS_MAX + 1,
            0,
        )?;
        let (archived, _) = &receipts[0];
        let error = insert_receipt(
            &authority,
            &mut root,
            archived,
            &"0".repeat(64),
            &serde_json::json!({"kind": "forged"}),
        )
        .expect_err("an archived identifier was inserted again");
        assert!(
            error
                .to_string()
                .contains("Conflict command already committed"),
            "unexpected error: {error}"
        );
        // A fresh identifier takes the same path without reading the archive.
        insert_receipt(
            &authority,
            &mut root,
            "fresh",
            &"1".repeat(64),
            &serde_json::json!({"kind": "fresh"}),
        )?;
        assert!(root
            .ownership
            .as_ref()
            .context("ownership missing")?
            .commands
            .contains_key("fresh"));
        Ok(())
    }

    #[test]
    fn command_archive_pointer_mutation_is_rejected() -> Result<()> {
        let (authority, _, _) = fixture()?;
        let mut previous = authority.snapshot()?.root;
        previous.version = 5;
        previous.ownership = Some(OwnershipCatalog {
            authority_term: 1,
            branches: BTreeMap::new(),
            commands: BTreeMap::new(),
            command_index: Some("a".repeat(64)),
        });
        let mut next = previous.clone();
        next.ownership
            .as_mut()
            .context("ownership missing")?
            .command_index = Some("b".repeat(64));
        let error =
            crate::authority::ownership::catalog_rules::prepare(&authority, &previous, &mut next)
                .expect_err("a transaction moved the command archive");
        assert!(
            error
                .to_string()
                .contains("command archive mutation forbidden"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[test]
    fn oversize_receipt_is_spooled_and_reads_back() -> Result<()> {
        let (authority, store, namespace) = fixture()?;
        let receipt = CommandReceipt {
            digest: blake3::hash(b"oversize").to_hex().to_string(),
            result: serde_json::json!({
                "payload": "x".repeat(super::super::super::COMMAND_ARCHIVE_VALUE_MAX + 1)
            }),
        };
        let bytes = serde_json::to_vec(&receipt)?;
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let mut root = authority.snapshot()?.root;
        root.version = 5;
        let mut catalog = OwnershipCatalog {
            authority_term: 1,
            ..OwnershipCatalog::default()
        };
        catalog.commands.insert("oversize".into(), receipt.clone());
        for index in 0..super::super::super::INLINE_COMMANDS_MAX {
            let identifier = format!("terminal-{index:04}");
            catalog.commands.insert(
                identifier.clone(),
                CommandReceipt {
                    digest: blake3::hash(identifier.as_bytes()).to_hex().to_string(),
                    result: serde_json::json!({"kind": "terminal"}),
                },
            );
        }
        root.ownership = Some(catalog);
        let index = PagedIndex::new(store.as_ref(), &namespace);
        authority.archive_commands(&mut root, &index)?;
        let pointer = root
            .ownership
            .as_ref()
            .context("ownership missing")?
            .command_index
            .clone()
            .context("commands were not archived")?;
        assert_eq!(
            index.get(Some(&pointer), "oversize")?,
            Some(serde_json::json!({"archive_object": hash.clone()})),
            "the oversize value was not replaced by its object digest"
        );
        assert_eq!(
            authority.archived_command(Some(&pointer), "oversize")?,
            Some(receipt)
        );
        // A spool object that no longer matches its digest is not a receipt.
        let key = namespace.key(&format!("authority/command-archive/{hash}"))?;
        store.put(&key, b"corrupted", crate::object_store::PutCondition::Any)?;
        assert!(authority
            .archived_command(Some(&pointer), "oversize")
            .is_err());
        Ok(())
    }
}
