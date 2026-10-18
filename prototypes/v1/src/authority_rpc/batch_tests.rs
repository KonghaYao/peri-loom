use super::*;
use crate::authority::Authority;
use crate::authority_rpc::client::AuthorityClient;
use crate::authority_rpc::protocol::{
    validate_segment_group, AuthorityCommand, SegmentGroupWrite, SessionOperation,
    MAX_GROUP_WRITES, MAX_RPC_BYTES, MAX_SEGMENT_BATCH,
};
use crate::authority_rpc::provider::RemoteAuthorityProvider;
use crate::manifest::{digest, SEGMENT_SIZE};
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::ownership::types::{
    GrantIntent, GrantReply, GrantRequest, LeaseIntent, NonceReply, RevokeRequest,
};
use crate::ownership::SystemLeaseClock;
use crate::runtime::DatabaseProvider;
use crate::wal::DurableWal;
use anyhow::Result;
use std::collections::BTreeMap;

#[test]
fn authenticated_batches_are_registered_only_by_live_writers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    let raw = Arc::new(MemoryObjectStore::default());
    let ownership = OwnershipAuthority::new(raw.clone(), Arc::new(SystemLeaseClock));
    let control_token = "a".repeat(64);
    let worker_token = "b".repeat(64);
    let other_token = "c".repeat(64);
    let auth = AuthConfig::new(
        control_token.clone(),
        BTreeMap::from([
            ("batch-worker".to_owned(), worker_token.clone()),
            ("other-worker".to_owned(), other_token.clone()),
        ]),
    )?;
    let server = Server::bind(
        "127.0.0.1:0".parse()?,
        ownership.clone(),
        auth,
        AuthorityStartup::acquire(directory.path(), false)?,
    )?;
    let endpoint = format!("http://{}/", server.local_addr());
    let handle = thread::spawn(move || server.run());
    let control = AuthorityClient::new(&endpoint, control_token)?;
    let worker = AuthorityClient::new(&endpoint, worker_token)?;
    let other = AuthorityClient::new(&endpoint, other_token)?;
    let database = DatabaseId::new(Namespace::new("batch", "database")?, "main")?;
    let intent = GrantIntent {
        role: AccessRole::Writer,
        database: database.clone(),
        worker_id: "batch-worker".to_owned(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "batch-lease".to_owned(),
        ttl_ms: 20_000,
    };
    let challenge: NonceReply =
        serde_json::from_value(control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Grant(intent.clone()),
        })?)?;
    let lease: GrantReply = serde_json::from_value(control.call(AuthorityCommand::Grant {
        request: GrantRequest {
            request_id: "batch-grant".to_owned(),
            nonce: challenge.nonce,
            intent,
        },
    })?)?;
    let writer = RemoteAuthorityProvider::new(worker.clone(), "batch-worker".to_owned())?;
    writer.install_lease(lease.clone(), AccessRole::Writer)?;
    let scoped = writer.open_store(&database, 1)?;
    let readonly = RemoteAuthorityProvider::new(worker.clone(), "batch-worker".to_owned())?;
    readonly.install_lease(lease.clone(), AccessRole::ReadOnly)?;
    let readonly_store = readonly.open_store(&database, 1)?;
    let readonly_session = worker.call(AuthorityCommand::OpenSession {
        binding: lease.binding.clone(),
        role: AccessRole::ReadOnly,
    })?["session"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(control
        .call(AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer,
        })
        .is_err());
    assert!(other
        .call(AuthorityCommand::OpenSession {
            binding: lease.binding.clone(),
            role: AccessRole::Writer,
        })
        .is_err());

    let first = vec![7; SEGMENT_SIZE];
    let second = vec![8; SEGMENT_SIZE];
    assert!(raw.upload_segments(std::slice::from_ref(&first)).is_err());
    assert!(readonly_store
        .upload_segments(std::slice::from_ref(&first))
        .is_err());
    assert!(worker
        .call(AuthorityCommand::Session {
            session: readonly_session.clone(),
            operation: SessionOperation::UploadSegments {
                segments: vec![first.clone()]
            },
        })
        .is_err());
    assert!(scoped.upload_segments(&[]).is_err());
    assert!(scoped
        .upload_segments(&[vec![0; SEGMENT_SIZE - 1]])
        .is_err());
    assert!(scoped
        .upload_segments(&vec![first.clone(); MAX_SEGMENT_BATCH + 1])
        .is_err());
    assert!(worker
        .call(AuthorityCommand::Session {
            session: readonly_session.clone(),
            operation: SessionOperation::UploadSegments {
                segments: vec![first.clone(); MAX_SEGMENT_BATCH + 1]
            },
        })
        .is_err());

    let entries = scoped.upload_segments(&[first.clone(), second.clone()])?;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].hash, digest(&first));
    assert_eq!(entries[1].hash, digest(&second));
    let authority = Authority::open(raw.clone(), database.namespace.clone())?;
    let before = authority.snapshot()?.root;
    assert!(!before
        .objects
        .contains_key(&format!("segments/{}", entries[0].hash)));
    assert_eq!(before.uploads.len(), 2);
    assert!(readonly_store.register_segments(&entries).is_err());
    assert!(worker
        .call(AuthorityCommand::Session {
            session: readonly_session.clone(),
            operation: SessionOperation::RegisterSegments {
                entries: entries.clone()
            },
        })
        .is_err());
    assert!(scoped.register_segments(&[]).is_err());
    assert!(scoped
        .register_segments(&vec![entries[0].clone(); MAX_SEGMENT_BATCH + 1])
        .is_err());
    assert!(other
        .call(AuthorityCommand::Session {
            session: readonly_session.clone(),
            operation: SessionOperation::RegisterSegments {
                entries: entries.clone()
            },
        })
        .is_err());
    scoped.register_segments(&entries)?;
    let after = authority.snapshot()?.root;
    assert!(after.uploads.is_empty());
    for (entry, bytes) in entries.iter().zip([&first, &second]) {
        assert_eq!(
            after.objects.get(&format!("segments/{}", entry.hash))?,
            Some(entry.clone())
        );
        assert_eq!(authority.read_blob(entry)?, *bytes);
    }
    let maximum: Vec<Vec<u8>> = (0..MAX_SEGMENT_BATCH)
        .map(|index| vec![index as u8 + 32; SEGMENT_SIZE])
        .collect();
    assert!(
        serde_json::to_vec(&SessionOperation::UploadSegments {
            segments: maximum.clone(),
        })?
        .len()
            < MAX_RPC_BYTES
    );
    let maximum_entries = scoped.upload_segments(&maximum)?;
    assert_eq!(maximum_entries.len(), MAX_SEGMENT_BATCH);
    scoped.register_segments(&maximum_entries)?;
    let published = authority.snapshot()?.root;
    for (entry, bytes) in maximum_entries.iter().zip(&maximum) {
        assert_eq!(
            published.objects.get(&format!("segments/{}", entry.hash))?,
            Some(entry.clone())
        );
        assert_eq!(authority.read_blob(entry)?, *bytes);
    }
    assert!(scoped
        .register_segments(&[crate::authority::ObjectEntry {
            hash: digest(&first),
            blob: "f".repeat(64),
            etag: "f".repeat(64),
            created_ms: 1,
        }])
        .is_err());
    // One durability group carries the segment body, its registration and the pin; the
    // caller's conditional manifest/HEAD writes stay outside it.
    let grouped = vec![0x5au8; SEGMENT_SIZE];
    let grouped_key = database
        .namespace
        .key(&format!("segments/{}", digest(&grouped)))?;
    let grouped_entry = scoped.put_group(&[crate::object_store::GroupWrite::absent(
        grouped_key.clone(),
        grouped.clone(),
    )])?;
    assert_eq!(grouped_entry.len(), 1);
    assert_eq!(
        scoped
            .get(&grouped_key)?
            .expect("grouped segment body missing")
            .bytes,
        grouped
    );
    assert!(readonly_store
        .put_group(&[crate::object_store::GroupWrite::absent(
            grouped_key,
            grouped.clone()
        )])
        .is_err());
    let grouped_root = authority.snapshot()?.root;
    assert!(grouped_root
        .objects
        .get(&format!("segments/{}", digest(&grouped)))?
        .is_some());
    assert!(grouped_root
        .uploads
        .get(
            &grouped_root
                .objects
                .get(&format!("segments/{}", digest(&grouped)))?
                .unwrap()
                .blob
        )?
        .is_none());
    control.call(AuthorityCommand::Revoke {
        request: RevokeRequest {
            request_id: "batch-revoke".to_owned(),
            database,
            expected_epoch: 1,
        },
    })?;
    assert!(scoped.upload_segments(&[first]).is_err());
    assert!(scoped.register_segments(&entries).is_err());
    worker.call(AuthorityCommand::CloseSession {
        session: readonly_session,
    })?;
    drop(readonly_store);
    drop(scoped);
    control.call(AuthorityCommand::Shutdown)?;
    handle.join().expect("authority server panicked")?;
    Ok(())
}

#[test]
fn wal_receipt_archive_round_trips_through_the_session_rpc() -> Result<()> {
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    let raw = Arc::new(MemoryObjectStore::default());
    let ownership = OwnershipAuthority::new(raw, Arc::new(SystemLeaseClock));
    let control_token = "a".repeat(64);
    let worker_token = "b".repeat(64);
    let auth = AuthConfig::new(
        control_token.clone(),
        BTreeMap::from([("archive-worker".to_owned(), worker_token.clone())]),
    )?;
    let server = Server::bind(
        "127.0.0.1:0".parse()?,
        ownership,
        auth,
        AuthorityStartup::acquire(directory.path(), false)?,
    )?;
    let endpoint = format!("http://{}/", server.local_addr());
    let handle = thread::spawn(move || server.run());
    let control = AuthorityClient::new(&endpoint, control_token)?;
    let worker = AuthorityClient::new(&endpoint, worker_token)?;
    let database = DatabaseId::new(Namespace::new("archive", "database")?, "main")?;
    let intent = GrantIntent {
        role: AccessRole::Writer,
        database: database.clone(),
        worker_id: "archive-worker".to_owned(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "archive-lease".to_owned(),
        ttl_ms: 20_000,
    };
    let challenge: NonceReply =
        serde_json::from_value(control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Grant(intent.clone()),
        })?)?;
    let lease: GrantReply = serde_json::from_value(control.call(AuthorityCommand::Grant {
        request: GrantRequest {
            request_id: "archive-grant".to_owned(),
            nonce: challenge.nonce,
            intent,
        },
    })?)?;
    let provider = RemoteAuthorityProvider::new(worker, "archive-worker".to_owned())?;
    provider.install_lease(lease, AccessRole::Writer)?;
    let namespace = database.namespace.clone();
    let wal = crate::wal::RemoteWal::new(provider.open_store(&database, 1)?);
    let request = |identity: &str, bytes: &[u8]| crate::wal::WalAppendRequest {
        namespace: namespace.clone(),
        epoch: 1,
        batch: crate::wal::WalBatch {
            operations: vec![crate::wal::WalOperation::Write {
                offset: 0,
                bytes: bytes.to_vec(),
            }],
        },
        position: None,
        append_id: identity.to_owned(),
        timestamp_millis: 9,
    };

    let archived = request("archived", &[1, 2, 3]);
    assert_eq!(wal.append_local(&archived)?, 1);
    assert_eq!(wal.append_local(&request("kept", &[4]))?, 2);

    wal.truncate(&namespace, 2)?;

    let index = wal.metadata(&namespace)?;
    assert!(!index.receipts.contains_key("archived"));
    assert!(index.receipts.contains_key("kept"));
    assert!(index.receipt_index.is_some());
    assert_eq!(wal.append_local(&archived)?, 1);
    assert_eq!(wal.metadata(&namespace)?.last_lsn, 2);

    let remote = provider.open_store(&database, 1)?;
    for ordinal in 3..=258 {
        assert_eq!(
            remote.append_wal(&request(&format!("segmented-{ordinal}"), &[ordinal as u8]))?,
            Some(ordinal)
        );
    }
    let segmented = crate::wal::RemoteWal::new(remote);
    let index = segmented.metadata(&namespace)?;
    assert!(index.entry_index.is_some());
    assert_eq!(index.entry_count, Some(258));
    assert!(index.entries.len() <= 256);
    assert_eq!(segmented.append_local(&request("kept", &[4]))?, 2);
    let reopened_store = provider.open_store(&database, 1)?;
    let reopened = crate::wal::RemoteWal::new(reopened_store.clone());
    assert_eq!(reopened.read(&namespace, 0)?.len(), 258);
    let bytes = reopened_store
        .get(&namespace.key("wal/index")?)?
        .unwrap()
        .bytes;
    assert_eq!(
        crate::recovery::validate_index_commit(&bytes, &namespace, |key| {
            Ok(reopened_store.get(key)?.unwrap().bytes)
        })?,
        None
    );
    drop(reopened);
    drop(reopened_store);
    drop(segmented);

    drop(wal);
    control.call(AuthorityCommand::Shutdown)?;
    handle.join().expect("authority server panicked")?;
    Ok(())
}

#[test]
fn segment_groups_require_full_content_addressed_bodies() -> Result<()> {
    let bytes = vec![0x77u8; SEGMENT_SIZE];
    let good = SegmentGroupWrite {
        key: format!("tenant/database/segments/{}", digest(&bytes)),
        bytes: bytes.clone(),
    };
    validate_segment_group(std::slice::from_ref(&good))?;
    let mut corrupt = good.clone();
    corrupt.bytes[0] ^= 1;
    assert!(validate_segment_group(&[corrupt]).is_err());
    let mut short = good.clone();
    short.bytes.pop();
    assert!(validate_segment_group(&[short]).is_err());
    let mut wrong_prefix = good.clone();
    wrong_prefix.key = "tenant/database/manifests/not-a-hash".to_owned();
    assert!(validate_segment_group(&[wrong_prefix]).is_err());
    let many = vec![good; MAX_GROUP_WRITES + 1];
    assert!(validate_segment_group(&many).is_err());
    Ok(())
}
