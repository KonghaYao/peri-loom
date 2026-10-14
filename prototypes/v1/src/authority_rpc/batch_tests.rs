use super::*;
use crate::authority::Authority;
use crate::authority_rpc::client::AuthorityClient;
use crate::authority_rpc::protocol::{
    AuthorityCommand, SessionOperation, MAX_RPC_BYTES, MAX_SEGMENT_BATCH,
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
    assert!(raw.upload_segments(&[first.clone()]).is_err());
    assert!(readonly_store.upload_segments(&[first.clone()]).is_err());
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
            after.objects.get(&format!("segments/{}", entry.hash)),
            Some(entry)
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
            published.objects.get(&format!("segments/{}", entry.hash)),
            Some(entry)
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
