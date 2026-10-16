use super::*;
use crate::authority::{Authority, ObjectEntry};
use crate::authority_rpc::client::AuthorityClient;
use crate::authority_rpc::protocol::{ImportSegment, MAX_IMPORT_SEGMENT_BATCH};
use crate::authority_rpc::provider::RemoteAuthorityProvider;
use crate::manifest::{digest, Manifest, SEGMENT_SIZE};
use crate::namespace::Namespace;
use crate::object_store::{MemoryObjectStore, PutCondition};
use crate::ownership::types::{
    GrantIntent, GrantReply, GrantRequest, LeaseClock, LeaseIntent, NonceReply,
};
use crate::runtime::DatabaseProvider;
use base64::Engine;
use std::sync::atomic::AtomicU64;

struct Clock(AtomicU64);

impl LeaseClock for Clock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

fn segment(fill: u8) -> ImportSegment {
    let bytes = vec![fill; SEGMENT_SIZE];
    ImportSegment {
        hash: digest(&bytes),
        data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
    }
}

#[test]
fn control_imports_verified_batches_after_writer_expiry_without_changing_head() -> Result<()> {
    let directory = tempfile::tempdir()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))?;
    }
    let raw = Arc::new(MemoryObjectStore::default());
    let clock = Arc::new(Clock(AtomicU64::new(1_000_000)));
    let ownership = OwnershipAuthority::new(raw.clone(), clock.clone());
    let server = Server::bind(
        "127.0.0.1:0".parse()?,
        ownership,
        AuthConfig::new(
            "a".repeat(64),
            BTreeMap::from([("worker".into(), "b".repeat(64))]),
        )?,
        AuthorityStartup::acquire(directory.path(), false)?,
    )?;
    let endpoint = format!("http://{}/", server.local_addr());
    let handle = thread::spawn(move || server.run());
    let control = AuthorityClient::new(&endpoint, "a".repeat(64))?;
    let worker = AuthorityClient::new(&endpoint, "b".repeat(64))?;
    let unknown = AuthorityClient::new(&endpoint, "c".repeat(64))?;
    let database = DatabaseId::new(Namespace::new("import", "synthetic")?, "main")?;
    let intent = GrantIntent {
        role: AccessRole::Writer,
        database: database.clone(),
        worker_id: "worker".into(),
        epoch: 1,
        expected_epoch: 0,
        lease_id: "lease".into(),
        ttl_ms: 20_000,
    };
    let challenge: NonceReply =
        serde_json::from_value(control.call(AuthorityCommand::Challenge {
            intent: LeaseIntent::Grant(intent.clone()),
        })?)?;
    let grant: GrantReply = serde_json::from_value(control.call(AuthorityCommand::Grant {
        request: GrantRequest {
            request_id: "import-grant".into(),
            nonce: challenge.nonce,
            intent,
        },
    })?)?;
    let provider = RemoteAuthorityProvider::new(worker.clone(), "worker".into())?;
    provider.install_lease(grant, AccessRole::Writer)?;
    let store = provider.open_store(&database, 1)?;
    let empty_bytes = serde_json::to_vec(&Manifest::default())?;
    let expected_head = digest(&empty_bytes);
    store.put(
        &database
            .namespace
            .key(&format!("manifests/{expected_head}"))?,
        &empty_bytes,
        PutCondition::Absent,
    )?;
    store.put(
        &database.namespace.key("HEAD")?,
        &serde_json::to_vec(&serde_json::json!({"manifest": expected_head}))?,
        PutCondition::Absent,
    )?;
    drop(store);
    let authority = Authority::open(raw, database.namespace.clone())?;
    let original = authority.snapshot()?.root;
    let import = |segments| AuthorityCommand::ImportSegments {
        database: database.clone(),
        expected_head: expected_head.clone(),
        segments,
    };
    let before = vec![segment(1), segment(2)];
    assert!(worker.call(import(before.clone())).is_err());
    assert!(unknown.call(import(before.clone())).is_err());
    assert!(control.call(import(vec![])).is_err());
    assert!(control
        .call(import(vec![segment(1); MAX_IMPORT_SEGMENT_BATCH + 1]))
        .is_err());
    let mut tampered = segment(3);
    tampered.hash = digest(&vec![4; SEGMENT_SIZE]);
    assert!(control.call(import(vec![segment(1), tampered])).is_err());
    let mut invalid = segment(3);
    invalid.data_base64.replace_range(0..1, "!");
    assert!(control.call(import(vec![invalid])).is_err());
    assert!(control
        .call(AuthorityCommand::ImportSegments {
            database: database.clone(),
            expected_head: "0".repeat(64),
            segments: vec![segment(1)],
        })
        .is_err());
    let rejected = authority.snapshot()?.root;
    assert_eq!(rejected.branches, original.branches);
    assert_eq!(rejected.objects, original.objects);
    assert_eq!(rejected.uploads, original.uploads);

    clock.0.store(1_050_000, Ordering::SeqCst);
    let entries: Vec<ObjectEntry> = serde_json::from_value(control.call(import(before.clone()))?)?;
    assert_eq!(entries.len(), 2);
    let after = authority.snapshot()?.root;
    assert_eq!(after.branches, original.branches);
    assert!(after.branches["main"].wal.is_none());
    for (entry, source) in entries.iter().zip(before.iter()) {
        assert_eq!(entry.hash, source.hash);
        assert_eq!(
            after.objects.get(&format!("segments/{}", entry.hash))?,
            Some(entry.clone())
        );
        assert_eq!(digest(&authority.read_blob(entry)?), source.hash);
    }
    assert!(worker.call(import(vec![segment(9)])).is_err());
    control.call(AuthorityCommand::Shutdown)?;
    handle.join().expect("authority server panicked")?;
    Ok(())
}
