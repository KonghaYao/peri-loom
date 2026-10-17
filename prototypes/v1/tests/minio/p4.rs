use super::{enabled, TestBucket};
use std::{collections::BTreeMap, sync::Arc, thread};

use anyhow::{ensure, Result};
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    authority_rpc::{
        auth::AuthConfig,
        protocol::{AuthorityCommand, MaintenanceOperation},
        server::Server,
        startup::AuthorityStartup,
        AuthorityClient, RemoteAuthorityProvider,
    },
    database::RemoteDatabase,
    namespace::Namespace,
    object_store::{ObjectStore, S3ObjectStore},
    ownership::{
        types::{AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseIntent},
        SystemLeaseClock,
    },
    runtime::{DatabaseId, DatabaseProvider},
};

#[test]
#[ignore = "requires explicit PERI_S3_TEST=1 and an isolated S3 test bucket"]
fn network_legacy_receipts_migrate_to_bounded_root() -> Result<()> {
    ensure!(enabled(), "real S3 tests require explicit opt-in");
    let bucket = TestBucket::new()?;
    let mut strict_config = bucket.config.clone();
    strict_config.retries = 0;
    let raw: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(strict_config)?);
    let namespace = Namespace::new("tenant", "receipt-migration")?;
    let database = DatabaseId::new(namespace.clone(), "main")?;
    let authority = Authority::open(raw.clone(), namespace.clone())?;
    for _ in 0..100 {
        authority.transact(None, |_| Ok(()))?;
    }
    let before = authority.snapshot()?.root;
    let root_key = namespace.key("authority/root")?;
    let old_size = raw.get(&root_key)?.unwrap().bytes.len();
    OwnershipAuthority::new(raw.clone(), Arc::new(SystemLeaseClock)).state(&database)?;
    let after = authority.snapshot()?.root;
    ensure!(
        after.version == 5
            && after.receipts.len() == 1
            && raw.get(&root_key)?.unwrap().bytes.len() < old_size / 2,
        "S3 receipt migration did not bound the online root"
    );
    for (id, revision) in before.receipts {
        ensure!(
            authority.receipt(&id)? == Some(revision),
            "S3 receipt lost during migration"
        );
    }
    Ok(())
}

fn grant(
    client: &AuthorityClient,
    database: &DatabaseId,
    worker: &str,
    epoch: u64,
    expected_epoch: u64,
    role: AccessRole,
) -> Result<GrantReply> {
    let intent = GrantIntent {
        database: database.clone(),
        worker_id: worker.into(),
        epoch,
        expected_epoch,
        lease_id: format!("live-lease-{epoch}"),
        ttl_ms: 60000,
        role,
    };
    let nonce = client.call(AuthorityCommand::Challenge {
        intent: LeaseIntent::Grant(intent.clone()),
    })?;
    Ok(serde_json::from_value(client.call(
        AuthorityCommand::Grant {
            request: GrantRequest {
                request_id: format!("live-grant-{epoch}"),
                nonce: nonce["nonce"].as_str().unwrap().into(),
                intent,
            },
        },
    )?)?)
}

#[test]
#[ignore = "requires explicit PERI_S3_TEST=1 or PERI_MINIO_TEST=1 and S3 test credentials"]
fn network_authority_fences_two_remote_workers_and_preserves_restored_snapshot() -> Result<()> {
    ensure!(enabled(), "real S3 tests require explicit opt-in");
    let bucket = TestBucket::new()?;
    let mut strict_config = bucket.config.clone();
    strict_config.retries = 0;
    let raw: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(strict_config)?);
    assert!(raw.supports_strict_fencing());
    assert!(!bucket.store.supports_strict_fencing());
    let state = tempfile::tempdir()?;
    let startup = AuthorityStartup::acquire(state.path().join("authority"), false)?;
    let control_token = "live-fixture-control-token-not-a-secret";
    let first_token = "live-fixture-first-worker-token-not-a-secret";
    let second_token = "live-fixture-second-worker-token-not-a-secret";
    let auth = AuthConfig::new(
        control_token.into(),
        BTreeMap::from([
            ("first".into(), first_token.into()),
            ("second".into(), second_token.into()),
        ]),
    )?;
    let ownership = OwnershipAuthority::new(raw, Arc::new(SystemLeaseClock));
    let server = Server::bind("127.0.0.1:0".parse()?, ownership, auth, startup)?;
    let endpoint = format!("http://{}/", server.local_addr());
    let task = thread::spawn(move || server.run());
    let control = AuthorityClient::new(&endpoint, control_token.into())?;
    let result: Result<()> = (|| {
        let database = DatabaseId::new(Namespace::new("tenant", "ownership-live")?, "main")?;
        let first = RemoteAuthorityProvider::new(
            AuthorityClient::new(&endpoint, first_token.into())?,
            "first".into(),
        )?;
        let second = RemoteAuthorityProvider::new(
            AuthorityClient::new(&endpoint, second_token.into())?,
            "second".into(),
        )?;
        let first_grant = grant(&control, &database, "first", 10, 0, AccessRole::Writer)?;
        first.install_lease(first_grant, AccessRole::Writer)?;
        let first_cache = tempfile::tempdir()?;
        let writer = RemoteDatabase::open(
            first.open_store(&database, 10)?,
            database.namespace.clone(),
            first_cache.path(),
            10,
        )?;
        writer
            .execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
        control.call(AuthorityCommand::Maintenance {
            request_id: "live-snapshot".into(),
            database: database.clone(),
            operation: MaintenanceOperation::Snapshot {
                name: "before".into(),
            },
        })?;
        writer.execute("BEGIN; INSERT INTO items VALUES(3)")?;
        let second_grant = grant(&control, &database, "second", 11, 10, AccessRole::Writer)?;
        second.install_lease(second_grant, AccessRole::Writer)?;
        let second_cache = tempfile::tempdir()?;
        let replacement = RemoteDatabase::open(
            second.open_store(&database, 11)?,
            database.namespace.clone(),
            second_cache.path(),
            11,
        )?;
        assert!(writer.query("SELECT * FROM items").is_err());
        assert!(writer.execute("COMMIT").is_err());
        assert!(writer.checkpoint().is_err());
        assert!(writer.compact_wal().is_err());
        replacement.execute("INSERT INTO items VALUES(2)")?;
        replacement.compact_wal()?;
        assert_eq!(
            replacement.query("SELECT count(*) FROM items")?[0][0].to_string(),
            "2"
        );
        drop(writer);
        drop(replacement);
        first_cache.close()?;
        second_cache.close()?;
        control.call(AuthorityCommand::Maintenance {
            request_id: "live-restore".into(),
            database: database.clone(),
            operation: MaintenanceOperation::Restore {
                snapshot: "before".into(),
            },
        })?;
        let reader_grant = grant(&control, &database, "second", 12, 11, AccessRole::ReadOnly)?;
        assert!(second
            .install_lease(reader_grant.clone(), AccessRole::Writer)
            .is_err());
        second.install_lease(reader_grant, AccessRole::ReadOnly)?;
        let fresh = tempfile::tempdir()?;
        let reader = RemoteDatabase::open(
            second.open_store(&database, 12)?,
            database.namespace,
            fresh.path(),
            12,
        )?;
        assert!(reader.is_readonly()?);
        assert_eq!(reader.query("SELECT id FROM items")?[0][0].to_string(), "1");
        assert_eq!(
            reader.query("PRAGMA integrity_check")?[0][0].to_string(),
            "ok"
        );
        assert!(reader
            .query("INSERT INTO items VALUES(4) RETURNING id")
            .is_err());
        drop(reader);
        Ok(())
    })();
    let shutdown = control.call(AuthorityCommand::Shutdown);
    let joined = task.join().unwrap();
    result?;
    shutdown?;
    joined?;
    eprintln!("PASS real S3 P4: retry-free credential authority, remote-only workers epoch10/11 fencing, stale cached SQL/COMMIT/checkpoint refusal, native readonly epoch12 and restored snapshot integrity");
    Ok(())
}
