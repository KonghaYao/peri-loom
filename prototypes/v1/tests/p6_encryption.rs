#[path = "p6_encryption_support.rs"]
mod support;

use anyhow::{Context, Result};
use peri_loom_v1::{
    authority_rpc::{protocol::MaintenanceOperation, RemoteAuthorityProvider},
    database::RemoteDatabase,
    encryption::{BrokerDatabaseKeyProvider, DatabaseKeyProvider},
    object_store::ObjectStore,
    ownership::{
        types::{AccessRole, OwnershipCommand},
        OwnershipRuntime,
    },
    runtime::{DatabaseProvider, WorkerLimits, WorkerRuntime},
};
use std::{fs, path::Path, sync::atomic::Ordering};
use support::{
    harness::{identifier, Harness},
    Broker,
};

fn no_marker(path: &Path, marker: &[u8]) -> Result<usize> {
    let mut files = 0;
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            files += no_marker(&path, marker)?;
        } else {
            let bytes = fs::read(path)?;
            assert!(!bytes.windows(marker.len()).any(|window| window == marker));
            files += 1;
        }
    }
    Ok(files)
}

#[test]
fn native_encrypted_wal_main_ssd_overflow_multisession_cold_readonly_wrong_key() -> Result<()> {
    let service = Harness::new()?;
    let id = identifier("encrypted-native")?;
    let lease = service.grant(id.clone(), "worker-a", 1, 0)?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(lease.clone(), AccessRole::Writer)?;
    let broker = Broker::new(&service, remote.clone())?;
    let keys = broker.provider(&service, remote.clone())?;
    let cache = tempfile::tempdir()?;
    let worker = WorkerRuntime::new_with_key_provider(
        remote.clone(),
        cache.path(),
        WorkerLimits::default(),
        keys.clone(),
    )?;
    let marker = "P6_SECRET_MARKER_UNIQUE_918273";
    let first = worker.open(id.clone(), 1)?;
    for sql in [
        "PRAGMA cipher='none'",
        "PRAGMA cipher='aes256gcm'",
        "PRAGMA hexkey='0000000000000000000000000000000000000000000000000000000000000000'",
        "ATTACH ':memory:' AS unencrypted",
    ] {
        assert!(
            first.execute(sql).is_err(),
            "encrypted engine accepted reconfiguration: {sql}"
        );
    }
    first.execute("PRAGMA reserved_bytes=0; PRAGMA encryption_cipher='none'")?;
    assert_eq!(
        first.query("PRAGMA cipher")?[0][0],
        turso_core::Value::Text("aegis256".into())
    );
    first.execute("CREATE TABLE secrets(id INTEGER PRIMARY KEY, payload TEXT)")?;
    let second = worker.open(id.clone(), 1)?;
    assert!(second.execute("PRAGMA cipher='none'").is_err());
    let payload = marker.repeat(2000);
    first.execute(&format!("INSERT INTO secrets VALUES(1,'{payload}')"))?;
    assert_eq!(
        second.query("SELECT length(payload) FROM secrets")?.len(),
        1
    );
    let scoped = remote.open_store(&id, 1)?;
    assert!(!scoped
        .list(&format!("{}/", id.namespace.key("wal/chunks")?))?
        .is_empty());
    for key in service.raw.list("")? {
        let bytes = service.raw.get(&key)?.unwrap().bytes;
        assert!(!bytes
            .windows(marker.len())
            .any(|window| window == marker.as_bytes()));
    }
    drop(second);
    drop(first);
    worker.close(&id)?;
    let cold = tempfile::tempdir()?;
    let recovered = WorkerRuntime::new_with_key_provider(
        remote.clone(),
        cold.path(),
        WorkerLimits::default(),
        keys.clone(),
    )?;
    let first = recovered.open(id.clone(), 1)?;
    assert_eq!(
        first.query("SELECT payload FROM secrets")?[0][0],
        turso_core::Value::Text(payload.into())
    );
    first.checkpoint()?;
    first.compact_wal()?;
    assert!(no_marker(cold.path(), marker.as_bytes())? > 0);
    assert!(!scoped
        .list(&format!("{}/", id.namespace.key("segments")?))?
        .is_empty());
    for key in service.raw.list("")? {
        let bytes = service.raw.get(&key)?.unwrap().bytes;
        assert!(!bytes
            .windows(marker.len())
            .any(|window| window == marker.as_bytes()));
    }
    drop(first);
    recovered.close(&id)?;
    broker.mode.store(12, Ordering::SeqCst);
    let wrong = broker.provider(&service, remote.clone())?.resolve(&id, 1)?;
    let wrong_cache = tempfile::tempdir()?;
    let wrong_open = RemoteDatabase::open_with_encryption_and_limits(
        remote.open_store(&id, 1)?,
        id.namespace.clone(),
        wrong_cache.path(),
        1,
        8 << 20,
        64 << 20,
        wrong,
    );
    assert!(wrong_open
        .and_then(|database| database.query("SELECT payload FROM secrets"))
        .is_err());
    let missing_cache = tempfile::tempdir()?;
    assert!(RemoteDatabase::open(
        remote.open_store(&id, 1)?,
        id.namespace.clone(),
        missing_cache.path(),
        1
    )
    .and_then(|database| database.query("SELECT payload FROM secrets"))
    .is_err());
    broker.mode.store(0, Ordering::SeqCst);
    remote.install_lease(lease, AccessRole::ReadOnly)?;
    let readonly_cache = tempfile::tempdir()?;
    let readonly = WorkerRuntime::new_with_key_provider(
        remote.clone(),
        readonly_cache.path(),
        WorkerLimits::default(),
        keys,
    )?;
    let reader = readonly.open(id.clone(), 1)?;
    assert!(reader.is_readonly()?);
    assert!(reader.execute("PRAGMA cipher='none'").is_err());
    assert_eq!(reader.query("SELECT id FROM secrets")?.len(), 1);
    assert!(reader.execute("DELETE FROM secrets").is_err());
    service.clock.0.store(10101, Ordering::SeqCst);
    assert!(reader.query("SELECT id FROM secrets").is_err());
    assert!(reader.checkpoint().is_err());
    Ok(())
}

#[test]
fn broker_fails_closed_for_identity_freshness_mapping_and_missing_keys() -> Result<()> {
    for mode in [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26,
        27, 28, 30, 31, 32, 33, 34,
    ] {
        let service = Harness::new()?;
        let id = identifier("broker-denials")?;
        let lease = service.grant(id.clone(), "worker-a", 1, 0)?;
        let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
        remote.install_lease(lease, AccessRole::Writer)?;
        let broker = Broker::new(&service, remote.clone())?;
        let keys = broker.provider(&service, remote.clone())?;
        keys.resolve(&id, 1)?;
        broker.mode.store(mode, Ordering::SeqCst);
        assert!(keys.resolve(&id, 1).is_err(), "mode {mode} accepted");
        assert_eq!(
            broker.calls.load(Ordering::SeqCst),
            2,
            "hidden retry for mode {mode}"
        );
    }
    Ok(())
}

#[test]
fn descriptor_rewrap_and_concurrent_renewal_preserve_domain_but_revision_cannot_regress(
) -> Result<()> {
    let service = Harness::new()?;
    let id = identifier("rewrap-renew")?;
    let lease = service.grant(id.clone(), "worker-a", 1, 0)?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(lease, AccessRole::Writer)?;
    let broker = Broker::new(&service, remote.clone())?;
    let keys = broker.provider(&service, remote.clone())?;
    let original = keys.resolve(&id, 1)?;
    broker.mode.store(29, Ordering::SeqCst);
    let renewed = keys.resolve(&id, 1)?;
    assert_eq!(original.dek_id(), renewed.dek_id());
    assert_eq!(
        remote.installed_key_lease(&id, 1)?.binding.renewal_sequence,
        1
    );
    broker.mode.store(19, Ordering::SeqCst);
    let rewrapped = keys.resolve(&id, 1)?;
    assert_eq!(rewrapped.descriptor_revision(), 3);
    assert_eq!(original.domain_id(), rewrapped.domain_id());
    broker.mode.store(0, Ordering::SeqCst);
    assert!(keys.resolve(&id, 1).is_err());
    let other = identifier("different-namespace")?;
    remote.install_lease(
        service.grant(other.clone(), "worker-a", 1, 0)?,
        AccessRole::Writer,
    )?;
    assert!(keys.resolve(&other, 1).is_err());
    Ok(())
}

#[test]
fn native_ramio_uses_real_aegis256_and_ciphertext_pages() -> Result<()> {
    use std::sync::Arc;
    use turso_core::io::{Buffer, Completion, OpenFlags, IO};
    use turso_core::{
        Database, DatabaseOpts, EncryptionKey, EncryptionOpts, MemoryIO, OpenOptions, SqliteDialect,
    };
    let io = Arc::new(MemoryIO::new());
    let options = OpenOptions::new(Arc::new(SqliteDialect))
        .db_opts(DatabaseOpts::new().with_attach(false).with_encryption(true))
        .encryption(EncryptionOpts {
            cipher: "aegis256".into(),
            hexkey: "19".repeat(32),
        });
    let engine = Database::open(io.clone(), "p6-ram.db", options)?;
    let first = engine.connect_with_encryption(Some(EncryptionKey::new_256([0x19; 32])))?;
    first.wal_auto_actions_disable();
    first.execute("CREATE TABLE secrets(value TEXT)")?;
    let marker = "P6_RAM_SECRET_MARKER_54321";
    first.execute(format!(
        "INSERT INTO secrets VALUES('{}')",
        marker.repeat(2000)
    ))?;
    let second = engine.connect_with_encryption(Some(EncryptionKey::new_256([0x19; 32])))?;
    let mut query = second.query("SELECT value FROM secrets")?.unwrap();
    assert!(matches!(query.step()?, turso_core::StepResult::Row));
    drop(query);
    let wal = io.open_file("p6-ram.db-wal", OpenFlags::default(), false)?;
    let bytes = Arc::new(Buffer::new_temporary(wal.size()? as usize));
    drop(wal.pread(0, Completion::new_read(bytes.clone(), |_| None))?);
    assert!(bytes.len() > 4096);
    assert!(!bytes
        .as_slice()
        .windows(marker.len())
        .any(|window| window == marker.as_bytes()));
    first.checkpoint(turso_core::CheckpointMode::Full)?;
    let main = io.open_file("p6-ram.db", OpenFlags::default(), false)?;
    let bytes = Arc::new(Buffer::new_temporary(main.size()? as usize));
    drop(main.pread(0, Completion::new_read(bytes.clone(), |_| None))?);
    assert!(bytes.len() > 4096);
    assert_eq!(bytes.as_slice()[20], 48);
    assert!(!bytes
        .as_slice()
        .windows(marker.len())
        .any(|window| window == marker.as_bytes()));
    Ok(())
}

#[test]
fn broker_endpoint_is_literal_loopback_only() -> Result<()> {
    let service = Harness::new()?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    for endpoint in [
        "https://127.0.0.1/",
        "http://localhost/",
        "http://example.com/",
        "http://127.0.0.1/?secret=x",
        "http://127.0.0.1/#fragment",
        "http://user@127.0.0.1/",
        "http://127.0.0.1/internal/key",
    ] {
        assert!(BrokerDatabaseKeyProvider::new(
            remote.clone(),
            endpoint,
            "x".repeat(32),
            service.clock.clone()
        )
        .is_err());
    }
    assert!(BrokerDatabaseKeyProvider::new(
        remote,
        "http://127.0.0.1/",
        "short".into(),
        service.clock.clone()
    )
    .is_err());
    Ok(())
}

#[test]
fn snapshot_clone_and_pitr_share_encryption_domain_without_copying_dek() -> Result<()> {
    let service = Harness::new()?;
    let id = identifier("encrypted-refs")?;
    let lease = service.grant(id.clone(), "worker-a", 1, 0)?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(lease, AccessRole::Writer)?;
    let broker = Broker::new(&service, remote.clone())?;
    let keys = broker.provider(&service, remote.clone())?;
    let cache = tempfile::tempdir()?;
    let worker = WorkerRuntime::new_with_key_provider(
        remote.clone(),
        cache.path(),
        WorkerLimits::default(),
        keys.clone(),
    )?;
    let native = worker.open(id.clone(), 1)?;
    native.execute(
        "CREATE TABLE secrets(value TEXT); INSERT INTO secrets VALUES('snapshot-secret')",
    )?;
    let point = service.owner.maintenance(
        &id,
        "snapshot-p6",
        MaintenanceOperation::Snapshot {
            name: "sealed".into(),
        },
    )?;
    let sequence = point["sequence"].as_u64().unwrap();
    native.execute("INSERT INTO secrets VALUES('later-secret')")?;
    drop(native);
    worker.close(&id)?;
    service.owner.maintenance(
        &id,
        "clone-p6",
        MaintenanceOperation::Clone {
            snapshot: "sealed".into(),
            name: "clone".into(),
        },
    )?;
    let mut clone = id.clone();
    clone.branch = "clone".into();
    let grant = service
        .grant(clone.clone(), "worker-a", 2, 1)
        .context("grant cloned branch")?;
    remote.install_lease(grant, AccessRole::Writer)?;
    let main_key = keys.resolve(&id, 1).context("resolve main key")?;
    let clone_key = keys.resolve(&clone, 2).context("resolve clone key")?;
    assert_eq!(main_key.domain_id(), clone_key.domain_id());
    assert_eq!(main_key.dek_id(), clone_key.dek_id());
    assert_eq!(
        worker
            .open(clone.clone(), 2)
            .context("open clone engine")?
            .query("SELECT value FROM secrets")?
            .len(),
        1
    );
    worker.close(&clone)?;
    let pitr =
        service
            .owner
            .maintenance(&id, "pitr-p6", MaintenanceOperation::Pitr { sequence })?;
    assert_eq!(pitr["sequence"], sequence);
    service.owner.maintenance(
        &id,
        "restore-p6",
        MaintenanceOperation::Restore {
            snapshot: "sealed".into(),
        },
    )?;
    assert!(keys.resolve(&id, 1).is_err());
    let lease = service
        .grant(id.clone(), "worker-a", 2, 1)
        .context("grant restored branch")?;
    remote.install_lease(lease, AccessRole::Writer)?;
    assert_eq!(
        worker
            .open(id.clone(), 2)
            .context("open restored engine")?
            .query("SELECT value FROM secrets")?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn ownership_constructor_preserves_guard_and_kms_failure_never_opens_plaintext() -> Result<()> {
    let service = Harness::new()?;
    let id = identifier("owned-encrypted")?;
    let lease = service.grant(id.clone(), "worker-a", 1, 0)?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    let broker = Broker::new(&service, remote.clone())?;
    let keys = broker.provider(&service, remote.clone())?;
    let cache = tempfile::tempdir()?;
    let worker = OwnershipRuntime::new_with_key_provider(
        "worker-a".into(),
        remote.clone(),
        cache.path(),
        WorkerLimits::default(),
        service.clock.clone(),
        keys,
    )?;
    broker.mode.store(1, Ordering::SeqCst);
    assert!(worker
        .command(OwnershipCommand::AcquireDatabase {
            lease: lease.clone(),
            role: AccessRole::Writer
        })
        .is_err());
    assert!(!service
        .raw
        .list("")?
        .iter()
        .any(|key| key.contains("wal/chunks/")));
    broker.mode.store(0, Ordering::SeqCst);
    let next_lease = service.grant(id.clone(), "worker-a", 2, 1)?;
    worker.command(OwnershipCommand::AcquireDatabase {
        lease: next_lease,
        role: AccessRole::Writer,
    })?;
    service.clock.0.store(10101, Ordering::SeqCst);
    assert!(worker.open_session(&id).is_err());
    Ok(())
}

#[test]
fn readonly_fresh_database_refuses_native_initialization() -> Result<()> {
    let service = Harness::new()?;
    let id = identifier("readonly-empty")?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(
        service.grant(id.clone(), "worker-a", 1, 0)?,
        AccessRole::ReadOnly,
    )?;
    let broker = Broker::new(&service, remote.clone())?;
    let cache = tempfile::tempdir()?;
    let worker = WorkerRuntime::new_with_key_provider(
        remote.clone(),
        cache.path(),
        WorkerLimits::default(),
        broker.provider(&service, remote)?,
    )?;
    assert!(worker.open(id, 1).is_err());
    Ok(())
}
