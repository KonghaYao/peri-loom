#[path = "p4_remote_helpers/harness.rs"]
mod harness;

use anyhow::{ensure, Result};
use harness::{identifier, private_directory, Harness};
use peri_loom_v1::authority_rpc::RemoteAuthorityProvider;
use peri_loom_v1::import_image::stage_encrypted_image;
use peri_loom_v1::manifest::{digest, Manifest, SEGMENT_SIZE};
use peri_loom_v1::object_store::PutCondition;
use peri_loom_v1::ownership::types::AccessRole;
use peri_loom_v1::runtime::DatabaseProvider;
use serde_json::{json, Value};
use std::fs;
use std::process::Command;
use turso_core::EncryptionKey;

#[test]
fn private_control_import_registers_staged_segments_without_writer_lease() -> Result<()> {
    let service = Harness::new()?;
    let database = identifier("import-control-cli")?;
    let grant = service.grant(database.clone(), "worker-a", 1, 0)?;
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(grant.clone(), AccessRole::Writer)?;
    let store = remote.open_store(&database, grant.binding.epoch)?;
    let empty = serde_json::to_vec(&Manifest::default())?;
    let initial = digest(&empty);
    store.put(
        &database.namespace.key(&format!("manifests/{initial}"))?,
        &empty,
        PutCondition::Absent,
    )?;
    store.put(
        &database.namespace.key("HEAD")?,
        &serde_json::to_vec(&json!({"manifest": initial}))?,
        PutCondition::Absent,
    )?;
    drop(store);

    let private = private_directory()?;
    let plain = private.path().join("plain.db");
    let created = Command::new("sqlite3")
        .arg(&plain)
        .arg("CREATE TABLE trial(id INTEGER PRIMARY KEY, value BLOB); INSERT INTO trial(value) VALUES (zeroblob(180000));")
        .output()?;
    ensure!(created.status.success(), "SQLite fixture failed");
    let converted = private.path().join("converted.db");
    let transformed = Command::new("sqlite3")
        .arg("-cmd")
        .arg(".filectrl reserve_bytes 48")
        .arg(&plain)
        .arg(format!("VACUUM INTO '{}'", converted.display()))
        .output()?;
    ensure!(
        transformed.status.success(),
        "SQLite reserve conversion failed"
    );
    let staged = private.path().join("staged");
    let image = stage_encrypted_image(&converted, &staged, &EncryptionKey::new_256([19; 32]))?;
    ensure!(
        image.manifest.segments.len() > 1,
        "fixture needs multiple segments"
    );

    let fixture = private.path().join("fixture.json");
    let credential = json!({
        "authority_endpoint": service.endpoint,
        "control_token": service.control_token,
        "database": database,
        "expected_head": initial,
        "destination": staged,
    });
    fs::write(&fixture, serde_json::to_vec(&credential)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o600))?;
    }
    let output = Command::new(env!("CARGO_BIN_EXE_peri-loom-v1"))
        .arg("import-control")
        .arg(&fixture)
        .output()?;
    ensure!(
        output.status.success(),
        "control import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(reply["registered_segments"], image.manifest.segments.len());
    let writer_checkpoint: Value =
        serde_json::from_slice(&fs::read(staged.join("publication.json"))?)?;
    assert_eq!(
        writer_checkpoint.as_object().map(|fields| fields.len()),
        Some(3)
    );
    assert_eq!(
        writer_checkpoint["next_index"],
        image.manifest.segments.len()
    );
    let remote = RemoteAuthorityProvider::new(service.worker.clone(), "worker-a".into())?;
    remote.install_lease(grant.clone(), AccessRole::ReadOnly)?;
    let store = remote.open_store(&database, grant.binding.epoch)?;
    for hash in image.manifest.segments.values() {
        let item = store
            .get(&database.namespace.key(&format!("segments/{hash}"))?)?
            .unwrap();
        assert_eq!(item.bytes.len(), SEGMENT_SIZE);
        assert_eq!(digest(&item.bytes), *hash);
    }
    assert_eq!(
        serde_json::from_slice::<Value>(
            &store.get(&database.namespace.key("HEAD")?)?.unwrap().bytes
        )?["manifest"],
        initial
    );
    Ok(())
}
