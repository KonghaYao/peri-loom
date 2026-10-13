#[path = "p6_encryption_support.rs"]
mod support;

use anyhow::{ensure, Result};
use peri_loom_v1::manifest::{digest, Manifest, SEGMENT_SIZE};
use peri_loom_v1::object_store::ObjectStore;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::atomic::Ordering;
use support::{
    harness::{identifier, private_directory, Harness},
    Broker,
};
use turso_core::{CipherMode, EncryptionContext, EncryptionKey};

fn synthetic_image(root: &Path) -> Result<std::path::PathBuf> {
    let source = root.join("synthetic.db");
    let output = Command::new("sqlite3")
        .arg(&source)
        .arg("CREATE TABLE synthetic(id INTEGER PRIMARY KEY, body BLOB); INSERT INTO synthetic SELECT value, zeroblob(2048) FROM generate_series(1,135);")
        .output()?;
    ensure!(output.status.success(), "synthetic SQLite setup failed");
    let image = root.join("image.db");
    let output = Command::new("sqlite3")
        .arg("-cmd")
        .arg(".filectrl reserve_bytes 48")
        .arg(&source)
        .arg(format!("VACUUM INTO '{}'", image.display()))
        .output()?;
    ensure!(
        output.status.success(),
        "synthetic SQLite conversion failed"
    );
    let output = Command::new("sqlite3")
        .arg(&image)
        .arg("PRAGMA quick_check; SELECT count(*) FROM synthetic;")
        .output()?;
    ensure!(
        output.status.success() && output.stdout == b"ok\n135\n",
        "synthetic image validation failed"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&image, fs::Permissions::from_mode(0o600))?;
    }
    Ok(image)
}

fn private_fixture(path: &Path, config: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec(config)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn invoke(path: &Path) -> Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_peri-loom-v1"))
        .arg("stage-image")
        .arg(path)
        .output()?)
}

#[test]
fn fixture_lease_broker_and_synthetic_image_only_publish_local_staging() -> Result<()> {
    let service = Harness::new()?;
    let database = identifier("stage-only")?;
    let lease = service.grant(database, "worker-a", 1, 0)?;
    let remote = peri_loom_v1::authority_rpc::RemoteAuthorityProvider::new(
        service.worker.clone(),
        "worker-a".into(),
    )?;
    remote.install_lease(
        lease.clone(),
        peri_loom_v1::ownership::types::AccessRole::ReadOnly,
    )?;
    let broker = Broker::new(&service, remote)?;
    let private = private_directory()?;
    let image = synthetic_image(private.path())?;
    let destination = private.path().join("staged");
    let fixture = private.path().join("fixture.json");
    let config = json!({
        "authority_endpoint": service.endpoint,
        "authority_token": service.worker_token,
        "broker_endpoint": broker.endpoint,
        "broker_token": broker.token,
        "binding": lease.binding,
        "epoch": lease.binding.epoch,
        "source": image,
        "destination": destination,
    });
    private_fixture(&fixture, &config)?;
    let prior = service.raw.list("")?;
    let output = invoke(&fixture)?;
    assert!(output.status.success(), "stage-image failed");
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    let reply: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(reply["ok"], true);
    assert_eq!(reply.as_object().map(|fields| fields.len()), Some(2));
    assert!(output.stderr.is_empty());
    assert_eq!(service.raw.list("")?, prior);
    let id = reply["manifest_id"].as_str().unwrap();
    let manifest_bytes = fs::read(destination.join("manifests").join(id))?;
    assert_eq!(digest(&manifest_bytes), id);
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    let image_bytes = fs::read(&image)?;
    let page_size = manifest.page_size as usize;
    let context = EncryptionContext::new(
        CipherMode::Aegis256,
        &EncryptionKey::new_256([0x19; 32]),
        page_size,
    )?;
    let mut recovered = Vec::new();
    for (segment_index, hash) in &manifest.segments {
        let bytes = fs::read(destination.join("segments").join(hash))?;
        assert_eq!(bytes.len(), SEGMENT_SIZE);
        assert_eq!(digest(&bytes), *hash);
        for (within, page) in bytes.chunks_exact(page_size).enumerate() {
            let page_number = *segment_index as usize * (SEGMENT_SIZE / page_size) + within + 1;
            if (page_number - 1) * page_size < image_bytes.len() {
                recovered.extend(context.decrypt_page(page, page_number)?);
            }
        }
    }
    assert_eq!(recovered, image_bytes);

    let rejected = private.path().join("rejected");
    let mut wrong = config;
    wrong["epoch"] = json!(2);
    wrong["destination"] = json!(rejected);
    private_fixture(&fixture, &wrong)?;
    let failure = invoke(&fixture)?;
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    assert!(!rejected.exists());
    assert_eq!(service.raw.list("")?, prior);
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);

    let mut forged = wrong;
    forged["epoch"] = json!(lease.binding.epoch);
    forged["binding"]["worker_id"] = json!("worker-b");
    private_fixture(&fixture, &forged)?;
    let failure = invoke(&fixture)?;
    assert!(!failure.status.success());
    assert!(failure.stdout.is_empty());
    assert!(!rejected.exists());
    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn unsafe_fixture_is_rejected_before_credentials_are_used() -> Result<()> {
    let root = private_directory()?;
    let fixture = root.path().join("fixture.json");
    fs::write(&fixture, b"{}")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o644))?;
    }
    let output = invoke(&fixture)?;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    Ok(())
}
