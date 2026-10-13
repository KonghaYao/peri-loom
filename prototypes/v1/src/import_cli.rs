use crate::authority_rpc::{client::AuthorityClient, provider::RemoteAuthorityProvider};
use crate::encryption::{BrokerDatabaseKeyProvider, DatabaseKeyProvider};
use crate::import_image::stage_encrypted_image;
use crate::ownership::{
    types::{AccessRole, LeaseBinding},
    SystemLeaseClock,
};
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    authority_endpoint: String,
    authority_token: Zeroizing<String>,
    broker_endpoint: String,
    broker_token: Zeroizing<String>,
    binding: LeaseBinding,
    epoch: u64,
    source: PathBuf,
    destination: PathBuf,
}

pub fn run(arguments: &[String]) -> Result<()> {
    ensure!(
        arguments.len() == 2 && arguments[0] == "stage-image",
        "usage: peri-loom-v1 stage-image PRIVATE_FIXTURE"
    );
    let fixture_path = Path::new(&arguments[1]);
    private_file(fixture_path)?;
    let fixture_bytes = Zeroizing::new(fs::read(fixture_path)?);
    ensure!(
        fixture_bytes.len() <= 16 * 1024,
        "stage-image fixture too large"
    );
    let fixture: Fixture =
        serde_json::from_slice(&fixture_bytes).context("invalid stage-image fixture")?;
    private_file(&fixture.source)?;
    private_destination(&fixture.destination)?;
    fixture.binding.database.validate()?;
    ensure!(
        fixture.epoch > 0 && fixture.binding.epoch == fixture.epoch,
        "stage-image epoch mismatch"
    );

    let client = AuthorityClient::new(
        &fixture.authority_endpoint,
        fixture.authority_token.to_string(),
    )?;
    let remote = RemoteAuthorityProvider::new(client, fixture.binding.worker_id.clone())?;
    let lease = remote.validate_lease(&fixture.binding, AccessRole::ReadOnly)?;
    remote.install_lease(lease, AccessRole::ReadOnly)?;
    let provider = BrokerDatabaseKeyProvider::new(
        remote,
        &fixture.broker_endpoint,
        fixture.broker_token.to_string(),
        Arc::new(SystemLeaseClock),
    )?;
    let resolved = provider.resolve(&fixture.binding.database, fixture.epoch)?;
    resolved.validate_binding(&fixture.binding, AccessRole::ReadOnly)?;
    let staged = stage_encrypted_image(
        &fixture.source,
        &fixture.destination,
        &resolved.native_key(),
    )?;
    let mut output = io::stdout().lock();
    serde_json::to_writer(
        &mut output,
        &serde_json::json!({"ok": true, "manifest_id": staged.manifest_id}),
    )?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn private_file(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "stage-image paths must be absolute");
    private_parent(path)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "stage-image input must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "stage-image input must be owned and private"
        );
    }
    Ok(())
}

fn private_destination(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "stage-image paths must be absolute");
    private_parent(path)?;
    ensure!(
        matches!(fs::symlink_metadata(path), Err(error) if error.kind() == io::ErrorKind::NotFound),
        "stage-image destination must not exist"
    );
    Ok(())
}

fn private_parent(path: &Path) -> Result<()> {
    let parent = path.parent().context("stage-image path has no parent")?;
    let metadata = fs::metadata(parent)?;
    ensure!(metadata.is_dir(), "stage-image parent is not a directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "stage-image parent must be owned and private"
        );
    }
    Ok(())
}
