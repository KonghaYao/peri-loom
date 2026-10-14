use crate::authority_rpc::{client::AuthorityClient, provider::RemoteAuthorityProvider};
use crate::encryption::{BrokerDatabaseKeyProvider, DatabaseKeyProvider};
use crate::import_image::stage_encrypted_image;
use crate::manifest::{digest, valid_hash, Head, Manifest, SEGMENT_SIZE};
use crate::object_store::PutCondition;
use crate::ownership::{
    types::{AccessRole, LeaseBinding},
    SystemLeaseClock,
};
use crate::runtime::DatabaseProvider;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
        arguments.len() == 2 && matches!(arguments[0].as_str(), "stage-image" | "publish-image"),
        "usage: peri-loom-v1 (stage-image|publish-image) PRIVATE_FIXTURE"
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
    if arguments[0] == "stage-image" {
        private_file(&fixture.source)?;
        private_destination(&fixture.destination)?;
    } else {
        private_directory(&fixture.destination)?;
    }
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
    let role = if arguments[0] == "stage-image" {
        AccessRole::ReadOnly
    } else {
        AccessRole::Writer
    };
    let lease = remote.validate_lease(&fixture.binding, role)?;
    remote.install_lease(lease, role)?;
    if arguments[0] == "publish-image" {
        return publish(&fixture, remote);
    }
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    manifest_id: String,
    next_index: usize,
    pending: bool,
}

fn publish(fixture: &Fixture, remote: Arc<RemoteAuthorityProvider>) -> Result<()> {
    let directory = &fixture.destination;
    let manifests = directory.join("manifests");
    let names = fs::read_dir(&manifests)?.collect::<std::io::Result<Vec<_>>>()?;
    ensure!(names.len() == 1, "staged image must contain one manifest");
    let identifier = names[0]
        .file_name()
        .into_string()
        .map_err(|_| anyhow::anyhow!("invalid manifest filename"))?;
    ensure!(
        valid_hash(&identifier),
        "invalid staged manifest identifier"
    );
    let manifest_path = manifests.join(&identifier);
    private_file(&manifest_path)?;
    let bytes = fs::read(&manifest_path)?;
    ensure!(
        digest(&bytes) == identifier,
        "staged manifest integrity failure"
    );
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    manifest.validate()?;
    ensure!(
        manifest.generation == 0
            && manifest.parent.is_none()
            && manifest.applied_lsn == 0
            && manifest.segments.len() as u64
                == manifest.logical_db_size.div_ceil(SEGMENT_SIZE as u64)
            && manifest
                .segments
                .keys()
                .copied()
                .eq(0..manifest.segments.len() as u64),
        "staged image is not a complete initial database image"
    );
    let mut unique = BTreeSet::new();
    ensure!(
        manifest.segments.values().all(|hash| unique.insert(hash)),
        "duplicate staged segments"
    );

    let store = remote.open_store(&fixture.binding.database, fixture.epoch)?;
    let namespace = &fixture.binding.database.namespace;
    ensure!(
        store.get(&namespace.key("wal/index")?)?.is_none(),
        "target has a WAL"
    );
    let (published_manifest, condition) = match store.get(&namespace.key("HEAD")?)? {
        None => (manifest.clone(), PutCondition::Absent),
        Some(head) => {
            let previous: Head = serde_json::from_slice(&head.bytes)?;
            ensure!(valid_hash(&previous.manifest), "invalid target manifest");
            let old = store
                .get(&namespace.key(&format!("manifests/{}", previous.manifest))?)?
                .context("target manifest missing")?;
            ensure!(
                digest(&old.bytes) == previous.manifest,
                "target manifest integrity failure"
            );
            let old: Manifest = serde_json::from_slice(&old.bytes)?;
            ensure!(
                old.generation == 0
                    && old.parent.is_none()
                    && old.logical_db_size == 0
                    && old.segments.is_empty()
                    && old.applied_lsn == 0,
                "target database is not an empty initial database"
            );
            let mut imported = manifest.clone();
            imported.generation = 1;
            imported.parent = Some(previous.manifest);
            (imported, PutCondition::Match(head.etag))
        }
    };
    let published_bytes = serde_json::to_vec(&published_manifest)?;
    let published_id = digest(&published_bytes);

    let checkpoint = directory.join("publication.json");
    let mut state = if checkpoint.exists() {
        private_file(&checkpoint)?;
        serde_json::from_slice::<Publication>(&fs::read(&checkpoint)?)?
    } else {
        Publication {
            manifest_id: identifier.clone(),
            next_index: 0,
            pending: false,
        }
    };
    ensure!(
        state.manifest_id == identifier && state.next_index <= manifest.segments.len(),
        "publication checkpoint mismatch"
    );
    ensure!(
        !state.pending,
        "publication outcome unknown; inspect authority state before retry"
    );
    let hashes = manifest.segments.values().collect::<Vec<_>>();
    while state.next_index < hashes.len() {
        let end = (state.next_index + crate::authority_rpc::protocol::MAX_SEGMENT_BATCH)
            .min(hashes.len());
        let mut segments = Vec::with_capacity(end - state.next_index);
        for hash in &hashes[state.next_index..end] {
            let path = directory.join("segments").join(hash);
            private_file(&path)?;
            let segment = fs::read(&path)?;
            ensure!(
                segment.len() == SEGMENT_SIZE && digest(&segment) == hash.as_str(),
                "staged segment integrity failure"
            );
            segments.push(segment);
        }
        state.pending = true;
        checkpoint_write(&checkpoint, &state)?;
        let entries = store.upload_segments(&segments)?;
        ensure!(
            entries.len() == segments.len(),
            "segment upload acknowledgement mismatch"
        );
        store.register_segments(&entries)?;
        state.next_index = end;
        state.pending = false;
        checkpoint_write(&checkpoint, &state)?;
        if end % 256 == 0 || end == hashes.len() {
            eprintln!("registered {end}/{} encrypted segments", hashes.len());
        }
    }
    state.pending = true;
    checkpoint_write(&checkpoint, &state)?;
    store.put(
        &namespace.key(&format!("manifests/{published_id}"))?,
        &published_bytes,
        PutCondition::Absent,
    )?;
    let head = serde_json::to_vec(&Head {
        manifest: published_id.clone(),
    })?;
    store.put(&namespace.key("HEAD")?, &head, condition)?;
    state.pending = false;
    checkpoint_write(&checkpoint, &state)?;
    println!(
        "{}",
        serde_json::json!({"ok":true,"manifest_id":published_id,"segments":hashes.len()})
    );
    Ok(())
}

fn checkpoint_write(path: &Path, state: &Publication) -> Result<()> {
    let pending = path.with_extension("pending");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&pending)?;
    serde_json::to_writer(&mut file, state)?;
    file.sync_all()?;
    fs::rename(pending, path)?;
    fs::File::open(path.parent().context("checkpoint parent missing")?)?.sync_all()?;
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

fn private_directory(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "staged image path must be absolute");
    private_parent(path)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "staged image must be a directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "staged image must be owned and private"
        );
    }
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
