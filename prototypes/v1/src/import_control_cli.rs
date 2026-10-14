use crate::authority_rpc::client::AuthorityClient;
use crate::authority_rpc::protocol::{AuthorityCommand, ImportSegment, MAX_IMPORT_SEGMENT_BATCH};
use crate::manifest::{digest, valid_hash, Manifest, SEGMENT_SIZE};
use crate::runtime::DatabaseId;
use anyhow::{ensure, Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    authority_endpoint: String,
    control_token: Zeroizing<String>,
    database: DatabaseId,
    expected_head: String,
    destination: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    manifest_id: String,
    expected_head: String,
    next_index: usize,
    pending: bool,
}

pub fn run(arguments: &[String]) -> Result<()> {
    ensure!(
        arguments.len() == 2 && arguments[0] == "import-control",
        "usage: peri-loom-v1 import-control PRIVATE_FIXTURE"
    );
    let fixture_path = Path::new(&arguments[1]);
    private_file(fixture_path)?;
    let encoded = Zeroizing::new(fs::read(fixture_path)?);
    ensure!(encoded.len() <= 16 * 1024, "import fixture too large");
    let fixture: Fixture = serde_json::from_slice(&encoded)?;
    fixture.database.validate()?;
    ensure!(valid_hash(&fixture.expected_head), "invalid expected HEAD");
    private_directory(&fixture.destination)?;
    let names = fs::read_dir(fixture.destination.join("manifests"))?
        .collect::<std::io::Result<Vec<_>>>()?;
    ensure!(names.len() == 1, "staged image must have one manifest");
    let manifest_id = names[0]
        .file_name()
        .into_string()
        .map_err(|_| anyhow::anyhow!("invalid manifest name"))?;
    ensure!(valid_hash(&manifest_id), "invalid manifest name");
    let manifest_path = fixture.destination.join("manifests").join(&manifest_id);
    private_file(&manifest_path)?;
    let manifest_bytes = fs::read(&manifest_path)?;
    ensure!(
        digest(&manifest_bytes) == manifest_id,
        "manifest integrity failure"
    );
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
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
        "incomplete initial image"
    );
    let hashes = manifest.segments.values().collect::<Vec<_>>();
    let mut unique = BTreeSet::new();
    ensure!(
        hashes.iter().all(|hash| unique.insert(hash.as_str())),
        "duplicate image segments"
    );
    let checkpoint = fixture.destination.join("control-publication.json");
    let mut progress = if checkpoint.exists() {
        private_file(&checkpoint)?;
        serde_json::from_slice::<Progress>(&fs::read(&checkpoint)?)?
    } else {
        Progress {
            manifest_id: manifest_id.clone(),
            expected_head: fixture.expected_head.clone(),
            next_index: 0,
            pending: false,
        }
    };
    ensure!(
        progress.manifest_id == manifest_id
            && progress.expected_head == fixture.expected_head
            && progress.next_index <= hashes.len(),
        "import checkpoint mismatch"
    );
    ensure!(
        !progress.pending,
        "import outcome unknown; inspect authority catalog before retry"
    );
    let client = AuthorityClient::new(
        &fixture.authority_endpoint,
        fixture.control_token.to_string(),
    )?;
    while progress.next_index < hashes.len() {
        let end = (progress.next_index + MAX_IMPORT_SEGMENT_BATCH).min(hashes.len());
        let mut segments = Vec::with_capacity(end - progress.next_index);
        for hash in &hashes[progress.next_index..end] {
            let path = fixture.destination.join("segments").join(hash);
            private_file(&path)?;
            let bytes = fs::read(&path)?;
            ensure!(
                bytes.len() == SEGMENT_SIZE && digest(&bytes) == hash.as_str(),
                "staged segment integrity failure"
            );
            segments.push(ImportSegment {
                hash: hash.to_string(),
                data_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            });
        }
        progress.pending = true;
        write_progress(&checkpoint, &progress)?;
        let entries: Vec<crate::authority::ObjectEntry> =
            serde_json::from_value(client.call(AuthorityCommand::ImportSegments {
                database: fixture.database.clone(),
                expected_head: fixture.expected_head.clone(),
                segments,
            })?)?;
        ensure!(
            entries.len() == end - progress.next_index
                && entries
                    .iter()
                    .zip(&hashes[progress.next_index..end])
                    .all(|(entry, hash)| entry.hash == hash.as_str()),
            "import acknowledgement mismatch"
        );
        progress.pending = false;
        progress.next_index = end;
        write_progress(&checkpoint, &progress)?;
        if end % 512 == 0 || end == hashes.len() {
            eprintln!("registered {end}/{} encrypted segments", hashes.len());
        }
    }
    let writer_checkpoint = fixture.destination.join("publication.json");
    ensure!(
        !writer_checkpoint.exists(),
        "writer publication already started"
    );
    write_progress(
        &writer_checkpoint,
        &serde_json::json!({
            "manifest_id": manifest_id,
            "next_index": hashes.len(),
            "pending": false
        }),
    )?;
    println!(
        "{}",
        serde_json::json!({"ok":true,"manifest_id":manifest_id,"registered_segments":hashes.len()})
    );
    Ok(())
}

fn write_progress(path: &Path, progress: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension("pending");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    serde_json::to_writer(&mut file, progress)?;
    file.flush()?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    File::open(path.parent().context("import checkpoint parent missing")?)?.sync_all()?;
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "import path must be absolute");
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "import path must be a directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "import path must be owned and private"
        );
    }
    Ok(())
}

fn private_file(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "import path must be absolute");
    private_directory(path.parent().context("import input has no parent")?)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "import input must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "import input must be owned and private"
        );
    }
    Ok(())
}
