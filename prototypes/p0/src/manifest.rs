use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const SEGMENT_SIZE: u32 = 128 * 1024;

#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WalPosition {
    pub salts: [u32; 2],
    pub frame_count: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub generation: u64,
    pub logical_db_size: u64,
    pub page_size: u32,
    pub segment_size: u32,
    pub segments: BTreeMap<u64, String>,
    pub applied_wal_position: WalPosition,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            generation: 0,
            logical_db_size: 0,
            page_size: 4096,
            segment_size: SEGMENT_SIZE,
            segments: BTreeMap::new(),
            applied_wal_position: WalPosition::default(),
        }
    }
}

impl Manifest {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.segment_size == SEGMENT_SIZE,
            "unsupported segment size"
        );
        ensure!(
            self.page_size.is_power_of_two() && (512..=65536).contains(&self.page_size),
            "invalid page size"
        );
        let segment_count = self.logical_db_size.div_ceil(u64::from(SEGMENT_SIZE));
        for (index, name) in &self.segments {
            ensure!(
                *index < segment_count,
                "segment index outside logical database"
            );
            ensure!(valid_segment_name(name), "invalid segment object name");
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    generation: u64,
    manifest: String,
    sha256: String,
}

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|character| character.is_ascii_digit() || (b'a'..=b'f').contains(&character))
}

pub(crate) fn valid_segment_name(name: &str) -> bool {
    name.strip_suffix(".seg").is_some_and(valid_digest)
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn temporary_file(directory: &Path) -> Result<(PathBuf, File)> {
    loop {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(".tmp-{}-{sequence}", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("create temporary object"),
        }
    }
}

pub(crate) fn write_immutable(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let destination = directory.join(name);
    match fs::read(&destination) {
        Ok(existing) => {
            ensure!(existing == bytes, "immutable object is corrupt: {name}");
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("read existing immutable object"),
    }
    let (temporary, mut file) = temporary_file(directory)?;
    let result = (|| {
        file.write_all(bytes).context("write immutable object")?;
        file.sync_all().context("sync immutable object")?;
        match fs::hard_link(&temporary, &destination) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    fs::read(&destination).context("read concurrent immutable object")? == bytes,
                    "immutable object is corrupt: {name}"
                );
            }
            Err(error) => return Err(error).context("install immutable object"),
        }
        sync_directory(directory)
    })();
    drop(file);
    let _ = fs::remove_file(&temporary);
    result
}

pub(crate) fn load(root: &Path) -> Result<Option<Manifest>> {
    let bytes = match fs::read(root.join("HEAD")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("read HEAD"),
    };
    let head: Head = serde_json::from_slice(&bytes).context("decode HEAD")?;
    ensure!(
        valid_digest(&head.sha256),
        "invalid manifest digest in HEAD"
    );
    let expected_name = format!("{:020}-{}.json", head.generation, head.sha256);
    ensure!(
        head.manifest == expected_name,
        "invalid manifest name in HEAD"
    );
    let bytes =
        fs::read(root.join("manifests").join(&head.manifest)).context("read HEAD manifest")?;
    ensure!(digest(&bytes) == head.sha256, "manifest checksum mismatch");
    let manifest: Manifest = serde_json::from_slice(&bytes).context("decode manifest")?;
    ensure!(
        manifest.generation == head.generation,
        "manifest generation mismatch"
    );
    manifest.validate()?;
    Ok(Some(manifest))
}

pub(crate) fn publish(root: &Path, manifest: &Manifest) -> Result<()> {
    manifest.validate()?;
    let bytes = serde_json::to_vec(manifest).context("encode manifest")?;
    let checksum = digest(&bytes);
    let name = format!("{:020}-{checksum}.json", manifest.generation);
    write_immutable(&root.join("manifests"), &name, &bytes)?;
    let head = Head {
        generation: manifest.generation,
        manifest: name,
        sha256: checksum,
    };
    let bytes = serde_json::to_vec(&head).context("encode HEAD")?;
    let (temporary, mut file) = temporary_file(root)?;
    let result = (|| {
        file.write_all(&bytes).context("write HEAD")?;
        file.sync_all().context("sync HEAD")?;
        fs::rename(&temporary, root.join("HEAD")).context("atomically replace HEAD")?;
        sync_directory(root)
    })();
    drop(file);
    let _ = fs::remove_file(&temporary);
    result
}

pub(crate) fn initialize(root: &Path) -> Result<Manifest> {
    fs::create_dir_all(root).context("create storage root")?;
    if let Some(manifest) = load(root)? {
        return Ok(manifest);
    }
    for directory in ["manifests", "segments"] {
        let path = root.join(directory);
        if path.exists() && fs::read_dir(&path)?.next().is_some() {
            bail!("HEAD is missing from nonempty storage");
        }
        fs::create_dir_all(&path).context("create object directory")?;
        sync_directory(&path)?;
    }
    sync_directory(root)?;
    if let Some(parent) = root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        sync_directory(parent)?;
    }
    let manifest = Manifest::default();
    publish(root, &manifest)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generations_are_immutable_and_head_selects_latest() -> Result<()> {
        let root = tempfile::tempdir()?;
        let mut manifest = initialize(root.path())?;
        let first_head = fs::read(root.path().join("HEAD"))?;
        manifest.generation = 1;
        manifest.applied_wal_position = WalPosition {
            salts: [12, 34],
            frame_count: 56,
        };
        publish(root.path(), &manifest)?;
        assert_eq!(load(root.path())?, Some(manifest));
        assert_eq!(fs::read_dir(root.path().join("manifests"))?.count(), 2);
        fs::write(root.path().join("HEAD"), first_head)?;
        assert_eq!(load(root.path())?, Some(Manifest::default()));
        Ok(())
    }

    #[test]
    fn corrupt_manifest_and_head_fail_closed() -> Result<()> {
        let root = tempfile::tempdir()?;
        initialize(root.path())?;
        let head: Head = serde_json::from_slice(&fs::read(root.path().join("HEAD"))?)?;
        fs::write(root.path().join("manifests").join(&head.manifest), b"{}")?;
        assert!(load(root.path()).is_err());
        fs::write(root.path().join("HEAD"), b"{}")?;
        assert!(initialize(root.path()).is_err());
        Ok(())
    }

    #[test]
    fn invalid_mapping_and_path_traversal_are_rejected() -> Result<()> {
        let root = tempfile::tempdir()?;
        initialize(root.path())?;
        let mut manifest = Manifest {
            logical_db_size: 1,
            ..Manifest::default()
        };
        manifest.segments.insert(0, "../../escape".into());
        assert!(publish(root.path(), &manifest).is_err());
        manifest
            .segments
            .insert(0, format!("{}.seg", "a".repeat(64)));
        manifest
            .segments
            .insert(1, format!("{}.seg", "b".repeat(64)));
        assert!(publish(root.path(), &manifest).is_err());
        let head = Head {
            generation: 0,
            manifest: "../../escape".into(),
            sha256: "a".repeat(64),
        };
        fs::write(root.path().join("HEAD"), serde_json::to_vec(&head)?)?;
        assert!(load(root.path()).is_err());
        Ok(())
    }

    #[test]
    fn missing_head_does_not_reset_existing_storage() -> Result<()> {
        let root = tempfile::tempdir()?;
        initialize(root.path())?;
        fs::remove_file(root.path().join("HEAD"))?;
        assert!(initialize(root.path()).is_err());
        Ok(())
    }
}
