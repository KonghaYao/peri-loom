use super::{
    check_condition, fresh_etag, validate_key, validate_prefix, ObjectStore, PreconditionFailed,
    PutCondition, StoredObject,
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub struct LocalObjectStore {
    root: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Envelope {
    key: String,
    bytes: Vec<u8>,
    etag: String,
    checksum: String,
}

impl LocalObjectStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        ensure!(
            !fs::symlink_metadata(root)?.file_type().is_symlink(),
            "symlink store root rejected"
        );
        let store = Self {
            root: root.canonicalize()?,
        };
        let _guard = store.lock()?;
        File::open(&store.root)?.sync_all()?;
        if let Some(parent) = store.root.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(store)
    }

    fn lock(&self) -> Result<File> {
        let path = self.root.join(".lock");
        reject_symlink(&path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        lock.lock()?;
        Ok(lock)
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root
            .join(format!("{}.obj", blake3::hash(key.as_bytes()).to_hex()))
    }

    fn read(&self, key: &str) -> Result<Option<StoredObject>> {
        let path = self.path(key);
        reject_symlink(&path)?;
        let encoded = match fs::read(path) {
            Ok(encoded) => encoded,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let envelope: Envelope =
            serde_json::from_slice(&encoded).context("invalid local object envelope")?;
        ensure!(envelope.key == key, "local object key mismatch");
        ensure!(
            envelope.checksum == blake3::hash(&envelope.bytes).to_hex().as_str(),
            "local object checksum mismatch"
        );
        ensure!(!envelope.etag.is_empty(), "missing local object etag");
        Ok(Some(StoredObject {
            bytes: envelope.bytes,
            etag: envelope.etag,
        }))
    }
}

fn reject_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "nonregular store file rejected"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

impl ObjectStore for LocalObjectStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        validate_key(key)?;
        let _guard = self.lock()?;
        self.read(key)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        validate_key(key)?;
        let _guard = self.lock()?;
        check_condition(self.read(key)?.as_ref(), &condition)?;
        let etag = fresh_etag(bytes);
        let envelope = Envelope {
            key: key.to_owned(),
            bytes: bytes.to_vec(),
            etag: etag.clone(),
            checksum: blake3::hash(bytes).to_hex().to_string(),
        };
        let temporary = self.root.join(format!(".{etag}.tmp"));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&serde_json::to_vec(&envelope)?)?;
            file.sync_all()?;
            fs::rename(&temporary, self.path(key))?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        validate_key(key)?;
        let _guard = self.lock()?;
        let Some(current) = self.read(key)? else {
            return Ok(());
        };
        if let Some(expected) = expected {
            ensure!(current.etag == expected, PreconditionFailed);
        }
        fs::remove_file(self.path(key))?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let _guard = self.lock()?;
        let mut keys = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "obj")
            {
                reject_symlink(&entry.path())?;
                let envelope: Envelope = serde_json::from_slice(&fs::read(entry.path())?)?;
                validate_key(&envelope.key)?;
                ensure!(
                    self.path(&envelope.key) == entry.path(),
                    "local object filename mismatch"
                );
                self.read(&envelope.key)?;
                if envelope.key.starts_with(prefix) {
                    keys.push(envelope.key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}
