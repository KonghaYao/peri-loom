//! 受目录能力约束的本地对象存储。
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use chrono::{DateTime, Utc};
use domain::error::Result;
use uuid::Uuid;

use crate::error::StorageError;
use crate::store::{ObjectMeta, ObjectStore};

const BUFFER: usize = 1024 * 1024;

fn sync_dir(dir: &Dir) -> io::Result<()> {
    dir.try_clone()?.into_std_file().sync_all()
}

/// 根目录在构造时打开；后续对象操作始终相对于该目录句柄。
#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    root: Arc<Dir>,
    tmp: Arc<Dir>,
}

impl LocalObjectStore {
    /// `root` 通常为 instance/objects，`tmp` 应位于同一文件系统的 instance/tmp。
    pub fn new(root: &Path, tmp: &Path) -> Result<Self> {
        std::fs::create_dir_all(root).map_err(|e| StorageError::io(root, e))?;
        std::fs::create_dir_all(tmp).map_err(|e| StorageError::io(tmp, e))?;
        let root = Dir::open_ambient_dir(root, ambient_authority())
            .map_err(|e| StorageError::io(root, e))?;
        let tmp = Dir::open_ambient_dir(tmp, ambient_authority())
            .map_err(|e| StorageError::io(tmp, e))?;
        Ok(Self {
            root: Arc::new(root),
            tmp: Arc::new(tmp),
        })
    }

    fn validate(key: &str) -> Result<()> {
        crate::snapshot::validate_relative_path(key)
    }

    fn parent(&self, key: &str, create: bool) -> Result<(Dir, String)> {
        Self::validate(key)?;
        let mut dir = self
            .root
            .open_dir(".")
            .map_err(|e| StorageError::io(key, e))?;
        let mut parts = key.split('/').peekable();
        while let Some(part) = parts.next() {
            if parts.peek().is_none() {
                return Ok((dir, part.to_owned()));
            }
            if create {
                match dir.create_dir(part) {
                    Ok(()) => {
                        sync_dir(&dir).map_err(|e| StorageError::io(key, e))?;
                    }
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(StorageError::io(key, e).into()),
                }
            }
            let meta = dir
                .symlink_metadata(part)
                .map_err(|e| StorageError::io(key, e))?;
            if !meta.is_dir() || meta.file_type().is_symlink() {
                return Err(
                    StorageError::InvalidArgument(format!("unsafe object path: {key}")).into(),
                );
            }
            dir = dir.open_dir(part).map_err(|e| StorageError::io(key, e))?;
        }
        unreachable!()
    }

    fn checked_file(&self, key: &str) -> Result<Option<(Dir, String, cap_std::fs::Metadata)>> {
        let (dir, name) = match self.parent(key, false) {
            Ok(pair) => pair,
            Err(e)
                if e.code == domain::error::ErrorCode::InvalidArgument
                    && e.message.contains("does not exist") =>
            {
                return Ok(None)
            }
            Err(e) => return Err(e),
        };
        match dir.symlink_metadata(&name) {
            Ok(meta) if meta.is_file() && !meta.file_type().is_symlink() => {
                Ok(Some((dir, name, meta)))
            }
            Ok(_) => {
                Err(StorageError::InvalidArgument(format!("unsafe object path: {key}")).into())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io(key, e).into()),
        }
    }

    fn write_from(&self, key: &str, source: &mut dyn Read) -> Result<u64> {
        let (parent, name) = self.parent(key, true)?;
        if parent
            .symlink_metadata(&name)
            .is_ok_and(|m| !m.is_file() || m.file_type().is_symlink())
        {
            return Err(StorageError::InvalidArgument(format!("unsafe object path: {key}")).into());
        }
        let temporary = format!("{}.part", Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = self
            .tmp
            .open_with(&temporary, &options)
            .map_err(|e| StorageError::io(key, e))?;
        let result = (|| {
            let mut buffer = [0u8; BUFFER];
            let mut size = 0u64;
            loop {
                let n = source
                    .read(&mut buffer)
                    .map_err(|e| StorageError::io(key, e))?;
                if n == 0 {
                    break;
                }
                file.write_all(&buffer[..n])
                    .map_err(|e| StorageError::io(key, e))?;
                size += n as u64;
            }
            file.sync_all().map_err(|e| StorageError::io(key, e))?;
            drop(file);
            self.tmp
                .rename(&temporary, &parent, &name)
                .map_err(|e| StorageError::io(key, e))?;
            sync_dir(&parent).map_err(|e| StorageError::io(key, e))?;
            sync_dir(&self.tmp).map_err(|e| StorageError::io(key, e))?;
            Ok(size)
        })();
        if result.is_err() {
            let _ = self.tmp.remove_file(&temporary);
        }
        result
    }

    fn read_into(&self, key: &str, writer: &mut dyn Write) -> Result<u64> {
        let (dir, name, _) = self
            .checked_file(key)?
            .ok_or_else(|| StorageError::NotFound {
                key: key.to_owned(),
            })?;
        let mut options = OpenOptions::new();
        options.read(true);
        let mut file = dir
            .open_with(&name, &options)
            .map_err(|e| StorageError::io(key, e))?;
        io::copy(&mut file, writer).map_err(|e| StorageError::io(key, e).into())
    }

    fn metadata(key: String, meta: cap_std::fs::Metadata) -> ObjectMeta {
        ObjectMeta {
            key,
            size: meta.len(),
            etag: None,
            last_modified: meta
                .modified()
                .ok()
                .map(|t| DateTime::<Utc>::from(t.into_std())),
        }
    }

    fn walk(&self, dir: &Dir, base: &str, out: &mut Vec<ObjectMeta>) -> Result<()> {
        for entry in dir.read_dir(".").map_err(|e| StorageError::io(base, e))? {
            let entry = entry.map_err(|e| StorageError::io(base, e))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = if base.is_empty() {
                name.clone()
            } else {
                format!("{base}/{name}")
            };
            let meta = dir
                .symlink_metadata(&name)
                .map_err(|e| StorageError::io(&key, e))?;
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                let child = dir.open_dir(&name).map_err(|e| StorageError::io(&key, e))?;
                self.walk(&child, &key, out)?;
            } else if meta.is_file() {
                out.push(Self::metadata(key, meta));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let this = self.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || this.write_from(&key, &mut data.as_ref()).map(|_| ()))
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
    }
    async fn put_file(&self, key: &str, path: &Path) -> Result<u64> {
        let this = self.clone();
        let key = key.to_owned();
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut source = File::open(&path).map_err(|e| StorageError::io(&path, e))?;
            this.write_from(&key, &mut source)
        })
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
    }
    async fn get(&self, key: &str) -> Result<Bytes> {
        let this = self.clone();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            this.read_into(&key, &mut out)?;
            Ok(Bytes::from(out))
        })
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
    }
    async fn get_to_file(&self, key: &str, path: &Path) -> Result<u64> {
        let this = self.clone();
        let key = key.to_owned();
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || {
            if this.checked_file(&key)?.is_none() {
                return Err(StorageError::NotFound { key }.into());
            }
            let mut output = File::create(&path).map_err(|e| StorageError::io(&path, e))?;
            let size = this.read_into(&key, &mut output)?;
            output.sync_all().map_err(|e| StorageError::io(&path, e))?;
            Ok(size)
        })
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
    }
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self
            .checked_file(key)?
            .map(|(_, _, meta)| Self::metadata(key.to_owned(), meta)))
    }
    async fn delete(&self, key: &str) -> Result<()> {
        if let Some((dir, name, _)) = self.checked_file(key)? {
            dir.remove_file(&name)
                .map_err(|e| StorageError::io(key, e))?;
            sync_dir(&dir).map_err(|e| StorageError::io(key, e))?;
        }
        Ok(())
    }
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        if !prefix.is_empty() {
            let trimmed = prefix.trim_end_matches('/');
            Self::validate(trimmed)?;
        }
        let mut all = Vec::new();
        self.walk(&self.root, "", &mut all)?;
        all.retain(|m| m.key.starts_with(prefix));
        all.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::error::ErrorCode;

    #[tokio::test]
    async fn contract_and_streaming() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            LocalObjectStore::new(&dir.path().join("objects"), &dir.path().join("tmp")).unwrap();
        let source = dir.path().join("source");
        let output = dir.path().join("output");
        std::fs::write(&source, vec![0x5a; 12 * 1024 * 1024]).unwrap();
        assert_eq!(
            store.put_file("a/b", &source).await.unwrap(),
            12 * 1024 * 1024
        );
        assert_eq!(
            store.get_to_file("a/b", &output).await.unwrap(),
            12 * 1024 * 1024
        );
        assert_eq!(
            std::fs::read(&output).unwrap(),
            std::fs::read(&source).unwrap()
        );
        assert_eq!(
            store.head("a/b").await.unwrap().unwrap().size,
            12 * 1024 * 1024
        );
        assert_eq!(store.list("a/").await.unwrap().len(), 1);
        store.delete("a/b").await.unwrap();
        store.delete("a/b").await.unwrap();
        assert!(store.head("a/b").await.unwrap().is_none());
        assert_eq!(
            store.get("a/b").await.unwrap_err().code,
            ErrorCode::SnapshotUnavailable
        );
    }

    #[tokio::test]
    async fn rejects_escape_and_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("objects");
        let store = LocalObjectStore::new(&root, &dir.path().join("tmp")).unwrap();
        for key in [
            "/etc/passwd",
            "../escape",
            "a/../escape",
            "a//b",
            "a/./b",
            "a\\b",
        ] {
            assert_eq!(
                store
                    .put(key, Bytes::from_static(b"x"))
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::InvalidArgument
            );
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.path(), root.join("escape")).unwrap();
            assert_eq!(
                store
                    .put("escape/out", Bytes::from_static(b"x"))
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::InvalidArgument
            );
            assert!(!dir.path().join("out").exists());
        }
    }
}
