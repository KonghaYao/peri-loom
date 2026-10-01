//! 完整的单机快照；恢复仅依赖这里列出的文件，不回放 Remote WAL。
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use chrono::Utc;
use domain::error::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::StorageError;
use crate::snapshot::{
    self, SnapshotFileEntry, TempPath, WriteTarget, COMPRESSION_ZSTD, DATA_SUFFIX,
};
use crate::store::ObjectStore;

pub const LOCAL_SNAPSHOT_FORMAT_VERSION: u32 = 1;
const LOCAL_MANIFEST_SUFFIX: &str = ".local.manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalSnapshotManifest {
    pub format_version: u32,
    pub database_id: String,
    pub snapshot_id: String,
    /// 必须由调用方在引擎一致性边界取得；该 artifact 不含远程 WAL 位置。
    pub engine_version: String,
    pub created_at_unix_ms: i64,
    pub total_size_bytes: u64,
    pub files: Vec<SnapshotFileEntry>,
    pub checksum: String,
}

impl LocalSnapshotManifest {
    pub fn manifest_key(&self) -> String {
        format!(
            "{}{id}{LOCAL_MANIFEST_SUFFIX}",
            snapshot::object_prefix(&self.database_id, &self.snapshot_id),
            id = self.snapshot_id
        )
    }
    fn entry_key(&self, entry: &SnapshotFileEntry) -> String {
        snapshot::object_key(
            &snapshot::object_prefix(&self.database_id, &self.snapshot_id),
            &entry.relative_path,
        )
    }
    pub fn validate(&self, expected_engine_version: &str) -> Result<()> {
        if self.format_version != LOCAL_SNAPSHOT_FORMAT_VERSION {
            return Err(StorageError::InvalidArgument(format!(
                "unsupported local snapshot version {}",
                self.format_version
            ))
            .into());
        }
        snapshot::validate_component("database_id", &self.database_id)?;
        snapshot::validate_component("snapshot_id", &self.snapshot_id)?;
        if expected_engine_version.is_empty() || self.engine_version != expected_engine_version {
            return Err(StorageError::InvalidArgument(format!(
                "incompatible engine version: {}",
                self.engine_version
            ))
            .into());
        }
        let mut paths = HashSet::new();
        let mut total = 0u64;
        for entry in &self.files {
            snapshot::validate_relative_path(&entry.relative_path)?;
            if !paths.insert(&entry.relative_path)
                || entry.compression != COMPRESSION_ZSTD
                || !valid_digest(&entry.checksum)
            {
                return Err(StorageError::InvalidArgument(format!(
                    "invalid local snapshot entry: {}",
                    entry.relative_path
                ))
                .into());
            }
            total = total
                .checked_add(entry.size_bytes)
                .ok_or_else(|| StorageError::InvalidArgument("snapshot size overflow".into()))?;
        }
        if !self
            .files
            .iter()
            .any(|entry| entry.relative_path == "main.db")
            || total != self.total_size_bytes
            || !valid_digest(&self.checksum)
            || self.checksum != checksum(self)
        {
            return Err(StorageError::SnapshotCorrupt(
                "local snapshot manifest incomplete or corrupt".into(),
            )
            .into());
        }
        Ok(())
    }
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn checksum(manifest: &LocalSnapshotManifest) -> String {
    let mut copy = manifest.clone();
    copy.checksum.clear();
    hex::encode(Sha256::digest(
        serde_json::to_vec(&copy).expect("serializing local manifest cannot fail"),
    ))
}

/// 调用方必须先用引擎生成包含本地 WAL 状态的一致文件集。
pub async fn upload_local_snapshot(
    store: &dyn ObjectStore,
    db_id: &str,
    snapshot_id: &str,
    engine_version: &str,
    source_files: &[(String, PathBuf)],
    tmp_dir: &Path,
) -> Result<LocalSnapshotManifest> {
    snapshot::validate_component("database_id", db_id)?;
    snapshot::validate_component("snapshot_id", snapshot_id)?;
    if engine_version.is_empty() || source_files.is_empty() {
        return Err(
            StorageError::InvalidArgument("engine_version and files required".into()).into(),
        );
    }
    let mut files = Vec::new();
    let mut seen = HashSet::new();
    let mut total = 0u64;
    for (relative, source) in source_files {
        snapshot::validate_relative_path(relative)?;
        if !seen.insert(relative) {
            return Err(
                StorageError::InvalidArgument(format!("duplicate path: {relative}")).into(),
            );
        }
        let spool = TempPath::reserve_in(tmp_dir, DATA_SUFFIX)?;
        let src = source.clone();
        let dst = spool.path().to_owned();
        let (size, digest) =
            tokio::task::spawn_blocking(move || snapshot::compress_file(&src, &dst))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))??;
        let key = snapshot::object_key(&snapshot::object_prefix(db_id, snapshot_id), relative);
        store.put_file(&key, spool.path()).await?;
        total = total
            .checked_add(size)
            .ok_or_else(|| StorageError::InvalidArgument("snapshot size overflow".into()))?;
        files.push(SnapshotFileEntry {
            relative_path: relative.clone(),
            size_bytes: size,
            checksum: digest,
            compression: COMPRESSION_ZSTD.into(),
        });
    }
    let mut manifest = LocalSnapshotManifest {
        format_version: LOCAL_SNAPSHOT_FORMAT_VERSION,
        database_id: db_id.into(),
        snapshot_id: snapshot_id.into(),
        engine_version: engine_version.into(),
        created_at_unix_ms: Utc::now().timestamp_millis(),
        total_size_bytes: total,
        files,
        checksum: String::new(),
    };
    manifest.checksum = checksum(&manifest);
    manifest.validate(engine_version)?;
    store
        .put(
            &manifest.manifest_key(),
            Bytes::from(
                serde_json::to_vec(&manifest).map_err(|e| StorageError::Internal(e.to_string()))?,
            ),
        )
        .await?;
    Ok(manifest)
}

pub async fn load_local_snapshot(
    store: &dyn ObjectStore,
    db_id: &str,
    snapshot_id: &str,
    engine_version: &str,
) -> Result<LocalSnapshotManifest> {
    snapshot::validate_component("database_id", db_id)?;
    snapshot::validate_component("snapshot_id", snapshot_id)?;
    let key = format!(
        "{}{snapshot_id}{LOCAL_MANIFEST_SUFFIX}",
        snapshot::object_prefix(db_id, snapshot_id)
    );
    let bytes = store.get(&key).await?;
    let manifest: LocalSnapshotManifest =
        serde_json::from_slice(&bytes).map_err(|e| StorageError::SnapshotCorrupt(e.to_string()))?;
    if manifest.database_id != db_id || manifest.snapshot_id != snapshot_id {
        return Err(StorageError::SnapshotCorrupt("manifest identity mismatch".into()).into());
    }
    manifest.validate(engine_version)?;
    Ok(manifest)
}

/// 解码到唯一暂存目录；校验全部通过后才原子发布目标目录。
/// 目标目录必须不存在，调用方负责关闭数据库并失效旧会话。
pub async fn restore_local_snapshot(
    store: &dyn ObjectStore,
    manifest: &LocalSnapshotManifest,
    engine_version: &str,
    destination: &Path,
    tmp_dir: &Path,
) -> Result<()> {
    manifest.validate(engine_version)?;
    if destination.exists() {
        return Err(
            StorageError::InvalidArgument("restore destination already exists".into()).into(),
        );
    }
    let parent = destination
        .parent()
        .ok_or_else(|| StorageError::InvalidArgument("destination has no parent".into()))?;
    let staging = parent.join(format!(".restore-{}", Uuid::new_v4()));
    std::fs::create_dir(&staging).map_err(|e| StorageError::io(&staging, e))?;
    let result = async {
        for entry in &manifest.files {
            let key = manifest.entry_key(entry);
            let spool = TempPath::reserve_in(tmp_dir, DATA_SUFFIX)?;
            store.get_to_file(&key, spool.path()).await?;
            let dest = staging.join(&entry.relative_path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(|e| StorageError::io(parent, e))?;
            }
            let (size, digest) = snapshot::decode_entry(
                spool,
                key.clone(),
                entry.compression.clone(),
                WriteTarget::FileLimited(dest.clone(), entry.size_bytes),
            )
            .await?;
            snapshot::check_entry(&key, entry, size, &digest)?;
            std::fs::File::open(&dest)
                .and_then(|f| f.sync_all())
                .map_err(|e| StorageError::io(&dest, e))?;
        }
        sync_tree(&staging)?;
        std::fs::rename(&staging, destination).map_err(|e| StorageError::io(destination, e))?;
        std::fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .map_err(|e| StorageError::io(parent, e))?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    result
}

fn sync_tree(path: &Path) -> Result<()> {
    for item in std::fs::read_dir(path).map_err(|e| StorageError::io(path, e))? {
        let item = item.map_err(|e| StorageError::io(path, e))?;
        if item
            .file_type()
            .map_err(|e| StorageError::io(item.path(), e))?
            .is_dir()
        {
            sync_tree(&item.path())?;
        }
    }
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|e| StorageError::io(path, e).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalObjectStore;
    use domain::error::ErrorCode;

    #[tokio::test]
    async fn local_roundtrip_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            LocalObjectStore::new(&dir.path().join("objects"), &dir.path().join("tmp")).unwrap();
        let src = dir.path().join("db");
        std::fs::write(&src, b"database state").unwrap();
        let manifest = upload_local_snapshot(
            &store,
            "db1",
            "s1",
            "engine-1",
            &[("main.db".into(), src)],
            &dir.path().join("tmp"),
        )
        .await
        .unwrap();
        let loaded = load_local_snapshot(&store, "db1", "s1", "engine-1")
            .await
            .unwrap();
        assert_eq!(loaded, manifest);
        let dest = dir.path().join("restored");
        restore_local_snapshot(&store, &loaded, "engine-1", &dest, &dir.path().join("tmp"))
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(dest.join("main.db")).unwrap(),
            b"database state"
        );
        assert_eq!(
            loaded.validate("engine-2").unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        let mut incomplete = loaded.clone();
        incomplete.files.clear();
        incomplete.checksum = checksum(&incomplete);
        assert_eq!(
            incomplete.validate("engine-1").unwrap_err().code,
            ErrorCode::SnapshotUnavailable
        );
        store
            .put(
                &loaded.entry_key(&loaded.files[0]),
                Bytes::from_static(b"bad"),
            )
            .await
            .unwrap();
        let failed = dir.path().join("failed");
        assert!(restore_local_snapshot(
            &store,
            &loaded,
            "engine-1",
            &failed,
            &dir.path().join("tmp")
        )
        .await
        .is_err());
        assert!(!failed.exists());

        let overrun = zstd::stream::encode_all(&b"xxxxxxxxxxxxxxxxxxxxxxxx"[..], 3).unwrap();
        store
            .put(&loaded.entry_key(&loaded.files[0]), Bytes::from(overrun))
            .await
            .unwrap();
        let mut forged = loaded.clone();
        forged.files[0].size_bytes = 1;
        forged.total_size_bytes = 1;
        forged.checksum = checksum(&forged);
        let bounded = dir.path().join("bounded");
        assert!(restore_local_snapshot(
            &store,
            &forged,
            "engine-1",
            &bounded,
            &dir.path().join("tmp")
        )
        .await
        .is_err());
        assert!(!bounded.exists());
    }
}
