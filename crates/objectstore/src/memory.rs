//! 进程内 [`ObjectStore`] 实现。
//!
//! 用途：单元测试（不依赖真实 S3 / RustFS）与无对象存储的本地开发。
//! 语义与 S3 实现保持一致：分段上传、key 升序 list、DELETE 幂等、GET 缺失报
//! `SNAPSHOT_UNAVAILABLE`。为了验证「大文件走分段路径」，这里记录每个对象的段数，
//! 通过 [`InMemoryObjectStore::part_count`] 暴露给测试。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use domain::error::Result;
use tokio::io::AsyncWriteExt;

use crate::error::StorageError;
use crate::snapshot::compute_sha256;
use crate::store::{read_part, ObjectMeta, ObjectStore, PART_SIZE_BYTES};

#[derive(Debug)]
struct StoredObject {
    data: Bytes,
    /// 写入段数（1 段 = 单次整对象写入）。
    parts: usize,
    etag: String,
    last_modified: DateTime<Utc>,
}

/// 内存对象存储。
///
/// 全部对象常驻内存，只适合测试与小规模本地开发；容量无上限也**没有**持久化保证。
#[derive(Debug, Default)]
pub struct InMemoryObjectStore {
    objects: Mutex<BTreeMap<String, StoredObject>>,
}

impl InMemoryObjectStore {
    /// 创建空 store。
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前对象数量。
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// 是否没有任何对象。
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// 对象是否存在。
    pub fn contains(&self, key: &str) -> bool {
        self.lock().contains_key(key)
    }

    /// 对象当前的写入段数；对象不存在返回 `None`。
    ///
    /// 用于断言「大文件确实走了分段写入路径」。
    pub fn part_count(&self, key: &str) -> Option<usize> {
        self.lock().get(key).map(|object| object.parts)
    }

    /// 互斥锁中毒（持锁期间 panic）时直接取回内部数据：测试 store 不应让
    /// 一次断言失败连带污染后续用例。
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, StoredObject>> {
        self.objects.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn store_object(&self, key: &str, data: Bytes, parts: usize) {
        let etag = compute_sha256(&data);
        self.lock().insert(
            key.to_string(),
            StoredObject {
                data,
                parts,
                etag,
                last_modified: Utc::now(),
            },
        );
    }

    fn get_object(&self, key: &str) -> Result<StoredObject> {
        self.lock()
            .get(key)
            .map(|object| StoredObject {
                data: object.data.clone(),
                parts: object.parts,
                etag: object.etag.clone(),
                last_modified: object.last_modified,
            })
            .ok_or_else(|| {
                StorageError::NotFound {
                    key: key.to_string(),
                }
                .into()
            })
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        // 单次整对象写入记为 1 段。
        self.store_object(key, data, 1);
        Ok(())
    }

    async fn put_file(&self, key: &str, path: &Path) -> Result<u64> {
        // 与 S3 实现同样的分段循环：保证大文件不会一次性读入内存。
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|err| StorageError::io(path, err))?;
        let mut buf = vec![0u8; PART_SIZE_BYTES as usize];
        let mut parts: Vec<Bytes> = Vec::new();
        let mut total = 0usize;
        while let Some(part) = read_part(path, &mut file, &mut buf).await? {
            total += part.len();
            parts.push(part);
        }
        let mut joined = BytesMut::with_capacity(total);
        for part in &parts {
            joined.extend_from_slice(part);
        }
        let part_count = parts.len();
        self.store_object(key, joined.freeze(), part_count);
        Ok(total as u64)
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        Ok(self.get_object(key)?.data)
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> Result<u64> {
        let object = self.get_object(key)?;
        let file = tokio::fs::File::create(path)
            .await
            .map_err(|err| StorageError::io(path, err))?;
        let mut writer = tokio::io::BufWriter::new(file);
        // 分块落盘，模拟真实存储的流式下载。
        for chunk in object.data.chunks(PART_SIZE_BYTES as usize) {
            writer
                .write_all(chunk)
                .await
                .map_err(|err| StorageError::io(path, err))?;
        }
        writer
            .flush()
            .await
            .map_err(|err| StorageError::io(path, err))?;
        Ok(object.data.len() as u64)
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.lock().get(key).map(|object| ObjectMeta {
            key: key.to_string(),
            size: object.data.len() as u64,
            etag: Some(object.etag.clone()),
            last_modified: Some(object.last_modified),
        }))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        // S3 DELETE 幂等：对象不存在同样返回成功。
        self.lock().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        // BTreeMap 天然按 key 升序，与 S3 list_objects_v2 的顺序语义一致。
        Ok(self
            .lock()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, object)| ObjectMeta {
                key: key.clone(),
                size: object.data.len() as u64,
                etag: Some(object.etag.clone()),
                last_modified: Some(object.last_modified),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 确定性伪随机数据（不引入 rand 依赖，保证用例可复现）。
    fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                (state >> 33) as u8
            })
            .collect()
    }

    #[tokio::test]
    async fn put_get_head_delete_semantics() {
        let store = InMemoryObjectStore::new();
        assert!(store.is_empty());

        store
            .put("snapshots/a/1/a.sql", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        store
            .put("snapshots/a/1/b.sql", Bytes::from_static(b"world!"))
            .await
            .unwrap();
        store
            .put("other/c.sql", Bytes::from_static(b"x"))
            .await
            .unwrap();

        assert_eq!(
            store.get("snapshots/a/1/a.sql").await.unwrap(),
            b"hello"[..]
        );
        assert_eq!(
            store.get("snapshots/a/1/b.sql").await.unwrap(),
            b"world!"[..]
        );

        let meta = store.head("snapshots/a/1/a.sql").await.unwrap().unwrap();
        assert_eq!(meta.key, "snapshots/a/1/a.sql");
        assert_eq!(meta.size, 5);
        assert!(meta.etag.is_some());
        assert!(meta.last_modified.is_some());
        // 小对象为单段写入。
        assert_eq!(store.part_count("snapshots/a/1/a.sql"), Some(1));

        // 前缀过滤 + key 升序。
        let listed = store.list("snapshots/a/").await.unwrap();
        let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["snapshots/a/1/a.sql", "snapshots/a/1/b.sql"]);

        // 缺失对象：get 报错（对象不存在 -> SNAPSHOT_UNAVAILABLE），head/delete 幂等。
        let err = store.get("snapshots/a/1/missing").await.unwrap_err();
        assert_eq!(err.code, domain::error::ErrorCode::SnapshotUnavailable);
        assert!(store.head("missing").await.unwrap().is_none());
        store.delete("missing").await.expect("delete 幂等");

        store.delete("snapshots/a/1/a.sql").await.unwrap();
        assert!(!store.contains("snapshots/a/1/a.sql"));
        assert_eq!(store.len(), 2);
    }

    #[tokio::test]
    async fn large_file_uses_segmented_path() {
        let dir = tempfile::tempdir().expect("临时目录");
        let src = dir.path().join("big.bin");
        let dst = dir.path().join("big.out");

        // > 8 MiB：必须走多段写入（8 MiB 段 * 2）。
        let content = pseudo_random(9 * 1024 * 1024 + 123, 0x5EED);
        tokio::fs::write(&src, &content).await.unwrap();

        let store = InMemoryObjectStore::new();
        let written = store
            .put_file("snapshots/db1/snap1/big.bin.zst", &src)
            .await
            .unwrap();
        assert_eq!(written, content.len() as u64);
        assert_eq!(
            store
                .head("snapshots/db1/snap1/big.bin.zst")
                .await
                .unwrap()
                .unwrap()
                .size,
            content.len() as u64
        );
        let parts = store.part_count("snapshots/db1/snap1/big.bin.zst").unwrap();
        assert_eq!(parts, 2, "9 MiB 文件应按 8 MiB 分段写入");

        // get_to_file 往返一致（用摘要比较，避免失败时打印 9 MiB 数据）。
        let read_back = store
            .get_to_file("snapshots/db1/snap1/big.bin.zst", &dst)
            .await
            .unwrap();
        assert_eq!(read_back, content.len() as u64);
        assert_eq!(
            compute_sha256(&tokio::fs::read(&dst).await.unwrap()),
            compute_sha256(&content)
        );

        // get 同样能取回完整内容。
        let in_memory = store.get("snapshots/db1/snap1/big.bin.zst").await.unwrap();
        assert_eq!(compute_sha256(&in_memory), compute_sha256(&content));
    }

    #[tokio::test]
    async fn put_file_reports_missing_source() {
        let store = InMemoryObjectStore::new();
        let err = store
            .put_file("k", Path::new("/nonexistent/peri-loom/file"))
            .await
            .unwrap_err();
        assert_eq!(err.code, domain::error::ErrorCode::InvalidArgument);
    }
}
