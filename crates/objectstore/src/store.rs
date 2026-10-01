//! [`ObjectStore`] 抽象：平台唯一的对象存储入口（架构 §17.9）。
//!
//! 上层（Scheduler / DBA / Worker 的 snapshot 流程）只依赖本 trait，
//! 不感知具体是 AWS S3、RustFS 还是 Ceph RGW。

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use domain::error::Result;
use tokio::io::AsyncReadExt;

use crate::error::StorageError;

/// 单个对象的元数据。
///
/// `etag` 去掉 S3 返回的引号；multipart 上传的 etag 形如 `<digest>-<parts>`，
/// 只用于变更检测 / 调试，**不作为数据完整性依据** —— 完整性由 snapshot manifest
/// 中的 sha256 负责。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// 对象 key（相对 bucket 根）。
    pub key: String,
    /// 对象字节数。
    pub size: u64,
    /// 对象 ETag。
    pub etag: Option<String>,
    /// 对象最后修改时间。
    pub last_modified: Option<DateTime<Utc>>,
}

/// S3-compatible 对象存储接口。
///
/// 实现约定：
/// - 所有方法返回 [`domain::error::Result`]；错误码契约见 crate 文档；
/// - [`ObjectStore::head`] 对不存在的对象返回 `Ok(None)`，[`ObjectStore::delete`]
///   对不存在的对象返回 `Ok(())`（S3 DELETE 幂等），只有 [`ObjectStore::get`]
///   把不存在视为 `SNAPSHOT_UNAVAILABLE`；
/// - [`ObjectStore::put_file`] / [`ObjectStore::get_to_file`] 必须支持大文件
///   （内部按固定大小分段处理，内存占用与文件大小无关）；
/// - 实现必须是 `Send + Sync + 'static`，可被多任务共享（`&dyn ObjectStore` 即可下传）。
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// 写入内存中的对象（小对象；大对象用 [`ObjectStore::put_file`]）。
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;

    /// 上传本地文件，返回上传的字节数。
    async fn put_file(&self, key: &str, path: &Path) -> Result<u64>;

    /// 读取整个对象到内存（小对象；大对象用 [`ObjectStore::get_to_file`]）。
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// 下载对象到本地文件，返回写入的字节数。
    async fn get_to_file(&self, key: &str, path: &Path) -> Result<u64>;

    /// 查询对象元数据；对象不存在返回 `Ok(None)`。
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>>;

    /// 删除对象；对象不存在也返回 `Ok(())`。
    async fn delete(&self, key: &str) -> Result<()>;

    /// 列出指定前缀下的全部对象（按 key 升序）。
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>>;
}

/// 分段读取文件时的默认段大小：8 MiB。
///
/// 取值必须 ≥ S3 multipart 的 5 MiB 最小段限制（除末段外），
/// 同时又不至于让单段驻留内存过大。
pub(crate) const PART_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// S3 multipart 单次上传最多 10000 个段。
pub(crate) const MAX_MULTIPART_PARTS: u64 = 10_000;

/// 计算上传 `len` 字节所需的段大小。
///
/// 常规文件用 8 MiB；超大文件（> 80000 MiB）按段数上限反推并向上取整到 MiB 边界，
/// 保证段数不超过 [`MAX_MULTIPART_PARTS`]。
pub(crate) fn part_size_for(len: u64) -> u64 {
    let by_part_limit = len
        .div_ceil(MAX_MULTIPART_PARTS)
        .next_multiple_of(1024 * 1024);
    by_part_limit.max(PART_SIZE_BYTES)
}

/// 从文件读取一段（最多 `buf.len()` 字节）并返回拷贝出来的 [`Bytes`]。
///
/// 读到 EOF 返回 `Ok(None)`；短读会继续读满，因此**中间段永远是满段**，
/// 只有末段可能不足 —— 这既是 S3 multipart 的要求，也让各实现的分段逻辑一致。
/// `buf` 由调用方复用，避免为大文件反复分配。
pub(crate) async fn read_part(
    path: &Path,
    file: &mut tokio::fs::File,
    buf: &mut [u8],
) -> Result<Option<Bytes>> {
    let want = buf.len();
    let mut filled = 0usize;
    while filled < want {
        let read = file
            .read(&mut buf[filled..want])
            .await
            .map_err(|err| StorageError::io(path, err))?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    if filled == 0 {
        return Ok(None);
    }
    Ok(Some(Bytes::copy_from_slice(&buf[..filled])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_size_respects_s3_limits() {
        // 小文件：固定 8 MiB（> S3 单段最小 5 MiB）。
        assert_eq!(part_size_for(0), PART_SIZE_BYTES);
        assert_eq!(part_size_for(1), PART_SIZE_BYTES);
        assert_eq!(part_size_for(PART_SIZE_BYTES), PART_SIZE_BYTES);
        // 常规大文件仍是 8 MiB。
        assert_eq!(part_size_for(9 * 1024 * 1024), PART_SIZE_BYTES);
        // 极端大文件：段数必须 <= 10000，且段大小是 MiB 整数倍。
        let huge = MAX_MULTIPART_PARTS * PART_SIZE_BYTES * 4;
        let size = part_size_for(huge);
        assert!(size > PART_SIZE_BYTES);
        assert_eq!(size % (1024 * 1024), 0);
        assert!(huge.div_ceil(size) <= MAX_MULTIPART_PARTS);
    }

    #[tokio::test]
    async fn read_part_returns_full_parts_except_last() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("data.bin");
        let content: Vec<u8> = (0..(13u32 * 1024)).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&path, &content).await.expect("写文件");

        let mut file = tokio::fs::File::open(&path).await.expect("打开文件");
        let mut buf = vec![0u8; 4096];
        let mut collected = Vec::new();
        let mut sizes = Vec::new();
        while let Some(part) = read_part(&path, &mut file, &mut buf).await.unwrap() {
            sizes.push(part.len());
            collected.extend_from_slice(&part);
        }
        assert_eq!(sizes, vec![4096, 4096, 4096, 1024]);
        assert_eq!(collected, content);

        // 恰好整段时不得多出一个空段。
        let exact = dir.path().join("exact.bin");
        tokio::fs::write(&exact, vec![7u8; 4096]).await.unwrap();
        let mut file = tokio::fs::File::open(&exact).await.unwrap();
        let mut buf = vec![0u8; 4096];
        assert!(read_part(&exact, &mut file, &mut buf)
            .await
            .unwrap()
            .is_some());
        assert!(read_part(&exact, &mut file, &mut buf)
            .await
            .unwrap()
            .is_none());

        // 空文件：一个段都不产生。
        let empty = dir.path().join("empty.bin");
        tokio::fs::write(&empty, b"").await.unwrap();
        let mut file = tokio::fs::File::open(&empty).await.unwrap();
        assert!(read_part(&empty, &mut file, &mut buf)
            .await
            .unwrap()
            .is_none());
    }
}
