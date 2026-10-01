//! Snapshot / Backup artifact（架构 §11.4 / §17.9）。
//!
//! ## 对象布局
//!
//! ```text
//! snapshots/<db_id>/<snapshot_id>/<relative_path>.zst   每个源文件一个对象（zstd 压缩）
//! snapshots/<db_id>/<snapshot_id>/<snapshot_id>.manifest.json   manifest（最后上传）
//! ```
//!
//! manifest 一定**最后**上传：只要能看到 manifest，就说明所有数据对象都已就位，
//! 恢复方不会读到半成品快照。
//!
//! ## 完整性
//!
//! - 每个 entry 记 `size_bytes`（**原始大小**，即解压后）与 `checksum`
//!   （原始内容的 sha256 小写十六进制）；
//! - manifest 自身还有 `checksum`：覆盖除该字段以外的全部元数据（[`compute_manifest_checksum`]），
//!   防止 manifest 被篡改后「合法地」指向别人的数据；
//! - 解压失败等价于数据损坏，返回 `CHECKSUM_MISMATCH` 而不是存储故障。
//!
//! ## 大文件
//!
//! 压缩 / 解压都在 `spawn_blocking` 里做流式处理，中间结果落到 spool 临时文件，
//! 再交给 [`ObjectStore::put_file`] / [`ObjectStore::get_to_file`]（内部走分段上传 / 下载），
//! 因此内存占用与文件大小无关（约等于一个 IO 缓冲区 + 一个段）。
//! spool 目录默认取系统临时目录，可用 `SNAPSHOT_SPOOL_DIR` 指到大盘。

use std::env;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use chrono::Utc;
use domain::error::{PlatformError, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::StorageError;
use crate::store::ObjectStore;

/// manifest 对象的后缀：`<prefix><snapshot_id>.manifest.json`。
pub const MANIFEST_SUFFIX: &str = ".manifest.json";

/// 数据对象后缀（压缩后）。
pub const DATA_SUFFIX: &str = ".zst";

/// 压缩算法标记：zstd（默认）。
pub const COMPRESSION_ZSTD: &str = "zstd";

/// 压缩算法标记：不压缩（历史 / 外部产出 artifact，读取时兼容）。
pub const COMPRESSION_NONE: &str = "none";

/// 默认 zstd 压缩级别（架构 §17.9：默认使用 Zstd）。
///
/// 3 级是 zstd 的默认权衡点：压缩率足够，CPU 开销对后台 snapshot 任务可忽略。
/// 注意这是**冻结的 artifact 参数**，改级别不影响正确性（解压与级别无关），
/// 但会让同尺寸数据的对象大小发生变化。
pub const ZSTD_LEVEL: i32 = 3;

/// spool 目录环境变量：快照压缩 / 解压的中间文件落盘位置。
const ENV_SPOOL_DIR: &str = "SNAPSHOT_SPOOL_DIR";

/// 默认 spool 子目录名（位于系统临时目录下）。
const SPOOL_DIR_NAME: &str = "peri-loom-snapshot";

/// 流式 IO 的缓冲区大小。
const IO_BUFFER_BYTES: usize = 1024 * 1024;

/// snapshot manifest：restore / backup 的唯一入口描述（架构 §11.4）。
///
/// 恢复流程：取 manifest -> 校验 -> 下载解压 base image ->
/// 从 [`SnapshotManifest::base_lsn`] 继续 replay Remote WAL（§11.2）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    /// 所属 database id。
    pub database_id: String,
    /// snapshot id（同一个 DB 下唯一）。
    pub snapshot_id: String,
    /// 快照基线 LSN：恢复时从此处之后 replay Remote WAL。
    pub base_lsn: u64,
    /// 生成快照时的 owner epoch（用于判定该快照是否仍与当前所有权一致）。
    pub owner_epoch: u64,
    /// 生成快照的 DB engine 版本。
    pub engine_version: String,
    /// schema 版本（catalog 侧迁移版本），恢复前必须能兼容。
    pub schema_version: i32,
    /// 生成时间（Unix 毫秒）。
    pub created_at_unix_ms: i64,
    /// 所有文件**原始（解压后）**字节数之和。
    pub total_size_bytes: u64,
    /// manifest 自身的完整性摘要，见 [`compute_manifest_checksum`]。
    pub checksum: String,
    /// 文件清单。
    pub files: Vec<SnapshotFileEntry>,
}

/// manifest 中的单个文件条目。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotFileEntry {
    /// 相对路径（位于 DB data dir 之下），下载时按同样结构恢复。
    pub relative_path: String,
    /// 原始（解压后）字节数。
    pub size_bytes: u64,
    /// 原始内容的 sha256（小写十六进制）。
    pub checksum: String,
    /// 压缩算法标记，见 [`COMPRESSION_ZSTD`] / [`COMPRESSION_NONE`]。
    pub compression: String,
}

impl SnapshotManifest {
    /// 该快照的对象前缀。
    pub fn prefix(&self) -> String {
        object_prefix(&self.database_id, &self.snapshot_id)
    }

    /// 指定 entry 对应的数据对象 key。
    pub fn object_key(&self, entry: &SnapshotFileEntry) -> String {
        object_key(&self.prefix(), &entry.relative_path)
    }

    /// manifest 自身的对象 key。
    pub fn manifest_key(&self) -> String {
        manifest_object_key(&self.database_id, &self.snapshot_id)
    }

    /// 序列化为 JSON（带缩进，便于人工排查；末尾补换行）。
    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec_pretty(self)
            .map_err(|err| StorageError::Internal(format!("manifest 序列化失败: {err}")))?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    /// 从 JSON 反序列化。
    ///
    /// 解析失败属于调用方给的字节流有问题 -> `INVALID_ARGUMENT`；
    /// 若字节流来自对象存储，请用 [`load_manifest`]（那边映射为 `SNAPSHOT_UNAVAILABLE`）。
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|err| {
            StorageError::InvalidArgument(format!("manifest JSON 解析失败: {err}")).into()
        })
    }
}

/// 数据对象 key 前缀：`snapshots/<db_id>/<snapshot_id>/`。
///
/// 调用方需保证两个 id 合法（见 [`upload_snapshot`] 的校验），否则可能拼出越界的 key。
pub fn object_prefix(db_id: &str, snapshot_id: &str) -> String {
    format!("snapshots/{db_id}/{snapshot_id}/")
}

/// 数据对象 key：`<prefix><relative_path>.zst`。
pub fn object_key(prefix: &str, relative_path: &str) -> String {
    format!("{prefix}{relative_path}{DATA_SUFFIX}")
}

/// manifest 对象 key：`<prefix><snapshot_id>.manifest.json`。
pub fn manifest_object_key(db_id: &str, snapshot_id: &str) -> String {
    format!(
        "{}{snapshot_id}{MANIFEST_SUFFIX}",
        object_prefix(db_id, snapshot_id)
    )
}

/// 计算 sha256（小写十六进制）。
pub fn compute_sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// 计算 manifest 的完整性摘要（忽略 `checksum` 字段自身）。
///
/// 覆盖全部元数据 + 文件清单，避免 manifest 被篡改后仍能通过校验。
pub fn compute_manifest_checksum(manifest: &SnapshotManifest) -> String {
    let mut hasher = Sha256::new();
    for field in [
        manifest.database_id.as_str(),
        manifest.snapshot_id.as_str(),
        manifest.engine_version.as_str(),
    ] {
        hasher.update(field.as_bytes());
        hasher.update([0u8]);
    }
    hasher.update(manifest.base_lsn.to_le_bytes());
    hasher.update(manifest.owner_epoch.to_le_bytes());
    hasher.update(manifest.schema_version.to_le_bytes());
    hasher.update(manifest.created_at_unix_ms.to_le_bytes());
    hasher.update(manifest.total_size_bytes.to_le_bytes());
    for entry in &manifest.files {
        hasher.update(entry.relative_path.as_bytes());
        hasher.update([0u8]);
        hasher.update(entry.size_bytes.to_le_bytes());
        hasher.update(entry.checksum.as_bytes());
        hasher.update([0u8]);
        hasher.update(entry.compression.as_bytes());
        hasher.update([0u8]);
    }
    hex::encode(hasher.finalize())
}

/// 校验相对路径：必须位于目标目录之下。
///
/// 拒绝绝对路径、`..` / `.` 分段、空分段与反斜杠 —— 防止被篡改的 manifest
/// 把文件写到 `dest_dir` 之外（路径穿越）。
pub fn validate_relative_path(relative_path: &str) -> Result<()> {
    let reject = |reason: &str| -> Result<()> {
        Err(StorageError::InvalidArgument(format!(
            "relative_path {relative_path:?} 非法: {reason}"
        ))
        .into())
    };
    if relative_path.is_empty() {
        return reject("不能为空");
    }
    if relative_path.starts_with('/') {
        return reject("不能是绝对路径");
    }
    if relative_path.contains('\\') {
        return reject("不能包含反斜杠");
    }
    if relative_path.chars().any(char::is_control) {
        return reject("不能包含控制字符");
    }
    for component in relative_path.split('/') {
        if component.is_empty() {
            return reject("不能包含空分段");
        }
        if component == "." || component == ".." {
            return reject("不能包含 . / .. 分段");
        }
    }
    Ok(())
}

/// 校验作为对象 key 分段的 id（db_id / snapshot_id）。
fn validate_component(field: &str, value: &str) -> Result<()> {
    let invalid = |reason: &str| -> Result<()> {
        Err(StorageError::InvalidArgument(format!("{field} {value:?} 非法: {reason}")).into())
    };
    if value.is_empty() {
        return invalid("不能为空");
    }
    if value == "." || value == ".." {
        return invalid("不能是 . 或 ..");
    }
    if value.contains('/') || value.contains('\\') {
        return invalid("不能包含路径分隔符");
    }
    if value.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return invalid("不能包含空白或控制字符");
    }
    Ok(())
}

/// 上传一个快照：逐文件 zstd 压缩 -> 上传数据对象 -> 最后上传 manifest。
///
/// `source_files` 为 `(相对路径, 本地绝对路径)`，相对路径决定对象 key 与恢复位置。
/// 返回已上传的 manifest（调用方通常存入 catalog 的 snapshots 表）。
#[allow(clippy::too_many_arguments)] // 参数由 artifact 契约（架构 §11.4）固定，不做结构体封装
pub async fn upload_snapshot(
    store: &dyn ObjectStore,
    db_id: &str,
    snapshot_id: &str,
    base_lsn: u64,
    owner_epoch: u64,
    engine_version: &str,
    schema_version: i32,
    source_files: &[(String, PathBuf)],
) -> Result<SnapshotManifest> {
    validate_component("database_id", db_id)?;
    validate_component("snapshot_id", snapshot_id)?;
    if engine_version.is_empty() {
        return Err(StorageError::InvalidArgument("engine_version 不能为空".to_string()).into());
    }
    let prefix = object_prefix(db_id, snapshot_id);
    let mut files: Vec<SnapshotFileEntry> = Vec::with_capacity(source_files.len());
    let mut total_size_bytes = 0u64;

    for (relative_path, source_path) in source_files {
        validate_relative_path(relative_path)?;
        if files
            .iter()
            .any(|entry| entry.relative_path == *relative_path)
        {
            // 同一 relative_path 会映射到同一个对象 key，后写覆盖先写，
            // 这是调用方的清单错误，必须显式拒绝。
            return Err(StorageError::InvalidArgument(format!(
                "source_files 中存在重复的 relative_path: {relative_path}"
            ))
            .into());
        }

        // 压缩 + 计算 sha256 是 CPU/磁盘密集操作，放阻塞线程池，避免卡住 runtime。
        let spool = TempPath::reserve(DATA_SUFFIX)?;
        let spool_path = spool.path().to_path_buf();
        let source = source_path.clone();
        let (size_bytes, checksum) =
            tokio::task::spawn_blocking(move || compress_file(&source, &spool_path))
                .await
                .map_err(|err| {
                    StorageError::Internal(format!("压缩任务执行失败: {err}")).to_platform_error()
                })??;

        let key = object_key(&prefix, relative_path);
        let uploaded = store.put_file(&key, spool.path()).await?;
        tracing::debug!(
            key = %key,
            original_bytes = size_bytes,
            stored_bytes = uploaded,
            "snapshot 数据对象已上传"
        );

        total_size_bytes += size_bytes;
        files.push(SnapshotFileEntry {
            relative_path: relative_path.clone(),
            size_bytes,
            checksum,
            compression: COMPRESSION_ZSTD.to_string(),
        });
        // spool 由 TempPath 的 Drop 清理（含上面的任一错误提前返回路径）。
    }

    let mut manifest = SnapshotManifest {
        database_id: db_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        base_lsn,
        owner_epoch,
        engine_version: engine_version.to_string(),
        schema_version,
        created_at_unix_ms: Utc::now().timestamp_millis(),
        total_size_bytes,
        checksum: String::new(),
        files,
    };
    manifest.checksum = compute_manifest_checksum(&manifest);

    // manifest 最后上传：它的存在即代表快照完整可用。
    store
        .put(
            &manifest.manifest_key(),
            Bytes::from(manifest.to_json_bytes()?),
        )
        .await?;
    Ok(manifest)
}

/// 从对象存储读取快照 manifest。
///
/// 对象不存在或 JSON 非法都映射为 `SNAPSHOT_UNAVAILABLE`（该快照不可用）；
/// 连接类故障按存储故障原样返回，调用方可退避重试。
pub async fn load_manifest(
    store: &dyn ObjectStore,
    db_id: &str,
    snapshot_id: &str,
) -> Result<SnapshotManifest> {
    validate_component("database_id", db_id)?;
    validate_component("snapshot_id", snapshot_id)?;
    let key = manifest_object_key(db_id, snapshot_id);
    let bytes = store.get(&key).await?;
    serde_json::from_slice(&bytes)
        .map_err(|err| StorageError::SnapshotCorrupt(format!("{key}: {err}")).to_platform_error())
}

/// 下载并解压快照到 `dest_dir`，按 `relative_path` 恢复原始目录结构。
///
/// 每个文件在写入时同步校验（解压后的 sha256 与大小），不一致会删除半成品并返回
/// `CHECKSUM_MISMATCH` —— 绝不把损坏数据留成「看起来可用」的恢复目录。
pub async fn download_snapshot(
    store: &dyn ObjectStore,
    manifest: &SnapshotManifest,
    dest_dir: &Path,
) -> Result<()> {
    validate_manifest(manifest)?;
    tokio::fs::create_dir_all(dest_dir)
        .await
        .map_err(|err| StorageError::io(dest_dir, err))?;

    for entry in &manifest.files {
        let dest = dest_dir.join(&entry.relative_path);
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| StorageError::io(parent, err))?;
        }
        let key = manifest.object_key(entry);
        let spool = fetch_to_spool(store, &key).await?;

        let decoded = decode_entry(
            spool,
            key.clone(),
            entry.compression.clone(),
            WriteTarget::File(dest.clone()),
        )
        .await;
        let (size, checksum) = match decoded {
            Ok(decoded) => decoded,
            Err(err) => {
                // 解压失败同样不能留下半成品文件。
                let _ = tokio::fs::remove_file(&dest).await;
                return Err(err);
            }
        };

        if let Err(err) = check_entry(&key, entry, size, &checksum) {
            // 删除半成品，避免下游把损坏文件当成可用快照。
            let _ = tokio::fs::remove_file(&dest).await;
            return Err(err);
        }
    }
    Ok(())
}

/// 校验整个快照：manifest 自身 + 每个对象的解压后 sha256 与大小。
///
/// 会完整下载并解压所有对象（解压到 sink，不落盘），因此是重操作，
/// 适合用于恢复前校验、后台巡检；遇到第一个不一致立即返回。
pub async fn verify_snapshot(store: &dyn ObjectStore, manifest: &SnapshotManifest) -> Result<()> {
    validate_manifest(manifest)?;

    let declared_total: u64 = manifest
        .files
        .iter()
        .fold(0u64, |acc, entry| acc.saturating_add(entry.size_bytes));
    if declared_total != manifest.total_size_bytes {
        return Err(mismatch(
            &manifest.manifest_key(),
            &manifest.total_size_bytes.to_string(),
            &declared_total.to_string(),
            json!({
                "kind": "total_size",
                "declared": manifest.total_size_bytes,
                "computed": declared_total,
            }),
        ));
    }

    for entry in &manifest.files {
        let key = manifest.object_key(entry);
        let spool = fetch_to_spool(store, &key).await?;
        let (size, checksum) = decode_entry(
            spool,
            key.clone(),
            entry.compression.clone(),
            WriteTarget::Sink,
        )
        .await?;
        check_entry(&key, entry, size, &checksum)?;
    }
    Ok(())
}

/// manifest 级校验：id 合法、路径合法、压缩算法已知、manifest 摘要自洽。
///
/// 放在下载 / 校验的最前面：manifest 本身被篡改时立刻失败，不必下载任何数据。
fn validate_manifest(manifest: &SnapshotManifest) -> Result<()> {
    validate_component("database_id", &manifest.database_id)?;
    validate_component("snapshot_id", &manifest.snapshot_id)?;
    for entry in &manifest.files {
        validate_relative_path(&entry.relative_path)?;
        match entry.compression.as_str() {
            COMPRESSION_ZSTD | COMPRESSION_NONE => {}
            other => {
                return Err(StorageError::InvalidArgument(format!(
                    "不支持的解压算法 {other:?}（{}）",
                    entry.relative_path
                ))
                .into())
            }
        }
    }
    let expected = compute_manifest_checksum(manifest);
    if expected != manifest.checksum {
        return Err(mismatch(
            &manifest.manifest_key(),
            &manifest.checksum,
            &expected,
            json!({ "kind": "manifest", "snapshot_id": manifest.snapshot_id }),
        ));
    }
    Ok(())
}

/// 逐个文件比对 (原始大小, sha256) 与 manifest 记录是否一致。
fn check_entry(key: &str, entry: &SnapshotFileEntry, size: u64, checksum: &str) -> Result<()> {
    if size != entry.size_bytes {
        return Err(mismatch(
            key,
            &entry.size_bytes.to_string(),
            &size.to_string(),
            json!({
                "kind": "size",
                "relative_path": entry.relative_path,
                "expected": entry.size_bytes,
                "actual": size,
            }),
        ));
    }
    if checksum != entry.checksum {
        return Err(mismatch(
            key,
            &entry.checksum,
            checksum,
            json!({
                "kind": "checksum",
                "relative_path": entry.relative_path,
                "expected": entry.checksum,
                "actual": checksum,
            }),
        ));
    }
    Ok(())
}

/// 构造带结构化 detail 的 `CHECKSUM_MISMATCH`。
fn mismatch(key: &str, expected: &str, actual: &str, detail: serde_json::Value) -> PlatformError {
    StorageError::ChecksumMismatch {
        key: key.to_string(),
        expected: expected.to_string(),
        actual: actual.to_string(),
    }
    .to_platform_error()
    .with_detail(detail)
}

/// 下载对象到 spool 临时文件（Drop 时自动清理）。
async fn fetch_to_spool(store: &dyn ObjectStore, key: &str) -> Result<TempPath> {
    let spool = TempPath::reserve(DATA_SUFFIX)?;
    store.get_to_file(key, spool.path()).await?;
    Ok(spool)
}

/// 解压目标：文件或丢弃（用于 verify）。
#[derive(Debug, Clone)]
enum WriteTarget {
    File(PathBuf),
    Sink,
}

/// 把 spool 中的对象解压 / 解包到目标，返回 (原始大小, sha256)。
///
/// 解压失败按数据损坏处理（`CHECKSUM_MISMATCH`）：能读到对象却解不开，
/// 说明 artifact 本身坏了，重试存储不会让内容变好。
async fn decode_entry(
    spool: TempPath,
    key: String,
    compression: String,
    target: WriteTarget,
) -> Result<(u64, String)> {
    tokio::task::spawn_blocking(move || {
        let path = spool.path().to_path_buf();
        match target {
            WriteTarget::File(dest) => {
                let file = File::create(&dest).map_err(|err| StorageError::io(&dest, err))?;
                decode_to(&path, &key, &compression, BufWriter::new(file))
            }
            WriteTarget::Sink => decode_to(&path, &key, &compression, io::sink()),
        }
    })
    .await
    .map_err(|err| StorageError::Internal(format!("解压任务执行失败: {err}")).to_platform_error())?
}

/// 同步解压：spool -> writer，边写边算 sha256 与字节数。
fn decode_to<W: Write>(
    spool: &Path,
    key: &str,
    compression: &str,
    writer: W,
) -> Result<(u64, String)> {
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, open_read(spool)?);
    let mut hashing = HashingWriter::new(writer);
    match compression {
        COMPRESSION_ZSTD => {
            zstd::stream::copy_decode(reader, &mut hashing).map_err(|err| {
                StorageError::ChecksumMismatch {
                    key: key.to_string(),
                    expected: "readable zstd stream".to_string(),
                    actual: format!("decompression failed: {err}"),
                }
                .to_platform_error()
            })?;
        }
        COMPRESSION_NONE => {
            io::copy(&mut reader, &mut hashing).map_err(|err| StorageError::io(spool, err))?;
        }
        other => {
            return Err(StorageError::InvalidArgument(format!("不支持的解压算法 {other:?}")).into())
        }
    }
    hashing
        .finish()
        .map_err(|err| StorageError::io(spool, err).into())
}

/// 同步压缩：src -> spool，返回 (原始大小, sha256)。
fn compress_file(src: &Path, spool: &Path) -> Result<(u64, String)> {
    let reader = BufReader::with_capacity(IO_BUFFER_BYTES, open_read(src)?);
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_write(spool)?);
    let mut hashing = HashingReader::new(reader);
    zstd::stream::copy_encode(&mut hashing, &mut writer, ZSTD_LEVEL)
        .map_err(|err| StorageError::io(spool, err))?;
    writer.flush().map_err(|err| StorageError::io(spool, err))?;
    Ok((hashing.count, hashing.digest()))
}

fn open_read(path: &Path) -> Result<File> {
    File::open(path).map_err(|err| StorageError::io(path, err).into())
}

fn create_write(path: &Path) -> Result<File> {
    File::create(path).map_err(|err| StorageError::io(path, err).into())
}

/// 读取时同步计算 sha256 与字节数。
struct HashingReader<R> {
    inner: R,
    hasher: Sha256,
    count: u64,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            count: 0,
        }
    }

    fn digest(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        self.count += read as u64;
        Ok(read)
    }
}

/// 写入时同步计算 sha256 与字节数。
struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    count: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            count: 0,
        }
    }

    /// flush 底层 writer 并返回 (写入字节数, sha256)。
    fn finish(mut self) -> io::Result<(u64, String)> {
        self.inner.flush()?;
        let count = self.count;
        let digest = hex::encode(self.hasher.finalize());
        Ok((count, digest))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.count += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// 临时文件路径守卫：Drop 时删除。
///
/// 用它包住所有 spool 文件，任何提前返回（含 `?`）都不会残留 GB 级中间文件。
struct TempPath(PathBuf);

impl TempPath {
    /// 在 spool 目录中预留一个唯一路径（此时并不创建文件）。
    fn reserve(suffix: &str) -> Result<Self> {
        let dir = spool_dir();
        std::fs::create_dir_all(&dir).map_err(|err| StorageError::io(&dir, err))?;
        Ok(Self(dir.join(format!("{}{suffix}", Uuid::new_v4()))))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// spool 目录：`SNAPSHOT_SPOOL_DIR`（部署时可指到大盘），否则系统临时目录。
fn spool_dir() -> PathBuf {
    env::var_os(ENV_SPOOL_DIR)
        .map(PathBuf::from)
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| env::temp_dir().join(SPOOL_DIR_NAME))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryObjectStore;
    use domain::error::ErrorCode;
    use tempfile::TempDir;

    /// 构造源文件：`(relative_path, 物理文件名, 内容)`；物理名故意与相对路径不同，
    /// 以证明对象 key 与恢复位置只由 relative_path 决定。
    fn make_sources(dir: &Path, files: &[(&str, &str, Vec<u8>)]) -> Vec<(String, PathBuf)> {
        files
            .iter()
            .map(|(relative, physical, content)| {
                let path = dir.join(physical);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&path, content).unwrap();
                ((*relative).to_string(), path)
            })
            .collect()
    }

    async fn upload_two_file_snapshot(
        store: &InMemoryObjectStore,
        dir: &Path,
    ) -> (SnapshotManifest, Vec<u8>, Vec<u8>) {
        // 高可压缩内容：验证「确实压缩后再上传」。
        let main_content = vec![b'x'; 300 * 1024];
        let meta_content = br#"{"page_size":4096,"schema_version":7}"#.to_vec();
        let sources = make_sources(
            dir,
            &[
                ("data/main.db", "main.db.raw", main_content.clone()),
                ("data/meta/config.json", "config.json", meta_content.clone()),
            ],
        );
        let manifest = upload_snapshot(
            store,
            "db-1",
            "snap-1",
            4096,
            7,
            "turso-v0.8.1",
            7,
            &sources,
        )
        .await
        .expect("上传快照");
        (manifest, main_content, meta_content)
    }

    #[test]
    fn sha256_known_vectors() {
        // FIPS 180-4 已知向量，锁定「十六进制小写」编码。
        assert_eq!(
            compute_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            compute_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            compute_sha256(b"The quick brown fox jumps over the lazy dog"),
            "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
        );
        // 输出必须是小写十六进制且长度固定。
        let digest = compute_sha256(&[0u8; 1024]);
        assert_eq!(digest.len(), 64);
        assert!(digest
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn object_keys_are_stable() {
        assert_eq!(object_prefix("db-1", "snap-9"), "snapshots/db-1/snap-9/");
        assert_eq!(
            object_key("snapshots/db-1/snap-9/", "data/page1.db"),
            "snapshots/db-1/snap-9/data/page1.db.zst"
        );
        assert_eq!(
            manifest_object_key("db-1", "snap-9"),
            "snapshots/db-1/snap-9/snap-9.manifest.json"
        );
        assert!(manifest_object_key("db-1", "snap-9").ends_with(MANIFEST_SUFFIX));
    }

    #[test]
    fn manifest_json_roundtrip() {
        let manifest = SnapshotManifest {
            database_id: "db-1".into(),
            snapshot_id: "snap-1".into(),
            base_lsn: 4096,
            owner_epoch: 12,
            engine_version: "turso-v0.8.1".into(),
            schema_version: 3,
            created_at_unix_ms: 1_760_000_000_000,
            total_size_bytes: 2048,
            checksum: "deadbeef".into(),
            files: vec![
                SnapshotFileEntry {
                    relative_path: "data/main.db".into(),
                    size_bytes: 1024,
                    checksum: "aa".into(),
                    compression: COMPRESSION_ZSTD.into(),
                },
                SnapshotFileEntry {
                    relative_path: "data/meta.json".into(),
                    size_bytes: 1024,
                    checksum: "bb".into(),
                    compression: COMPRESSION_ZSTD.into(),
                },
            ],
        };

        let bytes = manifest.to_json_bytes().unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'), "JSON 末尾补换行");
        let parsed = SnapshotManifest::from_json_bytes(&bytes).unwrap();
        assert_eq!(parsed, manifest, "序列化 / 反序列化必须无损");

        // 字段名是对外契约（catalog / 其他服务会直接读这份 JSON）。
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for key in [
            "database_id",
            "snapshot_id",
            "base_lsn",
            "owner_epoch",
            "engine_version",
            "schema_version",
            "created_at_unix_ms",
            "total_size_bytes",
            "checksum",
            "files",
        ] {
            assert!(value.get(key).is_some(), "manifest 缺少字段 {key}");
        }
        assert_eq!(value["files"][0]["relative_path"], "data/main.db");
        assert_eq!(value["files"][0]["compression"], "zstd");

        // 非法 JSON -> INVALID_ARGUMENT（不是内部错误）。
        let err = SnapshotManifest::from_json_bytes(b"{not json").unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);

        // 摘要覆盖元数据与文件清单：任一字段变化都会改变摘要。
        let baseline = compute_manifest_checksum(&manifest);
        let mut changed = manifest.clone();
        changed.base_lsn += 1;
        assert_ne!(compute_manifest_checksum(&changed), baseline);
        let mut changed = manifest.clone();
        changed.files[0].checksum = "cc".into();
        assert_ne!(compute_manifest_checksum(&changed), baseline);
        // checksum 字段自身不参与摘要（否则无法自洽）。
        let mut changed = manifest.clone();
        changed.checksum = "whatever".into();
        assert_eq!(compute_manifest_checksum(&changed), baseline);
    }

    #[tokio::test]
    async fn upload_verify_download_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, main_content, meta_content) =
            upload_two_file_snapshot(&store, dir.path()).await;

        // ---- manifest 内容
        assert_eq!(manifest.database_id, "db-1");
        assert_eq!(manifest.snapshot_id, "snap-1");
        assert_eq!(manifest.base_lsn, 4096);
        assert_eq!(manifest.owner_epoch, 7);
        assert_eq!(manifest.engine_version, "turso-v0.8.1");
        assert_eq!(manifest.schema_version, 7);
        assert_eq!(
            manifest.total_size_bytes,
            (main_content.len() + meta_content.len()) as u64
        );
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.files[0].relative_path, "data/main.db");
        assert_eq!(manifest.files[0].size_bytes, main_content.len() as u64);
        assert_eq!(manifest.files[0].checksum, compute_sha256(&main_content));
        assert_eq!(manifest.files[0].compression, COMPRESSION_ZSTD);
        assert_eq!(manifest.checksum, compute_manifest_checksum(&manifest));
        assert!(manifest.created_at_unix_ms > 0);

        // ---- 对象布局：每个文件一个 .zst 对象 + manifest，且确实被压缩
        let keys: Vec<String> = store
            .list(&manifest.prefix())
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.key)
            .collect();
        assert_eq!(
            keys,
            vec![
                "snapshots/db-1/snap-1/data/main.db.zst".to_string(),
                "snapshots/db-1/snap-1/data/meta/config.json.zst".to_string(),
                "snapshots/db-1/snap-1/snap-1.manifest.json".to_string(),
            ]
        );
        let stored = store.head(&keys[0]).await.unwrap().unwrap();
        assert!(
            stored.size < main_content.len() as u64 / 4,
            "300 KiB 的高可压缩内容压缩后应显著变小，实际 {} 字节",
            stored.size
        );

        // ---- 校验通过
        verify_snapshot(&store, &manifest)
            .await
            .expect("校验应通过");

        // ---- manifest 可重新装载
        let loaded = load_manifest(&store, "db-1", "snap-1").await.unwrap();
        assert_eq!(loaded, manifest);

        // ---- 下载恢复：目录结构与内容一致
        let dest = dir.path().join("restore");
        download_snapshot(&store, &manifest, &dest).await.unwrap();
        assert_eq!(
            tokio::fs::read(dest.join("data/main.db")).await.unwrap(),
            main_content
        );
        assert_eq!(
            tokio::fs::read(dest.join("data/meta/config.json"))
                .await
                .unwrap(),
            meta_content
        );
    }

    #[tokio::test]
    async fn verify_detects_tampered_object() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;
        let target = manifest.object_key(&manifest.files[0]);

        // (a) 直接写入非 zstd 垃圾数据：解压失败同样按校验失败处理。
        store
            .put(&target, Bytes::from_static(b"garbage"))
            .await
            .unwrap();
        let err = verify_snapshot(&store, &manifest).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);

        // (b) 写入合法 zstd 但内容不同：大小/摘要不匹配。
        let recompressed = zstd::stream::encode_all(&b"other content"[..], ZSTD_LEVEL).unwrap();
        store.put(&target, Bytes::from(recompressed)).await.unwrap();
        let err = verify_snapshot(&store, &manifest).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);

        // (c) 篡改条目声明的字节数（manifest 摘要与 total 都同步重算，模拟更强的伪造）
        let mut forged = manifest.clone();
        forged.files[0].size_bytes += 1;
        forged.total_size_bytes += 1;
        forged.checksum = compute_manifest_checksum(&forged);
        let err = verify_snapshot(&store, &forged).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);
        assert_eq!(err.detail.as_ref().unwrap()["kind"], "size");

        // 恢复原对象后校验重新通过（确认失败确实来自篡改）。
        let raw = tokio::fs::read(dir.path().join("main.db.raw"))
            .await
            .unwrap();
        let original = zstd::stream::encode_all(&raw[..], ZSTD_LEVEL).unwrap();
        store.put(&target, Bytes::from(original)).await.unwrap();
        verify_snapshot(&store, &manifest)
            .await
            .expect("恢复后应通过");
    }

    #[tokio::test]
    async fn verify_detects_tampered_manifest() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;

        // 篡改文件清单但不更新 manifest 摘要 -> manifest 自校验失败，无需下载数据。
        let mut forged = manifest.clone();
        forged.files[0].checksum = compute_sha256(b"attacker content");
        let err = verify_snapshot(&store, &forged).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);
        assert_eq!(err.detail.as_ref().unwrap()["kind"], "manifest");

        // 篡改 base_lsn（恢复起点！）同样会被摘要抓住。
        let mut forged = manifest.clone();
        forged.base_lsn = 0;
        let err = verify_snapshot(&store, &forged).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);

        // total_size_bytes 与文件清单不一致。
        let mut forged = manifest.clone();
        forged.total_size_bytes += 1;
        forged.checksum = compute_manifest_checksum(&forged);
        let err = verify_snapshot(&store, &forged).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);
        assert_eq!(err.detail.as_ref().unwrap()["kind"], "total_size");
    }

    #[tokio::test]
    async fn missing_object_is_snapshot_unavailable() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;

        // 数据对象丢失（生命周期规则误删 / bucket 配错）。
        let target = manifest.object_key(&manifest.files[1]);
        store.delete(&target).await.unwrap();
        let err = verify_snapshot(&store, &manifest).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SnapshotUnavailable);
        // 对象确实不存在时重试不会自愈（调用方应改用更早的快照），
        // 因此显式覆盖 domain 对 SNAPSHOT_UNAVAILABLE 的默认可重试语义。
        assert!(!err.retryable);

        // manifest 丢失。
        store.delete(&manifest.manifest_key()).await.unwrap();
        let err = load_manifest(&store, "db-1", "snap-1").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SnapshotUnavailable);
    }

    #[tokio::test]
    async fn load_manifest_detects_corrupt_json() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;
        store
            .put(
                &manifest.manifest_key(),
                Bytes::from_static(b"{\"truncated\":"),
            )
            .await
            .unwrap();
        let err = load_manifest(&store, "db-1", "snap-1").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SnapshotUnavailable);
    }

    #[tokio::test]
    async fn empty_file_roundtrip() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let sources = make_sources(dir.path(), &[("data/empty.db", "empty.db", Vec::new())]);
        let manifest = upload_snapshot(
            &store,
            "db-1",
            "snap-empty",
            1,
            1,
            "turso-v0.8.1",
            1,
            &sources,
        )
        .await
        .unwrap();
        assert_eq!(manifest.total_size_bytes, 0);
        assert_eq!(
            manifest.files[0].checksum,
            compute_sha256(b""),
            "空文件也要有确定的摘要"
        );
        verify_snapshot(&store, &manifest)
            .await
            .expect("空快照应通过校验");

        let dest = dir.path().join("restore");
        download_snapshot(&store, &manifest, &dest).await.unwrap();
        let restored = tokio::fs::metadata(dest.join("data/empty.db"))
            .await
            .unwrap();
        assert_eq!(restored.len(), 0);
    }

    #[tokio::test]
    async fn none_compression_artifact_is_supported() {
        // 兼容外部产出（未压缩）的 artifact：manifest 里 compression = "none"。
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let content = b"CREATE TABLE t(id INTEGER);".to_vec();
        let mut manifest = SnapshotManifest {
            database_id: "db-1".into(),
            snapshot_id: "snap-raw".into(),
            base_lsn: 10,
            owner_epoch: 1,
            engine_version: "turso-v0.8.1".into(),
            schema_version: 1,
            created_at_unix_ms: 1_760_000_000_000,
            total_size_bytes: content.len() as u64,
            checksum: String::new(),
            files: vec![SnapshotFileEntry {
                relative_path: "data/schema.sql".into(),
                size_bytes: content.len() as u64,
                checksum: compute_sha256(&content),
                compression: COMPRESSION_NONE.into(),
            }],
        };
        manifest.checksum = compute_manifest_checksum(&manifest);
        store
            .put(
                &manifest.object_key(&manifest.files[0]),
                Bytes::from(content.clone()),
            )
            .await
            .unwrap();

        verify_snapshot(&store, &manifest).await.unwrap();
        let dest = dir.path().join("restore");
        download_snapshot(&store, &manifest, &dest).await.unwrap();
        assert_eq!(
            tokio::fs::read(dest.join("data/schema.sql")).await.unwrap(),
            content
        );

        // 未知压缩算法 -> INVALID_ARGUMENT（不能猜，猜错就是静默损坏）。
        let mut unknown = manifest.clone();
        unknown.files[0].compression = "snappy".into();
        unknown.checksum = compute_manifest_checksum(&unknown);
        let err = verify_snapshot(&store, &unknown).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn upload_rejects_unsafe_inputs() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let file = dir.path().join("f.db");
        std::fs::write(&file, b"data").unwrap();

        for bad in [
            "../escape.db",
            "/etc/passwd",
            "data/../../escape.db",
            "data//double.db",
            "data/./dot.db",
            "data\\win.db",
            "",
        ] {
            let sources = vec![(bad.to_string(), file.clone())];
            let err = upload_snapshot(&store, "db-1", "snap-x", 1, 1, "turso-v0.8.1", 1, &sources)
                .await
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidArgument, "应拒绝路径 {bad:?}");
            assert!(store.is_empty(), "拒绝的参数不得产生任何对象");
        }

        // 重复 relative_path -> 对象 key 冲突。
        let sources = vec![
            ("data/a.db".to_string(), file.clone()),
            ("data/a.db".to_string(), file.clone()),
        ];
        let err = upload_snapshot(&store, "db-1", "snap-x", 1, 1, "turso-v0.8.1", 1, &sources)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);

        // id 非法。
        let sources = vec![("data/a.db".to_string(), file.clone())];
        for (db, snap) in [("", "s"), ("d/b", "s"), ("db", ""), ("db", "../s")] {
            let err = upload_snapshot(&store, db, snap, 1, 1, "turso-v0.8.1", 1, &sources)
                .await
                .unwrap_err();
            assert_eq!(
                err.code,
                ErrorCode::InvalidArgument,
                "应拒绝 id {db:?}/{snap:?}"
            );
        }

        // 源文件不存在 -> INVALID_ARGUMENT。
        let sources = vec![(
            "data/missing.db".to_string(),
            dir.path().join("does-not-exist"),
        )];
        let err = upload_snapshot(&store, "db-1", "snap-x", 1, 1, "turso-v0.8.1", 1, &sources)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn download_refuses_path_traversal() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;

        // 伪造一个「自洽」但越界的 manifest（摘要也重算过）：
        // 必须靠 relative_path 校验挡住，而不是靠摘要。
        let mut forged = manifest.clone();
        forged.files[0].relative_path = "../escaped.db".into();
        forged.checksum = compute_manifest_checksum(&forged);

        let dest = dir.path().join("restore");
        let err = download_snapshot(&store, &forged, &dest).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(
            !dir.path().join("escaped.db").exists(),
            "不得写出 dest 之外"
        );
    }

    #[tokio::test]
    async fn download_reports_corrupt_object_without_leaving_files() {
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;
        let target = manifest.object_key(&manifest.files[0]);
        store
            .put(&target, Bytes::from_static(b"garbage"))
            .await
            .unwrap();

        let dest = dir.path().join("restore");
        let err = download_snapshot(&store, &manifest, &dest)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);
        assert!(
            !dest.join("data/main.db").exists(),
            "损坏文件不得留在恢复目录"
        );
    }

    #[tokio::test]
    async fn download_rejects_mismatched_stored_manifest() {
        // 对象在 store 里，但 manifest 与实际内容不符（如指向了别的快照）：
        // 下载必须失败，不允许写入不匹配的数据。
        let dir = TempDir::new().unwrap();
        let store = InMemoryObjectStore::new();
        let (manifest, _, _) = upload_two_file_snapshot(&store, dir.path()).await;
        let mut forged = manifest.clone();
        forged.files[0].checksum = compute_sha256(b"different");
        forged.checksum = compute_manifest_checksum(&forged);

        let dest = dir.path().join("restore");
        let err = download_snapshot(&store, &forged, &dest).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::ChecksumMismatch);
    }

    #[test]
    fn validate_relative_path_rules() {
        for ok in ["a", "data/main.db", "db/x/y.sqlite3", "a-b_c.d"] {
            validate_relative_path(ok).unwrap_or_else(|err| panic!("{ok:?} 应通过: {err}"));
        }
        for bad in [
            "",
            "/abs",
            "a//b",
            "..",
            "../x",
            "a/../../b",
            "a/./b",
            "a\\b",
        ] {
            assert!(validate_relative_path(bad).is_err(), "{bad:?} 应被拒绝");
        }
    }

    #[tokio::test]
    async fn spool_files_are_cleaned_up() {
        let spool = TempPath::reserve(".zst").unwrap();
        let path = spool.path().to_path_buf();
        std::fs::write(&path, b"temporary").unwrap();
        assert!(path.exists());
        drop(spool);
        assert!(!path.exists(), "spool 文件必须在 Drop 时删除");
    }
}
