//! 冷启动 / 恢复编排（架构 §8 / §11.2）。
//!
//! 启动一个 DB 进程之前，Worker 必须把「工作集」准备好：
//!
//! ```text
//! 本地已有工作集（epoch 匹配）  ────────────────> 直接拉起（零网络）
//! 否则：
//!   Snapshot（Object Storage） ──> 解压到 <data_dir>/<db_id>/
//!   Remote WAL [base_lsn, last) ──> 按 wal_file_offset 回放到本地 WAL 文件
//!   ──> 写回工作集元数据（epoch / snapshot / applied_lsn）──> 拉起进程
//! ```
//!
//! **为什么 db-worker 不依赖 engine-adapter**：Worker 只做「字节搬运」，WAL 字节怎么
//! 解析、怎么 apply 到 engine 是 DB Process 自己的事（架构 §17.3 的进程边界）。回放规则
//! 只有两条，见 [`replay_wal_segments`]：
//!
//! 1. 段携带 `file_offset`，按该偏移写入本地 WAL 文件；
//! 2. 段标记 `reset_wal`，写之前先截断（新一代 WAL）。
//!
//! Epoch 语义：工作集元数据里记录生成它的 `owner_epoch`。只有请求的 epoch 与它**完全
//! 相等**时才允许复用本地文件；否则本地数据可能落后于（也可能领先于）远端 WAL ——
//! 两种情况都必须重新从 Snapshot + Remote WAL 恢复，绝不允许「接着本地文件继续跑」。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use domain::ids::DatabaseId;
use domain::wal::Lsn;
use objectstore::snapshot::{self, SnapshotManifest};
use objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use wal_client::{WalClient, WalSegment};

use crate::error::{Result, WorkerError};
use crate::paths;

/// 工作集元数据（Worker 私有，落在 DB 目录下，不参与快照上传）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkSetMeta {
    /// 所属数据库 id。
    pub database_id: String,
    /// 生成该工作集时的 owner epoch。
    pub owner_epoch: u64,
    /// 恢复所用的快照 id；空表示「无快照」。
    pub snapshot_id: String,
    /// 快照基线 LSN。
    pub base_lsn: u64,
    /// 本地 WAL 已回放到的 LSN（exclusive）。
    pub applied_lsn: u64,
    /// 写入时刻（Unix 毫秒）。
    pub updated_at_unix_ms: i64,
    /// 引擎版本（跨版本恢复需要校验兼容性）。
    pub engine_version: String,
}

/// Snapshot ledger 中的一条记录。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    /// 快照 id。
    pub snapshot_id: String,
    /// 快照基线 LSN。
    pub base_lsn: u64,
    /// 快照校验和（manifest 的 checksum）。
    pub checksum: String,
    /// 快照大小（字节）。
    pub size_bytes: u64,
    /// 生成时间（Unix 毫秒）。
    pub created_at_unix_ms: i64,
    /// 生成快照时的 owner epoch。
    pub owner_epoch: u64,
}

/// Snapshot ledger：本 Worker 已知的该 DB 快照列表（新的在前）。
///
/// 为什么不查 Catalog：Worker 不连 PostgreSQL（架构 §5.1 只要求它被 Server 控制）。
/// 恢复所需的最近快照信息由本 Worker 自己在生成快照时记下来；PrepareMove 则直接使用
/// Server 显式给定的 snapshot_id / base_lsn。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotLedger {
    /// 记录（按时间倒序，最多保留 [`SnapshotLedger::MAX_ENTRIES`] 条）。
    pub entries: Vec<LedgerEntry>,
}

impl SnapshotLedger {
    /// ledger 最多保留的记录数（防止无界增长）。
    pub const MAX_ENTRIES: usize = 8;

    /// 最近一次快照。
    pub fn latest(&self) -> Option<&LedgerEntry> {
        self.entries.first()
    }

    /// 记录一次快照（去重 + 截断）。
    pub fn record(&mut self, entry: LedgerEntry) {
        self.entries
            .retain(|item| item.snapshot_id != entry.snapshot_id);
        self.entries.insert(0, entry);
        self.entries.truncate(Self::MAX_ENTRIES);
    }
}

/// 工作集准备结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedWorkSet {
    /// 恢复所用快照 id（可能为空）。
    pub snapshot_id: String,
    /// 快照基线 LSN。
    pub base_lsn: u64,
    /// 本地 WAL 已回放到的 LSN。
    pub applied_lsn: u64,
    /// 是否直接复用了本地工作集（零网络路径）。
    pub reused_local: bool,
    /// 是否真的从对象存储下载过快照。
    pub downloaded_snapshot: bool,
}

impl PreparedWorkSet {
    /// 复用的本地工作集。
    fn from_local(meta: &WorkSetMeta) -> Self {
        Self {
            snapshot_id: meta.snapshot_id.clone(),
            base_lsn: meta.base_lsn,
            applied_lsn: meta.applied_lsn,
            reused_local: true,
            downloaded_snapshot: false,
        }
    }
}

/// 恢复来源（PrepareMove 会显式给出；StartDatabase 走 ledger / 本地元数据推断）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotSource {
    /// 快照 id。
    pub snapshot_id: String,
    /// 快照基线 LSN（0 表示以 manifest 中记录的为准）。
    pub base_lsn: u64,
}

/// 工作集准备请求。
#[derive(Clone, Debug)]
pub struct PrepareWork {
    /// 数据库 id。
    pub database_id: String,
    /// 目标 owner epoch。
    pub owner_epoch: u64,
    /// 显式指定的快照来源（Move 预拉取时使用）。
    pub snapshot: Option<SnapshotSource>,
    /// 是否允许复用本地工作集（崩溃重启 / 同 epoch 重新拉起时为 true）。
    pub allow_local_reuse: bool,
    /// 是否允许在没有快照的情况下从 Remote WAL 的可见起点回放
    /// （新建 DB、或本 Worker 从零接管时使用）。
    pub allow_wal_only: bool,
}

/// 工作集准备器（持有可选的 Snapshot / WAL 依赖）。
#[derive(Clone)]
pub struct WorkSetPreparer {
    data_dir: PathBuf,
    store: Option<Arc<dyn ObjectStore>>,
    wal: Option<Arc<WalClient>>,
    engine_version: String,
    /// 本 Worker 的身份：确立 WAL 所有权时作为写者上报（架构 §10）。
    worker_id: domain::WorkerId,
}

impl std::fmt::Debug for WorkSetPreparer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkSetPreparer")
            .field("data_dir", &self.data_dir)
            .field("object_store", &self.store.is_some())
            .field("wal_client", &self.wal.is_some())
            .field("engine_version", &self.engine_version)
            .field("worker_id", &self.worker_id)
            .finish()
    }
}

impl WorkSetPreparer {
    /// 构造（依赖可为空：开发环境可能不接对象存储）。
    pub fn new(
        data_dir: PathBuf,
        store: Option<Arc<dyn ObjectStore>>,
        wal: Option<Arc<WalClient>>,
        engine_version: impl Into<String>,
        worker_id: domain::WorkerId,
    ) -> Self {
        Self {
            data_dir,
            store,
            wal,
            engine_version: engine_version.into(),
            worker_id,
        }
    }

    /// 对象存储是否可用（决定快照能力）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn has_object_store(&self) -> bool {
        self.store.is_some()
    }

    /// Remote WAL 客户端是否可用（决定远端回放能力）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// 数据目录。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// 为 DB 创建工作集目录结构（幂等）。
    ///
    /// **只创建到工作集根目录 `<data_dir>/<db_id>/` 为止**：它下面的 `db`（主库）与
    /// `db-wal` 都是引擎自己创建的**文件**。把 `db` 建成目录会让 db-runtime 在
    /// `EngineAdapter::open` 时拿到 `EISDIR`（"is a directory"）而立刻退出，
    /// Worker 侧只能看到「子进程刚 spawn 就没了」。
    pub async fn prepare_dirs(&self, db_id: &str) -> Result<()> {
        let dir = paths::db_dir(&self.data_dir, db_id);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|err| WorkerError::io(format!("创建目录 {}", dir.display()), err))?;
        self.remove_stale_db_dir(db_id).await;
        Ok(())
    }

    /// 清理「主库路径上残留的目录」。
    ///
    /// 历史版本把 `<db_id>/db` 当目录创建（那正是「spawn 出来的 DB Process 立刻退出」
    /// 的根因），这样留下的工作集会让引擎永远 `EISDIR`、无法自愈。这里只清理**空目录**
    /// —— bug 版本从未成功打开过数据库，因此残留必然是空的；非空目录只告警，绝不删数据。
    async fn remove_stale_db_dir(&self, db_id: &str) {
        let db_path = paths::db_path(&self.data_dir, db_id);
        if !tokio::fs::symlink_metadata(&db_path)
            .await
            .is_ok_and(|meta| meta.is_dir())
        {
            return;
        }
        match tokio::fs::remove_dir(&db_path).await {
            Ok(()) => tracing::warn!(
                db_id = %db_id,
                path = %db_path.display(),
                "主库路径上残留着目录（历史版本把它当目录创建），已清理"
            ),
            Err(err) => tracing::warn!(
                db_id = %db_id,
                path = %db_path.display(),
                error = %err,
                "主库路径上是非空目录，无法自动清理；DB Process 打开数据库会失败"
            ),
        }
    }

    /// 在 Remote WAL 中确立本 Worker 对某 DB 的写所有权。
    ///
    /// 语义：`SetOwnerEpoch` 是单调的，WAL 会拒绝任何**更低** epoch 的后续 append。
    /// 因此这一步之后，旧 Owner 即使进程仍在也无法再写入（Storage-level Fencing）。
    async fn establish_wal_ownership(&self, db_id: &str, owner_epoch: u64) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            // 未配置 WAL 客户端的场景（本地开发）：没有远端存储可对齐，跳过。
            return Ok(());
        };
        let database_id: DatabaseId = db_id.parse().map_err(|err| {
            WorkerError::Restore(format!("database_id {db_id} 不是合法 UUID：{err}"))
        })?;
        // 0 表示「尚无 Owner」：WAL 侧要求 epoch 严格递增，用 0 去推进没有意义，
        // 这种情况交给后续真正携带 epoch 的启动流程处理。
        if owner_epoch == 0 {
            return Ok(());
        }
        let applied = wal
            .set_owner_epoch(&database_id, owner_epoch, &self.worker_id)
            .await
            .map_err(|err| WorkerError::Wal(format!("确立 WAL 所有权失败：{err}")))?;
        tracing::debug!(
            db_id = %db_id,
            requested_epoch = owner_epoch,
            applied_epoch = applied,
            "WAL owner epoch 已确立（旧 epoch 的写入将被 Storage 层拒绝）"
        );
        Ok(())
    }

    /// 准备（或复用）工作集。
    pub async fn prepare(&self, request: &PrepareWork) -> Result<PreparedWorkSet> {
        let db_id = request.database_id.as_str();
        self.prepare_dirs(db_id).await?;

        // 先确立 WAL 侧的 owner epoch，再谈恢复（架构 §10 / §11.3）。
        //
        // 为什么必须在最前面：Storage-level Fencing 的判据是 WAL 自己记住的 epoch。
        // 若不在启动时显式推进它，旧 Owner 的进程（可能还活着）在控制面已经改派之后
        // 仍可能用旧 epoch 写入成功 —— 唯一的防线就只剩控制面，而架构要求防线在存储层。
        self.establish_wal_ownership(&request.database_id, request.owner_epoch)
            .await?;

        if request.allow_local_reuse {
            if let Some(meta) = self.local_work_set(db_id, request.owner_epoch).await {
                tracing::info!(
                    db_id = %db_id,
                    owner_epoch = request.owner_epoch,
                    applied_lsn = meta.applied_lsn,
                    "复用本地工作集，跳过快照下载与 WAL 回放"
                );
                return Ok(PreparedWorkSet::from_local(&meta));
            }
        }

        let source = match request.snapshot.clone() {
            Some(source) => Some(source),
            None => self
                .ledger(db_id)
                .await
                .latest()
                .map(|entry| SnapshotSource {
                    snapshot_id: entry.snapshot_id.clone(),
                    base_lsn: entry.base_lsn,
                }),
        };

        // 下载快照（若有）
        let mut base_lsn = 0u64;
        let mut snapshot_id = String::new();
        let mut downloaded = false;
        let mut engine_version = self.engine_version.clone();
        if let Some(source) = source.as_ref() {
            let store = self.store.as_ref().ok_or_else(|| {
                WorkerError::Storage(format!(
                    "需要从快照 {} 恢复，但对象存储未配置",
                    source.snapshot_id
                ))
            })?;
            let manifest =
                snapshot::load_manifest(store.as_ref(), db_id, source.snapshot_id.as_str())
                    .await
                    .map_err(|err| {
                        WorkerError::Storage(format!("加载快照 manifest 失败：{err}"))
                    })?;
            if !manifest.engine_version.is_empty() {
                engine_version = manifest.engine_version.clone();
            }
            snapshot::download_snapshot(
                store.as_ref(),
                &manifest,
                &paths::db_dir(&self.data_dir, db_id),
            )
            .await
            .map_err(|err| WorkerError::Storage(format!("下载快照失败：{err}")))?;
            base_lsn = if source.base_lsn > 0 {
                source.base_lsn
            } else {
                manifest.base_lsn
            };
            snapshot_id = manifest.snapshot_id.clone();
            downloaded = true;
            tracing::info!(
                db_id = %db_id,
                snapshot_id = %snapshot_id,
                base_lsn,
                files = manifest.files.len(),
                "快照已解压到本地工作集"
            );
        }

        // 回放 Remote WAL
        let applied_lsn = self
            .replay_remote_wal(db_id, base_lsn, request.allow_wal_only)
            .await?;

        let meta = WorkSetMeta {
            database_id: db_id.to_string(),
            owner_epoch: request.owner_epoch,
            snapshot_id: snapshot_id.clone(),
            base_lsn,
            applied_lsn,
            updated_at_unix_ms: domain::time::now_unix_ms(),
            engine_version,
        };
        write_work_set_meta(&self.data_dir, db_id, &meta).await?;

        Ok(PreparedWorkSet {
            snapshot_id,
            base_lsn,
            applied_lsn,
            reused_local: false,
            downloaded_snapshot: downloaded,
        })
    }

    /// 本地工作集是否可复用（元数据 epoch 精确匹配）。
    pub async fn local_work_set(&self, db_id: &str, epoch: u64) -> Option<WorkSetMeta> {
        let meta = read_work_set_meta(&self.data_dir, db_id).await?;
        if meta.owner_epoch != epoch {
            tracing::info!(
                db_id = %db_id,
                local_epoch = meta.owner_epoch,
                requested_epoch = epoch,
                "本地工作集 epoch 不匹配，必须从快照 + Remote WAL 重新恢复"
            );
            return None;
        }
        Some(meta)
    }

    /// 读取 snapshot ledger。
    pub async fn ledger(&self, db_id: &str) -> SnapshotLedger {
        read_ledger(&self.data_dir, db_id).await
    }

    /// 记录一次快照（生成成功后调用）。
    pub async fn record_snapshot(&self, db_id: &str, entry: LedgerEntry) -> Result<()> {
        let mut ledger = self.ledger(db_id).await;
        ledger.record(entry);
        write_ledger(&self.data_dir, db_id, &ledger).await
    }

    /// 从 Remote WAL 回放到本地 WAL 文件，返回 applied LSN。
    ///
    /// 若未配置 WAL 客户端，直接返回 `base_lsn`（本地没有可回放的数据）。
    async fn replay_remote_wal(
        &self,
        db_id: &str,
        base_lsn: u64,
        allow_wal_only: bool,
    ) -> Result<u64> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(base_lsn);
        };
        let database_id: DatabaseId = db_id.parse().map_err(|err| {
            WorkerError::Restore(format!("database_id {db_id} 不是合法 UUID：{err}"))
        })?;

        let status = wal
            .status(&database_id)
            .await
            .map_err(|err| WorkerError::Wal(format!("查询 WAL 状态失败：{err}")))?;
        if !status.has_data || status.last_lsn.get() == 0 {
            // 远端没有该 DB 的 WAL：新建库的正常情形
            return Ok(base_lsn);
        }

        let first = status.first_lsn.get();
        let last = status.last_lsn.get();
        let start = if base_lsn == 0 {
            if !allow_wal_only && first > 0 {
                return Err(WorkerError::Restore(format!(
                    "db={db_id} 无本地快照且 Remote WAL 起点为 {first}（快照已被 trim），无法从零恢复"
                )));
            }
            first
        } else {
            if first > base_lsn {
                // 快照基线之后的一段 WAL 已被 trim：本地快照与现存 WAL 之间存在空洞，
                // 继续回放会得到「静默丢数据」的库，必须显式失败。
                return Err(WorkerError::Restore(format!(
                    "db={db_id} 快照 base_lsn={base_lsn} 早于 WAL first_lsn={first}，区间存在空洞"
                )));
            }
            base_lsn
        };

        if start >= last {
            return Ok(base_lsn.max(last));
        }

        let segments = wal
            .read_range(&database_id, Lsn::new(start), Lsn::new(last))
            .await
            .map_err(|err| WorkerError::Wal(format!("读取 WAL [{start}, {last}) 失败：{err}")))?;

        let wal_file = paths::wal_path(&self.data_dir, db_id);
        let applied = replay_wal_segments(&wal_file, &segments).await?;
        tracing::info!(
            db_id = %db_id,
            segments = segments.len(),
            from = start,
            applied_lsn = applied,
            path = %wal_file.display(),
            "Remote WAL 已回放到本地"
        );
        Ok(applied)
    }
}

/// 按 `file_offset` / `reset_wal` 把 WAL 段写入本地 WAL 文件，返回 applied LSN。
///
/// 规则（与 proto `WalChunk` / `AppendRequest` 一一对应）：
/// - `reset_wal = true`：先截断文件（新一代 WAL 从头写），再按 `file_offset` 写入；
/// - 否则：按 `file_offset` 定位写入；
/// - applied LSN = 段 `start_lsn` + 字节数（LSN 按字节推进，与引擎无关）。
///
/// 结尾做一次 `fsync`：回放结果必须在拉起 DB Process 之前真正落盘，否则进程起来后
/// 读到的可能是页缓存里的半截数据（崩溃后更糟）。
pub async fn replay_wal_segments(wal_file: &Path, segments: &[WalSegment]) -> Result<u64> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    if let Some(parent) = wal_file.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| WorkerError::io(format!("创建 {}", parent.display()), err))?;
    }

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(wal_file)
        .await
        .map_err(|err| WorkerError::io(format!("打开 {}", wal_file.display()), err))?;
    let mut file = file;

    let mut applied_lsn = 0u64;
    let mut written = 0usize;
    for segment in segments {
        if segment.reset_wal {
            file.set_len(0)
                .await
                .map_err(|err| WorkerError::io(format!("截断 {}", wal_file.display()), err))?;
        }
        file.seek(std::io::SeekFrom::Start(segment.file_offset))
            .await
            .map_err(|err| WorkerError::io(format!("定位 {}", wal_file.display()), err))?;
        file.write_all(&segment.data)
            .await
            .map_err(|err| WorkerError::io(format!("写入 {}", wal_file.display()), err))?;
        written = written.saturating_add(segment.data.len());
        applied_lsn = segment
            .start_lsn
            .get()
            .saturating_add(segment.data.len() as u64);
    }

    file.flush()
        .await
        .map_err(|err| WorkerError::io(format!("flush {}", wal_file.display()), err))?;
    file.sync_all()
        .await
        .map_err(|err| WorkerError::io(format!("fsync {}", wal_file.display()), err))?;

    tracing::debug!(
        path = %wal_file.display(),
        segments = segments.len(),
        bytes = written,
        applied_lsn,
        "WAL 回放完成"
    );
    Ok(applied_lsn)
}

/// 读取工作集元数据。
pub async fn read_work_set_meta(data_dir: &Path, db_id: &str) -> Option<WorkSetMeta> {
    let path = paths::work_set_meta_path(data_dir, db_id);
    let bytes = tokio::fs::read(&path).await.ok()?;
    match serde_json::from_slice::<WorkSetMeta>(&bytes) {
        Ok(meta) => Some(meta),
        Err(err) => {
            // 元数据损坏按「没有元数据」处理：结果是重新恢复（安全侧）
            tracing::warn!(path = %path.display(), error = %err, "工作集元数据损坏，将重新恢复");
            None
        }
    }
}

/// 写入工作集元数据（原子替换：先写临时文件再 rename）。
pub async fn write_work_set_meta(data_dir: &Path, db_id: &str, meta: &WorkSetMeta) -> Result<()> {
    let path = paths::work_set_meta_path(data_dir, db_id);
    let bytes = serde_json::to_vec_pretty(meta)
        .map_err(|err| WorkerError::Internal(format!("工作集元数据序列化失败：{err}")))?;
    write_atomic(&path, &bytes).await
}

/// 读取 snapshot ledger（缺失或损坏时返回空表）。
pub async fn read_ledger(data_dir: &Path, db_id: &str) -> SnapshotLedger {
    let path = paths::snapshot_ledger_path(data_dir, db_id);
    match tokio::fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|err| {
            tracing::warn!(path = %path.display(), error = %err, "snapshot ledger 损坏，按空表处理");
            SnapshotLedger::default()
        }),
        Err(_) => SnapshotLedger::default(),
    }
}

/// 写入 snapshot ledger。
pub async fn write_ledger(data_dir: &Path, db_id: &str, ledger: &SnapshotLedger) -> Result<()> {
    let path = paths::snapshot_ledger_path(data_dir, db_id);
    let bytes = serde_json::to_vec_pretty(ledger)
        .map_err(|err| WorkerError::Internal(format!("snapshot ledger 序列化失败：{err}")))?;
    write_atomic(&path, &bytes).await
}

/// 原子写文件（临时文件 + rename）：避免崩溃留下半截 JSON 让下次启动恢复失败。
async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| WorkerError::io(format!("创建 {}", parent.display()), err))?;
    }
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, bytes)
        .await
        .map_err(|err| WorkerError::io(format!("写入 {}", tmp.display()), err))?;
    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|err| WorkerError::io(format!("替换 {}", path.display()), err))
}

/// 冻结的本地 WAL 前缀（快照上传的输入之一）。
///
/// 生命周期固定：`freeze_wal_prefix` 落盘 -> 上传 -> [`FrozenWal::cleanup`]。
/// **成败都要 cleanup**，否则临时文件会留在工作集目录里直到下一次快照把它覆盖。
#[derive(Debug)]
pub struct FrozenWal {
    path: PathBuf,
}

impl FrozenWal {
    /// 临时文件路径（交给 `snapshot::upload_snapshot`）。
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 删除临时文件（幂等：文件已不在也算成功）。
    pub async fn cleanup(&self) -> Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(WorkerError::io(
                format!("清理快照临时文件 {}", self.path.display()),
                err,
            )),
        }
    }
}

/// 物化快照点上的本地 WAL 前缀：把 `db-wal` 的前 `base_lsn` 字节复制到临时文件。
///
/// ## 为什么必须裁剪，而不是直接上传 `db-wal`
///
/// `base_lsn` 是 DB Process 给出的**已 quorum durable 的末端**（架构 §11.4/§11.2）。
/// 本地 WAL 的写入总是先于远端确认落盘，因此文件尾部可能还躺着「已写本地、未拿到
/// 远端确认」的字节：把它们传上去，快照里就会包含从未对客户端承诺过的提交，
/// 恢复出来的库比 Remote WAL 还多一截 —— 那是最难查的一类分叉。
///
/// ## 为什么不整文件复制
///
/// 快照**不阻塞写入**：复制期间引擎仍可能继续向文件尾部追加。只读到 `base_lsn` 为止
/// （`AsyncReadExt::take`），快照点之后的字节一律不读，因此落盘的临时文件恰好是
/// `[0, base_lsn)` 这一段确定性字节流。
///
/// ## 平台不变量：本地 WAL 的文件偏移 == LSN
///
/// `base_lsn` 同时是「文件偏移」：`host::restore_from_remote` 把回放得到的 LSN 直接当
/// 文件偏移播种给 durable IO（`WalStreamSeed { durable_lsn, file_offset }` 两者同值），
/// 因此 `[0, base_lsn)` 就是那一份 durable 字节流。这里把它**显式校验**：本地文件短于
/// `base_lsn` 说明字节流与 durable 记账已经对不上，宁可失败也不能上传对不上的快照。
pub async fn freeze_wal_prefix(
    data_dir: &Path,
    db_id: &str,
    snapshot_id: &str,
    base_lsn: u64,
) -> Result<FrozenWal> {
    let source = paths::wal_path(data_dir, db_id);
    let dest = paths::frozen_wal_path(data_dir, db_id, snapshot_id);
    let available = match tokio::fs::metadata(&source).await {
        Ok(meta) => meta.len(),
        // 建库后没有任何提交：WAL 还不存在，与空文件等价（此时 base_lsn 必为 0）。
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => {
            return Err(WorkerError::io(
                format!("读取本地 WAL 元数据 {}", source.display()),
                err,
            ))
        }
    };
    if available < base_lsn {
        return Err(WorkerError::Storage(format!(
            "本地 WAL（{}）只有 {available} 字节，短于快照基线 base_lsn={base_lsn}；\
             本地字节流与 durable 记账不一致，拒绝上传不自洽的快照",
            source.display()
        )));
    }

    use tokio::io::AsyncReadExt;

    // 先开源文件再建目标文件：源文件打不开（例如已被删/权限不足）时不会留下垃圾文件。
    // base_lsn = 0 时没有任何 durable 字节可冻结（新建库 / 首次提交尚未确认），
    // 此时源文件甚至可能还不存在，直接产出一个空 WAL 入口即可。
    let mut src = if base_lsn > 0 {
        Some(
            tokio::fs::File::open(&source)
                .await
                .map_err(|err| WorkerError::io(format!("打开 {}", source.display()), err))?,
        )
    } else {
        None
    };
    let mut dst = tokio::fs::File::create(&dest)
        .await
        .map_err(|err| WorkerError::io(format!("创建 {}", dest.display()), err))?;
    let copied = match src.as_mut() {
        Some(src) => tokio::io::copy(&mut src.take(base_lsn), &mut dst)
            .await
            .map_err(|err| WorkerError::io(format!("复制 {} 的前缀", source.display()), err))?,
        None => 0,
    };
    if copied != base_lsn {
        // 复制期间文件被截断（WAL 换代属于异常路径）：少读一个字节都不能当成功。
        let _ = tokio::fs::remove_file(&dest).await;
        return Err(WorkerError::Storage(format!(
            "复制本地 WAL 前缀时只读到 {copied} 字节（期望 {base_lsn}），文件可能已被截断"
        )));
    }
    // 落盘之后才允许上传：否则上传读到的可能是页缓存里的半截数据。
    if let Err(err) = dst.sync_all().await {
        let _ = tokio::fs::remove_file(&dest).await;
        return Err(WorkerError::io(format!("fsync {}", dest.display()), err));
    }
    drop(dst);

    tracing::debug!(
        db_id = %db_id,
        snapshot_id = %snapshot_id,
        base_lsn,
        source_bytes = available,
        pending_bytes = available.saturating_sub(base_lsn),
        path = %dest.display(),
        "已冻结本地 WAL 前缀（快照点之后的字节不进快照）"
    );
    Ok(FrozenWal { path: dest })
}

/// 收集用于上传快照的文件清单：`(相对路径, 绝对路径)`。
///
/// 规则：
/// - 相对路径以 DB 工作集根目录为基准（恢复时按同样结构落盘）；
/// - **排除**引擎派生的本地文件（`db-wal` / `db-shm`）：本地 WAL 是活的、还在增长的
///   文件，直接上传会带上快照点之后的字节，因此由 [`freeze_wal_prefix`] 单独物化
///   `db-wal` 的冻结前缀后加入清单；
/// - **排除** Worker 私有元数据（`.worker-*.json` 与快照临时文件）。
pub async fn collect_snapshot_files(db_dir: &Path) -> Result<Vec<(String, PathBuf)>> {
    let root = db_dir.to_path_buf();
    let files = tokio::task::spawn_blocking(move || collect_files_blocking(&root))
        .await
        .map_err(|err| WorkerError::Internal(format!("收集快照文件任务失败：{err}")))??;
    Ok(files)
}

fn collect_files_blocking(root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|err| WorkerError::io(format!("遍历 {}", dir.display()), err))?;
        for entry in entries {
            let entry =
                entry.map_err(|err| WorkerError::io(format!("遍历 {}", dir.display()), err))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|err| WorkerError::io(format!("读取 {}", path.display()), err))?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if paths::is_worker_private_file(&path) || paths::is_engine_derived_file(&path) {
                continue;
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|_| WorkerError::Internal(format!("{} 不在工作集目录内", path.display())))?
                .to_string_lossy()
                .to_string();
            out.push((relative, path));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// 快照 manifest 与本地工作集目录是否一致（诊断用）。
#[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
pub fn manifest_matches_dir(manifest: &SnapshotManifest, db_dir: &Path) -> bool {
    manifest
        .files
        .iter()
        .all(|entry| db_dir.join(&entry.relative_path).exists())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn segment(start_lsn: u64, file_offset: u64, reset: bool, data: &[u8]) -> WalSegment {
        WalSegment {
            start_lsn: Lsn::new(start_lsn),
            file_offset,
            reset_wal: reset,
            data: Bytes::copy_from_slice(data),
        }
    }

    #[tokio::test]
    async fn replay_writes_at_file_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("wal/replay.wal");
        let segments = vec![
            segment(0, 0, true, b"AAAA"),
            segment(4, 4, false, b"BBBB"),
            segment(8, 8, false, b"CC"),
        ];
        let applied = replay_wal_segments(&wal, &segments).await.unwrap();
        assert_eq!(applied, 10);
        assert_eq!(std::fs::read(&wal).unwrap(), b"AAAABBBBCC");
    }

    #[tokio::test]
    async fn replay_truncates_on_reset_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("wal/replay.wal");

        // 先写一代 WAL（含尾部垃圾），再回放新一代
        replay_wal_segments(&wal, &[segment(0, 0, true, b"OLDOLDOLD")])
            .await
            .unwrap();
        let applied = replay_wal_segments(&wal, &[segment(100, 0, true, b"NEW")])
            .await
            .unwrap();
        assert_eq!(applied, 103);
        assert_eq!(std::fs::read(&wal).unwrap(), b"NEW");

        // 非 reset 的段按偏移覆盖，不会截断
        let applied = replay_wal_segments(&wal, &[segment(103, 3, false, b"ER")])
            .await
            .unwrap();
        assert_eq!(applied, 105);
        assert_eq!(std::fs::read(&wal).unwrap(), b"NEWER");
    }

    #[tokio::test]
    async fn replay_handles_empty_segments() {
        let dir = tempfile::tempdir().unwrap();
        let wal = dir.path().join("wal/replay.wal");
        let applied = replay_wal_segments(&wal, &[]).await.unwrap();
        assert_eq!(applied, 0);
        assert!(wal.exists(), "即使没有段也应创建 WAL 文件");
    }

    #[tokio::test]
    async fn work_set_meta_round_trip_and_epoch_gate() {
        let dir = tempfile::tempdir().unwrap();
        let preparer = WorkSetPreparer::new(
            dir.path().to_path_buf(),
            None,
            None,
            "0.1.0",
            domain::WorkerId::new("worker-test"),
        );
        preparer.prepare_dirs("db-1").await.unwrap();

        let meta = WorkSetMeta {
            database_id: "db-1".into(),
            owner_epoch: 7,
            snapshot_id: "snap-1".into(),
            base_lsn: 4096,
            applied_lsn: 8192,
            updated_at_unix_ms: domain::time::now_unix_ms(),
            engine_version: "0.1.0".into(),
        };
        write_work_set_meta(dir.path(), "db-1", &meta)
            .await
            .unwrap();
        let read = read_work_set_meta(dir.path(), "db-1").await.unwrap();
        assert_eq!(read, meta);

        // epoch 匹配 -> 可复用
        let reused = preparer
            .prepare(&PrepareWork {
                database_id: "db-1".into(),
                owner_epoch: 7,
                snapshot: None,
                allow_local_reuse: true,
                allow_wal_only: false,
            })
            .await
            .unwrap();
        assert!(reused.reused_local);
        assert_eq!(reused.applied_lsn, 8192);
        assert_eq!(reused.base_lsn, 4096);

        // epoch 不匹配 -> 必须重新恢复（这里没有快照/WAL，结果是最小前提下的空恢复）
        let reopened = preparer
            .prepare(&PrepareWork {
                database_id: "db-1".into(),
                owner_epoch: 8,
                snapshot: None,
                allow_local_reuse: true,
                allow_wal_only: true,
            })
            .await
            .unwrap();
        assert!(!reopened.reused_local);
        assert_eq!(reopened.base_lsn, 0);
        assert_eq!(
            read_work_set_meta(dir.path(), "db-1")
                .await
                .unwrap()
                .owner_epoch,
            8
        );
    }

    #[tokio::test]
    async fn prepare_without_dependencies_is_noop_restore() {
        let dir = tempfile::tempdir().unwrap();
        let preparer = WorkSetPreparer::new(
            dir.path().to_path_buf(),
            None,
            None,
            "0.1.0",
            domain::WorkerId::new("worker-test"),
        );
        let prepared = preparer
            .prepare(&PrepareWork {
                database_id: "db-new".into(),
                owner_epoch: 1,
                snapshot: None,
                allow_local_reuse: true,
                allow_wal_only: false,
            })
            .await
            .unwrap();

        assert!(!prepared.reused_local);
        assert!(!prepared.downloaded_snapshot);
        assert_eq!(prepared.applied_lsn, 0);
        // 工作集根目录已就绪；主库/WAL 是引擎要创建的**文件**，这里绝不能预先建目录
        assert!(paths::db_dir(dir.path(), "db-new").is_dir());
        assert!(!paths::db_path(dir.path(), "db-new").exists());
        assert!(paths::work_set_meta_path(dir.path(), "db-new").exists());
    }

    #[tokio::test]
    async fn prepare_dirs_clears_stale_db_directory() {
        let dir = tempfile::tempdir().unwrap();
        let preparer = WorkSetPreparer::new(
            dir.path().to_path_buf(),
            None,
            None,
            "0.1.0",
            domain::WorkerId::new("worker-test"),
        );

        // 历史版本把主库路径当目录创建：空目录必须被清掉，否则引擎永远 EISDIR
        let stale = paths::db_path(dir.path(), "db-1");
        std::fs::create_dir_all(&stale).unwrap();
        preparer.prepare_dirs("db-1").await.unwrap();
        assert!(!stale.exists(), "残留目录应被清理");

        // 非空目录不删（宁可让引擎报错，也不静默丢数据）
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("keep"), b"data").unwrap();
        preparer.prepare_dirs("db-1").await.unwrap();
        assert!(stale.join("keep").exists());
    }

    #[tokio::test]
    async fn explicit_snapshot_requires_object_store() {
        let dir = tempfile::tempdir().unwrap();
        let preparer = WorkSetPreparer::new(
            dir.path().to_path_buf(),
            None,
            None,
            "0.1.0",
            domain::WorkerId::new("worker-test"),
        );
        let err = preparer
            .prepare(&PrepareWork {
                database_id: "db-1".into(),
                owner_epoch: 1,
                snapshot: Some(SnapshotSource {
                    snapshot_id: "snap-1".into(),
                    base_lsn: 10,
                }),
                allow_local_reuse: false,
                allow_wal_only: false,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), domain::error::ErrorCode::StorageUnavailable);
    }

    #[tokio::test]
    async fn ledger_keeps_latest_and_dedupes() {
        let dir = tempfile::tempdir().unwrap();
        let preparer = WorkSetPreparer::new(
            dir.path().to_path_buf(),
            None,
            None,
            "0.1.0",
            domain::WorkerId::new("worker-test"),
        );
        preparer.prepare_dirs("db-1").await.unwrap();

        for index in 0..(SnapshotLedger::MAX_ENTRIES + 3) {
            preparer
                .record_snapshot(
                    "db-1",
                    LedgerEntry {
                        snapshot_id: format!("snap-{index}"),
                        base_lsn: index as u64,
                        checksum: "x".into(),
                        size_bytes: 1,
                        created_at_unix_ms: index as i64,
                        owner_epoch: 1,
                    },
                )
                .await
                .unwrap();
        }
        let ledger = preparer.ledger("db-1").await;
        assert_eq!(ledger.entries.len(), SnapshotLedger::MAX_ENTRIES);
        assert_eq!(
            ledger.latest().unwrap().snapshot_id,
            format!("snap-{}", SnapshotLedger::MAX_ENTRIES + 2)
        );

        // 重复记录同一 snapshot_id 不会产生重复项
        preparer
            .record_snapshot(
                "db-1",
                LedgerEntry {
                    snapshot_id: "snap-3".into(),
                    base_lsn: 3,
                    checksum: "y".into(),
                    size_bytes: 2,
                    created_at_unix_ms: 3,
                    owner_epoch: 2,
                },
            )
            .await
            .unwrap();
        let ledger = preparer.ledger("db-1").await;
        assert_eq!(
            ledger
                .entries
                .iter()
                .filter(|e| e.snapshot_id == "snap-3")
                .count(),
            1
        );
        assert_eq!(ledger.entries[0].snapshot_id, "snap-3");
    }

    #[tokio::test]
    async fn collect_snapshot_files_skips_local_wal_and_private_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("db-1");
        std::fs::create_dir_all(db_dir.join("extra")).unwrap();
        std::fs::write(db_dir.join("db"), b"main").unwrap();
        std::fs::write(db_dir.join("db-wal"), b"wal").unwrap();
        std::fs::write(db_dir.join("db-shm"), b"shm").unwrap();
        std::fs::write(db_dir.join("extra/note"), b"note").unwrap();
        std::fs::write(db_dir.join(".worker-work-set.json"), b"{}").unwrap();

        let files = collect_snapshot_files(&db_dir).await.unwrap();
        let names: Vec<&str> = files.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["db", "extra/note"]);
    }

    /// 冻结的前缀必须**只**含 `[0, base_lsn)`：快照点之后追加的字节不能进快照。
    #[tokio::test]
    async fn freeze_wal_prefix_drops_bytes_after_snapshot_point() {
        let dir = tempfile::tempdir().unwrap();
        let wal = paths::wal_path(dir.path(), "db-1");
        std::fs::create_dir_all(wal.parent().unwrap()).unwrap();
        std::fs::write(&wal, b"durable-part").unwrap();
        std::fs::write(&wal, b"durable-part+uncommitted-tail").unwrap();

        let frozen = freeze_wal_prefix(dir.path(), "db-1", "snap-1", 12)
            .await
            .unwrap();
        assert_eq!(std::fs::read(frozen.path()).unwrap(), b"durable-part");
        // 活的 WAL 里仍然留着尾部字节（裁剪只发生在上传用的副本上）
        assert!(
            std::fs::read(&wal).unwrap().len() > 12,
            "冻结不得改动源文件"
        );

        frozen.cleanup().await.unwrap();
        assert!(!paths::frozen_wal_path(dir.path(), "db-1", "snap-1").exists());
        // 幂等：重复清理不报错
        frozen.cleanup().await.unwrap();
    }

    /// 本地 WAL 短于 base_lsn：必须显式失败，不能上传一份对不上的快照。
    #[tokio::test]
    async fn freeze_wal_prefix_rejects_short_local_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal = paths::wal_path(dir.path(), "db-1");
        std::fs::create_dir_all(wal.parent().unwrap()).unwrap();
        std::fs::write(&wal, b"short").unwrap();

        let err = freeze_wal_prefix(dir.path(), "db-1", "snap-1", 64)
            .await
            .unwrap_err();
        assert_eq!(err.code(), domain::error::ErrorCode::StorageUnavailable);
        assert!(err.to_string().contains("base_lsn=64"), "真因要保留：{err}");
        assert!(!paths::frozen_wal_path(dir.path(), "db-1", "snap-1").exists());
    }

    /// 全新库（还没有提交、WAL 文件都不存在）也要能冻结出一个空 WAL。
    #[tokio::test]
    async fn freeze_wal_prefix_tolerates_missing_wal() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(paths::db_dir(dir.path(), "db-1")).unwrap();
        let frozen = freeze_wal_prefix(dir.path(), "db-1", "snap-1", 0)
            .await
            .unwrap();
        assert_eq!(std::fs::metadata(frozen.path()).unwrap().len(), 0);
        frozen.cleanup().await.unwrap();
    }

    #[test]
    fn corrupted_metadata_is_treated_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = paths::work_set_meta_path(dir.path(), "db-1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{not json").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(runtime
            .block_on(read_work_set_meta(dir.path(), "db-1"))
            .is_none());
    }
}
