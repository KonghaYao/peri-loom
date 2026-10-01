//! `PlatformDurableIO` —— **commit durability 的强制点**（架构 §11.1 / §17.3）。
//!
//! ```text
//! Turso WAL write
//!      │
//!      ▼
//! PlatformDurableIO.pwrite(<db>-wal)
//!      │
//!      ├── 本地 NVMe WAL write（inner IO）
//!      │
//!      └── 识别 commit frame ──► Remote WAL Append ──► quorum durable
//!                                                        │
//!                                                        ▼
//!                                            才允许 parent Completion 完成
//! ```
//!
//! 本模块是「Commit Success ⇒ Remote WAL Durable」的**唯一实现位置**（架构 §15.1）：
//!
//! 1. WAL 文件的 `pwrite` 先在本地落盘，同时把字节喂给
//!    [`crate::wal_frames::WalFrameParser`] 识别 commit frame；
//! 2. 只有完整提交前缀（WAL header + 事务内全部帧）才被送到 Remote WAL，未提交字节永远
//!    留在解析器里 —— Remote WAL 不会出现半个事务；
//! 3. **Remote append 得到 quorum durable 之前，engine 拿到的 `Completion` 不算完成**；
//!    append 失败（WAL 不可用、fencing 拒绝、超时、幂等响应不覆盖本批次）一律
//!    `abort` parent completion，并把 WAL 流置为 fenced：此后任何 WAL 写都直接失败，
//!    绝不静默成功。
//!
//! ## 线程模型（重要，db-runtime 必须满足）
//!
//! Remote append 是异步的：`pwrite` 在 tokio runtime 上启一条 **FIFO append 流水线**，
//! 由它按 WAL 字节顺序串行提交（顺序即 LSN 顺序，绝不允许并发乱序 append）。
//! 因此：
//! - [`DurableIoConfig::runtime`] 必须是**多线程** runtime 的 `Handle`；
//! - 引擎的阻塞调用（`EngineAdapter` / `EngineConnection`）**不得占用该 runtime 的
//!   worker 线程**（请放到专用线程或 `spawn_blocking`）。否则本模块的等待会与
//!   append 任务互相饿死；
//! - 本模块从不在 engine 线程上 `block_on`（那会在 runtime 上下文中 panic），而是用
//!   「提交任务 + 条件变量等待」的方式把结果交回 engine 线程。
//!
//! ## 进程重启后的本地 WAL 接续（**写路径在重启后能否继续的唯一保证**）
//!
//! 打开一个**已存在且非空**的本地 WAL 文件时（db-process 重启、被重新调度到同一
//! Worker），文件里躺着上一代进程写下的字节，而本进程的解析器游标从 0 开始：
//!
//! * 第一次 `pwrite` 会被判成「偏移不连续」→ fail-stop，写路径直接不可用；
//! * 即使对齐了游标，`durable_lsn` 若从 0 重算，append 会带着重复的 `start_lsn`
//!   覆盖已 durable 区间，被 WAL 服务端拒绝。
//!
//! 因此打开时执行一次**接续**（见 [`ResumeState`]）：读本地 WAL 头拿到 page size，
//! 把解析游标对齐到文件末尾；第一次 append 之前再从 Remote WAL 的
//! `GetWalStatus.last_lsn` 接续 durable LSN。二者缺一不可。
//!
//! ## 与 Turso WAL 内部类型的关系
//!
//! 本模块只处理字节与帧格式，不引用 Turso 的 WAL 类型（`storage::wal::*`）：即使上游
//! WAL 实现变化，只要磁盘格式不变，这里都不需要改。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use domain::error::PlatformError;
use domain::ids::DatabaseId;
use domain::wal::Lsn;
use parking_lot::{Condvar, Mutex};
use tokio::sync::mpsc;
use turso_core::io::clock::{Clock, MonotonicInstant, WallClockInstant};
use turso_core::io::{
    Buffer, Completion, File, FileSyncType, OpenFlags, SharedWalLockKind, SharedWalMappedRegion,
};
use turso_core::{CompletionError, LimboError, Result as TursoResult, IO};
use wal_client::{AppendWalRequest, WalClient};

use crate::durable::frame::{parse_wal_header_page_size, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
use crate::durable::remote::{describe_failure, RemoteWalAppender, WalClientAppender};
use crate::error::DURABLE_FAILURE_MARKER;
use crate::wal_frames::{WalFrameParser, WalStreamError, WalWriteCheckpoint};

/// WAL 文件后缀（Turso/SQLite 约定：数据库 `<path>` 对应的日志是 `<path>-wal`）。
pub const WAL_SUFFIX: &str = "-wal";

/// 等待 append 结果的条件变量宽限：流水线自身已有 `append_timeout` 超时，
/// 这里再加一点余量，让「任务已完成但结果尚未回传」不会先被判失败。
const APPEND_WAIT_GRACE: Duration = Duration::from_millis(250);

/// 单次远程 append 的默认等待上限（生产配置的缺省值）。
///
/// 取 3s 的理由：正常情况下 quorum 复制是毫秒级；一旦超过 3s 说明 WAL 组已经不可用，
/// 继续等待只会把引擎线程与 append 流水线一起拖住——durability 契约要求尽快显式失败。
pub const DEFAULT_APPEND_TIMEOUT: Duration = Duration::from_secs(3);

/// [`PlatformDurableIO`] 的构造参数。
#[derive(Clone)]
pub struct DurableIoConfig {
    /// 目标数据库（fencing 与幂等键的一部分）。
    pub database_id: DatabaseId,
    /// 写 Owner 的 epoch；Remote WAL 用它拒绝旧 Owner（架构 §11.3）。
    pub owner_epoch: u64,
    /// Remote WAL 客户端（生产路径）。
    pub wal_client: Arc<WalClient>,
    /// 执行 append 的 tokio runtime（必须多线程，见模块文档）。
    pub runtime: tokio::runtime::Handle,
    /// 单次 append 的等待上限；超时按「未 durable」处理，绝不放过。
    pub append_timeout: Duration,
}

impl DurableIoConfig {
    /// 生产构造：一次给齐 fencing 身份、append 目标与超时预算。
    #[must_use]
    pub fn new(
        database_id: DatabaseId,
        owner_epoch: u64,
        wal_client: Arc<WalClient>,
        runtime: tokio::runtime::Handle,
        append_timeout: Duration,
    ) -> Self {
        Self {
            database_id,
            owner_epoch,
            wal_client,
            runtime,
            append_timeout,
        }
    }
}

/// 恢复播种参数（配合 [`PlatformDurableIO::seed_wal_stream`]）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalStreamSeed {
    /// 已经 durable 的字节数（= 恢复后本地 WAL 的长度）。
    pub durable_lsn: u64,
    /// 恢复后本地 WAL 的写入末端偏移（帧边界）。
    pub file_offset: u64,
    /// 从本地 WAL header 读到的 page size。
    pub page_size: u32,
    /// 下一个 commit 批次是否要重新开启一代 WAL。
    pub reset_wal: bool,
}

/// 实现 [`turso_core::IO`] 的 WAL durability 包装。
pub struct PlatformDurableIO {
    inner: Arc<dyn IO>,
    ctx: Arc<DurableContext>,
}

impl PlatformDurableIO {
    /// 构造生产实例（append 走 [`WalClient`]）。
    #[must_use]
    pub fn new(inner: Arc<dyn IO>, config: DurableIoConfig) -> Self {
        let appender = Arc::new(WalClientAppender::new(config.wal_client.clone()));
        Self::new_with_appender(inner, config, appender)
    }

    /// 构造实例并注入自定义 append 实现（测试与后续非 gRPC 传输使用）。
    #[must_use]
    pub fn new_with_appender(
        inner: Arc<dyn IO>,
        config: DurableIoConfig,
        appender: Arc<dyn RemoteWalAppender>,
    ) -> Self {
        Self {
            inner,
            ctx: Arc::new(DurableContext::new(config, appender)),
        }
    }

    /// 已经确认 quorum durable 的末端 LSN（exclusive）。
    #[must_use]
    pub fn durable_lsn(&self) -> u64 {
        self.ctx.state.durable_lsn()
    }

    /// 最近一次 durability 失败的原因（成功后不清空：失败即 fail-stop）。
    #[must_use]
    pub fn last_error(&self) -> Option<String> {
        self.ctx.state.last_error()
    }

    /// 成功 append 的次数。
    #[must_use]
    pub fn succeeded_appends(&self) -> u64 {
        self.ctx.state.succeeded_appends()
    }

    /// 失败 append 的次数。
    #[must_use]
    pub fn failed_appends(&self) -> u64 {
        self.ctx.state.failed_appends()
    }

    /// WAL 流是否已被 fence（出现过 durability 失败，或所有权被拒绝）。
    ///
    /// 一旦为 true，后续所有 WAL 写入都直接失败：允许继续写会让「本地 WAL 已推进、
    /// Remote WAL 缺失」的分叉永久固化。
    #[must_use]
    pub fn fenced(&self) -> bool {
        self.ctx.state.fenced()
    }

    /// 目标数据库 id。
    #[must_use]
    pub fn database_id(&self) -> DatabaseId {
        self.ctx.database_id
    }

    /// 写 Owner epoch。
    #[must_use]
    pub fn owner_epoch(&self) -> u64 {
        self.ctx.owner_epoch
    }

    /// append 超时。
    #[must_use]
    pub fn append_timeout(&self) -> Duration {
        self.ctx.append_timeout
    }

    /// 恢复播种：把 WAL 流的游标与 durable LSN 对齐到「已经回放完成的边界」。
    ///
    /// 冷启动顺序必须是：`prepare_local_wal_for_restore` →
    /// `replay_wal_segments` → 读 page size → 本函数 → 打开引擎。
    /// 缺少这一步时，引擎恢复后第一次写入会因为偏移不连续被判定为协议违规（fail-stop），
    /// 这是刻意的：绝不允许在未知位置续写。
    ///
    /// # Errors
    /// page size 非法（必须来自本地 WAL header，见
    /// [`crate::recovery::read_wal_page_size`]）。
    pub fn seed_wal_stream(
        &self,
        wal_path: &str,
        seed: WalStreamSeed,
    ) -> domain::error::Result<()> {
        if !crate::wal_frames::is_valid_page_size(seed.page_size) {
            return Err(PlatformError::invalid_argument(format!(
                "seed_wal_stream: page_size={} 非法（必须是 512..=65536 的 2 的幂）",
                seed.page_size
            )));
        }
        if seed.file_offset != 0 && seed.file_offset < crate::wal_frames::WAL_HEADER_SIZE as u64 {
            return Err(PlatformError::invalid_argument(format!(
                "seed_wal_stream: file_offset={} 落在 WAL header 内，不是帧边界",
                seed.file_offset
            )));
        }
        let parser = self.ctx.parser_for(wal_path);
        let mut guard = parser.lock();
        guard.seed(seed.file_offset, seed.page_size, seed.reset_wal);
        // 播种必须整份生效：游标与未提交起点都落在恢复末端、page size 就是刚校验过的那个。
        // 任何一条对不上都说明 seed 参数与本地 WAL 实际内容不符 —— 放行等于让引擎在未知
        // 位置续写，宁可在这里以配置错误失败。
        let (cursor, pending_start, page_size) =
            (guard.cursor(), guard.pending_start(), guard.page_size());
        drop(guard);
        if cursor != seed.file_offset
            || pending_start != seed.file_offset
            || page_size != Some(seed.page_size)
        {
            return Err(PlatformError::invalid_argument(format!(
                "seed_wal_stream: 播种结果与请求不一致（cursor={cursor} pending_start={pending_start} \
                 page_size={page_size:?}，期望 file_offset={} page_size={}）",
                seed.file_offset, seed.page_size
            )));
        }
        // 恢复路径给出的位置来自远端回放，比「文件末尾」更权威：标记后打开文件不再接续。
        self.ctx.mark_seeded(wal_path);
        self.ctx.state.seed_durable_lsn(seed.durable_lsn);
        tracing::info!(
            db_id = %self.ctx.database_id,
            wal_path,
            durable_lsn = seed.durable_lsn,
            file_offset = seed.file_offset,
            reset_wal = seed.reset_wal,
            "WAL 流已播种（恢复后继续写入）"
        );
        Ok(())
    }
}

impl Clock for PlatformDurableIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for PlatformDurableIO {
    fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> TursoResult<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        if path.ends_with(WAL_SUFFIX) {
            // 只有 WAL 文件需要 durability 拦截；数据库主文件与临时文件直接透传。
            let wal = Arc::new(WalFile {
                inner: file,
                ctx: Arc::clone(&self.ctx),
                stream: self.ctx.stream_for(path),
                path: path.to_string(),
            });
            // 已存在的 WAL（重启后重开）必须先把游标接续到文件末尾，否则第一次写入
            // 会被判成偏移不连续而 fail-stop（见模块文档「进程重启后的本地 WAL 接续」）。
            wal.start_resume();
            return Ok(wal);
        }
        Ok(file)
    }

    fn remove_file(&self, path: &str) -> TursoResult<()> {
        self.inner.remove_file(path)
    }

    fn supports_shared_wal_coordination(&self) -> bool {
        // 共享 WAL 协调（多进程）由底层后端决定；平台不使用该模式。
        self.inner.supports_shared_wal_coordination()
    }

    /// `step()` 是引擎唯一的「推进 IO」入口。
    ///
    /// 我们的完成体由 append 流水线在 runtime 线程上完成，因此这里必须等待在途 append，
    /// 否则引擎会空转；等待失败时返回 `Err`（durability 违约不允许被当作「本次 IO 无事发生」）。
    fn step(&self) -> TursoResult<()> {
        self.ctx.wait_for_appends()
    }

    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    fn get_memory_io(&self) -> Arc<turso_core::MemoryIO> {
        // 临时/内存表用独立 MemoryIO：它们不参与 WAL durability，也不该被包进 WAL 拦截。
        Arc::new(turso_core::MemoryIO::new())
    }

    fn yield_now(&self) {
        self.inner.yield_now();
    }

    fn sleep(&self, duration: Duration) {
        self.inner.sleep(duration);
    }

    fn file_id(&self, path: &str) -> TursoResult<turso_core::io::FileId> {
        self.inner.file_id(path)
    }
}

/// 共享的 durability 上下文（`PlatformDurableIO` 与它产出的 `WalFile` 共用一份）。
struct DurableContext {
    database_id: DatabaseId,
    owner_epoch: u64,
    append_timeout: Duration,
    runtime: tokio::runtime::Handle,
    appender: Arc<dyn RemoteWalAppender>,
    state: DurableState,
    /// 每个 WAL 路径一份共享状态：同一路径被打开两次时必须共享游标，否则会重复 append。
    streams: Mutex<HashMap<String, Arc<WalStreamState>>>,
    /// FIFO append 流水线（懒启动）。
    pipeline: Mutex<Option<mpsc::UnboundedSender<AppendJob>>>,
    /// 幂等键前缀：每次构造一个随机值，避免进程重启后用同一个 `append_id` 命中
    /// 服务端幂等缓存（那会把「新字节」当成「已提交的旧字节」吞掉）。
    session: String,
}

impl DurableContext {
    fn new(config: DurableIoConfig, appender: Arc<dyn RemoteWalAppender>) -> Self {
        Self {
            database_id: config.database_id,
            owner_epoch: config.owner_epoch,
            append_timeout: config.append_timeout,
            runtime: config.runtime,
            appender,
            state: DurableState::default(),
            streams: Mutex::new(HashMap::new()),
            pipeline: Mutex::new(None),
            session: uuid::Uuid::new_v4().simple().to_string(),
        }
    }

    /// 取（或创建）一个 WAL 路径的共享状态。
    fn stream_for(&self, wal_path: &str) -> Arc<WalStreamState> {
        let mut streams = self.streams.lock();
        Arc::clone(
            streams
                .entry(wal_path.to_string())
                .or_insert_with(|| Arc::new(WalStreamState::new())),
        )
    }

    /// 解析器句柄（同一路径共享一份游标）。
    fn parser_for(&self, wal_path: &str) -> Arc<Mutex<WalFrameParser>> {
        Arc::clone(&self.stream_for(wal_path).parser)
    }

    /// 标记该 WAL 路径已经由恢复路径播种：打开文件时不再做接续。
    ///
    /// 播种（[`PlatformDurableIO::seed_wal_stream`]）给出的游标来自远端回放结果，
    /// 比「文件末尾」更权威，绝不能被接续覆盖。
    fn mark_seeded(&self, wal_path: &str) {
        self.stream_for(wal_path).resume.mark_done();
    }

    /// 解析一次本地 WAL 写入（同一把锁内完成，保证解析顺序 = 本地写顺序）。
    fn parse_write(
        &self,
        parser: &Mutex<WalFrameParser>,
        pos: u64,
        bytes: &[u8],
    ) -> Result<ParsedWrite, WalStreamError> {
        let mut guard = parser.lock();
        let checkpoint = guard.checkpoint();
        let batches = guard.on_write(pos, bytes)?;
        Ok(ParsedWrite {
            batches,
            checkpoint,
        })
    }

    /// WAL 字节流协议违规：本地 WAL 已不可信，立刻 fail-stop。
    fn on_stream_error(&self, ctx: &str, err: &WalStreamError) {
        let message = format!("本地 WAL 字节流违规（{ctx}）: {err}");
        tracing::error!(db_id = %self.database_id, wal_path = %ctx, error = %err, "本地 WAL 帧解析失败，WAL 流进入 fenced 状态");
        self.state.record_failure(&message);
    }

    /// 等待所有在途 append 落地；失败或超时返回 durability 错误。
    fn wait_for_appends(&self) -> TursoResult<()> {
        let budget = self.append_timeout + APPEND_WAIT_GRACE;
        if !self.state.wait_idle(budget) {
            let message = format!(
                "等待 remote WAL append 超过 {budget:?} 仍未完成（append 流水线可能已停摆）"
            );
            self.state.record_failure(&message);
            self.state.fail_in_flight(&message);
            return Err(durable_io_error(&message));
        }
        if let Some(reason) = self.state.fence_reason() {
            return Err(durable_io_error(&reason));
        }
        Ok(())
    }

    /// 把一批已提交前缀交给 append 流水线。
    fn enqueue(self: &Arc<Self>, job: AppendJob) -> Result<(), String> {
        let sender = self.pipeline()?;
        sender.send(job).map_err(|_| {
            let reason = "remote WAL append 流水线已停止（sender 无法投递）".to_string();
            self.state.record_failure(&reason);
            reason
        })
    }

    /// 懒启动 FIFO 流水线：所有 append 由单条任务按投递顺序串行执行，
    /// 顺序即 LSN 顺序（并发 append 会让 Remote WAL 的字节流错位）。
    fn pipeline(self: &Arc<Self>) -> Result<mpsc::UnboundedSender<AppendJob>, String> {
        let mut guard = self.pipeline.lock();
        if let Some(sender) = guard.as_ref() {
            return Ok(sender.clone());
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        let ctx = Arc::clone(self);
        // runtime 已关闭时 spawn 会 panic；这里显式转成可上报的失败，而不是把 panic
        // 传播到 WAL 写路径（那会让调用方无从判断写是否成功）。
        let spawned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime.spawn(async move {
                run_append_pipeline(ctx, receiver).await;
            });
        }));
        if spawned.is_err() {
            let reason = "tokio runtime 已关闭，无法启动 remote WAL append 流水线".to_string();
            self.state.record_failure(&reason);
            return Err(reason);
        }
        *guard = Some(sender.clone());
        Ok(sender)
    }

    /// 串行执行一次 append，并把结果落到 ticket 上。
    async fn dispatch_append(&self, job: AppendJob) {
        if let Some(reason) = self.state.fence_reason() {
            job.ticket
                .fail_durable(&format!("WAL 流此前已因 durability 失败被 fence：{reason}"));
            self.state.settle(&job.ticket);
            return;
        }

        // 本进程的第一次 append 之前，必须先把 durable LSN 接到 Remote WAL 的末端：
        // 重启后本地记账从 0 开始，直接 append 会带着重复的 start_lsn 覆盖已 durable
        // 区间（服务端拒绝），或者留下空洞（服务端接受，本地与远端永久分叉）。
        if !self.state.baseline_known() {
            if let Err(reason) = self.resume_baseline_lsn().await {
                self.state.record_failure(&reason);
                job.ticket.fail_durable(&reason);
                self.state.settle(&job.ticket);
                return;
            }
        }

        let start_lsn = self.state.durable_lsn();
        let len = job.data.len() as u64;
        let request = AppendWalRequest {
            database_id: self.database_id,
            owner_epoch: self.owner_epoch,
            start_lsn: Lsn::new(start_lsn),
            file_offset: job.file_offset,
            reset_wal: job.reset_wal,
            bytes: job.data,
            contains_commit_frame: true,
            append_id: self.next_append_id(),
        };

        let outcome =
            tokio::time::timeout(self.append_timeout, self.appender.append(request)).await;
        match outcome {
            Ok(Ok(outcome)) if outcome.durable_lsn.get() >= start_lsn + len => {
                self.state.record_success(outcome.durable_lsn.get());
                job.ticket.complete_one();
            }
            Ok(Ok(outcome)) => {
                // 服务端声称成功但 durable_lsn 没覆盖本批次：协议违约，按未 durable 处理。
                let reason = format!(
                    "remote WAL 返回的 durable_lsn={} 未覆盖本批次 [{start_lsn}, {})",
                    outcome.durable_lsn.get(),
                    start_lsn + len
                );
                self.state.record_failure(&reason);
                job.ticket.fail_durable(&reason);
            }
            Ok(Err(err)) => {
                // 统一走 `remote::describe_failure`：`last_error` 与日志只保留一份「错误码+消息」
                // 的格式，排障时不必在两个地方对格式。
                let reason = format!("remote WAL append 失败: {}", describe_failure(&err));
                self.state.record_failure(&reason);
                job.ticket.fail_durable(&reason);
            }
            Err(_) => {
                let reason = format!(
                    "remote WAL append 超过 append_timeout({:?}) 未返回",
                    self.append_timeout
                );
                self.state.record_failure(&reason);
                job.ticket.fail_durable(&reason);
            }
        }
        self.state.settle(&job.ticket);
    }

    fn next_append_id(&self) -> String {
        let seq = self.state.next_append_seq();
        format!(
            "{}-e{}-{}-{}",
            self.database_id, self.owner_epoch, self.session, seq
        )
    }

    /// 把 durable LSN 接续到 Remote WAL 的末端（`GetWalStatus.last_lsn`）。
    ///
    /// `Ok(None)`（实现不提供远端末端）时保持本地记账不变；查询失败或超时返回 `Err`，
    /// 由调用方按 durability 违约 fail-stop —— 用一个猜出来的 start_lsn 去 append，
    /// 比直接失败危险得多。
    async fn resume_baseline_lsn(&self) -> Result<(), String> {
        let queried = tokio::time::timeout(
            self.append_timeout,
            self.appender.last_lsn(&self.database_id),
        )
        .await;
        match queried {
            Ok(Ok(Some(last))) => {
                self.state.seed_durable_lsn(last.get());
                tracing::info!(
                    db_id = %self.database_id,
                    last_lsn = last.get(),
                    "durable LSN 已从 Remote WAL 末端接续"
                );
                Ok(())
            }
            Ok(Ok(None)) => {
                self.state.mark_baseline_known();
                Ok(())
            }
            Ok(Err(err)) => Err(format!(
                "读取 remote WAL 末端 LSN 失败: {}",
                describe_failure(&err)
            )),
            Err(_) => Err(format!(
                "读取 remote WAL 末端 LSN 超过 append_timeout({:?}) 未返回",
                self.append_timeout
            )),
        }
    }
}

/// FIFO append 流水线主体。
async fn run_append_pipeline(
    ctx: Arc<DurableContext>,
    mut receiver: mpsc::UnboundedReceiver<AppendJob>,
) {
    while let Some(job) = receiver.recv().await {
        ctx.dispatch_append(job).await;
    }
    tracing::debug!(db_id = %ctx.database_id, "remote WAL append 流水线结束");
}

/// 一次本地 WAL 写入的解析结果。
struct ParsedWrite {
    batches: Vec<crate::wal_frames::DurableBatch>,
    checkpoint: WalWriteCheckpoint,
}

/// 一次 append 任务（一个 commit 批次）。
struct AppendJob {
    ticket: Arc<WriteTicket>,
    file_offset: u64,
    reset_wal: bool,
    data: Bytes,
}

/// 一次本地 WAL 写入的完成回执：等所有 commit 批次都 durable 后才完成引擎的 `Completion`。
struct WriteTicket {
    parent: Completion,
    /// 本地实际写入字节数（成功时上报给引擎）。
    written: AtomicI32,
    /// 还有几个 commit 批次未得到 quorum durable 确认。
    remaining: AtomicUsize,
    /// 是否已经以失败结束（保证 parent 只被完成/终止一次）。
    failed: AtomicBool,
}

impl WriteTicket {
    fn new(parent: Completion, remaining: usize) -> Arc<Self> {
        Arc::new(Self {
            parent,
            written: AtomicI32::new(0),
            remaining: AtomicUsize::new(remaining),
            failed: AtomicBool::new(false),
        })
    }

    fn set_written(&self, written: i32) {
        self.written.store(written, Ordering::Release);
    }

    fn complete_one(&self) {
        let remaining = self.remaining.fetch_sub(1, Ordering::AcqRel);
        if remaining == 1 && !self.failed.load(Ordering::Acquire) {
            self.parent.complete(self.written.load(Ordering::Acquire));
        }
    }

    /// 本地写失败：原样上报（这不是 durability 违约）。
    fn fail_local(&self, err: CompletionError) {
        if !self.failed.swap(true, Ordering::AcqRel) {
            self.parent.error(err);
        }
    }

    /// durability 违约：绝不允许 parent 成功完成。
    fn fail_durable(&self, reason: &str) {
        if !self.failed.swap(true, Ordering::AcqRel) {
            tracing::error!(
                reason,
                "remote WAL 未 durable，终止本次 WAL 写入（Commit 不得返回成功）"
            );
            self.parent.error(CompletionError::IOError(
                std::io::ErrorKind::Other,
                DURABLE_FAILURE_MARKER,
            ));
        }
    }

    fn settled(&self) -> bool {
        self.failed.load(Ordering::Acquire) || self.remaining.load(Ordering::Acquire) == 0
    }
}

/// durability 状态（跨 `PlatformDurableIO` / `WalFile` / append 流水线共享）。
#[derive(Clone, Default)]
struct DurableState {
    inner: Arc<Mutex<DurableInner>>,
    progress: Arc<Condvar>,
}

#[derive(Default)]
struct DurableInner {
    durable_lsn: u64,
    /// 远端 durable 末端是否已知。进程重启后为 false：第一次 append 必须先从 Remote WAL
    /// 的 `GetWalStatus.last_lsn` 接续，而不是从 0 重算（见 [`ResumeState`]）。
    baseline_known: bool,
    last_error: Option<String>,
    fenced: bool,
    succeeded_appends: u64,
    failed_appends: u64,
    /// 尚未得到 durable 确认的写入（本地写完成 → remote durable 之间的窗口）。
    in_flight: Vec<Arc<WriteTicket>>,
    next_append_seq: u64,
}

impl DurableState {
    fn durable_lsn(&self) -> u64 {
        self.inner.lock().durable_lsn
    }

    fn last_error(&self) -> Option<String> {
        self.inner.lock().last_error.clone()
    }

    fn fenced(&self) -> bool {
        self.inner.lock().fenced
    }

    fn fence_reason(&self) -> Option<String> {
        let guard = self.inner.lock();
        if guard.fenced {
            guard
                .last_error
                .clone()
                .or_else(|| Some("WAL 流已 fence".to_string()))
        } else {
            None
        }
    }

    fn succeeded_appends(&self) -> u64 {
        self.inner.lock().succeeded_appends
    }

    fn failed_appends(&self) -> u64 {
        self.inner.lock().failed_appends
    }

    fn seed_durable_lsn(&self, lsn: u64) {
        let mut guard = self.inner.lock();
        guard.durable_lsn = guard.durable_lsn.max(lsn);
        // 播种值来自 Remote WAL 的末端（恢复）或它的 `last_lsn`（重启接续），两者都已权威。
        guard.baseline_known = true;
    }

    /// 远端 durable 末端是否已经确定；为 false 时第一次 append 前必须先问 Remote WAL。
    fn baseline_known(&self) -> bool {
        self.inner.lock().baseline_known
    }

    /// 标记「远端末端已知但无法读取」（实现不支持时）：不改动本地记账，只是不再重问。
    fn mark_baseline_known(&self) {
        self.inner.lock().baseline_known = true;
    }

    fn next_append_seq(&self) -> u64 {
        let mut guard = self.inner.lock();
        let seq = guard.next_append_seq;
        guard.next_append_seq = guard.next_append_seq.wrapping_add(1);
        seq
    }

    fn begin_write(&self, ticket: Arc<WriteTicket>) {
        let mut guard = self.inner.lock();
        guard.in_flight.push(ticket);
    }

    /// 写入窗口结束：无论成功失败都要把 ticket 从在途集合里摘掉，否则 `step()` 会永远等待。
    fn settle(&self, ticket: &Arc<WriteTicket>) {
        if !ticket.settled() {
            return;
        }
        let mut guard = self.inner.lock();
        guard
            .in_flight
            .retain(|candidate| !Arc::ptr_eq(candidate, ticket));
        if guard.in_flight.is_empty() {
            self.progress.notify_all();
        }
    }

    fn record_success(&self, durable_lsn: u64) {
        let mut guard = self.inner.lock();
        guard.durable_lsn = guard.durable_lsn.max(durable_lsn);
        guard.baseline_known = true;
        guard.succeeded_appends += 1;
        self.progress.notify_all();
    }

    fn record_failure(&self, reason: &str) {
        let mut guard = self.inner.lock();
        guard.failed_appends += 1;
        guard.fenced = true;
        guard.last_error = Some(reason.to_string());
        drop(guard);
        self.progress.notify_all();
    }

    /// 等待在途写入全部结束（或流被 fence）。返回 true 表示已无在途写入。
    fn wait_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.inner.lock();
        while !guard.in_flight.is_empty() && !guard.fenced {
            if self.progress.wait_until(&mut guard, deadline).timed_out() {
                break;
            }
        }
        guard.in_flight.is_empty()
    }

    /// 超时兜底：把仍未结算的 ticket 全部以 durability 失败终止。
    fn fail_in_flight(&self, reason: &str) {
        let tickets: Vec<Arc<WriteTicket>> = {
            let mut guard = self.inner.lock();
            std::mem::take(&mut guard.in_flight)
        };
        for ticket in tickets {
            ticket.fail_durable(reason);
        }
        self.progress.notify_all();
    }
}

/// 构造 durability 违约的引擎错误（会经 [`crate::error::map_engine_error`] 映射为
/// [`domain::error::ErrorCode::WalNotDurable`]）。
fn durable_io_error(reason: &str) -> LimboError {
    LimboError::IoBackendUnavailable(format!("{DURABLE_FAILURE_MARKER}: {reason}"))
}

/// 一个 WAL 路径的共享状态。
struct WalStreamState {
    /// 本地字节流的解析游标。
    parser: Arc<Mutex<WalFrameParser>>,
    /// 打开期的接续状态。
    resume: Arc<ResumeState>,
}

impl WalStreamState {
    fn new() -> Self {
        Self {
            parser: Arc::new(Mutex::new(WalFrameParser::new())),
            resume: Arc::new(ResumeState::default()),
        }
    }
}

/// 打开已存在的本地 WAL 时的**接续**状态（每个 WAL 路径一份，最多执行一次）。
///
/// 触发条件是「文件已有内容」：这些字节由上一代进程写下，且都已经过 durability 边界
/// （本模块只在远程 append 确认后才推进 `durable_lsn`），因此新进程必须从文件末尾续写，
/// 而不是从 0 重新解析。接续期间到达的本地写/截断一律排队 —— 在未知位置上解析字节
/// 只会得到错误的帧边界。
#[derive(Default)]
struct ResumeState {
    inner: Mutex<ResumeInner>,
}

#[derive(Default)]
struct ResumeInner {
    /// 已经完成、或判定无需接续（空文件 / 恢复路径已播种）。
    done: bool,
    /// 本地 WAL 头读取在途。
    in_flight: bool,
    /// 等待接续结果的本地操作（按到达顺序重放）。
    deferred: Vec<DeferredOp>,
}

/// 接续期间排队的一次本地操作：把原本要立即执行的逻辑原样留到对齐之后。
type DeferredOp = Box<dyn FnOnce(&Arc<WalFile>) + Send>;

impl ResumeState {
    fn lock(&self) -> parking_lot::MutexGuard<'_, ResumeInner> {
        self.inner.lock()
    }

    /// 接续是否已经完成；为 false 时本地操作必须排队。
    fn in_flight(&self) -> bool {
        let guard = self.lock();
        !guard.done && guard.in_flight
    }

    /// 标记接续已结束（无需再接续）。
    fn mark_done(&self) {
        let mut guard = self.lock();
        guard.done = true;
        guard.in_flight = false;
    }
}

/// WAL 文件包装：唯一需要拦截 IO 的文件。
struct WalFile {
    inner: Arc<dyn File>,
    ctx: Arc<DurableContext>,
    stream: Arc<WalStreamState>,
    path: String,
}

impl WalFile {
    /// 本地 WAL 文件末尾：上一代进程留下的字节都已经过 durability 边界。
    fn size(&self) -> TursoResult<u64> {
        self.inner.size()
    }

    /// 是否需要先把本地 WAL 对齐到文件末尾。
    fn resume_pending(&self) -> bool {
        self.stream.resume.in_flight()
    }

    /// 把本地操作排到接续之后执行（保持到达顺序）。
    fn defer(&self, op: DeferredOp) {
        self.stream.resume.lock().deferred.push(op);
    }

    /// 打开已存在的 WAL 文件后启动接续（见 [`ResumeState`]）。
    ///
    /// 只有「文件已有内容」才需要：空文件保持既有行为（游标 0，第一次 append 带
    /// `reset_wal`）；已经播种过的路径以播种结果为准。
    fn start_resume(self: &Arc<Self>) {
        {
            let mut guard = self.stream.resume.lock();
            if guard.done || guard.in_flight {
                return;
            }
            guard.in_flight = true;
        }

        match self.size() {
            // 空文件：新建库，或引擎刚清空过（随后会重写 WAL 头 = 新一代）。
            Ok(0) => {
                self.stream.resume.mark_done();
                return;
            }
            Ok(_) => {}
            Err(err) => {
                self.fail_resume(&format!("读取本地 WAL 文件长度失败: {err}"));
                return;
            }
        }

        // 读 32 字节 WAL 头：接续需要 page size 才能继续识别帧边界。
        let wal = Arc::clone(self);
        let buffer = Arc::new(Buffer::new_temporary(WAL_HEADER_SIZE));
        let completion = Completion::new_read(buffer, move |res| {
            wal.finish_resume(res);
            None
        });
        if let Err(err) = self.inner.pread(0, completion) {
            self.fail_resume(&format!("读取本地 WAL 头失败: {err}"));
        }
    }

    /// 接续结果落地：对齐解析游标，然后按顺序放行排队中的本地操作。
    fn finish_resume(self: &Arc<Self>, res: Result<(Arc<Buffer>, i32), CompletionError>) {
        let header = match res {
            Ok((buffer, read)) if read as usize >= WAL_HEADER_SIZE => {
                buffer.as_slice()[..WAL_HEADER_SIZE].to_vec()
            }
            Ok((_buffer, read)) => {
                self.fail_resume(&format!("本地 WAL 头读取不完整：只读到 {read} 字节"));
                return;
            }
            Err(err) => {
                self.fail_resume(&format!("本地 WAL 头读取失败: {err}"));
                return;
            }
        };
        let Some(page_size) = parse_wal_header_page_size(&header) else {
            self.fail_resume("本地 WAL 头非法（magic 或 page_size 不合法），无法在未知位置续写");
            return;
        };

        // 对齐点取**当前**文件长度：从发起读取到现在，文件只可能被引擎截断（本地写在
        // 接续期间一律排队），而截断之后引擎会重写 WAL 头，解析器会自然回到新一代。
        let len = match self.size() {
            Ok(len) => len,
            Err(err) => {
                self.fail_resume(&format!("读取本地 WAL 文件长度失败: {err}"));
                return;
            }
        };
        if len < WAL_HEADER_SIZE as u64 {
            // 连一个完整的头都不够：等于没有一个可续写的代，留给引擎写新头（offset 0）。
            self.stream.resume.mark_done();
            self.release_deferred();
            return;
        }
        // 末尾若不足一帧（崩溃留下的半帧），退到最后一个完整帧边界：续写点只能是帧边界。
        let frame_size = WAL_FRAME_HEADER_SIZE as u64 + u64::from(page_size);
        let frames_end = if len == WAL_HEADER_SIZE as u64 {
            len
        } else {
            WAL_HEADER_SIZE as u64 + (len - WAL_HEADER_SIZE as u64) / frame_size * frame_size
        };

        self.stream.parser.lock().seed(frames_end, page_size, false);
        tracing::info!(
            db_id = %self.ctx.database_id,
            wal_path = %self.path,
            file_offset = frames_end,
            file_len = len,
            page_size,
            "本地 WAL 已接续：解析游标对齐到现有字节的末尾"
        );
        self.stream.resume.lock().in_flight = false;
        self.stream.resume.mark_done();
        self.release_deferred();
    }

    /// 接续失败：本地 WAL 已经不可信（读不出头就读不出帧边界），按协议违规 fail-stop。
    fn fail_resume(self: &Arc<Self>, reason: &str) {
        tracing::error!(
            db_id = %self.ctx.database_id,
            wal_path = %self.path,
            error = %reason,
            "本地 WAL 接续失败，WAL 流进入 fenced 状态"
        );
        self.ctx
            .state
            .record_failure(&format!("本地 WAL 接续失败（{}）: {reason}", self.path));
        self.stream.resume.mark_done();
        self.release_deferred();
    }

    /// 放行排队中的本地操作（按到达顺序；接续失败时它们会在解析阶段被 fence 拦下）。
    fn release_deferred(self: &Arc<Self>) {
        let deferred = std::mem::take(&mut self.stream.resume.lock().deferred);
        for op in deferred {
            op(self);
        }
    }

    fn pwrite_frame(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> TursoResult<Completion> {
        // 接续未完成时不能解析：游标还没对齐到文件末尾，此时任何解析都基于错误的位置。
        // 引擎等待的是返回的 completion，因此这里把它原样交还，由排队后的写去完成它。
        if self.resume_pending() {
            let deferred = c.clone();
            self.defer(Box::new(move |wal: &Arc<WalFile>| {
                if wal.pwrite_frame(pos, buffer, deferred.clone()).is_err() {
                    // 写没能提交出去（解析违规等）：保证等待方一定被唤醒，绝不静默挂起。
                    // 已经 abort 过的 completion 再 abort 不会二次回调。
                    deferred.abort();
                }
            }));
            return Ok(c);
        }

        let bytes = buffer.as_slice();
        let parsed = match self.ctx.parse_write(&self.stream.parser, pos, bytes) {
            Ok(parsed) => parsed,
            Err(err) => {
                self.ctx.on_stream_error(&self.path, &err);
                c.abort();
                return Err(durable_io_error(&format!("本地 WAL 帧解析失败: {err}")));
            }
        };
        let ParsedWrite {
            batches,
            checkpoint,
        } = parsed;
        let produced_commit = !batches.is_empty();

        let parent = c;
        let ticket = if produced_commit {
            let ticket = WriteTicket::new(parent.clone(), batches.len());
            self.ctx.state.begin_write(Arc::clone(&ticket));
            Some(ticket)
        } else {
            None
        };

        let ctx = Arc::clone(&self.ctx);
        let parser = Arc::clone(&self.stream.parser);
        let local = Completion::new_write(move |res| match res {
            Ok(written) => {
                let Some(ticket) = ticket.as_ref() else {
                    // 没有 commit frame：不需要 remote durability，本地写成功即可完成。
                    parent.complete(written);
                    return;
                };
                ticket.set_written(written);
                // 闭包是 `Fn`（可能被重复调用），因此这里只能借用 captured 变量。
                for batch in &batches {
                    let job = AppendJob {
                        ticket: Arc::clone(ticket),
                        file_offset: batch.file_offset,
                        reset_wal: batch.reset_wal,
                        data: batch.data.clone(),
                    };
                    if let Err(reason) = ctx.enqueue(job) {
                        ticket.fail_durable(&reason);
                        return;
                    }
                }
            }
            Err(err) => {
                if !produced_commit {
                    // 本地写失败且本次没有产生提交批次：回滚解析状态，
                    // 让引擎在同一偏移重试时不会看到幽灵字节。
                    parser.lock().rollback_write(checkpoint);
                }
                match &ticket {
                    Some(ticket) => ticket.fail_local(err),
                    None => parent.error(err),
                }
            }
        });
        self.inner.pwrite(pos, buffer, local)
    }

    fn pwrite_vectored(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> TursoResult<Completion> {
        if buffers.len() == 1 {
            return self.pwrite_frame(pos, Arc::clone(&buffers[0]), c);
        }
        // 接续未完成：与 [`WalFile::pwrite_frame`] 同理，整段写入排队到对齐之后。
        if self.resume_pending() {
            let deferred = c.clone();
            self.defer(Box::new(move |wal: &Arc<WalFile>| {
                if wal.pwrite_vectored(pos, buffers, deferred.clone()).is_err() {
                    deferred.abort();
                }
            }));
            return Ok(c);
        }
        // 逻辑上连续的一段写入：拼接后一次性解析，避免逐段解析时把「半个事务」当作边界。
        let total: usize = buffers.iter().map(|buffer| buffer.len()).sum();
        let mut joined = Vec::with_capacity(total);
        for buffer in &buffers {
            joined.extend_from_slice(buffer.as_slice());
        }

        let parsed = match self.ctx.parse_write(&self.stream.parser, pos, &joined) {
            Ok(parsed) => parsed,
            Err(err) => {
                self.ctx.on_stream_error(&self.path, &err);
                c.abort();
                return Err(durable_io_error(&format!("本地 WAL 帧解析失败: {err}")));
            }
        };
        let ParsedWrite {
            batches,
            checkpoint,
        } = parsed;
        let produced_commit = !batches.is_empty();
        let written = joined.len() as i32;

        let parent = c;
        let ticket = if produced_commit {
            let ticket = WriteTicket::new(parent.clone(), batches.len());
            ticket.set_written(written);
            self.ctx.state.begin_write(Arc::clone(&ticket));
            Some(ticket)
        } else {
            None
        };

        let ctx = Arc::clone(&self.ctx);
        let parser = Arc::clone(&self.stream.parser);
        let local = Completion::new_write(move |res| match res {
            Ok(written) => {
                let Some(ticket) = ticket.as_ref() else {
                    parent.complete(written);
                    return;
                };
                ticket.set_written(written);
                // 同上：`Fn` 闭包里按引用遍历，避免把 captured 变量 move 出去。
                for batch in &batches {
                    let job = AppendJob {
                        ticket: Arc::clone(ticket),
                        file_offset: batch.file_offset,
                        reset_wal: batch.reset_wal,
                        data: batch.data.clone(),
                    };
                    if let Err(reason) = ctx.enqueue(job) {
                        ticket.fail_durable(&reason);
                        return;
                    }
                }
            }
            Err(err) => {
                if !produced_commit {
                    parser.lock().rollback_write(checkpoint);
                }
                match &ticket {
                    Some(ticket) => ticket.fail_local(err),
                    None => parent.error(err),
                }
            }
        });
        self.inner.pwritev(pos, buffers, local)
    }
}

impl File for WalFile {
    fn lock_file(&self, exclusive: bool) -> TursoResult<()> {
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> TursoResult<()> {
        self.inner.unlock_file()
    }

    fn pread(&self, pos: u64, c: Completion) -> TursoResult<Completion> {
        self.inner.pread(pos, c)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> TursoResult<Completion> {
        self.pwrite_frame(pos, buffer, c)
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> TursoResult<Completion> {
        self.pwrite_vectored(pos, buffers, c)
    }

    fn sync(&self, c: Completion, sync_type: FileSyncType) -> TursoResult<Completion> {
        // fsync 成功也必须意味着 remote durable：先等在途 append，再同步本地。
        if let Err(err) = self.ctx.wait_for_appends() {
            c.abort();
            return Err(err);
        }
        let parent = c;
        let local = Completion::new_sync(move |res| match res {
            Ok(byte_count) => parent.complete(byte_count),
            Err(err) => parent.error(err),
        });
        self.inner.sync(local, sync_type)
    }

    fn size(&self) -> TursoResult<u64> {
        self.inner.size()
    }

    fn truncate(&self, len: u64, c: Completion) -> TursoResult<Completion> {
        // 截断同样要排在接续之后：截断的合法性取决于「游标在哪」，而游标正是接续要定的。
        if self.resume_pending() {
            let deferred = c.clone();
            self.defer(Box::new(move |wal: &Arc<WalFile>| {
                if wal.truncate(len, deferred.clone()).is_err() {
                    deferred.abort();
                }
            }));
            return Ok(c);
        }
        if let Err(err) = self.stream.parser.lock().on_truncate(len) {
            self.ctx.on_stream_error(&self.path, &err);
            c.abort();
            return Err(durable_io_error(&format!("本地 WAL 截断语义非法: {err}")));
        }
        let parent = c;
        let local = Completion::new_trunc(move |res| match res {
            Ok(byte_count) => parent.complete(byte_count),
            Err(err) => parent.error(err),
        });
        self.inner.truncate(len, local)
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> TursoResult<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> TursoResult<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> TursoResult<()> {
        self.inner.shared_wal_unlock_byte(offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> TursoResult<()> {
        self.inner.shared_wal_set_len(len)
    }

    fn shared_wal_map(
        &self,
        offset: u64,
        len: usize,
    ) -> TursoResult<Box<dyn SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, len)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use domain::error::Result;
    use turso_core::UnixIO;
    use wal_client::{AppendOutcome, AppendWalRequest, WalClientConfig};

    use super::*;

    /// 测试用页大小（与线上一致）。
    const PAGE_SIZE: u32 = 4096;
    /// 一个完整帧的长度。
    const FRAME_SIZE: usize = WAL_FRAME_HEADER_SIZE + PAGE_SIZE as usize;
    /// 「上一代进程留下的 WAL」长度：头 + 2 帧 = 8272 字节（与线上复现时的长度一致）。
    const EXISTING_LEN: usize = WAL_HEADER_SIZE + 2 * FRAME_SIZE;

    /// WAL 头：magic + page_size 就是解析器需要的全部信息。
    fn wal_header() -> Vec<u8> {
        let mut header = vec![0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&crate::wal_frames::WAL_MAGIC_LE.to_be_bytes());
        header[8..12].copy_from_slice(&PAGE_SIZE.to_be_bytes());
        header
    }

    /// 一个完整帧；`commit` 决定它是否携带 `db_size`（= 事务提交边界）。
    fn frame(page_number: u32, commit: bool) -> Vec<u8> {
        let mut frame = vec![0u8; FRAME_SIZE];
        frame[0..4].copy_from_slice(&page_number.to_be_bytes());
        if commit {
            frame[4..8].copy_from_slice(&9u32.to_be_bytes());
        }
        frame
    }

    /// 记录 append 请求的替身：`head` 模拟 Remote WAL 当前的末端 LSN。
    #[derive(Debug)]
    struct RecordingAppender {
        head: Option<u64>,
        appends: StdMutex<Vec<AppendWalRequest>>,
        status_queries: AtomicUsize,
    }

    impl RecordingAppender {
        fn new(head: Option<u64>) -> Self {
            Self {
                head,
                appends: StdMutex::new(Vec::new()),
                status_queries: AtomicUsize::new(0),
            }
        }

        fn appends(&self) -> Vec<AppendWalRequest> {
            self.appends.lock().expect("测试用锁不得中毒").clone()
        }
    }

    #[async_trait]
    impl RemoteWalAppender for RecordingAppender {
        async fn append(&self, request: AppendWalRequest) -> Result<AppendOutcome> {
            let durable_lsn = request.start_lsn.get() + request.bytes.len() as u64;
            self.appends.lock().expect("测试用锁不得中毒").push(request);
            Ok(AppendOutcome {
                durable_lsn: Lsn::new(durable_lsn),
                acked_replicas: vec!["fake".to_string()],
                latency: Duration::ZERO,
                deduplicated: false,
            })
        }

        async fn last_lsn(&self, _database_id: &DatabaseId) -> Result<Option<Lsn>> {
            self.status_queries.fetch_add(1, Ordering::SeqCst);
            Ok(self.head.map(Lsn::new))
        }
    }

    /// `DurableIoConfig` 需要一份 `WalClient`；测试里所有 append 都走注入的替身，
    /// 端点只是一个不会被拨号的占位值。
    fn durable_config(runtime: tokio::runtime::Handle) -> DurableIoConfig {
        let client = Arc::new(
            WalClient::new(WalClientConfig {
                endpoints: vec!["http://127.0.0.1:1".to_string()],
                ..Default::default()
            })
            .expect("构造测试用 WalClient"),
        );
        DurableIoConfig::new(
            DatabaseId::new_v7(),
            7,
            client,
            runtime,
            DEFAULT_APPEND_TIMEOUT,
        )
    }

    /// 写一个已存在的本地 WAL 文件。
    fn write_existing_wal(wal_path: &std::path::Path, bytes: &[u8]) {
        std::fs::write(wal_path, bytes).expect("写上一代本地 WAL");
    }

    /// 在阻塞线程上执行一次本地 WAL 写入并等到完成（引擎线程模型：写路径不占用 runtime
    /// worker 线程，否则 append 流水线会被饿死）。
    async fn write_wal(
        durable: Arc<PlatformDurableIO>,
        wal_path: String,
        pos: u64,
        bytes: Vec<u8>,
    ) {
        tokio::task::spawn_blocking(move || {
            let file = durable
                .open_file(&wal_path, OpenFlags::Create, false)
                .expect("打开 WAL 文件");
            let completion = Completion::new_write(|_| {});
            let handle = completion.clone();
            // 返回的 completion 与传入的是同一个（排队时原样交还），这里只需等它完成。
            drop(
                file.pwrite(pos, Arc::new(Buffer::new(bytes)), completion)
                    .expect("pwrite 必须被接受（不得判成偏移不连续）"),
            );
            let deadline = Instant::now() + Duration::from_secs(10);
            while !handle.finished() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(handle.finished(), "本地写入与远程 append 必须在超时前完成");
        })
        .await
        .expect("写线程不得 panic");
    }

    /// 回归（线上故障的直接原因）：打开**已有内容**的本地 WAL 时，解析游标必须接续到
    /// 文件末尾 —— 否则第一次 pwrite 会被判成「偏移不连续」而 fail-stop（写路径全断）。
    /// 同时验证 durable LSN 从 Remote WAL 的末端接续，而不是从 0 重算。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumes_existing_wal_and_continues_durable_lsn() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = crate::recovery::wal_path_for(&dir.path().join("resume.db"));
        let wal_path_text = wal_path
            .to_str()
            .expect("WAL 路径必须合法 UTF-8")
            .to_string();

        let mut existing = wal_header();
        existing.extend_from_slice(&frame(1, false));
        existing.extend_from_slice(&frame(2, true));
        assert_eq!(existing.len(), EXISTING_LEN);
        write_existing_wal(&wal_path, &existing);

        // 远端已 durable 的末端 = 上一代进程 append 过的字节总数。
        let remote_head = EXISTING_LEN as u64;
        let appender = Arc::new(RecordingAppender::new(Some(remote_head)));
        let durable = Arc::new(PlatformDurableIO::new_with_appender(
            Arc::new(UnixIO::new().expect("UnixIO")),
            durable_config(tokio::runtime::Handle::current()),
            Arc::clone(&appender) as Arc<dyn RemoteWalAppender>,
        ));

        write_wal(
            Arc::clone(&durable),
            wal_path_text,
            remote_head,
            frame(3, true),
        )
        .await;

        assert!(
            !durable.fenced(),
            "接续后不得 fence，last_error={:?}",
            durable.last_error()
        );
        let appends = appender.appends();
        assert_eq!(appends.len(), 1, "一次提交只应产生一次 append");
        let append = &appends[0];
        assert_eq!(
            append.start_lsn.get(),
            remote_head,
            "durable LSN 必须从 Remote WAL 末端接续，不得从 0 重算"
        );
        assert_eq!(
            append.file_offset, remote_head,
            "append 必须带上本地 WAL 的真实偏移（文件末尾）"
        );
        assert!(!append.reset_wal, "续写同一代 WAL 不得带 reset_wal");
        assert!(append.contains_commit_frame);
        assert_eq!(append.bytes.len(), FRAME_SIZE);
        assert_eq!(durable.durable_lsn(), remote_head + FRAME_SIZE as u64);
    }

    /// 崩溃留下的半帧不算数据：接续点退到最后一个完整帧边界，而不是文件末尾的任意字节。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_rounds_down_to_last_frame_boundary() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = crate::recovery::wal_path_for(&dir.path().join("torn.db"));
        let wal_path_text = wal_path
            .to_str()
            .expect("WAL 路径必须合法 UTF-8")
            .to_string();

        let mut existing = wal_header();
        existing.extend_from_slice(&frame(1, true));
        existing.extend_from_slice(&[0xAB; 7]); // 崩溃写了一半的帧
        write_existing_wal(&wal_path, &existing);

        let frame_boundary = (WAL_HEADER_SIZE + FRAME_SIZE) as u64;
        let appender = Arc::new(RecordingAppender::new(Some(frame_boundary)));
        let durable = Arc::new(PlatformDurableIO::new_with_appender(
            Arc::new(UnixIO::new().expect("UnixIO")),
            durable_config(tokio::runtime::Handle::current()),
            Arc::clone(&appender) as Arc<dyn RemoteWalAppender>,
        ));

        write_wal(
            Arc::clone(&durable),
            wal_path_text,
            frame_boundary,
            frame(2, true),
        )
        .await;

        assert!(
            !durable.fenced(),
            "半帧必须被忽略，last_error={:?}",
            durable.last_error()
        );
        assert_eq!(appender.appends().len(), 1);
    }

    /// 恢复路径播种过的流以播种值为准：第一次 append 从播种的 durable_lsn 接续，
    /// 且不再多问一次远端（播种值本来就来自远端末端）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seeded_stream_continues_from_seeded_lsn() {
        let dir = tempfile::tempdir().expect("临时目录");
        let wal_path = crate::recovery::wal_path_for(&dir.path().join("seeded.db"));
        let wal_path_text = wal_path
            .to_str()
            .expect("WAL 路径必须合法 UTF-8")
            .to_string();

        let seeded_offset = (WAL_HEADER_SIZE + FRAME_SIZE) as u64;
        let mut existing = wal_header();
        existing.extend_from_slice(&frame(1, true));
        write_existing_wal(&wal_path, &existing);

        let appender = Arc::new(RecordingAppender::new(Some(999_999)));
        let durable = Arc::new(PlatformDurableIO::new_with_appender(
            Arc::new(UnixIO::new().expect("UnixIO")),
            durable_config(tokio::runtime::Handle::current()),
            Arc::clone(&appender) as Arc<dyn RemoteWalAppender>,
        ));
        durable
            .seed_wal_stream(
                &wal_path_text,
                WalStreamSeed {
                    durable_lsn: 4096,
                    file_offset: seeded_offset,
                    page_size: PAGE_SIZE,
                    reset_wal: false,
                },
            )
            .expect("播种必须成功");

        write_wal(
            Arc::clone(&durable),
            wal_path_text,
            seeded_offset,
            frame(2, true),
        )
        .await;

        let appends = appender.appends();
        assert_eq!(appends.len(), 1);
        assert_eq!(
            appends[0].start_lsn.get(),
            4096,
            "播种过的流必须从播种的 durable_lsn 接续"
        );
        assert_eq!(
            appender.status_queries.load(Ordering::SeqCst),
            0,
            "播种已经是权威值，不得再问一次远端"
        );
    }
}
