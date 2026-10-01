//! DB Process 的宿主：恢复 -> 引擎打开 -> 运行状态。
//!
//! 启动顺序是**冻结**的（见 [`engine_adapter::PlatformDurableIO::seed_wal_stream`] 的文档）：
//!
//! ```text
//! prepare_local_wal_for_restore  ->  replay_wal_segments  ->  seed_wal_stream  ->  EngineAdapter::open
//! ```
//!
//! 任何一步顺序颠倒，引擎都会在一个"未知偏移"的 WAL 上续写。特别是 seed 必须在 open
//! 之前：`PlatformDurableIO` 的帧解析器一旦进入 open 之后的写路径，就没有第二次播种机会。
//!
//! 关于快照恢复的边界（必须说清楚）：本进程**不下载**快照。快照对象存在对象存储里，
//! 下载与解包由具备对象存储凭据的 Worker 完成（`snapshot_files` 给出的就是它要放的
//! `db` / `wal` 两个文件），本进程负责的是**在快照之上把 Remote WAL 从 `base_lsn`
//! 回放到本地**。因此 `--snapshot-id` 对本进程的含义是"本地文件已经是该快照的内容"，
//! 它只参与日志与 Hello 上报，不触发任何网络下载。

use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use domain::error::ErrorCode;
use domain::ids::DatabaseId;
use domain::Lsn;
use engine_adapter::durable::WalStreamSeed;
use engine_adapter::{
    ensure_base_db_for_wal, prepare_local_wal_for_restore, replay_wal_segments, BaseDbState,
    DurableIoConfig, EngineAdapter, EngineOpenConfig, PlatformDurableIO, RemoteWalAppender,
    DEFAULT_APPEND_TIMEOUT,
};
use protocol::runtime_local as rt;
use tokio::sync::broadcast;
use turso_core::{UnixIO, IO};
use wal_client::{WalClient, WalClientConfig};

use crate::cancel::CancelRegistry;
use crate::config::RuntimeConfig;
use crate::session::{ExpiryReason, SessionManager};

/// 进程退出码：正常停机。
pub const EXIT_OK: i32 = 0;
/// 进程退出码：启动失败 / 异常终止。
pub const EXIT_FAILURE: i32 = 1;
/// 进程退出码：fencing（所有权已被取代，架构 §11.3）。
pub const EXIT_FENCED: i32 = 2;
/// 进程退出码：durable IO fail-stop（本地 WAL 字节流不可信，架构 §11.1 / §12.1）。
///
/// 非零是刻意的：Worker 据此把它当成"异常退出"，走既有的崩溃检测 + 自动重启路径，
/// 用一个干净进程重新建立一致性（见 [`Host::durable_fail_stop`]）。
pub const EXIT_DURABLE_STOP: i32 = 3;

/// 恢复播种用的 WAL 页大小。
///
/// 平台以默认 options 打开引擎（`EngineAdapter::open` 不暴露 page_size），Turso/SQLite
/// 的默认页大小就是 4096。这里**只为回放后的续写对齐服务**：若将来引擎允许自定义
/// page_size，这个常量必须与之一同改动，否则 WAL 帧解析会把页边界算错。
const DEFAULT_WAL_PAGE_SIZE: u32 = 4096;

/// 进程运行状态（对外通过 `HealthResponse.state` 暴露）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostState {
    /// 正常服务。
    Running,
    /// 正在优雅停机：拒绝新请求。
    Draining,
}

impl HostState {
    /// 健康响应用字符串。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            HostState::Running => "RUNNING",
            HostState::Draining => "DRAINING",
        }
    }

    fn encode(self) -> u8 {
        match self {
            HostState::Running => 0,
            HostState::Draining => 1,
        }
    }

    fn decode(raw: u8) -> Self {
        if raw == 1 {
            HostState::Draining
        } else {
            HostState::Running
        }
    }
}

/// DB Process 宿主。
pub struct Host {
    /// 启动配置。
    pub config: RuntimeConfig,
    /// 本库 id。
    pub database_id: DatabaseId,
    /// 引擎句柄。
    pub adapter: Arc<EngineAdapter>,
    /// durable IO（commit 的 durability 强制点）。
    pub durable: Arc<PlatformDurableIO>,
    /// 会话表。
    pub sessions: Arc<SessionManager>,
    /// 在途请求登记表。
    pub cancels: Arc<CancelRegistry>,
    /// 会话过期广播（连接任务订阅后转发 `SessionExpiredNotice`）。
    pub notices: broadcast::Sender<(String, ExpiryReason)>,
    /// 恢复到的 LSN（Hello 上报）。
    pub recovered_lsn: u64,
    /// 进程内已服务连接数（健康检查）。
    pub opened_connections: Arc<std::sync::atomic::AtomicU64>,
    /// 在途请求数（优雅停机要等它归零）。
    pub in_flight: Arc<std::sync::atomic::AtomicUsize>,
    /// 停机信号：任何一方（信号 / Shutdown 帧 / fencing / 父进程消失）都能唤醒主循环。
    pub shutdown: Arc<tokio::sync::Notify>,
    /// 期望退出码（0 = 优雅停机，1 = 异常，2 = fencing）。
    pub exit_code: Arc<std::sync::atomic::AtomicI32>,
    state: AtomicU8,
}

impl Host {
    /// 打开数据库：恢复 -> durable IO -> 引擎。
    ///
    /// `appender` 为 `None` 时走生产路径（append 直接打到 `--wal-cluster` 给出的
    /// Remote WAL）；`Some` 时注入自定义 append 实现（测试用本地 ACK 的 appender，
    /// 这样测试既不依赖真实 WAL 集群，又能覆盖 commit 的 durability 路径）。
    pub async fn open(
        config: RuntimeConfig,
        appender: Option<Arc<dyn RemoteWalAppender>>,
    ) -> Result<Arc<Self>> {
        // `database_id` 是 Catalog 的主键（UUID 文本）：Remote WAL 的幂等键与 fencing
        // 身份都由它派生，因此这里必须严格解析，不能用一个"差不多"的字符串顶替。
        let database_id = DatabaseId::from_str(config.database_id.trim())
            .map_err(|err| anyhow!("--database-id 必须是 UUID：{err}"))?;
        let endpoints = config.wal_endpoints();

        // 数据库文件目录可能还不存在（首次调度到本 Worker）。
        let db_dir = config.db_dir().to_path_buf();
        std::fs::create_dir_all(&db_dir)
            .with_context(|| format!("创建数据库目录失败：{}", db_dir.display()))?;

        let wal_client = Arc::new(
            WalClient::new(WalClientConfig {
                endpoints: endpoints.clone(),
                ..Default::default()
            })
            .map_err(|err| anyhow!("构造 Remote WAL 客户端失败：{err}"))?,
        );

        // ---- 恢复（failover 冷启动）----
        let mut recovered_lsn = config.base_lsn;
        let mut seed = None;
        if config.needs_recovery() {
            (recovered_lsn, seed) = restore_from_remote(&config, &database_id, &wal_client)
                .await
                .with_context(|| {
                    format!("从 Remote WAL 恢复失败（base_lsn={}）", config.base_lsn)
                })?;
        }
        let wal_path = config.resolved_wal_path();

        // ---- 恢复的最后一道保险：主库为空时用 WAL 的 page 1 重建库头 ----
        //
        // 必须在 open 之前：引擎一旦看到「主库零页 + WAL 有帧」，会认定 WAL 不属于这个库
        // 并把它删掉（已提交的数据在本地就此消失）。Worker 只回放 WAL 字节、不解包引擎
        // 格式，因此「谁来补上 page 1」只能由本进程负责（详见 ensure_base_db_for_wal）。
        // 注意这一步与 needs_recovery 无关：Worker 已经把 WAL 回放到本地时，这里的
        // base_lsn 仍是 0，但主库照样可能是空的（正是线上丢数据的那条路径）。
        match ensure_base_db_for_wal(&config.db_path, &wal_path)
            .with_context(|| format!("重建数据库头失败（db={}）", config.db_path.display()))?
        {
            BaseDbState::AlreadyPaged { .. } | BaseDbState::NoWalFrames => {}
            BaseDbState::Rebuilt {
                page_size,
                frame_offset,
            } => tracing::warn!(
                db_id = %database_id,
                db_path = %config.db_path.display(),
                page_size,
                frame_offset,
                "主库文件为空而本地 WAL 有帧：已从 WAL 重建库头"
            ),
        }

        // ---- durable IO：所有 commit 必须等 Remote WAL quorum ----
        let unix_io = UnixIO::new().map_err(|err| anyhow!("构造本地 IO 失败：{err}"))?;
        let local_io: Arc<dyn IO> = Arc::new(unix_io);
        let durable_config = DurableIoConfig::new(
            database_id,
            config.owner_epoch,
            Arc::clone(&wal_client),
            tokio::runtime::Handle::current(),
            DEFAULT_APPEND_TIMEOUT,
        );
        let durable = Arc::new(match appender {
            Some(appender) => {
                PlatformDurableIO::new_with_appender(local_io, durable_config, appender)
            }
            None => PlatformDurableIO::new(local_io, durable_config),
        });

        // ---- 播种必须在 open 之前 ----
        if let Some(seed) = seed {
            let wal_path_str = wal_path
                .to_str()
                .ok_or_else(|| anyhow!("WAL 路径不是合法 UTF-8：{}", wal_path.display()))?;
            durable
                .seed_wal_stream(wal_path_str, seed)
                .map_err(|err| anyhow!("恢复播种失败：{err}"))?;
        }

        let adapter = EngineAdapter::open(
            Arc::clone(&durable) as Arc<dyn IO>,
            EngineOpenConfig {
                db_path: config.db_path.clone(),
                durable_io: Some(Arc::clone(&durable)),
                owner_epoch: config.owner_epoch,
            },
        )
        .map_err(|err| anyhow!("打开数据库失败：{err}"))?;

        let (notices, _) = broadcast::channel(64);
        tracing::info!(
            db_id = %database_id,
            owner_epoch = config.owner_epoch,
            db_path = %config.db_path.display(),
            wal_path = %wal_path.display(),
            socket = %config.socket_path.display(),
            read_only = config.read_only,
            recovered_lsn,
            durable_lsn = durable.durable_lsn(),
            engine_version = engine_adapter::ENGINE_VERSION,
            "DB Process 已打开数据库"
        );

        Ok(Arc::new(Self {
            sessions: Arc::new(SessionManager::new(
                config.session_idle_timeout(),
                config.transaction_max_lifetime(),
            )),
            cancels: Arc::new(CancelRegistry::new()),
            database_id,
            adapter,
            durable,
            notices,
            recovered_lsn,
            opened_connections: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            exit_code: Arc::new(std::sync::atomic::AtomicI32::new(EXIT_OK)),
            state: AtomicU8::new(HostState::Running.encode()),
            config,
        }))
    }

    /// 触发 fencing 退出（架构 §11.3）。
    ///
    /// 本进程一旦发现自己的所有权存疑（epoch 不一致 / Dispatcher 明确拒绝），
    /// 唯一安全的动作是**立刻停止服务并退出**：继续跑就会和新的 Owner 同时写 WAL。
    pub fn fenced(&self, reason: &str) {
        tracing::error!(reason, db_id = %self.database_id, "所有权存疑，退出进程（fencing）");
        self.begin_drain();
        self.exit_code
            .store(EXIT_FENCED, std::sync::atomic::Ordering::Release);
        self.shutdown.notify_waiters();
    }

    /// durable IO fail-stop：本进程已经不可能再成功提交一次写入，按架构 §12.1 退出。
    ///
    /// 为什么不"继续运行等恢复"：本地 WAL 的字节级对应关系已经不可信，本进程没有第二次
    /// 播种机会（见 `PlatformDurableIO::seed_wal_stream`）。留在原地只会让 Server 与用户
    /// 看到一个"看似可用但不能写"的库；退出后 Worker 会在 <=500ms 内检测到并自动重启，
    /// 新进程从 Remote WAL 重新建立一致状态（§11.1 的 durability 契约不因此改变：
    /// 已提交的事务都在 Remote WAL 里，本地字节不构成权威）。
    pub fn durable_fail_stop(&self, reason: &str) {
        tracing::error!(
            db_id = %self.database_id,
            owner_epoch = self.config.owner_epoch,
            pid = std::process::id(),
            durable_lsn = self.durable.durable_lsn(),
            failed_appends = self.durable.failed_appends(),
            exit_code = EXIT_DURABLE_STOP,
            reason,
            "durable IO 已 fail-stop（本地 WAL 字节流不可信），本进程不再持有可写的 WAL，退出"
        );
        // 在途请求立刻以明确错误结束：取消标记在语句边界被观察，客户端拿到 CANCELLED，
        // 而不是挂到会话超时（架构 §15.6 / §13 的取消语义）。
        let cancelled = self
            .cancels
            .cancel_all("durable IO fail-stop：DB Process 即将退出");
        if cancelled > 0 {
            tracing::warn!(cancelled, "已取消在途请求（DB Process 即将退出）");
        }
        self.begin_drain();
        self.exit_code
            .store(EXIT_DURABLE_STOP, std::sync::atomic::Ordering::Release);
        self.shutdown.notify_waiters();
    }

    /// 请求停机（信号 / Shutdown 帧 / 父进程消失）。
    pub fn request_shutdown(&self, exit_code: i32) {
        self.exit_code
            .store(exit_code, std::sync::atomic::Ordering::Release);
        self.shutdown.notify_waiters();
    }

    /// 数据库 id 的文本形式（对外的身份字符串，与 Worker 的 `--database-id` 逐字一致）。
    #[must_use]
    pub fn database_id_text(&self) -> &str {
        &self.config.database_id
    }

    /// 当前状态。
    #[must_use]
    pub fn state(&self) -> HostState {
        HostState::decode(self.state.load(Ordering::Acquire))
    }

    /// 进入优雅停机（幂等）。
    pub fn begin_drain(&self) {
        self.state
            .store(HostState::Draining.encode(), Ordering::Release);
    }

    /// 已 quorum durable 的末端 LSN。
    #[must_use]
    pub fn durable_lsn(&self) -> u64 {
        self.durable.durable_lsn()
    }

    /// 广播会话过期通知。
    pub fn notify_expired(&self, session_id: &str, reason: ExpiryReason) {
        // 没有订阅者（还没有连接）时忽略错误：通知是观测手段，不是可靠投递。
        let _ = self.notices.send((session_id.to_string(), reason));
    }

    /// 优雅停机收尾：等在途请求结束（有上限）并回滚全部在途事务。
    ///
    /// 关于"flush WAL"：本平台的 durability 强制点在 `commit` 的写入路径上——
    /// 一次 commit 返回时该批字节**已经**在 Remote WAL quorum durable。因此停机时
    /// 不需要额外 fsync，只需要保证没有语句在飞行中（否则它可能在停机后写出半个事务）。
    pub async fn drain(&self, grace: Duration) {
        self.begin_drain();
        let deadline = tokio::time::Instant::now() + grace;
        while self.in_flight.load(Ordering::Acquire) > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let pending = self.in_flight.load(Ordering::Acquire);
        if pending > 0 {
            tracing::warn!(pending, "优雅停机超时，仍有在途请求");
        }
        for session in self.sessions.all() {
            session.close_and_rollback().await;
        }
        tracing::info!(
            db_id = %self.database_id,
            durable_lsn = self.durable_lsn(),
            "DB Process 已停止服务（WAL 末端 LSN 已确认 durable）"
        );
    }

    /// 只读实例是否拒绝该请求。
    #[must_use]
    pub fn read_only(&self) -> bool {
        self.config.read_only
    }

    /// 构造本进程的 `Hello` 帧内容。
    #[must_use]
    pub fn hello(&self) -> rt::Hello {
        rt::Hello {
            database_id: self.database_id_text().to_string(),
            owner_epoch: self.config.owner_epoch,
            process_id: format!("db-runtime:{}", std::process::id()),
            pid: i64::from(std::process::id()),
            engine_version: engine_adapter::ENGINE_VERSION.to_string(),
            local_socket: self.config.socket_path.display().to_string(),
            snapshot_id: self.config.snapshot_id.clone(),
            recovered_lsn: self.recovered_lsn,
            read_only: self.config.read_only,
        }
    }
}

/// 从 Remote WAL 恢复本地 WAL；返回 `(recovered_lsn, 播种参数)`。
async fn restore_from_remote(
    config: &RuntimeConfig,
    database_id: &DatabaseId,
    wal_client: &WalClient,
) -> Result<(u64, Option<WalStreamSeed>)> {
    let wal_path = config.resolved_wal_path();
    let base = Lsn::new(config.base_lsn);
    let status = wal_client
        .status(database_id)
        .await
        .map_err(|err| anyhow!("读取 Remote WAL 状态失败：{err}"))?;
    let last = status.last_lsn;

    if last.get() <= base.get() {
        tracing::info!(
            db_id = %database_id,
            base_lsn = base.get(),
            last_lsn = last.get(),
            "Remote WAL 没有超出 base_lsn 的数据，无需回放"
        );
        return Ok((base.get(), None));
    }

    // 上一代残留必须先清干净：WAL 与 SHM 混着旧字节会让引擎读到错误的页。
    prepare_local_wal_for_restore(&wal_path)
        .map_err(|err| anyhow!("清理旧本地 WAL 失败：{err}"))?;

    let segments = wal_client
        .read_range(database_id, base, last)
        .await
        .map_err(|err| anyhow!("读取 Remote WAL 段失败：{err}"))?;
    let file_offset = replay_wal_segments(&wal_path, &segments)
        .map_err(|err| anyhow!("回放 Remote WAL 段失败：{err}"))?;

    tracing::info!(
        db_id = %database_id,
        base_lsn = base.get(),
        last_lsn = last.get(),
        segments = segments.len(),
        file_offset,
        snapshot_id = %config.snapshot_id,
        "Remote WAL 回放完成"
    );

    Ok((
        last.get(),
        Some(WalStreamSeed {
            durable_lsn: last.get(),
            file_offset,
            page_size: DEFAULT_WAL_PAGE_SIZE,
            reset_wal: false,
        }),
    ))
}

/// 只读实例允许的语句前缀（保守白名单）。
///
/// 为什么要白名单而不是黑名单：`WITH ... INSERT` 这类语句能让黑名单失效，而漏判的代价是
/// 只读副本上出现写入。白名单只放过明确只读的关键字，代价是某些只读语句（如
/// `WITH ... SELECT`）会被拒绝——由调度器改派可写实例即可。
pub const READ_ONLY_ALLOWED_PREFIXES: &[&str] =
    &["select", "pragma", "explain", "values", "vacuum"];

/// 判断语句在只读实例上是否允许执行。
#[must_use]
pub fn is_read_only_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let head: String = trimmed
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_lowercase();
    READ_ONLY_ALLOWED_PREFIXES.contains(&head.as_str())
}

/// 只读拒绝错误。
#[must_use]
pub fn read_only_rejection() -> domain::error::PlatformError {
    domain::error::PlatformError::new(
        ErrorCode::PermissionDenied,
        "本实例是只读副本：拒绝执行写语句",
    )
}

/// 当前进程常驻内存（KiB），取不到时返回 0。
#[must_use]
pub fn rss_kib() -> u64 {
    // /proc/self/statm 第二列是常驻页数；除以页大小即可，避免依赖 procfs crate。
    if let Ok(raw) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(field) = raw.split_whitespace().nth(1) {
            if let Ok(pages) = field.parse::<u64>() {
                return pages.saturating_mul(4);
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_classification_is_conservative() {
        assert!(is_read_only_statement("  SELECT 1"));
        assert!(is_read_only_statement("select * from t"));
        assert!(is_read_only_statement("PRAGMA journal_mode"));
        assert!(is_read_only_statement("EXPLAIN SELECT 1"));
        assert!(!is_read_only_statement("insert into t values (1)"));
        assert!(!is_read_only_statement(
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x"
        ));
        assert!(!is_read_only_statement("CREATE TABLE t(x)"));
        assert!(!is_read_only_statement("-- 注释开头的语句"));
    }

    #[test]
    fn rss_is_reported_on_linux() {
        assert!(rss_kib() > 0, "Linux 上应能读到常驻内存");
    }
}
