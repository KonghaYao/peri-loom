//! # engine-adapter —— TursoDB 集成的**唯一入口**（架构 §17.3 / §11.1）
//!
//! 本 crate 是平台与 TursoDB 引擎之间的唯一边界：
//!
//! ```text
//! Server / Worker / WAL Service   —— 禁止依赖 turso_core
//!        │
//!        ▼
//! engine-adapter（本 crate）       —— TursoDB 类型只允许出现在这里与 db-runtime
//!        │
//!        ├── PlatformDurableIO     —— commit durability 强制点
//!        └── EngineAdapter         —— 打开 / 执行 / 事务 / 取消
//! ```
//!
//! ## 三个硬性契约
//!
//! 1. **Commit Success ⇒ Remote WAL Durable**（架构 §11.1 / §15.1）。
//!    [`PlatformDurableIO`] 拦截 WAL 文件写入，识别 SQLite/Turso 的 WAL 帧边界；
//!    只要一次写入包含 commit frame（帧头 `db_size != 0`），**在 Remote WAL 返回
//!    quorum durable 之前绝不向引擎报告写入完成**。远程失败时该次写入失败，
//!    引擎拿到的是 IO 错误，不可能出现「本地写了就当成功」的假提交。
//!
//! 2. **平台其他模块不理解 Turso WAL 内部格式**。帧格式只在本 crate 的
//!    [`durable::frame`] 里解析，通过 [`PlatformDurableIO`] 暴露的只有
//!    `durable_lsn` / 追加成功计数 / 失败原因这些平台语义。
//!
//! 3. **失败必须显式**。远程 WAL 不可用时写请求失败（而不是降级为假成功），
//!    并且失败原因通过 [`PlatformDurableIO::last_error`] / `fenced` / `failed`
//!    暴露给 db-runtime，由它决定「杀进程 + 故障转移」（架构 §11.2 / §12.1）。
//!
//! ## 典型用法（db-runtime）
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # fn run() -> domain::error::Result<()> {
//! use engine_adapter::{DurableIoConfig, EngineAdapter, EngineOpenConfig, PlatformDurableIO};
//! use std::path::PathBuf;
//!
//! // 1. 恢复流程（failover 冷启动）：先把本地 WAL 重建到与 Remote WAL 一致
//! //    —— 见 recovery 模块。
//! # let db_path = PathBuf::from("/var/lib/platform/dbs/demo.db");
//! # let runtime = tokio::runtime::Handle::current();
//! # let wal_client = Arc::new(wal_client::WalClient::new(wal_client::WalClientConfig {
//! #     endpoints: vec!["http://127.0.0.1:9200".into()], ..Default::default() })?);
//! // 2. 用 Remote WAL 包住本地 IO：所有 commit 都要等 Remote WAL durable
//! //    UnixIO 的构造错误是引擎错误，示例签名用的是平台 Result，故显式映射一次。
//! let local_io: Arc<dyn turso_core::IO> = Arc::new(
//!     turso_core::UnixIO::new().map_err(|err| engine_adapter::map_engine_error(&err))?,
//! );
//! let durable = Arc::new(PlatformDurableIO::new(
//!     local_io.clone(),
//!     DurableIoConfig::new(
//!         domain::DatabaseId::new_v7(),
//!         /* owner_epoch */ 835,
//!         wal_client,
//!         runtime,
//!         Duration::from_secs(3),
//!     ),
//! ));
//!
//! // 3. 引擎必须跑在同一个 durable IO 上，否则 durability 契约被绕过
//! let adapter = EngineAdapter::open(
//!     durable.clone(),
//!     EngineOpenConfig {
//!         db_path,
//!         durable_io: Some(durable.clone()),
//!         owner_epoch: 835,
//!     },
//! )?;
//! let conn = adapter.connect()?;
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

pub mod durable;
pub mod engine;
pub mod error;
pub mod recovery;

// WAL 字节流的帧解析：只服务本 crate 的 durability 实现（`durable_io` 通过
// `crate::wal_frames` 引用），**故意不公开** —— 平台其他模块不得理解 Turso WAL 内部格式。
pub(crate) mod wal_frames;

pub use durable::frame::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
pub use durable::{
    DurableIoConfig, PlatformDurableIO, RemoteWalAppender, WalClientAppender,
    DEFAULT_APPEND_TIMEOUT,
};
pub use engine::{EngineAdapter, EngineConnection, EngineOpenConfig, QueryOutcome, ENGINE_VERSION};
pub use error::{
    engine_error_code, is_durability_failure, map_engine_error, DURABILITY_ERROR_LABELS,
};
pub use recovery::{
    ensure_base_db_for_wal, prepare_local_wal_for_restore, replay_wal_segments,
    replay_wal_segments_from_generation_start, snapshot_files, wal_path_for, BaseDbState,
};

/// 本 crate 绑定的 TursoDB 引擎版本。
///
/// 架构 §17.3 要求 TursoDB 依赖固定在明确 tag 并提供 `Cargo.lock`，禁止跟随
/// floating semver。该常量是 `Cargo.toml` 中 `turso_core = "=0.8.1"` 的镜像，
/// 由 `tests::engine_version_is_pinned` 断言两处一致。
pub const TURSO_CORE_VERSION: &str = "0.8.1";
