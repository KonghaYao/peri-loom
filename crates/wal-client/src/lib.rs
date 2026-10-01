//! # wal-client —— Remote WAL 客户端（架构 §11 / §17.8）
//!
//! Remote WAL 是平台的 **commit durability 边界**：事务只有在 Remote WAL 已
//! quorum durable 之后才允许向 Client 返回 Commit Success（架构 §11.1）。本 crate
//! 是该契约的客户端一半，因此下面三条语义是硬性的，任何「性能优化」都不得削弱：
//!
//! 1. **只有 leader 返回成功才算成功**。`append` 返回 `Ok` 即对调用方承诺
//!    「本批次已 quorum durable」；收到非 leader / 传输错误 / 超时一律不得伪装成功。
//! 2. **重试幂等**。重试期间 `append_id` 保持不变，服务端按
//!    `(db_id, start_lsn, epoch)` 去重，因此「重试」永远不会重复写入 WAL。
//! 3. **fencing 拒绝不得重试**。服务端返回 `EPOCH_MISMATCH` /
//!    `WAL_APPEND_REJECTED` 说明本客户端的 `owner_epoch` 已过期（架构 §11.3），
//!    重试只会把旧的写 Owner 继续推给存储层，必须立即失败并把所有权问题暴露给调用方。
//!
//! 重试策略（有界，绝不无限重试）：按「已知 leader 端点 → 轮换其余副本」的顺序尝试，
//! 每次尝试间隔 5ms / 10ms / 20ms 的指数退避，次数上限由
//! [`WalClientConfig::max_attempts`] 决定。
//!
//! ```no_run
//! # async fn run() -> domain::error::Result<()> {
//! use wal_client::{AppendWalRequest, WalClient, WalClientConfig};
//!
//! let client = WalClient::new(WalClientConfig {
//!     endpoints: vec!["http://wal-1:9200".into(), "http://wal-2:9200".into()],
//!     ..Default::default()
//! })?;
//!
//! let outcome = client
//!     .append(AppendWalRequest {
//!         database_id: domain::DatabaseId::new_v7(),
//!         owner_epoch: 835,
//!         start_lsn: domain::Lsn::new(4096),
//!         file_offset: 0,
//!         reset_wal: false,
//!         bytes: bytes::Bytes::from_static(b"wal-frame"),
//!         contains_commit_frame: true,
//!         append_id: "append-0001".into(),
//!     })
//!     .await?;
//!
//! // 到这里才允许向 Client 返回 Commit Success
//! let _durable_lsn = outcome.durable_lsn;
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

mod channel;
mod client;
mod config;
mod endpoint;
mod error;
mod metrics;
mod model;

pub use client::WalClient;
pub use config::WalClientConfig;
pub use model::{AppendOutcome, AppendWalRequest, WalHealth, WalSegment, WalStatus};

/// 本 crate 使用的指标名（契约来自 `crates/observability/src/metrics.rs`，不得改名）。
///
/// 复制常量而不是依赖 observability crate：wal-client 是底层传输客户端，
/// 不应把整个观测栈（Prometheus exporter / OTLP）拖进调用方依赖图。
pub mod metric_names {
    /// Remote WAL append 延迟直方图，单位微秒。
    pub const WAL_APPEND_LATENCY_MICROS: &str = "wal_append_latency_micros";
    /// Remote WAL append 成功（durable）次数。
    pub const WAL_APPEND_TOTAL: &str = "wal_append_total";
    /// Remote WAL append 失败次数，标签 `reason`。
    pub const WAL_APPEND_ERROR_TOTAL: &str = "wal_append_error_total";
}

/// 本 crate 使用的 tracing span 字段名（契约来自 `crates/observability/src/context.rs`）。
pub mod field_names {
    /// 数据库 id。
    pub const DB_ID: &str = "db_id";
    /// Owner epoch（架构 §15.2）。
    pub const OWNER_EPOCH: &str = "owner_epoch";
    /// WAL LSN。
    pub const WAL_LSN: &str = "wal_lsn";
    /// Worker id。
    pub const WORKER_ID: &str = "worker_id";
}
