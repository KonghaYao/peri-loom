//! # routing —— Server Route Cache（架构 §4.2 / §16 / §17.4）
//!
//! Server 只维护到 Worker 粒度的路由：`db_id -> worker_id -> worker_endpoint`，
//! 绝不维护 `db_id -> PID / 进程端口`（进程属于 Worker 本地状态，由 Worker 自己管理）。
//!
//! ## 硬性规则：Route **绝不基于 TTL 主动过期**
//!
//! 这是架构 §16「Control Plane 故障」场景的验收要求：Control Plane 全不可用 30 分钟时，
//! 已经 RUNNING 的 DB 数据面成功率必须 >= 99.99%，**已缓存 Route 不得因 Control Plane
//! 不可用主动过期导致全量中断**。
//!
//! 因此本 crate 里没有任何过期时间、没有后台清理任务、没有 `Instant::now()` 判定：
//! 一条 Route 一旦写入就永久有效，直到下面三种**明确事件**之一发生：
//!
//! 1. 显式 [`RouteCache::invalidate`]（运维 / Control Plane 明确下令）；
//! 2. Worker 返回 `NOT_OWNER` / `EPOCH_MISMATCH` 后由调用方刷新
//!    （`domain::ErrorCode::NotOwner` / `EpochMismatch` / `RouteStale`）；
//! 3. Catalog 版本前进后的 reconcile（[`RouteCache::apply_catalog_snapshot`]）。
//!
//! 换句话说：**缓存的陈旧只会由「更新的事实」纠正，绝不会由「时间流逝」纠正**；
//! 陈旧检测走 epoch 与 catalog_version，不走时钟。
//!
//! ## 正确性
//!
//! - `catalog_version` 只允许单调前进：版本回退 / 重复的快照一律忽略（§17.4
//!   「PostgreSQL Catalog = source of truth，LISTEN/NOTIFY 只是加速提示，version reconcile
//!   才是 correctness path」）。
//! - owner epoch 只允许单调前进：epoch 倒退或同 epoch 换 owner 一律拒绝，
//!   避免把流量送到旧 Owner（§10 Split Brain）。

#![forbid(unsafe_code)]

mod cache;
mod entry;

pub use cache::{RouteCache, RouteCacheStats};
pub use entry::RouteEntry;
