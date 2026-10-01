//! # scheduler —— Resource Budget Packing 与 Failover Reserve（架构 §9 / §15.5）
//!
//! 职责边界：
//! - **只做 placement 决策**（`db -> worker`），不写 Catalog、不做 I/O、不发指令；
//!   候选集合由 Control Plane 从 Worker inventory 组装后传入，结果由调用方转成
//!   Worker Agent 的 Start / Stop / Move 指令（§4.4）。
//! - 全同步、无 async：决策必须在单次 Control Plane 调用内完成，不能等待网络。
//!
//! 冻结语义（不得调整）：
//!
//! ```text
//! Target Packing        70% ~ 75%     放置后落在该区间的候选得分最高
//! Stop New Placement    80%           >= 80% 停止普通新 placement
//! Emergency Protection  90%           >= 90% 紧急保护，绝不放置
//! Failover Reserve      >= max(1 whole Worker, 20% total effective capacity)
//! ```
//!
//! 关键设计点：
//! - **DB Count 不是主指标**，只作为 hard safety limit
//!   （[`domain::policy::MAX_DB_PROCESS_PER_WORKER_DEFAULT`]，Worker 显式配置时以配置为准）。
//! - **AdmissionCheck 只看五维**：CPU / Memory / FD / Disk / ProcessCount（§9 的 `CanStart`）。
//!   IOPS 是容量模型里的压力维度，但不属于 `CanStart` 公式，因此不阻塞启动；
//!   它仍然参与水位判定（水位取六维最大利用率）。
//! - **Failover Reserve 是集群级约束**：选择后集群剩余的 failover 可用容量
//!   （允许用到 Emergency 水位为止）不得低于请求携带的保留量；否则拒绝 placement，
//!   绝不允许「先跑起来再说」把集群的兜底容量吃光。
//! - **failover 路径豁免 reserve**：reserve 本来就是给 failover 用的
//!   （[`Scheduler::select_for_failover`]），但任何情况下都不得跨过 Emergency 水位。

#![forbid(unsafe_code)]

mod admission;
mod candidate;
mod error;
mod placement;
mod scheduler;
mod score;
#[cfg(test)]
mod test_support;
mod watermark;

pub use candidate::WorkerCandidate;
pub use error::ScheduleError;
pub use placement::{AdmissionDecision, PlacementDecision, PlacementRequest};
pub use scheduler::Scheduler;
pub use score::{hits_packing_target, packing_score};
pub use watermark::{EMERGENCY_PERCENT, PACKING_TARGET_MID, STOP_NEW_PLACEMENT_PERCENT};
