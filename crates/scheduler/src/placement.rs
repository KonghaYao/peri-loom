//! Placement 请求 / 结论类型。

use domain::policy::FailoverReserve;
use domain::{DatabaseId, ResourceBudget, WorkerId};
use serde::{Deserialize, Serialize};

/// 一次 placement 请求（架构 §4.4 Scheduler 输入）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementRequest {
    /// 待放置的数据库。
    pub database_id: DatabaseId,
    /// DB 资源画像（必须带 `process_slots >= 1`：一个 DB = 一个进程）。
    pub budget: ResourceBudget,
    /// 业务优先级：越大越高（当前只用于日志与上层排序，不改变水位的硬约束）。
    pub priority: i32,
    /// 目标区域（`Some` 时是硬约束，不允许跨 region 放置）。
    pub region: Option<String>,
    /// 目标可用区（`Some` 时是硬约束）。
    pub zone: Option<String>,
    /// 亲和 Worker（最高优先级的软偏好：可用就选它，不可用则退化为普通打分）。
    pub affinity_worker: Option<WorkerId>,
    /// 反亲和 Worker（硬约束：绝不放在同一个 Worker 上）。
    pub anti_affinity_worker: Option<WorkerId>,
    /// 放置后集群必须继续保留的 failover 容量。
    ///
    /// 零值表示本次调用不做 reserve 约束（例如单 Worker 开发环境）。
    pub reserved_for_failover_required: FailoverReserve,
    /// 明确排除的 Worker（例如该 DB 刚从这里失败退出）。
    pub exclude: Vec<WorkerId>,
}

impl PlacementRequest {
    /// 构造最小请求：只指定 DB 与资源预算，其余约束留空。
    #[must_use]
    pub fn new(database_id: DatabaseId, budget: ResourceBudget) -> Self {
        Self {
            database_id,
            budget,
            priority: 0,
            region: None,
            zone: None,
            affinity_worker: None,
            anti_affinity_worker: None,
            reserved_for_failover_required: FailoverReserve::default(),
            exclude: Vec::new(),
        }
    }

    /// 是否需要保留 failover 容量。
    #[must_use]
    pub fn requires_failover_reserve(&self) -> bool {
        !self.reserved_for_failover_required.is_zero()
    }

    /// 该 Worker 是否被显式排除。
    #[must_use]
    pub fn is_excluded(&self, worker_id: &WorkerId) -> bool {
        self.exclude.iter().any(|id| id == worker_id)
    }
}

/// 准入判定结论（架构 §9 `CanStart` + 水位）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdmissionDecision {
    /// 可以启动。
    Allow,
    /// 该 Worker 不再接收新 DB：资源不足、DB 数达 hard safety limit，
    /// 或放置后会越过 80% 停止线（只服务已有 DB，等待回收）。
    StopNewPlacement {
        /// 具体原因（可直接进日志 / 错误消息）。
        reason: String,
    },
    /// 紧急保护：Worker 已在 90% 水位之上，或放置后会突破 90%，绝不放行。
    Emergency {
        /// 具体原因。
        reason: String,
    },
}

impl AdmissionDecision {
    /// 是否放行。
    #[must_use]
    pub fn allows(&self) -> bool {
        matches!(self, AdmissionDecision::Allow)
    }

    /// 是否处于紧急保护。
    #[must_use]
    pub fn is_emergency(&self) -> bool {
        matches!(self, AdmissionDecision::Emergency { .. })
    }

    /// 拒绝原因（放行时为 `None`）。
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            AdmissionDecision::Allow => None,
            AdmissionDecision::StopNewPlacement { reason }
            | AdmissionDecision::Emergency { reason } => Some(reason),
        }
    }

    /// 指标 / 日志标签。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            AdmissionDecision::Allow => "ALLOW",
            AdmissionDecision::StopNewPlacement { .. } => "STOP_NEW_PLACEMENT",
            AdmissionDecision::Emergency { .. } => "EMERGENCY",
        }
    }
}

/// Placement 结论。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlacementDecision {
    /// 选中的 Worker。
    pub worker_id: WorkerId,
    /// 打分（越大越好，取值 `0.0 ~ 1.0`；见 [`crate::packing_score`]）。
    pub score: f64,
    /// 放置后的资源压力：CPU / Memory / FD / Disk / IOPS 的最大利用率。
    ///
    /// 口径与水位判定一致（DB count 维不参与，见 `WorkerCandidate::pressure_after`）。
    pub utilization_after: f64,
    /// 人类可读的选择理由（含打包命中情况与集群余量，便于审计 placement 决策）。
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_request_has_no_constraints() {
        let request = PlacementRequest::new(
            DatabaseId::new_v7(),
            ResourceBudget::new(500, 256, 64, 512, 1, 1000),
        );
        assert!(!request.requires_failover_reserve());
        assert_eq!(request.priority, 0);
        assert!(request.region.is_none());
        assert!(!request.is_excluded(&WorkerId::new("w1")));

        let mut with_exclude = request.clone();
        with_exclude.exclude.push(WorkerId::new("w1"));
        assert!(with_exclude.is_excluded(&WorkerId::new("w1")));

        let mut reserved = request;
        reserved.reserved_for_failover_required = FailoverReserve::new(2000, 4096);
        assert!(reserved.requires_failover_reserve());
    }

    #[test]
    fn admission_decision_helpers() {
        assert!(AdmissionDecision::Allow.allows());
        assert!(!AdmissionDecision::Allow.is_emergency());
        assert_eq!(AdmissionDecision::Allow.reason(), None);
        assert_eq!(AdmissionDecision::Allow.as_str(), "ALLOW");

        let stop = AdmissionDecision::StopNewPlacement {
            reason: "放置后 0.81 越过停止线".to_string(),
        };
        assert!(!stop.allows());
        assert!(!stop.is_emergency());
        assert_eq!(stop.reason(), Some("放置后 0.81 越过停止线"));
        assert_eq!(stop.as_str(), "STOP_NEW_PLACEMENT");

        let emergency = AdmissionDecision::Emergency {
            reason: "已在 0.95".to_string(),
        };
        assert!(emergency.is_emergency());
        assert_eq!(emergency.as_str(), "EMERGENCY");
    }
}
