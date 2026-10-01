//! 冻结的调度水位与 Failover Reserve 语义（架构 §9 / §15.5）。
//!
//! ```text
//! Resource Budget Packing
//! Target      = 70% ~ 75%
//! Stop Admit  = 80%
//! Emergency   = 90%
//! Failover Reserve >= max(1 Worker, 20% effective capacity)
//! ```
//!
//! 这些数值属于冻结语义，实现与运维默认值都不得调整；Scheduler 只能在此之上做启发式打分。

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::resources::{scale_percent, Utilization};

/// Packing 目标区间下沿（70%）。
pub const PACKING_TARGET_MIN: f64 = 0.70;
/// Packing 目标区间上沿（75%）。
pub const PACKING_TARGET_MAX: f64 = 0.75;
/// 停止新 placement 水位（80%）：达到即不再接受新的 DB，只做回收。
pub const STOP_NEW_PLACEMENT: f64 = 0.80;
/// 紧急保护水位（90%）：立即回收低价值 WARM DB，保护在跑的 HOT DB。
pub const EMERGENCY: f64 = 0.90;
/// Failover Reserve 下限（有效容量的 20%），与 [`FAILOVER_RESERVE_PERCENT`] 同义。
pub const FAILOVER_RESERVE_MIN_FRACTION: f64 = 0.20;
/// 计算 Failover Reserve 时使用的百分比（整数运算路径）。
pub const FAILOVER_RESERVE_PERCENT: u64 = 20;
/// 单 Worker DB 进程数的默认 hard safety limit。
///
/// DB 数量**不是**主要 Placement 指标（§9 / §15.5），它只在容量未显式配置时
/// 充当兜底的安全上限，避免单机进程数失控。
pub const MAX_DB_PROCESS_PER_WORKER_DEFAULT: u64 = 256;

/// Placement 放行结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlacementGate {
    /// 可放置（低于 80% 水位）。
    Allow,
    /// 停止新 placement（80% ~ 90%）：只服务既有 DB，不再新增。
    StopNewPlacement,
    /// 紧急保护（>= 90%）：需立即回收低价值 WARM DB。
    Emergency,
}

impl PlacementGate {
    /// 是否允许新 placement（只有 [`PlacementGate::Allow`] 允许）。
    #[must_use]
    pub const fn allows_new_placement(&self) -> bool {
        matches!(self, PlacementGate::Allow)
    }

    /// 是否处于紧急水位。
    #[must_use]
    pub const fn is_emergency(&self) -> bool {
        matches!(self, PlacementGate::Emergency)
    }

    /// 监控 / 日志标签字符串。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            PlacementGate::Allow => "ALLOW",
            PlacementGate::StopNewPlacement => "STOP_NEW_PLACEMENT",
            PlacementGate::Emergency => "EMERGENCY",
        }
    }
}

impl fmt::Display for PlacementGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for PlacementGate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PlacementGate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(match raw.as_str() {
            "ALLOW" => PlacementGate::Allow,
            "STOP_NEW_PLACEMENT" => PlacementGate::StopNewPlacement,
            // 未知取值按最保守处理：等价于紧急水位
            _ => PlacementGate::Emergency,
        })
    }
}

/// 依据利用率判定 Placement 是否放行。
///
/// 边界取“达到即生效”：`0.80` 判为 `StopNewPlacement`，`0.90` 判为 `Emergency`；
/// NaN 视为紧急（防御性，正常路径不会产生 NaN，见 `Utilization`）。
#[must_use]
pub fn placement_gate(utilization: f64) -> PlacementGate {
    if utilization.is_nan() || utilization >= EMERGENCY {
        PlacementGate::Emergency
    } else if utilization >= STOP_NEW_PLACEMENT {
        PlacementGate::StopNewPlacement
    } else {
        PlacementGate::Allow
    }
}

/// [`placement_gate`] 的便捷入口：以六维最大利用率判定。
#[must_use]
pub fn placement_gate_for_utilization(utilization: &Utilization) -> PlacementGate {
    placement_gate(utilization.max())
}

/// 利用率是否落在 Packing 目标区间 `[70%, 75%]` 内。
#[must_use]
pub fn within_packing_target(utilization: f64) -> bool {
    (PACKING_TARGET_MIN..=PACKING_TARGET_MAX).contains(&utilization)
}

/// 集群必须保留的 Failover 容量。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FailoverReserve {
    /// 保留 CPU（milli-core）。
    pub cpu_milli: u64,
    /// 保留内存（MiB）。
    pub memory_mib: u64,
}

impl FailoverReserve {
    /// 构造保留量。
    #[must_use]
    pub const fn new(cpu_milli: u64, memory_mib: u64) -> Self {
        Self {
            cpu_milli,
            memory_mib,
        }
    }

    /// 是否为零保留（空集群）。
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.cpu_milli == 0 && self.memory_mib == 0
    }
}

/// 计算集群必须保留的 Failover 容量：`>= max(1 个整 Worker, 20% 总有效容量)`（架构 §15.5 / §9）。
///
/// **「1 个整 Worker」= 最大单机容量**，不是集群均值：集群通常是异构的（机型 / 预留不一），
/// `total / worker_count` 在最大那台机器挂掉时接不住它的 DB ——
/// 例：CPU 40/20/20/20 的集群均值为 25，而最大单机是 40，按均值保留会在 failover 时缺容量。
/// 调用方从集群容量列表里按维度取最大值传入 `max_worker_*`（两个维度独立取 max，
/// 因此 reserve 至少能整体接住集群里最大的那台 Worker；这是保守且正确的取法）。
///
/// 20% 份额按「先乘后除」计算（见 [`crate::resources::scale_percent`]），
/// 避免 `total / 100 * 20` 把余数截掉（9999 必须得到 1999，而不是 1980）。
///
/// 边界：空集群（全 0）reserve 为 0；`total` 非 0 但 `max_worker_*` 传 0 时退化为 20%
/// —— 调用方漏传时只会保留得偏少，而不会静默放大成「保留全部容量」把集群锁死。
#[must_use]
pub fn failover_reserve_required(
    total_cpu_milli: u64,
    total_memory_mib: u64,
    max_worker_cpu_milli: u64,
    max_worker_memory_mib: u64,
) -> FailoverReserve {
    FailoverReserve {
        cpu_milli: max_worker_cpu_milli
            .max(scale_percent(total_cpu_milli, FAILOVER_RESERVE_PERCENT)),
        memory_mib: max_worker_memory_mib
            .max(scale_percent(total_memory_mib, FAILOVER_RESERVE_PERCENT)),
    }
}

/// **已废弃**：旧签名的 [`failover_reserve_required`]，假定集群**同构**（每台 Worker 容量相同），
/// 用 `total / worker_count` 近似「1 个整 Worker」。
///
/// 异构集群下均值必然低估最大的那台机器（见 [`failover_reserve_required`] 的说明），
/// 因此新代码请改用 `failover_reserve_required(total_*, max_worker_*)`；
/// 本函数只为兼容旧调用点保留，行为与旧实现完全一致。
///
/// `worker_count == 0` 时把全部容量视为保留（没有 Worker 就没有可放置空间，保守处理）。
/// 注意：单 Worker 集群的保留量等于全部容量，即**不满足 Reserve 要求**，
/// 因此生产集群至少需要 2 个 Worker；dev 环境如需放松应显式走运维开关，而不是改这里。
#[must_use]
pub fn failover_reserve_required_homogeneous(
    total_cpu_milli: u64,
    total_memory_mib: u64,
    worker_count: u64,
) -> FailoverReserve {
    // 无 Worker：没有可放置空间，全部容量视为保留（保守）；checked_div 避免除零。
    let (one_worker_cpu, one_worker_memory) = if worker_count == 0 {
        (total_cpu_milli, total_memory_mib)
    } else {
        (
            total_cpu_milli
                .checked_div(worker_count)
                .unwrap_or(total_cpu_milli),
            total_memory_mib
                .checked_div(worker_count)
                .unwrap_or(total_memory_mib),
        )
    };
    failover_reserve_required(
        total_cpu_milli,
        total_memory_mib,
        one_worker_cpu,
        one_worker_memory,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{ResourceBudget, WorkerCapacity, WorkerResourceUsage};

    #[test]
    fn frozen_watermarks_are_not_adjustable() {
        assert_eq!(PACKING_TARGET_MIN, 0.70);
        assert_eq!(PACKING_TARGET_MAX, 0.75);
        assert_eq!(STOP_NEW_PLACEMENT, 0.80);
        assert_eq!(EMERGENCY, 0.90);
        assert_eq!(FAILOVER_RESERVE_PERCENT, 20);
        assert!((FAILOVER_RESERVE_MIN_FRACTION - 0.20).abs() < f64::EPSILON);
    }

    #[test]
    fn placement_gate_boundaries() {
        assert_eq!(placement_gate(0.74), PlacementGate::Allow);
        assert_eq!(placement_gate(0.80), PlacementGate::StopNewPlacement);
        assert_eq!(placement_gate(0.90), PlacementGate::Emergency);

        // 边界：区间取“达到即生效”
        assert_eq!(placement_gate(0.0), PlacementGate::Allow);
        assert_eq!(placement_gate(0.70), PlacementGate::Allow);
        assert_eq!(placement_gate(0.75), PlacementGate::Allow);
        assert_eq!(placement_gate(0.7999), PlacementGate::Allow);
        assert_eq!(placement_gate(0.8999), PlacementGate::StopNewPlacement);
        assert_eq!(placement_gate(1.0), PlacementGate::Emergency);
        assert_eq!(placement_gate(3.5), PlacementGate::Emergency);
        assert_eq!(placement_gate(f64::NAN), PlacementGate::Emergency);
    }

    #[test]
    fn placement_gate_from_utilization_uses_max_dimension() {
        let capacity = WorkerCapacity::new(8000, 16384, 4096, 102400, 256, 20000);
        let used = ResourceBudget::new(6400, 100, 100, 100, 1, 100);
        let usage = WorkerResourceUsage::new(capacity, used);
        assert!((usage.max_utilization() - 0.8).abs() < 1e-12);
        assert_eq!(
            placement_gate_for_utilization(&usage.utilization()),
            PlacementGate::StopNewPlacement
        );
        assert!(!placement_gate_for_utilization(&usage.utilization()).allows_new_placement());
        assert!(PlacementGate::Allow.allows_new_placement());
        assert!(PlacementGate::Emergency.is_emergency());
        assert_eq!(placement_gate(0.74).to_string(), "ALLOW");
    }

    #[test]
    fn packing_target_window() {
        assert!(within_packing_target(0.70));
        assert!(within_packing_target(0.725));
        assert!(within_packing_target(0.75));
        assert!(!within_packing_target(0.69));
        assert!(!within_packing_target(0.76));
    }

    #[test]
    fn failover_reserve_takes_max_of_one_worker_and_twenty_percent() {
        // 4 个 Worker（同构 2000/4000）：单机容量大于 20%（1600/3200），取单机值
        let reserve = failover_reserve_required(8000, 16000, 2000, 4000);
        assert_eq!(reserve, FailoverReserve::new(2000, 4000));

        // 20 个 Worker（同构 500/1000）：单机容量小于 20%（2000/4000），取 20%
        let reserve = failover_reserve_required(10000, 20000, 500, 1000);
        assert_eq!(reserve, FailoverReserve::new(2000, 4000));

        // 2 个 Worker：单机容量与 20% 相同时仍满足
        let reserve = failover_reserve_required(10000, 20000, 5000, 10000);
        assert_eq!(reserve, FailoverReserve::new(5000, 10000));

        // 单 Worker：保留量等于全容量 => 不满足自身 Reserve，无法再做新 placement
        let reserve = failover_reserve_required(8000, 16000, 8000, 16000);
        assert_eq!(reserve, FailoverReserve::new(8000, 16000));

        // 边界：空集群
        assert!(failover_reserve_required(0, 0, 0, 0).is_zero());
        assert_eq!(
            failover_reserve_required(0, 0, 0, 0),
            FailoverReserve::new(0, 0)
        );

        // 调用方漏传最大单机容量（total 非 0 但 max=0）：退化为 20%，而不是锁死整个集群
        assert_eq!(
            failover_reserve_required(8000, 16000, 0, 0),
            FailoverReserve::new(1600, 3200)
        );
    }

    /// FIX-3：异构集群必须按**最大单机容量**保留，而不是按均值。
    #[test]
    fn failover_reserve_uses_max_worker_in_heterogeneous_cluster() {
        // CPU 40/20/20/20（milli-core，合计 10000），内存 8000/4000/4000/4000（合计 20000）
        let cluster = [
            WorkerCapacity::new(4000, 8000, 0, 0, 0, 0),
            WorkerCapacity::new(2000, 4000, 0, 0, 0, 0),
            WorkerCapacity::new(2000, 4000, 0, 0, 0, 0),
            WorkerCapacity::new(2000, 4000, 0, 0, 0, 0),
        ];
        let total_cpu: u64 = cluster.iter().map(|c| c.cpu_milli).sum();
        let total_memory: u64 = cluster.iter().map(|c| c.memory_mib).sum();
        let max_worker_cpu = cluster.iter().map(|c| c.cpu_milli).max().unwrap();
        let max_worker_memory = cluster.iter().map(|c| c.memory_mib).max().unwrap();
        assert_eq!((total_cpu, total_memory), (10000, 20000));
        assert_eq!((max_worker_cpu, max_worker_memory), (4000, 8000));

        // 20% 只有 2000/4000，必须被最大单机容量（4000/8000）抬上去
        let reserve =
            failover_reserve_required(total_cpu, total_memory, max_worker_cpu, max_worker_memory);
        assert_eq!(
            reserve,
            FailoverReserve::new(4000, 8000),
            "异构集群必须按最大单机保留，才能接住最大那台的 DB"
        );

        // 旧（同构假设）签名的结果 2500/5000 小于最大单机容量：
        // 最大的 Worker 一旦失效，reserve 不足以接管它的负载 —— 这正是 FIX-3 修掉的行为。
        let homogeneous = failover_reserve_required_homogeneous(total_cpu, total_memory, 4);
        assert_eq!(homogeneous, FailoverReserve::new(2500, 5000));
        assert!(
            homogeneous.cpu_milli < max_worker_cpu && homogeneous.memory_mib < max_worker_memory,
            "均值路径必然低估最大单机容量"
        );
    }

    /// FIX-2 在 reserve 路径上的表现：20% 份额不得因「先除后乘」被截断。
    #[test]
    fn failover_reserve_twenty_percent_is_exact() {
        // 9999 的 20% = 1999.8 -> 1999；旧实现 9999/100*20 只有 1980
        assert_eq!(
            failover_reserve_required(9999, 9999, 0, 0),
            FailoverReserve::new(1999, 1999)
        );

        // 小于 100 的总量不得被抹成 0 保留
        let reserve = failover_reserve_required(99, 99, 0, 0);
        assert_eq!(reserve, FailoverReserve::new(19, 19));
        assert!(!reserve.is_zero(), "小集群的 reserve 不得静默为 0");
    }

    /// 旧签名（同构假设）与旧实现逐值等价：改名不等于改行为，兼容调用点不受影响。
    #[test]
    fn homogeneous_legacy_signature_keeps_previous_behavior() {
        // 4 个 Worker：均值 2000/4000 大于 20%（1600/3200）
        assert_eq!(
            failover_reserve_required_homogeneous(8000, 16000, 4),
            FailoverReserve::new(2000, 4000)
        );
        // 20 个 Worker：均值 500/1000 小于 20%（2000/4000）
        assert_eq!(
            failover_reserve_required_homogeneous(10000, 20000, 20),
            FailoverReserve::new(2000, 4000)
        );
        // 单 Worker：保留量等于全部容量
        assert_eq!(
            failover_reserve_required_homogeneous(8000, 16000, 1),
            FailoverReserve::new(8000, 16000)
        );
        // 空集群
        assert!(failover_reserve_required_homogeneous(0, 0, 0).is_zero());
        assert_eq!(
            failover_reserve_required_homogeneous(0, 0, 3),
            FailoverReserve::new(0, 0)
        );
        // worker_count=0 但总容量非 0 —— 全部视为保留（保守）
        assert_eq!(
            failover_reserve_required_homogeneous(4000, 8000, 0),
            FailoverReserve::new(4000, 8000)
        );
    }

    #[test]
    fn failover_reserve_never_below_twenty_percent() {
        for (total_cpu, total_mem, workers) in [
            (8000u64, 16000u64, 4u64),
            (10000, 20000, 20),
            (64000, 131072, 8),
            (1000, 2000, 3),
        ] {
            let reserve = failover_reserve_required_homogeneous(total_cpu, total_mem, workers);
            // 与精确值比较：先乘后除，允许的只有整数向下取整的那一点差
            assert!(
                reserve.cpu_milli >= scale_percent(total_cpu, FAILOVER_RESERVE_PERCENT),
                "CPU 保留量低于 20%"
            );
            assert!(
                reserve.memory_mib >= scale_percent(total_mem, FAILOVER_RESERVE_PERCENT),
                "内存保留量低于 20%"
            );
            assert!(
                reserve.cpu_milli >= total_cpu / workers,
                "CPU 保留量低于 1 个整 Worker"
            );
        }
    }

    #[test]
    fn gate_serde_is_stable() {
        assert_eq!(
            serde_json::to_string(&PlacementGate::StopNewPlacement).unwrap(),
            "\"STOP_NEW_PLACEMENT\""
        );
        let parsed: PlacementGate = serde_json::from_str("\"ALLOW\"").unwrap();
        assert_eq!(parsed, PlacementGate::Allow);
        let parsed: PlacementGate = serde_json::from_str("\"WHATEVER\"").unwrap();
        assert_eq!(parsed, PlacementGate::Emergency);
    }
}
