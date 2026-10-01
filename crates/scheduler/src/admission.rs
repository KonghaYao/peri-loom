//! 准入判定（架构 §9 `CanStart` + 冻结水位）。
//!
//! 判定顺序（越靠前越精确，先给出最可诊断的结论）：
//!
//! ```text
//! 1. Resource      CPU / Memory / FD / Disk 中任一维装不下 -> 停止新 placement
//! 2. ProcessLimit  DB 进程数超过 hard safety limit         -> 停止新 placement
//! 3. Emergency     放置后资源压力 >= 90%                   -> 绝不放置
//! 4. Waterline     放置后资源压力 >= 80%                   -> 停止新 placement
//! 5. Allow
//! ```
//!
//! 两个关键口径（§9 / §15.5）：
//! - IOPS **不在** `CanStart` 公式里（§9 明确列出 CPU / Memory / FD / Disk / Process Count），
//!   所以缺 IOPS 不会阻塞启动；但 IOPS 参与水位判定。
//! - 水位用的是 [`WorkerCandidate::pressure_after`]：CPU / Memory / FD / Disk / IOPS 的最大值，
//!   **不含 DB count 维** —— DB count 只作 hard safety limit，不作为主指标
//!   （因此这里不使用 `domain::policy::placement_gate_for_utilization`，它的口径包含进程维）。

use domain::policy::{placement_gate, PlacementGate};
use domain::{ResourceBudget, WorkerCapacity};

use crate::candidate::WorkerCandidate;
use crate::placement::AdmissionDecision;
use crate::watermark::{EMERGENCY_PERCENT, STOP_NEW_PLACEMENT_PERCENT};

/// `CanStart` 五维中缺少的那一维。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShortDimension {
    Cpu,
    Memory,
    FileDescriptors,
    Disk,
}

impl ShortDimension {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            ShortDimension::Cpu => "cpu_milli",
            ShortDimension::Memory => "memory_mib",
            ShortDimension::FileDescriptors => "file_descriptors",
            ShortDimension::Disk => "disk_mib",
        }
    }
}

/// 内部判定结论。
///
/// 比对外 [`AdmissionDecision`] 更细：拆开「资源装不下」与「越过水位」两类，
/// 让 failover 路径只放行水位那一类（reserve 就是用来吃 80% ~ 90% 这段容量的）。
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Verdict {
    /// 放行。
    Allow,
    /// 放置后会越过 80% 停止线（仍低于 90%）。
    Waterline {
        /// 放置后的资源压力。
        pressure_after: f64,
    },
    /// 某一维装不下。
    Resource {
        /// 缺少的维度。
        dimension: ShortDimension,
        /// 本次需求。
        need: u64,
        /// 放置前该维剩余量。
        available: u64,
    },
    /// DB 进程数达 hard safety limit。
    ProcessLimit {
        /// 当前进程数。
        current: u64,
        /// 本次需求。
        need: u64,
        /// 生效上限。
        limit: u64,
    },
    /// 放置后（或当前）资源压力已达 90% 水位。
    Emergency {
        /// 放置后的资源压力。
        pressure_after: f64,
    },
}

impl Verdict {
    pub(crate) fn allows(&self) -> bool {
        matches!(self, Verdict::Allow)
    }

    pub(crate) fn is_emergency(&self) -> bool {
        matches!(self, Verdict::Emergency { .. })
    }

    /// 拒绝原因（放行时返回 `None`）。
    pub(crate) fn reason(&self) -> Option<String> {
        match self {
            Verdict::Allow => None,
            Verdict::Waterline { pressure_after } => Some(format!(
                "放置后资源压力 {pressure_after:.4} 越过 {STOP_NEW_PLACEMENT_PERCENT}% 停止线"
            )),
            Verdict::Resource {
                dimension,
                need,
                available,
            } => Some(format!(
                "{} 不足：需要 {need}，放置前仅剩 {available}",
                dimension.as_str()
            )),
            Verdict::ProcessLimit {
                current,
                need,
                limit,
            } => Some(format!(
                "DB 进程数达到 hard safety limit：{current} + {need} > {limit}"
            )),
            Verdict::Emergency { pressure_after } => Some(format!(
                "放置后资源压力 {pressure_after:.4} 达到 {EMERGENCY_PERCENT}% 紧急水位"
            )),
        }
    }
}

/// 判定一个候选能否承载 `budget`。
///
/// `budget` 应已归一化（`process_slots >= 1`，见 `Scheduler::normalize_budget`）。
pub(crate) fn verdict(candidate: &WorkerCandidate, budget: &ResourceBudget) -> Verdict {
    let pressure_after = candidate.pressure_after(budget);
    let capacity = candidate.effective_capacity();
    let used_after = candidate.used().saturating_add(budget);

    // 1) 五维 CanStart 中的前四维：装不下就先说清楚缺哪一维
    if let Some((dimension, need, available)) = first_shortfall(&capacity, budget, &used_after) {
        return Verdict::Resource {
            dimension,
            need,
            available,
        };
    }

    // 2) DB count 只作 hard safety limit（§9）：不是主指标，但绝不允许被绕过
    let current = candidate.effective_db_process_count();
    let after = current.saturating_add(budget.process_slots);
    if after > capacity.process_slots {
        return Verdict::ProcessLimit {
            current,
            need: budget.process_slots,
            limit: capacity.process_slots,
        };
    }

    // 3) 紧急保护：>= 90% 绝不放置（边界取「达到即生效」）
    if placement_gate(pressure_after).is_emergency() {
        return Verdict::Emergency { pressure_after };
    }

    // 4) 停止新 placement：>= 80%
    if !placement_gate(pressure_after).allows_new_placement() {
        return Verdict::Waterline { pressure_after };
    }
    debug_assert_eq!(placement_gate(pressure_after), PlacementGate::Allow);
    Verdict::Allow
}

/// 前四维（CPU / Memory / FD / Disk）中第一个装不下的维度。
///
/// 返回 `(维度, 本次需求, 放置前的剩余量)`，用于生成可诊断的原因。
fn first_shortfall(
    capacity: &WorkerCapacity,
    need: &ResourceBudget,
    used_after: &ResourceBudget,
) -> Option<(ShortDimension, u64, u64)> {
    let checks = [
        (
            ShortDimension::Cpu,
            need.cpu_milli,
            used_after.cpu_milli,
            capacity.cpu_milli,
        ),
        (
            ShortDimension::Memory,
            need.memory_mib,
            used_after.memory_mib,
            capacity.memory_mib,
        ),
        (
            ShortDimension::FileDescriptors,
            need.file_descriptors,
            used_after.file_descriptors,
            capacity.file_descriptors,
        ),
        (
            ShortDimension::Disk,
            need.disk_mib,
            used_after.disk_mib,
            capacity.disk_mib,
        ),
    ];
    for (dimension, need_value, used_value, capacity_value) in checks {
        if used_value > capacity_value {
            let used_before = used_value.saturating_sub(need_value);
            return Some((
                dimension,
                need_value,
                capacity_value.saturating_sub(used_before),
            ));
        }
    }
    None
}

/// 把内部结论映射为对外结论。
///
/// 对调用方而言「资源不足」与「越过水位」的处置相同（该 Worker 不再接收新 DB），
/// 因此统一归入 [`AdmissionDecision::StopNewPlacement`]，只在 reason 里区分。
pub(crate) fn to_admission_decision(verdict: &Verdict) -> AdmissionDecision {
    if verdict.allows() {
        return AdmissionDecision::Allow;
    }
    let reason = verdict.reason().unwrap_or_else(|| "拒绝".to_string());
    if verdict.is_emergency() {
        AdmissionDecision::Emergency { reason }
    } else {
        AdmissionDecision::StopNewPlacement { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{candidate, db_budget, memory_candidate};
    use domain::policy::MAX_DB_PROCESS_PER_WORKER_DEFAULT;
    use domain::WorkerState;

    #[test]
    fn verdict_allows_a_comfortable_worker() {
        // 1600/16000 = 0.10，放置 200 MiB 后 0.1125
        let c = memory_candidate("w1", 1600);
        assert_eq!(verdict(&c, &db_budget(200)), Verdict::Allow);
        assert!(to_admission_decision(&verdict(&c, &db_budget(200))).allows());
    }

    #[test]
    fn waterline_boundaries_are_inclusive() {
        // 恰好落在 80%：>= 0.80 即停止新 placement
        let at_80 = memory_candidate("w1", 12_600); // +200 MiB -> 12800/16000 = 0.80
        assert_eq!(
            verdict(&at_80, &db_budget(200)),
            Verdict::Waterline {
                pressure_after: 0.80
            }
        );
        // 略低于 80% 仍可放置
        let below = memory_candidate("w1", 12_500); // +200 -> 12700/16000 = 0.79375
        assert_eq!(verdict(&below, &db_budget(200)), Verdict::Allow);

        // 恰好 90%：紧急保护
        let at_90 = memory_candidate("w1", 14_200); // +200 -> 14400/16000 = 0.90
        assert_eq!(
            verdict(&at_90, &db_budget(200)),
            Verdict::Emergency {
                pressure_after: 0.90
            }
        );
        // 已在 90% 之上：即使零预算也拒绝
        let above = memory_candidate("w1", 15_000);
        assert!(verdict(&above, &ResourceBudget::ZERO).is_emergency());
        assert!(verdict(&above, &db_budget(0))
            .reason()
            .unwrap()
            .contains("紧急水位"));
    }

    #[test]
    fn resource_shortfall_reports_the_dimension() {
        // 容量 4096 FD、已用 100，请求 5000 FD：装不下
        let c = candidate("w1", 100, 100);
        let budget = ResourceBudget::new(10, 10, 5000, 10, 1, 10);
        let checked = verdict(&c, &budget);
        assert_eq!(
            checked,
            Verdict::Resource {
                dimension: ShortDimension::FileDescriptors,
                need: 5000,
                available: 3996,
            }
        );
        assert!(checked.reason().unwrap().contains("file_descriptors"));

        // 内存维不足（内存总量 16384，已用 16000，请求 400）
        let tight_memory = candidate("w2", 100, 16_000);
        let checked = verdict(&tight_memory, &ResourceBudget::new(10, 400, 1, 1, 1, 1));
        assert_eq!(
            checked,
            Verdict::Resource {
                dimension: ShortDimension::Memory,
                need: 400,
                available: 384,
            }
        );
    }

    #[test]
    fn iops_is_not_part_of_can_start() {
        // iops_total 未上报（0）：再大的 IOPS 需求也不阻塞启动
        let mut c = memory_candidate("w1", 1600);
        c.capacity.iops = 0;
        c.usage.iops_total = 0;
        let budget = ResourceBudget::new(100, 200, 16, 64, 1, 1_000_000);
        assert_eq!(verdict(&c, &budget), Verdict::Allow);

        // 但 IOPS 利用率越过 90% 时仍会被紧急水位拦住（水位是资源压力口径）
        let mut hot = memory_candidate("w1", 1600);
        hot.capacity.iops = 1000;
        hot.usage.iops_total = 1000;
        hot.usage.iops_used = 950;
        let checked = verdict(&hot, &db_budget(0));
        assert!(checked.is_emergency(), "IOPS 超卖必须在紧急水位上拦住");
        assert!(checked.reason().unwrap().contains("紧急水位"));
    }

    #[test]
    fn db_count_is_a_hard_safety_limit() {
        let mut c = memory_candidate("w1", 1600);
        c.capacity.process_slots = 8;
        c.usage.db_process_limit = 8;
        c.usage.db_process_count = 7;
        c.db_process_count = 7;
        assert_eq!(verdict(&c, &db_budget(10)), Verdict::Allow);

        c.db_process_count = 8;
        let checked = verdict(&c, &db_budget(10));
        assert_eq!(
            checked,
            Verdict::ProcessLimit {
                current: 8,
                need: 1,
                limit: 8
            }
        );
        assert!(checked.reason().unwrap().contains("hard safety limit"));
        assert!(!checked.allows());
    }

    #[test]
    fn db_count_alone_never_trips_the_waterline() {
        // 进程位占满（128/128）但 CPU/内存压力很低：不是紧急水位，只是「满了」
        let mut c = memory_candidate("w1", 1600);
        c.usage.db_process_count = 127;
        c.db_process_count = 127;
        assert_eq!(
            verdict(&c, &db_budget(10)),
            Verdict::Allow,
            "DB count 不是主指标，占满前不应触发水位"
        );
        c.usage.db_process_count = 128;
        c.db_process_count = 128;
        assert!(matches!(
            verdict(&c, &db_budget(10)),
            Verdict::ProcessLimit { .. }
        ));
    }

    #[test]
    fn unset_process_limit_falls_back_to_default() {
        let mut c = memory_candidate("w1", 1600);
        c.capacity.process_slots = 0;
        c.usage.db_process_limit = 0;
        c.db_process_count = MAX_DB_PROCESS_PER_WORKER_DEFAULT;
        c.usage.db_process_count = MAX_DB_PROCESS_PER_WORKER_DEFAULT;
        assert!(matches!(
            verdict(&c, &db_budget(10)),
            Verdict::ProcessLimit { .. }
        ));
    }

    #[test]
    fn emergency_is_reported_when_capacity_is_sufficient() {
        // 资源装得下，但放置后越过 90%：必须报紧急而不是资源不足
        let c = memory_candidate("w1", 14_200);
        let checked = verdict(&c, &db_budget(200));
        assert!(checked.is_emergency());
    }

    #[test]
    fn admission_check_maps_verdicts() {
        use crate::Scheduler;

        let scheduler = Scheduler::new();
        assert!(scheduler
            .admission_check(&memory_candidate("w1", 1600), &db_budget(200))
            .allows());

        let stop = scheduler.admission_check(&memory_candidate("w1", 12_600), &db_budget(200));
        assert_eq!(stop.as_str(), "STOP_NEW_PLACEMENT");
        assert!(stop.reason().unwrap().contains("停止线"));

        let emergency = scheduler.admission_check(&memory_candidate("w1", 15_000), &db_budget(200));
        assert!(emergency.is_emergency());
        assert!(emergency.reason().unwrap().contains("紧急水位"));

        // 状态与 respawning 不属于 admission 的职责（由 select 过滤），这里只看资源与水位
        let mut suspect = memory_candidate("w1", 1600);
        suspect.state = WorkerState::Suspect;
        assert!(scheduler
            .admission_check(&suspect, &db_budget(200))
            .allows());
    }

    #[test]
    fn admission_check_normalizes_a_zero_process_slot_budget() {
        use crate::Scheduler;

        // 调用方漏填 process_slots 时按 1 记账（一个 DB = 一个进程），
        // 不允许用 0 绕过 DB count hard limit。
        let scheduler = Scheduler::new();
        let mut c = memory_candidate("w1", 1600);
        c.capacity.process_slots = 4;
        c.usage.db_process_limit = 4;
        c.db_process_count = 4;
        c.usage.db_process_count = 4;
        let decision = scheduler.admission_check(&c, &ResourceBudget::new(10, 10, 1, 1, 0, 1));
        assert!(
            matches!(decision, AdmissionDecision::StopNewPlacement { .. }),
            "进程数已满时漏填 process_slots 也必须被拦住，实际 {decision:?}"
        );
    }
}
