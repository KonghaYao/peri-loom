//! Worker 候选快照：Scheduler 的输入单元。
//!
//! 候选由 Control Plane 从 Worker inventory（`workers` 表 + 最近心跳）组装。
//! 这里刻意保留 `capacity` 与 `usage` 两份数据（分别来自「声明容量」与「最近上报」），
//! 因为两者可能短暂不一致，而 placement 必须在这段时间里仍然安全。

use domain::policy::MAX_DB_PROCESS_PER_WORKER_DEFAULT;
use domain::records::WorkerRecord;
use domain::{
    ResourceBudget, Utilization, WorkerCapacity, WorkerId, WorkerResourceUsage, WorkerState,
};
use serde::{Deserialize, Serialize};

use crate::watermark::EMERGENCY_PERCENT;

/// 参与 placement 的单个 Worker 快照。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerCandidate {
    /// Worker 标识。
    pub worker_id: WorkerId,
    /// Worker 状态（只有 `ACTIVE` 能接收新 DB）。
    pub state: WorkerState,
    /// 区域（region 过滤用）。
    pub region: String,
    /// 可用区（zone 过滤用）。
    pub zone: String,
    /// 声明容量。
    pub capacity: WorkerCapacity,
    /// 最近上报的占用快照（含上报时看到的总量分母）。
    pub usage: WorkerResourceUsage,
    /// Worker 当前实际承载的 DB 进程数。
    ///
    /// 与 `usage.db_process_count` 冗余存在：心跳里的 usage 可能滞后，
    /// 而 DB count 是 hard safety limit 的判定依据，宁可保守取较大者（见
    /// [`WorkerCandidate::effective_db_process_count`]）。
    pub db_process_count: u64,
    /// 该 Worker 正在重启（或已被保留作 failover reserve），不参与普通 placement。
    ///
    /// 语义：它对**普通 placement** 不可用（进程在重启 / 容量要留给故障接管），
    /// 但**可以被 failover 使用** —— reserve 存在的意义就是承接失效 Worker 的 DB。
    pub respawning: bool,
}

impl WorkerCandidate {
    /// 由 Catalog 的 Worker inventory 记录组装候选。
    ///
    /// `reserved_for_failover = true` 的 Worker 会被折叠进 `respawning`
    /// （对普通 placement 同样「不可用」，而 failover 路径依然可以接管它的容量）。
    #[must_use]
    pub fn from_record(record: &WorkerRecord, respawning: bool) -> Self {
        Self {
            worker_id: record.id.clone(),
            state: record.state,
            region: record.region.clone(),
            zone: record.zone.clone(),
            capacity: record.capacity(),
            usage: record.usage(),
            db_process_count: record.process_slots_used,
            respawning: respawning || record.reserved_for_failover,
        }
    }

    /// 是否可承接**普通** placement：状态为 `ACTIVE` 且未处于重启 / reserve 保留状态。
    #[must_use]
    pub fn serves_normal_placement(&self) -> bool {
        self.state.accepts_new_placement() && !self.respawning
    }

    /// 是否可承接 **failover**：只要求状态 `ACTIVE`。
    ///
    /// 重启中的 / 被保留的 Worker 正是 reserve 的载体，failover 必须能用它们，
    /// 否则 reserve 就只是纸面数字（§9 / §15.5）。
    #[must_use]
    pub fn serves_failover(&self) -> bool {
        self.state.accepts_new_placement()
    }

    /// 用于判定的有效容量。
    ///
    /// 规则：某一维在 `capacity` 里为 0 视为**未配置**，回落到最近上报的总量；
    /// 两边都是 0 则该维容量为 0（任何非零需求都会被拒绝，符合 `domain` 的保守约定）。
    /// 进程维额外回落到默认 hard safety limit。
    #[must_use]
    pub fn effective_capacity(&self) -> WorkerCapacity {
        let declared = self.capacity;
        let reported = self.usage.capacity();
        WorkerCapacity {
            cpu_milli: pick(declared.cpu_milli, reported.cpu_milli),
            memory_mib: pick(declared.memory_mib, reported.memory_mib),
            file_descriptors: pick(declared.file_descriptors, reported.file_descriptors),
            disk_mib: pick(declared.disk_mib, reported.disk_mib),
            process_slots: pick_process_limit(declared.process_slots, reported.process_slots),
            iops: pick(declared.iops, reported.iops),
        }
    }

    /// 用于判定的 DB 进程数：取「上报占用」与「独立计数」的较大者（保守）。
    #[must_use]
    pub fn effective_db_process_count(&self) -> u64 {
        self.db_process_count.max(self.usage.db_process_count)
    }

    /// 当前占用（进程维取 [`WorkerCandidate::effective_db_process_count`]）。
    #[must_use]
    pub fn used(&self) -> ResourceBudget {
        let mut used = self.usage.used();
        used.process_slots = self.effective_db_process_count();
        used
    }

    /// 放置 `budget` 之后的占用快照（分母已对齐到 [`WorkerCandidate::effective_capacity`]）。
    #[must_use]
    pub fn usage_after(&self, budget: &ResourceBudget) -> WorkerResourceUsage {
        let mut usage = self.usage;
        let capacity = self.effective_capacity();
        // 分母统一用 effective_capacity：避免 usage 里滞后的总量与声明容量打架，
        // 让 placement 后的利用率计算与准入判定使用同一组数字。
        usage.cpu_milli_total = capacity.cpu_milli;
        usage.memory_mib_total = capacity.memory_mib;
        usage.fd_total = capacity.file_descriptors;
        usage.disk_mib_total = capacity.disk_mib;
        usage.iops_total = capacity.iops;
        usage.db_process_limit = capacity.process_slots;
        usage.db_process_count = self.effective_db_process_count();
        usage.saturating_add_budget(budget);
        usage
    }

    /// 放置 `budget` 之后的六维利用率。
    #[must_use]
    pub fn utilization_after(&self, budget: &ResourceBudget) -> Utilization {
        self.usage_after(budget).utilization()
    }

    /// 放置 `budget` 之后的**资源压力**（水位判定与打分口径）。
    ///
    /// 取 CPU / Memory / FD / Disk / IOPS 的最大利用率，**不含 DB count 维**：
    /// 架构 §9 / §15.5 规定 DB Count 只作 hard safety limit，
    /// 不能让它顶到水位线把 Worker 误判成「紧急」。
    #[must_use]
    pub fn pressure_after(&self, budget: &ResourceBudget) -> f64 {
        let utilization = self.utilization_after(budget);
        [
            utilization.cpu,
            utilization.memory,
            utilization.fd,
            utilization.disk,
            utilization.iops,
        ]
        .into_iter()
        .fold(0.0_f64, f64::max)
    }

    /// failover 可用余量：允许使用到 Emergency 水位（不含）为止的容量。
    ///
    /// reserve 的度量单位就是它 —— 集群「还能再接多少」不是剩余物理容量，
    /// 而是**在不越过 90% 紧急水位的前提下还能接多少**（§9：reserve 是兜底能力，
    /// 不是账面空闲量）。
    #[must_use]
    pub fn failover_usable(&self) -> ResourceBudget {
        self.failover_usable_after(&ResourceBudget::ZERO)
    }

    /// 放置 `budget` 之后的 failover 可用余量。
    #[must_use]
    pub fn failover_usable_after(&self, budget: &ResourceBudget) -> ResourceBudget {
        let capacity = self.effective_capacity();
        let used = self.used().saturating_add(budget);
        ResourceBudget {
            cpu_milli: emergency_headroom(capacity.cpu_milli, used.cpu_milli),
            memory_mib: emergency_headroom(capacity.memory_mib, used.memory_mib),
            file_descriptors: emergency_headroom(capacity.file_descriptors, used.file_descriptors),
            disk_mib: emergency_headroom(capacity.disk_mib, used.disk_mib),
            process_slots: emergency_headroom(capacity.process_slots, used.process_slots),
            iops: emergency_headroom(capacity.iops, used.iops),
        }
    }
}

/// `declared` 为 0（未配置）时回落到 `reported`。
fn pick(declared: u64, reported: u64) -> u64 {
    if declared == 0 {
        reported
    } else {
        declared
    }
}

/// 进程维上限：显式配置优先，未配置（0）回落到默认 hard safety limit。
fn pick_process_limit(declared: u64, reported: u64) -> u64 {
    if declared != 0 {
        declared
    } else if reported != 0 {
        reported
    } else {
        MAX_DB_PROCESS_PER_WORKER_DEFAULT
    }
}

/// `total * 90% - used`（饱和，下限 0）。整数运算避免浮点误差。
fn emergency_headroom(total: u64, used: u64) -> u64 {
    total
        .saturating_mul(EMERGENCY_PERCENT)
        .saturating_div(100)
        .saturating_sub(used)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::candidate;

    #[test]
    fn effective_capacity_falls_back_when_unset() {
        let mut c = candidate("w1", 800, 1000);
        assert_eq!(c.effective_capacity().cpu_milli, 8000);

        // 声明容量缺省时回落到最近上报的总量
        c.capacity = WorkerCapacity::ZERO;
        assert_eq!(c.effective_capacity().cpu_milli, 8000);
        assert_eq!(c.effective_capacity().process_slots, 128);

        // 两边都缺省：进程维回落到默认 hard safety limit，其余维为 0
        c.usage = WorkerResourceUsage::default();
        assert_eq!(
            c.effective_capacity().process_slots,
            MAX_DB_PROCESS_PER_WORKER_DEFAULT
        );
        assert_eq!(c.effective_capacity().cpu_milli, 0);
    }

    #[test]
    fn db_process_count_takes_the_larger_of_the_two_sources() {
        let mut c = candidate("w1", 800, 1000);
        assert_eq!(c.effective_db_process_count(), 4);
        c.db_process_count = 9;
        assert_eq!(
            c.effective_db_process_count(),
            9,
            "心跳滞后时按较大值保守判定"
        );
        assert_eq!(c.used().process_slots, 9);
    }

    #[test]
    fn usage_after_aligns_denominators_and_adds_budget() {
        let c = candidate("w1", 800, 1000);
        let budget = ResourceBudget::new(200, 128, 10, 100, 1, 10);
        let after = c.usage_after(&budget);
        assert_eq!(after.cpu_milli_used, 1000);
        assert_eq!(after.cpu_milli_total, 8000);
        assert_eq!(after.memory_mib_used, 1128);
        assert_eq!(after.db_process_count, 5);
        assert_eq!(after.db_process_limit, 128);

        let utilization = c.utilization_after(&budget);
        assert!((utilization.cpu - 0.125).abs() < 1e-12);
        // 六维里 CPU 最紧（0.125），其余维度都在 0.01~0.07
        assert!((utilization.max() - 0.125).abs() < 1e-12);
        assert!((c.pressure_after(&budget) - 0.125).abs() < 1e-12);

        // 进程维占满不影响资源压力：DB count 只作 hard safety limit
        let mut full_process = candidate("w2", 800, 1000);
        full_process.db_process_count = 128;
        full_process.usage.db_process_count = 128;
        assert!(full_process.utilization_after(&budget).process >= 1.0);
        assert!(
            full_process.pressure_after(&budget) < 0.30,
            "DB count 维不得进入资源压力"
        );
    }

    #[test]
    fn failover_usable_stops_at_the_emergency_watermark() {
        // 8000 * 0.9 = 7200 可用上限；已用 800 -> 余量 6400
        let c = candidate("w1", 800, 1000);
        let usable = c.failover_usable();
        assert_eq!(usable.cpu_milli, 7200 - 800);
        assert_eq!(usable.memory_mib, 16384 * 90 / 100 - 1000);

        // 已越过紧急水位：余量为 0，不得为负
        let hot = candidate("w2", 7800, 16_000);
        assert_eq!(hot.failover_usable().cpu_milli, 0);
        assert_eq!(hot.failover_usable().memory_mib, 0);

        // 放置后余量单调下降
        let after = c.failover_usable_after(&ResourceBudget::new(600, 500, 0, 0, 0, 0));
        assert_eq!(after.cpu_milli, usable.cpu_milli - 600);
        assert_eq!(after.memory_mib, usable.memory_mib - 500);
    }

    #[test]
    fn from_record_folds_reserved_for_failover_into_respawning() {
        let mut record = WorkerRecord {
            id: WorkerId::new("w1"),
            endpoint: "http://w1:9000".to_string(),
            control_endpoint: None,
            data_endpoint: None,
            state: WorkerState::Active,
            region: "r1".to_string(),
            zone: "z1".to_string(),
            version: "0.1.0".to_string(),
            cpu_milli_total: 8000,
            memory_mib_total: 16384,
            fd_total: 4096,
            disk_mib_total: 102_400,
            process_slots_total: 128,
            iops_total: 20_000,
            cpu_milli_used: 800,
            memory_mib_used: 1000,
            fd_used: 100,
            disk_mib_used: 1000,
            process_slots_used: 4,
            iops_used: 1000,
            last_heartbeat_at: None,
            missed_heartbeats: 0,
            inventory_version: 1,
            reserved_for_failover: false,
            labels: serde_json::json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let c = WorkerCandidate::from_record(&record, false);
        assert!(!c.respawning);
        assert!(c.serves_normal_placement());
        assert_eq!(c.db_process_count, 4);
        assert_eq!(c.effective_capacity().cpu_milli, 8000);
        assert_eq!(c.used().cpu_milli, 800);

        record.reserved_for_failover = true;
        let reserved = WorkerCandidate::from_record(&record, false);
        assert!(reserved.respawning);
        assert!(
            !reserved.serves_normal_placement(),
            "reserve Worker 不做普通 placement"
        );
        assert!(reserved.serves_failover(), "reserve 就是给 failover 用的");
    }

    #[test]
    fn state_gates_placement_paths() {
        let mut c = candidate("w1", 800, 1000);
        for state in [
            WorkerState::Suspect,
            WorkerState::Draining,
            WorkerState::Empty,
            WorkerState::Unavailable,
        ] {
            c.state = state;
            assert!(
                !c.serves_normal_placement(),
                "{state} 不得接收普通 placement"
            );
            assert!(!c.serves_failover(), "{state} 不得作为 failover 目标");
        }
        c.state = WorkerState::Active;
        assert!(c.serves_normal_placement());
        assert!(c.serves_failover());
    }
}
