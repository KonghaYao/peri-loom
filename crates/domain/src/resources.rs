//! Worker 资源预算模型（架构 §9）。
//!
//! Worker 不能按“DB 数量”判断容量，因为 DB 之间资源差异很大；因此容量与占用都用
//! **资源预算** 表达，Scheduler 据此做 Resource Budget Packing。
//!
//! 单位约定：`cpu_milli` = milli-core（1000 = 1 vCPU），`memory_mib` / `disk_mib` = MiB，
//! `file_descriptors` / `process_slots` / `iops` = 个数。全部为整数运算。

use serde::{Deserialize, Serialize};

use crate::policy::MAX_DB_PROCESS_PER_WORKER_DEFAULT;

/// 资源预算（DB 请求量）或占用量的六维表示。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceBudget {
    /// CPU，单位 milli-core。
    pub cpu_milli: u64,
    /// 内存，单位 MiB。
    pub memory_mib: u64,
    /// 文件描述符数量。
    pub file_descriptors: u64,
    /// 本地磁盘，单位 MiB。
    pub disk_mib: u64,
    /// 进程位数量（一个 DB = 一个进程，通常为 1）。
    pub process_slots: u64,
    /// IOPS 提示上限（读 + 写）。
    pub iops: u64,
}

impl ResourceBudget {
    /// 全零预算。
    pub const ZERO: ResourceBudget = ResourceBudget {
        cpu_milli: 0,
        memory_mib: 0,
        file_descriptors: 0,
        disk_mib: 0,
        process_slots: 0,
        iops: 0,
    };

    /// 构造预算。
    #[must_use]
    pub const fn new(
        cpu_milli: u64,
        memory_mib: u64,
        file_descriptors: u64,
        disk_mib: u64,
        process_slots: u64,
        iops: u64,
    ) -> Self {
        Self {
            cpu_milli,
            memory_mib,
            file_descriptors,
            disk_mib,
            process_slots,
            iops,
        }
    }

    /// 饱和相加（不会溢出回绕，避免超卖判定被绕过）。
    #[must_use]
    pub const fn saturating_add(&self, other: &ResourceBudget) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: self.cpu_milli.saturating_add(other.cpu_milli),
            memory_mib: self.memory_mib.saturating_add(other.memory_mib),
            file_descriptors: self.file_descriptors.saturating_add(other.file_descriptors),
            disk_mib: self.disk_mib.saturating_add(other.disk_mib),
            process_slots: self.process_slots.saturating_add(other.process_slots),
            iops: self.iops.saturating_add(other.iops),
        }
    }

    /// 饱和相减（下限 0）。
    #[must_use]
    pub const fn saturating_sub(&self, other: &ResourceBudget) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: self.cpu_milli.saturating_sub(other.cpu_milli),
            memory_mib: self.memory_mib.saturating_sub(other.memory_mib),
            file_descriptors: self.file_descriptors.saturating_sub(other.file_descriptors),
            disk_mib: self.disk_mib.saturating_sub(other.disk_mib),
            process_slots: self.process_slots.saturating_sub(other.process_slots),
            iops: self.iops.saturating_sub(other.iops),
        }
    }

    /// 取全字段最小值（用于多维度合并收紧）。
    #[must_use]
    pub const fn min_with(&self, other: &ResourceBudget) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: min_u64(self.cpu_milli, other.cpu_milli),
            memory_mib: min_u64(self.memory_mib, other.memory_mib),
            file_descriptors: min_u64(self.file_descriptors, other.file_descriptors),
            disk_mib: min_u64(self.disk_mib, other.disk_mib),
            process_slots: min_u64(self.process_slots, other.process_slots),
            iops: min_u64(self.iops, other.iops),
        }
    }

    /// 是否为零预算。
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.cpu_milli == 0
            && self.memory_mib == 0
            && self.file_descriptors == 0
            && self.disk_mib == 0
            && self.process_slots == 0
            && self.iops == 0
    }

    /// 按百分比缩放（整数向下取整），用于 failover reserve 之类的预算推导。
    ///
    /// **必须先乘后除**：`v / 100 * pct` 会先把百分位以下的余数截掉再放大，
    /// 于是 9999 的 20% 变成 1980（正确值 1999），小于 100 的值直接变成 0 ——
    /// 对 reserve 这类下界语义是危险的（保留量被悄悄算小）。
    /// 这里用 u128 中间量做 `v * pct / 100`：既保精度又不会回绕，超过 u64 时饱和。
    #[must_use]
    pub const fn percent(&self, pct: u64) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: scale_percent(self.cpu_milli, pct),
            memory_mib: scale_percent(self.memory_mib, pct),
            file_descriptors: scale_percent(self.file_descriptors, pct),
            disk_mib: scale_percent(self.disk_mib, pct),
            process_slots: scale_percent(self.process_slots, pct),
            iops: scale_percent(self.iops, pct),
        }
    }
}

/// `value` 的 `pct`%（整数向下取整），精确到 1 个单位。
///
/// 先乘后除：`value * pct / 100`，中间量用 u128 防溢出；结果超出 u64 时饱和到 `u64::MAX`
/// （不 panic、不回绕）。`pct > 100` 时按同一公式放大。
pub(crate) const fn scale_percent(value: u64, pct: u64) -> u64 {
    let scaled = (value as u128) * (pct as u128) / 100;
    if scaled > u64::MAX as u128 {
        u64::MAX
    } else {
        scaled as u64
    }
}

const fn min_u64(a: u64, b: u64) -> u64 {
    if a < b {
        a
    } else {
        b
    }
}

/// Worker 容量（总量）。字段与 [`ResourceBudget`] 同名，语义为“总量”。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerCapacity {
    /// CPU 总量，单位 milli-core。
    pub cpu_milli: u64,
    /// 内存总量，单位 MiB。
    pub memory_mib: u64,
    /// 文件描述符上限。
    pub file_descriptors: u64,
    /// 本地磁盘总量，单位 MiB。
    pub disk_mib: u64,
    /// 进程位上限。
    pub process_slots: u64,
    /// IOPS 上限。
    pub iops: u64,
}

impl WorkerCapacity {
    /// 全零容量。
    pub const ZERO: WorkerCapacity = WorkerCapacity {
        cpu_milli: 0,
        memory_mib: 0,
        file_descriptors: 0,
        disk_mib: 0,
        process_slots: 0,
        iops: 0,
    };

    /// 构造容量。
    #[must_use]
    pub const fn new(
        cpu_milli: u64,
        memory_mib: u64,
        file_descriptors: u64,
        disk_mib: u64,
        process_slots: u64,
        iops: u64,
    ) -> Self {
        Self {
            cpu_milli,
            memory_mib,
            file_descriptors,
            disk_mib,
            process_slots,
            iops,
        }
    }

    /// 视为预算（便于统一做算术）。
    #[must_use]
    pub const fn as_budget(&self) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: self.cpu_milli,
            memory_mib: self.memory_mib,
            file_descriptors: self.file_descriptors,
            disk_mib: self.disk_mib,
            process_slots: self.process_slots,
            iops: self.iops,
        }
    }

    /// 由预算构造容量。
    #[must_use]
    pub const fn from_budget(budget: ResourceBudget) -> Self {
        Self {
            cpu_milli: budget.cpu_milli,
            memory_mib: budget.memory_mib,
            file_descriptors: budget.file_descriptors,
            disk_mib: budget.disk_mib,
            process_slots: budget.process_slots,
            iops: budget.iops,
        }
    }

    /// 可用余量（容量 - 已用），不会为负。
    #[must_use]
    pub const fn remaining_against(&self, used: &ResourceBudget) -> ResourceBudget {
        self.as_budget().saturating_sub(used)
    }

    /// 是否为零容量。
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.as_budget().is_zero()
    }
}

/// Worker 资源占用快照（对应 proto `WorkerResourceUsage`）。
///
/// 每维都是 `used / total` 成对出现，便于直接上报与计算利用率。
/// 进程维使用 `db_process_count / db_process_limit`（架构 §9：DB 数量只作 hard safety limit）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerResourceUsage {
    /// 已用 CPU（milli-core）。
    pub cpu_milli_used: u64,
    /// CPU 总量（milli-core，分母）。
    pub cpu_milli_total: u64,
    /// 已用内存（MiB）。
    pub memory_mib_used: u64,
    /// 内存总量（MiB，分母）。
    pub memory_mib_total: u64,
    /// 已用 FD。
    pub fd_used: u64,
    /// FD 上限（分母）。
    pub fd_total: u64,
    /// 已用磁盘（MiB）。
    pub disk_mib_used: u64,
    /// 磁盘总量（MiB，分母）。
    pub disk_mib_total: u64,
    /// 已用 IOPS 提示值。
    pub iops_used: u64,
    /// IOPS 上限（分母）。
    pub iops_total: u64,
    /// 当前 DB 进程数。
    pub db_process_count: u64,
    /// DB 进程数上限；**0 表示未配置**，按 [`MAX_DB_PROCESS_PER_WORKER_DEFAULT`] 处理。
    pub db_process_limit: u64,
}

impl WorkerResourceUsage {
    /// 由容量与占用量构造。
    #[must_use]
    pub const fn new(capacity: WorkerCapacity, used: ResourceBudget) -> Self {
        Self {
            cpu_milli_used: used.cpu_milli,
            cpu_milli_total: capacity.cpu_milli,
            memory_mib_used: used.memory_mib,
            memory_mib_total: capacity.memory_mib,
            fd_used: used.file_descriptors,
            fd_total: capacity.file_descriptors,
            disk_mib_used: used.disk_mib,
            disk_mib_total: capacity.disk_mib,
            iops_used: used.iops,
            iops_total: capacity.iops,
            db_process_count: used.process_slots,
            db_process_limit: capacity.process_slots,
        }
    }

    /// 当前占用（进程维取 `db_process_count`）。
    #[must_use]
    pub const fn used(&self) -> ResourceBudget {
        ResourceBudget {
            cpu_milli: self.cpu_milli_used,
            memory_mib: self.memory_mib_used,
            file_descriptors: self.fd_used,
            disk_mib: self.disk_mib_used,
            process_slots: self.db_process_count,
            iops: self.iops_used,
        }
    }

    /// 当前容量（进程维取生效上限）。
    #[must_use]
    pub const fn capacity(&self) -> WorkerCapacity {
        WorkerCapacity {
            cpu_milli: self.cpu_milli_total,
            memory_mib: self.memory_mib_total,
            file_descriptors: self.fd_total,
            disk_mib: self.disk_mib_total,
            process_slots: self.effective_db_process_limit(),
            iops: self.iops_total,
        }
    }

    /// 生效的 DB 进程上限：未配置（0）时回落到默认 hard safety limit。
    #[must_use]
    pub const fn effective_db_process_limit(&self) -> u64 {
        if self.db_process_limit == 0 {
            MAX_DB_PROCESS_PER_WORKER_DEFAULT
        } else {
            self.db_process_limit
        }
    }

    /// 六维利用率，取值 `0.0` 起（可能 > 1.0 表示超卖）。
    ///
    /// 分母为 0 的维度返回 `0.0`（绝不产生 NaN / Inf，保证判定与测试确定性）。
    #[must_use]
    pub fn utilization(&self) -> Utilization {
        Utilization {
            cpu: ratio(self.cpu_milli_used, self.cpu_milli_total),
            memory: ratio(self.memory_mib_used, self.memory_mib_total),
            fd: ratio(self.fd_used, self.fd_total),
            disk: ratio(self.disk_mib_used, self.disk_mib_total),
            process: ratio(self.db_process_count, self.effective_db_process_limit()),
            iops: ratio(self.iops_used, self.iops_total),
        }
    }

    /// 六维利用率的最大值（Placement 主指标）。
    #[must_use]
    pub fn max_utilization(&self) -> f64 {
        self.utilization().max()
    }

    /// 累加一个 DB 的资源占用（饱和运算，不会回绕）。
    pub const fn saturating_add_budget(&mut self, budget: &ResourceBudget) {
        self.cpu_milli_used = self.cpu_milli_used.saturating_add(budget.cpu_milli);
        self.memory_mib_used = self.memory_mib_used.saturating_add(budget.memory_mib);
        self.fd_used = self.fd_used.saturating_add(budget.file_descriptors);
        self.disk_mib_used = self.disk_mib_used.saturating_add(budget.disk_mib);
        self.iops_used = self.iops_used.saturating_add(budget.iops);
        self.db_process_count = self.db_process_count.saturating_add(budget.process_slots);
    }

    /// 扣减一个 DB 的资源占用（下限 0）。
    pub const fn saturating_sub_budget(&mut self, budget: &ResourceBudget) {
        self.cpu_milli_used = self.cpu_milli_used.saturating_sub(budget.cpu_milli);
        self.memory_mib_used = self.memory_mib_used.saturating_sub(budget.memory_mib);
        self.fd_used = self.fd_used.saturating_sub(budget.file_descriptors);
        self.disk_mib_used = self.disk_mib_used.saturating_sub(budget.disk_mib);
        self.iops_used = self.iops_used.saturating_sub(budget.iops);
        self.db_process_count = self.db_process_count.saturating_sub(budget.process_slots);
    }

    /// 准入判定（架构 §9 `CanStart`）：六维全部满足 `used + need <= total`。
    ///
    /// 任一分母为 0（容量未配置）时，任何非零需求都会被拒绝 —— 保守优先。
    /// 零预算在数学上总是“装得下”，但会绕过进程位 hard limit，因此构造 DB 预算时
    /// 必须带上 `process_slots = 1`（见 `DatabaseRecord::resource_budget`）。
    #[must_use]
    pub const fn fits(&self, budget: &ResourceBudget) -> bool {
        let capacity = self.capacity();
        self.used().saturating_add(budget).cpu_milli <= capacity.cpu_milli
            && self.used().saturating_add(budget).memory_mib <= capacity.memory_mib
            && self.used().saturating_add(budget).file_descriptors <= capacity.file_descriptors
            && self.used().saturating_add(budget).disk_mib <= capacity.disk_mib
            && self.used().saturating_add(budget).process_slots <= capacity.process_slots
            && self.used().saturating_add(budget).iops <= capacity.iops
    }

    /// 剩余可用预算（六维）。
    #[must_use]
    pub const fn remaining(&self) -> ResourceBudget {
        self.capacity().remaining_against(&self.used())
    }
}

/// 六维利用率快照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Utilization {
    /// CPU 利用率。
    pub cpu: f64,
    /// 内存利用率。
    pub memory: f64,
    /// 文件描述符利用率。
    pub fd: f64,
    /// 本地磁盘利用率。
    pub disk: f64,
    /// 进程位利用率。
    pub process: f64,
    /// IOPS 利用率。
    pub iops: f64,
}

impl Utilization {
    /// 最大维度利用率。
    #[must_use]
    pub fn max(&self) -> f64 {
        self.all()
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max)
            .max(0.0)
    }

    /// 六维取值。
    #[must_use]
    pub const fn all(&self) -> [f64; 6] {
        [
            self.cpu,
            self.memory,
            self.fd,
            self.disk,
            self.process,
            self.iops,
        ]
    }
}

/// `used / total`；分母为 0 时返回 `0.0`（不是 NaN）。
fn ratio(used: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        used as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(cpu: u64, mem: u64) -> ResourceBudget {
        ResourceBudget::new(cpu, mem, 512, 1024, 1, 2000)
    }

    #[test]
    fn utilization_with_zero_denominator_is_zero_not_nan() {
        let usage = WorkerResourceUsage::default();
        let u = usage.utilization();
        for value in u.all() {
            assert!(!value.is_nan(), "分母为 0 时不得产生 NaN");
            assert_eq!(value, 0.0);
        }
        assert_eq!(usage.max_utilization(), 0.0);
        // 有占用但容量未上报：CPU 维依然是 0.0（保守由 fits() 拒绝，而不是靠 NaN 传播）
        let usage = WorkerResourceUsage {
            cpu_milli_used: 500,
            db_process_count: 7,
            ..Default::default()
        };
        assert_eq!(usage.utilization().cpu, 0.0);
        // 进程维分母 0 时回落到默认 hard limit，因此永远不是 NaN
        assert!(
            (usage.utilization().process - 7.0 / MAX_DB_PROCESS_PER_WORKER_DEFAULT as f64).abs()
                < 1e-12
        );
    }

    #[test]
    fn utilization_and_max() {
        let capacity = WorkerCapacity::new(8000, 16384, 4096, 102400, 128, 20000);
        let used = ResourceBudget::new(4000, 4096, 1024, 10240, 12, 6000);
        let usage = WorkerResourceUsage::new(capacity, used);
        let u = usage.utilization();
        assert!((u.cpu - 0.5).abs() < f64::EPSILON);
        assert!((u.memory - 0.25).abs() < f64::EPSILON);
        assert!((u.fd - 0.25).abs() < f64::EPSILON);
        assert!((u.disk - 0.1).abs() < f64::EPSILON);
        assert!((u.process - 12.0 / 128.0).abs() < f64::EPSILON);
        assert!((u.iops - 0.3).abs() < f64::EPSILON);
        assert!((usage.max_utilization() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn process_dimension_uses_default_limit_when_unset() {
        let mut usage = WorkerResourceUsage::new(
            WorkerCapacity::new(8000, 16384, 4096, 102400, 0, 20000),
            ResourceBudget::ZERO,
        );
        assert_eq!(
            usage.effective_db_process_limit(),
            MAX_DB_PROCESS_PER_WORKER_DEFAULT
        );
        usage.db_process_count = MAX_DB_PROCESS_PER_WORKER_DEFAULT;
        assert!((usage.utilization().process - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn fits_and_remaining_respect_every_dimension() {
        let capacity = WorkerCapacity::new(8000, 16384, 4096, 102400, 128, 20000);
        let mut usage = WorkerResourceUsage::new(capacity, ResourceBudget::ZERO);
        assert!(usage.fits(&budget(500, 256)));
        assert_eq!(usage.remaining().cpu_milli, 8000);

        usage.saturating_add_budget(&budget(500, 256));
        assert_eq!(usage.used().cpu_milli, 500);
        assert_eq!(usage.used().memory_mib, 256);
        assert_eq!(usage.db_process_count, 1);

        // 内存是短板：只剩 16128，要 20000 就不满足
        assert!(!usage.fits(&ResourceBudget::new(1, 20000, 0, 0, 0, 0)));
        // 进程位是短板：hits 128 后不能再放（当前 1 + 127 可以，+128 不行）
        assert!(usage.fits(&ResourceBudget::new(1, 1, 0, 0, 127, 0)));
        assert!(!usage.fits(&ResourceBudget::new(1, 1, 0, 0, 128, 0)));

        // 零预算不消耗任何资源，数学上总是装得下（真实 DB 预算含 process_slots=1）
        assert!(usage.fits(&ResourceBudget::ZERO));

        // 未配置容量则拒绝任何真实需求（分母 0）
        let empty = WorkerResourceUsage::default();
        assert!(!empty.fits(&budget(1, 1)));
    }

    #[test]
    fn add_and_sub_are_saturating() {
        let mut usage = WorkerResourceUsage::new(
            WorkerCapacity::new(100, 100, 100, 100, 100, 100),
            ResourceBudget::ZERO,
        );
        usage.saturating_add_budget(&ResourceBudget::new(u64::MAX, u64::MAX, 0, 0, 0, 0));
        assert_eq!(usage.used().cpu_milli, u64::MAX);
        usage.saturating_add_budget(&ResourceBudget::new(10, 10, 0, 0, 0, 0));
        assert_eq!(usage.used().cpu_milli, u64::MAX, "饱和相加不得回绕");

        usage.saturating_sub_budget(&ResourceBudget::new(
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            u64::MAX,
        ));
        assert!(usage.used().is_zero(), "扣减不得下溢");
    }

    #[test]
    fn budget_arithmetic() {
        let a = ResourceBudget::new(1000, 512, 64, 100, 1, 200);
        let b = ResourceBudget::new(2000, 256, 32, 50, 1, 100);
        assert_eq!(a.saturating_add(&b).cpu_milli, 3000);
        assert_eq!(a.saturating_add(&b).process_slots, 2);
        assert_eq!(b.saturating_sub(&a).cpu_milli, 1000);
        assert_eq!(a.saturating_sub(&b).cpu_milli, 0);
        assert_eq!(a.percent(20).cpu_milli, 200);
        assert_eq!(a.min_with(&b).memory_mib, 256);
        assert!(ResourceBudget::ZERO.is_zero());
        assert!(!a.is_zero());
    }

    /// FIX-2：百分比必须「先乘后除」，否则小数值会被截断（旧实现 9999 -> 1980，甚至 0）。
    #[test]
    fn percent_keeps_precision_by_multiplying_before_dividing() {
        // 9999 的 20% = 1999.8 -> 1999；`9999 / 100 * 20` 只有 1980
        let budget = ResourceBudget::new(9999, 9999, 9999, 9999, 9999, 9999);
        assert_eq!(
            budget.percent(20),
            ResourceBudget::new(1999, 1999, 1999, 1999, 1999, 1999)
        );

        // 小于 100 的总量不得被抹成 0（reserve 的下界语义不允许「静默算小」）
        let small = ResourceBudget::new(99, 99, 99, 99, 99, 99);
        assert_eq!(
            small.percent(20),
            ResourceBudget::new(19, 19, 19, 19, 19, 19)
        );
        assert!(!small.percent(20).is_zero());

        // 100% 是恒等；小于 1 个单位的余数仍然向下取整
        assert_eq!(budget.percent(100), budget);
        assert_eq!(
            ResourceBudget::new(9, 9, 9, 9, 9, 9).percent(10),
            ResourceBudget::ZERO
        );

        // 溢出饱和：不 panic、不回绕（u64::MAX 的 101% 必然溢出）
        let huge = ResourceBudget::new(u64::MAX, u64::MAX, 0, 0, 0, 0);
        assert_eq!(huge.percent(100).cpu_milli, u64::MAX);
        assert_eq!(
            huge.percent(101).cpu_milli,
            u64::MAX,
            "溢出必须饱和而不是回绕"
        );
        assert_eq!(huge.percent(101).memory_mib, u64::MAX);
    }

    #[test]
    fn usage_and_capacity_conversions_round_trip() {
        let capacity = WorkerCapacity::new(8000, 16384, 4096, 102400, 128, 20000);
        let used = ResourceBudget::new(4000, 4096, 1024, 10240, 12, 6000);
        let usage = WorkerResourceUsage::new(capacity, used);
        assert_eq!(usage.used(), used);
        assert_eq!(usage.capacity(), capacity);
        assert_eq!(WorkerCapacity::from_budget(capacity.as_budget()), capacity);
        assert_eq!(capacity.remaining_against(&used).cpu_milli, 4000);
    }
}
