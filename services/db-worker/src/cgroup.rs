//! cgroup v2 per-DB 资源隔离（架构 §9 / §17.6）。
//!
//! 每个 DB Process 放入独立 cgroup：`<CGROUP_ROOT>/db-platform/<db_id>`，写入：
//!
//! ```text
//! memory.max   DB 内存硬上限（budget.memory_mib）
//! cpu.max      "quota period"（cpu_milli 换算，period 固定 100000us）
//! pids.max     DB 进程/线程数上限（budget.process_slots，至少 1）
//! cgroup.procs 启动后把子进程 PID 写入（进程树随 cgroup 一起被限制）
//! ```
//!
//! **降级是硬要求**：容器 / 开发机上 cgroup v2 常常不可写（只读挂载、未委派、
//! 无 SYS_ADMIN）。此时必须记 warning 并继续启动 DB —— 资源隔离是优化，不是可用性前提。
//!
//! 换算约定：
//! - `cpu_milli = 1000` 表示 1 个 vCPU；`cpu_milli = 1500` 表示 1.5 个 vCPU。
//! - `quota = cpu_milli * period / 1000`，且不得低于内核下限 1ms（1000us）。
//! - `cpu_milli = 0` 表示不限制 CPU（写 `max`），而不是「零配额」（后者会让 DB 饿死）。

use std::fs;
use std::path::{Path, PathBuf};

use domain::resources::ResourceBudget;

use crate::error::{Result, WorkerError};
use crate::paths::sanitize_id;

/// cgroup 控制器的 cpu 周期（微秒）。内核默认值也是 100000，显式写死便于换算可复现。
pub const CPU_PERIOD_US: u64 = 100_000;

/// 内核允许的最小 cpu quota（1ms）。低于它会写失败（EINVAL）。
pub const MIN_CPU_QUOTA_US: u64 = 1_000;

/// Worker 在 cgroup 根下创建的命名空间目录。
pub const CGROUP_NAMESPACE: &str = "db-platform";

/// 需要启用的控制器（父节点 subtree_control）。
pub const REQUIRED_CONTROLLERS: &str = "+cpu +memory +pids";

/// `cpu.max` 的配额换算（微秒）。`cpu_milli == 0` 返回 `None`（表示不限制）。
pub fn cpu_quota_us(cpu_milli: u64, period_us: u64) -> Option<u64> {
    if cpu_milli == 0 {
        return None;
    }
    // period 为 0 时退化为无限制，避免除零
    if period_us == 0 {
        return None;
    }
    let quota = cpu_milli.saturating_mul(period_us) / 1_000;
    Some(quota.max(MIN_CPU_QUOTA_US))
}

/// `cpu.max` 的完整取值：`"<quota> <period>"` 或 `"max <period>"`。
pub fn cpu_max_value(cpu_milli: u64, period_us: u64) -> String {
    match cpu_quota_us(cpu_milli, period_us) {
        Some(quota) => format!("{quota} {period_us}"),
        None => format!("max {period_us}"),
    }
}

/// `memory.max` 的取值（字节）或 `max`（不限）。
pub fn memory_max_value(memory_mib: u64) -> String {
    if memory_mib == 0 {
        return "max".to_string();
    }
    memory_mib.saturating_mul(1024 * 1024).to_string()
}

/// `pids.max` 的取值：至少 1（0 会让 DB 连线程都创建不出来）。
pub fn pids_max_value(process_slots: u64) -> String {
    // pids.max 限制的是**线程数**（cgroup v2 的 pids 控制器把线程也计入）。
    // DB Process 至少需要一个多线程 tokio runtime（worker 线程 + blocking 池），
    // 再叠加 TursoDB 自身可能创建的线程，因此下限必须显著大于 1。
    //
    // 曾经的 `max(1)` 会让 db-runtime 一启动就 panic：
    // "OS can't spawn worker thread: Resource temporarily unavailable"。
    process_slots.max(MIN_PIDS_LIMIT).to_string()
}

/// `pids.max` 的下限。
///
/// 取值依据：tokio 多线程 runtime 默认 worker 数 = CPU 数（本平台单库上限远小于 64），
/// 加上 blocking 线程池与 TursoDB 内部线程，64 足够跑起来且仍远低于 Worker 的进程总量。
/// 注意这是**下限**：调用方给出的 process_slots 更大时以调用方为准。
pub const MIN_PIDS_LIMIT: u64 = 64;

/// 把 cpu_milli 换算为「核数」（用于 metric / 上报，1.0 = 1 core）。
pub fn cpu_milli_to_cores(cpu_milli: u64) -> f64 {
    cpu_milli as f64 / 1000.0
}

/// cgroup v2 管理器。
///
/// `enabled = false` 时所有操作都是 no-op，调用方无需分支。
#[derive(Debug, Clone)]
pub struct CgroupManager {
    root: PathBuf,
    enabled: bool,
    /// 本 cgroup 命名空间路径：`<root>/db-platform`。
    namespace: PathBuf,
}

impl CgroupManager {
    /// 构造管理器。`enabled = false` 或根目录不是 cgroup v2 时自动降级。
    pub fn new(root: PathBuf, enabled: bool) -> Self {
        let namespace = root.join(CGROUP_NAMESPACE);
        let usable = enabled && is_cgroup2_root(&root);
        if enabled && !usable {
            tracing::warn!(
                root = %root.display(),
                "未检测到可用的 cgroup v2 文件系统（cgroup.controllers 缺失），资源隔离降级为不限制"
            );
        }
        Self {
            root,
            enabled: usable,
            namespace,
        }
    }

    /// 是否真正启用了 cgroup 限制。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// cgroup 根路径。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 命名空间目录（`<root>/db-platform`）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn namespace(&self) -> &Path {
        &self.namespace
    }

    /// 某个 DB 的 cgroup 路径：`<root>/db-platform/<db_id 消毒后>`。
    pub fn path_for(&self, db_id: &str) -> PathBuf {
        self.namespace.join(sanitize_id(db_id))
    }

    /// 启用父节点控制器（best effort；容器内常因未委派而失败）。
    pub fn enable_controllers(&self) {
        if !self.enabled {
            return;
        }
        if let Err(err) = fs::create_dir_all(&self.namespace) {
            tracing::warn!(
                path = %self.namespace.display(),
                error = %err,
                "创建 cgroup 命名空间目录失败，降级为不限制"
            );
            return;
        }
        for dir in [&self.root, &self.namespace] {
            let control = dir.join("cgroup.subtree_control");
            if !control.exists() {
                continue;
            }
            if let Err(err) = fs::write(&control, REQUIRED_CONTROLLERS) {
                // EBUSY（已有子 cgroup 持有进程）等属常见情况，按降级处理
                tracing::warn!(
                    path = %control.display(),
                    error = %err,
                    "启用 cgroup 控制器失败（该维度将不受限）"
                );
            }
        }
    }

    /// 为 DB 创建 cgroup 并写入限制。返回 `None` 表示降级（不限制）。
    pub fn create(&self, db_id: &str, budget: &ResourceBudget) -> Option<PathBuf> {
        if !self.enabled {
            return None;
        }
        let path = self.path_for(db_id);
        if let Err(err) = fs::create_dir_all(&path) {
            tracing::warn!(
                db_id = %db_id,
                path = %path.display(),
                error = %err,
                "创建 per-DB cgroup 失败，该 DB 不受 cgroup 限制"
            );
            return None;
        }
        self.write_limit(
            &path,
            "memory.max",
            &memory_max_value(budget.memory_mib),
            db_id,
        );
        self.write_limit(
            &path,
            "cpu.max",
            &cpu_max_value(budget.cpu_milli, CPU_PERIOD_US),
            db_id,
        );
        self.write_limit(
            &path,
            "pids.max",
            &pids_max_value(budget.process_slots),
            db_id,
        );
        Some(path)
    }

    /// 写单个限制文件；失败只记 warning（降级）。
    fn write_limit(&self, path: &Path, file: &str, value: &str, db_id: &str) {
        let target = path.join(file);
        if let Err(err) = fs::write(&target, value) {
            tracing::warn!(
                db_id = %db_id,
                path = %target.display(),
                value = %value,
                error = %err,
                "写入 cgroup 限制失败（该维度降级为不限制）"
            );
        }
    }

    /// 把 PID 加入 cgroup（必须在 spawn 之后立刻调用，否则子进程会短暂逃逸限制）。
    pub fn assign(&self, cgroup: &Path, pid: i32) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let procs = cgroup.join("cgroup.procs");
        fs::write(&procs, pid.to_string())
            .map_err(|err| WorkerError::io(format!("写入 {} ({pid})", procs.display()), err))
    }

    /// 读取 `memory.current`（字节）。
    pub fn read_memory_current_bytes(&self, cgroup: &Path) -> Option<u64> {
        read_u64(&cgroup.join("memory.current"))
    }

    /// 读取 `memory.peak`（字节，内核 5.19+）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn read_memory_peak_bytes(&self, cgroup: &Path) -> Option<u64> {
        read_u64(&cgroup.join("memory.peak"))
    }

    /// 读取 `cpu.stat` 中的 `usage_usec`（累计 CPU 时间，微秒）。
    pub fn read_cpu_usage_usec(&self, cgroup: &Path) -> Option<u64> {
        let content = fs::read_to_string(cgroup.join("cpu.stat")).ok()?;
        parse_cpu_stat_usage_usec(&content)
    }

    /// 删除 per-DB cgroup 目录（DB 进程退出后必须调用，否则会残留目录）。
    pub fn remove(&self, cgroup: &Path) {
        if !self.enabled {
            return;
        }
        // rmdir 要求目录为空；进程退出后 cgroup.procs 为空即可删除
        if let Err(err) = fs::remove_dir(cgroup) {
            tracing::debug!(
                path = %cgroup.display(),
                error = %err,
                "删除 per-DB cgroup 失败（可能仍有残留进程），忽略"
            );
        }
    }
}

/// 判断某个目录是否是 cgroup v2 统一层级根（存在 `cgroup.controllers`）。
pub fn is_cgroup2_root(root: &Path) -> bool {
    root.join("cgroup.controllers").exists()
}

/// 从 `cpu.stat` 文本解析 `usage_usec`。
pub fn parse_cpu_stat_usage_usec(content: &str) -> Option<u64> {
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("usage_usec ") {
            return rest.trim().parse::<u64>().ok();
        }
    }
    None
}

/// 从 `memory.current` 之类只含一个整数的文件读取数值。
fn read_u64(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::resources::ResourceBudget;

    // ------------------------------------------------------------------ 换算

    #[test]
    fn cpu_max_conversion_uses_100ms_period() {
        // 1 vCPU -> quota = period
        assert_eq!(cpu_quota_us(1000, CPU_PERIOD_US), Some(100_000));
        assert_eq!(cpu_max_value(1000, CPU_PERIOD_US), "100000 100000");
        // 1.5 vCPU
        assert_eq!(cpu_max_value(1500, CPU_PERIOD_US), "150000 100000");
        // 0.5 vCPU
        assert_eq!(cpu_max_value(500, CPU_PERIOD_US), "50000 100000");
        // 8 vCPU
        assert_eq!(cpu_max_value(8000, CPU_PERIOD_US), "800000 100000");
    }

    #[test]
    fn cpu_max_handles_degenerate_inputs() {
        // 0 = 不限制（而非 0 配额）
        assert_eq!(cpu_quota_us(0, CPU_PERIOD_US), None);
        assert_eq!(cpu_max_value(0, CPU_PERIOD_US), "max 100000");
        // 极小配额被抬到内核下限
        assert_eq!(cpu_quota_us(1, CPU_PERIOD_US), Some(MIN_CPU_QUOTA_US));
        // period 为 0 时不产生除零
        assert_eq!(cpu_quota_us(1000, 0), None);
        // 极大值不溢出（饱和乘法后整除）
        assert_eq!(
            cpu_quota_us(u64::MAX, CPU_PERIOD_US),
            Some(u64::MAX / 1_000)
        );
    }

    #[test]
    fn memory_and_pids_values() {
        assert_eq!(memory_max_value(512), (512u64 * 1024 * 1024).to_string());
        assert_eq!(memory_max_value(0), "max");
        // 下限保护：DB Process 至少要能创建多线程 async runtime 的线程，
        // 因此过小的 process_slots 会被抬到 MIN_PIDS_LIMIT（而不是 1）。
        assert_eq!(pids_max_value(1), MIN_PIDS_LIMIT.to_string());
        assert_eq!(pids_max_value(0), MIN_PIDS_LIMIT.to_string());
        assert_eq!(pids_max_value(2048), "2048");
        assert!((cpu_milli_to_cores(1500) - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_cpu_stat_extracts_usage() {
        let content = "usage_usec 123456\nuser_usec 100\nsystem_usec 50\nnr_periods 10\n";
        assert_eq!(parse_cpu_stat_usage_usec(content), Some(123_456));
        assert_eq!(parse_cpu_stat_usage_usec("nr_periods 3\n"), None);
        assert_eq!(parse_cpu_stat_usage_usec("usage_usec not-a-number\n"), None);
    }

    // ------------------------------------------------------------------ 路径与降级

    #[test]
    fn path_joins_namespace_and_sanitized_id() {
        let manager = CgroupManager::new(PathBuf::from("/sys/fs/cgroup"), false);
        assert!(!manager.is_enabled());
        assert_eq!(
            manager.path_for("db-1"),
            PathBuf::from("/sys/fs/cgroup/db-platform/db-1")
        );
        // 穿越尝试被消毒
        assert_eq!(
            manager.path_for("../../x"),
            PathBuf::from("/sys/fs/cgroup/db-platform/.._.._x")
        );
        assert_eq!(manager.namespace(), Path::new("/sys/fs/cgroup/db-platform"));
    }

    #[test]
    fn disabled_manager_never_touches_filesystem() {
        let dir = tempfile::tempdir().unwrap();
        let manager = CgroupManager::new(dir.path().to_path_buf(), false);
        let budget = ResourceBudget::new(1000, 128, 0, 0, 1, 0);
        assert!(manager.create("db-1", &budget).is_none());
        // 不创建任何目录：降级路径必须零副作用
        assert!(!dir.path().join(CGROUP_NAMESPACE).exists());
        manager.remove(&manager.path_for("db-1"));
    }

    #[test]
    fn create_writes_limits_and_assign_writes_procs_in_fake_root() {
        let dir = tempfile::tempdir().unwrap();
        // 伪造一个 cgroup v2 根：只要有 cgroup.controllers 就视为可用
        fs::write(dir.path().join("cgroup.controllers"), "cpu memory pids\n").unwrap();
        let manager = CgroupManager::new(dir.path().to_path_buf(), true);
        assert!(manager.is_enabled());
        manager.enable_controllers();

        let budget = ResourceBudget::new(1500, 256, 0, 0, 4, 0);
        let cgroup = manager.create("db-1", &budget).expect("应创建 cgroup");

        assert_eq!(
            fs::read_to_string(cgroup.join("cpu.max")).unwrap(),
            "150000 100000"
        );
        assert_eq!(
            fs::read_to_string(cgroup.join("memory.max")).unwrap(),
            (256u64 * 1024 * 1024).to_string()
        );
        // 下限保护：预算里的 4 太小（DB Process 需要多线程 async runtime），
        // 实际写入的是 MIN_PIDS_LIMIT。
        assert_eq!(
            fs::read_to_string(cgroup.join("pids.max")).unwrap(),
            MIN_PIDS_LIMIT.to_string()
        );

        manager.assign(&cgroup, std::process::id() as i32).unwrap();
        assert_eq!(
            fs::read_to_string(cgroup.join("cgroup.procs")).unwrap(),
            std::process::id().to_string()
        );

        // 统计读取：伪造 memory.current / cpu.stat
        fs::write(cgroup.join("memory.current"), "1048576\n").unwrap();
        fs::write(cgroup.join("cpu.stat"), "usage_usec 9000\n").unwrap();
        assert_eq!(manager.read_memory_current_bytes(&cgroup), Some(1_048_576));
        assert_eq!(manager.read_cpu_usage_usec(&cgroup), Some(9_000));

        // 真实 cgroupfs 上目录只含虚拟文件，rmdir 直接成功；普通文件系统上
        // 需要先清掉测试写进去的文件，这里验证的是「不 panic + 目录可移除」
        manager.remove(&cgroup);
        for file in [
            "cpu.max",
            "memory.max",
            "pids.max",
            "cgroup.procs",
            "memory.current",
            "cpu.stat",
        ] {
            let _ = fs::remove_file(cgroup.join(file));
        }
        manager.remove(&cgroup);
        assert!(!cgroup.exists());
    }

    #[test]
    fn create_degrades_when_directory_is_unwritable() {
        // 根存在但没有 cgroup.controllers -> 判定为不可用，直接降级
        let dir = tempfile::tempdir().unwrap();
        let manager = CgroupManager::new(dir.path().to_path_buf(), true);
        assert!(!manager.is_enabled());
        let budget = ResourceBudget::new(1000, 128, 0, 0, 1, 0);
        assert!(manager.create("db-1", &budget).is_none());
    }
}
