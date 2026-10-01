//! 资源采集与上报（架构 §9）。
//!
//! 两条数据流必须分清：
//!
//! 1. **承诺量（budget）**：注册表中各 DB 预算之和，喂给 Scheduler 的 Resource Budget
//!    Packing。这是不可抖动的契约量 —— 用实测值会让打包判定随负载漂移。
//! 2. **实测值（usage）**：每个 DB 进程的 RSS / CPU，用于指标（`db_process_cpu` /
//!    `db_process_memory_mib`）、饱和告警与排障。优先读 cgroup v2
//!    （`memory.current` / `cpu.stat`），降级时读 `/proc/<pid>`。
//!
//! CPU 是**速率**：cgroup 只给累计 `usage_usec`，必须两次采样求差除以间隔。因此采样器
//! 保存上一轮的计数与时刻；第一轮没有基准，按 0 上报（而不是把累计值当瞬时值）。

use std::collections::HashMap;
use std::time::Instant;

use domain::resources::{ResourceBudget, WorkerCapacity, WorkerResourceUsage};
use parking_lot::Mutex;

use crate::cgroup::CgroupManager;
use crate::registry::LocalDbRegistry;

/// 单个 DB 进程的实测占用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbUsage {
    /// 数据库 id。
    pub database_id: String,
    /// PID（未知为 0）。
    pub pid: i32,
    /// RSS（MiB）。
    pub rss_mib: u64,
    /// CPU 占用，单位 milli-core（1000 = 1 core）。
    pub cpu_milli: u64,
}

/// 上一轮采样的 CPU 基准。
#[derive(Clone, Copy, Debug)]
struct CpuBaseline {
    /// cgroup `cpu.stat usage_usec` 累计值（无 cgroup 时为 0）。
    cgroup_usec: u64,
    /// `/proc/<pid>/stat` 的 utime+stime（时钟滴答，无 /proc 时为 0）。
    proc_ticks: u64,
    /// 采样时刻。
    at: Instant,
}

/// 资源采样器。
#[derive(Debug)]
pub struct ResourceSampler {
    capacity: WorkerCapacity,
    cgroups: CgroupManager,
    baseline: Mutex<HashMap<String, CpuBaseline>>,
    last_usage: Mutex<WorkerResourceUsage>,
    last_per_db: Mutex<Vec<DbUsage>>,
}

impl ResourceSampler {
    /// 构造采样器。初始上报为「零占用」。
    pub fn new(capacity: WorkerCapacity, cgroups: CgroupManager) -> Self {
        Self {
            capacity,
            cgroups,
            baseline: Mutex::new(HashMap::new()),
            last_usage: Mutex::new(WorkerResourceUsage::new(capacity, ResourceBudget::ZERO)),
            last_per_db: Mutex::new(Vec::new()),
        }
    }

    /// Worker 容量（配置值）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn capacity(&self) -> WorkerCapacity {
        self.capacity
    }

    /// 最近一次采样的上报值（不产生 IO，供心跳与状态查询使用）。
    pub fn current(&self) -> WorkerResourceUsage {
        *self.last_usage.lock()
    }

    /// 最近一次采样的每 DB 明细。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn per_db(&self) -> Vec<DbUsage> {
        self.last_per_db.lock().clone()
    }

    /// 读取指定 DB 的最近一次实测 RSS（MiB）。
    pub fn rss_mib_of(&self, db_id: &str) -> u64 {
        self.last_per_db
            .lock()
            .iter()
            .find(|usage| usage.database_id == db_id)
            .map(|usage| usage.rss_mib)
            .unwrap_or(0)
    }

    /// 当前六维最大饱和度（0.0 起，可能 > 1.0）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn saturation(&self) -> f64 {
        self.current().max_utilization()
    }

    /// 采样一次：更新缓存并返回上报值。
    ///
    /// 文件读取量大（每 DB 1~3 个小文件），放到阻塞线程池执行，避免占用 reactor。
    pub async fn refresh(&self, registry: &LocalDbRegistry) -> WorkerResourceUsage {
        let targets: Vec<SampleTarget> = registry
            .snapshot()
            .into_iter()
            .map(|db| SampleTarget {
                database_id: db.database_id,
                pid: db.pid.unwrap_or(0),
                cgroup: db.cgroup,
            })
            .collect();

        let cgroups = self.cgroups.clone();
        let capacity = self.capacity;
        // 占用（承诺量）= 注册表预算之和；实测值只用于指标
        let used = registry.used_budget();
        let previous = self.baseline.lock().clone();

        let outcome = tokio::task::spawn_blocking(move || {
            let (per_db, baseline) = sample_processes(&cgroups, targets, previous);
            (per_db, baseline, capacity, used)
        })
        .await;

        let (per_db, baseline, capacity, used) = match outcome {
            Ok(value) => value,
            Err(err) => {
                // 采样失败不得影响控制面：保留上一轮数据
                tracing::warn!(error = %err, "资源采样任务失败，沿用上一轮数据");
                return self.current();
            }
        };

        let usage = WorkerResourceUsage::new(capacity, used);
        {
            let mut guard = self.baseline.lock();
            *guard = baseline;
        }
        for item in &per_db {
            crate::metrics::record_db_usage(item);
        }
        crate::metrics::record_saturation(usage.max_utilization());

        *self.last_usage.lock() = usage;
        *self.last_per_db.lock() = per_db;
        usage
    }
}

/// 采样目标（从注册表快照转换而来，供阻塞线程池消费）。
#[derive(Clone, Debug)]
struct SampleTarget {
    database_id: String,
    pid: i32,
    cgroup: Option<std::path::PathBuf>,
}

/// 采样所有目标进程，返回明细与新的 CPU 基准。
///
/// 纯函数（只读文件系统），便于单测：传入伪造的 cgroup 目录即可验证换算。
fn sample_processes(
    cgroups: &CgroupManager,
    targets: Vec<SampleTarget>,
    previous: HashMap<String, CpuBaseline>,
) -> (Vec<DbUsage>, HashMap<String, CpuBaseline>) {
    let now = Instant::now();
    let mut per_db = Vec::with_capacity(targets.len());
    let mut baseline = HashMap::with_capacity(targets.len());

    for target in targets {
        let cgroup_usec = target
            .cgroup
            .as_deref()
            .and_then(|path| cgroups.read_cpu_usage_usec(path))
            .unwrap_or(0);

        // RSS：cgroup 更准（含 DB 全部线程），取不到时回落 /proc
        let rss_mib = target
            .cgroup
            .as_deref()
            .and_then(|path| cgroups.read_memory_current_bytes(path))
            .map(|bytes| bytes / (1024 * 1024))
            .or_else(|| read_proc_rss_kb(target.pid).map(|kb| kb / 1024))
            .unwrap_or(0);

        let proc_ticks = if cgroup_usec == 0 {
            read_proc_cpu_ticks(target.pid).unwrap_or(0)
        } else {
            0
        };

        let cpu_milli = match previous.get(&target.database_id) {
            Some(prev) => {
                let elapsed = now.saturating_duration_since(prev.at).as_micros() as u64;
                if cgroup_usec > 0 {
                    cpu_milli_from_delta(cgroup_usec.saturating_sub(prev.cgroup_usec), elapsed)
                } else {
                    cpu_milli_from_ticks(
                        proc_ticks.saturating_sub(prev.proc_ticks),
                        elapsed,
                        clock_ticks_per_second(),
                    )
                }
            }
            // 首轮无基准：按 0 上报，绝不把累计值当瞬时速率
            None => 0,
        };

        baseline.insert(
            target.database_id.clone(),
            CpuBaseline {
                cgroup_usec,
                proc_ticks,
                at: now,
            },
        );
        per_db.push(DbUsage {
            database_id: target.database_id,
            pid: target.pid,
            rss_mib,
            cpu_milli,
        });
    }

    (per_db, baseline)
}

/// 由 cgroup 的 `usage_usec` 增量换算 milli-core。
pub fn cpu_milli_from_delta(delta_usec: u64, elapsed_usec: u64) -> u64 {
    if elapsed_usec == 0 {
        return 0;
    }
    // delta/elapsed 是核数；乘 1000 得 milli-core（先乘后除避免整数截断为 0）
    (delta_usec as u128 * 1000 / elapsed_usec as u128) as u64
}

/// 由 `/proc` 的时钟滴答增量换算 milli-core。
pub fn cpu_milli_from_ticks(delta_ticks: u64, elapsed_usec: u64, ticks_per_second: u64) -> u64 {
    if elapsed_usec == 0 || ticks_per_second == 0 {
        return 0;
    }
    let delta_usec = delta_ticks as u128 * 1_000_000 / ticks_per_second as u128;
    (delta_usec * 1000 / elapsed_usec as u128) as u64
}

/// 每秒时钟滴答数（`_SC_CLK_TCK`，失败时回落 100）。
pub fn clock_ticks_per_second() -> u64 {
    // SAFETY: sysconf 是线程安全的只读查询
    let value = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if value <= 0 {
        100
    } else {
        value as u64
    }
}

/// 读 `/proc/<pid>/status` 的 `VmRSS`（KiB）。
pub fn read_proc_rss_kb(pid: i32) -> Option<u64> {
    if pid <= 0 {
        return None;
    }
    let content = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    parse_status_vm_rss_kb(&content)
}

/// 读 `/proc/<pid>/stat` 的 utime+stime（时钟滴答）。
pub fn read_proc_cpu_ticks(pid: i32) -> Option<u64> {
    if pid <= 0 {
        return None;
    }
    let content = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    parse_stat_cpu_ticks(&content)
}

/// 解析 `VmRSS:   12345 kB`。
pub fn parse_status_vm_rss_kb(content: &str) -> Option<u64> {
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok();
        }
    }
    None
}

/// 解析 `/proc/<pid>/stat` 的 utime(14) + stime(15)。
///
/// stat 的第 2 个字段是命令名，可能含空格与括号，因此必须从**最后一个** `)` 之后开始
/// 切分，否则字段会错位。
pub fn parse_stat_cpu_ticks(content: &str) -> Option<u64> {
    let tail = content.rsplit_once(')')?.1;
    let fields: Vec<&str> = tail.split_whitespace().collect();
    // 去掉 ')' 后，utime/stime 是第 12/13 个（原第 14/15 个）
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    Some(utime.saturating_add(stime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cgroup::CgroupManager;
    use crate::registry::LocalDatabase;
    use domain::lifecycle::LifecycleState;
    use domain::time::now_unix_ms;
    use std::path::PathBuf;

    fn capacity() -> WorkerCapacity {
        WorkerCapacity::new(8000, 16384, 4096, 102400, 256, 20000)
    }

    fn disabled_cgroups() -> CgroupManager {
        CgroupManager::new(PathBuf::from("/sys/fs/cgroup"), false)
    }

    #[test]
    fn cpu_conversion_from_cgroup_delta() {
        // 1 秒内累计 0.5 秒 CPU = 0.5 core = 500 milli
        assert_eq!(cpu_milli_from_delta(500_000, 1_000_000), 500);
        // 满核
        assert_eq!(cpu_milli_from_delta(1_000_000, 1_000_000), 1000);
        // 多核（超卖）
        assert_eq!(cpu_milli_from_delta(4_000_000, 1_000_000), 4000);
        // 无间隔不产生除零
        assert_eq!(cpu_milli_from_delta(500_000, 0), 0);
        // 亚毫核的增量按整数截断为 0（1us/1s = 0.001 milli-core，属噪声）
        assert_eq!(cpu_milli_from_delta(1, 1_000_000), 0);
        // 1 秒内积累 1ms CPU = 1 milli-core
        assert_eq!(cpu_milli_from_delta(1_000, 1_000_000), 1);
    }

    #[test]
    fn cpu_conversion_from_proc_ticks() {
        // 100Hz 时钟，1 秒内 50 ticks = 0.5 core
        assert_eq!(cpu_milli_from_ticks(50, 1_000_000, 100), 500);
        assert_eq!(cpu_milli_from_ticks(0, 1_000_000, 100), 0);
        assert_eq!(cpu_milli_from_ticks(50, 1_000_000, 0), 0);
    }

    #[test]
    fn parse_proc_files() {
        let status = "Name:\tdb-runtime\nVmPeak:\t 100 kB\nVmRSS:\t  20480 kB\nThreads:\t7\n";
        assert_eq!(parse_status_vm_rss_kb(status), Some(20_480));
        assert_eq!(parse_status_vm_rss_kb("VmPeak: 1 kB\n"), None);

        // 命令名含空格与括号也不能让字段错位（utime=100, stime=23）
        let stat = "1234 (db runtime (x)) S 1 1234 1234 0 -1 4194560 100 0 0 0 100 23 0 0 20 0 7\n";
        assert_eq!(parse_stat_cpu_ticks(stat), Some(123));
        assert_eq!(parse_stat_cpu_ticks("garbage"), None);
    }

    #[test]
    fn read_self_process_usage_from_proc() {
        let pid = std::process::id() as i32;
        assert!(read_proc_rss_kb(pid).unwrap_or(0) > 0, "自身 RSS 应 > 0");
        assert!(read_proc_cpu_ticks(pid).is_some(), "自身 CPU 计数应可读");
        assert_eq!(read_proc_rss_kb(0), None);
        assert_eq!(read_proc_cpu_ticks(-1), None);
        assert!(clock_ticks_per_second() >= 1);
    }

    #[test]
    fn sampling_uses_cgroup_files_first() {
        let dir = tempfile::tempdir().unwrap();
        let cgroup_dir = dir.path().join("db-1");
        std::fs::create_dir_all(&cgroup_dir).unwrap();
        std::fs::write(
            cgroup_dir.join("memory.current"),
            (7u64 * 1024 * 1024).to_string(),
        )
        .unwrap();
        std::fs::write(cgroup_dir.join("cpu.stat"), "usage_usec 1000\n").unwrap();

        let cgroups = disabled_cgroups();
        let targets = vec![SampleTarget {
            database_id: "db-1".into(),
            // pid 指向自身：若错误地回落到 /proc，RSS 会与 cgroup 值不同
            pid: std::process::id() as i32,
            cgroup: Some(cgroup_dir.clone()),
        }];

        let (per_db, baseline) = sample_processes(&cgroups, targets, HashMap::new());
        assert_eq!(per_db[0].rss_mib, 7, "应优先使用 cgroup memory.current");
        assert_eq!(per_db[0].cpu_milli, 0, "首轮无基准按 0 上报");
        assert_eq!(baseline.get("db-1").unwrap().cgroup_usec, 1000);

        // 第二轮：累计值增加 0.5 core*1s -> 需等待真实时间，这里直接验证换算函数
        let mut previous = HashMap::new();
        previous.insert(
            "db-1".to_string(),
            CpuBaseline {
                cgroup_usec: 1_000,
                proc_ticks: 0,
                at: Instant::now() - std::time::Duration::from_secs(1),
            },
        );
        std::fs::write(cgroup_dir.join("cpu.stat"), "usage_usec 501000\n").unwrap();
        let targets = vec![SampleTarget {
            database_id: "db-1".into(),
            pid: 0,
            cgroup: Some(cgroup_dir),
        }];
        let (per_db, _) = sample_processes(&cgroups, targets, previous);
        assert!(
            per_db[0].cpu_milli >= 400 && per_db[0].cpu_milli <= 600,
            "近似 0.5 core，实测 {}",
            per_db[0].cpu_milli
        );
    }

    #[test]
    fn sampling_falls_back_to_proc_when_cgroup_missing() {
        let cgroups = disabled_cgroups();
        let targets = vec![SampleTarget {
            database_id: "db-1".into(),
            pid: std::process::id() as i32,
            cgroup: None,
        }];
        let (per_db, baseline) = sample_processes(&cgroups, targets, HashMap::new());
        assert!(per_db[0].rss_mib > 0, "/proc 回落应读到自身 RSS");
        assert!(baseline.get("db-1").unwrap().proc_ticks > 0);
    }

    #[tokio::test]
    async fn refresh_reports_budget_sum_as_used() {
        let sampler = ResourceSampler::new(capacity(), disabled_cgroups());
        let registry = LocalDbRegistry::new();
        registry.register(LocalDatabase {
            database_id: "db-1".into(),
            state: LifecycleState::Warm,
            pid: Some(std::process::id() as i32),
            local_socket: PathBuf::from("/run/sockets/db-1.sock"),
            owner_epoch: 1,
            budget: ResourceBudget::new(2000, 512, 64, 1024, 1, 100),
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: None,
            read_only: false,
        });

        let usage = sampler.refresh(&registry).await;
        // 占用是承诺量（预算），不是实测值
        assert_eq!(usage.cpu_milli_used, 2000);
        assert_eq!(usage.memory_mib_used, 512);
        assert_eq!(usage.db_process_count, 1);
        assert_eq!(usage.cpu_milli_total, 8000);
        assert_eq!(sampler.saturation(), 0.25);
        assert!(sampler.rss_mib_of("db-1") > 0);
        assert_eq!(sampler.current().memory_mib_used, 512);
        // 未注册 DB 的实测值为 0
        assert_eq!(sampler.rss_mib_of("db-unknown"), 0);
    }
}
