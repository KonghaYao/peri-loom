//! 本地 DB 注册表与 Epoch Fencing（架构 §5.2 / §10 / §11.3）。
//!
//! 注册表是 Worker 的**权威本地视图**：`db_id -> {state, pid, local_socket,
//! owner_epoch, started_at, last_activity, crash_count}`。所有数据面请求在转发给 DB
//! Process 之前必须经过本表校验 epoch；所有控制面指令在改变进程状态之前同样必须校验。
//!
//! 为什么 epoch 校验必须分两种语义：
//! - **数据面**（Execute / Session / …）：请求必须携带**精确等于**本地 epoch 的值。
//!   高于本地说明本 Worker 已被新 Owner 取代（Split Brain 防护，架构 §10），
//!   低于本地说明请求来自过期路由。两者都不能被当作有效请求。
//! - **控制面**（Start / Restart / FinalizeMove）：新 Owner 接管时 epoch 会**变大**，
//!   因此只在「低于本地」时拒绝（fencing），允许更高的 epoch 建立新所有权。
//!
//! 注册表本身不持有进程句柄（句柄在 [`crate::supervisor`]），只保存元数据，
//! 因此可以独立单测。

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use domain::lifecycle::LifecycleState;
use domain::resources::ResourceBudget;
use domain::time::now_unix_ms;

use crate::error::{Result, WorkerError};

/// 本地 DB 注册项。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalDatabase {
    /// 数据库 id（原始值，未消毒）。
    pub database_id: String,
    /// 生命周期状态。
    pub state: LifecycleState,
    /// DB Process PID；COLD 时为 `None`。
    pub pid: Option<i32>,
    /// 本地 UDS 路径。
    pub local_socket: PathBuf,
    /// 当前 Owner Epoch（fencing 依据）。
    pub owner_epoch: u64,
    /// 本次启动时下发的资源预算（用于准入累加）。
    pub budget: ResourceBudget,
    /// 启动时刻（Unix 毫秒）。
    pub started_at_unix_ms: i64,
    /// 最近一次数据面活动时刻（Unix 毫秒），用于空闲回收决策。
    pub last_activity_unix_ms: i64,
    /// 本 epoch 内的崩溃次数（自动重启累计）。
    pub crash_count: u32,
    /// per-DB cgroup 路径（降级时为 `None`）。
    pub cgroup: Option<PathBuf>,
    /// 工作集来源（恢复信息），仅用于诊断与上报。
    pub restored_from: Option<RestoredFrom>,
    /// 进程由本次 Placement 拉起，还是仅为 Move 预热（prepare_only）。
    pub read_only: bool,
}

/// 工作集恢复来源（诊断用）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoredFrom {
    /// 快照 id；空表示「无快照，直接回放远端 WAL / 使用本地工作集」。
    pub snapshot_id: String,
    /// 快照基线 LSN。
    pub base_lsn: u64,
    /// 回放后本地已持久化的 LSN。
    pub applied_lsn: u64,
}

impl LocalDatabase {
    /// 该 DB 是否可服务数据面请求。
    pub fn is_serving(&self) -> bool {
        self.state.is_serving() && self.pid.is_some()
    }
}

/// 本地 DB 注册表。
#[derive(Debug, Default)]
pub struct LocalDbRegistry {
    entries: DashMap<String, LocalDatabase>,
    /// 单调递增的本地版本号：每次状态变化 +1（心跳据此让 Server 判断是否需要全量拉取）。
    inventory_version: AtomicU64,
}

impl LocalDbRegistry {
    /// 空注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前 inventory 版本号。
    pub fn inventory_version(&self) -> u64 {
        self.inventory_version.load(Ordering::Acquire)
    }

    /// 状态变更后递增版本号，返回新值。
    fn bump_inventory(&self) -> u64 {
        self.inventory_version.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// 注册（或覆盖）一个 DB 项。覆盖等价于新 epoch 接管，会递增版本号。
    pub fn register(&self, entry: LocalDatabase) {
        self.entries.insert(entry.database_id.clone(), entry);
        self.bump_inventory();
    }

    /// 查询。
    pub fn get(&self, db_id: &str) -> Option<LocalDatabase> {
        self.entries.get(db_id).map(|entry| entry.clone())
    }

    /// 是否存在。
    pub fn contains(&self, db_id: &str) -> bool {
        self.entries.contains_key(db_id)
    }

    /// 注销（进程已退出 / 已迁移）。返回被移除项。
    pub fn remove(&self, db_id: &str) -> Option<LocalDatabase> {
        let removed = self.entries.remove(db_id).map(|(_, entry)| entry);
        if removed.is_some() {
            self.bump_inventory();
        }
        removed
    }

    /// 全部注册项（顺序不确定，调用方需自行排序以便对比）。
    pub fn snapshot(&self) -> Vec<LocalDatabase> {
        let mut items: Vec<LocalDatabase> =
            self.entries.iter().map(|entry| entry.clone()).collect();
        items.sort_by(|a, b| a.database_id.cmp(&b.database_id));
        items
    }

    /// 注册项数量。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空（Drain 收敛到 EMPTY 的判据之一）。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 处于可服务状态的 DB 数量。
    pub fn serving_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.is_serving())
            .count()
    }

    /// 已占用的资源（各 DB 预算之和）。
    ///
    /// 用预算而不是实测值：Scheduler 的 Resource Budget Packing 依赖「承诺量」，
    /// 实测值会随负载抖动，导致打包判定不可复现（架构 §9）。
    ///
    /// COLD 项不计入：它没有进程，也就不占用 CPU / 内存 / 进程位（架构 §7）。
    pub fn used_budget(&self) -> ResourceBudget {
        let mut used = ResourceBudget::ZERO;
        for entry in self.entries.iter() {
            if !entry.state.occupies_process() {
                continue;
            }
            used = used.saturating_add(&entry.budget);
        }
        used
    }

    /// 是否还有可服务的 DB（Drain 收敛判据）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn has_serving(&self) -> bool {
        self.serving_count() > 0
    }

    /// 数据面 epoch 校验：必须精确匹配。
    ///
    /// 返回注册项快照；未注册、过期、被取代三种情况都返回明确错误，
    /// 便于 Server 侧区分「刷新路由重试」与「彻底失败」。
    pub fn check_data_epoch(&self, db_id: &str, epoch: u64) -> Result<LocalDatabase> {
        let entry = self.entries.get(db_id).map(|e| e.clone()).ok_or_else(|| {
            // 数据面命中未注册 DB = 请求被路由到了错误的 Worker（路由过期）
            WorkerError::NotOwner {
                db_id: db_id.to_string(),
                requested: epoch,
                local: 0,
            }
        })?;
        Self::compare_epoch(db_id, epoch, entry.owner_epoch)?;
        Ok(entry)
    }

    /// 数据面前置校验：DB 已注册、epoch 一致且当前可服务（WARM / HOT + 有进程）。
    ///
    /// 与 [`LocalDbRegistry::check_data_epoch`] 的区别：本方法额外要求进程确实存在，
    /// 用于「准备转发请求」的场景，避免把请求发给一个正在启动/已崩溃的 DB。
    pub fn check_serving(&self, db_id: &str) -> Result<LocalDatabase> {
        let entry =
            self.entries
                .get(db_id)
                .map(|e| e.clone())
                .ok_or_else(|| WorkerError::NotOwner {
                    db_id: db_id.to_string(),
                    requested: 0,
                    local: 0,
                })?;
        if !entry.is_serving() {
            return Err(WorkerError::InvalidState(format!(
                "db={db_id} 当前状态 {} 不可服务（pid={:?}）",
                entry.state, entry.pid
            )));
        }
        Ok(entry)
    }

    /// 控制面 epoch 校验：允许「更大的 epoch 接管」，拒绝回退。
    ///
    /// 返回 `Some(entry)` 表示该 DB 已在本节点注册（调用方据此判断是否幂等/接管），
    /// `None` 表示本节点从未持有该 DB。
    pub fn check_command_epoch(&self, db_id: &str, epoch: u64) -> Result<Option<LocalDatabase>> {
        match self.entries.get(db_id).map(|e| e.clone()) {
            None => Ok(None),
            Some(entry) => {
                if epoch < entry.owner_epoch {
                    return Err(WorkerError::EpochStale {
                        db_id: db_id.to_string(),
                        requested: epoch,
                        local: entry.owner_epoch,
                    });
                }
                Ok(Some(entry))
            }
        }
    }

    /// 单纯比较两个 epoch 的 fencing 语义。
    fn compare_epoch(db_id: &str, requested: u64, local: u64) -> Result<()> {
        if requested == local {
            return Ok(());
        }
        if requested < local {
            Err(WorkerError::EpochStale {
                db_id: db_id.to_string(),
                requested,
                local,
            })
        } else {
            Err(WorkerError::NotOwner {
                db_id: db_id.to_string(),
                requested,
                local,
            })
        }
    }

    /// 状态迁移（校验合法性）。返回是否发生变更。
    ///
    /// 非法转换被拒绝并记录（不 panic）：状态机是安全属性，宁可停在原状态。
    pub fn transition(&self, db_id: &str, next: LifecycleState) -> bool {
        let mut entry = match self.entries.get_mut(db_id) {
            Some(entry) => entry,
            None => return false,
        };
        if entry.state == next {
            return false;
        }
        if !entry.state.can_transition_to(next) {
            tracing::warn!(
                db_id = %db_id,
                from = %entry.state,
                to = %next,
                "拒绝非法的生命周期状态迁移"
            );
            return false;
        }
        entry.state = next;
        drop(entry);
        self.bump_inventory();
        true
    }

    /// 记录数据面活动时间（用于空闲回收与热度判定）。
    pub fn mark_activity(&self, db_id: &str) {
        if let Some(mut entry) = self.entries.get_mut(db_id) {
            entry.last_activity_unix_ms = now_unix_ms();
        }
    }

    /// 记录一次进程崩溃，返回累计次数。
    pub fn record_crash(&self, db_id: &str) -> u32 {
        match self.entries.get_mut(db_id) {
            Some(mut entry) => {
                entry.crash_count = entry.crash_count.saturating_add(1);
                entry.crash_count
            }
            None => 0,
        }
    }

    /// 更新 PID（重启后 PID 会变）。
    pub fn set_pid(&self, db_id: &str, pid: i32, cgroup: Option<PathBuf>) {
        if let Some(mut entry) = self.entries.get_mut(db_id) {
            entry.pid = Some(pid);
            entry.cgroup = cgroup;
            entry.started_at_unix_ms = now_unix_ms();
        }
        self.bump_inventory();
    }

    /// 清空 PID 与 cgroup（进程已退出）。
    pub fn clear_process(&self, db_id: &str) {
        if let Some(mut entry) = self.entries.get_mut(db_id) {
            entry.pid = None;
            entry.cgroup = None;
        }
        self.bump_inventory();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::error::ErrorCode;

    fn entry(db_id: &str, epoch: u64) -> LocalDatabase {
        LocalDatabase {
            database_id: db_id.to_string(),
            state: LifecycleState::Warm,
            pid: Some(4242),
            local_socket: PathBuf::from(format!("/run/sockets/{db_id}.sock")),
            owner_epoch: epoch,
            budget: ResourceBudget::new(1000, 256, 64, 1024, 1, 100),
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: None,
            read_only: false,
        }
    }

    #[test]
    fn register_get_remove_bumps_inventory_version() {
        let registry = LocalDbRegistry::new();
        assert_eq!(registry.inventory_version(), 0);
        registry.register(entry("db-1", 1));
        assert_eq!(registry.inventory_version(), 1);
        assert!(registry.contains("db-1"));
        assert_eq!(registry.get("db-1").unwrap().owner_epoch, 1);

        registry.register(entry("db-1", 2));
        assert_eq!(registry.inventory_version(), 2);
        assert_eq!(registry.get("db-1").unwrap().owner_epoch, 2);

        assert!(registry.remove("db-1").is_some());
        assert_eq!(registry.inventory_version(), 3);
        assert!(registry.remove("db-1").is_none());
        assert_eq!(registry.inventory_version(), 3);
        assert!(registry.is_empty());
    }

    #[test]
    fn data_path_requires_exact_epoch() {
        let registry = LocalDbRegistry::new();
        registry.register(entry("db-1", 834));

        assert!(registry.check_data_epoch("db-1", 834).is_ok());

        // 旧 epoch = 过期 Owner
        let stale = registry.check_data_epoch("db-1", 833).unwrap_err();
        assert_eq!(stale.code(), ErrorCode::EpochMismatch);

        // 新 epoch = 本节点已被取代（Split Brain 防护）
        let superseded = registry.check_data_epoch("db-1", 835).unwrap_err();
        assert_eq!(superseded.code(), ErrorCode::NotOwner);

        // 未注册 = 路由到了错误的 Worker
        let unregistered = registry.check_data_epoch("db-9", 1).unwrap_err();
        assert_eq!(unregistered.code(), ErrorCode::NotOwner);
    }

    #[test]
    fn control_path_allows_new_ownership_but_not_regression() {
        let registry = LocalDbRegistry::new();
        // 未注册：允许（首次 Placement）
        assert!(registry.check_command_epoch("db-1", 1).unwrap().is_none());

        registry.register(entry("db-1", 834));
        // 同 epoch：幂等
        assert!(registry.check_command_epoch("db-1", 834).unwrap().is_some());
        // 更高 epoch：新 Owner 接管，允许
        assert_eq!(
            registry
                .check_command_epoch("db-1", 835)
                .unwrap()
                .unwrap()
                .owner_epoch,
            834
        );
        // 更低 epoch：fencing 拒绝
        assert_eq!(
            registry
                .check_command_epoch("db-1", 833)
                .unwrap_err()
                .code(),
            ErrorCode::EpochMismatch
        );
    }

    #[test]
    fn lifecycle_transitions_are_validated() {
        let registry = LocalDbRegistry::new();
        registry.register(entry("db-1", 1)); // WARM

        assert!(registry.transition("db-1", LifecycleState::Hot));
        assert!(!registry.transition("db-1", LifecycleState::Hot)); // 同状态不算转换
        assert!(registry.transition("db-1", LifecycleState::Draining));
        assert!(registry.transition("db-1", LifecycleState::Stopping));
        // STOPPING -> WARM 非法（必须先 COLD/FAILED）
        assert!(!registry.transition("db-1", LifecycleState::Warm));
        assert!(registry.transition("db-1", LifecycleState::Cold));
        // COLD -> HOT 非法（必须经 STARTING）
        assert!(!registry.transition("db-1", LifecycleState::Hot));
        assert_eq!(registry.get("db-1").unwrap().state, LifecycleState::Cold);
    }

    #[test]
    fn used_budget_sums_registered_databases() {
        let registry = LocalDbRegistry::new();
        registry.register(entry("db-1", 1));
        let mut second = entry("db-2", 1);
        second.budget = ResourceBudget::new(500, 128, 32, 0, 1, 0);
        registry.register(second);

        let used = registry.used_budget();
        assert_eq!(used.cpu_milli, 1500);
        assert_eq!(used.memory_mib, 384);
        assert_eq!(used.process_slots, 2);
        assert_eq!(registry.serving_count(), 2);
    }

    #[test]
    fn crash_count_and_pid_updates() {
        let registry = LocalDbRegistry::new();
        registry.register(entry("db-1", 1));
        assert_eq!(registry.record_crash("db-1"), 1);
        assert_eq!(registry.record_crash("db-1"), 2);
        registry.set_pid("db-1", 99, Some(PathBuf::from("/sys/fs/cgroup/x")));
        let item = registry.get("db-1").unwrap();
        assert_eq!(item.pid, Some(99));
        assert_eq!(item.crash_count, 2);
        registry.clear_process("db-1");
        assert_eq!(registry.get("db-1").unwrap().pid, None);
    }
}
