//! Worker 自身状态机：ACTIVE -> DRAINING -> EMPTY（架构 §12.3）。
//!
//! 语义要点：
//! - 只有 **ACTIVE** 接受新 Placement；DRAINING / EMPTY 一律拒绝（`WORKER_DRAINING`）。
//! - DRAINING 表示「不再接受新 Placement，等待既有 DB 收敛」；`serves_traffic()`
//!   反映的是 **Server 侧路由决策**（DRAINING 的 Worker 不再被路由），本地既有 DB
//!   仍会继续服务在途请求，直到被 Stop / Move 收敛。
//! - EMPTY 表示已排空（无任何本地 DB），可重新被纳入 Placement（-> ACTIVE）。
//! - 实际状态迁移合法性交给 [`domain::lifecycle::WorkerState::can_transition_to`]，
//!   本模块只负责并发保护与「排空完成」的收敛判定，避免自造第二套状态机。

use std::time::{Duration, Instant};

use parking_lot::RwLock;

use domain::lifecycle::WorkerState;

use crate::error::{Result, WorkerError};
use crate::registry::LocalDbRegistry;

/// 进入 EMPTY 之后，要经过多久才会接受「重新接纳」授权（架构 §12.3 的 EMPTY -> ACTIVE）。
///
/// 为什么需要冷却期而不是立刻回归：EMPTY 通常刚刚发生过一次排空（故障迁移、维护、
/// 容量腾挪），刚排空的节点上残留的故障因素（资源抖动、坏盘、网络分区）往往还没消失。
/// 冷却期让控制面有观察窗口，也避免「刚 drain 完就被重新 placement」的抖动循环。
/// 冷却期内节点是空闲的，代价只是最多几十秒的容量延迟，远小于「把 DB 放回一个刚出过问题的
/// 节点再迁一次」的代价。
pub const EMPTY_RESUME_COOLDOWN: Duration = Duration::from_secs(30);

/// 线程安全的 Worker 状态机。
#[derive(Debug)]
pub struct WorkerStateMachine {
    worker_id: String,
    state: RwLock<WorkerState>,
    /// 进入 EMPTY 的时刻（`None` = 当前不在 EMPTY）。重新接纳的冷却期锚点。
    empty_since: RwLock<Option<Instant>>,
}

impl WorkerStateMachine {
    /// 以 ACTIVE 启动。
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            // Worker 进程启动即注册（架构 §12.3 的 ACTIVE 起点）
            state: RwLock::new(WorkerState::Active),
            empty_since: RwLock::new(None),
        }
    }

    /// 当前状态。
    pub fn current(&self) -> WorkerState {
        *self.state.read()
    }

    /// 本节点 worker id。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// 迁移到目标状态（校验合法性）。
    pub fn transition(&self, next: WorkerState) -> Result<WorkerState> {
        let mut guard = self.state.write();
        let current = *guard;
        if current == next {
            return Ok(current);
        }
        if !current.can_transition_to(next) {
            return Err(WorkerError::InvalidState(format!(
                "Worker 状态迁移非法：{current} -> {next}"
            )));
        }
        *guard = next;
        self.stamp_empty_entry(current, next);
        tracing::info!(
            worker_id = %self.worker_id,
            from = %current,
            to = %next,
            "Worker 状态迁移"
        );
        Ok(next)
    }

    /// 维护 EMPTY 的进入时刻（冷却期锚点）。
    ///
    /// 只在**进入** EMPTY 的那一刻打点：离开 EMPTY（重新接纳 / 下线）时清空，
    /// 停留在 EMPTY 期间不刷新 —— 否则冷却期会被心跳反复续命、永远等不到头。
    fn stamp_empty_entry(&self, from: WorkerState, to: WorkerState) {
        let mut empty_since = self.empty_since.write();
        if to == WorkerState::Empty {
            if from != WorkerState::Empty {
                *empty_since = Some(Instant::now());
            }
        } else {
            *empty_since = None;
        }
    }

    /// 距进入 EMPTY 已有多久（不在 EMPTY 时为 `None`）。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn emptied_for(&self) -> Option<Duration> {
        self.empty_since.read().map(|since| since.elapsed())
    }

    /// 是否接受新 Placement（仅 ACTIVE）。
    ///
    /// 注意：这是「新 DB」的门槛；已在本地运行的 DB（重启 / Move 完成）不受它限制，
    /// 否则 DRAINING 期间的迁移收尾会被自己拒绝。
    pub fn accepts_new_placement(&self) -> bool {
        self.current().accepts_new_placement()
    }

    /// 是否仍可承载既有 DB 流量。
    pub fn serves_traffic(&self) -> bool {
        self.current().serves_traffic()
    }

    /// 校验本节点可以接受新 Placement，否则返回 `WORKER_DRAINING`。
    pub fn ensure_accepts_new_placement(&self) -> Result<()> {
        if self.accepts_new_placement() {
            return Ok(());
        }
        Err(WorkerError::Draining {
            worker_id: self.worker_id.clone(),
            state: self.current().to_db_str().to_string(),
        })
    }

    /// 请求排空（Server 下发 DrainWorker 或心跳发现 Server 要求 draining）。
    pub fn begin_drain(&self) -> Result<WorkerState> {
        let current = self.current();
        if current == WorkerState::Draining || current == WorkerState::Empty {
            // 幂等：重复 drain 不报错
            return Ok(current);
        }
        self.transition(WorkerState::Draining)
    }

    /// 排空收敛判定：处于 DRAINING 且本地已无任何 DB 时迁移到 EMPTY。
    ///
    /// 返回是否发生了迁移。EMPTY 是「可以安全下线」的信号。
    pub fn settle_if_drained(&self, registry: &LocalDbRegistry) -> bool {
        if self.current() != WorkerState::Draining || !registry.is_empty() {
            return false;
        }
        match self.transition(WorkerState::Empty) {
            Ok(_) => true,
            Err(err) => {
                tracing::warn!(error = %err, "排空收敛失败");
                false
            }
        }
    }

    /// EMPTY -> ACTIVE 的回归路径（架构 §12.3）：**只**在 Server 明确授权（心跳回执
    /// `accept_new_placement`）时由调用方触发。
    ///
    /// 三个必要条件，全部满足才迁移，返回是否发生了迁移：
    ///
    /// 1. 本节点当前处于 EMPTY —— 排空已完成的节点才谈得上「重新接纳」；
    /// 2. 本地注册表为空 —— 排空之后又冒出来的 DB（例如 Server 提前下达的恢复）
    ///    说明本地并不空闲，此时回归会把「已排空」这个前提弄脏；
    /// 3. 距进入 EMPTY 已超过 [`EMPTY_RESUME_COOLDOWN`]，且 `healthy`（调用方按
    ///    资源水位判定，见 db-worker::heartbeat）。
    ///
    /// 为什么必须由 Server 授权、而不是 Worker 自行回归：EMPTY 是**控制面下达的排空**
    /// 的结果，是否重新纳入集群属于控制面的容量决策（架构 §12.3）。Worker 自己拍板会
    /// 让「运维刚排空一个节点，它立刻又接活」变成可能。
    pub fn try_resume_from_empty(
        &self,
        registry: &LocalDbRegistry,
        healthy: bool,
        now: Instant,
    ) -> bool {
        if self.current() != WorkerState::Empty {
            return false;
        }
        if !registry.is_empty() {
            tracing::debug!(
                worker_id = %self.worker_id,
                databases = registry.len(),
                "收到重新接纳授权，但本地仍有 DB：保持 EMPTY"
            );
            return false;
        }
        if !healthy {
            tracing::debug!(
                worker_id = %self.worker_id,
                "收到重新接纳授权，但资源水位不健康：保持 EMPTY"
            );
            return false;
        }
        match *self.empty_since.read() {
            Some(since) if now.saturating_duration_since(since) >= EMPTY_RESUME_COOLDOWN => {}
            Some(since) => {
                tracing::debug!(
                    worker_id = %self.worker_id,
                    emptied_for_ms = now.saturating_duration_since(since).as_millis() as u64,
                    cooldown_ms = EMPTY_RESUME_COOLDOWN.as_millis() as u64,
                    "收到重新接纳授权，但冷却期未满：保持 EMPTY"
                );
                return false;
            }
            // 理论上不可达（EMPTY 一定打过点）；保守起见视为冷却期未满。
            None => return false,
        }

        match self.transition(WorkerState::Active) {
            Ok(_) => {
                tracing::info!(
                    worker_id = %self.worker_id,
                    "Server 已授权重新接纳，EMPTY -> ACTIVE（可接受新 Placement）"
                );
                true
            }
            Err(err) => {
                tracing::warn!(error = %err, "从 EMPTY 回归 ACTIVE 失败");
                false
            }
        }
    }

    /// proto 枚举值。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn to_proto_i32(&self) -> i32 {
        self.current().to_proto_i32()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::LocalDatabase;
    use domain::error::ErrorCode;
    use domain::lifecycle::LifecycleState;
    use domain::resources::ResourceBudget;
    use domain::time::now_unix_ms;
    use std::path::PathBuf;

    fn sample_db(db_id: &str) -> LocalDatabase {
        LocalDatabase {
            database_id: db_id.to_string(),
            state: LifecycleState::Warm,
            pid: Some(1),
            local_socket: PathBuf::from("/run/sockets/db.sock"),
            owner_epoch: 1,
            budget: ResourceBudget::ZERO,
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: None,
            read_only: false,
        }
    }

    #[test]
    fn active_accepts_placement() {
        let sm = WorkerStateMachine::new("worker-1");
        assert_eq!(sm.current(), WorkerState::Active);
        assert!(sm.accepts_new_placement());
        assert!(sm.ensure_accepts_new_placement().is_ok());
    }

    #[test]
    fn draining_rejects_new_placement() {
        let sm = WorkerStateMachine::new("worker-1");
        sm.begin_drain().unwrap();
        assert_eq!(sm.current(), WorkerState::Draining);
        assert!(!sm.accepts_new_placement());
        // Server 侧不再把新流量路由到 DRAINING 的 Worker（既有 DB 仍在本机运行）
        assert!(!sm.serves_traffic());

        let err = sm.ensure_accepts_new_placement().unwrap_err();
        assert_eq!(err.code(), ErrorCode::WorkerDraining);

        // 幂等
        assert_eq!(sm.begin_drain().unwrap(), WorkerState::Draining);
    }

    #[test]
    fn drain_settles_to_empty_only_when_registry_drained() {
        let sm = WorkerStateMachine::new("worker-1");
        let registry = LocalDbRegistry::new();
        registry.register(sample_db("db-1"));

        sm.begin_drain().unwrap();
        assert!(!sm.settle_if_drained(&registry));
        assert_eq!(sm.current(), WorkerState::Draining);

        registry.remove("db-1");
        assert!(sm.settle_if_drained(&registry));
        assert_eq!(sm.current(), WorkerState::Empty);
        // EMPTY 同样不接受新 Placement，直到被重新激活
        assert!(!sm.accepts_new_placement());

        sm.transition(WorkerState::Active).unwrap();
        assert!(sm.accepts_new_placement());
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        let sm = WorkerStateMachine::new("worker-1");
        // ACTIVE -> EMPTY 非法（必须先 DRAINING）
        let err = sm.transition(WorkerState::Empty).unwrap_err();
        assert_eq!(err.code(), ErrorCode::DatabaseNotReady);
        assert_eq!(sm.current(), WorkerState::Active);
    }

    /// 把状态机推进到「已排空（EMPTY）」。
    fn drained(sm: &WorkerStateMachine, registry: &LocalDbRegistry) {
        sm.begin_drain().unwrap();
        assert!(sm.settle_if_drained(registry));
        assert_eq!(sm.current(), WorkerState::Empty);
    }

    /// 缺陷 1 的回归测试：EMPTY 必须有回归路径（Server 授权 + 冷却期 + 健康 + 空闲）。
    #[test]
    fn empty_resumes_to_active_when_authorized_and_healthy() {
        let sm = WorkerStateMachine::new("worker-1");
        let registry = LocalDbRegistry::new();
        drained(&sm, &registry);
        let now = Instant::now();

        // 冷却期未满：授权到了也不能回归（否则「刚 drain 完立刻接活」）
        assert!(!sm.try_resume_from_empty(&registry, true, now));
        assert_eq!(sm.current(), WorkerState::Empty);

        // 资源不健康：同样不回归
        let after_cooldown = now + EMPTY_RESUME_COOLDOWN + Duration::from_millis(1);
        assert!(!sm.try_resume_from_empty(&registry, false, after_cooldown));
        assert_eq!(sm.current(), WorkerState::Empty);

        // 冷却期满 + 健康 + 空闲 -> EMPTY -> ACTIVE，且立刻可以接新 Placement
        assert!(sm.try_resume_from_empty(&registry, true, after_cooldown));
        assert_eq!(sm.current(), WorkerState::Active);
        assert!(sm.accepts_new_placement());
        assert!(sm.ensure_accepts_new_placement().is_ok());
        // 离开 EMPTY 后冷却期锚点被清掉，不会残留到下一次排空
        assert!(sm.emptied_for().is_none());
        assert!(!sm.try_resume_from_empty(&registry, true, after_cooldown));
    }

    /// 本地还有 DB 时不得回归：EMPTY 的前提就是「一个 DB 都没有」。
    #[test]
    fn empty_stays_empty_while_local_databases_remain() {
        let sm = WorkerStateMachine::new("worker-1");
        let registry = LocalDbRegistry::new();
        drained(&sm, &registry);
        registry.register(sample_db("db-late"));

        let now = Instant::now() + EMPTY_RESUME_COOLDOWN + Duration::from_millis(1);
        assert!(!sm.try_resume_from_empty(&registry, true, now));
        assert_eq!(sm.current(), WorkerState::Empty);
        assert!(!sm.accepts_new_placement());
    }

    /// §12.3：DRAINING 期间不得接新 Placement —— 回归路径对 DRAINING 无效，
    /// 只有真正排空（EMPTY）之后才谈得上重新接纳。
    #[test]
    fn resume_is_rejected_while_draining() {
        let sm = WorkerStateMachine::new("worker-1");
        let registry = LocalDbRegistry::new();
        sm.begin_drain().unwrap();
        let registry_with_db = LocalDbRegistry::new();
        registry_with_db.register(sample_db("db-1"));

        let now = Instant::now() + EMPTY_RESUME_COOLDOWN + Duration::from_millis(1);
        // DRAINING 且仍有本地 DB：不迁移
        assert!(!sm.try_resume_from_empty(&registry_with_db, true, now));
        assert_eq!(sm.current(), WorkerState::Draining);
        // DRAINING 且已空：也不得跳过 EMPTY 直接回到 ACTIVE（必须先收敛到 EMPTY）
        assert!(!sm.try_resume_from_empty(&registry, true, now));
        assert_eq!(sm.current(), WorkerState::Draining);
        assert!(!sm.accepts_new_placement());
    }
}
