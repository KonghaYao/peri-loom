//! Process Supervisor：把 DB Process 当作**普通子进程**管理（架构 §12.1 / §17.6）。
//!
//! 职责边界：
//!
//! ```text
//! spawn 子进程（DB_RUNTIME_BIN）
//!   ├── 写入 per-DB cgroup（memory.max / cpu.max / pids.max）
//!   ├── pidfd 跟踪（无 PID 复用竞态的信号投递）
//!   ├── 退出检测（子进程 wait + pidfd 双通道，<= 500ms）
//!   └── Crash 处理（清本地 Route / 指标 / 有界自动重启）
//! ```
//!
//! 关键设计：
//! - **Child 由独立的 reaper 任务独占**（只做 `child.wait()`），监视任务通过 oneshot
//!   拿到退出状态。这样信号投递完全不依赖 `&mut Child`，也就不存在 `select!` 里
//!   `Child::wait` 被取消带来的状态问题。
//! - **信号一律优先走 pidfd**：先 SIGTERM（给引擎 flush 机会），超时再 SIGKILL；
//!   pidfd 不可用时退化为按 PID 发信号（老内核）。
//! - **自动重启有界**（默认 3 次，退避 100/200/400ms）：满足 §16「Crash 后 1s 内可服务」，
//!   同时避免「启动即崩」的 DB 把 Worker 打进无限重启循环。
//! - **正常停止（Stop/Kill 指令）不触发重启、不计崩溃指标**：运维回收不能污染告警。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use domain::lifecycle::LifecycleState;
use domain::resources::ResourceBudget;
use domain::time::now_unix_ms;
use protocol::runtime_local as rt;

use crate::cgroup::CgroupManager;
use crate::cli::WorkerConfig;
use crate::error::{Result, WorkerError};
use crate::metrics;
use crate::pidfd::{self, PidFd, ProcessSignal};
use crate::registry::{LocalDatabase, LocalDbRegistry, RestoredFrom};
use crate::restore::{PrepareWork, PreparedWorkSet, SnapshotSource, WorkSetPreparer};
use crate::uds::{DbConnectionPool, UdsConnection};

/// 启动一个 DB 进程所需的信息。
#[derive(Clone, Debug)]
pub struct StartSpec {
    /// 数据库 id。
    pub database_id: String,
    /// Owner epoch（fencing 依据）。
    pub owner_epoch: u64,
    /// 资源预算。
    pub budget: ResourceBudget,
    /// 显式快照来源（Move / 指定恢复点）。
    pub snapshot: Option<SnapshotSource>,
    /// 是否允许复用本地工作集（崩溃重启为 true；接管新 epoch 为 false）。
    pub allow_local_reuse: bool,
    /// 是否允许在没有快照时从 Remote WAL 可见起点回放。
    pub allow_wal_only: bool,
    /// 只读启动（Move 预拉取或降级只读）。
    pub read_only: bool,
    /// 启动 deadline（含恢复与 READY 等待）。
    pub deadline: Option<Instant>,
}

impl StartSpec {
    /// 常规启动（无显式快照、允许复用本地工作集）。
    pub fn new(database_id: impl Into<String>, owner_epoch: u64, budget: ResourceBudget) -> Self {
        Self {
            database_id: database_id.into(),
            owner_epoch,
            budget,
            snapshot: None,
            allow_local_reuse: true,
            allow_wal_only: true,
            read_only: false,
            deadline: None,
        }
    }

    /// 设置 deadline。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn with_deadline(mut self, deadline: Option<Instant>) -> Self {
        self.deadline = deadline;
        self
    }
}

/// 启动结果。
#[derive(Clone, Debug)]
pub struct StartOutcome {
    /// 进程 PID。
    pub pid: i32,
    /// 本地 UDS 路径。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub socket: PathBuf,
    /// per-DB cgroup（降级时为 None）。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub cgroup: Option<PathBuf>,
    /// 工作集准备结果。
    #[allow(dead_code)]
    // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub prepared: PreparedWorkSet,
}

/// 进程退出原因（用于指标标签与日志）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExitCause {
    /// 正常退出（exit code = 0）。
    Exited,
    /// 非零退出码。
    ExitCode(i32),
    /// 被信号杀死。
    Signal(i32),
    /// 无法取得退出状态。
    Unknown,
}

impl ExitCause {
    /// 是否属于异常退出（崩溃）。
    pub fn is_abnormal(&self) -> bool {
        !matches!(self, ExitCause::Exited)
    }

    /// 指标标签。
    pub fn label(&self) -> &'static str {
        match self {
            ExitCause::Exited => "exited",
            ExitCause::ExitCode(_) => "exit_code",
            ExitCause::Signal(_) => "signal",
            ExitCause::Unknown => "unknown",
        }
    }
}

/// 进程监视任务收到的指令。
#[derive(Debug)]
enum ProcessCommand {
    /// 优雅 / 强制停止（等待进程真正退出后回复）。
    Stop {
        graceful: bool,
        deadline: Instant,
        reply: oneshot::Sender<Result<()>>,
    },
    /// 立即 SIGKILL。
    Kill {
        reason: String,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// 进程的共享状态（supervisor 与监视任务都能看到）。
#[derive(Debug)]
struct ProcessShared {
    database_id: String,
    owner_epoch: u64,
    budget: ResourceBudget,
    snapshot: Option<SnapshotSource>,
    read_only: bool,
    /// 正常停止请求：置位后退出不再触发自动重启、不计崩溃。
    stop_requested: AtomicBool,
    /// 已自动重启次数。
    restart_count: AtomicU32,
    /// 当前 PID 的 pidfd（重启后会替换）。
    pidfd: Mutex<Option<Arc<PidFd>>>,
    /// 当前 PID。
    pid: Mutex<i32>,
    /// 当前 cgroup 路径。
    cgroup: Mutex<Option<PathBuf>>,
}

impl ProcessShared {
    fn mark_stop_requested(&self) {
        self.stop_requested.store(true, Ordering::Release);
    }

    fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }
}

/// 注册表中的进程条目。
///
/// `Clone` 是必需的：查询路径（[`ProcessSupervisor::start`]）要先取一份快照再决定
/// 是幂等返回还是停旧进程，不能在持有 `procs` 锁的情况下 await。
#[derive(Debug, Clone)]
struct ManagedEntry {
    shared: Arc<ProcessShared>,
    tx: mpsc::Sender<ProcessCommand>,
    pid: i32,
}

/// DB Process 管理器。
#[derive(Debug)]
pub struct ProcessSupervisor {
    cfg: Arc<WorkerConfig>,
    registry: Arc<LocalDbRegistry>,
    pool: Arc<DbConnectionPool>,
    work_sets: WorkSetPreparer,
    cgroups: CgroupManager,
    procs: Mutex<HashMap<String, ManagedEntry>>,
    /// 进程级关停标记：置位后不再自动重启。
    shutting_down: AtomicBool,
}

impl ProcessSupervisor {
    /// 构造（返回 `Arc`：监视任务需要持有 supervisor 以便崩溃后重启）。
    pub fn new(
        cfg: Arc<WorkerConfig>,
        registry: Arc<LocalDbRegistry>,
        pool: Arc<DbConnectionPool>,
        work_sets: WorkSetPreparer,
    ) -> Arc<Self> {
        let cgroups = CgroupManager::new(cfg.cgroup_root.clone(), !cfg.cgroup_disabled);
        cgroups.enable_controllers();
        Arc::new(Self {
            cfg,
            registry,
            pool,
            work_sets,
            cgroups,
            procs: Mutex::new(HashMap::new()),
            shutting_down: AtomicBool::new(false),
        })
    }

    /// cgroup 管理器（诊断 / 资源采集复用）。
    pub fn cgroups(&self) -> &CgroupManager {
        &self.cgroups
    }

    /// 工作集准备器。
    pub fn work_sets(&self) -> &WorkSetPreparer {
        &self.work_sets
    }

    /// 配置。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn config(&self) -> &WorkerConfig {
        &self.cfg
    }

    /// 某个 DB 是否由本节点管理且进程在跑。
    pub fn is_running(&self, db_id: &str) -> bool {
        self.procs.lock().contains_key(db_id)
    }

    /// 当前在跑的进程数。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn running_count(&self) -> usize {
        self.procs.lock().len()
    }

    /// 当前在跑的进程快照 `(db_id, pid)`。
    #[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
    pub fn running(&self) -> Vec<(String, i32)> {
        let mut items: Vec<(String, i32)> = self
            .procs
            .lock()
            .iter()
            .map(|(db_id, entry)| (db_id.clone(), entry.pid))
            .collect();
        items.sort();
        items
    }

    /// 启动一个 DB 进程（含工作集准备与 READY 等待）。
    ///
    /// deadline 语义：整个「准备 + spawn + READY」必须在 deadline 内完成，超时返回
    /// [`domain::error::ErrorCode::WakeupTimeout`]，并且**必须清理掉半启动的进程**。
    ///
    /// 取 `&Arc<Self>` 而不是 `&self`：spawn 后要把 supervisor 交给监视任务（崩溃
    /// 重启需要它），因此这里必须能克隆出一个 `Arc`。
    pub async fn start(self: &Arc<Self>, spec: StartSpec) -> Result<StartOutcome> {
        let started = Instant::now();
        let db_id = spec.database_id.clone();

        // 抢占式检查 deadline：已经过期就不必再折腾文件与进程
        if let Some(deadline) = spec.deadline {
            if Instant::now() >= deadline {
                metrics::record_db_start("timeout", 0);
                return Err(WorkerError::WakeupTimeout(format!(
                    "db={db_id} 启动前已超过 deadline"
                )));
            }
        }

        // 已在运行：同 epoch 视为幂等成功，不同 epoch 必须先停旧进程。
        // 拷贝一份快照后立刻释放锁：`if let` 的临时值会存活到整个 if 块结束，
        // 而块内有 await，parking_lot 的 guard 不是 Send，会把整个 future 拖成非 Send。
        let running = self.procs.lock().get(&db_id).cloned();
        if let Some(entry) = running {
            if entry.shared.owner_epoch == spec.owner_epoch {
                let socket = self.cfg.socket_path(&db_id);
                tracing::info!(db_id = %db_id, pid = entry.pid, "DB 进程已在运行，按幂等处理");
                return Ok(StartOutcome {
                    pid: entry.pid,
                    socket,
                    cgroup: entry.shared.cgroup.lock().clone(),
                    prepared: PreparedWorkSet {
                        snapshot_id: String::new(),
                        base_lsn: 0,
                        applied_lsn: 0,
                        reused_local: true,
                        downloaded_snapshot: false,
                    },
                });
            }
            // 旧 epoch 的进程仍在：这是接管场景，先停掉（调用方已通过 fence 校验）
            self.stop(&db_id, true, Instant::now() + self.cfg.runtime_stop_grace)
                .await?;
        }

        // 工作集准备（快照 + WAL 回放）也必须在 deadline 内完成。
        // 请求体必须先落到局部变量：future 借用它，而 await 发生在后面的另一个语句里。
        let work = PrepareWork {
            database_id: db_id.clone(),
            owner_epoch: spec.owner_epoch,
            snapshot: spec.snapshot.clone(),
            allow_local_reuse: spec.allow_local_reuse,
            allow_wal_only: spec.allow_wal_only,
        };
        let prepare = self.work_sets.prepare(&work);
        let prepared = match spec.deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline.into(), prepare).await {
                Ok(result) => {
                    metrics::record_restore("ok", started.elapsed().as_micros() as u64);
                    result?
                }
                Err(_) => {
                    metrics::record_restore("timeout", started.elapsed().as_micros() as u64);
                    metrics::record_db_start("timeout", started.elapsed().as_micros() as u64);
                    return Err(WorkerError::WakeupTimeout(format!(
                        "db={db_id} 工作集恢复超过 deadline"
                    )));
                }
            },
            None => {
                let result = prepare.await;
                metrics::record_restore(
                    if result.is_ok() { "ok" } else { "error" },
                    started.elapsed().as_micros() as u64,
                );
                result?
            }
        };

        // 进程注册（STARTING）：先落注册表，调度与数据面都能看到「正在启动」
        let socket = self.cfg.socket_path(&db_id);
        let shared = Arc::new(ProcessShared {
            database_id: db_id.clone(),
            owner_epoch: spec.owner_epoch,
            budget: spec.budget,
            snapshot: spec.snapshot.clone(),
            read_only: spec.read_only,
            stop_requested: AtomicBool::new(false),
            restart_count: AtomicU32::new(0),
            pidfd: Mutex::new(None),
            pid: Mutex::new(0),
            cgroup: Mutex::new(None),
        });

        let spawn_result = self
            .spawn_and_wait_ready(&db_id, 0, &socket, &shared, &prepared, spec.deadline)
            .await;

        match spawn_result {
            Ok((pid, cgroup, conn)) => {
                self.registry.register(LocalDatabase {
                    database_id: db_id.clone(),
                    state: LifecycleState::Warm,
                    pid: Some(pid),
                    local_socket: socket.clone(),
                    owner_epoch: spec.owner_epoch,
                    budget: spec.budget,
                    started_at_unix_ms: now_unix_ms(),
                    last_activity_unix_ms: now_unix_ms(),
                    crash_count: 0,
                    cgroup: cgroup.clone(),
                    restored_from: Some(RestoredFrom {
                        snapshot_id: prepared.snapshot_id.clone(),
                        base_lsn: prepared.base_lsn,
                        applied_lsn: prepared.applied_lsn,
                    }),
                    read_only: spec.read_only,
                });
                self.pool.adopt(conn).await;
                metrics::record_db_start("ok", started.elapsed().as_micros() as u64);
                metrics::record_registry_size(self.registry.len());
                Ok(StartOutcome {
                    pid,
                    socket,
                    cgroup,
                    prepared,
                })
            }
            Err(err) => {
                metrics::record_db_start(
                    match err.code() {
                        domain::error::ErrorCode::WakeupTimeout => "timeout",
                        _ => "error",
                    },
                    started.elapsed().as_micros() as u64,
                );
                Err(err)
            }
        }
    }

    /// spawn 子进程 + 等待 READY（socket 出现且握手成功）。
    ///
    /// `restart_seed` 为自动重启时的重启次数（用于日志与指标）。
    async fn spawn_and_wait_ready(
        self: &Arc<Self>,
        db_id: &str,
        restart_seed: u32,
        socket: &std::path::Path,
        shared: &Arc<ProcessShared>,
        prepared: &PreparedWorkSet,
        deadline: Option<Instant>,
    ) -> Result<(i32, Option<PathBuf>, Arc<UdsConnection>)> {
        // cgroup 必须在 spawn 之前创建好（spawn 与 assign 之间只有极短窗口）
        let cgroup = self.cgroups.create(db_id, &shared.budget);

        // 残留的 socket 文件会让「等待 socket 出现」立即误判为就绪，必须先清理
        let _ = tokio::fs::remove_file(socket).await;

        let mut command = tokio::process::Command::new(&self.cfg.db_runtime_bin);
        command
            .args(
                self.cfg
                    .runtime_args(db_id, shared.owner_epoch, &shared.budget),
            )
            .envs(
                self.cfg
                    .runtime_env(db_id, shared.owner_epoch, &shared.budget),
            )
            .stdin(std::process::Stdio::null())
            // 子进程日志直接继承 Worker 的 stdout/stderr：容器运行时负责收集，
            // 避免 Worker 自己再实现一遍日志转发与背压。
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            // 让子进程先不接收信号（我们通过 pidfd/kill 显式投递），
            // 避免 Ctrl-C 打到 Worker 时子进程被连带杀死而不走优雅停止路径。
            .process_group(0);

        if shared.read_only {
            command.arg("--read-only");
            command.env("DB_RUNTIME_READ_ONLY", "1");
        }
        if !prepared.snapshot_id.is_empty() {
            command
                .arg("--snapshot-id")
                .arg(&prepared.snapshot_id)
                .arg("--base-lsn")
                .arg(prepared.base_lsn.to_string());
            command
                .env("DB_RUNTIME_SNAPSHOT_ID", &prepared.snapshot_id)
                .env("DB_RUNTIME_BASE_LSN", prepared.base_lsn.to_string());
        }

        let child = command.spawn().map_err(|err| {
            WorkerError::Spawn(format!(
                "启动 {} 失败：{err}（db_id={db_id}）",
                self.cfg.db_runtime_bin.display()
            ))
        })?;

        let pid = child
            .id()
            .ok_or_else(|| WorkerError::Spawn("子进程缺少 PID".to_string()))?
            as i32;
        *shared.pid.lock() = pid;
        *shared.cgroup.lock() = cgroup.clone();

        // pidfd：信号投递与退出检测都优先走它（无 PID 复用竞态）
        let signal_fd = match PidFd::open(pid as u32) {
            Ok(fd) => {
                let fd = Arc::new(fd);
                *shared.pidfd.lock() = Some(Arc::clone(&fd));
                Some(fd)
            }
            Err(err) => {
                tracing::warn!(
                    db_id = %db_id,
                    pid,
                    error = %err,
                    "pidfd 不可用（内核过旧？），退化为按 PID 发信号"
                );
                None
            }
        };

        // 加入 cgroup（失败只降级，不影响可用性）
        if let Some(path) = cgroup.as_ref() {
            if let Err(err) = self.cgroups.assign(path, pid) {
                tracing::warn!(db_id = %db_id, error = %err, "把子进程加入 cgroup 失败（降级为不限制）");
            }
        }

        tracing::info!(
            db_id = %db_id,
            pid,
            owner_epoch = shared.owner_epoch,
            restart = restart_seed,
            cgroup = ?cgroup,
            "DB Process 已启动，等待 READY"
        );

        // 监视任务（含 reaper）
        let (tx, rx) = mpsc::channel(8);
        {
            let mut procs = self.procs.lock();
            procs.insert(
                db_id.to_string(),
                ManagedEntry {
                    shared: Arc::clone(shared),
                    tx,
                    pid,
                },
            );
        }
        self.registry.clear_process(db_id);
        self.spawn_monitor(Arc::clone(self), Arc::clone(shared), child, signal_fd, rx);

        // 等待 READY：socket 出现 + 握手成功
        let effective_deadline = match deadline {
            Some(deadline) => deadline.min(Instant::now() + self.cfg.runtime_ready_timeout),
            None => Instant::now() + self.cfg.runtime_ready_timeout,
        };
        match self
            .wait_ready_at(db_id, shared.owner_epoch, socket, effective_deadline)
            .await
        {
            Ok(conn) => Ok((pid, cgroup, conn)),
            Err(err) => {
                // 半启动的进程必须清理：否则会留下一个永远不 READY 的孤儿进程
                tracing::warn!(db_id = %db_id, pid, error = %err, "DB 启动未 READY，回滚");
                self.force_cleanup(db_id, shared, pid).await;
                Err(err)
            }
        }
    }

    /// 等待 DB Process READY。
    pub async fn wait_ready_at(
        &self,
        db_id: &str,
        owner_epoch: u64,
        socket: &std::path::Path,
        deadline: Instant,
    ) -> Result<Arc<UdsConnection>> {
        let mut last_error = String::from("DB Process 未在 deadline 内就绪");
        loop {
            if Instant::now() >= deadline {
                return Err(WorkerError::WakeupTimeout(last_error));
            }
            if socket.exists() {
                match UdsConnection::connect(
                    socket,
                    db_id,
                    owner_epoch,
                    &self.cfg.worker_id,
                    Duration::from_millis(500).min(self.cfg.runtime_ready_timeout),
                    Duration::from_millis(500).min(self.cfg.runtime_ready_timeout),
                )
                .await
                {
                    Ok(conn) => return Ok(conn),
                    Err(err) => last_error = err.to_string(),
                }
            }
            // 25ms 轮询：既满足 §16 的 READY 时延要求，又不至于空转打满 CPU
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// 停掉半启动的进程（不等待优雅退出）。
    async fn force_cleanup(self: &Arc<Self>, db_id: &str, shared: &Arc<ProcessShared>, pid: i32) {
        shared.mark_stop_requested();
        let _ = self.signal(shared, ProcessSignal::Kill);
        // 给 reaper 一点时间回收（监视任务会做 cgroup / 注册表清理）
        let waited = Instant::now() + Duration::from_secs(2);
        while Instant::now() < waited && self.is_running(db_id) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if self.is_running(db_id) {
            // 监视任务没来得及清理：这里兜底
            tracing::warn!(db_id = %db_id, pid, "监视任务未在 2s 内完成清理，交由兜底路径处理");
        }
        self.registry.clear_process(db_id);
        self.registry.remove(db_id);
        if let Some(cgroup) = shared.cgroup.lock().clone() {
            self.cgroups.remove(&cgroup);
        }
        self.pool.drop_database(db_id);
    }

    /// 优雅 / 强制停止。
    ///
    /// 返回 Ok 时，进程已退出、cgroup 已删除、注册表已收敛（COLD）。
    pub async fn stop(&self, db_id: &str, graceful: bool, deadline: Instant) -> Result<()> {
        for attempt in 0..3 {
            let entry = self.procs.lock().get(db_id).map(|entry| ManagedEntry {
                shared: Arc::clone(&entry.shared),
                tx: entry.tx.clone(),
                pid: entry.pid,
            });

            let Some(entry) = entry else {
                // 已经不在跑：幂等成功
                return Ok(());
            };
            entry.shared.mark_stop_requested();

            let (reply_tx, reply_rx) = oneshot::channel();
            let send = entry
                .tx
                .send(ProcessCommand::Stop {
                    graceful,
                    deadline,
                    reply: reply_tx,
                })
                .await;
            if send.is_err() {
                // 监视任务已退出（进程刚崩）：等它清理完就当作完成
                tracing::debug!(db_id = %db_id, attempt, "监视任务已结束，等待清理");
                tokio::time::sleep(Duration::from_millis(20)).await;
                if !self.is_running(db_id) {
                    return Ok(());
                }
                continue;
            }

            return match tokio::time::timeout_at(
                (deadline + Duration::from_secs(1)).into(),
                reply_rx,
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Ok(()), // 监视任务提前退出，视作已完成
                Err(_) => {
                    // 停止指令超时：直接硬杀，绝不让调用方永久挂住
                    tracing::warn!(db_id = %db_id, "停止指令超时，改发 SIGKILL");
                    let _ = self.signal(&entry.shared, ProcessSignal::Kill);
                    Ok(())
                }
            };
        }
        Err(WorkerError::InvalidState(format!(
            "db={db_id} 停止失败：监视任务反复退出"
        )))
    }

    /// 立即终止（KillDatabase）。返回后进程已退出。
    pub async fn kill(&self, db_id: &str, reason: &str) -> Result<()> {
        let entry = self.procs.lock().get(db_id).map(|entry| ManagedEntry {
            shared: Arc::clone(&entry.shared),
            tx: entry.tx.clone(),
            pid: entry.pid,
        });
        let Some(entry) = entry else {
            return Ok(());
        };
        entry.shared.mark_stop_requested();
        let (reply_tx, reply_rx) = oneshot::channel();
        if entry
            .tx
            .send(ProcessCommand::Kill {
                reason: reason.to_string(),
                reply: reply_tx,
            })
            .await
            .is_ok()
        {
            // 硬杀应当很快返回；超时也不阻塞调用方
            let _ = tokio::time::timeout(Duration::from_secs(3), reply_rx).await;
        } else {
            let _ = self.signal(&entry.shared, ProcessSignal::Kill);
        }
        Ok(())
    }

    /// 请求 DB Process 优雅关闭（发 UDS Shutdown 帧）。
    ///
    /// 仅发通知，不等待退出：真正的退出等待由 [`ProcessSupervisor::stop`] 负责。
    pub async fn notify_shutdown(
        &self,
        db_id: &str,
        graceful: bool,
        deadline: Instant,
    ) -> Result<bool> {
        let conn = self.pool.connection(db_id).await?;
        let frame = conn.frame_for(
            "",
            rt::frame::Message::Shutdown(rt::ShutdownRequest {
                graceful,
                deadline_unix_ms: protocol::convert::deadline_ms_from(Some(deadline)),
            }),
        );
        conn.send(frame).await?;
        Ok(true)
    }

    /// 停掉本节点所有 DB（DrainWorker / 进程退出）。
    pub async fn shutdown_all(&self, grace: Duration) -> usize {
        self.shutting_down.store(true, Ordering::Release);
        let db_ids: Vec<String> = self.procs.lock().keys().cloned().collect();
        let deadline = Instant::now() + grace;
        let mut stopped = 0;
        for db_id in db_ids {
            match self.stop(&db_id, true, deadline).await {
                Ok(()) => stopped += 1,
                Err(err) => tracing::warn!(db_id = %db_id, error = %err, "关停 DB 失败"),
            }
        }
        stopped
    }

    /// 给子进程发信号（优先 pidfd）。
    fn signal(&self, shared: &Arc<ProcessShared>, signal: ProcessSignal) -> std::io::Result<()> {
        let pid = *shared.pid.lock();
        let guard = shared.pidfd.lock();
        let cloned = guard.clone();
        drop(guard);
        if let Some(fd) = cloned {
            match fd.send_signal(signal) {
                Ok(()) => {
                    tracing::debug!(
                        db_id = %shared.database_id,
                        pid,
                        signal = signal.as_str(),
                        "已通过 pidfd 投递信号"
                    );
                    return Ok(());
                }
                Err(err) if err.raw_os_error() == Some(libc::ESRCH) => {
                    // 进程已退出：不是错误
                    return Ok(());
                }
                Err(err) => {
                    tracing::warn!(
                        db_id = %shared.database_id,
                        pid,
                        error = %err,
                        "pidfd 发信号失败，退化为按 PID 发信号"
                    );
                }
            }
        }
        // 退化路径（无 pidfd）必须先校验 pid：只有 pid > 0 才代表「某个具体的 DB 进程」。
        // `kill(0, sig)` 的语义不是「给 0 号进程发信号」，而是「给**本进程所在的整个进程组**
        // 发信号」；`kill(pid < 0, sig)` 同理是发往进程组 |pid|。进程退出后 `shared.pid`
        // 会被清零（见 `on_process_exit` 的「重启次数达上限」分支），若不在这里拦下 0 / 负数，
        // 「清理一个已经退出的 DB」就会把自己连同同进程组的兄弟进程一起 SIGKILL 掉 ——
        // 现象是整个 Worker（在测试里是 cargo test 进程组）被信号打死、退出码 137。
        if pid <= 0 {
            tracing::debug!(
                db_id = %shared.database_id,
                id = pid,
                signal = signal.as_str(),
                "进程已退出（pid 已清零），跳过信号投递"
            );
            return Ok(());
        }
        pidfd::send_signal_by_pid(pid, signal)
    }

    /// 启动监视任务：独占 Child（reaper）+ 处理停止指令 + 退出后的崩溃处理。
    fn spawn_monitor(
        self: &Arc<Self>,
        supervisor: Arc<Self>,
        shared: Arc<ProcessShared>,
        mut child: tokio::process::Child,
        signal_fd: Option<Arc<PidFd>>,
        mut rx: mpsc::Receiver<ProcessCommand>,
    ) {
        // reaper：Child 只在这里被 wait，避免 select! 取消语义问题
        let (reaper_tx, reaper_rx) = oneshot::channel();
        tokio::spawn(async move {
            let status = child.wait().await;
            let _ = reaper_tx.send(status);
        });

        // 退出检测的第二通道：pidfd 可读即代表进程已退出
        let (pidfd_tx, pidfd_rx) = oneshot::channel();
        if let Some(fd) = signal_fd {
            let fd = Arc::try_unwrap(fd).unwrap_or_else(|_arc| {
                // 还有其它 Arc 引用（信号路径持有）：复制一个 fd 用于等待
                PidFd::open(*shared.pid.lock() as u32).unwrap_or_else(|_| {
                    // 打不开就退化为不使用该通道：把等待变成永不就绪
                    unreachable_fd_placeholder()
                })
            });
            tokio::spawn(async move {
                let _ = fd.wait_exit().await;
                let _ = pidfd_tx.send(());
            });
        } else {
            // 无 pidfd：丢弃 sender，接收端立即 Err（视为不触发）
            drop(pidfd_tx);
        }

        tokio::spawn(async move {
            let mut reaper_rx = reaper_rx;
            let mut pidfd_rx = pidfd_rx;
            let mut stop_deadline: Option<Instant> = None;
            let mut graceful_requested = false;

            // 收敛到第一个「终止事件」：reaper（带退出状态）/ pidfd（已退出）/ 停止指令。
            // 用带标签的块而不是 loop：每个分支都直接产出退出状态，没有「再跑一轮」的语义。
            let exit_status = 'monitor: {
                tokio::select! {
                    biased;
                    status = &mut reaper_rx => break 'monitor status.ok().and_then(|item| item.ok()),
                    Some(command) = rx.recv() => {
                        match command {
                            ProcessCommand::Stop { graceful, deadline, reply } => {
                                graceful_requested = graceful;
                                stop_deadline = Some(deadline);
                                // 优雅路径：先通知 DB Process 自行收敛（best effort）
                                if graceful {
                                    let _ = supervisor
                                        .notify_shutdown(&shared.database_id, true, deadline)
                                        .await;
                                }
                                supervisor.signal(&shared, ProcessSignal::Term).ok();
                                // 等待退出：reaper / pidfd / 超时后 SIGKILL
                                let outcome = supervisor
                                    .await_exit(&mut reaper_rx, &mut pidfd_rx, deadline)
                                    .await;
                                match outcome {
                                    Some(status) => {
                                        let _ = reply.send(Ok(()));
                                        break 'monitor Some(status);
                                    }
                                    None => {
                                        // 宽限期内没退出 -> SIGKILL，再等一小会
                                        tracing::warn!(
                                            db_id = %shared.database_id,
                                            "优雅停止超时，发送 SIGKILL"
                                        );
                                        supervisor.signal(&shared, ProcessSignal::Kill).ok();
                                        let hard_deadline = Instant::now() + Duration::from_secs(2);
                                        let status = supervisor
                                            .await_exit(&mut reaper_rx, &mut pidfd_rx, hard_deadline)
                                            .await;
                                        let _ = reply.send(Ok(()));
                                        break 'monitor status;
                                    }
                                }
                            }
                            ProcessCommand::Kill { reason, reply } => {
                                graceful_requested = false;
                                supervisor.signal(&shared, ProcessSignal::Kill).ok();
                                tracing::warn!(
                                    db_id = %shared.database_id,
                                    reason = %reason,
                                    "KillDatabase：已发送 SIGKILL"
                                );
                                let hard_deadline = Instant::now() + Duration::from_secs(2);
                                let status = supervisor
                                    .await_exit(&mut reaper_rx, &mut pidfd_rx, hard_deadline)
                                    .await;
                                let _ = reply.send(Ok(()));
                                break 'monitor status;
                            }
                        }
                    }
                    _ = &mut pidfd_rx => {
                        // pidfd 可读 = 进程已退出；真正的退出状态仍以 reaper 为准
                        let status = match tokio::time::timeout(Duration::from_millis(500), &mut reaper_rx).await {
                            Ok(Ok(item)) => item.ok(),
                            _ => None,
                        };
                        break 'monitor status;
                    }
                }
            };

            let cause = classify_exit(exit_status);
            let _ = stop_deadline;
            let stop_requested = shared.is_stop_requested() || graceful_requested;
            supervisor
                .on_process_exit(&shared, cause, stop_requested)
                .await;
        });
    }

    /// 等待进程退出；返回退出状态（None = 超时未退出）。
    async fn await_exit(
        &self,
        reaper_rx: &mut oneshot::Receiver<std::io::Result<std::process::ExitStatus>>,
        pidfd_rx: &mut oneshot::Receiver<()>,
        deadline: Instant,
    ) -> Option<std::process::ExitStatus> {
        tokio::select! {
            // 用重借用而不是把 `&mut` 移进分支：pidfd 分支里还要再等一次 reaper
            result = &mut *reaper_rx => match result {
                Ok(status) => Some(status.ok()?),
                Err(_) => None,
            },
            _ = &mut *pidfd_rx => {
                match tokio::time::timeout(Duration::from_millis(500), &mut *reaper_rx).await {
                    Ok(Ok(status)) => status.ok(),
                    _ => None,
                }
            }
            _ = tokio::time::sleep_until(deadline.into()) => None,
        }
    }

    /// 进程异常退出后的状态收敛。
    ///
    /// 走注册表的状态机，而不是直接改写 `state`：异常退出就是「停止过程以失败告终」，
    /// 对应 WARM/HOT -> STOPPING -> FAILED。已经在 FAILED 时什么都不做（避免重复告警）。
    fn converge_failed(&self, db_id: &str) {
        if self.registry.get(db_id).map(|db| db.state) == Some(LifecycleState::Failed) {
            return;
        }
        self.registry.transition(db_id, LifecycleState::Stopping);
        self.registry.transition(db_id, LifecycleState::Failed);
    }

    /// 自动重启成功后的状态收敛：FAILED -> STARTING -> WARM
    /// （状态机里「失败后重试启动并成功」的路径）。
    fn converge_warm(&self, db_id: &str) {
        if self.registry.get(db_id).map(|db| db.state) == Some(LifecycleState::Warm) {
            return;
        }
        self.registry.transition(db_id, LifecycleState::Starting);
        self.registry.transition(db_id, LifecycleState::Warm);
    }

    /// 进程退出后的收敛：清理 cgroup / 注册表 / 连接，并按策略决定是否自动重启。
    async fn on_process_exit(
        self: &Arc<Self>,
        shared: &Arc<ProcessShared>,
        cause: ExitCause,
        stop_requested: bool,
    ) {
        let db_id = shared.database_id.clone();
        let abnormal = cause.is_abnormal() && !stop_requested;
        metrics::record_process_exit(&db_id, abnormal, cause.label());

        // 清理本地 Route / 连接 / cgroup（架构 §12.1：Mark unhealthy + Clear local route）
        self.pool.drop_database(&db_id);
        if let Some(cgroup) = shared.cgroup.lock().clone() {
            self.cgroups.remove(&cgroup);
        }
        *shared.pidfd.lock() = None;
        self.registry.clear_process(&db_id);

        if stop_requested || self.shutting_down.load(Ordering::Acquire) {
            // 正常停止：DB 回到 COLD（保留 epoch 记录，所有权不变）
            self.registry.transition(&db_id, LifecycleState::Cold);
            self.procs.lock().remove(&db_id);
            metrics::record_registry_size(self.registry.len());
            tracing::info!(db_id = %db_id, cause = cause.label(), "DB Process 已停止");
            return;
        }

        // 异常退出：先标记 FAILED（失败可见），再尝试有界自动重启
        self.converge_failed(&db_id);
        let crashes = self.registry.record_crash(&db_id);
        let restarts = shared.restart_count.fetch_add(1, Ordering::AcqRel) + 1;
        tracing::error!(
            db_id = %db_id,
            pid = *shared.pid.lock(),
            cause = cause.label(),
            crashes,
            restarts,
            max_restarts = self.cfg.runtime_max_restarts,
            "DB Process 异常退出"
        );

        if restarts > self.cfg.runtime_max_restarts {
            tracing::error!(
                db_id = %db_id,
                "自动重启次数已达上限，停止重启并保留 FAILED 状态等待 Server 决策"
            );
            // 停止后不再持有进程条目（无进程可管），但保留注册项让 Server 看到 FAILED
            self.procs.lock().remove(&db_id);
            *shared.pid.lock() = 0;
            metrics::record_registry_size(self.registry.len());
            return;
        }

        // 退避：100ms -> 200ms -> 400ms（上限 1s），保证「Crash 后 1s 内可服务」
        let backoff = Duration::from_millis(
            self.cfg
                .runtime_restart_backoff
                .as_millis()
                .saturating_mul(1u128 << (restarts - 1).min(3)) as u64,
        )
        .min(Duration::from_secs(1));
        tokio::time::sleep(backoff).await;

        if self.shutting_down.load(Ordering::Acquire) || shared.is_stop_requested() {
            self.procs.lock().remove(&db_id);
            return;
        }

        // 重启：复用同一 shared（保留 epoch 与重启计数），重建进程与 READY 校验
        let prepared = self
            .work_sets
            .prepare(&PrepareWork {
                database_id: db_id.clone(),
                owner_epoch: shared.owner_epoch,
                snapshot: shared.snapshot.clone(),
                allow_local_reuse: true,
                allow_wal_only: true,
            })
            .await
            .unwrap_or_else(|err| {
                tracing::error!(db_id = %db_id, error = %err, "重启前工作集准备失败，按空工作集继续");
                PreparedWorkSet {
                    snapshot_id: String::new(),
                    base_lsn: 0,
                    applied_lsn: 0,
                    reused_local: false,
                    downloaded_snapshot: false,
                }
            });

        let socket = self.cfg.socket_path(&db_id);
        self.procs.lock().remove(&db_id);
        match self
            .spawn_and_wait_ready(
                &db_id,
                restarts,
                &socket,
                shared,
                &prepared,
                Some(Instant::now() + self.cfg.runtime_ready_timeout),
            )
            .await
        {
            Ok((pid, cgroup, conn)) => {
                self.registry.set_pid(&db_id, pid, cgroup);
                self.converge_warm(&db_id);
                self.pool.adopt(conn).await;
                metrics::record_db_start("restart", 0);
                tracing::info!(db_id = %db_id, pid, restarts, "DB Process 自动重启完成");
            }
            Err(err) => {
                tracing::error!(db_id = %db_id, error = %err, "DB Process 自动重启失败");
                self.converge_failed(&db_id);
                self.procs.lock().remove(&db_id);
            }
        }
        metrics::record_registry_size(self.registry.len());
    }
}

/// 把 tokio 的退出状态归类。
fn classify_exit(status: Option<std::process::ExitStatus>) -> ExitCause {
    use std::os::unix::process::ExitStatusExt;
    match status {
        None => ExitCause::Unknown,
        Some(status) => {
            if let Some(code) = status.code() {
                if code == 0 {
                    ExitCause::Exited
                } else {
                    ExitCause::ExitCode(code)
                }
            } else if let Some(signal) = status.signal() {
                ExitCause::Signal(signal)
            } else {
                ExitCause::Unknown
            }
        }
    }
}

/// 占位：pidfd 复制失败时使用（该分支实际不会走到，保留是为了避免 unwrap panic）。
fn unreachable_fd_placeholder() -> PidFd {
    // 打开 0 号进程（内核会拒绝）是不可能成功的，这里用自己作为兜底：
    // 退化为「不会触发」的等待通道，不影响主流程的 reaper 退出检测。
    PidFd::open(std::process::id()).expect("自身 pidfd 必定可用")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uds::PoolConfig;

    /// 测试用配置：所有超时都压到毫秒级。
    ///
    /// 失败路径（进程不 READY、崩溃重启）**必须有界**：每个测试都要在显式的
    /// ready-timeout / stop-grace / backoff 之内收敛，不能依赖「等轮询超时」。
    fn test_config(
        dir: &std::path::Path,
        runtime: &std::path::Path,
        max_restarts: u32,
    ) -> WorkerConfig {
        let cli = <crate::cli::Cli as clap::Parser>::try_parse_from([
            "db-worker",
            "--worker-id",
            "worker-test",
            "--data-dir",
            dir.join("data").to_str().unwrap(),
            "--run-dir",
            dir.join("run").to_str().unwrap(),
            "--db-runtime-bin",
            runtime.to_str().unwrap(),
            "--runtime-ready-timeout-ms",
            "500",
            "--runtime-stop-grace-ms",
            "200",
            "--runtime-max-restarts",
            &max_restarts.to_string(),
            "--runtime-restart-backoff-ms",
            "10",
            "--cgroup-disabled",
        ])
        .unwrap();
        let config = WorkerConfig::from_cli(cli).unwrap();
        config.ensure_dirs().unwrap();
        config
    }

    struct Harness {
        supervisor: Arc<ProcessSupervisor>,
        registry: Arc<LocalDbRegistry>,
        #[allow(dead_code)]
        // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
        pool: Arc<DbConnectionPool>,
        _dir: tempfile::TempDir,
    }

    fn harness_with(runtime: &std::path::Path, max_restarts: u32) -> Harness {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Arc::new(test_config(dir.path(), runtime, max_restarts));
        let registry = Arc::new(LocalDbRegistry::new());
        let pool = Arc::new(DbConnectionPool::new(
            PoolConfig {
                worker_id: cfg.worker_id.clone(),
                run_dir: cfg.run_dir.clone(),
                connect_timeout: Duration::from_millis(300),
                handshake_timeout: Duration::from_millis(300),
                ..Default::default()
            },
            Arc::clone(&registry),
        ));
        let work_sets = WorkSetPreparer::new(
            cfg.data_dir.clone(),
            None,
            None,
            cfg.version.clone(),
            domain::WorkerId::new(cfg.worker_id.clone()),
        );
        let supervisor = ProcessSupervisor::new(
            Arc::clone(&cfg),
            Arc::clone(&registry),
            Arc::clone(&pool),
            work_sets,
        );
        Harness {
            supervisor,
            registry,
            pool,
            _dir: dir,
        }
    }

    /// 假 db-runtime：一个**真正可执行的脚本**（带 shebang + 0755），替代以前的 `/bin/sh`。
    ///
    /// 为什么不能用 `/bin/sh` 直接当假运行时：supervisor 会传入
    /// `--worker-id / --database-id / --owner-epoch / --socket-path / ...`，`/bin/sh` 把这些
    /// 当成**它自己的**选项解析，立刻以 `Illegal option --` 退出。子进程秒退会触发自动重启，
    /// 「等 READY」的测试于是永远等不到收敛（外部表现：测试挂住、进程组最后被信号打死）。
    /// 换成带 shebang 的脚本后，这些参数只是脚本的 `$1..$n`，被脚本自然忽略。
    struct FakeRuntime {
        dir: tempfile::TempDir,
        script: PathBuf,
        pid_file: PathBuf,
    }

    impl FakeRuntime {
        /// `body` 是脚本在「忽略 supervisor 参数」之后执行的正文。
        /// 脚本总是先把自己的 PID 写进 pid 文件（供「PID 跟踪 / 无孤儿」断言使用）。
        fn new(body: &str) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let script = dir.path().join("fake-db-runtime.sh");
            let pid_file = dir.path().join("fake-db-runtime.pid");
            std::fs::write(
                &script,
                format!(
                    "#!/bin/sh\n\
                     # 假 db-runtime：$1..$n 是 supervisor 传的 --worker-id 等参数，直接忽略\n\
                     echo $$ > \"{pid}\"\n\
                     {body}\n",
                    pid = pid_file.display(),
                ),
            )
            .unwrap();
            // 必须真正可执行：supervisor 用 `Command::new(bin)` 直接 exec，不经过任何 shell。
            let mut perm = std::fs::metadata(&script).unwrap().permissions();
            std::os::unix::fs::PermissionsExt::set_mode(&mut perm, 0o755);
            std::fs::set_permissions(&script, perm).unwrap();
            Self {
                dir,
                script,
                pid_file,
            }
        }

        /// 长期存活、且会创建 socket 文件的假运行时。
        /// 它**不做协议握手**，所以 supervisor 的 READY 轮询会一直连不上 —— 用来验证失败路径。
        fn socket_only() -> Self {
            Self::new(
                "mkdir -p \"$(dirname \"$DB_RUNTIME_SOCKET_PATH\")\"\n\
                 : > \"$DB_RUNTIME_SOCKET_PATH\"\n\
                 exec sleep 30",
            )
        }

        /// 长期存活的假运行时（只为了有一个「活着且归 supervisor 管」的进程）。
        fn long_lived() -> Self {
            Self::new("exec sleep 30")
        }

        fn script(&self) -> &std::path::Path {
            &self.script
        }

        fn dir(&self) -> &std::path::Path {
            self.dir.path()
        }

        /// 脚本进程自己写下的 PID（脚本跑起来之后才存在，进程死后仍然可读）。
        fn spawned_pid(&self) -> Option<i32> {
            std::fs::read_to_string(&self.pid_file)
                .ok()?
                .trim()
                .parse()
                .ok()
        }
    }

    /// 有界轮询：等 `check` 给出结果，最多等 `limit`。
    /// 「等状态收敛」的地方一律走这里，避免任何形式的无限等待。
    async fn wait_for<T>(limit: Duration, mut check: impl FnMut() -> Option<T>) -> Option<T> {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(value) = check() {
                return Some(value);
            }
            if Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn start_spawns_process_and_tracks_pid() {
        // 假运行时：创建 socket（让 READY 轮询真的去连一次）但**不做协议握手**，
        // 因此 start 必然以「等待 READY 超时」失败；进程本身长期存活，
        // 便于观察 PID 跟踪，最后必须被清理路径回收（不留孤儿）。
        let runtime = FakeRuntime::socket_only();
        // max_restarts=0：本次只验证「spawn + 跟踪 + 失败清理」，不叠加重启 churn。
        // 注意：Harness 必须整体绑定（不能只解构出字段），否则 TempDir 会被立即丢弃。
        let harness = harness_with(runtime.script(), 0);
        let supervisor = Arc::clone(&harness.supervisor);
        let registry = Arc::clone(&harness.registry);
        const DB_ID: &str = "db-spawn-tracked";

        let start_task = {
            let supervisor = Arc::clone(&supervisor);
            tokio::spawn(async move {
                supervisor
                    .start(StartSpec::new(
                        DB_ID,
                        1,
                        ResourceBudget::new(100, 64, 0, 0, 1, 0),
                    ))
                    .await
            })
        };

        // PID 跟踪：进程一旦被 spawn，就必须出现在 supervisor 的进程表里。
        // 观察窗口以「start 自身何时返回」为界（start 的上界是 ready-timeout），
        // 所以这个循环同样是有界的。
        let mut tracked = None;
        while !start_task.is_finished() {
            if let Some(pid) = supervisor
                .running()
                .into_iter()
                .find(|(db_id, _)| db_id == DB_ID)
                .map(|(_, pid)| pid)
            {
                tracked = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let tracked = tracked.expect("spawn 之后进程必须进入 supervisor 的进程表（PID 跟踪）");

        // 失败收敛必须有界：不做握手的假运行时永远不 READY，start 必须在 ready-timeout
        // （500ms）内以 WakeupTimeout 返回，而不是一直等下去。
        let outcome = start_task.await.unwrap();
        assert_eq!(
            outcome
                .expect_err("假运行时不做握手，start 必然失败")
                .code(),
            domain::error::ErrorCode::WakeupTimeout
        );

        // 清理必须彻底：进程表与注册表都不能留下条目（否则下次 start 会误判「已在运行」）。
        assert!(!supervisor.is_running(DB_ID));
        assert_eq!(supervisor.running_count(), 0);
        assert!(registry.get(DB_ID).is_none());

        // 无孤儿：跟踪到的 PID 必须就是子进程真实 PID，且它真的已经消失。
        let spawned = wait_for(Duration::from_secs(1), || runtime.spawned_pid())
            .await
            .expect("假运行时必须写下自己的 PID");
        assert_eq!(
            tracked, spawned,
            "supervisor 跟踪的 PID 必须是子进程真实 PID"
        );
        assert!(
            wait_for(Duration::from_secs(1), || (!pidfd::process_alive(spawned))
                .then_some(()))
            .await
            .is_some(),
            "启动失败的子进程必须被回收，不能留孤儿：pid={spawned}"
        );
    }

    #[tokio::test]
    async fn stop_and_kill_are_idempotent_on_unknown_db() {
        let runtime = FakeRuntime::long_lived();
        let Harness {
            supervisor, _dir, ..
        } = harness_with(runtime.script(), 3);
        supervisor
            .stop(
                "db-absent",
                true,
                Instant::now() + Duration::from_millis(50),
            )
            .await
            .unwrap();
        supervisor.kill("db-absent", "test").await.unwrap();
        assert!(!supervisor.is_running("db-absent"));
    }

    #[tokio::test]
    async fn running_process_can_be_stopped_by_signal() {
        // 手工注册一个进程条目来验证停止路径（不依赖 db-runtime 的真实行为）
        let runtime = FakeRuntime::long_lived();
        let Harness {
            supervisor,
            registry,
            _dir,
            ..
        } = harness_with(runtime.script(), 3);

        let mut child = tokio::process::Command::new(runtime.script())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap() as i32;
        let shared = Arc::new(ProcessShared {
            database_id: "db-manual".into(),
            owner_epoch: 3,
            budget: ResourceBudget::new(100, 64, 0, 0, 1, 0),
            snapshot: None,
            read_only: false,
            stop_requested: AtomicBool::new(false),
            restart_count: AtomicU32::new(0),
            pidfd: Mutex::new(PidFd::open(pid as u32).ok().map(Arc::new)),
            pid: Mutex::new(pid),
            cgroup: Mutex::new(None),
        });
        *shared.pidfd.lock() = PidFd::open(pid as u32).ok().map(Arc::new);
        let (tx, mut rx) = mpsc::channel(4);
        {
            let mut procs = supervisor.procs.lock();
            procs.insert(
                "db-manual".to_string(),
                ManagedEntry {
                    shared: Arc::clone(&shared),
                    tx,
                    pid,
                },
            );
        }
        registry.register(LocalDatabase {
            database_id: "db-manual".into(),
            state: LifecycleState::Warm,
            pid: Some(pid),
            local_socket: supervisor.config().socket_path("db-manual"),
            owner_epoch: 3,
            budget: ResourceBudget::new(100, 64, 0, 0, 1, 0),
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: None,
            read_only: false,
        });
        assert!(supervisor.is_running("db-manual"));

        // 监视任务：把子进程交给它（复用生产代码）
        let supervisor_clone = Arc::clone(&supervisor);
        let shared_clone = Arc::clone(&shared);
        tokio::spawn(async move {
            let command = rx.recv().await.expect("等待停止指令");
            match command {
                ProcessCommand::Stop { reply, .. } => {
                    supervisor_clone
                        .signal(&shared_clone, ProcessSignal::Kill)
                        .unwrap();
                    let _ = child.wait().await;
                    let _ = reply.send(Ok(()));
                }
                ProcessCommand::Kill { reply, .. } => {
                    supervisor_clone
                        .signal(&shared_clone, ProcessSignal::Kill)
                        .unwrap();
                    let _ = child.wait().await;
                    let _ = reply.send(Ok(()));
                }
            }
            supervisor_clone
                .on_process_exit(&shared_clone, ExitCause::Signal(libc::SIGKILL), true)
                .await;
        });

        supervisor
            .stop("db-manual", false, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(!supervisor.is_running("db-manual"));
        // 正常停止 -> COLD（保留所有权记录），不计崩溃
        assert_eq!(
            registry.get("db-manual").unwrap().state,
            LifecycleState::Cold
        );
        assert_eq!(registry.get("db-manual").unwrap().crash_count, 0);
    }

    #[test]
    fn exit_classification() {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(classify_exit(None), ExitCause::Unknown);
        assert_eq!(
            classify_exit(Some(std::process::ExitStatus::from_raw(0 << 8))),
            ExitCause::Exited
        );
        assert_eq!(
            classify_exit(Some(std::process::ExitStatus::from_raw(3 << 8))),
            ExitCause::ExitCode(3)
        );
        assert_eq!(
            classify_exit(Some(std::process::ExitStatus::from_raw(libc::SIGKILL))),
            ExitCause::Signal(libc::SIGKILL)
        );
        assert!(!ExitCause::Exited.is_abnormal());
        assert!(ExitCause::Signal(9).is_abnormal());
        assert_eq!(ExitCause::Signal(9).label(), "signal");
    }

    #[test]
    fn fake_runtime_script_helper_is_usable() {
        // 自检：脚本必须自带 shebang + 可执行位，能被内核直接 exec（不走 `sh -c`），
        // 并且能吞掉 supervisor 实际传入的那组 `--*` 参数 —— 这正是 `/bin/sh` 做不到的事。
        let runtime = FakeRuntime::new(
            "mkdir -p \"$(dirname \"$DB_RUNTIME_SOCKET_PATH\")\"\n: > \"$DB_RUNTIME_SOCKET_PATH\"",
        );
        let socket = runtime.dir().join("self-check.sock");
        let socket_arg = socket.to_str().unwrap().to_string();
        let status = std::process::Command::new(runtime.script())
            .args([
                "--worker-id",
                "worker-test",
                "--database-id",
                "db-self-check",
                "--owner-epoch",
                "7",
                "--socket-path",
                socket_arg.as_str(),
            ])
            .env("DB_RUNTIME_SOCKET_PATH", &socket)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "假运行时脚本必须忽略 supervisor 传入的 --* 参数并正常退出"
        );
        assert!(socket.exists(), "脚本必须创建 socket 文件");
        assert!(
            runtime.spawned_pid().is_some_and(|pid| pid > 0),
            "脚本必须写下自己的 PID"
        );
    }

    /// 回归：进程退出后 `shared.pid` 会被清零（见 `on_process_exit` 的「重启次数达上限」分支）。
    /// 此时若无条件回退到按 PID 发信号，`kill(0, SIGKILL)` 会把信号广播给**本进程所在的整个
    /// 进程组**（`kill(负数, sig)` 同理），Worker 于是把自己和同组的兄弟进程一起杀掉 ——
    /// 外部表现正是「测试挂住不动、最后被信号杀死（exit 137）」。
    #[tokio::test]
    async fn signal_skips_when_pid_already_cleared() {
        let runtime = FakeRuntime::long_lived();
        let Harness {
            supervisor, _dir, ..
        } = harness_with(runtime.script(), 0);
        let shared = Arc::new(ProcessShared {
            database_id: "db-ghost".into(),
            owner_epoch: 1,
            budget: ResourceBudget::new(100, 64, 0, 0, 1, 0),
            snapshot: None,
            read_only: false,
            stop_requested: AtomicBool::new(false),
            restart_count: AtomicU32::new(0),
            pidfd: Mutex::new(None),
            pid: Mutex::new(0),
            cgroup: Mutex::new(None),
        });

        supervisor
            .signal(&shared, ProcessSignal::Kill)
            .expect("pid 已清零不是「0 号进程」：应当跳过投递，而不是发往进程组");

        // 能执行到这里，就说明测试进程（及其进程组）没有被自己发的 SIGKILL 打死。
        assert!(!supervisor.is_running("db-ghost"));
    }
}
