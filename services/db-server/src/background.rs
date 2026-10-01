//! 控制面后台任务（架构 §7 / §8 / §12 / §16 / §17.5）。
//!
//! 全部任务共用一条纪律：**控制面故障不得影响数据面**。
//! 具体到实现：
//! - 任何一次 tick 失败只记日志并等下一次，不 panic、不退出、不清空本地 Route Cache；
//! - 状态推进一律写 Catalog（PostgreSQL 是唯一权威），本地缓存只是读取加速；
//! - 队列只有 PostgreSQL `jobs` 表（`lease_job` + `FOR UPDATE SKIP LOCKED`），
//!   不引入 Redis / Kafka 等额外中间件（架构 §17.5「明确不引入」）。
//!
//! 任务清单：
//! | 任务 | 周期 | 职责 |
//! |------|------|------|
//! | [`spawn_reconciler`] | `ROUTE_RECONCILE_INTERVAL_MS` | 全量 reconcile Route Cache（正确性路径） |
//! | [`spawn_catalog_watcher`] | 事件驱动 | LISTEN/NOTIFY 加速失效（仅加速） |
//! | [`spawn_heartbeat_monitor`] | 1s | 心跳超时判定 + Owner 失效接管 |
//! | [`spawn_ownership_gc`] | 15s | 回收租约过期的 ownership |
//! | [`spawn_eviction`] | 30s | WARM/空闲 DB 的生命周期回收 |
//! | [`spawn_job_runner`] | 事件驱动 + 2s 兜底 | 执行长操作 job |
//! | [`spawn_session_sweeper`] | 30s | 清理本地过期会话并关闭 Worker 侧会话 |

use std::time::Duration;

use catalog::{Catalog, DatabaseFilter};
// JobRecord 属于领域模型（domain::records），catalog 只负责存取
use chrono::Utc;
use domain::error::ErrorCode;
use domain::ids::{DatabaseId, WorkerId};
use domain::lifecycle::{LifecycleState, WorkerState};
use domain::policy;
use domain::records::JobRecord;
use domain::records::{DatabaseRecord, SnapshotRecord};
use scheduler::{PlacementRequest, WorkerCandidate};
use serde_json::json;
use tokio::sync::Notify;

use crate::config::{
    EVICTION_INTERVAL, HEARTBEAT_CHECK_INTERVAL, JOB_POLL_INTERVAL, OWNERSHIP_GC_INTERVAL,
    OWNERSHIP_LEASE_TTL, OWNERSHIP_STALE_GRACE, SESSION_SWEEP_INTERVAL, WORKER_CONTROL_TIMEOUT,
};
use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::state::AppState;

// ==================================================================== Job 种类

/// 平台长操作对应的 job kind。
///
/// 与 `operations.kind` 是**两套取值**：operation 是对外契约（取值被 migration 的
/// CHECK 冻结），job 是内部执行载体（`jobs.kind` 无 CHECK，可以按执行需要细分）。
pub mod job_kind {
    /// 创建数据库的收尾（登记 storage prefix 等元数据）。
    pub const DB_CREATE: &str = "DB_CREATE";
    /// 启动数据库（冷启动 / 显式 start）。
    pub const DB_START: &str = "DB_START";
    /// 停止数据库。
    pub const DB_STOP: &str = "DB_STOP";
    /// 重启数据库。
    pub const DB_RESTART: &str = "DB_RESTART";
    /// 迁移数据库到指定 / 自动选择的 Worker。
    pub const DB_MOVE: &str = "DB_MOVE";
    /// 删除数据库（先停再软删）。
    pub const DB_DELETE: &str = "DB_DELETE";
    /// 触发一次快照并登记元数据。
    pub const DB_SNAPSHOT: &str = "DB_SNAPSHOT";
    /// 备份（快照 + backup_jobs 记录）。
    pub const DB_BACKUP: &str = "DB_BACKUP";
    /// 从快照恢复。
    pub const DB_RESTORE: &str = "DB_RESTORE";
    /// 排空 Worker。
    pub const WORKER_DRAIN: &str = "WORKER_DRAIN";

    /// 本进程会抢占的全部 job kind（其余 kind 留给其它组件，避免抢走别人的活）。
    pub const ALL: &[&str] = &[
        DB_CREATE,
        DB_START,
        DB_STOP,
        DB_RESTART,
        DB_MOVE,
        DB_DELETE,
        DB_SNAPSHOT,
        DB_BACKUP,
        DB_RESTORE,
        WORKER_DRAIN,
    ];
}

// ==================================================================== Job 队列

/// 进程内 job 投递器。
///
/// 存在的意义只有一条：**降低长操作的空转延迟**。job 本身始终写 PostgreSQL，
/// `Notify` 只是让 Runner 不必等满一个轮询周期；即便通知丢失，兜底轮询也会捞起来。
#[derive(Debug)]
pub struct JobQueue {
    catalog: Catalog,
    notify: Notify,
}

impl JobQueue {
    /// 构造。
    #[must_use]
    pub fn new(catalog: Catalog) -> Self {
        Self {
            catalog,
            notify: Notify::new(),
        }
    }

    /// 投递一个 job 并唤醒 Runner。
    ///
    /// # Errors
    /// Catalog 写入失败时返回错误（调用方必须让请求失败：任务没进队列就不能声称已受理）。
    pub async fn submit(
        &self,
        kind: &str,
        payload: serde_json::Value,
        priority: i32,
        idempotency_key: Option<&str>,
    ) -> ApiResult<JobRecord> {
        let job = self
            .catalog
            .enqueue_job(kind, payload, priority, None, idempotency_key)
            .await
            .api()?;
        self.notify.notify_one();
        Ok(job)
    }

    /// 主动唤醒 Runner（例如 reconcile 后发现需要立即处理）。
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// 等待「有新 job」或超时（Runner 的等待原语）。
    pub async fn wait(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.notify.notified()).await;
    }
}

// ==================================================================== 任务启动

/// 启动全部后台任务；返回的 JoinHandle 交由 `main` 在关闭时统一 abort。
#[must_use]
pub fn spawn_all(state: &AppState) -> Vec<tokio::task::JoinHandle<()>> {
    vec![
        spawn_reconciler(state.clone()),
        spawn_catalog_watcher(state.clone()),
        spawn_heartbeat_monitor(state.clone()),
        spawn_ownership_gc(state.clone()),
        spawn_eviction(state.clone()),
        spawn_job_runner(state.clone()),
        spawn_session_sweeper(state.clone()),
    ]
}

/// 周期性全量 reconcile Route Cache（架构 §17.4「version reconcile = correctness path」）。
///
/// **Control Plane 不可用时不得清空缓存**：本函数在任何失败路径上都只记日志，
/// 让缓存继续服务已运行的 DB。
#[must_use]
pub fn spawn_reconciler(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = state.config.route_reconcile_interval;
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match state.router.reconcile_all().await {
                Ok(count) => {
                    // 第一次成功 reconcile 之后才算「就绪」：此时 Route Cache 才有全量视图。
                    state.readiness.mark_reconciler_ready();
                    tracing::debug!(routes = count, "route reconcile 完成");
                }
                Err(err) => {
                    tracing::warn!(
                        code = err.code().as_str(),
                        message = %err,
                        "route reconcile 失败：保留现有 Route Cache（Control Plane 短暂不可用不影响已运行 DB）"
                    );
                }
            }
        }
    })
}

/// 监听 Catalog 变更并即时刷新受影响的路由（**仅加速**，正确性由 reconcile 保证）。
#[must_use]
pub fn spawn_catalog_watcher(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // LISTEN 断开后必须重连；重连前做的任何事都不影响正确性，因此失败只退避重试。
        const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);
        loop {
            let mut watcher = match state.catalog.watch_catalog_changes().await {
                Ok(watcher) => watcher,
                Err(err) => {
                    tracing::warn!(
                        code = err.code.as_str(),
                        message = %err.message,
                        "订阅 catalog 变更失败，退避后重试（仅加速通道）"
                    );
                    tokio::time::sleep(RECONNECT_BACKOFF).await;
                    continue;
                }
            };
            tracing::info!("catalog 变更订阅已建立（LISTEN/NOTIFY，仅用于加速失效）");

            loop {
                match watcher.recv().await {
                    Ok(change) => {
                        if let Err(err) = apply_catalog_change(&state, &change).await {
                            tracing::warn!(
                                code = err.code().as_str(),
                                message = %err,
                                "处理 catalog 变更失败（等待下一次 reconcile 兜底）"
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            code = err.code.as_str(),
                            message = %err.message,
                            "catalog 变更订阅中断，准备重连"
                        );
                        break;
                    }
                }
            }
            tokio::time::sleep(RECONNECT_BACKOFF).await;
        }
    })
}

/// 应用一条 Catalog 变更：只刷新受影响的路由，不做全量拉取。
async fn apply_catalog_change(state: &AppState, change: &catalog::CatalogChange) -> ApiResult<()> {
    // 只有 databases 表的变更能定位到具体路由；其它表（workers / operations …）
    // 不改变 db -> worker 的映射，交给周期 reconcile 即可。
    match change_database_id(change) {
        Some(database_id) => match state.router.refresh_route(database_id).await {
            Ok(_) => observability::metrics::record_route_refresh_micros(0),
            // DB 被删除：立刻摘掉路由，避免继续往已删除的库发请求。
            Err(err) if err.code() == ErrorCode::DbNotFound => {
                state.routes.invalidate(&database_id);
            }
            Err(err) => return Err(err),
        },
        None => {
            tracing::debug!(
                table = %change.table,
                op = %change.op,
                "catalog 变更不改变路由映射，交由周期 reconcile"
            );
        }
    }
    Ok(())
}

/// 从变更通知里解析出受影响的数据库 ID。
///
/// 约定：只有 `databases` 表的行级变更才带得动路由；其余表返回 `None`。
#[must_use]
pub fn change_database_id(change: &catalog::CatalogChange) -> Option<DatabaseId> {
    if change.table != "databases" {
        return None;
    }
    change.id.parse::<DatabaseId>().ok()
}

// ==================================================================== 心跳监控

/// Worker 心跳监控（架构 §16）。
///
/// 判定链：`missed_heartbeats >= 3` -> `SUSPECT`（停止新放置，保留已有 DB）；
/// 判定 `UNAVAILABLE` 后立刻做 Owner 失效接管：为它名下的每个 DB 重新选 Worker、
/// 推进 ownership epoch（fencing 掉旧进程）、再拉起新进程。
#[must_use]
pub fn spawn_heartbeat_monitor(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(HEARTBEAT_CHECK_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // 先把就绪标记置上：监控循环本身已经存在，能否判定超时依赖 Catalog 的
        // last_heartbeat_at，而不是「跑满一轮」。
        state.readiness.mark_heartbeat_monitor_ready();
        loop {
            ticker.tick().await;
            if let Err(err) = heartbeat_tick(&state).await {
                tracing::warn!(
                    code = err.code().as_str(),
                    message = %err,
                    "心跳检查失败（下一个周期重试）"
                );
            }
        }
    })
}

/// 一次心跳检查。
async fn heartbeat_tick(state: &AppState) -> ApiResult<()> {
    // 本轮不做「刚刚收到过心跳」的排除：Catalog 在每次心跳里会把计数清零，
    // 因此这条 SQL 只会命中真正超时的 Worker。
    let missed = state.catalog.mark_missed_heartbeats(&[]).await.api()?;
    for (worker_id, count) in missed {
        if count < state.background.suspect_threshold {
            continue;
        }
        let state_now = if count >= state.background.suspect_threshold {
            WorkerState::Suspect
        } else {
            WorkerState::Active
        };
        // SUSPECT 只停止新放置；连续两次判定仍不恢复才升级为 UNAVAILABLE，
        // 避免一次网络抖动就触发全量 failover（那会造成不必要的进程重启）。
        if count == state.background.suspect_threshold {
            tracing::warn!(worker_id = %worker_id, missed = count, "Worker 心跳超时，标记 SUSPECT");
            mark_worker_state(state, &worker_id, state_now).await?;
            continue;
        }
        tracing::error!(worker_id = %worker_id, missed = count, "Worker 连续心跳超时，判定 UNAVAILABLE");
        mark_worker_state(state, &worker_id, WorkerState::Unavailable).await?;
        failover_worker(state, &worker_id).await?;
    }
    Ok(())
}

async fn mark_worker_state(
    state: &AppState,
    worker_id: &WorkerId,
    new_state: WorkerState,
) -> ApiResult<()> {
    // 已经是终态则不再重复写（UNAVAILABLE 是幂等的，重复写只会刷 updated_at）。
    match state.catalog.get_worker(worker_id.clone()).await {
        Ok(worker) if worker.state == new_state => return Ok(()),
        Ok(_) => {}
        // Worker 行不存在 = 已不可用（proto 未定义 WORKER_NOT_FOUND，沿用 WORKER_UNAVAILABLE）
        Err(err) if err.code == ErrorCode::WorkerUnavailable => return Ok(()),
        Err(err) => return Err(ApiError::from(err)),
    }
    state
        .catalog
        .mark_worker_state(worker_id.clone(), new_state)
        .await
        .api()?;
    Ok(())
}

/// 为故障 Worker 名下的 DB 做 Owner 失效接管（架构 §11.2 / §12.2）。
async fn failover_worker(state: &AppState, dead_worker: &WorkerId) -> ApiResult<()> {
    let databases = state
        .catalog
        .list_databases(DatabaseFilter {
            worker_id: Some(dead_worker.clone()),
            limit: Some(1000),
            ..Default::default()
        })
        .await
        .api()?;

    for record in databases {
        if record.is_deleted() || !record.state.occupies_process() {
            continue;
        }
        match failover_one(state, &record, dead_worker).await {
            Ok(worker_id) => tracing::info!(
                database_id = %record.id,
                from = %dead_worker,
                to = %worker_id,
                "Owner 失效接管完成"
            ),
            Err(err) => tracing::error!(
                database_id = %record.id,
                code = err.code().as_str(),
                message = %err,
                "Owner 失效接管失败（保留 ownership，等待租约 GC 或人工介入）"
            ),
        }
    }
    Ok(())
}

/// 单个 DB 的失效接管：选新家 -> 推进 epoch -> 拉起进程。
async fn failover_one(
    state: &AppState,
    record: &DatabaseRecord,
    dead_worker: &WorkerId,
) -> ApiResult<WorkerId> {
    let workers = state.catalog.list_workers().await.api()?;
    // respawning = true：允许选到「正在重启 / 保留容量」的 Worker（它们正是 reserve 载体）。
    let candidates: Vec<WorkerCandidate> = workers
        .iter()
        .map(|worker| WorkerCandidate::from_record(worker, true))
        .collect();

    let mut request = PlacementRequest::new(record.id, record.resource_budget());
    request.priority = record.priority;
    request.affinity_worker = record.affinity_worker_id.clone();
    request.anti_affinity_worker = record.anti_affinity_worker_id.clone();
    // 绝不回迁到刚判定故障的 Worker。
    request.exclude = vec![dead_worker.clone()];

    let decision = state
        .router
        .scheduler()
        .select_for_failover(&request, &candidates)
        .map_err(|err| ApiError::from(err.to_platform_error()))?;
    let target = decision.worker_id;

    // epoch 单调递增：旧 Worker 即便短暂恢复，它的写入也会被 Storage Fencing 拒绝。
    let new_epoch = state
        .catalog
        .bump_ownership(
            record.id,
            record.owner_epoch.get(),
            target.clone(),
            OWNERSHIP_LEASE_TTL,
        )
        .await
        .api()?;
    state.routes.invalidate(&record.id);

    // 走「带存储层对齐」的启动入口：接管后若被 Remote WAL fencing 拒绝（Catalog 的
    // epoch 落后于存储层记录值），控制面会向 WAL 对齐并重试一次（§11.3），
    // 否则这个 DB 会在新 Worker 上稳定启动失败。
    state
        .router
        .start_on_worker_with_epoch_realign(record, &target, new_epoch, WORKER_CONTROL_TIMEOUT)
        .await?;
    Ok(target)
}

// ==================================================================== Ownership GC

/// 周期性回收租约过期的 ownership（架构 §10 / §16「超时自动过期」）。
#[must_use]
pub fn spawn_ownership_gc(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(OWNERSHIP_GC_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // 幂等键清理比 ownership GC 低频得多（每 ~10 分钟一次即可）。
        let purge_every = 40u32;
        let mut ticks: u32 = 0;
        loop {
            ticker.tick().await;
            ticks = ticks.wrapping_add(1);
            match state
                .catalog
                .clear_stale_ownership(OWNERSHIP_STALE_GRACE)
                .await
            {
                Ok(reclaimed) if !reclaimed.is_empty() => {
                    tracing::warn!(count = reclaimed.len(), "回收租约过期的 ownership");
                    // 路由必须立刻失效：这些 DB 已经没有合法 Owner，继续路由会打到旧进程。
                    for database_id in reclaimed {
                        state.routes.invalidate(&database_id);
                    }
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(
                    code = err.code.as_str(),
                    message = %err.message,
                    "ownership GC 失败"
                ),
            }

            if ticks.is_multiple_of(purge_every) {
                match state.catalog.purge_expired_idempotency().await {
                    Ok(purged) if purged > 0 => {
                        tracing::info!(purged, "清理过期的 idempotency key")
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(
                        code = err.code.as_str(),
                        message = %err.message,
                        "idempotency 清理失败"
                    ),
                }
            }
        }
    })
}

// ==================================================================== 生命周期回收

/// WARM/空闲 DB 的驱逐（架构 §7「idle / eviction」）。
///
/// 判定条件（全部满足才驱逐，宁可少回收也不要误杀）：
/// 1. 状态为 WARM（HOT 说明有持续流量，不回收）；
/// 2. `evictable` 为 true（用户可标记为常驻）；
/// 3. 空闲时长超过 WARM_IDLE_EVICTION_AFTER（见 config）；
/// 4. Owner Worker 的资源水位达到停止线（[`policy::STOP_NEW_PLACEMENT`]）——
///    没有资源压力时驱逐只会造成后续请求的冷启动抖动。
#[must_use]
pub fn spawn_eviction(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(EVICTION_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(err) = eviction_tick(&state).await {
                tracing::warn!(
                    code = err.code().as_str(),
                    message = %err,
                    "生命周期回收失败（下一个周期重试）"
                );
            }
        }
    })
}

async fn eviction_tick(state: &AppState) -> ApiResult<()> {
    let idle_after = chrono::Duration::from_std(state.background.warm_idle_eviction_after)
        .unwrap_or_else(|_| chrono::Duration::minutes(10));
    let cutoff = Utc::now() - idle_after;

    let warm = state
        .catalog
        .list_databases(DatabaseFilter {
            state: Some(LifecycleState::Warm),
            limit: Some(500),
            ..Default::default()
        })
        .await
        .api()?;
    if warm.is_empty() {
        return Ok(());
    }

    // 水位按 Worker 聚合后再判断：同一 Worker 上的多个空闲库只需查一次 Worker。
    let mut workers = std::collections::HashMap::new();
    for record in &warm {
        let Some(owner) = record.owner_worker_id.clone() else {
            continue;
        };
        if workers.contains_key(&owner) {
            continue;
        }
        match state.catalog.get_worker(owner.clone()).await {
            Ok(worker) => {
                workers.insert(owner, worker);
            }
            Err(err) if err.code == ErrorCode::WorkerUnavailable => continue,
            Err(err) => return Err(ApiError::from(err)),
        }
    }

    for record in warm {
        if !record.evictable || record.updated_at > cutoff {
            continue;
        }
        let Some(owner) = record.owner_worker_id.clone() else {
            continue;
        };
        let Some(worker) = workers.get(&owner) else {
            continue;
        };
        let utilization = WorkerCandidate::from_record(worker, false)
            .utilization_after(&record.resource_budget())
            .max();
        if utilization < policy::STOP_NEW_PLACEMENT {
            continue;
        }

        tracing::info!(
            database_id = %record.id,
            worker_id = %owner,
            utilization,
            "Worker 资源水位偏高且 DB 空闲，执行生命周期回收"
        );
        match stop_and_cool(state, &record).await {
            Ok(()) => observability::metrics::record_snapshot("evicted"),
            Err(err) => tracing::warn!(
                database_id = %record.id,
                code = err.code().as_str(),
                message = %err,
                "驱逐失败"
            ),
        }
    }
    Ok(())
}

/// 停止 DB 并把 Catalog 状态置回 COLD（释放 Owner）。
async fn stop_and_cool(state: &AppState, record: &DatabaseRecord) -> ApiResult<()> {
    if let Some(worker_id) = record.owner_worker_id.clone() {
        // 尽力停止：Worker 不可达时不阻塞状态推进（进程会被 Worker 侧回收）。
        if let Err(err) = state
            .router
            .stop_on_worker(
                record.id,
                &worker_id,
                record.owner_epoch.get(),
                true,
                WORKER_CONTROL_TIMEOUT,
            )
            .await
        {
            tracing::warn!(
                database_id = %record.id,
                code = err.code().as_str(),
                message = %err,
                "停止 DB 失败，仍将状态置为 COLD 并交由租约 GC 兜底"
            );
        }
    }
    state
        .catalog
        .set_lifecycle_state(record.id, LifecycleState::Cold, None)
        .await
        .api()?;
    state.routes.invalidate(&record.id);
    Ok(())
}

// ==================================================================== Job Runner

/// 长操作 Job Runner。
#[must_use]
pub fn spawn_job_runner(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let lease_owner = format!("db-server-{}", std::process::id());
        loop {
            // 事件驱动 + 兜底轮询：notify 让排队 job 立刻被执行，轮询保证不丢事件。
            state.jobs.wait(JOB_POLL_INTERVAL).await;
            loop {
                let leased = match state
                    .catalog
                    .lease_job(&lease_owner, state.background.job_lease_ttl, job_kind::ALL)
                    .await
                {
                    Ok(Some(job)) => job,
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(
                            code = err.code.as_str(),
                            message = %err.message,
                            "抢占 job 失败"
                        );
                        break;
                    }
                };
                run_job(&state, &leased).await;
            }
        }
    })
}

/// 执行一个 job，并把结果同步到关联的 operation。
async fn run_job(state: &AppState, job: &JobRecord) {
    let operation_id = job
        .payload
        .get("operation_id")
        .and_then(|v| v.as_str())
        .and_then(|raw| raw.parse::<domain::ids::OperationId>().ok());

    // 推进到 RUNNING（重试时会重新置为 RUNNING，语义正确：这是「本次尝试」的开始）。
    if let Some(operation_id) = operation_id {
        if let Err(err) = state
            .catalog
            .update_operation(operation_id, "RUNNING", 10, None, json!({}))
            .await
        {
            tracing::warn!(job_id = %job.id, error = %err.message, "更新 operation 为 RUNNING 失败");
        }
    }

    let started = std::time::Instant::now();
    let outcome = execute_job(state, job).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    match outcome {
        Ok(result) => {
            tracing::info!(job_id = %job.id, kind = %job.kind, elapsed_ms, "job 执行成功");
            if let Some(operation_id) = operation_id {
                if let Err(err) = state
                    .catalog
                    .update_operation(operation_id, "SUCCEEDED", 100, None, result.clone())
                    .await
                {
                    tracing::error!(job_id = %job.id, error = %err.message, "回写 operation 成功状态失败");
                }
            }
            if let Err(err) = state.catalog.complete_job(job.id, true, None).await {
                tracing::error!(job_id = %job.id, error = %err.message, "标记 job 完成失败");
            }
        }
        Err(err) => {
            tracing::warn!(
                job_id = %job.id,
                kind = %job.kind,
                code = err.code().as_str(),
                message = %err,
                elapsed_ms,
                "job 执行失败"
            );
            if let Some(operation_id) = operation_id {
                if let Err(write_err) = state
                    .catalog
                    .update_operation(
                        operation_id,
                        "FAILED",
                        100,
                        Some((err.code(), err.to_string())),
                        json!({}),
                    )
                    .await
                {
                    tracing::error!(error = %write_err.message, "回写 operation 失败状态失败");
                }
            }
            if let Err(err) = state
                .catalog
                .complete_job(job.id, false, Some(err.to_string()))
                .await
            {
                tracing::error!(job_id = %job.id, error = %err.message, "标记 job 失败失败");
            }
        }
    }
}

/// job 分发。
///
/// # Errors
/// 任一业务步骤失败即返回错误；Runner 负责把它写进 operation 与 job。
async fn execute_job(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    match job.kind.as_str() {
        job_kind::DB_CREATE => job_db_create(state, job).await,
        job_kind::DB_START => job_db_start(state, job).await,
        job_kind::DB_STOP => job_db_stop(state, job).await,
        job_kind::DB_RESTART => job_db_restart(state, job).await,
        job_kind::DB_MOVE => job_db_move(state, job).await,
        job_kind::DB_DELETE => job_db_delete(state, job).await,
        job_kind::DB_SNAPSHOT => job_db_snapshot(state, job, false).await,
        job_kind::DB_BACKUP => job_db_snapshot(state, job, true).await,
        job_kind::DB_RESTORE => job_db_restore(state, job).await,
        job_kind::WORKER_DRAIN => job_worker_drain(state, job).await,
        other => Err(ApiError::invalid_argument(format!(
            "未知 job kind '{other}'"
        ))),
    }
}

/// 从 payload 里取必需的数据库 ID。
fn payload_database_id(job: &JobRecord) -> ApiResult<DatabaseId> {
    payload_str(job, "database_id")?
        .parse::<DatabaseId>()
        .map_err(|err| ApiError::invalid_argument(format!("job payload database_id 非法: {err}")))
}

fn payload_str(job: &JobRecord, key: &str) -> ApiResult<String> {
    job.payload
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| {
            ApiError::invalid_argument(format!("job {} payload 缺少字段 '{key}'", job.id))
        })
}

fn payload_bool(job: &JobRecord, key: &str) -> bool {
    job.payload
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// DB_CREATE：创建操作的收尾。
///
/// 创建在 Serverless 模型下**不需要**预先物化工作集：DB 落地即 COLD，
/// 首次访问由冷启动路径把进程拉起来（架构 §7 / §8）。这个 job 因此只做
/// 「确认元数据就绪」这一件可验证的事，不做无意义的等待。
async fn job_db_create(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;
    Ok(json!({
        "database_id": record.id.to_string(),
        "state": record.state.to_db_str(),
        "storage_prefix": record.storage_prefix,
    }))
}

/// DB_START：走与冷启动完全相同的路径（Coalesce + Placement + StartDatabase）。
async fn job_db_start(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;
    if record.state.is_serving() {
        // 已在服务：幂等成功，不重复拉起进程。
        return Ok(json!({ "state": record.state.to_db_str(), "noop": true }));
    }
    let route = state
        .router
        .ensure_started(database_id, WORKER_CONTROL_TIMEOUT)
        .await?;
    Ok(json!({
        "state": route.state.to_db_str(),
        "worker_id": route.worker_id.to_string(),
        "owner_epoch": route.owner_epoch,
    }))
}

/// DB_STOP：停止进程并把状态置回 COLD。
async fn job_db_stop(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;
    if record.is_deleted() {
        return Err(ApiError::not_found(format!("数据库 {database_id} 已删除")));
    }
    stop_and_cool(state, &record).await?;
    Ok(json!({ "state": LifecycleState::Cold.to_db_str() }))
}

/// DB_RESTART：先停后起；不等 Worker 的 Restart RPC，因为「停 + 起」的语义
/// 与 Worker 内部实现无关，且能复用同一条冷启动路径（含 placement 复查）。
async fn job_db_restart(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;
    stop_and_cool(state, &record).await?;
    let route = state
        .router
        .ensure_started(database_id, WORKER_CONTROL_TIMEOUT)
        .await?;
    Ok(json!({
        "state": route.state.to_db_str(),
        "worker_id": route.worker_id.to_string(),
    }))
}

/// DB_MOVE：迁移到指定 / 自动选择的 Worker。
///
/// 实现为 **stop-and-start + epoch 推进**（而不是 Worker 侧的热迁移预拉取）：
/// 1. 选目标 Worker（显式指定则必须存在且能通过准入检查）；
/// 2. `bump_ownership` 推进 epoch —— 旧进程随后的任何写入都会被 fencing 拒绝；
/// 3. 在新 Worker 上 `StartDatabase(restore_from_snapshot = true)`；
/// 4. 最后 Kill 旧 Worker 上的残留进程。
///
/// 这是「正确但非最优」的迁移：语义（epoch 推进 + 旧进程失效）是完备的，
/// 代价是一次冷启动；热迁移的 `PrepareMove` 预拉取留待后续优化。
async fn job_db_move(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;
    if record.is_deleted() {
        return Err(ApiError::not_found(format!("数据库 {database_id} 已删除")));
    }
    let previous_owner = record.owner_worker_id.clone();
    let previous_epoch = record.owner_epoch.get();

    let target = match job.payload.get("target_worker_id").and_then(|v| v.as_str()) {
        Some(raw) if !raw.trim().is_empty() => WorkerId::new(raw.trim()),
        _ => {
            let workers = state.catalog.list_workers().await.api()?;
            let candidates: Vec<WorkerCandidate> = workers
                .iter()
                .map(|worker| WorkerCandidate::from_record(worker, false))
                .collect();
            let mut request = PlacementRequest::new(record.id, record.resource_budget());
            request.priority = record.priority;
            request.affinity_worker = record.affinity_worker_id.clone();
            request.anti_affinity_worker = record.anti_affinity_worker_id.clone();
            // 排除当前 Owner：否则「迁移」可能原地不动。
            if let Some(current) = previous_owner.clone() {
                request.exclude = vec![current];
            }
            state
                .router
                .scheduler()
                .select(&request, &candidates)
                .map_err(|err| err.to_platform_error())
                .api()?
                .worker_id
        }
    };

    // 目标 Worker 必须存在且能通过准入检查（显式指定时尤其重要）。
    let worker = state.catalog.get_worker(target.clone()).await.api()?;
    let admission = state.router.scheduler().admission_check(
        &WorkerCandidate::from_record(&worker, false),
        &record.resource_budget(),
    );
    if !admission.allows() {
        let reason = admission.reason().unwrap_or("资源不足").to_string();
        observability::metrics::record_admission_denied(admission.as_str());
        return Err(ApiError::new(
            ErrorCode::AdmissionDenied,
            format!("目标 Worker {target} 拒绝放置：{reason}"),
        ));
    }

    if previous_owner.as_ref() == Some(&target) {
        return Ok(
            json!({ "worker_id": target.to_string(), "moved": false, "reason": "已在目标 Worker" }),
        );
    }

    let new_epoch = state
        .catalog
        .bump_ownership(
            record.id,
            previous_epoch,
            target.clone(),
            OWNERSHIP_LEASE_TTL,
        )
        .await
        .api()?;
    state.routes.invalidate(&record.id);

    let refreshed = state.catalog.get_database(database_id).await.api()?;
    state
        .router
        .start_on_worker(&refreshed, &target, new_epoch, true, WORKER_CONTROL_TIMEOUT)
        .await?;

    // 旧进程清理：失败只告警 —— epoch 已经递增，旧进程无法再写 WAL / Storage。
    // 用 as_ref 借用而不是移动：后面构造响应体时还要用 previous_owner。
    if let Some(old_worker) = previous_owner.as_ref() {
        if old_worker != &target {
            if let Err(err) = state
                .router
                .kill_on_worker(database_id, old_worker, previous_epoch, "moved away")
                .await
            {
                tracing::warn!(
                    database_id = %database_id,
                    worker_id = %old_worker,
                    message = %err,
                    "旧 Owner 上的进程清理失败（epoch 已递增，其写入会被 fencing 拒绝）"
                );
            }
        }
    }

    Ok(json!({
        "worker_id": target.to_string(),
        "previous_worker_id": previous_owner.map(|w| w.to_string()),
        "owner_epoch": new_epoch,
        "moved": true,
    }))
}

/// DB_DELETE：先停（尽力），再软删除。
async fn job_db_delete(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    match state.catalog.get_database(database_id).await {
        Ok(record) => {
            // 先停（尽力）再软删：不先停会让进程在库被删后继续持有本地 working set。
            let _ = stop_and_cool(state, &record).await;
        }
        Err(err) if err.code == ErrorCode::DbNotFound => {
            // 已经不存在：删除天然幂等，直接成功。
            return Ok(json!({
                "database_id": database_id.to_string(),
                "deleted_at": serde_json::Value::Null,
                "noop": true,
            }));
        }
        Err(err) => return Err(ApiError::from(err)),
    }

    let deleted = state
        .catalog
        .soft_delete_database(database_id)
        .await
        .api()?;
    state.routes.invalidate(&database_id);
    Ok(json!({
        "database_id": database_id.to_string(),
        "deleted_at": deleted.deleted_at.map(|t| t.to_rfc3339()),
    }))
}

/// DB_SNAPSHOT / DB_BACKUP：触发 Worker 侧快照并登记元数据。
///
/// `backup = true` 时额外写 `backup_jobs`（对外可查询的备份历史）。
async fn job_db_snapshot(
    state: &AppState,
    job: &JobRecord,
    backup: bool,
) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;
    let worker_id = record.owner_worker_id.clone().ok_or_else(|| {
        ApiError::new(
            ErrorCode::DatabaseNotReady,
            format!("数据库 {database_id} 当前没有 Owner，无法快照（先 start）"),
        )
    })?;

    let backup_job = if backup {
        Some(
            state
                .catalog
                .create_backup_job(catalog::CreateBackupJobParams {
                    database_id,
                    kind: "BACKUP".to_string(),
                    operation_id: job
                        .payload
                        .get("operation_id")
                        .and_then(|v| v.as_str())
                        .and_then(|raw| raw.parse::<domain::ids::OperationId>().ok()),
                    snapshot_id: None,
                    target_time: None,
                })
                .await
                .api()?,
        )
    } else {
        None
    };

    let result = state
        .router
        .trigger_snapshot(
            database_id,
            &worker_id,
            record.owner_epoch.get(),
            WORKER_CONTROL_TIMEOUT,
        )
        .await;

    let meta = match result {
        Ok(meta) => meta,
        Err(err) => {
            if let Some(backup_job) = backup_job {
                let _ = state
                    .catalog
                    .update_backup_job_state(
                        backup_job.id,
                        catalog::BackupJobUpdate {
                            state: "FAILED".to_string(),
                            error_message: Some(err.to_string()),
                            ..Default::default()
                        },
                    )
                    .await;
            }
            return Err(err);
        }
    };

    // 登记 Snapshot 元数据：Worker 才是快照的产出方，Server 只做权威登记。
    // `SnapshotMeta` 不含 compression / schema_version：这两项属于 Catalog 侧补充元数据，
    // Worker 回传后由 Server 用当前部署的约定值补齐（压缩算法由 Worker 侧配置决定，
    // 上线后固定为 zstd；schema_version 在 schema 迁移机制落地前恒为 1）。
    let snapshot = SnapshotRecord {
        id: meta
            .snapshot_id
            .parse::<domain::ids::SnapshotId>()
            .unwrap_or_else(|_| domain::ids::SnapshotId::new_v7()),
        database_id,
        base_lsn: domain::wal::Lsn::new(meta.base_lsn),
        checksum: meta.checksum.clone(),
        size_bytes: meta.size_bytes,
        object_key: meta.object_key.clone(),
        compression: DEFAULT_SNAPSHOT_COMPRESSION.to_string(),
        owner_epoch: domain::wal::OwnerEpoch::new(meta.owner_epoch_at_snapshot),
        engine_version: meta.engine_version.clone(),
        schema_version: DEFAULT_SCHEMA_VERSION,
        state: "AVAILABLE".to_string(),
        created_at: Utc::now(),
        verified_at: Some(Utc::now()),
    };
    let stored = state.catalog.insert_snapshot(snapshot).await.api()?;

    if let Some(backup_job) = backup_job {
        let _ = state
            .catalog
            .update_backup_job_state(
                backup_job.id,
                catalog::BackupJobUpdate {
                    state: "SUCCEEDED".to_string(),
                    snapshot_id: Some(stored.id.to_string()),
                    actual_point: Some(Utc::now()),
                    bytes_transferred: Some(stored.size_bytes as i64),
                    error_message: None,
                },
            )
            .await;
    }

    observability::metrics::record_snapshot(if backup { "backup" } else { "snapshot" });
    Ok(json!({
        "snapshot_id": stored.id.to_string(),
        "base_lsn": stored.base_lsn.get(),
        "size_bytes": stored.size_bytes,
        "object_key": stored.object_key,
    }))
}

/// DB_RESTORE：从快照恢复（停止当前进程 -> 释放 Owner -> 重新冷启动）。
///
/// 简化说明：不做 PITR（目标时间点回放），只恢复快照 + 从快照 LSN 之后重放 Remote WAL，
/// 后者由 Worker 侧在启动时完成（它知道本地 working set 的 LSN）。
async fn job_db_restore(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let database_id = payload_database_id(job)?;
    let record = state.catalog.get_database(database_id).await.api()?;

    let snapshot = match job.payload.get("snapshot_id").and_then(|v| v.as_str()) {
        Some(raw) if !raw.trim().is_empty() => {
            let snapshot_id = raw
                .trim()
                .parse::<domain::ids::SnapshotId>()
                .map_err(|err| ApiError::invalid_argument(format!("snapshot_id 非法: {err}")))?;
            state
                .catalog
                .list_snapshots(database_id, 1000)
                .await
                .api()?
                .into_iter()
                .find(|s| s.id == snapshot_id)
                .ok_or_else(|| {
                    ApiError::new(
                        ErrorCode::SnapshotUnavailable,
                        format!("数据库 {database_id} 下找不到快照 {snapshot_id}"),
                    )
                })?
        }
        _ => state
            .catalog
            .latest_snapshot(database_id)
            .await
            .api()?
            .ok_or_else(|| {
                ApiError::new(
                    ErrorCode::SnapshotUnavailable,
                    format!("数据库 {database_id} 没有任何可用快照，无法恢复"),
                )
            })?,
    };

    // 先把当前进程停掉并释放 Owner：恢复必须从冷状态开始，否则会在旧进程上
    // 恢复出「半新半旧」的工作集。
    stop_and_cool(state, &record).await?;
    let route = state
        .router
        .ensure_started(database_id, WORKER_CONTROL_TIMEOUT)
        .await?;

    Ok(json!({
        "snapshot_id": snapshot.id.to_string(),
        "base_lsn": snapshot.base_lsn.get(),
        "state": route.state.to_db_str(),
        "worker_id": route.worker_id.to_string(),
    }))
}

/// WORKER_DRAIN：标记 DRAINING 并请求 Worker 迁移 / 停止其上的 DB。
async fn job_worker_drain(state: &AppState, job: &JobRecord) -> ApiResult<serde_json::Value> {
    let worker_id = WorkerId::new(payload_str(job, "worker_id")?);
    let stop_cold_and_warm = payload_bool(job, "stop_cold_and_warm");

    let worker = state.catalog.get_worker(worker_id.clone()).await.api()?;
    if worker.state != WorkerState::Draining {
        state
            .catalog
            .mark_worker_state(worker_id.clone(), WorkerState::Draining)
            .await
            .api()?;
    }

    let (migrated, remaining) = state
        .router
        .drain_worker(&worker_id, stop_cold_and_warm, WORKER_CONTROL_TIMEOUT)
        .await?;

    // 排空过程中 DB 换了 Owner，本地路由必须失效重建。
    let owned = state
        .catalog
        .list_databases(DatabaseFilter {
            worker_id: Some(worker_id.clone()),
            limit: Some(1000),
            ..Default::default()
        })
        .await
        .api()?;
    for record in owned {
        state.routes.invalidate(&record.id);
    }

    if remaining == 0 {
        state
            .catalog
            .mark_worker_state(worker_id.clone(), WorkerState::Empty)
            .await
            .api()?;
    }

    Ok(json!({
        "worker_id": worker_id.to_string(),
        "migrated": migrated,
        "remaining": remaining,
    }))
}

// ==================================================================== 会话清理

/// 定期清理本地过期会话，并尽力关闭 Worker 侧会话（架构 §13.2）。
#[must_use]
pub fn spawn_session_sweeper(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(SESSION_SWEEP_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let expired = state.sessions.take_expired(Utc::now());
            for binding in expired {
                if let Err(err) = state
                    .router
                    .close_session(
                        &binding.database_id,
                        &binding.worker_id,
                        &binding.session_id,
                    )
                    .await
                {
                    tracing::debug!(
                        session_id = %binding.session_id,
                        message = %err,
                        "关闭过期会话失败（Worker 侧会按自己的空闲计时回收）"
                    );
                }
            }
        }
    })
}

// ==================================================================== 常量

/// 快照压缩算法：由 Worker 侧配置决定，部署后固定。
const DEFAULT_SNAPSHOT_COMPRESSION: &str = "zstd";
/// 当前 schema 版本（schema 迁移机制落地前恒为 1）。
const DEFAULT_SCHEMA_VERSION: i32 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_kinds_are_unique_and_non_empty() {
        let mut seen = std::collections::HashSet::new();
        for kind in job_kind::ALL {
            assert!(!kind.is_empty());
            assert!(seen.insert(*kind), "job kind 重复: {kind}");
        }
        assert_eq!(seen.len(), job_kind::ALL.len());
    }

    #[test]
    fn payload_helpers_reject_missing_fields() {
        let job = JobRecord {
            id: domain::ids::JobId::new_v7(),
            kind: job_kind::DB_START.to_string(),
            payload: json!({}),
            state: "LEASED".to_string(),
            priority: 0,
            run_after: Utc::now(),
            lease_owner: None,
            lease_expires_at: None,
            attempts: 1,
            max_attempts: 3,
            last_error: None,
            idempotency_key: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            finished_at: None,
        };
        let err = payload_database_id(&job).expect_err("缺字段必须报错");
        assert_eq!(err.code(), ErrorCode::InvalidArgument);
        assert!(!payload_bool(&job, "stop_cold_and_warm"));
    }

    #[test]
    fn change_database_id_only_matches_databases_table() {
        let db_id = DatabaseId::new_v7();
        let db_change = catalog::CatalogChange {
            version: 3,
            table: "databases".to_string(),
            op: "UPDATE".to_string(),
            id: db_id.to_string(),
        };
        assert_eq!(change_database_id(&db_change), Some(db_id));

        // 其它表的变更不影响 db -> worker 映射
        let worker_change = catalog::CatalogChange {
            version: 4,
            table: "workers".to_string(),
            op: "UPDATE".to_string(),
            id: WorkerId::new("w1").to_string(),
        };
        assert_eq!(change_database_id(&worker_change), None);

        // id 不是 UUID（例如版本行）时安全返回 None，不 panic
        let version_change = catalog::CatalogChange {
            version: 5,
            table: "databases".to_string(),
            op: "UPDATE".to_string(),
            id: "not-a-uuid".to_string(),
        };
        assert_eq!(change_database_id(&version_change), None);
    }
}
