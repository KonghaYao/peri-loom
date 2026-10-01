//! DB Router：Route Cache 热路径 + 冷启动透明 Wake + Stale Route 透明重试（架构 §4.2 / §8 / §16）。
//!
//! 三条不可违反的规则：
//! 1. **热路径只读本地缓存**。命中且 `WARM`/`HOT` 时，本模块不会碰 PostgreSQL ——
//!    Control Plane 短暂不可用时，已运行 DB 的正常 SQL 不受影响。
//! 2. **Control Plane 不可用不得主动清空缓存**。reconcile 失败只记日志（见
//!    [`DbRouter::reconcile_all`] 的调用方 `background::spawn_reconciler`），
//!    缓存里的路由继续服务，直到全量 reconcile 成功才被替换。
//! 3. **不把 `WAKING` 泄漏给客户端**。冷启动要么在 deadline 内 READY 并继续执行原请求，
//!    要么返回明确的 `WAKEUP_TIMEOUT` / `DATABASE_NOT_READY`。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use catalog::{Catalog, RoutingEntry};
use domain::error::{ErrorCode, PlatformError};
use domain::ids::{DatabaseId, WorkerId};
use domain::lifecycle::LifecycleState;
use domain::records::{DatabaseRecord, WorkerRecord};
use domain::time::now_unix_ms;
use futures::{Stream, StreamExt};
use protocol::control::{
    DrainWorkerRequest, KillDatabaseRequest, RestartDatabaseRequest, StartDatabaseRequest,
    StopDatabaseRequest, TriggerSnapshotRequest,
};
use protocol::convert::platform_error_from_proto;
use protocol::data::{StreamFrame, Value};
use routing::{RouteCache, RouteEntry};
use scheduler::{PlacementRequest, Scheduler, WorkerCandidate};
use wal_client::WalClient;

use crate::clients::{
    check_proto_error, close_session_request, data_request_context, execute_request,
    is_fencing_rejection, is_stale_route_error, is_stale_route_status, session_execute_request,
    status_to_api_error, CancelGuard, ChannelPool, CONTROL_RPC_FALLBACK_TIMEOUT_MS,
};
use crate::config::{OWNERSHIP_LEASE_TTL, WORKER_CONTROL_TIMEOUT};
use crate::error::{ApiError, ApiResult, PlatformResultExt};

/// 数据面 RPC 的统一返回形态：`Ok(Ok(v))` 成功、`Ok(Err(e))` Worker 结构化错误、
/// `Err(status)` 传输层错误。
pub type DataRpcOutcome<T> = Result<Result<T, PlatformError>, tonic::Status>;

/// 数据面帧流（`ExecuteStream` / `SessionExecuteStream` 的统一类型）。
pub type FrameStream = Pin<Box<dyn Stream<Item = Result<StreamFrame, tonic::Status>> + Send>>;

/// Router 行为参数。
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// 透明 Wake 的等待上限（超过即 `WAKEUP_TIMEOUT`）。
    pub wakeup_timeout: Duration,
    /// 透明 Wake 的轮询间隔。
    pub wakeup_poll_interval: Duration,
    /// 转发给 Worker 的单次执行「结果集内联上限」（字节），超出部分由 Worker 走流式。
    pub inline_result_limit_bytes: usize,
}

/// 一次数据面调用的目标形态。
#[derive(Debug, Clone)]
pub enum StreamTarget {
    /// 无会话单次查询（走 `ExecuteStream`）。
    Stateless {
        /// SQL 文本。
        sql: String,
        /// 绑定参数。
        params: Vec<Value>,
    },
    /// 会话内查询（走 `SessionExecuteStream`，**不做**透明重试：会话不可迁移）。
    Session {
        /// 会话 ID。
        session_id: String,
        /// SQL 文本。
        sql: String,
        /// 绑定参数。
        params: Vec<Value>,
    },
}

/// DB Router。
///
/// `Arc` 共享；内部只持有无状态组件（Catalog 是连接池句柄、RouteCache 是 DashMap、
/// Scheduler 是零大小结构），因此可以放进 axum 的 `State` 里。
pub struct DbRouter {
    catalog: Catalog,
    routes: Arc<RouteCache>,
    scheduler: Scheduler,
    channels: Arc<ChannelPool>,
    /// Remote WAL 客户端。
    ///
    /// 控制面**不写** WAL：它只在「启动被存储层 fencing 拒绝」时**读**一次已记录的
    /// owner epoch，用于把 Catalog 对齐到存储层（§11.3）。未配置 WAL 时为 `None`，
    /// 该恢复路径降级为「原样返回错误」。
    wal: Option<Arc<WalClient>>,
    config: RouterConfig,
}

impl std::fmt::Debug for DbRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbRouter")
            .field("routes", &self.routes.stats())
            .field("wakeup_timeout_ms", &self.config.wakeup_timeout.as_millis())
            .finish()
    }
}

impl DbRouter {
    /// 构造 router。
    #[must_use]
    pub fn new(
        catalog: Catalog,
        routes: Arc<RouteCache>,
        channels: Arc<ChannelPool>,
        wal: Option<Arc<WalClient>>,
        config: RouterConfig,
    ) -> Self {
        Self {
            catalog,
            routes,
            scheduler: Scheduler::new(),
            channels,
            wal,
            config,
        }
    }

    /// Route Cache 句柄（供 `/metrics` 与 reconcile 使用）。
    #[must_use]
    pub fn route_cache(&self) -> &Arc<RouteCache> {
        &self.routes
    }

    /// Catalog 句柄。
    #[must_use]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Scheduler 句柄（控制面后台任务做 placement / 准入判定时使用）。
    #[must_use]
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// 确保 DB 已启动并返回可路由的目标（控制面 job 与冷启动共用同一条路径）。
    ///
    /// 与热路径的差别只有一点：调用方给的是**相对超时**而不是请求的绝对 deadline。
    /// 内部仍走同一套 Coalesce + Placement + `StartDatabase` 流程，
    /// 避免出现「两条启动路径语义不一致」。
    ///
    /// # Errors
    /// 超时返回 `WAKEUP_TIMEOUT`；放置失败返回 `ADMISSION_DENIED` / `NO_ELIGIBLE_WORKER`。
    pub async fn ensure_started(
        &self,
        database_id: DatabaseId,
        timeout: Duration,
    ) -> ApiResult<RouteEntry> {
        self.wake(database_id, Some(Instant::now() + timeout)).await
    }

    /// 关闭 Worker 侧会话（`WorkerData.CloseSession`）。
    ///
    /// 失败不致命：Worker 侧还有自己的空闲计时会把会话收掉，这里只是让它更早释放。
    ///
    /// # Errors
    /// Worker 不可达 / 拒绝时返回错误（调用方可忽略）。
    pub async fn close_session(
        &self,
        database_id: &DatabaseId,
        worker_id: &WorkerId,
        session_id: &str,
    ) -> ApiResult<()> {
        let endpoint = self.data_endpoint_of(worker_id).await?;
        let mut client = self.channels.data(&endpoint).await?;
        let context = data_request_context(
            &crate::middleware::current_request_id(),
            *database_id,
            worker_id,
            0,
            Some(session_id.to_string()),
            0,
        );
        let response = tokio::time::timeout(
            WORKER_CONTROL_TIMEOUT,
            client.close_session(close_session_request(context, session_id)),
        )
        .await
        .map_err(|_| ApiError::new(ErrorCode::DeadlineExceeded, "关闭会话超时"))?
        .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        check_proto_error(response.into_inner().error.as_ref(), worker_id.as_ref())
    }

    // ------------------------------------------------------------ 热路径

    /// 热路径：只读本地 Route Cache，**绝不访问 PostgreSQL**。
    ///
    /// 返回 `Some` 表示可以立刻向该 Worker 发起数据面调用；`None` 表示缓存未命中，
    /// 调用方需要走 [`DbRouter::resolve_target`]（含控制面回源与透明 Wake）。
    #[must_use]
    pub fn cached_route(&self, database_id: &DatabaseId) -> Option<RouteEntry> {
        self.routes.get(database_id)
    }

    /// 解析可用的数据面目标（热路径命中直接返回；否则回源 Catalog，必要时透明 Wake）。
    ///
    /// # Errors
    /// - `DB_NOT_FOUND`：DB 不存在或已删除；
    /// - `DATABASE_NOT_READY`：DB 处于 DRAINING / STOPPING / FAILED；
    /// - `WAKEUP_TIMEOUT`：冷启动在 deadline 内未 READY（**不泄漏内部 WAKING 状态**）；
    /// - `ADMISSION_DENIED` / `WORKER_UNAVAILABLE`：放置或调用 Worker 失败。
    pub async fn resolve_target(
        &self,
        database_id: DatabaseId,
        deadline: Option<Instant>,
    ) -> ApiResult<RouteEntry> {
        // 1) 热路径
        if let Some(route) = self.routes.get(&database_id) {
            if route.is_serving() {
                return Ok(route);
            }
        }

        // 2) 缓存未命中 / 非服务态 -> 回源 Catalog（控制面路径，允许访问 PostgreSQL）
        let record = self.catalog.get_database(database_id).await.api()?;
        if record.is_deleted() {
            return Err(ApiError::not_found(format!("数据库 {database_id} 已删除")));
        }

        match record.state {
            LifecycleState::Warm | LifecycleState::Hot => {
                let owner = record.owner_worker_id.clone().ok_or_else(|| {
                    ApiError::new(
                        ErrorCode::DatabaseNotReady,
                        format!(
                            "数据库 {database_id} 状态为 {} 但没有 Owner，Catalog 数据不一致",
                            record.state.to_db_str()
                        ),
                    )
                })?;
                self.publish_route(&record, &owner).await
            }
            LifecycleState::Cold => self.wake(database_id, deadline).await,
            // STARTING：已有启动流程在跑，等同一个 READY 结果（同样受 Wake 上限约束，
            // 不能让一个卡住的启动把请求无限期挂住）。
            LifecycleState::Starting => {
                self.await_ready(
                    database_id,
                    clamp_deadline(deadline, self.config.wakeup_timeout),
                )
                .await
            }
            LifecycleState::Draining | LifecycleState::Stopping | LifecycleState::Failed => {
                Err(ApiError::new(
                    ErrorCode::DatabaseNotReady,
                    format!(
                        "数据库 {database_id} 当前状态为 {}，暂不可服务",
                        record.state.to_db_str()
                    ),
                ))
            }
        }
    }

    /// 把某条路由从缓存里摘掉并按需重新解析（stale route 重试路径使用）。
    async fn retry_route(
        &self,
        database_id: DatabaseId,
        deadline: Option<Instant>,
    ) -> ApiResult<RouteEntry> {
        // 必须显式失效：否则 resolve_target 会直接命中那条已知陈旧的路由。
        self.routes.invalidate(&database_id);
        self.resolve_target(database_id, deadline).await
    }

    /// 用 Catalog 的当前状态刷新单条路由（不触发 Wake），返回是否已发布。
    ///
    /// # Errors
    /// Catalog 访问失败时返回错误；返回 `Ok(None)` 表示该 DB 目前没有可路由的 Owner。
    pub async fn refresh_route(&self, database_id: DatabaseId) -> ApiResult<Option<RouteEntry>> {
        let record = self.catalog.get_database(database_id).await.api()?;
        if record.is_deleted() {
            self.routes.invalidate(&database_id);
            return Ok(None);
        }
        let Some(owner) = record.owner_worker_id.clone() else {
            self.routes.invalidate(&database_id);
            return Ok(None);
        };
        if !record.state.occupies_process() {
            // COLD / FAILED 不应继续留在缓存里（否则会路由到已经不存在的进程）
            self.routes.invalidate(&database_id);
            return Ok(None);
        }
        self.publish_route(&record, &owner).await.map(Some)
    }

    /// 把 (DB, Owner) 发布到 Route Cache。
    async fn publish_route(
        &self,
        record: &DatabaseRecord,
        owner: &WorkerId,
    ) -> ApiResult<RouteEntry> {
        let endpoint = self.data_endpoint_of(owner).await?;
        let entry = RouteEntry::new(
            record.id,
            owner.clone(),
            endpoint,
            record.owner_epoch.get(),
            record.state,
        );
        self.routes.upsert(entry.clone());
        Ok(entry)
    }

    // ------------------------------------------------------------ 全量 reconcile

    /// 用 Catalog 全量视图覆盖 Route Cache（正确性路径）。
    ///
    /// `LISTEN/NOTIFY` 只是加速提示，真正保证收敛的是本函数：Server 断线恢复、
    /// 漏掉通知、多个实例各自维护缓存，最终都靠它对齐。
    ///
    /// **失败时不清空缓存**：本函数要么整体替换（成功），要么原样返回错误（失败），
    /// 绝不执行「先清空再填充」—— 那会让 Control Plane 抖动直接变成数据面全量故障。
    ///
    /// # Errors
    /// Catalog 访问失败时返回错误（调用方只应记录日志）。
    pub async fn reconcile_all(&self) -> ApiResult<usize> {
        let started = Instant::now();
        let entries: Vec<RoutingEntry> = self.catalog.list_routing_entries().await.api()?;
        let workers = self.catalog.list_workers().await.api()?;
        let version = self.catalog.current_catalog_version().await.api()?;

        // 只发布 endpoint 可用的路由：endpoint 为空的路由写进缓存也连不上，
        // 反而会把「Worker 尚未上报地址」伪装成「路由命中」。
        let endpoints: HashMap<WorkerId, String> = workers
            .iter()
            .map(|worker| (worker.id.clone(), data_endpoint_of(worker)))
            .filter(|(_, endpoint)| !endpoint.trim().is_empty())
            .collect();

        let routes: Vec<RouteEntry> = entries
            .into_iter()
            .filter_map(|entry| {
                let endpoint = endpoints.get(&entry.owner_worker_id)?.clone();
                Some(RouteEntry::new(
                    entry.database_id,
                    entry.owner_worker_id,
                    endpoint,
                    entry.owner_epoch,
                    entry.state,
                ))
            })
            .collect();

        let published = routes.len();
        self.routes.apply_catalog_snapshot(routes, version);
        observability::metrics::record_route_refresh_micros(started.elapsed().as_micros() as u64);
        tracing::debug!(
            published,
            catalog_version = version,
            elapsed_ms = started.elapsed().as_millis(),
            "Route Cache 全量 reconcile 完成"
        );
        Ok(published)
    }

    // ------------------------------------------------------------ 冷启动透明 Wake

    /// 冷启动透明 Wake（架构 §8）。
    ///
    /// Coalesce 由 Catalog 的 `wakeup_in_progress` 原子位完成：
    /// - [`WakeupDecision::Leader`]：本请求负责执行 Placecment -> `bump_ownership` ->
    ///   `WorkerControl.StartDatabase`，结束后**无论成败**都要 `end_wakeup` 释放标记；
    /// - `Waiter` / `AlreadyRunning(STARTING)`：跳过启动，直接等同一个 READY 结果。
    async fn wake(
        &self,
        database_id: DatabaseId,
        deadline: Option<Instant>,
    ) -> ApiResult<RouteEntry> {
        let wake_deadline = clamp_deadline(deadline, self.config.wakeup_timeout);
        let decision = self.catalog.try_begin_wakeup(database_id).await.api()?;

        if decision.is_leader() {
            let started = Instant::now();
            let outcome = self.start_by_placement(database_id, wake_deadline).await;
            // 必须释放 coalesce 标记，否则这个 DB 会永久卡在「有人正在启动」。
            if let Err(err) = self.catalog.end_wakeup(database_id).await {
                tracing::warn!(
                    database_id = %database_id,
                    error = %err.message,
                    "释放 wakeup 标记失败（该 DB 可能需人工 end_wakeup）"
                );
            }
            match outcome {
                Ok(()) => {
                    observability::metrics::record_cold_start_micros(
                        "success",
                        started.elapsed().as_micros() as u64,
                    );
                }
                Err(err) => {
                    observability::metrics::record_cold_start_micros(
                        if err.code() == ErrorCode::WakeupTimeout {
                            "timeout"
                        } else {
                            "failed"
                        },
                        started.elapsed().as_micros() as u64,
                    );
                    return Err(err);
                }
            }
        } else {
            tracing::debug!(
                database_id = %database_id,
                decision = ?decision,
                "冷启动 coalesce：非 Leader，等待同一个 READY 结果"
            );
        }

        // Leader 成功之后同样要等 READY：StartDatabase 只是「进程起来了」，
        // 真正可服务仍以 Catalog 状态为准（可能由心跳回写）。
        self.await_ready(database_id, wake_deadline).await
    }

    /// 为冷启动 DB 选 Worker 并接管 Ownership（架构 §8 / §9 / §10）。
    async fn start_by_placement(
        &self,
        database_id: DatabaseId,
        deadline: Instant,
    ) -> ApiResult<()> {
        let record = self.catalog.get_database(database_id).await.api()?;
        if record.is_deleted() {
            return Err(ApiError::not_found(format!("数据库 {database_id} 已删除")));
        }
        // 启动期间状态可能已被其他路径推进（心跳 / 失败接管）：直接沿用现状。
        if let Some(owner) = record.owner_worker_id.clone() {
            if record.state.occupies_process() {
                tracing::debug!(
                    database_id = %database_id,
                    worker_id = %owner,
                    state = record.state.to_db_str(),
                    "冷启动发现已有 Owner，跳过 placement"
                );
                return Ok(());
            }
        }

        let worker_id = self.select_worker(&record).await?;
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;

        // §9：启动前再显式做一次准入校验（select 内部已含，但这里针对「目标 Worker」
        // 单独复查并留下可读的拒绝原因，便于排障）。
        let candidate = WorkerCandidate::from_record(&worker, false);
        let admission = self
            .scheduler
            .admission_check(&candidate, &record.resource_budget());
        if !admission.allows() {
            let reason = admission.reason().unwrap_or("资源不足").to_string();
            observability::metrics::record_admission_denied(admission.as_str());
            return Err(ApiError::new(
                ErrorCode::AdmissionDenied,
                format!("Worker {worker_id} 拒绝放置：{reason}"),
            ));
        }

        // 接管 Ownership：epoch 校验 + 单调递增在同一事务内完成（防 Split Brain）。
        let new_epoch = match self
            .catalog
            .bump_ownership(
                database_id,
                record.owner_epoch.get(),
                worker_id.clone(),
                OWNERSHIP_LEASE_TTL,
            )
            .await
        {
            Ok(epoch) => epoch,
            Err(err) if err.code == ErrorCode::EpochMismatch => {
                // 其他路径刚推进了 epoch（并发 Start / Failover）：让它去等 READY。
                tracing::info!(
                    database_id = %database_id,
                    error = %err.message,
                    "接管 ownership 时 epoch 已变化，改为等待现有启动流程"
                );
                return Ok(());
            }
            Err(err) => return Err(ApiError::from(err)),
        };

        // 调用 Worker 真正拉起进程；失败不回收 ownership —— 交由租约过期 GC 处理，
        // 这样「Worker 稍后自己起来了」仍然能自愈。
        //
        // 唯一的例外是**存储层 fencing 拒绝**：那说明控制面的 epoch 落后于 Remote WAL
        // 已记录值，重试同样的 epoch 永远不会成功。这种情况按 §11.3 向存储层对齐后
        // 重试一次（见 [`DbRouter::retry_start_after_epoch_realign`]）。
        self.start_on_worker_with_epoch_realign(
            &record,
            &worker_id,
            new_epoch,
            deadline_remaining(deadline),
        )
        .await
        .map(|_| ())
    }

    /// 在指定 Worker 上启动 DB；被存储层 fencing 拒绝时向 WAL 对齐 epoch 后重试**一次**。
    ///
    /// 所有启动路径（透明 Wake、故障接管、手工 Start）都应走这里，而不是直接调
    /// [`DbRouter::start_on_worker`]：只有这样「控制面向存储层权威对齐」（§11.3）
    /// 才是全局行为，而不是某一条路径的偶然补丁。
    ///
    /// # Errors
    /// Worker 拒绝或不可达时返回错误（对齐不适用时原样返回首次错误）。
    pub async fn start_on_worker_with_epoch_realign(
        &self,
        record: &DatabaseRecord,
        worker_id: &WorkerId,
        owner_epoch: u64,
        timeout: Duration,
    ) -> ApiResult<LifecycleState> {
        match self
            .start_on_worker(record, worker_id, owner_epoch, true, timeout)
            .await
        {
            Ok(state) => Ok(state),
            Err(err) => {
                self.retry_start_after_epoch_realign(record, worker_id, owner_epoch, err, timeout)
                    .await
            }
        }
    }

    /// 启动被存储层 fencing 拒绝时：读 WAL 已记录 epoch -> 对齐 Catalog -> 重试**一次**。
    ///
    /// 为什么必须由存储层定调：Storage-level Fencing 的权威是 Remote WAL 自己记住的
    /// epoch（架构 §11.3）。Catalog 的 `owner_epoch` 一旦落后于它（手工干预、从备份恢复、
    /// 开发期重置数据），`SetOwnerEpoch` 会稳定返回 `WAL_APPEND_REJECTED`
    /// （"owner epoch 必须严格递增：已记录 N，请求 M"），而**重试同样落后的 epoch 永远
    /// 不会成功** —— DB 从此起不来，只能人工清库。
    ///
    /// 判定用**证据**而不是错误文案：先向 WAL 读回已记录的 epoch，只有确认
    /// `wal_epoch >= 本次尝试的 epoch`（这正是 `SetOwnerEpoch` 被拒的充要条件）才动手；
    /// 否则原样返回调用方的错误，绝不把「S3 挂了」之类的问题伪装成 epoch 问题。
    ///
    /// 有界性：整条路径最多读一次 WAL、对齐一次、重试一次；任何一步失败都立刻返回
    /// 原始错误（不放宽成无限重试）。
    async fn retry_start_after_epoch_realign(
        &self,
        record: &DatabaseRecord,
        worker_id: &WorkerId,
        attempted_epoch: u64,
        original: ApiError,
        timeout: Duration,
    ) -> ApiResult<LifecycleState> {
        if !is_fencing_rejection(&original) {
            return Err(original);
        }
        let Some(wal) = self.wal.as_ref() else {
            tracing::warn!(
                database_id = %record.id,
                code = original.error.code.as_str(),
                "启动被拒且未配置 Remote WAL 端点，无法向存储层对齐 epoch（返回原始错误）"
            );
            return Err(original);
        };

        let status = match wal.status(&record.id).await {
            Ok(status) => status,
            Err(err) => {
                tracing::warn!(
                    database_id = %record.id,
                    error = %err,
                    "读取 Remote WAL 状态失败，无法判断是否需要对齐 epoch（返回原始错误）"
                );
                return Err(original);
            }
        };
        if status.owner_epoch < attempted_epoch {
            // WAL 记录的 epoch 低于本次尝试 -> 本次拒绝与 epoch 落后无关，不做解释性修补。
            tracing::warn!(
                database_id = %record.id,
                attempted_epoch,
                wal_epoch = status.owner_epoch,
                code = original.error.code.as_str(),
                "启动被拒，但 Remote WAL 记录的 epoch 并不高于本次尝试（非 epoch 落后问题）"
            );
            return Err(original);
        }

        tracing::warn!(
            database_id = %record.id,
            worker_id = %worker_id,
            attempted_epoch,
            wal_epoch = status.owner_epoch,
            "启动被 Remote WAL fencing 拒绝：Catalog 落后于存储层，按 §11.3 向 WAL 对齐 epoch 后重试一次"
        );

        // 对齐只抬 epoch（并写审计）；不改变 owner / state / 租约。
        let aligned = self
            .catalog
            .align_ownership_epoch_with_storage(
                record.id,
                status.owner_epoch,
                catalog::REASON_EPOCH_REALIGN_FROM_STORAGE,
            )
            .await
            .map_err(ApiError::from)?;

        // 对齐后重新接管：新 epoch = aligned + 1，严格大于 WAL 已记录值，必然被接受。
        let retry_epoch = match self
            .catalog
            .bump_ownership(record.id, aligned, worker_id.clone(), OWNERSHIP_LEASE_TTL)
            .await
        {
            Ok(epoch) => epoch,
            Err(err) => {
                tracing::warn!(
                    database_id = %record.id,
                    worker_id = %worker_id,
                    aligned_epoch = aligned,
                    error = %err.message,
                    "对齐 epoch 后重新接管失败（可能有并发的 Start / Failover）"
                );
                return Err(original);
            }
        };

        self.start_on_worker(record, worker_id, retry_epoch, true, timeout)
            .await
    }

    /// 通过 Scheduler 选 Worker。
    async fn select_worker(&self, record: &DatabaseRecord) -> ApiResult<WorkerId> {
        let workers = self.catalog.list_workers().await.api()?;
        let candidates: Vec<WorkerCandidate> = workers
            .iter()
            .map(|worker| WorkerCandidate::from_record(worker, false))
            .collect();

        let mut request = PlacementRequest::new(record.id, record.resource_budget());
        request.priority = record.priority;
        request.affinity_worker = record.affinity_worker_id.clone();
        request.anti_affinity_worker = record.anti_affinity_worker_id.clone();

        self.scheduler
            .select(&request, &candidates)
            .map(|decision| decision.worker_id)
            .map_err(|err| ApiError::from(err.to_platform_error()))
    }

    /// 等待 DB READY（轮询 Catalog 状态）。
    ///
    /// 为什么是轮询而不是订阅：READY 可能来自 Worker 心跳（`record_heartbeat` 回写状态），
    /// 也可能来自本进程刚刚的 StartDatabase 响应；轮询 Catalog 是唯一同时覆盖两者的读法。
    /// 间隔默认 20ms，配合 `WAKEUP_TIMEOUT_MS` 的等待窗口，开销可忽略。
    async fn await_ready(
        &self,
        database_id: DatabaseId,
        deadline: Instant,
    ) -> ApiResult<RouteEntry> {
        loop {
            let record = self.catalog.get_database(database_id).await.api()?;
            if record.is_deleted() {
                return Err(ApiError::not_found(format!("数据库 {database_id} 已删除")));
            }
            match record.state {
                LifecycleState::Warm | LifecycleState::Hot => {
                    if let Some(owner) = record.owner_worker_id.clone() {
                        return self.publish_route(&record, &owner).await;
                    }
                }
                LifecycleState::Failed => {
                    return Err(ApiError::new(
                        ErrorCode::DatabaseNotReady,
                        format!("数据库 {database_id} 启动失败（FAILED），需要人工介入"),
                    ));
                }
                LifecycleState::Draining | LifecycleState::Stopping => {
                    return Err(ApiError::new(
                        ErrorCode::DatabaseNotReady,
                        format!(
                            "数据库 {database_id} 正在 {}，暂不可服务",
                            record.state.to_db_str()
                        ),
                    ));
                }
                LifecycleState::Cold | LifecycleState::Starting => {}
            }

            if Instant::now() >= deadline {
                // 明确报超时，绝不把内部 WAKING 状态返回给客户端（架构 §8）。
                return Err(ApiError::new(
                    ErrorCode::WakeupTimeout,
                    format!(
                        "数据库 {database_id} 在 {}ms 内未完成冷启动",
                        self.config.wakeup_timeout.as_millis()
                    ),
                )
                .with_detail(serde_json::json!({
                    "database_id": database_id.to_string(),
                    "state": record.state.to_db_str(),
                    "wakeup_timeout_ms": self.config.wakeup_timeout.as_millis(),
                })));
            }
            tokio::time::sleep(self.config.wakeup_poll_interval).await;
        }
    }

    // ------------------------------------------------------------ 控制面指令

    /// 在指定 Worker 上启动 DB（`WorkerControl.StartDatabase`）。
    ///
    /// 成功后按 Worker 回报的状态回写 Catalog —— Worker 的响应就是「本地进程已经起来」
    /// 的权威证据，比等下一次心跳更快，也让冷启动路径少一次往返。
    ///
    /// # Errors
    /// Worker 拒绝或不可达时返回错误；`ownership` 不回滚（由租约 GC 兜底）。
    pub async fn start_on_worker(
        &self,
        record: &DatabaseRecord,
        worker_id: &WorkerId,
        owner_epoch: u64,
        restore_from_snapshot: bool,
        timeout: Duration,
    ) -> ApiResult<LifecycleState> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = control_endpoint_of(&worker);
        let mut client = self.channels.control(&endpoint).await?;

        let context = control_context(
            &record.tenant_id.to_string(),
            &record.id.to_string(),
            worker_id,
            owner_epoch,
            timeout,
        );

        let request = StartDatabaseRequest {
            database_id: record.id.to_string(),
            worker_id: worker_id.to_string(),
            owner_epoch,
            budget: Some(record.resource_budget().into()),
            restore_from_snapshot,
            deadline_unix_ms: deadline_unix_ms_from_now(timeout),
            prepare_only: false,
            context: Some(context.into()),
        };

        let response = tokio::time::timeout(timeout, client.start_database(request))
            .await
            .map_err(|_| {
                ApiError::new(
                    ErrorCode::WakeupTimeout,
                    format!("Worker {worker_id} 启动数据库超时"),
                )
            })?
            .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        let response = response.into_inner();
        check_proto_error(response.error.as_ref(), worker_id.as_ref())?;

        let reported = LifecycleState::from_proto_i32(response.state);
        observability::metrics::record_start_db("success");

        // 只有 Worker 明确回报「可服务」时才立刻回写；否则等心跳 / 轮询。
        let state = if reported.is_serving() {
            self.catalog
                .set_lifecycle_state(record.id, reported, Some(worker_id.clone()))
                .await
                .api()?;
            reported
        } else {
            record.state
        };
        tracing::info!(
            database_id = %record.id,
            worker_id = %worker_id,
            owner_epoch,
            state = state.to_db_str(),
            pid = response.pid,
            "Worker 已启动数据库"
        );
        Ok(state)
    }

    /// 停止 DB（`WorkerControl.StopDatabase`），成功后把 Catalog 状态置为 COLD 并释放 Owner。
    ///
    /// # Errors
    /// Worker 不可达时返回错误；调用方可以选择「仍然置 COLD」（强制回收）或保留现状。
    pub async fn stop_on_worker(
        &self,
        database_id: DatabaseId,
        worker_id: &WorkerId,
        owner_epoch: u64,
        graceful: bool,
        timeout: Duration,
    ) -> ApiResult<()> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = control_endpoint_of(&worker);
        let mut client = self.channels.control(&endpoint).await?;

        let context = control_context(
            "",
            &database_id.to_string(),
            worker_id,
            owner_epoch,
            timeout,
        );
        let request = StopDatabaseRequest {
            database_id: database_id.to_string(),
            owner_epoch,
            graceful,
            deadline_unix_ms: deadline_unix_ms_from_now(timeout),
            context: Some(context.into()),
        };

        let response = tokio::time::timeout(timeout, client.stop_database(request))
            .await
            .map_err(|_| {
                ApiError::new(
                    ErrorCode::DeadlineExceeded,
                    format!("Worker {worker_id} 停止数据库超时"),
                )
            })?
            .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        check_proto_error(response.into_inner().error.as_ref(), worker_id.as_ref())?;
        Ok(())
    }

    /// 重启 DB（`WorkerControl.RestartDatabase`）。
    ///
    /// # Errors
    /// Worker 拒绝或不可达时返回错误。
    pub async fn restart_on_worker(
        &self,
        database_id: DatabaseId,
        worker_id: &WorkerId,
        owner_epoch: u64,
        timeout: Duration,
    ) -> ApiResult<LifecycleState> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = control_endpoint_of(&worker);
        let mut client = self.channels.control(&endpoint).await?;

        let context = control_context(
            "",
            &database_id.to_string(),
            worker_id,
            owner_epoch,
            timeout,
        );
        let request = RestartDatabaseRequest {
            database_id: database_id.to_string(),
            owner_epoch,
            deadline_unix_ms: deadline_unix_ms_from_now(timeout),
            context: Some(context.into()),
        };

        let response = tokio::time::timeout(timeout, client.restart_database(request))
            .await
            .map_err(|_| {
                ApiError::new(
                    ErrorCode::DeadlineExceeded,
                    format!("Worker {worker_id} 重启数据库超时"),
                )
            })?
            .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        let response = response.into_inner();
        check_proto_error(response.error.as_ref(), worker_id.as_ref())?;
        Ok(LifecycleState::from_proto_i32(response.state))
    }

    /// 强制终止 DB 进程（`WorkerControl.KillDatabase`），故障回收 / 驱逐时使用。
    ///
    /// # Errors
    /// Worker 不可达时返回错误。
    pub async fn kill_on_worker(
        &self,
        database_id: DatabaseId,
        worker_id: &WorkerId,
        owner_epoch: u64,
        reason: &str,
    ) -> ApiResult<()> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = control_endpoint_of(&worker);
        let mut client = self.channels.control(&endpoint).await?;

        let context = control_context(
            "",
            &database_id.to_string(),
            worker_id,
            owner_epoch,
            WORKER_CONTROL_TIMEOUT,
        );
        let request = KillDatabaseRequest {
            database_id: database_id.to_string(),
            owner_epoch,
            reason: reason.to_string(),
            context: Some(context.into()),
        };
        let response = tokio::time::timeout(WORKER_CONTROL_TIMEOUT, client.kill_database(request))
            .await
            .map_err(|_| {
                ApiError::new(
                    ErrorCode::DeadlineExceeded,
                    format!("Worker {worker_id} 终止数据库超时"),
                )
            })?
            .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        check_proto_error(response.into_inner().error.as_ref(), worker_id.as_ref())?;
        Ok(())
    }

    /// 触发一次本地快照（`WorkerControl.TriggerSnapshot`），返回 Worker 上报的快照元数据。
    ///
    /// # Errors
    /// Worker 拒绝（例如不是 Owner）或不可达时返回错误。
    pub async fn trigger_snapshot(
        &self,
        database_id: DatabaseId,
        worker_id: &WorkerId,
        owner_epoch: u64,
        timeout: Duration,
    ) -> ApiResult<protocol::common::SnapshotMeta> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = control_endpoint_of(&worker);
        let mut client = self.channels.control(&endpoint).await?;

        let context = control_context(
            "",
            &database_id.to_string(),
            worker_id,
            owner_epoch,
            timeout,
        );
        let request = TriggerSnapshotRequest {
            database_id: database_id.to_string(),
            owner_epoch,
            context: Some(context.into()),
        };
        let response = tokio::time::timeout(timeout, client.trigger_snapshot(request))
            .await
            .map_err(|_| {
                ApiError::new(
                    ErrorCode::DeadlineExceeded,
                    format!("Worker {worker_id} 触发快照超时"),
                )
            })?
            .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        let response = response.into_inner();
        check_proto_error(response.error.as_ref(), worker_id.as_ref())?;
        response.snapshot.ok_or_else(|| {
            ApiError::new(
                ErrorCode::SnapshotUnavailable,
                format!("Worker {worker_id} 未返回快照元数据"),
            )
        })
    }

    /// 排空 Worker（`WorkerControl.DrainWorker`），返回 (已迁移, 剩余)。
    ///
    /// # Errors
    /// Worker 不可达时返回错误。
    pub async fn drain_worker(
        &self,
        worker_id: &WorkerId,
        stop_cold_and_warm: bool,
        timeout: Duration,
    ) -> ApiResult<(u32, u32)> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = control_endpoint_of(&worker);
        let mut client = self.channels.control(&endpoint).await?;

        // Drain 作用于 Worker 而非单个 DB：database_id 留空（proto3 空串语义 = 无）。
        let context = control_context("", "", worker_id, 0, timeout);
        let request = DrainWorkerRequest {
            worker_id: worker_id.to_string(),
            stop_cold_and_warm,
            deadline_unix_ms: deadline_unix_ms_from_now(timeout),
            context: Some(context.into()),
        };
        let response = tokio::time::timeout(timeout, client.drain_worker(request))
            .await
            .map_err(|_| {
                ApiError::new(
                    ErrorCode::DeadlineExceeded,
                    format!("Worker {worker_id} 排空超时"),
                )
            })?
            .map_err(|status| status_to_api_error(status, worker_id.as_ref()))?;
        let response = response.into_inner();
        check_proto_error(response.error.as_ref(), worker_id.as_ref())?;
        Ok((response.migrated, response.remaining))
    }

    // ------------------------------------------------------------ 数据面

    /// 执行一次数据面 RPC，带 **stale route 透明重试（最多 1 次）**。
    ///
    /// `call` 会拿到当前路由，返回 [`DataRpcOutcome`]。Worker 报 `NOT_OWNER` /
    /// `EPOCH_MISMATCH` / `ROUTE_STALE` 时刷新路由并重试一次；第二次仍失败
    /// 就把 `route_retry_count = 1` 写进错误体（架构 §16「路由失效」场景）。
    ///
    /// # Errors
    /// 路由解析失败、或两次调用都失败时返回错误。
    pub async fn call_data<T, F, Fut>(
        &self,
        database_id: DatabaseId,
        deadline: Option<Instant>,
        mut call: F,
    ) -> ApiResult<T>
    where
        F: FnMut(RouteEntry) -> Fut,
        Fut: Future<Output = DataRpcOutcome<T>>,
    {
        let mut retry = 0u32;
        let mut route = self.resolve_target(database_id, deadline).await?;
        loop {
            match call(route.clone()).await {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(error)) if retry == 0 && is_stale_route_error(&error) => {
                    tracing::info!(
                        database_id = %database_id,
                        worker_id = %route.worker_id,
                        code = error.code.as_str(),
                        "路由过期（Worker 结构化错误），刷新 Route 后重试一次"
                    );
                    retry += 1;
                    route = self.retry_route(database_id, deadline).await?;
                }
                Ok(Err(error)) => {
                    return Err(ApiError::from(error).with_route_retry(retry));
                }
                Err(status) if retry == 0 && is_stale_route_status(&status) => {
                    tracing::info!(
                        database_id = %database_id,
                        worker_id = %route.worker_id,
                        code = ?status.code(),
                        "路由过期（gRPC 状态），刷新 Route 后重试一次"
                    );
                    retry += 1;
                    route = self.retry_route(database_id, deadline).await?;
                }
                Err(status) => {
                    return Err(status_to_api_error(status, route.worker_id.as_ref())
                        .with_route_retry(retry));
                }
            }
        }
    }

    /// 描述语句时复用会话所属 Worker；不允许把失效会话降级为新连接。
    pub async fn describe(
        &self,
        database_id: DatabaseId,
        session: Option<&crate::state::SessionBinding>,
        sql: &str,
        request_id: &str,
    ) -> ApiResult<protocol::data::DescribeResponse> {
        let route = self.resolve_target(database_id, None).await?;
        if let Some(binding) = session {
            if binding.worker_id != route.worker_id || binding.owner_epoch != route.owner_epoch {
                return Err(ApiError::new(ErrorCode::SessionLost, "describe 会话所属路由已失效"));
            }
        }
        let mut client = self.channels.data(&route.worker_endpoint).await?;
        let context = data_request_context(
            request_id, database_id, &route.worker_id, route.owner_epoch,
            session.map(|binding| binding.session_id.clone()), 0,
        );
        let mut guard = CancelGuard::new(client.clone(), context.clone());
        let response = client.describe(crate::clients::describe_request(context, sql)).await
            .map_err(|status| status_to_api_error(status, route.worker_id.as_ref()))?
            .into_inner();
        guard.disarm();
        check_proto_error(response.error.as_ref(), route.worker_id.as_ref())?;
        Ok(response)
    }

    /// 打开一条数据面结果流（`ExecuteStream` / `SessionExecuteStream`）。
    ///
    /// 返回的 [`CancelGuard`] 必须随流一起交给响应体：流被 drop（客户端断开）时
    /// guard 会把 `Cancel` 传播到 Worker。
    ///
    /// # Errors
    /// 路由解析失败、Worker 不可达、或**首帧**就是错误时返回错误
    /// （此时还没写出任何字节，可以给出正确的 HTTP 状态码）。
    pub async fn open_stream(
        &self,
        database_id: DatabaseId,
        target: StreamTarget,
        request_id: &str,
        deadline: Option<Instant>,
    ) -> ApiResult<(RouteEntry, FrameStream, CancelGuard)> {
        // 会话内执行不做透明重试：会话 pin 在特定 Worker 上，重试到别的 Worker
        // 只会得到一个必然失败的会话（架构 §13.3 明确 failover 后会话不可恢复）。
        let max_retry = match target {
            StreamTarget::Stateless { .. } => 1u32,
            StreamTarget::Session { .. } => 0u32,
        };

        let mut retry = 0u32;
        let mut route = self.resolve_target(database_id, deadline).await?;
        loop {
            let mut client = self.channels.data(&route.worker_endpoint).await?;
            let context = data_request_context(
                request_id,
                database_id,
                &route.worker_id,
                route.owner_epoch,
                match &target {
                    StreamTarget::Session { session_id, .. } => Some(session_id.clone()),
                    StreamTarget::Stateless { .. } => None,
                },
                deadline_unix_ms_from_option(deadline),
            );
            let mut guard = CancelGuard::new(client.clone(), context.clone());

            let response = match &target {
                StreamTarget::Stateless { sql, params } => {
                    let request = execute_request(
                        context,
                        sql,
                        params.clone(),
                        self.config.inline_result_limit_bytes as u32,
                    );
                    client.execute_stream(request).await
                }
                StreamTarget::Session {
                    session_id,
                    sql,
                    params,
                } => {
                    let request = session_execute_request(context, session_id, sql, params.clone());
                    client.session_execute_stream(request).await
                }
            };

            let mut stream = match response {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    if retry < max_retry && is_stale_route_status(&status) {
                        retry += 1;
                        route = self.retry_route(database_id, deadline).await?;
                        continue;
                    }
                    return Err(status_to_api_error(status, route.worker_id.as_ref())
                        .with_route_retry(retry));
                }
            };

            // 窥探首帧：Worker 的 fencing 失败通常以「第一帧带 error」的形式出现。
            // 此时尚未向客户端写出任何字节，仍可返回正确的 HTTP 错误。
            match stream.message().await {
                Ok(Some(first)) => {
                    if let Some(error) = first.error.as_ref().map(platform_error_from_proto) {
                        if retry < max_retry && is_stale_route_error(&error) {
                            retry += 1;
                            route = self.retry_route(database_id, deadline).await?;
                            continue;
                        }
                        return Err(ApiError::from(error).with_route_retry(retry));
                    }
                    let rest: FrameStream = Box::pin(stream);
                    // 显式标注 Item 类型：`stream::once` 的错误类型无法从上下文推断
                    let first_item: Result<StreamFrame, tonic::Status> = Ok(first);
                    let frames: FrameStream =
                        Box::pin(futures::stream::once(async move { first_item }).chain(rest));
                    return Ok((route, frames, guard));
                }
                Ok(None) => {
                    // Worker 直接关流且没有任何帧：当成空结果，交给上层决定语义。
                    guard.disarm();
                    return Ok((route, Box::pin(futures::stream::empty()), guard));
                }
                Err(status) => {
                    if retry < max_retry && is_stale_route_status(&status) {
                        retry += 1;
                        route = self.retry_route(database_id, deadline).await?;
                        continue;
                    }
                    return Err(status_to_api_error(status, route.worker_id.as_ref())
                        .with_route_retry(retry));
                }
            }
        }
    }

    // ------------------------------------------------------------ endpoint 解析

    /// 某 Worker 的 Data Path endpoint。
    async fn data_endpoint_of(&self, worker_id: &WorkerId) -> ApiResult<String> {
        let worker = self.catalog.get_worker(worker_id.clone()).await.api()?;
        let endpoint = data_endpoint_of(&worker);
        if endpoint.trim().is_empty() {
            return Err(ApiError::new(
                ErrorCode::WorkerUnavailable,
                format!("Worker {worker_id} 未上报 Data Path endpoint"),
            ));
        }
        Ok(endpoint)
    }
}

/// Worker 记录里的 Data Path endpoint（缺失时回落通用 endpoint）。
///
/// `RouteCache` 的条目只带一个 endpoint，热路径（数据面）用它，因此这里统一取
/// Data Path；Control Path 的地址通过 [`control_endpoint_of`] 单独解析，
/// 只在控制面路径（非热路径）使用。
#[must_use]
pub fn data_endpoint_of(worker: &WorkerRecord) -> String {
    worker
        .data_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| worker.endpoint.trim())
        .to_string()
}

/// Worker 记录里的 Control Path endpoint（缺失时回落通用 endpoint）。
#[must_use]
pub fn control_endpoint_of(worker: &WorkerRecord) -> String {
    worker
        .control_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| worker.endpoint.trim())
        .to_string()
}

/// 取 `deadline` 与「now + cap」中更早的一个。
#[must_use]
/// 构造控制面 RPC 的 [`protocol::convert::RequestContext`]。
///
/// deadline 用进程内单调时钟承载（`Instant`），转 wire 时才由 `convert` 变成绝对
/// Unix 毫秒 —— 墙钟跳变不会让一次 StartDatabase 提前/延后超时。
fn control_context(
    tenant_id: &str,
    database_id: &str,
    worker_id: &WorkerId,
    owner_epoch: u64,
    timeout: Duration,
) -> protocol::convert::RequestContext {
    protocol::convert::RequestContext {
        request_id: crate::middleware::current_request_id(),
        trace_id: String::new(),
        tenant_id: tenant_id.to_string(),
        database_id: database_id.to_string(),
        owner_epoch,
        deadline: Some(Instant::now() + timeout),
        session_id: String::new(),
        transaction_id: String::new(),
        worker_id: worker_id.to_string(),
        idempotency_key: String::new(),
    }
}

fn clamp_deadline(deadline: Option<Instant>, cap: Duration) -> Instant {
    let capped = Instant::now() + cap;
    match deadline {
        Some(value) => value.min(capped),
        None => capped,
    }
}

/// deadline 距现在还剩多久（不为负）。
#[must_use]
fn deadline_remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// 把「剩余时长」转成 Unix 毫秒绝对时间；0 表示无 deadline。
#[must_use]
fn deadline_unix_ms_from_now(remaining: Duration) -> u64 {
    let now = now_unix_ms().max(0) as u64;
    now.saturating_add(remaining.as_millis().min(u64::MAX as u128) as u64)
}

/// 把可选 deadline 转成 Unix 毫秒绝对时间。
#[must_use]
fn deadline_unix_ms_from_option(deadline: Option<Instant>) -> u64 {
    match deadline {
        Some(instant) => deadline_unix_ms_from_now(deadline_remaining(instant)),
        // 0 = 无 deadline（proto 约定）
        None => 0,
    }
}

/// 控制面 RPC 兜底超时（供调用方使用）。
#[must_use]
pub fn control_fallback_timeout() -> Duration {
    Duration::from_millis(CONTROL_RPC_FALLBACK_TIMEOUT_MS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::lifecycle::WorkerState;

    fn worker_record(data: Option<&str>, control: Option<&str>, generic: &str) -> WorkerRecord {
        WorkerRecord {
            id: WorkerId::new("w1"),
            endpoint: generic.to_string(),
            control_endpoint: control.map(ToString::to_string),
            data_endpoint: data.map(ToString::to_string),
            state: WorkerState::Active,
            region: "default".into(),
            zone: "default".into(),
            version: "0.1.0".into(),
            cpu_milli_total: 1000,
            memory_mib_total: 1024,
            fd_total: 1024,
            disk_mib_total: 1024,
            process_slots_total: 16,
            iops_total: 1000,
            cpu_milli_used: 0,
            memory_mib_used: 0,
            fd_used: 0,
            disk_mib_used: 0,
            process_slots_used: 0,
            iops_used: 0,
            last_heartbeat_at: None,
            missed_heartbeats: 0,
            inventory_version: 0,
            reserved_for_failover: false,
            labels: serde_json::json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn data_endpoint_prefers_data_path_and_falls_back() {
        assert_eq!(
            data_endpoint_of(&worker_record(Some("w:9101"), Some("w:9100"), "w:9000")),
            "w:9101"
        );
        // data_endpoint 为空白 -> 回落通用 endpoint
        assert_eq!(
            data_endpoint_of(&worker_record(Some("  "), Some("w:9100"), "w:9000")),
            "w:9000"
        );
        assert_eq!(
            data_endpoint_of(&worker_record(None, None, "w:9000")),
            "w:9000"
        );
    }

    #[test]
    fn control_endpoint_prefers_control_path_and_falls_back() {
        assert_eq!(
            control_endpoint_of(&worker_record(Some("w:9101"), Some("w:9100"), "w:9000")),
            "w:9100"
        );
        assert_eq!(
            control_endpoint_of(&worker_record(Some("w:9101"), None, "w:9000")),
            "w:9000"
        );
    }

    #[test]
    fn clamp_deadline_never_exceeds_wakeup_cap() {
        let cap = Duration::from_millis(50);
        // 无 deadline -> now + cap
        let clamped = clamp_deadline(None, cap);
        assert!(clamped <= Instant::now() + cap);

        // 请求 deadline 更近 -> 用它；更远 -> 用 cap
        let far = Instant::now() + Duration::from_secs(60);
        let clamped_far = clamp_deadline(Some(far), cap);
        assert!(clamped_far <= Instant::now() + cap);

        let near = Instant::now() + Duration::from_millis(1);
        assert!(clamp_deadline(Some(near), cap) <= near);
    }

    #[test]
    fn deadline_conversion_zero_means_no_deadline() {
        assert_eq!(deadline_unix_ms_from_option(None), 0);
        let soon = Instant::now() + Duration::from_secs(5);
        let value = deadline_unix_ms_from_option(Some(soon));
        assert!(value > now_unix_ms().max(0) as u64);
    }

    #[test]
    fn expired_deadline_reports_zero_remaining() {
        let past = Instant::now() - Duration::from_secs(1);
        assert_eq!(deadline_remaining(past), Duration::ZERO);
    }
}
