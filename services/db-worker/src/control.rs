//! Worker Agent 控制面：`protocol::platform::control::v1::WorkerControl` 的服务端实现。
//!
//! 边界（架构 §5.1 / §12.3 / §17.6）：
//!
//! - Worker 与 Server 的**常规方向是反的** —— 心跳由 Worker 作为 client 主动上报
//!   （见 [`crate::heartbeat`]），本模块只实现 Server -> Worker 的指令面。
//!   [`WorkerControl::heartbeat`] 仍然实现（proto 要求服务端方法齐全），语义是
//!   「被 Server 主动探测时回一份本节点当前视图」，不承担上报职责。
//! - 控制面指令**不得成为数据面瓶颈**：所有方法要么是廉价本地查询，要么是一次进程
//!   操作；重活（快照生成与上传、工作集恢复）都带 deadline 且有界。
//!
//! Epoch 语义（架构 §10 / §11.3）在本模块的落点：
//!
//! - `StartDatabase` / `FinalizeMove` 是**建立或接管所有权**，允许请求 epoch 大于
//!   本地记录（新 Owner 接管），只拒绝回退（`EPOCH_MISMATCH`）；
//! - 其余指令（Stop / Restart / Kill / TriggerSnapshot）同样拒绝低于本地的 epoch ——
//!   一条迟到的旧 Owner 指令不得改变新 Owner 的进程状态。

use std::sync::Arc;
use std::time::{Duration, Instant};

use domain::lifecycle::LifecycleState;
use domain::resources::{ResourceBudget, WorkerCapacity, WorkerResourceUsage};
use domain::time::now_unix_ms;
use objectstore::snapshot::{self as snapshot_io, SnapshotManifest};
use protocol::common;
use protocol::control::{
    worker_control_server::WorkerControl, DatabaseCommandResponse, DrainWorkerRequest,
    DrainWorkerResponse, FinalizeMoveRequest, GetWorkerStatusRequest, GetWorkerStatusResponse,
    KillDatabaseRequest, LocalDatabaseState, PrepareMoveRequest, PrepareMoveResponse,
    RestartDatabaseRequest, StartDatabaseRequest, StopDatabaseRequest, TriggerSnapshotRequest,
    TriggerSnapshotResponse, WorkerInfo,
};

use protocol::convert;
use protocol::runtime_local as rt;
use tonic::{Request, Response, Status};

use crate::cli::WorkerConfig;
use crate::error::{Result, WorkerError};
use crate::registry::{LocalDatabase, LocalDbRegistry, RestoredFrom};
use crate::resources::ResourceSampler;
use crate::restore::{LedgerEntry, PrepareWork, PreparedWorkSet, SnapshotSource};
use crate::supervisor::{ProcessSupervisor, StartOutcome, StartSpec};
use crate::uds::DbConnectionPool;
use crate::worker_state::WorkerStateMachine;

/// 快照 schema 版本（当前冻结为 1，与 `migrations/0001_init.sql` 的 snapshots 表一致）。
const SNAPSHOT_SCHEMA_VERSION: i32 = 1;

/// TriggerSnapshot 未声明 deadline 时的上限。
///
/// 快照是重操作（engine checkpoint + 逐文件压缩上传），可能远超普通 RPC；但**必须有
/// 上限**，否则一次卡住的快照会永久占用控制面任务。
const FALLBACK_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(60);

/// Stop / Kill / Drain 未声明 deadline 时的兜底上限。
const FALLBACK_CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

/// 准入判定（架构 §9 `CanStart`）的纯函数形态。
///
/// 抽成自由函数是为了可单测：真实调用点只负责从注册表与配置汇总输入。
/// 返回 `Err(reason)` 时 `reason` 是稳定的机器标签（用于 `admission_denied_total` 指标）。
///
/// 两类判定必须分开：
/// - **硬容量**（`used + need <= capacity`）：任何启动都要过；
/// - **水位闸门**（80% 起停止新 Placement）：**只对新 Placement 生效**。既有 DB 的
///   重启 / Move 收尾已经计入 `used`，再卡水位会把「恢复既有 DB」也挡掉，反而放大故障面。
pub fn evaluate_admission(
    capacity: WorkerCapacity,
    process_limit: u64,
    serving_count: u64,
    used: &ResourceBudget,
    budget: &ResourceBudget,
    new_placement: bool,
) -> std::result::Result<(), &'static str> {
    // DB 预算必须至少占 1 个进程位，否则会绕过进程数 hard limit
    if budget.process_slots == 0 {
        return Err("zero_process_slots");
    }
    if new_placement && serving_count >= process_limit {
        return Err("process_limit");
    }
    let usage = WorkerResourceUsage::new(capacity, *used);
    if !usage.fits(budget) {
        return Err("capacity");
    }
    if new_placement {
        let projected = WorkerResourceUsage::new(capacity, used.saturating_add(budget));
        let gate = domain::policy::placement_gate(projected.max_utilization());
        if !gate.allows_new_placement() {
            // 水位标签直接取自 domain policy，避免自造第二套阈值
            return Err(match gate {
                domain::policy::PlacementGate::Emergency => "watermark_emergency",
                _ => "watermark_stop_new_placement",
            });
        }
    }
    Ok(())
}

/// Worker 控制面服务。
pub struct WorkerControlService {
    cfg: Arc<WorkerConfig>,
    state: Arc<WorkerStateMachine>,
    registry: Arc<LocalDbRegistry>,
    supervisor: Arc<ProcessSupervisor>,
    pool: Arc<DbConnectionPool>,
    sampler: Arc<ResourceSampler>,
    /// 快照上传目标；未配置对象存储时 `TriggerSnapshot` 返回 `STORAGE_UNAVAILABLE`。
    store: Option<Arc<dyn objectstore::ObjectStore>>,
    /// Worker 进程启动时刻（Heartbeat / GetWorkerStatus 上报）。
    started_at_unix_ms: i64,
}

impl std::fmt::Debug for WorkerControlService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerControlService")
            .field("worker_id", &self.cfg.worker_id)
            .field("state", &self.state.current())
            .field("object_store", &self.store.is_some())
            .finish()
    }
}

impl WorkerControlService {
    /// 构造。
    #[allow(clippy::too_many_arguments)] // 依赖均为进程级单例，封装成结构体反而增加一层无意义包装
    pub fn new(
        cfg: Arc<WorkerConfig>,
        state: Arc<WorkerStateMachine>,
        registry: Arc<LocalDbRegistry>,
        supervisor: Arc<ProcessSupervisor>,
        pool: Arc<DbConnectionPool>,
        sampler: Arc<ResourceSampler>,
        store: Option<Arc<dyn objectstore::ObjectStore>>,
        started_at_unix_ms: i64,
    ) -> Self {
        Self {
            cfg,
            state,
            registry,
            supervisor,
            pool,
            sampler,
            store,
            started_at_unix_ms,
        }
    }

    // ------------------------------------------------------------ 公共视图

    /// 本节点的 WorkerInfo（心跳与状态查询共用）。
    pub fn worker_info(&self) -> WorkerInfo {
        WorkerInfo {
            worker_id: self.cfg.worker_id.clone(),
            endpoint: self.cfg.advertise_endpoint.clone(),
            state: convert::worker_state_to_proto(self.state.current()) as i32,
            region: self.cfg.region.clone(),
            zone: self.cfg.zone.clone(),
            version: self.cfg.version.clone(),
            started_at_unix_ms: self.started_at_unix_ms.max(0) as u64,
            data_endpoint: self.advertised_data_endpoint(),
        }
    }

    /// 对外通告的数据面端点。
    ///
    /// 取值顺序：
    /// 1. `WORKER_ADVERTISE_DATA_ENDPOINT`（显式配置，容器编排用）；
    /// 2. 由控制面通告地址的 host + `data_listen` 的端口推导（同主机不同端口的常见情形）。
    fn advertised_data_endpoint(&self) -> String {
        let explicit = self.cfg.advertise_data_endpoint.trim();
        if !explicit.is_empty() {
            return explicit.to_owned();
        }
        let control = self.cfg.advertise_endpoint.trim();
        let port = self.cfg.data_listen.port();
        match control.rsplit_once(':') {
            Some((host, _)) if !host.is_empty() => format!("{host}:{port}"),
            // 没有可用的 host：退回监听地址（至少在单机/端口转发场景下可用）
            _ => self.cfg.data_listen.to_string(),
        }
    }

    /// 本地 DB 注册表摘要（心跳 / 状态查询共用）。
    pub fn local_databases(&self) -> Vec<LocalDatabaseState> {
        self.registry
            .snapshot()
            .into_iter()
            .map(|db| LocalDatabaseState {
                database_id: db.database_id,
                state: convert::lifecycle_state_to_proto(db.state) as i32,
                owner_epoch: db.owner_epoch,
                pid: db.pid.unwrap_or(0) as i64,
            })
            .collect()
    }

    // ------------------------------------------------------------ 内部工具

    /// 校验指令目标 Worker（proto 中为空表示未指定）。
    fn ensure_target_worker(&self, target: &str) -> Result<()> {
        if target.trim().is_empty() || target == self.cfg.worker_id {
            return Ok(());
        }
        Err(WorkerError::WrongWorker {
            target: target.to_string(),
            local: self.cfg.worker_id.clone(),
        })
    }

    /// 请求 deadline 解析：取「请求字段」与「context」中更早的一个，都没有则用兜底值。
    ///
    /// 为什么取更早：两个字段都可能被填写，取最小值保证调用方不会等得比自己声明的更久。
    fn resolve_deadline(
        deadline_unix_ms: u64,
        context: Option<&common::RequestContext>,
        fallback: Duration,
    ) -> Instant {
        let from_field = convert::deadline_from_ms(deadline_unix_ms);
        let from_context = context
            .map(|ctx| convert::deadline_from_ms(ctx.deadline_unix_ms))
            .unwrap_or(None);
        match (from_field, from_context) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (None, None) => Instant::now() + fallback,
        }
    }

    /// 启动路径的兜底 deadline：恢复阶段允许 4 倍 READY 超时。
    ///
    /// 恢复 + spawn + READY 是三个串联阶段，READY 本身在 supervisor 内还有一次
    /// `runtime_ready_timeout` 限制，所以这里只需要给「工作集准备」一个边界。
    fn start_fallback_timeout(&self) -> Duration {
        self.cfg.runtime_ready_timeout.saturating_mul(4)
    }

    /// 资源准入（架构 §9）。`used` 会先扣除该 DB 已计入的预算，避免自己与自己叠加。
    fn admission_check(
        &self,
        db_id: &str,
        budget: &ResourceBudget,
        new_placement: bool,
    ) -> Result<()> {
        let existing = self.registry.get(db_id);
        let already_serving = existing.as_ref().map(|db| db.is_serving()).unwrap_or(false);
        let mut used = self.registry.used_budget();
        if let Some(db) = existing.as_ref() {
            used = used.saturating_sub(&db.budget);
        }
        // 已在服务的 DB 不重复占坑：它的进程位已经算在 serving_count 里
        let serving = self.registry.serving_count() as u64;
        let effective_new = new_placement && !already_serving;

        evaluate_admission(
            self.cfg.capacity,
            self.cfg.process_limit(),
            serving,
            &used,
            budget,
            effective_new,
        )
        .map_err(|reason| {
            crate::metrics::record_admission_denied(reason);
            WorkerError::AdmissionDenied(format!("db={db_id} 被拒绝：{reason}"))
        })
    }

    /// 命令响应（成功路径）。
    fn command_ok(&self, db_id: &str, pid: i32, elapsed: Duration) -> DatabaseCommandResponse {
        let entry = self.registry.get(db_id);
        DatabaseCommandResponse {
            error: None,
            database_id: db_id.to_string(),
            state: entry
                .as_ref()
                .map(|db| convert::lifecycle_state_to_proto(db.state) as i32)
                .unwrap_or(common::LifecycleState::Cold as i32),
            owner_epoch: entry.as_ref().map(|db| db.owner_epoch).unwrap_or(0),
            pid: pid as i64,
            local_socket: entry
                .as_ref()
                .map(|db| db.local_socket.display().to_string())
                .unwrap_or_default(),
            elapsed_micros: elapsed.as_micros() as u64,
        }
    }

    /// 命令响应（失败路径）：错误以 in-band `PlatformError` 返回，而不是 gRPC 错误状态。
    ///
    /// 理由：`StartDatabase` 的失败原因（epoch 过期 / 准入被拒 / 唤醒超时）是**业务**结论，
    /// Server 需要结构化错误码做决策；只有协议级 / 传输级故障才用 `Status`。
    fn command_err(
        &self,
        db_id: &str,
        err: &WorkerError,
        elapsed: Duration,
    ) -> DatabaseCommandResponse {
        let entry = self.registry.get(db_id);
        DatabaseCommandResponse {
            error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
            database_id: db_id.to_string(),
            state: entry
                .as_ref()
                .map(|db| convert::lifecycle_state_to_proto(db.state) as i32)
                .unwrap_or(common::LifecycleState::Cold as i32),
            owner_epoch: entry.as_ref().map(|db| db.owner_epoch).unwrap_or(0),
            pid: 0,
            local_socket: String::new(),
            elapsed_micros: elapsed.as_micros() as u64,
        }
    }

    /// 预备工作集（`prepare_only` 与 `PrepareMove` 共用的纯预拉取路径）。
    async fn prepare_work_set(
        &self,
        db_id: &str,
        owner_epoch: u64,
        snapshot: Option<SnapshotSource>,
        deadline: Instant,
    ) -> Result<PreparedWorkSet> {
        // 请求体必须先落到局部变量：prepare 的 future 借用它，而 timeout 在之后才 await
        let work = PrepareWork {
            database_id: db_id.to_string(),
            owner_epoch,
            snapshot,
            // 本地工作集 epoch 完全一致时才复用（restore 模块内部判定）
            allow_local_reuse: true,
            allow_wal_only: true,
        };
        let prepare = self.supervisor.work_sets().prepare(&work);
        match tokio::time::timeout_at(deadline.into(), prepare).await {
            Ok(result) => result,
            Err(_) => Err(WorkerError::WakeupTimeout(format!(
                "db={db_id} 预拉取工作集超过 deadline"
            ))),
        }
    }

    /// 记录一个「仅预拉取、尚未接管」的注册项（COLD + 无进程）。
    ///
    /// 为什么要落注册表：`GetWorkerStatus` / 心跳要能看到「本节点已为该 epoch 备好工作集」，
    /// `FinalizeMove` 也据此复用同一份数据而不必重新下载。COLD 不 `is_serving`，
    /// 因此数据面不会把请求路由进来（`check_serving` 会拒绝）。
    fn record_prepared(
        &self,
        db_id: &str,
        owner_epoch: u64,
        budget: ResourceBudget,
        prepared: &PreparedWorkSet,
    ) {
        self.registry.register(LocalDatabase {
            database_id: db_id.to_string(),
            state: LifecycleState::Cold,
            pid: None,
            local_socket: self.cfg.socket_path(db_id),
            owner_epoch,
            budget,
            started_at_unix_ms: now_unix_ms(),
            last_activity_unix_ms: now_unix_ms(),
            crash_count: 0,
            cgroup: None,
            restored_from: Some(RestoredFrom {
                snapshot_id: prepared.snapshot_id.clone(),
                base_lsn: prepared.base_lsn,
                applied_lsn: prepared.applied_lsn,
            }),
            read_only: false,
        });
    }

    /// 拉起 DB 进程（StartDatabase / RestartDatabase / FinalizeMove 共用）。
    async fn start_process(&self, spec: StartSpec) -> Result<StartOutcome> {
        self.supervisor.start(spec).await
    }

    /// 生成快照并上传 Object Storage（架构 §11.4）。
    async fn snapshot_and_upload(
        &self,
        db_id: &str,
        owner_epoch: u64,
        deadline: Instant,
    ) -> Result<SnapshotManifest> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| WorkerError::Storage("未配置对象存储（S3_*），无法上传快照".into()))?;

        let conn = self.pool.connection(db_id).await?;
        // snapshot_id 由 Worker 生成，它同时是**两个地方的主键**：Object Storage 的
        // 对象前缀（`snapshot::object_prefix`）与 Catalog 的 `snapshots.id`。
        // Server 用 `SnapshotId::from_str` 解析回传值，解析失败会另生成一个 UUID，
        // 于是「登记的 id」与「对象前缀」永久错位 —— 按 id 恢复会去下载一份不存在的
        // manifest。所以这里必须用平台 ID（UUID v7），epoch / 时间戳只进日志。
        let snapshot_id = domain::ids::SnapshotId::new_v7().to_string();

        let mut frame = conn.frame_for(
            "",
            rt::frame::Message::SnapshotRequest(rt::SnapshotRequest {
                snapshot_id: snapshot_id.clone(),
                // 0 = 由 DB Process 报告自己 checkpoint 到的 LSN
                base_lsn: 0,
                object_key: String::new(),
            }),
        );
        frame.deadline_unix_ms = convert::deadline_ms_from(Some(deadline));

        let mut lease = self.pool.lease_on(&conn).await?;
        lease.send(frame).await?;

        let response = tokio::time::timeout_at(deadline.into(), async {
            loop {
                let frame = lease.recv_required(Some(deadline)).await?;
                // 错误响应帧**只**带 `error`、不带 message（`db_runtime::frame::error_reply`
                // 的纪律：有 error 就没有 message）。漏掉这一支只会得到「空载荷」这种
                // 把真因藏起来的误报，所以必须先看 error。
                if let Some(error) = frame.error.as_ref() {
                    return Err(WorkerError::DbProcess(
                        protocol::convert::platform_error_from_proto(error),
                    ));
                }
                match frame.message {
                    Some(rt::frame::Message::SnapshotResponse(response)) => return Ok(response),
                    // 通知类帧（WalDurableNotice 等）不消费本次请求的响应，跳过
                    Some(_) => continue,
                    None => {
                        return Err(WorkerError::Uds(
                            "DB Process 返回了既无 error 也无 message 的空帧".into(),
                        ))
                    }
                }
            }
        })
        .await
        .map_err(|_| WorkerError::WakeupTimeout(format!("db={db_id} 快照生成超过 deadline")))??;

        // 快照点已经拿到，**立即归还连接租约**：后面的冻结 / 压缩 / 上传是纯 Worker 侧的
        // 重活（可能持续数秒），而租约占的是数据面共用连接池的一个读半连接
        // （`uds::DbConnectionPool`，每 DB 有上限）。捧着它做上传等于让快照挤占写请求的
        // 并发额度 —— 架构 §11.4 明确要求快照不阻塞正常写入，因此这里显式 drop。
        drop(lease);

        // 快照内容 = 主库文件 + 本地 WAL 在快照点（`base_lsn`）上的冻结前缀
        // （见 restore::freeze_wal_prefix：快照点之后的字节尚未 quorum durable，不进快照）。
        let mut files = crate::restore::collect_snapshot_files(&self.cfg.db_dir(db_id)).await?;
        let frozen = crate::restore::freeze_wal_prefix(
            &self.cfg.data_dir,
            db_id,
            &snapshot_id,
            response.base_lsn,
        )
        .await?;
        // relative_path 用引擎实际打开的文件名（`db-wal`），下载端才能原样还原到位。
        files.push((
            crate::paths::WAL_FILE_NAME.to_string(),
            frozen.path().to_path_buf(),
        ));
        if files.is_empty() {
            return Err(WorkerError::Storage(format!(
                "db={db_id} 工作集为空，拒绝上传空快照"
            )));
        }

        // 引擎版本优先取 DB Process 自报值（Worker 版本只是兜底）
        let engine_version = if conn.hello().engine_version.is_empty() {
            self.cfg.version.clone()
        } else {
            conn.hello().engine_version.clone()
        };

        let uploaded = snapshot_io::upload_snapshot(
            store.as_ref(),
            db_id,
            &snapshot_id,
            response.base_lsn,
            owner_epoch,
            &engine_version,
            SNAPSHOT_SCHEMA_VERSION,
            &files,
        )
        .await;
        // 临时文件与上传结果无关：失败时同样要清掉，否则它会一直占着工作集目录的磁盘。
        if let Err(err) = frozen.cleanup().await {
            tracing::warn!(db_id = %db_id, error = %err, "清理快照临时文件失败");
        }
        // 上传失败必须显式失败（不能留下「有 manifest 就是成功」的假象）：
        // upload_snapshot 只在 manifest 写成功后返回，因此这里报错 = 快照不可用。
        let manifest =
            uploaded.map_err(|err| WorkerError::Storage(format!("上传快照失败：{err}")))?;

        // 本地 ledger 记录：下次冷启动可直接复用这份快照，无需向 Server 询问。
        self.supervisor
            .work_sets()
            .record_snapshot(
                db_id,
                LedgerEntry {
                    snapshot_id: manifest.snapshot_id.clone(),
                    base_lsn: manifest.base_lsn,
                    checksum: manifest.checksum.clone(),
                    size_bytes: manifest.total_size_bytes,
                    created_at_unix_ms: manifest.created_at_unix_ms,
                    owner_epoch,
                },
            )
            .await?;

        tracing::info!(
            db_id = %db_id,
            snapshot_id = %manifest.snapshot_id,
            base_lsn = manifest.base_lsn,
            files = manifest.files.len(),
            size_bytes = manifest.total_size_bytes,
            "快照已生成并上传"
        );
        Ok(manifest)
    }
}

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    /// 拉起一个 DB（新建 / 唤醒 / 接管 / 崩溃后由 Server 显式重启）。
    async fn start_database(
        &self,
        request: Request<StartDatabaseRequest>,
    ) -> std::result::Result<Response<DatabaseCommandResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let db_id = req.database_id.clone();

        let outcome: Result<DatabaseCommandResponse> = async {
            self.ensure_target_worker(&req.worker_id)?;
            crate::paths::checked_id(&db_id)?;
            // proto 的 budget 是可选字段：未下发时退化为最小预算（与 Move 收尾兜底一致）
            let budget = req
                .budget
                .as_ref()
                .map(proto_budget)
                .unwrap_or_else(minimal_budget);

            if req.prepare_only {
                // 预拉取：不接管所有权、不拉起进程，因此不受 DRAINING 限制
                let deadline = Self::resolve_deadline(
                    req.deadline_unix_ms,
                    req.context.as_ref(),
                    self.start_fallback_timeout(),
                );
                let prepared = self
                    .prepare_work_set(&db_id, req.owner_epoch, None, deadline)
                    .await?;
                self.record_prepared(&db_id, req.owner_epoch, budget, &prepared);
                return Ok(self.command_ok(&db_id, 0, started.elapsed()));
            }

            // 新 Placement 才受 Worker 状态机限制（既有 DB 的重启 / 接管不在此列）
            let is_new = !self.registry.contains(&db_id);
            if is_new {
                self.state.ensure_accepts_new_placement()?;
            }
            self.registry.check_command_epoch(&db_id, req.owner_epoch)?;
            self.admission_check(&db_id, &budget, is_new)?;

            // 快照来源：proto 的 StartDatabase 不带 snapshot_id（冻结契约），因此交给
            // restore 模块按「本地工作集 -> 本地 ledger 最近快照 -> Remote WAL」顺序恢复。
            // restore_from_snapshot=false 表示优先复用本地工作集（epoch 精确匹配才复用，
            // 不匹配时 restore 模块仍会走快照 + WAL 重放，绝不允许接着旧文件跑）。
            let deadline = Self::resolve_deadline(
                req.deadline_unix_ms,
                req.context.as_ref(),
                self.start_fallback_timeout(),
            );
            let mut spec = StartSpec::new(&db_id, req.owner_epoch, budget);
            spec.deadline = Some(deadline);
            spec.allow_local_reuse = !req.restore_from_snapshot;
            let outcome = self.start_process(spec).await?;
            Ok(self.command_ok(&db_id, outcome.pid, started.elapsed()))
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(db_id = %db_id, code = err.code().as_str(), error = %err, "StartDatabase 失败");
                self.command_err(&db_id, &err, started.elapsed())
            }
        }))
    }

    /// 停止一个 DB（回到 COLD，保留 epoch 记录）。
    async fn stop_database(
        &self,
        request: Request<StopDatabaseRequest>,
    ) -> std::result::Result<Response<DatabaseCommandResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let db_id = req.database_id.clone();
        let deadline = Self::resolve_deadline(
            req.deadline_unix_ms,
            req.context.as_ref(),
            FALLBACK_CONTROL_TIMEOUT,
        );

        let outcome: Result<DatabaseCommandResponse> = async {
            // 未注册 = 本节点本来就没有这个 DB 的进程，Stop 幂等成功
            self.registry.check_command_epoch(&db_id, req.owner_epoch)?;
            if self.registry.contains(&db_id) {
                self.registry.transition(&db_id, LifecycleState::Stopping);
            }
            self.supervisor.stop(&db_id, req.graceful, deadline).await?;
            self.registry.transition(&db_id, LifecycleState::Cold);
            Ok(self.command_ok(&db_id, 0, started.elapsed()))
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(db_id = %db_id, error = %err, "StopDatabase 失败");
                self.command_err(&db_id, &err, started.elapsed())
            }
        }))
    }

    /// 重启一个 DB（同 epoch、同预算、复用本地工作集）。
    async fn restart_database(
        &self,
        request: Request<RestartDatabaseRequest>,
    ) -> std::result::Result<Response<DatabaseCommandResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let db_id = req.database_id.clone();

        let outcome: Result<DatabaseCommandResponse> = async {
            let entry = self
                .registry
                .check_command_epoch(&db_id, req.owner_epoch)?
                .ok_or_else(|| WorkerError::DbNotRegistered {
                    db_id: db_id.clone(),
                })?;
            if req.owner_epoch != entry.owner_epoch {
                // 更高 epoch 的 Restart 等价于接管（可能涉及新工作集），交给 StartDatabase
                return Err(WorkerError::InvalidState(format!(
                    "db={db_id} Restart 只接受当前 epoch {}，收到 {}；接管请走 StartDatabase",
                    entry.owner_epoch, req.owner_epoch
                )));
            }

            self.registry.transition(&db_id, LifecycleState::Stopping);
            // 用配置宽限而非请求 deadline：supervisor 内部超时会兜底 SIGKILL，
            // 这里给足时间让引擎 flush（停止不是可放弃的操作）。
            let stop_deadline = Instant::now() + self.cfg.runtime_stop_grace;
            self.supervisor.stop(&db_id, true, stop_deadline).await?;
            self.registry.transition(&db_id, LifecycleState::Cold);

            let deadline = Self::resolve_deadline(
                req.deadline_unix_ms,
                req.context.as_ref(),
                self.start_fallback_timeout(),
            );
            let mut spec = StartSpec::new(&db_id, entry.owner_epoch, entry.budget);
            spec.deadline = Some(deadline);
            // 重启必须复用本地工作集（epoch 未变，数据文件就是最新的）
            spec.allow_local_reuse = true;
            spec.snapshot = entry
                .restored_from
                .as_ref()
                .filter(|restored| !restored.snapshot_id.is_empty())
                .map(|restored| SnapshotSource {
                    snapshot_id: restored.snapshot_id.clone(),
                    base_lsn: restored.base_lsn,
                });
            let outcome = self.start_process(spec).await?;
            Ok(self.command_ok(&db_id, outcome.pid, started.elapsed()))
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(db_id = %db_id, error = %err, "RestartDatabase 失败");
                self.command_err(&db_id, &err, started.elapsed())
            }
        }))
    }

    /// 立即终止一个 DB（不走优雅关闭）。
    async fn kill_database(
        &self,
        request: Request<KillDatabaseRequest>,
    ) -> std::result::Result<Response<DatabaseCommandResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let db_id = req.database_id.clone();

        let outcome: Result<DatabaseCommandResponse> = async {
            self.registry.check_command_epoch(&db_id, req.owner_epoch)?;
            tracing::warn!(db_id = %db_id, reason = %req.reason, "收到 KillDatabase 指令");
            self.supervisor.kill(&db_id, &req.reason).await?;
            // kill 只负责发信号；这里先让 Server 看到「不在服务中」，
            // 进程退出与 cgroup 回收由监视任务继续完成。
            self.registry.clear_process(&db_id);
            if self.registry.contains(&db_id) {
                self.registry.transition(&db_id, LifecycleState::Stopping);
                self.registry.transition(&db_id, LifecycleState::Cold);
            }
            Ok(self.command_ok(&db_id, 0, started.elapsed()))
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(db_id = %db_id, error = %err, "KillDatabase 失败");
                self.command_err(&db_id, &err, started.elapsed())
            }
        }))
    }

    /// Worker 排空（架构 §12.3）：ACTIVE -> DRAINING（-> EMPTY）。
    async fn drain_worker(
        &self,
        request: Request<DrainWorkerRequest>,
    ) -> std::result::Result<Response<DrainWorkerResponse>, Status> {
        let req = request.into_inner();
        let deadline = Self::resolve_deadline(
            req.deadline_unix_ms,
            req.context.as_ref(),
            FALLBACK_CONTROL_TIMEOUT,
        );

        let fail = |err: WorkerError| {
            Response::new(DrainWorkerResponse {
                error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
                migrated: 0,
                remaining: self.registry.len() as u32,
            })
        };

        if let Err(err) = self.ensure_target_worker(&req.worker_id) {
            return Ok(fail(err));
        }
        match self.state.begin_drain() {
            Ok(state) => tracing::info!(
                worker_id = %self.cfg.worker_id,
                state = %state,
                stop_cold_and_warm = req.stop_cold_and_warm,
                "Worker 进入排空"
            ),
            Err(err) => return Ok(fail(err)),
        }

        // `stop_cold_and_warm=true` 表示「本节点不再保留这些 DB，就地停掉」；
        // false 表示只清掉 COLD 残留，进程类等待 Server 下发的 Move / Stop 指令。
        let mut migrated = 0u32;
        if req.stop_cold_and_warm {
            for db in self.registry.snapshot() {
                if Instant::now() >= deadline {
                    tracing::warn!(db_id = %db.database_id, "排空 deadline 已到，剩余 DB 未处理");
                    break;
                }
                match self.supervisor.stop(&db.database_id, true, deadline).await {
                    Ok(()) => {
                        migrated += 1;
                        self.registry.remove(&db.database_id);
                    }
                    Err(err) => {
                        tracing::warn!(db_id = %db.database_id, error = %err, "排空时停止 DB 失败")
                    }
                }
            }
        } else {
            for db in self.registry.snapshot() {
                if !db.is_serving() {
                    self.registry.remove(&db.database_id);
                    migrated += 1;
                }
            }
        }

        self.state.settle_if_drained(&self.registry);
        Ok(Response::new(DrainWorkerResponse {
            error: None,
            migrated,
            remaining: self.registry.len() as u32,
        }))
    }

    /// Move 预拉取：把 Snapshot + Remote WAL 落到本地工作集，**不接管所有权**。
    async fn prepare_move(
        &self,
        request: Request<PrepareMoveRequest>,
    ) -> std::result::Result<Response<PrepareMoveResponse>, Status> {
        let req = request.into_inner();
        let db_id = req.database_id.clone();

        let outcome: Result<u64> = async {
            crate::paths::checked_id(&db_id)?;
            if !req.source_worker_id.is_empty() && req.source_worker_id == self.cfg.worker_id {
                return Err(WorkerError::MoveConflict(format!(
                    "db={db_id} 的 source_worker_id 就是本节点，Move 目标与源相同"
                )));
            }
            // 本节点已有运行进程：准备新工作集会覆盖正在运行的 DB 的数据目录
            if let Some(existing) = self.registry.get(&db_id) {
                if existing.is_serving() && existing.owner_epoch != req.new_owner_epoch {
                    return Err(WorkerError::MoveConflict(format!(
                        "db={db_id} 正以 epoch {} 在本节点运行，不能作为 Move 目标",
                        existing.owner_epoch
                    )));
                }
            }
            self.registry
                .check_command_epoch(&db_id, req.new_owner_epoch)?;

            let deadline = Self::resolve_deadline(
                req.deadline_unix_ms,
                req.context.as_ref(),
                self.start_fallback_timeout(),
            );
            // 同 start_database：未下发预算时用最小预算兜底
            let budget = req
                .budget
                .as_ref()
                .map(proto_budget)
                .unwrap_or_else(minimal_budget);
            let snapshot = if req.snapshot_id.is_empty() {
                None
            } else {
                Some(SnapshotSource {
                    snapshot_id: req.snapshot_id.clone(),
                    base_lsn: req.base_lsn,
                })
            };
            let prepared = self
                .prepare_work_set(&db_id, req.new_owner_epoch, snapshot, deadline)
                .await?;
            self.record_prepared(&db_id, req.new_owner_epoch, budget, &prepared);
            tracing::info!(
                db_id = %db_id,
                new_owner_epoch = req.new_owner_epoch,
                applied_lsn = prepared.applied_lsn,
                reused_local = prepared.reused_local,
                "Move 预拉取完成"
            );
            Ok(prepared.applied_lsn)
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(ready_lsn) => PrepareMoveResponse {
                error: None,
                ready_lsn,
            },
            Err(err) => {
                tracing::warn!(db_id = %db_id, error = %err, "PrepareMove 失败");
                PrepareMoveResponse {
                    error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
                    ready_lsn: 0,
                }
            }
        }))
    }

    /// Move 收尾：以新 epoch 拉起进程，完成 Ownership Cutover。
    async fn finalize_move(
        &self,
        request: Request<FinalizeMoveRequest>,
    ) -> std::result::Result<Response<DatabaseCommandResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let db_id = req.database_id.clone();

        let outcome: Result<DatabaseCommandResponse> = async {
            if !req.source_worker_id.is_empty() && req.source_worker_id == self.cfg.worker_id {
                return Err(WorkerError::MoveConflict(format!(
                    "db={db_id} 的 source_worker_id 就是本节点"
                )));
            }
            // 接管 = 新 Placement，DRAINING 的 Worker 必须拒绝（架构 §12.3）
            self.state.ensure_accepts_new_placement()?;
            let existing = self
                .registry
                .check_command_epoch(&db_id, req.new_owner_epoch)?;
            let budget = existing
                .as_ref()
                .map(|db| db.budget)
                .unwrap_or_else(minimal_budget);
            self.admission_check(&db_id, &budget, existing.is_none())?;

            let deadline = Self::resolve_deadline(
                req.deadline_unix_ms,
                req.context.as_ref(),
                self.start_fallback_timeout(),
            );
            let mut spec = StartSpec::new(&db_id, req.new_owner_epoch, budget);
            spec.deadline = Some(deadline);
            // PrepareMove 已把工作集准备好，epoch 一致才复用（restore 模块判定）
            spec.allow_local_reuse = true;
            spec.snapshot = existing
                .as_ref()
                .and_then(|db| db.restored_from.as_ref())
                .filter(|restored| !restored.snapshot_id.is_empty())
                .map(|restored| SnapshotSource {
                    snapshot_id: restored.snapshot_id.clone(),
                    base_lsn: restored.base_lsn,
                });
            let outcome = self.start_process(spec).await?;
            tracing::info!(
                db_id = %db_id,
                new_owner_epoch = req.new_owner_epoch,
                pid = outcome.pid,
                "Move 收尾完成，所有权已切换"
            );
            Ok(self.command_ok(&db_id, outcome.pid, started.elapsed()))
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(db_id = %db_id, error = %err, "FinalizeMove 失败");
                self.command_err(&db_id, &err, started.elapsed())
            }
        }))
    }

    /// 查询本节点当前状态（reconcile 用）。
    async fn get_worker_status(
        &self,
        _request: Request<GetWorkerStatusRequest>,
    ) -> std::result::Result<Response<GetWorkerStatusResponse>, Status> {
        // 状态查询不产生采样 IO：用最近一次（心跳刷新过）的结果即可
        Ok(Response::new(GetWorkerStatusResponse {
            error: None,
            worker: Some(self.worker_info()),
            usage: Some(self.sampler.current().into()),
            databases: self.local_databases(),
            inventory_version: self.registry.inventory_version(),
        }))
    }

    /// 触发一次异步快照上传（不进 Commit Hot Path，架构 §11.4）。
    async fn trigger_snapshot(
        &self,
        request: Request<TriggerSnapshotRequest>,
    ) -> std::result::Result<Response<TriggerSnapshotResponse>, Status> {
        let req = request.into_inner();
        let db_id = req.database_id.clone();
        let deadline = Self::resolve_deadline(0, req.context.as_ref(), FALLBACK_SNAPSHOT_TIMEOUT);

        let outcome: Result<SnapshotManifest> = async {
            // 数据面语义的精确 epoch 校验：快照必须属于当前 Owner
            self.registry.check_data_epoch(&db_id, req.owner_epoch)?;
            self.registry.check_serving(&db_id)?;
            self.snapshot_and_upload(&db_id, req.owner_epoch, deadline)
                .await
        }
        .await;

        Ok(Response::new(match outcome {
            Ok(manifest) => {
                observability::metrics::record_snapshot("ok");
                // 先取出 object_key：manifest_key() 借用 manifest，而下面要移动它的字段
                let object_key = manifest.manifest_key();
                TriggerSnapshotResponse {
                    error: None,
                    snapshot: Some(common::SnapshotMeta {
                        database_id: db_id,
                        snapshot_id: manifest.snapshot_id,
                        base_lsn: manifest.base_lsn,
                        checksum: manifest.checksum,
                        created_at_unix_ms: manifest.created_at_unix_ms.max(0) as u64,
                        owner_epoch_at_snapshot: manifest.owner_epoch,
                        engine_version: manifest.engine_version,
                        size_bytes: manifest.total_size_bytes,
                        object_key,
                    }),
                }
            }
            Err(err) => {
                observability::metrics::record_snapshot("error");
                tracing::warn!(db_id = %db_id, error = %err, "TriggerSnapshot 失败");
                TriggerSnapshotResponse {
                    error: Some(convert::platform_error_to_proto(&err.to_platform_error())),
                    snapshot: None,
                }
            }
        }))
    }
}

/// proto 预算 -> domain 预算；未填写（全 0）时退化为最小预算。
///
/// 为什么兜底而不是拒绝：`process_slots = 0` 会绕过进程数 hard limit，
/// 而 `cpu_milli = 0` + `memory_mib = 0` 会让 DB 进程落进「零配额」cgroup（被内核饿死）。
/// 因此统一保证至少 1 个进程位，其余维度保持原样（0 在 cgroup 层表示「不限制」）。
fn proto_budget(budget: &common::ResourceBudget) -> ResourceBudget {
    let converted: ResourceBudget = (*budget).into();
    if converted.process_slots == 0 {
        ResourceBudget {
            process_slots: 1,
            ..converted
        }
    } else {
        converted
    }
}

/// 最小可用预算（Move 收尾时若本地没有注册项，用它兜底）。
fn minimal_budget() -> ResourceBudget {
    ResourceBudget {
        process_slots: 1,
        ..ResourceBudget::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capacity(cpu_milli: u64, memory_mib: u64, slots: u64) -> WorkerCapacity {
        // fd / 磁盘留足余量：这些用例只想让 CPU（或进程位）成为瓶颈。
        // 若把它们也设成 1024，则「预算 = 容量」会让这两维恒为 100%，
        // 水位闸门永远落在 EMERGENCY，就测不出 80% / 90% 两档了。
        WorkerCapacity::new(cpu_milli, memory_mib, 1_000_000, 1_000_000, slots, 0)
    }

    fn budget(cpu_milli: u64, memory_mib: u64, slots: u64) -> ResourceBudget {
        ResourceBudget::new(cpu_milli, memory_mib, 1024, 1024, slots, 0)
    }

    #[test]
    fn admission_denies_when_capacity_is_exceeded() {
        let err = evaluate_admission(
            capacity(2000, 512, 16),
            16,
            0,
            &ResourceBudget::ZERO,
            &budget(4000, 256, 1),
            true,
        )
        .unwrap_err();
        assert_eq!(err, "capacity");
    }

    #[test]
    fn admission_denies_new_placement_above_watermark() {
        // 容量 1000 milli，请求 850 -> 放置后 85% > 80% 水位
        let err = evaluate_admission(
            capacity(1000, 1024, 16),
            16,
            0,
            &ResourceBudget::ZERO,
            &budget(850, 128, 1),
            true,
        )
        .unwrap_err();
        assert_eq!(err, "watermark_stop_new_placement");

        // 同一预算在「非新 Placement」路径（重启既有 DB）必须放行
        assert!(evaluate_admission(
            capacity(1000, 1024, 16),
            16,
            0,
            &ResourceBudget::ZERO,
            &budget(850, 128, 1),
            false,
        )
        .is_ok());
    }

    #[test]
    fn admission_denies_emergency_watermark() {
        let err = evaluate_admission(
            capacity(1000, 1024, 16),
            16,
            0,
            &ResourceBudget::ZERO,
            &budget(950, 128, 1),
            true,
        )
        .unwrap_err();
        assert_eq!(err, "watermark_emergency");
    }

    #[test]
    fn admission_denies_when_process_limit_reached() {
        let err = evaluate_admission(
            capacity(100_000, 4096, 4),
            4,
            4, // 已经跑满
            &ResourceBudget::ZERO,
            &budget(100, 16, 1),
            true,
        )
        .unwrap_err();
        assert_eq!(err, "process_limit");
    }

    #[test]
    fn admission_requires_process_slot() {
        let err = evaluate_admission(
            capacity(100_000, 4096, 4),
            4,
            0,
            &ResourceBudget::ZERO,
            &budget(100, 16, 0),
            true,
        )
        .unwrap_err();
        assert_eq!(err, "zero_process_slots");
    }

    #[test]
    fn proto_budget_falls_back_to_one_process_slot() {
        let converted = proto_budget(&common::ResourceBudget {
            cpu_milli: 1000,
            memory_mib: 256,
            file_descriptors: 64,
            disk_mib: 128,
            process_slots: 0,
            iops: 0,
        });
        assert_eq!(converted.process_slots, 1);
        assert_eq!(converted.cpu_milli, 1000);

        let explicit = proto_budget(&common::ResourceBudget {
            process_slots: 8,
            ..Default::default()
        });
        assert_eq!(explicit.process_slots, 8);
    }

    #[test]
    fn deadline_resolution_prefers_earliest_source() {
        let now = Instant::now();
        let soon = now + Duration::from_secs(5);
        let later = now + Duration::from_secs(30);
        let context = common::RequestContext {
            deadline_unix_ms: convert::deadline_ms_from(Some(later)),
            ..Default::default()
        };
        let resolved = WorkerControlService::resolve_deadline(
            convert::deadline_ms_from(Some(soon)),
            Some(&context),
            FALLBACK_CONTROL_TIMEOUT,
        );
        let remaining = resolved.saturating_duration_since(Instant::now());
        // 应当取更早的那个（5s），而不是 30s
        assert!(
            remaining <= Duration::from_secs(6),
            "remaining={remaining:?}"
        );
    }

    #[test]
    fn deadline_resolution_uses_fallback_when_absent() {
        let resolved = WorkerControlService::resolve_deadline(0, None, Duration::from_secs(7));
        let remaining = resolved.saturating_duration_since(Instant::now());
        assert!(remaining <= Duration::from_secs(8) && remaining >= Duration::from_secs(5));
    }
}
