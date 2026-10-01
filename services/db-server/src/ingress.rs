//! Worker 上报入口（gRPC `ServerIngress`）。
//!
//! ## 为什么需要这个模块
//!
//! 心跳方向是 **Worker -> Server**，而 `WorkerControl` 服务由 Worker 实现（承载
//! Server -> Worker 的 Start/Stop/Move/Drain 指令）。把 Heartbeat 放进 `WorkerControl`
//! 会出现「实现方与调用方向相反」的矛盾，所以协议里单列了由 Server 实现的
//! `ServerIngress`（见 `proto/platform/control.proto`）。
//!
//! 本模块就是 db-server 侧的实现：
//! - 首次收到某 Worker 的心跳时自动注册它（不能要求运维先手工登记）；
//! - 之后每次心跳更新资源占用与租约，并回报 Server 侧的期望状态；
//! - **心跳失败不得影响数据面**：这里只做控制面的记账，任何错误都以 in-band
//!   错误返回给 Worker，由 Worker 退避重试，而不是让 Server 崩掉。
//!
//! 心跳语义（架构 §16，冻结）：1s 一次；连续 3 次 miss 判定 Suspect / Unavailable。
//! Worker 侧的超时判定与 Server 侧的 `missed_heartbeats` 计数共同实现该语义。

use std::sync::Arc;

use domain::error::ErrorCode;
use domain::ids::WorkerId;
use domain::lifecycle::WorkerState;
use domain::resources::WorkerResourceUsage;
use protocol::common::PlatformError;
use protocol::control::server_ingress_server::{ServerIngress, ServerIngressServer};
use protocol::control::{
    HeartbeatRequest, HeartbeatResponse, LocalDatabaseState as ProtoLocalDbState,
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};

use crate::state::DistributedState;

/// Worker 上报服务实现。
pub struct ServerIngressService {
    state: Arc<DistributedState>,
}

impl ServerIngressService {
    /// 构造。
    #[must_use]
    pub fn new(state: Arc<DistributedState>) -> Self {
        Self { state }
    }

    /// 组装 tonic 服务（供 `app` 启动 gRPC server 使用）。
    #[must_use]
    pub fn into_server(self) -> ServerIngressServer<Self> {
        ServerIngressServer::new(self)
    }

    /// 把 proto 的 `WorkerResourceUsage` 转成领域模型。
    ///
    /// proto 里的总量字段（`*_total_*`）就是 Worker 的容量，因此首次心跳不需要
    /// 另一个「注册」RPC 就能完成注册。
    fn usage_from_proto(
        usage: Option<&protocol::common::WorkerResourceUsage>,
    ) -> WorkerResourceUsage {
        // proto <-> domain 的字段映射已由 protocol::convert 的 From 实现统一维护，
        // 这里直接复用，避免两处字段名漂移。
        usage
            .map(|u| WorkerResourceUsage::from(*u))
            .unwrap_or_default()
    }

    /// 把 proto 的本地 DB 状态转成领域模型。
    fn dbs_from_proto(input: &[ProtoLocalDbState]) -> Vec<catalog::LocalDatabaseState> {
        input
            .iter()
            .map(|entry| catalog::LocalDatabaseState {
                database_id: entry.database_id.parse().unwrap_or_else(|_| {
                    // 非法 UUID 不能让整次心跳失败：跳过这一条即可（Worker 会在
                    // 下一次全量上报时纠正）。
                    domain::DatabaseId::new_v7()
                }),
                state: domain::LifecycleState::from_proto_i32(entry.state),
                owner_epoch: domain::OwnerEpoch::new(entry.owner_epoch),
                pid: Some(entry.pid),
            })
            .collect()
    }
}

#[tonic::async_trait]
impl ServerIngress for ServerIngressService {
    /// 接收 Worker 心跳：首次视为注册，之后更新占用与租约。
    #[instrument(skip_all, fields(worker_id = %request.get_ref().worker.as_ref().map(|w| w.worker_id.clone()).unwrap_or_default()))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let Some(info) = request.worker.clone() else {
            return Err(Status::invalid_argument("心跳缺少 worker 信息"));
        };
        if info.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("心跳缺少 worker_id"));
        }

        let worker_id = WorkerId::new(info.worker_id.clone());
        let usage = Self::usage_from_proto(request.usage.as_ref());
        let dbs = Self::dbs_from_proto(&request.databases);
        // Worker 自报的本地状态：Catalog 只用它判定 EMPTY -> ACTIVE 的重新接纳
        // （见 catalog::Catalog::record_heartbeat 的说明），其余状态迁移仍由控制面驱动。
        let reported_state = WorkerState::from_proto_i32(info.state);

        // 首次心跳即注册：容量直接取自心跳里的总量字段。
        // 先尝试 upsert（幂等），避免「必须先有人手工登记 Worker」这一不合理的运维要求。
        let params = catalog::UpsertWorkerParams {
            id: worker_id.clone(),
            endpoint: if info.endpoint.is_empty() {
                // 没有显式 endpoint 时用 worker_id 占位：真实地址由 Server 从
                // 服务发现/配置推导，后续心跳会补全。
                info.worker_id.clone()
            } else {
                info.endpoint.clone()
            },
            // 控制面用一个 endpoint，数据面必须单独存：两者是不同端口上的不同服务，
            // 混用会让 SQL 请求打到控制端口并得到 gRPC Unimplemented。
            control_endpoint: Some(info.endpoint.clone()),
            data_endpoint: Some(match info.data_endpoint.trim() {
                "" => info.endpoint.clone(),
                other => other.to_owned(),
            }),
            region: Some(info.region.clone()),
            zone: Some(info.zone.clone()),
            version: Some(info.version.clone()),
            cpu_milli_total: Some(usage.cpu_milli_total as i64),
            memory_mib_total: Some(usage.memory_mib_total as i64),
            fd_total: Some(usage.fd_total as i64),
            disk_mib_total: Some(usage.disk_mib_total as i64),
            process_slots_total: Some(usage.db_process_limit as i64),
            iops_total: Some(usage.iops_total as i64),
            reserved_for_failover: None,
            labels: None,
        };

        let registered = self.state.catalog.upsert_worker(params).await;
        let first_seen = registered
            .as_ref()
            .map(|w| w.created_at == w.updated_at)
            .unwrap_or(false);
        if let Err(err) = registered {
            // 注册失败不致命：可能只是并发 upsert 冲突，继续尝试记录心跳。
            debug!(worker_id = %worker_id, error = %err, "Worker upsert 失败，继续尝试记录心跳");
        } else if first_seen {
            info!(worker_id = %worker_id, "Worker 首次心跳，已注册到 Catalog");
        }

        let outcome = self
            .state
            .catalog
            .record_heartbeat(
                worker_id.clone(),
                &usage,
                request.inventory_version as i64,
                &dbs,
                reported_state,
            )
            .await
            .map_err(|err| {
                warn!(worker_id = %worker_id, error = %err, "记录心跳失败");
                Status::unavailable(format!("记录心跳失败: {err}"))
            })?;

        // 就绪标记：至少一个 Worker 上报过，说明控制面与 Worker 的链路可用。
        self.state.readiness.mark_heartbeat_monitor_ready();

        // 对账回收（架构 §12.2）：Catalog 认为归该 Worker、而它本地已不存在的 DB 已被
        // 置回 COLD。必须立刻失效对应路由 —— 否则请求会继续命中缓存里那条已经指向
        // 「不存在进程」的路由并拿到 NOT_OWNER，白等一次 stale-route 重试。
        for reclaimed in &outcome.reclaimed {
            self.state.routes.invalidate(&reclaimed.database_id);
            info!(
                worker_id = %worker_id,
                database_id = %reclaimed.database_id,
                from_epoch = reclaimed.from_epoch,
                to_epoch = reclaimed.to_epoch,
                reason = reclaimed.reason,
                "Worker inventory 缺失，已回收 ownership（DB 置回 COLD，可被重新放置）"
            );
        }

        let states = self.state.catalog.list_workers().await.unwrap_or_default();
        let draining = states
            .iter()
            .find(|w| w.id == worker_id)
            .map(|w| matches!(w.state, WorkerState::Draining | WorkerState::Empty))
            .unwrap_or(false);

        Ok(Response::new(HeartbeatResponse {
            request_full_inventory: outcome.request_full_inventory,
            catalog_version: outcome.catalog_version as u64,
            draining: draining || outcome.draining,
            // 排空完成（EMPTY）后的明确重新接纳授权：Worker 据此可以自行回到 ACTIVE。
            // DRAINING 期间 outcome 恒为 false —— 排空未完成不得接新 Placement（§12.3）。
            accept_new_placement: outcome.accept_new_placement,
        }))
    }
}

/// 把领域错误转成 gRPC 状态（仅日志用；对外错误走 in-band 响应体）。
#[allow(dead_code)]
fn status_for(code: ErrorCode, message: impl Into<String>) -> Status {
    let message = message.into();
    let _ = code;
    Status::internal(message)
}

/// 供 `app` 组装 gRPC server 的便捷函数。
#[must_use]
pub fn server(state: Arc<DistributedState>) -> ServerIngressServer<ServerIngressService> {
    ServerIngressService::new(state).into_server()
}

/// 平台错误 -> proto（保持与其它模块一致的错误语义）。
#[must_use]
pub fn to_platform_error(err: domain::error::PlatformError) -> PlatformError {
    protocol::convert::platform_error_to_proto(&err)
}
