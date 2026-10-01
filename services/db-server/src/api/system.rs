//! 系统端点：liveness / readiness / metrics / OpenAPI（架构 §17.4 / §17.14）。

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::api_doc;
use crate::state::AppState;

/// liveness 响应。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthStatus {
    /// 固定为 `ok`：进程活着且事件循环能处理请求。
    pub status: String,
    /// 服务名。
    pub service: String,
    /// 版本。
    pub version: String,
}

/// readiness 响应。
///
/// 未就绪时返回 **503**：让负载均衡把实例摘掉，而不是让它带着未知状态接请求。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReadinessStatus {
    /// 是否整体就绪。
    pub ready: bool,
    pub metadata: bool,
    /// PostgreSQL Catalog 是否可达（权威事实源，不可用则不接控制面流量）。
    pub postgres: Option<bool>,
    /// Worker 心跳监控是否已启动。
    pub heartbeat_monitor: Option<bool>,
    /// Route Cache 是否完成过至少一次全量 reconcile。
    pub route_reconciler: Option<bool>,
    /// 未就绪时的原因说明。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// 系统路由。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/v1/deployment", get(deployment))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
}

/// Liveness 探针。
///
/// 语义刻意保持最弱：**只**回答「进程是否还能处理请求」。依赖检查放在 `/readyz`，
/// 否则一次 Catalog 抖动会导致编排器把健康的实例反复重启。
#[utoipa::path(
    get,
    path = "/healthz",
    tag = "system",
    responses(
        (status = 200, description = "进程存活", body = HealthStatus)
    )
)]
pub async fn healthz() -> Json<HealthStatus> {
    Json(HealthStatus {
        status: "ok".to_string(),
        service: "db-server".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Readiness 探针。
///
/// 就绪 = PostgreSQL 可达 **且** 心跳监控已就绪（架构 §17.14 的 Start Order：
/// PostgreSQL Ready -> Server Ready -> Worker 管理就绪）。
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "system",
    responses(
        (status = 200, description = "可以接流量", body = ReadinessStatus),
        (status = 503, description = "尚未就绪（依赖未满足）", body = ReadinessStatus)
    )
)]
pub async fn readyz(State(state): State<AppState>) -> Response {
    // 每次探针都真查一次 PostgreSQL：缓存「上次可用」会让已经失联的实例继续接流量。
    let postgres = match state.catalog.health_check().await {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(code = err.code.as_str(), message = %err.message, "readyz: PostgreSQL 不可达");
            false
        }
    };
    let heartbeat_monitor = state.readiness.heartbeat_monitor_ready();
    let route_reconciler = state.readiness.reconciler_ready();
    let distributed = matches!(
        state.deployment,
        crate::deployment::Deployment::Distributed(_)
    );
    let ready = postgres && state.readiness.is_ready();

    let detail = if ready {
        None
    } else if !postgres {
        Some("PostgreSQL Catalog 不可达".to_string())
    } else if !heartbeat_monitor {
        Some("Worker 心跳监控尚未就绪".to_string())
    } else {
        Some("Route Cache 尚未完成首次全量 reconcile".to_string())
    };

    let body = ReadinessStatus {
        ready,
        metadata: postgres,
        postgres: distributed.then_some(postgres),
        heartbeat_monitor: distributed.then_some(heartbeat_monitor),
        route_reconciler: distributed.then_some(route_reconciler),
        detail,
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(body)).into_response()
}

/// Prometheus 指标。
///
/// **安全约束（架构 §17.4）**：该端点只允许集群内抓取，**不得经公网 / edge 网络暴露**。
/// 部署上它应当只监听内部端口（`OPS_LISTEN`）或由 Ingress 拒绝外部来源；
/// 指标里包含 worker_id / db_id 等内部拓扑信息。
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "system",
    responses(
        (status = 200, description = "Prometheus 文本格式指标（仅集群内抓取）", body = String)
    )
)]
pub async fn metrics(
    State(state): State<AppState>,
    principal: Result<crate::auth::Principal, crate::error::ApiError>,
) -> Response {
    if matches!(state.deployment, crate::deployment::Deployment::Simple(_)) {
        match principal.and_then(|p| p.require(crate::auth::permission::DB_ADMIN)) {
            Ok(()) => {}
            Err(error) => return error.into_response(),
        }
    }
    let body = state.metrics.render();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

/// OpenAPI 契约（JSON）。
#[utoipa::path(
    get,
    path = "/api/v1/openapi.json",
    tag = "system",
    responses((status = 200, description = "OpenAPI 3.1 文档", body = serde_json::Value))
)]
pub async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(api_doc())
}

/// `/swagger-ui` -> `/swagger-ui/`（Swagger UI 的资源路径带尾斜杠）。
/// 交互式文档入口。
///
/// 平台**不在服务端托管 Swagger UI**：`utoipa-swagger-ui` 的 build.rs 需要在构建期
/// 联网下载前端资源，在离线/受限网络下会直接让整次构建失败。契约本身由
/// [`openapi_json`] 提供，前端用 orval 从该 URL 生成 TypeScript client。
/// 需要交互式文档时，把任意 Swagger UI 静态站点指向 `/api/v1/openapi.json` 即可。
pub async fn swagger_redirect() -> impl IntoResponse {
    (
        StatusCode::FOUND,
        [(header::LOCATION, "/api/v1/openapi.json")],
    )
}

/// 客户端在执行管理动作前读取能力；这里不包含任何凭据或拓扑地址。
pub async fn deployment(State(state): State<AppState>) -> Json<crate::deployment::DeploymentInfo> {
    Json(state.deployment.info())
}
