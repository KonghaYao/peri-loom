//! HTTP 出口：路由装配、OpenAPI 契约、长操作提交与审计（架构 §17.4）。
//!
//! 出口路径固定（Frozen）：
//!
//! ```text
//! /                -> 由 web 容器提供 SPA，db-server 不实现
//! /api/v1/*        -> Management / DBA REST JSON（OpenAPI 3.1）
//! /data/v1/*       -> SQL / Data HTTP API
//! /db/{db_id}/v2/pipeline -> TursoDB / libsql 客户端兼容端点（Hrana over HTTP v2，见 `hrana`）
//! /healthz         -> liveness
//! /readyz          -> readiness
//! /metrics         -> 仅集群内抓取，**不得经公网暴露**
//! /api/v1/openapi.json + /swagger-ui -> 契约与交互式文档
//! ```
//!
//! 三条工程约束贯穿本模块：
//! 1. **错误体冻结**：所有失败都经 [`crate::error::ApiError`] 转成
//!    `{"error":{code,message,request_id,retryable}}`（见 `error.rs`）。
//! 2. **长操作异步化**：`Create/Start/Stop/Restart/Move/Delete/Snapshot/Backup/Restore`
//!    一律「写 operation + 投递 job + 立刻 202」，执行由后台 Runner 完成。
//! 3. **DBA 写操作 100% 审计**：执行任何副作用前先调用 [`audit_success`] /
//!    [`audit_failure`]，两者覆盖成功与失败两条路径。

pub mod auth_routes;
pub mod data;
pub mod dto;
pub mod hrana;
pub mod management;
pub mod system;

use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, Method};
use axum::routing::{get, post};
use axum::Router;
use catalog::IdempotencyOutcome;
use domain::error::{ErrorCode, PlatformError};
use domain::ids::{DatabaseId, OperationId, WorkerId};
use utoipa::openapi::security::{ApiKey, ApiKeyValue, Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::auth::Principal;
use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::idempotency::{extract_idempotency_key, request_fingerprint};
use crate::middleware::{current_request_id, current_source_ip, inject_request_meta};
use crate::state::AppState;

/// 单次请求体上限（SQL 文本 + 参数）。
///
/// 16 MiB 的依据：SQL 语句与绑定参数都不该接近这个量级，而「批量 INSERT 的巨型字面量」
/// 超过它时更合理的做法是走 COPY / 分段提交。设上限是为了避免一个畸形请求把内存吃光。
pub const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// 组装完整的 HTTP 出口路由。
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(system::routes())
        // 认证路由：登录本身不需要认证（否则无法首次获取凭据）
        .merge(
            Router::new()
                .route("/api/v1/auth/login", post(auth_routes::login))
                .route("/api/v1/auth/me", get(auth_routes::me)),
        )
        .merge(
            management::routes()
                .route("/api/v1/openapi.json", get(system::openapi_json))
                // 不在服务端托管 Swagger UI：utoipa-swagger-ui 的 build.rs 会联网下载
                // 前端资源，在国内网络/离线构建下必然失败。契约本身由
                // /api/v1/openapi.json 提供（前端用 orval 从该契约生成 TS client），
                // 需要交互式文档时可用任意 Swagger UI 静态托管指向该 URL。
                .route("/swagger-ui", get(system::swagger_redirect)),
        )
        .merge(data::routes())
        // TursoDB / libsql 客户端兼容端点（Hrana over HTTP v2）。它**不是**平台自有
        // 契约的一部分，因此不进 OpenAPI 文档（见 `hrana` 模块头）。
        .merge(hrana::routes())
        .with_state(state)
        // 请求上下文（request_id / source_ip）必须在最外层：错误体与审计都依赖它。
        .layer(axum::middleware::from_fn(inject_request_meta))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
}

/// OpenAPI 文档（由 Rust 生成，前端 client 以它为唯一契约来源）。
#[derive(OpenApi)]
#[openapi(
    info(
        title = "TursoDB DB Platform — Server Plane API",
        version = "0.1.0",
        description = "平台唯一公网出口：Management / DBA REST + SQL Data API。\
                       长操作返回 202 与 operation_id，通过 GET /api/v1/operations/{operation_id} 查询进展。\
                       错误体统一为 {\"error\":{code,message,request_id,retryable}}。\
                       认证：Authorization: Bearer <JWT> 或 x-api-token: <platform token>。"
    ),
    paths(
        // ---- auth
        auth_routes::login,
        auth_routes::me,
        // ---- system
        system::healthz,
        system::readyz,
        system::metrics,
        system::openapi_json,
        // ---- databases
        management::create_database,
        management::list_databases,
        management::get_database,
        management::delete_database,
        management::start_database,
        management::stop_database,
        management::restart_database,
        management::move_database,
        management::snapshot_database,
        management::backup_database,
        management::restore_database,
        // ---- operations
        management::get_operation,
        management::list_operations,
        // ---- workers
        management::list_workers,
        management::get_worker,
        management::drain_worker,
        // ---- snapshots / audit
        management::list_snapshots,
        management::list_audit,
        // ---- tokens
        management::create_token,
        management::list_tokens,
        management::revoke_token,
        // ---- panel
        management::get_preference,
        management::put_preference,
        management::list_saved_queries,
        management::create_saved_query,
        management::delete_saved_query,
        management::list_slow_queries,
        // ---- data
        data::query_database,
        data::batch_database,
        data::open_session,
        data::session_query,
        data::close_session,
    ),
    components(schemas(
        dto::LoginRequest,
        dto::LoginResponse,
        dto::Viewer,
        crate::error::ErrorBody,
        crate::error::ErrorEnvelope,
        dto::Page<dto::DatabaseView>,
        dto::Page<dto::OperationView>,
        dto::Page<dto::AuditView>,
        dto::CreateDatabaseRequest,
        dto::DatabaseView,
        dto::BudgetView,
        dto::OperationAccepted,
        dto::OperationView,
        dto::JobView,
        dto::WorkerView,
        dto::UsageView,
        dto::CapacityView,
        dto::DrainWorkerRequest,
        dto::MoveDatabaseRequest,
        dto::RestoreDatabaseRequest,
        dto::SnapshotView,
        dto::AuditView,
        dto::CreateTokenRequest,
        dto::TokenCreated,
        dto::TokenView,
        dto::CreateSavedQueryRequest,
        dto::SavedQueryView,
        dto::PreferenceView,
        dto::PutPreferenceRequest,
        dto::SlowQueryView,
        dto::QueryRequest,
        dto::BatchRequest,
        dto::BatchStatementDto,
        dto::QueryResponse,
        dto::BatchResponse,
        dto::ResultSetView,
        dto::ColumnView,
        dto::SessionOpened,
        dto::SessionClosed,
    )),
    modifiers(&SecuritySchemes),
    tags(
        (name = "system", description = "健康检查、指标与契约"),
        (name = "databases", description = "数据库生命周期管理"),
        (name = "operations", description = "长操作跟踪"),
        (name = "workers", description = "Worker 与资源"),
        (name = "snapshots", description = "快照与备份"),
        (name = "audit", description = "审计日志"),
        (name = "tokens", description = "API Token 管理"),
        (name = "panel", description = "Panel 偏好与 Saved SQL"),
        (name = "data", description = "SQL / Data HTTP API"),
    )
)]
pub struct ApiDoc;

/// 声明两种认证方式（不在全局强制，因为 `/healthz` 等端点匿名可访问）。
struct SecuritySchemes;

impl Modify for SecuritySchemes {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
            components.add_security_scheme(
                "api_token",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("x-api-token"))),
            );
        }
    }
}

/// 生成 OpenAPI 文档（`--dump-openapi` 与 `/api/v1/openapi.json` 共用）。
#[must_use]
pub fn api_doc() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

// ==================================================================== 长操作

/// 长操作提交规格。
#[derive(Debug, Clone)]
pub struct LongOperationSpec {
    /// `operations.kind`（必须属于 `catalog::OPERATION_KINDS`）。
    pub kind: &'static str,
    /// `jobs.kind`（内部执行载体）。
    pub job_kind: &'static str,
    /// job 负载（本函数会补上 `operation_id`）。
    pub payload: serde_json::Value,
    /// 关联数据库。
    pub database_id: Option<DatabaseId>,
    /// 关联 Worker。
    pub worker_id: Option<WorkerId>,
    /// 优先级（越小越先执行）。
    pub priority: i32,
}

/// `prepare` 的返回值：长操作所需的资源在**幂等判定之后**才被创建。
#[derive(Debug, Clone, Default)]
pub struct PreparedOperation {
    /// 关联数据库（创建场景下就是刚建出来的那个）。
    pub database_id: Option<DatabaseId>,
    /// 合并进 job payload 的额外字段（例如 target_worker_id / snapshot_id）。
    pub extra: serde_json::Value,
}

/// 提交一次长操作：幂等判定 -> (`prepare` 建资源) -> 建 operation -> 投递 job -> 202。
///
/// 幂等语义（架构 §16「相同 Idempotency-Key 重复提交 100 次只产生 1 个 Operation」）：
/// - `First`：用 Catalog 预分配的 operation_id，先执行 `prepare`（创建资源），
///   再建 Operation、投递 job，最后把 202 响应体写进幂等记录让重放返回同一份结果；
/// - `Replay`：直接回放首次的响应体（`replayed = true`），**不执行 `prepare`** ——
///   这正是「重复提交 100 次只创建 1 个 DB / 1 个 operation」的实现方式；
/// - `Conflict`：同一个 key 换了请求体 -> `IDEMPOTENCY_CONFLICT`。
///
/// `prepare` 放在幂等判定之后、operation 落库之前：先建 operation 再建资源的话，
/// 资源创建失败会留下一条指向不存在资源的 operation。
///
/// # Errors
/// - `INVALID_ARGUMENT`：`Idempotency-Key` 头非法；
/// - `IDEMPOTENCY_CONFLICT`：同 key 不同请求；
/// - 其余错误透传（任务没进队列就不能声称已受理）。
// 参数偏多是因为它要一次性收齐幂等判定所需的全部请求要素（headers / method / path / body）；
// 拆成结构体会让每个调用点都多一层装配噪音。
#[allow(clippy::too_many_arguments)]
pub async fn submit_long_operation<F, Fut>(
    state: &AppState,
    principal: &Principal,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    body: &[u8],
    spec: LongOperationSpec,
    prepare: F,
) -> ApiResult<dto::OperationAccepted>
where
    F: FnOnce(OperationId) -> Fut,
    Fut: std::future::Future<Output = ApiResult<PreparedOperation>>,
{
    if let crate::deployment::Deployment::Simple(local) = &state.deployment {
        return local
            .submit(principal, headers, method, path, body, spec, None)
            .await;
    }
    let catalog = &state.distributed()?.catalog;
    let key = extract_idempotency_key(headers).map_err(ApiError::invalid_argument)?;
    let fingerprint = request_fingerprint(method, path, body);

    let (operation_id, replayed) = match key.as_deref() {
        Some(key) => match catalog.begin_idempotent(key, &fingerprint).await.api()? {
            IdempotencyOutcome::First { operation_id } => (operation_id, false),
            IdempotencyOutcome::Replay { body, .. } => {
                // 回放：首次请求的 202 响应体就是权威答案，不再产生任何副作用。
                let mut accepted: dto::OperationAccepted = serde_json::from_value(body)
                    .unwrap_or_else(|err| {
                        tracing::warn!(error = %err, "幂等记录中的响应体无法解析，回退为通用响应");
                        dto::OperationAccepted {
                            operation_id: None,
                            state: "SUCCEEDED".to_string(),
                            database_id: None,
                            replayed: true,
                        }
                    });
                accepted.replayed = true;
                return Ok(accepted);
            }
            IdempotencyOutcome::Conflict => {
                return Err(ApiError::new(
                    ErrorCode::IdempotencyConflict,
                    "相同 Idempotency-Key 已经用于不同的请求（method/path/body 不一致）",
                )
                .with_detail(serde_json::json!({ "idempotency_key": key })));
            }
        },
        None => (OperationId::new_v7(), false),
    };

    // 资源创建（仅首次请求执行）：失败直接返回，不留下悬空的 operation。
    let prepared = prepare(operation_id).await?;
    let database_id = prepared.database_id.or(spec.database_id);

    catalog
        .create_operation_with_id(
            operation_id,
            spec.kind,
            database_id,
            spec.worker_id.clone(),
            Some(principal.user_id),
            key.as_deref(),
        )
        .await
        .api()?;

    let mut payload = match (spec.payload, prepared.extra) {
        (serde_json::Value::Object(base), serde_json::Value::Object(extra)) => {
            let mut merged = base;
            merged.extend(extra);
            serde_json::Value::Object(merged)
        }
        (base, serde_json::Value::Object(extra)) => {
            let mut merged = extra;
            merged.insert("request".to_string(), base);
            serde_json::Value::Object(merged)
        }
        (base, _) => base,
    };
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "operation_id".to_string(),
            serde_json::Value::String(operation_id.to_string()),
        );
    }
    // job 的幂等键沿用请求的 Idempotency-Key：即便进程在两次投递之间崩溃，
    // 唯一索引也能保证同 key 只产生一个 job。
    state
        .jobs
        .submit(
            spec.job_kind,
            payload,
            spec.priority,
            key.as_deref().map(|k| format!("op:{k}")).as_deref(),
        )
        .await?;

    let accepted = dto::OperationAccepted {
        operation_id: Some(operation_id.to_string()),
        state: "PENDING".to_string(),
        database_id: database_id.map(|id| id.to_string()),
        replayed,
    };

    if let Some(key) = key.as_deref() {
        // 写失败不影响受理结果（operation 已经存在），但会导致重放拿不到原响应。
        if let Err(err) = catalog
            .complete_idempotent(
                key,
                202,
                serde_json::to_value(&accepted).unwrap_or_default(),
            )
            .await
        {
            tracing::warn!(
                idempotency_key = key,
                error = %err.message,
                "写入幂等响应体失败（重放将退回通用响应）"
            );
        }
    }

    Ok(accepted)
}

/// 不需要在幂等判定后创建资源的场景（绝大多数接口）。
///
/// # Errors
/// 同 [`submit_long_operation`]。
pub async fn submit_simple_operation(
    state: &AppState,
    principal: &Principal,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    body: &[u8],
    spec: LongOperationSpec,
) -> ApiResult<dto::OperationAccepted> {
    submit_long_operation(
        state,
        principal,
        headers,
        method,
        path,
        body,
        spec,
        |_| async { Ok(PreparedOperation::default()) },
    )
    .await
}

/// 无副作用路径的统一回执（目标状态已达成时使用，例如「已在服务」的 start 请求）。
#[must_use]
pub fn noop_accepted(database_id: DatabaseId, state: &str) -> dto::OperationAccepted {
    dto::OperationAccepted {
        operation_id: None,
        state: state.to_string(),
        database_id: Some(database_id.to_string()),
        replayed: false,
    }
}

// ==================================================================== 审计

/// 记录一次 DBA 写操作的结果（成功与失败都必须调用）。
///
/// 审计写失败**不能**让业务请求失败：审计是旁路，业务状态已经变了，
/// 把成功改判为失败只会让调用方重试、造成更严重的重复副作用。失败会打 ERROR 日志，
/// 由告警链路兜底。
pub async fn audit<T>(
    state: &AppState,
    principal: &Principal,
    action: &str,
    target: (&str, &str),
    database_id: Option<DatabaseId>,
    outcome: &ApiResult<T>,
) {
    let entry = match outcome {
        Ok(_) => {
            let mut entry = catalog::AuditEntry::success(action);
            entry.actor_id = Some(principal.user_id);
            entry.actor_name = principal.username.clone();
            entry
        }
        Err(err) => {
            let mut entry = catalog::AuditEntry::failure(action, err.code());
            entry.actor_id = Some(principal.user_id);
            entry.actor_name = principal.username.clone();
            entry.error_code = Some(err.code().as_str().to_string());
            entry.detail = serde_json::json!({ "message": err.to_string() });
            entry
        }
    };

    let entry = entry
        .with_target(target.0, target.1)
        .with_request_id(current_request_id());
    let entry = match (principal.tenant_id, database_id) {
        (Some(tenant), Some(db)) => entry.with_tenant(tenant).with_database(db),
        (Some(tenant), None) => entry.with_tenant(tenant),
        (None, Some(db)) => entry.with_database(db),
        (None, None) => entry,
    };
    let entry = match current_source_ip() {
        Some(ip) => entry.with_source_ip(ip),
        None => entry,
    };

    if let Err(err) = state.catalog.append_audit(entry).await {
        tracing::error!(
            action,
            code = err.code.as_str(),
            message = %err.message,
            "写入审计日志失败（业务已提交，不因此回滚）"
        );
    }
}

// ==================================================================== 解析辅助

/// 解析数据库 ID 路径参数。
///
/// # Errors
/// 非 UUID 返回 `INVALID_ARGUMENT`。
pub fn db_id(raw: &str) -> ApiResult<DatabaseId> {
    dto::parse_database_id(raw).map_err(ApiError::from)
}

/// 解析操作 ID 路径参数。
///
/// # Errors
/// 非 UUID 返回 `INVALID_ARGUMENT`。
pub fn operation_id(raw: &str) -> ApiResult<OperationId> {
    dto::parse_operation_id(raw).map_err(ApiError::from)
}

/// 解析 Worker ID 路径参数。
///
/// # Errors
/// 空串返回 `INVALID_ARGUMENT`。
pub fn worker_id(raw: &str) -> ApiResult<WorkerId> {
    dto::parse_worker_id(raw).map_err(ApiError::from)
}

/// 把任意 [`PlatformError`] 转成 HTTP 错误。
#[must_use]
pub fn to_api_error(error: PlatformError) -> ApiError {
    ApiError::from(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_contains_all_frozen_public_routes() {
        let doc = api_doc();
        let paths = doc.paths.paths;
        assert!(paths.len() >= 30, "OpenAPI 路径数量异常: {}", paths.len());

        // 架构 §17.4 冻结的出口路径必须全部出现。
        for required in [
            "/api/v1/databases",
            "/api/v1/databases/{db_id}",
            "/api/v1/databases/{db_id}/start",
            "/api/v1/databases/{db_id}/stop",
            "/api/v1/databases/{db_id}/restart",
            "/api/v1/databases/{db_id}/move",
            "/api/v1/databases/{db_id}/snapshot",
            "/api/v1/databases/{db_id}/backup",
            "/api/v1/databases/{db_id}/restore",
            "/api/v1/operations",
            "/api/v1/operations/{operation_id}",
            "/api/v1/workers",
            "/api/v1/workers/{worker_id}",
            "/api/v1/workers/{worker_id}/drain",
            "/api/v1/snapshots",
            "/api/v1/audit",
            "/api/v1/tokens",
            "/api/v1/tokens/{token_id}",
            "/api/v1/panel/preferences/{key}",
            "/api/v1/saved-queries",
            "/api/v1/slow-queries",
            "/api/v1/openapi.json",
            "/data/v1/databases/{db_id}/query",
            "/data/v1/databases/{db_id}/batch",
            "/data/v1/databases/{db_id}/sessions",
            "/data/v1/sessions/{session_id}/query",
            "/data/v1/sessions/{session_id}",
            "/healthz",
            "/readyz",
            "/metrics",
        ] {
            assert!(paths.contains_key(required), "OpenAPI 缺少路径 {required}");
        }
    }

    #[test]
    fn openapi_declares_both_auth_schemes() {
        let doc = api_doc();
        let components = doc.components.expect("组件段必须存在");
        let schemes = components.security_schemes;
        assert!(schemes.contains_key("bearer"));
        assert!(schemes.contains_key("api_token"));
    }

    #[test]
    fn openapi_json_is_serializable_and_versioned() {
        let json = serde_json::to_value(api_doc()).expect("OpenAPI 必须可序列化");
        assert_eq!(json["openapi"], "3.1.0");
        assert!(json["info"]["title"]
            .as_str()
            .unwrap_or("")
            .contains("Server Plane"));
    }

    #[test]
    fn id_parsing_reports_invalid_argument() {
        let err = db_id("not-a-uuid").expect_err("非法 UUID 必须报错");
        assert_eq!(err.code(), ErrorCode::InvalidArgument);
        assert_eq!(err.code().http_status(), 400);

        let err = worker_id("   ").expect_err("空 worker id 必须报错");
        assert_eq!(err.code(), ErrorCode::InvalidArgument);
    }
}
