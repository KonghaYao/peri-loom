//! `/api/v1/*` Management / DBA REST 出口（架构 §17.4）。
//!
//! 三条贯穿全模块的约定：
//! 1. **长操作一律 202**：`{operation_id, state}`，执行由 `background` 的 Job Runner 完成；
//!    目标状态已达成时返回 `operation_id = null` 的幂等回执（[`dto::OperationAccepted`] 的语义）。
//! 2. **写操作 100% 审计**：每个副作用都经 [`crate::api::audit`]，成功与失败两条路径都留痕。
//! 3. **`Idempotency-Key` 由 `api::submit_*_operation` 统一处理**，模块内不各写一套。
//!
//! 子模块按资源切分（databases / operations / workers / observability / panel），
//! 但**函数名与 `api::mod` 的 OpenAPI `paths(...)` 逐一对齐**，因此这里全部重导出。

mod databases;
mod observability;
mod operations;
mod panel;
mod workers;

// 用 glob 重导出而不是逐个列出：`#[utoipa::path]` 生成的 `__path_*` 标记类型与函数
// 同模块，逐个重导出会让 `management::__path_create_database` 找不到（api::mod 的
// OpenAPI `paths(...)` 正是按 `management::<fn>` 展开的）。
pub use databases::*;
pub use observability::*;
pub use operations::*;
pub use panel::*;
pub use workers::*;

use axum::http::{Method, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use domain::ids::{DatabaseId, TenantId};
use domain::records::DatabaseRecord;

use crate::api::{audit, dto};
use crate::auth::Principal;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;

/// 长操作 job 的缺省优先级。
///
/// 管理面操作的延迟预算是「分钟级」，不需要抢占心跳 / 驱逐这类后台任务的即时性；
/// 用同一个常量也避免每个接口各拍一个数字。
pub(crate) const DEFAULT_JOB_PRIORITY: i32 = 100;

/// Management 路由表。
pub fn routes() -> Router<AppState> {
    Router::new()
        // ---- databases
        .route(
            "/api/v1/databases",
            get(list_databases).post(create_database),
        )
        .route(
            "/api/v1/databases/{db_id}",
            get(get_database).delete(delete_database),
        )
        .route("/api/v1/databases/{db_id}/start", post(start_database))
        .route("/api/v1/databases/{db_id}/stop", post(stop_database))
        .route("/api/v1/databases/{db_id}/restart", post(restart_database))
        .route("/api/v1/databases/{db_id}/move", post(move_database))
        .route(
            "/api/v1/databases/{db_id}/snapshot",
            post(snapshot_database),
        )
        .route("/api/v1/databases/{db_id}/backup", post(backup_database))
        .route("/api/v1/databases/{db_id}/restore", post(restore_database))
        // ---- operations
        .route("/api/v1/operations", get(list_operations))
        .route("/api/v1/operations/{operation_id}", get(get_operation))
        // ---- workers
        .route("/api/v1/workers", get(list_workers))
        .route("/api/v1/workers/{worker_id}", get(get_worker))
        .route("/api/v1/workers/{worker_id}/drain", post(drain_worker))
        // ---- snapshots / audit
        .route("/api/v1/snapshots", get(list_snapshots))
        .route("/api/v1/audit", get(list_audit))
        // ---- tokens
        .route("/api/v1/tokens", get(list_tokens).post(create_token))
        .route(
            "/api/v1/tokens/{token_id}",
            axum::routing::delete(revoke_token),
        )
        // ---- panel
        .route(
            "/api/v1/panel/preferences/{key}",
            get(get_preference).put(put_preference),
        )
        .route(
            "/api/v1/saved-queries",
            get(list_saved_queries).post(create_saved_query),
        )
        .route(
            "/api/v1/saved-queries/{query_id}",
            axum::routing::delete(delete_saved_query),
        )
        .route("/api/v1/slow-queries", get(list_slow_queries))
}

/// 长操作收尾：写审计 + 返回 202。
///
/// 审计在**构造响应之前**完成，且无论成功失败都调用：审计是旁路，不改变业务结果
/// （`api::audit` 内部对写失败只记 ERROR 日志）。
pub(crate) async fn accepted(
    state: &AppState,
    principal: &Principal,
    action: &str,
    target: (&str, &str),
    database_id: Option<DatabaseId>,
    outcome: ApiResult<dto::OperationAccepted>,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    audit(state, principal, action, target, database_id, &outcome).await;
    outcome.map(|body| (StatusCode::ACCEPTED, Json(body)))
}

/// 读取数据库记录，并把「已软删除」当成不存在。
///
/// 列表接口默认不返回已删除项，详情 / 变更接口也必须一致：否则删除后的 DB 仍能被
/// start / snapshot，产生一堆指向墓碑资源的 operation。
pub(crate) async fn load_database(
    state: &AppState,
    principal: &Principal,
    database_id: DatabaseId,
) -> ApiResult<DatabaseRecord> {
    crate::api::authorize_database(state, principal, database_id).await
}

/// 非数据库资源的 404。
///
/// `domain::error::ErrorCode` 里 404 语义的码只有 `DB_NOT_FOUND`（还有事务 / 会话专用的
/// 几个），非 DB 资源（偏好 / Saved SQL / Token）复用它，换取「404 状态码 + 冻结错误体」
/// 两条契约同时成立；新增通用 `NOT_FOUND` 码属于 proto 契约变更，不在这里私自扩大。
pub(crate) fn not_found(resource: &str, id: &str) -> ApiError {
    ApiError::not_found(format!("{resource} '{id}' 不存在"))
}

/// 解析租户 ID。
///
/// # Errors
/// 非 UUID 返回 `INVALID_ARGUMENT`。
pub(crate) fn parse_tenant_id(raw: &str) -> ApiResult<TenantId> {
    raw.trim().parse::<TenantId>().map_err(|err| {
        ApiError::invalid_argument(format!("tenant_id '{raw}' 不是合法 UUID: {err}"))
    })
}

/// 幂等指纹 / 审计使用的规范化请求体字节。
///
/// 用「反序列化后再序列化」的结果而不是原始 body：`Json` 提取器已经把 body 读走，
/// 而结构体字段顺序固定，因此同 key 的同语义请求必然得到同一指纹（字段顺序无关）。
pub(crate) fn stable_body_bytes<T: serde::Serialize>(body: &T) -> Vec<u8> {
    serde_json::to_vec(body).unwrap_or_default()
}

/// 空请求体的指纹字节。
pub(crate) fn empty_body() -> &'static [u8] {
    b""
}

/// 固定方法 + 实际请求路径的 `Idempotency-Key` 指纹输入。
///
/// 路径必须用**真实 URI**而不是模板：不同 DB 的 `/start` 若共用模板路径，
/// 同一个 key 会跨库串味（第二次请求拿到第一次的 operation）。
pub(crate) fn post_method() -> &'static Method {
    &Method::POST
}
