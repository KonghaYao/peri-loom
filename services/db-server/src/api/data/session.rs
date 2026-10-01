//! 显式会话端点：open / query / close（架构 §13）。
//!
//! 会话**不写 Catalog**：它只是 Worker 本地的连接上下文（见
//! [`crate::state::SessionBinding`]）。Server 只保存「谁 pin 在哪个 Worker / 哪个
//! epoch」，因此：
//!
//! - Server 重启会丢会话 -> 客户端收到 `SESSION_NOT_FOUND` 重新开会话（可接受降级）；
//! - epoch 变了（发生过 failover）-> 会话不可恢复，返回 `SESSION_LOST`，而不是把
//!   语句重试到新 Owner 上（那会在没有事务上下文的新进程里执行，语义完全变了）。

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::Json;
use chrono::Utc;
use domain::error::ErrorCode;

use super::stream;
use crate::api::{db_id, dto};
use crate::auth::{permission, Principal};
use crate::error::{ApiError, ApiResult};
use crate::middleware::current_request_id;
use crate::router::StreamTarget;
use crate::state::{AppState, SessionBinding};

/// 打开显式会话。
#[utoipa::path(
    post,
    path = "/data/v1/databases/{db_id}/sessions",
    tag = "data",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 201, description = "会话已建立", body = dto::SessionOpened),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope),
        (status = 409, description = "数据库当前状态不可服务", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn open_session(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
) -> ApiResult<Json<dto::SessionOpened>> {
    // 会话内可以执行任意语句（含 DML），与 /query 用同一档权限。
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    let (binding, expires_at_unix_ms) = open_binding(&state, database_id).await?;

    Ok(Json(dto::SessionOpened {
        session_id: binding.session_id,
        worker_id: binding.worker_id.as_ref().map(ToString::to_string),
        database_id: database_id.to_string(),
        expires_at_unix_ms,
        request_id: current_request_id(),
    }))
}

/// 建立一条显式会话并在本地登记，返回绑定与 Worker 上报的过期时间（Unix 毫秒）。
///
/// `/data/v1/databases/{id}/sessions` 与 Hrana 兼容层（`/db/{id}/v2/pipeline` 的 baton）
/// 共用这段序列：`call_data` 解析路由（含透明 Wake 与 stale-route 重试一次）
/// -> `OpenSession` -> 登记本地绑定。两处各写一遍必然漂移，因此收敛到这里。
pub(crate) async fn open_binding(
    state: &AppState,
    database_id: domain::ids::DatabaseId,
) -> ApiResult<(SessionBinding, u64)> {
    let binding = state
        .execution
        .open_session(
            database_id,
            &current_request_id(),
            state.config.session_idle_timeout_ms,
        )
        .await?;
    let expires = binding.expires_at.timestamp_millis() as u64;
    state
        .sessions
        .insert(binding.session_id.clone(), binding.clone());
    Ok((binding, expires))
}

/// 会话内查询（结果集出口形态与无会话查询完全一致）。
#[utoipa::path(
    post,
    path = "/data/v1/sessions/{session_id}/query",
    tag = "data",
    params(("session_id" = String, Path, description = "会话 ID")),
    request_body = dto::QueryRequest,
    responses(
        (status = 200, description = "查询结果（内联 JSON 或 NDJSON 流）", body = dto::QueryResponse),
        (status = 404, description = "会话不存在或已过期", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn session_query(
    State(state): State<AppState>,
    principal: Principal,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<dto::QueryRequest>,
) -> ApiResult<Response> {
    principal.require(permission::DB_WRITE)?;
    if request.sql.trim().is_empty() {
        return Err(ApiError::invalid_argument("sql 不能为空"));
    }
    let binding = live_binding(&state, &session_id)?;
    let params = super::query::proto_params(&request.params)?;

    state.execution.validate_session(&binding).await?;

    stream::execute(
        state.execution.as_ref(),
        state.config.inline_result_limit_bytes,
        binding.database_id,
        StreamTarget::Session {
            session_id,
            sql: request.sql,
            params,
        },
        &current_request_id(),
        None,
        super::query::wants_ndjson(&headers),
    )
    .await
}

/// 取会话绑定，并顺手清理已过期会话（惰性 GC，避免为单个会话起定时任务）。
pub(crate) fn live_binding(state: &AppState, session_id: &str) -> ApiResult<SessionBinding> {
    let binding = state
        .sessions
        .get(session_id)
        .ok_or_else(|| ApiError::new(ErrorCode::SessionNotFound, session_not_found(session_id)))?;
    if binding.is_expired_at(Utc::now()) {
        state.sessions.remove(session_id);
        return Err(ApiError::new(
            ErrorCode::SessionIdleTimeout,
            format!("会话 {session_id} 已空闲超时"),
        ));
    }
    state
        .sessions
        .touch(session_id, state.config.session_idle_timeout_ms);
    Ok(binding)
}

fn session_not_found(session_id: &str) -> String {
    // Server 重启会丢本地会话注册表，提示里说清楚「重开会话」而不是「重试」。
    format!("会话 {session_id} 不存在（Server 重启或会话已过期，请重新打开会话）")
}

/// 关闭会话。
#[utoipa::path(
    delete,
    path = "/data/v1/sessions/{session_id}",
    tag = "data",
    params(("session_id" = String, Path, description = "会话 ID")),
    responses(
        (status = 200, description = "会话已关闭（known=false 表示本地注册表里已不存在）", body = dto::SessionClosed)
    )
)]
pub async fn close_session(
    State(state): State<AppState>,
    principal: Principal,
    Path(session_id): Path<String>,
) -> ApiResult<Json<dto::SessionClosed>> {
    principal.require(permission::DB_READ)?;
    let binding = state.sessions.remove(&session_id);
    let known = binding.is_some();

    if let Some(binding) = binding {
        // 通知 Worker 立即释放连接上下文；失败不致命（Worker 侧还有空闲计时兜底），
        // 但要让调用方在日志里看得到，否则「关闭变慢」会变成无头案。
        if let Err(err) = state.execution.close_session(&binding).await {
            tracing::warn!(
                session_id,
                worker_id = ?binding.worker_id,
                code = err.code().as_str(),
                "通知 Worker 关闭会话失败（依赖 Worker 侧空闲回收）"
            );
        }
    }

    Ok(Json(dto::SessionClosed { session_id, known }))
}
