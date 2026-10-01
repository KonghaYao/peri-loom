//! 单次查询与批量执行（架构 §17.4）。
//!
//! 权限取舍：**数据面一律要求 `db:write`**。平台不解析 SQL，无法可靠区分
//! `SELECT` 与 `WITH ... DELETE`，若对疑似只读语句放行 `db:read`，只读主体就能借
//! `/query` 完成写入（越权）。宁可让只读用户走 Panel 的元数据接口，也不留这条旁路。

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap};
use axum::response::Response;
use axum::Json;

use super::stream;
use crate::api::{db_id, dto};
use crate::auth::{permission, Principal};
use crate::error::{ApiError, ApiResult};
use crate::middleware::current_request_id;
use crate::router::StreamTarget;
use crate::state::AppState;

/// 客户端是否显式要求 NDJSON（`Accept: application/x-ndjson`）。
pub(super) fn wants_ndjson(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().starts_with(stream::NDJSON_CONTENT_TYPE))
        })
        .unwrap_or(false)
}

/// JSON 绑定参数 -> proto 值（任一非法即整体拒绝，避免「部分绑定」的隐蔽语义）。
pub(super) fn proto_params(params: &[serde_json::Value]) -> ApiResult<Vec<protocol::data::Value>> {
    dto::sql_values_from_json(params).map_err(ApiError::invalid_argument)
}

/// 单次查询。
///
/// 出口形态由 `Accept` 与结果集体量共同决定：内联 JSON，或 NDJSON 流。
#[utoipa::path(
    post,
    path = "/data/v1/databases/{db_id}/query",
    tag = "data",
    params(("db_id" = String, Path, description = "数据库 ID")),
    request_body = dto::QueryRequest,
    responses(
        (status = 200, description = "查询结果（内联 JSON；Accept: application/x-ndjson 或超过内联上限时为 NDJSON 流）", body = dto::QueryResponse),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope),
        (status = 409, description = "数据库当前状态不可服务", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn query_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<dto::QueryRequest>,
) -> ApiResult<Response> {
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    crate::api::authorize_database(&state, &principal, database_id).await?;
    if request.sql.trim().is_empty() {
        return Err(ApiError::invalid_argument("sql 不能为空"));
    }
    let params = proto_params(&request.params)?;

    stream::execute(
        state.execution.as_ref(),
        state.config.inline_result_limit_bytes,
        database_id,
        StreamTarget::Stateless {
            sql: request.sql,
            params,
        },
        &current_request_id(),
        None,
        wants_ndjson(&headers),
    )
    .await
}

/// 批量执行（同请求内多语句，`atomic` 决定是否整体包一个事务）。
#[utoipa::path(
    post,
    path = "/data/v1/databases/{db_id}/batch",
    tag = "data",
    params(("db_id" = String, Path, description = "数据库 ID")),
    request_body = dto::BatchRequest,
    responses(
        (status = 200, description = "每条语句的结果集", body = dto::BatchResponse),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn batch_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    Json(request): Json<dto::BatchRequest>,
) -> ApiResult<Json<dto::BatchResponse>> {
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    crate::api::authorize_database(&state, &principal, database_id).await?;
    if request.statements.is_empty() {
        return Err(ApiError::invalid_argument("statements 不能为空"));
    }
    let atomic = request.atomic.unwrap_or(true);

    let mut statements = Vec::with_capacity(request.statements.len());
    for (index, statement) in request.statements.iter().enumerate() {
        if statement.sql.trim().is_empty() {
            return Err(ApiError::invalid_argument(format!(
                "statements[{index}].sql 不能为空"
            )));
        }
        let params = proto_params(&statement.params)
            .map_err(|err| ApiError::invalid_argument(format!("statements[{index}]: {err}")))?;
        statements.push((statement.sql.clone(), params));
    }

    let request_id = current_request_id();
    let response = state
        .execution
        .batch(database_id, statements, atomic, &request_id)
        .await?;

    let results = response
        .results
        .into_iter()
        .map(|result| dto::ResultSetView::from_result_set(&domain::value::ResultSet::from(result)))
        .collect();

    Ok(Json(dto::BatchResponse {
        results,
        wal_lsn: state.execution.remote_lsn().then_some(response.wal_lsn),
        elapsed_micros: response.elapsed_micros,
        request_id,
    }))
}
