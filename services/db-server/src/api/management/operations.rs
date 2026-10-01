//! 长操作跟踪端点：`GET /api/v1/operations`、`GET /api/v1/operations/{operation_id}`。
//!
//! 这两个接口是「202 异步语义」的另一半：客户端拿到 `operation_id` 之后唯一的权威
//! 进展来源就是这里（job 的物理形态不对外暴露）。

use axum::Json;
use axum::extract::{Path, Query, State};

use crate::api::{dto, operation_id};
use crate::auth::{permission, Principal};
use crate::error::{ApiResult, PlatformResultExt};
use crate::state::AppState;

/// 长操作详情。
#[utoipa::path(
    get,
    path = "/api/v1/operations/{operation_id}",
    tag = "operations",
    params(("operation_id" = String, Path, description = "操作 ID")),
    responses(
        (status = 200, description = "操作详情", body = dto::OperationView),
        (status = 404, description = "操作不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn get_operation(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_operation_id): Path<String>,
) -> ApiResult<Json<dto::OperationView>> {
    principal.require(permission::DB_READ)?;
    let id = operation_id(&raw_operation_id)?;
    let record = state.catalog.get_operation(id).await.api()?;
    if let Some(database_id) = record.database_id {
        crate::api::authorize_database(&state, &principal, database_id).await?;
    } else {
        principal.require_control_plane()?;
    }
    Ok(Json(dto::OperationView::from(&record)))
}

/// 长操作列表（按创建时间倒序）。
#[utoipa::path(
    get,
    path = "/api/v1/operations",
    tag = "operations",
    responses(
        (status = 200, description = "操作列表", body = dto::Page<dto::OperationView>)
    )
)]
pub async fn list_operations(
    State(state): State<AppState>,
    principal: Principal,
    Query(params): Query<dto::PageParams>,
) -> ApiResult<Json<dto::Page<dto::OperationView>>> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let (limit, offset) = params.normalized();
    let records = state.catalog.list_operations(limit, offset).await.api()?;
    Ok(Json(dto::Page {
        items: records.iter().map(dto::OperationView::from).collect(),
        limit,
        offset,
    }))
}
