//! 只读观测端点：快照列表与审计日志（架构 §17.4 / §17.11）。
//!
//! 两者都是「读多写少、必须可分页」的表，因此统一夹取 limit（缺省 50，上限 500），
//! 避免一次请求把整张表读进内存。

use axum::Json;
use axum::extract::{Query, State};
use catalog::AuditFilter;
use domain::ids::{DatabaseId, TenantId, UserId};
use serde::Deserialize;

use crate::api::{db_id, dto};
use crate::auth::{permission, Principal};
use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::state::AppState;

/// 列表缺省条数。
const DEFAULT_LIST_LIMIT: i64 = 50;
/// 单次列表返回上限。
const MAX_LIST_LIMIT: i64 = 500;

/// 夹取 limit。
fn clamp_limit(raw: Option<i64>, default: i64) -> i64 {
    raw.unwrap_or(default).clamp(1, MAX_LIST_LIMIT)
}

/// 快照列表查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct SnapshotListParams {
    /// 数据库 ID（必填：快照按库存储，全表扫描没有意义）。
    pub database_id: String,
    /// 返回上限，缺省 50，夹取到 1..=500。
    #[serde(default)]
    pub limit: Option<i64>,
}

/// 审计日志查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct AuditListParams {
    /// 操作者用户 ID。
    #[serde(default)]
    pub actor_id: Option<String>,
    /// 数据库 ID。
    #[serde(default)]
    pub database_id: Option<String>,
    /// 租户 ID。
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// 动作精确匹配（例如 `DB_START`）。
    #[serde(default)]
    pub action: Option<String>,
    /// 结果过滤：SUCCESS / FAILURE。
    #[serde(default)]
    pub result: Option<String>,
    /// 每页条数，缺省 50。
    #[serde(default)]
    pub limit: Option<i64>,
    /// 偏移量，缺省 0。
    #[serde(default)]
    pub offset: Option<i64>,
}

/// 快照列表（按创建时间倒序）。
#[utoipa::path(
    get,
    path = "/api/v1/snapshots",
    tag = "snapshots",
    params(SnapshotListParams),
    responses(
        (status = 200, description = "快照列表", body = Vec<dto::SnapshotView>),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn list_snapshots(
    State(state): State<AppState>,
    principal: Principal,
    Query(params): Query<SnapshotListParams>,
) -> ApiResult<Json<Vec<dto::SnapshotView>>> {
    principal.require(permission::DB_READ)?;
    let database_id = db_id(&params.database_id)?;
    crate::api::authorize_database(&state, &principal, database_id).await?;
    let snapshots = state
        .catalog
        .list_snapshots(database_id, clamp_limit(params.limit, DEFAULT_LIST_LIMIT))
        .await
        .api()?;
    Ok(Json(
        snapshots
            .iter()
            .map(|record| dto::SnapshotView::for_deployment(record, state.execution.remote_lsn()))
            .collect(),
    ))
}

/// 审计日志（按时间倒序）。
///
/// 需要 `audit:read` 而不是 `db:read`：审计里含跨库的操作者、来源 IP 与失败详情。
#[utoipa::path(
    get,
    path = "/api/v1/audit",
    tag = "audit",
    params(AuditListParams),
    responses(
        (status = 200, description = "审计日志", body = dto::Page<dto::AuditView>),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn list_audit(
    State(state): State<AppState>,
    principal: Principal,
    Query(params): Query<AuditListParams>,
) -> ApiResult<Json<dto::Page<dto::AuditView>>> {
    principal.require_control_plane()?;
    principal.require(permission::AUDIT_READ)?;
    let limit = clamp_limit(params.limit, DEFAULT_LIST_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);

    let filter = AuditFilter {
        actor_id: params
            .actor_id
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
            .map(parse_uuid::<UserId>)
            .transpose()?,
        database_id: params
            .database_id
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
            .map(parse_uuid::<DatabaseId>)
            .transpose()?,
        tenant_id: params
            .tenant_id
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
            .map(parse_uuid::<TenantId>)
            .transpose()?,
        action: params.action.clone().filter(|raw| !raw.trim().is_empty()),
        result: parse_audit_result(params.result.as_deref())?,
    };

    let records = state
        .catalog
        .list_audit(limit, offset, filter)
        .await
        .api()?;
    Ok(Json(dto::Page {
        items: records.iter().map(dto::AuditView::from).collect(),
        limit,
        offset,
    }))
}

/// 解析 UUID 类路径 / 查询参数。
fn parse_uuid<T: std::str::FromStr>(raw: &str) -> ApiResult<T> {
    raw.trim()
        .parse::<T>()
        .map_err(|_| ApiError::invalid_argument(format!("'{raw}' 不是合法 UUID")))
}

/// 校验审计结果过滤值。
fn parse_audit_result(raw: Option<&str>) -> ApiResult<Option<String>> {
    match raw.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(value) if catalog::AUDIT_RESULTS.contains(&value) => Ok(Some(value.to_string())),
        Some(value) => Err(ApiError::invalid_argument(format!(
            "result '{value}' 非法（只能是 SUCCESS 或 FAILURE）"
        ))),
    }
}
