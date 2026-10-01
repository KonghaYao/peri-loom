//! Panel 辅助端点：API Token、用户偏好、Saved SQL、慢查询（架构 §17.4 Panel Plane）。
//!
//! 权限边界刻意分成两类：
//! - **平台级资源**（API Token）需要 `token:admin`；
//! - **用户自属资源**（偏好 / Saved SQL）作用域是 `principal.user_id`，任何具备
//!   `db:read` 的登录用户都能管理自己的那一份 —— 读到别人的偏好是越权，
//!   但「读不到 README 级别的 UI 设置」会让人以为接口坏了。

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, HeaderMap, header};
use axum::Json;
use catalog::{NewApiToken, NewSavedQuery};
use domain::ids::TokenId;
use serde::Deserialize;
use uuid::Uuid;

use super::{not_found, parse_tenant_id};
use crate::api::{audit, db_id, dto};
use crate::auth::{permission, Principal};
use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::idempotency::hash_secret;
use crate::state::AppState;

/// 列表缺省条数。
const DEFAULT_LIST_LIMIT: i64 = 50;
/// 单次列表返回上限。
const MAX_LIST_LIMIT: i64 = 500;

/// 偏好键 / Saved SQL 名称这类短标识的长度上限（避免超长 key 撑爆索引与日志）。
const MAX_KEY_LEN: usize = 128;

/// 归一化偏好键。
fn normalize_key(raw: &str) -> ApiResult<String> {
    let key = raw.trim();
    if key.is_empty() {
        return Err(ApiError::invalid_argument("偏好键不能为空"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(ApiError::invalid_argument(format!(
            "偏好键长度不能超过 {MAX_KEY_LEN}"
        )));
    }
    Ok(key.to_string())
}

/// Saved SQL 列表查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct SavedQueryListParams {
    /// 返回上限，缺省 50，夹取到 1..=500。
    #[serde(default)]
    pub limit: Option<i64>,
}

/// 慢查询列表查询参数。
#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct SlowQueryListParams {
    /// 数据库 ID（必填）。
    pub database_id: String,
    /// 返回上限，缺省 50，夹取到 1..=500。
    #[serde(default)]
    pub limit: Option<i64>,
    /// 只返回耗时不低于该值（微秒）的记录，缺省 0。
    #[serde(default)]
    pub min_duration_micros: Option<i64>,
}

// ==================================================================== 偏好

/// 读取当前用户的某个偏好。
#[utoipa::path(
    get,
    path = "/api/v1/panel/preferences/{key}",
    tag = "panel",
    params(("key" = String, Path, description = "偏好键")),
    responses(
        (status = 200, description = "偏好值", body = dto::PreferenceView),
        (status = 404, description = "偏好不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn get_preference(
    State(state): State<AppState>,
    principal: Principal,
    Path(key): Path<String>,
) -> ApiResult<Json<dto::PreferenceView>> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let key = normalize_key(&key)?;
    let value = state
        .catalog
        .get_preference(principal.user_id, &key)
        .await
        .api()?
        .ok_or_else(|| not_found("偏好", &key))?;
    Ok(Json(dto::PreferenceView { key, value }))
}

/// 写入当前用户的某个偏好（幂等覆盖）。
#[utoipa::path(
    put,
    path = "/api/v1/panel/preferences/{key}",
    tag = "panel",
    params(("key" = String, Path, description = "偏好键")),
    request_body = dto::PutPreferenceRequest,
    responses(
        (status = 200, description = "写入后的偏好", body = dto::PreferenceView)
    )
)]
pub async fn put_preference(
    State(state): State<AppState>,
    principal: Principal,
    Path(key): Path<String>,
    Json(request): Json<dto::PutPreferenceRequest>,
) -> ApiResult<Json<dto::PreferenceView>> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let key = normalize_key(&key)?;

    let outcome: ApiResult<dto::PreferenceView> = match state
        .catalog
        .set_preference(principal.user_id, &key, &request.value)
        .await
    {
        Ok(()) => Ok(dto::PreferenceView {
            key: key.clone(),
            value: request.value,
        }),
        Err(err) => Err(err.into()),
    };
    audit(
        &state,
        &principal,
        "PANEL_PREFERENCE_PUT",
        ("preference", &key),
        None,
        &outcome,
    )
    .await;
    outcome.map(Json)
}

// ==================================================================== Saved SQL

/// 当前用户的 Saved SQL 列表。
#[utoipa::path(
    get,
    path = "/api/v1/saved-queries",
    tag = "panel",
    params(SavedQueryListParams),
    responses(
        (status = 200, description = "Saved SQL 列表", body = Vec<dto::SavedQueryView>)
    )
)]
pub async fn list_saved_queries(
    State(state): State<AppState>,
    principal: Principal,
    Query(params): Query<SavedQueryListParams>,
) -> ApiResult<Json<Vec<dto::SavedQueryView>>> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let records = state
        .catalog
        .list_saved_queries(
            principal.user_id,
            params
                .limit
                .unwrap_or(DEFAULT_LIST_LIMIT)
                .clamp(1, MAX_LIST_LIMIT),
        )
        .await
        .api()?;
    Ok(Json(
        records.iter().map(dto::SavedQueryView::from).collect(),
    ))
}

/// 保存一段 SQL。
#[utoipa::path(
    post,
    path = "/api/v1/saved-queries",
    tag = "panel",
    request_body = dto::CreateSavedQueryRequest,
    responses(
        (status = 201, description = "已创建", body = dto::SavedQueryView),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn create_saved_query(
    State(state): State<AppState>,
    principal: Principal,
    Json(request): Json<dto::CreateSavedQueryRequest>,
) -> ApiResult<(StatusCode, Json<dto::SavedQueryView>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let name = request.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError::invalid_argument("Saved SQL 名称不能为空"));
    }
    if request.sql.trim().is_empty() {
        return Err(ApiError::invalid_argument("Saved SQL 内容不能为空"));
    }
    let database_id = match request.database_id.as_deref() {
        Some(raw) => Some(db_id(raw)?),
        None => None,
    };

    let outcome: ApiResult<dto::SavedQueryView> = match state
        .catalog
        .create_saved_query(NewSavedQuery {
            user_id: principal.user_id,
            database_id,
            name: name.clone(),
            sql: request.sql.clone(),
            description: request.description.clone(),
            tags: request.tags.clone(),
        })
        .await
    {
        Ok(record) => Ok(dto::SavedQueryView::from(&record)),
        Err(err) => Err(err.into()),
    };

    let target_id = outcome
        .as_ref()
        .map(|view| view.id.clone())
        .unwrap_or_else(|_| name.clone());
    audit(
        &state,
        &principal,
        "SAVED_QUERY_CREATE",
        ("saved_query", target_id.as_str()),
        database_id,
        &outcome,
    )
    .await;
    outcome.map(|view| (StatusCode::CREATED, Json(view)))
}

/// 删除一条 Saved SQL（只能删自己的）。
#[utoipa::path(
    delete,
    path = "/api/v1/saved-queries/{query_id}",
    tag = "panel",
    params(("query_id" = String, Path, description = "Saved SQL ID")),
    responses(
        (status = 204, description = "已删除"),
        (status = 404, description = "不存在或不属于当前用户", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn delete_saved_query(
    State(state): State<AppState>,
    principal: Principal,
    Path(query_id): Path<String>,
) -> ApiResult<StatusCode> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let id = Uuid::parse_str(query_id.trim())
        .map_err(|_| ApiError::invalid_argument(format!("'{query_id}' 不是合法 UUID")))?;

    let outcome: ApiResult<()> = match state
        .catalog
        .delete_saved_query(id, principal.user_id)
        .await
    {
        // Catalog 按 (id, user_id) 删除：false 表示不存在或不属于当前用户，两者都回 404
        // （区分开等于告诉攻击者「这个 id 存在但归别人」）。
        Ok(true) => Ok(()),
        Ok(false) => Err(not_found("Saved SQL", &query_id)),
        Err(err) => Err(err.into()),
    };
    audit(
        &state,
        &principal,
        "SAVED_QUERY_DELETE",
        ("saved_query", &query_id),
        None,
        &outcome,
    )
    .await;
    outcome.map(|()| StatusCode::NO_CONTENT)
}

// ==================================================================== 慢查询

/// 慢查询列表。
#[utoipa::path(
    get,
    path = "/api/v1/slow-queries",
    tag = "panel",
    params(SlowQueryListParams),
    responses(
        (status = 200, description = "慢查询列表", body = Vec<dto::SlowQueryView>),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn list_slow_queries(
    State(state): State<AppState>,
    principal: Principal,
    Query(params): Query<SlowQueryListParams>,
) -> ApiResult<Json<Vec<dto::SlowQueryView>>> {
    principal.require(permission::DB_READ)?;
    let database_id = db_id(&params.database_id)?;
    crate::api::authorize_database(&state, &principal, database_id).await?;
    let records = state
        .catalog
        .list_slow_queries(
            database_id,
            params
                .limit
                .unwrap_or(DEFAULT_LIST_LIMIT)
                .clamp(1, MAX_LIST_LIMIT),
            params.min_duration_micros.unwrap_or(0).max(0),
        )
        .await
        .api()?;
    Ok(Json(records.iter().map(dto::SlowQueryView::from).collect()))
}

// ==================================================================== API Token

/// 创建 API Token（明文只在本次响应里出现）。
#[utoipa::path(
    post,
    path = "/api/v1/tokens",
    tag = "tokens",
    request_body = dto::CreateTokenRequest,
    responses(
        (status = 201, description = "已创建（明文 token 仅此一次）", body = dto::TokenCreated),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn create_token(
    State(state): State<AppState>,
    principal: Principal,
    Json(request): Json<dto::CreateTokenRequest>,
) -> ApiResult<(StatusCode, HeaderMap, Json<dto::TokenCreated>)> {
    principal.require_token_admin()?;
    let name = request.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError::invalid_argument("Token 名称不能为空"));
    }
    let database_id = db_id(&request.database_id)?;
    let database = crate::api::authorize_database(&state, &principal, database_id).await?;
    let tenant_id = Some(database.tenant_id);
    if let Some(raw) = request.tenant_id.as_deref() {
        if parse_tenant_id(raw)? != database.tenant_id {
            return Err(ApiError::invalid_argument(
                "tenant_id 必须与 database_id 对应数据库的租户一致",
            ));
        }
    }

    // 新 token 默认仅授予数据库读写；显式权限必须是当前 Principal 的权限子集。
    // 空数组通常是 UI 配置错误，拒绝而非意外继承用户角色。
    let requested_permissions = request.permissions.unwrap_or_else(|| {
        vec![
            permission::DB_READ.to_string(),
            permission::DB_WRITE.to_string(),
        ]
    });
    if requested_permissions.is_empty() {
        return Err(ApiError::invalid_argument(
            "Token permissions 不能为空；省略该字段可使用默认的 db:read、db:write",
        ));
    }
    if requested_permissions
        .iter()
        .any(|p| p != permission::DB_READ && p != permission::DB_WRITE)
    {
        return Err(ApiError::invalid_argument(
            "单库 API Token 仅支持 db:read 与 db:write 权限",
        ));
    }
    for requested in &requested_permissions {
        if !principal.can(requested) {
            return Err(ApiError::permission_denied(format!(
                "不能签发当前主体不具备的 Token 权限 '{requested}'"
            ))
            .with_detail(serde_json::json!({ "requested_permission": requested })));
        }
    }

    // 明文只在内存里活到响应写出为止：Catalog 只存哈希。
    let plaintext = crate::auth::generate_api_token();
    let new_token = NewApiToken {
        user_id: principal.user_id,
        name: name.clone(),
        token_hash: hash_secret(&plaintext),
        tenant_id,
        database_id: Some(database_id),
        permissions: serde_json::json!(requested_permissions),
        expires_at: request.expires_at,
    };
    let created = if request.rotate {
        state.catalog.rotate_api_token(new_token).await
    } else {
        state.catalog.create_api_token(new_token).await
    };
    let outcome: ApiResult<dto::TokenCreated> = match created {
        Ok(record) => Ok(dto::TokenCreated {
            id: record.id.to_string(),
            name: record.name.clone(),
            database_id: database_id.to_string(),
            token: plaintext,
            permissions: dto::permissions_from_json(&record.permissions),
            expires_at: record.expires_at,
            created_at: record.created_at,
        }),
        Err(err) => Err(err.into()),
    };

    // 审计里绝不能出现明文 token：这里只记录 id / 名称。
    let target_id = outcome
        .as_ref()
        .map(|created| created.id.clone())
        .unwrap_or_else(|_| name.clone());
    audit(
        &state,
        &principal,
        "TOKEN_CREATE",
        ("token", target_id.as_str()),
        None,
        &outcome,
    )
    .await;
    outcome.map(|created| {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CACHE_CONTROL,
            "no-store".parse().expect("static header"),
        );
        (StatusCode::CREATED, headers, Json(created))
    })
}

/// 当前用户可见的 API Token 列表（不含明文与哈希）。
#[utoipa::path(
    get,
    path = "/api/v1/tokens",
    tag = "tokens",
    responses(
        (status = 200, description = "Token 列表", body = Vec<dto::TokenView>)
    )
)]
pub async fn list_tokens(
    State(state): State<AppState>,
    principal: Principal,
) -> ApiResult<Json<Vec<dto::TokenView>>> {
    principal.require_token_admin()?;
    let records = state
        .catalog
        .list_tokens_for_user(principal.user_id)
        .await
        .api()?;
    let mut visible = Vec::with_capacity(records.len());
    for record in &records {
        if let Some(database_id) = record.database_id {
            if crate::api::authorize_database(&state, &principal, database_id)
                .await
                .is_err()
            {
                continue;
            }
        }
        visible.push(dto::TokenView::from(record));
    }
    Ok(Json(visible))
}

/// 吊销 API Token。
#[utoipa::path(
    delete,
    path = "/api/v1/tokens/{token_id}",
    tag = "tokens",
    params(("token_id" = String, Path, description = "Token ID")),
    responses(
        (status = 204, description = "已吊销"),
        (status = 404, description = "Token 不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn revoke_token(
    State(state): State<AppState>,
    principal: Principal,
    Path(token_id): Path<String>,
) -> ApiResult<StatusCode> {
    principal.require_token_admin()?;
    let id: TokenId = token_id
        .trim()
        .parse()
        .map_err(|_| ApiError::invalid_argument(format!("'{token_id}' 不是合法 UUID")))?;

    let owned = state
        .catalog
        .list_tokens_for_user(principal.user_id)
        .await
        .api()?;
    let Some(record) = owned.iter().find(|record| record.id == id) else {
        return Err(not_found("Token", &token_id));
    };
    if let Some(database_id) = record.database_id {
        crate::api::authorize_database(&state, &principal, database_id).await?;
    }

    let outcome: ApiResult<()> = match state.catalog.revoke_api_token(id).await {
        Ok(true) => Ok(()),
        // 已经吊销过也算成功：DELETE 是幂等的，重复调用不该报错。
        Ok(false) => Err(not_found("Token", &token_id)),
        Err(err) => Err(err.into()),
    };
    audit(
        &state,
        &principal,
        "TOKEN_REVOKE",
        ("token", &token_id),
        None,
        &outcome,
    )
    .await;
    outcome.map(|()| StatusCode::NO_CONTENT)
}
