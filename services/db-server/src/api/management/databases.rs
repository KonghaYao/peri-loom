//! 数据库生命周期端点：create / list / get / delete / start / stop / restart / move /
//! snapshot / backup / restore（架构 §17.4）。
//!
//! 全部变更都走「写 operation + 投递 job + 立刻 202」：HTTP 请求的耗时与 DB 进程
//! 启停、快照上传的耗时解耦，客户端用 `GET /api/v1/operations/{id}` 跟踪进展。

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::Json;
use catalog::{CreateDatabaseParams, DatabaseFilter};
use domain::ids::{DatabaseId, WorkerId};
use domain::lifecycle::LifecycleState;

use super::{
    accepted, load_database, parse_tenant_id, post_method, stable_body_bytes, DEFAULT_JOB_PRIORITY,
};
use crate::api::{
    db_id, dto, noop_accepted, submit_long_operation, submit_simple_operation, LongOperationSpec,
    PreparedOperation,
};
use crate::auth::{permission, Principal};
use crate::background::job_kind;
use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::state::AppState;

/// 解析生命周期状态过滤参数。
fn parse_lifecycle_state(raw: &str) -> ApiResult<LifecycleState> {
    LifecycleState::from_db_str(raw)
        .map_err(|_| ApiError::invalid_argument(format!("state '{raw}' 不是合法的生命周期状态")))
}

/// 列出数据库。
#[utoipa::path(
    get,
    path = "/api/v1/databases",
    tag = "databases",
    responses(
        (status = 200, description = "数据库列表（按 name_prefix / state / tenant 过滤）", body = dto::Page<dto::DatabaseView>),
        (status = 400, description = "过滤参数非法", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn list_databases(
    State(state): State<AppState>,
    principal: Principal,
    Query(params): Query<dto::DatabaseListParams>,
) -> ApiResult<Json<dto::Page<dto::DatabaseView>>> {
    principal.require(permission::DB_READ)?;
    let (limit, offset) = params.page().normalized();

    // API Token 的列表视图只有其绑定库一个条目；JWT 管理用户走常规可筛选列表。
    if let Some(database_id) = principal.database_id {
        let record = crate::api::authorize_database(&state, &principal, database_id).await?;
        return Ok(Json(dto::Page {
            items: if offset == 0 {
                vec![dto::DatabaseView::for_deployment(
                    &record,
                    state.execution.remote_lsn(),
                )]
            } else {
                Vec::new()
            },
            limit,
            offset,
        }));
    }

    let requested_tenant = params
        .tenant_id
        .as_deref()
        .map(parse_tenant_id)
        .transpose()?;
    if !principal.is_superuser
        && principal
            .tenant_id
            .zip(requested_tenant)
            .is_some_and(|(own, requested)| own != requested)
    {
        return Err(ApiError::permission_denied(
            "主体无权列出租户范围之外的数据库",
        ));
    }
    let filter = DatabaseFilter {
        tenant_id: requested_tenant.or(principal.tenant_id),
        worker_id: params
            .worker_id
            .as_deref()
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .map(WorkerId::new),
        state: params
            .state
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
            .map(parse_lifecycle_state)
            .transpose()?,
        name_prefix: params
            .name_prefix
            .clone()
            .filter(|prefix| !prefix.trim().is_empty()),
        include_deleted: params.include_deleted.unwrap_or(false),
        limit: Some(limit),
        offset: Some(offset),
    };

    let records = state.catalog.list_databases(filter).await.api()?;
    Ok(Json(dto::Page {
        items: records
            .iter()
            .map(|record| dto::DatabaseView::for_deployment(record, state.execution.remote_lsn()))
            .collect(),
        limit,
        offset,
    }))
}

/// 创建数据库。
///
/// 资源创建放在 `prepare` 里：它只在**幂等判定通过**之后执行，因此同一个
/// `Idempotency-Key` 重复提交 100 次只会真正建出一个库。
#[utoipa::path(
    post,
    path = "/api/v1/databases",
    tag = "databases",
    request_body = dto::CreateDatabaseRequest,
    responses(
        (status = 202, description = "已受理（202 + operation_id）", body = dto::OperationAccepted),
        (status = 400, description = "参数非法", body = crate::error::ErrorEnvelope),
        (status = 409, description = "同名数据库已存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn create_database(
    State(state): State<AppState>,
    principal: Principal,
    headers: HeaderMap,
    uri: Uri,
    Json(request): Json<dto::CreateDatabaseRequest>,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_WRITE)?;

    let name = request.name.trim().to_string();
    if name.is_empty() {
        return Err(ApiError::invalid_argument("数据库名不能为空"));
    }
    let tenant_id = match request.tenant_id.as_deref() {
        Some(raw) => Some(parse_tenant_id(raw)?),
        None => principal.tenant_id,
    };
    if !principal.is_superuser
        && principal
            .tenant_id
            .zip(tenant_id)
            .is_some_and(|(own, requested)| own != requested)
    {
        return Err(ApiError::permission_denied(
            "不能在主体所属租户之外创建数据库",
        ));
    }

    let params = CreateDatabaseParams {
        id: None,
        tenant_id,
        name: name.clone(),
        // 落地即 COLD：首次访问由冷启动路径拉起，创建时不预占 Worker 资源。
        state: None,
        cpu_milli: request.cpu_milli,
        memory_mib: request.memory_mib,
        fd_limit: request.fd_limit,
        disk_mib: request.disk_mib,
        iops_limit: request.iops_limit,
        priority: request.priority,
        evictable: request.evictable,
        storage_region: request.storage_region.clone(),
        storage_prefix: None,
        engine_version: None,
        schema_version: None,
        labels: request.labels.clone(),
    };
    let spec = LongOperationSpec {
        kind: "CREATE_DB",
        job_kind: job_kind::DB_CREATE,
        payload: serde_json::json!({}),
        // database_id 在 prepare 之后才存在，因此由 PreparedOperation 回填进 job payload。
        database_id: None,
        worker_id: None,
        priority: DEFAULT_JOB_PRIORITY,
    };
    let body = stable_body_bytes(&request);
    let prepare_state = state.clone();

    let outcome = if let crate::deployment::Deployment::Simple(local) = &state.deployment {
        if request.cpu_milli.is_some()
            || request.memory_mib.is_some()
            || request.fd_limit.is_some()
            || request.disk_mib.is_some()
            || request.iops_limit.is_some()
            || request.storage_region.is_some()
        {
            Err(crate::deployment::unsupported())
        } else {
            local
                .submit(
                    &principal,
                    &headers,
                    post_method(),
                    uri.path(),
                    &body,
                    spec,
                    Some(params),
                )
                .await
        }
    } else {
        submit_long_operation(
            &state,
            &principal,
            &headers,
            post_method(),
            uri.path(),
            &body,
            spec,
            |_operation_id| async move {
                let record = prepare_state.catalog.create_database(params).await.api()?;
                Ok(PreparedOperation {
                    database_id: Some(record.id),
                    extra: serde_json::json!({ "database_id": record.id.to_string() }),
                })
            },
        )
        .await
    };

    let database_id = outcome.as_ref().ok().and_then(|accepted| {
        accepted
            .database_id
            .as_deref()
            .and_then(|raw| raw.parse::<DatabaseId>().ok())
    });
    accepted(
        &state,
        &principal,
        "DB_CREATE",
        ("database", &name),
        database_id,
        outcome,
    )
    .await
}

/// 数据库详情。
#[utoipa::path(
    get,
    path = "/api/v1/databases/{db_id}",
    tag = "databases",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 200, description = "数据库详情", body = dto::DatabaseView),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn get_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
) -> ApiResult<Json<dto::DatabaseView>> {
    principal.require(permission::DB_READ)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    Ok(Json(dto::DatabaseView::for_deployment(
        &record,
        state.execution.remote_lsn(),
    )))
}

/// 删除数据库（先停进程，再软删记录）。
#[utoipa::path(
    delete,
    path = "/api/v1/databases/{db_id}",
    tag = "databases",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn delete_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_ADMIN)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    let spec = LongOperationSpec {
        kind: "DELETE_DB",
        job_kind: job_kind::DB_DELETE,
        payload: serde_json::json!({ "database_id": id_text }),
        database_id: Some(database_id),
        worker_id: record.owner_worker_id.clone(),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        &axum::http::Method::DELETE,
        uri.path(),
        super::empty_body(),
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "DB_DELETE",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 启动数据库。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/start",
    tag = "databases",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 202, description = "已受理（已在服务时 operation_id 为 null）", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn start_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    let outcome = if record.state.is_serving() {
        // 已在服务：不产生新 operation。再拉一次只会变成「重启」，与调用方意图不符。
        Ok(noop_accepted(database_id, record.state.to_db_str()))
    } else {
        let spec = LongOperationSpec {
            kind: "START_DB",
            job_kind: job_kind::DB_START,
            payload: serde_json::json!({ "database_id": id_text }),
            database_id: Some(database_id),
            worker_id: record.owner_worker_id.clone(),
            priority: DEFAULT_JOB_PRIORITY,
        };
        submit_simple_operation(
            &state,
            &principal,
            &headers,
            post_method(),
            uri.path(),
            super::empty_body(),
            spec,
        )
        .await
    };

    accepted(
        &state,
        &principal,
        "DB_START",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 停止数据库（进程回收后状态回到 COLD）。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/stop",
    tag = "databases",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 202, description = "已受理（本来就是 COLD 时 operation_id 为 null）", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn stop_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    let outcome = if record.state.occupies_process() {
        let spec = LongOperationSpec {
            kind: "STOP_DB",
            job_kind: job_kind::DB_STOP,
            payload: serde_json::json!({ "database_id": id_text }),
            database_id: Some(database_id),
            worker_id: record.owner_worker_id.clone(),
            priority: DEFAULT_JOB_PRIORITY,
        };
        submit_simple_operation(
            &state,
            &principal,
            &headers,
            post_method(),
            uri.path(),
            super::empty_body(),
            spec,
        )
        .await
    } else {
        // 本来就没有进程：目标状态（COLD）已达成，直接回执。
        Ok(noop_accepted(database_id, "COLD"))
    };

    accepted(
        &state,
        &principal,
        "DB_STOP",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 重启数据库（stop 后按原 Owner 重新拉起）。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/restart",
    tag = "databases",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn restart_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    let spec = LongOperationSpec {
        kind: "RESTART_DB",
        job_kind: job_kind::DB_RESTART,
        payload: serde_json::json!({ "database_id": id_text }),
        database_id: Some(database_id),
        worker_id: record.owner_worker_id.clone(),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        post_method(),
        uri.path(),
        super::empty_body(),
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "DB_RESTART",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 迁移数据库到指定（或自动选择的）Worker。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/move",
    tag = "databases",
    params(("db_id" = String, Path, description = "数据库 ID")),
    request_body = dto::MoveDatabaseRequest,
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn move_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Json(request): Json<dto::MoveDatabaseRequest>,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_ADMIN)?;
    state.deployment.require_cluster()?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    // target_worker_id 为空表示交给 Scheduler 自动放置（job 侧按缺席处理）。
    let spec = LongOperationSpec {
        kind: "MOVE_DB",
        job_kind: job_kind::DB_MOVE,
        payload: serde_json::json!({
            "database_id": id_text,
            "target_worker_id": request.target_worker_id,
        }),
        database_id: Some(database_id),
        worker_id: record.owner_worker_id.clone(),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let body = stable_body_bytes(&request);
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        post_method(),
        uri.path(),
        &body,
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "DB_MOVE",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 触发一次快照（登记 `snapshots` 记录）。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/snapshot",
    tag = "snapshots",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn snapshot_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    let spec = LongOperationSpec {
        kind: "SNAPSHOT_DB",
        job_kind: job_kind::DB_SNAPSHOT,
        payload: serde_json::json!({ "database_id": id_text }),
        database_id: Some(database_id),
        worker_id: record.owner_worker_id.clone(),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        post_method(),
        uri.path(),
        super::empty_body(),
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "DB_SNAPSHOT",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 备份：快照 + 对象存储归档 + `backup_jobs` 记录。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/backup",
    tag = "snapshots",
    params(("db_id" = String, Path, description = "数据库 ID")),
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "数据库不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn backup_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_WRITE)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    let spec = LongOperationSpec {
        kind: "BACKUP_DB",
        job_kind: job_kind::DB_BACKUP,
        payload: serde_json::json!({ "database_id": id_text }),
        database_id: Some(database_id),
        worker_id: record.owner_worker_id.clone(),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        post_method(),
        uri.path(),
        super::empty_body(),
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "DB_BACKUP",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}

/// 从快照恢复。
#[utoipa::path(
    post,
    path = "/api/v1/databases/{db_id}/restore",
    tag = "snapshots",
    params(("db_id" = String, Path, description = "数据库 ID")),
    request_body = dto::RestoreDatabaseRequest,
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "数据库或快照不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn restore_database(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_db_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    Json(request): Json<dto::RestoreDatabaseRequest>,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::DB_ADMIN)?;
    let database_id = db_id(&raw_db_id)?;
    let record = load_database(&state, &principal, database_id).await?;
    let id_text = database_id.to_string();

    // snapshot_id 缺省（或为 null）时由 job 取最近一个 AVAILABLE 快照。
    let spec = LongOperationSpec {
        kind: "RESTORE_DB",
        job_kind: job_kind::DB_RESTORE,
        payload: serde_json::json!({
            "database_id": id_text,
            "snapshot_id": request.snapshot_id,
        }),
        database_id: Some(database_id),
        worker_id: record.owner_worker_id.clone(),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let body = stable_body_bytes(&request);
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        post_method(),
        uri.path(),
        &body,
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "DB_RESTORE",
        ("database", &id_text),
        Some(database_id),
        outcome,
    )
    .await
}
