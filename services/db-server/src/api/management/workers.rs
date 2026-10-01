//! Worker 运维端点：列表 / 详情 / 排空（架构 §17.4）。
//!
//! Worker 自身不上报 endpoint 之外的信息，这里的用量与容量全部来自 Catalog 的
//! `workers` 表（由心跳写入），因此读接口永远只走 PostgreSQL，不触碰 Worker。

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use catalog::DatabaseFilter;

use super::{accepted, post_method, DEFAULT_JOB_PRIORITY};
use crate::api::{dto, submit_simple_operation, worker_id, LongOperationSpec};
use crate::auth::{permission, Principal};
use crate::error::{ApiResult, PlatformResultExt};
use crate::state::AppState;

/// 详情接口里内联的「运行中数据库」上限。
///
/// 详情页需要看到该 Worker 上有哪些库，但不该把整台机器上千个库一次性吐出来
/// （响应体会失控）；超出的部分用列表接口带 `worker_id` 过滤分页获取。
const RUNNING_DATABASES_LIMIT: i64 = 200;

/// Worker 列表。
#[utoipa::path(
    get,
    path = "/api/v1/workers",
    tag = "workers",
    responses(
        (status = 200, description = "Worker 列表", body = Vec<dto::WorkerView>)
    )
)]
pub async fn list_workers(
    State(state): State<AppState>,
    principal: Principal,
) -> ApiResult<Json<Vec<dto::WorkerView>>> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let workers = state.distributed()?.catalog.list_workers().await.api()?;
    Ok(Json(
        workers
            .iter()
            // 列表接口不内联运行中的库：N 个 Worker 各带一次 DB 查询会让列表退化成 N+1。
            .map(|worker| dto::WorkerView::from_record(worker, Vec::new()))
            .collect(),
    ))
}

/// Worker 详情（附带其上的数据库）。
#[utoipa::path(
    get,
    path = "/api/v1/workers/{worker_id}",
    tag = "workers",
    params(("worker_id" = String, Path, description = "Worker ID")),
    responses(
        (status = 200, description = "Worker 详情", body = dto::WorkerView),
        (status = 404, description = "Worker 不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn get_worker(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_worker_id): Path<String>,
) -> ApiResult<Json<dto::WorkerView>> {
    principal.require_control_plane()?;
    principal.require(permission::DB_READ)?;
    let id = worker_id(&raw_worker_id)?;
    let record = state
        .distributed()?
        .catalog
        .get_worker(id.clone())
        .await
        .api()?;

    let filter = DatabaseFilter {
        worker_id: Some(id),
        limit: Some(RUNNING_DATABASES_LIMIT),
        offset: Some(0),
        ..DatabaseFilter::default()
    };
    let running = state.catalog.list_databases(filter).await.api()?;

    Ok(Json(dto::WorkerView::from_record(
        &record,
        running.iter().map(dto::DatabaseView::from).collect(),
    )))
}

/// 排空 Worker（把库迁走 / 停掉，随后 Worker 进入可下线状态）。
#[utoipa::path(
    post,
    path = "/api/v1/workers/{worker_id}/drain",
    tag = "workers",
    params(("worker_id" = String, Path, description = "Worker ID")),
    request_body = dto::DrainWorkerRequest,
    responses(
        (status = 202, description = "已受理", body = dto::OperationAccepted),
        (status = 404, description = "Worker 不存在", body = crate::error::ErrorEnvelope)
    )
)]
pub async fn drain_worker(
    State(state): State<AppState>,
    principal: Principal,
    Path(raw_worker_id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
    body: Option<Json<dto::DrainWorkerRequest>>,
) -> ApiResult<(StatusCode, Json<dto::OperationAccepted>)> {
    principal.require_control_plane()?;
    principal.require(permission::WORKER_ADMIN)?;
    let id = worker_id(&raw_worker_id)?;
    // 先确认 Worker 存在：drain 一个不存在的 Worker 只会在 job 里失败，
    // 那是一条「202 之后才知道参数错了」的糟糕路径。
    let record = state
        .distributed()?
        .catalog
        .get_worker(id.clone())
        .await
        .api()?;
    let id_text = record.id.to_string();
    let stop_cold_and_warm = body
        .and_then(|Json(request)| request.stop_cold_and_warm)
        .unwrap_or(false);

    let spec = LongOperationSpec {
        kind: "DRAIN_WORKER",
        job_kind: crate::background::job_kind::WORKER_DRAIN,
        payload: serde_json::json!({
            "worker_id": id_text,
            "stop_cold_and_warm": stop_cold_and_warm,
        }),
        // drain 作用于 Worker 而非单个 DB：operation 的 database_id 留空。
        database_id: None,
        worker_id: Some(id),
        priority: DEFAULT_JOB_PRIORITY,
    };
    let body_bytes = serde_json::to_vec(&serde_json::json!({
        "stop_cold_and_warm": stop_cold_and_warm,
    }))
    .unwrap_or_default();
    let outcome = submit_simple_operation(
        &state,
        &principal,
        &headers,
        post_method(),
        uri.path(),
        &body_bytes,
        spec,
    )
    .await;

    accepted(
        &state,
        &principal,
        "WORKER_DRAIN",
        ("worker", &id_text),
        None,
        outcome,
    )
    .await
}
