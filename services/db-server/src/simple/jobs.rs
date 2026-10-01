//! 单执行器领取持久作业；实例锁替代跨节点执行者选举。
use super::SimpleServices;
use crate::{
    error::{ApiError, ApiResult},
    state::AppState,
};
use chrono::Utc;
use domain::{
    records::{JobRecord, SnapshotRecord},
    DatabaseId, LifecycleState, OperationId, SnapshotId,
};
use objectstore::ObjectStore;
use std::{sync::Arc, time::Duration};

pub async fn recover(state: &AppState, local: &Arc<SimpleServices>) -> ApiResult<()> {
    while let Some(job) = local
        .catalog
        .lease_job("local", Duration::from_secs(3600), &[])
        .await?
    {
        run_job(state, local, job).await?;
    }
    Ok(())
}
pub fn spawn(state: AppState, local: Arc<SimpleServices>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tick.tick().await;
            if local.stopping.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            if let Err(error) = recover(&state, &local).await {
                tracing::error!(%error, "本地作业执行失败，保留状态供重试");
            }
        }
    })
}
async fn run_job(state: &AppState, local: &Arc<SimpleServices>, job: JobRecord) -> ApiResult<()> {
    let op: OperationId = required(&job, "operation_id")?
        .parse()
        .map_err(|_| ApiError::internal("作业 operation_id 非法"))?;
    let operation = local.catalog.get_operation(op).await?;
    let backup = if job.kind == "DB_BACKUP" {
        let db: DatabaseId = required(&job, "database_id")?
            .parse()
            .map_err(|_| ApiError::internal("作业 database_id 非法"))?;
        Some(
            local
                .catalog
                .ensure_backup_job_for_operation(db, op)
                .await?,
        )
    } else {
        None
    };
    if operation.is_terminal() {
        if let Some(record) = backup {
            if record.state != "SUCCEEDED" && record.state != "FAILED" {
                let success = operation.state == "SUCCEEDED";
                local
                    .catalog
                    .update_backup_job_state(
                        record.id,
                        catalog::BackupJobUpdate {
                            state: if success { "SUCCEEDED" } else { "FAILED" }.into(),
                            snapshot_id: success.then(|| op.to_string()),
                            error_message: (!success).then(|| {
                                operation.error_message.unwrap_or_else(|| "操作失败".into())
                            }),
                            ..Default::default()
                        },
                    )
                    .await?;
            }
        }
        fenced_complete(local, &job, true, None).await?;
        return Ok(());
    }
    if let Some(record) = &backup {
        if record.state != "SUCCEEDED" {
            local
                .catalog
                .update_backup_job_state(
                    record.id,
                    catalog::BackupJobUpdate {
                        state: "RUNNING".into(),
                        ..Default::default()
                    },
                )
                .await?;
        }
    }
    local
        .catalog
        .update_operation(op, "RUNNING", 10, None, None, None)
        .await?;
    let result = execute(state, local, &job, op).await;
    match result {
        Ok(result) => {
            local
                .catalog
                .update_operation(op, "SUCCEEDED", 100, None, None, Some(result))
                .await?;
            fenced_complete(local, &job, true, None).await?;
        }
        Err(error) => {
            let completed = fenced_complete(local, &job, false, Some(error.to_string())).await?;
            if completed.state == "FAILED" {
                if let Some(record) = backup {
                    local
                        .catalog
                        .update_backup_job_state(
                            record.id,
                            catalog::BackupJobUpdate {
                                state: "FAILED".into(),
                                error_message: Some(error.to_string()),
                                ..Default::default()
                            },
                        )
                        .await?;
                }
                local
                    .catalog
                    .update_operation(
                        op,
                        "FAILED",
                        100,
                        Some(error.code()),
                        Some(&error.to_string()),
                        None,
                    )
                    .await?;
            }
        }
    }
    Ok(())
}
fn required<'a>(job: &'a JobRecord, key: &str) -> ApiResult<&'a str> {
    job.payload
        .get(key)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::internal(format!("作业缺少 {key}")))
}
async fn execute(
    state: &AppState,
    local: &Arc<SimpleServices>,
    job: &JobRecord,
    op: OperationId,
) -> ApiResult<serde_json::Value> {
    let db: DatabaseId = required(job, "database_id")?
        .parse()
        .map_err(|_| ApiError::internal("作业 database_id 非法"))?;
    let _permit = tokio::time::timeout(Duration::from_secs(35), local.gate.write())
        .await
        .map_err(|_| {
            ApiError::new(
                domain::error::ErrorCode::DeadlineExceeded,
                "维护等待执行排空超时",
            )
        })?;
    match job.kind.as_str() {
        "DB_CREATE" => {
            local.catalog.get_database(db).await?;
        }
        "DB_START" => {
            if local.catalog.get_database(db).await?.state == LifecycleState::Failed {
                local.host.close_db(db).await?;
                cool(local, db).await?;
            }
            local.ensure_available(db).await?;
        }
        "DB_STOP" | "DB_RESTART" => {
            local.host.close_db(db).await?;
            state.sessions.remove_database(db);
            cool(local, db).await?;
            if job.kind == "DB_RESTART" {
                local.ensure_available(db).await?;
            }
        }
        "DB_DELETE" => {
            local.host.close_db(db).await?;
            state.sessions.remove_database(db);
            let dir = local.root.join("databases").join(db.to_string());
            if dir.exists() {
                tokio::fs::remove_dir_all(&dir).await.map_err(io_error)?;
                super::instance::sync_dir(&local.root.join("databases")).map_err(io_error)?;
            }
            // 文件消失但软删除未完成时可安全重试；提交后数据库不可再被懒加载。
            if local.catalog.get_database(db).await.is_ok() {
                local.catalog.soft_delete_database(db).await?;
            }
        }
        "DB_SNAPSHOT" | "DB_BACKUP" => {
            let snapshot: SnapshotId = op
                .to_string()
                .parse()
                .map_err(|_| ApiError::internal("快照 ID 非法"))?;
            let db_id = db.to_string();
            let snapshot_id = snapshot.to_string();
            let key = format!(
                "{}{snapshot_id}.local.manifest.json",
                objectstore::snapshot::object_prefix(&db_id, &snapshot_id)
            );
            let manifest = if local.objects.head(&key).await?.is_some() {
                // 上次执行可能已发布 manifest；校验后复用，不能把相同 ID 指向更新后的数据。
                objectstore::load_local_snapshot(
                    local.objects.as_ref(),
                    &db_id,
                    &snapshot_id,
                    engine_adapter::ENGINE_VERSION,
                )
                .await?
            } else {
                local.ensure_available(db).await?;
                let stage = local.root.join("tmp").join(format!("snapshot-{op}"));
                if stage.exists() {
                    tokio::fs::remove_dir_all(&stage).await.map_err(io_error)?;
                }
                local.host.snapshot(db, stage.clone()).await?;
                let mut files = Vec::new();
                let mut entries = tokio::fs::read_dir(&stage).await.map_err(io_error)?;
                while let Some(entry) = entries.next_entry().await.map_err(io_error)? {
                    if entry.file_type().await.map_err(io_error)?.is_file() {
                        files.push((
                            entry.file_name().to_string_lossy().into_owned(),
                            entry.path(),
                        ));
                    }
                }
                let result = objectstore::upload_local_snapshot(
                    local.objects.as_ref(),
                    &db_id,
                    &snapshot_id,
                    engine_adapter::ENGINE_VERSION,
                    &files,
                    &local.root.join("tmp"),
                )
                .await;
                let _ = tokio::fs::remove_dir_all(stage).await;
                result?
            };
            // 完整回读并解压到暂存目录：只有所有对象字节数与摘要吻合才能标记 AVAILABLE。
            let verify = local.root.join("tmp").join(format!("verify-{op}"));
            if verify.exists() {
                tokio::fs::remove_dir_all(&verify).await.map_err(io_error)?;
            }
            let result = objectstore::restore_local_snapshot(
                local.objects.as_ref(),
                &manifest,
                engine_adapter::ENGINE_VERSION,
                &verify,
                &local.root.join("tmp"),
            )
            .await;
            if result.is_ok() {
                tokio::fs::remove_dir_all(&verify).await.map_err(io_error)?;
            }
            result?;
            local
                .catalog
                .insert_snapshot(SnapshotRecord {
                    id: snapshot,
                    database_id: db,
                    base_lsn: domain::Lsn::new(0),
                    owner_epoch: domain::OwnerEpoch::new(0),
                    checksum: manifest.checksum.clone(),
                    size_bytes: manifest.total_size_bytes,
                    object_key: manifest.manifest_key(),
                    compression: "zstd".into(),
                    engine_version: manifest.engine_version,
                    schema_version: 1,
                    state: "AVAILABLE".into(),
                    created_at: Utc::now(),
                    verified_at: Some(Utc::now()),
                })
                .await?;
            if job.kind == "DB_BACKUP" {
                local
                    .catalog
                    .update_backup_job_state(
                        op.into_uuid(),
                        catalog::BackupJobUpdate {
                            state: "SUCCEEDED".into(),
                            snapshot_id: Some(snapshot_id.clone()),
                            actual_point: Some(Utc::now()),
                            bytes_transferred: Some(
                                i64::try_from(manifest.total_size_bytes).unwrap_or(i64::MAX),
                            ),
                            ..Default::default()
                        },
                    )
                    .await?;
            }
            return Ok(serde_json::json!({"snapshot_id":snapshot,"format":"local_complete_v1"}));
        }
        "DB_RESTORE" => {
            let snapshot = required(job, "snapshot_id")?;
            let manifest = objectstore::load_local_snapshot(
                local.objects.as_ref(),
                &db.to_string(),
                snapshot,
                engine_adapter::ENGINE_VERSION,
            )
            .await?;
            let stage = local.root.join("tmp").join(format!("restore-{op}"));
            if stage.exists() {
                tokio::fs::remove_dir_all(&stage).await.map_err(io_error)?;
            }
            objectstore::restore_local_snapshot(
                local.objects.as_ref(),
                &manifest,
                engine_adapter::ENGINE_VERSION,
                &stage,
                &local.root.join("tmp"),
            )
            .await?;
            state.sessions.remove_database(db);
            local.host.restore(db, stage).await?;
            cool(local, db).await?;
        }
        _ => return Err(crate::deployment::unsupported()),
    }
    Ok(serde_json::json!({"database_id":db}))
}
async fn cool(local: &SimpleServices, db: DatabaseId) -> ApiResult<()> {
    let record = local.catalog.get_database(db).await?;
    if record.state != LifecycleState::Cold {
        if record.state.is_serving() {
            local
                .catalog
                .set_lifecycle_state(db, LifecycleState::Stopping, None)
                .await?;
        }
        local
            .catalog
            .set_lifecycle_state(db, LifecycleState::Cold, None)
            .await?;
    }
    Ok(())
}
fn io_error(error: impl std::fmt::Display) -> ApiError {
    ApiError::new(
        domain::error::ErrorCode::StorageUnavailable,
        error.to_string(),
    )
}

async fn fenced_complete(
    local: &SimpleServices,
    job: &JobRecord,
    success: bool,
    error: Option<String>,
) -> ApiResult<JobRecord> {
    local
        .catalog
        .complete_job_fenced(job.id, "local", success, error)
        .await?
        .ok_or_else(|| ApiError::new(domain::error::ErrorCode::TransactionLost, "作业租约已失效"))
}
