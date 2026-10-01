//! Snapshot / Backup：Snapshot metadata 与 Backup / Restore / PITR 作业记录（架构 §11.4、§16）。
//!
//! Snapshot 是恢复基线，metadata 必须先于对象存储可用性被持久化；
//! Backup Job 记录用于计算 RPO 与 Restore 成功率，因此状态流转必须落库。

use chrono::{DateTime, Utc};
use domain::error::{ErrorCode, Result};
use domain::ids::{DatabaseId, OperationId, SnapshotId};
use domain::records::SnapshotRecord;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::{
    catalog_error, map_sqlx_error, platform_error, CatalogError, ConflictAs, NotFoundAs,
};
use crate::pg::{col, decode_uuid_id, invalid_argument, SNAPSHOT_COLUMNS};
use crate::Catalog;

/// `snapshots.state` 的合法取值。
pub const SNAPSHOT_STATES: [&str; 4] = ["PENDING", "AVAILABLE", "CORRUPTED", "DELETED"];
/// `backup_jobs.kind` 的合法取值。
pub const BACKUP_JOB_KINDS: [&str; 3] = ["BACKUP", "RESTORE", "PITR"];
/// `backup_jobs.state` 的合法取值。
pub const BACKUP_JOB_STATES: [&str; 5] = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"];

pub fn is_valid_snapshot_state(state: &str) -> bool {
    SNAPSHOT_STATES.contains(&state)
}

pub fn is_valid_backup_job_kind(kind: &str) -> bool {
    BACKUP_JOB_KINDS.contains(&kind)
}

pub fn is_valid_backup_job_state(state: &str) -> bool {
    BACKUP_JOB_STATES.contains(&state)
}

const BACKUP_JOB_COLUMNS: &str =
    "id, database_id, operation_id, kind, state, snapshot_id, target_time, \
     actual_point, bytes_transferred, error_message, created_at, finished_at";

/// backup_jobs 行（domain 目前没有对应记录类型，读模型定义在 Catalog 内）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackupJobRecord {
    pub id: Uuid,
    pub database_id: DatabaseId,
    pub operation_id: Option<OperationId>,
    pub kind: String,
    pub state: String,
    pub snapshot_id: Option<String>,
    pub target_time: Option<DateTime<Utc>>,
    pub actual_point: Option<DateTime<Utc>>,
    pub bytes_transferred: i64,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

struct BackupJobRow(BackupJobRecord);

impl FromRow<'_, PgRow> for BackupJobRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(BackupJobRow(BackupJobRecord {
            id: col(row, "id")?,
            database_id: decode_uuid_id(col(row, "database_id")?, "database_id")?,
            operation_id: match col::<Option<Uuid>>(row, "operation_id")? {
                Some(v) => Some(decode_uuid_id::<OperationId>(v, "operation_id")?),
                None => None,
            },
            kind: col(row, "kind")?,
            state: col(row, "state")?,
            snapshot_id: col(row, "snapshot_id")?,
            target_time: col(row, "target_time")?,
            actual_point: col(row, "actual_point")?,
            bytes_transferred: col(row, "bytes_transferred")?,
            error_message: col(row, "error_message")?,
            created_at: col(row, "created_at")?,
            finished_at: col(row, "finished_at")?,
        }))
    }
}

/// 创建 backup job 的参数。
#[derive(Debug, Clone)]
pub struct CreateBackupJobParams {
    pub database_id: DatabaseId,
    /// BACKUP / RESTORE / PITR
    pub kind: String,
    pub operation_id: Option<OperationId>,
    pub snapshot_id: Option<String>,
    /// PITR 目标时间点
    pub target_time: Option<DateTime<Utc>>,
}

/// backup job 状态推进（None 字段保持原值）。
#[derive(Debug, Clone, Default)]
pub struct BackupJobUpdate {
    pub state: String,
    pub snapshot_id: Option<String>,
    pub actual_point: Option<DateTime<Utc>>,
    pub bytes_transferred: Option<i64>,
    pub error_message: Option<String>,
}

impl Catalog {
    /// 注册 Snapshot metadata。
    ///
    /// 采用 upsert：Backup Worker 崩溃重试时同一 snapshot_id 会再次上报，
    /// 保持幂等；列值以最后一次上报为准。
    #[tracing::instrument(skip(self, snapshot), fields(snapshot_id = %snapshot.id))]
    pub async fn insert_snapshot(&self, snapshot: SnapshotRecord) -> Result<SnapshotRecord> {
        if !is_valid_snapshot_state(&snapshot.state) {
            return Err(invalid_argument(format!(
                "unknown snapshot state '{}', expected one of {SNAPSHOT_STATES:?}",
                snapshot.state
            )));
        }
        let sql = format!(
            "INSERT INTO snapshots
                 (id, database_id, base_lsn, checksum, size_bytes, object_key, compression,
                  owner_epoch, engine_version, schema_version, state, created_at, verified_at)
             VALUES ($1::text, $2::uuid, $3::bigint, $4::text, $5::bigint, $6::text, $7::text,
                     $8::bigint, $9::text, $10::int, $11::text, $12::timestamptz, $13::timestamptz)
             ON CONFLICT (id) DO UPDATE SET
                 base_lsn = EXCLUDED.base_lsn,
                 checksum = EXCLUDED.checksum,
                 size_bytes = EXCLUDED.size_bytes,
                 object_key = EXCLUDED.object_key,
                 compression = EXCLUDED.compression,
                 -- 快照记录的 owner_epoch 同样只增不减：迟到的重传（Worker 重启后重放一次
                 -- 旧快照）不能把「这份快照属于哪个所有权世代」改小 —— 恢复路径会拿它去
                 -- 对外部存储层做 fencing 判断（架构 §11.3）。
                 owner_epoch = GREATEST(snapshots.owner_epoch, EXCLUDED.owner_epoch),
                 engine_version = EXCLUDED.engine_version,
                 schema_version = EXCLUDED.schema_version,
                 state = EXCLUDED.state,
                 verified_at = EXCLUDED.verified_at
             RETURNING {SNAPSHOT_COLUMNS}"
        );

        let row = sqlx::query_as::<_, crate::pg::SnapshotRow>(&sql)
            .bind(snapshot.id.to_string())
            .bind(crate::pg::id_to_uuid(&snapshot.database_id)?)
            .bind(crate::pg::saturating_i64(snapshot.base_lsn.get()))
            .bind(snapshot.checksum.as_str())
            .bind(crate::pg::saturating_i64(snapshot.size_bytes))
            .bind(snapshot.object_key.as_str())
            .bind(snapshot.compression.as_str())
            .bind(crate::pg::saturating_i64(snapshot.owner_epoch.get()))
            .bind(snapshot.engine_version.as_str())
            .bind(snapshot.schema_version)
            .bind(snapshot.state.as_str())
            .bind(snapshot.created_at)
            .bind(snapshot.verified_at)
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Snapshot, ConflictAs::Snapshot))?;
        Ok(row.0)
    }

    /// 最近一个可恢复的 Snapshot（仅 AVAILABLE；CORRUPTED/DELETED 绝不能作为恢复基线）。
    pub async fn latest_snapshot(&self, db_id: DatabaseId) -> Result<Option<SnapshotRecord>> {
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshots
             WHERE database_id = $1::uuid AND state = 'AVAILABLE'
             ORDER BY created_at DESC LIMIT 1"
        );
        let row = sqlx::query_as::<_, crate::pg::SnapshotRow>(&sql)
            .bind(crate::pg::id_to_uuid(&db_id)?)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Snapshot, ConflictAs::Snapshot))?;
        Ok(row.map(|r| r.0))
    }

    pub async fn list_snapshots(
        &self,
        db_id: DatabaseId,
        limit: i64,
    ) -> Result<Vec<SnapshotRecord>> {
        let sql = format!(
            "SELECT {SNAPSHOT_COLUMNS} FROM snapshots
             WHERE database_id = $1::uuid ORDER BY created_at DESC LIMIT $2::bigint"
        );
        let rows = sqlx::query_as::<_, crate::pg::SnapshotRow>(&sql)
            .bind(crate::pg::id_to_uuid(&db_id)?)
            .bind(limit.clamp(1, 1000))
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Snapshot, ConflictAs::Snapshot))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// 更新 Snapshot 状态；置 AVAILABLE 时补 verified_at（校验完成时间）。
    #[tracing::instrument(skip(self), fields(snapshot_id = %snapshot_id, state))]
    pub async fn mark_snapshot_state(
        &self,
        snapshot_id: SnapshotId,
        state: &str,
    ) -> Result<SnapshotRecord> {
        if !is_valid_snapshot_state(state) {
            return Err(invalid_argument(format!(
                "unknown snapshot state '{state}', expected one of {SNAPSHOT_STATES:?}"
            )));
        }
        let sql = format!(
            "UPDATE snapshots SET
                 state = $2::text,
                 verified_at = CASE WHEN $2::text = 'AVAILABLE' THEN now() ELSE verified_at END
             WHERE id = $1::text
             RETURNING {SNAPSHOT_COLUMNS}"
        );
        let row = sqlx::query_as::<_, crate::pg::SnapshotRow>(&sql)
            .bind(snapshot_id.to_string())
            .bind(state)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Snapshot, ConflictAs::Snapshot))?
            .ok_or_else(|| {
                catalog_error(CatalogError::SnapshotNotFound(snapshot_id.to_string()))
            })?;
        Ok(row.0)
    }

    /// 创建 Backup / Restore / PITR 作业记录。
    #[tracing::instrument(skip(self, params), fields(database_id = %params.database_id, kind = %params.kind))]
    pub async fn create_backup_job(
        &self,
        params: CreateBackupJobParams,
    ) -> Result<BackupJobRecord> {
        if !is_valid_backup_job_kind(&params.kind) {
            return Err(invalid_argument(format!(
                "unknown backup job kind '{}', expected one of {BACKUP_JOB_KINDS:?}",
                params.kind
            )));
        }
        let id = Uuid::now_v7();
        let sql = format!(
            "INSERT INTO backup_jobs
                 (id, database_id, operation_id, kind, state, snapshot_id, target_time)
             VALUES ($1::uuid, $2::uuid, $3::uuid, $4::text, 'PENDING', $5::text, $6::timestamptz)
             RETURNING {BACKUP_JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, BackupJobRow>(&sql)
            .bind(id)
            .bind(crate::pg::id_to_uuid(&params.database_id)?)
            .bind(match params.operation_id {
                Some(op) => Some(crate::pg::id_to_uuid(&op)?),
                None => None,
            })
            .bind(params.kind.as_str())
            .bind(params.snapshot_id.as_deref())
            .bind(params.target_time)
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Database))?;
        Ok(row.0)
    }

    /// 推进 backup job 状态；进入终态时补 finished_at。
    #[tracing::instrument(skip(self, update), fields(backup_job_id = %id, state = %update.state))]
    pub async fn update_backup_job_state(
        &self,
        id: Uuid,
        update: BackupJobUpdate,
    ) -> Result<BackupJobRecord> {
        if !is_valid_backup_job_state(&update.state) {
            return Err(invalid_argument(format!(
                "unknown backup job state '{}', expected one of {BACKUP_JOB_STATES:?}",
                update.state
            )));
        }
        let sql = format!(
            "UPDATE backup_jobs SET
                 state = $2::text,
                 snapshot_id = COALESCE($3::text, snapshot_id),
                 actual_point = COALESCE($4::timestamptz, actual_point),
                 bytes_transferred = COALESCE($5::bigint, bytes_transferred),
                 error_message = $6::text,
                 finished_at = CASE WHEN $2::text IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                                    THEN now() ELSE finished_at END
             WHERE id = $1::uuid
             RETURNING {BACKUP_JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, BackupJobRow>(&sql)
            .bind(id)
            .bind(update.state.as_str())
            .bind(update.snapshot_id.as_deref())
            .bind(update.actual_point)
            .bind(update.bytes_transferred)
            .bind(update.error_message.as_deref())
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Database))?
            .ok_or_else(|| {
                platform_error(
                    ErrorCode::InvalidArgument,
                    format!("backup job not found: {id}"),
                )
            })?;
        Ok(row.0)
    }

    pub async fn list_backup_jobs(
        &self,
        db_id: DatabaseId,
        limit: i64,
    ) -> Result<Vec<BackupJobRecord>> {
        let sql = format!(
            "SELECT {BACKUP_JOB_COLUMNS} FROM backup_jobs
             WHERE database_id = $1::uuid ORDER BY created_at DESC LIMIT $2::bigint"
        );
        let rows = sqlx::query_as::<_, BackupJobRow>(&sql)
            .bind(crate::pg::id_to_uuid(&db_id)?)
            .bind(limit.clamp(1, 1000))
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Database))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_whitelists_match_schema_checks() {
        assert!(is_valid_snapshot_state("PENDING"));
        assert!(is_valid_snapshot_state("AVAILABLE"));
        assert!(is_valid_snapshot_state("CORRUPTED"));
        assert!(is_valid_snapshot_state("DELETED"));
        assert!(!is_valid_snapshot_state("READY"));

        assert!(is_valid_backup_job_kind("BACKUP"));
        assert!(is_valid_backup_job_kind("RESTORE"));
        assert!(is_valid_backup_job_kind("PITR"));
        assert!(!is_valid_backup_job_kind("SNAPSHOT"));

        assert!(is_valid_backup_job_state("PENDING"));
        assert!(is_valid_backup_job_state("RUNNING"));
        assert!(is_valid_backup_job_state("SUCCEEDED"));
        assert!(is_valid_backup_job_state("FAILED"));
        assert!(is_valid_backup_job_state("CANCELLED"));
        assert!(!is_valid_backup_job_state("DONE"));
    }
}
