//! Job queue：PostgreSQL Job Table + `FOR UPDATE SKIP LOCKED` + lease（架构 §17.5）。
//!
//! 不引入 RabbitMQ/Kafka/NATS：后台任务只是「带 lease 的行」，抢锁用 SKIP LOCKED，
//! 崩溃恢复用 lease 过期回收，重试用 attempts/max_attempts。

use std::time::Duration;

use chrono::{DateTime, Utc};
use domain::error::{ErrorCode, Result};
use domain::ids::JobId;
use domain::records::JobRecord;

use crate::error::{
    catalog_error, map_sqlx_error, platform_error, CatalogError, ConflictAs, NotFoundAs,
};
use crate::pg::{invalid_argument, millis, JOB_COLUMNS};
use crate::Catalog;

/// `jobs.state` 的合法取值。
pub const JOB_STATES: [&str; 5] = ["READY", "LEASED", "DONE", "FAILED", "CANCELLED"];

pub fn is_valid_job_state(state: &str) -> bool {
    JOB_STATES.contains(&state)
}

/// 默认 lease 时长：任务必须在该窗口内完成或续租，否则被其他 Worker 回收。
pub const DEFAULT_JOB_LEASE: Duration = Duration::from_secs(60);

/// 失败重试的退避基数与上限。
pub const JOB_RETRY_BASE_BACKOFF_MS: i64 = 1_000;
pub const JOB_RETRY_MAX_BACKOFF_MS: i64 = 300_000;
/// 2 的幂次上限，避免大 attempts 造成移位溢出。
const JOB_RETRY_MAX_SHIFT: i32 = 20;

/// 第 `attempts` 次失败后的退避时长（指数退避 + 上限）。
pub fn job_retry_backoff_millis(attempts: i32) -> i64 {
    let shift = attempts.saturating_sub(1).clamp(0, JOB_RETRY_MAX_SHIFT) as u32;
    let factor = 1_i64.checked_shl(shift).unwrap_or(i64::MAX);
    JOB_RETRY_BASE_BACKOFF_MS
        .saturating_mul(factor)
        .min(JOB_RETRY_MAX_BACKOFF_MS)
}

impl Catalog {
    /// 入队。带 idempotency_key 时重复入队返回已存在的 job（不重复执行副作用）。
    #[tracing::instrument(skip(self, payload, idempotency_key), fields(kind, priority))]
    pub async fn enqueue_job(
        &self,
        kind: &str,
        payload: serde_json::Value,
        priority: i32,
        run_after: Option<DateTime<Utc>>,
        idempotency_key: Option<&str>,
    ) -> Result<JobRecord> {
        if kind.trim().is_empty() {
            return Err(invalid_argument("job kind 不能为空"));
        }
        let job_id = JobId::new_v7();
        let sql = format!(
            "INSERT INTO jobs (id, kind, payload, state, priority, run_after, idempotency_key)
             VALUES ($1::uuid, $2::text, $3::jsonb, 'READY', $4::int,
                     COALESCE($5::timestamptz, now()), $6::text)
             ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
             RETURNING {JOB_COLUMNS}"
        );

        let inserted = sqlx::query_as::<_, crate::pg::JobRow>(&sql)
            .bind(crate::pg::id_to_uuid(&job_id)?)
            .bind(kind)
            .bind(payload.clone())
            .bind(priority)
            .bind(run_after)
            .bind(idempotency_key)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        match inserted {
            Some(row) => Ok(row.0),
            None => {
                // 幂等命中：返回已存在的 job
                let key = idempotency_key.ok_or_else(|| {
                    platform_error(
                        ErrorCode::InternalError,
                        "job 插入未返回行且没有 idempotency_key",
                    )
                })?;
                let sql =
                    format!("SELECT {JOB_COLUMNS} FROM jobs WHERE idempotency_key = $1::text");
                let row = sqlx::query_as::<_, crate::pg::JobRow>(&sql)
                    .bind(key)
                    .fetch_one(self.pool())
                    .await
                    .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;
                Ok(row.0)
            }
        }
    }

    /// 抢占一个可执行任务。
    ///
    /// 单事务内两件事（顺序不可交换）：
    /// 1) 回收 lease 已过期的行（原持有者已崩溃），attempts 用尽则直接 FAILED；
    /// 2) `ORDER BY priority, run_after` + `FOR UPDATE SKIP LOCKED LIMIT 1` 抢占，
    ///    再更新为 LEASED 并 attempts + 1。
    /// SKIP LOCKED 保证多个 Worker 并发抢占不会互相阻塞、也不会抢到同一行。
    #[tracing::instrument(skip(self, kinds), fields(lease_owner, kinds = ?kinds))]
    pub async fn lease_job(
        &self,
        lease_owner: &str,
        lease_ttl: Duration,
        kinds: &[&str],
    ) -> Result<Option<JobRecord>> {
        if lease_owner.trim().is_empty() {
            return Err(invalid_argument("lease_owner 不能为空"));
        }
        // 空 kinds 表示不抢占任何类型的任务
        if kinds.is_empty() {
            return Ok(None);
        }
        let kinds: Vec<String> = kinds.iter().map(|k| (*k).to_string()).collect();

        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        let reclaimed = sqlx::query(
            "UPDATE jobs SET
                 state = CASE WHEN attempts >= max_attempts THEN 'FAILED' ELSE 'READY' END,
                 finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE finished_at END,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 last_error = COALESCE(last_error, $1::text),
                 updated_at = now()
             WHERE state = 'LEASED' AND lease_expires_at IS NOT NULL AND lease_expires_at < now()",
        )
        .bind("lease expired, reclaimed")
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;
        if reclaimed.rows_affected() > 0 {
            tracing::warn!(
                reclaimed = reclaimed.rows_affected(),
                "回收 lease 过期的 job"
            );
        }

        let sql = format!(
            "SELECT {JOB_COLUMNS} FROM jobs
             WHERE state = 'READY' AND run_after <= now() AND kind = ANY($1::text[])
             ORDER BY priority ASC, run_after ASC
             FOR UPDATE SKIP LOCKED
             LIMIT 1"
        );
        let candidate = sqlx::query_as::<_, crate::pg::JobRow>(&sql)
            .bind(&kinds)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        let Some(candidate) = candidate else {
            tx.commit()
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;
            return Ok(None);
        };

        let sql = format!(
            "UPDATE jobs SET
                 state = 'LEASED',
                 lease_owner = $2::text,
                 lease_expires_at = now() + ($3::bigint * INTERVAL '1 millisecond'),
                 attempts = attempts + 1,
                 updated_at = now()
             WHERE id = $1::uuid
             RETURNING {JOB_COLUMNS}"
        );
        let leased = sqlx::query_as::<_, crate::pg::JobRow>(&sql)
            .bind(crate::pg::id_to_uuid(&candidate.0.id)?)
            .bind(lease_owner)
            .bind(millis(lease_ttl))
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;
        Ok(Some(leased.0))
    }

    /// 完成任务。
    ///
    /// 失败时按 attempts / max_attempts 决定：还能重试则回到 READY 并写入指数退避的
    /// run_after；否则置 FAILED。整个过程在事务内并对目标行加锁，避免与 lease 回收竞争。
    #[tracing::instrument(skip(self, error), fields(job_id = %id, success))]
    pub async fn complete_job(
        &self,
        id: JobId,
        success: bool,
        error: Option<String>,
    ) -> Result<JobRecord> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        let current: Option<(i32, i32)> = sqlx::query_as(
            "SELECT attempts, max_attempts FROM jobs WHERE id = $1::uuid FOR UPDATE",
        )
        .bind(crate::pg::id_to_uuid(&id)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        let (attempts, max_attempts) =
            current.ok_or_else(|| catalog_error(CatalogError::JobNotFound(id.to_string())))?;

        let sql = if success || attempts >= max_attempts {
            format!(
                "UPDATE jobs SET
                     state = $2::text,
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = $3::text,
                     finished_at = now(),
                     updated_at = now()
                 WHERE id = $1::uuid
                 RETURNING {JOB_COLUMNS}"
            )
        } else {
            format!(
                "UPDATE jobs SET
                     state = 'READY',
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     last_error = $3::text,
                     run_after = now() + ($4::bigint * INTERVAL '1 millisecond'),
                     updated_at = now()
                 WHERE id = $1::uuid
                 RETURNING {JOB_COLUMNS}"
            )
        };

        let state = if success { "DONE" } else { "FAILED" };
        let mut query = sqlx::query_as::<_, crate::pg::JobRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .bind(state)
            .bind(error.clone());
        if !success && attempts < max_attempts {
            query = query.bind(job_retry_backoff_millis(attempts));
        }

        let updated = query
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;
        Ok(updated.0)
    }

    /// 带 lease_owner 校验的完成：lease 已被回收/转交时返回 `None`，
    /// 防止「僵尸 Worker」重复完成同一任务（与 ownership epoch 同一防护思路）。
    pub async fn complete_job_fenced(
        &self,
        id: JobId,
        lease_owner: &str,
        success: bool,
        error: Option<String>,
    ) -> Result<Option<JobRecord>> {
        let owner: Option<String> =
            sqlx::query_scalar("SELECT lease_owner FROM jobs WHERE id = $1::uuid")
                .bind(crate::pg::id_to_uuid(&id)?)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;

        match owner {
            None => Err(catalog_error(CatalogError::JobNotFound(id.to_string()))),
            Some(current) if current != lease_owner => Ok(None),
            Some(_) => self.complete_job(id, success, error).await.map(Some),
        }
    }

    /// 续租（长任务必须续租，否则会被其他 Worker 回收）。
    pub async fn extend_job_lease(
        &self,
        id: JobId,
        lease_owner: &str,
        lease_ttl: Duration,
    ) -> Result<bool> {
        let updated: Option<uuid::Uuid> = sqlx::query_scalar(
            "UPDATE jobs SET lease_expires_at = now() + ($3::bigint * INTERVAL '1 millisecond'),
                             updated_at = now()
             WHERE id = $1::uuid AND state = 'LEASED' AND lease_owner = $2::text
             RETURNING id",
        )
        .bind(crate::pg::id_to_uuid(&id)?)
        .bind(lease_owner)
        .bind(millis(lease_ttl))
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?;
        Ok(updated.is_some())
    }

    pub async fn get_job(&self, id: JobId) -> Result<JobRecord> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1::uuid");
        let row = sqlx::query_as::<_, crate::pg::JobRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Job, ConflictAs::Idempotency))?
            .ok_or_else(|| catalog_error(CatalogError::JobNotFound(id.to_string())))?;
        Ok(row.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially_and_is_capped() {
        assert_eq!(job_retry_backoff_millis(0), JOB_RETRY_BASE_BACKOFF_MS);
        assert_eq!(job_retry_backoff_millis(1), JOB_RETRY_BASE_BACKOFF_MS);
        assert_eq!(job_retry_backoff_millis(2), JOB_RETRY_BASE_BACKOFF_MS * 2);
        assert_eq!(job_retry_backoff_millis(3), JOB_RETRY_BASE_BACKOFF_MS * 4);
        assert_eq!(job_retry_backoff_millis(100), JOB_RETRY_MAX_BACKOFF_MS);
        // 极端值不 panic、不溢出
        assert_eq!(job_retry_backoff_millis(i32::MAX), JOB_RETRY_MAX_BACKOFF_MS);
        assert_eq!(
            job_retry_backoff_millis(i32::MIN),
            JOB_RETRY_BASE_BACKOFF_MS
        );
    }

    #[test]
    fn job_state_whitelist_matches_schema_check() {
        assert!(is_valid_job_state("READY"));
        assert!(is_valid_job_state("LEASED"));
        assert!(is_valid_job_state("DONE"));
        assert!(is_valid_job_state("FAILED"));
        assert!(is_valid_job_state("CANCELLED"));
        assert!(!is_valid_job_state("RUNNING"));
        assert!(!is_valid_job_state("ready"));
    }
}
