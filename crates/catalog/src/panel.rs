//! Panel 数据：UI preferences / Saved SQL / Slow query（架构 §17.5.1）。
//!
//! 这些表是 Panel 的权威事实源，必须留在 PostgreSQL（不做 TursoDB dogfood）。
//! last_used / updated_at 类字段由 SQL 侧 `now()` 维护，避免各端时钟漂移。

use chrono::{DateTime, Utc};
use domain::error::Result;
use domain::ids::{DatabaseId, UserId, WorkerId};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::{map_sqlx_error, ConflictAs, NotFoundAs};
use crate::pg::{col, decode_opt_text_id, decode_uuid_id, invalid_argument};
use crate::Catalog;

// ------------------------------------------------------------------ 读模型

/// panel_preferences 行。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PreferenceRecord {
    pub user_id: UserId,
    pub key: String,
    pub value: Value,
    pub updated_at: DateTime<Utc>,
}

struct PreferenceRow(PreferenceRecord);

impl FromRow<'_, PgRow> for PreferenceRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        Ok(PreferenceRow(PreferenceRecord {
            user_id: decode_uuid_id(col(row, "user_id")?, "user_id")?,
            key: col(row, "key")?,
            value: col(row, "value")?,
            updated_at: col(row, "updated_at")?,
        }))
    }
}

/// saved_queries 行（id 用原始 uuid：domain 没有 SavedQueryId）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SavedQueryRecord {
    pub id: Uuid,
    pub user_id: UserId,
    pub database_id: Option<DatabaseId>,
    pub name: String,
    pub sql: String,
    pub description: String,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

struct SavedQueryRow(SavedQueryRecord);

impl FromRow<'_, PgRow> for SavedQueryRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let database_id: Option<Uuid> = col(row, "database_id")?;
        Ok(SavedQueryRow(SavedQueryRecord {
            id: col(row, "id")?,
            user_id: decode_uuid_id(col(row, "user_id")?, "user_id")?,
            database_id: match database_id {
                Some(v) => Some(decode_uuid_id::<DatabaseId>(v, "database_id")?),
                None => None,
            },
            name: col(row, "name")?,
            sql: col(row, "sql")?,
            description: col(row, "description")?,
            tags: col(row, "tags")?,
            created_at: col(row, "created_at")?,
            updated_at: col(row, "updated_at")?,
        }))
    }
}

/// slow_queries 行。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlowQueryRecord {
    pub id: i64,
    pub database_id: DatabaseId,
    pub worker_id: Option<WorkerId>,
    pub session_id: Option<String>,
    pub fingerprint: String,
    pub sql_text: String,
    pub duration_micros: i64,
    pub rows_returned: i64,
    pub error_code: Option<String>,
    pub created_at: DateTime<Utc>,
}

struct SlowQueryRow(SlowQueryRecord);

impl FromRow<'_, PgRow> for SlowQueryRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let worker_id: Option<String> = col(row, "worker_id")?;
        Ok(SlowQueryRow(SlowQueryRecord {
            id: col(row, "id")?,
            database_id: decode_uuid_id(col(row, "database_id")?, "database_id")?,
            worker_id: decode_opt_text_id::<WorkerId>(worker_id.as_deref(), "worker_id")?,
            session_id: col(row, "session_id")?,
            fingerprint: col(row, "fingerprint")?,
            sql_text: col(row, "sql_text")?,
            duration_micros: col(row, "duration_micros")?,
            rows_returned: col(row, "rows_returned")?,
            error_code: col(row, "error_code")?,
            created_at: col(row, "created_at")?,
        }))
    }
}

// ------------------------------------------------------------------ 入参

#[derive(Debug, Clone)]
pub struct NewSavedQuery {
    pub user_id: UserId,
    pub database_id: Option<DatabaseId>,
    pub name: String,
    pub sql: String,
    pub description: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NewSlowQuery {
    pub database_id: DatabaseId,
    pub worker_id: Option<WorkerId>,
    pub session_id: Option<String>,
    pub fingerprint: Option<String>,
    pub sql_text: String,
    pub duration_micros: i64,
    pub rows_returned: i64,
    pub error_code: Option<String>,
}

// ------------------------------------------------------------------ 实现

impl Catalog {
    pub async fn get_preference(&self, user_id: UserId, key: &str) -> Result<Option<Value>> {
        let value: Option<Value> = sqlx::query_scalar(
            "SELECT value FROM panel_preferences WHERE user_id = $1::uuid AND key = $2::text",
        )
        .bind(crate::pg::id_to_uuid(&user_id)?)
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(value)
    }

    /// 写入偏好（upsert，Panel 保存操作天然幂等）。
    pub async fn set_preference(&self, user_id: UserId, key: &str, value: &Value) -> Result<()> {
        if key.trim().is_empty() {
            return Err(invalid_argument("preference key 不能为空"));
        }
        sqlx::query(
            "INSERT INTO panel_preferences (id, user_id, key, value)
             VALUES ($1::uuid, $2::uuid, $3::text, $4::jsonb)
             ON CONFLICT (user_id, key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = now()",
        )
        .bind(Uuid::now_v7())
        .bind(crate::pg::id_to_uuid(&user_id)?)
        .bind(key)
        .bind(value.clone())
        .execute(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(())
    }

    pub async fn list_preferences(&self, user_id: UserId) -> Result<Vec<PreferenceRecord>> {
        let rows = sqlx::query_as::<_, PreferenceRow>(
            "SELECT user_id, key, value, updated_at FROM panel_preferences
             WHERE user_id = $1::uuid ORDER BY key",
        )
        .bind(crate::pg::id_to_uuid(&user_id)?)
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    pub async fn create_saved_query(&self, new_query: NewSavedQuery) -> Result<SavedQueryRecord> {
        if new_query.name.trim().is_empty() {
            return Err(invalid_argument("saved query name 不能为空"));
        }
        if new_query.sql.trim().is_empty() {
            return Err(invalid_argument("saved query sql 不能为空"));
        }
        let sql =
            "INSERT INTO saved_queries (id, user_id, database_id, name, sql, description, tags)
                   VALUES ($1::uuid, $2::uuid, $3::uuid, $4::text, $5::text,
                           COALESCE($6::text, ''), $7::text[])
                   RETURNING id, user_id, database_id, name, sql, description, tags,
                             created_at, updated_at";
        let row = sqlx::query_as::<_, SavedQueryRow>(sql)
            .bind(Uuid::now_v7())
            .bind(crate::pg::id_to_uuid(&new_query.user_id)?)
            .bind(match new_query.database_id {
                Some(d) => Some(crate::pg::id_to_uuid(&d)?),
                None => None,
            })
            .bind(new_query.name.as_str())
            .bind(new_query.sql.as_str())
            .bind(new_query.description.as_deref())
            .bind(new_query.tags.clone())
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(row.0)
    }

    pub async fn list_saved_queries(
        &self,
        user_id: UserId,
        limit: i64,
    ) -> Result<Vec<SavedQueryRecord>> {
        let rows = sqlx::query_as::<_, SavedQueryRow>(
            "SELECT id, user_id, database_id, name, sql, description, tags, created_at, updated_at
             FROM saved_queries WHERE user_id = $1::uuid
             ORDER BY updated_at DESC LIMIT $2::bigint",
        )
        .bind(crate::pg::id_to_uuid(&user_id)?)
        .bind(limit.clamp(1, 1000))
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// 删除自己的 saved query（同时校验 user_id，避免越权删除他人条目）。
    pub async fn delete_saved_query(&self, id: Uuid, user_id: UserId) -> Result<bool> {
        let deleted: Option<Uuid> = sqlx::query_scalar(
            "DELETE FROM saved_queries WHERE id = $1::uuid AND user_id = $2::uuid RETURNING id",
        )
        .bind(id)
        .bind(crate::pg::id_to_uuid(&user_id)?)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(deleted.is_some())
    }

    /// 写入慢查询记录，返回自增 id。
    pub async fn insert_slow_query(&self, query: NewSlowQuery) -> Result<i64> {
        if query.duration_micros < 0 {
            return Err(invalid_argument("duration_micros 不能为负"));
        }
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO slow_queries (database_id, worker_id, session_id, fingerprint, sql_text,
                                       duration_micros, rows_returned, error_code)
             VALUES ($1::uuid, $2::text, $3::text, COALESCE($4::text, ''), $5::text,
                     $6::bigint, $7::bigint, $8::text)
             RETURNING id",
        )
        .bind(crate::pg::id_to_uuid(&query.database_id)?)
        .bind(query.worker_id.map(|w| w.to_string()))
        .bind(query.session_id.as_deref())
        .bind(query.fingerprint.as_deref())
        .bind(query.sql_text.as_str())
        .bind(query.duration_micros)
        .bind(query.rows_returned)
        .bind(query.error_code.as_deref())
        .fetch_one(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(id)
    }

    pub async fn list_slow_queries(
        &self,
        db_id: DatabaseId,
        limit: i64,
        min_duration_micros: i64,
    ) -> Result<Vec<SlowQueryRecord>> {
        let rows = sqlx::query_as::<_, SlowQueryRow>(
            "SELECT id, database_id, worker_id, session_id, fingerprint, sql_text,
                    duration_micros, rows_returned, error_code, created_at
             FROM slow_queries
             WHERE database_id = $1::uuid AND duration_micros >= $2::bigint
             ORDER BY created_at DESC LIMIT $3::bigint",
        )
        .bind(crate::pg::id_to_uuid(&db_id)?)
        .bind(min_duration_micros.max(0))
        .bind(limit.clamp(1, 1000))
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Database, ConflictAs::Database))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}
