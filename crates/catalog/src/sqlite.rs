//! 单进程 SQLite 元数据。协调接口不在此处实现。
use crate::metadata::{BackupStore, DatabaseStore, IdentityStore, Metadata, PanelStore, TaskStore};
use crate::{
    ApiTokenRecord, AuditEntry, AuditFilter, AuditLogRecord, AuthenticatedToken, BackupJobRecord,
    BackupJobUpdate, CreateBackupJobParams, DatabaseFilter, NewApiToken, NewSavedQuery,
    NewSlowQuery, NewUser, PreferenceRecord, SavedQueryRecord, SlowQueryRecord, UserRecord,
};
use crate::{CreateDatabaseParams, NewOperation};
use chrono::{DateTime, Utc};
use domain::ids::TokenId;
use domain::records::SnapshotRecord;
use domain::{
    error::{ErrorCode, PlatformError, Result},
    ids::{DatabaseId, JobId, OperationId, TenantId, UserId},
    lifecycle::LifecycleState,
    records::{DatabaseRecord, JobRecord, OperationRecord},
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Row, SqlitePool,
};
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn stage_metrics_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("PERI_LOOM_SIMPLE_STAGE_METRICS").as_deref()
            == Some(std::ffi::OsStr::new("1"))
    })
}

fn record_simple_stage(stage: &'static str, started: Option<Instant>) {
    if let Some(started) = started {
        metrics::histogram!("simple_stage_micros", "stage" => stage)
            .record(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
}

#[derive(Clone, Debug)]
pub struct SqliteCatalog {
    pool: SqlitePool,
    read_pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct LocalSubmission {
    pub operation: NewOperation,
    pub create_database: Option<CreateDatabaseParams>,
    pub job_kind: String,
    pub job_payload: Value,
    pub priority: i32,
    pub idempotency_key: Option<String>,
    pub request_hash: Option<String>,
}
#[derive(Debug, Clone)]
pub struct LocalSubmissionOutcome {
    pub database: Option<DatabaseRecord>,
    pub operation: OperationRecord,
    pub job: JobRecord,
    pub replayed: bool,
}

fn err(code: ErrorCode, detail: impl Into<String>) -> PlatformError {
    PlatformError::new(code, detail.into())
}
fn storage(e: impl std::fmt::Display) -> PlatformError {
    err(
        ErrorCode::StorageUnavailable,
        format!("SQLite metadata: {e}"),
    )
}
fn sqlite_write_error(error: sqlx::Error, conflict: ErrorCode) -> PlatformError {
    if let sqlx::Error::Database(ref database) = error {
        if database.is_unique_violation() {
            return err(conflict, format!("SQLite metadata conflict: {database}"));
        }
        if database.is_foreign_key_violation() {
            return err(
                ErrorCode::InvalidArgument,
                format!("SQLite metadata reference: {database}"),
            );
        }
    }
    storage(error)
}
fn encode<T: Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(storage)
}
fn decode<T: DeserializeOwned>(s: String) -> Result<T> {
    serde_json::from_str(&s).map_err(storage)
}
fn now() -> DateTime<Utc> {
    Utc::now()
}
fn missing(what: &str) -> PlatformError {
    err(ErrorCode::InvalidArgument, format!("{what} not found"))
}

impl SqliteCatalog {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .map_err(storage)?;
        sqlx::migrate!("./sqlite_migrations")
            .run(&pool)
            .await
            .map_err(storage)?;
        let integrity: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(&pool)
            .await
            .map_err(storage)?;
        if integrity != "ok" {
            return Err(storage(format!(
                "metadata integrity check failed: {integrity}"
            )));
        }
        // 写路径保持单连接串行；只读连接不运行可能需要独占锁的 WAL 配置 PRAGMA。
        let read_pool = SqlitePoolOptions::new()
            .max_connections(4)
            // 本地只读连接归还时 SQLx 仍会 ping；避免每次复用前再向 SQLite worker 往返。
            .test_before_acquire(false)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .read_only(true)
                    .foreign_keys(true)
                    .busy_timeout(Duration::from_secs(5)),
            )
            .await
            .map_err(storage)?;
        Ok(Self { pool, read_pool })
    }
    pub async fn close(self) -> Result<()> {
        self.read_pool.close().await;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        self.pool.close().await;
        Ok(())
    }
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }
    pub async fn current_catalog_version(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT version FROM catalog_version WHERE id=1")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)
    }
    pub async fn has_pending_mutation(&self, id: DatabaseId) -> Result<bool> {
        let pending: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE database_id=? AND kind IN ('DB_DELETE','DB_RESTORE') AND state IN ('READY','LEASED'))",
        )
        .bind(id.to_string())
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        Ok(pending != 0)
    }
    /// Simple 查询入口在同一个 SQLite 快照中读取库状态和删除/恢复作业。
    /// 即使库记录不存在也返回 pending，以维持准入检查的错误优先级。
    pub async fn database_admission_snapshot(
        &self,
        id: DatabaseId,
    ) -> Result<(bool, Option<DatabaseRecord>)> {
        let row = sqlx::query(
            "SELECT (SELECT record FROM databases WHERE id=?) AS record, \
             EXISTS(SELECT 1 FROM jobs WHERE database_id=? AND kind IN ('DB_DELETE','DB_RESTORE') AND state IN ('READY','LEASED')) AS pending",
        )
        .bind(id.to_string())
        .bind(id.to_string())
        .fetch_one(&self.read_pool)
        .await
        .map_err(storage)?;
        let pending: i64 = row.try_get("pending").map_err(storage)?;
        if pending != 0 {
            return Ok((true, None));
        }
        let record: Option<String> = row.try_get("record").map_err(storage)?;
        Ok((false, record.map(decode).transpose()?))
    }
    /// Simple `/query` 同时需要资源授权和准入状态；这里始终解码库记录，
    /// 使不存在/租户错误先于 pending 作业。其他准入路径仍使用上面的方法。
    pub async fn database_query_snapshot(
        &self,
        id: DatabaseId,
    ) -> Result<(bool, Option<DatabaseRecord>)> {
        let acquire_started = stage_metrics_enabled().then(Instant::now);
        let acquired = self.read_pool.acquire().await;
        record_simple_stage("query_snapshot_read_pool_acquire", acquire_started);
        let mut connection = acquired.map_err(storage)?;
        let fetch_started = stage_metrics_enabled().then(Instant::now);
        let fetched = sqlx::query(
            "SELECT (SELECT record FROM databases WHERE id=?) AS record, \
             EXISTS(SELECT 1 FROM jobs WHERE database_id=? AND kind IN ('DB_DELETE','DB_RESTORE') AND state IN ('READY','LEASED')) AS pending",
        )
        .bind(id.to_string())
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await;
        record_simple_stage("query_snapshot_sql_fetch", fetch_started);
        drop(connection);
        let row = fetched.map_err(storage)?;
        let pending: i64 = row.try_get("pending").map_err(storage)?;
        let record: Option<String> = row.try_get("record").map_err(storage)?;
        Ok((pending != 0, record.map(decode).transpose()?))
    }
    /// Simple JWT `/query` 的单语句快照。库记录保持原始 JSON，交给 handler 在权限、
    /// 路径 ID 和库绑定检查之后解码，以维持原有错误优先级。
    pub async fn jwt_query_snapshot(
        &self,
        subject: &str,
        database_id: &str,
    ) -> Result<(Option<(UserRecord, Vec<String>)>, (bool, Option<String>))> {
        let (by_id, lookup) = match subject.parse::<Uuid>() {
            Ok(id) => (true, id.to_string()),
            Err(_) => (false, subject.to_owned()),
        };
        let sql = if by_id {
            "WITH principal AS (SELECT id,record FROM users WHERE id=?) \
             SELECT (SELECT record FROM principal) AS user_record, \
             (SELECT json_group_array(permission) FROM \
               (SELECT DISTINCT p.permission FROM role_bindings b JOIN role_permissions p ON p.role_id=b.role_id \
                WHERE b.user_id=(SELECT id FROM principal))) AS permissions, \
             (SELECT record FROM databases WHERE id=?) AS db_record, \
             EXISTS(SELECT 1 FROM jobs WHERE database_id=? AND kind IN ('DB_DELETE','DB_RESTORE') \
               AND state IN ('READY','LEASED')) AS pending"
        } else {
            "WITH principal AS (SELECT id,record FROM users WHERE username=?) \
             SELECT (SELECT record FROM principal) AS user_record, \
             (SELECT json_group_array(permission) FROM \
               (SELECT DISTINCT p.permission FROM role_bindings b JOIN role_permissions p ON p.role_id=b.role_id \
                WHERE b.user_id=(SELECT id FROM principal))) AS permissions, \
             (SELECT record FROM databases WHERE id=?) AS db_record, \
             EXISTS(SELECT 1 FROM jobs WHERE database_id=? AND kind IN ('DB_DELETE','DB_RESTORE') \
               AND state IN ('READY','LEASED')) AS pending"
        };
        let acquire_started = stage_metrics_enabled().then(Instant::now);
        let acquired = self.read_pool.acquire().await;
        record_simple_stage("jwt_query_read_pool_acquire", acquire_started);
        let mut connection = acquired.map_err(storage)?;
        let fetch_started = stage_metrics_enabled().then(Instant::now);
        let fetched = sqlx::query(sql)
            .bind(lookup)
            .bind(database_id)
            .bind(database_id)
            .fetch_one(&mut *connection)
            .await;
        record_simple_stage("jwt_query_sql_fetch", fetch_started);
        drop(connection);
        let row = fetched.map_err(storage)?;
        let pending: i64 = row.try_get("pending").map_err(storage)?;
        let db_record: Option<String> = row.try_get("db_record").map_err(storage)?;
        let user_record: Option<String> = row.try_get("user_record").map_err(storage)?;
        let user = if let Some(encoded) = user_record {
            let user: UserRecord = decode(encoded)?;
            let mut permissions = if user.is_active() {
                let encoded: String = row.try_get("permissions").map_err(storage)?;
                serde_json::from_str::<Vec<String>>(&encoded).map_err(storage)?
            } else {
                Vec::new()
            };
            if user.is_superuser {
                permissions.push("*".into());
            }
            permissions.sort();
            permissions.dedup();
            Some((user, permissions))
        } else {
            None
        };
        Ok((user, (pending != 0, db_record)))
    }
    pub async fn has_unfinished_jobs(&self) -> Result<bool> {
        let pending: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE state IN ('READY','LEASED')) OR EXISTS(SELECT 1 FROM operations WHERE state IN ('PENDING','RUNNING'))",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        Ok(pending != 0)
    }
    pub async fn create_database(&self, params: CreateDatabaseParams) -> Result<DatabaseRecord> {
        let record = Self::database_record(params)?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        sqlx::query(
            "INSERT INTO databases(id,tenant_id,name,state,deleted_at,record) VALUES(?,?,?,?,?,?)",
        )
        .bind(record.id.to_string())
        .bind(record.tenant_id.to_string())
        .bind(&record.name)
        .bind(record.state.to_string())
        .bind(Option::<String>::None)
        .bind(encode(&record)?)
        .execute(&mut *tx)
        .await
        .map_err(|e| sqlite_write_error(e, ErrorCode::DbAlreadyExists))?;
        sqlx::query("UPDATE catalog_version SET version=version+1 WHERE id=1")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(record)
    }
    fn database_record(params: CreateDatabaseParams) -> Result<DatabaseRecord> {
        if params.name.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "database name is empty"));
        }
        let at = now();
        let id = params.id.unwrap_or_else(DatabaseId::new_v7);
        Ok(DatabaseRecord {
            id,
            tenant_id: params.tenant_id.unwrap_or_else(crate::default_tenant_id),
            name: params.name,
            state: params.state.unwrap_or(LifecycleState::Cold),
            owner_worker_id: None,
            owner_epoch: domain::wal::OwnerEpoch::new(0),
            lease_expires_at: None,
            wakeup_in_progress: false,
            wakeup_started_at: None,
            storage_region: params.storage_region.unwrap_or_else(|| "local".into()),
            storage_prefix: params
                .storage_prefix
                .unwrap_or_else(|| format!("databases/{id}")),
            last_snapshot_id: None,
            last_snapshot_lsn: None,
            cpu_milli: params.cpu_milli.unwrap_or(1000),
            memory_mib: params.memory_mib.unwrap_or(512),
            fd_limit: params.fd_limit.unwrap_or(1024),
            disk_mib: params.disk_mib.unwrap_or(1024),
            iops_limit: params.iops_limit.unwrap_or(1000),
            priority: params.priority.unwrap_or(0),
            evictable: params.evictable.unwrap_or(true),
            engine_version: params.engine_version.unwrap_or_default(),
            schema_version: params.schema_version.unwrap_or(0),
            affinity_worker_id: None,
            anti_affinity_worker_id: None,
            labels: params.labels.unwrap_or(Value::Object(Default::default())),
            created_at: at,
            updated_at: at,
            deleted_at: None,
        })
    }
    pub async fn get_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        let value: Option<String> = sqlx::query_scalar("SELECT record FROM databases WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.read_pool)
            .await
            .map_err(storage)?;
        decode(value.ok_or_else(|| err(ErrorCode::DbNotFound, "database not found"))?)
    }
    pub async fn get_database_by_name(
        &self,
        tenant: TenantId,
        name: &str,
    ) -> Result<DatabaseRecord> {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT record FROM databases WHERE tenant_id=? AND name=? AND deleted_at IS NULL",
        )
        .bind(tenant.to_string())
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        decode(value.ok_or_else(|| err(ErrorCode::DbNotFound, "database not found"))?)
    }
    pub async fn list_databases(
        &self,
        filter: crate::DatabaseFilter,
    ) -> Result<Vec<DatabaseRecord>> {
        let rows = sqlx::query("SELECT record FROM databases ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        let mut all = Vec::new();
        for r in rows {
            let db: DatabaseRecord = decode(r.try_get("record").map_err(storage)?)?;
            if !filter.include_deleted && db.deleted_at.is_some() {
                continue;
            }
            if filter.tenant_id.is_some_and(|v| v != db.tenant_id)
                || filter
                    .worker_id
                    .as_ref()
                    .is_some_and(|v| db.owner_worker_id.as_ref() != Some(v))
                || filter.state.is_some_and(|v| v != db.state)
                || filter
                    .name_prefix
                    .as_ref()
                    .is_some_and(|v| !db.name.starts_with(v))
            {
                continue;
            }
            all.push(db);
        }
        Ok(all
            .into_iter()
            .skip(filter.offset.unwrap_or(0).max(0) as usize)
            .take(filter.limit.unwrap_or(i64::MAX).max(0) as usize)
            .collect())
    }
    pub async fn set_lifecycle_state(
        &self,
        id: DatabaseId,
        state: LifecycleState,
        _owner: Option<domain::ids::WorkerId>,
    ) -> Result<DatabaseRecord> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT record FROM databases WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        let mut db: DatabaseRecord =
            decode(existing.ok_or_else(|| err(ErrorCode::DbNotFound, "database not found"))?)?;
        crate::validate_lifecycle_transition(db.state, state)?;
        db.state = state;
        db.updated_at = now();
        sqlx::query("UPDATE databases SET state=?,record=? WHERE id=?")
            .bind(state.to_string())
            .bind(encode(&db)?)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        sqlx::query("UPDATE catalog_version SET version=version+1 WHERE id=1")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(db)
    }
    pub async fn soft_delete_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT record FROM databases WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        let mut db: DatabaseRecord =
            decode(existing.ok_or_else(|| err(ErrorCode::DbNotFound, "database not found"))?)?;
        if db.deleted_at.is_none() {
            db.deleted_at = Some(now());
            db.updated_at = now();
            sqlx::query("UPDATE databases SET deleted_at=?,record=? WHERE id=?")
                .bind(db.deleted_at.map(|v| v.to_rfc3339()))
                .bind(encode(&db)?)
                .bind(id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            sqlx::query("UPDATE catalog_version SET version=version+1 WHERE id=1")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(db)
    }
}
impl SqliteCatalog {
    pub async fn create_operation(
        &self,
        kind: &str,
        database_id: Option<DatabaseId>,
        worker_id: Option<domain::ids::WorkerId>,
        requested_by: Option<UserId>,
        idempotency_key: Option<&str>,
    ) -> Result<OperationRecord> {
        let spec = NewOperation {
            kind: kind.into(),
            database_id,
            worker_id,
            requested_by,
        };
        spec.validate()?;
        if let Some(key) = idempotency_key {
            let existing: Option<String> =
                sqlx::query_scalar("SELECT record FROM operations WHERE idempotency_key=?")
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(storage)?;
            if let Some(v) = existing {
                return decode(v);
            }
        }
        let at = now();
        let rec = OperationRecord {
            id: OperationId::new_v7(),
            kind: kind.into(),
            state: "PENDING".into(),
            database_id,
            worker_id: spec.worker_id,
            tenant_id: None,
            requested_by,
            idempotency_key: idempotency_key.map(str::to_owned),
            progress: 0,
            error_code: None,
            error_message: None,
            result: Value::Null,
            created_at: at,
            updated_at: at,
            finished_at: None,
        };
        sqlx::query("INSERT INTO operations(id,database_id,kind,state,idempotency_key,created_at,record) VALUES(?,?,?,?,?,?,?)")
            .bind(rec.id.to_string()).bind(database_id.map(|v|v.to_string())).bind(kind).bind(&rec.state).bind(idempotency_key).bind(at.to_rfc3339()).bind(encode(&rec)?).execute(&self.pool).await.map_err(storage)?;
        Ok(rec)
    }
    pub async fn get_operation(&self, id: OperationId) -> Result<OperationRecord> {
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM operations WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        decode(v.ok_or_else(|| missing("operation"))?)
    }
    pub async fn list_operations(&self, limit: i64, offset: i64) -> Result<Vec<OperationRecord>> {
        let rows =
            sqlx::query("SELECT record FROM operations ORDER BY created_at DESC LIMIT ? OFFSET ?")
                .bind(limit.max(0))
                .bind(offset.max(0))
                .fetch_all(&self.pool)
                .await
                .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
    pub async fn update_operation(
        &self,
        id: OperationId,
        state: &str,
        progress: i16,
        error_code: Option<ErrorCode>,
        error_message: Option<&str>,
        result: Option<Value>,
    ) -> Result<OperationRecord> {
        if !crate::is_valid_operation_state(state) {
            return Err(err(ErrorCode::InvalidArgument, "invalid operation state"));
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let existing: Option<String> =
            sqlx::query_scalar("SELECT record FROM operations WHERE id=?")
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        let mut rec: OperationRecord = decode(existing.ok_or_else(|| missing("operation"))?)?;
        if rec.is_terminal() && rec.state != state {
            return Err(err(
                ErrorCode::InvalidArgument,
                "operation already terminal",
            ));
        }
        rec.state = state.into();
        rec.progress = progress.clamp(0, 100);
        rec.error_code = error_code;
        rec.error_message = error_message.map(str::to_owned);
        if let Some(value) = result {
            rec.result = value
        }
        rec.updated_at = now();
        if rec.is_terminal() {
            rec.finished_at = Some(now())
        }
        sqlx::query("UPDATE operations SET state=?,record=? WHERE id=?")
            .bind(state)
            .bind(encode(&rec)?)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(rec)
    }
    pub async fn enqueue_job(
        &self,
        kind: &str,
        payload: Value,
        priority: i32,
        run_after: Option<DateTime<Utc>>,
        idempotency_key: Option<&str>,
    ) -> Result<JobRecord> {
        if kind.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "job kind is empty"));
        }
        if let Some(key) = idempotency_key {
            let old: Option<String> =
                sqlx::query_scalar("SELECT record FROM jobs WHERE idempotency_key=?")
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(storage)?;
            if let Some(v) = old {
                return decode(v);
            }
        }
        let at = now();
        let rec = JobRecord {
            id: JobId::new_v7(),
            kind: kind.into(),
            payload,
            state: "READY".into(),
            priority,
            run_after: run_after.unwrap_or(at),
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: 5,
            last_error: None,
            idempotency_key: idempotency_key.map(str::to_owned),
            created_at: at,
            updated_at: at,
            finished_at: None,
        };
        let database_id = rec.payload.get("database_id").and_then(Value::as_str);
        sqlx::query("INSERT INTO jobs(id,database_id,kind,state,priority,run_after,lease_expires_at,idempotency_key,created_at,record) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(rec.id.to_string()).bind(database_id).bind(kind).bind(&rec.state).bind(priority).bind(rec.run_after.to_rfc3339()).bind(Option::<String>::None).bind(idempotency_key).bind(at.to_rfc3339()).bind(encode(&rec)?).execute(&self.pool).await.map_err(storage)?;
        Ok(rec)
    }
    pub async fn get_job(&self, id: JobId) -> Result<JobRecord> {
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM jobs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        decode(v.ok_or_else(|| missing("job"))?)
    }
    pub async fn lease_job(
        &self,
        owner: &str,
        ttl: Duration,
        kinds: &[&str],
    ) -> Result<Option<JobRecord>> {
        if owner.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "lease owner is empty"));
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let mut builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT record FROM jobs WHERE (state='READY' OR (state='LEASED' AND lease_expires_at<=",
        );
        builder.push_bind(now().to_rfc3339());
        builder.push(")) AND run_after<=");
        builder.push_bind(now().to_rfc3339());
        if !kinds.is_empty() {
            builder.push(" AND kind IN (");
            let mut separated = builder.separated(",");
            for kind in kinds {
                separated.push_bind(*kind);
            }
            separated.push_unseparated(")");
        }
        builder.push(" ORDER BY priority,run_after LIMIT 100");
        let rows = builder.build().fetch_all(&mut *tx).await.map_err(storage)?;
        for row in rows {
            let mut job: JobRecord = decode(row.try_get("record").map_err(storage)?)?;
            if job.state == "LEASED" && job.lease_expires_at.is_some_and(|v| v > now()) {
                continue;
            }
            if job.attempts >= job.max_attempts {
                job.state = "FAILED".into();
                job.finished_at = Some(now());
            } else {
                job.state = "LEASED".into();
                job.lease_owner = Some(owner.into());
                job.lease_expires_at =
                    Some(now() + chrono::Duration::from_std(ttl).map_err(storage)?);
                job.attempts += 1;
            }
            job.updated_at = now();
            sqlx::query("UPDATE jobs SET state=?,lease_expires_at=?,record=? WHERE id=?")
                .bind(&job.state)
                .bind(job.lease_expires_at.map(|v| v.to_rfc3339()))
                .bind(encode(&job)?)
                .bind(job.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            if job.state == "LEASED" {
                tx.commit().await.map_err(storage)?;
                return Ok(Some(job));
            }
        }
        tx.commit().await.map_err(storage)?;
        Ok(None)
    }
    pub async fn complete_job(
        &self,
        id: JobId,
        success: bool,
        error: Option<String>,
    ) -> Result<JobRecord> {
        let mut job = self.get_job(id).await?;
        if job.state != "LEASED" {
            return Err(err(ErrorCode::InvalidArgument, "job is not leased"));
        }
        job.state = if success {
            "DONE"
        } else if job.can_retry() {
            "READY"
        } else {
            "FAILED"
        }
        .into();
        job.last_error = error;
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.updated_at = now();
        if job.state == "READY" {
            job.run_after = now()
                + chrono::Duration::milliseconds(crate::job_retry_backoff_millis(job.attempts));
        } else {
            job.finished_at = Some(now())
        }
        sqlx::query(
            "UPDATE jobs SET state=?,run_after=?,lease_expires_at=NULL,record=? WHERE id=?",
        )
        .bind(&job.state)
        .bind(job.run_after.to_rfc3339())
        .bind(encode(&job)?)
        .bind(id.to_string())
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(job)
    }
    pub async fn complete_job_fenced(
        &self,
        id: JobId,
        lease_owner: &str,
        success: bool,
        error: Option<String>,
    ) -> Result<Option<JobRecord>> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let value: Option<String> = sqlx::query_scalar("SELECT record FROM jobs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
        let Some(value) = value else { return Ok(None) };
        let mut job: JobRecord = decode(value)?;
        if job.state != "LEASED"
            || job.lease_owner.as_deref() != Some(lease_owner)
            || job.lease_expires_at.is_none_or(|expiry| expiry <= now())
        {
            return Ok(None);
        }
        job.state = if success {
            "DONE"
        } else if job.can_retry() {
            "READY"
        } else {
            "FAILED"
        }
        .into();
        job.last_error = error;
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.updated_at = now();
        if job.state == "READY" {
            job.run_after = now()
                + chrono::Duration::milliseconds(crate::job_retry_backoff_millis(job.attempts));
        } else {
            job.finished_at = Some(now());
        }
        sqlx::query(
            "UPDATE jobs SET state=?,run_after=?,lease_expires_at=NULL,record=? WHERE id=?",
        )
        .bind(&job.state)
        .bind(job.run_after.to_rfc3339())
        .bind(encode(&job)?)
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(Some(job))
    }
    pub async fn recover_local_jobs(&self) -> Result<u64> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let rows = sqlx::query("SELECT record FROM jobs WHERE state='LEASED'")
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
        let mut count = 0;
        for row in rows {
            let mut job: JobRecord = decode(row.try_get("record").map_err(storage)?)?;
            job.state = if job.can_retry() { "READY" } else { "FAILED" }.into();
            job.lease_owner = None;
            job.lease_expires_at = None;
            job.updated_at = now();
            if job.state == "FAILED" {
                job.finished_at = Some(now())
            }
            sqlx::query("UPDATE jobs SET state=?,lease_expires_at=NULL,record=? WHERE id=?")
                .bind(&job.state)
                .bind(encode(&job)?)
                .bind(job.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            count += 1;
        }
        let operations =
            sqlx::query("SELECT record FROM operations WHERE state IN ('PENDING','RUNNING')")
                .fetch_all(&mut *tx)
                .await
                .map_err(storage)?;
        for row in operations {
            let mut operation: OperationRecord = decode(row.try_get("record").map_err(storage)?)?;
            let active: i64 = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE json_extract(record,'$.payload.operation_id')=? AND state IN ('READY','LEASED'))",
            ).bind(operation.id.to_string()).fetch_one(&mut *tx).await.map_err(storage)?;
            if active == 0 {
                operation.state = "FAILED".into();
                operation.error_code = Some(ErrorCode::InternalError);
                operation.error_message = Some("local job unavailable after recovery".into());
                operation.updated_at = now();
                operation.finished_at = Some(now());
                sqlx::query("UPDATE operations SET state='FAILED',record=? WHERE id=?")
                    .bind(encode(&operation)?)
                    .bind(operation.id.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
        }
        // 重启后没有本进程的打开句柄，运行态必须重新按需打开。
        let databases = sqlx::query("SELECT record FROM databases WHERE deleted_at IS NULL AND state NOT IN ('COLD','FAILED')")
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
        for row in databases {
            let mut db: DatabaseRecord = decode(row.try_get("record").map_err(storage)?)?;
            db.state = LifecycleState::Cold;
            db.updated_at = now();
            sqlx::query("UPDATE databases SET state='COLD',record=? WHERE id=?")
                .bind(encode(&db)?)
                .bind(db.id.to_string())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            sqlx::query("UPDATE catalog_version SET version=version+1 WHERE id=1")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(count)
    }
}
impl SqliteCatalog {
    /// 本地生命周期的数据库行、Operation、Job 和幂等键在同一写事务提交。
    pub async fn submit_operation_job(
        &self,
        req: LocalSubmission,
    ) -> Result<LocalSubmissionOutcome> {
        req.operation.validate()?;
        if req.job_kind.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "job kind is empty"));
        }
        if !req.job_payload.is_object() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "job payload must be an object",
            ));
        }
        if req.idempotency_key.is_some() != req.request_hash.is_some() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "idempotency key and request hash must be paired",
            ));
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        if let Some(key) = &req.idempotency_key {
            let row = sqlx::query(
                "SELECT request_hash,operation_record,job_record,database_record FROM idempotency_keys WHERE key=?",
            )
            .bind(key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
            if let Some(row) = row {
                let hash: String = row.try_get("request_hash").map_err(storage)?;
                if Some(&hash) != req.request_hash.as_ref() {
                    return Err(err(
                        ErrorCode::IdempotencyConflict,
                        "idempotency key has different request hash",
                    ));
                }
                let op_s: String = row.try_get("operation_record").map_err(storage)?;
                let job_s: String = row.try_get("job_record").map_err(storage)?;
                let operation: OperationRecord = decode(op_s)?;
                let job: JobRecord = decode(job_s)?;
                let database: Option<DatabaseRecord> = row
                    .try_get::<Option<String>, _>("database_record")
                    .map_err(storage)?
                    .map(decode)
                    .transpose()?;
                return Ok(LocalSubmissionOutcome {
                    database,
                    operation,
                    job,
                    replayed: true,
                });
            }
        }
        let database = if let Some(params) = req.create_database {
            let db = Self::database_record(params)?;
            sqlx::query("INSERT INTO databases(id,tenant_id,name,state,deleted_at,record) VALUES(?,?,?,?,?,?)")
                .bind(db.id.to_string()).bind(db.tenant_id.to_string()).bind(&db.name).bind(db.state.to_string()).bind(Option::<String>::None).bind(encode(&db)?).execute(&mut *tx).await.map_err(|e| sqlite_write_error(e, ErrorCode::DbAlreadyExists))?;
            Some(db)
        } else {
            None
        };
        let db_id = database
            .as_ref()
            .map(|d| d.id)
            .or(req.operation.database_id);
        let at = now();
        let operation = OperationRecord {
            id: OperationId::new_v7(),
            kind: req.operation.kind,
            state: "PENDING".into(),
            database_id: db_id,
            worker_id: req.operation.worker_id,
            tenant_id: database.as_ref().map(|d| d.tenant_id),
            requested_by: req.operation.requested_by,
            idempotency_key: req.idempotency_key.clone(),
            progress: 0,
            error_code: None,
            error_message: None,
            result: Value::Null,
            created_at: at,
            updated_at: at,
            finished_at: None,
        };
        let mut job_payload = req.job_payload;
        if let Value::Object(fields) = &mut job_payload {
            fields.insert(
                "operation_id".into(),
                Value::String(operation.id.to_string()),
            );
            if let Some(id) = db_id {
                fields.insert("database_id".into(), Value::String(id.to_string()));
            }
        }
        let job = JobRecord {
            id: JobId::new_v7(),
            kind: req.job_kind,
            payload: job_payload,
            state: "READY".into(),
            priority: req.priority,
            run_after: at,
            lease_owner: None,
            lease_expires_at: None,
            attempts: 0,
            max_attempts: 5,
            last_error: None,
            idempotency_key: req
                .idempotency_key
                .as_ref()
                .map(|k| format!("local-job:{k}")),
            created_at: at,
            updated_at: at,
            finished_at: None,
        };
        sqlx::query("INSERT INTO operations(id,database_id,kind,state,idempotency_key,created_at,record) VALUES(?,?,?,?,?,?,?)")
            .bind(operation.id.to_string()).bind(db_id.map(|v|v.to_string())).bind(&operation.kind).bind(&operation.state).bind(&operation.idempotency_key).bind(at.to_rfc3339()).bind(encode(&operation)?).execute(&mut *tx).await.map_err(storage)?;
        sqlx::query("INSERT INTO jobs(id,database_id,kind,state,priority,run_after,lease_expires_at,idempotency_key,created_at,record) VALUES(?,?,?,?,?,?,?,?,?,?)")
            .bind(job.id.to_string()).bind(db_id.map(|v|v.to_string())).bind(&job.kind).bind(&job.state).bind(job.priority).bind(at.to_rfc3339()).bind(Option::<String>::None).bind(&job.idempotency_key).bind(at.to_rfc3339()).bind(encode(&job)?).execute(&mut *tx).await.map_err(storage)?;
        if let (Some(key), Some(hash)) = (req.idempotency_key, req.request_hash) {
            sqlx::query("INSERT INTO idempotency_keys(key,request_hash,operation_id,job_id,operation_record,job_record,database_record) VALUES(?,?,?,?,?,?,?)")
                .bind(key).bind(hash).bind(operation.id.to_string()).bind(job.id.to_string()).bind(encode(&operation)?).bind(encode(&job)?).bind(database.as_ref().map(encode).transpose()?).execute(&mut *tx).await.map_err(storage)?;
        }
        if database.is_some() {
            sqlx::query("UPDATE catalog_version SET version=version+1 WHERE id=1")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        tx.commit().await.map_err(storage)?;
        Ok(LocalSubmissionOutcome {
            database,
            operation,
            job,
            replayed: false,
        })
    }
}
impl SqliteCatalog {
    pub async fn find_user_by_username(&self, username: &str) -> Result<Option<crate::UserRecord>> {
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM users WHERE username=?")
            .bind(username)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        v.map(decode).transpose()
    }
    pub async fn find_user(&self, id: UserId) -> Result<Option<crate::UserRecord>> {
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM users WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        v.map(decode).transpose()
    }
    pub async fn find_user_with_permissions(
        &self,
        subject: &str,
    ) -> Result<Option<(crate::UserRecord, Vec<String>)>> {
        // 一条语句取得当前用户状态和角色权限，避免认证与授权跨两个快照。
        let (by_id, lookup) = match subject.parse::<Uuid>() {
            Ok(id) => (true, id.to_string()),
            Err(_) => (false, subject.to_owned()),
        };
        let sql = if by_id {
            "SELECT u.record, (SELECT json_group_array(permission) FROM (SELECT DISTINCT p.permission FROM role_bindings b JOIN role_permissions p ON p.role_id=b.role_id WHERE b.user_id=u.id)) AS permissions FROM users u WHERE u.id=?"
        } else {
            "SELECT u.record, (SELECT json_group_array(permission) FROM (SELECT DISTINCT p.permission FROM role_bindings b JOIN role_permissions p ON p.role_id=b.role_id WHERE b.user_id=u.id)) AS permissions FROM users u WHERE u.username=?"
        };
        let acquire_started = stage_metrics_enabled().then(Instant::now);
        let acquired = self.read_pool.acquire().await;
        record_simple_stage("jwt_read_pool_acquire", acquire_started);
        let mut connection = acquired.map_err(storage)?;
        let fetch_started = stage_metrics_enabled().then(Instant::now);
        let fetched = sqlx::query(sql)
            .bind(lookup)
            .fetch_optional(&mut *connection)
            .await;
        record_simple_stage("jwt_sql_fetch", fetch_started);
        drop(connection);
        let row = fetched.map_err(storage)?;
        let Some(row) = row else { return Ok(None) };
        let user: crate::UserRecord = decode(row.try_get("record").map_err(storage)?)?;
        if !user.is_active() {
            return Ok(Some((user, Vec::new())));
        }
        let encoded: String = row.try_get("permissions").map_err(storage)?;
        let mut permissions: Vec<String> = serde_json::from_str(&encoded).map_err(storage)?;
        if user.is_superuser {
            permissions.push("*".into());
        }
        permissions.sort();
        permissions.dedup();
        Ok(Some((user, permissions)))
    }
    pub async fn create_user(&self, user: crate::NewUser) -> Result<crate::UserRecord> {
        if user.username.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "username is empty"));
        }
        let at = now();
        let rec = crate::UserRecord {
            id: UserId::new_v7(),
            tenant_id: user.tenant_id,
            username: user.username,
            display_name: user.display_name.unwrap_or_default(),
            password_hash: user.password_hash,
            oidc_subject: user.oidc_subject,
            email: user.email,
            status: "ACTIVE".into(),
            is_superuser: user.is_superuser,
            created_at: at,
            updated_at: at,
        };
        sqlx::query("INSERT INTO users(id,username,status,record) VALUES(?,?,?,?)")
            .bind(rec.id.to_string())
            .bind(&rec.username)
            .bind(&rec.status)
            .bind(encode(&rec)?)
            .execute(&self.pool)
            .await
            .map_err(|e| sqlite_write_error(e, ErrorCode::InvalidArgument))?;
        Ok(rec)
    }
    pub async fn list_users(&self, limit: i64, offset: i64) -> Result<Vec<crate::UserRecord>> {
        let rows = sqlx::query("SELECT record FROM users ORDER BY username LIMIT ? OFFSET ?")
            .bind(limit.max(0))
            .bind(offset.max(0))
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
    pub async fn create_api_token(
        &self,
        token: crate::NewApiToken,
    ) -> Result<crate::ApiTokenRecord> {
        if token.token_hash.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "token hash is empty"));
        }
        let database_id = token.database_id.ok_or_else(|| {
            err(
                ErrorCode::InvalidArgument,
                "API token must be bound to a database",
            )
        })?;
        if self.find_user(token.user_id).await?.is_none() {
            return Err(missing("user"));
        }
        let rec = crate::ApiTokenRecord {
            id: domain::ids::TokenId::new_v7(),
            user_id: token.user_id,
            name: token.name,
            token_hash: token.token_hash,
            tenant_id: token.tenant_id,
            database_id: Some(database_id),
            permissions: token.permissions,
            expires_at: token.expires_at,
            last_used_at: None,
            revoked_at: None,
            created_at: now(),
        };
        sqlx::query("INSERT INTO api_tokens(id,user_id,token_hash,revoked_at,expires_at,created_at,record,database_id) VALUES(?,?,?,?,?,?,?,?)")
            .bind(rec.id.to_string()).bind(rec.user_id.to_string()).bind(&rec.token_hash).bind(Option::<String>::None).bind(rec.expires_at.map(|v|v.to_rfc3339())).bind(rec.created_at.to_rfc3339()).bind(encode(&rec)?).bind(database_id.to_string()).execute(&self.pool).await.map_err(|e| sqlite_write_error(e, ErrorCode::InvalidArgument))?;
        Ok(rec)
    }
    pub async fn rotate_api_token(
        &self,
        token: crate::NewApiToken,
    ) -> Result<crate::ApiTokenRecord> {
        if token.token_hash.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "token hash is empty"));
        }
        let database_id = token.database_id.ok_or_else(|| {
            err(
                ErrorCode::InvalidArgument,
                "API token must be bound to a database",
            )
        })?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let existing = sqlx::query(
            "SELECT id,record FROM api_tokens WHERE database_id=? AND revoked_at IS NULL",
        )
        .bind(database_id.to_string())
        .fetch_all(&mut *tx)
        .await
        .map_err(storage)?;
        let revoked_at = now();
        for row in existing {
            let id: String = row.try_get("id").map_err(storage)?;
            let mut old: crate::ApiTokenRecord = decode(row.try_get("record").map_err(storage)?)?;
            old.revoked_at = Some(revoked_at);
            sqlx::query(
                "UPDATE api_tokens SET revoked_at=?,record=? WHERE id=? AND revoked_at IS NULL",
            )
            .bind(revoked_at.to_rfc3339())
            .bind(encode(&old)?)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }

        let rec = crate::ApiTokenRecord {
            id: TokenId::new_v7(),
            user_id: token.user_id,
            name: token.name,
            token_hash: token.token_hash,
            tenant_id: token.tenant_id,
            database_id: Some(database_id),
            permissions: token.permissions,
            expires_at: token.expires_at,
            last_used_at: None,
            revoked_at: None,
            created_at: now(),
        };
        sqlx::query("INSERT INTO api_tokens(id,user_id,token_hash,revoked_at,expires_at,created_at,record,database_id) VALUES(?,?,?,?,?,?,?,?)")
            .bind(rec.id.to_string()).bind(rec.user_id.to_string()).bind(&rec.token_hash).bind(Option::<String>::None).bind(rec.expires_at.map(|v|v.to_rfc3339())).bind(rec.created_at.to_rfc3339()).bind(encode(&rec)?).bind(database_id.to_string()).execute(&mut *tx).await.map_err(|e| sqlite_write_error(e, ErrorCode::InvalidArgument))?;
        tx.commit().await.map_err(storage)?;
        Ok(rec)
    }
    pub async fn find_user_by_token_hash(
        &self,
        hash: &str,
    ) -> Result<Option<crate::AuthenticatedToken>> {
        let v:Option<String>=sqlx::query_scalar("SELECT record FROM api_tokens WHERE token_hash=? AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at>?)")
            .bind(hash).bind(now().to_rfc3339()).fetch_optional(&self.pool).await.map_err(storage)?;
        let Some(v) = v else { return Ok(None) };
        let token: crate::ApiTokenRecord = decode(v)?;
        let Some(user) = self.find_user(token.user_id).await? else {
            return Ok(None);
        };
        if !user.is_active() {
            return Ok(None);
        }
        Ok(Some(crate::AuthenticatedToken { user, token }))
    }
    pub async fn revoke_api_token(&self, id: domain::ids::TokenId) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let v: Option<String> =
            sqlx::query_scalar("SELECT record FROM api_tokens WHERE id=? AND revoked_at IS NULL")
                .bind(id.to_string())
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage)?;
        let Some(v) = v else { return Ok(false) };
        let mut rec: crate::ApiTokenRecord = decode(v)?;
        rec.revoked_at = Some(now());
        sqlx::query(
            "UPDATE api_tokens SET revoked_at=?,record=? WHERE id=? AND revoked_at IS NULL",
        )
        .bind(rec.revoked_at.map(|v| v.to_rfc3339()))
        .bind(encode(&rec)?)
        .bind(id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(true)
    }
    pub async fn list_tokens_for_user(&self, id: UserId) -> Result<Vec<crate::ApiTokenRecord>> {
        let rows =
            sqlx::query("SELECT record FROM api_tokens WHERE user_id=? ORDER BY created_at DESC")
                .bind(id.to_string())
                .fetch_all(&self.pool)
                .await
                .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
    pub async fn touch_token_last_used(&self, id: domain::ids::TokenId) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM api_tokens WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage)?;
        let mut token: crate::ApiTokenRecord = decode(v.ok_or_else(|| missing("token"))?)?;
        token.last_used_at = Some(now());
        sqlx::query("UPDATE api_tokens SET record=? WHERE id=?")
            .bind(encode(&token)?)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(())
    }
    pub async fn resolve_permissions(&self, id: UserId) -> Result<Vec<String>> {
        let user = self.find_user(id).await?.ok_or_else(|| missing("user"))?;
        if !user.is_active() {
            return Ok(Vec::new());
        }
        let rows=sqlx::query("SELECT DISTINCT p.permission FROM role_bindings b JOIN role_permissions p ON p.role_id=b.role_id WHERE b.user_id=? ORDER BY p.permission")
            .bind(id.to_string()).fetch_all(&self.pool).await.map_err(storage)?;
        let mut permissions = Vec::new();
        for row in rows {
            permissions.push(row.try_get("permission").map_err(storage)?)
        }
        if user.is_superuser {
            permissions.push("*".into());
        }
        permissions.sort();
        permissions.dedup();
        Ok(permissions)
    }
    pub async fn append_audit(&self, entry: crate::AuditEntry) -> Result<i64> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let at = now();
        let rec = crate::AuditLogRecord {
            id: 0,
            actor_id: entry.actor_id,
            actor_name: entry.actor_name,
            tenant_id: entry.tenant_id,
            database_id: entry.database_id,
            action: entry.action,
            target_type: entry.target_type,
            target_id: entry.target_id,
            result: if entry.result.is_empty() {
                "SUCCESS".into()
            } else {
                entry.result
            },
            error_code: entry.error_code,
            source_ip: entry.source_ip,
            request_id: entry.request_id,
            detail: entry.detail,
            created_at: at,
        };
        let result = sqlx::query(
            "INSERT INTO audit_log(actor_id,action,result,created_at,record) VALUES(?,?,?,?,?)",
        )
        .bind(rec.actor_id.map(|v| v.to_string()))
        .bind(&rec.action)
        .bind(&rec.result)
        .bind(at.to_rfc3339())
        .bind(encode(&rec)?)
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        let id = result.last_insert_rowid();
        let mut saved = rec;
        saved.id = id;
        sqlx::query("UPDATE audit_log SET record=? WHERE id=?")
            .bind(encode(&saved)?)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(id)
    }
    pub async fn list_audit(
        &self,
        limit: i64,
        offset: i64,
        filter: crate::AuditFilter,
    ) -> Result<Vec<crate::AuditLogRecord>> {
        let rows = sqlx::query("SELECT record FROM audit_log ORDER BY id DESC")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        let mut records = Vec::new();
        for row in rows {
            let rec: crate::AuditLogRecord = decode(row.try_get("record").map_err(storage)?)?;
            if filter.actor_id.is_some_and(|v| Some(v) != rec.actor_id)
                || filter
                    .database_id
                    .is_some_and(|v| Some(v) != rec.database_id)
                || filter.tenant_id.is_some_and(|v| Some(v) != rec.tenant_id)
                || filter.action.as_ref().is_some_and(|v| v != &rec.action)
                || filter.result.as_ref().is_some_and(|v| v != &rec.result)
            {
                continue;
            }
            records.push(rec)
        }
        Ok(records
            .into_iter()
            .skip(offset.max(0) as usize)
            .take(limit.max(0) as usize)
            .collect())
    }
}
impl SqliteCatalog {
    pub async fn get_preference(&self, user: UserId, key: &str) -> Result<Option<Value>> {
        let s: Option<String> =
            sqlx::query_scalar("SELECT record FROM panel_preferences WHERE user_id=? AND key=?")
                .bind(user.to_string())
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(storage)?;
        s.map(|v| {
            let rec: crate::PreferenceRecord = decode(v)?;
            Ok(rec.value)
        })
        .transpose()
    }
    pub async fn set_preference(&self, user: UserId, key: &str, value: &Value) -> Result<()> {
        if key.trim().is_empty() {
            return Err(err(ErrorCode::InvalidArgument, "preference key is empty"));
        }
        let rec = crate::PreferenceRecord {
            user_id: user,
            key: key.into(),
            value: value.clone(),
            updated_at: now(),
        };
        sqlx::query("INSERT INTO panel_preferences(user_id,key,record) VALUES(?,?,?) ON CONFLICT(user_id,key) DO UPDATE SET record=excluded.record")
            .bind(user.to_string()).bind(key).bind(encode(&rec)?).execute(&self.pool).await.map_err(storage)?;
        Ok(())
    }
    pub async fn list_preferences(&self, user: UserId) -> Result<Vec<crate::PreferenceRecord>> {
        let rows = sqlx::query("SELECT record FROM panel_preferences WHERE user_id=? ORDER BY key")
            .bind(user.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
    pub async fn create_saved_query(
        &self,
        query: crate::NewSavedQuery,
    ) -> Result<crate::SavedQueryRecord> {
        if query.name.trim().is_empty() || query.sql.trim().is_empty() {
            return Err(err(
                ErrorCode::InvalidArgument,
                "saved query name or SQL is empty",
            ));
        }
        let at = now();
        let rec = crate::SavedQueryRecord {
            id: uuid::Uuid::now_v7(),
            user_id: query.user_id,
            database_id: query.database_id,
            name: query.name,
            sql: query.sql,
            description: query.description.unwrap_or_default(),
            tags: query.tags,
            created_at: at,
            updated_at: at,
        };
        sqlx::query("INSERT INTO saved_queries(id,user_id,updated_at,record) VALUES(?,?,?,?)")
            .bind(rec.id.to_string())
            .bind(rec.user_id.to_string())
            .bind(at.to_rfc3339())
            .bind(encode(&rec)?)
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(rec)
    }
    pub async fn list_saved_queries(
        &self,
        user: UserId,
        limit: i64,
    ) -> Result<Vec<crate::SavedQueryRecord>> {
        let rows = sqlx::query(
            "SELECT record FROM saved_queries WHERE user_id=? ORDER BY updated_at DESC LIMIT ?",
        )
        .bind(user.to_string())
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
    pub async fn delete_saved_query(&self, id: uuid::Uuid, user: UserId) -> Result<bool> {
        let rows = sqlx::query("DELETE FROM saved_queries WHERE id=? AND user_id=?")
            .bind(id.to_string())
            .bind(user.to_string())
            .execute(&self.pool)
            .await
            .map_err(storage)?
            .rows_affected();
        Ok(rows > 0)
    }
    pub async fn insert_slow_query(&self, query: crate::NewSlowQuery) -> Result<i64> {
        let at = now();
        let rec = crate::SlowQueryRecord {
            id: 0,
            database_id: query.database_id,
            worker_id: query.worker_id,
            session_id: query.session_id,
            fingerprint: query.fingerprint.unwrap_or_default(),
            sql_text: query.sql_text,
            duration_micros: query.duration_micros,
            rows_returned: query.rows_returned,
            error_code: query.error_code,
            created_at: at,
        };
        let id=sqlx::query("INSERT INTO slow_queries(database_id,duration_micros,created_at,record) VALUES(?,?,?,?)")
            .bind(rec.database_id.to_string()).bind(rec.duration_micros).bind(at.to_rfc3339()).bind(encode(&rec)?).execute(&self.pool).await.map_err(storage)?.last_insert_rowid();
        let mut saved = rec;
        saved.id = id;
        sqlx::query("UPDATE slow_queries SET record=? WHERE id=?")
            .bind(encode(&saved)?)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(id)
    }
    pub async fn list_slow_queries(
        &self,
        db: DatabaseId,
        limit: i64,
        min_duration_micros: i64,
    ) -> Result<Vec<crate::SlowQueryRecord>> {
        let rows=sqlx::query("SELECT record FROM slow_queries WHERE database_id=? AND duration_micros>=? ORDER BY created_at DESC LIMIT ?")
            .bind(db.to_string()).bind(min_duration_micros).bind(limit.max(0)).fetch_all(&self.pool).await.map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
}
impl SqliteCatalog {
    pub async fn insert_snapshot(
        &self,
        snapshot: domain::records::SnapshotRecord,
    ) -> Result<domain::records::SnapshotRecord> {
        if !crate::is_valid_snapshot_state(&snapshot.state) {
            return Err(err(ErrorCode::InvalidArgument, "invalid snapshot state"));
        }
        sqlx::query("INSERT INTO snapshots(id,database_id,state,created_at,record) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,record=excluded.record")
            .bind(snapshot.id.to_string()).bind(snapshot.database_id.to_string()).bind(&snapshot.state).bind(snapshot.created_at.to_rfc3339()).bind(encode(&snapshot)?).execute(&self.pool).await.map_err(storage)?;
        Ok(snapshot)
    }
    pub async fn latest_snapshot(
        &self,
        db: DatabaseId,
    ) -> Result<Option<domain::records::SnapshotRecord>> {
        let v:Option<String>=sqlx::query_scalar("SELECT record FROM snapshots WHERE database_id=? AND state='AVAILABLE' ORDER BY created_at DESC LIMIT 1").bind(db.to_string()).fetch_optional(&self.pool).await.map_err(storage)?;
        v.map(decode).transpose()
    }
    pub async fn list_snapshots(
        &self,
        db: DatabaseId,
        limit: i64,
    ) -> Result<Vec<domain::records::SnapshotRecord>> {
        let rows = sqlx::query(
            "SELECT record FROM snapshots WHERE database_id=? ORDER BY created_at DESC LIMIT ?",
        )
        .bind(db.to_string())
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
    pub async fn mark_snapshot_state(
        &self,
        id: domain::ids::SnapshotId,
        state: &str,
    ) -> Result<domain::records::SnapshotRecord> {
        if !crate::is_valid_snapshot_state(state) {
            return Err(err(ErrorCode::InvalidArgument, "invalid snapshot state"));
        }
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM snapshots WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        let mut rec: domain::records::SnapshotRecord =
            decode(v.ok_or_else(|| missing("snapshot"))?)?;
        rec.state = state.into();
        if state == "AVAILABLE" {
            rec.verified_at = Some(now())
        }
        sqlx::query("UPDATE snapshots SET state=?,record=? WHERE id=?")
            .bind(state)
            .bind(encode(&rec)?)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(rec)
    }
    /// 单机备份以 Operation UUID 作为 backup_jobs 主键，重试不会新增历史行。
    pub async fn ensure_backup_job_for_operation(
        &self,
        database_id: DatabaseId,
        operation_id: OperationId,
    ) -> Result<crate::BackupJobRecord> {
        let id = operation_id.into_uuid();
        let record = crate::BackupJobRecord {
            id,
            database_id,
            operation_id: Some(operation_id),
            kind: "BACKUP".into(),
            state: "PENDING".into(),
            snapshot_id: None,
            target_time: None,
            actual_point: None,
            bytes_transferred: 0,
            error_message: None,
            created_at: now(),
            finished_at: None,
        };
        sqlx::query("INSERT INTO backup_jobs(id,database_id,state,created_at,record) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING")
            .bind(id.to_string()).bind(database_id.to_string()).bind(&record.state)
            .bind(record.created_at.to_rfc3339()).bind(encode(&record)?)
            .execute(&self.pool).await.map_err(storage)?;
        let raw: String = sqlx::query_scalar("SELECT record FROM backup_jobs WHERE id=?")
            .bind(id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        let existing: crate::BackupJobRecord = decode(raw)?;
        if existing.database_id != database_id
            || existing.operation_id != Some(operation_id)
            || existing.kind != "BACKUP"
        {
            return Err(err(
                ErrorCode::InvalidArgument,
                "backup operation identity conflict",
            ));
        }
        Ok(existing)
    }

    pub async fn create_backup_job(
        &self,
        params: crate::CreateBackupJobParams,
    ) -> Result<crate::BackupJobRecord> {
        if !crate::is_valid_backup_job_kind(&params.kind) {
            return Err(err(ErrorCode::InvalidArgument, "invalid backup job kind"));
        }
        let rec = crate::BackupJobRecord {
            id: uuid::Uuid::now_v7(),
            database_id: params.database_id,
            operation_id: params.operation_id,
            kind: params.kind,
            state: "PENDING".into(),
            snapshot_id: params.snapshot_id,
            target_time: params.target_time,
            actual_point: None,
            bytes_transferred: 0,
            error_message: None,
            created_at: now(),
            finished_at: None,
        };
        sqlx::query(
            "INSERT INTO backup_jobs(id,database_id,state,created_at,record) VALUES(?,?,?,?,?)",
        )
        .bind(rec.id.to_string())
        .bind(rec.database_id.to_string())
        .bind(&rec.state)
        .bind(rec.created_at.to_rfc3339())
        .bind(encode(&rec)?)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(rec)
    }
    pub async fn update_backup_job_state(
        &self,
        id: uuid::Uuid,
        update: crate::BackupJobUpdate,
    ) -> Result<crate::BackupJobRecord> {
        if !crate::is_valid_backup_job_state(&update.state) {
            return Err(err(ErrorCode::InvalidArgument, "invalid backup job state"));
        }
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM backup_jobs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        let mut rec: crate::BackupJobRecord = decode(v.ok_or_else(|| missing("backup job"))?)?;
        rec.state = update.state;
        if let Some(v) = update.snapshot_id {
            rec.snapshot_id = Some(v)
        }
        if let Some(v) = update.actual_point {
            rec.actual_point = Some(v)
        }
        if let Some(v) = update.bytes_transferred {
            rec.bytes_transferred = v
        }
        if let Some(v) = update.error_message {
            rec.error_message = Some(v)
        }
        if matches!(rec.state.as_str(), "SUCCEEDED" | "FAILED" | "CANCELLED") {
            rec.finished_at = Some(now())
        }
        sqlx::query("UPDATE backup_jobs SET state=?,record=? WHERE id=?")
            .bind(&rec.state)
            .bind(encode(&rec)?)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(rec)
    }
    pub async fn list_backup_jobs(
        &self,
        db: DatabaseId,
        limit: i64,
    ) -> Result<Vec<crate::BackupJobRecord>> {
        let rows = sqlx::query(
            "SELECT record FROM backup_jobs WHERE database_id=? ORDER BY created_at DESC LIMIT ?",
        )
        .bind(db.to_string())
        .bind(limit.max(0))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.into_iter()
            .map(|r| decode(r.try_get("record").map_err(storage)?))
            .collect()
    }
}

#[async_trait::async_trait]
impl Metadata for SqliteCatalog {
    async fn health_check(&self) -> Result<()> {
        self.health_check().await
    }
}
#[async_trait::async_trait]
impl IdentityStore for SqliteCatalog {
    async fn find_user_by_username(&self, username: &str) -> Result<Option<UserRecord>> {
        self.find_user_by_username(username).await
    }
    async fn find_user(&self, id: UserId) -> Result<Option<UserRecord>> {
        self.find_user(id).await
    }
    async fn find_user_with_permissions(
        &self,
        subject: &str,
    ) -> Result<Option<(UserRecord, Vec<String>)>> {
        self.find_user_with_permissions(subject).await
    }
    async fn create_user(&self, user: NewUser) -> Result<UserRecord> {
        self.create_user(user).await
    }
    async fn list_users(&self, limit: i64, offset: i64) -> Result<Vec<UserRecord>> {
        self.list_users(limit, offset).await
    }
    async fn create_api_token(&self, token: NewApiToken) -> Result<ApiTokenRecord> {
        self.create_api_token(token).await
    }
    async fn rotate_api_token(&self, token: NewApiToken) -> Result<ApiTokenRecord> {
        self.rotate_api_token(token).await
    }
    async fn find_user_by_token_hash(&self, hash: &str) -> Result<Option<AuthenticatedToken>> {
        self.find_user_by_token_hash(hash).await
    }
    async fn revoke_api_token(&self, id: TokenId) -> Result<bool> {
        self.revoke_api_token(id).await
    }
    async fn list_tokens_for_user(&self, id: UserId) -> Result<Vec<ApiTokenRecord>> {
        self.list_tokens_for_user(id).await
    }
    async fn touch_token_last_used(&self, id: TokenId) -> Result<()> {
        self.touch_token_last_used(id).await
    }
    async fn resolve_permissions(&self, id: UserId) -> Result<Vec<String>> {
        self.resolve_permissions(id).await
    }
    async fn append_audit(&self, entry: AuditEntry) -> Result<i64> {
        self.append_audit(entry).await
    }
    async fn list_audit(
        &self,
        limit: i64,
        offset: i64,
        filter: AuditFilter,
    ) -> Result<Vec<AuditLogRecord>> {
        self.list_audit(limit, offset, filter).await
    }
}
#[async_trait::async_trait]
impl DatabaseStore for SqliteCatalog {
    async fn create_database(&self, params: CreateDatabaseParams) -> Result<DatabaseRecord> {
        self.create_database(params).await
    }
    async fn get_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        self.get_database(id).await
    }
    async fn get_database_by_name(
        &self,
        tenant: domain::ids::TenantId,
        name: &str,
    ) -> Result<DatabaseRecord> {
        self.get_database_by_name(tenant, name).await
    }
    async fn list_databases(&self, filter: DatabaseFilter) -> Result<Vec<DatabaseRecord>> {
        self.list_databases(filter).await
    }
    async fn soft_delete_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        self.soft_delete_database(id).await
    }
    async fn set_lifecycle_state(
        &self,
        id: DatabaseId,
        state: LifecycleState,
        owner_worker: Option<domain::ids::WorkerId>,
    ) -> Result<DatabaseRecord> {
        self.set_lifecycle_state(id, state, owner_worker).await
    }
    async fn current_catalog_version(&self) -> Result<i64> {
        self.current_catalog_version().await
    }
}
#[async_trait::async_trait]
impl TaskStore for SqliteCatalog {
    async fn create_operation(
        &self,
        kind: &str,
        database_id: Option<DatabaseId>,
        worker_id: Option<domain::ids::WorkerId>,
        requested_by: Option<UserId>,
        idempotency_key: Option<&str>,
    ) -> Result<OperationRecord> {
        self.create_operation(kind, database_id, worker_id, requested_by, idempotency_key)
            .await
    }
    async fn get_operation(&self, id: OperationId) -> Result<OperationRecord> {
        self.get_operation(id).await
    }
    async fn list_operations(&self, limit: i64, offset: i64) -> Result<Vec<OperationRecord>> {
        self.list_operations(limit, offset).await
    }
    async fn enqueue_job(
        &self,
        kind: &str,
        payload: Value,
        priority: i32,
        run_after: Option<DateTime<Utc>>,
        idempotency_key: Option<&str>,
    ) -> Result<JobRecord> {
        self.enqueue_job(kind, payload, priority, run_after, idempotency_key)
            .await
    }
    async fn lease_job(
        &self,
        owner: &str,
        ttl: Duration,
        kinds: &[&str],
    ) -> Result<Option<JobRecord>> {
        self.lease_job(owner, ttl, kinds).await
    }
    async fn complete_job(
        &self,
        id: JobId,
        success: bool,
        error: Option<String>,
    ) -> Result<JobRecord> {
        self.complete_job(id, success, error).await
    }
    async fn complete_job_fenced(
        &self,
        id: JobId,
        lease_owner: &str,
        success: bool,
        error: Option<String>,
    ) -> Result<Option<JobRecord>> {
        self.complete_job_fenced(id, lease_owner, success, error)
            .await
    }
    async fn get_job(&self, id: JobId) -> Result<JobRecord> {
        self.get_job(id).await
    }
}
#[async_trait::async_trait]
impl PanelStore for SqliteCatalog {
    async fn get_preference(&self, user: UserId, key: &str) -> Result<Option<Value>> {
        self.get_preference(user, key).await
    }
    async fn set_preference(&self, user: UserId, key: &str, value: &Value) -> Result<()> {
        self.set_preference(user, key, value).await
    }
    async fn list_preferences(&self, user: UserId) -> Result<Vec<PreferenceRecord>> {
        self.list_preferences(user).await
    }
    async fn create_saved_query(&self, query: NewSavedQuery) -> Result<SavedQueryRecord> {
        self.create_saved_query(query).await
    }
    async fn list_saved_queries(&self, user: UserId, limit: i64) -> Result<Vec<SavedQueryRecord>> {
        self.list_saved_queries(user, limit).await
    }
    async fn delete_saved_query(&self, id: Uuid, user: UserId) -> Result<bool> {
        self.delete_saved_query(id, user).await
    }
    async fn insert_slow_query(&self, query: NewSlowQuery) -> Result<i64> {
        self.insert_slow_query(query).await
    }
    async fn list_slow_queries(
        &self,
        db: DatabaseId,
        limit: i64,
        min_duration_micros: i64,
    ) -> Result<Vec<SlowQueryRecord>> {
        self.list_slow_queries(db, limit, min_duration_micros).await
    }
}
#[async_trait::async_trait]
impl BackupStore for SqliteCatalog {
    async fn insert_snapshot(&self, snapshot: SnapshotRecord) -> Result<SnapshotRecord> {
        self.insert_snapshot(snapshot).await
    }
    async fn latest_snapshot(&self, db: DatabaseId) -> Result<Option<SnapshotRecord>> {
        self.latest_snapshot(db).await
    }
    async fn list_snapshots(&self, db: DatabaseId, limit: i64) -> Result<Vec<SnapshotRecord>> {
        self.list_snapshots(db, limit).await
    }
    async fn mark_snapshot_state(
        &self,
        id: domain::ids::SnapshotId,
        state: &str,
    ) -> Result<SnapshotRecord> {
        self.mark_snapshot_state(id, state).await
    }
    async fn create_backup_job(&self, params: CreateBackupJobParams) -> Result<BackupJobRecord> {
        self.create_backup_job(params).await
    }
    async fn update_backup_job_state(
        &self,
        id: Uuid,
        update: BackupJobUpdate,
    ) -> Result<BackupJobRecord> {
        self.update_backup_job_state(id, update).await
    }
    async fn list_backup_jobs(&self, db: DatabaseId, limit: i64) -> Result<Vec<BackupJobRecord>> {
        self.list_backup_jobs(db, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn readonly_pool_observes_commits_during_parallel_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.db");
        let catalog = SqliteCatalog::connect(&path).await.unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new("parallel-read"))
            .await
            .unwrap();
        assert!(
            sqlx::query("UPDATE databases SET name='should-fail' WHERE id=?")
                .bind(db.id.to_string())
                .execute(&catalog.read_pool)
                .await
                .is_err()
        );

        let mut readers = Vec::new();
        for _ in 0..8 {
            let catalog = catalog.clone();
            readers.push(tokio::spawn(async move {
                for _ in 0..100 {
                    assert_eq!(catalog.get_database(db.id).await.unwrap().id, db.id);
                    let (pending, record) =
                        catalog.database_admission_snapshot(db.id).await.unwrap();
                    assert!(!pending && record.unwrap().id == db.id);
                }
            }));
        }
        for _ in 0..100 {
            catalog
                .append_audit(crate::AuditEntry::success("parallel-read"))
                .await
                .unwrap();
        }
        for reader in readers {
            reader.await.unwrap();
        }
        catalog.close().await.unwrap();
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        assert_eq!(reopened.get_database(db.id).await.unwrap().id, db.id);
        assert_eq!(
            reopened
                .list_audit(200, 0, crate::AuditFilter::default())
                .await
                .unwrap()
                .len(),
            100
        );
    }
    #[tokio::test]
    async fn admission_snapshot_keeps_database_and_pending_job_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let id = DatabaseId::new_v7();
        let (pending, record) = catalog.database_admission_snapshot(id).await.unwrap();
        assert!(!pending && record.is_none());
        let (pending, record) = catalog.database_query_snapshot(id).await.unwrap();
        assert!(!pending && record.is_none());

        let db = catalog
            .create_database(CreateDatabaseParams::new("admission-test"))
            .await
            .unwrap();
        let (pending, record) = catalog.database_admission_snapshot(db.id).await.unwrap();
        assert!(!pending);
        assert_eq!(record.unwrap().id, db.id);
        catalog
            .set_lifecycle_state(db.id, LifecycleState::Starting, None)
            .await
            .unwrap();
        catalog
            .set_lifecycle_state(db.id, LifecycleState::Warm, None)
            .await
            .unwrap();
        assert_eq!(
            catalog
                .database_admission_snapshot(db.id)
                .await
                .unwrap()
                .1
                .unwrap()
                .state,
            LifecycleState::Warm
        );
        let warm_db = catalog.get_database(db.id).await.unwrap();

        let job = catalog
            .enqueue_job(
                "DB_DELETE",
                serde_json::json!({"database_id": db.id.to_string()}),
                0,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(catalog.database_admission_snapshot(db.id).await.unwrap().0);
        let (pending, record) = catalog.database_query_snapshot(db.id).await.unwrap();
        assert!(pending);
        assert_eq!(record.unwrap().id, db.id);
        sqlx::query("UPDATE databases SET record='{invalid' WHERE id=?")
            .bind(db.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        assert!(catalog.database_admission_snapshot(db.id).await.unwrap().0);
        assert!(catalog.database_query_snapshot(db.id).await.is_err());
        sqlx::query("UPDATE databases SET record=? WHERE id=?")
            .bind(encode(&warm_db).unwrap())
            .bind(db.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE jobs SET state='LEASED' WHERE id=?")
            .bind(job.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        assert!(catalog.database_admission_snapshot(db.id).await.unwrap().0);
        sqlx::query("UPDATE jobs SET state='DONE' WHERE id=?")
            .bind(job.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        assert!(!catalog.database_admission_snapshot(db.id).await.unwrap().0);
    }
    #[tokio::test]
    async fn durable_submission_replays_without_duplicate_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.db");
        let catalog = SqliteCatalog::connect(&path).await.unwrap();
        let request = LocalSubmission {
            operation: NewOperation::new("CREATE_DB"),
            create_database: Some(CreateDatabaseParams::new("alpha")),
            job_kind: "CREATE_DB".into(),
            job_payload: serde_json::json!({"step":"create"}),
            priority: 0,
            idempotency_key: Some("key-1".into()),
            request_hash: Some("hash-1".into()),
        };
        let first = catalog.submit_operation_job(request.clone()).await.unwrap();
        assert!(!first.replayed);
        assert_eq!(
            first.job.payload["database_id"],
            first.database.as_ref().unwrap().id.to_string()
        );
        assert_eq!(
            first.job.payload["operation_id"],
            first.operation.id.to_string()
        );
        catalog
            .update_operation(first.operation.id, "RUNNING", 10, None, None, None)
            .await
            .unwrap();
        drop(catalog);
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        let replay = reopened.submit_operation_job(request).await.unwrap();
        assert!(replay.replayed);
        assert_eq!(first.operation.id, replay.operation.id);
        assert_eq!(first.job.id, replay.job.id);
        assert_eq!(replay.operation.state, "PENDING");
        assert_eq!(
            reopened
                .list_databases(DatabaseFilter::default())
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(reopened.get_job(first.job.id).await.unwrap().state, "READY");
    }
    #[tokio::test]
    async fn token_revocation_and_job_recovery_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.db");
        let catalog = SqliteCatalog::connect(&path).await.unwrap();
        let user = catalog.create_user(NewUser::new("alice")).await.unwrap();
        let database = catalog
            .create_database(CreateDatabaseParams::new("token-test-db"))
            .await
            .unwrap();
        let token = catalog
            .create_api_token(NewApiToken {
                user_id: user.id,
                name: "test".into(),
                token_hash: "sha256:123".into(),
                tenant_id: None,
                database_id: Some(database.id),
                permissions: Value::Array(vec![]),
                expires_at: None,
            })
            .await
            .unwrap();
        let job = catalog
            .enqueue_job("TEST", Value::Null, 0, None, None)
            .await
            .unwrap();
        let leased = catalog
            .lease_job("local", Duration::from_secs(60), &[])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(leased.id, job.id);
        drop(catalog);
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        assert!(reopened
            .find_user_by_token_hash(&token.token_hash)
            .await
            .unwrap()
            .is_some());
        assert!(reopened.revoke_api_token(token.id).await.unwrap());
        assert!(reopened
            .find_user_by_token_hash(&token.token_hash)
            .await
            .unwrap()
            .is_none());
        assert_eq!(reopened.recover_local_jobs().await.unwrap(), 1);
        assert_eq!(reopened.get_job(job.id).await.unwrap().state, "READY");
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    #[tokio::test]
    async fn concurrent_submission_has_one_operation_and_job() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let request = LocalSubmission {
            operation: NewOperation::new("CREATE_DB"),
            create_database: Some(CreateDatabaseParams::new("only-once")),
            job_kind: "CREATE_DB".into(),
            job_payload: serde_json::json!({}),
            priority: 0,
            idempotency_key: Some("same-key".into()),
            request_hash: Some("same-hash".into()),
        };
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let catalog = catalog.clone();
            let request = request.clone();
            tasks.push(tokio::spawn(async move {
                catalog.submit_operation_job(request).await.unwrap()
            }));
        }
        let mut ids = std::collections::HashSet::new();
        let mut first = 0;
        for task in tasks {
            let outcome = task.await.unwrap();
            ids.insert((outcome.operation.id, outcome.job.id));
            if !outcome.replayed {
                first += 1
            }
        }
        assert_eq!(ids.len(), 1);
        assert_eq!(first, 1);
        assert_eq!(
            catalog
                .list_databases(DatabaseFilter::default())
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    #[tokio::test]
    async fn pending_delete_blocks_open_and_stale_job_cannot_complete() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new("to-delete"))
            .await
            .unwrap();
        let job = catalog
            .enqueue_job(
                "DB_DELETE",
                serde_json::json!({"database_id":db.id.to_string()}),
                0,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(catalog.has_pending_mutation(db.id).await.unwrap());
        let leased = catalog
            .lease_job("owner-a", Duration::from_secs(30), &["DB_DELETE"])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(leased.id, job.id);
        assert!(catalog
            .complete_job_fenced(job.id, "owner-b", true, None)
            .await
            .unwrap()
            .is_none());
        assert!(catalog
            .complete_job_fenced(job.id, "owner-a", true, None)
            .await
            .unwrap()
            .is_some());
        assert!(!catalog.has_pending_mutation(db.id).await.unwrap());
    }
    #[tokio::test]
    async fn audit_record_id_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.db");
        let catalog = SqliteCatalog::connect(&path).await.unwrap();
        let id = catalog
            .append_audit(crate::AuditEntry::success("test"))
            .await
            .unwrap();
        drop(catalog);
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        let audit = reopened
            .list_audit(10, 0, crate::AuditFilter::default())
            .await
            .unwrap();
        assert_eq!(audit[0].id, id);
    }
}

#[cfg(test)]
mod permission_tests {
    use super::*;
    #[tokio::test]
    async fn jwt_query_snapshot_keeps_user_and_raw_database_in_one_read() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let mut user = catalog
            .create_user(NewUser::new("query-user"))
            .await
            .unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new("query-db"))
            .await
            .unwrap();
        sqlx::query("INSERT INTO role_bindings(id,user_id,role_id) VALUES(?,?,?)")
            .bind(Uuid::now_v7().to_string())
            .bind(user.id.to_string())
            .bind("00000000-0000-0000-0000-000000000012")
            .execute(catalog.pool())
            .await
            .unwrap();
        let job = catalog
            .enqueue_job(
                "DB_DELETE",
                serde_json::json!({"database_id":db.id.to_string()}),
                0,
                None,
                None,
            )
            .await
            .unwrap();
        let (found, (pending, raw)) = catalog
            .jwt_query_snapshot(&user.id.to_string().to_uppercase(), &db.id.to_string())
            .await
            .unwrap();
        assert!(pending);
        assert_eq!(found.unwrap().1, vec!["db:read", "db:write"]);
        assert_eq!(decode::<DatabaseRecord>(raw.unwrap()).unwrap().id, db.id);

        sqlx::query("UPDATE databases SET record='{invalid' WHERE id=?")
            .bind(db.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        let (_, (_, raw)) = catalog
            .jwt_query_snapshot(&user.username, &db.id.to_string())
            .await
            .unwrap();
        assert_eq!(raw.as_deref(), Some("{invalid"));

        sqlx::query("DELETE FROM role_bindings WHERE user_id=?")
            .bind(user.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        user.status = "DISABLED".into();
        sqlx::query("UPDATE users SET status=?, record=? WHERE id=?")
            .bind(&user.status)
            .bind(encode(&user).unwrap())
            .bind(user.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        let (found, (pending, _)) = catalog
            .jwt_query_snapshot(&user.username, &db.id.to_string())
            .await
            .unwrap();
        assert!(pending);
        assert!(!found.unwrap().0.is_active());
        assert!(catalog
            .jwt_query_snapshot("missing", &db.id.to_string())
            .await
            .unwrap()
            .0
            .is_none());
        sqlx::query("UPDATE jobs SET state='DONE' WHERE id=?")
            .bind(job.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn jwt_subject_reads_current_user_and_permissions_together() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let mut user = catalog
            .create_user(NewUser::new("jwt-viewer"))
            .await
            .unwrap();
        sqlx::query("INSERT INTO role_bindings(id,user_id,role_id) VALUES(?,?,?)")
            .bind(Uuid::now_v7().to_string())
            .bind(user.id.to_string())
            .bind("00000000-0000-0000-0000-000000000013")
            .execute(catalog.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO role_bindings(id,user_id,role_id) VALUES(?,?,?)")
            .bind(Uuid::now_v7().to_string())
            .bind(user.id.to_string())
            .bind("00000000-0000-0000-0000-000000000013")
            .execute(catalog.pool())
            .await
            .unwrap();
        for subject in [
            user.id.to_string(),
            user.id.to_string().to_uppercase(),
            user.username.clone(),
        ] {
            let (found, permissions) = catalog
                .find_user_with_permissions(&subject)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(found.id, user.id);
            assert_eq!(permissions, vec!["db:read"]);
        }
        sqlx::query("DELETE FROM role_bindings WHERE user_id=?")
            .bind(user.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        assert!(catalog
            .find_user_with_permissions(&user.username)
            .await
            .unwrap()
            .unwrap()
            .1
            .is_empty());
        user.status = "DISABLED".into();
        sqlx::query("UPDATE users SET status=?,record=? WHERE id=?")
            .bind(&user.status)
            .bind(encode(&user).unwrap())
            .bind(user.id.to_string())
            .execute(catalog.pool())
            .await
            .unwrap();
        let (found, permissions) = catalog
            .find_user_with_permissions(&user.username)
            .await
            .unwrap()
            .unwrap();
        assert!(!found.is_active());
        assert!(permissions.is_empty());
        assert!(catalog
            .find_user_with_permissions("missing-user")
            .await
            .unwrap()
            .is_none());
        let admin = catalog
            .create_user(NewUser {
                is_superuser: true,
                ..NewUser::new("jwt-admin")
            })
            .await
            .unwrap();
        assert_eq!(
            catalog
                .find_user_with_permissions(&admin.id.to_string())
                .await
                .unwrap()
                .unwrap()
                .1,
            vec!["*"]
        );
    }
    #[tokio::test]
    async fn builtin_roles_match_local_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let viewer = catalog.create_user(NewUser::new("viewer")).await.unwrap();
        sqlx::query("INSERT INTO role_bindings(id,user_id,role_id) VALUES(?,?,?)")
            .bind(uuid::Uuid::now_v7().to_string())
            .bind(viewer.id.to_string())
            .bind("00000000-0000-0000-0000-000000000013")
            .execute(catalog.pool())
            .await
            .unwrap();
        assert_eq!(
            catalog.resolve_permissions(viewer.id).await.unwrap(),
            vec!["db:read"]
        );
        let dba = catalog.create_user(NewUser::new("dba")).await.unwrap();
        sqlx::query("INSERT INTO role_bindings(id,user_id,role_id) VALUES(?,?,?)")
            .bind(uuid::Uuid::now_v7().to_string())
            .bind(dba.id.to_string())
            .bind("00000000-0000-0000-0000-000000000011")
            .execute(catalog.pool())
            .await
            .unwrap();
        let permissions = catalog.resolve_permissions(dba.id).await.unwrap();
        assert!(permissions.contains(&"db:write".into()));
        assert!(permissions.contains(&"db:admin".into()));
        assert!(!permissions.contains(&"worker:admin".into()));
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    #[tokio::test]
    async fn migration_backfills_scopes_preserves_latest_and_enforces_unique_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old-metadata.db");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();

        // Apply only the released schema. The production 0001 migration stays untouched.
        let all_migrations = sqlx::migrate!("./sqlite_migrations");
        let old_migrations = sqlx::migrate::Migrator {
            migrations: std::borrow::Cow::Owned(vec![all_migrations
                .iter()
                .find(|migration| migration.version == 1)
                .unwrap()
                .clone()]),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        old_migrations.run(&pool).await.unwrap();

        let old_catalog = SqliteCatalog {
            pool: pool.clone(),
            read_pool: pool.clone(),
        };
        let user = old_catalog
            .create_user(NewUser::new("migration-token-user"))
            .await
            .unwrap();
        let database = old_catalog
            .create_database(CreateDatabaseParams::new("migration-token-db"))
            .await
            .unwrap();
        let other_database = old_catalog
            .create_database(CreateDatabaseParams::new("migration-other-token-db"))
            .await
            .unwrap();

        let before = Utc::now() - chrono::Duration::days(2);
        let newest = Utc::now() - chrono::Duration::days(1);
        let records = [
            crate::ApiTokenRecord {
                id: TokenId::new_v7(),
                user_id: user.id,
                name: "oldest".into(),
                token_hash: "migration-oldest-hash".into(),
                tenant_id: None,
                database_id: Some(database.id),
                permissions: serde_json::json!(["db:read"]),
                expires_at: None,
                last_used_at: None,
                revoked_at: None,
                created_at: before,
            },
            crate::ApiTokenRecord {
                id: TokenId::new_v7(),
                user_id: user.id,
                name: "latest".into(),
                token_hash: "migration-latest-hash".into(),
                tenant_id: None,
                database_id: Some(database.id),
                permissions: serde_json::json!(["db:read"]),
                expires_at: None,
                last_used_at: None,
                revoked_at: None,
                created_at: newest,
            },
            crate::ApiTokenRecord {
                id: TokenId::new_v7(),
                user_id: user.id,
                name: "other database".into(),
                token_hash: "migration-other-hash".into(),
                tenant_id: None,
                database_id: Some(other_database.id),
                permissions: serde_json::json!(["db:read"]),
                expires_at: None,
                last_used_at: None,
                revoked_at: None,
                created_at: Utc::now(),
            },
            crate::ApiTokenRecord {
                id: TokenId::new_v7(),
                user_id: user.id,
                name: "legacy unscoped".into(),
                token_hash: "migration-null-hash".into(),
                tenant_id: None,
                database_id: None,
                permissions: serde_json::json!(["db:read"]),
                expires_at: None,
                last_used_at: None,
                revoked_at: None,
                created_at: Utc::now() + chrono::Duration::days(1),
            },
        ];
        for token in &records {
            sqlx::query("INSERT INTO api_tokens(id,user_id,token_hash,revoked_at,expires_at,created_at,record) VALUES(?,?,?,NULL,NULL,?,?)")
                .bind(token.id.to_string())
                .bind(token.user_id.to_string())
                .bind(&token.token_hash)
                .bind(token.created_at.to_rfc3339())
                .bind(encode(token).unwrap())
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;

        // Opening through the normal path applies 0002 to this real old-format database.
        let catalog = SqliteCatalog::connect(&path).await.unwrap();
        let tokens = catalog.list_tokens_for_user(user.id).await.unwrap();
        let oldest = tokens
            .iter()
            .find(|token| token.id == records[0].id)
            .unwrap();
        let latest = tokens
            .iter()
            .find(|token| token.id == records[1].id)
            .unwrap();
        let other = tokens
            .iter()
            .find(|token| token.id == records[2].id)
            .unwrap();
        let legacy = tokens
            .iter()
            .find(|token| token.id == records[3].id)
            .unwrap();
        assert!(oldest.is_revoked());
        assert!(latest.revoked_at.is_none());
        assert!(other.revoked_at.is_none());
        assert!(legacy.revoked_at.is_none());
        assert_eq!(legacy.database_id, None);

        let (revoked_at, record, database_id): (String, String, String) =
            sqlx::query_as("SELECT revoked_at,record,database_id FROM api_tokens WHERE id=?")
                .bind(records[0].id.to_string())
                .fetch_one(catalog.pool())
                .await
                .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&record).unwrap()["revoked_at"],
            revoked_at
        );
        assert_eq!(database_id, database.id.to_string());
        let other_database_id: String =
            sqlx::query_scalar("SELECT database_id FROM api_tokens WHERE id=?")
                .bind(records[2].id.to_string())
                .fetch_one(catalog.pool())
                .await
                .unwrap();
        assert_eq!(other_database_id, other_database.id.to_string());
        let null_database_id: Option<String> =
            sqlx::query_scalar("SELECT database_id FROM api_tokens WHERE id=?")
                .bind(records[3].id.to_string())
                .fetch_one(catalog.pool())
                .await
                .unwrap();
        assert!(null_database_id.is_none());

        let duplicate_create = || NewApiToken {
            user_id: user.id,
            name: "duplicate".into(),
            token_hash: "fresh-migration-conflict-hash".into(),
            tenant_id: None,
            database_id: Some(database.id),
            permissions: serde_json::json!(["db:read"]),
            expires_at: None,
        };
        assert_eq!(
            catalog
                .create_api_token(duplicate_create())
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );

        // A failed replacement must roll back revocation of the current token.
        let failed_rotation = NewApiToken {
            user_id: user.id,
            name: "duplicate token hash".into(),
            token_hash: records[1].token_hash.clone(),
            tenant_id: None,
            database_id: Some(database.id),
            permissions: serde_json::json!(["db:read"]),
            expires_at: None,
        };
        assert!(catalog.rotate_api_token(failed_rotation).await.is_err());
        assert!(catalog
            .find_user_by_token_hash(&records[1].token_hash)
            .await
            .unwrap()
            .is_some());
        let missing_user_rotation = NewApiToken {
            user_id: UserId::new_v7(),
            name: "missing user".into(),
            token_hash: "missing-user-rotation-hash".into(),
            tenant_id: None,
            database_id: Some(database.id),
            permissions: serde_json::json!(["db:read"]),
            expires_at: None,
        };
        assert!(catalog
            .rotate_api_token(missing_user_rotation)
            .await
            .is_err());
        assert!(catalog
            .find_user_by_token_hash(&records[1].token_hash)
            .await
            .unwrap()
            .is_some());

        drop(catalog);
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        assert_eq!(
            reopened
                .create_api_token(duplicate_create())
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidArgument
        );
        let reopened_tokens = reopened.list_tokens_for_user(user.id).await.unwrap();
        assert!(reopened_tokens
            .iter()
            .any(|token| token.id == records[1].id && !token.is_revoked()));
        assert!(reopened_tokens
            .iter()
            .any(|token| token.id == records[2].id && !token.is_revoked()));
        assert!(reopened_tokens
            .iter()
            .any(|token| token.id == records[3].id && !token.is_revoked()));
    }

    #[tokio::test]
    async fn public_metadata_tables_can_read_their_records() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let user = catalog
            .create_user(NewUser::new("metadata-user"))
            .await
            .unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new("metadata-db"))
            .await
            .unwrap();
        let token = catalog
            .create_api_token(NewApiToken {
                user_id: user.id,
                name: "t".into(),
                token_hash: "hash-t".into(),
                tenant_id: None,
                database_id: Some(db.id),
                permissions: Value::Array(vec![]),
                expires_at: None,
            })
            .await
            .unwrap();
        assert_eq!(
            catalog.list_tokens_for_user(user.id).await.unwrap()[0].id,
            token.id
        );
        catalog.touch_token_last_used(token.id).await.unwrap();
        assert!(catalog.list_tokens_for_user(user.id).await.unwrap()[0]
            .last_used_at
            .is_some());
        catalog
            .set_preference(user.id, "theme", &serde_json::json!("dark"))
            .await
            .unwrap();
        assert_eq!(
            catalog.list_preferences(user.id).await.unwrap()[0].value,
            "dark"
        );
        let query = catalog
            .create_saved_query(NewSavedQuery {
                user_id: user.id,
                database_id: Some(db.id),
                name: "q".into(),
                sql: "SELECT 1".into(),
                description: None,
                tags: vec![],
            })
            .await
            .unwrap();
        assert_eq!(
            catalog.list_saved_queries(user.id, 10).await.unwrap()[0].id,
            query.id
        );
        let slow = catalog
            .insert_slow_query(NewSlowQuery {
                database_id: db.id,
                worker_id: None,
                session_id: None,
                fingerprint: None,
                sql_text: "SELECT 1".into(),
                duration_micros: 101,
                rows_returned: 1,
                error_code: None,
            })
            .await
            .unwrap();
        assert_eq!(
            catalog.list_slow_queries(db.id, 10, 100).await.unwrap()[0].id,
            slow
        );
        let snapshot = SnapshotRecord {
            id: domain::ids::SnapshotId::new_v7(),
            database_id: db.id,
            base_lsn: domain::wal::Lsn::new(0),
            checksum: "abc".into(),
            size_bytes: 42,
            object_key: "objects/snap".into(),
            compression: "zstd".into(),
            owner_epoch: domain::wal::OwnerEpoch::new(0),
            engine_version: "local".into(),
            schema_version: 0,
            state: "PENDING".into(),
            created_at: now(),
            verified_at: None,
        };
        catalog.insert_snapshot(snapshot.clone()).await.unwrap();
        catalog
            .mark_snapshot_state(snapshot.id, "AVAILABLE")
            .await
            .unwrap();
        assert_eq!(
            catalog.latest_snapshot(db.id).await.unwrap().unwrap().id,
            snapshot.id
        );
        assert_eq!(catalog.list_snapshots(db.id, 10).await.unwrap().len(), 1);
        let backup = catalog
            .create_backup_job(CreateBackupJobParams {
                database_id: db.id,
                kind: "BACKUP".into(),
                operation_id: None,
                snapshot_id: None,
                target_time: None,
            })
            .await
            .unwrap();
        let done = catalog
            .update_backup_job_state(
                backup.id,
                BackupJobUpdate {
                    state: "SUCCEEDED".into(),
                    snapshot_id: Some(snapshot.id.to_string()),
                    actual_point: None,
                    bytes_transferred: Some(42),
                    error_message: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(done.bytes_transferred, 42);
        assert_eq!(
            catalog.list_backup_jobs(db.id, 10).await.unwrap()[0].id,
            backup.id
        );
        assert!(catalog.revoke_api_token(token.id).await.unwrap());
        assert!(catalog.list_tokens_for_user(user.id).await.unwrap()[0].is_revoked());
    }

    #[tokio::test]
    async fn concurrent_token_create_and_rotation_leave_one_active_token_per_database() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SqliteCatalog::connect(dir.path().join("metadata.db"))
            .await
            .unwrap();
        let user = catalog
            .create_user(NewUser::new("token-race-user"))
            .await
            .unwrap();
        let database = catalog
            .create_database(CreateDatabaseParams::new("token-race-db"))
            .await
            .unwrap();
        let other_database = catalog
            .create_database(CreateDatabaseParams::new("token-race-other-db"))
            .await
            .unwrap();
        let make_token = |database_id, hash: String| NewApiToken {
            user_id: user.id,
            name: "race".into(),
            token_hash: hash,
            tenant_id: None,
            database_id: Some(database_id),
            permissions: serde_json::json!(["db:read"]),
            expires_at: None,
        };
        let other = catalog
            .create_api_token(make_token(other_database.id, "other-db-token".into()))
            .await
            .unwrap();

        let (a, b) = tokio::join!(
            catalog.create_api_token(make_token(database.id, "race-create-a".into())),
            catalog.create_api_token(make_token(database.id, "race-create-b".into())),
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let tokens = catalog.list_tokens_for_user(user.id).await.unwrap();
        assert_eq!(
            tokens
                .iter()
                .filter(|token| token.database_id == Some(database.id) && !token.is_revoked())
                .count(),
            1
        );

        let (a, b) = tokio::join!(
            catalog.rotate_api_token(make_token(database.id, "race-rotate-a".into())),
            catalog.rotate_api_token(make_token(database.id, "race-rotate-b".into())),
        );
        assert!(a.is_ok() && b.is_ok());
        let tokens = catalog.list_tokens_for_user(user.id).await.unwrap();
        assert_eq!(
            tokens
                .iter()
                .filter(|token| token.database_id == Some(database.id) && !token.is_revoked())
                .count(),
            1
        );
        assert_eq!(
            tokens
                .iter()
                .filter(|token| token.database_id == Some(database.id) && token.is_revoked())
                .count(),
            2
        );
        assert!(tokens
            .iter()
            .any(|token| token.id == other.id && !token.is_revoked()));
    }
}

#[cfg(test)]
mod local_backup_idempotency_tests {
    use super::*;

    #[tokio::test]
    async fn backup_record_uses_stable_operation_id_across_retries_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metadata.db");
        let catalog = SqliteCatalog::connect(&path).await.unwrap();
        let db = catalog
            .create_database(CreateDatabaseParams::new("backup-test"))
            .await
            .unwrap();
        let op = OperationId::new_v7();
        let first = catalog
            .ensure_backup_job_for_operation(db.id, op)
            .await
            .unwrap();
        let again = catalog
            .ensure_backup_job_for_operation(db.id, op)
            .await
            .unwrap();
        assert_eq!(first.id, op.into_uuid());
        assert_eq!(first, again);
        assert_eq!(catalog.list_backup_jobs(db.id, 10).await.unwrap().len(), 1);
        drop(catalog);
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        let after_restart = reopened
            .ensure_backup_job_for_operation(db.id, op)
            .await
            .unwrap();
        assert_eq!(first, after_restart);
        assert_eq!(reopened.list_backup_jobs(db.id, 10).await.unwrap().len(), 1);
    }
}
