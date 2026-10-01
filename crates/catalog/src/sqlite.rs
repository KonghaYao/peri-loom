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
use std::{path::Path, time::Duration};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct SqliteCatalog {
    pool: SqlitePool,
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
            return err(ErrorCode::InvalidArgument, format!("SQLite metadata reference: {database}"));
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
            return Err(storage(format!("metadata integrity check failed: {integrity}")));
        }
        Ok(Self { pool })
    }
    pub async fn close(self) -> Result<()> {
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
    pub async fn has_unfinished_jobs(&self) -> Result<bool> {
        let pending: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE state IN ('READY','LEASED')) OR EXISTS(SELECT 1 FROM operations WHERE state IN ('PENDING','RUNNING'))",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        Ok(pending != 0)
    }
    async fn bump_version(&self) -> Result<()> {
        sqlx::query("UPDATE catalog_version SET version=version+1 WHERE id=1")
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(())
    }
    pub async fn create_database(&self, params: CreateDatabaseParams) -> Result<DatabaseRecord> {
        let record = Self::database_record(params)?;
        self.insert_database(&record).await?;
        self.bump_version().await?;
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
    async fn insert_database(&self, db: &DatabaseRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO databases(id,tenant_id,name,state,deleted_at,record) VALUES(?,?,?,?,?,?)",
        )
        .bind(db.id.to_string())
        .bind(db.tenant_id.to_string())
        .bind(&db.name)
        .bind(db.state.to_string())
        .bind(db.deleted_at.map(|v| v.to_rfc3339()))
        .bind(encode(db)?)
        .execute(&self.pool)
        .await
        .map_err(|e| sqlite_write_error(e, ErrorCode::DbAlreadyExists))?;
        Ok(())
    }
    pub async fn get_database(&self, id: DatabaseId) -> Result<DatabaseRecord> {
        let value: Option<String> = sqlx::query_scalar("SELECT record FROM databases WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
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
        let mut db = self.get_database(id).await?;
        if db.deleted_at.is_none() {
            db.deleted_at = Some(now());
            db.updated_at = now();
            sqlx::query("UPDATE databases SET deleted_at=?,record=? WHERE id=?")
                .bind(db.deleted_at.map(|v| v.to_rfc3339()))
                .bind(encode(&db)?)
                .bind(id.to_string())
                .execute(&self.pool)
                .await
                .map_err(storage)?;
            self.bump_version().await?;
        }
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
        let mut rec = self.get_operation(id).await?;
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
            .execute(&self.pool)
            .await
            .map_err(storage)?;
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
            for kind in kinds { separated.push_bind(*kind); }
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
        if job.state != "LEASED" || job.lease_owner.as_deref() != Some(lease_owner)
            || job.lease_expires_at.is_none_or(|expiry| expiry <= now()) {
            return Ok(None);
        }
        job.state = if success { "DONE" } else if job.can_retry() { "READY" } else { "FAILED" }.into();
        job.last_error = error;
        job.lease_owner = None;
        job.lease_expires_at = None;
        job.updated_at = now();
        if job.state == "READY" {
            job.run_after = now() + chrono::Duration::milliseconds(crate::job_retry_backoff_millis(job.attempts));
        } else {
            job.finished_at = Some(now());
        }
        sqlx::query("UPDATE jobs SET state=?,run_after=?,lease_expires_at=NULL,record=? WHERE id=?")
            .bind(&job.state).bind(job.run_after.to_rfc3339()).bind(encode(&job)?).bind(id.to_string())
            .execute(&mut *tx).await.map_err(storage)?;
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
                .bind(db.id.to_string()).bind(db.tenant_id.to_string()).bind(&db.name).bind(db.state.to_string()).bind(Option::<String>::None).bind(encode(&db)?).execute(&mut *tx).await.map_err(storage)?;
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
            fields.insert("operation_id".into(), Value::String(operation.id.to_string()));
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
            .map_err(storage)?;
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
        if token.token_hash.trim().is_empty() { return Err(err(ErrorCode::InvalidArgument, "token hash is empty")); }
        if self.find_user(token.user_id).await?.is_none() {
            return Err(missing("user"));
        }
        let rec = crate::ApiTokenRecord {
            id: domain::ids::TokenId::new_v7(),
            user_id: token.user_id,
            name: token.name,
            token_hash: token.token_hash,
            tenant_id: token.tenant_id,
            database_id: token.database_id,
            permissions: token.permissions,
            expires_at: token.expires_at,
            last_used_at: None,
            revoked_at: None,
            created_at: now(),
        };
        sqlx::query("INSERT INTO api_tokens(id,user_id,token_hash,revoked_at,expires_at,record) VALUES(?,?,?,?,?,?)")
            .bind(rec.id.to_string()).bind(rec.user_id.to_string()).bind(&rec.token_hash).bind(Option::<String>::None).bind(rec.expires_at.map(|v|v.to_rfc3339())).bind(encode(&rec)?).execute(&self.pool).await.map_err(storage)?;
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
        let v: Option<String> =
            sqlx::query_scalar("SELECT record FROM api_tokens WHERE id=? AND revoked_at IS NULL")
                .bind(id.to_string())
                .fetch_optional(&self.pool)
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
        .execute(&self.pool)
        .await
        .map_err(storage)?;
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
        let v: Option<String> = sqlx::query_scalar("SELECT record FROM api_tokens WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        let mut token: crate::ApiTokenRecord = decode(v.ok_or_else(|| missing("token"))?)?;
        token.last_used_at = Some(now());
        sqlx::query("UPDATE api_tokens SET record=? WHERE id=?")
            .bind(encode(&token)?)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(storage)?;
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
        if user.is_superuser { permissions.push("*".into()); }
        permissions.sort(); permissions.dedup();
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
    async fn create_user(&self, user: NewUser) -> Result<UserRecord> {
        self.create_user(user).await
    }
    async fn list_users(&self, limit: i64, offset: i64) -> Result<Vec<UserRecord>> {
        self.list_users(limit, offset).await
    }
    async fn create_api_token(&self, token: NewApiToken) -> Result<ApiTokenRecord> {
        self.create_api_token(token).await
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
    async fn complete_job_fenced(&self, id: JobId, lease_owner: &str, success: bool, error: Option<String>) -> Result<Option<JobRecord>> {
        self.complete_job_fenced(id, lease_owner, success, error).await
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
        drop(catalog);
        let reopened = SqliteCatalog::connect(&path).await.unwrap();
        let replay = reopened.submit_operation_job(request).await.unwrap();
        assert!(replay.replayed);
        assert_eq!(first.operation.id, replay.operation.id);
        assert_eq!(first.job.id, replay.job.id);
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
        let token = catalog
            .create_api_token(NewApiToken {
                user_id: user.id,
                name: "test".into(),
                token_hash: "sha256:123".into(),
                tenant_id: None,
                database_id: None,
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
        let dir=tempfile::tempdir().unwrap();
        let catalog=SqliteCatalog::connect(dir.path().join("metadata.db")).await.unwrap();
        let request=LocalSubmission{operation:NewOperation::new("CREATE_DB"),create_database:Some(CreateDatabaseParams::new("only-once")),job_kind:"CREATE_DB".into(),job_payload:serde_json::json!({}),priority:0,idempotency_key:Some("same-key".into()),request_hash:Some("same-hash".into())};
        let mut tasks=Vec::new();for _ in 0..100 {let catalog=catalog.clone();let request=request.clone();tasks.push(tokio::spawn(async move{catalog.submit_operation_job(request).await.unwrap()}));}
        let mut ids=std::collections::HashSet::new();let mut first=0;
        for task in tasks {let outcome=task.await.unwrap();ids.insert((outcome.operation.id,outcome.job.id));if !outcome.replayed{first+=1}}
        assert_eq!(ids.len(),1);assert_eq!(first,1);
        assert_eq!(catalog.list_databases(DatabaseFilter::default()).await.unwrap().len(),1);
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    #[tokio::test]
    async fn pending_delete_blocks_open_and_stale_job_cannot_complete() {
        let dir=tempfile::tempdir().unwrap();let catalog=SqliteCatalog::connect(dir.path().join("metadata.db")).await.unwrap();
        let db=catalog.create_database(CreateDatabaseParams::new("to-delete")).await.unwrap();
        let job=catalog.enqueue_job("DB_DELETE",serde_json::json!({"database_id":db.id.to_string()}),0,None,None).await.unwrap();
        assert!(catalog.has_pending_mutation(db.id).await.unwrap());
        let leased=catalog.lease_job("owner-a",Duration::from_secs(30),&["DB_DELETE"]).await.unwrap().unwrap();assert_eq!(leased.id,job.id);
        assert!(catalog.complete_job_fenced(job.id,"owner-b",true,None).await.unwrap().is_none());
        assert!(catalog.complete_job_fenced(job.id,"owner-a",true,None).await.unwrap().is_some());
        assert!(!catalog.has_pending_mutation(db.id).await.unwrap());
    }
    #[tokio::test]
    async fn audit_record_id_is_durable() {
        let dir=tempfile::tempdir().unwrap();let path=dir.path().join("metadata.db");
        let catalog=SqliteCatalog::connect(&path).await.unwrap();let id=catalog.append_audit(crate::AuditEntry::success("test")).await.unwrap();drop(catalog);
        let reopened=SqliteCatalog::connect(&path).await.unwrap();let audit=reopened.list_audit(10,0,crate::AuditFilter::default()).await.unwrap();assert_eq!(audit[0].id,id);
    }
}
