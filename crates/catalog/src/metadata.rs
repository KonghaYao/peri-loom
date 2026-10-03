//! 部署无关的业务元数据接口；协调、租约与通知仍属于 PostgreSQL 路径。
use crate::{
    ApiTokenRecord, AuditEntry, AuditFilter, AuditLogRecord, AuthenticatedToken, BackupJobRecord,
    BackupJobUpdate, CreateBackupJobParams, CreateDatabaseParams, DatabaseFilter, NewApiToken,
    NewSavedQuery, NewSlowQuery, NewUser, PreferenceRecord, SavedQueryRecord, SlowQueryRecord,
    UserRecord,
};
use chrono::{DateTime, Utc};
use domain::error::Result;
use domain::ids::{DatabaseId, JobId, OperationId, TokenId, UserId};
use domain::lifecycle::LifecycleState;
use domain::records::{DatabaseRecord, JobRecord, OperationRecord, SnapshotRecord};
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait IdentityStore: Send + Sync {
    async fn find_user_by_username(&self, username: &str) -> Result<Option<UserRecord>>;
    async fn find_user(&self, id: UserId) -> Result<Option<UserRecord>>;
    /// JWT subject 与当前权限。Simple 可在同一 SQLite 快照中读取；其他后端
    /// 保留现有逐项查询语义。
    async fn find_user_with_permissions(
        &self,
        subject: &str,
    ) -> Result<Option<(UserRecord, Vec<String>)>> {
        let user = match subject.parse::<Uuid>() {
            Ok(id) => self.find_user(UserId::from_uuid(id)).await?,
            Err(_) => self.find_user_by_username(subject).await?,
        };
        let Some(user) = user else { return Ok(None) };
        let permissions = if user.is_active() {
            self.resolve_permissions(user.id).await?
        } else {
            Vec::new()
        };
        Ok(Some((user, permissions)))
    }
    async fn create_user(&self, user: NewUser) -> Result<UserRecord>;
    async fn list_users(&self, limit: i64, offset: i64) -> Result<Vec<UserRecord>>;
    async fn create_api_token(&self, token: NewApiToken) -> Result<ApiTokenRecord>;
    /// Atomically revoke the active token for this database and create its replacement.
    async fn rotate_api_token(&self, token: NewApiToken) -> Result<ApiTokenRecord>;
    async fn find_user_by_token_hash(&self, hash: &str) -> Result<Option<AuthenticatedToken>>;
    async fn revoke_api_token(&self, id: TokenId) -> Result<bool>;
    async fn list_tokens_for_user(&self, id: UserId) -> Result<Vec<ApiTokenRecord>>;
    async fn touch_token_last_used(&self, id: TokenId) -> Result<()>;
    async fn resolve_permissions(&self, id: UserId) -> Result<Vec<String>>;
    async fn append_audit(&self, entry: AuditEntry) -> Result<i64>;
    async fn list_audit(
        &self,
        limit: i64,
        offset: i64,
        filter: AuditFilter,
    ) -> Result<Vec<AuditLogRecord>>;
}
#[async_trait::async_trait]
pub trait DatabaseStore: Send + Sync {
    async fn create_database(&self, params: CreateDatabaseParams) -> Result<DatabaseRecord>;
    async fn get_database(&self, id: DatabaseId) -> Result<DatabaseRecord>;
    async fn get_database_by_name(
        &self,
        tenant: domain::ids::TenantId,
        name: &str,
    ) -> Result<DatabaseRecord>;
    async fn list_databases(&self, filter: DatabaseFilter) -> Result<Vec<DatabaseRecord>>;
    async fn soft_delete_database(&self, id: DatabaseId) -> Result<DatabaseRecord>;
    async fn set_lifecycle_state(
        &self,
        id: DatabaseId,
        state: LifecycleState,
        owner_worker: Option<domain::ids::WorkerId>,
    ) -> Result<DatabaseRecord>;
    async fn current_catalog_version(&self) -> Result<i64>;
}
#[async_trait::async_trait]
pub trait TaskStore: Send + Sync {
    async fn create_operation(
        &self,
        kind: &str,
        database_id: Option<DatabaseId>,
        worker_id: Option<domain::ids::WorkerId>,
        requested_by: Option<UserId>,
        idempotency_key: Option<&str>,
    ) -> Result<OperationRecord>;
    async fn get_operation(&self, id: OperationId) -> Result<OperationRecord>;
    async fn list_operations(&self, limit: i64, offset: i64) -> Result<Vec<OperationRecord>>;
    async fn enqueue_job(
        &self,
        kind: &str,
        payload: Value,
        priority: i32,
        run_after: Option<DateTime<Utc>>,
        idempotency_key: Option<&str>,
    ) -> Result<JobRecord>;
    async fn lease_job(
        &self,
        owner: &str,
        ttl: Duration,
        kinds: &[&str],
    ) -> Result<Option<JobRecord>>;
    async fn complete_job(
        &self,
        id: JobId,
        success: bool,
        error: Option<String>,
    ) -> Result<JobRecord>;
    async fn complete_job_fenced(
        &self,
        id: JobId,
        lease_owner: &str,
        success: bool,
        error: Option<String>,
    ) -> Result<Option<JobRecord>>;
    async fn get_job(&self, id: JobId) -> Result<JobRecord>;
}
#[async_trait::async_trait]
pub trait PanelStore: Send + Sync {
    async fn get_preference(&self, user: UserId, key: &str) -> Result<Option<Value>>;
    async fn set_preference(&self, user: UserId, key: &str, value: &Value) -> Result<()>;
    async fn list_preferences(&self, user: UserId) -> Result<Vec<PreferenceRecord>>;
    async fn create_saved_query(&self, query: NewSavedQuery) -> Result<SavedQueryRecord>;
    async fn list_saved_queries(&self, user: UserId, limit: i64) -> Result<Vec<SavedQueryRecord>>;
    async fn delete_saved_query(&self, id: Uuid, user: UserId) -> Result<bool>;
    async fn insert_slow_query(&self, query: NewSlowQuery) -> Result<i64>;
    async fn list_slow_queries(
        &self,
        db: DatabaseId,
        limit: i64,
        min_duration_micros: i64,
    ) -> Result<Vec<SlowQueryRecord>>;
}
#[async_trait::async_trait]
pub trait BackupStore: Send + Sync {
    async fn insert_snapshot(&self, snapshot: SnapshotRecord) -> Result<SnapshotRecord>;
    async fn latest_snapshot(&self, db: DatabaseId) -> Result<Option<SnapshotRecord>>;
    async fn list_snapshots(&self, db: DatabaseId, limit: i64) -> Result<Vec<SnapshotRecord>>;
    async fn mark_snapshot_state(
        &self,
        id: domain::ids::SnapshotId,
        state: &str,
    ) -> Result<SnapshotRecord>;
    async fn create_backup_job(&self, params: CreateBackupJobParams) -> Result<BackupJobRecord>;
    async fn update_backup_job_state(
        &self,
        id: Uuid,
        update: BackupJobUpdate,
    ) -> Result<BackupJobRecord>;
    async fn list_backup_jobs(&self, db: DatabaseId, limit: i64) -> Result<Vec<BackupJobRecord>>;
}
#[async_trait::async_trait]
pub trait Metadata:
    IdentityStore + DatabaseStore + TaskStore + PanelStore + BackupStore + Send + Sync
{
    async fn health_check(&self) -> Result<()>;
}

#[async_trait::async_trait]
impl Metadata for crate::Catalog {
    async fn health_check(&self) -> Result<()> {
        self.health_check().await
    }
}

// 分布式适配器直接转发到原有业务方法，原有 SQL 与事务语义不变。
#[async_trait::async_trait]
impl IdentityStore for crate::Catalog {
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
impl DatabaseStore for crate::Catalog {
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
impl TaskStore for crate::Catalog {
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
impl PanelStore for crate::Catalog {
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
impl BackupStore for crate::Catalog {
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
