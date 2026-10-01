//! RBAC / Token / Audit（架构 §17.11）。
//!
//! 安全约束：
//! - API Token 只存哈希，明文永不进入 Catalog；
//! - 审计日志 append-only，写入失败必须向上抛出（不允许静默丢失审计）；
//! - 认证查询只返回「未吊销 + 未过期 + 用户 ACTIVE」的 token。

use chrono::{DateTime, Utc};
use domain::error::{ErrorCode, Result};
use domain::ids::{DatabaseId, TenantId, TokenId, UserId};
use sqlx::postgres::PgRow;
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::{catalog_error, map_sqlx_error, CatalogError, ConflictAs, NotFoundAs};
use crate::pg::{col, decode_uuid_id, invalid_argument, SqlBuilder, SqlParam};
use crate::Catalog;

/// `users.status` 的合法取值。
pub const USER_STATUSES: [&str; 3] = ["ACTIVE", "DISABLED", "DELETED"];
/// `audit_log.result` 的合法取值。
pub const AUDIT_RESULTS: [&str; 2] = ["SUCCESS", "FAILURE"];

/// 超管权限通配符：`resolve_permissions` 对 superuser 追加该标记。
pub const PERMISSION_WILDCARD: &str = "*";

const USER_COLUMNS: &str = "id, tenant_id, username, display_name, password_hash, oidc_subject, \
     email, status, is_superuser, created_at, updated_at";

const API_TOKEN_COLUMNS: &str =
    "id, user_id, name, token_hash, tenant_id, database_id, permissions, \
     expires_at, last_used_at, revoked_at, created_at";

const AUDIT_COLUMNS: &str =
    "id, actor_id, actor_name, tenant_id, database_id, action, target_type, \
     target_id, result, error_code, source_ip, request_id, detail, created_at";

// ------------------------------------------------------------------ 读模型

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserRecord {
    pub id: UserId,
    pub tenant_id: Option<TenantId>,
    pub username: String,
    pub display_name: String,
    pub password_hash: Option<String>,
    pub oidc_subject: Option<String>,
    pub email: Option<String>,
    pub status: String,
    pub is_superuser: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl UserRecord {
    pub fn is_active(&self) -> bool {
        self.status == "ACTIVE"
    }
}

struct UserRow(UserRecord);

impl FromRow<'_, PgRow> for UserRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let tenant_id: Option<Uuid> = col(row, "tenant_id")?;
        Ok(UserRow(UserRecord {
            id: decode_uuid_id(col(row, "id")?, "id")?,
            tenant_id: match tenant_id {
                Some(v) => Some(decode_uuid_id::<TenantId>(v, "tenant_id")?),
                None => None,
            },
            username: col(row, "username")?,
            display_name: col(row, "display_name")?,
            password_hash: col(row, "password_hash")?,
            oidc_subject: col(row, "oidc_subject")?,
            email: col(row, "email")?,
            status: col(row, "status")?,
            is_superuser: col(row, "is_superuser")?,
            created_at: col(row, "created_at")?,
            updated_at: col(row, "updated_at")?,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ApiTokenRecord {
    pub id: TokenId,
    pub user_id: UserId,
    pub name: String,
    /// 仅哈希；明文只在创建时由调用方向用户返回一次
    pub token_hash: String,
    pub tenant_id: Option<TenantId>,
    pub database_id: Option<DatabaseId>,
    pub permissions: serde_json::Value,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl ApiTokenRecord {
    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }
}

struct ApiTokenRow(ApiTokenRecord);

impl FromRow<'_, PgRow> for ApiTokenRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let tenant_id: Option<Uuid> = col(row, "tenant_id")?;
        let database_id: Option<Uuid> = col(row, "database_id")?;
        Ok(ApiTokenRow(ApiTokenRecord {
            id: decode_uuid_id(col(row, "id")?, "id")?,
            user_id: decode_uuid_id(col(row, "user_id")?, "user_id")?,
            name: col(row, "name")?,
            token_hash: col(row, "token_hash")?,
            tenant_id: match tenant_id {
                Some(v) => Some(decode_uuid_id::<TenantId>(v, "tenant_id")?),
                None => None,
            },
            database_id: match database_id {
                Some(v) => Some(decode_uuid_id::<DatabaseId>(v, "database_id")?),
                None => None,
            },
            permissions: col(row, "permissions")?,
            expires_at: col(row, "expires_at")?,
            last_used_at: col(row, "last_used_at")?,
            revoked_at: col(row, "revoked_at")?,
            created_at: col(row, "created_at")?,
        }))
    }
}

/// 认证结果：token 及其所属用户。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedToken {
    pub user: UserRecord,
    pub token: ApiTokenRecord,
}

/// 审计日志读模型。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditLogRecord {
    pub id: i64,
    pub actor_id: Option<UserId>,
    pub actor_name: String,
    pub tenant_id: Option<TenantId>,
    pub database_id: Option<DatabaseId>,
    pub action: String,
    pub target_type: String,
    pub target_id: String,
    pub result: String,
    pub error_code: Option<String>,
    pub source_ip: Option<String>,
    pub request_id: Option<String>,
    pub detail: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

struct AuditLogRow(AuditLogRecord);

impl FromRow<'_, PgRow> for AuditLogRow {
    fn from_row(row: &PgRow) -> std::result::Result<Self, sqlx::Error> {
        let actor_id: Option<Uuid> = col(row, "actor_id")?;
        let tenant_id: Option<Uuid> = col(row, "tenant_id")?;
        let database_id: Option<Uuid> = col(row, "database_id")?;
        Ok(AuditLogRow(AuditLogRecord {
            id: col(row, "id")?,
            actor_id: match actor_id {
                Some(v) => Some(decode_uuid_id::<UserId>(v, "actor_id")?),
                None => None,
            },
            actor_name: col(row, "actor_name")?,
            tenant_id: match tenant_id {
                Some(v) => Some(decode_uuid_id::<TenantId>(v, "tenant_id")?),
                None => None,
            },
            database_id: match database_id {
                Some(v) => Some(decode_uuid_id::<DatabaseId>(v, "database_id")?),
                None => None,
            },
            action: col(row, "action")?,
            target_type: col(row, "target_type")?,
            target_id: col(row, "target_id")?,
            result: col(row, "result")?,
            error_code: col(row, "error_code")?,
            source_ip: col(row, "source_ip")?,
            request_id: col(row, "request_id")?,
            detail: col(row, "detail")?,
            created_at: col(row, "created_at")?,
        }))
    }
}

// ------------------------------------------------------------------ 入参

#[derive(Debug, Clone)]
pub struct NewUser {
    pub username: String,
    pub display_name: Option<String>,
    /// 已由调用方完成哈希（Catalog 不做口令哈希）
    pub password_hash: Option<String>,
    pub tenant_id: Option<TenantId>,
    pub oidc_subject: Option<String>,
    pub email: Option<String>,
    pub is_superuser: bool,
}

impl NewUser {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            display_name: None,
            password_hash: None,
            tenant_id: None,
            oidc_subject: None,
            email: None,
            is_superuser: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewApiToken {
    pub user_id: UserId,
    pub name: String,
    /// token 明文的哈希（调用方计算）；Catalog 不接触明文
    pub token_hash: String,
    pub tenant_id: Option<TenantId>,
    pub database_id: Option<DatabaseId>,
    pub permissions: serde_json::Value,
    pub expires_at: Option<DateTime<Utc>>,
}

/// 审计入口。所有管理面副作用都必须写一条。
#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub actor_id: Option<UserId>,
    pub actor_name: String,
    pub tenant_id: Option<TenantId>,
    pub database_id: Option<DatabaseId>,
    pub action: String,
    pub target_type: String,
    pub target_id: String,
    pub result: String,
    pub error_code: Option<String>,
    pub source_ip: Option<String>,
    pub request_id: Option<String>,
    pub detail: serde_json::Value,
}

impl Default for AuditEntry {
    fn default() -> Self {
        Self {
            actor_id: None,
            actor_name: String::new(),
            tenant_id: None,
            database_id: None,
            action: String::new(),
            target_type: String::new(),
            target_id: String::new(),
            result: "SUCCESS".to_string(),
            error_code: None,
            source_ip: None,
            request_id: None,
            detail: serde_json::json!({}),
        }
    }
}

impl AuditEntry {
    pub fn success(action: impl Into<String>) -> Self {
        Self {
            action: action.into(),
            ..Default::default()
        }
    }

    pub fn failure(action: impl Into<String>, code: ErrorCode) -> Self {
        Self {
            action: action.into(),
            result: "FAILURE".to_string(),
            error_code: Some(code.as_str().to_string()),
            ..Default::default()
        }
    }

    pub fn with_actor(mut self, actor_id: UserId, actor_name: impl Into<String>) -> Self {
        self.actor_id = Some(actor_id);
        self.actor_name = actor_name.into();
        self
    }

    pub fn with_tenant(mut self, tenant_id: TenantId) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    pub fn with_database(mut self, database_id: DatabaseId) -> Self {
        self.database_id = Some(database_id);
        self
    }

    pub fn with_target(
        mut self,
        target_type: impl Into<String>,
        target_id: impl Into<String>,
    ) -> Self {
        self.target_type = target_type.into();
        self.target_id = target_id.into();
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_source_ip(mut self, source_ip: impl Into<String>) -> Self {
        self.source_ip = Some(source_ip.into());
        self
    }

    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = detail;
        self
    }
}

/// 审计查询过滤条件。
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub actor_id: Option<UserId>,
    pub database_id: Option<DatabaseId>,
    pub tenant_id: Option<TenantId>,
    pub action: Option<String>,
    /// SUCCESS / FAILURE
    pub result: Option<String>,
}

// ------------------------------------------------------------------ 实现

impl Catalog {
    /// 按用户名查找（登录流程使用，含非 ACTIVE 用户，由调用方决定是否放行）。
    pub async fn find_user_by_username(&self, username: &str) -> Result<Option<UserRecord>> {
        let sql = format!("SELECT {USER_COLUMNS} FROM users WHERE username = $1::text");
        let row = sqlx::query_as::<_, UserRow>(&sql)
            .bind(username)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(row.map(|r| r.0))
    }

    pub async fn find_user(&self, id: UserId) -> Result<Option<UserRecord>> {
        let sql = format!("SELECT {USER_COLUMNS} FROM users WHERE id = $1::uuid");
        let row = sqlx::query_as::<_, UserRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(row.map(|r| r.0))
    }

    pub async fn create_user(&self, new_user: NewUser) -> Result<UserRecord> {
        if new_user.username.trim().is_empty() {
            return Err(invalid_argument("username 不能为空"));
        }
        let id = UserId::new_v7();
        let sql = format!(
            "INSERT INTO users (id, tenant_id, username, display_name, password_hash, oidc_subject,
                                email, status, is_superuser)
             VALUES ($1::uuid, $2::uuid, $3::text, COALESCE($4::text, ''), $5::text, $6::text,
                     $7::text, 'ACTIVE', $8::boolean)
             RETURNING {USER_COLUMNS}"
        );
        let row = sqlx::query_as::<_, UserRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .bind(match new_user.tenant_id {
                Some(t) => Some(crate::pg::id_to_uuid(&t)?),
                None => None,
            })
            .bind(new_user.username.as_str())
            .bind(new_user.display_name.as_deref())
            .bind(new_user.password_hash.as_deref())
            .bind(new_user.oidc_subject.as_deref())
            .bind(new_user.email.as_deref())
            .bind(new_user.is_superuser)
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(row.0)
    }

    pub async fn list_users(&self, limit: i64, offset: i64) -> Result<Vec<UserRecord>> {
        let sql = format!(
            "SELECT {USER_COLUMNS} FROM users ORDER BY created_at DESC
             LIMIT $1::bigint OFFSET $2::bigint"
        );
        let rows = sqlx::query_as::<_, UserRow>(&sql)
            .bind(limit.clamp(1, 1000))
            .bind(offset.max(0))
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// 存储 API Token（只存哈希）。token_hash 唯一冲突返回 INVALID_ARGUMENT。
    pub async fn create_api_token(&self, new_token: NewApiToken) -> Result<ApiTokenRecord> {
        if new_token.token_hash.trim().is_empty() {
            return Err(invalid_argument("token_hash 不能为空"));
        }
        if new_token.database_id.is_none() {
            return Err(invalid_argument("API token 必须绑定一个数据库"));
        }
        let id = TokenId::new_v7();
        let sql = format!(
            "INSERT INTO api_tokens (id, user_id, name, token_hash, tenant_id, database_id,
                                     permissions, expires_at)
             VALUES ($1::uuid, $2::uuid, COALESCE($3::text, ''), $4::text, $5::uuid, $6::uuid,
                     COALESCE($7::jsonb, '[]'::jsonb), $8::timestamptz)
             RETURNING {API_TOKEN_COLUMNS}"
        );
        let row = sqlx::query_as::<_, ApiTokenRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .bind(crate::pg::id_to_uuid(&new_token.user_id)?)
            .bind(new_token.name.as_str())
            .bind(new_token.token_hash.as_str())
            .bind(match new_token.tenant_id {
                Some(t) => Some(crate::pg::id_to_uuid(&t)?),
                None => None,
            })
            .bind(match new_token.database_id {
                Some(d) => Some(crate::pg::id_to_uuid(&d)?),
                None => None,
            })
            .bind(new_token.permissions.clone())
            .bind(new_token.expires_at)
            .fetch_one(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::ApiToken))?;
        Ok(row.0)
    }

    /// 同一事务中吊销目标数据库的现有 token 并创建替代 token。
    pub async fn rotate_api_token(&self, new_token: NewApiToken) -> Result<ApiTokenRecord> {
        if new_token.token_hash.trim().is_empty() {
            return Err(invalid_argument("token_hash 不能为空"));
        }
        let database_id = new_token
            .database_id
            .ok_or_else(|| invalid_argument("API token 必须绑定一个数据库"))?;
        let mut tx = self.pool().begin().await.map_err(|e| {
            map_sqlx_error(e, NotFoundAs::User, ConflictAs::ApiToken)
        })?;
        let database_uuid = crate::pg::id_to_uuid(&database_id)?;
        let database_exists: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM databases WHERE id = $1::uuid FOR UPDATE")
                .bind(database_uuid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::ApiToken))?;
        if database_exists.is_none() {
            return Err(invalid_argument("API token database not found"));
        }
        let now = Utc::now();
        sqlx::query(
            "UPDATE api_tokens SET revoked_at = $1::timestamptz
             WHERE database_id = $2::uuid AND revoked_at IS NULL",
        )
        .bind(now)
        .bind(database_uuid)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::ApiToken))?;

        let id = TokenId::new_v7();
        let sql = format!(
            "INSERT INTO api_tokens (id, user_id, name, token_hash, tenant_id, database_id,
                                     permissions, expires_at)
             VALUES ($1::uuid, $2::uuid, COALESCE($3::text, ''), $4::text, $5::uuid, $6::uuid,
                     COALESCE($7::jsonb, '[]'::jsonb), $8::timestamptz)
             RETURNING {API_TOKEN_COLUMNS}"
        );
        let row = sqlx::query_as::<_, ApiTokenRow>(&sql)
            .bind(crate::pg::id_to_uuid(&id)?)
            .bind(crate::pg::id_to_uuid(&new_token.user_id)?)
            .bind(new_token.name.as_str())
            .bind(new_token.token_hash.as_str())
            .bind(match new_token.tenant_id {
                Some(t) => Some(crate::pg::id_to_uuid(&t)?),
                None => None,
            })
            .bind(database_uuid)
            .bind(new_token.permissions.clone())
            .bind(new_token.expires_at)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::ApiToken))?;
        tx.commit()
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::ApiToken))?;
        Ok(row.0)
    }

    /// 认证查询：只返回未吊销、未过期且用户为 ACTIVE 的 token。
    pub async fn find_user_by_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<AuthenticatedToken>> {
        let sql = format!(
            "SELECT {API_TOKEN_COLUMNS} FROM api_tokens
             WHERE token_hash = $1::text
               AND revoked_at IS NULL
               AND (expires_at IS NULL OR expires_at > now())"
        );
        let token = sqlx::query_as::<_, ApiTokenRow>(&sql)
            .bind(token_hash)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Token, ConflictAs::ApiToken))?;

        let Some(token) = token else {
            return Ok(None);
        };

        let user = self.find_user(token.0.user_id).await?;
        match user {
            Some(user) if user.is_active() => Ok(Some(AuthenticatedToken {
                user,
                token: token.0,
            })),
            _ => Ok(None),
        }
    }

    /// 吊销 token（幂等：已吊销时返回 false）。
    pub async fn revoke_api_token(&self, token_id: TokenId) -> Result<bool> {
        let updated: Option<Uuid> = sqlx::query_scalar(
            "UPDATE api_tokens SET revoked_at = now()
             WHERE id = $1::uuid AND revoked_at IS NULL RETURNING id",
        )
        .bind(crate::pg::id_to_uuid(&token_id)?)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::Token, ConflictAs::ApiToken))?;
        Ok(updated.is_some())
    }

    pub async fn list_tokens_for_user(&self, user_id: UserId) -> Result<Vec<ApiTokenRecord>> {
        let sql = format!(
            "SELECT {API_TOKEN_COLUMNS} FROM api_tokens WHERE user_id = $1::uuid
             ORDER BY created_at DESC"
        );
        let rows = sqlx::query_as::<_, ApiTokenRow>(&sql)
            .bind(crate::pg::id_to_uuid(&user_id)?)
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Token, ConflictAs::ApiToken))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// 更新最后使用时间。认证热路径不自动写库（避免写放大），由调用方按需节流调用。
    pub async fn touch_token_last_used(&self, token_id: TokenId) -> Result<()> {
        sqlx::query("UPDATE api_tokens SET last_used_at = now() WHERE id = $1::uuid")
            .bind(crate::pg::id_to_uuid(&token_id)?)
            .execute(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::Token, ConflictAs::ApiToken))?;
        Ok(())
    }

    /// 汇总用户权限：JOIN role_bindings + roles 去重合并。
    ///
    /// superuser 额外追加通配符 `*`，调用方按需处理（既支持 contains("db:read")，
    /// 也支持 wildcard 语义）。
    pub async fn resolve_permissions(&self, user_id: UserId) -> Result<Vec<String>> {
        let user_uuid = crate::pg::id_to_uuid(&user_id)?;
        let is_superuser: Option<bool> =
            sqlx::query_scalar("SELECT is_superuser FROM users WHERE id = $1::uuid")
                .bind(user_uuid)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        let is_superuser = is_superuser
            .ok_or_else(|| catalog_error(CatalogError::UserNotFound(user_id.to_string())))?;

        let permissions: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT p.permission
             FROM role_bindings rb
             JOIN roles r ON r.id = rb.role_id
             CROSS JOIN LATERAL jsonb_array_elements_text(r.permissions) AS p(permission)
             WHERE rb.user_id = $1::uuid
             ORDER BY p.permission",
        )
        .bind(user_uuid)
        .fetch_all(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;

        let mut merged: Vec<String> = permissions;
        if is_superuser {
            merged.push(PERMISSION_WILDCARD.to_string());
        }
        merged.sort();
        merged.dedup();
        Ok(merged)
    }

    /// 追加审计日志（append-only）。必须落库成功，失败向上抛出，返回自增 id。
    #[tracing::instrument(skip(self, entry), fields(action = %entry.action, result = %entry.result))]
    pub async fn append_audit(&self, entry: AuditEntry) -> Result<i64> {
        if entry.action.trim().is_empty() {
            return Err(invalid_argument("audit action 不能为空"));
        }
        if !AUDIT_RESULTS.contains(&entry.result.as_str()) {
            return Err(invalid_argument(format!(
                "audit result 必须是 {AUDIT_RESULTS:?} 之一，收到 '{}'",
                entry.result
            )));
        }
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO audit_log (actor_id, actor_name, tenant_id, database_id, action,
                                    target_type, target_id, result, error_code, source_ip,
                                    request_id, detail)
             VALUES ($1::uuid, $2::text, $3::uuid, $4::uuid, $5::text, $6::text, $7::text,
                     $8::text, $9::text, $10::text, $11::text, $12::jsonb)
             RETURNING id",
        )
        .bind(match entry.actor_id {
            Some(v) => Some(crate::pg::id_to_uuid(&v)?),
            None => None,
        })
        .bind(entry.actor_name.as_str())
        .bind(match entry.tenant_id {
            Some(v) => Some(crate::pg::id_to_uuid(&v)?),
            None => None,
        })
        .bind(match entry.database_id {
            Some(v) => Some(crate::pg::id_to_uuid(&v)?),
            None => None,
        })
        .bind(entry.action.as_str())
        .bind(entry.target_type.as_str())
        .bind(entry.target_id.as_str())
        .bind(entry.result.as_str())
        .bind(entry.error_code.as_deref())
        .bind(entry.source_ip.as_deref())
        .bind(entry.request_id.as_deref())
        .bind(entry.detail.clone())
        .fetch_one(self.pool())
        .await
        .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(id)
    }

    pub async fn list_audit(
        &self,
        limit: i64,
        offset: i64,
        filter: AuditFilter,
    ) -> Result<Vec<AuditLogRecord>> {
        let (sql, params) = build_audit_list_query(limit, offset, &filter)?;
        let query = crate::pg::bind_all(sqlx::query_as::<_, AuditLogRow>(&sql), &params);
        let rows = query
            .fetch_all(self.pool())
            .await
            .map_err(|e| map_sqlx_error(e, NotFoundAs::User, ConflictAs::User))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}

/// 构造审计查询 SQL（纯函数，便于单测）。
pub(crate) fn build_audit_list_query(
    limit: i64,
    offset: i64,
    filter: &AuditFilter,
) -> Result<(String, Vec<SqlParam>)> {
    let mut b = SqlBuilder::new();
    if let Some(actor) = &filter.actor_id {
        let n = b.next_placeholder();
        b.push(
            format!("actor_id = ${n}::uuid"),
            Some(SqlParam::Uuid(crate::pg::id_to_uuid(actor)?)),
        );
    }
    if let Some(tenant) = &filter.tenant_id {
        let n = b.next_placeholder();
        b.push(
            format!("tenant_id = ${n}::uuid"),
            Some(SqlParam::Uuid(crate::pg::id_to_uuid(tenant)?)),
        );
    }
    if let Some(db) = &filter.database_id {
        let n = b.next_placeholder();
        b.push(
            format!("database_id = ${n}::uuid"),
            Some(SqlParam::Uuid(crate::pg::id_to_uuid(db)?)),
        );
    }
    if let Some(action) = &filter.action {
        let n = b.next_placeholder();
        b.push(
            format!("action = ${n}::text"),
            Some(SqlParam::Text(action.clone())),
        );
    }
    if let Some(result) = &filter.result {
        let n = b.next_placeholder();
        b.push(
            format!("result = ${n}::text"),
            Some(SqlParam::Text(result.clone())),
        );
    }

    let where_clause = b.where_clause();
    let mut params = b.params().to_vec();
    let limit_n = params.len() + 1;
    params.push(SqlParam::Int(limit.clamp(1, 1000)));
    let offset_n = params.len() + 1;
    params.push(SqlParam::Int(offset.max(0)));

    let sql = format!(
        "SELECT {AUDIT_COLUMNS} FROM audit_log{where_clause} \
         ORDER BY created_at DESC LIMIT ${limit_n}::bigint OFFSET ${offset_n}::bigint"
    );
    Ok((sql, params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_query_without_filter_only_orders_and_paginates() {
        let (sql, params) = build_audit_list_query(50, 10, &AuditFilter::default()).unwrap();
        assert!(sql.contains("FROM audit_log ORDER BY created_at DESC"));
        assert!(sql.contains("LIMIT $1::bigint OFFSET $2::bigint"));
        assert_eq!(params, vec![SqlParam::Int(50), SqlParam::Int(10)]);
    }

    #[test]
    fn audit_query_binds_all_filters_in_order() {
        let actor: UserId = "00000000-0000-0000-0000-000000000010".parse().unwrap();
        let filter = AuditFilter {
            actor_id: Some(actor),
            action: Some("DB_STOP".into()),
            result: Some("FAILURE".into()),
            ..Default::default()
        };
        let (sql, params) = build_audit_list_query(10, 0, &filter).unwrap();
        assert!(sql.contains("actor_id = $1::uuid"));
        assert!(sql.contains("action = $2::text"));
        assert!(sql.contains("result = $3::text"));
        assert!(sql.contains("LIMIT $4::bigint OFFSET $5::bigint"));
        assert_eq!(params.len(), 5);
        assert_eq!(params[1], SqlParam::Text("DB_STOP".into()));
        assert_eq!(params[2], SqlParam::Text("FAILURE".into()));
    }

    #[test]
    fn audit_entry_builders_set_expected_fields() {
        let entry = AuditEntry::success("DB_CREATE").with_request_id("req-1");
        assert_eq!(entry.result, "SUCCESS");
        assert!(entry.error_code.is_none());
        assert_eq!(entry.request_id.as_deref(), Some("req-1"));

        let entry = AuditEntry::failure("DB_CREATE", ErrorCode::QuotaExceeded);
        assert_eq!(entry.result, "FAILURE");
        assert_eq!(entry.error_code.as_deref(), Some("QUOTA_EXCEEDED"));

        let user: UserId = "00000000-0000-0000-0000-000000000010".parse().unwrap();
        let db: DatabaseId = "00000000-0000-0000-0000-000000000020".parse().unwrap();
        let tenant: TenantId = "00000000-0000-0000-0000-000000000001".parse().unwrap();
        let entry = AuditEntry::success("DB_STOP")
            .with_actor(user, "alice")
            .with_tenant(tenant)
            .with_database(db)
            .with_target("database", db.to_string())
            .with_source_ip("10.0.0.1")
            .with_detail(serde_json::json!({"forced": true}));
        assert_eq!(entry.actor_name, "alice");
        assert_eq!(entry.target_type, "database");
        assert!(entry.tenant_id.is_some());
        assert_eq!(entry.detail["forced"], true);
    }

    #[test]
    fn user_status_and_token_helpers() {
        assert!(USER_STATUSES.contains(&"ACTIVE"));
        assert_eq!(PERMISSION_WILDCARD, "*");
        assert_eq!(AUDIT_RESULTS.len(), 2);
    }
}
