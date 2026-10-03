//! 认证（OIDC/JWT + 平台 API Token）与 RBAC 判定（架构 §17.11）。
//!
//! 两条并行的认证通道：
//! 1. `Authorization: Bearer <jwt>` —— OIDC/JWT，本服务用 HS256 校验签名与有效期
//!    （开发密钥来自 `JWT_SECRET_FILE`；生产接 OIDC 时替换为 JWKS 校验即可，
//!    调用点不变）。
//! 2. `Authorization: Bearer dbp_...` 或 `x-api-token: <token>` —— 平台自签的不透明
//!    token，只在数据库里存哈希，请求时哈希后查 `users`/`api_tokens`。支持 Bearer
//!    是为了兼容 libSQL/Turso 客户端的 `authToken` 配置。
//!
//! RBAC：`catalog.resolve_permissions(user_id)` 汇总用户权限集合，接口按所需权限判定。
//! 超级管理员得到通配符 `*`（[`catalog::PERMISSION_WILDCARD`]）。

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use domain::error::ErrorCode;
use domain::ids::{DatabaseId, TokenId, UserId};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use tokio::sync::OwnedRwLockReadGuard;

use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::idempotency::hash_secret;
use crate::state::AppState;

// ------------------------------------------------------------------ 权限

/// 平台权限标识（与 roles.permissions JSONB 中的取值一致）。
pub mod permission {
    /// 读取数据库 / 元数据。
    pub const DB_READ: &str = "db:read";
    /// 变更数据库（start / stop / restart / snapshot 等）。
    pub const DB_WRITE: &str = "db:write";
    /// 破坏性数据库操作（delete / move / restore）。
    pub const DB_ADMIN: &str = "db:admin";
    /// Worker 运维（drain）。
    pub const WORKER_ADMIN: &str = "worker:admin";
    /// 审计日志读取。
    pub const AUDIT_READ: &str = "audit:read";
    /// API Token 管理。
    pub const TOKEN_ADMIN: &str = "token:admin";
}

/// 权限判定（纯函数，便于单测）。
///
/// 规则（刻意保持最小、可预测，不做隐式继承）：
/// - 授予集合含通配符 `*` -> 放行（超级管理员）；
/// - 授予集合精确包含所需权限 -> 放行；
/// - 其余一律拒绝。
///
/// **不做** `db:admin` 蕴含 `db:write` 之类的推断：权限一旦有隐式继承，
/// 排查「为什么这个人能删库」就要读代码而不是读角色配置。
#[must_use]
pub fn has_permission(granted: &[String], required: &str) -> bool {
    granted
        .iter()
        .any(|p| p == catalog::PERMISSION_WILDCARD || p == required)
}

/// 将 token 声明的权限与 owner 此刻的权限求交集。
/// 空 token 权限代表历史记录的 owner 权限；`*` 只在交集两侧都允许时保留。
#[must_use]
fn effective_token_permissions(token: &[String], owner: &[String]) -> Vec<String> {
    let owner_is_superuser = owner.iter().any(|p| p == catalog::PERMISSION_WILDCARD);
    if token.is_empty() {
        return owner.to_vec();
    }
    if owner_is_superuser {
        return token.to_vec();
    }
    if token.iter().any(|p| p == catalog::PERMISSION_WILDCARD) {
        return owner.to_vec();
    }
    token
        .iter()
        .filter(|permission| owner.iter().any(|p| p == *permission))
        .cloned()
        .collect()
}

/// API Token 无论其历史权限字段为何，最终只可拥有单库 SQL 数据面权限。
#[must_use]
fn api_token_permissions(token: &[String], owner: &[String]) -> Vec<String> {
    let effective = effective_token_permissions(token, owner);
    if effective
        .iter()
        .any(|permission| permission == catalog::PERMISSION_WILDCARD)
    {
        return vec![
            permission::DB_READ.to_string(),
            permission::DB_WRITE.to_string(),
        ];
    }
    effective
        .into_iter()
        .filter(|item| item == permission::DB_READ || item == permission::DB_WRITE)
        .collect()
}

/// 认证方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// OIDC / JWT Bearer。
    Jwt,
    /// 平台 API Token。
    ApiToken,
}

/// 通过认证的调用主体。
#[derive(Debug, Clone)]
pub struct Principal {
    /// 用户 ID。
    pub user_id: UserId,
    /// 用户名（审计用）。
    pub username: String,
    /// 展示名。
    pub display_name: String,
    /// 默认租户。
    pub tenant_id: Option<domain::ids::TenantId>,
    /// API Token 固定绑定的数据库；JWT 管理凭据为 None。
    pub database_id: Option<DatabaseId>,
    /// 生效的权限集合。
    pub permissions: Vec<String>,
    /// 是否超级管理员。
    pub is_superuser: bool,
    /// 使用的 API Token（JWT 路径为 `None`）。
    pub token_id: Option<TokenId>,
    /// 认证方式。
    pub method: AuthMethod,
}

impl Principal {
    /// 是否拥有某权限。
    #[must_use]
    pub fn can(&self, required: &str) -> bool {
        has_permission(&self.permissions, required)
    }

    /// 要求某权限，否则返回 `PERMISSION_DENIED`。
    ///
    /// # Errors
    /// 权限不足时返回 [`ErrorCode::PermissionDenied`]。
    pub fn require(&self, required: &str) -> ApiResult<()> {
        if self.can(required) {
            Ok(())
        } else {
            Err(ApiError::permission_denied(format!(
                "需要权限 '{required}'，当前主体 '{0}' 不具备",
                self.username
            ))
            .with_detail(serde_json::json!({ "required_permission": required })))
        }
    }

    /// 对 API Token 强制单库边界；JWT 管理凭据不限定单库。
    pub fn require_database(&self, database_id: DatabaseId) -> ApiResult<()> {
        match self.method {
            AuthMethod::Jwt => Ok(()),
            AuthMethod::ApiToken if self.database_id == Some(database_id) => Ok(()),
            AuthMethod::ApiToken => Err(ApiError::permission_denied(
                "API Token 仅可访问其绑定的数据库",
            )
            .with_detail(serde_json::json!({ "database_id": database_id.to_string() }))),
        }
    }

    /// 要求管理面凭据。平台 API Token 仅用于绑定数据库的数据/元数据访问。
    pub fn require_control_plane(&self) -> ApiResult<()> {
        if self.method == AuthMethod::Jwt {
            Ok(())
        } else {
            Err(ApiError::permission_denied("此操作需要管理员 JWT 凭据"))
        }
    }

    /// Turso/libSQL Hrana 客户端只接受单库平台 Token，避免把管理 JWT 当 SDK 凭据复用。
    pub fn require_sdk_database(&self, database_id: DatabaseId) -> ApiResult<()> {
        if self.method != AuthMethod::ApiToken {
            return Err(ApiError::unauthenticated(
                "Hrana 端点需要绑定单库的 API Token，请在 SDK authToken 中使用该 Token",
            ));
        }
        self.require_database(database_id)
    }

    /// 要求管理员 JWT 凭据与 token:admin 权限。
    pub fn require_token_admin(&self) -> ApiResult<()> {
        self.require_control_plane()?;
        self.require(permission::TOKEN_ADMIN)
    }
}

// ------------------------------------------------------------------ JWT

/// JWT claims（只取本服务真正使用的字段；未知字段忽略）。
#[derive(Debug, serde::Deserialize)]
pub struct JwtClaims {
    /// 主体：优先当作 user id（uuid），否则当作 username。
    pub sub: String,
    /// 过期时间（Unix 秒）。
    pub exp: u64,
    /// issuer。
    #[serde(default)]
    pub iss: Option<String>,
    /// 用户名（OIDC 常见 claim）。
    #[serde(default)]
    pub preferred_username: Option<String>,
    /// 展示名。
    #[serde(default)]
    pub name: Option<String>,
}

/// JWT 校验器（持有解码密钥，构造一次后共享）。
pub struct JwtVerifier {
    key: DecodingKey,
    validation: Validation,
}

impl JwtVerifier {
    /// 用 HS256 开发密钥构造。
    ///
    /// `issuer` 非空时会校验 `iss`；`exp` 始终要求存在且未过期。
    #[must_use]
    pub fn hs256(secret: &[u8], issuer: &str) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.required_spec_claims = std::iter::once("exp".to_string()).collect();
        if !issuer.trim().is_empty() {
            validation.set_issuer(&[issuer]);
        }
        Self {
            key: DecodingKey::from_secret(secret),
            validation,
        }
    }

    /// 校验并解出 claims。
    ///
    /// # Errors
    /// 签名错误 / 过期 / issuer 不匹配一律返回 [`ErrorCode::Unauthenticated`]，
    /// 且不把底层错误细节回给客户端（避免成为签名探测的旁路）。
    pub fn verify(&self, token: &str) -> ApiResult<JwtClaims> {
        jsonwebtoken::decode::<JwtClaims>(token, &self.key, &self.validation)
            .map(|data| data.claims)
            .map_err(|err| {
                tracing::debug!(error = %err, "JWT 校验失败");
                ApiError::unauthenticated("JWT 无效或已过期")
            })
    }
}

impl std::fmt::Debug for JwtVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 绝不打印密钥
        f.debug_struct("JwtVerifier")
            .field("algorithm", &"HS256")
            .field("issuer", &self.validation.iss)
            .finish()
    }
}

// ------------------------------------------------------------------ 提取

/// 从请求头解析凭据。
///
/// 同时给出两种凭据时（都带）优先 `x-api-token`：它更具体，且调用方明确表达了
/// 「用这个 token」的意图。Bearer token 仅当符合平台自有 `dbp_` 格式时按 API token
/// 查库；其余仍严格进入 JWT 校验。
enum Credential {
    Bearer(String),
    ApiToken(String),
}

fn extract_credential(headers: &HeaderMap) -> ApiResult<Credential> {
    if let Some(value) = headers.get("x-api-token") {
        let text = value
            .to_str()
            .map_err(|_| ApiError::unauthenticated("x-api-token 不是合法的可见 ASCII"))?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Err(ApiError::unauthenticated("x-api-token 不能为空"));
        }
        return Ok(Credential::ApiToken(trimmed.to_string()));
    }

    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| {
            ApiError::unauthenticated("缺少认证凭据：请提供 Authorization: Bearer 或 x-api-token")
        })?;
    let text = value
        .to_str()
        .map_err(|_| ApiError::unauthenticated("Authorization 头不是合法 ASCII"))?;
    let token = text
        .strip_prefix("Bearer ")
        .or_else(|| text.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| ApiError::unauthenticated("Authorization 必须是 'Bearer <token>'"))?;
    if token.starts_with("dbp_") {
        Ok(Credential::ApiToken(token.to_string()))
    } else {
        Ok(Credential::Bearer(token.to_string()))
    }
}

impl FromRequestParts<AppState> for Principal {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        match extract_credential(&parts.headers)? {
            Credential::Bearer(token) => authenticate_jwt(state, &token).await,
            Credential::ApiToken(token) => authenticate_api_token(state, &token).await,
        }
    }
}

async fn authenticate_jwt(state: &AppState, token: &str) -> ApiResult<Principal> {
    let verifier = state.jwt.as_ref().ok_or_else(|| {
        ApiError::unauthenticated(
            "本实例未配置 JWT_SECRET，Bearer 认证不可用（请使用 x-api-token）",
        )
    })?;
    let claims = verifier.verify(token)?;

    // `sub` 既可能是 user uuid（OIDC 规范做法），也可能是 username（自签 token 常见）。
    let _catalog_timer = matches!(&state.deployment, crate::deployment::Deployment::Simple(_))
        .then(|| crate::simple::metrics::StageTimer::start("jwt_user_permissions"));
    let (user, permissions) = state
        .catalog
        .find_user_with_permissions(&claims.sub)
        .await
        .api()?
        .ok_or_else(|| ApiError::unauthenticated("JWT 主体在平台中不存在"))?;
    drop(_catalog_timer);

    principal_from_jwt(claims, user, permissions)
}

fn principal_from_jwt(
    claims: JwtClaims,
    user: catalog::UserRecord,
    permissions: Vec<String>,
) -> ApiResult<Principal> {
    if !user.is_active() {
        return Err(ApiError::unauthenticated("用户已被禁用"));
    }
    Ok(Principal {
        user_id: user.id,
        username: user.username.clone(),
        display_name: if user.display_name.is_empty() {
            claims
                .name
                .clone()
                .or(claims.preferred_username.clone())
                .unwrap_or_else(|| user.username.clone())
        } else {
            user.display_name.clone()
        },
        tenant_id: user.tenant_id,
        database_id: None,
        permissions,
        is_superuser: user.is_superuser,
        token_id: None,
        method: AuthMethod::Jwt,
    })
}

/// 仅 Simple `/query` 的 JWT 凭据合并读取用户、角色和库准入快照。
/// API Token 与 Distributed 保持原认证路径。
pub(crate) async fn authenticate_query(
    state: &AppState,
    headers: &HeaderMap,
    raw_db_id: &str,
) -> ApiResult<(
    Principal,
    Option<(OwnedRwLockReadGuard<()>, (bool, Option<String>))>,
)> {
    match extract_credential(headers)? {
        Credential::ApiToken(token) => Ok((authenticate_api_token(state, &token).await?, None)),
        Credential::Bearer(token) => {
            let crate::deployment::Deployment::Simple(services) = &state.deployment else {
                return Ok((authenticate_jwt(state, &token).await?, None));
            };
            let verifier = state.jwt.as_ref().ok_or_else(|| {
                ApiError::unauthenticated(
                    "本实例未配置 JWT_SECRET，Bearer 认证不可用（请使用 x-api-token）",
                )
            })?;
            let claims = verifier.verify(&token)?;
            let permit = services.gate.clone().read_owned().await;
            // 非法路径 ID 的报错仍由 handler 在 DB_WRITE 判定后给出；合法 UUID
            // 需要规范化，和既有 db_id() 路径一致。
            let lookup_db_id = crate::api::db_id(raw_db_id)
                .map(|id| id.to_string())
                .unwrap_or_else(|_| raw_db_id.to_owned());
            let _timer = crate::simple::metrics::StageTimer::start("jwt_query_snapshot");
            let (user, snapshot) = services
                .catalog
                .jwt_query_snapshot(&claims.sub, &lookup_db_id)
                .await
                .api()?;
            let (user, permissions) =
                user.ok_or_else(|| ApiError::unauthenticated("JWT 主体在平台中不存在"))?;
            let principal = principal_from_jwt(claims, user, permissions)?;
            Ok((principal, Some((permit, snapshot))))
        }
    }
}

async fn authenticate_api_token(state: &AppState, token: &str) -> ApiResult<Principal> {
    let hash = hash_secret(token);
    let authenticated = state
        .catalog
        .find_user_by_token_hash(&hash)
        .await
        .api()?
        .ok_or_else(|| ApiError::unauthenticated("API token 无效、已吊销或已过期"))?;

    if !authenticated.user.is_active() {
        return Err(ApiError::unauthenticated("用户已被禁用"));
    }
    let database_id = authenticated.token.database_id.ok_or_else(|| {
        ApiError::unauthenticated("API Token 未绑定数据库，必须重新申请单库 Token")
    })?;

    // token 权限始终受 owner 当前角色权限约束。这样即使 owner 被降权或 token 中
    // 留有旧权限，也不会保留已撤销的能力。历史空权限 token 仍按 owner 权限兼容。
    let owner_permissions = state
        .catalog
        .resolve_permissions(authenticated.user.id)
        .await
        .api()?;
    let token_permissions = authenticated
        .token
        .permissions
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let permissions = api_token_permissions(&token_permissions, &owner_permissions);

    // last_used_at 只是可观测性信息，写失败不影响请求（也不该让请求失败）。
    if let Err(err) = state
        .catalog
        .touch_token_last_used(authenticated.token.id)
        .await
    {
        tracing::warn!(error = %err.message, "更新 token last_used_at 失败");
    }

    Ok(Principal {
        user_id: authenticated.user.id,
        username: authenticated.user.username.clone(),
        display_name: authenticated.user.display_name.clone(),
        tenant_id: authenticated
            .token
            .tenant_id
            .or(authenticated.user.tenant_id),
        database_id: Some(database_id),
        permissions,
        // API Token 是单库数据凭据，不继承用户的全局管理员身份。
        is_superuser: false,
        token_id: Some(authenticated.token.id),
        method: AuthMethod::ApiToken,
    })
}

/// 生成一个新的 API Token 明文（返回给用户一次，之后只剩哈希）。
///
/// 用 UUID v4 的两段拼接：122 位熵 × 2，长度 64 hex 字符；不依赖 rand 的 API 细节。
#[must_use]
pub fn generate_api_token() -> String {
    let a = uuid::Uuid::new_v4().simple().to_string();
    let b = uuid::Uuid::new_v4().simple().to_string();
    format!("dbp_{a}{b}")
}

// ------------------------------------------------------------------ 引导管理员

/// 首次启动时创建管理员（仅当 `users` 表为空）。
///
/// 返回 `Ok(Some(username))` 表示本次创建了管理员；`Ok(None)` 表示已存在用户，
/// 什么都没做（**绝不会**重置已有账号的密码）。
///
/// # Errors
/// 口令哈希失败或写库失败时返回错误；调用方应视为启动失败（没有管理员 = 平台不可用）。
pub async fn bootstrap_admin(
    catalog: &dyn catalog::Metadata,
    username: &str,
    password: &str,
) -> domain::error::Result<Option<String>> {
    let existing = catalog.list_users(1, 0).await?;
    if !existing.is_empty() {
        tracing::info!(users = existing.len(), "users 表非空，跳过引导管理员创建");
        return Ok(None);
    }

    let password_hash = hash_password(password).map_err(|err| {
        domain::error::PlatformError::new(
            ErrorCode::InternalError,
            format!("计算引导管理员口令哈希失败: {err}"),
        )
    })?;

    let mut new_user = catalog::NewUser::new(username);
    new_user.display_name = Some("平台管理员".to_string());
    new_user.password_hash = Some(password_hash);
    new_user.tenant_id = Some(catalog::default_tenant_id());
    new_user.is_superuser = true;

    let user = catalog.create_user(new_user).await?;
    tracing::warn!(
        username = %user.username,
        "users 表为空，已创建引导管理员（请登录后立即轮换口令）"
    );
    Ok(Some(user.username))
}

/// argon2 口令哈希（库用 thiserror，故这里返回 `Result<_, String>` 便于上层包装）。
///
/// 盐由 `password-hash` 内部用系统随机源生成（每个口令唯一），不手工拼盐。
///
/// # Errors
/// 哈希参数不合法或随机源不可用时返回错误描述。
pub fn hash_password(password: &str) -> Result<String, String> {
    use argon2::{Argon2, PasswordHash, PasswordHasher};

    // 显式限定 trait 泛型参数：`PasswordVerifier`/`PasswordHasher` 对同一类型有多个实现，
    // 全限定调用避免方法解析歧义。
    <Argon2<'_> as PasswordHasher<PasswordHash>>::hash_password(
        &Argon2::default(),
        password.as_bytes(),
    )
    .map(|hash| hash.to_string())
    .map_err(|err| format!("argon2 哈希失败: {err}"))
}

/// 校验口令（供将来的密码登录流程使用；当前 API 只走 JWT / token）。
///
/// # Errors
/// 哈希串格式非法时返回错误描述。
pub fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    use argon2::{Argon2, PasswordVerifier};

    Ok(<Argon2<'_> as PasswordVerifier<str>>::verify_password(
        &Argon2::default(),
        password.as_bytes(),
        hash,
    )
    .is_ok())
}

/// 构造审计条目并补齐请求上下文（`request_id` / `source_ip`）。
///
/// 所有 DBA 写操作都必须调用它，保证审计完整率 100%（架构 §17.11）。
#[must_use]
pub fn audit_entry(action: &str, principal: &Principal) -> catalog::AuditEntry {
    let mut entry = catalog::AuditEntry::success(action);
    entry.actor_id = Some(principal.user_id);
    entry.actor_name = principal.username.clone();
    entry.tenant_id = principal.tenant_id;
    entry.source_ip = crate::middleware::current_source_ip();
    entry.request_id = Some(crate::middleware::current_request_id());
    entry
}

#[cfg(test)]
mod tests {
    use super::*;
    // 只在测试里签发 token：生产路径只做校验，不持有签名密钥。
    use jsonwebtoken::EncodingKey;

    fn perms(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn wildcard_grants_everything() {
        let granted = perms(&[catalog::PERMISSION_WILDCARD]);
        assert!(has_permission(&granted, permission::DB_ADMIN));
        assert!(has_permission(&granted, "anything:else"));
    }

    #[test]
    fn exact_permission_is_required_without_inheritance() {
        let granted = perms(&[permission::DB_READ]);
        assert!(has_permission(&granted, permission::DB_READ));
        assert!(!has_permission(&granted, permission::DB_WRITE));
        assert!(!has_permission(&granted, permission::DB_ADMIN));
        // db:write 不蕴含 db:read（刻意不做隐式继承）
        let writer = perms(&[permission::DB_WRITE]);
        assert!(!has_permission(&writer, permission::DB_READ));
    }

    #[test]
    fn empty_permissions_deny() {
        assert!(!has_permission(&[], permission::DB_READ));
    }

    #[test]
    fn principal_require_reports_denied_code() {
        let principal = Principal {
            user_id: UserId::new_v7(),
            username: "alice".to_string(),
            display_name: "Alice".to_string(),
            tenant_id: None,
            database_id: None,
            permissions: perms(&[permission::DB_READ]),
            is_superuser: false,
            token_id: None,
            method: AuthMethod::ApiToken,
        };
        assert!(principal.require(permission::DB_READ).is_ok());
        let err = principal.require(permission::WORKER_ADMIN).unwrap_err();
        assert_eq!(err.code(), ErrorCode::PermissionDenied);
    }

    #[test]
    fn api_token_requires_a_database_binding_even_if_principal_is_malformed() {
        let mut principal = Principal {
            user_id: UserId::new_v7(),
            username: "alice".to_string(),
            display_name: "Alice".to_string(),
            tenant_id: None,
            database_id: None,
            permissions: perms(&[permission::DB_READ]),
            is_superuser: false,
            token_id: None,
            method: AuthMethod::ApiToken,
        };
        let database_id = DatabaseId::new_v7();
        assert!(principal.require_database(database_id).is_err());
        principal.database_id = Some(database_id);
        assert!(principal.require_database(database_id).is_ok());
        assert!(principal.require_database(DatabaseId::new_v7()).is_err());
    }

    #[test]
    fn api_token_permissions_never_keep_management_rights() {
        let owner = perms(&[catalog::PERMISSION_WILDCARD]);
        let legacy = perms(&[catalog::PERMISSION_WILDCARD]);
        assert_eq!(
            api_token_permissions(&legacy, &owner),
            perms(&[permission::DB_READ, permission::DB_WRITE]),
        );
        assert!(api_token_permissions(&perms(&[]), &owner)
            .iter()
            .all(|item| item == permission::DB_READ || item == permission::DB_WRITE));
    }

    #[test]
    fn generated_api_token_has_prefix_and_is_unique() {
        let a = generate_api_token();
        let b = generate_api_token();
        assert!(a.starts_with("dbp_"));
        assert_eq!(a.len(), 4 + 64);
        assert_ne!(a, b);
    }

    #[test]
    fn bearer_platform_token_is_classified_as_api_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer dbp_example".parse().unwrap(),
        );
        assert!(matches!(
            extract_credential(&headers).unwrap(),
            Credential::ApiToken(_)
        ));

        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer ey.jwt.signature".parse().unwrap(),
        );
        assert!(matches!(
            extract_credential(&headers).unwrap(),
            Credential::Bearer(_)
        ));

        headers.insert("x-api-token", "explicit-api-token".parse().unwrap());
        assert!(matches!(
            extract_credential(&headers).unwrap(),
            Credential::ApiToken(value) if value == "explicit-api-token"
        ));
    }

    #[test]
    fn explicit_token_permissions_are_bounded_by_current_owner_permissions() {
        let owner = perms(&[permission::DB_READ, permission::DB_WRITE]);
        let token = perms(&[permission::DB_READ, permission::DB_ADMIN]);
        let effective = effective_token_permissions(&token, &owner);
        assert_eq!(effective, perms(&[permission::DB_READ]));
        assert_eq!(effective_token_permissions(&perms(&[]), &owner), owner);
        assert_eq!(
            effective_token_permissions(&perms(&[catalog::PERMISSION_WILDCARD]), &owner),
            owner,
        );
    }

    #[test]
    fn jwt_verifier_rejects_garbage_and_wrong_secret() {
        let verifier = JwtVerifier::hs256(b"secret-a", "db-platform");
        assert!(verifier.verify("not-a-jwt").is_err());

        // 用另一个密钥签发的 token 必须被拒绝
        #[derive(serde::Serialize)]
        struct Claims<'a> {
            sub: &'a str,
            exp: u64,
            iss: &'a str,
        }
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &Claims {
                sub: "someone",
                exp: 4_102_444_800,
                iss: "db-platform",
            },
            &EncodingKey::from_secret(b"secret-b"),
        )
        .expect("签发测试 token");
        assert!(verifier.verify(&token).is_err());

        let good = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &Claims {
                sub: "someone",
                exp: 4_102_444_800,
                iss: "db-platform",
            },
            &EncodingKey::from_secret(b"secret-a"),
        )
        .expect("签发测试 token");
        assert_eq!(verifier.verify(&good).expect("校验通过").sub, "someone");
    }

    #[test]
    fn jwt_verifier_enforces_issuer() {
        let verifier = JwtVerifier::hs256(b"secret", "db-platform");
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &serde_json::json!({ "sub": "x", "exp": 4_102_444_800u64, "iss": "somebody-else" }),
            &EncodingKey::from_secret(b"secret"),
        )
        .expect("签发测试 token");
        assert!(verifier.verify(&token).is_err());
    }

    #[test]
    fn password_hash_roundtrip() {
        let hash = hash_password("correct horse battery staple").expect("哈希成功");
        assert!(verify_password("correct horse battery staple", &hash).expect("校验成功"));
        assert!(!verify_password("wrong", &hash).expect("校验成功"));
    }
}

// ------------------------------------------------------------------ JWT 签发

/// JWT 签发器（本地登录用）。
///
/// 为什么需要它：架构 §17.11 规定对外身份是 OIDC / OAuth2 JWT，生产环境由 IdP 签发；
/// 但平台自身也必须能在没有 IdP 的环境（本地部署、Compose、验收测试）中可用，
/// 否则 bootstrap 出来的管理员无法换取任何凭据 —— 平台将完全不可用。
/// 因此这里提供与 [`JwtVerifier`] 对称的 HS256 签发能力，claim 结构与校验端一致。
pub struct JwtIssuer {
    key: EncodingKey,
    issuer: String,
    ttl_seconds: u64,
}

/// 签发出的 token 及其有效期。
#[derive(Debug, Clone)]
pub struct IssuedToken {
    /// 紧凑序列化后的 JWT。
    pub token: String,
    /// 有效期（秒），与 token 内的 exp 一致。
    pub expires_in: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct IssuedClaims {
    /// 用户 ID（与校验端的 `sub` 语义一致：优先当 user id 使用）。
    sub: String,
    exp: u64,
    iat: u64,
    iss: String,
    preferred_username: String,
    name: String,
}

impl JwtIssuer {
    /// 用 HS256 开发密钥构造；`ttl_seconds` 为 0 时回退到 1 小时。
    #[must_use]
    pub fn hs256(secret: &[u8], issuer: &str, ttl_seconds: u64) -> Self {
        Self {
            key: EncodingKey::from_secret(secret),
            issuer: issuer.to_owned(),
            ttl_seconds: if ttl_seconds == 0 { 3600 } else { ttl_seconds },
        }
    }

    /// 为指定用户签发 JWT。
    ///
    /// # Errors
    /// 编码失败（极少见，通常意味着系统时钟异常）返回 [`ErrorCode::InternalError`]。
    pub fn issue(
        &self,
        user_id: &UserId,
        username: &str,
        display_name: &str,
    ) -> ApiResult<IssuedToken> {
        // now_unix_ms 返回 i64（可为负的“1970 前”），这里转成 u64 秒；
        // 系统时钟异常导致负数时按 0 处理，宁可由 exp 判定已过期也不 panic。
        let now = (domain::time::now_unix_ms() / 1000).max(0) as u64;
        let claims = IssuedClaims {
            sub: user_id.to_string(),
            exp: now + self.ttl_seconds,
            iat: now,
            iss: self.issuer.clone(),
            preferred_username: username.to_owned(),
            name: display_name.to_owned(),
        };
        let token = jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.key)
            .map_err(|err| {
                tracing::error!(error = %err, "JWT 签发失败");
                ApiError::internal("JWT 签发失败")
            })?;
        Ok(IssuedToken {
            token,
            expires_in: self.ttl_seconds,
        })
    }
}

impl std::fmt::Debug for JwtIssuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 绝不打印密钥
        f.debug_struct("JwtIssuer")
            .field("algorithm", &"HS256")
            .field("issuer", &self.issuer)
            .field("ttl_seconds", &self.ttl_seconds)
            .finish()
    }
}
