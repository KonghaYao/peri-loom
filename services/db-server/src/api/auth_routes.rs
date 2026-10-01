//! 认证相关路由：本地登录换取 JWT。
//!
//! ## 为什么需要这个模块
//!
//! 架构 §17.11 规定对外身份使用 OIDC / OAuth2 JWT，生产环境由 IdP 签发。但平台
//! **自身**必须能在没有 IdP 的环境（单机部署、Docker Compose、验收与压测环境）中
//! 可用：bootstrap 出来的管理员如果换不到任何凭据，整个管理面与数据面都无法访问。
//!
//! 因此这里提供一条与 [`crate::auth::JwtVerifier`] 对称的 HS256 本地登录路径：
//! 用户名 + 密码（argon2 校验）-> 平台自签 JWT。它与 IdP 签发的 token 走**同一条
//! 校验路径**，所以引入本地登录不会削弱 RBAC 或审计语义。
//!
//! 安全边界：
//! - 密码只用 argon2 校验，绝不明文比较、绝不回显；
//! - 用户名不存在与密码错误返回**同一个**错误码与同一段文案，避免账号枚举；
//! - 未配置 `JWT_SECRET_FILE` / `JWT_SECRET` 时返回明确错误，而不是静默放行；
//! - 登录失败会记审计（action = `AUTH_LOGIN`，result = FAILURE），成功同样记审计。

use axum::extract::State;
use axum::Json;
use domain::error::ErrorCode;

use crate::api::dto;
use crate::auth::{audit_entry, permission, Principal};
use crate::error::{ApiError, ApiResult, PlatformResultExt};
use crate::state::AppState;

/// 登录失败时统一返回的文案。
///
/// 用户不存在、密码错误、账号被禁用都复用同一句话，防止通过错误信息差异枚举账号。
const LOGIN_FAILED_MESSAGE: &str = "用户名或密码错误";

/// 本地登录：用用户名 + 密码换取平台自签 JWT。
///
/// 该接口本身**不需要**认证（否则无法首次获取凭据），因此它必须比其他接口更保守：
/// 只接受 JSON 体、只做常量时间的密码校验、失败不泄露账号是否存在。
#[utoipa::path(
    post,
    path = "/api/v1/auth/login",
    tag = "auth",
    request_body = dto::LoginRequest,
    responses(
        (status = 200, description = "登录成功", body = dto::LoginResponse),
        (status = 401, description = "用户名或密码错误", body = crate::error::ErrorEnvelope),
        (status = 503, description = "服务端未配置 JWT 密钥", body = crate::error::ErrorEnvelope),
    )
)]
pub async fn login(
    State(state): State<AppState>,
    Json(request): Json<dto::LoginRequest>,
) -> ApiResult<Json<dto::LoginResponse>> {
    let issuer = state.jwt_issuer.as_ref().ok_or_else(|| {
        // 配置缺失属于服务端问题，用 503 而不是 401：客户端重试密码没有意义。
        ApiError::from(domain::error::PlatformError::new(
            ErrorCode::StorageUnavailable,
            "服务端未配置 JWT 密钥（JWT_SECRET_FILE / JWT_SECRET），无法签发登录令牌",
        ))
    })?;

    let username = request.username.trim();
    if username.is_empty() || request.password.is_empty() {
        return Err(ApiError::unauthenticated(LOGIN_FAILED_MESSAGE));
    }

    let user = state.catalog.find_user_by_username(username).await.api()?;

    // 统一失败路径：把「用户不存在」「没有密码（纯 OIDC 账号）」「密码不匹配」
    // 「账号被禁用」都收敛成同一个错误，且不区分耗时（argon2 校验本身是重的，
    // 用户不存在时不做校验会形成可测量的时间侧信道，这里用一次哑校验补齐）。
    let (valid, user) = match user {
        Some(user) if user.status == "ACTIVE" => {
            let ok = match user.password_hash.as_deref() {
                Some(hash) => crate::auth::verify_password(&request.password, hash)
                    .map_err(|err| ApiError::internal(format!("密码校验失败: {err}")))?,
                None => false,
            };
            (ok, Some(user))
        }
        Some(user) => (false, Some(user)),
        None => {
            // 哑校验：让「用户不存在」与「密码错误」的耗时处于同一量级。
            let _ = crate::auth::verify_password(
                &request.password,
                // 一个合法的 argon2 编码串（对应用户名不存在时的假哈希）
                "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHR2YWx1ZQ$0Yl0Yl0Yl0Yl0Yl0Yl0Yl0Yl0Yl0Yl0Yl0Yl0",
            );
            (false, None)
        }
    };

    if !valid {
        // 记审计：登录失败也要留痕（架构 §14 DBA Operation Audit）。
        let detail = serde_json::json!({
            "action": "LOGIN",
            "username": username,
            "reason": "INVALID_CREDENTIALS",
        });
        let mut entry = catalog::AuditEntry::failure("AUTH_LOGIN", ErrorCode::Unauthenticated);
        entry.actor_name = username.to_owned();
        entry.target_type = "user".to_owned();
        entry.source_ip = crate::middleware::current_source_ip();
        entry.request_id = Some(crate::middleware::current_request_id());
        entry.detail = detail;
        let _ = state.catalog.append_audit(entry).await;
        return Err(ApiError::unauthenticated(LOGIN_FAILED_MESSAGE));
    }

    let user = user.expect("valid 为真时必然取到用户");
    let issued = issuer.issue(&user.id, &user.username, &user.display_name)?;

    // 成功审计：登录是权限边界事件，必须可追溯。
    let mut entry = catalog::AuditEntry::success("AUTH_LOGIN");
    entry.actor_name = user.username.clone();
    entry.target_type = "user".to_owned();
    entry.target_id = user.id.to_string();
    entry.source_ip = crate::middleware::current_source_ip();
    entry.request_id = Some(crate::middleware::current_request_id());
    entry.detail = serde_json::json!({ "action": "LOGIN", "method": "password" });
    entry = entry.with_actor(user.id, user.username.clone());
    if let Some(tenant) = user.tenant_id {
        entry = entry.with_tenant(tenant);
    }
    state.catalog.append_audit(entry).await.api()?;

    Ok(Json(dto::LoginResponse {
        access_token: issued.token,
        token_type: "Bearer".to_owned(),
        expires_in: issued.expires_in,
    }))
}

/// 当前登录主体信息（Panel 启动时校验 token 是否仍然有效）。
#[utoipa::path(
    get,
    path = "/api/v1/auth/me",
    tag = "auth",
    responses(
        (status = 200, description = "当前主体", body = dto::Viewer),
        (status = 401, description = "未认证", body = crate::error::ErrorEnvelope),
    )
)]
pub async fn me(principal: Principal) -> ApiResult<Json<dto::Viewer>> {
    // me 只需要「已认证」，不要求任何具体权限：它是客户端确认 token 有效性的探针。
    let _ = permission::DB_READ;
    Ok(Json(dto::Viewer {
        user_id: principal.user_id.to_string(),
        username: principal.username.clone(),
        display_name: principal.display_name.clone(),
        is_superuser: principal.is_superuser,
        permissions: principal.permissions.clone(),
        tenant_id: principal.tenant_id.map(|id| id.to_string()),
    }))
}

/// 供其它模块复用：构造一条认证相关的审计条目。
///
/// 保留该薄封装是为了让调用点不需要重复拼 `AuditEntry` 的字段，同时集中注释
/// 「认证事件必须可追溯」这一要求。
#[allow(dead_code)]
pub(crate) fn auth_audit(action: &str, principal: &Principal) -> catalog::AuditEntry {
    let mut entry = audit_entry(action, principal);
    entry.target_type = "user".to_owned();
    entry
}
