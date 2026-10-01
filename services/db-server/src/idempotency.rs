//! `Idempotency-Key` 提取与请求指纹（架构 §17.4 / §16 Public HTTP Contract）。
//!
//! 契约：相同 `Idempotency-Key` 重复提交 100 次只能产生 1 个 Operation。
//! 这里只负责「提取 + 指纹」两件纯逻辑的事，真正的去重由
//! [`catalog::Catalog::begin_idempotent`] 在 PostgreSQL 里用唯一键完成。
//!
//! 指纹的作用是区分「重放」与「同 key 不同请求」：后者必须拒绝
//! （`IDEMPOTENCY_CONFLICT`），否则两个语义不同的请求会互相拿到对方的结果。

use axum::http::{HeaderMap, Method};
use sha2::{Digest, Sha256};

/// 幂等键请求头。
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// 幂等键长度上限：超过一律截断拒绝，避免把超长字符串写进唯一索引。
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 255;

/// 从请求头提取 `Idempotency-Key`。
///
/// 返回 `Ok(None)` 表示「调用方没有声明幂等」（每个请求都是新的副作用）；
/// 返回 `Err` 表示声明了但内容非法，必须直接拒绝而不是静默降级 —— 静默降级会让
/// 本以为幂等的重试真的执行两次。
pub fn extract_idempotency_key(headers: &HeaderMap) -> Result<Option<String>, String> {
    let raw = match headers.get(IDEMPOTENCY_KEY_HEADER) {
        Some(value) => value,
        None => return Ok(None),
    };
    let text = raw
        .to_str()
        .map_err(|_| "Idempotency-Key 必须是可见 ASCII".to_string())?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("Idempotency-Key 不能为空".to_string());
    }
    if trimmed.len() > MAX_IDEMPOTENCY_KEY_LEN {
        return Err(format!(
            "Idempotency-Key 长度不能超过 {MAX_IDEMPOTENCY_KEY_LEN}"
        ));
    }
    // 控制字符会让日志与审计难以阅读，也几乎不可能是合法 key
    if trimmed.chars().any(|c| c.is_control()) {
        return Err("Idempotency-Key 不能包含控制字符".to_string());
    }
    Ok(Some(trimmed.to_string()))
}

/// 计算请求指纹：`sha256(method + "\n" + path + "\n" + body)`（小写十六进制）。
///
/// 组成里带 method / path 是必要的：同一个 key 用在 `POST .../start` 与
/// `POST .../backup` 上是调用方的错误，必须被判成冲突而不是重放。
#[must_use]
pub fn request_fingerprint(method: &Method, path: &str, body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(method.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(path.as_bytes());
    hasher.update(b"\n");
    hasher.update(body);
    hex::encode(hasher.finalize())
}

/// 计算 API Token 的存储哈希（Catalog 只存哈希，明文只返回一次）。
#[must_use]
pub fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers_with(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            IDEMPOTENCY_KEY_HEADER,
            HeaderValue::from_str(value).expect("合法 header"),
        );
        headers
    }

    #[test]
    fn missing_header_means_not_idempotent() {
        assert_eq!(extract_idempotency_key(&HeaderMap::new()), Ok(None));
    }

    #[test]
    fn key_is_trimmed_and_returned() {
        let key = extract_idempotency_key(&headers_with("  abc-123  ")).expect("解析成功");
        assert_eq!(key.as_deref(), Some("abc-123"));
    }

    #[test]
    fn blank_key_is_rejected_instead_of_silently_ignored() {
        assert!(extract_idempotency_key(&headers_with("   ")).is_err());
    }

    #[test]
    fn oversized_key_is_rejected() {
        let long = "k".repeat(MAX_IDEMPOTENCY_KEY_LEN + 1);
        assert!(extract_idempotency_key(&headers_with(&long)).is_err());
    }

    #[test]
    fn fingerprint_is_stable_and_path_sensitive() {
        let a = request_fingerprint(&Method::POST, "/api/v1/databases", b"{}");
        let b = request_fingerprint(&Method::POST, "/api/v1/databases", b"{}");
        assert_eq!(a, b, "同输入必须得到同指纹（否则重放会被误判为冲突）");

        let other_path = request_fingerprint(&Method::POST, "/api/v1/tokens", b"{}");
        assert_ne!(a, other_path);

        let other_method = request_fingerprint(&Method::PUT, "/api/v1/databases", b"{}");
        assert_ne!(a, other_method);

        let other_body = request_fingerprint(&Method::POST, "/api/v1/databases", b"{\"a\":1}");
        assert_ne!(a, other_body);
    }

    #[test]
    fn hash_secret_is_deterministic_hex() {
        let hash = hash_secret("token-plaintext");
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(hash, hash_secret("token-plaintext"));
        assert_ne!(hash, hash_secret("token-plaintext2"));
    }

    /// 幂等契约的核心：同 key + 同请求体 → 同一个指纹（-> Catalog 判为 Replay），
    /// 同 key + 不同请求体 → 不同指纹（-> Catalog 判为 Conflict）。
    #[test]
    fn same_key_100_times_yields_single_fingerprint() {
        let fingerprints: std::collections::HashSet<String> = (0..100)
            .map(|_| request_fingerprint(&Method::POST, "/api/v1/databases", b"{\"name\":\"db\"}"))
            .collect();
        assert_eq!(fingerprints.len(), 1);
    }
}
