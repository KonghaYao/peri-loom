//! 平台 ID 类型。
//!
//! 全部为 newtype，避免不同类型互相赋值；UUID 类 ID 默认使用 **UUID v7**（时间有序），
//! 让 B-Tree 主键写入与 `ORDER BY id` 近似按创建时间排序。
//! `WorkerId` 例外：它在 Catalog 中是 `TEXT`（worker 由部署侧命名 / 注册时生成字符串），
//! 因此保持字符串语义。

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// 为 UUID newtype 生成统一的构造 / 转换 / 展示实现。
macro_rules! uuid_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        #[repr(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// 由已有 UUID 构造（例如从 Catalog 列读回）。
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// 生成时间有序的 UUID v7（推荐用于新建实体）。
            #[must_use]
            pub fn new_v7() -> Self {
                Self(Uuid::now_v7())
            }

            /// 生成随机 UUID v4（仅在需要不可猜测性时使用）。
            #[must_use]
            pub fn new_v4() -> Self {
                Self(Uuid::new_v4())
            }

            /// 内部 UUID。
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }

            /// 取出内部 UUID。
            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }

            /// 是否为全 0 UUID（占位 / 未设置）。
            #[must_use]
            pub const fn is_nil(&self) -> bool {
                self.0.is_nil()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(s)?))
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

uuid_id! {
    /// Tenant（租户）ID，对应 `tenants.id`。
    TenantId
}
uuid_id! {
    /// 用户数据库 ID，对应 `databases.id`。
    DatabaseId
}
uuid_id! {
    /// 显式会话 ID（Interactive Transaction 场景）。
    SessionId
}
uuid_id! {
    /// 事务 ID；Stateless 请求内部事务也会生成，便于追踪。
    TransactionId
}
uuid_id! {
    /// 长操作（202 + operation_id）ID，对应 `operations.id`。
    OperationId
}
uuid_id! {
    /// 快照 ID，对应 `snapshots.id`（TEXT 主键，内部用 UUID 生成）。
    SnapshotId
}
uuid_id! {
    /// 单次请求 ID，用于错误体与日志关联。
    RequestId
}
uuid_id! {
    /// Job 队列任务 ID，对应 `jobs.id`。
    JobId
}
uuid_id! {
    /// 平台用户 ID，对应 `users.id`。
    UserId
}
uuid_id! {
    /// API Token ID，对应 `api_tokens.id`（明文只在创建时返回一次）。
    TokenId
}

/// Worker ID。Catalog 中为 `TEXT` 主键，保持字符串语义（非 UUID 强制）。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    /// 由字符串构造（部署侧命名或注册流程生成）。
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// 生成 UUID v7 字符串形式的 Worker ID（未显式命名时的默认值）。
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    /// 生成 UUID v4 字符串形式的 Worker ID。
    #[must_use]
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// 字符串视图。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 取出内部字符串。
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }

    /// 是否为空字符串（非法的 Worker ID）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for WorkerId {
    /// 任意字符串都是合法 Worker ID，因此不会失败。
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl From<&str> for WorkerId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for WorkerId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl AsRef<str> for WorkerId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_ids_round_trip_as_string() {
        let cases = [
            TenantId::new_v7().to_string(),
            DatabaseId::new_v7().to_string(),
            OperationId::new_v7().to_string(),
            SnapshotId::new_v7().to_string(),
            RequestId::new_v7().to_string(),
            JobId::new_v7().to_string(),
            UserId::new_v7().to_string(),
            TokenId::new_v7().to_string(),
            SessionId::new_v7().to_string(),
            TransactionId::new_v7().to_string(),
        ];
        for text in cases {
            assert_eq!(Uuid::parse_str(&text).unwrap().to_string(), text);
        }
        let id = DatabaseId::new_v7();
        assert_eq!(DatabaseId::from_str(&id.to_string()).unwrap(), id);
        assert!(DatabaseId::from_str("not-a-uuid").is_err());
    }

    #[test]
    fn v7_ids_are_time_ordered() {
        // v7 的时间前缀保证后生成的 ID 严格更大（同毫秒内由随机位决定，故用毫秒间隔）。
        let a = DatabaseId::new_v7();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = DatabaseId::new_v7();
        assert!(a.as_uuid() < b.as_uuid(), "uuid v7 必须按时间单调");
    }

    #[test]
    fn ids_are_unique_and_hashable() {
        let a = OperationId::new_v4();
        let b = OperationId::new_v4();
        assert_ne!(a, b);
        let mut set = std::collections::HashSet::new();
        set.insert(a);
        set.insert(b);
        set.insert(a);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn serde_is_transparent_string() {
        let id = DatabaseId::new_v7();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        let back: DatabaseId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn worker_id_is_string_typed() {
        let id = WorkerId::from("worker-01");
        assert_eq!(id.as_str(), "worker-01");
        assert_eq!(id.to_string(), "worker-01");
        assert_eq!(
            WorkerId::from_str("worker-02").unwrap().as_str(),
            "worker-02"
        );
        assert!(WorkerId::new_v7().as_str().contains('-'));
        assert_eq!(serde_json::to_string(&id).unwrap(), "\"worker-01\"");
        let back: WorkerId = serde_json::from_str("\"worker-01\"").unwrap();
        assert_eq!(back, id);
    }
}
