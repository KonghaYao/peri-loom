//! Session / Transaction 语义与冻结默认值（架构 §13 / §15.3）。
//!
//! 平台是 **Stateless by Default + Explicit Stateful Session**：
//! 普通请求结束后不保留 connection context；只有显式 OpenSession 才 pin 住
//! worker / db process / 底层连接，从而支持 Interactive Transaction。
//!
//! 冻结默认值：Session idle timeout = 60s，Transaction max lifetime = 30s。
//! Failover 后**不做透明恢复**：未完成事务返回 `TRANSACTION_LOST`，
//! 会话返回 `SESSION_LOST`，由客户端重新 OpenSession 并重试业务事务。

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::UnknownEnumValue;

/// 会话空闲超时（秒）—— 冻结值，不得调整。
pub const SESSION_IDLE_TIMEOUT_SECS: u64 = 60;
/// 会话空闲超时。
pub const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(SESSION_IDLE_TIMEOUT_SECS);
/// 事务最大生命周期（秒）—— 冻结值，不得调整。
pub const TRANSACTION_MAX_LIFETIME_SECS: u64 = 30;
/// 事务最大生命周期。
pub const TRANSACTION_MAX_LIFETIME: Duration = Duration::from_secs(TRANSACTION_MAX_LIFETIME_SECS);

/// 显式会话状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// 已建立并 pin 到某个 Worker / DB Process，可执行语句。
    Active,
    /// 正在关闭（提交/回滚收尾并释放 connection context）。
    Closing,
    /// 正常关闭（客户端 DELETE 或显式关闭）。
    Closed,
    /// 因 Worker / DB Process 故障丢失 —— 不得透明恢复，返回 `SESSION_LOST`。
    Lost,
    /// 空闲超过 [`SESSION_IDLE_TIMEOUT_SECS`] 被回收，返回 `SESSION_IDLE_TIMEOUT`。
    TimedOut,
}

impl SessionState {
    /// 全部状态。
    pub const ALL: &'static [SessionState] = &[
        SessionState::Active,
        SessionState::Closing,
        SessionState::Closed,
        SessionState::Lost,
        SessionState::TimedOut,
    ];

    /// 稳定字符串（日志 / HTTP / serde）。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            SessionState::Active => "ACTIVE",
            SessionState::Closing => "CLOSING",
            SessionState::Closed => "CLOSED",
            SessionState::Lost => "LOST",
            SessionState::TimedOut => "TIMED_OUT",
        }
    }

    /// 严格解析。
    pub fn from_str_strict(value: &str) -> Result<Self, UnknownEnumValue> {
        let upper = value.trim().to_ascii_uppercase();
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.as_str() == upper)
            .ok_or_else(|| UnknownEnumValue::new("SessionState", value))
    }

    /// 宽松解析：未知取值落到 [`SessionState::Lost`]（保守：视为不可继续使用）。
    #[must_use]
    pub fn from_str_lossy(value: &str) -> Self {
        Self::from_str_strict(value).unwrap_or(SessionState::Lost)
    }

    /// 是否可执行语句。
    #[must_use]
    pub const fn accepts_statements(&self) -> bool {
        matches!(self, SessionState::Active)
    }

    /// 是否已终结（不会再有语句执行）。
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            SessionState::Closed | SessionState::Lost | SessionState::TimedOut
        )
    }

    /// 状态机是否允许该转换；同一状态不算转换，未列出的边一律非法。
    #[must_use]
    pub const fn can_transition_to(&self, next: SessionState) -> bool {
        use SessionState as S;
        match self {
            S::Active => matches!(next, S::Closing | S::Lost | S::TimedOut),
            // 关闭过程中若 worker 失联，同样只能记为 LOST
            S::Closing => matches!(next, S::Closed | S::Lost),
            S::Closed | S::Lost | S::TimedOut => false,
        }
    }
}

impl fmt::Display for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SessionState {
    type Err = UnknownEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_strict(s)
    }
}

/// 事务状态（显式会话内的 Interactive Transaction，或单请求内原子事务）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionState {
    /// 当前没有活动事务（会话空闲，可开始新事务）。
    Idle,
    /// 已 BEGIN，语句可执行。
    Active,
    /// 正在提交：Commit Success 必须发生在 Remote WAL durable **之后**。
    Committing,
    /// 已提交（成功，且已 durable）。
    Committed,
    /// 正在回滚。
    RollingBack,
    /// 已回滚。
    RolledBack,
    /// 执行中出错（含约束冲突），事务只能回滚。
    Failed,
    /// 因 failover 丢失，返回 `TRANSACTION_LOST`，不做透明恢复。
    Lost,
    /// 超过 [`TRANSACTION_MAX_LIFETIME_SECS`]，返回 `TRANSACTION_MAX_LIFETIME_EXCEEDED`。
    MaxLifetimeExceeded,
}

impl TransactionState {
    /// 全部状态。
    pub const ALL: &'static [TransactionState] = &[
        TransactionState::Idle,
        TransactionState::Active,
        TransactionState::Committing,
        TransactionState::Committed,
        TransactionState::RollingBack,
        TransactionState::RolledBack,
        TransactionState::Failed,
        TransactionState::Lost,
        TransactionState::MaxLifetimeExceeded,
    ];

    /// 稳定字符串（日志 / HTTP / serde）。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            TransactionState::Idle => "IDLE",
            TransactionState::Active => "ACTIVE",
            TransactionState::Committing => "COMMITTING",
            TransactionState::Committed => "COMMITTED",
            TransactionState::RollingBack => "ROLLING_BACK",
            TransactionState::RolledBack => "ROLLED_BACK",
            TransactionState::Failed => "FAILED",
            TransactionState::Lost => "LOST",
            TransactionState::MaxLifetimeExceeded => "MAX_LIFETIME_EXCEEDED",
        }
    }

    /// 严格解析。
    pub fn from_str_strict(value: &str) -> Result<Self, UnknownEnumValue> {
        let upper = value.trim().to_ascii_uppercase();
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.as_str() == upper)
            .ok_or_else(|| UnknownEnumValue::new("TransactionState", value))
    }

    /// 宽松解析：未知取值落到 [`TransactionState::Lost`]（保守：视为不可继续）。
    #[must_use]
    pub fn from_str_lossy(value: &str) -> Self {
        Self::from_str_strict(value).unwrap_or(TransactionState::Lost)
    }

    /// 是否处于“事务开着”的状态。
    #[must_use]
    pub const fn is_open(&self) -> bool {
        matches!(
            self,
            TransactionState::Active | TransactionState::Committing | TransactionState::RollingBack
        )
    }

    /// 是否已终结。
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            TransactionState::Committed
                | TransactionState::RolledBack
                | TransactionState::Failed
                | TransactionState::Lost
                | TransactionState::MaxLifetimeExceeded
        )
    }

    /// 是否仍可执行语句。
    #[must_use]
    pub const fn accepts_statements(&self) -> bool {
        matches!(self, TransactionState::Active)
    }

    /// 状态机是否允许该转换；同一状态不算转换，未列出的边一律非法。
    #[must_use]
    pub const fn can_transition_to(&self, next: TransactionState) -> bool {
        use TransactionState as T;
        match self {
            T::Idle => matches!(next, T::Active),
            T::Active => matches!(
                next,
                T::Committing | T::RollingBack | T::Failed | T::Lost | T::MaxLifetimeExceeded
            ),
            // 提交结果只能收敛为 Committed / Failed（WAL 未 durable 时不得返回成功），
            // 或在 failover 下直接 LOST。
            T::Committing => matches!(next, T::Committed | T::Failed | T::Lost),
            T::RollingBack => matches!(next, T::RolledBack | T::Lost),
            T::Committed | T::RolledBack | T::Failed | T::Lost | T::MaxLifetimeExceeded => false,
        }
    }
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TransactionState {
    type Err = UnknownEnumValue;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_strict(s)
    }
}

// serde 以稳定字符串为线格式；反序列化宽松降级（未知 -> LOST）。
impl Serialize for SessionState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SessionState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(SessionState::from_str_lossy(&raw))
    }
}

impl Serialize for TransactionState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TransactionState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Ok(TransactionState::from_str_lossy(&raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_timeouts() {
        assert_eq!(SESSION_IDLE_TIMEOUT_SECS, 60);
        assert_eq!(TRANSACTION_MAX_LIFETIME_SECS, 30);
        assert_eq!(SESSION_IDLE_TIMEOUT, Duration::from_secs(60));
        assert_eq!(TRANSACTION_MAX_LIFETIME, Duration::from_secs(30));
    }

    #[test]
    fn session_state_machine() {
        assert!(SessionState::Active.can_transition_to(SessionState::Closing));
        assert!(SessionState::Active.can_transition_to(SessionState::Lost));
        assert!(SessionState::Active.can_transition_to(SessionState::TimedOut));
        assert!(SessionState::Closing.can_transition_to(SessionState::Closed));
        // 边界：终态不得再转移，同一状态不算转换
        for terminal in [
            SessionState::Closed,
            SessionState::Lost,
            SessionState::TimedOut,
        ] {
            for next in SessionState::ALL {
                assert!(
                    !terminal.can_transition_to(*next),
                    "{terminal} -> {next} 不应合法"
                );
            }
        }
        assert!(!SessionState::Active.can_transition_to(SessionState::Active));
        assert!(!SessionState::Active.can_transition_to(SessionState::Closed));
    }

    #[test]
    fn session_state_helpers_and_parsing() {
        assert!(SessionState::Active.accepts_statements());
        assert!(!SessionState::Closing.accepts_statements());
        assert!(SessionState::TimedOut.is_terminal());
        assert_eq!("lost".parse::<SessionState>().unwrap(), SessionState::Lost);
        assert!(SessionState::from_str_strict("WAKING").is_err());
        assert_eq!(SessionState::from_str_lossy("WAKING"), SessionState::Lost);
        assert_eq!(
            serde_json::to_string(&SessionState::Active).unwrap(),
            "\"ACTIVE\""
        );
        let parsed: SessionState = serde_json::from_str("\"weird\"").unwrap();
        assert_eq!(parsed, SessionState::Lost);
    }

    #[test]
    fn transaction_state_machine() {
        assert!(TransactionState::Idle.can_transition_to(TransactionState::Active));
        assert!(TransactionState::Active.can_transition_to(TransactionState::Committing));
        assert!(TransactionState::Active.can_transition_to(TransactionState::Failed));
        assert!(TransactionState::Committing.can_transition_to(TransactionState::Committed));
        assert!(TransactionState::Committing.can_transition_to(TransactionState::Failed));
        assert!(TransactionState::RollingBack.can_transition_to(TransactionState::RolledBack));
        assert!(!TransactionState::RollingBack.can_transition_to(TransactionState::Committed));

        // 边界：未 BEGIN 不能 COMMIT；终态不得复活
        assert!(!TransactionState::Idle.can_transition_to(TransactionState::Committing));
        for terminal in [
            TransactionState::Committed,
            TransactionState::RolledBack,
            TransactionState::Failed,
            TransactionState::Lost,
            TransactionState::MaxLifetimeExceeded,
        ] {
            for next in TransactionState::ALL {
                assert!(
                    !terminal.can_transition_to(*next),
                    "{terminal} -> {next} 不应合法"
                );
            }
            assert!(terminal.is_terminal());
            assert!(!terminal.is_open());
        }
    }

    #[test]
    fn transaction_state_helpers() {
        assert!(TransactionState::Active.is_open());
        assert!(TransactionState::Committing.is_open());
        assert!(TransactionState::Active.accepts_statements());
        assert!(!TransactionState::Committing.accepts_statements());
        assert_eq!(TransactionState::Idle.to_string(), "IDLE");
        assert_eq!(
            "max_lifetime_exceeded".parse::<TransactionState>().unwrap(),
            TransactionState::MaxLifetimeExceeded
        );
        assert_eq!(
            TransactionState::from_str_lossy("?!"),
            TransactionState::Lost
        );
        let parsed: TransactionState = serde_json::from_str("\"COMMITTED\"").unwrap();
        assert_eq!(parsed, TransactionState::Committed);
    }
}
