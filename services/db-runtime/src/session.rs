//! 会话与显式事务的生命周期（架构 §13 / §15.3，冻结语义）。
//!
//! 冻结默认值：会话空闲 60s、单事务最大存活 30s（两者都能被请求里的字段收紧，
//! 但**不能被放宽**超过冻结值——放宽等于把"卡住的会话"变成资源泄漏）。
//!
//! 三条必须守住的语义：
//!
//! 1. **超时即终结**：会话空闲超时 -> 关闭并回 `SESSION_IDLE_TIMEOUT`；
//!    事务超过最大存活 -> 回滚并回 `TRANSACTION_MAX_LIFETIME_EXCEEDED`。
//! 2. **超时后的迟到请求也要拿到正确的错误码**，不能退化成 `SESSION_NOT_FOUND`：
//!    因此会话被回收后留下墓碑（tombstone），短期内仍能回答"它为什么没了"。
//! 3. **fencing / 重启后失效的事务一律 `TRANSACTION_LOST`**（`Commit` / `Rollback`
//!    找不到会话或事务时），绝不能假装提交成功。
//!
//! 并发模型：会话状态由**一个** tokio 互斥量保护——连接、事务、活动时间同属一份状态，
//! 分成多把锁只会制造"事务字段已更新、连接还在跑上一条语句"的窗口。请求侧通过
//! `lock_owned` 拿到守卫后交给 `spawn_blocking`，这样即使外层 deadline 先到期，
//! 引擎调用结束时守卫才释放，同一会话上的语句永远不会并行进入引擎。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use domain::error::{ErrorCode, PlatformError};
use domain::time::now_unix_ms;
use engine_adapter::EngineConnection;

/// 当前 Unix 毫秒（统一入口：`domain::time::now_unix_ms` 是 i64，平台内部一律用 u64）。
#[must_use]
pub fn now_ms() -> u64 {
    u64::try_from(now_unix_ms()).unwrap_or(0)
}

/// 墓碑保留时长：足够让"超时后立刻到达"的请求拿到正确错误码，又不会无限增长。
const TOMBSTONE_TTL: Duration = Duration::from_secs(300);

/// 会话终结原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryReason {
    /// 空闲超过会话上限。
    IdleTimeout,
    /// 事务存活超过上限。
    TransactionLifetimeExceeded,
}

impl ExpiryReason {
    /// 稳定字符串（日志 / 通知）。
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            ExpiryReason::IdleTimeout => "SESSION_IDLE_TIMEOUT",
            ExpiryReason::TransactionLifetimeExceeded => "TRANSACTION_MAX_LIFETIME_EXCEEDED",
        }
    }

    /// 终结原因对应的平台错误码。
    #[must_use]
    pub const fn error_code(&self) -> ErrorCode {
        match self {
            ExpiryReason::IdleTimeout => ErrorCode::SessionIdleTimeout,
            ExpiryReason::TransactionLifetimeExceeded => ErrorCode::TransactionMaxLifetimeExceeded,
        }
    }

    /// 构造对外错误。
    #[must_use]
    pub fn to_error(self, session_id: &str) -> PlatformError {
        PlatformError::new(
            self.error_code(),
            format!("会话 {session_id} 已终结：{}", self.as_str()),
        )
    }
}

/// 空闲 / 事务超时的纯判定（超时逻辑的核心，单测直接打这里）。
///
/// 事务超时优先于空闲超时：事务超时是更具体、更可操作的结论，而且它同时意味着
/// "必须回滚"，运维看到 `TRANSACTION_MAX_LIFETIME_EXCEEDED` 才知道该去查长事务。
#[must_use]
pub fn decide_expiry(
    last_activity_ms: u64,
    transaction_expires_at_ms: u64,
    idle_timeout_ms: u64,
    now_ms: u64,
) -> Option<ExpiryReason> {
    if transaction_expires_at_ms != 0 && now_ms >= transaction_expires_at_ms {
        return Some(ExpiryReason::TransactionLifetimeExceeded);
    }
    if now_ms.saturating_sub(last_activity_ms) >= idle_timeout_ms {
        return Some(ExpiryReason::IdleTimeout);
    }
    None
}

/// 会话内的事务状态。
#[derive(Debug, Clone)]
pub struct Transaction {
    /// 事务 id（回给 Dispatcher，后续 Commit/Rollback 必须原样带回）。
    pub id: String,
    /// 是否只读事务。
    pub read_only: bool,
    /// 绝对到期时刻（unix ms）。
    pub expires_at_ms: u64,
}

/// 互斥保护的会话状态。
pub struct SessionState {
    /// 本会话独占的引擎连接（一个会话一条连接，事务语义依赖于此）。
    pub conn: EngineConnection,
    /// 当前显式事务。
    pub transaction: Option<Transaction>,
}

/// 一个显式会话。
pub struct Session {
    id: String,
    inner: Arc<tokio::sync::Mutex<SessionState>>,
    idle_timeout: Duration,
    max_transaction_lifetime: Duration,
    /// 最近活动时刻（unix ms）；reaper 无锁读取，写入发生在持锁期间。
    last_activity_ms: AtomicU64,
    /// 事务到期时刻（unix ms，0 = 无事务）；同上。
    transaction_expires_at_ms: AtomicU64,
}

impl Session {
    /// 会话 id。
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 空闲超时（会话级，已被冻结值夹紧）。
    #[must_use]
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// 事务最大存活（会话级）。
    #[must_use]
    pub fn max_transaction_lifetime(&self) -> Duration {
        self.max_transaction_lifetime
    }

    /// 会话状态的共享句柄（`lock_owned` 后交给阻塞线程）。
    #[must_use]
    pub fn state(&self) -> Arc<tokio::sync::Mutex<SessionState>> {
        Arc::clone(&self.inner)
    }

    /// 会话绝对过期时刻（unix ms）。
    #[must_use]
    pub fn expires_at_ms(&self) -> u64 {
        self.last_activity_ms.load(Ordering::Acquire) + self.idle_timeout.as_millis() as u64
    }

    /// 标记一次活动（必须在持有状态锁时调用，保证镜像与状态一致）。
    pub fn touch(&self, now_ms: u64) {
        self.last_activity_ms.store(now_ms, Ordering::Release);
    }

    /// 记录事务到期时刻（0 = 无事务）。
    pub fn set_transaction_deadline(&self, expires_at_ms: u64) {
        self.transaction_expires_at_ms
            .store(expires_at_ms, Ordering::Release);
    }

    /// 当前是否应当被回收（reaper 的无锁判定）。
    #[must_use]
    pub fn expiry(&self, now_ms: u64) -> Option<ExpiryReason> {
        decide_expiry(
            self.last_activity_ms.load(Ordering::Acquire),
            self.transaction_expires_at_ms.load(Ordering::Acquire),
            self.idle_timeout.as_millis() as u64,
            now_ms,
        )
    }

    /// 由 reaper 调用：拿到状态锁，回滚在途事务。
    ///
    /// 回滚是引擎调用，必须放到阻塞线程上（`spawn_blocking`），否则一次慢回滚会卡住
    /// 整个 tokio 运行时。拿不到锁时不排队等待：在途请求自己会在语句结束时善后，
    /// reaper 不能被一个卡死的语句拖住。
    pub async fn close_and_rollback(self: Arc<Self>) {
        let Ok(guard) = self.inner.clone().try_lock_owned() else {
            tracing::warn!(session_id = %self.id, "会话回收时状态锁被占用，跳过回滚");
            return;
        };
        let session_id = self.id.clone();
        let joined = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            if guard.transaction.take().is_some() {
                if let Err(err) = guard.conn.rollback() {
                    tracing::warn!(
                        session_id = %session_id,
                        error = %err,
                        "回收会话时回滚事务失败（会话已终结）"
                    );
                }
            }
        })
        .await;
        if let Err(err) = joined {
            tracing::warn!(session_id = %self.id, error = %err, "回滚线程异常");
        }
    }
}

/// 会话查找结果。
pub enum SessionLookup {
    /// 找到且可继续使用。
    Found(Arc<Session>),
    /// 会话已因超时终结（墓碑命中）。
    Expired {
        /// 终结原因。
        reason: ExpiryReason,
    },
    /// 会话从未存在，或墓碑已过期（进程重启 / fencing 之后的常见情形）。
    Unknown,
}

/// 会话管理器。
pub struct SessionManager {
    sessions: DashMap<String, Arc<Session>>,
    tombstones: DashMap<String, (ExpiryReason, u64)>,
    default_idle_timeout: Duration,
    default_max_transaction_lifetime: Duration,
}

impl SessionManager {
    /// 新建管理器（默认值来自冻结常量）。
    #[must_use]
    pub fn new(default_idle_timeout: Duration, default_max_transaction_lifetime: Duration) -> Self {
        Self {
            sessions: DashMap::new(),
            tombstones: DashMap::new(),
            default_idle_timeout,
            default_max_transaction_lifetime,
        }
    }

    /// 打开会话。
    ///
    /// 请求里的超时只能**收紧**：`idle_timeout_ms = 0` 表示用默认值，超过冻结值的请求
    /// 一律夹到冻结值（架构 §15.3 是平台对外承诺的上界，客户端不能自选更宽的窗口）。
    pub fn open(
        &self,
        conn: EngineConnection,
        idle_timeout_ms: u32,
        max_lifetime_ms: u64,
    ) -> Arc<Session> {
        let idle = clamp_timeout(
            Duration::from_millis(u64::from(idle_timeout_ms)),
            self.default_idle_timeout,
        );
        let txn = clamp_timeout(
            Duration::from_millis(max_lifetime_ms),
            self.default_max_transaction_lifetime,
        );
        let session = Arc::new(Session {
            id: uuid::Uuid::now_v7().to_string(),
            inner: Arc::new(tokio::sync::Mutex::new(SessionState {
                conn,
                transaction: None,
            })),
            idle_timeout: idle,
            max_transaction_lifetime: txn,
            last_activity_ms: AtomicU64::new(now_ms()),
            transaction_expires_at_ms: AtomicU64::new(0),
        });
        self.sessions
            .insert(session.id.clone(), Arc::clone(&session));
        session
    }

    /// 查找会话（含墓碑判定）。
    #[must_use]
    pub fn lookup(&self, session_id: &str) -> SessionLookup {
        if let Some(entry) = self.sessions.get(session_id) {
            let session = Arc::clone(entry.value());
            // 先放掉读锁再判定，避免长时间持有分片锁
            drop(entry);
            // 惰性判定：即使 reaper 还没跑到，也不能让过期会话继续执行语句。
            if let Some(reason) = session.expiry(now_ms()) {
                return SessionLookup::Expired { reason };
            }
            return SessionLookup::Found(session);
        }
        match self.tombstones.get(session_id) {
            Some(entry) => SessionLookup::Expired { reason: entry.0 },
            None => SessionLookup::Unknown,
        }
    }

    /// 关闭会话并回滚在途事务；返回是否确实存在过。
    pub async fn close(&self, session_id: &str) -> bool {
        match self.sessions.remove(session_id) {
            Some((_, session)) => {
                session.close_and_rollback().await;
                true
            }
            None => false,
        }
    }

    /// 会话数。
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.sessions.len()
    }

    /// 全部会话（优雅停机时逐个收尾）。
    #[must_use]
    pub fn all(&self) -> Vec<Arc<Session>> {
        self.sessions
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// 回收超时会话，返回被摘除的会话及其终结原因。
    ///
    /// 只做"摘除 + 记墓碑"，回滚由调用方驱动（可能阻塞，属于 IO 决策，不该藏在这里）。
    pub fn reap(&self, now_ms: u64) -> Vec<(Arc<Session>, ExpiryReason)> {
        for entry in self.tombstones.iter() {
            if entry.value().1 <= now_ms {
                let key = entry.key().clone();
                drop(entry);
                self.tombstones.remove(&key);
            }
        }

        let mut expired = Vec::new();
        for entry in self.sessions.iter() {
            let session = Arc::clone(entry.value());
            let Some(reason) = session.expiry(now_ms) else {
                continue;
            };
            let session_id = session.id().to_string();
            drop(entry);
            self.sessions.remove(&session_id);
            self.tombstones.insert(
                session_id,
                (reason, now_ms + TOMBSTONE_TTL.as_millis() as u64),
            );
            expired.push((session, reason));
        }
        expired
    }
}

/// 把请求给的超时夹到 `[0, ceiling]`；0 表示未指定 -> 用 ceiling。
fn clamp_timeout(requested: Duration, ceiling: Duration) -> Duration {
    if requested.is_zero() || requested > ceiling {
        ceiling
    } else {
        requested
    }
}

/// 显式事务开启失败时的统一错误（把引擎语义收敛成一个对外错误码）。
pub fn transaction_lost(session_id: &str, detail: &str) -> PlatformError {
    PlatformError::transaction_lost(format!("会话 {session_id} 的事务不可用：{detail}"))
}

/// 会话不存在 / 已失效时的错误码（`SessionLost` 而不是 `NotFound`：
/// 对客户端而言"会话没了"的正确动作是重新 OpenSession）。
pub fn session_lost(session_id: &str, detail: &str) -> PlatformError {
    PlatformError::new(
        ErrorCode::SessionLost,
        format!("会话 {session_id} 不可用：{detail}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_timeout_is_detected() {
        // 空闲 60s：59.999s 仍可用，60s 整判定超时
        assert_eq!(decide_expiry(1_000, 0, 60_000, 60_999), None);
        assert_eq!(
            decide_expiry(1_000, 0, 60_000, 61_000),
            Some(ExpiryReason::IdleTimeout)
        );
    }

    #[test]
    fn transaction_lifetime_wins_over_idle() {
        // 事务到期且空闲也超时 -> 报更具体的结论
        assert_eq!(
            decide_expiry(1_000, 30_000, 60_000, 61_000),
            Some(ExpiryReason::TransactionLifetimeExceeded)
        );
    }

    #[test]
    fn live_transaction_keeps_session_alive() {
        // 事务未到期，且活动时间被刷新 -> 不回收
        assert_eq!(decide_expiry(50_000, 70_000, 60_000, 60_000), None);
    }

    #[test]
    fn expiry_reason_maps_to_frozen_codes() {
        assert_eq!(
            ExpiryReason::IdleTimeout.error_code(),
            ErrorCode::SessionIdleTimeout
        );
        assert_eq!(
            ExpiryReason::TransactionLifetimeExceeded.error_code(),
            ErrorCode::TransactionMaxLifetimeExceeded
        );
    }

    #[test]
    fn requested_timeouts_only_shrink() {
        let ceiling = domain::session::SESSION_IDLE_TIMEOUT;
        // 请求给 0 -> 默认；给得更小 -> 接受；给得更大 -> 夹到冻结值
        assert_eq!(clamp_timeout(Duration::ZERO, ceiling), ceiling);
        assert_eq!(
            clamp_timeout(Duration::from_millis(50), ceiling),
            Duration::from_millis(50)
        );
        assert_eq!(clamp_timeout(Duration::from_secs(3600), ceiling), ceiling);
    }
}
