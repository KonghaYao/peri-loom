//! 进程内共享状态。
//!
//! `AppState` 是 axum 的 `State`，因此必须 `Clone + Send + Sync`：
//! 内部全部是 `Arc` / 连接池句柄 / 零拷贝结构，克隆成本可以忽略。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use catalog::Catalog;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use domain::ids::{DatabaseId, WorkerId};
use metrics_exporter_prometheus::PrometheusHandle;
use routing::RouteCache;

use crate::auth::{JwtIssuer, JwtVerifier};
use crate::background::JobQueue;
use crate::clients::ChannelPool;
use crate::config::ServerConfig;
use crate::router::DbRouter;

/// 显式会话的本地绑定（架构 §13.2）。
///
/// Catalog 里**没有** session 表：会话是 Worker 本地连接上下文，Server 只保存
/// 「这个 session 属于哪个 DB / 哪个 Worker / 哪个 epoch」用于路由与失效判定。
/// Server 重启会丢失本地会话（客户端收到 `SESSION_NOT_FOUND` 重新开会话），
/// 这是可接受的降级，也比把会话状态写进权威 Catalog 更符合「会话不进控制面」的定位。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBinding {
    /// 会话 ID（注册表以它为 key，这里再存一份便于清理任务直接拿到完整信息）。
    pub session_id: String,
    /// 所属数据库。
    pub database_id: DatabaseId,
    /// 会话 pin 住的 Worker。
    pub worker_id: WorkerId,
    /// 建会话时的 Owner Epoch；epoch 变化即说明发生过 failover，会话不可恢复。
    pub owner_epoch: u64,
    /// 会话建立时间。
    pub created_at: DateTime<Utc>,
    /// 本地判定的过期时间（Worker 侧还有自己的空闲计时）。
    pub expires_at: DateTime<Utc>,
}

impl SessionBinding {
    /// 是否已过期（按 Server 侧的空闲超时判定）。
    #[must_use]
    pub fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at <= now
    }
}

/// 显式会话注册表。
///
/// 除了会话绑定本身，还托管**会话附属的 SQL 缓存**（Hrana `store_sql`）：兼容层把
/// `sql_id -> SQL 文本` 存在这里而不是另起一张表，因为它的生命周期与会话严格一致 ——
/// 会话失效 / 过期 / 关闭时必须一起丢弃，否则会留下引用已死会话的内存。
#[derive(Debug, Default)]
pub struct SessionRegistry {
    sessions: DashMap<String, SessionBinding>,
    /// `session_id -> (sql_id -> SQL 文本)`。
    sql_caches: DashMap<String, Arc<DashMap<i64, String>>>,
    /// `session_id -> 该会话连接当前的 autocommit 状态`。
    ///
    /// Hrana v3 的 `get_autocommit` 请求与批处理里的 `is_autocommit` 条件都要求
    /// **服务端权威回答**「这条连接现在是不是在事务里」。Session Plane 上唯一知道
    /// 答案的是 DB Process（引擎的 `sqlite3_get_autocommit()` 等价物），所以这里
    /// 存的是**最近一次执行回传的观测值**（流式 trailer 的 `is_autocommit`），
    /// 而不是 Server 自己从 SQL 文本猜出来的结论 —— 猜会在语句失败、引擎回滚、
    /// 隐式事务等情形下出错，而客户端明确把这个值当作「唯一可靠信号」。
    ///
    /// 初值 `true`：会话对应的连接刚建立、还没执行过任何语句，必然处于 autocommit。
    /// 与会话同生命周期地清理（理由同 `sql_caches`）。
    autocommit: DashMap<String, Arc<AtomicBool>>,
}

impl SessionRegistry {
    /// 构造空注册表。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 登记会话。
    pub fn insert(&self, session_id: String, binding: SessionBinding) {
        self.sessions.insert(session_id, binding);
    }

    /// 取（必要时创建）会话附属的 SQL 缓存。
    ///
    /// 只为**已登记**的会话创建缓存由调用方保证：兼容层总是先取到 [`SessionBinding`]
    /// 再写入 SQL，不会给不存在的会话开缓存。
    #[must_use]
    pub fn sql_cache(&self, session_id: &str) -> Arc<DashMap<i64, String>> {
        self.sql_caches
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(DashMap::new()))
            .clone()
    }

    /// 取会话附属的 SQL 缓存（不存在时返回 `None`，不创建）。
    #[must_use]
    pub fn sql_cache_existing(&self, session_id: &str) -> Option<Arc<DashMap<i64, String>>> {
        self.sql_caches.get(session_id).map(|entry| entry.clone())
    }

    /// 查一条已缓存的 SQL 文本。
    #[must_use]
    pub fn sql_cache_get(&self, session_id: &str, sql_id: i64) -> Option<String> {
        self.sql_caches.get(session_id).and_then(|cache| {
            // 必须在闭包里就把值克隆出来：内层 `Ref` 借用的是外层 `Ref` 里的 `Arc`。
            let entry = cache.get(&sql_id)?;
            Some(entry.value().clone())
        })
    }

    /// 取（必要时创建）会话的 autocommit 状态槽。
    ///
    /// 与 [`SessionRegistry::sql_cache`] 同约定：只为**已登记**的会话创建由调用方保证。
    /// 返回 `Arc` 而不是值，调用方才能在一次请求内持续读到别的路径写入的新观测值。
    #[must_use]
    pub fn autocommit(&self, session_id: &str) -> Arc<AtomicBool> {
        self.autocommit
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::new(AtomicBool::new(true)))
            .clone()
    }

    /// 查询会话绑定。
    #[must_use]
    pub fn get(&self, session_id: &str) -> Option<SessionBinding> {
        self.sessions.get(session_id).map(|entry| entry.clone())
    }

    /// 注销会话（关闭 / 失效），同时丢弃会话附属的 SQL 缓存与 autocommit 观测值。
    pub fn remove(&self, session_id: &str) -> Option<SessionBinding> {
        self.sql_caches.remove(session_id);
        self.autocommit.remove(session_id);
        self.sessions.remove(session_id).map(|(_, value)| value)
    }

    /// 当前会话数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// 是否为空。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// 摘除已过期会话并返回它们的绑定信息。
    ///
    /// 返回绑定而不是计数：调用方需要 `worker_id` 才能把 `CloseSession` 送到 Worker，
    /// 否则 Worker 侧的连接只能等它自己的空闲计时回收。
    pub fn take_expired(&self, now: DateTime<Utc>) -> Vec<SessionBinding> {
        let expired: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.value().is_expired_at(now))
            .map(|entry| entry.key().clone())
            .collect();
        expired
            .into_iter()
            .filter_map(|key| {
                self.sql_caches.remove(&key);
                self.autocommit.remove(&key);
                self.sessions.remove(&key).map(|(_, value)| value)
            })
            .collect()
    }
}

/// 就绪状态标记。
///
/// `/readyz` 的语义是「能不能开始接流量」：PostgreSQL 可达 + 心跳监控已在跑。
/// 未就绪时返回 503，让负载均衡把实例摘掉，而不是让它带着未知状态接请求。
#[derive(Debug, Default)]
pub struct Readiness {
    heartbeat_monitor: AtomicBool,
    reconciler: AtomicBool,
}

impl Readiness {
    /// 新建。
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 标记心跳监控已启动。
    pub fn mark_heartbeat_monitor_ready(&self) {
        self.heartbeat_monitor.store(true, Ordering::Release);
    }

    /// 标记 route reconcile 已启动。
    pub fn mark_reconciler_ready(&self) {
        self.reconciler.store(true, Ordering::Release);
    }

    /// 心跳监控是否就绪。
    #[must_use]
    pub fn heartbeat_monitor_ready(&self) -> bool {
        self.heartbeat_monitor.load(Ordering::Acquire)
    }

    /// reconcile 是否就绪。
    #[must_use]
    pub fn reconciler_ready(&self) -> bool {
        self.reconciler.load(Ordering::Acquire)
    }

    /// 是否整体就绪。
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.heartbeat_monitor_ready() && self.reconciler_ready()
    }
}

/// 后台任务的运行参数（与 `ServerConfig` 分开，便于测试注入短周期）。
#[derive(Debug, Clone)]
pub struct BackgroundConfig {
    /// 心跳检查周期。
    pub heartbeat_interval: Duration,
    /// 心跳超时判定使用的「连续 miss」阈值。
    pub suspect_threshold: i32,
    /// ownership 租约 GC 周期。
    pub ownership_gc_interval: Duration,
    /// 判定 ownership 过期的额外余量。
    pub ownership_stale_grace: Duration,
    /// 生命周期回收周期。
    pub eviction_interval: Duration,
    /// WARM 空闲多久允许被驱逐。
    pub warm_idle_eviction_after: Duration,
    /// Job Runner 轮询周期。
    pub job_poll_interval: Duration,
    /// Job lease 时长。
    pub job_lease_ttl: Duration,
    /// 本地会话清理周期。
    pub session_sweep_interval: Duration,
}

impl Default for BackgroundConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: crate::config::HEARTBEAT_CHECK_INTERVAL,
            suspect_threshold: crate::config::SUSPECT_MISS_THRESHOLD,
            ownership_gc_interval: crate::config::OWNERSHIP_GC_INTERVAL,
            ownership_stale_grace: crate::config::OWNERSHIP_STALE_GRACE,
            eviction_interval: crate::config::EVICTION_INTERVAL,
            warm_idle_eviction_after: crate::config::WARM_IDLE_EVICTION_AFTER,
            job_poll_interval: crate::config::JOB_POLL_INTERVAL,
            job_lease_ttl: crate::config::JOB_LEASE_TTL,
            session_sweep_interval: crate::config::SESSION_SWEEP_INTERVAL,
        }
    }
}

/// 全局共享状态。
#[derive(Clone)]
pub struct AppState {
    /// 运行配置。
    pub config: Arc<ServerConfig>,
    /// 后台任务参数。
    pub background: Arc<BackgroundConfig>,
    /// Catalog（PostgreSQL）句柄。
    pub catalog: Catalog,
    /// DB Router（热路径 + 透明 Wake）。
    pub router: Arc<DbRouter>,
    /// Route Cache（供 metrics / reconcile 直接访问）。
    pub routes: Arc<RouteCache>,
    /// Worker gRPC 连接池。
    pub channels: Arc<ChannelPool>,
    /// 显式会话注册表。
    pub sessions: Arc<SessionRegistry>,
    /// 长操作 job 投递器（写 PostgreSQL + 唤醒本地 Runner）。
    pub jobs: Arc<JobQueue>,
    /// 就绪标记。
    pub readiness: Arc<Readiness>,
    /// JWT 校验器（未配置密钥时为 `None`，此时只能用 API Token）。
    pub jwt: Option<Arc<JwtVerifier>>,
    /// JWT 签发器（与校验器同时配置；未配置时登录接口返回有明确语义的错误）。
    pub jwt_issuer: Option<Arc<JwtIssuer>>,
    /// Prometheus 渲染句柄。
    pub metrics: PrometheusHandle,
}

impl AppState {
    /// 组装状态。
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        config: Arc<ServerConfig>,
        background: Arc<BackgroundConfig>,
        catalog: Catalog,
        router: Arc<DbRouter>,
        routes: Arc<RouteCache>,
        channels: Arc<ChannelPool>,
        readiness: Arc<Readiness>,
        jwt: Option<Arc<JwtVerifier>>,
        jwt_issuer: Option<Arc<JwtIssuer>>,
        metrics: PrometheusHandle,
        jobs: Arc<JobQueue>,
    ) -> Self {
        Self {
            config,
            background,
            catalog,
            router,
            routes,
            channels,
            sessions: Arc::new(SessionRegistry::new()),
            jobs,
            readiness,
            jwt,
            jwt_issuer,
            metrics,
        }
    }
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 不打印 config（含密钥字段）与 catalog（连接串）
        f.debug_struct("AppState")
            .field("routes", &self.routes.stats())
            .field("sessions", &self.sessions.len())
            .field("ready", &self.readiness.is_ready())
            .field("jwt_enabled", &self.jwt.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn binding(minutes: i64) -> SessionBinding {
        let now = Utc::now();
        SessionBinding {
            session_id: "s1".to_string(),
            database_id: DatabaseId::new_v7(),
            worker_id: WorkerId::new("w1"),
            owner_epoch: 3,
            created_at: now,
            expires_at: now + ChronoDuration::minutes(minutes),
        }
    }

    #[test]
    fn session_registry_insert_get_remove() {
        let registry = SessionRegistry::new();
        let value = binding(5);
        registry.insert("s1".to_string(), value.clone());
        assert_eq!(registry.get("s1"), Some(value));
        assert_eq!(registry.len(), 1);
        assert!(registry.remove("s1").is_some());
        assert!(registry.get("s1").is_none());
        assert!(registry.is_empty());
    }

    #[test]
    fn take_expired_removes_only_expired_sessions_and_returns_bindings() {
        let registry = SessionRegistry::new();
        registry.insert("alive".to_string(), binding(5));
        registry.insert("dead".to_string(), binding(-5));
        let expired = registry.take_expired(Utc::now());
        assert_eq!(expired.len(), 1);
        // 返回的绑定必须能定位到 Worker —— 清理任务要靠它把 CloseSession 送出去。
        assert_eq!(expired[0].worker_id, WorkerId::new("w1"));
        assert!(registry.get("alive").is_some());
        assert!(registry.get("dead").is_none());
    }

    #[test]
    fn readiness_requires_all_components() {
        let readiness = Readiness::new();
        assert!(!readiness.is_ready());
        readiness.mark_heartbeat_monitor_ready();
        assert!(!readiness.is_ready());
        readiness.mark_reconciler_ready();
        assert!(readiness.is_ready());
    }
}
