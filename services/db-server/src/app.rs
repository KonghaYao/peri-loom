//! db-server 装配层（架构 §17.14 启动顺序）。
//!
//! 启动顺序是「依赖倒序 + 逐级确认」，任何一步都不把上一步的「进程已启动」当成
//! 下游就绪：
//!
//! ```text
//! 配置解析 -> telemetry -> Catalog 连接(退避重试) -> migrations -> 引导管理员
//!          -> AppState -> Route Cache 全量 reconcile -> 后台任务 -> HTTP 监听
//! ```
//!
//! 本文件只做装配，不含业务逻辑：HTTP 出口在 [`crate::api`]，路由与透明 Wake 在
//! [`crate::router`]，后台作业在 [`crate::background`]。
//!
//! 关于 gRPC：`SERVER_GRPC_LISTEN` 的 Control Path **服务端**尚未实现（本 crate 目前
//! 只有出向的 Worker 客户端 [`crate::clients`]），因此这里不启动 gRPC 监听——
//! 占着一个没人服务的端口比不监听更容易被误判成「Control Path 已就绪」。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use catalog::Catalog;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use observability::{TelemetryConfig, TelemetryGuard};
use routing::RouteCache;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::auth::{JwtIssuer, JwtVerifier};
use crate::background::{self, JobQueue};
use crate::clients::ChannelPool;
use crate::config::ServerConfig;
use crate::router::{DbRouter, RouterConfig};
use crate::state::{AppState, BackgroundConfig, DistributedState, HttpConfig, Readiness};

/// 服务名（日志 / trace 的 service 标识，冻结）。
pub const SERVICE_NAME: &str = "db-server";

/// 构造 Remote WAL 客户端（控制面**只读** WAL 状态）。
///
/// 用途单一但关键：启动被存储层 fencing 拒绝时，控制面需要读回 WAL 已记录的
/// `owner_epoch` 才能把 Catalog 对齐到存储层（架构 §11.3）。端点未配置（或全部非法）
/// 时返回 `None` —— 这是一条恢复能力，不是启动前提，控制面必须能照常起来。
fn build_wal_client(config: &ServerConfig) -> Option<Arc<wal_client::WalClient>> {
    if config.wal_endpoints.is_empty() {
        warn!("未配置 WAL_ENDPOINTS / WAL_CLUSTER：启动被 fencing 拒绝时无法向存储层对齐 epoch");
        return None;
    }
    match wal_client::WalClient::new(wal_client::WalClientConfig {
        endpoints: config.wal_endpoints.clone(),
        ..wal_client::WalClientConfig::default()
    }) {
        Ok(client) => Some(Arc::new(client)),
        Err(err) => {
            warn!(error = %err, "WAL 客户端构造失败，降级为「不读存储层 epoch」");
            None
        }
    }
}

/// Catalog 重试的初始退避。
const RETRY_BASE: Duration = Duration::from_millis(500);
/// Catalog 重试的退避上限：退避到分钟级只会让依赖恢复后本进程仍长时间不 Ready。
const RETRY_MAX: Duration = Duration::from_secs(10);
/// migration 连续失败上限：连接不上属于「下游没起来」（可无限等），而迁移本身反复
/// 失败更可能是 schema 与代码不匹配，必须有限次后显式失败，否则会安静地永远卡住。
const MIGRATION_ATTEMPTS: u32 = 10;

/// 装配并运行 db-server，直到收到停机信号。
///
/// # Errors
/// Catalog 不可达（重试耗尽的是 migration 环节）、引导管理员创建失败、监听端口不可用、
/// HTTP 服务异常退出时返回错误；调用方应映射为非零退出码，让编排系统重启/告警，
/// 而不是让进程「带着坏状态」继续活着。
pub async fn run(config: ServerConfig) -> Result<()> {
    let config = Arc::new(config);

    // guard 必须活到进程结束：drop 会关闭日志与 trace 的提交
    let telemetry = install_telemetry();
    let metrics = install_prometheus();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %crate::config::redacted_summary(&config),
        "db-server 启动中"
    );

    let catalog = connect_catalog(&config).await?;
    bootstrap_admin(&config, &catalog).await?;

    let routes = Arc::new(RouteCache::new(config.route_cache_max_entries));
    let channels = Arc::new(ChannelPool::new());
    let wal = build_wal_client(&config);
    let router = Arc::new(DbRouter::new(
        catalog.clone(),
        routes.clone(),
        channels.clone(),
        wal,
        RouterConfig {
            wakeup_timeout: config.wakeup_timeout,
            wakeup_poll_interval: config.wakeup_poll_interval,
            inline_result_limit_bytes: config.inline_result_limit_bytes,
        },
    ));
    let jobs = Arc::new(JobQueue::new(Arc::new(catalog.clone())));
    let jwt = config
        .jwt_secret
        .as_deref()
        .map(|secret| Arc::new(JwtVerifier::hs256(secret, &config.jwt_issuer)));
    // 签发器与校验器共用同一密钥：本地登录（POST /api/v1/auth/login）用它换 JWT。
    // 生产接 OIDC 时仍可保留：平台自身签发的 token 与 IdP 的 token 走同一条校验路径。
    let jwt_issuer = config.jwt_secret.as_deref().map(|secret| {
        Arc::new(JwtIssuer::hs256(
            secret,
            &config.jwt_issuer,
            config.jwt_ttl_seconds,
        ))
    });
    if jwt.is_none() {
        warn!("未配置 JWT_SECRET_FILE / JWT_SECRET：本次只接受 x-api-token 认证");
    }

    let shared = AppState {
        config: Arc::new(HttpConfig {
            inline_result_limit_bytes: config.inline_result_limit_bytes,
            session_idle_timeout_ms: config.session_idle_timeout_ms,
        }),
        catalog: Arc::new(catalog.clone()),
        execution: Arc::new(crate::execution::DistributedExecutor {
            router: router.clone(),
            channels: channels.clone(),
        }),
        sessions: Arc::new(crate::state::SessionRegistry::new()),
        readiness: Arc::new(Readiness::new()),
        jwt,
        jwt_issuer,
        metrics,
        jobs,
        deployment: crate::deployment::Deployment::Distributed(Arc::new(
            crate::deployment::DistributedServices {
                catalog: catalog.clone(),
                router: router.clone(),
            },
        )),
    };
    let state = DistributedState {
        shared,
        config: config.clone(),
        background: Arc::new(BackgroundConfig::default()),
        catalog,
        router,
        routes,
        channels,
    };

    // 先 bind 再跑后台任务：端口占用这类错误必须在启动早期暴露，而不是等一堆后台
    // 任务起来了、对外却接不了请求。
    let http_listener = TcpListener::bind(config.http_listen)
        .await
        .with_context(|| format!("绑定 HTTP 监听 {} 失败", config.http_listen))?;
    let ops_listener = match config.ops_listen {
        Some(addr) => Some(
            TcpListener::bind(addr)
                .await
                .with_context(|| format!("绑定 OPS 监听 {addr} 失败"))?,
        ),
        None => None,
    };

    // 冷启动窗口里 Route Cache 还是空的：先做一次全量 reconcile，避免第一个请求把
    // 已经 READY 的 DB 判成「无路由」。失败不阻断启动——控制面短暂读不到 Catalog 只
    // 影响新路由，后台 reconciler 会周期重试（架构 §17.4：reconcile 是正确性路径，
    // 但不是「启动这一刻」的阻塞条件）。
    match state.router.reconcile_all().await {
        Ok(count) => {
            state.readiness.mark_reconciler_ready();
            info!(routes = count, "启动时 Route Cache 全量 reconcile 完成");
        }
        Err(err) => warn!(
            code = err.code().as_str(),
            message = %err,
            "启动时 Route Cache reconcile 失败，交由后台 reconciler 周期重试"
        ),
    }

    let background_tasks = background::spawn_all(&state);

    // 两个监听器共用同一份停机决定：外部信号只翻译一次成 watch 置位，避免
    // 「一个监听器走了、另一个还挂着」的半停状态。
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_task = tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            wait_for_shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        }
    });

    let http_router = crate::api::build_router(state.shared.clone());
    info!(addr = %local_addr(&http_listener), "HTTP 服务启动");
    let http_task = tokio::spawn(serve(http_listener, http_router, shutdown_rx.clone()));

    // Worker 上报入口（gRPC）：心跳方向是 Worker -> Server，必须由 Server 侧监听。
    // 没有它 Worker 永远注册不上，Scheduler 会因为「候选总数 0」而无法放置任何 DB。
    let grpc_task = tokio::spawn(serve_ingress(
        Arc::new(state.clone()),
        config.grpc_listen,
        shutdown_rx.clone(),
    ));

    let ops_task = ops_listener.map(|listener| {
        // OPS 端口只服务 /healthz /readyz /metrics：探针与指标口不应该混在对外 HTTP 出口上
        let ops_router = crate::api::system::routes().with_state(state.shared.clone());
        info!(addr = %local_addr(&listener), "OPS 服务启动（/healthz /readyz /metrics）");
        tokio::spawn(serve(listener, ops_router, shutdown_rx.clone()))
    });

    // 主监听器结束即服务生命周期结束：正常关停与运行时错误都收敛到这一条路径
    let outcome = match http_task.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            error!(error = %err, "HTTP 服务异常退出");
            Err(anyhow::Error::new(err))
        }
        Err(err) => {
            error!(error = %err, "HTTP 服务任务异常（panic / 被取消）");
            Err(anyhow::Error::new(err))
        }
    };

    // 收敛停机：无论 HTTP 是收到信号后正常退出，还是自己挂了，其余部分都要停
    let _ = shutdown_tx.send(true);
    signal_task.abort();
    // grpc 监听器与 HTTP 同生命周期：任一退出都收敛到下面的收尾逻辑
    let _ = grpc_task.await;

    if let Some(task) = ops_task {
        let _ = task.await;
    }
    for task in background_tasks {
        // 后台任务都是「这一轮失败就等下一轮」的循环，没有需要 flush 的状态，
        // 停机时直接取消，不必再等满一个心跳/驱逐周期。
        task.abort();
    }
    if let Some(guard) = telemetry {
        guard.shutdown();
    }
    info!("db-server 已停止");
    outcome
}

/// 安装平台统一 telemetry。
///
/// 关于 `METRICS_LISTEN_ADDR`：本服务显式清空该字段。两个原因：
/// 1. `/metrics` 由 [`crate::api`] 用 `AppState::metrics` 渲染，而 `observability::init`
///    安装 recorder 时**不返回句柄**，进程内就再拿不到渲染入口；
/// 2. observability 的 recorder 自带 http-listener，会与 OPS_LISTEN 形成两个采集端点。
///
/// 因此指标统一走 OPS_LISTEN（见 [`install_prometheus`]）。
///
/// 观测安装失败不阻断服务：日志/指标缺失不该让控制面整体不可用。
fn install_telemetry() -> Option<TelemetryGuard> {
    let mut telemetry = TelemetryConfig::from_env(SERVICE_NAME, env!("CARGO_PKG_VERSION"));
    if let Some(addr) = telemetry.metrics_listen_addr {
        warn!(
            addr = %addr,
            "METRICS_LISTEN_ADDR 已设置但被忽略：指标统一由 OPS_LISTEN 的 /metrics 暴露"
        );
        telemetry.metrics_listen_addr = None;
    }
    match observability::init(telemetry) {
        Ok(guard) => Some(guard),
        Err(err) => {
            eprintln!("telemetry 初始化失败（不影响 db-server）：{err}");
            None
        }
    }
}

/// 安装全局 Prometheus recorder，只取渲染句柄、不开额外监听端口。
///
/// 分桶与 observability 保持一致（延迟类指标按**微秒**分桶），否则同一条曲线在两个
/// 端点上会呈现不同的分位数语义。
pub(crate) fn install_prometheus() -> PrometheusHandle {
    let installed = PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_micros".to_owned()),
            observability::metrics::LATENCY_BUCKETS_MICROS,
        )
        .and_then(|builder| builder.install_recorder());
    match installed {
        Ok(handle) => {
            preinitialize_route_cache_counters();
            handle
        }
        Err(err) => {
            // 指标不是业务路径：recorder 被占用时降级为空句柄（/metrics 渲染空文本），
            // 不影响 HTTP 出口与数据面。
            warn!(error = %err, "/metrics 将不可用（Prometheus recorder 安装失败）");
            PrometheusBuilder::new().build_recorder().handle()
        }
    }
}

/// 启动时以 0 值预注册 Route Cache 计数器的时间序列。
///
/// 为什么必须预注册：Prometheus 只在时间序列**首次被写入**后才导出它。命中率
/// `hits / (hits + misses)`（架构 §16 的 >= 99.9%）是**两个**序列的比值，
/// 只要 `misses` 从未被写过（例如本进程还没遇到过冷路由），该序列就不存在，
/// 整个比值表达式在 PromQL 里无数据：Grafana 面板空白、命中率告警永远不会触发。
/// 同理，「没有未命中」与「指标没接线」在 /metrics 上也会变得无法区分
/// （验收脚本此前只能 SKIP 的原因）。写 0 后，0 明确表示「一次都没发生」。
///
/// 计数器在热路径上仍是纯 `Relaxed` 自增，这里只做一次注册，不影响写路径。
fn preinitialize_route_cache_counters() {
    for name in [
        observability::metrics::metric_names::ROUTE_CACHE_HIT_TOTAL,
        observability::metrics::metric_names::ROUTE_CACHE_MISS_TOTAL,
        observability::metrics::metric_names::ROUTE_CACHE_INVALIDATION_TOTAL,
        observability::metrics::metric_names::ROUTE_CACHE_STALE_DETECTED_TOTAL,
    ] {
        metrics::counter!(name).increment(0);
    }
}

/// 连接 PostgreSQL，失败则退避重试直到成功，然后跑 migrations。
///
/// 为什么不直接退出（架构 §17.14）：**「容器已启动」不等于下游 Ready**。PostgreSQL
/// 与本服务常在同一编排里滚动，端口可能还没开始 accept；此时崩溃退出会把「启动顺序」
/// 问题变成 CrashLoop 告警，而控制面本可以等待。
async fn connect_catalog(config: &ServerConfig) -> Result<Catalog> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match Catalog::connect(&config.database_url, config.catalog_max_connections).await {
            Ok(catalog) => {
                migrate_catalog(&catalog).await?;
                return Ok(catalog);
            }
            Err(err) => {
                let backoff = retry_backoff(attempt);
                warn!(
                    attempt,
                    code = err.code.as_str(),
                    message = %err,
                    backoff_ms = backoff.as_millis(),
                    "连接 PostgreSQL Catalog 失败，退避后重试（服务尚未 Ready）"
                );
                sleep(backoff).await;
            }
        }
    }
}

/// 跑 Catalog migrations（有限次退避重试）。
async fn migrate_catalog(catalog: &Catalog) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match catalog.migrate().await {
            Ok(()) => {
                info!(attempt, "Catalog migrations 已应用");
                return Ok(());
            }
            Err(err) if attempt < MIGRATION_ATTEMPTS => {
                let backoff = retry_backoff(attempt);
                warn!(
                    attempt,
                    message = %err,
                    backoff_ms = backoff.as_millis(),
                    "Catalog migrations 失败，退避后重试"
                );
                sleep(backoff).await;
            }
            Err(err) => {
                return Err(anyhow::Error::new(err)
                    .context("Catalog migrations 连续失败（schema 与代码版本可能不匹配）"))
            }
        }
    }
}

/// 首次启动创建引导管理员（仅当 `users` 表为空）。
///
/// 没有管理员等于平台无人可登录，因此这一步失败必须让启动失败，而不是「先跑起来再说」。
/// 未配置 BOOTSTRAP_ADMIN_* 时只告警：集群可能用 OIDC / 已有账号，强行阻止启动会
/// 让「首次部署忘记配置」变成无法自愈的 CrashLoop。
async fn bootstrap_admin(config: &ServerConfig, catalog: &Catalog) -> Result<()> {
    let (Some(username), Some(password)) = (
        config.bootstrap_admin_user.as_deref(),
        config.bootstrap_admin_password.as_deref(),
    ) else {
        warn!(
            "未配置 BOOTSTRAP_ADMIN_USER / BOOTSTRAP_ADMIN_PASSWORD(_FILE)：若 users 表为空则无人可登录"
        );
        return Ok(());
    };

    // 口令只在内存里传给 argon2 做哈希，绝不进日志/审计
    match crate::auth::bootstrap_admin(catalog, username, password).await {
        Ok(Some(created)) => info!(username = %created, "users 表为空，已创建引导管理员"),
        Ok(None) => info!("users 表已有用户，跳过引导管理员创建"),
        Err(err) => return Err(anyhow::Error::new(err).context("创建引导管理员失败")),
    }
    Ok(())
}

/// 启动一个 axum 监听器，直到 `shutdown` 置位或通道关闭。
async fn serve(
    listener: TcpListener,
    router: axum::Router,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            loop {
                // 先判断当前值：信号可能早于监听器启动（例如启动过程中 Ctrl-C）
                let already_requested = *shutdown.borrow_and_update();
                if already_requested {
                    return;
                }
                // Err = 发送端已 drop，等价于停机
                if shutdown.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
}

/// 监听器实际绑定的地址（用于日志；`:0` 这类配置只有 bind 之后才知道真实端口）。
fn local_addr(listener: &TcpListener) -> String {
    listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_owned())
}

/// 第 `attempt` 次重试前的退避：指数增长并封顶。
fn retry_backoff(attempt: u32) -> Duration {
    // 移位封顶在 5：5500ms 已经超过上限，继续放大只会浪费位数
    let factor = 1u32 << attempt.saturating_sub(1).min(5);
    (RETRY_BASE * factor).min(RETRY_MAX)
}

/// 等待停机信号（SIGINT / SIGTERM）。
///
/// 两个都要接：SIGINT 对应本地 Ctrl-C，SIGTERM 对应容器编排（docker stop / 滚动更新）。
/// 不接 SIGTERM 会让每次发布都退化成「强杀 + 重放」。
pub(crate) async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(err) => {
                warn!(error = %err, "无法注册 SIGTERM 处理，仅等待 SIGINT");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    warn!(error = %err, "等待 SIGINT 失败");
                }
            }
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// 启动 ServerIngress gRPC 服务（Worker 心跳入口）。
///
/// 绑定失败属于启动期致命错误：没有心跳入口就没有任何 Worker 可用，服务起来也是废的。
/// 因此这里返回 error 而不是静默降级。
async fn serve_ingress(
    state: Arc<DistributedState>,
    addr: SocketAddr,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("绑定 gRPC 监听 {addr} 失败（Worker 心跳入口）"))?;
    let local = listener.local_addr().unwrap_or(addr);
    info!(addr = %local, "gRPC 服务启动（ServerIngress：Worker 心跳）");

    let service = crate::ingress::server(state);
    tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async move {
                // watch 已关闭（发送端 drop）时 recv 会返回 Err，同样视为停机信号
                let _ = shutdown.changed().await;
            },
        )
        .await
        .context("gRPC 服务异常退出")
}
