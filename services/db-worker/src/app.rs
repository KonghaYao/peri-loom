//! db-worker 装配层：把 [`WorkerConfig`] 接成可运行的服务图（架构 §5 / §17.6 / §17.14）。
//!
//! 为什么单独一层而不是全塞进 `main.rs`：`main.rs` 只表达「解析配置 -> 交给装配」，
//! 启动顺序（共享状态 -> 预绑定监听 -> 后台任务 -> 停机）集中在一处，便于审阅与测试。
//!
//! 启动顺序的硬约束（架构 §17.14：**不得把「进程已启动」当作下游 Ready**）：
//! 1. 所有监听端口在起任何后台任务之前**同步预绑定**，端口占用必须在启动阶段暴露，
//!    而不是让「服务起来了但没人能连上」；绑定失败按有界退避重试（容器重启时上一代
//!    进程的端口可能尚未释放），仍失败则整个进程启动失败。
//! 2. 只有在监听全部绑定、全部后台任务都已 spawn 之后才打印「Worker 就绪」。
//! 3. 停机时先排空（`/readyz` 立刻 503）再撤服务，最后才处理 DB 子进程。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use protocol::control::worker_control_server::WorkerControlServer;
use protocol::data::worker_data_server::WorkerDataServer;
use protocol::framing::MAX_FRAME_BYTES;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::Server;

use observability::TelemetryConfig;

use crate::cli::WorkerConfig;
use crate::control::WorkerControlService;
use crate::dispatch::WorkerDataService;
use crate::heartbeat::{self, HeartbeatContext};
use crate::metrics;
use crate::ops::{self, OpsState};
use crate::registry::LocalDbRegistry;
use crate::resources::ResourceSampler;
use crate::restore::WorkSetPreparer;
use crate::supervisor::ProcessSupervisor;
use crate::uds::{DbConnectionPool, PoolConfig};
use crate::worker_state::WorkerStateMachine;

/// 遥测中的服务名（与 Ops `/healthz` 的 `service` 字段保持一致）。
const SERVICE_NAME: &str = "db-worker";

/// 单个监听的最大绑定尝试次数（含首次）。
const BIND_MAX_ATTEMPTS: u32 = 5;
/// 首次绑定失败后的退避。
const BIND_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
/// 绑定退避上限：启动耗时必须有界，编排系统的存活探测不会等太久。
const BIND_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// 停机时等待后台任务的宽限期；超过后不再等待，直接进入 DB 子进程处理。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// 装配并运行 Worker，直到收到停机信号。
pub async fn run(config: WorkerConfig) -> anyhow::Result<()> {
    let telemetry = init_telemetry(&config)?;

    // 数据目录 / 运行目录必须在 spawn DB Process 之前存在，否则 db-runtime 的
    // `--db-path` 会落在不存在的目录上（失败点太靠后，排障成本高）。
    config.ensure_dirs()?;
    warn_if_runtime_missing(&config);

    let cfg = Arc::new(config);
    metrics::set_worker_id_label(&cfg.worker_id);

    let state = Arc::new(WorkerStateMachine::new(cfg.worker_id.clone()));
    let registry = Arc::new(LocalDbRegistry::new());
    let pool = Arc::new(DbConnectionPool::new(
        PoolConfig {
            worker_id: cfg.worker_id.clone(),
            run_dir: cfg.run_dir.clone(),
            max_connections_per_db: cfg.db_max_connections,
            ..PoolConfig::default()
        },
        Arc::clone(&registry),
    ));

    // WAL 客户端允许缺席：Remote WAL 可能在 Worker 之后才就绪，构造期不建链（见
    // `WalClient::new`），只做端点校验；校验失败降级为「无远端回放」而不是拒绝启动。
    let wal = build_wal_client(&cfg);
    // 对象存储由 **Worker** 真正读写：快照上传（TriggerSnapshot）与冷启动时从快照
    // 预拉取工作集都在本进程完成（架构 §11.4 / §17.9）。缺席时这两条路径会返回
    // `STORAGE_UNAVAILABLE`，因此凭据齐全就必须构造出来 —— 不能像以前那样恒传 None。
    let store = build_object_store().await;
    let work_sets = WorkSetPreparer::new(
        cfg.data_dir.clone(),
        store.clone(),
        wal,
        cfg.version.clone(),
        domain::WorkerId::new(cfg.worker_id.clone()),
    );
    let supervisor = ProcessSupervisor::new(
        Arc::clone(&cfg),
        Arc::clone(&registry),
        Arc::clone(&pool),
        work_sets,
    );
    let sampler = Arc::new(ResourceSampler::new(
        cfg.capacity,
        supervisor.cgroups().clone(),
    ));

    let control = Arc::new(WorkerControlService::new(
        Arc::clone(&cfg),
        Arc::clone(&state),
        Arc::clone(&registry),
        Arc::clone(&supervisor),
        Arc::clone(&pool),
        Arc::clone(&sampler),
        // 与 WorkSetPreparer 共用同一个 store：快照的上传方与消费方必须是同一份配置，
        // 否则会出现「写得进、读不出」的静默不一致。
        store,
        domain::time::now_unix_ms(),
    ));
    let data = WorkerDataService::new(
        Arc::clone(&cfg),
        Arc::clone(&registry),
        Arc::clone(&pool),
        Arc::clone(&sampler),
    );

    // 预绑定：三个端口任一不可用都视为启动失败（§17.14）
    let control_listener = bind_with_retry(cfg.control_listen, "WORKER_CONTROL_LISTEN").await?;
    let data_listener = bind_with_retry(cfg.data_listen, "WORKER_DATA_LISTEN").await?;
    let ops_listener = bind_with_retry(cfg.ops_listen, "OPS_LISTEN").await?;

    // 三种服务共用一个进程内停机信号：watch(false) -> 置 true 即停机。
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let ops_state = Arc::new(OpsState {
        cfg: Arc::clone(&cfg),
        state: Arc::clone(&state),
        registry: Arc::clone(&registry),
        // `/metrics` 由 ops 自己在 `OPS_LISTEN` 上提供（observability 的 recorder 自带
        // 独立 http-listener，若让它监听 OPS_LISTEN 会和这里抢端口）。
        metrics: ops::install_prometheus(),
    });

    let mut tasks: Vec<(&'static str, JoinHandle<()>)> = Vec::new();

    let control_shutdown = shutdown_rx.clone();
    let control_service = Arc::clone(&control);
    tasks.push((
        "grpc/control",
        tokio::spawn(async move {
            let service = WorkerControlServer::from_arc(control_service)
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES);
            match Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    incoming_stream(control_listener, control_shutdown.clone()),
                    shutdown_fired(control_shutdown),
                )
                .await
            {
                Ok(()) => tracing::info!("WorkerControl gRPC 服务已停止"),
                Err(err) => tracing::error!(error = %err, "WorkerControl gRPC 服务异常退出"),
            }
        }),
    ));

    let data_shutdown = shutdown_rx.clone();
    tasks.push((
        "grpc/data",
        tokio::spawn(async move {
            // 单帧上限与 UDS 本地帧一致：数据面 gRPC 直接转发本地帧，两边上限不同会
            // 出现「本地能收、gRPC 收不下」的隐性截断。
            let service = WorkerDataServer::new(data)
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES);
            match Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    incoming_stream(data_listener, data_shutdown.clone()),
                    shutdown_fired(data_shutdown),
                )
                .await
            {
                Ok(()) => tracing::info!("WorkerData gRPC 服务已停止"),
                Err(err) => tracing::error!(error = %err, "WorkerData gRPC 服务异常退出"),
            }
        }),
    ));

    let ops_shutdown = shutdown_rx.clone();
    tasks.push((
        "ops/http",
        tokio::spawn(async move {
            if let Err(err) = ops::serve(ops_listener, ops_state, ops_shutdown).await {
                tracing::error!(error = %err, "Ops HTTP 服务异常退出");
            }
        }),
    ));

    // 心跳放在最后启动：它上报的是「本 Worker 已可服务」的状态，先保证控制面/数据面
    // 已就绪，否则 Server 会在 Worker 还接不了请求时把它纳入路由。
    tasks.push((
        "heartbeat",
        tokio::spawn(heartbeat::run(
            HeartbeatContext {
                cfg: Arc::clone(&cfg),
                state: Arc::clone(&state),
                registry: Arc::clone(&registry),
                sampler: Arc::clone(&sampler),
                control: Arc::clone(&control),
            },
            shutdown_rx.clone(),
        )),
    ));

    tracing::info!(
        worker_id = %cfg.worker_id,
        control_listen = %cfg.control_listen,
        data_listen = %cfg.data_listen,
        ops_listen = %cfg.ops_listen,
        server_control_endpoint = %cfg.server_control_endpoint,
        version = %cfg.version,
        "db-worker 就绪"
    );

    let signal = wait_for_shutdown_signal().await;
    tracing::info!(signal, "收到停机信号，开始优雅停机");

    // 先排空：`/readyz` 立刻返回 503，Server 侧不再把新流量路由过来（架构 §12.3）。
    // 这里只做状态迁移，不等排空完成 —— 进程退出本身就会断开既有连接。
    if let Err(err) = state.begin_drain() {
        tracing::warn!(error = %err, "停机时切换 DRAINING 失败");
    }

    // 取消停机信号：gRPC 走 graceful shutdown（等在途请求结束），ops / 心跳同步退出
    if shutdown_tx.send(true).is_err() {
        tracing::debug!("停机信号无接收者（任务已先行退出）");
    }
    let deadline = Instant::now() + SHUTDOWN_GRACE;
    for (name, handle) in tasks {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, handle).await {
            Ok(Ok(())) => tracing::debug!(task = name, "任务已退出"),
            Ok(Err(err)) => tracing::warn!(task = name, error = %err, "任务异常结束"),
            Err(_) => tracing::warn!(task = name, "任务未在宽限期内退出，放弃等待"),
        }
    }

    // 最后停 DB 子进程：Worker 退出后注册表（内存态）随之消失，新任 Worker 无法重新
    // 接管这些 fork 出来的进程；留下它们只会在同一数据目录上与新进程争抢（架构 §10
    // 的 Split Brain 风险）。显式停掉还能给 db-runtime 一个在宽限期内收尾（刷 WAL）
    // 的机会，比随容器命名空间一起被 SIGKILL 干净。
    let stopped = supervisor.shutdown_all(cfg.runtime_stop_grace).await;
    tracing::info!(stopped, "DB 子进程已停止");

    telemetry.shutdown();
    Ok(())
}

/// 初始化 tracing / metrics / OTLP。
fn init_telemetry(config: &WorkerConfig) -> anyhow::Result<observability::TelemetryGuard> {
    let mut telemetry = TelemetryConfig::from_env(SERVICE_NAME, config.version.clone());
    telemetry.instance_id = config.worker_id.clone();
    // `/metrics` 统一由 ops 在 OPS_LISTEN 暴露（见 `ops::install_prometheus` 的说明），
    // 因此即使环境里配了 METRICS_LISTEN_ADDR 也在这里忽略它，避免 recorder 另开端口。
    telemetry.metrics_listen_addr = None;
    observability::init(telemetry).map_err(|err| anyhow::anyhow!("telemetry 初始化失败：{err}"))
}

/// DB Runtime 二进制缺席只降级为警告：本 Worker 仍能服务既有 DB / 上报状态，
/// 拒绝启动反而会把「一个 DB 起不来」放大成「整节点下线」（§17.14）。
fn warn_if_runtime_missing(config: &WorkerConfig) {
    match std::fs::metadata(&config.db_runtime_bin) {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => tracing::warn!(
            bin = %config.db_runtime_bin.display(),
            "DB_RUNTIME_BIN 不是普通文件，StartDatabase 会失败"
        ),
        Err(err) => tracing::warn!(
            bin = %config.db_runtime_bin.display(),
            error = %err,
            "DB_RUNTIME_BIN 不可用，StartDatabase 会失败"
        ),
    }
}

/// 构造 Remote WAL 客户端；端点为空或非法时返回 `None`（WAL 回放能力缺席，不影响启动）。
fn build_wal_client(config: &WorkerConfig) -> Option<Arc<wal_client::WalClient>> {
    if config.wal_endpoints.is_empty() {
        tracing::info!("未配置 WAL_ENDPOINTS / WAL_CLUSTER，跳过 Remote WAL 回放能力");
        return None;
    }
    match wal_client::WalClient::new(wal_client::WalClientConfig {
        endpoints: config.wal_endpoints.clone(),
        ..wal_client::WalClientConfig::default()
    }) {
        Ok(client) => Some(Arc::new(client)),
        Err(err) => {
            tracing::warn!(error = %err, "WAL 客户端构造失败，降级为无远端回放");
            None
        }
    }
}

/// 构造对象存储客户端；`S3_*` 不完整时返回 `None`（快照 / 从快照恢复缺席，不影响启动）。
///
/// 与 WAL 客户端的处理一致：**能力缺席只降级、不拒绝启动**，但必须把「为什么缺席」
/// 如实打进日志 —— 否则 `TriggerSnapshot` 返回的 `STORAGE_UNAVAILABLE` 就成了唯一线索。
///
/// 「配得上」的判定与 db-server 的 `ObjectStoreConfig::is_configured()` 完全一致
/// （endpoint + bucket + 凭据），两边口径一致才不会出现「日志说配置了、接口说没配」。
async fn build_object_store() -> Option<Arc<dyn objectstore::ObjectStore>> {
    // S3Config::from_env 读的是与 db-server 同一份 env（含 `*_FILE` 形式，
    // compose 里凭据由 docker secrets 挂载）。
    let config = match objectstore::S3Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            // 只可能是一对凭据里只给了一半：错在部署，必须显式告警。
            tracing::warn!(error = %err, "对象存储配置非法，快照相关能力缺席");
            return None;
        }
    };

    let mut missing = Vec::new();
    if config.endpoint.is_none() {
        missing.push(objectstore::s3::ENV_ENDPOINT);
    }
    if config.access_key_id.is_none() {
        missing.push(objectstore::s3::ENV_ACCESS_KEY_ID);
        missing.push(objectstore::s3::ENV_SECRET_ACCESS_KEY);
    }
    if !missing.is_empty() {
        tracing::info!(
            bucket = %config.bucket,
            missing = ?missing,
            "未配置完整的对象存储（S3_*），快照上传 / 从快照恢复不可用"
        );
        return None;
    }

    let endpoint = config.endpoint.clone().unwrap_or_default();
    let bucket = config.bucket.clone();
    // S3ObjectStore::new 只做配置装配（AWS 配置链加载），不建链、不发请求；
    // 显式给了凭据就不会去访问实例元数据（IMDS）。
    match objectstore::S3ObjectStore::new(config).await {
        Ok(store) => {
            // 确保 bucket 存在：把这一步放进平台自己而不是部署时的初始化容器，
            // 否则「忘了跑初始化」或「换了对象存储实现」只会在第一次快照时才暴露，
            // 且错误信息与根因相距很远（例如 RustFS / MinIO / Ceph 的建桶工具各不相同）。
            // 幂等：已存在时 head 一下就返回；失败只告警，不阻断 Worker 启动
            // （Worker 的核心职责是跑 DB 进程，快照能力可以后补）。
            match store.ensure_bucket().await {
                Ok(()) => tracing::info!(
                    endpoint = %endpoint,
                    bucket = %bucket,
                    "对象存储已就绪：快照上传 / 冷启动从快照恢复可用"
                ),
                Err(err) => tracing::warn!(
                    endpoint = %endpoint,
                    bucket = %bucket,
                    error = %err,
                    "对象存储 bucket 校验/创建失败：快照能力可能在首次使用时才报错"
                ),
            }
            Some(Arc::new(store))
        }
        Err(err) => {
            tracing::warn!(error = %err, "对象存储构造失败，快照相关能力缺席");
            None
        }
    }
}

/// 绑定监听地址，失败按指数退避重试；仍失败则返回错误让启动路径失败。
///
/// 为什么要重试：容器滚动重启时上一代进程的监听可能尚未释放（TIME_WAIT / 端口回收
/// 延迟），立刻失败会让编排系统去重启一个其实马上就能起来的进程。
async fn bind_with_retry(addr: SocketAddr, role: &'static str) -> anyhow::Result<TcpListener> {
    let mut backoff = BIND_INITIAL_BACKOFF;
    let mut attempt = 1u32;
    loop {
        match TcpListener::bind(addr).await {
            Ok(listener) => {
                tracing::info!(role, addr = %addr, "监听已绑定");
                return Ok(listener);
            }
            Err(err) if attempt < BIND_MAX_ATTEMPTS => {
                tracing::warn!(
                    role,
                    addr = %addr,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %err,
                    "监听绑定失败，退避后重试"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BIND_MAX_BACKOFF);
                attempt += 1;
            }
            Err(err) => {
                return Err(anyhow::anyhow!("{role} 绑定 {addr} 失败：{err}"));
            }
        }
    }
}

/// 把 TCP listener 变成 tonic 需要的连接流。
///
/// accept 出错（fd 耗尽等）不应终止 server：记录后继续接受，否则一次瞬时错误会让
/// 整个节点永久停止服务；停机时流结束，tonic 进入 graceful shutdown。
fn incoming_stream(
    listener: TcpListener,
    shutdown: watch::Receiver<bool>,
) -> impl futures::Stream<Item = Result<TcpStream, std::io::Error>> + Send + 'static {
    futures::stream::unfold(listener, move |listener| {
        let mut shutdown = shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        // 停机信号置位或发送端消失都视为「不再接受新连接」
                        if *shutdown.borrow() || shutdown.has_changed().is_err() {
                            return None;
                        }
                    }
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _peer)) => {
                            // 内部 RPC：禁用 Nagle，避免小请求被攒批放大延迟
                            let _ = stream.set_nodelay(true);
                            return Some((Ok(stream), listener));
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "接受 gRPC 连接失败");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    },
                }
            }
        }
    })
}

/// 等待停机信号置位（或发送端消失）。
async fn shutdown_fired(mut shutdown: watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            // 发送端已丢弃 = 进程正在退出
            return;
        }
    }
}

/// 等待 SIGTERM / SIGINT。
///
/// 容器编排默认发 SIGTERM，本地调试用 Ctrl-C（SIGINT）：两者都必须走同一条优雅停机路径。
async fn wait_for_shutdown_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};

    match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(mut term), Ok(mut intr)) => {
            tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = intr.recv() => "SIGINT",
            }
        }
        (Ok(mut term), Err(err)) => {
            tracing::warn!(error = %err, "SIGINT 注册失败，仅响应 SIGTERM");
            term.recv().await;
            "SIGTERM"
        }
        (Err(err), Ok(mut intr)) => {
            tracing::warn!(error = %err, "SIGTERM 注册失败，仅响应 SIGINT");
            intr.recv().await;
            "SIGINT"
        }
        (Err(err), Err(_)) => {
            tracing::error!(error = %err, "信号注册失败，等待 Ctrl-C 兜底");
            let _ = tokio::signal::ctrl_c().await;
            "ctrl_c"
        }
    }
}
