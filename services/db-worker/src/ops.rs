//! Ops HTTP（axum）：`/healthz` / `/readyz` / `/metrics`。
//!
//! 这是 Worker 唯一对外的 HTTP 端口（架构 §17.4：Ops 端点只允许内网暴露）。
//! 三个端点的语义必须分清，否则编排系统会做出错误决策：
//!
//! | 端点 | 语义 | 不健康时 |
//! | --- | --- | --- |
//! | `/healthz` | **liveness**：进程还活着（不检查任何依赖） | 由编排系统重启容器 |
//! | `/readyz` | **readiness**：是否还能承接数据面流量 | 从负载均衡摘除，**不重启** |
//! | `/metrics` | Prometheus 抓取 | — |
//!
//! `/readyz` 复用 Worker 状态机：DRAINING / EMPTY 时返回 503，这正是排空期间
//! 「不要再把新流量路由过来」的表达（架构 §12.3）。**注意**：DRAINING 的 Worker 上
//! 仍有既有 DB 在服务在途请求，因此 503 只影响新流量接入，不影响进程存活。

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::Serialize;
use tokio::sync::watch;

use crate::cli::WorkerConfig;
use crate::registry::LocalDbRegistry;
use crate::worker_state::WorkerStateMachine;

/// Ops 服务共享状态。
pub struct OpsState {
    /// 配置（上报 worker_id / 版本）。
    pub cfg: Arc<WorkerConfig>,
    /// Worker 状态机（readiness 判据）。
    pub state: Arc<WorkerStateMachine>,
    /// 本地注册表（readiness 附带本地 DB 数，便于排障）。
    pub registry: Arc<LocalDbRegistry>,
    /// Prometheus 渲染句柄；`None` 表示 recorder 未安装（`/metrics` 返回 503）。
    pub metrics: Option<PrometheusHandle>,
}

impl OpsState {
    /// 就绪判据：仍在服务既有流量。
    ///
    /// `serves_traffic()` 在 DRAINING / EMPTY 时返回 false（见 worker_state 模块），
    /// 与「摘除新流量」的运维动作一致；ACTIVE 时为 true。
    fn is_ready(&self) -> bool {
        self.state.serves_traffic()
    }
}

/// liveness 响应体。
#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    service: &'static str,
    worker_id: String,
    version: String,
}

/// readiness 响应体。
#[derive(Serialize)]
struct ReadyBody {
    status: &'static str,
    worker_id: String,
    worker_state: String,
    databases: usize,
    serving: usize,
}

/// `GET /healthz`：进程存活即可，不检查任何依赖。
///
/// 为什么不检查依赖：Worker 的依赖（Server 控制面 / Remote WAL / Object Store）都允许
/// 短期不可用，而**不应导致重启** —— 重启会杀掉正在服务的 DB 进程，把可用性问题放大成
/// 数据面故障。
async fn healthz(State(state): State<Arc<OpsState>>) -> impl IntoResponse {
    Json(HealthBody {
        status: "ok",
        service: "db-worker",
        worker_id: state.cfg.worker_id.clone(),
        version: state.cfg.version.clone(),
    })
}

/// `GET /readyz`：是否可承接数据面流量。
async fn readyz(State(state): State<Arc<OpsState>>) -> Response {
    let ready = state.is_ready();
    let body = ReadyBody {
        status: if ready { "ready" } else { "draining" },
        worker_id: state.cfg.worker_id.clone(),
        worker_state: state.state.current().to_string(),
        databases: state.registry.len(),
        serving: state.registry.serving_count(),
    };
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(body)).into_response()
}

/// `GET /metrics`：Prometheus 文本。
///
/// 未安装 recorder 时返回 503 而不是空 200 —— 空 200 会让抓取端把「指标缺失」
/// 误读成「一切为 0」。
async fn metrics(State(state): State<Arc<OpsState>>) -> Response {
    match state.metrics.as_ref() {
        Some(handle) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            handle.render(),
        )
            .into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "metrics recorder 未安装\n").into_response(),
    }
}

/// 组装 Ops 路由。
pub fn router(state: Arc<OpsState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// 安装全局 Prometheus recorder，返回渲染句柄。
///
/// 为什么由 Ops 模块自己安装而不是交给 `observability::init`：`observability` 的
/// recorder 自带 http-listener（会另起一个端口），而 Worker 要求 `/metrics` 与
/// `/healthz` 复用同一个 `OPS_LISTEN`。这里用同一套桶配置（延迟类指标按**微秒**分桶）
/// 只取 recorder，不开它自己的监听器。
///
/// 返回 `None` 表示 recorder 已被占用（例如进程里已有其它 recorder）或配置失败，
/// 此时 `/metrics` 返回 503，不影响其它端点与数据面。
pub fn install_prometheus() -> Option<PrometheusHandle> {
    let builder = match PrometheusBuilder::new().set_buckets_for_metric(
        metrics_exporter_prometheus::Matcher::Suffix("_micros".to_string()),
        observability::metrics::LATENCY_BUCKETS_MICROS,
    ) {
        Ok(builder) => builder,
        Err(err) => {
            tracing::warn!(error = %err, "Prometheus 分桶配置失败，/metrics 不可用");
            return None;
        }
    };
    match builder.install_recorder() {
        Ok(handle) => Some(handle),
        Err(err) => {
            tracing::warn!(error = %err, "/metrics 不可用（recorder 安装失败）");
            None
        }
    }
}

/// 在 `OPS_LISTEN` 上启动 Ops HTTP 服务，直到 `shutdown` 置位。
pub async fn serve(
    listener: tokio::net::TcpListener,
    state: Arc<OpsState>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    tracing::info!(addr = %addr, "Ops HTTP 服务启动（/healthz /readyz /metrics）");

    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            // 通道关闭（进程退出）与显式置位都视为关停
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    break;
                }
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::lifecycle::WorkerState;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn ops_state(dir: &std::path::Path) -> (Arc<OpsState>, Arc<WorkerStateMachine>) {
        let cli = <crate::cli::Cli as clap::Parser>::try_parse_from([
            "db-worker",
            "--worker-id",
            "worker-ops",
            "--data-dir",
            dir.join("data").to_str().unwrap(),
            "--run-dir",
            dir.join("run").to_str().unwrap(),
            "--cgroup-disabled",
        ])
        .unwrap();
        let cfg = Arc::new(WorkerConfig::from_cli(cli).unwrap());
        let sm = Arc::new(WorkerStateMachine::new(cfg.worker_id.clone()));
        let registry = Arc::new(crate::registry::LocalDbRegistry::new());
        (
            Arc::new(OpsState {
                cfg,
                state: Arc::clone(&sm),
                registry,
                metrics: None,
            }),
            sm,
        )
    }

    #[test]
    fn readiness_follows_worker_state() {
        let dir = tempfile::tempdir().unwrap();
        let (ops, sm) = ops_state(dir.path());
        assert!(ops.is_ready());

        sm.begin_drain().unwrap();
        // DRAINING：不再承接新流量，但进程仍然健康（liveness 不受影响）
        assert!(!ops.is_ready());
        assert_eq!(sm.current(), WorkerState::Draining);
    }

    #[tokio::test]
    async fn ops_endpoints_respond_over_http() {
        let dir = tempfile::tempdir().unwrap();
        let (ops, sm) = ops_state(dir.path());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = watch::channel(false);
        let server = tokio::spawn(serve(listener, ops, rx));

        async fn request(addr: std::net::SocketAddr, path: &str) -> String {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let req =
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            stream.write_all(req.as_bytes()).await.unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf).to_string()
        }

        let health = request(addr, "/healthz").await;
        assert!(health.contains("200 OK"), "healthz 响应异常：{health}");
        assert!(health.contains("\"status\":\"ok\""));

        let ready = request(addr, "/readyz").await;
        assert!(ready.contains("200 OK"), "readyz 响应异常：{ready}");
        assert!(ready.contains("\"status\":\"ready\""));

        let metrics = request(addr, "/metrics").await;
        assert!(
            metrics.contains("503 Service Unavailable"),
            "未安装 recorder 时 /metrics 必须 503：{metrics}"
        );

        // 进入 DRAINING 后 readiness 必须变成 503
        sm.begin_drain().unwrap();
        let ready = request(addr, "/readyz").await;
        assert!(
            ready.contains("503 Service Unavailable"),
            "DRAINING 时 readyz 必须 503：{ready}"
        );

        server.abort();
    }
}
