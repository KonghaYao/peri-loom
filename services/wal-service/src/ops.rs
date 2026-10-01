//! Ops HTTP server：`/healthz`、`/readyz`、`/metrics`（架构 §14 / §17.10）。
//!
//! # 端点语义
//!
//! - `/healthz`（liveness）：进程活着就返回 200。**不**看 Raft 状态 —— 选举中的节点
//!   并不需要被重启，把 liveness 与业务状态绑定会让容器编排在集群抖动时反复杀进程，
//!   反而放大故障。
//! - `/readyz`（readiness）：只有「集群已选出 leader」才 200。没有 leader 时
//!   Append / SetOwnerEpoch 必然返回 `WAL_NOT_LEADER`，此时把节点摘出流量是对的。
//!   apply 落后**不**影响 readiness：它只影响 `ReadRange` 的可见区间，而该语义已经由
//!   `WAL_NOT_DURABLE`（可重试）表达；把毫秒级的 apply 追平抖动当成「不ready」会让
//!   负载均衡不停摘挂实例。
//! - `/metrics`：Prometheus 文本。**该端点只允许集群内网抓取，禁止经公网暴露**
//!   （架构 §17.4：指标端口与公网 API 必须隔离）。部署上 OPS_LISTEN 只绑内网地址，
//!   公网入口（db-server / Ingress）不得转发 9300。
//!
//! # 指标值的刷新方式
//!
//! Raft 运行态（term / commit / applied / 是否 leader）在**抓取时**读取并写入 gauge，
//! 而不是后台定时采样：拉模型下抓取频率由 Prometheus 决定，按需读取既没有额外常驻
//! 任务，也不会出现「采样频率与抓取频率不一致」导致的锯齿。

use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use metrics_exporter_prometheus::{PrometheusHandle, PrometheusRecorder};
use observability::metrics::metric_names;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::WalConfig;
use crate::error::{WalError, WalResult};
use crate::raft_group::WalHandle;
use crate::storage::WalStorage;

/// Prometheus upkeep 周期。
///
/// `build_recorder()`（自建 axum 端点而不是用 exporter 自带 listener）不会自动跑 upkeep，
/// 需要显式调用：它负责清理长期无写入的 idle 序列，避免时间序列无限增长。
const UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Ops server 共享状态。
#[derive(Clone)]
pub struct OpsState {
    handle: WalHandle,
    storage: Arc<WalStorage>,
    config: Arc<WalConfig>,
    prometheus: PrometheusHandle,
    started_at: Instant,
}

impl OpsState {
    /// 构造共享状态。
    pub fn new(
        handle: WalHandle,
        storage: Arc<WalStorage>,
        config: Arc<WalConfig>,
        prometheus: PrometheusHandle,
    ) -> Self {
        Self {
            handle,
            storage,
            config,
            prometheus,
            started_at: Instant::now(),
        }
    }
}

/// 安装全局 Prometheus recorder 并返回文本渲染句柄。
///
/// 安装失败（全局 recorder 已被占用）**不是**致命错误：说明进程里已经有别的观测
/// 设施接管了指标，此时 /metrics 仍应能用（退化为空输出）而不是让服务起不来。
/// 因此这里返回 `Option`，由调用方决定日志级别。
pub fn install_metrics_recorder() -> WalResult<PrometheusHandle> {
    let recorder: PrometheusRecorder =
        metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    match metrics::set_global_recorder(recorder) {
        Ok(()) => {
            preinitialize_fencing_counter();
            Ok(handle)
        }
        Err(err) => Err(WalError::Internal(format!(
            "Prometheus recorder 安装失败（已被占用）：{err}"
        ))),
    }
}

/// 启动时以 0 值预注册 fencing 拒绝计数器的时间序列。
///
/// 为什么必须预注册：Prometheus 只在时间序列**首次被写入**后才导出它。若计数器只在
/// 「真的发生了 fencing 拒绝」时才出现，那么「集群健康、从未拒绝过」与「指标根本没接线」
/// 在 /metrics 上完全一样 —— 验收脚本与告警都无法区分这两者（这正是 §16 durability
/// 检查此前只能 SKIP 的原因）。预注册后，值为 0 明确表示「没有发生过拒绝」。
///
/// 只用类型化入口已知的两个 label 组合：真实拒绝发生时新增的序列也会照常导出。
fn preinitialize_fencing_counter() {
    metrics::counter!(
        metric_names::WAL_FENCED_REJECTED_TOTAL,
        "kind" => "append",
        "reason" => "stale_epoch",
    )
    .increment(0);
    metrics::counter!(
        metric_names::WAL_FENCED_REJECTED_TOTAL,
        "kind" => "set_owner_epoch",
        "reason" => "epoch_not_monotonic",
    )
    .increment(0);
}

/// 构造 Ops 路由。
pub fn router(state: OpsState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// liveness：进程存活即 200（见模块注释的语义说明）。
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, axum::Json(json!({ "status": "ok" })))
}

/// readiness：有 leader 才 200。
async fn readyz(State(state): State<OpsState>) -> impl IntoResponse {
    let health = state.handle.health();
    let ready = health.leader_id != 0;
    let body = json!({
        "ready": ready,
        "node_id": state.config.node_id,
        "shard_id": state.config.shard_id,
        "is_leader": health.is_leader,
        "leader_id": health.leader_id,
        "term": health.term,
        "commit_index": health.commit_index,
        "applied_index": health.applied_index,
        "uptime_seconds": state.started_at.elapsed().as_secs(),
    });
    let status = if ready {
        StatusCode::OK
    } else {
        // 503：调用方（LB）据此把本节点摘出流量，直到选出 leader
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, axum::Json(body))
}

/// Prometheus 文本导出。
async fn metrics(State(state): State<OpsState>) -> impl IntoResponse {
    refresh_runtime_gauges(&state);
    let body = state.prometheus.render();
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

/// 抓取时刷新 Raft / 存储运行态 gauge。
fn refresh_runtime_gauges(state: &OpsState) {
    let health = state.handle.health();
    metrics::gauge!("wal_raft_is_leader").set(if health.is_leader { 1.0 } else { 0.0 });
    metrics::gauge!("wal_raft_term").set(health.term as f64);
    metrics::gauge!("wal_raft_leader_id").set(health.leader_id as f64);
    metrics::gauge!("wal_raft_commit_index").set(health.commit_index as f64);
    metrics::gauge!("wal_raft_applied_index").set(health.applied_index as f64);
    metrics::gauge!("wal_engine_used_bytes").set(state.storage.used_bytes() as f64);
    metrics::gauge!("wal_raft_members").set(state.storage.conf_state().voters.len() as f64);
}

/// 启动 Ops server。
///
/// 传入已完成绑定的 listener（绑定失败必须在启动阶段暴露，见 `service::bind_listener`）。
pub async fn serve(listener: TcpListener, state: OpsState, shutdown: CancellationToken) {
    let local_addr = listener
        .local_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_owned());

    let upkeep_handle = state.prometheus.clone();
    let upkeep_shutdown = shutdown.clone();
    let upkeep = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(UPKEEP_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = upkeep_shutdown.cancelled() => return,
                _ = ticker.tick() => upkeep_handle.run_upkeep(),
            }
        }
    });

    info!(
        local_addr = %local_addr,
        "Ops HTTP 已启动（/healthz、/readyz、/metrics；仅限内网抓取）"
    );
    let router = router(state);
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await;
    match result {
        Ok(()) => debug!("Ops HTTP 已停止"),
        Err(err) => warn!(error = %err, "Ops HTTP 异常退出"),
    }
    let _ = upkeep.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::group_id_from_shard;

    /// 构造一个只依赖本地 raft-engine 与内存状态机的 OpsState（不触网、不启 Raft）。
    fn state_for_test() -> OpsState {
        let config = Arc::new(WalConfig::default());
        // raft-engine 会持有目录内文件的 fd；测试里让目录活到进程结束（keep），
        // 避免「目录已删、引擎仍持有句柄」的半删除状态干扰断言。
        let dir = tempfile::Builder::new()
            .prefix("wal-ops-test")
            .tempdir()
            .expect("创建临时目录");
        let group_id = group_id_from_shard(&config.shard_id);
        let storage = Arc::new(
            WalStorage::open(&dir.keep(), group_id, &[config.node_id]).expect("打开 raft-engine"),
        );
        let handle = crate::raft_group::tests_support::offline_handle(config.clone());
        let prometheus = metrics_exporter_prometheus::PrometheusBuilder::new()
            .build_recorder()
            .handle();
        OpsState::new(handle, storage, config, prometheus)
    }

    #[tokio::test]
    async fn healthz_is_ok_without_leader() {
        // liveness 不依赖 Raft 状态：选举中也要 200，否则容器会被反复重启
        let response = healthz().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_reports_not_ready_without_leader() {
        let state = state_for_test();
        let response = readyz(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_endpoint_renders_prometheus_text() {
        let state = state_for_test();
        let response = metrics(State(state)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(
            content_type.starts_with("text/plain"),
            "必须是 Prometheus 文本格式"
        );
    }

    #[test]
    fn recorder_install_is_fallible_not_panicking() {
        // 全局 recorder 是进程级单例：重复安装必须返回 Err，而不是 panic。
        // 第一个可能失败（其它测试已装），第二个必然失败，两者都不能 panic。
        let _ = install_metrics_recorder();
        assert!(install_metrics_recorder().is_err());
    }

    /// 未发生任何 fencing 拒绝时，`/metrics` 也必须能看到该计数器（值为 0）。
    ///
    /// 「没拒绝过」与「指标没接线」必须在抓取结果上可区分，否则验收脚本与告警
    /// 会把前者误判成后者。
    #[test]
    fn fencing_counter_is_exported_before_any_rejection() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, preinitialize_fencing_counter);

        let rendered = handle.render();
        assert!(
            rendered.contains("wal_fenced_rejected_total{"),
            "缺少 fencing 拒绝计数器: {rendered}"
        );
        assert!(
            rendered.contains(r#"kind="append",reason="stale_epoch""#),
            "缺少 append 维度: {rendered}"
        );
        assert!(
            rendered.contains(r#"kind="set_owner_epoch",reason="epoch_not_monotonic""#),
            "缺少 set_owner_epoch 维度: {rendered}"
        );
        assert!(
            !rendered
                .contains(r#"wal_fenced_rejected_total{kind="append",reason="stale_epoch"} 1"#),
            "预注册必须写 0，不能凭空计一次拒绝: {rendered}"
        );
    }
}
