//! wal-service —— Remote WAL Service（架构 §17.8）。
//!
//! ```text
//! DB Runtime / Worker ──gRPC(RemoteWal)──> 本服务 ──Raft(3 副本)──> raft-engine
//! ```
//!
//! 本进程只做三件事：
//! 1. 承载**一个** WAL Shard（= 一个 Raft Group，承载大量 DB，不是一 DB 一 Group）；
//! 2. 成功 Append = quorum durable；旧 owner_epoch 的 Append 在服务内被拒（Storage-level Fencing）；
//! 3. 对外暴露 Ops HTTP（/healthz、/readyz、/metrics）。
//!
//! 装配逻辑在 [`runtime`]（集成测试复用同一份装配），本文件只负责
//! 「解析配置 -> 安装观测 -> 启动 -> 等信号 -> 优雅停机」。

mod command;
mod config;
#[cfg(test)]
mod e2e_tests;
mod error;
mod logging;
mod ops;
mod peer;
mod raft_group;
mod runtime;
mod service;
mod state_machine;
mod storage;
mod time;

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use observability::TelemetryConfig;
use tracing::{error, info, warn};

use crate::config::WalConfig;

/// 服务名（日志 / trace 的 service 标识，冻结）。
const SERVICE_NAME: &str = "wal-service";

#[tokio::main]
async fn main() -> ExitCode {
    // 配置解析必须最先做：CLI/环境变量错误要以 usage 形式直接返回
    let config = Arc::new(WalConfig::parse());
    // guard 必须在进程存活期间一直持有：drop 会关闭 trace 导出与日志提交
    let _telemetry_guard = install_telemetry();

    if let Err(err) = config.validate() {
        error!(error = %err, "配置非法");
        return ExitCode::FAILURE;
    }

    info!(
        node_id = config.node_id,
        shard = %config.shard_id,
        listen = %config.listen,
        peer_listen = %config.peer_listen,
        ops_listen = %config.ops_listen,
        data_dir = %config.data_dir.display(),
        cluster = %config.cluster,
        append_timeout_ms = config.append_timeout_ms,
        version = env!("CARGO_PKG_VERSION"),
        "wal-service 启动中"
    );

    let node = match runtime::start(config.clone()).await {
        Ok(node) => node,
        Err(err) => {
            // 启动失败必须带错误码退出：容器编排据此做 CrashLoop 告警而不是静默重启
            error!(error = %err, "节点启动失败");
            return ExitCode::FAILURE;
        }
    };

    info!(
        grpc_addr = %node.grpc_addr,
        peer_addr = %node.peer_addr,
        ops_addr = %node.ops_addr,
        "wal-service 已启动"
    );

    wait_for_shutdown_signal().await;
    info!("收到停机信号，开始优雅停机");
    node.shutdown().await;
    info!("wal-service 已停止");
    ExitCode::SUCCESS
}

/// 安装平台统一 telemetry（日志 / 指标 / trace）。
///
/// 关于 `METRICS_LISTEN_ADDR`：本服务**忽略**它。Ops 端口（OPS_LISTEN）已经承载
/// `/metrics`，若再由 telemetry 另起一个 Prometheus listener，就会出现两个采集端点
/// 与两个全局 recorder（后者必然失败）。因此这里显式清空该字段，指标统一走 OPS_LISTEN。
fn install_telemetry() -> Option<observability::TelemetryGuard> {
    let mut telemetry = TelemetryConfig::from_env(SERVICE_NAME, env!("CARGO_PKG_VERSION"));
    if let Some(addr) = telemetry.metrics_listen_addr {
        warn!(
            addr = %addr,
            "METRICS_LISTEN_ADDR 已设置但被忽略：wal-service 的指标统一由 OPS_LISTEN 的 /metrics 暴露"
        );
        telemetry.metrics_listen_addr = None;
    }
    // 重复安装（例如测试进程已装）不是致命错误：服务本身与观测解耦
    match observability::init(telemetry) {
        Ok(guard) => Some(guard),
        Err(err) => {
            eprintln!("telemetry 初始化失败（不影响 WAL 服务）：{err}");
            None
        }
    }
}

/// 等待停机信号（SIGINT / SIGTERM）。
///
/// 两个信号都要接：SIGINT 对应本地 Ctrl-C，SIGTERM 对应容器编排（docker stop /
/// k8s 滚动更新）。不接 SIGTERM 会让每次发布都变成一次「强杀 + 恢复重放」。
async fn wait_for_shutdown_signal() {
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
