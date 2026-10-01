//! db-worker —— Worker Plane（架构 §5 / §9 / §17.6）。
//!
//! 职责：
//! - Worker Agent（控制面）：生命周期指令、心跳与资源上报、排空（ACTIVE -> DRAINING -> EMPTY）。
//! - Data Dispatcher（数据面）：`db_id -> 本地 DB Process` 的稳定入口，gRPC/HTTP2 streaming。
//! - Process Supervisor：DB Process 是**普通子进程**，由本进程 spawn 并放入独立 cgroup。
//! - Local DB Registry：`db_id -> state/pid/socket/owner_epoch` + epoch fencing。
//!
//! 硬性边界（架构 §17.6）：**不链接 turso_core / engine-adapter**，DB Process 由
//! `DB_RUNTIME_BIN`（默认 `/usr/local/bin/db-runtime`）作为普通子进程拉起。
//!
//! 本文件只做两件事：声明模块树、解析配置后交给 [`app::run`]。真正的装配顺序
//! （共享状态 -> 预绑定监听 -> 后台任务 -> 优雅停机）在 `app.rs`，便于审阅与复用。

use std::process::ExitCode;

use tracing::error;

mod app;
mod cgroup;
mod cli;
mod control;
mod dispatch;
mod error;
mod heartbeat;
mod metrics;
mod ops;
mod paths;
mod pidfd;
mod registry;
mod resources;
mod restore;
mod supervisor;
mod uds;
mod worker_state;

#[tokio::main]
async fn main() -> ExitCode {
    // 配置解析发生在 telemetry 之前，因此这里只能写 stderr：日志订阅者还没装好，
    // 用 tracing 打出来的启动错误会丢失。
    let cli = cli::parse_cli();
    let config = match cli::WorkerConfig::from_cli(cli) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("db-worker 配置非法: {err}");
            return ExitCode::FAILURE;
        }
    };

    match app::run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // `{:#}` 打出错误链：启动失败的根因通常在最后一层
            error!("db-worker 启动失败: {:#}", err);
            ExitCode::FAILURE
        }
    }
}
