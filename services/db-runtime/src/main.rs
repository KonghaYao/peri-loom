//! db-runtime —— DB Process 入口（架构 §1.2 / §5.3 / §13）。
//!
//! 一个数据库 = 一个进程：本进程是 TursoDB 引擎的宿主，对外**只**通过 Unix Domain
//! Socket 与 Worker Data Dispatcher 通信，不监听任何 TCP 端口。
//!
//! 启动顺序（每一步失败都必须终止启动，绝不带病服务）：
//!
//! ```text
//! 解析配置 -> observability 初始化 -> 恢复（快照 + Remote WAL）-> durable IO
//!          -> open 引擎 -> 监听 UDS -> 握手（fencing）-> 服务循环
//! ```
//!
//! 退出码：`0` 优雅停机（SIGTERM / Shutdown 帧）；`1` 启动失败或异常终止；
//! `2` fencing（所有权已被取代，架构 §11.3）；`3` durable IO fail-stop
//! （本地 WAL 字节流不可信，架构 §11.1 / §12.1：退出后由 Worker 重启一个干净进程，
//! 见 [`fatal`] 与 [`host::Host::durable_fail_stop`]）。

mod cancel;
mod config;
mod dispatch;
mod fatal;
mod fencing;
mod frame;
mod host;
mod server;
mod session;

#[cfg(test)]
mod e2e_tests;

use std::process::ExitCode;

use config::RuntimeConfig;
use host::Host;
use observability::TelemetryConfig;

fn main() -> ExitCode {
    // 多线程运行时是硬要求：`spawn_blocking` 池要能承载引擎调用，
    // 而 `PlatformDurableIO` 的远程 append 也从这里拿执行器（架构 §17.3）。
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("db-runtime: 创建 tokio 运行时失败：{err}");
            return ExitCode::from(host::EXIT_FAILURE as u8);
        }
    };
    runtime.block_on(async_main())
}

async fn async_main() -> ExitCode {
    let config = RuntimeConfig::from_args().unwrap_or_else(|err| {
        // clap 的用法错误会自己打印，这里只处理取值校验失败。
        eprintln!("db-runtime: 配置错误：{err}");
        std::process::exit(host::EXIT_FAILURE);
    });

    // 日志必须先就绪：后面每一步（恢复、打开引擎、握手）都需要可观测。
    let _telemetry = match init_telemetry(&config) {
        Ok(guard) => guard,
        Err(err) => {
            eprintln!("db-runtime: 初始化 observability 失败：{err}");
            return ExitCode::from(host::EXIT_FAILURE as u8);
        }
    };

    tracing::info!(
        db_id = %config.database_id,
        worker_id = %config.worker_id,
        owner_epoch = config.owner_epoch,
        pid = std::process::id(),
        socket = %config.socket_path.display(),
        cpu_milli = config.cpu_milli,
        memory_mib = config.memory_mib,
        disk_mib = config.disk_mib,
        process_slots = config.process_slots,
        "DB Process 启动"
    );

    let host = match Host::open(config, None).await {
        Ok(host) => host,
        Err(err) => {
            tracing::error!(error = %err, "打开数据库失败，进程退出");
            return ExitCode::from(host::EXIT_FAILURE as u8);
        }
    };

    match server::run(host).await {
        Ok(code) => ExitCode::from(code as u8),
        Err(err) => {
            tracing::error!(error = %err, "服务循环异常退出");
            ExitCode::from(host::EXIT_FAILURE as u8)
        }
    }
}

/// 初始化日志 / 指标。**不**启动任何 HTTP 监听：本进程不允许有 TCP 端口。
fn init_telemetry(config: &RuntimeConfig) -> Result<observability::TelemetryGuard, String> {
    let mut telemetry = TelemetryConfig::new("db-runtime", env!("CARGO_PKG_VERSION"));
    telemetry.log_level = config.log_level.clone();
    telemetry.instance_id = config.log_identity();
    observability::init(telemetry).map_err(|err| err.to_string())
}
