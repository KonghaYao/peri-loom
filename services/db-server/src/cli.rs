//! db-server —— Control Plane（架构 §17.4）。
//!
//! 本文件只负责「CLI -> 配置 -> 交给装配层」：真正的启动顺序在 [`db_server::app::run`]，
//! 集成测试可以复用同一份装配，而不必再抄一遍 main。
//!
//! ```text
//! db-server                 # 启动服务
//! db-server dump-openapi    # 导出 OpenAPI 契约（JSON 到 stdout）后退出
//! ```

use crate as db_server;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing::error;

/// 命令行参数。
#[derive(Debug, Parser)]
#[command(
    name = "db-server",
    version,
    about = "TursoDB DB Platform Control Plane"
)]
struct Cli {
    /// 等价于 `db-server dump-openapi`：两种写法都支持，脚本与文档不一致时也不会只报 usage 错误。
    #[arg(long)]
    dump_openapi: bool,
    /// 子命令（缺省即启动服务）。
    #[command(subcommand)]
    command: Option<Command>,
}

/// 子命令。
#[derive(Debug, Subcommand)]
enum Command {
    /// 停机导出完整实例。
    Export {
        #[arg(long)]
        data_dir: std::path::PathBuf,
        #[arg(long)]
        output: std::path::PathBuf,
    },
    /// 校验导出并恢复到新目录。
    Import {
        #[arg(long)]
        input: std::path::PathBuf,
        #[arg(long)]
        data_dir: std::path::PathBuf,
    },
    /// 显式部署模式；Simple 不读取集群环境配置。
    Serve {
        #[arg(long, value_enum, default_value = "distributed")]
        mode: Mode,
        #[command(flatten)]
        simple: db_server::simple::config::SimpleConfig,
    },
    /// 把 `/api/v1/openapi.json` 的契约打到 stdout 后退出。
    DumpOpenapi,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Mode {
    Simple,
    Distributed,
}

pub async fn run_cli() -> ExitCode {
    let cli = Cli::parse();
    if cli.dump_openapi || matches!(cli.command, Some(Command::DumpOpenapi)) {
        return dump_openapi();
    }

    match &cli.command {
        Some(Command::Export { data_dir, output }) => {
            return export_result(
                db_server::simple::export::export_instance(data_dir, output).await,
            )
        }
        Some(Command::Import { input, data_dir }) => {
            return export_result(db_server::simple::export::import_instance(input, data_dir).await)
        }
        _ => {}
    }
    if let Some(Command::Serve {
        mode: Mode::Simple,
        simple,
    }) = cli.command
    {
        return match db_server::simple::run(simple).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Simple 启动/运行失败: {error:#}");
                ExitCode::FAILURE
            }
        };
    }

    let config = match db_server::config::ServerConfig::from_env() {
        Ok(config) => config,
        Err(err) => {
            // 这一步发生在 telemetry 之前，因此只能写 stderr（stdout 要留给契约导出）
            eprintln!("db-server 配置非法: {err}");
            return ExitCode::FAILURE;
        }
    };

    match db_server::app::run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // 用 `{:#}` 打出 anyhow 的错误链：启动失败的根因通常在最后一层
            error!("db-server 启动失败: {:#}", err);
            ExitCode::FAILURE
        }
    }
}

/// 导出 OpenAPI 契约：stdout **只允许**有 JSON，任何诊断信息都走 stderr，
/// 否则 `db-server dump-openapi > openapi.json` 会产出无法解析的文件。
fn dump_openapi() -> ExitCode {
    match serde_json::to_string_pretty(&db_server::api::api_doc()) {
        Ok(json) => {
            println!("{json}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("OpenAPI 契约序列化失败: {err}");
            ExitCode::FAILURE
        }
    }
}

fn export_result(result: anyhow::Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("导出/恢复失败: {error:#}");
            ExitCode::FAILURE
        }
    }
}
