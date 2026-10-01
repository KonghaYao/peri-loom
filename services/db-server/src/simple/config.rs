use clap::Args;
use std::{net::SocketAddr, path::PathBuf};

/// 本地配置独立解析，不读取 DATABASE_URL、WAL_CLUSTER 或 S3 secret。
#[derive(Debug, Clone, Args)]
pub struct SimpleConfig {
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,
    #[arg(long, default_value_t = 64)]
    pub max_open_databases: usize,
    #[arg(long, default_value_t = 128)]
    pub max_sessions_per_database: usize,
    #[arg(long, default_value_t = 64)]
    pub queue_capacity: usize,
    #[arg(long, default_value_t = 262144)]
    pub max_result_frame_bytes: usize,
    #[arg(long, default_value_t = 262144)]
    pub inline_result_limit_bytes: usize,
    #[arg(long, default_value = "info")]
    pub log_level: String,
    #[arg(long, default_value = "admin")]
    pub admin_user: String,
    #[arg(long)]
    pub admin_password_file: Option<PathBuf>,
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    #[arg(long, requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,
}
