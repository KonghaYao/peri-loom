//! Telemetry 配置：服务标识、日志格式、metrics 监听地址、OTLP endpoint。

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::TelemetryError;

/// 环境变量名（容器编排按这些 key 注入配置）。
pub mod env_keys {
    /// 日志格式：`json` | `pretty`。
    pub const LOG_FORMAT: &str = "LOG_FORMAT";
    /// 日志过滤指令，语义同 `RUST_LOG`（如 `info,db_server=debug`）。
    pub const LOG_LEVEL: &str = "LOG_LEVEL";
    /// 标准 tracing 过滤变量，优先级高于 [`LOG_LEVEL`]。
    pub const RUST_LOG: &str = "RUST_LOG";
    /// Prometheus 抓取监听地址，如 `0.0.0.0:9100`。
    pub const METRICS_LISTEN_ADDR: &str = "METRICS_LISTEN_ADDR";
    /// OTLP collector endpoint，如 `http://otel-collector:4317`。
    pub const OTLP_ENDPOINT: &str = "OTLP_ENDPOINT";
    /// 慢查询阈值（毫秒）。
    pub const SLOW_QUERY_THRESHOLD_MS: &str = "SLOW_QUERY_THRESHOLD_MS";
    /// 实例标识，优先于 [`POD_NAME`] / [`HOSTNAME`]。
    pub const INSTANCE_ID: &str = "INSTANCE_ID";
    /// K8s pod 名，作为 instance_id 兜底之一。
    pub const POD_NAME: &str = "POD_NAME";
    /// 主机名，作为 instance_id 最后兜底。
    pub const HOSTNAME: &str = "HOSTNAME";
}

/// 日志输出格式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// 结构化 JSON（生产默认，直供 Loki 等日志后端；本地可用 `LOG_FORMAT=pretty` 覆盖）。
    #[default]
    Json,
    /// 人类可读多行文本（仅本地开发）。
    Pretty,
}

impl LogFormat {
    /// 配置取值。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Pretty => "pretty",
        }
    }
}

impl FromStr for LogFormat {
    type Err = TelemetryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            // text 作为 pretty 的别名，兼容常见写法
            "pretty" | "text" => Ok(Self::Pretty),
            other => Err(TelemetryError::InvalidLogFormat(other.to_owned())),
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// telemetry 配置。
///
/// `metrics_listen_addr` 与 `otlp_endpoint` 为可选：未配置时分别表示不暴露
/// Prometheus 抓取端点、不导出 trace（不阻塞启动）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// 服务名，作为日志/trace 的 service 标识。
    pub service_name: String,
    /// 服务版本。
    pub service_version: String,
    /// 实例标识，同一服务多副本时用于区分。
    pub instance_id: String,
    /// 日志格式。
    pub log_format: LogFormat,
    /// 日志过滤指令（EnvFilter 语法）。
    pub log_level: String,
    /// Prometheus 抓取监听地址；
    /// 该端点只允许内网/集群内暴露，禁止经公网暴露（架构 §17.4）。
    pub metrics_listen_addr: Option<SocketAddr>,
    /// OTLP collector endpoint，仅 `otlp` feature 打开时生效。
    pub otlp_endpoint: Option<String>,
    /// 慢查询判定阈值（毫秒），默认 1000。
    pub slow_query_threshold_ms: u64,
}

impl TelemetryConfig {
    /// 慢查询默认阈值（毫秒）。
    pub const DEFAULT_SLOW_QUERY_THRESHOLD_MS: u64 = 1000;

    /// 用默认值构造配置（不读环境变量）。
    pub fn new(service_name: impl Into<String>, service_version: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
            instance_id: "unknown".to_owned(),
            log_format: LogFormat::default(),
            log_level: crate::DEFAULT_LOG_LEVEL.to_owned(),
            metrics_listen_addr: None,
            otlp_endpoint: None,
            slow_query_threshold_ms: Self::DEFAULT_SLOW_QUERY_THRESHOLD_MS,
        }
    }

    /// 从环境变量构造配置。
    ///
    /// 该函数不失败：取值非法的环境变量按“未设置”处理并回落到默认值
    /// （例如 `METRICS_LISTEN_ADDR` 解析失败等价于不暴露 /metrics），因此运维
    /// 需以 `init` 打印的生效配置为准。
    pub fn from_env(service_name: impl Into<String>, service_version: impl Into<String>) -> Self {
        let mut config = Self::new(service_name, service_version);

        if let Some(value) = env_value(env_keys::INSTANCE_ID) {
            config.instance_id = value;
        } else if let Some(value) =
            env_value(env_keys::POD_NAME).or_else(|| env_value(env_keys::HOSTNAME))
        {
            config.instance_id = value;
        } else {
            config.instance_id = format!("pid-{}", std::process::id());
        }

        if let Some(value) = env_value(env_keys::LOG_FORMAT).and_then(|v| v.parse().ok()) {
            config.log_format = value;
        }

        if let Some(value) =
            env_value(env_keys::RUST_LOG).or_else(|| env_value(env_keys::LOG_LEVEL))
        {
            config.log_level = value;
        }

        config.metrics_listen_addr =
            env_value(env_keys::METRICS_LISTEN_ADDR).and_then(|v| v.parse::<SocketAddr>().ok());

        config.otlp_endpoint = env_value(env_keys::OTLP_ENDPOINT);

        if let Some(value) =
            env_value(env_keys::SLOW_QUERY_THRESHOLD_MS).and_then(|v| v.parse::<u64>().ok())
        {
            config.slow_query_threshold_ms = value;
        }

        config
    }

    /// 慢查询阈值。
    pub const fn slow_query_threshold(&self) -> Duration {
        Duration::from_millis(self.slow_query_threshold_ms)
    }

    /// 是否暴露 Prometheus 抓取端点。
    pub const fn metrics_enabled(&self) -> bool {
        self.metrics_listen_addr.is_some()
    }

    /// 是否导出 trace：需要 `otlp` feature 与 endpoint 同时具备。
    pub fn otlp_enabled(&self) -> bool {
        cfg!(feature = "otlp") && self.otlp_endpoint.is_some()
    }
}

fn env_value(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_stable() {
        let config = TelemetryConfig::new("db-server", "1.2.3");
        assert_eq!(config.service_name, "db-server");
        assert_eq!(config.service_version, "1.2.3");
        assert_eq!(config.log_format, LogFormat::Json);
        assert_eq!(config.log_level, crate::DEFAULT_LOG_LEVEL);
        assert_eq!(config.slow_query_threshold_ms, 1000);
        assert_eq!(config.slow_query_threshold(), Duration::from_millis(1000));
        assert!(config.metrics_listen_addr.is_none());
        assert!(config.otlp_endpoint.is_none());
        assert!(!config.metrics_enabled());
        // 默认构建未开启 otlp feature
        assert!(!config.otlp_enabled() || cfg!(feature = "otlp"));
    }

    #[test]
    fn log_format_parsing_accepts_aliases() {
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!(" pretty ".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Pretty);

        let err = "xml".parse::<LogFormat>().unwrap_err();
        assert!(matches!(err, TelemetryError::InvalidLogFormat(_)));
        assert_eq!(LogFormat::Json.as_str(), "json");
        assert_eq!(LogFormat::Pretty.to_string(), "pretty");
    }

    #[test]
    fn from_env_falls_back_to_defaults_without_env() {
        // 这些变量若被外部设置过，from_env 仍应产出可解析的配置
        let config = TelemetryConfig::from_env("svc", "0.1.0");
        assert_eq!(config.service_name, "svc");
        assert!(!config.instance_id.is_empty());
        assert!(!config.log_level.is_empty());
    }
}
