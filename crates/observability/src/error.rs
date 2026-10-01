//! 观测层错误类型。
//!
//! 观测安装失败是进程内部故障，因此统一映射到 `domain` 的 [`ErrorCode`]
//! （内部错误；配置取值非法归为参数非法），细节保留在 Display / source 链中。

use domain::ErrorCode;
use thiserror::Error;

/// telemetry 初始化错误。
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// 本进程已安装过 telemetry（重复 init），或全局 recorder 已被占用。
    #[error("telemetry 已初始化：全局 tracing subscriber 或 metrics recorder 已被占用")]
    AlreadyInitialized,

    /// 全局 tracing subscriber 安装失败，通常表示调用方已自行安装过 subscriber。
    #[error("tracing 全局 subscriber 安装失败: {0}")]
    TracingInstall(#[source] tracing::subscriber::SetGlobalDefaultError),

    /// Prometheus exporter 安装失败（端口被占用、recorder 冲突等）。
    #[error("metrics exporter 安装失败: {0}")]
    MetricsInstall(#[source] metrics_exporter_prometheus::BuildError),

    /// `LOG_FORMAT` 取值非法。
    #[error("log_format 非法: {0}（可选 json / pretty）")]
    InvalidLogFormat(String),

    /// OTLP exporter 构建失败（endpoint 非法、TLS 配置缺失等）。
    #[cfg(feature = "otlp")]
    #[error("OTLP exporter 初始化失败: {0}")]
    OtlpInit(#[source] opentelemetry_otlp::ExporterBuildError),

    /// OTLP exporter 的 gRPC channel 绑定 Tokio reactor，必须在 runtime 内构建。
    #[cfg(feature = "otlp")]
    #[error("OTLP 导出需要 Tokio runtime 上下文（请在 #[tokio::main] 等 runtime 内调用 init）")]
    OtlpRequiresRuntime,
}

impl TelemetryError {
    /// 映射到平台统一错误码。
    pub const fn code(&self) -> ErrorCode {
        match self {
            // 安装失败属于进程内部故障，调用方无法通过重试修复
            Self::AlreadyInitialized => ErrorCode::InternalError,
            Self::TracingInstall(_) => ErrorCode::InternalError,
            Self::MetricsInstall(_) => ErrorCode::InternalError,
            // 配置取值非法属于入参问题
            Self::InvalidLogFormat(_) => ErrorCode::InvalidArgument,
            #[cfg(feature = "otlp")]
            Self::OtlpInit(_) => ErrorCode::InternalError,
            #[cfg(feature = "otlp")]
            Self::OtlpRequiresRuntime => ErrorCode::InternalError,
        }
    }

    /// 转成对外统一错误体（架构 §17.4）。
    pub fn to_platform_error(&self) -> domain::PlatformError {
        // message 只含本枚举的 Display 文案，不含配置里的 endpoint/token 等敏感信息
        domain::PlatformError::new(self.code(), self.to_string())
    }
}

impl From<tracing::subscriber::SetGlobalDefaultError> for TelemetryError {
    fn from(err: tracing::subscriber::SetGlobalDefaultError) -> Self {
        Self::TracingInstall(err)
    }
}

impl From<metrics_exporter_prometheus::BuildError> for TelemetryError {
    fn from(err: metrics_exporter_prometheus::BuildError) -> Self {
        Self::MetricsInstall(err)
    }
}

#[cfg(feature = "otlp")]
impl From<opentelemetry_otlp::ExporterBuildError> for TelemetryError {
    fn from(err: opentelemetry_otlp::ExporterBuildError) -> Self {
        Self::OtlpInit(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_map_to_domain() {
        assert_eq!(
            TelemetryError::AlreadyInitialized.code(),
            ErrorCode::InternalError
        );
        assert_eq!(
            TelemetryError::InvalidLogFormat("x".into()).code(),
            ErrorCode::InvalidArgument
        );
        // 对外错误体中的 code 字符串与 proto 契约一致
        assert_eq!(
            TelemetryError::AlreadyInitialized.code().as_str(),
            "INTERNAL_ERROR"
        );
    }

    #[test]
    fn converts_to_platform_error() {
        let body = TelemetryError::InvalidLogFormat("xml".into()).to_platform_error();
        assert_eq!(body.code, ErrorCode::InvalidArgument);
        assert_eq!(body.code.http_status(), 400);
        // retryable 由错误码语义决定（此处不可重试）
        assert_eq!(body.retryable, ErrorCode::InvalidArgument.retryable());
        assert!(!body.message.is_empty());
    }

    #[test]
    fn error_display_is_not_empty() {
        let err = TelemetryError::AlreadyInitialized;
        assert!(!err.to_string().is_empty());
    }
}
