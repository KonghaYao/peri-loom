//! OTLP gRPC trace 导出（仅 `otlp` feature 编译）。
//!
//! 业务代码不感知该模块：它只在全局 subscriber 里增加一个导出 layer，
//! 把 `tracing` span 送到 OTLP collector（架构 §17.10）。

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::Layer;

use crate::config::TelemetryConfig;
use crate::error::TelemetryError;
use crate::{BoxedLayer, SERVICE_INSTRUMENTATION_NAME};

/// OTLP 导出所需的组件：layer（装入 subscriber）+ provider（交给 guard 管理生命周期）。
pub(crate) struct OtlpParts {
    /// trace 导出 layer。
    pub(crate) layer: BoxedLayer,
    /// tracer provider，负责批量导出与 flush。
    pub(crate) provider: SdkTracerProvider,
}

/// 构建 OTLP 导出组件；未配置 endpoint 时返回 `None`（不导出，也不阻塞启动）。
///
/// 必须在 Tokio runtime 上下文内调用：tonic 的 channel 会捕获当前 reactor，
/// 缺失时上游会 panic，这里改为返回 [`TelemetryError::OtlpRequiresRuntime`]。
pub(crate) fn build(config: &TelemetryConfig) -> Result<Option<OtlpParts>, TelemetryError> {
    let Some(endpoint) = config.otlp_endpoint.as_deref() else {
        return Ok(None);
    };

    if tokio::runtime::Handle::try_current().is_err() {
        return Err(TelemetryError::OtlpRequiresRuntime);
    }

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(TelemetryError::OtlpInit)?;

    // resource 携带 service 标识，Tempo/Jaeger 按此聚合
    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new(
            "service.version",
            config.service_version.clone(),
        ))
        .with_attribute(KeyValue::new(
            "service.instance.id",
            config.instance_id.clone(),
        ))
        .build();

    // BatchSpanProcessor：导出在独立线程做，不阻塞业务线程
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(SERVICE_INSTRUMENTATION_NAME);
    let layer = tracing_opentelemetry::layer().with_tracer(tracer).boxed();

    Ok(Some(OtlpParts { layer, provider }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_endpoint(endpoint: &str) -> TelemetryConfig {
        let mut config = TelemetryConfig::new("svc", "0.0.1");
        config.otlp_endpoint = Some(endpoint.to_owned());
        config
    }

    #[test]
    fn no_endpoint_means_no_export() {
        let config = TelemetryConfig::new("svc", "0.0.1");
        assert!(build(&config).unwrap().is_none());
    }

    #[test]
    fn requires_tokio_runtime() {
        // 无 runtime 时返回错误而不是 panic（上游 tonic/hyper-util 会 panic）
        assert!(matches!(
            build(&config_with_endpoint("http://127.0.0.1:4317")),
            Err(TelemetryError::OtlpRequiresRuntime)
        ));
    }

    #[tokio::test]
    async fn builds_provider_and_shuts_down() {
        // endpoint 指向未监听的地址也可构建（channel 为 lazy 建立）
        let parts = build(&config_with_endpoint("http://127.0.0.1:4317"))
            .unwrap()
            .expect("配置了 endpoint 应构建 exporter");
        assert!(parts.provider.force_flush().is_ok());
        assert!(parts.provider.shutdown().is_ok());

        let _layer = parts.layer;
    }

    #[tokio::test]
    async fn invalid_endpoint_is_reported() {
        assert!(matches!(
            build(&config_with_endpoint("not a url")),
            Err(TelemetryError::OtlpInit(_))
        ));
    }
}
