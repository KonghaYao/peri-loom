//! observability —— 平台统一观测层（架构 §14 / §17.10）。
//!
//! 设计边界：
//! - 业务代码只产生 `tracing` span/event 与 `metrics`，**不得**直接依赖
//!   Grafana / Loki / Tempo / Prometheus 的 SDK；所有后端绑定都收敛在本 crate。
//! - 日志走 `tracing-subscriber`（JSON 或 Pretty），指标走 `metrics` facade +
//!   Prometheus exporter，trace 只在开启 `otlp` feature 时导出 OTLP。
//! - 所有安装动作都是进程级的，重复执行必须安全失败，不能 panic。
//!
//! ```no_run
//! fn main() -> Result<(), observability::TelemetryError> {
//!     let config = observability::TelemetryConfig::from_env("db-server", "0.1.0");
//!     let guard = observability::init(config)?;
//!
//!     // 业务代码：只创建 span 与指标
//!     let request = observability::RequestSpan::builder()
//!         .external_request_id("req-1")
//!         .db_id("db-1")
//!         .build()
//!         .start();
//!     let _entered = request.enter();
//!     observability::metrics::record_query_latency_micros("query", 1_200);
//!     request.record_wal_lsn(4096);
//!
//!     guard.shutdown();
//!     Ok(())
//! }
//! ```

mod config;
mod context;
mod error;
pub mod metrics;
#[cfg(feature = "otlp")]
mod otlp;

use std::sync::atomic::{AtomicBool, Ordering};

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use tracing::Subscriber;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::{fmt, EnvFilter, Registry};

pub use crate::config::{env_keys, LogFormat, TelemetryConfig};
pub use crate::context::{
    field_names, RequestSpan, RequestSpanBuilder, RequestSpanHandle, REQUEST_SPAN_NAME,
};
pub use crate::error::TelemetryError;

/// 未显式配置过滤级别时的默认值。
pub const DEFAULT_LOG_LEVEL: &str = "info";

/// OTLP instrumentation scope 名（固定，便于 collector 侧识别来源）。
pub const SERVICE_INSTRUMENTATION_NAME: &str = "observability";

/// 进程级初始化标记：保证 `init` 只生效一次。
static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// 统一的 layer 类型，使 JSON / Pretty / OTLP 组合后仍保持单一 subscriber 类型。
type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync + 'static>;

/// 类型擦除后的全局 subscriber。
type BoxedSubscriber = Box<dyn Subscriber + Send + Sync + 'static>;

/// 安装平台全局 telemetry。
///
/// 行为契约：
/// - 安装全局 tracing subscriber（registry + EnvFilter + fmt layer；`Json` 走
///   结构化 JSON，`Pretty` 走人类可读），并打印一条生效配置的 INFO 日志。
/// - `metrics_listen_addr` 有值时安装 Prometheus recorder 并监听该地址。
///   **该 /metrics 端点只允许集群内抓取，禁止经公网暴露**（架构 §17.4）。
/// - `otlp` feature 打开且 `otlp_endpoint` 有值时构建 OTLP gRPC exporter 与
///   tracing-opentelemetry layer；导出失败只影响 trace，不影响日志与指标。
///   该 exporter 的 gRPC channel 绑定 Tokio reactor，因此启用 OTLP 时必须在
///   runtime 上下文内调用本函数（服务 `main` 通常已是 `#[tokio::main]`），
///   否则返回 [`TelemetryError::OtlpRequiresRuntime`]。
/// - 重复调用（或调用方已自行安装全局 subscriber）返回
///   [`TelemetryError::AlreadyInitialized`] / [`TelemetryError::TracingInstall`]，
///   不 panic；安装失败会复位内部标记，允许修正配置后重试。
///
/// 注意：
/// - 本函数不是事务性的。若 metrics 安装失败，tracing subscriber 已经生效
///   （全局安装无法撤销），此时调用方通常应直接终止启动。
/// - JSON 日志行内不带 `service_name` / `instance_id`，这两项由采集端
///   （Promtail / Loki static label）注入，避免每条日志重复写常量字段。
pub fn init(config: TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    if INITIALIZED.swap(true, Ordering::AcqRel) {
        return Err(TelemetryError::AlreadyInitialized);
    }

    match install(&config) {
        Ok(guard) => Ok(guard),
        Err(err) => {
            // 安装失败不是终态：允许调用方修正配置后重试
            INITIALIZED.store(false, Ordering::Release);
            Err(err)
        }
    }
}

/// 安装测试用 subscriber：幂等、不安装 metrics recorder。
///
/// 只用于测试环境；已被占用（含其它测试安装过）时静默返回，避免测试相互干扰。
/// 若先调用了本函数，后续 `init` 会因全局 subscriber 已被占用而失败。
pub fn init_test_subscriber() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL));
    // try_init：已安装时返回 Err，这里静默忽略以满足幂等要求
    let _ = fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_target(true)
        .try_init();
}

fn install(config: &TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    // OTLP layer 必须在全局 subscriber 安装前构建出来
    #[cfg(feature = "otlp")]
    let (otel_layer, tracer_provider) = match otlp::build(config)? {
        Some(parts) => (Some(parts.layer), Some(parts.provider)),
        None => (None, None),
    };
    #[cfg(not(feature = "otlp"))]
    let otel_layer: Option<BoxedLayer> = None;

    let subscriber = build_subscriber(config, otel_layer);
    tracing::subscriber::set_global_default(subscriber).map_err(TelemetryError::TracingInstall)?;

    if let Some(addr) = config.metrics_listen_addr {
        // 已存在全局 recorder 时返回错误而不是 panic
        prometheus_builder()?.with_http_listener(addr).install()?;
    }

    tracing::info!(
        service_name = %config.service_name,
        service_version = %config.service_version,
        instance_id = %config.instance_id,
        log_format = %config.log_format,
        log_level = %config.log_level,
        metrics_listen_addr = %config
            .metrics_listen_addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "disabled".to_owned()),
        otlp_endpoint = %config.otlp_endpoint.as_deref().unwrap_or("disabled"),
        otlp_active = config.otlp_enabled(),
        slow_query_threshold_ms = config.slow_query_threshold_ms,
        "telemetry initialized"
    );

    Ok(TelemetryGuard {
        config: config.clone(),
        #[cfg(feature = "otlp")]
        tracer_provider,
    })
}

/// 构造 Prometheus recorder builder。
///
/// 延迟类指标单位是微秒，而 exporter 默认输出 summary（无法跨实例聚合 P95/P99）
/// 且默认桶是秒级的；这里按 `_micros` 后缀统一改成按微秒分桶的真直方图。
pub(crate) fn prometheus_builder() -> Result<PrometheusBuilder, TelemetryError> {
    Ok(PrometheusBuilder::new().set_buckets_for_metric(
        Matcher::Suffix("_micros".to_owned()),
        metrics::LATENCY_BUCKETS_MICROS,
    )?)
}

/// 组装 layer 栈：可选 OTLP 层 + fmt 层，EnvFilter 置最外层做全局过滤。
///
/// 注意 layer 顺序：类型擦除的 layer（`Box<dyn Layer<Registry>>`）只能紧贴
/// `Registry`，否则无法满足后续组合的类型约束；过滤语义与顺序无关。
fn build_subscriber(config: &TelemetryConfig, otel_layer: Option<BoxedLayer>) -> BoxedSubscriber {
    let filter =
        EnvFilter::try_new(&config.log_level).unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_LEVEL));

    // 顺序敏感：per-layer filter 只作用于其内侧的 layer，EnvFilter 必须最后加入
    // （最外层）才是全局过滤。
    match config.log_format {
        LogFormat::Json => Box::new(
            Registry::default()
                .with(otel_layer)
                .with(
                    fmt::layer()
                        .json()
                        // event 字段提到顶层，便于日志后端按字段查询
                        .flatten_event(true)
                        .with_current_span(true)
                        // span 列表承载 trace context（架构 §17.10）
                        .with_span_list(true),
                )
                .with(filter),
        ),
        LogFormat::Pretty => Box::new(
            Registry::default()
                .with(otel_layer)
                .with(fmt::layer())
                .with(filter),
        ),
    }
}

/// telemetry 生命周期句柄。
///
/// tracing / metrics 的全局安装是进程级的，无法撤销；Drop 只负责 flush 缓冲中的
/// trace（OTLP 批量导出），需要确定性关闭时调用 [`TelemetryGuard::shutdown`]。
#[derive(Debug)]
pub struct TelemetryGuard {
    config: TelemetryConfig,
    #[cfg(feature = "otlp")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl TelemetryGuard {
    /// 生效的配置。
    pub fn config(&self) -> &TelemetryConfig {
        &self.config
    }

    /// 是否正在导出 trace。
    pub fn otlp_active(&self) -> bool {
        self.config.otlp_enabled()
    }

    /// 显式关闭：flush 未导出的 trace 并释放导出资源。
    pub fn shutdown(mut self) {
        self.flush();
    }

    fn flush(&mut self) {
        #[cfg(feature = "otlp")]
        if let Some(provider) = self.tracer_provider.as_ref() {
            if let Err(err) = provider.force_flush() {
                tracing::warn!(error = %err, "OTLP force_flush 失败");
            }
            if let Err(err) = provider.shutdown() {
                tracing::warn!(error = %err, "OTLP shutdown 失败");
            }
            self.tracer_provider = None;
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.flush();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    // 注意：涉及全局 subscriber / recorder 安装的用例已移到
    // tests/init_global.rs（独立进程），避免与同进程其它用例竞争不可撤销的全局状态。

    #[test]
    fn subscriber_builds_for_both_formats_and_applies_filter() {
        for format in [LogFormat::Json, LogFormat::Pretty] {
            let mut config = TelemetryConfig::new("svc", "0.0.1");
            config.log_format = format;

            config.log_level = "warn".to_owned();
            let subscriber = build_subscriber(&config, None);
            tracing::subscriber::with_default(subscriber, || {
                assert!(
                    !tracing::enabled!(tracing::Level::INFO),
                    "{format} 应过滤 INFO"
                );
                assert!(tracing::enabled!(tracing::Level::WARN));
            });

            config.log_level = "info".to_owned();
            let subscriber = build_subscriber(&config, None);
            tracing::subscriber::with_default(subscriber, || {
                assert!(tracing::enabled!(tracing::Level::INFO));
            });
        }
    }

    #[test]
    fn test_subscriber_install_is_idempotent() {
        init_test_subscriber();
        init_test_subscriber();

        // 安装测试 subscriber 后仍可正常产生 span 与 event
        let span = RequestSpan::builder().db_id("db-test").start();
        let _entered = span.enter();
        tracing::info!("probe");
    }
}
