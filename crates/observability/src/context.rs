//! 贯穿 Server / Worker / DB Process 的 trace context（架构 §17.10）。
//!
//! 业务代码只负责填充字段并创建 span；span 到 OTLP/Tempo 的导出由本 crate 的
//! subscriber 完成，业务代码不接触任何 tracing 后端 SDK。

/// 请求 span 的字段名，供业务代码在 `span.record(..)` 时复用，避免字符串漂移。
pub mod field_names {
    /// 外部请求 id（网关下发，用于跨系统关联）。
    pub const EXTERNAL_REQUEST_ID: &str = "external_request_id";
    /// trace id（W3C traceparent）。
    pub const TRACE_ID: &str = "trace_id";
    /// 数据库 id。
    pub const DB_ID: &str = "db_id";
    /// 租户 id。
    pub const TENANT_ID: &str = "tenant_id";
    /// Worker id。
    pub const WORKER_ID: &str = "worker_id";
    /// DB 进程 id。
    pub const DB_PROCESS_ID: &str = "db_process_id";
    /// Owner epoch（单调递增，见架构 §15.2）。
    pub const OWNER_EPOCH: &str = "owner_epoch";
    /// 会话 id。
    pub const SESSION_ID: &str = "session_id";
    /// WAL LSN。
    pub const WAL_LSN: &str = "wal_lsn";
}

/// 请求 span 名：下游按 name 检索该链路。
pub const REQUEST_SPAN_NAME: &str = "request";

/// 请求级 trace context。
///
/// 字符串字段在 `None` 时记录为空串（架构 §17.10 要求字段始终存在，便于按字段检索）；
/// 数值字段在 `None` 时用 `Empty` 占位，可在运行期补记（见 [`RequestSpanHandle`]）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestSpan {
    /// 外部请求 id。
    pub external_request_id: Option<String>,
    /// trace id。
    pub trace_id: Option<String>,
    /// 数据库 id。
    pub db_id: Option<String>,
    /// 租户 id。
    pub tenant_id: Option<String>,
    /// Worker id。
    pub worker_id: Option<String>,
    /// DB 进程 id。
    pub db_process_id: Option<String>,
    /// Owner epoch。
    pub owner_epoch: Option<u64>,
    /// 会话 id。
    pub session_id: Option<String>,
    /// WAL LSN。
    pub wal_lsn: Option<u64>,
}

impl RequestSpan {
    /// 构造 builder。
    pub fn builder() -> RequestSpanBuilder {
        RequestSpanBuilder::default()
    }

    /// 创建带全部字段的 span（不进入）。
    pub fn span(&self) -> tracing::Span {
        let span = tracing::info_span!(
            REQUEST_SPAN_NAME,
            external_request_id = %text(&self.external_request_id),
            trace_id = %text(&self.trace_id),
            db_id = %text(&self.db_id),
            tenant_id = %text(&self.tenant_id),
            worker_id = %text(&self.worker_id),
            db_process_id = %text(&self.db_process_id),
            owner_epoch = tracing::field::Empty,
            session_id = %text(&self.session_id),
            wal_lsn = tracing::field::Empty,
        );
        // 数值字段创建时若已知则立即补记，未知则等待运行期 record
        if let Some(epoch) = self.owner_epoch {
            span.record(field_names::OWNER_EPOCH, epoch);
        }
        if let Some(lsn) = self.wal_lsn {
            span.record(field_names::WAL_LSN, lsn);
        }
        span
    }

    /// 创建 span 并返回可继续记录字段的句柄。
    pub fn start(&self) -> RequestSpanHandle {
        RequestSpanHandle::new(self.clone())
    }
}

/// 已创建请求 span 的句柄：保留原始 context，便于运行期补记字段。
#[derive(Debug, Clone)]
pub struct RequestSpanHandle {
    request: RequestSpan,
    span: tracing::Span,
}

impl RequestSpanHandle {
    /// 用给定 context 创建 span 句柄。
    pub fn new(request: RequestSpan) -> Self {
        let span = request.span();
        Self { request, span }
    }

    /// 底层 span，可用于 `enter()` 之外的显式传递（如注入 OTLP parent）。
    pub fn span(&self) -> &tracing::Span {
        &self.span
    }

    /// 原始 trace context。
    pub fn request(&self) -> &RequestSpan {
        &self.request
    }

    /// 进入 span；返回的 guard 在 Drop 时退出。
    pub fn enter(&self) -> tracing::span::Entered<'_> {
        self.span.enter()
    }

    /// 补记 WAL LSN（Commit durability 检查点，架构 §11.1）。
    ///
    /// span 创建时 `wal_lsn` 以 `Empty` 占位，因此这里可以安全覆盖。
    pub fn record_wal_lsn(&self, lsn: u64) {
        self.span.record(field_names::WAL_LSN, lsn);
    }
}

/// [`RequestSpan`] 的 builder。
#[derive(Debug, Clone, Default)]
pub struct RequestSpanBuilder {
    inner: RequestSpan,
}

impl RequestSpanBuilder {
    /// 空 builder。
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置外部请求 id。
    pub fn external_request_id(mut self, value: impl Into<String>) -> Self {
        self.inner.external_request_id = Some(value.into());
        self
    }

    /// 设置 trace id。
    pub fn trace_id(mut self, value: impl Into<String>) -> Self {
        self.inner.trace_id = Some(value.into());
        self
    }

    /// 设置数据库 id。
    pub fn db_id(mut self, value: impl Into<String>) -> Self {
        self.inner.db_id = Some(value.into());
        self
    }

    /// 设置租户 id。
    pub fn tenant_id(mut self, value: impl Into<String>) -> Self {
        self.inner.tenant_id = Some(value.into());
        self
    }

    /// 设置 Worker id。
    pub fn worker_id(mut self, value: impl Into<String>) -> Self {
        self.inner.worker_id = Some(value.into());
        self
    }

    /// 设置 DB 进程 id。
    pub fn db_process_id(mut self, value: impl Into<String>) -> Self {
        self.inner.db_process_id = Some(value.into());
        self
    }

    /// 设置 Owner epoch。
    pub fn owner_epoch(mut self, value: u64) -> Self {
        self.inner.owner_epoch = Some(value);
        self
    }

    /// 设置会话 id。
    pub fn session_id(mut self, value: impl Into<String>) -> Self {
        self.inner.session_id = Some(value.into());
        self
    }

    /// 设置 WAL LSN。
    pub fn wal_lsn(mut self, value: u64) -> Self {
        self.inner.wal_lsn = Some(value);
        self
    }

    /// 产出 trace context。
    pub fn build(self) -> RequestSpan {
        self.inner
    }

    /// 直接创建 span 句柄。
    pub fn start(self) -> RequestSpanHandle {
        self.build().start()
    }
}

/// `None` 记为 `""`，保证 span 字段始终可检索。
fn text(value: &Option<String>) -> &str {
    value.as_deref().unwrap_or("")
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::*;

    /// 把 span 新建/补记的字段值抓取出来，用于断言契约。
    #[derive(Default)]
    struct FieldCapture {
        values: Arc<Mutex<Vec<(String, FieldValue)>>>,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum FieldValue {
        Str(String),
        U64(u64),
    }

    struct Visitor<'a>(&'a FieldCapture);

    impl tracing::field::Visit for Visitor<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0
                .values
                .lock()
                .unwrap()
                .push((field.name().to_owned(), FieldValue::Str(value.to_owned())));
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0
                .values
                .lock()
                .unwrap()
                .push((field.name().to_owned(), FieldValue::U64(value)));
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.values.lock().unwrap().push((
                field.name().to_owned(),
                FieldValue::Str(format!("{value:?}")),
            ));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for FieldCapture {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: Context<'_, S>,
        ) {
            attrs.record(&mut Visitor(self));
        }

        fn on_record(
            &self,
            _id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: Context<'_, S>,
        ) {
            values.record(&mut Visitor(self));
        }
    }

    fn capture<F: FnOnce()>(f: F) -> Vec<(String, FieldValue)> {
        let capture = FieldCapture::default();
        let values = Arc::clone(&capture.values);
        let subscriber = tracing_subscriber::registry().with(capture);
        tracing::subscriber::with_default(subscriber, f);
        let captured = values.lock().unwrap().clone();
        captured
    }

    fn value_of<'a>(captured: &'a [(String, FieldValue)], name: &str) -> Option<&'a FieldValue> {
        captured
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value)
    }

    #[test]
    fn span_name_and_empty_fields_are_stable() {
        let captured = capture(|| {
            let span = RequestSpan::default().span();
            assert_eq!(span.metadata().unwrap().name(), REQUEST_SPAN_NAME);
        });

        // 未设置的字符串字段记为 ""
        for field in [
            field_names::EXTERNAL_REQUEST_ID,
            field_names::TRACE_ID,
            field_names::DB_ID,
            field_names::TENANT_ID,
            field_names::WORKER_ID,
            field_names::DB_PROCESS_ID,
            field_names::SESSION_ID,
        ] {
            assert_eq!(
                value_of(&captured, field),
                Some(&FieldValue::Str(String::new())),
                "字段 {field} 应存在且为空串"
            );
        }
        // 数值字段用 Empty 占位，不产生值
        assert!(value_of(&captured, field_names::OWNER_EPOCH).is_none());
        assert!(value_of(&captured, field_names::WAL_LSN).is_none());
    }

    #[test]
    fn builder_records_all_context_fields() {
        let request = RequestSpan::builder()
            .external_request_id("req-1")
            .trace_id("trace-1")
            .db_id("db-1")
            .tenant_id("tenant-1")
            .worker_id("worker-1")
            .db_process_id("proc-1")
            .owner_epoch(7)
            .session_id("session-1")
            .wal_lsn(1024)
            .build();

        let captured = capture(|| {
            let _handle = request.start();
        });

        assert_eq!(
            value_of(&captured, field_names::EXTERNAL_REQUEST_ID),
            Some(&FieldValue::Str("req-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::TRACE_ID),
            Some(&FieldValue::Str("trace-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::DB_ID),
            Some(&FieldValue::Str("db-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::TENANT_ID),
            Some(&FieldValue::Str("tenant-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::WORKER_ID),
            Some(&FieldValue::Str("worker-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::DB_PROCESS_ID),
            Some(&FieldValue::Str("proc-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::SESSION_ID),
            Some(&FieldValue::Str("session-1".to_owned()))
        );
        assert_eq!(
            value_of(&captured, field_names::OWNER_EPOCH),
            Some(&FieldValue::U64(7))
        );
        assert_eq!(
            value_of(&captured, field_names::WAL_LSN),
            Some(&FieldValue::U64(1024))
        );
    }

    #[test]
    fn record_wal_lsn_updates_span() {
        let captured = capture(|| {
            let handle = RequestSpan::default().start();
            handle.record_wal_lsn(4096);
            assert!(handle.request().wal_lsn.is_none());
            assert_eq!(handle.span().metadata().unwrap().name(), REQUEST_SPAN_NAME);
        });

        assert_eq!(
            value_of(&captured, field_names::WAL_LSN),
            Some(&FieldValue::U64(4096))
        );
    }

    #[test]
    fn enter_keeps_span_current() {
        let subscriber = tracing_subscriber::registry();
        tracing::subscriber::with_default(subscriber, || {
            let handle = RequestSpan::builder().db_id("db-9").start();
            let _entered = handle.enter();
            assert_eq!(
                tracing::Span::current().metadata().unwrap().name(),
                REQUEST_SPAN_NAME
            );
        });
    }
}
