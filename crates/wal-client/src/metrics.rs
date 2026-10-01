//! 指标记录。
//!
//! 指标名与标签名是**冻结契约**：Grafana 面板 / 告警按名字查询，订阅方是
//! `crates/observability/src/metrics.rs` 的 `metric_names`（§14 / §17.10）。
//! 本 crate 只用 `metrics` facade 记数，不引入任何 exporter —— exporter 属于进程的
//! `observability::init`，传输层客户端不该替调用方决定指标后端。
//!
//! 未安装 recorder 时这些函数是空操作（facade 语义），因此可以安全地放在热路径上。

use crate::metric_names;

/// 标签值类型：`metrics` 的 `Label` 由 `String` 转换而来。
fn label(value: &str) -> String {
    value.to_owned()
}

/// 记录一次 append 的客户端观测延迟（单位微秒）。
///
/// 成功与失败都记录：失败请求同样占用 commit 热路径的时间，只看成功的直方图会低估尾延迟。
pub(crate) fn record_append_latency_micros(micros: u64) {
    metrics::histogram!(metric_names::WAL_APPEND_LATENCY_MICROS).record(micros as f64);
}

/// 记录一次 append 成功（即 quorum durable 落地）。
pub(crate) fn record_append_success() {
    metrics::counter!(metric_names::WAL_APPEND_TOTAL).increment(1);
}

/// 记录一次 append 失败。`reason` 必须是低基数枚举短串（见 `AttemptFailure::metric_reason`）。
pub(crate) fn record_append_error(reason: &str) {
    metrics::counter!(
        metric_names::WAL_APPEND_ERROR_TOTAL,
        "reason" => label(reason),
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use metrics::atomics::AtomicU64;
    use metrics::{
        Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString,
        Unit,
    };

    use super::*;

    /// 记录键：`name` 或 `name{label=value,...}`（标签按 key 排序，保证可断言）。
    fn render_key(key: &Key) -> String {
        let labels: Vec<(String, String)> = key
            .labels()
            .map(|label| (label.key().to_owned(), label.value().to_owned()))
            .collect();
        if labels.is_empty() {
            return key.name().to_owned();
        }
        let mut rendered: Vec<String> = labels
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        rendered.sort();
        format!("{}{{{}}}", key.name(), rendered.join(","))
    }

    /// 极简 recorder：只关心「记了哪些键、值是多少」。
    #[derive(Clone, Default)]
    struct RecordingRecorder {
        counters: Arc<Mutex<HashMap<String, u64>>>,
        histograms: Arc<Mutex<Vec<(String, f64)>>>,
    }

    struct RecordingCounter {
        key: String,
        sink: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl metrics::CounterFn for RecordingCounter {
        fn increment(&self, value: u64) {
            *self
                .sink
                .lock()
                .unwrap()
                .entry(self.key.clone())
                .or_insert(0) += value;
        }

        fn absolute(&self, value: u64) {
            self.sink.lock().unwrap().insert(self.key.clone(), value);
        }
    }

    struct RecordingHistogram {
        key: String,
        sink: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl HistogramFn for RecordingHistogram {
        fn record(&self, value: f64) {
            self.sink.lock().unwrap().push((self.key.clone(), value));
        }
    }

    impl Recorder for RecordingRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _desc: SharedString) {}
        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _desc: SharedString) {}
        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _desc: SharedString) {}

        fn register_counter(&self, key: &Key, _meta: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(RecordingCounter {
                key: render_key(key),
                sink: self.counters.clone(),
            }))
        }

        fn register_gauge(&self, _key: &Key, _meta: &Metadata<'_>) -> Gauge {
            // wal-client 不使用 gauge；返回一个哑实现即可
            Gauge::from_arc(Arc::new(AtomicU64::new(0)))
        }

        fn register_histogram(&self, key: &Key, _meta: &Metadata<'_>) -> Histogram {
            Histogram::from_arc(Arc::new(RecordingHistogram {
                key: render_key(key),
                sink: self.histograms.clone(),
            }))
        }
    }

    #[test]
    fn successful_append_records_total_and_latency() {
        let recorder = RecordingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_append_success();
            record_append_latency_micros(1_500);
        });

        let counters = recorder.counters.lock().unwrap();
        assert_eq!(
            counters.get(metric_names::WAL_APPEND_TOTAL).copied(),
            Some(1),
            "成功 append 必须计入 wal_append_total"
        );
        let histograms = recorder.histograms.lock().unwrap();
        assert_eq!(
            histograms.as_slice(),
            [(metric_names::WAL_APPEND_LATENCY_MICROS.to_owned(), 1_500.0)]
        );
    }

    #[test]
    fn failed_append_records_reason_label() {
        let recorder = RecordingRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_append_error("transport");
            record_append_error("transport");
            record_append_error("not_leader");
        });

        let counters = recorder.counters.lock().unwrap();
        assert_eq!(
            counters
                .get(&format!(
                    "{}{{{}}}",
                    metric_names::WAL_APPEND_ERROR_TOTAL,
                    "reason=transport"
                ))
                .copied(),
            Some(2)
        );
        assert_eq!(
            counters
                .get(&format!(
                    "{}{{{}}}",
                    metric_names::WAL_APPEND_ERROR_TOTAL,
                    "reason=not_leader"
                ))
                .copied(),
            Some(1)
        );
    }

    #[test]
    fn recording_without_recorder_is_noop() {
        // 未安装 recorder 时不得 panic（facade 契约），也不得产生全局副作用
        metrics::with_local_recorder(&metrics::NoopRecorder, || {
            record_append_success();
            record_append_latency_micros(10);
            record_append_error("internal");
        });
    }

    /// 指标名必须与 `crates/observability/src/metrics.rs`（契约单一来源）逐字一致。
    #[test]
    fn metric_names_match_observability_contract() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../observability/src/metrics.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("读取 {} 失败：{err}", path.display()));
        for name in [
            metric_names::WAL_APPEND_LATENCY_MICROS,
            metric_names::WAL_APPEND_TOTAL,
            metric_names::WAL_APPEND_ERROR_TOTAL,
        ] {
            assert!(
                source.contains(&format!("\"{name}\"")),
                "指标名 {name} 未出现在 observability 契约文件中"
            );
        }
        // 标签名同样来自契约（observability 的 label_names::REASON）
        assert!(source.contains("\"reason\""), "reason 标签名与契约不一致");
    }
}
