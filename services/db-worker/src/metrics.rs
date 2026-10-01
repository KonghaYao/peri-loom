//! db-worker 指标入口。
//!
//! 指标名是冻结契约（`crates/observability/src/metrics.rs` 的 [`metric_names`]），
//! 本模块只做「参数适配」，禁止在这里自造指标名 —— Grafana 面板与告警按名字查询。
//!
//! 所有函数在未安装 recorder 时是空操作（`metrics` facade 语义），因此可以放心出现在
//! 热路径与降级路径上。

use observability::metrics::metric_names;

use crate::registry::LocalDatabase;
use crate::resources::DbUsage;

/// 记录一次 DB 进程退出。
///
/// `abnormal = true` 时计入契约指标 `db_process_crash_total`（标签 `reason`）；
/// 正常停止（Stop/Kill 指令）不计入 —— 否则运维正常回收进程会污染崩溃告警。
/// 无论是否异常都写一条带 db_id 的明细计数，便于按库排障。
pub fn record_process_exit(db_id: &str, abnormal: bool, reason: &str) {
    metrics::counter!(
        "worker_db_process_exit_total",
        "db_id" => db_id.to_string(),
        "reason" => reason.to_string(),
        "abnormal" => abnormal.to_string(),
    )
    .increment(1);
    if abnormal {
        observability::metrics::record_db_process_crash(reason);
    }
}

/// 记录一次 DB 启动（含冷启动恢复与崩溃重启）。
pub fn record_db_start(outcome: &str, micros: u64) {
    metrics::counter!(
        metric_names::START_DB_TOTAL,
        "outcome" => outcome.to_string(),
    )
    .increment(1);
    if outcome == "ok" {
        observability::metrics::record_cold_start_micros(outcome, micros);
    }
}

/// 记录准入被拒。
pub fn record_admission_denied(reason: &str) {
    metrics::counter!(
        metric_names::ADMISSION_DENIED_TOTAL,
        "reason" => reason.to_string(),
    )
    .increment(1);
}

/// 上报单个 DB 进程的实测占用（CPU / 内存）。
pub fn record_db_usage(usage: &DbUsage) {
    observability::metrics::record_db_process_cpu(
        &usage.database_id,
        crate::cgroup::cpu_milli_to_cores(usage.cpu_milli),
    );
    observability::metrics::record_db_process_memory_mib(&usage.database_id, usage.rss_mib as f64);
}

/// 上报 Worker 六维最大饱和度（0.0 ~ N.0）。
pub fn record_saturation(ratio: f64) {
    metrics::gauge!(
        metric_names::WORKER_SATURATION,
        observability::metrics::label_names::WORKER_ID => WORKER_ID_LABEL.get().map(String::as_str).unwrap_or("unknown").to_string(),
    )
    .set(ratio);
}

/// Worker 指标里需要固定携带 worker_id 标签，这里在启动时注入一次。
///
/// 用 `OnceLock` 而不是每个函数都传 `worker_id`：worker_id 在进程生命周期内不变，
/// 而指标函数被热路径调用，多传一个参数没有信息增益。
static WORKER_ID_LABEL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// 启动时设置 worker_id 标签（幂等，重复调用以首次为准）。
pub fn set_worker_id_label(worker_id: &str) {
    let _ = WORKER_ID_LABEL.set(worker_id.to_string());
}

/// Data Dispatcher 请求计数。
pub fn record_dispatch(kind: &str, outcome: &str, micros: u64) {
    observability::metrics::record_query_latency_micros(kind, micros);
    metrics::counter!(
        "worker_dispatch_total",
        "kind" => kind.to_string(),
        "outcome" => outcome.to_string(),
    )
    .increment(1);
}

/// 本地注册表规模（DB 进程数）与上报状态。
pub fn record_registry_size(count: usize) {
    metrics::gauge!("worker_db_process_count").set(count as f64);
}

/// 记录一次恢复（Snapshot + WAL replay）结果。
pub fn record_restore(outcome: &str, micros: u64) {
    metrics::counter!("worker_restore_total", "outcome" => outcome.to_string()).increment(1);
    metrics::histogram!("worker_restore_micros").record(micros as f64);
}

/// 记录本地 DB 生命周期状态（标签为状态字符串，基数受限于 7 个状态）。
#[allow(dead_code)] // 组件内部 API：供诊断 / 运维端点与测试使用，二进制 crate 里不构成生产调用点
pub fn record_db_state(entry: &LocalDatabase) {
    metrics::gauge!(
        "worker_db_state",
        "state" => entry.state.to_db_str().to_string(),
    )
    .set(1.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_id_label_is_set_once() {
        set_worker_id_label("worker-test");
        set_worker_id_label("worker-other");
        assert_eq!(
            WORKER_ID_LABEL.get().map(String::as_str),
            Some("worker-test")
        );
        // 未安装 recorder 时也必须安全（不得 panic）
        record_saturation(0.75);
        record_db_start("ok", 1200);
        record_admission_denied("watermark");
        record_process_exit("db-1", true, "signal");
        record_process_exit("db-1", false, "requested");
        record_restore("ok", 100);
        record_registry_size(3);
        record_dispatch("query", "ok", 42);
    }
}
