//! 平台指标契约：名字、标签与记录入口（架构 §14 / §17.10）。
//!
//! 约定：
//! - 指标名是冻结契约：Grafana 面板与告警按这些名字查询，业务代码必须引用
//!   [`metric_names`] 常量，不得自行拼字符串。
//! - 单位后缀含在名字里：`*_micros` 为微秒直方图，`*_mib` 为 MiB，`*_total` 为
//!   单调递增计数器，`*_cpu` 为进程 CPU 比例（0.0 ~ N.0，1.0 = 1 core），
//!   `*_saturation` 为 0.0 ~ 1.0 的饱和度比例。
//! - 标签只允许低基数维度（方法/路由模板/结果/原因/db_id）。禁止把 request_id、
//!   session_id、SQL 文本等高基数或敏感信息放进标签。
//! - 这些函数在未安装 recorder 时是空操作，不会 panic，因此可安全地散布在热路径。

/// 延迟直方图的桶边界（单位：微秒，表示每个桶的上界）。
///
/// 平台 P95 / P99 验收指标依赖 `histogram_quantile()` 跨实例聚合，因此延迟类指标
/// 必须导出为 Prometheus 直方图而不是 summary；桶按微秒量级给出，覆盖 100us ~ 10s。
pub const LATENCY_BUCKETS_MICROS: &[f64] = &[
    100.0,
    250.0,
    500.0,
    1_000.0,
    2_500.0,
    5_000.0,
    10_000.0,
    25_000.0,
    50_000.0,
    100_000.0,
    250_000.0,
    500_000.0,
    1_000_000.0,
    2_500_000.0,
    5_000_000.0,
    10_000_000.0,
];

/// 指标名常量（冻结）。
pub mod metric_names {
    /// 请求延迟直方图，单位微秒。
    pub const REQUEST_LATENCY_MICROS: &str = "request_latency_micros";
    /// SQL/批量查询延迟直方图，单位微秒。
    pub const QUERY_LATENCY_MICROS: &str = "query_latency_micros";
    /// DB 进程 CPU 占用（1.0 = 1 core）。
    pub const DB_PROCESS_CPU: &str = "db_process_cpu";
    /// DB 进程内存占用，单位 MiB。
    pub const DB_PROCESS_MEMORY_MIB: &str = "db_process_memory_mib";
    /// Worker 饱和度（0.0 ~ 1.0，Stop Admit 基线 0.8，架构 §15.5）。
    pub const WORKER_SATURATION: &str = "worker_saturation";
    /// 冷启动耗时直方图，单位微秒。
    pub const COLD_START_MICROS: &str = "cold_start_micros";
    /// DB 进程崩溃次数。
    pub const DB_PROCESS_CRASH_TOTAL: &str = "db_process_crash_total";
    /// 慢查询次数（阈值见 `TelemetryConfig::slow_query_threshold_ms`）。
    pub const SLOW_QUERY_TOTAL: &str = "slow_query_total";
    /// Route Cache 命中次数。
    pub const ROUTE_CACHE_HIT_TOTAL: &str = "route_cache_hit_total";
    /// Route Cache 未命中次数。
    pub const ROUTE_CACHE_MISS_TOTAL: &str = "route_cache_miss_total";
    /// Route Cache 失效删除的条目数（显式 invalidate + 快照 reconcile 清理）。
    pub const ROUTE_CACHE_INVALIDATION_TOTAL: &str = "route_cache_invalidation_total";
    /// Route Cache 检测到的陈旧信号数（epoch 倒退拒绝 / epoch 不一致 / 快照版本回退）。
    pub const ROUTE_CACHE_STALE_DETECTED_TOTAL: &str = "route_cache_stale_detected_total";
    /// Route 刷新耗时直方图，单位微秒。
    pub const ROUTE_REFRESH_MICROS: &str = "route_refresh_micros";
    /// Remote WAL append 延迟直方图，单位微秒。
    pub const WAL_APPEND_LATENCY_MICROS: &str = "wal_append_latency_micros";
    /// Remote WAL append 次数。
    pub const WAL_APPEND_TOTAL: &str = "wal_append_total";
    /// Remote WAL append 失败次数。
    pub const WAL_APPEND_ERROR_TOTAL: &str = "wal_append_error_total";
    /// Remote WAL 因 fencing（旧 owner_epoch）被拒绝的请求次数（架构 §11.3 / §16）。
    pub const WAL_FENCED_REJECTED_TOTAL: &str = "wal_fenced_rejected_total";
    /// DB 启动（冷启动 / 拉起）次数。
    pub const START_DB_TOTAL: &str = "start_db_total";
    /// 准入被拒次数（Stop Admit / Emergency，架构 §15.5）。
    pub const ADMISSION_DENIED_TOTAL: &str = "admission_denied_total";
    /// 会话建立次数。
    pub const SESSION_OPEN_TOTAL: &str = "session_open_total";
    /// 事务提交次数。
    pub const TRANSACTION_COMMIT_TOTAL: &str = "transaction_commit_total";
    /// 事务回滚次数。
    pub const TRANSACTION_ROLLBACK_TOTAL: &str = "transaction_rollback_total";
    /// 事务丢失次数（Failover，业务语义见架构 §15.3）。
    pub const TRANSACTION_LOST_TOTAL: &str = "transaction_lost_total";
    /// Snapshot 次数（创建 / 恢复）。
    pub const SNAPSHOT_TOTAL: &str = "snapshot_total";
}

/// 标签名常量（冻结）。
pub mod label_names {
    /// HTTP 方法，如 `POST`。
    pub const METHOD: &str = "method";
    /// 路由模板，必须是模板（如 `/data/v1/databases/{db_id}/query`）而非具体路径。
    pub const ROUTE: &str = "route";
    /// HTTP 状态码。
    pub const STATUS: &str = "status";
    /// 操作类别，如 `query` / `batch` / `session_query`。
    pub const KIND: &str = "kind";
    /// 结果，如 `ok` / `timeout` / `error` / `failed`。
    pub const OUTCOME: &str = "outcome";
    /// 失败原因（枚举化短字符串，不得带自由文本）。
    pub const REASON: &str = "reason";
    /// 数据库 id。
    pub const DB_ID: &str = "db_id";
    /// Worker id。
    pub const WORKER_ID: &str = "worker_id";
}

/// 记录请求延迟（单位微秒）。
///
/// 标签：`method`、`route`（路由模板）、`status`（HTTP 状态码）。
pub fn record_request_latency_micros(method: &str, route: &str, status: u16, micros: u64) {
    metrics::histogram!(
        metric_names::REQUEST_LATENCY_MICROS,
        label_names::METHOD => text(method),
        label_names::ROUTE => text(route),
        label_names::STATUS => status.to_string(),
    )
    .record(micros as f64);
}

/// 记录查询延迟（单位微秒）。
///
/// 标签：`kind`（`query` / `batch` / `session_query`）。
pub fn record_query_latency_micros(kind: &str, micros: u64) {
    metrics::histogram!(
        metric_names::QUERY_LATENCY_MICROS,
        label_names::KIND => text(kind),
    )
    .record(micros as f64);
}

/// 记录 DB 进程 CPU 占用（1.0 = 1 core）。
///
/// 标签：`db_id`（基数上界 = 平台 DB 总数，可接受）。
pub fn record_db_process_cpu(db_id: &str, cores: f64) {
    metrics::gauge!(
        metric_names::DB_PROCESS_CPU,
        label_names::DB_ID => text(db_id),
    )
    .set(cores);
}

/// 记录 DB 进程内存占用（单位 MiB）。
///
/// 标签：`db_id`。
pub fn record_db_process_memory_mib(db_id: &str, mib: f64) {
    metrics::gauge!(
        metric_names::DB_PROCESS_MEMORY_MIB,
        label_names::DB_ID => text(db_id),
    )
    .set(mib);
}

/// 记录 Worker 饱和度（0.0 ~ 1.0）。
///
/// 标签：`worker_id`。告警基线：0.8 停止准入，0.9 紧急（架构 §15.5）。
pub fn record_worker_saturation(worker_id: &str, ratio: f64) {
    metrics::gauge!(
        metric_names::WORKER_SATURATION,
        label_names::WORKER_ID => text(worker_id),
    )
    .set(ratio);
}

/// 记录冷启动耗时（单位微秒）。
///
/// 标签：`outcome`（`ok` / `timeout` / `error`）。
pub fn record_cold_start_micros(outcome: &str, micros: u64) {
    metrics::histogram!(
        metric_names::COLD_START_MICROS,
        label_names::OUTCOME => text(outcome),
    )
    .record(micros as f64);
}

/// 记录一次 DB 进程崩溃。标签：`reason`（如 `oom` / `signal` / `panic`）。
pub fn record_db_process_crash(reason: &str) {
    metrics::counter!(
        metric_names::DB_PROCESS_CRASH_TOTAL,
        label_names::REASON => text(reason),
    )
    .increment(1);
}

/// 记录一次慢查询。标签：`kind`（`query` / `batch` / `session_query`）。
pub fn record_slow_query(kind: &str) {
    metrics::counter!(
        metric_names::SLOW_QUERY_TOTAL,
        label_names::KIND => text(kind),
    )
    .increment(1);
}

/// 记录一次 Route Cache 命中。
pub fn record_route_cache_hit() {
    metrics::counter!(metric_names::ROUTE_CACHE_HIT_TOTAL).increment(1);
}

/// 记录一次 Route Cache 未命中。
pub fn record_route_cache_miss() {
    metrics::counter!(metric_names::ROUTE_CACHE_MISS_TOTAL).increment(1);
}

/// 记录 `count` 次 Route 失效删除（显式 `invalidate` 与快照 reconcile 清理同口径）。
///
/// `count` 为 0 时不产生任何时间序列：避免「没有失效」也被记成一次事件。
pub fn record_route_cache_invalidations(count: u64) {
    if count == 0 {
        return;
    }
    metrics::counter!(metric_names::ROUTE_CACHE_INVALIDATION_TOTAL).increment(count);
}

/// 记录 `count` 次 Route Cache 陈旧信号。
///
/// 口径与 `RouteCacheStats::stale_detected` 完全一致（epoch 倒退拒绝、同 epoch 换 owner、
/// 缓存 epoch 与调用方不一致、catalog 快照版本回退），因此两个数字可以互相校验。
pub fn record_route_cache_stale_detected(count: u64) {
    if count == 0 {
        return;
    }
    metrics::counter!(metric_names::ROUTE_CACHE_STALE_DETECTED_TOTAL).increment(count);
}

/// 记录 Route 刷新耗时（单位微秒）。
pub fn record_route_refresh_micros(micros: u64) {
    metrics::histogram!(metric_names::ROUTE_REFRESH_MICROS).record(micros as f64);
}

/// 记录 Remote WAL append 延迟（单位微秒）。Commit durability 的关键指标（架构 §11.1）。
pub fn record_wal_append_latency_micros(micros: u64) {
    metrics::histogram!(metric_names::WAL_APPEND_LATENCY_MICROS).record(micros as f64);
}

/// 记录一次 Remote WAL append。
pub fn record_wal_append() {
    metrics::counter!(metric_names::WAL_APPEND_TOTAL).increment(1);
}

/// 记录一次 Remote WAL append 失败。标签：`reason`。
pub fn record_wal_append_error(reason: &str) {
    metrics::counter!(
        metric_names::WAL_APPEND_ERROR_TOTAL,
        label_names::REASON => text(reason),
    )
    .increment(1);
}

/// 记录一次因 fencing 被拒的 WAL 写请求（架构 §11.3 / §16）。
///
/// 标签：`kind`（`append` / `set_owner_epoch`）、`reason`（`stale_epoch` /
/// `epoch_not_monotonic`）。两者都是有界枚举：`database_id` 不能进标签
/// （单 shard 上万 DB，会把时间序列打爆）。
///
/// 语义边界：只统计**因 owner_epoch 冲突而拒绝**的请求。其它失败（非 leader、超时、
/// 存储故障）走 [`record_wal_append_error`]，不要混进来，否则「fencing 是否在生效」
/// 这个告警会变成噪音。
pub fn record_wal_fenced_rejected(kind: &str, reason: &str) {
    metrics::counter!(
        metric_names::WAL_FENCED_REJECTED_TOTAL,
        label_names::KIND => text(kind),
        label_names::REASON => text(reason),
    )
    .increment(1);
}

/// 记录一次 DB 启动。标签：`outcome`（`ok` / `failed` / `timeout`）。
pub fn record_start_db(outcome: &str) {
    metrics::counter!(
        metric_names::START_DB_TOTAL,
        label_names::OUTCOME => text(outcome),
    )
    .increment(1);
}

/// 记录一次准入被拒。标签：`reason`（`worker_full` / `emergency` / `no_capacity`）。
pub fn record_admission_denied(reason: &str) {
    metrics::counter!(
        metric_names::ADMISSION_DENIED_TOTAL,
        label_names::REASON => text(reason),
    )
    .increment(1);
}

/// 记录一次会话建立。
pub fn record_session_open() {
    metrics::counter!(metric_names::SESSION_OPEN_TOTAL).increment(1);
}

/// 记录一次事务提交。
pub fn record_transaction_commit() {
    metrics::counter!(metric_names::TRANSACTION_COMMIT_TOTAL).increment(1);
}

/// 记录一次事务回滚。
pub fn record_transaction_rollback() {
    metrics::counter!(metric_names::TRANSACTION_ROLLBACK_TOTAL).increment(1);
}

/// 记录一次事务丢失（Failover 中未提交事务，返回 `TRANSACTION_LOST`）。
pub fn record_transaction_lost() {
    metrics::counter!(metric_names::TRANSACTION_LOST_TOTAL).increment(1);
}

/// 记录一次 Snapshot。标签：`kind`（`create` / `restore`）。
pub fn record_snapshot(kind: &str) {
    metrics::counter!(
        metric_names::SNAPSHOT_TOTAL,
        label_names::KIND => text(kind),
    )
    .increment(1);
}

/// 标签值需要 `'static` 或 owned，这里统一转 owned（metrics 内部会做字符串比较与缓存）。
fn text(value: &str) -> String {
    value.to_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// 所有指标名常量，新增指标时必须同步登记。
    const ALL_METRIC_NAMES: &[&str] = &[
        metric_names::REQUEST_LATENCY_MICROS,
        metric_names::QUERY_LATENCY_MICROS,
        metric_names::DB_PROCESS_CPU,
        metric_names::DB_PROCESS_MEMORY_MIB,
        metric_names::WORKER_SATURATION,
        metric_names::COLD_START_MICROS,
        metric_names::DB_PROCESS_CRASH_TOTAL,
        metric_names::SLOW_QUERY_TOTAL,
        metric_names::ROUTE_CACHE_HIT_TOTAL,
        metric_names::ROUTE_CACHE_MISS_TOTAL,
        metric_names::ROUTE_CACHE_INVALIDATION_TOTAL,
        metric_names::ROUTE_CACHE_STALE_DETECTED_TOTAL,
        metric_names::ROUTE_REFRESH_MICROS,
        metric_names::WAL_APPEND_LATENCY_MICROS,
        metric_names::WAL_APPEND_TOTAL,
        metric_names::WAL_APPEND_ERROR_TOTAL,
        metric_names::WAL_FENCED_REJECTED_TOTAL,
        metric_names::START_DB_TOTAL,
        metric_names::ADMISSION_DENIED_TOTAL,
        metric_names::SESSION_OPEN_TOTAL,
        metric_names::TRANSACTION_COMMIT_TOTAL,
        metric_names::TRANSACTION_ROLLBACK_TOTAL,
        metric_names::TRANSACTION_LOST_TOTAL,
        metric_names::SNAPSHOT_TOTAL,
    ];

    #[test]
    fn metric_names_are_non_empty_and_unique() {
        assert_eq!(ALL_METRIC_NAMES.len(), 24, "指标数量与架构 §14 契约一致");
        let unique: HashSet<&&str> = ALL_METRIC_NAMES.iter().collect();
        assert_eq!(unique.len(), ALL_METRIC_NAMES.len(), "指标名不得重复");
        for name in ALL_METRIC_NAMES {
            assert!(!name.trim().is_empty(), "指标名不得为空");
            assert!(
                name.chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()),
                "指标名 {name} 必须是小写 snake_case"
            );
        }
    }

    #[test]
    fn label_names_are_non_empty() {
        for label in [
            label_names::METHOD,
            label_names::ROUTE,
            label_names::STATUS,
            label_names::KIND,
            label_names::OUTCOME,
            label_names::REASON,
            label_names::DB_ID,
            label_names::WORKER_ID,
        ] {
            assert!(!label.trim().is_empty());
        }
    }

    /// 指标名与标签必须按契约渲染成 Prometheus 文本格式（Grafana / 告警依赖这些字符串）。
    ///
    /// 用与 `init` 相同的 recorder 配置（微秒直方图桶）验证，不触碰全局状态。
    #[test]
    fn metrics_render_with_expected_names_and_labels() {
        let recorder = crate::prometheus_builder().unwrap().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            record_query_latency_micros("query", 1_200);
            record_wal_append();
            record_worker_saturation("worker-1", 0.5);
            record_db_process_cpu("db-1", 1.5);
        });

        let rendered = handle.render();
        assert!(
            rendered.contains("query_latency_micros_bucket"),
            "延迟指标必须导出为直方图: {rendered}"
        );
        assert!(rendered.contains(r#"kind="query""#), "缺少标签: {rendered}");
        assert!(
            rendered.contains("wal_append_total"),
            "缺少计数器: {rendered}"
        );
        assert!(
            rendered.contains(r#"worker_id="worker-1""#),
            "缺少 worker 标签: {rendered}"
        );
        assert!(
            rendered.contains(r#"db_id="db-1""#),
            "缺少 db 标签: {rendered}"
        );
    }

    /// 未安装 recorder 时，所有记录入口都必须是空操作（不得 panic）。
    ///
    /// 用 noop recorder 固定“无 recorder”语义：全局 recorder 是否已被 init 安装
    /// 会因测试执行顺序而不同，这里不受其影响。
    #[test]
    fn recording_without_recorder_is_noop() {
        metrics::with_local_recorder(&metrics::NoopRecorder, || {
            record_request_latency_micros("POST", "/data/v1/databases/{db_id}/query", 200, 1_500);
            record_query_latency_micros("query", 1_200);
            record_db_process_cpu("db-1", 1.5);
            record_db_process_memory_mib("db-1", 256.0);
            record_worker_saturation("worker-1", 0.82);
            record_cold_start_micros("ok", 12_000);
            record_db_process_crash("oom");
            record_slow_query("query");
            record_route_cache_hit();
            record_route_cache_miss();
            record_route_cache_invalidations(2);
            record_route_cache_stale_detected(1);
            record_route_refresh_micros(3_000);
            record_wal_append_latency_micros(900);
            record_wal_append();
            record_wal_append_error("timeout");
            record_wal_fenced_rejected("append", "stale_epoch");
            record_start_db("ok");
            record_admission_denied("worker_full");
            record_session_open();
            record_transaction_commit();
            record_transaction_rollback();
            record_transaction_lost();
            record_snapshot("create");
        });
    }
}
