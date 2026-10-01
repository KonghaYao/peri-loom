//! domain 领域类型 <-> proto 生成类型 的双向转换。
//!
//! 这里是跨服务契约的唯一收口点：业务代码只碰 domain 类型，wire 上只跑 proto 类型，
//! 两侧的字段增删、枚举数值变化都必须在本文档内一次性处理完。
//!
//! 两条硬约束：
//! 1. **不 panic**：proto 来自网络，任何字段都可能缺失或越界；未知枚举值一律落到安全默认
//!    （例如未知 [`LifecycleState`] -> `Cold`，未知 [`WorkerState`] -> `Unavailable`）。
//! 2. **无损**：合法输入往返转换后必须与原值一致（`NULL` / 空字符串 / 空 blob 三者互不混淆）。

use std::time::Instant;

use domain::error::{ErrorCode, PlatformError};
use domain::lifecycle::{LifecycleState, WorkerState};
use domain::resources::{ResourceBudget, WorkerResourceUsage};
use domain::time::{deadline_from_unix_ms, now_unix_ms};
use domain::value::{ColumnMeta, ResultSet, SqlValue};

use crate::common;
use crate::data;

// ---------------------------------------------------------------- 错误码

/// domain 错误码 -> proto 错误码（数值即对外 HTTP 错误体中的 code）。
pub fn error_code_to_proto(code: ErrorCode) -> common::ErrorCode {
    let raw = code.to_proto_i32();
    // domain 里存在 proto 未定义的错误码时说明两侧枚举脱钩，暴露为内部错误而不是静默降级
    common::ErrorCode::try_from(raw).unwrap_or(common::ErrorCode::InternalError)
}

/// proto 错误码 -> domain 错误码；未知/未指定一律落到 `InternalError`（domain 侧约定）。
pub fn error_code_from_proto(raw: i32) -> ErrorCode {
    ErrorCode::from_proto_i32(raw)
}

/// proto 错误码 -> domain 错误码。
pub fn error_code_from_proto_enum(code: common::ErrorCode) -> ErrorCode {
    error_code_from_proto(code as i32)
}

/// domain 结构化错误 -> proto 结构化错误。
///
/// `retryable` 按本地取值原样写出（含生产者用 [`PlatformError::with_retryable`] 做的显式覆盖），
/// 仅供对端排障与版本漂移比对；读取方会按**自己的错误码语义**重新推导该标志
/// （见 [`platform_error_from_proto`]），因此不要用它跨进程传递重试策略 ——
/// 需要「跨进程仍可重试」的语义时，应当选一个本地语义就可重试的错误码。
pub fn platform_error_to_proto(err: &PlatformError) -> common::PlatformError {
    common::PlatformError {
        code: error_code_to_proto(err.code) as i32,
        message: err.message.clone(),
        retryable: err.retryable,
        request_id: err.request_id.clone().unwrap_or_default(),
        // detail 只用于诊断，序列化失败不应让错误转换本身失败
        detail_json: err
            .detail
            .as_ref()
            .and_then(|detail| serde_json::to_string(detail).ok())
            .unwrap_or_default(),
        route_retry_count: err.route_retry_count,
    }
}

/// proto 结构化错误 -> domain 结构化错误。
///
/// **`retryable` 以本地错误码语义为准，不信任 wire 上的取值**：反序列化方拥有一份
/// 与 proto 同步演进的错误码语义表（[`ErrorCode::retryable`]），它才是权威。
/// 对端可能是旧版本、或被中间层改写，一旦把 `WAL_NOT_DURABLE` 这类「本次未确认 durable
/// 但可能已经落地」的错误标成可重试，调用方就会重放**整个写事务**，得到
/// 「报告失败但实际生效」+「重试再生效」的双写。
/// 两者不一致时按本地取值，并记一条 debug 便于定位版本漂移。
pub fn platform_error_from_proto(err: &common::PlatformError) -> PlatformError {
    let code = error_code_from_proto(err.code);
    let retryable = code.retryable();
    if err.retryable != retryable {
        // 只记 code / 两个布尔值：错误消息可能含敏感内容，不进日志
        tracing::debug!(
            code = code.as_str(),
            wire_retryable = err.retryable,
            local_retryable = retryable,
            "wire 上的 retryable 与本地错误码语义不一致，按本地语义处理"
        );
    }
    PlatformError {
        code,
        message: err.message.clone(),
        retryable,
        request_id: none_if_empty(&err.request_id),
        detail: parse_detail(&err.detail_json),
        route_retry_count: err.route_retry_count,
    }
}

impl From<&PlatformError> for common::PlatformError {
    fn from(err: &PlatformError) -> Self {
        platform_error_to_proto(err)
    }
}

impl From<PlatformError> for common::PlatformError {
    fn from(err: PlatformError) -> Self {
        platform_error_to_proto(&err)
    }
}

impl From<common::PlatformError> for PlatformError {
    fn from(err: common::PlatformError) -> Self {
        platform_error_from_proto(&err)
    }
}

/// framing 层错误 -> 平台结构化错误，便于 Dispatcher 直接把编解码失败回报给 Server。
impl From<crate::framing::FramingError> for PlatformError {
    fn from(err: crate::framing::FramingError) -> Self {
        // retryable 交给错误码语义决定（见 FramingError::error_code）
        PlatformError::new(err.error_code(), err.to_string())
    }
}

/// 空字符串在 domain 侧一律表示 `None`（proto3 没有 optional 标量）。
fn none_if_empty(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// detail_json 不是合法 JSON 时按「无 detail」处理。
fn parse_detail(detail_json: &str) -> Option<serde_json::Value> {
    if detail_json.is_empty() {
        return None;
    }
    serde_json::from_str(detail_json).ok()
}

// ---------------------------------------------------------------- 生命周期

/// domain 生命周期状态 -> proto 枚举。
pub fn lifecycle_state_to_proto(state: LifecycleState) -> common::LifecycleState {
    common::LifecycleState::try_from(state.to_proto_i32())
        .unwrap_or(common::LifecycleState::Unspecified)
}

/// proto 枚举 -> domain 生命周期状态；未指定/未知一律 `Cold`（最保守的「未占用资源」语义）。
pub fn lifecycle_state_from_proto(raw: i32) -> LifecycleState {
    // 保守默认由 domain 定义（未知 -> Cold），这里只做 wire 数值的入口收口
    LifecycleState::from_proto_i32(raw)
}

/// domain Worker 状态 -> proto 枚举。
pub fn worker_state_to_proto(state: WorkerState) -> common::WorkerState {
    common::WorkerState::try_from(state.to_proto_i32()).unwrap_or(common::WorkerState::Unspecified)
}

/// proto 枚举 -> domain Worker 状态；未指定/未知一律 `Unavailable`（不能路由到状态不明的 Worker）。
pub fn worker_state_from_proto(raw: i32) -> WorkerState {
    WorkerState::from_proto_i32(raw)
}

// ---------------------------------------------------------------- 资源画像

impl From<ResourceBudget> for common::ResourceBudget {
    fn from(budget: ResourceBudget) -> Self {
        common::ResourceBudget {
            cpu_milli: budget.cpu_milli,
            memory_mib: budget.memory_mib,
            file_descriptors: budget.file_descriptors,
            disk_mib: budget.disk_mib,
            process_slots: budget.process_slots,
            iops: budget.iops,
        }
    }
}

impl From<common::ResourceBudget> for ResourceBudget {
    fn from(budget: common::ResourceBudget) -> Self {
        ResourceBudget {
            cpu_milli: budget.cpu_milli,
            memory_mib: budget.memory_mib,
            file_descriptors: budget.file_descriptors,
            disk_mib: budget.disk_mib,
            process_slots: budget.process_slots,
            iops: budget.iops,
        }
    }
}

// 注意：两侧字段名刻意不同（domain 用 `cpu_milli_used`，proto 用 `cpu_used_milli`），
// 逐字段手写映射就是为了在这里把顺序钉死，杜绝「宏式自动映射」造成的静默错位。
impl From<WorkerResourceUsage> for common::WorkerResourceUsage {
    fn from(usage: WorkerResourceUsage) -> Self {
        common::WorkerResourceUsage {
            cpu_used_milli: usage.cpu_milli_used,
            cpu_total_milli: usage.cpu_milli_total,
            memory_used_mib: usage.memory_mib_used,
            memory_total_mib: usage.memory_mib_total,
            fd_used: usage.fd_used,
            fd_total: usage.fd_total,
            disk_used_mib: usage.disk_mib_used,
            disk_total_mib: usage.disk_mib_total,
            iops_used: usage.iops_used,
            iops_total: usage.iops_total,
            db_process_count: usage.db_process_count,
            db_process_limit: usage.db_process_limit,
        }
    }
}

impl From<common::WorkerResourceUsage> for WorkerResourceUsage {
    fn from(usage: common::WorkerResourceUsage) -> Self {
        WorkerResourceUsage {
            cpu_milli_used: usage.cpu_used_milli,
            cpu_milli_total: usage.cpu_total_milli,
            memory_mib_used: usage.memory_used_mib,
            memory_mib_total: usage.memory_total_mib,
            fd_used: usage.fd_used,
            fd_total: usage.fd_total,
            disk_mib_used: usage.disk_used_mib,
            disk_mib_total: usage.disk_total_mib,
            iops_used: usage.iops_used,
            iops_total: usage.iops_total,
            db_process_count: usage.db_process_count,
            db_process_limit: usage.db_process_limit,
        }
    }
}

// ---------------------------------------------------------------- 值 / 结果集

impl From<SqlValue> for data::Value {
    fn from(value: SqlValue) -> Self {
        use data::value::Kind;
        let kind = match value {
            // NULL 必须显式用 null_value 变体表达，不能靠「缺省 kind」隐式表达
            SqlValue::Null => Kind::NullValue(data::NullValue::Null as i32),
            SqlValue::Integer(v) => Kind::Integer(v),
            SqlValue::Real(v) => Kind::Real(v),
            SqlValue::Text(v) => Kind::Text(v),
            SqlValue::Blob(v) => Kind::Blob(v),
        };
        data::Value { kind: Some(kind) }
    }
}

impl From<data::Value> for SqlValue {
    fn from(value: data::Value) -> Self {
        match value.kind {
            // 未设置 kind 的 Value 语义上就是 NULL
            None => SqlValue::Null,
            Some(data::value::Kind::NullValue(_)) => SqlValue::Null,
            Some(data::value::Kind::Integer(v)) => SqlValue::Integer(v),
            Some(data::value::Kind::Real(v)) => SqlValue::Real(v),
            Some(data::value::Kind::Text(v)) => SqlValue::Text(v),
            Some(data::value::Kind::Blob(v)) => SqlValue::Blob(v),
        }
    }
}

impl From<ColumnMeta> for data::ColumnMeta {
    fn from(meta: ColumnMeta) -> Self {
        data::ColumnMeta {
            name: meta.name,
            type_name: meta.type_name,
            nullable: meta.nullable,
        }
    }
}

impl From<data::ColumnMeta> for ColumnMeta {
    fn from(meta: data::ColumnMeta) -> Self {
        ColumnMeta {
            name: meta.name,
            type_name: meta.type_name,
            nullable: meta.nullable,
        }
    }
}

// domain 侧一行就是 `Vec<SqlValue>`（没有单独的 Row 类型），所以行转换用函数而不是 From，
// 避免给 `Vec<SqlValue>` 挂上含义不明的隐式转换。

/// domain 行 -> proto `Row`。
pub fn row_to_proto(values: Vec<SqlValue>) -> data::Row {
    data::Row {
        values: values.into_iter().map(data::Value::from).collect(),
    }
}

/// proto `Row` -> domain 行。
pub fn row_from_proto(row: data::Row) -> Vec<SqlValue> {
    row.values.into_iter().map(SqlValue::from).collect()
}

impl From<ResultSet> for data::ResultSet {
    fn from(rs: ResultSet) -> Self {
        data::ResultSet {
            columns: rs.columns.into_iter().map(data::ColumnMeta::from).collect(),
            rows: rs.rows.into_iter().map(row_to_proto).collect(),
            affected_rows: rs.affected_rows,
            truncated: rs.truncated,
        }
    }
}

impl From<data::ResultSet> for ResultSet {
    fn from(rs: data::ResultSet) -> Self {
        ResultSet {
            columns: rs.columns.into_iter().map(ColumnMeta::from).collect(),
            rows: rs.rows.into_iter().map(row_from_proto).collect(),
            affected_rows: rs.affected_rows,
            truncated: rs.truncated,
        }
    }
}

// ---------------------------------------------------------------- 请求上下文

/// 贯穿 Server -> Worker -> DB Process 的最小请求上下文。
///
/// 与 `platform.common.v1.RequestContext` 一一对应，但 deadline 用相对时间 [`Instant`]
/// 承载：跨进程传递必须是绝对时间戳（时钟不同源），进程内部等待则必须用单调时钟
/// （避免墙上时钟跳变导致超时判定错乱），转换只在本结构边界发生。
///
/// 空字符串表示「无」（与 proto3 标量语义一致），避免 `Option<String>` 在两侧反复包装。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestContext {
    /// 请求唯一标识，用于幂等、取消与全链路追踪。
    pub request_id: String,
    /// 分布式追踪 ID。
    pub trace_id: String,
    /// 租户。
    pub tenant_id: String,
    /// 目标数据库。
    pub database_id: String,
    /// Owner Epoch：Storage 层据此拒绝旧 Owner 写入（fencing）。
    pub owner_epoch: u64,
    /// 请求截止时间（进程内单调时钟）；`None` 表示不限时。
    pub deadline: Option<Instant>,
    /// 显式会话场景必填，Stateless 场景为空。
    pub session_id: String,
    /// 显式事务场景必填。
    pub transaction_id: String,
    /// Server 指定、Worker 校验的目标 Worker。
    pub worker_id: String,
    /// 幂等键（仅 Management 副作用请求使用）。
    pub idempotency_key: String,
}

/// 相对 deadline 转 wire 上的绝对 Unix 毫秒。
///
/// `None` -> 0（proto 约定 0 表示无 deadline）；**已过期**的 deadline 返回「当前时间」
/// 而不是 0，否则下游会把「已超时」误读成「不限时」。
pub fn deadline_ms_from(deadline: Option<Instant>) -> u64 {
    let Some(deadline) = deadline else {
        return 0;
    };
    let now_ms = unix_ms_u64();
    let remaining_ms = u64::try_from(
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    now_ms.saturating_add(remaining_ms)
}

/// wire 上的绝对 Unix 毫秒 -> 进程内相对 deadline（0 或已过期值由 domain 侧语义决定）。
pub fn deadline_from_ms(deadline_unix_ms: u64) -> Option<Instant> {
    deadline_from_unix_ms(deadline_unix_ms)
}

/// 当前 Unix 毫秒（非负）。
fn unix_ms_u64() -> u64 {
    // domain 的 now_unix_ms 在 int64 域；负值（时钟异常）夹到 0 而不是回绕成巨大值
    now_unix_ms().max(0) as u64
}

impl From<&RequestContext> for common::RequestContext {
    fn from(ctx: &RequestContext) -> Self {
        common::RequestContext {
            request_id: ctx.request_id.clone(),
            trace_id: ctx.trace_id.clone(),
            tenant_id: ctx.tenant_id.clone(),
            database_id: ctx.database_id.clone(),
            owner_epoch: ctx.owner_epoch,
            deadline_unix_ms: deadline_ms_from(ctx.deadline),
            session_id: ctx.session_id.clone(),
            transaction_id: ctx.transaction_id.clone(),
            worker_id: ctx.worker_id.clone(),
            idempotency_key: ctx.idempotency_key.clone(),
        }
    }
}

impl From<RequestContext> for common::RequestContext {
    fn from(ctx: RequestContext) -> Self {
        common::RequestContext::from(&ctx)
    }
}

impl From<common::RequestContext> for RequestContext {
    fn from(ctx: common::RequestContext) -> Self {
        RequestContext {
            request_id: ctx.request_id,
            trace_id: ctx.trace_id,
            tenant_id: ctx.tenant_id,
            database_id: ctx.database_id,
            owner_epoch: ctx.owner_epoch,
            deadline: deadline_from_ms(ctx.deadline_unix_ms),
            session_id: ctx.session_id,
            transaction_id: ctx.transaction_id,
            worker_id: ctx.worker_id,
            idempotency_key: ctx.idempotency_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn error_code_roundtrip_over_all_variants() {
        // 直接遍历 domain 的 ALL，保证新增错误码时这条契约测试立刻覆盖到
        assert!(!ErrorCode::ALL.is_empty());
        for &code in ErrorCode::ALL {
            let raw = code.to_proto_i32();
            // 1) i32 往返
            assert_eq!(
                ErrorCode::from_proto_i32(raw),
                code,
                "{} 的 i32 往返失败",
                code.as_str()
            );

            // 2) 转换函数往返
            let proto = error_code_to_proto(code);
            assert_eq!(proto as i32, raw, "{} 的 proto 数值不一致", code.as_str());
            assert_eq!(error_code_from_proto(raw), code);
            assert_eq!(error_code_from_proto_enum(proto), code);

            // 3) 对外错误字符串必须与 proto 枚举名逐字一致（HTTP 错误体契约）
            assert_eq!(
                code.as_str(),
                proto.as_str_name(),
                "{} 与 proto 名称不一致",
                code.as_str()
            );
        }
    }

    #[test]
    fn unknown_error_code_falls_back_to_internal_error() {
        // proto 上未定义 / 空洞数值都不能 panic，也不能伪装成成功
        for raw in [i32::MIN, -1, 2, 99, 606, 700, i32::MAX] {
            assert_eq!(
                error_code_from_proto(raw),
                ErrorCode::InternalError,
                "raw={raw}"
            );
        }

        // 已定义数值（含 UNSPECIFIED 与 OK）正常解析
        assert_eq!(error_code_from_proto(0), ErrorCode::ErrorCodeUnspecified);
        assert_eq!(error_code_from_proto(1), ErrorCode::Ok);
        assert_eq!(error_code_from_proto(600), ErrorCode::InternalError);
    }

    #[test]
    fn platform_error_roundtrip_keeps_detail_and_request_id() {
        // retryable 取错误码的规范值（读回时以本地语义为准，见 FIX-4 的测试）
        let err = PlatformError::new(ErrorCode::EpochMismatch, "epoch 过期")
            .with_request_id("req-1")
            .with_detail(serde_json::json!({"epoch_expected": 834, "epoch_actual": 835}))
            .with_route_retry_count(2);

        let proto = platform_error_to_proto(&err);
        assert_eq!(proto.code, common::ErrorCode::EpochMismatch as i32);
        // detail 以 JSON 字符串承载；键序无关，只要内容一致即可
        let detail: serde_json::Value =
            serde_json::from_str(&proto.detail_json).expect("detail_json 必须是合法 JSON");
        assert_eq!(
            detail,
            serde_json::json!({"epoch_expected": 834, "epoch_actual": 835})
        );

        let back = platform_error_from_proto(&proto);
        assert_eq!(back, err);
        assert_eq!(back.retryable, ErrorCode::EpochMismatch.retryable());
    }

    /// FIX-4：proto -> domain 时，`retryable` 以**本地错误码语义**为准。
    #[test]
    fn platform_error_from_proto_uses_local_retryable_semantics() {
        // wire 声称可重试，但本地语义说 WAL_NOT_DURABLE 不可重试：
        // 若采信 wire，调用方会重放整个写事务 -> 「报告失败但实际生效 + 重试再生效」的双写
        let wire = common::PlatformError {
            code: common::ErrorCode::WalNotDurable as i32,
            message: "append timeout".to_string(),
            retryable: true,
            request_id: String::new(),
            detail_json: String::new(),
            route_retry_count: 0,
        };
        let err = platform_error_from_proto(&wire);
        assert_eq!(err.code, ErrorCode::WalNotDurable);
        assert!(
            !err.retryable,
            "必须以本地语义为准：WAL_NOT_DURABLE 不得被 wire 洗成可重试"
        );
        assert!(!err.route_retry_allowed());

        // 反方向同理：wire 声称不可重试，本地语义说「换 leader 端点可以重试」
        let wire = common::PlatformError {
            code: common::ErrorCode::WalNotLeader as i32,
            message: "not leader".to_string(),
            retryable: false,
            request_id: String::new(),
            detail_json: String::new(),
            route_retry_count: 0,
        };
        let err = platform_error_from_proto(&wire);
        assert_eq!(err.code, ErrorCode::WalNotLeader);
        assert!(err.retryable, "本地语义才是权威，wire 的 false 不生效");

        // 全部错误码：无论 wire 写什么，读回值都等于本地 retryable()
        for &code in ErrorCode::ALL {
            for wire_value in [true, false] {
                let wire = common::PlatformError {
                    code: error_code_to_proto(code) as i32,
                    message: String::new(),
                    retryable: wire_value,
                    request_id: String::new(),
                    detail_json: String::new(),
                    route_retry_count: 0,
                };
                assert_eq!(
                    platform_error_from_proto(&wire).retryable,
                    code.retryable(),
                    "{} 的 retryable 必须以本地语义为准（wire={wire_value}）",
                    code.as_str()
                );
            }
        }
    }

    /// 未知错误码（跨版本新增 / 空洞数值）落到 INTERNAL_ERROR，retryable 同样按本地语义推导。
    #[test]
    fn unknown_wire_code_uses_local_internal_semantics() {
        let wire = common::PlatformError {
            code: 9_999,
            message: "from the future".to_string(),
            retryable: false,
            request_id: String::new(),
            detail_json: String::new(),
            route_retry_count: 0,
        };
        let err = platform_error_from_proto(&wire);
        assert_eq!(err.code, ErrorCode::InternalError);
        assert_eq!(err.retryable, ErrorCode::InternalError.retryable());
    }

    #[test]
    fn platform_error_without_optional_fields_roundtrips() {
        let err = PlatformError {
            code: ErrorCode::InternalError,
            message: "boom".to_string(),
            retryable: true,
            request_id: None,
            detail: None,
            route_retry_count: 0,
        };
        let proto = common::PlatformError::from(&err);
        assert!(proto.request_id.is_empty());
        assert!(proto.detail_json.is_empty());
        assert_eq!(PlatformError::from(proto), err);
    }

    #[test]
    fn platform_error_tolerates_broken_detail_json() {
        let proto = common::PlatformError {
            code: common::ErrorCode::SqlError as i32,
            message: "syntax".to_string(),
            retryable: false,
            request_id: String::new(),
            detail_json: "{不是 JSON".to_string(),
            route_retry_count: 0,
        };
        let err = platform_error_from_proto(&proto);
        assert_eq!(err.detail, None);
        assert_eq!(err.request_id, None);
        assert_eq!(err.code, ErrorCode::SqlError);
    }

    #[test]
    fn resource_budget_roundtrip() {
        let budget = ResourceBudget {
            cpu_milli: 1500,
            memory_mib: 4096,
            file_descriptors: 1024,
            disk_mib: 20480,
            process_slots: 32,
            iops: 5000,
        };
        let proto: common::ResourceBudget = budget.into();
        assert_eq!(proto.cpu_milli, 1500);
        assert_eq!(proto.iops, 5000);
        assert_eq!(ResourceBudget::from(proto), budget);
    }

    #[test]
    fn worker_resource_usage_roundtrip() {
        // 用互不相同的数值，确保 used/total 字段名错位能被立刻发现
        let usage = WorkerResourceUsage {
            cpu_milli_used: 6001,
            cpu_milli_total: 8002,
            memory_mib_used: 12003,
            memory_mib_total: 16384,
            fd_used: 300,
            fd_total: 4096,
            disk_mib_used: 5004,
            disk_mib_total: 100005,
            iops_used: 1006,
            iops_total: 1000,
            db_process_count: 12,
            db_process_limit: 64,
        };
        let proto: common::WorkerResourceUsage = usage.into();
        assert_eq!(proto.cpu_used_milli, 6001);
        assert_eq!(proto.cpu_total_milli, 8002);
        assert_eq!(proto.memory_used_mib, 12003);
        assert_eq!(proto.memory_total_mib, 16384);
        assert_eq!(proto.disk_used_mib, 5004);
        assert_eq!(proto.disk_total_mib, 100005);
        assert_eq!(proto.iops_used, 1006);
        assert_eq!(proto.db_process_count, 12);
        assert_eq!(WorkerResourceUsage::from(proto), usage);
    }

    #[test]
    fn lifecycle_state_roundtrip_and_unknown_default() {
        for state in [
            LifecycleState::Cold,
            LifecycleState::Starting,
            LifecycleState::Warm,
            LifecycleState::Hot,
            LifecycleState::Draining,
            LifecycleState::Stopping,
            LifecycleState::Failed,
        ] {
            let proto = lifecycle_state_to_proto(state);
            assert_eq!(lifecycle_state_from_proto(proto as i32), state);
        }

        // 未知 / 未指定 -> Cold（最保守：不认为该 DB 正在占用资源）
        assert_eq!(
            lifecycle_state_from_proto(common::LifecycleState::Unspecified as i32),
            LifecycleState::Cold
        );
        assert_eq!(lifecycle_state_from_proto(9999), LifecycleState::Cold);
    }

    #[test]
    fn worker_state_roundtrip_and_unknown_default() {
        for state in [
            WorkerState::Active,
            WorkerState::Suspect,
            WorkerState::Draining,
            WorkerState::Empty,
            WorkerState::Unavailable,
        ] {
            let proto = worker_state_to_proto(state);
            assert_eq!(worker_state_from_proto(proto as i32), state);
        }

        // 未知 / 未指定 -> Unavailable（状态不明时绝不能路由过去）
        assert_eq!(
            worker_state_from_proto(common::WorkerState::Unspecified as i32),
            WorkerState::Unavailable
        );
        assert_eq!(worker_state_from_proto(-5), WorkerState::Unavailable);
    }

    #[test]
    fn sql_value_roundtrip_all_variants() {
        let values = vec![
            SqlValue::Integer(i64::MIN),
            SqlValue::Integer(0),
            SqlValue::Integer(i64::MAX),
            SqlValue::Real(-1.5),
            SqlValue::Text(String::new()),
            SqlValue::Text("文本".to_string()),
            SqlValue::Blob(Vec::new()),
            SqlValue::Blob(vec![0x00, 0xFF]),
        ];
        for value in values {
            let proto: data::Value = value.clone().into();
            assert_eq!(SqlValue::from(proto), value, "{value:?} 往返失败");
        }
    }

    #[test]
    fn null_empty_text_and_empty_blob_stay_distinct() {
        let null: data::Value = SqlValue::Null.into();
        let empty_text: data::Value = SqlValue::Text(String::new()).into();
        let empty_blob: data::Value = SqlValue::Blob(Vec::new()).into();

        assert_eq!(null.kind, Some(data::value::Kind::NullValue(0)));
        assert_eq!(
            empty_text.kind,
            Some(data::value::Kind::Text(String::new()))
        );
        assert_eq!(empty_blob.kind, Some(data::value::Kind::Blob(Vec::new())));

        assert_eq!(SqlValue::from(null), SqlValue::Null);
        assert_eq!(
            SqlValue::from(empty_text),
            SqlValue::Text(String::new()),
            "空字符串不能被当成 NULL"
        );
        assert_eq!(
            SqlValue::from(empty_blob),
            SqlValue::Blob(Vec::new()),
            "空 blob 不能被当成 NULL"
        );

        // 缺省 kind 的 Value（proto 上的空 message）按 NULL 解释
        assert_eq!(SqlValue::from(data::Value { kind: None }), SqlValue::Null);
    }

    #[test]
    fn null_real_is_not_silently_nan() {
        // Real 走 f64 直传，NULL 与 NaN 语义不同（NaN 是合法值）
        let nan: data::Value = SqlValue::Real(f64::NAN).into();
        match SqlValue::from(nan) {
            SqlValue::Real(v) => assert!(v.is_nan()),
            other => panic!("期望 Real，实际 {other:?}"),
        }
    }

    #[test]
    fn result_set_roundtrip() {
        let rs = ResultSet {
            columns: vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_name: "INTEGER".to_string(),
                    nullable: false,
                },
                ColumnMeta {
                    name: "note".to_string(),
                    type_name: "TEXT".to_string(),
                    nullable: true,
                },
            ],
            rows: vec![
                vec![SqlValue::Integer(1), SqlValue::Text("a".to_string())],
                vec![SqlValue::Integer(2), SqlValue::Null],
            ],
            affected_rows: 2,
            truncated: true,
        };

        let proto: data::ResultSet = rs.clone().into();
        assert_eq!(proto.rows.len(), 2);
        assert_eq!(proto.rows[1].values.len(), 2);
        assert!(proto.columns[1].nullable, "note 列必须保留 nullable 标记");
        assert_eq!(ResultSet::from(proto), rs);
    }

    #[test]
    fn row_roundtrip_keeps_null_and_empty_string() {
        let row = vec![SqlValue::Null, SqlValue::Text(String::new())];
        let proto = row_to_proto(row.clone());
        assert_eq!(proto.values.len(), 2);
        assert_eq!(row_from_proto(proto), row);
    }

    #[test]
    fn empty_result_set_roundtrip() {
        let rs = ResultSet {
            columns: vec![],
            rows: vec![],
            affected_rows: 0,
            truncated: false,
        };
        let proto: data::ResultSet = rs.clone().into();
        assert_eq!(ResultSet::from(proto), rs);
    }

    fn sample_context() -> RequestContext {
        RequestContext {
            request_id: "req-1".to_string(),
            trace_id: "trace-1".to_string(),
            tenant_id: "tenant-1".to_string(),
            database_id: "db-1".to_string(),
            owner_epoch: 834,
            deadline: None,
            session_id: "sess-1".to_string(),
            transaction_id: "txn-1".to_string(),
            worker_id: "worker-1".to_string(),
            idempotency_key: "idem-1".to_string(),
        }
    }

    #[test]
    fn request_context_roundtrip() {
        let ctx = sample_context();
        let proto: common::RequestContext = (&ctx).into();
        assert_eq!(proto.owner_epoch, 834);
        assert_eq!(proto.deadline_unix_ms, 0, "无 deadline 必须编码为 0");
        assert_eq!(proto.request_id, "req-1");

        let back = RequestContext::from(proto);
        assert_eq!(back, ctx);

        // 拥有所有权的转换与按引用转换等价
        assert_eq!(
            common::RequestContext::from(sample_context()),
            common::RequestContext::from(&ctx)
        );
    }

    #[test]
    fn request_context_deadline_roundtrip_is_lossless_enough() {
        let mut ctx = sample_context();
        ctx.deadline = Some(Instant::now() + Duration::from_secs(30));

        let proto: common::RequestContext = (&ctx).into();
        let now = unix_ms_u64();
        assert!(
            proto.deadline_unix_ms >= now + 29_000 && proto.deadline_unix_ms <= now + 30_100,
            "deadline_unix_ms={} now={now}",
            proto.deadline_unix_ms
        );

        // 回来仍是一个未来的 Instant
        let back = RequestContext::from(proto);
        let remaining = back
            .deadline
            .expect("proto 非 0 应还原为 Some")
            .saturating_duration_since(Instant::now());
        assert!(remaining <= Duration::from_secs(31));
    }

    #[test]
    fn expired_deadline_is_not_encoded_as_u64_zero() {
        // 已过期必须编码成「当前时间」而不是 0，否则下游会当成无限期
        let ms = deadline_ms_from(Some(Instant::now()));
        assert!(ms > 0, "已过期的 deadline 不能编码为 0");
        assert!(ms <= unix_ms_u64() + 1000);
        assert_eq!(deadline_ms_from(None), 0);
    }

    #[test]
    fn request_context_from_proto_zero_deadline_is_none() {
        let mut proto = common::RequestContext::from(&sample_context());
        proto.deadline_unix_ms = 0;
        assert_eq!(RequestContext::from(proto).deadline, None);
    }
}
