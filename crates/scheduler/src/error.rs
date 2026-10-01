//! Scheduler 错误类型与 [`domain::error::ErrorCode`] 映射。

use domain::error::{ErrorCode, PlatformError};
use serde_json::json;

/// Placement 失败的原因。所有变体都能映射到 proto [`ErrorCode`]。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScheduleError {
    /// 请求本身非法（例如零预算、affinity 与 anti-affinity 指向同一个 Worker）。
    ///
    /// 这类错误是调用方 bug，重试不会成功。
    #[error("placement 请求非法：{reason}")]
    InvalidRequest {
        /// 具体原因。
        reason: String,
    },

    /// 结构性筛选后没有任何候选 Worker（状态、排除列表、region/zone、anti-affinity 全部拒绝）。
    #[error("没有可参与 placement 的 Worker：{reason}")]
    NoEligibleWorker {
        /// 各候选被拒的简要原因。
        reason: String,
    },

    /// 存在候选，但全部被准入判定拒绝（资源不足 / 越过 80% 水位 / DB 数达 hard limit / Emergency）。
    #[error("所有候选 Worker 都被准入拒绝：{reason}")]
    AdmissionDenied {
        /// 各候选被拒的简要原因。
        reason: String,
    },

    /// 集群无法在放置后继续保留要求的 failover reserve。
    ///
    /// 这是**集群级**约束：单机装得下不代表能放 —— 放下去之后若集群再无余力承接
    /// Worker 失效，就必须先把 DB 放到别处（或拒绝）。
    #[error(
        "failover reserve 不足：需要 cpu={required_cpu_milli} memory={required_memory_mib}，\
         放置后最多可用 cpu={available_cpu_milli} memory={available_memory_mib}（{reason}）"
    )]
    InsufficientFailoverReserve {
        /// 请求要求保留的 CPU（milli-core）。
        required_cpu_milli: u64,
        /// 请求要求保留的内存（MiB）。
        required_memory_mib: u64,
        /// 最好的候选放置后集群仍可用的 failover CPU（milli-core）。
        available_cpu_milli: u64,
        /// 最好的候选放置后集群仍可用的 failover 内存（MiB）。
        available_memory_mib: u64,
        /// 补充说明（候选规模等）。
        reason: String,
    },
}

impl ScheduleError {
    /// 对应的平台错误码（proto `platform.common.v1.ErrorCode`，字符串与 HTTP 错误体一致）。
    #[must_use]
    pub fn code(&self) -> ErrorCode {
        match self {
            ScheduleError::InvalidRequest { .. } => ErrorCode::InvalidArgument,
            // 没有可用的 Worker：等价于「Worker 不可用」，调用方可以稍后重试 / 触发扩容。
            ScheduleError::NoEligibleWorker { .. } => ErrorCode::WorkerUnavailable,
            ScheduleError::AdmissionDenied { .. } => ErrorCode::AdmissionDenied,
            // 容量不足属于资源耗尽，不应被当作「Worker 挂了」重试到同一个 Worker 上。
            ScheduleError::InsufficientFailoverReserve { .. } => ErrorCode::ResourceExhausted,
        }
    }

    /// 转为平台结构化错误体。
    #[must_use]
    pub fn to_platform_error(&self) -> PlatformError {
        let mut error = PlatformError::new(self.code(), self.to_string());
        // reserve 相关错误把结构化数字放进 detail，便于 Control Plane 直接展示与告警。
        if let ScheduleError::InsufficientFailoverReserve {
            required_cpu_milli,
            required_memory_mib,
            available_cpu_milli,
            available_memory_mib,
            ..
        } = self
        {
            error.detail = Some(json!({
                "required_cpu_milli": required_cpu_milli,
                "required_memory_mib": required_memory_mib,
                "available_cpu_milli": available_cpu_milli,
                "available_memory_mib": available_memory_mib,
            }));
        }
        error
    }

    pub(crate) fn invalid_request(reason: impl Into<String>) -> Self {
        ScheduleError::InvalidRequest {
            reason: reason.into(),
        }
    }

    pub(crate) fn no_eligible_worker(reason: impl Into<String>) -> Self {
        ScheduleError::NoEligibleWorker {
            reason: reason.into(),
        }
    }

    pub(crate) fn admission_denied(reason: impl Into<String>) -> Self {
        ScheduleError::AdmissionDenied {
            reason: reason.into(),
        }
    }
}

impl From<ScheduleError> for PlatformError {
    fn from(value: ScheduleError) -> Self {
        value.to_platform_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_variant_maps_to_a_distinct_error_code() {
        assert_eq!(
            ScheduleError::invalid_request("x").code(),
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            ScheduleError::no_eligible_worker("x").code(),
            ErrorCode::WorkerUnavailable
        );
        assert_eq!(
            ScheduleError::admission_denied("x").code(),
            ErrorCode::AdmissionDenied
        );
        assert_eq!(
            ScheduleError::InsufficientFailoverReserve {
                required_cpu_milli: 1,
                required_memory_mib: 2,
                available_cpu_milli: 0,
                available_memory_mib: 0,
                reason: String::new(),
            }
            .code(),
            ErrorCode::ResourceExhausted
        );
    }

    #[test]
    fn reserve_error_carries_structured_detail() {
        let error = ScheduleError::InsufficientFailoverReserve {
            required_cpu_milli: 2000,
            required_memory_mib: 4096,
            available_cpu_milli: 1500,
            available_memory_mib: 2048,
            reason: "只有 2 个候选".to_string(),
        };
        let platform = error.to_platform_error();
        assert_eq!(platform.code, ErrorCode::ResourceExhausted);
        assert_eq!(
            platform
                .detail
                .as_ref()
                .and_then(|d| d["required_cpu_milli"].as_u64()),
            Some(2000)
        );
        assert!(platform.message.contains("2000"));
        // 错误码字符串与 proto 契约一致（供 HTTP 错误体直接透出）
        assert_eq!(platform.code.as_str(), "RESOURCE_EXHAUSTED");
    }
}
