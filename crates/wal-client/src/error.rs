//! 失败分类、重试决策与 proto -> domain 错误码映射。
//!
//! 这里是 durability 契约的判定中心：**只有**本模块判定为「可重试」的失败才会换端点
//! 重试，其余一律立即返回给调用方。判定错误会直接破坏「Ok == quorum durable」这一
//! 承诺，所以分类表显式列出每个错误码的归属，不做通配兜底式猜测。

use domain::error::{ErrorCode, PlatformError};
use protocol::common;

/// fencing detail 中「服务端权威 epoch」的字段名（跨服务契约，两侧同名）。
///
/// 值 = 服务端**已记录**的 epoch（收到这个 epoch 的写者才是当前 Owner）。
pub(crate) const FENCING_CURRENT_EPOCH_KEY: &str = "current_epoch";

/// fencing detail 中「本次请求携带的 epoch」的字段名。
///
/// 与 [`FENCING_CURRENT_EPOCH_KEY`] 成对出现才有排障价值：只报其中一个无法判断
/// 「是客户端用了旧 epoch」还是「服务端丢了新 epoch」。
pub(crate) const FENCING_REQUESTED_EPOCH_KEY: &str = "requested_epoch";

/// 本轮失败的原因。
///
/// 与「错误码」分开建模的原因：同一个错误码（例如 `WAL_NOT_DURABLE`）在 append 路径上
/// 表示「本副本没提交成功、可以换 leader 再试」，在 `status` 路径上却只是普通失败；
/// 而「能否重试 / 换端点」才是重试循环真正要判断的东西。
#[derive(Debug, Clone)]
pub(crate) enum AttemptFailure {
    /// leader 相关拒绝：当前端点不是 leader（或不再是该 DB 的 owner）。
    ///
    /// 可换端点重试；耗尽后对外报 [`ErrorCode::WalNotLeader`]。
    NotLeader {
        /// 出错端点。
        endpoint: String,
        /// 服务端消息。
        message: String,
        /// 服务端给出的 leader 端点提示（可能为空）。
        hint: Option<String>,
    },
    /// 传输层失败：连接失败 / 超时 / 流中断 / gRPC 非 OK status。
    ///
    /// 可换端点重试（同一 append_id 幂等）；耗尽后对外报 [`ErrorCode::WalNotDurable`]。
    Transport {
        /// 出错端点。
        endpoint: String,
        /// 失败详情（仅诊断用，不含 secret）。
        message: String,
    },
    /// 服务端明确表示本批次**尚未 durable**（如 `WAL_NOT_DURABLE` / 背压 / 限流）。
    ///
    /// 可换端点重试；耗尽后原样回传服务端错误码（`WAL_NOT_DURABLE` 即
    /// [`ErrorCode::WalNotDurable`]）。
    NotDurable {
        endpoint: String,
        error: PlatformError,
    },
    /// 响应自相矛盾：声称成功但 `durable_lsn` 未覆盖本批次字节（协议违例）。
    ///
    /// 视作未 durable：可重试，耗尽后报 [`ErrorCode::WalNotDurable`]。
    Incomplete {
        /// 出错端点。
        endpoint: String,
        /// 详情。
        message: String,
    },
    /// fencing 拒绝（epoch 过期）：**终态，不得重试**。
    Fencing(PlatformError),
    /// 其他终态错误（参数 / 权限 / 数据不存在等）：原样回传。
    Terminal(PlatformError),
}

/// 是否属于 fencing 类错误码（epoch 过期 / 写入被拒）。
///
/// 这三个码都表示「本次请求的写者身份不被存储面认可」，重试同一个身份永远不会成功
/// （必须由上层重新获取 Owner 后换 epoch 再写），因此客户端必须短路，不能换端点重试。
pub(crate) fn is_fencing_code(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::EpochMismatch | ErrorCode::WalAppendRejected | ErrorCode::NotOwner
    )
}

/// 重试决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// 换端点重试。
    Retry,
    /// 立即返回错误。
    Terminal,
}

impl AttemptFailure {
    /// 传输层失败。
    pub(crate) fn transport(endpoint: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Transport {
            endpoint: endpoint.into(),
            message: message.into(),
        }
    }

    /// 服务端 status（非 in-band 错误）-> 传输层失败。
    ///
    /// gRPC status 只承载「RPC 本身失败」，不承载业务语义；`UNAVAILABLE` /
    /// `DEADLINE_EXCEEDED` / 连接层错误都可能是 leader 已切换，因此一律换端点重试。
    pub(crate) fn from_status(endpoint: impl Into<String>, status: &tonic::Status) -> Self {
        let endpoint = endpoint.into();
        // gRPC 不区分「服务端超时」与「客户端 deadline 到期」：tonic 服务端会按
        // grpc-timeout 主动取消请求并返回 CANCELLED + "Timeout expired"。这类失败
        // 在语义上就是「未在规定时间内确认 quorum durable」，消息必须显式体现超时，
        // 否则排障时会被误判成纯网络故障（验收要求错误语义正确率 100%）。
        let timed_out = status.code() == tonic::Code::DeadlineExceeded
            || status.message().contains("Timeout expired")
            || status.message().contains("deadline");
        let message = if timed_out {
            format!(
                "请求超时（gRPC status {}: {}）",
                status.code(),
                status.message()
            )
        } else {
            format!("gRPC status {}: {}", status.code(), status.message())
        };
        Self::Transport { endpoint, message }
    }

    /// 服务端 in-band 平台错误 -> 分类。
    ///
    /// `requested_epoch` 是本次请求携带的 epoch，用于 fencing detail 的
    /// [`FENCING_REQUESTED_EPOCH_KEY`]。
    pub(crate) fn from_server(
        endpoint: impl Into<String>,
        error: PlatformError,
        requested_epoch: Option<u64>,
    ) -> Self {
        let endpoint = endpoint.into();
        match error.code {
            // ---- leader 相关：换端点重试
            ErrorCode::WalNotLeader => {
                let hint = leader_hint(&error).filter(|hint| !hint.is_empty());
                Self::NotLeader {
                    endpoint,
                    message: error.message,
                    hint,
                }
            }
            // ---- fencing：epoch 已过期 / 本节点不再是该 DB 的 Owner，重试只会让旧
            // Owner 继续写，必须立即失败。
            //
            // NOT_OWNER 归入这里而不是「leader 相关可重试」：换端点重试携带的仍是同一个
            // 已失效的 owner_epoch，任何副本都会同样拒绝；把它当可重试只会把尝试额度耗光，
            // 再把「所有权已被别人接管」这个必须由上层处理的状态伪装成 WAL_NOT_LEADER。
            ErrorCode::EpochMismatch | ErrorCode::WalAppendRejected | ErrorCode::NotOwner => {
                Self::fencing_from_server(
                    endpoint,
                    error,
                    requested_epoch,
                    // 服务端把当前 epoch 写在 detail 里时优先采用
                    None,
                )
            }
            // ---- 未 durable / 暂时不可用：换端点重试（幂等键保证不会重复写）
            ErrorCode::WalNotDurable
            | ErrorCode::StorageUnavailable
            | ErrorCode::ResourceExhausted
            | ErrorCode::RateLimited
            | ErrorCode::QuotaExceeded
            | ErrorCode::DeadlineExceeded
            | ErrorCode::Cancelled
            | ErrorCode::WorkerUnavailable
            | ErrorCode::DatabaseNotReady
            | ErrorCode::WakeupTimeout
            | ErrorCode::InternalError
            | ErrorCode::ErrorCodeUnspecified => Self::NotDurable { endpoint, error },
            // ---- 其余（参数 / 权限 / 不存在 / 已存在 / 未实现 …）终态：重试没有意义
            _ => Self::Terminal(error),
        }
    }

    /// 构造 fencing 失败（`EPOCH_MISMATCH` / `NOT_OWNER` / `WAL_APPEND_REJECTED`），
    /// 带上 current/requested epoch。
    ///
    /// `current_epoch` 优先取调用方从响应体里读到的值（比 detail 更权威），
    /// 为 `None` 时回退到服务端 detail 中的提示值。
    pub(crate) fn fencing_from_server(
        endpoint: impl Into<String>,
        error: PlatformError,
        requested_epoch: Option<u64>,
        current_epoch: Option<u64>,
    ) -> Self {
        Self::Fencing(enrich_fencing(
            endpoint.into(),
            error,
            requested_epoch,
            current_epoch,
        ))
    }

    /// 判定本失败是否可重试。
    pub(crate) fn disposition(&self) -> Disposition {
        match self {
            Self::NotLeader { .. }
            | Self::Transport { .. }
            | Self::NotDurable { .. }
            | Self::Incomplete { .. } => Disposition::Retry,
            Self::Fencing(_) | Self::Terminal(_) => Disposition::Terminal,
        }
    }

    /// leader 提示端点（若服务端给出）。
    pub(crate) fn hint(&self) -> Option<&str> {
        match self {
            Self::NotLeader { hint, .. } => hint.as_deref(),
            _ => None,
        }
    }

    /// 是否为 leader 相关失败（决定耗尽后的对外错误码）。
    pub(crate) fn is_leader_related(&self) -> bool {
        matches!(self, Self::NotLeader { .. })
    }

    /// 出错端点（终态错误没有端点语义时返回空串）。
    pub(crate) fn endpoint(&self) -> &str {
        match self {
            Self::NotLeader { endpoint, .. }
            | Self::Transport { endpoint, .. }
            | Self::NotDurable { endpoint, .. }
            | Self::Incomplete { endpoint, .. } => endpoint,
            Self::Fencing(_) | Self::Terminal(_) => "",
        }
    }

    /// 低基数指标标签取值（`wal_append_error_total{reason=..}`）。
    ///
    /// 禁止把端点、db_id、消息文本放进标签：它们都会打爆 Prometheus 基数。
    pub(crate) fn metric_reason(&self) -> &'static str {
        match self {
            Self::NotLeader { .. } => "not_leader",
            Self::Transport { .. } => "transport",
            Self::Incomplete { .. } => "incomplete",
            Self::NotDurable { error, .. } => match error.code {
                ErrorCode::WalNotDurable => "not_durable",
                ErrorCode::StorageUnavailable => "storage_unavailable",
                ErrorCode::ResourceExhausted => "resource_exhausted",
                ErrorCode::RateLimited => "rate_limited",
                ErrorCode::QuotaExceeded => "quota_exceeded",
                ErrorCode::DeadlineExceeded => "deadline_exceeded",
                ErrorCode::Cancelled => "cancelled",
                ErrorCode::WorkerUnavailable => "worker_unavailable",
                ErrorCode::DatabaseNotReady => "database_not_ready",
                ErrorCode::WakeupTimeout => "wakeup_timeout",
                ErrorCode::ErrorCodeUnspecified => "unspecified",
                _ => "internal",
            },
            Self::Fencing(error) => match error.code {
                ErrorCode::EpochMismatch => "epoch_mismatch",
                _ => "append_rejected",
            },
            Self::Terminal(error) => match error.code {
                ErrorCode::InvalidArgument => "invalid_argument",
                ErrorCode::PermissionDenied => "permission_denied",
                ErrorCode::Unauthenticated => "unauthenticated",
                ErrorCode::DbNotFound => "db_not_found",
                ErrorCode::NotImplemented => "not_implemented",
                _ => "terminal",
            },
        }
    }

    /// 终态失败的对外错误（保留 detail / code）。
    pub(crate) fn terminal_error(&self) -> PlatformError {
        match self {
            Self::Fencing(error) | Self::Terminal(error) => error.clone(),
            // disposition() 保证不会走到这里；保守回退为「未 durable」而不是成功
            _ => PlatformError::new(ErrorCode::WalNotDurable, self.describe()),
        }
    }

    /// 人类可读描述（诊断用，不含 secret）。
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::NotLeader {
                endpoint,
                message,
                hint,
            } => match hint {
                Some(hint) => format!("{message} (endpoint={endpoint}, leader_hint={hint})"),
                None => format!("{message} (endpoint={endpoint})"),
            },
            Self::Transport { endpoint, message } => {
                format!("{message} (endpoint={endpoint})")
            }
            Self::NotDurable { endpoint, error } => {
                format!("{} (endpoint={endpoint})", error.message)
            }
            Self::Incomplete { endpoint, message } => {
                format!("{message} (endpoint={endpoint})")
            }
            Self::Fencing(error) | Self::Terminal(error) => error.message.clone(),
        }
    }
}

/// 一轮尝试的失败记录，用于决定耗尽后的对外错误。
#[derive(Debug, Clone, Default)]
pub(crate) struct FailureLog {
    failures: Vec<AttemptFailure>,
}

impl FailureLog {
    /// 记录一次失败。
    pub(crate) fn push(&mut self, failure: AttemptFailure) {
        self.failures.push(failure);
    }

    /// 已尝试的端点（按顺序，含重复），用于诊断。
    pub(crate) fn endpoints(&self) -> Vec<String> {
        self.failures
            .iter()
            .map(|failure| failure.endpoint().to_owned())
            .filter(|endpoint| !endpoint.is_empty())
            .collect()
    }

    /// 尝试耗尽后的对外错误。
    ///
    /// 规则（冻结）：
    /// - 本轮出现过 leader 相关失败 -> [`ErrorCode::WalNotLeader`]：
    ///   调用方最需要知道「写路径没有找到 leader / 已失去 owner」，而不是笼统的未 durable。
    /// - 否则按最后一次失败归类：传输/协议违例 -> [`ErrorCode::WalNotDurable`]；
    ///   服务端明确报错 -> 原样回传其错误码（`WAL_NOT_DURABLE` 即 `WalNotDurable`）。
    pub(crate) fn exhausted_error(&self, op: &str, attempts: u32) -> PlatformError {
        let Some(last) = self.failures.last() else {
            // 没有任何失败记录却耗尽，只可能出现在调用方逻辑 bug；保守报未 durable
            return PlatformError::new(
                ErrorCode::WalNotDurable,
                format!("{op}: 未执行任何尝试即耗尽"),
            );
        };
        // 把尝试过的端点带进消息：跨副本失败时排障最需要的信息就是「试过哪些地址」
        let tried = self.endpoints();
        let suffix = if tried.is_empty() {
            String::new()
        } else {
            format!("（已尝试端点：{}）", tried.join(", "))
        };

        if self.failures.iter().any(AttemptFailure::is_leader_related) {
            return PlatformError::new(
                ErrorCode::WalNotLeader,
                format!(
                    "{op}: {attempts} 次尝试均未命中 WAL leader：{}{suffix}",
                    last.describe()
                ),
            );
        }

        match last {
            AttemptFailure::Transport { .. } | AttemptFailure::Incomplete { .. } => {
                PlatformError::new(
                    ErrorCode::WalNotDurable,
                    format!(
                        "{op}: {attempts} 次尝试后仍无法确认 quorum durable（{}）{suffix}，禁止返回 Commit Success",
                        last.describe()
                    ),
                )
            }
            AttemptFailure::NotDurable { error, .. } => {
                let mut error = error.clone();
                error.message = format!(
                    "{op}: {attempts} 次尝试后仍未 durable：{}{suffix}",
                    error.message
                );
                error
            }
            // leader 相关在上面已处理；终态失败不会进入耗尽路径
            AttemptFailure::NotLeader { .. } => PlatformError::new(
                ErrorCode::WalNotLeader,
                format!("{op}: {attempts} 次尝试均未命中 WAL leader{suffix}"),
            ),
            AttemptFailure::Fencing(error) | AttemptFailure::Terminal(error) => error.clone(),
        }
    }
}

/// 为 fencing 拒绝补上 `current_epoch` / `requested_epoch`（架构 §11.3）。
///
/// 字段名与 wal-service 统一（`current_epoch` = 服务端权威值，`requested_epoch` = 请求
/// 携带值）：早期实现两侧各写一套（服务端 `epoch_expected`/`epoch_actual`、客户端
/// `expected_epoch`/`actual_epoch`），且服务端那两个键的含义与客户端预期的正好相反
/// （服务端的 `epoch_expected` 是「服务端已记录值」），排障时会把结论引反。
/// 因此这里：
/// 1. 写入统一的 `current_epoch` / `requested_epoch`；
/// 2. 把旧键**归一化**掉（读进来时按正确含义映射，输出只保留一套 schema），
///    调用方不需要知道历史上存在过哪些命名。
///
/// `current_epoch` 先取调用方提供的权威值（响应体字段），再回退到服务端 detail 的提示；
/// 都拿不到就填 `null` —— 编造一个数字会让排障者以为「旧 epoch 就是它」，比缺失更危险。
fn enrich_fencing(
    endpoint: String,
    mut error: PlatformError,
    requested_epoch: Option<u64>,
    current_epoch: Option<u64>,
) -> PlatformError {
    let current_epoch = current_epoch.or_else(|| current_epoch_hint(&error));
    let requested_epoch = requested_epoch.or_else(|| requested_epoch_hint(&error));
    let mut detail = match error.detail.take() {
        Some(serde_json::Value::Object(map)) => map,
        // 服务端 detail 不是对象（或为空）时，不丢失原始信息
        Some(other) => {
            let mut map = serde_json::Map::new();
            map.insert("server_detail".to_owned(), other);
            map
        }
        None => serde_json::Map::new(),
    };
    // 归一化：旧键（服务端的 epoch_expected/epoch_actual、客户端历史上写出的
    // expected_epoch/actual_epoch）已经从上面的 hint 提取过含义，这里把它们删掉，
    // 避免同一份 detail 里新旧两套名字并存，读者再次把含义看反。
    for legacy_key in LEGACY_FENCING_EPOCH_KEYS {
        detail.remove(*legacy_key);
    }
    map_insert_u64(&mut detail, FENCING_CURRENT_EPOCH_KEY, current_epoch);
    map_insert_u64(&mut detail, FENCING_REQUESTED_EPOCH_KEY, requested_epoch);
    detail.insert(
        "endpoint".to_owned(),
        serde_json::Value::from(endpoint.clone()),
    );
    // 原始服务端错误码保留在 detail 里：对外统一是 WAL_APPEND_REJECTED，
    // 但排障时需要知道服务端到底报的是 EPOCH_MISMATCH / NOT_OWNER / WAL_APPEND_REJECTED
    detail.insert(
        "server_code".to_owned(),
        serde_json::Value::from(error.code.as_str()),
    );
    error.detail = Some(serde_json::Value::Object(detail));
    // 对外统一为「WAL 拒绝该次 append」（架构 §11.3 的 fencing 语义）
    error.code = ErrorCode::WalAppendRejected;
    // fencing 是所有权问题，重试同一个 owner 不会有结果
    error.retryable = false;
    error.message = format!("WAL 拒绝本次写入（fencing）：{}", error.message);
    error
}

/// 历史遗留的 fencing epoch 键名（读取时兼容，输出时一律归一化掉）。
///
/// 注意含义：`epoch_expected` 在 wal-service 早期版本里表示**服务端已记录值**、
/// `epoch_actual` 表示**请求携带值** —— 与客户端旧键 `expected_epoch`（请求携带）/
/// `actual_epoch`（服务端当前）恰好互换，这正是必须统一命名的原因。
const LEGACY_FENCING_EPOCH_KEYS: &[&str] = &[
    "epoch_expected",
    "epoch_actual",
    "expected_epoch",
    "actual_epoch",
];

fn map_insert_u64(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<u64>,
) {
    map.insert(
        key.to_owned(),
        value.map_or(serde_json::Value::Null, serde_json::Value::from),
    );
}

/// 从服务端 detail 中提取「服务端当前 epoch」（权威值）。
///
/// 统一键名 [`FENCING_CURRENT_EPOCH_KEY`] 优先；其余为兼容旧版本服务端 / 旧版客户端
/// 写出的别名。**顺序即优先级**：新键代表服务端的准确表述，旧键只在缺席时兜底。
fn current_epoch_hint(error: &PlatformError) -> Option<u64> {
    epoch_from_detail(
        error,
        &[
            FENCING_CURRENT_EPOCH_KEY,
            // 服务端早期键名：epoch_expected = 服务端已记录值（含义见 LEGACY_...）
            "epoch_expected",
            // 客户端早期键名：actual_epoch = 服务端当前值
            "actual_epoch",
            // 更宽松的别名：字段名本身没有歧义，可以安全采纳
            "owner_epoch",
            "known_epoch",
            "applied_epoch",
            "epoch",
        ],
    )
}

/// 从服务端 detail 中提取「本次请求携带的 epoch」。
///
/// 只在调用方没给出请求 epoch 时才需要（读路径没有请求 epoch 概念）。
fn requested_epoch_hint(error: &PlatformError) -> Option<u64> {
    epoch_from_detail(
        error,
        &[
            FENCING_REQUESTED_EPOCH_KEY,
            "expected_epoch",
            "epoch_actual",
            "requested",
        ],
    )
}

/// 按给定键顺序取第一个可解析为 u64 的值。
fn epoch_from_detail(error: &PlatformError, keys: &[&str]) -> Option<u64> {
    let detail = error.detail.as_ref()?;
    for key in keys {
        if let Some(value) = detail.get(*key) {
            if let Some(epoch) = as_u64(value) {
                return Some(epoch);
            }
        }
    }
    None
}

fn as_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse().ok()))
}

/// 从服务端错误里提取 leader 端点提示。
///
/// 契约优先级：
/// 1. `detail_json` 的显式字段（`leader_endpoint` / `leader_address` / `leader` / `endpoint`）；
/// 2. 消息文本中出现的 `http(s)://...` URL（部分实现只把 leader 写进 message）。
///
/// 返回的提示会先做 URI 形态校验，避免把一段普通文本当成地址去拨号。
pub(crate) fn leader_hint(error: &PlatformError) -> Option<String> {
    if let Some(detail) = error.detail.as_ref() {
        for key in ["leader_endpoint", "leader_address", "leader", "endpoint"] {
            if let Some(hint) = detail.get(key).and_then(|value| value.as_str()) {
                if let Some(hint) = normalize_hint(hint) {
                    return Some(hint);
                }
            }
        }
    }
    scan_endpoint_in_text(&error.message)
}

/// 从自由文本里扫描 `http(s)://host[:port][/path]`。
fn scan_endpoint_in_text(text: &str) -> Option<String> {
    for scheme in ["http://", "https://"] {
        let Some(start) = text.find(scheme) else {
            continue;
        };
        let rest = &text[start..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == ')' || c == ']')
            .unwrap_or(rest.len());
        if let Some(hint) = normalize_hint(&rest[..end]) {
            return Some(hint);
        }
    }
    None
}

/// 校验并规范化 leader 提示：必须是 `http(s)://host[:port]` 形态。
fn normalize_hint(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    let rest = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))?;
    // 只保留 host[:port]：去掉 path/query，避免把提示里的路径当成端点的一部分
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() || authority.contains(|c: char| c.is_whitespace()) {
        return None;
    }
    if trimmed.starts_with("https://") {
        Some(format!("https://{authority}"))
    } else {
        Some(format!("http://{authority}"))
    }
}

/// proto 错误码 -> domain 错误码（唯一入口，不在此处做重试决策）。
pub(crate) fn error_code_from_proto(raw: i32) -> ErrorCode {
    protocol::convert::error_code_from_proto(raw)
}

/// proto 错误体 -> domain 错误体。
pub(crate) fn platform_error_from_proto(error: &common::PlatformError) -> PlatformError {
    protocol::convert::platform_error_from_proto(error)
}

/// 判断 in-band 错误体是否表示「成功」。
///
/// proto3 里未设置的 `error` 字段读出来是默认值（`code` = 0 = `ERROR_CODE_UNSPECIFIED`），
/// 因此「`error` 缺失 / code 为 OK / code 为 UNSPECIFIED」都表示这次 RPC 没有返回错误；
/// 其余情况才当作失败处理。
pub(crate) fn is_success_error(error: Option<&common::PlatformError>) -> bool {
    match error {
        None => true,
        Some(error) => matches!(
            error_code_from_proto(error.code),
            ErrorCode::Ok | ErrorCode::ErrorCodeUnspecified
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn platform_error(code: ErrorCode, message: &str) -> PlatformError {
        PlatformError::new(code, message)
    }

    fn proto_error(
        code: ErrorCode,
        message: &str,
        detail: Option<serde_json::Value>,
    ) -> common::PlatformError {
        let mut error = platform_error(code, message);
        error.detail = detail;
        common::PlatformError::from(error)
    }

    #[test]
    fn proto_error_codes_map_to_domain_codes() {
        // requirement: NOT_LEADER / EPOCH_MISMATCH / NOT_DURABLE 三类映射各一例
        use protocol::common::ErrorCode as Proto;

        assert_eq!(
            error_code_from_proto(Proto::WalNotLeader as i32),
            ErrorCode::WalNotLeader
        );
        assert_eq!(
            error_code_from_proto(Proto::EpochMismatch as i32),
            ErrorCode::EpochMismatch
        );
        assert_eq!(
            error_code_from_proto(Proto::WalNotDurable as i32),
            ErrorCode::WalNotDurable
        );
        // 未知数值不得 panic，也不得伪装成 OK
        assert_eq!(error_code_from_proto(9_999), ErrorCode::InternalError);
        assert_eq!(
            error_code_from_proto(Proto::Unspecified as i32),
            ErrorCode::ErrorCodeUnspecified
        );
    }

    #[test]
    fn not_leader_is_retryable_and_exhausts_to_wal_not_leader() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::WalNotLeader,
            "not leader",
            Some(serde_json::json!({"leader_endpoint": "http://wal-2:9200"})),
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, Some(835));

        assert_eq!(failure.disposition(), Disposition::Retry);
        assert_eq!(failure.hint(), Some("http://wal-2:9200"));
        assert_eq!(failure.metric_reason(), "not_leader");

        let mut log = FailureLog::default();
        log.push(failure);
        let exhausted = log.exhausted_error("append", 4);
        assert_eq!(exhausted.code, ErrorCode::WalNotLeader);
    }

    #[test]
    fn epoch_mismatch_is_terminal_fencing_with_epoch_detail() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::EpochMismatch,
            "epoch 834 已被取代",
            Some(serde_json::json!({"owner_epoch": 835})),
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, Some(834));

        assert_eq!(failure.disposition(), Disposition::Terminal);
        let error = failure.terminal_error();
        assert_eq!(error.code, ErrorCode::WalAppendRejected);
        assert!(!error.retryable, "fencing 拒绝不得被上层当成可重试");
        let detail = error.detail.expect("fencing 必须带 detail");
        assert_eq!(detail[FENCING_REQUESTED_EPOCH_KEY], serde_json::json!(834));
        assert_eq!(detail[FENCING_CURRENT_EPOCH_KEY], serde_json::json!(835));
        assert_eq!(detail["server_code"], serde_json::json!("EPOCH_MISMATCH"));
        // 旧键必须被归一化掉：同一份 detail 里不允许新旧两套名字并存
        assert!(detail.get("expected_epoch").is_none());
        assert!(detail.get("actual_epoch").is_none());
    }

    /// 旧版本服务端的 detail 键名（`epoch_expected` = 服务端已记录值、
    /// `epoch_actual` = 请求携带值）必须被正确**按原义**归一化成新键。
    #[test]
    fn legacy_server_fencing_keys_are_normalized_with_correct_meaning() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::WalAppendRejected,
            "fenced by newer owner",
            // 注意这里是「服务端记的是 900，本次请求携带 835」的旧写法
            Some(serde_json::json!({"epoch_expected": 900, "epoch_actual": 835})),
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, Some(835));
        assert_eq!(failure.disposition(), Disposition::Terminal);

        let detail = failure.terminal_error().detail.expect("detail");
        assert_eq!(
            detail[FENCING_CURRENT_EPOCH_KEY],
            serde_json::json!(900),
            "旧键 epoch_expected 表示服务端权威值，归一到 current_epoch"
        );
        assert_eq!(
            detail[FENCING_REQUESTED_EPOCH_KEY],
            serde_json::json!(835),
            "请求携带值以请求本身为准"
        );
        assert!(detail.get("epoch_expected").is_none());
        assert!(detail.get("epoch_actual").is_none());
    }

    /// 新键在场时必须优先于旧键（服务端升级过程中两套键可能同时出现）。
    #[test]
    fn current_epoch_prefers_canonical_key_over_legacy() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::WalAppendRejected,
            "fenced",
            Some(serde_json::json!({
                "current_epoch": 901,
                "epoch_expected": 900,
                "actual_epoch": 899,
            })),
        ));
        let detail = AttemptFailure::from_server("http://wal-1:9200", error, Some(835))
            .terminal_error()
            .detail
            .expect("detail");
        assert_eq!(detail[FENCING_CURRENT_EPOCH_KEY], serde_json::json!(901));
    }

    /// NOT_OWNER 是 fencing 类的终态：换端点重试携带的仍是同一个失效 epoch。
    #[test]
    fn not_owner_is_terminal_fencing_not_retryable() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::NotOwner,
            "本节点不是该 DB 的 Owner",
            Some(serde_json::json!({"current_epoch": 900})),
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, Some(835));

        assert_eq!(
            failure.disposition(),
            Disposition::Terminal,
            "NOT_OWNER 不得换端点重试（重试只会在每个副本上各失败一次）"
        );
        assert!(!failure.is_leader_related(), "不得退化成 WAL_NOT_LEADER");
        let error = failure.terminal_error();
        assert_eq!(error.code, ErrorCode::WalAppendRejected);
        assert!(!error.retryable);
        let detail = error.detail.expect("detail");
        assert_eq!(detail["server_code"], serde_json::json!("NOT_OWNER"));
        assert_eq!(detail[FENCING_CURRENT_EPOCH_KEY], serde_json::json!(900));
        assert_eq!(detail[FENCING_REQUESTED_EPOCH_KEY], serde_json::json!(835));
    }

    #[test]
    fn append_rejected_is_terminal_even_without_server_detail() {
        let error =
            platform_error_from_proto(&proto_error(ErrorCode::WalAppendRejected, "fenced", None));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, Some(7));
        assert_eq!(failure.disposition(), Disposition::Terminal);
        assert_eq!(failure.metric_reason(), "append_rejected");

        let error = failure.terminal_error();
        assert_eq!(error.code, ErrorCode::WalAppendRejected);
        let detail = error.detail.expect("detail");
        assert_eq!(detail[FENCING_REQUESTED_EPOCH_KEY], serde_json::json!(7));
        // 服务端没给 current epoch 时必须为 null，而不是编造
        assert_eq!(detail[FENCING_CURRENT_EPOCH_KEY], serde_json::Value::Null);
    }

    #[test]
    fn wal_not_durable_is_retryable_and_exhausts_to_wal_not_durable() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::WalNotDurable,
            "append timeout",
            None,
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, None);

        assert_eq!(failure.disposition(), Disposition::Retry);
        assert!(!failure.is_leader_related());
        assert_eq!(failure.metric_reason(), "not_durable");

        let mut log = FailureLog::default();
        log.push(failure);
        let exhausted = log.exhausted_error("append", 4);
        assert_eq!(exhausted.code, ErrorCode::WalNotDurable);
    }

    #[test]
    fn transport_failure_exhausts_to_wal_not_durable() {
        let mut log = FailureLog::default();
        log.push(AttemptFailure::transport(
            "http://wal-1:9200",
            "connect refused",
        ));
        let exhausted = log.exhausted_error("append", 2);
        assert_eq!(exhausted.code, ErrorCode::WalNotDurable);
        assert!(exhausted.message.contains("wal-1:9200"));
    }

    #[test]
    fn leader_failure_takes_priority_over_transport_in_mixed_run() {
        let mut log = FailureLog::default();
        log.push(AttemptFailure::from_server(
            "http://wal-2:9200",
            platform_error(ErrorCode::WalNotLeader, "not leader"),
            None,
        ));
        log.push(AttemptFailure::transport("http://wal-3:9200", "timeout"));
        assert_eq!(
            log.exhausted_error("append", 2).code,
            ErrorCode::WalNotLeader
        );
    }

    #[test]
    fn invalid_argument_is_terminal_passthrough() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::InvalidArgument,
            "bad lsn range",
            None,
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, None);
        assert_eq!(failure.disposition(), Disposition::Terminal);
        assert_eq!(failure.terminal_error().code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn leader_hint_from_message_text() {
        let error = platform_error(
            ErrorCode::WalNotLeader,
            "this node is not leader, try http://wal-3:9200, or wait for a new election",
        );
        assert_eq!(attacker_hint(&error), Some("http://wal-3:9200".to_owned()));
    }

    /// 通过 `from_server` 触发 `leader_hint`（`leader_hint` 自身是私有实现细节）。
    fn attacker_hint(error: &PlatformError) -> Option<String> {
        match AttemptFailure::from_server("ep", error.clone(), None) {
            AttemptFailure::NotLeader { hint, .. } => hint,
            other => panic!("期望 NotLeader，实际 {other:?}"),
        }
    }

    #[test]
    fn leader_hint_rejects_non_endpoint_text() {
        let error = platform_error(ErrorCode::WalNotLeader, "leader is wal-3 (node id)");
        // 只有 node id、没有地址：不得瞎猜地址，交给轮换策略处理
        assert_eq!(attacker_hint(&error), None);
    }

    #[test]
    fn leader_hint_normalizes_trailing_path_and_slash() {
        let error = platform_error(
            ErrorCode::WalNotLeader,
            "leader: http://wal-3:9200/internal/path/",
        );
        assert_eq!(attacker_hint(&error), Some("http://wal-3:9200".to_owned()));
    }

    #[test]
    fn success_error_detection() {
        assert!(is_success_error(None));
        let ok = common::PlatformError {
            code: ErrorCode::Ok.to_proto_i32(),
            ..Default::default()
        };
        assert!(is_success_error(Some(&ok)));
        let unspecified = common::PlatformError::default();
        assert!(is_success_error(Some(&unspecified)));
        let not_leader = common::PlatformError {
            code: ErrorCode::WalNotLeader.to_proto_i32(),
            ..Default::default()
        };
        assert!(!is_success_error(Some(&not_leader)));
    }

    #[test]
    fn other_server_codes_stay_retryable_or_terminal_as_documented() {
        // 限流 / 背压允许换端点再试（service 端 detail 用 NotDurable 承载原错误码）
        for code in [
            ErrorCode::RateLimited,
            ErrorCode::ResourceExhausted,
            ErrorCode::StorageUnavailable,
            ErrorCode::DeadlineExceeded,
            ErrorCode::InternalError,
        ] {
            let failure =
                AttemptFailure::from_server("http://wal-1:9200", platform_error(code, "x"), None);
            assert_eq!(failure.disposition(), Disposition::Retry, "{code:?}");
            assert_eq!(failure.terminal_error().code, ErrorCode::WalNotDurable);
        }
        // 权限 / 参数类错误重试没有意义
        for code in [
            ErrorCode::PermissionDenied,
            ErrorCode::Unauthenticated,
            ErrorCode::DbNotFound,
            ErrorCode::NotImplemented,
        ] {
            let failure =
                AttemptFailure::from_server("http://wal-1:9200", platform_error(code, "x"), None);
            assert_eq!(failure.disposition(), Disposition::Terminal, "{code:?}");
        }
    }

    /// 幂等冲突必须原样回传自己的错误码，且**不得重试**。
    ///
    /// 为什么不能折叠成 WAL_APPEND_REJECTED：那个码表示「所有权已变」（fencing），
    /// 调用方会去重新获取 Owner 再重试；而幂等冲突说明同一 append_id 被用在了不同内容上，
    /// 换 epoch / 换端点重试永远不会成功，只会掩盖真正的客户端 bug（同样参数无限重试）。
    #[test]
    fn idempotency_conflict_is_terminal_and_keeps_its_code() {
        let error = platform_error_from_proto(&proto_error(
            ErrorCode::IdempotencyConflict,
            "append_id ap-1 已被 start_lsn=0 使用",
            Some(serde_json::json!({
                "append_id": "ap-1",
                "recorded_start_lsn": 0,
                "requested_start_lsn": 8,
            })),
        ));
        let failure = AttemptFailure::from_server("http://wal-1:9200", error, Some(835));

        assert_eq!(failure.disposition(), Disposition::Terminal);
        assert_eq!(failure.metric_reason(), "terminal");
        let error = failure.terminal_error();
        assert_eq!(error.code, ErrorCode::IdempotencyConflict);
        let detail = error.detail.expect("detail 必须保留服务端给出的冲突区间");
        assert_eq!(detail["append_id"], serde_json::json!("ap-1"));
        assert_eq!(detail["recorded_start_lsn"], serde_json::json!(0));
        assert_eq!(detail["requested_start_lsn"], serde_json::json!(8));
    }

    #[test]
    fn exhausted_without_failures_still_not_ok() {
        // 没有任何失败记录就走到「耗尽」分支只可能是调用方 bug，此时也必须报未 durable
        let log = FailureLog::default();
        assert_eq!(
            log.exhausted_error("append", 1).code,
            ErrorCode::WalNotDurable
        );
    }
}
