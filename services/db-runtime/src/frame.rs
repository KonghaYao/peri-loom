//! 本地帧信封（[`rt::Frame`]）的构造与错误映射。
//!
//! 为什么单独一层：一个请求可能回多个帧（流式），每帧都要带上与请求一致的
//! `request_id` / `database_id` / `owner_epoch` / `session_id` / `transaction_id`。
//! 这些字段散落在各个 handler 里手写，迟早会出现"某条错误路径忘了回填 owner_epoch"
//! 这种让 Dispatcher 无法归属的帧。这里统一从请求帧派生。

use domain::error::PlatformError;
use protocol::convert;
use protocol::runtime_local as rt;

/// 从请求帧派生响应帧的信封字段（除 `message` / `error` 外全部继承）。
#[must_use]
pub fn reply_envelope(request: &rt::Frame, seq: u64) -> rt::Frame {
    rt::Frame {
        seq,
        reply_to_seq: request.seq,
        request_id: request.request_id.clone(),
        database_id: request.database_id.clone(),
        owner_epoch: request.owner_epoch,
        session_id: request.session_id.clone(),
        transaction_id: request.transaction_id.clone(),
        deadline_unix_ms: request.deadline_unix_ms,
        error: None,
        message: None,
    }
}

/// 成功响应帧。
#[must_use]
pub fn reply(request: &rt::Frame, seq: u64, message: rt::frame::Message) -> rt::Frame {
    let mut frame = reply_envelope(request, seq);
    frame.message = Some(message);
    frame
}

/// 错误响应帧：**只**带 `error`，不带任何 message。
///
/// 为什么这条纪律重要：Dispatcher 用「有 error 就当作失败」判定结果，若同时带上
/// 部分成功的 message，会出现"错误帧里还夹着一个看似正常的结果集"这种自相矛盾的响应。
#[must_use]
pub fn error_reply(request: &rt::Frame, seq: u64, error: &PlatformError) -> rt::Frame {
    let mut frame = reply_envelope(request, seq);
    frame.error = Some(convert::platform_error_to_proto(error));
    frame
}

/// 流式响应帧（`StreamHeader` / `RowBatch` / `StreamEnd` / 流中途的错误）。
///
/// 流帧同样使用 `reply_to_seq` 关联请求，Dispatcher 才能把同一连接上并行的多条流分开。
#[must_use]
pub fn stream_reply(request: &rt::Frame, seq: u64, message: rt::frame::Message) -> rt::Frame {
    reply(request, seq, message)
}

/// 会话过期通知（单向，`reply_to_seq = 0`）。
#[must_use]
pub fn session_expired_notice(
    database_id: &str,
    owner_epoch: u64,
    session_id: &str,
    reason: &str,
) -> rt::Frame {
    rt::Frame {
        seq: 0,
        reply_to_seq: 0,
        request_id: String::new(),
        database_id: database_id.to_string(),
        owner_epoch,
        session_id: session_id.to_string(),
        transaction_id: String::new(),
        deadline_unix_ms: 0,
        error: None,
        message: Some(rt::frame::Message::SessionExpired(
            rt::SessionExpiredNotice {
                session_id: session_id.to_string(),
                reason: reason.to_string(),
            },
        )),
    }
}
