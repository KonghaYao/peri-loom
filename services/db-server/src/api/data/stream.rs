//! 数据面结果集的两种出口形态：内联 JSON 与 `application/x-ndjson` 流（架构 §17.4）。
//!
//! 关键设计：**一次执行、两种出口**。SQL 只送到 Worker 执行一次，Server 侧边收帧边
//! 缓冲：
//!
//! - 缓冲量始终没超过 `INLINE_RESULT_LIMIT_BYTES` 且客户端没要 NDJSON -> 收全后返回
//!   一个完整 JSON 结果集；
//! - 客户端用 `Accept: application/x-ndjson` 明确要求流式，或缓冲量超限 -> 立刻切换成
//!   NDJSON：先吐已缓冲的 header / rows，再把余下的帧逐帧转发。
//!
//! 不能做的事是「超限了再重新执行一次流式查询」：那会把一条 `INSERT` 执行两遍。
//!
//! **取消传播**：`CancelGuard` 被移进响应体生成器，随流一起存活。客户端断开 ->
//! axum 丢弃响应体 -> 生成器被 drop -> guard 的 `Drop` 向 Worker 发 `Cancel`
//! （800ms 预算，满足「1s 内送达」的契约）。

use std::time::Instant;

use axum::body::Body;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use domain::ids::DatabaseId;
use domain::value::{ColumnMeta, SqlValue};
use futures::StreamExt;

use crate::api::dto;
use crate::clients::{status_to_api_error, CancelGuard};
use crate::error::{ApiError, ApiResult};
use crate::router::{DbRouter, FrameStream, StreamTarget};

/// NDJSON 的 MIME 类型。
pub const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";

/// 一次数据面执行（`ExecuteStream` / `SessionExecuteStream`）并决定出口形态。
///
/// # Errors
/// 路由解析失败、Worker 不可达、或**首帧**即错误时返回错误（此时还没写出任何字节，
/// 可以给出正确的 HTTP 状态码）。
pub async fn execute(
    router: &DbRouter,
    inline_limit_bytes: usize,
    database_id: DatabaseId,
    target: StreamTarget,
    request_id: &str,
    deadline: Option<Instant>,
    want_ndjson: bool,
) -> ApiResult<Response> {
    let (route, mut frames, guard) = router
        .open_stream(database_id, target, request_id, deadline)
        .await?;
    let worker_id = route.worker_id.to_string();

    if want_ndjson {
        return ndjson_response(ndjson_stream(
            frames,
            guard,
            request_id,
            worker_id,
            Vec::new(),
            Vec::new(),
        ));
    }

    let mut columns: Vec<ColumnMeta> = Vec::new();
    let mut rows: Vec<Vec<SqlValue>> = Vec::new();
    let mut buffered_bytes = 0usize;
    let mut trailer: Option<(u64, u64, u64)> = None;

    while let Some(item) = frames.next().await {
        let frame = item.map_err(|status| status_to_api_error(status, &worker_id))?;
        if let Some(error) = proto_error(frame.error.as_ref()) {
            // 流中途的结构化错误：此时仍未写出任何字节，可以正常返回错误响应。
            return Err(ApiError::from(error));
        }
        match frame.frame {
            Some(protocol::data::stream_frame::Frame::Header(header)) => {
                columns = header.columns.iter().map(column_meta).collect();
            }
            Some(protocol::data::stream_frame::Frame::Rows(batch)) => {
                for row in batch.rows {
                    let values = protocol::convert::row_from_proto(row);
                    buffered_bytes += approximate_bytes(&values);
                    rows.push(values);
                }
                if buffered_bytes > inline_limit_bytes {
                    // 超过内联上限：切成 NDJSON，已缓冲的部分作为前缀先写出去。
                    return ndjson_response(ndjson_stream(
                        frames, guard, request_id, worker_id, columns, rows,
                    ));
                }
            }
            Some(protocol::data::stream_frame::Frame::Trailer(t)) => {
                trailer = Some((t.affected_rows, t.wal_lsn, t.elapsed_micros));
            }
            None => {}
        }
    }

    let (affected_rows, wal_lsn, elapsed_micros) = trailer.unwrap_or((0, 0, 0));
    let body = dto::QueryResponse {
        columns: columns.iter().map(dto::ColumnView::from).collect(),
        rows: rows
            .iter()
            .map(|row| row.iter().map(crate::ndjson::encode_value).collect())
            .collect(),
        affected_rows,
        // 流式路径下 Worker 会发完全部行，不存在「被截断」的语义（truncated 只在内联
        // 单次执行里由 Worker 依据 inline_row_limit 置位）。
        truncated: false,
        wal_lsn,
        elapsed_micros,
        request_id: request_id.to_string(),
    };
    Ok(axum::Json(body).into_response())
}

/// 构造 NDJSON 响应：只设置确定性的头，body 由生成器逐行产出。
fn ndjson_response<S>(stream: S) -> ApiResult<Response>
where
    S: futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, NDJSON_CONTENT_TYPE)
        // 结果集可能很大，禁止任何中间层缓存。
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from_stream(stream))
        .map_err(|err| ApiError::internal(format!("构造 NDJSON 响应失败: {err}")))
}

/// NDJSON 行生成器（前缀行 + 余下帧）。
fn ndjson_stream(
    mut frames: FrameStream,
    guard: CancelGuard,
    request_id: &str,
    worker_id: String,
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<SqlValue>>,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    let request_id = request_id.to_string();
    async_stream::stream! {
        // guard 与流同生命周期：流被 drop（客户端断开）时它负责把 Cancel 传到 Worker。
        let _guard = guard;

        if !columns.is_empty() {
            yield Ok(crate::ndjson::header_line(&columns));
        }
        for row in rows {
            yield Ok(crate::ndjson::row_line(&row));
        }

        while let Some(item) = frames.next().await {
            match item {
                Ok(frame) => {
                    if let Some(error) = proto_error(frame.error.as_ref()) {
                        // 已经写出字节，无法再改 HTTP 状态码：错误只能作为一行下发。
                        yield Ok(crate::ndjson::error_line_from(&error, &request_id));
                        return;
                    }
                    match frame.frame {
                        Some(protocol::data::stream_frame::Frame::Header(header)) => {
                            let columns: Vec<ColumnMeta> =
                                header.columns.iter().map(column_meta).collect();
                            yield Ok(crate::ndjson::header_line(&columns));
                        }
                        Some(protocol::data::stream_frame::Frame::Rows(batch)) => {
                            for row in batch.rows {
                                let values = protocol::convert::row_from_proto(row);
                                yield Ok(crate::ndjson::row_line(&values));
                            }
                        }
                        Some(protocol::data::stream_frame::Frame::Trailer(t)) => {
                            yield Ok(crate::ndjson::trailer_line(
                                t.affected_rows,
                                t.wal_lsn,
                                t.elapsed_micros,
                            ));
                        }
                        None => {}
                    }
                }
                Err(status) => {
                    let err = status_to_api_error(status, &worker_id);
                    yield Ok(crate::ndjson::error_line_from(&err.error, &request_id));
                    return;
                }
            }
        }
    }
}

/// proto 结构化错误 -> 领域错误；`OK` 与 `None` 都视为成功。
pub(crate) fn proto_error(
    error: Option<&protocol::common::PlatformError>,
) -> Option<domain::error::PlatformError> {
    error
        .filter(|err| err.code != protocol::common::ErrorCode::Ok as i32)
        .map(protocol::convert::platform_error_from_proto)
}

/// proto 列元数据 -> 领域列元数据。
pub(crate) fn column_meta(column: &protocol::data::ColumnMeta) -> ColumnMeta {
    ColumnMeta::new(
        column.name.clone(),
        column.type_name.clone(),
        column.nullable,
    )
}

/// 行数据的内存占用估算（用于判断是否超过内联上限）。
///
/// 只求量级正确：字符串 / 二进制按实际字节数，其余标量按 8 字节。
/// 目的是「大结果集不要在 Server 内存里堆积」，不是精确计量。
pub(crate) fn approximate_bytes(values: &[SqlValue]) -> usize {
    values
        .iter()
        .map(|value| match value {
            SqlValue::Null => 8,
            SqlValue::Integer(_) | SqlValue::Real(_) => 8,
            SqlValue::Text(text) => text.len(),
            SqlValue::Blob(bytes) => bytes.len(),
        })
        .sum()
}
