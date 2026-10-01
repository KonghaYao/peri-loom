//! Worker Data Dispatcher <-> DB Process 之间的本地 UDS 帧编解码。
//!
//! 冻结 wire format（架构 §17.7）：
//!
//! ```text
//! UnixStream + 4 字节大端长度前缀 + protobuf message
//! ```
//!
//! 本层**不做** gRPC / HTTP2：本机 UDS 上再跑一层完整 HTTP2 只增加开销与故障面。
//! 因此 streaming（多 frame response）与 cancel（按 request_id）都由 [`Frame`] 自身承载。
//!
//! Backpressure：本层不缓存、不排队，写入侧的背压由调用方 `await` [`write_frame`]
//! 或 [`Framed`] 的 `Sink::send` 自然获得（读者慢 -> 内核 socket buffer 满 -> 写者挂起）。
//! 读取侧以长度前缀为界，单帧大小受 [`MAX_FRAME_BYTES`] 限制，避免对端声明超大长度导致 OOM。

use bytes::{Buf, BufMut, Bytes, BytesMut};
use domain::error::ErrorCode;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::runtime_local::Frame;

/// 单帧长度上限（默认 64 MiB）。
///
/// 长度前缀是 4 字节，理论上可声明 4 GiB；若不设上限，对端只要发 4 字节头
/// 就能让本端按声明长度预分配，构成 OOM 攻击面。超限一律拒绝并断开连接。
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// 长度前缀字节数（大端 u32）。
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// 本地帧编解码错误。
///
/// 每个变体都能映射到平台统一错误码（[`FramingError::error_code`]），
/// 便于向上层透出结构化错误而不是裸 IO 错误。
#[derive(Debug, thiserror::Error)]
pub enum FramingError {
    /// 帧长度超过上限：对端声明或本端待发的帧过大。
    #[error("帧长度 {size} 字节超过上限 {max} 字节")]
    FrameTooLarge {
        /// 实际（或声明）长度。
        size: usize,
        /// 当前上限。
        max: usize,
    },

    /// 载荷不是合法 protobuf：对端发送了损坏/错位的帧。
    #[error("帧载荷 protobuf 解码失败：{0}")]
    Decode(#[from] prost::DecodeError),

    /// 本端序列化失败（容量不足等），属于内部错误。
    #[error("帧载荷 protobuf 编码失败：{0}")]
    Encode(#[from] prost::EncodeError),

    /// 连接在帧中途关闭：收到的字节数少于长度前缀声明。
    #[error("连接在帧中途关闭：期望 {expected} 字节，实际 {actual} 字节")]
    Truncated {
        /// 期望字节数。
        expected: usize,
        /// 实际读到的字节数。
        actual: usize,
    },

    /// 底层 UDS I/O 失败。
    #[error("本地 UDS I/O 失败：{0}")]
    Io(#[from] std::io::Error),
}

/// 本模块的便捷 `Result`（错误固定为 [`FramingError`]）。
pub type Result<T> = std::result::Result<T, FramingError>;

impl FramingError {
    /// 映射到平台统一错误码（跨服务契约，不能随手换）。
    pub fn error_code(&self) -> ErrorCode {
        match self {
            // 帧超限：拒绝分配属于资源保护语义
            FramingError::FrameTooLarge { .. } => ErrorCode::ResourceExhausted,
            // 对端发来的 protobuf 非法 -> 输入问题
            FramingError::Decode(_) => ErrorCode::InvalidArgument,
            // 本端编码失败 / 连接异常 -> 内部问题
            FramingError::Encode(_) | FramingError::Truncated { .. } | FramingError::Io(_) => {
                ErrorCode::InternalError
            }
        }
    }

    /// 该错误是否可安全重试（由错误码语义决定）。
    pub fn retryable(&self) -> bool {
        self.error_code().retryable()
    }
}

/// `Frame` 的 length-delimited 编解码器。
///
/// 与 `tokio_util::codecs::LengthDelimitedCodec` 的区别：长度上限参与解码前置校验
/// （先看 4 字节头再决定是否继续），且解码失败会给出结构化错误码而非 `io::Error`。
#[derive(Debug, Clone, Copy)]
pub struct LocalFrameCodec {
    max_frame_bytes: usize,
}

impl LocalFrameCodec {
    /// 使用默认上限 [`MAX_FRAME_BYTES`]。
    pub const fn new() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
        }
    }

    /// 自定义上限；超过 `u32::MAX` 的部分会被夹掉（长度前缀只有 4 字节）。
    pub const fn with_max_frame_bytes(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes: if max_frame_bytes > u32::MAX as usize {
                u32::MAX as usize
            } else {
                max_frame_bytes
            },
        }
    }

    /// 当前生效的单帧上限。
    pub const fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// 编码到已有缓冲（复用 `Encoder` 实现，避免 `encode_frame` 复制代码）。
    fn encode_into(&self, frame: &Frame, dst: &mut BytesMut) -> Result<()> {
        let len = frame.encoded_len();
        if len > self.max_frame_bytes {
            return Err(FramingError::FrameTooLarge {
                size: len,
                max: self.max_frame_bytes,
            });
        }

        // 前缀 + 载荷一次 reserve，保证后续 prost::encode 不会因容量不足而失败
        dst.reserve(LENGTH_PREFIX_BYTES + len);
        // len <= max_frame_bytes <= u32::MAX，截断不可能发生
        dst.put_u32(len as u32);
        frame.encode(dst)?;
        Ok(())
    }
}

impl Default for LocalFrameCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for LocalFrameCodec {
    type Item = Frame;
    type Error = FramingError;

    fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Frame>, FramingError> {
        if src.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }

        // 只读取头部 4 字节，不消费；等确认整帧到齐再切分
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len > self.max_frame_bytes {
            // 关键：绝不按对端声明的长度预分配；消费掉头部使流不可恢复（调用方会断开连接）
            src.advance(LENGTH_PREFIX_BYTES);
            return Err(FramingError::FrameTooLarge {
                size: len,
                max: self.max_frame_bytes,
            });
        }

        if src.len() - LENGTH_PREFIX_BYTES < len {
            // 半包：预留整帧容量（已校验上限）后等待更多字节
            src.reserve(LENGTH_PREFIX_BYTES + len - src.len());
            return Ok(None);
        }

        src.advance(LENGTH_PREFIX_BYTES);
        let body = src.split_to(len);
        Ok(Some(Frame::decode(body)?))
    }
}

impl Encoder<Frame> for LocalFrameCodec {
    type Error = FramingError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> std::result::Result<(), FramingError> {
        self.encode_into(&item, dst)
    }
}

/// 编码单帧为「4 字节大端长度前缀 + protobuf 载荷」。
pub fn encode_frame(frame: &Frame) -> Result<Bytes> {
    let mut buf = BytesMut::new();
    LocalFrameCodec::new().encode_into(frame, &mut buf)?;
    Ok(buf.freeze())
}

/// 从缓冲中解出至多一帧。
///
/// 返回 `Ok(None)` 表示缓冲内还没有完整帧（调用方应继续读 socket）。
/// 解码成功时只消费该帧的字节，剩余字节留给下一次调用（支持一个 buffer 内多帧）。
pub fn decode_frame(src: &mut BytesMut) -> Result<Option<Frame>> {
    LocalFrameCodec::new().decode(src)
}

/// 写入单帧；写入完成即产生背压（内核 socket buffer 满时挂起调用方）。
pub async fn write_frame<W>(writer: &mut W, frame: &Frame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = encode_frame(frame)?;
    writer.write_all(&bytes).await?;
    // 本层无缓冲，flush 只为兼容带缓冲的实现（如 BufWriter）
    writer.flush().await?;
    Ok(())
}

/// 读取单帧。
///
/// 连接在**帧边界**正常关闭时返回 `Ok(None)`；帧中途关闭是协议错误。
pub async fn read_frame<R>(reader: &mut R) -> Result<Option<Frame>>
where
    R: AsyncRead + Unpin,
{
    let mut prefix = [0u8; LENGTH_PREFIX_BYTES];
    let filled = read_full(reader, &mut prefix).await?;
    if filled == 0 {
        return Ok(None);
    }
    if filled < LENGTH_PREFIX_BYTES {
        return Err(FramingError::Truncated {
            expected: LENGTH_PREFIX_BYTES,
            actual: filled,
        });
    }

    let len = u32::from_be_bytes(prefix) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(FramingError::FrameTooLarge {
            size: len,
            max: MAX_FRAME_BYTES,
        });
    }

    let mut body = vec![0u8; len];
    let filled = read_full(reader, &mut body).await?;
    if filled < len {
        return Err(FramingError::Truncated {
            expected: len,
            actual: filled,
        });
    }

    Ok(Some(Frame::decode(&body[..])?))
}

/// 将任意 `AsyncRead + AsyncWrite` 流包装为帧流（Dispatcher 侧一条 UDS 连接一个 `Framed`）。
pub fn framed<S>(stream: S) -> Framed<S, LocalFrameCodec>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    Framed::new(stream, LocalFrameCodec::new())
}

/// 读满 `buf`；对端提前关闭时返回实际读到的字节数（不报错，由调用方判定语义）。
async fn read_full<R>(reader: &mut R, buf: &mut [u8]) -> Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{value, Value};
    use crate::runtime_local::{frame, ExecuteRequest, HealthRequest, HealthResponse};

    fn health_frame(seq: u64) -> Frame {
        Frame {
            seq,
            request_id: format!("req-{seq}"),
            database_id: "db-1".to_string(),
            owner_epoch: 7,
            message: Some(frame::Message::Health(HealthRequest {})),
            ..Default::default()
        }
    }

    fn blob_frame(size: usize) -> Frame {
        Frame {
            seq: 9,
            request_id: "req-big".to_string(),
            database_id: "db-big".to_string(),
            owner_epoch: 42,
            message: Some(frame::Message::Execute(ExecuteRequest {
                sql: "INSERT INTO t VALUES (?)".to_string(),
                params: vec![Value {
                    kind: Some(value::Kind::Blob(vec![0xAB; size])),
                }],
                atomic: true,
                want_stream: false,
                inline_row_limit: 0,
            })),
            ..Default::default()
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let frame = health_frame(1);
        let bytes = encode_frame(&frame).expect("编码应成功");
        assert_eq!(&bytes[..4], &(bytes.len() as u32 - 4).to_be_bytes());

        let mut buf = BytesMut::from(&bytes[..]);
        let decoded = decode_frame(&mut buf).expect("解码应成功");
        assert_eq!(decoded, Some(frame));
        assert!(buf.is_empty(), "单帧解码后缓冲应被消费完");
    }

    #[test]
    fn encode_decode_roundtrip_large_frame() {
        // 1 MiB 载荷：验证大帧不被截断
        let frame = blob_frame(1024 * 1024);
        let bytes = encode_frame(&frame).expect("编码应成功");
        assert!(bytes.len() > 1024 * 1024);

        let mut buf = BytesMut::from(&bytes[..]);
        assert_eq!(decode_frame(&mut buf).expect("解码应成功"), Some(frame));
    }

    #[test]
    fn decode_handles_fragmented_packets() {
        // 半包分片：逐字节喂入，未收齐时必须返回 None
        let frame = health_frame(3);
        let bytes = encode_frame(&frame).unwrap();

        let mut codec = LocalFrameCodec::new();
        let mut buf = BytesMut::new();
        for (i, byte) in bytes.iter().enumerate() {
            buf.extend_from_slice(&[*byte]);
            let got = codec.decode(&mut buf).expect("分片解码不应报错");
            if i + 1 < bytes.len() {
                assert!(got.is_none(), "第 {} 字节时不应解出完整帧", i + 1);
            } else {
                assert_eq!(got, Some(frame.clone()));
            }
        }
    }

    #[test]
    fn decode_two_frames_from_one_buffer() {
        let a = health_frame(1);
        let b = health_frame(2);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&encode_frame(&a).unwrap());
        buf.extend_from_slice(&encode_frame(&b).unwrap());

        let mut codec = LocalFrameCodec::new();
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(a));
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(b));
        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn oversize_frame_is_rejected_without_allocating() {
        // 只提供 4 字节头（声明 64 MiB + 1），解码必须立刻失败而不是预分配
        let mut buf = BytesMut::new();
        buf.put_u32((MAX_FRAME_BYTES + 1) as u32);

        let err = decode_frame(&mut buf).expect_err("超限帧必须报错");
        assert!(matches!(err, FramingError::FrameTooLarge { .. }));
        assert_eq!(err.error_code(), ErrorCode::ResourceExhausted);
        // retryable 语义跟随错误码（由 domain 定义），这里只断言两者一致
        assert_eq!(err.retryable(), ErrorCode::ResourceExhausted.retryable());
    }

    #[test]
    fn max_frame_boundary_is_inclusive() {
        let frame = health_frame(5);
        let len = frame.encoded_len();

        // 恰好等于上限：允许
        let mut codec = LocalFrameCodec::with_max_frame_bytes(len);
        let mut buf = BytesMut::new();
        assert!(codec.encode(frame.clone(), &mut buf).is_ok());
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(frame));

        // 上限少 1 字节：拒绝
        let mut small = LocalFrameCodec::with_max_frame_bytes(len - 1);
        let mut buf = BytesMut::new();
        let err = small
            .encode(health_frame(5), &mut buf)
            .expect_err("超限必须报错");
        assert_eq!(err.error_code(), ErrorCode::ResourceExhausted);
    }

    #[test]
    fn invalid_payload_maps_to_invalid_argument() {
        let mut buf = BytesMut::new();
        buf.put_u32(3);
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF]);

        let err = decode_frame(&mut buf).expect_err("非法 protobuf 必须报错");
        assert!(matches!(err, FramingError::Decode(_)));
        assert_eq!(err.error_code(), ErrorCode::InvalidArgument);
    }

    #[test]
    fn empty_payload_decodes_to_default_frame() {
        // 长度 0 是合法的 protobuf（所有字段取默认值），不应 panic
        let mut buf = BytesMut::new();
        buf.put_u32(0);

        assert_eq!(decode_frame(&mut buf).unwrap(), Some(Frame::default()));
    }

    #[tokio::test]
    async fn write_then_read_frame_over_stream() {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024);
        let frame = health_frame(11);

        let writer = tokio::spawn(async move {
            write_frame(&mut client, &frame).await.unwrap();
        });

        let got = read_frame(&mut server).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got, Some(health_frame(11)));
    }

    #[tokio::test]
    async fn read_frame_returns_none_on_clean_eof() {
        let (client, mut server) = tokio::io::duplex(1024);
        drop(client);
        assert_eq!(read_frame(&mut server).await.unwrap(), None);
    }

    #[tokio::test]
    async fn read_frame_reports_truncated_body() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let bytes = encode_frame(&health_frame(13)).unwrap();

        // 写入长度前缀 + 一半载荷后关闭：属于协议错误而非正常 EOF
        let half = LENGTH_PREFIX_BYTES + (bytes.len() - LENGTH_PREFIX_BYTES) / 2;
        client.write_all(&bytes[..half]).await.unwrap();
        drop(client);

        let err = read_frame(&mut server)
            .await
            .expect_err("帧中途关闭必须报错");
        match err {
            FramingError::Truncated { expected, actual } => {
                assert_eq!(expected, bytes.len() - LENGTH_PREFIX_BYTES);
                assert_eq!(actual, half - LENGTH_PREFIX_BYTES);
            }
            other => panic!("错误类型不符：{other:?}"),
        }
        assert_eq!(err.error_code(), ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn framed_stream_roundtrip() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let mut sink = framed(client);
        let mut stream = framed(server);

        use futures::{SinkExt, StreamExt};
        sink.send(health_frame(21)).await.unwrap();
        sink.send(Frame {
            seq: 22,
            message: Some(frame::Message::HealthResponse(HealthResponse {
                database_id: "db-1".to_string(),
                owner_epoch: 7,
                state: "HOT".to_string(),
                rss_kib: 2048,
                opened_connections: 1,
                active_sessions: 0,
                applied_lsn: 100,
            })),
            ..Default::default()
        })
        .await
        .unwrap();

        assert_eq!(stream.next().await.unwrap().unwrap(), health_frame(21));
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.seq, 22);
        match second.message {
            Some(frame::Message::HealthResponse(resp)) => {
                assert_eq!(resp.state, "HOT");
                assert_eq!(resp.applied_lsn, 100);
            }
            other => panic!("消息类型不符：{other:?}"),
        }
    }

    #[test]
    fn error_codes_are_mapped() {
        assert_eq!(
            FramingError::FrameTooLarge { size: 1, max: 0 }.error_code(),
            ErrorCode::ResourceExhausted
        );
        assert_eq!(
            FramingError::Truncated {
                expected: 8,
                actual: 2
            }
            .error_code(),
            ErrorCode::InternalError
        );
        assert_eq!(
            FramingError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "x")).error_code(),
            ErrorCode::InternalError
        );
    }
}
