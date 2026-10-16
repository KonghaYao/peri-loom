use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{ensure, Result};

/// How long an idle pooled connection may wait for its next request. Bounds the
/// worker thread and inflight slot a parked connection holds, and drives how
/// quickly shutdown can proceed.
pub(super) const KEEP_ALIVE_IDLE: Duration = Duration::from_secs(5);

/// Longest single wait inside the idle loop, so worker shutdown is not delayed
/// by a parked connection.
const IDLE_SLICE: Duration = Duration::from_millis(250);

pub(super) struct Request {
    pub path: String,
    pub authorization: String,
    pub body: Vec<u8>,
    /// The peer is willing to reuse this connection for another request.
    pub keep_alive: bool,
}

pub(super) fn token_matches(supplied: &str, expected: &str) -> bool {
    let supplied = blake3::hash(supplied.as_bytes());
    let expected = blake3::hash(expected.as_bytes());
    supplied
        .as_bytes()
        .iter()
        .zip(expected.as_bytes())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

/// Wait until the peer starts sending another request.
///
/// Returns `false` on clean EOF, on idle timeout, or once the worker is
/// stopping, so a pooled connection cannot hold a thread or an inflight slot
/// indefinitely. Never consumes bytes: the caller re-reads the request itself.
fn request_pending(stream: &mut TcpStream, idle: Duration, stopped: &AtomicBool) -> Result<bool> {
    let deadline = Instant::now() + idle;
    loop {
        if stopped.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        stream.set_read_timeout(Some(remaining.min(IDLE_SLICE)))?;
        match stream.peek(&mut [0u8; 1]) {
            Ok(0) => return Ok(false),
            Ok(_) => return Ok(true),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) =>
            {
                continue
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn read_request(
    stream: &mut TcpStream,
    max_body_bytes: usize,
    idle: Option<Duration>,
    stopped: &AtomicBool,
) -> Result<Option<Request>> {
    if let Some(idle) = idle {
        if !request_pending(stream, idle, stopped)? {
            return Ok(None);
        }
    }
    stream.set_nonblocking(false)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream);
    let mut headers = BTreeMap::new();
    let mut consumed = 0;
    let first = header_line(&mut reader, &mut consumed, deadline)?;
    let parts = first.split(' ').collect::<Vec<_>>();
    ensure!(
        parts.len() == 3
            && parts[0] == "POST"
            && parts[2] == "HTTP/1.1"
            && parts[1].starts_with('/')
            && parts[1].bytes().all(|byte| byte.is_ascii_graphic()),
        "invalid HTTP request line"
    );
    let path = parts[1].to_owned();
    loop {
        let line = header_line(&mut reader, &mut consumed, deadline)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid HTTP header"))?;
        ensure!(
            !name.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid HTTP header name"
        );
        ensure!(
            headers
                .insert(name.to_ascii_lowercase(), value.trim().to_owned())
                .is_none(),
            "duplicate HTTP header"
        );
    }
    ensure!(
        !headers.contains_key("transfer-encoding"),
        "chunked requests unsupported"
    );
    ensure!(
        headers.get("host").is_some_and(|host| !host.is_empty()),
        "HTTP Host required"
    );
    let length: usize = headers
        .get("content-length")
        .ok_or_else(|| anyhow::anyhow!("HTTP Content-Length required"))?
        .parse()?;
    ensure!(
        length > 0 && length <= max_body_bytes,
        "worker HTTP body budget exceeded"
    );
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("HTTP request deadline elapsed"))?;
    reader.get_mut().set_read_timeout(Some(remaining))?;
    let mut body = Vec::with_capacity(length);
    reader.take(length as u64).read_to_end(&mut body)?;
    ensure!(
        body.len() == length && Instant::now() <= deadline,
        "incomplete or expired HTTP body"
    );
    let connection = headers
        .get("connection")
        .map(|value| value.to_ascii_lowercase());
    let keep_alive = match connection.as_deref() {
        Some("close") => false,
        // HTTP/1.1 keeps the connection open unless the peer asks to close.
        _ => true,
    };
    Ok(Some(Request {
        path,
        authorization: headers.remove("authorization").unwrap_or_default(),
        body,
        keep_alive,
    }))
}

pub(super) fn reply(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    keep_alive: bool,
) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut output = std::io::BufWriter::new(&mut *stream);
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Error",
    };
    write!(
        output,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n",
        body.len(),
        if keep_alive { "keep-alive" } else { "close" }
    )?;
    output.write_all(body)?;
    output.flush()?;
    Ok(())
}

fn header_line(
    reader: &mut BufReader<&mut TcpStream>,
    consumed: &mut usize,
    deadline: Instant,
) -> Result<String> {
    const MAX_HEADERS: usize = 32 * 1024;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| anyhow::anyhow!("HTTP header deadline elapsed"))?;
    reader.get_mut().set_read_timeout(Some(remaining))?;
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take((MAX_HEADERS - *consumed + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    *consumed += bytes.len();
    ensure!(
        *consumed <= MAX_HEADERS && bytes.ends_with(b"\r\n"),
        "invalid or oversized HTTP headers"
    );
    bytes.truncate(bytes.len() - 2);
    let text = String::from_utf8(bytes)?;
    ensure!(
        text.bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ' || byte == b'\t'),
        "invalid HTTP header bytes"
    );
    Ok(text)
}
