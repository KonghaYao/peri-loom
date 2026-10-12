use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};

use super::protocol::MAX_RPC_BYTES;

pub(crate) struct Request {
    pub path: String,
    pub authorization: String,
    pub body: Vec<u8>,
}

pub(crate) fn read_request(stream: &mut TcpStream) -> Result<Request> {
    stream.set_nonblocking(false)?;
    let deadline = Instant::now() + Duration::from_secs(15);
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream);
    let mut headers = BTreeMap::new();
    let mut consumed = 0;
    let first = line(&mut reader, &mut consumed, deadline)?;
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
        let line = line(&mut reader, &mut consumed, deadline)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').context("invalid HTTP header")?;
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
        .context("HTTP Content-Length required")?
        .parse()?;
    ensure!(
        length > 0 && length <= MAX_RPC_BYTES,
        "invalid HTTP body size"
    );
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .context("HTTP request deadline elapsed")?;
    reader.get_mut().set_read_timeout(Some(remaining))?;
    let mut body = Vec::with_capacity(length);
    reader.take(length as u64).read_to_end(&mut body)?;
    ensure!(
        body.len() == length && Instant::now() <= deadline,
        "incomplete or expired HTTP body"
    );
    Ok(Request {
        path,
        authorization: headers.remove("authorization").unwrap_or_default(),
        body,
    })
}

fn line(
    reader: &mut BufReader<&mut TcpStream>,
    consumed: &mut usize,
    deadline: Instant,
) -> Result<String> {
    const MAX_HEADERS: usize = 32 * 1024;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .context("HTTP header deadline elapsed")?;
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

pub(crate) fn reply(stream: &mut TcpStream, status: u16, body: &[u8]) -> Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    write_reply(stream, status, body)
}

fn write_reply(writer: &mut impl Write, status: u16, body: &[u8]) -> Result<()> {
    let mut output = BufWriter::new(writer);
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        503 => "Service Unavailable",
        _ => "Error",
    };
    write!(output, "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())?;
    output.write_all(body)?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Writes {
        pieces: Vec<Vec<u8>>,
    }

    impl Write for Writes {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.pieces.push(bytes.to_vec());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn small_reply_does_not_fragment_header_and_body_into_tiny_writes() -> Result<()> {
        let body = br#"{"ok":true}"#;
        let mut output = Writes::default();
        write_reply(&mut output, 200, body)?;
        assert_eq!(output.pieces.len(), 1);
        assert!(output.pieces[0].starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(output.pieces[0].ends_with(body));
        assert!(String::from_utf8_lossy(&output.pieces[0]).contains("Content-Length: 11\r\n"));
        Ok(())
    }

    #[test]
    fn large_reply_preserves_body_and_content_length_without_unbounded_buffering() -> Result<()> {
        let body = vec![83; 16384];
        let mut output = Writes::default();
        write_reply(&mut output, 200, &body)?;
        let bytes = output.pieces.concat();
        let offset = bytes
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            + 4;
        assert_eq!(&bytes[offset..], body);
        assert!(String::from_utf8_lossy(&bytes[..offset]).contains("Content-Length: 16384\r\n"));
        Ok(())
    }
}
