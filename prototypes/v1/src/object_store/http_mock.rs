use peri_loom_v1::object_store::{S3Config, S3ObjectStore};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

pub(super) struct Request {
    pub(super) method: String,
    pub(super) target: String,
    pub(super) headers: BTreeMap<String, String>,
    pub(super) body: Vec<u8>,
}

pub(super) fn read_request(stream: &mut TcpStream) -> Request {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let boundary = loop {
        let mut buffer = [0; 4096];
        let count = stream.read(&mut buffer).unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8(bytes[..boundary].to_vec()).unwrap();
    let mut lines = head.lines();
    let first: Vec<_> = lines.next().unwrap().split_whitespace().collect();
    let headers: BTreeMap<_, _> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .map(|value| value.parse().unwrap())
        .unwrap_or(0);
    while bytes.len() < boundary + length {
        let mut buffer = [0; 4096];
        let count = stream.read(&mut buffer).unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    Request {
        method: first[0].into(),
        target: first[1].into(),
        headers,
        body: bytes[boundary..boundary + length].to_vec(),
    }
}

pub(super) fn reply(stream: &mut TcpStream, status: u16, headers: &[(&str, &str)], body: &[u8]) {
    write!(
        stream,
        "HTTP/1.1 {status} Mock\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .unwrap();
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").unwrap();
    }
    stream.write_all(b"\r\n").unwrap();
    stream.write_all(body).unwrap();
}

pub(super) fn config(endpoint: String) -> S3Config {
    S3Config {
        endpoint,
        region: "us-east-1".into(),
        bucket: "bucket".into(),
        access_key: "TESTACCESS".into(),
        secret_key: "never-log-this-secret".into(),
        session_token: Some("session-token".into()),
        timeout: Duration::from_secs(2),
        retries: 1,
    }
}

pub(super) fn mock(
    handler: impl FnOnce(TcpListener) + Send + 'static,
) -> (S3ObjectStore, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || handler(listener));
    (S3ObjectStore::new(config(endpoint)).unwrap(), worker)
}

pub(super) fn verify_signature(request: &Request) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    fn mac(key: &[u8], text: &str) -> Vec<u8> {
        let mut signer = Hmac::<Sha256>::new_from_slice(key).unwrap();
        signer.update(text.as_bytes());
        signer.finalize().into_bytes().to_vec()
    }
    let auth = &request.headers["authorization"];
    let names = auth
        .split("SignedHeaders=")
        .nth(1)
        .unwrap()
        .split(',')
        .next()
        .unwrap();
    let canonical_headers: String = names
        .split(';')
        .map(|name| format!("{name}:{}\n", request.headers[name]))
        .collect();
    let (path, query) = request
        .target
        .split_once('?')
        .unwrap_or((&request.target, ""));
    let canonical = format!(
        "{}\n{path}\n{query}\n{canonical_headers}\n{names}\n{}",
        request.method,
        hex(&Sha256::digest(&request.body))
    );
    let timestamp = &request.headers["x-amz-date"];
    let scope = format!("{}/us-east-1/s3/aws4_request", &timestamp[..8]);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let key = mac(b"AWS4never-log-this-secret", &timestamp[..8]);
    let key = mac(&key, "us-east-1");
    let key = mac(&key, "s3");
    let key = mac(&key, "aws4_request");
    assert!(auth.ends_with(&hex(&mac(&key, &to_sign))));
    assert_eq!(request.headers["x-amz-security-token"], "session-token");
}
