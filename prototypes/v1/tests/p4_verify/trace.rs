use anyhow::{ensure, Context, Result};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

pub struct Trace {
    pub address: String,
    stopped: Arc<AtomicBool>,
    driver: Option<thread::JoinHandle<()>>,
}

impl Trace {
    pub fn start(target: String) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let stopped = Arc::new(AtomicBool::new(false));
        let active = stopped.clone();
        let driver = thread::spawn(move || {
            let mut requests = Vec::new();
            while !active.load(Ordering::SeqCst) {
                requests.retain(|request: &thread::JoinHandle<()>| !request.is_finished());
                match listener.accept() {
                    Ok((stream, _)) => {
                        let target = target.clone();
                        requests.push(thread::spawn(move || {
                            if let Err(error) = forward(stream, &target) {
                                eprintln!("P4_RPC_TRACE forward_error={error}");
                            }
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2))
                    }
                    Err(_) => break,
                }
            }
            for request in requests {
                let _ = request.join();
            }
        });
        Ok(Self {
            address,
            stopped,
            driver: Some(driver),
        })
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        if let Some(driver) = self.driver.take() {
            let _ = driver.join();
        }
    }
}

fn forward(mut client: TcpStream, target: &str) -> Result<()> {
    client.set_nonblocking(false)?;
    client.set_read_timeout(Some(Duration::from_secs(8)))?;
    client.set_write_timeout(Some(Duration::from_secs(8)))?;
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        ensure!(headers.len() < 32768, "trace header limit");
        let mut byte = [0];
        client.read_exact(&mut byte).with_context(|| {
            format!(
                "trace request header read failed header_bytes={}",
                headers.len()
            )
        })?;
        headers.push(byte[0]);
    }
    let text = std::str::from_utf8(&headers)?;
    let length = text
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .context("trace Content-Length missing")?
        .1
        .trim()
        .parse::<usize>()?;
    ensure!(length <= 32 * 1024 * 1024, "trace body limit");
    let mut body = vec![0; length];
    client
        .read_exact(&mut body)
        .context("trace request body read failed")?;
    let label = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(value) => format!(
            "{}/{}",
            value["command"]["command"].as_str().unwrap_or("unknown"),
            value["command"]["operation"]["operation"]
                .as_str()
                .unwrap_or("none")
        ),
        Err(_) => "invalid-json".into(),
    };
    let decoded =
        serde_json::from_slice::<peri_loom_v1::authority_rpc::protocol::RpcRequest>(&body)
            .map(|_| "ok".to_owned())
            .unwrap_or_else(|error| error.to_string());
    let mut upstream = TcpStream::connect(target)?;
    upstream.set_read_timeout(Some(Duration::from_secs(8)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(8)))?;
    upstream.write_all(&headers)?;
    upstream.write_all(&body)?;
    let mut response = Vec::new();
    let read = upstream.read_to_end(&mut response);
    let status = String::from_utf8_lossy(&response)
        .split_whitespace()
        .nth(1)
        .unwrap_or("empty")
        .to_owned();
    eprintln!(
        "P4_RPC_TRACE operation={label} request_bytes={length} schema={decoded} status={status}"
    );
    if status != "200" || read.is_err() || decoded != "ok" {
        let separator = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
            .unwrap_or(response.len());
        let error_body = &response[separator..];
        let error_body = &error_body[..error_body.len().min(1024)];
        eprintln!("P4_RPC_TRACE operation={label} request_bytes={length} schema={decoded} status={status} read_error={:?} error_body={}",
            read.as_ref().err().map(std::io::Error::kind), String::from_utf8_lossy(error_body));
    }
    read.context("trace upstream response read failed")?;
    client.write_all(&response)?;
    client.flush()?;
    Ok(())
}
