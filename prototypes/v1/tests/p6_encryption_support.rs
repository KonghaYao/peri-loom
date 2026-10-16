#[path = "p4_remote_helpers/harness.rs"]
#[allow(dead_code)]
pub mod harness;

use anyhow::{Context, Result};
use peri_loom_v1::{authority_rpc::RemoteAuthorityProvider, encryption::BrokerDatabaseKeyProvider};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub struct Broker {
    pub endpoint: String,
    pub token: String,
    pub mode: Arc<AtomicUsize>,
    pub calls: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl Broker {
    pub fn new(service: &harness::Harness, remote: Arc<RemoteAuthorityProvider>) -> Result<Self> {
        service.verify_preconditions()?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}/", listener.local_addr()?);
        let token = harness::token()?;
        let expected = token.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let mode = Arc::new(AtomicUsize::new(0));
        let configured = mode.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let clock = service.clock.clone();
        let owner = service.owner.clone();
        let thread = thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(accepted) => accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                stream.set_nonblocking(false)?;
                stream.set_read_timeout(Some(Duration::from_secs(3)))?;
                let mut bytes = Vec::new();
                let split = loop {
                    let mut byte = [0];
                    stream.read_exact(&mut byte)?;
                    bytes.push(byte[0]);
                    if bytes.ends_with(b"\r\n\r\n") {
                        break bytes.len();
                    }
                    anyhow::ensure!(bytes.len() < 16384, "test header oversized");
                };
                let headers = std::str::from_utf8(&bytes)?.to_ascii_lowercase();
                assert!(headers.starts_with("post /internal/key http/1.1\r\n"));
                assert!(headers.contains(&format!(
                    "authorization: bearer {}\r\n",
                    expected.to_ascii_lowercase()
                )));
                let size: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .context("missing content length")?
                    .parse()?;
                bytes.resize(split + size, 0);
                stream.read_exact(&mut bytes[split..])?;
                let request: Value = serde_json::from_slice(&bytes[split..])?;
                assert_eq!(request.as_object().unwrap().len(), 5);
                assert_eq!(request["version"], 1);
                assert_eq!(request["database"], request["binding"]["database"]);
                let binding = serde_json::from_value(request["binding"].clone())?;
                let role = serde_json::from_value(request["access_role"].clone())?;
                owner.validate_lease(&binding, role)?;
                counted.fetch_add(1, Ordering::SeqCst);
                let selected = configured.load(Ordering::SeqCst);
                if selected == 1 {
                    stream.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")?;
                    continue;
                }
                let mut result = json!({
                    "descriptor_id": "descriptor-a", "descriptor_revision": 2,
                    "domain_id": "domain-a", "dek_id": "dek-a", "cipher": "aegis256",
                    "dek_hex": "19".repeat(32), "validated_binding": binding,
                    "access_role": role, "valid_for_ms": 1000,
                });
                match selected {
                    2 => result["validated_binding"]["worker_id"] = json!("worker-b"),
                    3 => result["cipher"] = json!("aes256gcm"),
                    4 => result["dek_hex"] = json!("19".repeat(16)),
                    5 => result["domain_id"] = json!("changed-domain"),
                    6 => {
                        clock.0.fetch_add(996, Ordering::SeqCst);
                    }
                    7 => remote.remove_lease(&binding.database, binding.epoch)?,
                    8 => result["validated_binding"]["root_incarnation"] = json!("wrong-root"),
                    9 => {
                        result["validated_binding"]["database"]["namespace"]["database"] =
                            json!("wrong-db")
                    }
                    10 => result["validated_binding"]["epoch"] = json!(99),
                    11 => result["descriptor_revision"] = json!(1),
                    12 => result["dek_hex"] = json!("29".repeat(32)),
                    13 => result["access_role"] = json!("read_only"),
                    14 => {
                        clock.0.fetch_sub(1, Ordering::SeqCst);
                    }
                    15 => {
                        clock.0.fetch_add(10001, Ordering::SeqCst);
                    }
                    16 => result["valid_for_ms"] = json!(5),
                    17 => result["descriptor_id"] = json!("different-descriptor"),
                    18 => result["dek_id"] = json!("different-dek"),
                    19 => result["descriptor_revision"] = json!(3),
                    20 => {
                        remote.install_lease(
                            peri_loom_v1::ownership::types::GrantReply {
                                binding: binding.clone(),
                                remaining_ttl_ms: 1000,
                            },
                            peri_loom_v1::ownership::types::AccessRole::ReadOnly,
                        )?;
                    }
                    23 => result["validated_binding"]["authority_term"] = json!(99),
                    24 => result["validated_binding"]["branch_incarnation"] = json!(99),
                    25 => result["validated_binding"]["lease_id"] = json!("wrong-lease"),
                    26 => result["validated_binding"]["grant_id"] = json!("wrong-grant"),
                    27 => result["validated_binding"]["database"]["branch"] = json!("wrong-branch"),
                    28 => {
                        result["validated_binding"]["database"]["namespace"]["tenant"] =
                            json!("wrong-tenant")
                    }
                    29 => {
                        use peri_loom_v1::ownership::types::{
                            LeaseIntent, RenewIntent, RenewRequest,
                        };
                        let intent = RenewIntent {
                            binding: binding.clone(),
                            sequence: binding.renewal_sequence + 1,
                            ttl_ms: 10000,
                        };
                        let nonce = owner.challenge(LeaseIntent::Renew(intent.clone()))?;
                        let renewed = owner.renew(RenewRequest {
                            request_id: harness::token()?,
                            nonce: nonce.nonce,
                            intent,
                        })?;
                        remote.install_lease(renewed.clone(), role)?;
                        result["validated_binding"] = serde_json::to_value(renewed.binding)?;
                    }
                    30 => result["valid_for_ms"] = json!(0),
                    31 => result["valid_for_ms"] = json!(1001),
                    32 => result["dek_hex"] = json!("zz".repeat(32)),
                    33 => result["descriptor_id"] = json!(""),
                    34 => result["validated_binding"]["role"] = json!("read_only"),
                    _ => (),
                }
                if selected != 15 {
                    let _ = owner.validate_lease(&binding, role);
                }
                let mut reply = json!({ "version": 1, "request_id": request["request_id"], "ok": true, "result": result, "error": null });
                if selected == 21 {
                    reply["request_id"] = json!("replay-id");
                }
                if selected == 22 {
                    reply["unexpected"] = json!(true);
                }
                let body = serde_json::to_vec(&reply)?;
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())?;
                stream.write_all(&body)?;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            token,
            mode,
            calls,
            stop,
            thread: Some(thread),
        })
    }

    pub fn provider(
        &self,
        service: &harness::Harness,
        remote: Arc<RemoteAuthorityProvider>,
    ) -> Result<Arc<BrokerDatabaseKeyProvider>> {
        BrokerDatabaseKeyProvider::new(
            remote,
            &self.endpoint,
            self.token.clone(),
            service.clock.clone(),
        )
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            if !std::thread::panicking() {
                thread
                    .join()
                    .expect("mock broker panic")
                    .expect("mock broker failure");
            }
        }
    }
}
