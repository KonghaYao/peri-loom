//! Connection reuse for the authority client, and the evidence that reuse never
//! turns a transport failure into a silent resend.
//!
//! The fixture speaks the real authority framing on a loopback socket and keeps
//! a ledger of every request it saw. Counting what the *server* observed is the
//! point: a client that replayed a request would show up as one request id
//! appearing twice, which no amount of client-side bookkeeping could hide.

use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;

use crate::authority_rpc::http;
use crate::authority_rpc::ingress::{Acceptor, Wakeup};
use crate::authority_rpc::protocol::{AuthorityCommand, RpcRequest, RpcResponse, PROTOCOL_VERSION};

use super::{AuthorityClient, Connections, IDLE_CONNECTIONS, IDLE_TIMEOUT, LIVE_CONNECTIONS};

const TOKEN: &str = "authority-client-reuse-fixture-token-0000001";

/// What the fixture does with the connection it accepted.
#[derive(Clone, Copy, Debug)]
enum Behavior {
    /// Answer every request and keep the socket open.
    Serve,
    /// Answer the first request, then close without waiting for another.
    CloseAfterFirst,
    /// Read the first request, then close without answering it.
    DropAfterFirst,
    /// Read the first request, then half-close and keep reading.
    HalfCloseAfterFirst,
}

#[derive(Debug)]
struct Observed {
    request_id: String,
    authorization: String,
}

#[derive(Default)]
struct Ledger {
    accepted: AtomicUsize,
    observed: Mutex<Vec<Observed>>,
}

impl Ledger {
    fn record(&self, observed: Observed) {
        self.observed
            .lock()
            .expect("fixture ledger is never poisoned")
            .push(observed);
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    fn observed(&self) -> Vec<(String, String)> {
        self.observed
            .lock()
            .expect("fixture ledger is never poisoned")
            .iter()
            .map(|entry| (entry.request_id.clone(), entry.authorization.clone()))
            .collect()
    }

    fn occurrences(&self, request_id: &str) -> usize {
        self.observed()
            .iter()
            .filter(|(identifier, _)| identifier == request_id)
            .count()
    }
}

struct Fixture {
    address: SocketAddr,
    ledger: Arc<Ledger>,
    stop: Arc<AtomicBool>,
    wakeup: Wakeup,
    acceptor: Option<thread::JoinHandle<()>>,
}

impl Fixture {
    fn start(behaviors: Vec<Behavior>) -> Result<Self> {
        let acceptor = Acceptor::bind("127.0.0.1:0".parse()?)?;
        let address = acceptor.local_addr()?;
        let wakeup = acceptor.wakeup();
        let ledger = Arc::new(Ledger::default());
        let stop = Arc::new(AtomicBool::new(false));
        let thread_ledger = ledger.clone();
        let thread_stop = stop.clone();
        let acceptor_thread = thread::spawn(move || {
            let mut index = 0;
            'accept: while !thread_stop.load(Ordering::SeqCst) {
                if acceptor.park().is_err() {
                    return;
                }
                while !thread_stop.load(Ordering::SeqCst) {
                    let stream = match acceptor.take() {
                        Ok(Some(stream)) => stream,
                        Ok(None) => continue 'accept,
                        Err(_) => return,
                    };
                    let behavior = behaviors.get(index).copied().unwrap_or(Behavior::Serve);
                    index += 1;
                    thread_ledger.accepted.fetch_add(1, Ordering::SeqCst);
                    let ledger = thread_ledger.clone();
                    let stop = thread_stop.clone();
                    thread::spawn(move || {
                        let _ = handle(stream, behavior, &ledger, &stop);
                    });
                }
            }
        });
        Ok(Self {
            address,
            ledger,
            stop,
            wakeup,
            acceptor: Some(acceptor_thread),
        })
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn client(&self) -> Result<Arc<AuthorityClient>> {
        AuthorityClient::new(&self.endpoint(), TOKEN.to_owned())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.wakeup.wake();
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
    }
}

fn handle(
    mut stream: TcpStream,
    behavior: Behavior,
    ledger: &Ledger,
    stop: &AtomicBool,
) -> Result<()> {
    let mut first = true;
    loop {
        let request = match http::read_request_optional(
            &mut stream,
            if first {
                None
            } else {
                Some(http::KEEP_ALIVE_IDLE)
            },
            stop,
        )? {
            Some(request) => request,
            None => return Ok(()),
        };
        first = false;
        let parsed: RpcRequest =
            serde_json::from_slice(&request.body).context("fixture rejected the request body")?;
        ledger.record(Observed {
            request_id: parsed.request_id.clone(),
            authorization: request.authorization.clone(),
        });
        match behavior {
            Behavior::Serve | Behavior::CloseAfterFirst => {
                let keep_alive = matches!(behavior, Behavior::Serve);
                let body = serde_json::to_vec(&RpcResponse {
                    version: PROTOCOL_VERSION,
                    request_id: parsed.request_id,
                    ok: true,
                    result: Some(json!({"served": true})),
                    error: None,
                    precondition_failed: false,
                })?;
                http::reply_with(&mut stream, 200, &body, keep_alive)?;
                if !keep_alive {
                    return Ok(());
                }
            }
            Behavior::DropAfterFirst => return Ok(()),
            Behavior::HalfCloseAfterFirst => {
                stream.shutdown(Shutdown::Write)?;
                thread::sleep(Duration::from_millis(200));
                return Ok(());
            }
        }
    }
}

/// A pooled socket carries a whole sequence of calls: the authority sees one
/// connection, not one per request.
#[test]
fn pooled_socket_serves_many_calls_on_a_single_connection() -> Result<()> {
    let fixture = Fixture::start(vec![Behavior::Serve])?;
    let client = fixture.client()?;
    for _ in 0..6 {
        assert_eq!(
            client.call(AuthorityCommand::GetMetrics)?["served"],
            json!(true)
        );
    }
    let observed = fixture.ledger.observed();
    assert_eq!(observed.len(), 6);
    let mut identifiers = observed
        .iter()
        .map(|(identifier, _)| identifier.clone())
        .collect::<Vec<_>>();
    identifiers.sort();
    identifiers.dedup();
    assert_eq!(identifiers.len(), 6, "each call carries its own identity");
    assert_eq!(fixture.ledger.accepted(), 1, "calls did not share a socket");
    Ok(())
}

/// Connection reuse is not a way for a request to ride on the previous
/// request's credential: every call on the shared socket authenticates.
#[test]
fn every_call_on_a_reused_socket_authenticates() -> Result<()> {
    let fixture = Fixture::start(vec![Behavior::Serve])?;
    let client = fixture.client()?;
    for _ in 0..3 {
        client.call(AuthorityCommand::GetMetrics)?;
    }
    assert_eq!(fixture.ledger.accepted(), 1);
    let observed = fixture.ledger.observed();
    assert_eq!(observed.len(), 3);
    for (_, authorization) in observed {
        assert_eq!(authorization, format!("Bearer {TOKEN}"));
    }
    Ok(())
}

/// A socket the authority retired while it was parked is replaced, and the
/// caller never sees the stale socket as a failed call.
#[test]
fn socket_retired_while_parked_is_replaced_without_replaying() -> Result<()> {
    let fixture = Fixture::start(vec![Behavior::CloseAfterFirst, Behavior::Serve])?;
    let client = fixture.client()?;
    client.call(AuthorityCommand::GetMetrics)?;
    thread::sleep(Duration::from_millis(200));
    client.call(AuthorityCommand::GetMetrics)?;
    let observed = fixture.ledger.observed();
    assert_eq!(observed.len(), 2);
    for (identifier, _) in &observed {
        assert_eq!(
            fixture.ledger.occurrences(identifier),
            1,
            "request {identifier} was replayed"
        );
    }
    Ok(())
}

/// The core safety assertion: a connection that dies after the request was
/// serialized produces a definite failure, and the request is never sent again.
#[test]
fn connection_dropped_after_the_request_is_unknown_and_never_replayed() -> Result<()> {
    let fixture = Fixture::start(vec![Behavior::DropAfterFirst, Behavior::Serve])?;
    let client = fixture.client()?;
    let error = client
        .call(AuthorityCommand::GetMetrics)
        .expect_err("a dropped connection must not read as success");
    assert!(
        error.to_string().contains("outcome may be unknown"),
        "unexpected failure classification: {error}"
    );
    let observed = fixture.ledger.observed();
    assert_eq!(
        fixture.ledger.accepted(),
        1,
        "the client opened a second socket after the request was written"
    );
    assert_eq!(observed.len(), 1, "the request reached the authority twice");
    Ok(())
}

/// Half-close is the shape a real authority shutdown takes: the client sees a
/// clean EOF with no response, which is an unknown outcome, not a retry cue.
#[test]
fn half_closed_connection_is_unknown_and_never_replayed() -> Result<()> {
    let fixture = Fixture::start(vec![Behavior::HalfCloseAfterFirst, Behavior::Serve])?;
    let client = fixture.client()?;
    let error = client
        .call(AuthorityCommand::GetMetrics)
        .expect_err("a half-closed connection must not read as success");
    assert!(
        error.to_string().contains("outcome may be unknown"),
        "unexpected failure classification: {error}"
    );
    let observed = fixture.ledger.observed();
    assert_eq!(
        fixture.ledger.accepted(),
        1,
        "the client opened a second socket after the request was written"
    );
    assert_eq!(observed.len(), 1, "the request reached the authority twice");
    Ok(())
}

/// Reuse must not turn into unbounded growth. An unbounded client opens one
/// socket per caller; this one queues callers on its budget, so the sockets it
/// asks the authority for stay under `LIVE_CONNECTIONS` (checked out) plus
/// `IDLE_CONNECTIONS` (parked) plus the ones the pool is still handing back.
#[test]
fn concurrent_callers_stay_within_the_socket_budget() -> Result<()> {
    let fixture = Fixture::start(vec![Behavior::Serve])?;
    let client = fixture.client()?;
    let mut callers = Vec::new();
    for _ in 0..64 {
        let client = client.clone();
        callers.push(thread::spawn(move || {
            client.call(AuthorityCommand::GetMetrics)
        }));
    }
    for caller in callers {
        caller.join().expect("caller thread")?;
    }
    assert_eq!(fixture.ledger.observed().len(), 64);
    let accepted = fixture.ledger.accepted();
    let bound = LIVE_CONNECTIONS + IDLE_CONNECTIONS + LIVE_CONNECTIONS;
    assert!(
        accepted <= bound,
        "{accepted} sockets opened for a budget of {bound} across 64 callers"
    );
    Ok(())
}

/// The call budget itself is a hard bound, independent of how the pool behaves.
#[test]
fn call_slots_never_admit_more_than_the_limit() {
    let connections = Arc::new(Connections::new(4));
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut callers = Vec::new();
    for _ in 0..32 {
        let connections = connections.clone();
        let live = live.clone();
        let peak = peak.clone();
        callers.push(thread::spawn(move || {
            let slot = connections.acquire(Duration::from_secs(5)).expect("slot");
            let held = live.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(held, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(2));
            live.fetch_sub(1, Ordering::SeqCst);
            drop(slot);
        }));
    }
    for caller in callers {
        caller.join().expect("caller thread");
    }
    assert!(peak.load(Ordering::SeqCst) <= 4);
    assert!(
        connections.acquire(Duration::from_millis(1)).is_some(),
        "every slot must come back"
    );
}

/// The client must retire a parked socket before the authority does, so a
/// request never starts on a socket the peer has already decided to close.
#[test]
fn client_idle_timeout_precedes_the_authority_reclamation() {
    assert!(
        IDLE_TIMEOUT < http::KEEP_ALIVE_IDLE,
        "client parks sockets for {IDLE_TIMEOUT:?}, authority reclaims at {:?}",
        http::KEEP_ALIVE_IDLE
    );
}

/// The client's own bound is what the authority sizes its connection budget
/// against, so it has to stay a number.
#[test]
fn socket_budget_is_bounded_above() {
    let budget = LIVE_CONNECTIONS + IDLE_CONNECTIONS;
    assert!(
        budget <= crate::authority_rpc::server::MAX_CONNECTIONS,
        "a single client may want {budget} sockets"
    );
}
