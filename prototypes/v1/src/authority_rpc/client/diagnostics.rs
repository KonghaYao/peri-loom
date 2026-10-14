use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::Arc;

const CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionCause {
    TransitionActive,
    AdmissionClosed,
    Unavailable,
    ExpectedEpoch,
    NonceMissing,
    NonceStaleOrMismatch,
    NonceOwnershipChanged,
    LeaseExpired,
    LeaseUnavailable,
    LeaseIdentity,
    AuthorityTerm,
    NamespaceBlocked,
    NonceCapacity,
    Unclassified,
}

fn classify(message: &str) -> RejectionCause {
    match message {
        "Retryable admission transition busy" => RejectionCause::TransitionActive,
        "Retryable admission closed" => RejectionCause::AdmissionClosed,
        "Unavailable admission gate"
        | "Unavailable namespace registry"
        | "Unavailable lease memory"
        | "Unavailable lease clock"
        | "Unavailable monotonic clock regression" => RejectionCause::Unavailable,
        "Fenced expected epoch" => RejectionCause::ExpectedEpoch,
        "Unauthorized nonce missing" => RejectionCause::NonceMissing,
        "Unauthorized stale or mismatched nonce" => RejectionCause::NonceStaleOrMismatch,
        "Fenced nonce expected ownership changed" => RejectionCause::NonceOwnershipChanged,
        "LeaseExpired" | "LeaseExpired grant completed too late" => RejectionCause::LeaseExpired,
        "Fenced lease unavailable" => RejectionCause::LeaseUnavailable,
        "Fenced lease identity" | "Fenced grant replay" => RejectionCause::LeaseIdentity,
        "Fenced authority term" => RejectionCause::AuthorityTerm,
        "UnknownOutcome namespace blocked" => RejectionCause::NamespaceBlocked,
        "nonce capacity exceeded" => RejectionCause::NonceCapacity,
        _ => RejectionCause::Unclassified,
    }
}

#[derive(Default)]
struct State {
    sequence: AtomicU64,
    inflight: AtomicUsize,
    unavailable: AtomicBool,
    capture_active: AtomicBool,
}

struct Event {
    sequence: u64,
    cause: RejectionCause,
}

pub struct RejectionObserver {
    sender: SyncSender<Event>,
    state: Arc<State>,
}

pub struct RejectionReceiver {
    receiver: Receiver<Event>,
    state: Arc<State>,
}

pub struct GrantCapture {
    sequence: u64,
    clean: bool,
    state: Arc<State>,
    owned: bool,
}

impl Drop for GrantCapture {
    fn drop(&mut self) {
        if self.owned {
            self.state.capture_active.store(false, Ordering::SeqCst);
        }
    }
}

pub fn rejection_observer() -> (RejectionObserver, RejectionReceiver) {
    let (sender, receiver) = sync_channel(CAPACITY);
    let state = Arc::new(State::default());
    (
        RejectionObserver {
            sender,
            state: state.clone(),
        },
        RejectionReceiver { receiver, state },
    )
}

pub(super) struct GrantObservation<'observer> {
    observer: &'observer RejectionObserver,
    sequence: u64,
}

impl RejectionObserver {
    pub(super) fn begin_grant(&self) -> GrantObservation<'_> {
        if self.state.inflight.fetch_add(1, Ordering::SeqCst) != 0 {
            self.state.unavailable.store(true, Ordering::SeqCst);
        }
        let (sequence, overflow) = self
            .state
            .sequence
            .fetch_add(1, Ordering::SeqCst)
            .overflowing_add(1);
        if overflow {
            self.state.unavailable.store(true, Ordering::SeqCst);
        }
        GrantObservation {
            observer: self,
            sequence,
        }
    }
}

impl GrantObservation<'_> {
    pub(super) fn reject(&self, message: &str) {
        if self
            .observer
            .sender
            .try_send(Event {
                sequence: self.sequence,
                cause: classify(message),
            })
            .is_err()
        {
            self.observer
                .state
                .unavailable
                .store(true, Ordering::SeqCst);
        }
    }
}

impl Drop for GrantObservation<'_> {
    fn drop(&mut self) {
        self.observer.state.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RejectionReceiver {
    pub fn begin(&mut self) -> GrantCapture {
        let owned = self
            .state
            .capture_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        let clean = owned
            && matches!(self.receiver.try_recv(), Err(TryRecvError::Empty))
            && self.state.inflight.load(Ordering::SeqCst) == 0
            && !self.state.unavailable.load(Ordering::SeqCst);
        if !clean {
            self.state.unavailable.store(true, Ordering::SeqCst);
        }
        GrantCapture {
            sequence: self.state.sequence.load(Ordering::SeqCst),
            clean,
            state: self.state.clone(),
            owned,
        }
    }

    pub fn finish(&mut self, capture: GrantCapture) -> Option<RejectionCause> {
        let Some(expected) = capture.sequence.checked_add(1) else {
            self.state.unavailable.store(true, Ordering::SeqCst);
            return None;
        };
        if !capture.clean
            || self.state.unavailable.load(Ordering::SeqCst)
            || self.state.inflight.load(Ordering::SeqCst) != 0
            || self.state.sequence.load(Ordering::SeqCst) != expected
        {
            self.state.unavailable.store(true, Ordering::SeqCst);
            return None;
        }
        let event = self.receiver.try_recv().ok()?;
        if event.sequence != expected
            || !matches!(self.receiver.try_recv(), Err(TryRecvError::Empty))
        {
            self.state.unavailable.store(true, Ordering::SeqCst);
            return None;
        }
        Some(event.cause)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use serde_json::{json, Value};

    use super::{classify, rejection_observer, RejectionCause, CAPACITY};
    use crate::authority_rpc::client::AuthorityClient;
    use crate::authority_rpc::protocol::{AuthorityCommand, MAX_RPC_BYTES};
    use crate::object_store::PreconditionFailed;

    type MockServer = (
        String,
        Arc<AtomicUsize>,
        std::thread::JoinHandle<Result<()>>,
    );

    fn grant() -> Result<AuthorityCommand> {
        Ok(serde_json::from_value(
            json!({"command":"grant","request":{
            "request_id":"grant_probe","nonce":"synthetic_nonce","intent":{
            "database":{"namespace":{"tenant":"unit","database":"database"},"branch":"main"},
            "worker_id":"worker","epoch":2,"expected_epoch":1,"lease_id":"lease",
            "ttl_ms":1000,"role":"writer"}}}),
        )?)
    }

    fn rejection(message: &str) -> Value {
        json!({"version":1,"request_id":"grant_probe","ok":false,
            "result":null,"error":message,"precondition_failed":false})
    }

    fn server(body: Value, headers: String) -> Result<MockServer> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = format!("http://{}/", listener.local_addr()?);
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        let worker = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::yield_now()
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            crate::authority_rpc::http::read_request(&mut stream)?;
            counted.fetch_add(1, Ordering::SeqCst);
            let bytes = serde_json::to_vec(&body)?;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nConnection: close\r\n{headers}\r\n"
            )?;
            stream.write_all(&bytes)?;
            Ok(())
        });
        Ok((address, requests, worker))
    }

    #[test]
    fn validated_rejection_uses_same_http_one_send_and_exact_opaque_error() -> Result<()> {
        let (address, requests, worker) = server(
            rejection("Retryable admission transition busy"),
            String::new(),
        )?;
        let (observer, mut receiver) = rejection_observer();
        let client = AuthorityClient::new_with_rejection_observer(
            &address,
            "synthetic_control_credential_0123456789".into(),
            observer,
        )?;
        let original_http = std::ptr::addr_of!(client.http);
        let capture = receiver.begin();
        let error = client.call(grant()?).unwrap_err();
        assert_eq!(error.to_string(), "authority operation rejected");
        assert_eq!(std::ptr::addr_of!(client.http), original_http);
        assert_eq!(
            receiver.finish(capture),
            Some(RejectionCause::TransitionActive)
        );
        worker.join().unwrap()?;
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn malformed_or_precondition_replies_never_emit_rejection() -> Result<()> {
        let mut wrong_id = rejection("Retryable admission transition busy");
        wrong_id["request_id"] = json!("wrong");
        let mut wrong_version = rejection("Retryable admission transition busy");
        wrong_version["version"] = json!(2);
        let mut missing_result = rejection("Retryable admission transition busy");
        missing_result.as_object_mut().unwrap().remove("result");
        let mut inconsistent = rejection("Retryable admission transition busy");
        inconsistent["result"] = json!(true);
        let mut bad_success = rejection("Retryable admission transition busy");
        bad_success["ok"] = json!(true);
        let mut missing_error = rejection("Retryable admission transition busy");
        missing_error["error"] = Value::Null;
        let mut extra_field = rejection("Retryable admission transition busy");
        extra_field["private"] = json!("private token namespace SQL");
        let mut precondition = rejection("private token namespace SQL");
        precondition["precondition_failed"] = json!(true);
        for (body, headers, typed) in [
            (wrong_id, String::new(), false),
            (wrong_version, String::new(), false),
            (missing_result, String::new(), false),
            (inconsistent, String::new(), false),
            (bad_success, String::new(), false),
            (missing_error, String::new(), false),
            (extra_field, String::new(), false),
            (precondition, String::new(), true),
            (
                rejection("private token namespace SQL"),
                format!("Content-Length: {}\r\n", MAX_RPC_BYTES + 1),
                false,
            ),
        ] {
            let (address, requests, worker) = server(body, headers)?;
            let (observer, mut receiver) = rejection_observer();
            let client = AuthorityClient::new_with_rejection_observer(
                &address,
                "synthetic_control_credential_0123456789".into(),
                observer,
            )?;
            let capture = receiver.begin();
            let error = client.call(grant()?).unwrap_err();
            assert_eq!(error.is::<PreconditionFailed>(), typed);
            assert!(receiver.finish(capture).is_none());
            assert!(!format!("{error:#}").contains("private token"));
            worker.join().unwrap()?;
            assert_eq!(requests.load(Ordering::SeqCst), 1);
        }
        Ok(())
    }

    #[test]
    fn default_off_and_full_or_disconnected_observers_preserve_errors() -> Result<()> {
        for mode in ["default", "full", "disconnected"] {
            let (address, requests, worker) =
                server(rejection("private token namespace SQL"), String::new())?;
            let (observer, receiver) = rejection_observer();
            if mode == "full" {
                for _ in 0..CAPACITY {
                    observer
                        .begin_grant()
                        .reject("Retryable admission transition busy");
                }
            }
            if mode == "disconnected" {
                drop(receiver);
            }
            let client = if mode == "default" {
                AuthorityClient::new(&address, "synthetic_control_credential_0123456789".into())?
            } else {
                AuthorityClient::new_with_rejection_observer(
                    &address,
                    "synthetic_control_credential_0123456789".into(),
                    observer,
                )?
            };
            assert_eq!(client.observer.is_none(), mode == "default");
            let error = client.call(grant()?).unwrap_err();
            assert_eq!(format!("{error:#}"), "authority operation rejected");
            if let Some(observer) = &client.observer {
                assert!(observer.state.unavailable.load(Ordering::SeqCst));
            }
            worker.join().unwrap()?;
            assert_eq!(requests.load(Ordering::SeqCst), 1);
        }
        Ok(())
    }

    #[test]
    fn stale_overlap_missing_and_extra_calls_are_never_attributed() {
        for mode in ["stale", "overlap", "missing", "extra"] {
            let (observer, mut receiver) = rejection_observer();
            if mode == "stale" {
                observer
                    .begin_grant()
                    .reject("Retryable admission transition busy");
            }
            let capture = receiver.begin();
            let first = observer.begin_grant();
            if mode == "overlap" {
                observer.begin_grant().reject("Retryable admission closed");
            }
            if mode != "missing" {
                first.reject("Retryable admission transition busy");
            }
            drop(first);
            if mode == "extra" {
                drop(observer.begin_grant());
            }
            assert!(receiver.finish(capture).is_none());
        }
    }

    #[test]
    fn overlapping_capture_cannot_attribute_another_grants_rejection() {
        let (observer, mut receiver) = rejection_observer();
        let owner = receiver.begin();
        let failed_challenge = receiver.begin();
        observer
            .begin_grant()
            .reject("Retryable admission transition busy");
        assert!(receiver.finish(failed_challenge).is_none());
        assert!(receiver.finish(owner).is_none());
    }

    #[test]
    fn invalid_capture_drop_cannot_release_owner_reservation() {
        let (observer, mut receiver) = rejection_observer();
        let owner = receiver.begin();
        let invalid = receiver.begin();
        drop(invalid);
        assert!(observer.state.capture_active.load(Ordering::SeqCst));
        drop(owner);
        let later = receiver.begin();
        assert!(observer.state.capture_active.load(Ordering::SeqCst));
        drop(later);
        assert!(!observer.state.capture_active.load(Ordering::SeqCst));
    }

    #[test]
    fn abandoned_and_early_return_captures_release_without_accepting_stale_events() {
        for mode in ["abandoned", "stale", "missing", "overflow"] {
            let (observer, mut receiver) = rejection_observer();
            if mode == "overflow" {
                observer.state.sequence.store(u64::MAX, Ordering::SeqCst);
            }
            let capture = receiver.begin();
            if mode == "stale" {
                observer
                    .begin_grant()
                    .reject("Retryable admission transition busy");
            }
            if mode == "abandoned" || mode == "stale" {
                drop(capture);
            } else {
                assert!(receiver.finish(capture).is_none());
            }
            assert!(!observer.state.capture_active.load(Ordering::SeqCst));
            let later = receiver.begin();
            assert_eq!(later.clean, mode == "abandoned");
            drop(later);
            assert!(!observer.state.capture_active.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn classifications_are_exact_closed_and_do_not_leak_private_suffixes() {
        for (message, expected) in [
            (
                "Retryable admission transition busy",
                RejectionCause::TransitionActive,
            ),
            (
                "Retryable admission closed",
                RejectionCause::AdmissionClosed,
            ),
            ("Unavailable admission gate", RejectionCause::Unavailable),
            (
                "Retryable admission transition busy private token",
                RejectionCause::Unclassified,
            ),
            (
                "private namespace: Retryable admission transition busy",
                RejectionCause::Unclassified,
            ),
        ] {
            let cause = classify(message);
            assert_eq!(cause, expected);
            assert!(!format!("{cause:?}").contains("private"));
        }
    }
}
