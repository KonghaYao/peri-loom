use super::*;
use crate::slow_diagnostics::{Context, Counts, RequestGuard};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

fn runtime(capacity: usize) -> (&'static Runtime, Receiver<Event>) {
    let (sender, receiver) = sync_channel(capacity);
    let runtime = Box::leak(Box::new(Runtime {
        origin: Instant::now(),
        sender,
        counts: Arc::new(Counts::default()),
    }));
    (runtime, receiver)
}

fn context(identifier: Option<&str>) -> RequestGuard {
    let previous = CONTEXT.with(|slot| {
        slot.replace(Context {
            request_id: identifier.map(str::to_owned),
            native: None,
        })
    });
    RequestGuard(Some(previous), PhantomData)
}

fn record(receiver: &Receiver<Event>) -> serde_json::Value {
    serde_json::to_value(receiver.try_recv().expect("one completion event")).unwrap()
}

#[test]
fn disabled_link_never_reads_context_command_or_clock() {
    CONTEXT.with(|slot| {
        let _borrow = slot.borrow_mut();
        let mut link = AuthorityRpcLink::start(
            None,
            || panic!("disabled link classified a command"),
            || panic!("disabled link read a clock"),
        );
        link.child("must-not-be-copied");
        link.phase(RpcPhase::Send);
        link.complete("ok");
        drop(link);
    });
}

#[test]
fn fast_success_records_exact_parent_child_and_never_asserts_complete() {
    let (runtime, receiver) = runtime(2);
    let _context = context(Some("worker-1"));
    let started = runtime.origin + Duration::from_millis(1);
    let mut link = AuthorityRpcLink::start(Some(runtime), || "get", || started);
    link.child("authority-2");
    link.phase(RpcPhase::Send);
    link.phase(RpcPhase::ResponseHeaders);
    link.complete("ok");
    link.finish_at(started, false);
    drop(link);
    let event = record(&receiver);
    assert_eq!(event["schema"], "peri-authority-rpc-link-v1");
    assert_eq!(event["event"], "authority_rpc_link");
    assert_eq!(event["request_id"], "authority-2");
    assert_eq!(event["parent_request_id"], "worker-1");
    assert_eq!(event["duration_ns"], 0);
    assert_eq!(event["start_mono_ns"], 1_000_000);
    assert_eq!(event["result"], "ok");
    assert_eq!(event["phase"], "complete");
    assert_eq!(event["link_available"], true);
    assert_eq!(event["send_invoked"], true);
    assert_eq!(event["response_headers_received"], true);
    assert_eq!(event["complete"], false);
    assert_eq!(event["evidence_class"], "non_harness");
    assert!(event["retry_count"].is_null());
    assert!(event["op_id"].is_null());
    assert!(receiver.try_recv().is_err(), "drop emitted a second record");
}

#[test]
fn early_id_failure_records_unassigned_child_without_inventing_delivery() {
    let (runtime, receiver) = runtime(1);
    let _context = context(Some("worker-parent"));
    let call = || -> anyhow::Result<()> {
        let _link = AuthorityRpcLink::start(Some(runtime), || "open_session", Instant::now);
        anyhow::bail!("an internal error string that must not appear in diagnostics")
    };
    let returned = call().unwrap_err();
    assert!(returned.to_string().starts_with("an internal error"));
    let event = record(&receiver);
    assert_eq!(event["parent_request_id"], "worker-parent");
    assert!(event["request_id"].is_null());
    assert_eq!(event["child_id_status"], "unassigned");
    assert_eq!(event["phase"], "request_id");
    assert_eq!(event["result"], "error_or_early_return");
    assert_eq!(event["send_invoked"], false);
    assert_eq!(event["link_available"], false);
    assert!(!event.to_string().contains("an internal error"));
}

#[test]
fn malformed_ids_are_omitted_without_relabeling_them_as_valid() {
    for bad in [
        "",
        "secret\nvalue",
        "SELECT secret FROM private",
        "plk1_private",
        "prefix-pldb1.private-suffix",
        &"x".repeat(129),
    ] {
        let (runtime, receiver) = runtime(1);
        let _context = context(Some(bad));
        let mut link = AuthorityRpcLink::start(Some(runtime), || "grant", Instant::now);
        link.child(bad);
        drop(link);
        let event = record(&receiver);
        assert!(event["request_id"].is_null());
        assert!(event["parent_request_id"].is_null());
        assert_eq!(event["child_id_status"], "rejected");
        assert_eq!(event["parent_id_status"], "rejected");
        assert_eq!(event["link_available"], false);
    }
}

#[test]
fn absent_parent_is_unlinked_and_not_guessed_from_nearby_requests() {
    let (runtime, receiver) = runtime(1);
    let _context = context(None);
    let mut link = AuthorityRpcLink::start(Some(runtime), || "get_metrics", Instant::now);
    link.child("metrics-call");
    link.complete("ok");
    drop(link);
    let event = record(&receiver);
    assert!(event["parent_request_id"].is_null());
    assert_eq!(event["parent_id_status"], "absent");
    assert_eq!(event["request_id"], "metrics-call");
    assert_eq!(event["link_available"], false);
}

#[test]
fn nested_scopes_keep_the_parent_captured_at_each_call_entry() {
    let (runtime, receiver) = runtime(3);
    let _outer = context(Some("outer-worker"));
    let mut outer_link = AuthorityRpcLink::start(Some(runtime), || "get", Instant::now);
    outer_link.child("outer-child");
    {
        let _inner = context(Some("inner-worker"));
        let mut inner_link = AuthorityRpcLink::start(Some(runtime), || "put", Instant::now);
        inner_link.child("inner-child");
        drop(inner_link);
        // Finish the outer call while another scope is current. The link must not steal it.
        drop(outer_link);
    }
    let inner = record(&receiver);
    let outer = record(&receiver);
    assert_eq!(inner["parent_request_id"], "inner-worker");
    assert_eq!(outer["parent_request_id"], "outer-worker");
    assert_eq!(inner["request_id"], "inner-child");
    assert_eq!(outer["request_id"], "outer-child");
    assert_eq!(super::super::request_id().as_deref(), Some("outer-worker"));
}

#[test]
fn errors_keep_the_last_boundary_and_separate_send_from_response() {
    let phases = [
        RpcPhase::ValidateRequest,
        RpcPhase::EncodeRequest,
        RpcPhase::Deadline,
        RpcPhase::AcquireSlot,
        RpcPhase::Send,
        RpcPhase::ResponseHeaders,
        RpcPhase::ReadBody,
        RpcPhase::DecodeResponse,
        RpcPhase::ValidateResponse,
    ];
    let (runtime, receiver) = runtime(1);
    let _context = context(Some("worker"));
    for (end, expected) in phases.iter().enumerate() {
        let mut link = AuthorityRpcLink::start(Some(runtime), || "append_wal", Instant::now);
        link.child("child");
        for phase in &phases[..=end] {
            link.phase(*phase);
        }
        drop(link);
        let event = record(&receiver);
        assert_eq!(event["phase"], expected.name());
        assert_eq!(event["result"], "error_or_early_return");
        assert_eq!(event["send_invoked"], end >= 4);
        assert_eq!(event["response_headers_received"], end >= 5);
    }
}

#[test]
fn remote_rejections_preserve_result_categories_without_error_text() {
    let (runtime, receiver) = runtime(1);
    for result in ["rejected", "precondition_failed"] {
        let mut link = AuthorityRpcLink::start(Some(runtime), || "put", Instant::now);
        link.child("authority-child");
        link.phase(RpcPhase::Send);
        link.phase(RpcPhase::ResponseHeaders);
        link.complete(result);
        drop(link);
        let event = record(&receiver);
        assert_eq!(event["result"], result);
        assert_eq!(event["phase"], "complete");
        assert!(event.get("error").is_none());
    }
}

#[test]
fn unwinding_and_saturated_or_disconnected_sink_do_not_replace_business_results() {
    let (runtime, receiver) = runtime(1);
    let result = std::panic::catch_unwind(|| {
        let _link = AuthorityRpcLink::start(Some(runtime), || "get", Instant::now);
        panic!("synthetic original panic");
    });
    assert!(result.is_err());
    assert_eq!(record(&receiver)["result"], "unwind");
    for _ in 0..2 {
        drop(AuthorityRpcLink::start(
            Some(runtime),
            || "get",
            Instant::now,
        ));
    }
    assert_eq!(
        runtime
            .counts
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    drop(receiver);
    drop(AuthorityRpcLink::start(
        Some(runtime),
        || "get",
        Instant::now,
    ));
    assert_eq!(
        runtime
            .counts
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
}

#[test]
fn command_labels_do_not_copy_session_keys_payload_or_sql() {
    for (command, expected) in [
        (
            AuthorityCommand::Session {
                session: "PRIVATE_SESSION".into(),
                operation: SessionOperation::Get {
                    key: "PRIVATE_KEY".into(),
                },
            },
            "get",
        ),
        (
            AuthorityCommand::CloseSession {
                session: "PRIVATE_SESSION".into(),
            },
            "close_session",
        ),
        (
            AuthorityCommand::Session {
                session: "PRIVATE_SESSION".into(),
                operation: SessionOperation::Put {
                    key: "PRIVATE_KEY".into(),
                    bytes: b"PRIVATE_PAYLOAD".to_vec(),
                    condition: crate::object_store::PutCondition::Any,
                },
            },
            "put",
        ),
    ] {
        let (runtime, receiver) = runtime(1);
        drop(AuthorityRpcLink::start(
            Some(runtime),
            || command_name(&command),
            Instant::now,
        ));
        let event = record(&receiver);
        assert_eq!(event["command"], expected);
        let serialized = event.to_string();
        for forbidden in [
            "PRIVATE",
            "sql",
            "payload",
            "credential",
            "endpoint",
            "token",
        ] {
            assert!(!serialized.contains(forbidden));
        }
    }
}
