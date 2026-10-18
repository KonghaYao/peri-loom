use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Error;
use serde::Serialize;
use turso_core::{CompletionError, LimboError};

use crate::ownership::types::validate_label;
use crate::session::AdmissionError;

const MAX_DIAGNOSTICS: usize = 64;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Action {
    Execute,
    Query,
    Checkpoint,
    Compact,
}

#[derive(Default)]
pub(super) struct Diagnostics {
    emitted: AtomicUsize,
}

#[derive(Serialize)]
struct Record<'request> {
    event: &'static str,
    action: Action,
    request_id: &'request str,
    cause_class: &'static str,
    execution_completed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    native_cause_class: Option<&'static str>,
    /// Wall-clock milliseconds, so a worker line can be aligned with a maintenance line
    /// without establishing a cross-process monotonic clock.
    unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    lane_wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    writer_wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maintenance_blocked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_depth: Option<usize>,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn policy(message: &str) -> Option<&'static str> {
    match message {
        "query duration budget exceeded" => Some("duration_budget"),
        "VM step budget exceeded" => Some("vm_step_budget"),
        "SQL byte budget exceeded" => Some("sql_byte_budget"),
        "result row budget exceeded" | "result byte budget exceeded" => Some("result_budget"),
        "local ownership revoked or detached"
        | "local ownership epoch mismatch"
        | "scoped store generation revoked"
        | "Fenced lease unavailable"
        | "Fenced lease identity"
        | "Fenced root identity"
        | "Fenced ownership identity" => Some("local_fence"),
        "LeaseExpired" => Some("lease_expired"),
        "ReadOnly" => Some("readonly"),
        "lease clock moved backwards" => Some("lease_clock"),
        "draining rejects new transactions" => Some("draining"),
        _ => None,
    }
}

fn classify(error: &Error) -> &'static str {
    if error
        .downcast_ref::<crate::session::UnsettledSqlOutcome>()
        .is_some()
    {
        return "outcome_unsettled";
    }
    classify_source(error)
}

fn classify_source(error: &Error) -> &'static str {
    for cause in error.chain() {
        if let Some(admission) = cause.downcast_ref::<AdmissionError>() {
            return match admission {
                AdmissionError::Conflict => "admission_conflict",
                AdmissionError::QueueFull => "admission_queue_full",
                AdmissionError::Deadline => "admission_deadline",
                AdmissionError::Closing => "admission_closing",
                AdmissionError::Quarantine => "admission_quarantine",
            };
        }
        if let Some(native) = cause.downcast_ref::<LimboError>() {
            return match native {
                LimboError::Busy => "busy",
                LimboError::BusySnapshot => "busy_snapshot",
                LimboError::Constraint(_)
                | LimboError::ForeignKeyConstraint(_)
                | LimboError::Raise(..) => "constraint",
                LimboError::Interrupt => "interrupt",
                LimboError::ReadOnly => "readonly",
                LimboError::CacheError(_) => "cache",
                LimboError::DatabaseFull => "database_full",
                LimboError::Corrupt(_) | LimboError::NotADB => "corrupt",
                LimboError::TooBig => "too_big",
                LimboError::TxError(_) => "transaction",
                LimboError::LockingError(_) => "locking",
                LimboError::IoBackendUnavailable(_) => "io",
                LimboError::CompletionError(completion) => match completion {
                    CompletionError::IOError(..)
                    | CompletionError::ShortWrite
                    | CompletionError::ShortRead { .. }
                    | CompletionError::ShortReadWalFrame { .. } => "io",
                    #[cfg(target_family = "unix")]
                    CompletionError::RustixIOError(_) => "io",
                    _ => "other_native",
                },
                LimboError::InternalError(message) => policy(message)
                    .or_else(|| message.starts_with("remote IO: ").then_some("remote_io"))
                    .unwrap_or("other_native"),
                _ => "other_native",
            };
        }
    }
    if error.chain().any(|cause| cause.is::<std::io::Error>()) {
        return "io";
    }
    error
        .chain()
        .last()
        .and_then(|cause| policy(&cause.to_string()))
        .unwrap_or("not_determined")
}

impl Diagnostics {
    fn record(&self, action: Action, request_id: &str, error: &Error) -> Option<Vec<u8>> {
        if validate_label(request_id).is_err()
            || self
                .emitted
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    (count < MAX_DIAGNOSTICS).then_some(count + 1)
                })
                .is_err()
        {
            return None;
        }
        // Wait context is recorded beside the error by the worker thread, not inside the
        // error chain, so observation cannot change the client-visible message.
        let lane_wait = crate::work_metrics::lane_wait();
        let writer_wait = crate::work_metrics::admission_wait();
        let mut bytes = serde_json::to_vec(&Record {
            event: if super::outcome::execution_completed(error) {
                "native_sql_terminal_error"
            } else if super::outcome::execution_rejected(error) {
                "native_sql_rejected"
            } else {
                "native_sql_unsettled_outcome"
            },
            action,
            request_id,
            cause_class: classify(error),
            execution_completed: super::outcome::execution_completed(error),
            unix_ms: unix_ms(),
            lane_wait_ms: lane_wait.map(|wait| wait.waited_ns / 1_000_000),
            writer_wait_ms: writer_wait.map(|wait| wait.waited_ns / 1_000_000),
            storage_wait_ms: crate::work_metrics::request_lock_wait_ns(
                crate::work_metrics::LockDomain::Storage,
            )
            .map(|ns| ns / 1_000_000),
            maintenance_blocked: writer_wait.map(|wait| wait.maintenance_blocked),
            queue_depth: writer_wait.map(|wait| wait.queue_depth),
            native_cause_class: match action {
                Action::Execute | Action::Query if !super::outcome::execution_completed(error) => {
                    Some(classify_source(error))
                }
                Action::Execute | Action::Query => None,
                Action::Checkpoint | Action::Compact => Some(
                    if error.root_cause().to_string()
                        == "checkpoint did not backfill the complete WAL"
                    {
                        "checkpoint_partial_backfill"
                    } else {
                        classify_source(error)
                    },
                ),
            },
        })
        .ok()?;
        bytes.push(b'\n');
        Some(bytes)
    }

    fn emit_to(&self, action: Action, request_id: &str, error: &Error, writer: &mut impl Write) {
        if let Some(bytes) = self.record(action, request_id, error) {
            let _ = writer.write_all(&bytes);
        }
    }

    pub(super) fn emit(&self, action: Action, request_id: &str, error: &Error) {
        if self.emitted.load(Ordering::Relaxed) < MAX_DIAGNOSTICS {
            self.emit_to(action, request_id, error, &mut std::io::stderr().lock());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::sync::Arc;
    use std::thread;

    use crate::session::AdmissionError;
    use crate::work_metrics::{AdmissionWait, LaneWait, LockDomain};
    use anyhow::{anyhow, Context, Result};
    use serde_json::Value;
    use turso_core::{CompletionError, LimboError};

    use super::{classify, Action, Diagnostics};

    fn with_context(error: LimboError) -> anyhow::Error {
        Err::<(), _>(error)
            .context("step bounded remote SQL")
            .context("privateSecret SQL SELECT credential FROM secrets")
            .unwrap_err()
            .context(crate::session::SqlEvidence::Settled)
    }

    #[test]
    fn checkpoint_diagnostics_classify_native_failures_without_payload() -> Result<()> {
        for (native, expected) in [
            (LimboError::DatabaseFull, "database_full"),
            (LimboError::TxError("privateSecret".into()), "transaction"),
            (
                LimboError::InternalError("remote IO: privateSecret".into()),
                "remote_io",
            ),
        ] {
            let error = with_context(native).context(crate::session::UnsettledSqlOutcome);
            let bytes = Diagnostics::default()
                .record(Action::Checkpoint, "safe-request", &error)
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes)?;
            assert_eq!(value["native_cause_class"], expected);
            assert!(!std::str::from_utf8(&bytes)?.contains("privateSecret"));
        }
        Ok(())
    }

    #[test]
    fn maintenance_unsettled_records_preserve_busy_and_io_without_payload() -> Result<()> {
        for (action, expected_action) in [
            (Action::Checkpoint, "checkpoint"),
            (Action::Compact, "compact"),
        ] {
            for (native, expected) in [
                (LimboError::Busy, "busy"),
                (
                    LimboError::IoBackendUnavailable("privateSecret token namespace SQL".into()),
                    "io",
                ),
                (
                    LimboError::CompletionError(CompletionError::ShortWrite),
                    "io",
                ),
            ] {
                let error = with_context(native).context(crate::session::UnsettledSqlOutcome);
                let bytes = Diagnostics::default()
                    .record(action, "safe-request", &error)
                    .unwrap();
                let value: Value = serde_json::from_slice(&bytes)?;
                assert_eq!(value.as_object().unwrap().len(), 7);
                assert_eq!(value["action"], expected_action);
                assert_eq!(value["event"], "native_sql_unsettled_outcome");
                assert_eq!(value["cause_class"], "outcome_unsettled");
                assert_eq!(value["native_cause_class"], expected);
                assert_eq!(value["execution_completed"], false);
                assert!(bytes.len() < 512);
                for forbidden in ["privateSecret", "token", "namespace", "SQL"] {
                    assert!(!std::str::from_utf8(&bytes)?.contains(forbidden));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn maintenance_partial_backfill_requires_exact_fixed_root_cause() -> Result<()> {
        for (message, expected) in [
            (
                "checkpoint did not backfill the complete WAL",
                "checkpoint_partial_backfill",
            ),
            (
                "checkpoint did not backfill the complete WAL privateSecret",
                "not_determined",
            ),
        ] {
            let error = anyhow!(message).context(crate::session::UnsettledSqlOutcome);
            let bytes = Diagnostics::default()
                .record(Action::Checkpoint, "safe-request", &error)
                .unwrap();
            let value: Value = serde_json::from_slice(&bytes)?;
            assert_eq!(value["native_cause_class"], expected);
            assert_eq!(value["cause_class"], "outcome_unsettled");
            assert_eq!(value["execution_completed"], false);
            assert!(!std::str::from_utf8(&bytes)?.contains("privateSecret"));
        }
        Ok(())
    }

    #[test]
    fn typed_admission_errors_have_fixed_classes_without_native_busy_or_payload() -> Result<()> {
        let diagnostics = Diagnostics::default();
        for (error, expected) in [
            (AdmissionError::Conflict, "admission_conflict"),
            (AdmissionError::QueueFull, "admission_queue_full"),
            (AdmissionError::Deadline, "admission_deadline"),
            (AdmissionError::Closing, "admission_closing"),
            (AdmissionError::Quarantine, "admission_quarantine"),
        ] {
            let error = anyhow::Error::new(error).context("private SQL session namespace token");
            assert_eq!(classify(&error), expected);
            let record = diagnostics
                .record(Action::Execute, "safe-request", &error)
                .unwrap();
            let value: Value = serde_json::from_slice(&record)?;
            assert_eq!(value["cause_class"], expected);
            assert!(!String::from_utf8(record)?.contains("private SQL"));
        }
        Ok(())
    }

    #[test]
    fn unsettled_outcome_is_not_a_completed_terminal_error_or_native_busy() -> Result<()> {
        let error = anyhow::Error::new(LimboError::Busy)
            .context(crate::session::UnsettledSqlOutcome)
            .context("private SQL namespace token");
        let record = Diagnostics::default()
            .record(Action::Execute, "same-request", &error)
            .unwrap();
        let value: Value = serde_json::from_slice(&record)?;
        assert_eq!(value.as_object().unwrap().len(), 7);
        assert_eq!(value["event"], "native_sql_unsettled_outcome");
        assert_eq!(value["cause_class"], "outcome_unsettled");
        assert_eq!(value["native_cause_class"], "busy");
        assert_eq!(value["execution_completed"], false);
        assert!(!String::from_utf8(record)?.contains("private SQL"));
        Ok(())
    }

    #[test]
    fn typed_native_errors_survive_context_without_payload_classification() {
        let cases = [
            (LimboError::Busy, "busy"),
            (LimboError::BusySnapshot, "busy_snapshot"),
            (
                LimboError::Constraint("LeaseExpired privateSecret".into()),
                "constraint",
            ),
            (
                LimboError::ForeignKeyConstraint("ReadOnly privateSecret".into()),
                "constraint",
            ),
            (LimboError::Interrupt, "interrupt"),
            (LimboError::ReadOnly, "readonly"),
            (
                LimboError::IoBackendUnavailable("privateSecret".into()),
                "io",
            ),
            (
                LimboError::CompletionError(CompletionError::ShortWrite),
                "io",
            ),
            (LimboError::SqlError("LeaseExpired".into()), "other_native"),
            (
                LimboError::InternalError("privateSecret".into()),
                "other_native",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(classify(&with_context(error)), expected);
        }
    }

    #[test]
    fn exact_policy_constants_only_not_mock_prefixes_or_user_constraint() {
        for (message, expected) in [
            ("query duration budget exceeded", "duration_budget"),
            ("VM step budget exceeded", "vm_step_budget"),
            ("SQL byte budget exceeded", "sql_byte_budget"),
            ("result row budget exceeded", "result_budget"),
            ("result byte budget exceeded", "result_budget"),
            ("local ownership revoked or detached", "local_fence"),
            ("local ownership epoch mismatch", "local_fence"),
            ("scoped store generation revoked", "local_fence"),
            ("LeaseExpired", "lease_expired"),
            ("ReadOnly", "readonly"),
        ] {
            assert_eq!(classify(&anyhow!(message)), expected);
            assert_eq!(
                classify(&with_context(LimboError::InternalError(message.into()))),
                expected
            );
            assert_eq!(
                classify(&with_context(LimboError::Constraint(message.into()))),
                "constraint"
            );
        }
        for message in [
            "LeaseExpired test local clock",
            "Fenced test local revocation",
            "query duration budget exceeded privateSecret",
            "SQL SELECT 'LeaseExpired'",
        ] {
            assert_eq!(classify(&anyhow!(message)), "not_determined");
        }
        let misleading = anyhow!("privateSecret").context("LeaseExpired");
        assert_eq!(classify(&misleading), "not_determined");
        assert_eq!(
            classify(&anyhow!(std::io::Error::other("privateSecret"))),
            "io"
        );
        assert_eq!(
            classify(&with_context(LimboError::CompletionError(
                CompletionError::DecryptionError { page_idx: 999 },
            ))),
            "other_native"
        );
        assert_eq!(
            classify(&with_context(LimboError::Busy).context("ReadOnly")),
            "busy"
        );
    }

    #[test]
    fn query_and_execute_records_have_only_allowlisted_fields_and_no_secret() -> Result<()> {
        let diagnostics = Diagnostics::default();
        for (action, expected) in [(Action::Execute, "execute"), (Action::Query, "query")] {
            let bytes = diagnostics
                .record(
                    action,
                    "safe_request-1",
                    &with_context(LimboError::Constraint(
                        "trueSecret SQL INSERT INTO secrets token session namespace rows".into(),
                    )),
                )
                .unwrap();
            assert!(bytes.len() < 512);
            let text = std::str::from_utf8(&bytes)?;
            for forbidden in [
                "trueSecret",
                "privateSecret",
                "SQL",
                "INSERT",
                "token",
                "session",
                "namespace",
                "rows",
            ] {
                assert!(!text.contains(forbidden));
            }
            let record: Value = serde_json::from_slice(&bytes)?;
            assert_eq!(record.as_object().unwrap().len(), 6);
            assert!(record["unix_ms"].as_u64().is_some());
            assert_eq!(record["event"], "native_sql_terminal_error");
            assert_eq!(record["action"], expected);
            assert_eq!(record["request_id"], "safe_request-1");
            assert_eq!(record["cause_class"], "constraint");
            assert_eq!(record["execution_completed"], true);
        }
        let unknown = diagnostics
            .record(Action::Query, "unknown_request", &anyhow!("trueSecret SQL"))
            .unwrap();
        let unknown: Value = serde_json::from_slice(&unknown)?;
        assert_eq!(unknown["cause_class"], "not_determined");
        assert!(!unknown.to_string().contains("trueSecret"));
        Ok(())
    }

    #[test]
    fn deadline_wait_context_names_the_blocked_stage() -> Result<()> {
        crate::work_metrics::set_enabled(true);
        crate::work_metrics::begin_request_observation();
        crate::work_metrics::note_admission_wait(AdmissionWait {
            waited_ns: 4_900_000_000,
            maintenance_blocked: true,
            queue_depth: 1,
        });
        // The error chain stays exactly what the client would see without observation. The
        // deadline is a typed root error, matching `RequestDeadline::check`.
        let writer = anyhow::Error::from(AdmissionError::Deadline).context("query failed");
        let record: Value = serde_json::from_slice(
            &Diagnostics::default()
                .record(Action::Query, "writer_wait", &writer)
                .unwrap(),
        )?;
        assert_eq!(record["writer_wait_ms"], 4_900);
        assert_eq!(record["maintenance_blocked"], true);
        assert_eq!(record["queue_depth"], 1);
        assert_eq!(record["cause_class"], "admission_deadline");
        assert_eq!(
            format!("{writer:#}"),
            "query failed: writer admission deadline"
        );

        crate::work_metrics::begin_request_observation();
        crate::work_metrics::note_lane_wait(LaneWait {
            waited_ns: 4_950_000_000,
        });
        let lane = anyhow::Error::from(AdmissionError::Deadline).context("query failed");
        let record: Value = serde_json::from_slice(
            &Diagnostics::default()
                .record(Action::Query, "lane_wait", &lane)
                .unwrap(),
        )?;
        assert_eq!(record["lane_wait_ms"], 4_950);
        assert!(record.get("writer_wait_ms").is_none());
        Ok(())
    }

    #[test]
    fn request_boundary_drops_the_previous_requests_wait() {
        crate::work_metrics::set_enabled(true);
        crate::work_metrics::begin_request_observation();
        crate::work_metrics::note_admission_wait(AdmissionWait {
            waited_ns: 1,
            maintenance_blocked: true,
            queue_depth: 1,
        });
        crate::work_metrics::note_lane_wait(LaneWait { waited_ns: 2 });
        assert!(crate::work_metrics::admission_wait().is_some());
        assert!(crate::work_metrics::lane_wait().is_some());
        crate::work_metrics::begin_request_observation();
        assert!(crate::work_metrics::admission_wait().is_none());
        assert!(crate::work_metrics::lane_wait().is_none());
        assert_eq!(
            crate::work_metrics::request_lock_wait_ns(LockDomain::Storage),
            Some(0)
        );
        // A thread that never served a request cannot populate the slots either.
        std::thread::spawn(|| {
            crate::work_metrics::note_admission_wait(AdmissionWait {
                waited_ns: 3,
                maintenance_blocked: false,
                queue_depth: 0,
            });
            crate::work_metrics::note_lane_wait(LaneWait { waited_ns: 4 });
            assert!(crate::work_metrics::admission_wait().is_none());
            assert!(crate::work_metrics::lane_wait().is_none());
        })
        .join()
        .unwrap();
    }

    #[test]
    fn invalid_request_ids_never_emit_or_consume_capacity() {
        let diagnostics = Diagnostics::default();
        for request_id in [
            "",
            "secret\nSQL",
            "authorization:secret",
            "../bad/path",
            &"a".repeat(129),
        ] {
            assert!(diagnostics
                .record(Action::Execute, request_id, &anyhow!("privateSecret"))
                .is_none());
        }
        for _ in 0..64 {
            assert!(diagnostics
                .record(Action::Query, "valid", &anyhow!("privateSecret"))
                .is_some());
        }
        assert!(diagnostics
            .record(Action::Query, "valid", &anyhow!("privateSecret"))
            .is_none());
    }

    #[test]
    fn concurrent_overflow_is_bounded_per_worker_and_does_not_reset() {
        let diagnostics = Arc::new(Diagnostics::default());
        let mut workers = Vec::new();
        for _ in 0..16 {
            let diagnostics = diagnostics.clone();
            workers.push(thread::spawn(move || {
                (0..16)
                    .filter(|_| {
                        diagnostics
                            .record(Action::Execute, "valid", &anyhow!("privateSecret"))
                            .is_some()
                    })
                    .count()
            }));
        }
        let emitted: usize = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum();
        assert_eq!(emitted, 64);
        assert!(diagnostics
            .record(Action::Query, "valid", &anyhow!("privateSecret"))
            .is_none());
        assert!(Diagnostics::default()
            .record(Action::Query, "valid", &anyhow!("privateSecret"))
            .is_some());
    }

    #[test]
    fn failing_log_writer_preserves_terminal_error_and_caps_attempts() {
        struct FailingWriter {
            attempts: usize,
        }

        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                self.attempts += 1;
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "privateSecret"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "privateSecret"))
            }
        }

        let diagnostics = Diagnostics::default();
        let error = with_context(LimboError::Busy);
        let original = format!("{error:#}");
        let mut writer = FailingWriter { attempts: 0 };
        for _ in 0..100 {
            diagnostics.emit_to(Action::Execute, "valid", &error, &mut writer);
        }
        assert_eq!(writer.attempts, 64);
        assert_eq!(format!("{error:#}"), original);
        assert!(matches!(
            error.downcast_ref::<LimboError>(),
            Some(LimboError::Busy)
        ));
        assert!(diagnostics.record(Action::Query, "valid", &error).is_none());
    }
}
