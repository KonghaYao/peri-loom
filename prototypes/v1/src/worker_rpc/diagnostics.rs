use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Error;
use serde::Serialize;
use turso_core::{CompletionError, LimboError};

use crate::ownership::types::validate_label;

const MAX_DIAGNOSTICS: usize = 64;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Action {
    Execute,
    Query,
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
    for cause in error.chain() {
        if let Some(native) = cause.downcast_ref::<LimboError>() {
            return match native {
                LimboError::Busy => "busy",
                LimboError::BusySnapshot => "busy_snapshot",
                LimboError::Constraint(_)
                | LimboError::ForeignKeyConstraint(_)
                | LimboError::Raise(..) => "constraint",
                LimboError::Interrupt => "interrupt",
                LimboError::ReadOnly => "readonly",
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
                LimboError::InternalError(message) => policy(message).unwrap_or("other_native"),
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
        let mut bytes = serde_json::to_vec(&Record {
            event: "native_sql_terminal_error",
            action,
            request_id,
            cause_class: classify(error),
            execution_completed: true,
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

    use anyhow::{anyhow, Context, Result};
    use serde_json::Value;
    use turso_core::{CompletionError, LimboError};

    use super::{classify, Action, Diagnostics};

    fn with_context(error: LimboError) -> anyhow::Error {
        Err::<(), _>(error)
            .context("step bounded remote SQL")
            .context("privateSecret SQL SELECT credential FROM secrets")
            .unwrap_err()
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
            assert_eq!(record.as_object().unwrap().len(), 5);
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
