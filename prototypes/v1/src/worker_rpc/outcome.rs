use anyhow::Error;

use crate::session::{SqlEvidence, UnsettledSqlOutcome};

use super::protocol::{SessionState, WorkerResponse};

pub(super) fn execution_completed(error: &Error) -> bool {
    error.downcast_ref::<UnsettledSqlOutcome>().is_none()
        && error.downcast_ref::<SqlEvidence>() == Some(&SqlEvidence::Settled)
}

pub(super) fn execution_rejected(error: &Error) -> bool {
    error.downcast_ref::<UnsettledSqlOutcome>().is_none()
        && error.downcast_ref::<SqlEvidence>() == Some(&SqlEvidence::NotExecuted)
}

pub(super) fn failure_response(
    version: u32,
    request_id: String,
    control: bool,
    error: Error,
    terminal: Option<SessionState>,
) -> WorkerResponse {
    let completed = execution_completed(&error);
    WorkerResponse {
        version,
        request_id,
        ok: false,
        result: None,
        error: Some(format!("{error:#}")),
        execution_completed: if control {
            None
        } else if completed {
            Some(true)
        } else if execution_rejected(&error) {
            Some(false)
        } else {
            None
        },
        session_state: if completed { terminal } else { None },
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{Error, Result};
    use turso_core::LimboError;

    use crate::namespace::Namespace;
    use crate::ownership::types::{AccessRole, LeaseBinding};
    use crate::runtime::DatabaseId;
    use crate::session::{AdmissionError, UnsettledSqlOutcome};

    use super::super::protocol::SessionState;
    use super::{execution_completed, failure_response};

    fn hint() -> Result<SessionState> {
        Ok(SessionState {
            session: "a".repeat(64),
            binding: LeaseBinding {
                database: DatabaseId::new(Namespace::new("tenant", "database")?, "main")?,
                root_incarnation: "root".into(),
                authority_term: 1,
                branch_incarnation: 1,
                epoch: 1,
                worker_id: "worker".into(),
                lease_id: "lease".into(),
                grant_id: "grant".into(),
                renewal_sequence: 0,
                role: AccessRole::Writer,
            },
            owner: true,
            sequence: 3,
        })
    }

    fn unsettled() -> Error {
        Error::new(LimboError::Busy)
            .context(UnsettledSqlOutcome)
            .context("private SQL namespace token")
    }

    #[test]
    fn typed_context_overrides_original_native_source_and_nested_contexts() {
        let error = unsettled();
        assert!(!execution_completed(&error));
        assert!(error.downcast_ref::<UnsettledSqlOutcome>().is_some());
        assert!(matches!(
            error.downcast_ref::<LimboError>(),
            Some(LimboError::Busy)
        ));
    }

    #[test]
    fn unsettled_v1_and_v2_are_ambiguous_and_never_export_owner_hint() -> Result<()> {
        for version in [1, 2] {
            let response = failure_response(
                version,
                "same-request".into(),
                false,
                unsettled(),
                Some(hint()?),
            );
            let response = serde_json::to_value(response)?;
            assert_eq!(response["version"], version);
            assert_eq!(response["request_id"], "same-request");
            assert_eq!(response["ok"], false);
            assert!(response.get("execution_completed").is_none());
            assert!(response["result"].is_null());
            assert!(response.get("session_state").is_none());
        }
        Ok(())
    }

    #[test]
    fn later_quarantine_and_known_native_errors_remain_completed() -> Result<()> {
        for version in [1, 2] {
            for error in [
                Error::new(AdmissionError::Quarantine),
                Error::new(AdmissionError::Deadline),
                Error::new(LimboError::Busy),
                Error::new(LimboError::Constraint("private prefix".into())),
            ] {
                assert!(!execution_completed(&error));
                let error = error.context(crate::session::SqlEvidence::Settled);
                assert!(execution_completed(&error));
                let terminal = if version == 2 { Some(hint()?) } else { None };
                let response =
                    failure_response(version, "same-request".into(), false, error, terminal);
                assert_eq!(response.execution_completed, Some(true));
                assert_eq!(response.session_state.is_some(), version == 2);
            }
        }
        Ok(())
    }

    #[test]
    fn identical_errors_require_distinct_execution_evidence() {
        for version in [1, 2] {
            for (evidence, expected) in [
                (crate::session::SqlEvidence::NotExecuted, false),
                (crate::session::SqlEvidence::Settled, true),
            ] {
                let error = Error::new(AdmissionError::Deadline).context(evidence);
                let response = failure_response(version, "evidence".into(), false, error, None);
                assert_eq!(response.execution_completed, Some(expected));
            }
            let response = failure_response(
                version,
                "unproven".into(),
                false,
                Error::new(AdmissionError::Deadline),
                None,
            );
            assert_eq!(response.execution_completed, None);
        }
    }

    #[test]
    fn control_failure_has_no_sql_evidence() {
        let response = failure_response(1, "control".into(), true, unsettled(), None);
        assert_eq!(response.execution_completed, None);
        assert!(response.session_state.is_none());
        let response = failure_response(
            1,
            "control".into(),
            true,
            Error::new(AdmissionError::Quarantine),
            None,
        );
        assert!(response.execution_completed.is_none());
    }
}
