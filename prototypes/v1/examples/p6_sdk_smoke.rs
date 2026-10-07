use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::time::Duration;

use peri_loom_v1::sdk::{Client, ClientOptions, DatabaseId, ManagementRequest, SdkError};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Connection {
    endpoint: String,
    project_api_key: String,
    database: DatabaseId,
    ca_file: PathBuf,
}

#[derive(Debug)]
enum SmokeFailure {
    Stage(&'static str),
    Sdk {
        stage: &'static str,
        failure: SdkError,
    },
}

impl From<&'static str> for SmokeFailure {
    fn from(stage: &'static str) -> Self {
        Self::Stage(stage)
    }
}

fn query_then_close<Query, Close>(query: Query, close: Close) -> Result<(), SmokeFailure>
where
    Query: FnOnce() -> Result<Vec<Vec<Value>>, SmokeFailure>,
    Close: FnOnce() -> Result<(), SmokeFailure>,
{
    let rows = query()?;
    if rows.len() != 1
        || rows[0].len() != 2
        || rows[0][0].as_i64() != Some(9223372036854775807)
        || rows[0][1].as_i64() != Some(-9007199254740993)
    {
        return Err("native integer precision or row shape incorrect".into());
    }
    close()
}

fn smoke() -> Result<(), SmokeFailure> {
    let mut arguments = std::env::args_os().skip(1);
    let path = arguments.next().ok_or("connection path required")?;
    if arguments.next().is_some() {
        return Err("only a connection path is accepted".into());
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "connection file unavailable")?;
    let metadata = file
        .metadata()
        .map_err(|_| "connection metadata unavailable")?;
    let current_uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.uid() != current_uid
        || metadata.len() > 65536
    {
        return Err("connection file must be owner-controlled mode 0600 and bounded".into());
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| "connection read failed")?;
    if bytes.len() > 65536 {
        return Err("connection file exceeds limit".into());
    }
    let connection: Connection =
        serde_json::from_slice(&bytes).map_err(|_| "connection format invalid")?;
    if !connection.endpoint.starts_with("https://") {
        return Err("live gate requires verified HTTPS".into());
    }
    let certificate = std::fs::read(&connection.ca_file).map_err(|_| "CA file unavailable")?;
    if certificate.len() > 1 << 20 {
        return Err("CA file exceeds limit".into());
    }
    let client = Client::with_options(
        &connection.endpoint,
        connection.project_api_key.clone(),
        ClientOptions {
            timeout: Duration::from_secs(3),
            trusted_ca_pem: Some(certificate),
            ..ClientOptions::default()
        },
    )
    .map_err(|failure| SmokeFailure::Sdk {
        stage: "client initialization",
        failure,
    })?;
    let status = client
        .management(
            "status",
            ManagementRequest::new(connection.database.clone()),
        )
        .map_err(|failure| SmokeFailure::Sdk {
            stage: "readonly management status",
            failure,
        })?;
    if !status.is_object() {
        return Err("management status shape invalid".into());
    }
    let session = client
        .open(&connection.database)
        .map_err(|failure| SmokeFailure::Sdk {
            stage: "session open",
            failure,
        })?;
    query_then_close(
        || {
            session
                .query(
                    "SELECT 9223372036854775807 AS max_i64, -9007199254740993 AS negative_i64",
                    Some("rust-live-exact-integers"),
                )
                .map_err(|failure| SmokeFailure::Sdk {
                    stage: "SELECT exact integers",
                    failure,
                })
        },
        || {
            session.close().map_err(|failure| SmokeFailure::Sdk {
                stage: "explicit session close",
                failure,
            })
        },
    )?;
    println!("Rust SDK live verified-TLS SELECT exact-i64/status/explicit-close PASS");
    Ok(())
}

fn main() {
    if let Err(failure) = smoke() {
        match failure {
            SmokeFailure::Stage(stage) => {
                eprintln!("Rust SDK live FAIL: {stage} code=helper_precondition")
            }
            SmokeFailure::Sdk { stage, failure } => {
                eprintln!("Rust SDK live FAIL: {stage} code={}", failure.code)
            }
        }
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{query_then_close, SmokeFailure};
    use peri_loom_v1::sdk::SdkError;
    use serde_json::{json, Value};
    use std::cell::Cell;

    fn rows() -> Vec<Vec<Value>> {
        vec![vec![
            json!(9223372036854775807i64),
            json!(-9007199254740993i64),
        ]]
    }

    fn unknown(stage: &'static str) -> SmokeFailure {
        SmokeFailure::Sdk {
            stage,
            failure: SdkError {
                code: "outcome_unknown",
                request_id: "fake_request".into(),
                outcome_unknown: true,
                execution_completed: false,
                may_have_committed: false,
            },
        }
    }

    #[test]
    fn query_unknown_preserves_failure_without_close() {
        let closes = Cell::new(0);
        let result = query_then_close(
            || Err(unknown("query")),
            || {
                closes.set(closes.get() + 1);
                Ok(())
            },
        );
        assert_eq!(closes.get(), 0);
        assert!(
            matches!(result, Err(SmokeFailure::Sdk { stage: "query", failure }) if failure.code == "outcome_unknown" && failure.outcome_unknown)
        );
    }

    #[test]
    fn disconnected_query_does_not_close() {
        let closes = Cell::new(0);
        let result = query_then_close(
            || Err("fake socket disconnected".into()),
            || {
                closes.set(1);
                Ok(())
            },
        );
        assert_eq!(closes.get(), 0);
        assert!(matches!(
            result,
            Err(SmokeFailure::Stage("fake socket disconnected"))
        ));
    }

    #[test]
    fn invalid_oracle_does_not_close() {
        let closes = Cell::new(0);
        let result = query_then_close(
            || Ok(vec![vec![json!(0)]]),
            || {
                closes.set(1);
                Ok(())
            },
        );
        assert_eq!(closes.get(), 0);
        assert!(matches!(
            result,
            Err(SmokeFailure::Stage(
                "native integer precision or row shape incorrect"
            ))
        ));
    }

    #[test]
    fn valid_oracle_closes_once() {
        let closes = Cell::new(0);
        let result = query_then_close(
            || Ok(rows()),
            || {
                closes.set(closes.get() + 1);
                Ok(())
            },
        );
        assert!(result.is_ok());
        assert_eq!(closes.get(), 1);
    }

    #[test]
    fn close_unknown_is_not_success_or_retried() {
        let closes = Cell::new(0);
        let result = query_then_close(
            || Ok(rows()),
            || {
                closes.set(closes.get() + 1);
                Err(unknown("close"))
            },
        );
        assert_eq!(closes.get(), 1);
        assert!(
            matches!(result, Err(SmokeFailure::Sdk { stage: "close", failure }) if failure.code == "outcome_unknown")
        );
    }
}
