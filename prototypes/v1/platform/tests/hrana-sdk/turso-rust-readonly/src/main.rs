use std::{env, fs};

use serde_json::Value;
use turso_serverless::{Builder, TransactionBehavior, Value as SqlValue};

fn field<'a>(fixture: &'a Value, path: &[&str]) -> Result<&'a str, &'static str> {
    path.iter()
        .try_fold(fixture, |value, key| value.get(key))
        .and_then(Value::as_str)
        .ok_or("invalid fixture")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let file = arguments
        .next()
        .ok_or("usage: turso-rust-readonly-probe <private connection.json> [--loopback-http]")?;
    let allow_loopback_http = arguments.next().as_deref() == Some("--loopback-http");
    let disposable_write = arguments.next().as_deref() == Some("--disposable-write");
    if arguments.next().is_some()
        || disposable_write && (!allow_loopback_http || !file.starts_with("/tmp/peri-v3-json."))
    {
        return Err(
            "disposable writes require the isolated /tmp/peri-v3-json.* loopback fixture".into(),
        );
    }
    let fixture: Value = serde_json::from_slice(&fs::read(file)?)?;
    let endpoint = field(&fixture, &["endpoint"])?;
    let scheme = if endpoint.starts_with("https://127.0.0.1:") {
        "https://127.0.0.1:"
    } else if allow_loopback_http && endpoint.starts_with("http://127.0.0.1:") {
        "http://127.0.0.1:"
    } else {
        return Err("only HTTPS localhost or explicitly opted-in loopback HTTP is allowed".into());
    };
    if endpoint == scheme
        || !endpoint[scheme.len()..]
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return Err("invalid local endpoint".into());
    }
    let health_url = format!("{endpoint}/health");
    let default_trust = reqwest::Client::new().get(&health_url).send().await.is_ok();
    if scheme.starts_with("https:") {
        let certificate =
            reqwest::Certificate::from_pem(&fs::read(field(&fixture, &["ca_file"])?)?)?;
        let trusted_client = reqwest::Client::builder()
            .add_root_certificate(certificate)
            .build()?;
        let verified = trusted_client.get(&health_url).send().await.is_ok();
        println!("reqwest 0.13: default_trust={default_trust} explicit_ca_verified={verified}");
        if !verified {
            return Err("local HTTPS endpoint did not validate against fixture CA".into());
        }
    } else if !default_trust {
        return Err("isolated loopback HTTP endpoint unavailable".into());
    }
    let tenant = field(&fixture, &["database", "namespace", "tenant"])?;
    let database = field(&fixture, &["database", "namespace", "database"])?;
    let branch = field(&fixture, &["database", "branch"])?;
    if [tenant, database, branch].iter().any(|segment| {
        segment.is_empty()
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    }) {
        return Err("invalid database path".into());
    }
    let url = format!("{endpoint}/db/{tenant}/{database}/{branch}");
    let handle = Builder::new_remote(url)
        .with_auth_token(field(&fixture, &["project_api_key"])?)
        .build()
        .await?;
    let connection = handle.connect()?;
    let mut rows = match connection.query("SELECT 1", ()).await {
        Ok(rows) => rows,
        Err(error) => {
            let message = error.to_string();
            eprintln!(
                "query failed: {}",
                if !default_trust {
                    "SDK default client does not trust fixture CA"
                } else if message.contains("HTTP status") {
                    "HTTP status (see gateway logs)"
                } else {
                    "transport or protocol (see gateway logs)"
                }
            );
            return Err("read-only SDK query failed".into());
        }
    };
    let row = rows.next().await?.ok_or("missing SELECT 1 row")?;
    if row.get_value(0)? != SqlValue::Integer(1) || rows.next().await?.is_some() {
        return Err("unexpected SELECT 1 result".into());
    }
    println!("turso_serverless 0.1.3: /v3/cursor SELECT 1 succeeded");
    let affected = connection
        .execute("SELECT 1", ())
        .await
        .map_err(|_| "read-only SDK pipeline failed")?;
    println!("turso_serverless 0.1.3: /v3/pipeline SELECT 1 succeeded (affected={affected})");
    if disposable_write {
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS __peri_v3_compat_smoke(value INTEGER)",
                (),
            )
            .await?;
        connection
            .execute("DELETE FROM __peri_v3_compat_smoke", ())
            .await?;
        connection
            .transactional_batch(
                [
                    (
                        "INSERT INTO __peri_v3_compat_smoke(value) VALUES (?1)",
                        (1,),
                    ),
                    (
                        "INSERT INTO __peri_v3_compat_smoke(value) VALUES (?1)",
                        (2,),
                    ),
                ],
                TransactionBehavior::Immediate,
            )
            .await?;
        let mut rows = connection
            .query("SELECT COUNT(*) FROM __peri_v3_compat_smoke", ())
            .await?;
        let row = rows.next().await?.ok_or("missing write verification row")?;
        if row.get_value(0)? != SqlValue::Integer(2) {
            return Err("transactional batch write verification failed".into());
        }
        if connection
            .transactional_batch(
                [
                    (
                        "INSERT INTO __peri_v3_compat_smoke(value) VALUES (?1)",
                        (3,),
                    ),
                    (
                        "INSERT INTO __peri_v3_compat_smoke(missing_column) VALUES (?1)",
                        (4,),
                    ),
                ],
                TransactionBehavior::Immediate,
            )
            .await
            .is_ok()
        {
            return Err("failed transactional batch unexpectedly succeeded".into());
        }
        let mut rows = connection
            .query("SELECT COUNT(*) FROM __peri_v3_compat_smoke", ())
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or("missing rollback verification row")?;
        if row.get_value(0)? != SqlValue::Integer(2) {
            return Err("failed transactional batch did not roll back".into());
        }
        let mut returning = connection
            .query(
                "INSERT INTO __peri_v3_compat_smoke(value) VALUES (9) RETURNING value",
                (),
            )
            .await?;
        let returned = returning
            .next()
            .await?
            .ok_or("missing write RETURNING row")?;
        if returned.get_value(0)? != SqlValue::Integer(9) || returning.next().await?.is_some() {
            return Err("write RETURNING cursor verification failed".into());
        }
        println!(
            "turso_serverless 0.1.3: disposable v3 transaction, rollback and write cursor passed"
        );
    }
    Ok(())
}
