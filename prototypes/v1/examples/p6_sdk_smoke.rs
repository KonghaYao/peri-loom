use std::error::Error;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::time::Duration;

use peri_loom_v1::sdk::{Client, ClientOptions, DatabaseId, ManagementRequest};
use serde::Deserialize;

#[derive(Deserialize)]
struct Connection {
    endpoint: String,
    project_api_key: String,
    database: DatabaseId,
    ca_file: PathBuf,
}

fn diagnose_transport(connection: &Connection, certificate: &[u8]) {
    let Ok(certificate) = reqwest::Certificate::from_pem(certificate) else {
        return;
    };
    let Ok(client) = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .add_root_certificate(certificate)
        .build()
    else {
        return;
    };
    let result = client
        .post(format!("{}/v1/databases/status", connection.endpoint))
        .bearer_auth(&connection.project_api_key)
        .json(&serde_json::json!({"request_id":"rust-live-status-diagnostic","database":connection.database}))
        .send();
    match result {
        Ok(response) => eprintln!(
            "Rust verified transport HTTP={}",
            response.status().as_u16()
        ),
        Err(failure) => {
            let mut cause: Option<&(dyn Error + 'static)> = Some(&failure);
            while let Some(error) = cause {
                let description = error.to_string();
                for category in [
                    "CaUsedAsEndEntity",
                    "UnknownIssuer",
                    "NotValidYet",
                    "Expired",
                    "NameMismatch",
                ] {
                    if description.contains(category) {
                        eprintln!("Rust verified transport certificate error={category}");
                    }
                }
                cause = error.source();
            }
        }
    }
}

fn smoke() -> Result<(), &'static str> {
    let mut arguments = std::env::args_os().skip(1);
    let path = arguments.next().ok_or("connection path required")?;
    if arguments.next().is_some() {
        return Err("only a connection path is accepted");
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
        return Err("connection file must be owner-controlled mode 0600 and bounded");
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| "connection read failed")?;
    if bytes.len() > 65536 {
        return Err("connection file exceeds limit");
    }
    let connection: Connection =
        serde_json::from_slice(&bytes).map_err(|_| "connection format invalid")?;
    if !connection.endpoint.starts_with("https://") {
        return Err("live gate requires verified HTTPS");
    }
    let certificate = std::fs::read(&connection.ca_file).map_err(|_| "CA file unavailable")?;
    if certificate.len() > 1 << 20 {
        return Err("CA file exceeds limit");
    }
    let client = Client::with_options(
        &connection.endpoint,
        connection.project_api_key.clone(),
        ClientOptions {
            timeout: Duration::from_secs(3),
            trusted_ca_pem: Some(certificate.clone()),
            ..ClientOptions::default()
        },
    )
    .map_err(|_| "SDK client initialization failed")?;
    let status = client
        .management(
            "status",
            ManagementRequest::new(connection.database.clone()),
        )
        .map_err(|failure| {
            eprintln!("Rust SDK status failure code={}", failure.code);
            diagnose_transport(&connection, &certificate);
            "SDK readonly management status failed"
        })?;
    if !status.is_object() {
        return Err("management status shape invalid");
    }
    let session = client
        .open(&connection.database)
        .map_err(|_| "SDK session open failed")?;
    let queried = session.query(
        "SELECT 9223372036854775807 AS max_i64, -9007199254740993 AS negative_i64",
        Some("rust-live-exact-integers"),
    );
    let closed = session.close();
    let rows = queried.map_err(|_| "SDK SELECT failed")?;
    closed.map_err(|_| "SDK explicit close failed")?;
    if rows.len() != 1
        || rows[0].len() != 2
        || rows[0][0].as_i64() != Some(9223372036854775807)
        || rows[0][1].as_i64() != Some(-9007199254740993)
    {
        return Err("native integer precision or row shape incorrect");
    }
    println!("Rust SDK live verified-TLS SELECT exact-i64/status/explicit-close PASS");
    Ok(())
}

fn main() {
    if let Err(stage) = smoke() {
        eprintln!("Rust SDK live FAIL: {stage}");
        std::process::exit(1);
    }
}
