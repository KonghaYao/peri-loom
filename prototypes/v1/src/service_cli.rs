use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use serde_json::json;

use crate::authority::ownership::OwnershipAuthority;
use crate::authority_rpc::{
    auth::AuthConfig, client::AuthorityClient, provider::RemoteAuthorityProvider, server::Server,
    startup::AuthorityStartup,
};
use crate::ownership::{OwnershipRuntime, SystemLeaseClock};
use crate::runtime::WorkerLimits;
use crate::worker_rpc::server::WorkerServer;

pub fn handles(command: &str) -> bool {
    matches!(
        command,
        "authority" | "owned-worker" | "stage-image" | "publish-image" | "import-control"
    )
}

pub fn run(arguments: &[String]) -> Result<()> {
    match arguments.first().map(String::as_str) {
        Some("authority") => authority(arguments),
        Some("owned-worker") => worker(arguments),
        Some("stage-image" | "publish-image") => crate::import_cli::run(arguments),
        Some("import-control") => crate::import_control_cli::run(arguments),
        _ => anyhow::bail!("unknown service command"),
    }
}

fn authority(arguments: &[String]) -> Result<()> {
    ensure!(
        arguments.len() == 4 || arguments.len() == 5 && arguments[4] == "--reconciled",
        "usage: peri-loom-v1 authority STORE LISTEN STATE_DIR [--reconciled]"
    );
    let address: SocketAddr = arguments[2].parse()?;
    ensure!(
        address.ip().is_loopback(),
        "P4 authority listener must be loopback-only"
    );
    let control = credential("PERI_AUTHORITY_CONTROL_TOKEN")?;
    let workers_path = std::env::var("PERI_AUTHORITY_WORKERS_FILE")
        .context("PERI_AUTHORITY_WORKERS_FILE is required")?;
    let workers = worker_credentials(Path::new(&workers_path))?;
    let auth = AuthConfig::new(control, workers)?;
    let startup = AuthorityStartup::acquire(Path::new(&arguments[3]), arguments.len() == 5)?;
    let store = crate::config::authority_backend(&arguments[1])?;
    let store = Arc::new(crate::object_store::metrics::MetricsObjectStore::new(
        store,
        crate::metrics::Metrics::global(),
    ));
    let authority = OwnershipAuthority::new(store, Arc::new(SystemLeaseClock));
    let server = Server::bind(address, authority, auth, startup)?;
    ready("authority", server.local_addr())?;
    server.run()
}

fn worker(arguments: &[String]) -> Result<()> {
    ensure!(
        arguments.len() == 5,
        "usage: peri-loom-v1 owned-worker AUTHORITY CACHE WORKER_ID LISTEN"
    );
    let client = AuthorityClient::new(&arguments[1], credential("PERI_WORKER_AUTHORITY_TOKEN")?)?;
    let provider = RemoteAuthorityProvider::new(client, arguments[3].clone())?;
    let clock = Arc::new(SystemLeaseClock);
    let limits = WorkerLimits {
        max_open_databases: setting("PERI_WORKER_MAX_DATABASES", 32)?,
        max_total_sessions: setting("PERI_WORKER_MAX_SESSIONS", 128)?,
        max_sessions_per_database: setting("PERI_WORKER_MAX_DB_SESSIONS", 16)?,
        segment_ram_bytes: setting("PERI_WORKER_SEGMENT_RAM_BYTES", 8 * 1024 * 1024)?,
        segment_disk_bytes: setting("PERI_WORKER_SEGMENT_DISK_BYTES", 64 * 1024 * 1024)?,
    };
    let runtime = match std::env::var("PERI_KMS_ENDPOINT") {
        Ok(endpoint) => {
            let keys = crate::encryption::BrokerDatabaseKeyProvider::new(
                provider.clone(),
                &endpoint,
                credential("PERI_WORKER_KEY_TOKEN")?,
                clock.clone(),
            )?;
            OwnershipRuntime::new_with_key_provider(
                arguments[3].clone(),
                provider,
                Path::new(&arguments[2]),
                limits,
                clock,
                keys,
            )?
        }
        Err(std::env::VarError::NotPresent) => {
            ensure!(
                std::env::var_os("PERI_WORKER_KEY_TOKEN").is_none(),
                "key token without broker endpoint"
            );
            OwnershipRuntime::new(
                arguments[3].clone(),
                provider,
                Path::new(&arguments[2]),
                limits,
                clock,
            )?
        }
        Err(_) => anyhow::bail!("invalid PERI_KMS_ENDPOINT"),
    };
    let budget = crate::session::Budget {
        max_sql_bytes: setting("PERI_SQL_MAX_BYTES", 64 * 1024)?,
        max_rows: setting("PERI_SQL_MAX_ROWS", 10_000)?,
        max_result_bytes: setting("PERI_SQL_MAX_RESULT_BYTES", 4 * 1024 * 1024)?,
        max_vm_steps: setting("PERI_SQL_MAX_VM_STEPS", 10_000_000)?,
        max_query_duration: std::time::Duration::from_millis(setting(
            "PERI_SQL_MAX_DURATION_MS",
            5_000,
        )?),
    };
    let writer_profile = writer_profile_setting()?;
    let server = WorkerServer::bind_with_limits_and_writer_profile(
        arguments[4].parse()?,
        runtime,
        credential("PERI_WORKER_CONTROL_TOKEN")?,
        credential("PERI_WORKER_SQL_TOKEN")?,
        crate::worker_rpc::server::WorkerLimits {
            max_inflight: setting("PERI_WORKER_MAX_INFLIGHT", 128)?,
            control_reserve: setting("PERI_WORKER_CONTROL_RESERVE", 16)?,
            max_sessions: setting("PERI_WORKER_MAX_SESSIONS", 128)?,
            max_request_bytes: setting("PERI_WORKER_MAX_REQUEST_BYTES", 512 * 1024)?,
        },
        writer_profile,
    )?;
    crate::session::Budget::set_default(budget)?;
    ready("owned-worker", server.local_addr()?)?;
    server.run()
}

fn writer_profile_setting() -> Result<bool> {
    match std::env::var("PERI_WORKER_WRITER_PROFILE") {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" => Ok(false),
        Err(std::env::VarError::NotPresent) => Ok(false),
        _ => anyhow::bail!("invalid PERI_WORKER_WRITER_PROFILE"),
    }
}

fn setting<Value: std::str::FromStr>(name: &str, default: Value) -> Result<Value> {
    match std::env::var(name) {
        Ok(value) => value.parse().map_err(|_| anyhow::anyhow!("invalid {name}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => anyhow::bail!("invalid {name}"),
    }
}

fn credential(name: &str) -> Result<String> {
    let token = std::env::var(name).with_context(|| format!("{name} is required"))?;
    ensure!(
        token.len() >= 32
            && token.len() <= 256
            && token.bytes().all(|byte| byte.is_ascii_graphic()),
        "{name} must contain 32..256 non-whitespace ASCII bytes"
    );
    Ok(token)
}

fn worker_credentials(path: &Path) -> Result<BTreeMap<String, String>> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "worker credential file must be regular, not a symlink"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "worker credential file must not be accessible by group or others"
        );
    }
    ensure!(
        metadata.len() <= 1024 * 1024,
        "worker credential file too large"
    );
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn ready(service: &str, address: SocketAddr) -> Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(
        &mut output,
        &json!({"ready":true,"service":service,"address":address.to_string()}),
    )?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}
