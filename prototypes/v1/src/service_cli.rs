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
    matches!(command, "authority" | "owned-worker")
}

pub fn run(arguments: &[String]) -> Result<()> {
    match arguments.first().map(String::as_str) {
        Some("authority") => authority(arguments),
        Some("owned-worker") => worker(arguments),
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
    let runtime = OwnershipRuntime::new(
        arguments[3].clone(),
        provider,
        Path::new(&arguments[2]),
        WorkerLimits::default(),
        Arc::new(SystemLeaseClock),
    )?;
    let server = WorkerServer::bind(
        arguments[4].parse()?,
        runtime,
        credential("PERI_WORKER_CONTROL_TOKEN")?,
        credential("PERI_WORKER_SQL_TOKEN")?,
    )?;
    ready("owned-worker", server.local_addr()?)?;
    server.run()
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
