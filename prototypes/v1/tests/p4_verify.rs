#[path = "p4_verify/backend.rs"]
mod backend;
#[path = "p4_verify/http_regression.rs"]
mod http_regression;
#[path = "p4_verify/leases.rs"]
mod leases;
#[path = "p4_verify/process.rs"]
mod process;
#[path = "p4_verify/process_support.rs"]
mod process_support;
#[path = "p4_verify/refs.rs"]
mod refs;
#[path = "p4_verify/roles.rs"]
mod roles;
#[path = "p4_verify/stages.rs"]
mod stages;
#[path = "p4_verify/support.rs"]
mod support;
#[path = "p4_verify/trace.rs"]
mod trace;

use anyhow::Result;
use peri_loom_v1::{
    authority::ownership::OwnershipAuthority,
    namespace::Namespace,
    ownership::types::{AccessRole, GrantIntent, GrantReply, GrantRequest, LeaseIntent},
    runtime::DatabaseId,
};

fn identifier(database: &str) -> DatabaseId {
    DatabaseId::new(Namespace::new("independent-p4", database).unwrap(), "main").unwrap()
}

fn grant_request(
    authority: &OwnershipAuthority,
    database: DatabaseId,
    epoch: u64,
    expected_epoch: u64,
    ttl_ms: u64,
    request_id: &str,
) -> Result<GrantRequest> {
    let intent = GrantIntent {
        database,
        worker_id: format!("worker-{epoch}"),
        epoch,
        expected_epoch,
        lease_id: format!("lease-{epoch}"),
        ttl_ms,
        role: AccessRole::Writer,
    };
    let nonce = authority.challenge(LeaseIntent::Grant(intent.clone()))?;
    Ok(GrantRequest {
        request_id: request_id.into(),
        nonce: nonce.nonce,
        intent,
    })
}

fn grant(
    authority: &OwnershipAuthority,
    database: DatabaseId,
    epoch: u64,
    ttl_ms: u64,
) -> Result<GrantReply> {
    let previous = authority.state(&database)?.epoch;
    authority.grant(grant_request(
        authority,
        database,
        epoch,
        previous,
        ttl_ms,
        &format!("grant-{epoch}"),
    )?)
}
