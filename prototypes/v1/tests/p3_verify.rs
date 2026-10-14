#[path = "p3_verify/lifecycle.rs"]
mod lifecycle;
#[path = "p3_verify/payload.rs"]
mod payload;
#[path = "p3_verify/process.rs"]
mod process;

use anyhow::Result;
use peri_loom_v1::{
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore},
    provider::LocalAuthorityProvider,
    runtime::{DatabaseId, WorkerLimits, WorkerRuntime},
};
use std::sync::Arc;

fn identifier(tenant: &str, database: &str) -> DatabaseId {
    DatabaseId::new(Namespace::new(tenant, database).unwrap(), "main").unwrap()
}

fn runtime(
    store: Arc<dyn ObjectStore>,
    cache: &std::path::Path,
    limits: WorkerLimits,
) -> Result<Arc<WorkerRuntime>> {
    WorkerRuntime::new(Arc::new(LocalAuthorityProvider::new(store)), cache, limits)
}

fn memory() -> Arc<dyn ObjectStore> {
    Arc::new(MemoryObjectStore::default())
}
