use std::sync::Arc;

use anyhow::Result;

use crate::authority::Authority;
use crate::object_store::ObjectStore;
use crate::runtime::{DatabaseId, DatabaseProvider};

pub struct LocalAuthorityProvider {
    store: Arc<dyn ObjectStore>,
}

impl LocalAuthorityProvider {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

impl DatabaseProvider for LocalAuthorityProvider {
    fn open_store(&self, identifier: &DatabaseId, _epoch: u64) -> Result<Arc<dyn ObjectStore>> {
        identifier.validate()?;
        let authority = Authority::open(self.store.clone(), identifier.namespace.clone())?;
        Ok(authority.branch_store(&identifier.branch)?)
    }
}
