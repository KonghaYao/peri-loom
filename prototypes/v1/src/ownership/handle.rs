use std::sync::Arc;

use anyhow::Result;
use turso_core::Value;

use crate::resources::ResourceUsage;
use crate::runtime::DatabaseHandle;

use super::context::LeaseContext;
use super::runtime::OwnershipRuntime;

pub struct OwnedHandle {
    pub(super) handle: Option<DatabaseHandle>,
    pub(super) context: Arc<LeaseContext>,
    pub(super) runtime: Arc<OwnershipRuntime>,
}

impl OwnedHandle {
    pub fn execute(&self, sql: &str) -> Result<()> {
        self.handle.as_ref().unwrap().execute(sql)
    }

    pub fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.handle.as_ref().unwrap().query(sql)
    }

    pub fn in_transaction(&self) -> Result<bool> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().in_transaction()
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.context.check(Some(false))?;
        self.handle.as_ref().unwrap().checkpoint()
    }

    pub fn compact_wal(&self) -> Result<()> {
        self.context.check(Some(false))?;
        self.handle.as_ref().unwrap().compact_wal()
    }

    pub fn resources(&self) -> Result<ResourceUsage> {
        self.handle.as_ref().unwrap().resources()
    }

    pub fn is_readonly(&self) -> Result<bool> {
        self.context.check(None)?;
        self.handle.as_ref().unwrap().is_readonly()
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        drop(self.handle.take());
        let final_handle = {
            let mut state = self
                .context
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ensure_handle_count(&mut state.handles);
            state.handles == 0
        };
        if final_handle {
            let _ = self.runtime.finish_detach(&self.context);
        }
    }
}

fn ensure_handle_count(handles: &mut usize) {
    assert!(*handles > 0, "owned session accounting underflow");
    *handles -= 1;
}
