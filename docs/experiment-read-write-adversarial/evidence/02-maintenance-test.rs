
#[cfg(test)]
mod rw02_maintenance {
    use super::*;
    use turso_core::io::{Buffer, Completion, IO};
    #[test]
    fn rw02_maintenance_gate_is_independent() -> Result<()> {
        let cache = tempfile::tempdir()?;
        let database = RemoteDatabase::open(Arc::new(crate::object_store::MemoryObjectStore::default()), Namespace::new("rw02", "maintenance")?, cache.path(), 0)?;
        let deadline = RequestDeadline::new(&Budget::trusted_default())?;
        let held = database.maintenance_operation(&deadline)?;
        assert!(matches!(database.operations.try_read(), Err(TryLockError::WouldBlock)));
        let wal = database.io.open_file(&format!("{}-wal", database.io.database_path()), OpenFlags::Create, false)?;
        let b = Arc::new(Buffer::new_temporary(32));
        assert!(wal.pread(0, Completion::new_read(b, |_| None))?.finished());
        println!("RW02 {}", serde_json::json!({"hypothesis":"H3","arm":"maintenance_gate","operations_read_available":false,"wal_pread_completed":true}));
        drop(held);
        assert!(database.operations.try_read().is_ok());
        Ok(())
    }
}
