use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use crate::database::RemoteDatabase;
use crate::namespace::Namespace;
use crate::object_store::MemoryObjectStore;
use crate::session::{AdmissionError, Budget, SqlEvidence};
use anyhow::Result;

thread_local! {
    static AFTER_DONE: Cell<Option<Duration>> = const { Cell::new(None) };
}

pub(super) fn after_done() {
    if let Some(delay) = AFTER_DONE.take() {
        std::thread::sleep(delay);
    }
}

#[test]
fn committed_statement_late_budget_failure_has_terminal_evidence() -> Result<()> {
    let cache = tempfile::tempdir()?;
    let database = Arc::new(RemoteDatabase::open(
        Arc::new(MemoryObjectStore::default()),
        Namespace::new("evidence", "done")?,
        cache.path(),
        1,
    )?);
    let session = database.session()?;
    session.execute("CREATE TABLE items(value INTEGER)")?;
    let budget = Budget {
        max_query_duration: Duration::from_millis(100),
        ..Budget::default()
    };
    // 仅在引擎 Done 后注入等待，避免依赖宿主速度碰巧越过预算。
    AFTER_DONE.set(Some(Duration::from_millis(250)));
    let error = session
        .execute_with_budget("INSERT INTO items VALUES(1)", &budget)
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<SqlEvidence>(),
        Some(&SqlEvidence::Settled),
        "{error:#}"
    );
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::Deadline)
    );
    assert_eq!(
        database.session()?.query("SELECT value FROM items")?[0][0].to_string(),
        "1"
    );
    Ok(())
}
