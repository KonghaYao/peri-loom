use std::sync::Arc;

use anyhow::Result;
use peri_loom_v1::authority::Authority;
use peri_loom_v1::capabilities::Capabilities;
use peri_loom_v1::database::RemoteDatabase;
use peri_loom_v1::namespace::Namespace;
use peri_loom_v1::object_store::LocalObjectStore;

fn open() -> Result<(tempfile::TempDir, tempfile::TempDir, Arc<RemoteDatabase>)> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let database = Arc::new(RemoteDatabase::open(
        store,
        Namespace::new("sessions", "database")?,
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)")?;
    Ok((objects, cache, database))
}

#[test]
fn transactions_span_calls_but_not_connections() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let first = database.session()?;
    let second = database.session()?;
    first.execute("BEGIN")?;
    first.execute("INSERT INTO items VALUES(1)")?;
    assert!(first.in_transaction()?);
    assert!(!second.in_transaction()?);
    assert_eq!(
        first.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    assert_eq!(
        second.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "0"
    );
    assert!(second.execute("INSERT INTO items VALUES(2)").is_err());
    first.execute("COMMIT")?;
    assert!(!first.in_transaction()?);
    second.execute("INSERT INTO items VALUES(2)")?;
    first.execute("BEGIN; INSERT INTO items VALUES(3); ROLLBACK")?;
    assert_eq!(
        database.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    Ok(())
}

#[test]
fn dropping_writer_rolls_back_and_releases_native_lock() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let survivor = database.session()?;
    for identifier in 1..=12 {
        let writer = database.session()?;
        writer.execute(&format!("BEGIN; INSERT INTO items VALUES({identifier})"))?;
        assert!(writer.in_transaction()?);
        drop(writer);
        survivor.execute(&format!("INSERT INTO items VALUES({})", identifier + 100))?;
    }
    assert_eq!(
        survivor.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "12"
    );
    assert_eq!(
        survivor.query("SELECT min(id) FROM items")?[0][0].to_string(),
        "101"
    );
    database.compact_wal()?;
    Ok(())
}

#[test]
fn read_transaction_keeps_snapshot_and_drop_releases_checkpoint_lock() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    database.execute("INSERT INTO items VALUES(1)")?;
    let reader = database.session()?;
    reader.execute("BEGIN")?;
    assert_eq!(
        reader.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    database.execute("INSERT INTO items VALUES(2)")?;
    assert_eq!(
        reader.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "1"
    );
    assert!(database.compact_wal().is_err());
    drop(reader);
    database.compact_wal()?;
    assert_eq!(
        database.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    Ok(())
}

#[test]
fn active_writer_blocks_compaction_without_discarding_transaction() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    let writer = database.session()?;
    writer.execute("BEGIN; INSERT INTO items VALUES(1)")?;
    let confirmed_lsn = database.io().wal_lsn();
    assert!(database.compact_wal().is_err());
    assert_eq!(database.io().wal_lsn(), confirmed_lsn);
    assert!(writer.in_transaction()?);
    writer.execute("COMMIT")?;
    database.compact_wal()?;
    assert_eq!(
        database.query("SELECT id FROM items")?[0][0].to_string(),
        "1"
    );
    Ok(())
}

#[test]
fn every_session_statement_restores_full_sync_before_write() -> Result<()> {
    let (objects, cache, database) = open()?;
    let store = Arc::new(LocalObjectStore::open(objects.path())?);
    let session = database.session()?;
    let before = database.io().wal_lsn();
    session.execute("PRAGMA synchronous=OFF; INSERT INTO items VALUES(7)")?;
    assert!(database.io().wal_lsn() > before);
    session.query("PRAGMA synchronous=OFF")?;
    session.execute("INSERT INTO items VALUES(8)")?;
    drop(session);
    drop(database);
    std::fs::remove_dir_all(cache.path())?;
    let cold = tempfile::tempdir()?;
    let restored = RemoteDatabase::open(
        store,
        Namespace::new("sessions", "database")?,
        cold.path(),
        0,
    )?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    Ok(())
}

#[test]
fn stale_sessions_reject_ingress_but_drop_still_aborts_transaction() -> Result<()> {
    let objects = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let namespace = Namespace::new("sessions", "restore")?;
    let authority = Authority::open(
        Arc::new(LocalObjectStore::open(objects.path())?),
        namespace.clone(),
    )?;
    let capabilities = Capabilities::new(authority.clone());
    let database = Arc::new(RemoteDatabase::open(
        authority.branch_store("main")?,
        namespace.clone(),
        cache.path(),
        0,
    )?);
    database.execute("CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)")?;
    capabilities.snapshot("main", "saved")?;
    let session = database.session()?;
    session.execute("BEGIN; INSERT INTO items VALUES(2)")?;
    capabilities.restore("main", "saved")?;
    assert!(session.execute("COMMIT").is_err());
    assert!(session.query("SELECT count(*) FROM items").is_err());
    assert!(session.in_transaction().is_err());
    assert!(database.session().is_err());
    drop(session);
    drop(database);
    let cold = tempfile::tempdir()?;
    let restored = Arc::new(RemoteDatabase::open(
        authority.branch_store("main")?,
        namespace,
        cold.path(),
        0,
    )?);
    restored.session()?.execute("INSERT INTO items VALUES(3)")?;
    assert_eq!(
        restored.query("SELECT count(*) FROM items")?[0][0].to_string(),
        "2"
    );
    Ok(())
}

#[test]
fn native_sessions_support_parallel_reads_without_shared_transaction() -> Result<()> {
    let (_objects, _cache, database) = open()?;
    database.execute("INSERT INTO items VALUES(1)")?;
    let first = database.session()?;
    let second = database.session()?;
    std::thread::scope(|scope| {
        let left = scope.spawn(|| -> Result<()> {
            first.execute("BEGIN")?;
            for _ in 0..25 {
                assert_eq!(first.query("SELECT id FROM items")?[0][0].to_string(), "1");
            }
            first.execute("ROLLBACK")
        });
        let right = scope.spawn(|| -> Result<()> {
            for _ in 0..25 {
                assert!(!second.in_transaction()?);
                assert_eq!(second.query("SELECT id FROM items")?[0][0].to_string(), "1");
            }
            Ok(())
        });
        left.join().unwrap()?;
        right.join().unwrap()
    })
}
