use std::fs::OpenOptions;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PreconditionFailed, PutCondition};
use peri_loom_v1::wal::{WalBatch, WalOperation};

#[test]
fn immutable_atomic_read_does_not_wait_for_an_unrelated_writer_lock() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(directory.path())?);
    let etag = store.put("stable", b"known durable bytes", PutCondition::Absent)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.path().join(".lock"))?;
    lock.lock()?;
    let (sent, received) = mpsc::channel();
    let reader = store.clone();
    let task = thread::spawn(move || {
        let _ = sent.send(reader.get("stable"));
    });
    let result = received.recv_timeout(Duration::from_secs(1));
    lock.unlock()?;
    task.join().unwrap();
    let object = result??.unwrap();
    assert_eq!(object.bytes, b"known durable bytes");
    assert_eq!(object.etag, etag);
    Ok(())
}

#[test]
fn compact_wal_payload_preserves_legacy_replay_bytes_and_rejects_malformed_base64() -> Result<()> {
    let bytes = vec![173; 65536];
    let legacy = serde_json::json!({"operations": [{"Write": {"offset": 32, "bytes": bytes}}]});
    let batch: WalBatch = serde_json::from_value(legacy)?;
    assert_eq!(
        batch.operations,
        vec![WalOperation::Write {
            offset: 32,
            bytes: bytes.clone()
        }]
    );
    let encoded = serde_json::to_vec(&batch)?;
    let mut compact: serde_json::Value = serde_json::from_slice(&encoded)?;
    assert!(compact["operations"][0]["Write"]["bytes"].is_string());
    assert!(encoded.len() < bytes.len() * 2);
    assert_eq!(serde_json::from_slice::<WalBatch>(&encoded)?, batch);
    compact["operations"][0]["Write"]["bytes"] = serde_json::json!("invalid-base64!");
    assert!(serde_json::from_value::<WalBatch>(compact).is_err());
    Ok(())
}

#[test]
fn concurrent_atomic_reads_never_observe_torn_payload_and_cas_stays_strict() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalObjectStore::open(directory.path())?);
    let first = vec![17; 65536];
    let second = vec![91; 65536];
    let original = store.put("mutable", &first, PutCondition::Absent)?;
    thread::scope(|threads| -> Result<()> {
        let writer = store.clone();
        let previous = original.clone();
        let initial = first.clone();
        let replacement = second.clone();
        let writing = threads.spawn(move || -> Result<String> {
            let mut etag = previous;
            for iteration in 0..8 {
                let bytes = if iteration % 2 == 0 {
                    &replacement
                } else {
                    &initial
                };
                etag = writer.put("mutable", bytes, PutCondition::Match(etag))?;
            }
            Ok(etag)
        });
        for _iteration in 0..32 {
            let object = store.get("mutable")?.unwrap();
            assert!(object.bytes == first || object.bytes == second);
        }
        let final_etag = writing.join().unwrap()?;
        assert_ne!(final_etag, original);
        assert!(store
            .put("mutable", b"stale", PutCondition::Match(original))
            .unwrap_err()
            .is::<PreconditionFailed>());
        assert_eq!(store.get("mutable")?.unwrap().etag, final_etag);
        Ok(())
    })
}

#[test]
fn local_envelope_reads_legacy_bytes_and_writes_compact_payload_without_changing_content(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalObjectStore::open(directory.path())?;
    let bytes = vec![37; 65536];
    let legacy = serde_json::json!({
        "key": "legacy",
        "bytes": bytes,
        "etag": "legacy-etag",
        "checksum": blake3::hash(&bytes).to_hex().to_string()
    });
    let legacy_path = directory
        .path()
        .join(format!("{}.obj", blake3::hash(b"legacy").to_hex()));
    std::fs::write(&legacy_path, serde_json::to_vec(&legacy)?)?;
    let loaded = store.get("legacy")?.unwrap();
    assert_eq!(loaded.bytes, bytes);
    assert_eq!(loaded.etag, "legacy-etag");
    let etag = store.put("legacy", &bytes, PutCondition::Match(loaded.etag))?;
    let encoded = std::fs::read(&legacy_path)?;
    let compact: serde_json::Value = serde_json::from_slice(&encoded)?;
    assert!(compact["bytes"].is_string());
    assert!(encoded.len() < bytes.len() * 2);
    let loaded = store.get("legacy")?.unwrap();
    assert_eq!(loaded.bytes, bytes);
    assert_eq!(loaded.etag, etag);
    let mut malformed = compact;
    malformed["bytes"] = serde_json::json!("invalid-base64!");
    std::fs::write(&legacy_path, serde_json::to_vec(&malformed)?)?;
    assert!(store.get("legacy").is_err());
    Ok(())
}
