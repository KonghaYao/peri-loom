use super::{SessionOperation, MAX_SEGMENT_BATCH};
use crate::manifest::SEGMENT_SIZE;
use crate::object_store::{PutCondition, StoredObject};
use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Serialize;
use serde_json::{json, Value};

/// A body in the range a WAL chunk reaches, which is what the cold load reads
/// back through `Get`.
const BODY_BYTES: usize = 4 * 1024 * 1024;

/// The wire shape these fields had before they moved to base64: the same
/// structures under serde's own `Vec<u8>` encoding, so the size comparison
/// measures both sides instead of repeating a hard-coded ratio.
#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum LegacySessionOperation {
    UploadSegments {
        segments: Vec<Vec<u8>>,
    },
    Put {
        key: String,
        bytes: Vec<u8>,
        condition: PutCondition,
    },
}

#[derive(Serialize)]
struct LegacyStoredObject {
    bytes: Vec<u8>,
    etag: String,
}

/// High-entropy filler: a repeating pattern would compress the integer
/// encoding and understate the difference the comparison exists to show.
fn body(len: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

#[test]
fn put_request_carries_base64_and_still_reads_legacy_arrays() -> Result<()> {
    let bytes = body(4 * 1024);
    let wire = serde_json::to_value(SessionOperation::Put {
        key: "wal/chunks/head".to_owned(),
        bytes: bytes.clone(),
        condition: PutCondition::Any,
    })?;
    let encoded = wire["bytes"].as_str().context("put body is not a string")?;
    assert_eq!(STANDARD.decode(encoded)?, bytes);

    let legacy = serde_json::to_value(LegacySessionOperation::Put {
        key: "wal/chunks/head".to_owned(),
        bytes: bytes.clone(),
        condition: PutCondition::Any,
    })?;
    assert!(legacy["bytes"].is_array());
    let decoded: SessionOperation = serde_json::from_value(legacy)?;
    let SessionOperation::Put { bytes: decoded, .. } = decoded else {
        panic!("legacy put request decoded as another operation");
    };
    assert_eq!(decoded, bytes);
    Ok(())
}

#[test]
fn segment_batch_carries_base64_and_still_reads_legacy_arrays() -> Result<()> {
    let segments = vec![body(4 * 1024), body(8 * 1024)];
    let wire = serde_json::to_value(SessionOperation::UploadSegments {
        segments: segments.clone(),
    })?;
    let encoded = wire["segments"]
        .as_array()
        .context("segment batch is not an array")?;
    assert_eq!(encoded.len(), segments.len());
    for (encoded, segment) in encoded.iter().zip(&segments) {
        let encoded = encoded.as_str().context("segment is not a string")?;
        assert_eq!(&STANDARD.decode(encoded)?, segment);
    }

    let legacy = serde_json::to_value(LegacySessionOperation::UploadSegments {
        segments: segments.clone(),
    })?;
    assert!(legacy["segments"][0].is_array());
    let decoded: SessionOperation = serde_json::from_value(legacy)?;
    let SessionOperation::UploadSegments { segments: decoded } = decoded else {
        panic!("legacy segment batch decoded as another operation");
    };
    assert_eq!(decoded, segments);
    Ok(())
}

#[test]
fn stored_object_reply_carries_base64_and_still_reads_legacy_arrays() -> Result<()> {
    let object = StoredObject {
        bytes: body(4 * 1024),
        etag: "e".repeat(64),
    };
    let wire = serde_json::to_value(&object)?;
    let encoded = wire["bytes"]
        .as_str()
        .context("stored object body is not a string")?;
    assert_eq!(STANDARD.decode(encoded)?, object.bytes);

    let legacy = serde_json::to_value(LegacyStoredObject {
        bytes: object.bytes.clone(),
        etag: object.etag.clone(),
    })?;
    assert!(legacy["bytes"].is_array());
    let decoded: Option<StoredObject> = serde_json::from_value(legacy)?;
    assert_eq!(decoded.map(|decoded| decoded.bytes), Some(object.bytes));

    let missing: Option<StoredObject> = serde_json::from_value(Value::Null)?;
    assert!(missing.is_none());
    Ok(())
}

#[test]
fn base64_bodies_are_smaller_than_the_legacy_integer_arrays() -> Result<()> {
    let bytes = body(BODY_BYTES);
    let encoded = serde_json::to_vec(&SessionOperation::Put {
        key: "wal/chunks/head".to_owned(),
        bytes: bytes.clone(),
        condition: PutCondition::Any,
    })?;
    let legacy = serde_json::to_vec(&LegacySessionOperation::Put {
        key: "wal/chunks/head".to_owned(),
        bytes: bytes.clone(),
        condition: PutCondition::Any,
    })?;
    println!(
        "put body {} bytes: legacy JSON {} bytes, base64 JSON {} bytes",
        bytes.len(),
        legacy.len(),
        encoded.len()
    );
    assert!(legacy.len() > encoded.len() * 2);
    let decoded: SessionOperation = serde_json::from_slice(&encoded)?;
    let SessionOperation::Put { bytes: decoded, .. } = decoded else {
        panic!("encoded put request decoded as another operation");
    };
    assert_eq!(decoded, bytes);

    let segments: Vec<Vec<u8>> = (0..MAX_SEGMENT_BATCH).map(|_| body(SEGMENT_SIZE)).collect();
    let encoded = serde_json::to_vec(&SessionOperation::UploadSegments {
        segments: segments.clone(),
    })?;
    let legacy = serde_json::to_vec(&LegacySessionOperation::UploadSegments {
        segments: segments.clone(),
    })?;
    println!(
        "segment batch of {} x {} bytes: legacy JSON {} bytes, base64 JSON {} bytes",
        segments.len(),
        SEGMENT_SIZE,
        legacy.len(),
        encoded.len()
    );
    assert!(legacy.len() > encoded.len() * 2);
    let decoded: SessionOperation = serde_json::from_slice(&encoded)?;
    let SessionOperation::UploadSegments { segments: decoded } = decoded else {
        panic!("encoded segment batch decoded as another operation");
    };
    assert_eq!(decoded, segments);
    Ok(())
}

/// Every body position rejects a malformed body instead of accepting a partial
/// one, in both encodings: the legacy element type is `u8`, so an integer that
/// does not fit it fails rather than being narrowed.
#[test]
fn malformed_bodies_are_rejected_instead_of_truncated() -> Result<()> {
    let mut put = serde_json::to_value(SessionOperation::Put {
        key: "wal/chunks/head".to_owned(),
        bytes: vec![1, 2, 3],
        condition: PutCondition::Any,
    })?;
    for malformed in [
        json!([256]),
        json!([-1]),
        json!([1.5]),
        json!(["x"]),
        json!("not+base64"),
        json!("AAA"),
    ] {
        put["bytes"] = malformed.clone();
        let error = serde_json::from_value::<SessionOperation>(put.clone())
            .err()
            .with_context(|| format!("put body {malformed} was accepted"))?;
        println!("put body {malformed}: {error}");
    }

    let encoded = STANDARD.encode([1, 2, 3]);
    let mut batch = serde_json::to_value(SessionOperation::UploadSegments {
        segments: vec![vec![1, 2, 3]],
    })?;
    for malformed in [
        json!("not an array"),
        json!([[256]]),
        json!([["x"]]),
        json!(["not+base64"]),
        json!([encoded.clone(), "not+base64", encoded.clone()]),
    ] {
        batch["segments"] = malformed.clone();
        let error = serde_json::from_value::<SessionOperation>(batch.clone())
            .err()
            .with_context(|| format!("segment batch {malformed} was accepted"))?;
        println!("segment batch {malformed}: {error}");
    }

    let mut object = serde_json::to_value(StoredObject {
        bytes: vec![1, 2, 3],
        etag: "e".repeat(64),
    })?;
    for malformed in [json!([256]), json!(["x"]), json!("not+base64")] {
        object["bytes"] = malformed.clone();
        let error = serde_json::from_value::<StoredObject>(object.clone())
            .err()
            .with_context(|| format!("stored object body {malformed} was accepted"))?;
        println!("stored object body {malformed}: {error}");
    }
    Ok(())
}
