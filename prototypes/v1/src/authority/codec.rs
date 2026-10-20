use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::CatalogRoot;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<'input> {
    #[serde(borrow)]
    root: &'input RawValue,
    checksum: String,
}

pub(super) fn encode(root: &CatalogRoot) -> Result<Vec<u8>> {
    Ok(encode_with_digest(root)?.0)
}

/// The envelope checksum is blake3 over the serialized head, so it already names the exact
/// content a reader decodes. Handing it back costs nothing and is what lets an incarnation
/// tell "the head I validated myself" from "a head that appeared behind my back" without
/// re-encoding or re-reading anything.
pub(super) fn encode_with_digest(root: &CatalogRoot) -> Result<(Vec<u8>, String)> {
    #[cfg(test)]
    let _probe = super::scaling_probe::stage("encode", super::scaling_probe::entries(root));
    let serialized = serde_json::to_string(root)?;
    let checksum = blake3::hash(serialized.as_bytes()).to_hex().to_string();
    let bytes = serde_json::to_vec(&Envelope {
        root: RawValue::from_string(serialized)?.as_ref(),
        checksum: checksum.clone(),
    })?;
    // The inline identity tables have their own ceilings; this one bounds the document that
    // the CAS actually carries, including the envelope, ownership and receipt state. It runs
    // before the write, so an oversized candidate is rejected with no published state.
    ensure!(
        bytes.len() <= super::types::MAX_ROOT_BYTES,
        "authority root is {} bytes, above the {}-byte publication ceiling",
        bytes.len(),
        super::types::MAX_ROOT_BYTES
    );
    crate::work_metrics::observe_root_encoding(bytes.len(), root.identity_counts());
    Ok((bytes, checksum))
}

/// The decoded head, shared rather than copied: a reader that only inspects the head pays
/// neither the parse nor a deep copy of a document it did not change. A caller that needs
/// to own one — the publication path attaches the store to the registries — clones it.
pub(super) fn decode(bytes: &[u8]) -> Result<Arc<CatalogRoot>> {
    Ok(decode_with_digest(bytes)?.0)
}

pub(super) fn decode_with_digest(bytes: &[u8]) -> Result<(Arc<CatalogRoot>, String)> {
    // The digest is taken over the bytes as they were read, so a hit hands back the
    // decode of this document and not of a document that happened to be here before it.
    let digest = blake3::hash(bytes).to_hex().to_string();
    if let Some((root, checksum)) = super::decode_cache::held(&digest) {
        return Ok((root, checksum));
    }
    let (root, checksum) = parse(bytes)?;
    let root = Arc::new(root);
    super::decode_cache::remember(digest, checksum.clone(), root.clone(), bytes.len());
    Ok((root, checksum))
}

fn parse(bytes: &[u8]) -> Result<(CatalogRoot, String)> {
    #[cfg(test)]
    let _probe = super::scaling_probe::stage("decode", bytes.len());
    let envelope: Envelope<'_> =
        serde_json::from_slice(bytes).context("invalid authority root envelope")?;
    ensure!(
        envelope.checksum
            == blake3::hash(envelope.root.get().as_bytes())
                .to_hex()
                .as_str(),
        "authority root checksum mismatch"
    );
    let root: CatalogRoot = serde_json::from_str(envelope.root.get())?;
    match root.pin_schema {
        None => {
            // No schema means either the original pre-split shape (non-empty legacy
            // vectors) or the short-lived split-transitional shape written before the
            // marker existed (empty legacy vectors, entries already split out). The two
            // representations may not coexist; an empty legacy vector side is accepted
            // and upgraded to the current schema on the next publication.
            let legacy_vectors = root.pins.values().any(|entries| !entries.is_empty());
            if legacy_vectors {
                ensure!(
                    root.pin_entries.is_empty(),
                    "legacy reader pin root mixes pin representations"
                );
            }
        }
        Some(schema) => {
            ensure!(
                matches!(schema, 1 | super::types::PIN_SCHEMA_VERSION),
                "unsupported reader pin schema"
            );
            ensure!(
                root.pins.values().all(|entries| entries.is_empty()),
                "split reader pin root carries legacy inline entries"
            );
        }
    }
    ensure!(
        matches!(root.version, 1 | 2 | 4 | 5),
        "unsupported authority root version"
    );
    ensure!(
        if root.version == 1 {
            root.ownership.is_none()
        } else {
            root.ownership.is_some()
        },
        "authority ownership schema version mismatch"
    );
    // The decoder is the one path every reader shares, including ownership views that do
    // not run the publication shape check. Rejecting an over-ceiling head here keeps a
    // document that predates the ceilings from being treated as current.
    root.validate_identity_ceilings()?;
    Ok((root, envelope.checksum))
}
