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
    Ok((bytes, checksum))
}

pub(super) fn decode(bytes: &[u8]) -> Result<CatalogRoot> {
    Ok(decode_with_digest(bytes)?.0)
}

pub(super) fn decode_with_digest(bytes: &[u8]) -> Result<(CatalogRoot, String)> {
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
    ensure!(
        matches!(root.version, 1 | 2 | 4),
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
    Ok((root, envelope.checksum))
}
