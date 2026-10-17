//! Decoded authority heads, held per process and keyed by the digest of the exact bytes
//! they were decoded from.
//!
//! A lease validation reads the head and decodes it whole, and the worker asks for that
//! validation once per request, so a process that serves requests re-parses the same
//! unchanged document request after request. Measured on a 95 KB head (dev profile): the
//! decode is 2.9 ms of a 4.5 ms validation, and a pre-v5 head was dominated by the
//! idempotency ledger, which only grew. The ledger window is now bounded and its overflow
//! is paged, so the inline head no longer follows how long the namespace has been running
//! (see `LEASE_COST` in the scaling tests); the cache still removes the repeated parse of
//! whatever head remains.
//!
//! Reusing a decode is sound because the key is computed from the bytes in hand: a hit
//! can only ever return the decode of the very bytes that were read now, so nothing is
//! assumed unchanged and the read stays live. A head that moved is decoded in full, and a
//! lock that cannot be taken reads as a miss. What this removes is the repeated parse and
//! nothing else: every check the caller runs on the returned head still runs, on the
//! document the store returned for this request.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};

use super::CatalogRoot;

/// Heads held at once. Several databases share one authority process and every operation
/// on one of them re-reads its head, so the working set is the databases being served;
/// beyond it the oldest head leaves and is decoded again on its next read.
const MAX_ENTRIES: usize = 32;
/// Encoded bytes those heads may stand for. A head that alone exceeds this is never held:
/// a process must not pin memory proportional to one document an operator grew.
const MAX_BYTES: usize = 16 * 1024 * 1024;

struct Held {
    digest: String,
    checksum: String,
    root: Arc<CatalogRoot>,
    bytes: usize,
}

fn cache() -> &'static Mutex<VecDeque<Held>> {
    static CACHE: OnceLock<Mutex<VecDeque<Held>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(VecDeque::new()))
}

/// The decode of exactly these bytes, if this process already made one. A failed lock
/// reports a miss, which costs a decode and cannot weaken anything.
pub(super) fn held(digest: &str) -> Option<(Arc<CatalogRoot>, String)> {
    let cache = cache().lock().ok()?;
    cache
        .iter()
        .find(|held| held.digest == digest)
        .map(|held| (held.root.clone(), held.checksum.clone()))
}

/// Records a decode under the digest of the bytes it produced. A refused or evicted head
/// only makes the next read decode again, so a full cache never becomes a stale answer.
pub(super) fn remember(digest: String, checksum: String, root: Arc<CatalogRoot>, bytes: usize) {
    if bytes > MAX_BYTES {
        return;
    }
    let Ok(mut cache) = cache().lock() else {
        return;
    };
    if cache.iter().any(|held| held.digest == digest) {
        return;
    }
    let mut total = cache.iter().map(|held| held.bytes).sum::<usize>() + bytes;
    while !cache.is_empty() && (cache.len() >= MAX_ENTRIES || total > MAX_BYTES) {
        let Some(oldest) = cache.pop_front() else {
            break;
        };
        total -= oldest.bytes;
    }
    cache.push_back(Held {
        digest,
        checksum,
        root,
        bytes,
    });
}
