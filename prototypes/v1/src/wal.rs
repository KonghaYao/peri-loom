use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::manifest::WalPosition;
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition};

pub trait DurableWal: Send + Sync {
    fn append(&self, namespace: &Namespace, epoch: u64, batch: WalBatch) -> Result<u64>;
    fn read(&self, namespace: &Namespace, from_lsn: u64) -> Result<Vec<WalRecord>>;
    fn truncate(&self, namespace: &Namespace, before_lsn: u64) -> Result<()>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalBatch {
    pub operations: Vec<WalOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOperation {
    Write {
        offset: u64,
        #[serde(with = "crate::object_store::payload")]
        bytes: Vec<u8>,
    },
    Truncate {
        size: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub append_id: String,
    pub namespace: Namespace,
    pub epoch: u64,
    pub lsn: u64,
    pub timestamp_millis: u64,
    pub batch: WalBatch,
    #[serde(default)]
    pub commit: Option<WalCommit>,
}

/// One immutable record and the canonical bytes that name its chunk and receipt.
pub(crate) struct EncodedWalRecord {
    record: WalRecord,
    bytes: Vec<u8>,
    hash: String,
}

impl EncodedWalRecord {
    fn new(record: WalRecord) -> Result<Self> {
        let bytes = serde_json::to_vec(&record)?;
        let hash = blake3::hash(&bytes).to_hex().to_string();
        Ok(Self {
            record,
            bytes,
            hash,
        })
    }

    pub(crate) fn record(&self) -> &WalRecord {
        &self.record
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalCommit {
    pub lsn: u64,
    pub position: WalPosition,
}

/// A receipt whose record is no longer individually addressable.
///
/// `digest` is the chunk identity the append published: it proves a retry carries the exact
/// request that produced `lsn` without retaining the record itself. Legacy indexes predate
/// `receipt_digests`, so an archived legacy receipt has no digest and a retry must fail
/// closed instead of appending a second copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchivedReceipt {
    pub(crate) lsn: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) digest: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WalMetadata {
    pub epoch: u64,
    pub last_lsn: u64,
    pub replay_baseline_lsn: u64,
    pub(crate) entries: Vec<Entry>,
    pub(crate) receipts: BTreeMap<String, u64>,
    #[serde(default)]
    pub(crate) receipt_digests: BTreeMap<String, String>,
    /// Root of the receipt archive written by `truncate`.
    ///
    /// Receipts for records folded into the baseline leave the inline map and become page
    /// entries here. The head stays small, but a duplicate append id can still be answered
    /// with the digest proof it was originally committed with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) receipt_index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) entry_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) entry_index: Option<String>,
    #[serde(default)]
    pub commit: Option<WalCommit>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) lsn: u64,
    pub(crate) hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WalIndexSegment {
    pub(crate) version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) previous: Option<String>,
    pub(crate) entries: Vec<Entry>,
}

pub(crate) const WAL_INDEX_INLINE_ENTRIES: usize = 256;
pub(crate) const WAL_INDEX_SEGMENT_LIMIT: usize = WAL_INDEX_INLINE_ENTRIES;

impl WalIndexSegment {
    pub(crate) fn decode(hash: &str, bytes: &[u8]) -> Result<Self> {
        ensure!(
            valid_entry_index(hash) && blake3::hash(bytes).to_hex().as_str() == hash,
            "WAL index segment checksum mismatch"
        );
        let segment: Self = serde_json::from_slice(bytes)?;
        ensure!(
            segment.version == 1
                && !segment.entries.is_empty()
                && segment.entries.len() <= WAL_INDEX_SEGMENT_LIMIT,
            "invalid WAL index segment"
        );
        ensure!(
            segment.previous.as_deref().is_none_or(valid_entry_index),
            "invalid WAL index segment predecessor"
        );
        ensure!(
            segment
                .entries
                .iter()
                .all(|entry| entry.lsn > 0 && valid_entry_index(&entry.hash)),
            "invalid WAL index entry hash"
        );
        Ok(segment)
    }

    pub(crate) fn encode(
        previous: Option<String>,
        entries: Vec<Entry>,
    ) -> Result<(String, Vec<u8>)> {
        ensure!(
            !entries.is_empty() && entries.len() <= WAL_INDEX_SEGMENT_LIMIT,
            "invalid WAL index segment size"
        );
        ensure!(
            entries
                .windows(2)
                .all(|pair| pair[0].lsn.checked_add(1) == Some(pair[1].lsn)
                    || (pair[0].lsn == pair[1].lsn && pair[0].lsn > 0)),
            "unordered WAL index segment"
        );
        let segment = Self {
            version: 1,
            previous,
            entries,
        };
        let bytes = serde_json::to_vec(&segment)?;
        Ok((blake3::hash(&bytes).to_hex().to_string(), bytes))
    }
}

impl WalMetadata {
    pub(crate) fn normalize_entries(&mut self) -> Result<()> {
        ensure!(
            self.entry_count.is_some() || self.entry_index.is_none(),
            "missing WAL entry count"
        );
        self.entry_count.get_or_insert(self.entries.len() as u64);
        let count = self.entry_count.unwrap();
        ensure!(
            count >= self.entries.len() as u64
                && (self.entry_index.is_some() || count == self.entries.len() as u64),
            "invalid WAL entry count"
        );
        ensure!(
            self.entry_index.as_deref().is_none_or(valid_entry_index),
            "invalid WAL index segment root"
        );
        ensure!(
            self.entries.last().map_or(0, |entry| entry.lsn) == self.last_lsn,
            "incomplete WAL index"
        );
        validate_entry_sequence(&self.entries, self.replay_baseline_lsn)?;
        if self.last_lsn == 0 {
            ensure!(
                count == 0 && self.entry_index.is_none() && self.replay_baseline_lsn == 0,
                "invalid empty WAL index"
            );
        } else {
            ensure!(
                !self.entries.is_empty() && self.replay_baseline_lsn <= self.last_lsn,
                "invalid WAL baseline"
            );
            if self.entry_index.is_none() {
                ensure!(
                    self.entries[0].lsn == self.replay_baseline_lsn.max(1),
                    "invalid WAL replay baseline"
                );
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_entry_sequence(entries: &[Entry], baseline: u64) -> Result<()> {
    ensure!(
        entries
            .iter()
            .all(|entry| entry.lsn > 0 && valid_entry_index(&entry.hash)),
        "invalid WAL entry identity"
    );
    ensure!(
        entries
            .windows(2)
            .all(|pair| pair[0].lsn.checked_add(1) == Some(pair[1].lsn)
                || (baseline > 0 && pair[0].lsn == baseline && pair[1].lsn == baseline)),
        "unordered WAL index"
    );
    Ok(())
}

pub(crate) fn valid_entry_index(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub struct RemoteWal {
    store: Arc<dyn ObjectStore>,
}

/// One idempotency answer, from the inline window or the durable archive.
struct AppendReceipt {
    lsn: u64,
    digest: Option<String>,
    archived: bool,
}

#[derive(Debug)]
struct ChunkUploadFailure(anyhow::Error);

impl std::fmt::Display for ChunkUploadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ChunkUploadFailure {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalAppendRequest {
    pub namespace: Namespace,
    pub epoch: u64,
    pub batch: WalBatch,
    pub position: Option<WalPosition>,
    pub append_id: String,
    pub timestamp_millis: u64,
}

/// The largest raw `Write` payload and operation count one append record may carry.
///
/// These are the limits [`WalAppendRequest::validate`] enforces on the authority. A pending
/// native batch can legitimately exceed them (a large committing transaction writes more frames
/// than one RPC body can hold), so the worker splits such a batch before it ever becomes a
/// request: an over-limit batch is not a reason to fail the database closed.
pub(crate) const MAX_APPEND_PAYLOAD_BYTES: usize =
    crate::authority_rpc::protocol::MAX_RPC_BYTES / 2;
pub(crate) const MAX_APPEND_OPERATIONS: usize = 16_384;

/// The largest raw replay image a single baseline record may carry.
///
/// A baseline record is uploaded as one object whose `Write` payload is base64 in JSON, and
/// the object-store RPC encodes that body once more. 12 MiB stays well below `MAX_RPC_BYTES`
/// after both expansions, so truncating a large WAL no longer depends on the image fitting in
/// one request. Every part shares the baseline LSN: LSNs name logical records, and the parts
/// are one logical baseline record split for transport.
const WAL_BASELINE_PART_BYTES: usize = 12 * 1024 * 1024;

/// Branch-scoped content-addressed pages for receipts whose records were truncated away.
pub(crate) const WAL_RECEIPT_PAGE_PREFIX: &str = "wal/receipt-pages";
/// Receipts written per page-tree update. Batching bounds both the page count touched and
/// the bytes each archive attempt rewrites; the tree itself keeps every page small.
pub(crate) const WAL_RECEIPT_ARCHIVE_BATCH: usize = 64;
/// Digested receipts kept inline in the WAL head before older ones roll out. Legacy
/// receipts without a digest are exempt and remain inline, so the effective inline count is
/// `legacy_count + 1` while such receipts exist. Older digested receipts move to the durable
/// page tree in batches, so a normal append only rewrites a bounded hot window.
pub(crate) const WAL_RECEIPT_INLINE_LIMIT: usize = 256;
/// Slack left after a roll. Archiving one receipt per append would make every append a roll
/// and pay the archive validation walk each time; a batch amortises that cost.
pub(crate) const WAL_RECEIPT_ROLL_BATCH: usize = 64;

impl WalAppendRequest {
    fn record(&self, lsn: u64) -> WalRecord {
        WalRecord {
            append_id: self.append_id.clone(),
            namespace: self.namespace.clone(),
            epoch: self.epoch,
            lsn,
            timestamp_millis: self.timestamp_millis,
            batch: self.batch.clone(),
            commit: self.position.map(|position| WalCommit { lsn, position }),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        Namespace::new(
            self.namespace.tenant.clone(),
            self.namespace.database.clone(),
        )?;
        ensure!(
            self.epoch > 0
                && self.append_id.len() <= 128
                && !self.append_id.is_empty()
                && self
                    .append_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid WAL append identity"
        );
        ensure!(
            self.batch.operations.len() <= MAX_APPEND_OPERATIONS,
            "WAL operation limit exceeded"
        );
        let mut bytes = 0usize;
        for operation in &self.batch.operations {
            if let WalOperation::Write { bytes: payload, .. } = operation {
                bytes = bytes
                    .checked_add(payload.len())
                    .context("WAL payload overflow")?;
            }
        }
        ensure!(
            bytes <= MAX_APPEND_PAYLOAD_BYTES,
            "WAL payload limit exceeded"
        );
        Ok(())
    }
}

/// Partitions a batch into records [`WalAppendRequest::validate`] accepts, preserving operation
/// order.
///
/// An append of several records is still one logical write: the caller publishes them in order
/// and puts the native commit position on the last record only. A prefix that survives an
/// interrupted append replays as the exact frames the engine wrote without a commit marker, so
/// recovery ignores the tail instead of inventing a transaction. Splitting one `Write` at a byte
/// boundary is equivalent because replay applies writes by absolute offset.
fn split_batch(batch: WalBatch) -> Result<Vec<WalBatch>> {
    fn flush(current: &mut WalBatch, bytes: &mut usize, chunks: &mut Vec<WalBatch>) {
        if !current.operations.is_empty() {
            chunks.push(std::mem::take(current));
            *bytes = 0;
        }
    }

    let mut chunks = Vec::new();
    let mut current = WalBatch::default();
    let mut bytes = 0usize;
    for operation in batch.operations {
        match operation {
            WalOperation::Write {
                offset,
                bytes: payload,
            } => {
                if payload.is_empty() {
                    if current.operations.len() == MAX_APPEND_OPERATIONS {
                        flush(&mut current, &mut bytes, &mut chunks);
                    }
                    current.operations.push(WalOperation::Write {
                        offset,
                        bytes: payload,
                    });
                    continue;
                }
                let mut rest = payload;
                let mut cursor = 0usize;
                while !rest.is_empty() {
                    if bytes == MAX_APPEND_PAYLOAD_BYTES
                        || current.operations.len() == MAX_APPEND_OPERATIONS
                    {
                        flush(&mut current, &mut bytes, &mut chunks);
                    }
                    let take = rest.len().min(MAX_APPEND_PAYLOAD_BYTES - bytes);
                    let tail = rest.split_off(take);
                    let at = offset
                        .checked_add(u64::try_from(cursor)?)
                        .context("WAL write offset overflow")?;
                    current.operations.push(WalOperation::Write {
                        offset: at,
                        bytes: rest,
                    });
                    bytes += take;
                    cursor += take;
                    rest = tail;
                }
            }
            WalOperation::Truncate { size } => {
                if current.operations.len() == MAX_APPEND_OPERATIONS {
                    flush(&mut current, &mut bytes, &mut chunks);
                }
                current.operations.push(WalOperation::Truncate { size });
            }
        }
    }
    flush(&mut current, &mut bytes, &mut chunks);
    // An empty batch stays one (empty) record: `append_commit` has always published a record
    // for it, and callers' receipts are keyed by the append identity.
    if chunks.is_empty() {
        chunks.push(WalBatch::default());
    }
    Ok(chunks)
}

/// Bytes a replay may hold, and the window that budget buys.
///
/// Without a window a cold open pays one serial round trip per chunk, so its cost is the whole
/// history. The budget is stated in *bytes* rather than in chunks because it is memory, not
/// request count, that must not grow with the history. Charging a constant per unit makes the
/// bound arithmetic on that constant rather than an estimate from the sizes seen so far — a later
/// chunk may be larger than any earlier one, so an estimate could be exceeded by the very fetch it
/// admitted. The window is capped separately as well: every fetch occupies one of the authority
/// client's call slots (`authority_rpc::client::LIVE_CONNECTIONS`) that the other sessions on this
/// worker need.
const WAL_REPLAY_BYTES_ENV: &str = "PERI_WORKER_WAL_REPLAY_BYTES";
/// What the ledger charges one unit of residency, in the form the fetch actually holds it.
///
/// Deliberately not the payload ceiling `WalAppendRequest::validate` measures. That ceiling caps
/// the raw `Write` payloads of one append, while the bytes a replay holds are those payloads
/// encoded twice: a chunk is JSON with each payload base64 encoded (at most 4/3 of them) and the
/// authority `Get` returning it base64 encodes that chunk again (at most 16/9 of the payloads).
/// The RPC ceiling covers either form, and it is not a number invented here — it is what the
/// client refuses to read as a response body, so a larger fetch would fail rather than be held.
pub(crate) const WAL_CHUNK_CHARGE: usize = crate::authority_rpc::protocol::MAX_RPC_BYTES;
/// Units of residency the default budget buys: the four fetches of the window this replaces, plus
/// the record being handed to the visitor.
const WAL_REPLAY_DEFAULT_UNITS: usize = 5;
const WAL_REPLAY_BYTES_DEFAULT: usize = WAL_REPLAY_DEFAULT_UNITS * WAL_CHUNK_CHARGE;
const WAL_REPLAY_WINDOW_MAX: usize = 8;
/// Fetches every WAL replay on this worker may run at once.
///
/// A per-replay window bounds one replay, not the worker. Cold opens of *different* databases run
/// concurrently — the registry merges same-key opens and lets other keys proceed outside its lock
/// — so `N` of them would each spend a whole window and the reservation below would hold only for
/// the first. The permits are process-wide for the same reason the gate they protect is: one
/// authority client per worker.
const WAL_REPLAY_FETCH_PERMITS: usize = crate::authority_rpc::client::LIVE_CONNECTIONS / 3;

// One replay has to be able to fill its window out of the shared permits, and every replay
// together — however many databases are cold at once — has to stay inside the fraction of the
// call gate that the per-replay cap was asserting on its own.
const _: () = assert!(
    WAL_REPLAY_WINDOW_MAX <= WAL_REPLAY_FETCH_PERMITS,
    "one WAL replay has to be able to fill its window from the shared fetch permits"
);
const _: () = assert!(
    WAL_REPLAY_FETCH_PERMITS * 3 <= crate::authority_rpc::client::LIVE_CONNECTIONS,
    "WAL replays may hold at most a third of the authority client call gate"
);

// The charge has to cover the twice-encoded payload ceiling, or the ledger bounds less than one
// fetch holds — the state this constant was corrected out of, where the conversion from payload
// bytes to held bytes was left implicit.
const _: () = assert!(
    WAL_CHUNK_CHARGE * 9 >= 16 * (crate::authority_rpc::protocol::MAX_RPC_BYTES / 2),
    "the fetch charge has to cover the twice-encoded payload ceiling"
);

/// The shared fetch permits, held by a replay thread across the fetch round trip and the decode
/// that follows it — the span the authority call slot is occupied for — and released before the
/// record reaches the visitor.
///
/// Delivery residency is deliberately not covered here: a record waiting to be handed over is
/// charged a whole [`WAL_CHUNK_CHARGE`] by the byte ledger, which is the account that bounds
/// memory, while this one bounds call slots and nothing else. Charging delivery to both would make
/// the permit count stop meaning "calls in flight".
///
/// A caller that runs out waits for a fetch to finish instead of failing, so the wait is bounded
/// by the authority request timeout and no caller is admitted past the limit.
struct FetchPermits {
    available: Mutex<usize>,
    released: Condvar,
}

static WAL_REPLAY_PERMITS: FetchPermits = FetchPermits::new(WAL_REPLAY_FETCH_PERMITS);

struct FetchPermit<'permits>(&'permits FetchPermits);

impl FetchPermits {
    const fn new(limit: usize) -> Self {
        Self {
            available: Mutex::new(limit),
            released: Condvar::new(),
        }
    }

    fn acquire(&self) -> FetchPermit<'_> {
        let mut available = self.lock();
        while *available == 0 {
            available = self
                .released
                .wait(available)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *available -= 1;
        FetchPermit(self)
    }

    /// A poisoned mutex would otherwise leak a permit permanently; the counter is the only thing
    /// in here, so recovering it is safe.
    fn lock(&self) -> std::sync::MutexGuard<'_, usize> {
        self.available
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for FetchPermit<'_> {
    fn drop(&mut self) {
        let mut available = self.0.lock();
        *available += 1;
        self.0.released.notify_one();
    }
}

/// Illegal values fail closed: replay is on the open path, and a budget silently coerced to
/// something else would hide the misconfiguration behind a slower or hungrier open.
fn wal_replay_window() -> Result<usize> {
    let budget = match std::env::var(WAL_REPLAY_BYTES_ENV) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("invalid {WAL_REPLAY_BYTES_ENV}"))?,
        Err(std::env::VarError::NotPresent) => WAL_REPLAY_BYTES_DEFAULT,
        Err(error) => return Err(error.into()),
    };
    ensure!(
        budget > 0,
        "invalid {WAL_REPLAY_BYTES_ENV}: expected a positive byte budget"
    );
    Ok(wal_replay_window_for(budget))
}

/// The budget expressed as a window. One of the units it buys is the record the visitor is being
/// handed rather than a fetch: the refill starts before that delivery, so the window behind it is
/// still in flight. A replay that fetched nothing cannot make progress, so every accepted budget —
/// including one smaller than a single charge — buys at least one fetch; the upper cap keeps the
/// call-slot gate described above intact.
fn wal_replay_window_for(budget: usize) -> usize {
    (budget / WAL_CHUNK_CHARGE)
        .saturating_sub(1)
        .clamp(1, WAL_REPLAY_WINDOW_MAX)
}

/// Reads and verifies one chunk. Runs on a prefetch thread, so it borrows the store and
/// owns nothing but the key it derives.
fn load_chunk_record(
    store: &dyn ObjectStore,
    namespace: &Namespace,
    entry: &Entry,
) -> Result<WalRecord> {
    // Released when this thread returns, before the record reaches the visitor: the record's
    // residency is the ledger's account, not this one.
    let _permit = WAL_REPLAY_PERMITS.acquire();
    let object = store
        .get(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?
        .context("committed WAL chunk missing")?;
    ensure!(
        blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
        "WAL chunk checksum mismatch"
    );
    let record: WalRecord = serde_json::from_slice(&object.bytes)?;
    ensure!(
        record.namespace == *namespace && record.lsn == entry.lsn,
        "WAL record identity mismatch"
    );
    Ok(record)
}

impl RemoteWal {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    fn check_writer(&self) -> Result<()> {
        ensure!(
            self.store.access_role() == crate::ownership::types::AccessRole::Writer,
            "read-only WAL mutation"
        );
        self.store.validate_session()
    }

    fn index(&self, namespace: &Namespace) -> Result<(WalMetadata, Option<String>)> {
        self.store.validate_session()?;
        match self.store.get(&namespace.key("wal/index")?)? {
            Some(object) => {
                let mut index: WalMetadata = serde_json::from_slice(&object.bytes)?;
                index.normalize_entries()?;
                ensure!(
                    index
                        .receipts
                        .values()
                        .all(|lsn| *lsn > 0 && *lsn <= index.last_lsn),
                    "invalid WAL append receipts"
                );
                ensure!(
                    index
                        .receipt_digests
                        .keys()
                        .all(|identity| index.receipts.contains_key(identity)),
                    "orphan WAL receipt digest"
                );
                ensure!(
                    index.receipt_index.as_deref().is_none_or(|root| {
                        root.len() == 64
                            && root
                                .bytes()
                                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    }),
                    "invalid WAL receipt archive"
                );
                ensure!(
                    index
                        .commit
                        .is_none_or(|commit| commit.lsn > 0 && commit.lsn == index.last_lsn),
                    "invalid WAL index commit LSN"
                );
                crate::work_metrics::observe_wal_index_read(
                    object.bytes.len(),
                    index.entries.len(),
                );
                Ok((index, Some(object.etag)))
            }
            None => Ok((
                WalMetadata {
                    entry_count: Some(0),
                    ..WalMetadata::default()
                },
                None,
            )),
        }
    }

    pub fn metadata(&self, namespace: &Namespace) -> Result<WalMetadata> {
        Ok(self.index(namespace)?.0)
    }

    fn chunk(&self, encoded: &EncodedWalRecord) -> Result<String> {
        self.check_writer()?;
        let hash = encoded.hash();
        let key = encoded
            .record()
            .namespace
            .key(&format!("wal/chunks/{hash}"))?;
        if let Err(error) = self.store.put(&key, encoded.bytes(), PutCondition::Absent) {
            let existing = self.store.get(&key)?.context("WAL chunk upload failed")?;
            ensure!(
                existing.bytes.as_slice() == encoded.bytes(),
                "immutable WAL chunk conflict: {error}"
            );
        }
        Ok(hash.to_owned())
    }

    fn index_segment(
        &self,
        namespace: &Namespace,
        entries: Vec<Entry>,
        previous: Option<String>,
    ) -> Result<String> {
        let (hash, bytes) = WalIndexSegment::encode(previous, entries)?;
        let key = namespace.key(&format!("wal/index-segments/{hash}"))?;
        if let Err(error) = self.store.put(&key, &bytes, PutCondition::Absent) {
            let existing = self
                .store
                .get(&key)?
                .context("WAL index segment upload failed")?;
            ensure!(
                existing.bytes == bytes,
                "immutable WAL index segment conflict: {error}"
            );
        }
        Ok(hash)
    }

    /// Moves the oldest digested receipts into the page archive before the head CAS.
    ///
    /// The receipt this append adds stays inline: a roll must move at least one older receipt,
    /// so a legacy head whose inline window is entirely undigested never changes the archive
    /// root without a removal. A legacy receipt itself stays inline because moving it would
    /// leave no proof that a retry is the original request.
    fn roll_receipts(
        &self,
        namespace: &Namespace,
        index: &mut WalMetadata,
        appended: Option<&str>,
    ) -> Result<()> {
        let excess = index
            .receipts
            .len()
            .saturating_sub(WAL_RECEIPT_INLINE_LIMIT);
        if excess == 0 {
            return Ok(());
        }
        let target = excess.saturating_add(WAL_RECEIPT_ROLL_BATCH);
        let mut candidates = index
            .receipts
            .iter()
            .filter(|(identity, _)| {
                appended != Some(identity.as_str()) && index.receipt_digests.contains_key(*identity)
            })
            .map(|(identity, lsn)| (*lsn, identity.clone()))
            .collect::<Vec<_>>();
        candidates.sort();
        candidates.truncate(target.min(candidates.len()));
        if candidates.is_empty() {
            return Ok(());
        }
        let pending =
            std::cell::RefCell::new(crate::authority::paged_index::PendingPages::default());
        let pages = crate::authority::paged_index::PagedIndex::with_prefix(
            self.store.as_ref(),
            namespace,
            WAL_RECEIPT_PAGE_PREFIX,
        )
        .deferring(&pending);
        let mut root = index.receipt_index.clone();
        for batch in candidates.chunks(WAL_RECEIPT_ARCHIVE_BATCH) {
            let mut updates = BTreeMap::new();
            for (lsn, identity) in batch {
                let digest = index
                    .receipt_digests
                    .get(identity)
                    .cloned()
                    .context("missing WAL receipt digest")?;
                updates.insert(
                    identity.clone(),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: *lsn,
                        digest: Some(digest),
                    })?,
                );
            }
            root = Some(pages.insert_many(root.as_deref(), updates)?);
        }
        // One durability group for the whole roll when the store can publish pages in
        // batch; the per-page path below is what every other store keeps doing. Publishing
        // the pages before the head that references them is the ordering that makes a lost
        // head harmless.
        let staged = pending.borrow();
        if !self.store.publish_pages(staged.writes())? {
            for write in staged.writes() {
                if let Err(error) = self
                    .store
                    .put(&write.key, &write.bytes, PutCondition::Absent)
                {
                    let existing = self
                        .store
                        .get(&write.key)?
                        .context("index page upload unresolved")?;
                    ensure!(
                        existing.bytes == write.bytes,
                        "immutable index page conflict: {error}"
                    );
                }
            }
        }
        drop(staged);
        for (_, identity) in &candidates {
            index.receipts.remove(identity);
            index.receipt_digests.remove(identity);
        }
        index.receipt_index = root;
        Ok(())
    }

    /// Rebuilds the bounded head tail after truncate. The prefix is written as an ordered
    /// segment chain before the head CAS: baseline parts first, retained entries after them.
    fn segment_truncated_entries(
        &self,
        namespace: &Namespace,
        index: &mut WalMetadata,
    ) -> Result<()> {
        if index.entries.len() <= WAL_INDEX_INLINE_ENTRIES {
            return Ok(());
        }
        let split = index.entries.len() - WAL_INDEX_INLINE_ENTRIES;
        let tail = index.entries.split_off(split);
        let mut root = None;
        for entries in index.entries.chunks(WAL_INDEX_INLINE_ENTRIES) {
            root = Some(self.index_segment(namespace, entries.to_vec(), root)?);
        }
        index.entries = tail;
        index.entry_index = root;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn resolved_entries(&self, namespace: &Namespace) -> Result<Vec<Entry>> {
        self.resolve_index(namespace, &self.index(namespace)?.0)
    }

    fn resolve_index(&self, namespace: &Namespace, index: &WalMetadata) -> Result<Vec<Entry>> {
        crate::recovery::resolve_entries(index, |hash| {
            let key = namespace.key(&format!("wal/index-segments/{hash}"))?;
            Ok(self
                .store
                .get(&key)?
                .context("WAL index segment missing")?
                .bytes)
        })
    }

    fn commit(
        &self,
        namespace: &Namespace,
        metadata: &WalMetadata,
        etag: Option<String>,
    ) -> Result<()> {
        self.check_writer()?;
        let encoded = serde_json::to_vec(metadata)?;
        crate::work_metrics::observe_wal_index_commit(encoded.len(), metadata.receipts.len());
        self.store.put(
            &namespace.key("wal/index")?,
            &encoded,
            etag.map_or(PutCondition::Absent, PutCondition::Match),
        )?;
        Ok(())
    }

    pub fn fence(&self, namespace: &Namespace, epoch: u64) -> Result<()> {
        self.check_writer()?;
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            ensure!(epoch >= index.epoch, "stale WAL epoch");
            index.epoch = epoch;
            match self.commit(namespace, &index, etag.clone()) {
                Ok(()) => return Ok(()),
                Err(error) if self.index(namespace)?.1 == etag => return Err(error),
                Err(_) => crate::work_metrics::observe_wal_index_retry(),
            }
        }
        bail!("WAL fencing CAS contention")
    }
    pub fn append_commit(
        &self,
        namespace: &Namespace,
        epoch: u64,
        batch: WalBatch,
        position: Option<WalPosition>,
    ) -> Result<u64> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let append_id = format!(
            "{}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let timestamp_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?;
        let chunks = split_batch(batch)?;
        let records = chunks.len();
        let mut lsn = 0;
        for (index, chunk) in chunks.into_iter().enumerate() {
            // One record keeps the caller's identity so its receipt is unchanged. A split batch
            // derives one identity per prefix, deterministically: a retry of the same batch
            // addresses the same records and the authority's receipt check can answer it.
            let append_id = if records == 1 {
                append_id.clone()
            } else {
                format!("{append_id}-{index}")
            };
            let request = WalAppendRequest {
                namespace: namespace.clone(),
                epoch,
                batch: chunk,
                position: (index + 1 == records).then_some(position).flatten(),
                append_id,
                timestamp_millis,
            };
            let timer = crate::slow_diagnostics::Timer::begin();
            let appended = self.append_one(&request);
            if let Some(timer) = timer {
                timer.finish(
                    crate::slow_diagnostics::outcome(&appended),
                    None,
                    |duration| crate::slow_diagnostics::Detail::WalDurability {
                        append_id: crate::slow_diagnostics::append_id(&request.append_id),
                        bytes: crate::slow_diagnostics::wal_bytes(&request.batch),
                        bytes_scope: "wal_write_payload",
                        record_index: index,
                        record_count: records,
                        append_duration_ns: duration,
                        commit_duration_ns: None,
                        commit_position_present: request.position.is_some(),
                        attempts: None,
                        unknown: appended.as_ref().ok().map(|_| false),
                    },
                );
            }
            lsn = appended?;
        }
        Ok(lsn)
    }

    fn append_one(&self, request: &WalAppendRequest) -> Result<u64> {
        if let Some(lsn) = self.store.append_wal(request)? {
            return Ok(lsn);
        }
        self.append_local(request)
    }

    pub(crate) fn append_local(&self, request: &WalAppendRequest) -> Result<u64> {
        self.append_using(request, |encoded, index, etag| {
            self.chunk(encoded)
                .map_err(|error| anyhow::Error::new(ChunkUploadFailure(error)))?;
            self.commit(&encoded.record().namespace, index, etag)
        })
    }

    pub(crate) fn append_using<Publish>(
        &self,
        request: &WalAppendRequest,
        mut publish: Publish,
    ) -> Result<u64>
    where
        Publish: FnMut(&EncodedWalRecord, &WalMetadata, Option<String>) -> Result<()>,
    {
        let WalAppendRequest {
            namespace,
            epoch,
            append_id,
            ..
        } = request;
        self.check_writer()?;
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            ensure!(*epoch >= index.epoch, "stale WAL epoch");
            if let Some(receipt) = self.receipt(namespace, &index, append_id)? {
                return self.verified_receipt(request, &index, receipt);
            }
            let lsn = index.last_lsn.checked_add(1).context("WAL LSN exhausted")?;
            let encoded = EncodedWalRecord::new(request.record(lsn))?;
            let entry = Entry {
                lsn,
                hash: encoded.hash().to_owned(),
            };
            if index.entries.len() >= WAL_INDEX_SEGMENT_LIMIT {
                for entries in std::mem::take(&mut index.entries).chunks(WAL_INDEX_SEGMENT_LIMIT) {
                    index.entry_index = Some(self.index_segment(
                        namespace,
                        entries.to_vec(),
                        index.entry_index.clone(),
                    )?);
                }
            }
            index.entries.push(entry);
            index.entry_count = Some(
                index
                    .entry_count
                    .unwrap_or(0)
                    .checked_add(1)
                    .context("WAL entry count exhausted")?,
            );
            index.receipts.insert(append_id.clone(), lsn);
            index
                .receipt_digests
                .insert(append_id.clone(), encoded.hash().to_owned());
            self.roll_receipts(namespace, &mut index, Some(append_id))?;
            index.last_lsn = lsn;
            index.epoch = *epoch;
            index.commit = encoded.record().commit;
            match publish(&encoded, &index, etag.clone()) {
                Ok(()) => return Ok(lsn),
                Err(error) => {
                    if error.is::<ChunkUploadFailure>() {
                        return Err(error.downcast::<ChunkUploadFailure>().unwrap().0);
                    }
                    let (current, current_etag) = self.index(namespace)?;
                    ensure!(*epoch >= current.epoch, "stale WAL epoch");
                    if let Some(receipt) = self.receipt(namespace, &current, append_id)? {
                        return self.verified_receipt(request, &current, receipt);
                    }
                    if current_etag == etag {
                        return Err(error);
                    }
                    // The conditional write lost to another appender, so this attempt is
                    // rebuilt from the index that won.
                    crate::work_metrics::observe_wal_index_retry();
                    crate::slow_diagnostics::retry();
                }
            }
        }
        bail!("WAL append CAS contention")
    }

    /// Resolves an append identity through the inline window first, then the durable
    /// archive. A miss in the archive is a genuine new append: the root page and the path
    /// to the key are verified by content address, so a corrupt or missing page fails the
    /// append instead of silently treating a duplicate as new.
    fn receipt(
        &self,
        namespace: &Namespace,
        index: &WalMetadata,
        append_id: &str,
    ) -> Result<Option<AppendReceipt>> {
        if let Some(lsn) = index.receipts.get(append_id) {
            return Ok(Some(AppendReceipt {
                lsn: *lsn,
                digest: index.receipt_digests.get(append_id).cloned(),
                archived: false,
            }));
        }
        let Some(root) = index.receipt_index.as_deref() else {
            return Ok(None);
        };
        let pages = crate::authority::paged_index::PagedIndex::with_prefix(
            self.store.as_ref(),
            namespace,
            WAL_RECEIPT_PAGE_PREFIX,
        );
        let Some(value) = pages.get(Some(root), append_id)? else {
            return Ok(None);
        };
        let archived: ArchivedReceipt = serde_json::from_value(value)?;
        ensure!(
            archived.lsn > 0 && archived.lsn <= index.last_lsn,
            "invalid archived WAL receipt"
        );
        Ok(Some(AppendReceipt {
            lsn: archived.lsn,
            digest: archived.digest,
            archived: true,
        }))
    }

    fn verified_receipt(
        &self,
        request: &WalAppendRequest,
        index: &WalMetadata,
        receipt: AppendReceipt,
    ) -> Result<u64> {
        self.check_writer()?;
        let expected = blake3::hash(&serde_json::to_vec(&request.record(receipt.lsn))?);
        if let Some(digest) = &receipt.digest {
            ensure!(
                digest == expected.to_hex().as_str(),
                "WAL receipt request identity or content conflict"
            );
        }
        if receipt.archived || receipt.lsn <= index.replay_baseline_lsn {
            // The original record is folded into the baseline, so the digest is the remaining
            // proof that this request produced that LSN. A legacy receipt without one cannot be
            // proven identical and must fail closed. This also covers a legacy inline receipt
            // that truncate deliberately left in place because it had no digest: an entry with
            // the same LSN now names a baseline part, not the original record.
            ensure!(
                receipt.digest.is_some(),
                "WAL receipt content unavailable; refusing replay"
            );
            self.check_writer()?;
            return Ok(receipt.lsn);
        }
        let resolved;
        let entries = if index.entries.iter().any(|entry| entry.lsn == receipt.lsn) {
            &index.entries
        } else {
            resolved = self.resolve_index(&request.namespace, index)?;
            &resolved
        };
        let entry = entries
            .iter()
            .find(|entry| entry.lsn == receipt.lsn)
            .context("WAL receipt content unavailable; refusing replay")?;
        let object = self
            .store
            .get(
                &request
                    .namespace
                    .key(&format!("wal/chunks/{}", entry.hash))?,
            )?
            .context("WAL receipt chunk unavailable; refusing replay")?;
        ensure!(
            blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
            "WAL receipt checksum mismatch"
        );
        let record: WalRecord = serde_json::from_slice(&object.bytes)?;
        let persisted = blake3::hash(&serde_json::to_vec(&record)?);
        ensure!(
            persisted == expected,
            "WAL receipt request identity or content conflict"
        );
        self.check_writer()?;
        Ok(receipt.lsn)
    }

    pub fn for_each_record(
        &self,
        namespace: &Namespace,
        from_lsn: u64,
        visit: impl FnMut(WalRecord) -> Result<()>,
    ) -> Result<()> {
        self.for_each_record_with_window(namespace, from_lsn, wal_replay_window()?, visit)
    }

    /// Same contract as [`Self::for_each_record`], with the fetch window given rather than
    /// derived from the environment.
    ///
    /// chunk 按索引顺序交付，错误也按最低 LSN 返回；后续 chunk 即使先返回，
    /// 也不会提前交付或报错。分段索引按顺序物化为 entries，因此索引内存随历史增长。
    /// chunk 数据最多保留 `window` 个预取结果及当前交付的 record，各占用
    /// [`WAL_CHUNK_CHARGE`] 预算。已交付的 chunk 不保留，失败后不再补充预取。
    fn for_each_record_with_window(
        &self,
        namespace: &Namespace,
        from_lsn: u64,
        window: usize,
        mut visit: impl FnMut(WalRecord) -> Result<()>,
    ) -> Result<()> {
        ensure!(window > 0, "invalid WAL replay window");
        let index = self.index(namespace)?.0;
        ensure!(
            from_lsn == 0 || from_lsn >= index.replay_baseline_lsn,
            "requested WAL history was truncated; replay from baseline"
        );
        let entries = self.resolve_index(namespace, &index)?;
        let mut pending = entries.iter().filter(|entry| entry.lsn >= from_lsn);
        let store: &dyn ObjectStore = &*self.store;
        std::thread::scope(|scope| -> Result<()> {
            let mut in_flight: VecDeque<std::thread::ScopedJoinHandle<'_, Result<WalRecord>>> =
                VecDeque::with_capacity(window);
            while in_flight.len() < window {
                let Some(entry) = pending.next() else {
                    break;
                };
                in_flight
                    .push_back(scope.spawn(move || load_chunk_record(store, namespace, entry)));
            }
            while let Some(handle) = in_flight.pop_front() {
                let record = match handle.join() {
                    Ok(record) => record?,
                    Err(_) => bail!("WAL chunk prefetch worker panicked"),
                };
                // Refill once the chunk was accepted, before the visitor sees it, so the
                // window stays full while the visitor applies it. Threads still in flight are
                // joined by the scope, including on the error and `visit` failure paths.
                if let Some(entry) = pending.next() {
                    in_flight
                        .push_back(scope.spawn(move || load_chunk_record(store, namespace, entry)));
                }
                visit(record)?;
            }
            Ok(())
        })
    }
}

impl DurableWal for RemoteWal {
    fn append(&self, namespace: &Namespace, epoch: u64, batch: WalBatch) -> Result<u64> {
        self.append_commit(namespace, epoch, batch, None)
    }

    fn read(&self, namespace: &Namespace, from_lsn: u64) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        self.for_each_record(namespace, from_lsn, |record| {
            records.push(record);
            Ok(())
        })?;
        Ok(records)
    }

    fn truncate(&self, namespace: &Namespace, before_lsn: u64) -> Result<()> {
        self.truncate_with_part_bytes(namespace, before_lsn, WAL_BASELINE_PART_BYTES)
    }
}

impl RemoteWal {
    /// Truncates history below `before_lsn` into a replay baseline.
    ///
    /// `part_bytes` is injectable so a test can force several parts with a small image. Every
    /// part is written before the index CAS; a failed or losing attempt leaves only immutable,
    /// unreferenced chunks, which the next attempt reuses by content address.
    fn truncate_with_part_bytes(
        &self,
        namespace: &Namespace,
        before_lsn: u64,
        part_bytes: usize,
    ) -> Result<()> {
        ensure!(part_bytes > 0, "invalid WAL baseline part size");
        self.check_writer()?;
        for _ in 0..64 {
            let (mut index, etag) = self.index(namespace)?;
            index.entries = self.resolve_index(namespace, &index)?;
            index.entry_index = None;
            let Some(baseline_lsn) = index
                .entries
                .iter()
                .filter(|entry| entry.lsn < before_lsn)
                .map(|entry| entry.lsn)
                .max()
            else {
                return Ok(());
            };
            let mut image = Vec::new();
            let mut timestamp_millis = None;
            let mut baseline_commit = None;
            for entry in index
                .entries
                .iter()
                .filter(|entry| entry.lsn <= baseline_lsn)
            {
                let object = self
                    .store
                    .get(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?
                    .context("missing baseline record")?;
                ensure!(
                    blake3::hash(&object.bytes).to_hex().as_str() == entry.hash,
                    "WAL baseline chunk checksum mismatch"
                );
                let record: WalRecord = serde_json::from_slice(&object.bytes)?;
                ensure!(
                    record.namespace == *namespace
                        && record.lsn == entry.lsn
                        && record.epoch <= index.epoch,
                    "WAL baseline record identity mismatch"
                );
                if entry.lsn == baseline_lsn {
                    timestamp_millis = Some(record.timestamp_millis);
                    baseline_commit = record.commit;
                }
                apply_batch(&mut image, &record.batch)?;
            }
            let timestamp_millis = timestamp_millis.context("missing baseline record")?;
            let parts = image.len().div_ceil(part_bytes).max(1);
            let mut segments = Vec::with_capacity(parts);
            for part in 0..parts {
                let start = part * part_bytes;
                let end = (start + part_bytes).min(image.len());
                let mut operations = Vec::with_capacity(2);
                if part == 0 {
                    operations.push(WalOperation::Truncate { size: 0 });
                }
                operations.push(WalOperation::Write {
                    offset: start as u64,
                    bytes: image[start..end].to_vec(),
                });
                let record = WalRecord {
                    append_id: format!("baseline-{baseline_lsn}-{part}"),
                    namespace: namespace.clone(),
                    epoch: index.epoch,
                    lsn: baseline_lsn,
                    timestamp_millis,
                    batch: WalBatch { operations },
                    // Only the final part is the end of this logical record, so only it may
                    // carry the commit marker the baseline record had.
                    commit: if part + 1 == parts {
                        baseline_commit
                    } else {
                        None
                    },
                };
                let encoded = EncodedWalRecord::new(record)?;
                ensure!(
                    encoded.bytes().len() <= crate::authority_rpc::protocol::MAX_RPC_BYTES,
                    "WAL baseline part exceeds the RPC limit"
                );
                let hash = self.chunk(&encoded)?;
                segments.push(Entry {
                    lsn: baseline_lsn,
                    hash,
                });
            }
            // Receipts for records folded into the baseline leave the inline map here.
            // Pages are content addressed and uploaded before the head CAS, so a published
            // index can never point at an archive that was not durable first.
            let archived: Vec<(String, u64)> = index
                .receipts
                .iter()
                .filter(|(identity, lsn)| {
                    **lsn < before_lsn && index.receipt_digests.contains_key(*identity)
                })
                .map(|(identity, lsn)| (identity.clone(), *lsn))
                .collect();
            if !archived.is_empty() {
                let pages = crate::authority::paged_index::PagedIndex::with_prefix(
                    self.store.as_ref(),
                    namespace,
                    WAL_RECEIPT_PAGE_PREFIX,
                );
                let mut root = index.receipt_index.clone();
                for batch in archived.chunks(WAL_RECEIPT_ARCHIVE_BATCH) {
                    let mut updates = BTreeMap::new();
                    for (identity, lsn) in batch {
                        updates.insert(
                            identity.clone(),
                            serde_json::to_value(ArchivedReceipt {
                                lsn: *lsn,
                                digest: index.receipt_digests.get(identity).cloned(),
                            })?,
                        );
                    }
                    root = Some(pages.insert_many(root.as_deref(), updates)?);
                }
                for (identity, _) in &archived {
                    index.receipts.remove(identity);
                    index.receipt_digests.remove(identity);
                }
                index.receipt_index = root;
            }
            self.roll_receipts(namespace, &mut index, None)?;
            index.entries.retain(|entry| entry.lsn >= before_lsn);
            index.entries.splice(0..0, segments);
            index.entry_count = Some(index.entries.len() as u64);
            index.entry_index = None;
            self.segment_truncated_entries(namespace, &mut index)?;
            index.replay_baseline_lsn = baseline_lsn;
            match self.commit(namespace, &index, etag.clone()) {
                Ok(()) => return Ok(()),
                Err(error) if self.index(namespace)?.1 == etag => return Err(error),
                Err(_) => {}
            }
        }
        bail!("WAL truncate CAS contention")
    }
}

pub(crate) fn apply_batch(target: &mut Vec<u8>, batch: &WalBatch) -> Result<()> {
    for operation in &batch.operations {
        match operation {
            WalOperation::Write { offset, bytes } => {
                let offset = usize::try_from(*offset)?;
                let end = offset
                    .checked_add(bytes.len())
                    .context("WAL write overflow")?;
                if target.len() < end {
                    target.try_reserve(end - target.len())?;
                    target.resize(end, 0);
                }
                target[offset..end].copy_from_slice(bytes);
            }
            WalOperation::Truncate { size } => {
                let size = usize::try_from(*size)?;
                if size > target.len() {
                    target.try_reserve(size - target.len())?;
                }
                target.resize(size, 0);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod receipt_tests {
    use super::*;
    use crate::object_store::LocalObjectStore;

    fn request() -> Result<WalAppendRequest> {
        Ok(WalAppendRequest {
            namespace: Namespace::new("receipt", "binding")?,
            epoch: 10,
            batch: WalBatch::default(),
            position: None,
            append_id: "controlled-id".into(),
            timestamp_millis: 1,
        })
    }

    fn change(request: &mut WalAppendRequest, field: &str) {
        match field {
            "payload" => request.batch.operations.push(WalOperation::Write {
                offset: 7,
                bytes: vec![42],
            }),
            "position" => request.position = Some(WalPosition::default()),
            "epoch" => request.epoch = 11,
            "timestamp" => request.timestamp_millis = 2,
            "same" => {}
            _ => unreachable!(),
        }
    }

    #[test]
    fn receipt_initial_and_publication_error_paths_bind_full_content() -> Result<()> {
        for legacy in [false, true] {
            for field in ["same", "payload", "position", "epoch", "timestamp"] {
                let directory = tempfile::tempdir()?;
                let store = Arc::new(LocalObjectStore::open(directory.path())?);
                let wal = RemoteWal::new(store.clone());
                let original = request()?;
                assert_eq!(wal.append_local(&original)?, 1);
                if legacy {
                    let (mut index, etag) = wal.index(&original.namespace)?;
                    index.receipt_digests.clear();
                    wal.commit(&original.namespace, &index, etag)?;
                }
                let mut changed = original.clone();
                change(&mut changed, field);
                let before = store.get(&original.namespace.key("wal/index")?)?.unwrap();
                let result = wal.append_local(&changed);
                assert_eq!(
                    result.is_ok(),
                    field == "same",
                    "initial legacy={legacy} field={field}"
                );
                assert_eq!(
                    store
                        .get(&original.namespace.key("wal/index")?)?
                        .unwrap()
                        .bytes,
                    before.bytes
                );
                let mut stale = original.clone();
                stale.epoch = 9;
                assert!(wal
                    .append_local(&stale)
                    .unwrap_err()
                    .to_string()
                    .contains("stale WAL epoch"));
            }
            for field in ["same", "payload", "position", "epoch", "timestamp"] {
                let directory = tempfile::tempdir()?;
                let store = Arc::new(LocalObjectStore::open(directory.path())?);
                let wal = RemoteWal::new(store);
                let original = request()?;
                let mut competing = original.clone();
                change(&mut competing, field);
                let result = wal.append_using(&original, |_, _, _| {
                    wal.append_local(&competing)?;
                    if legacy {
                        let (mut index, etag) = wal.index(&original.namespace)?;
                        index.receipt_digests.clear();
                        wal.commit(&original.namespace, &index, etag)?;
                    }
                    bail!("publication response lost after physical terminal")
                });
                assert_eq!(
                    result.is_ok(),
                    field == "same",
                    "readback legacy={legacy} field={field}"
                );
                assert_eq!(wal.index(&original.namespace)?.0.last_lsn, 1);
            }
        }
        Ok(())
    }

    #[test]
    fn legacy_receipt_without_original_chunk_refuses_replay() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let request = request()?;
        wal.append_local(&request)?;
        let (mut index, etag) = wal.index(&request.namespace)?;
        index.receipt_digests.clear();
        wal.commit(&request.namespace, &index, etag)?;
        store.delete(
            &request
                .namespace
                .key(&format!("wal/chunks/{}", index.entries[0].hash))?,
            None,
        )?;
        assert!(wal
            .append_local(&request)
            .unwrap_err()
            .to_string()
            .contains("unavailable"));
        Ok(())
    }
}

#[cfg(test)]
mod split_tests {
    use super::*;
    use crate::object_store::{LocalObjectStore, PutCondition, StoredObject};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn namespace() -> Result<Namespace> {
        Namespace::new("split", "batch")
    }

    fn write_bytes(operation: &WalOperation) -> usize {
        match operation {
            WalOperation::Write { bytes, .. } => bytes.len(),
            WalOperation::Truncate { .. } => 0,
        }
    }

    fn replay(wal: &RemoteWal, namespace: &Namespace) -> Result<Vec<u8>> {
        let mut image = Vec::new();
        wal.for_each_record(namespace, 0, |record| {
            apply_batch(&mut image, &record.batch)
        })?;
        Ok(image)
    }

    #[test]
    fn oversized_write_is_split_and_only_the_last_record_commits() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let large = vec![7u8; MAX_APPEND_PAYLOAD_BYTES + 3];
        let batch = WalBatch {
            operations: vec![
                WalOperation::Write {
                    offset: 0,
                    bytes: large.clone(),
                },
                // A second operation after the split proves the chunks keep the batch order
                // and that the commit marker describes the final record, not the split point.
                WalOperation::Write {
                    offset: large.len() as u64,
                    bytes: vec![9u8; 5],
                },
            ],
        };
        let mut expected = Vec::new();
        apply_batch(&mut expected, &batch)?;
        let position = WalPosition {
            salts: [3, 4],
            frame_count: 11,
        };
        assert_eq!(wal.append_commit(&namespace, 1, batch, Some(position))?, 2);

        let (index, _) = wal.index(&namespace)?;
        assert_eq!(index.entries.len(), 2);
        assert_eq!(index.last_lsn, 2);
        assert_eq!(index.commit, Some(WalCommit { lsn: 2, position }));
        assert_eq!(index.receipts.len(), 2);

        let mut seen = Vec::new();
        wal.for_each_record(&namespace, 0, |record| {
            let raw: usize = record.batch.operations.iter().map(write_bytes).sum();
            assert!(raw <= MAX_APPEND_PAYLOAD_BYTES);
            assert!(record.batch.operations.len() <= MAX_APPEND_OPERATIONS);
            seen.push((record.lsn, record.commit));
            Ok(())
        })?;
        assert_eq!(
            seen,
            vec![(1, None), (2, Some(WalCommit { lsn: 2, position }))]
        );
        for entry in &index.entries {
            let object = store
                .get(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?
                .context("split chunk missing")?;
            assert!(object.bytes.len() <= crate::authority_rpc::protocol::MAX_RPC_BYTES);
        }
        assert_eq!(replay(&wal, &namespace)?, expected);
        Ok(())
    }

    #[test]
    fn recovery_accepts_a_split_record_chain_ending_in_a_commit() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let frame_size = crate::recovery::fixture::FRAME_SIZE;
        // One frame past the payload ceiling forces the split; the last frame is the commit.
        let frames = MAX_APPEND_PAYLOAD_BYTES / frame_size + 2;
        let mut builder = crate::recovery::fixture::WalBuilder::new([9, 10]);
        let image = builder.image(frames);
        assert!(image.len() > MAX_APPEND_PAYLOAD_BYTES);
        let position = WalPosition {
            salts: [9, 10],
            frame_count: frames as u64,
        };
        let batch = WalBatch {
            operations: vec![WalOperation::Write {
                offset: 0,
                bytes: image.clone(),
            }],
        };
        assert_eq!(wal.append_commit(&namespace, 1, batch, Some(position))?, 2);
        assert_eq!(replay(&wal, &namespace)?, image);
        let index_object = store
            .get(&namespace.key("wal/index")?)?
            .context("split index missing")?;
        let validated = crate::recovery::validate_index_state(
            &index_object.bytes,
            &namespace,
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                    .context("split segment missing")?
                    .bytes)
            },
            |key| Ok(store.get(key)?.context("split chunk missing")?.bytes),
            |_| bail!("unexpected receipt page in split fixture"),
        )?;
        assert_eq!(validated.commit(), Some(WalCommit { lsn: 2, position }));
        Ok(())
    }

    #[test]
    fn operation_limit_is_split_without_reordering() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store);
        let namespace = namespace()?;
        let operations = (0..=MAX_APPEND_OPERATIONS)
            .map(|index| WalOperation::Write {
                offset: index as u64,
                bytes: vec![index as u8],
            })
            .collect::<Vec<_>>();
        let batch = WalBatch { operations };
        let mut expected = Vec::new();
        apply_batch(&mut expected, &batch)?;
        assert_eq!(wal.append_commit(&namespace, 1, batch, None)?, 2);
        let mut counts = Vec::new();
        wal.for_each_record(&namespace, 0, |record| {
            counts.push(record.batch.operations.len());
            Ok(())
        })?;
        assert_eq!(counts, vec![MAX_APPEND_OPERATIONS, 1]);
        assert_eq!(replay(&wal, &namespace)?, expected);
        Ok(())
    }

    #[test]
    fn authority_limits_reject_an_unsplit_record() {
        let batch = WalBatch {
            operations: vec![WalOperation::Write {
                offset: 0,
                bytes: vec![0u8; MAX_APPEND_PAYLOAD_BYTES + 1],
            }],
        };
        let request = WalAppendRequest {
            namespace: Namespace::new("split", "limits").unwrap(),
            epoch: 1,
            batch,
            position: None,
            append_id: "limit-check".into(),
            timestamp_millis: 1,
        };
        assert!(request
            .validate()
            .unwrap_err()
            .to_string()
            .contains("WAL payload limit exceeded"));
        let overflow = WalBatch {
            operations: (0..=MAX_APPEND_OPERATIONS)
                .map(|offset| WalOperation::Write {
                    offset: offset as u64,
                    bytes: Vec::new(),
                })
                .collect(),
        };
        let request = WalAppendRequest {
            batch: overflow,
            ..request
        };
        assert!(request
            .validate()
            .unwrap_err()
            .to_string()
            .contains("WAL operation limit exceeded"));
    }

    /// Fails one chunk upload by body, leaving any earlier chunks published. The append must
    /// stop there: no retry, no commit marker, and a prefix that replay can still read.
    struct FailChunkUpload {
        inner: LocalObjectStore,
        chunks: AtomicUsize,
        fail_at: usize,
    }

    impl ObjectStore for FailChunkUpload {
        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
            if key.contains("/wal/chunks/")
                && self.chunks.fetch_add(1, Ordering::SeqCst) == self.fail_at
            {
                bail!("injected chunk upload failure");
            }
            self.inner.put(key, bytes, condition)
        }

        fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
            self.inner.delete(key, expected)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }
    }

    #[test]
    fn interrupted_split_keeps_the_prefix_and_no_commit() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(FailChunkUpload {
            inner: LocalObjectStore::open(directory.path())?,
            chunks: AtomicUsize::new(0),
            fail_at: 1,
        });
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let batch = WalBatch {
            operations: vec![WalOperation::Write {
                offset: 0,
                bytes: vec![7u8; MAX_APPEND_PAYLOAD_BYTES + 10],
            }],
        };
        let position = WalPosition {
            salts: [1, 2],
            frame_count: 3,
        };
        assert!(wal
            .append_commit(&namespace, 1, batch, Some(position))
            .unwrap_err()
            .to_string()
            .contains("chunk"));
        assert_eq!(store.chunks.load(Ordering::SeqCst), 2);
        let (index, _) = wal.index(&namespace)?;
        assert_eq!(index.last_lsn, 1);
        assert_eq!(index.entries.len(), 1);
        assert_eq!(index.commit, None);
        let mut image = Vec::new();
        wal.for_each_record(&namespace, 0, |record| {
            assert_eq!(record.commit, None);
            apply_batch(&mut image, &record.batch)
        })?;
        assert_eq!(image.len(), MAX_APPEND_PAYLOAD_BYTES);
        assert!(image.iter().all(|byte| *byte == 7));
        let index_object = store
            .get(&namespace.key("wal/index")?)?
            .context("split index missing")?;
        let validated = crate::recovery::validate_index_state(
            &index_object.bytes,
            &namespace,
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                    .context("split segment missing")?
                    .bytes)
            },
            |key| Ok(store.get(key)?.context("split chunk missing")?.bytes),
            |_| bail!("unexpected receipt page in split fixture"),
        )?;
        assert_eq!(validated.commit(), None);
        Ok(())
    }
}

#[cfg(test)]
mod truncate_tests {
    use super::*;
    use crate::authority::paged_index::PagedIndex;
    use crate::object_store::{LocalObjectStore, PutCondition};
    use crate::recovery::fixture::{WalBuilder, FRAME_SIZE, PAGE_SIZE};
    use std::collections::BTreeSet;

    fn namespace() -> Result<Namespace> {
        Namespace::new("truncate", "split")
    }

    fn position(frame_count: u64) -> Option<WalPosition> {
        Some(WalPosition {
            salts: [3, 4],
            frame_count,
        })
    }

    fn append(wal: &RemoteWal, namespace: &Namespace, offset: u64, len: usize) -> Result<u64> {
        wal.append_commit(
            namespace,
            1,
            WalBatch {
                operations: vec![WalOperation::Write {
                    offset,
                    bytes: vec![7u8; len],
                }],
            },
            position(len as u64),
        )
    }

    fn index_bytes(store: &LocalObjectStore, namespace: &Namespace) -> Result<Vec<u8>> {
        Ok(store
            .get(&namespace.key("wal/index")?)?
            .context("missing WAL index")?
            .bytes)
    }

    fn validated_commit(
        store: &LocalObjectStore,
        namespace: &Namespace,
    ) -> Result<Option<WalCommit>> {
        let bytes = index_bytes(store, namespace)?;
        Ok(crate::recovery::validate_index_state(
            &bytes,
            namespace,
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                    .context("missing WAL segment")?
                    .bytes)
            },
            |key| Ok(store.get(key)?.context("missing WAL chunk")?.bytes),
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/receipt-pages/{hash}"))?)?
                    .context("missing WAL receipt page")?
                    .bytes)
            },
        )?
        .commit())
    }

    #[test]
    fn baseline_is_split_into_parts_that_replay_as_one_image() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let mut builder = WalBuilder::new([3, 4]);
        let header = builder.header();
        let mut image = Vec::new();
        let mut records = Vec::new();
        let mut page = 0u32;
        for (index, frames) in [3usize, 3, 3, 1].into_iter().enumerate() {
            let mut bytes = if index == 0 {
                header.clone()
            } else {
                Vec::new()
            };
            for frame in 0..frames {
                page += 1;
                let commit = if frame + 1 == frames {
                    PAGE_SIZE as u32
                } else {
                    0
                };
                bytes.extend_from_slice(&builder.frame(page, commit));
            }
            records.push((image.len() as u64, bytes.clone(), page as u64));
            image.extend_from_slice(&bytes);
        }
        for (offset, bytes, frame_count) in &records {
            wal.append_commit(
                &namespace,
                1,
                WalBatch {
                    operations: vec![WalOperation::Write {
                        offset: *offset,
                        bytes: bytes.clone(),
                    }],
                },
                position(*frame_count),
            )?;
        }
        assert_eq!(wal.index(&namespace)?.0.last_lsn, 4);

        wal.truncate_with_part_bytes(&namespace, 4, 2 * FRAME_SIZE)?;

        let (index, _) = wal.index(&namespace)?;
        assert_eq!(index.last_lsn, 4);
        assert_eq!(index.replay_baseline_lsn, 3);
        let baseline_parts = index
            .entries
            .iter()
            .take_while(|entry| entry.lsn == 3)
            .count();
        let baseline_bytes = builder.header().len() + 9 * FRAME_SIZE;
        assert_eq!(
            baseline_parts,
            baseline_bytes.div_ceil(2 * FRAME_SIZE),
            "{baseline_bytes} baseline bytes at two frames per part"
        );
        assert!(index.entries[baseline_parts..]
            .iter()
            .all(|entry| entry.lsn >= 4));
        assert_eq!(
            index.commit,
            Some(WalCommit {
                lsn: 4,
                position: position(10).unwrap()
            })
        );

        let validated = validated_commit(&store, &namespace)?;
        assert_eq!(validated, index.commit);
        let mut replayed = Vec::new();
        wal.for_each_record(&namespace, 0, |record| {
            apply_batch(&mut replayed, &record.batch)
        })?;
        assert_eq!(replayed, image);
        for entry in &index.entries {
            let object = store
                .get(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?
                .context("missing WAL chunk")?;
            assert!(object.bytes.len() <= crate::authority_rpc::protocol::MAX_RPC_BYTES);
        }
        Ok(())
    }

    #[test]
    fn baseline_that_removes_every_record_keeps_the_final_commit() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let mut builder = WalBuilder::new([3, 4]);
        let header = builder.header();
        let mut image = Vec::new();
        let mut records = Vec::new();
        let mut page = 0u32;
        for (index, frames) in [2usize, 2].into_iter().enumerate() {
            let mut bytes = if index == 0 {
                header.clone()
            } else {
                Vec::new()
            };
            for frame in 0..frames {
                page += 1;
                let commit = if frame + 1 == frames {
                    PAGE_SIZE as u32
                } else {
                    0
                };
                bytes.extend_from_slice(&builder.frame(page, commit));
            }
            records.push((image.len() as u64, bytes.clone(), page as u64));
            image.extend_from_slice(&bytes);
        }
        for (offset, bytes, frame_count) in &records {
            wal.append_commit(
                &namespace,
                1,
                WalBatch {
                    operations: vec![WalOperation::Write {
                        offset: *offset,
                        bytes: bytes.clone(),
                    }],
                },
                position(*frame_count),
            )?;
        }

        wal.truncate_with_part_bytes(&namespace, 3, FRAME_SIZE)?;

        let (index, _) = wal.index(&namespace)?;
        assert_eq!(index.last_lsn, 2);
        assert_eq!(index.replay_baseline_lsn, 2);
        assert!(index.entries.iter().all(|entry| entry.lsn == 2));
        assert_eq!(
            index.entries.len(),
            image.len().div_ceil(FRAME_SIZE),
            "{} bytes at one frame per part",
            image.len()
        );
        assert_eq!(
            index.commit,
            Some(WalCommit {
                lsn: 2,
                position: position(4).unwrap()
            })
        );

        let validated = validated_commit(&store, &namespace)?;
        assert_eq!(validated, index.commit);
        let mut replayed = Vec::new();
        wal.for_each_record(&namespace, 0, |record| {
            apply_batch(&mut replayed, &record.batch)
        })?;
        assert_eq!(replayed, image);
        Ok(())
    }

    #[test]
    fn baseline_continuation_may_not_reset_the_replay_image() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        for part in 0..3 {
            append(&wal, &namespace, part * 300, 300)?;
        }
        wal.truncate_with_part_bytes(&namespace, 3, 256)?;
        let (mut index, etag) = wal.index(&namespace)?;
        assert!(index.entries.len() > 1 && index.entries[0].lsn == index.entries[1].lsn);
        let forged = WalRecord {
            append_id: "forged-baseline".into(),
            namespace: namespace.clone(),
            epoch: index.epoch,
            lsn: index.replay_baseline_lsn,
            timestamp_millis: 1,
            batch: WalBatch {
                operations: vec![WalOperation::Truncate { size: 0 }],
            },
            commit: None,
        };
        let hash = wal.chunk(&EncodedWalRecord::new(forged)?)?;
        index.entries[1].hash = hash;
        wal.commit(&namespace, &index, etag)?;

        let bytes = index_bytes(&store, &namespace)?;
        let failure = match crate::recovery::validate_index_state(
            &bytes,
            &namespace,
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                    .context("missing WAL segment")?
                    .bytes)
            },
            |key| Ok(store.get(key)?.context("missing WAL chunk")?.bytes),
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/receipt-pages/{hash}"))?)?
                    .context("missing WAL receipt page")?
                    .bytes)
            },
        ) {
            Ok(_) => bail!("forged baseline continuation was accepted"),
            Err(error) => error.to_string(),
        };
        assert!(failure.contains("continuation must not reset"), "{failure}");
        Ok(())
    }

    #[test]
    fn duplicate_lsn_outside_the_baseline_is_rejected() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        append(&wal, &namespace, 0, 64)?;
        append(&wal, &namespace, 64, 64)?;
        let (mut index, etag) = wal.index(&namespace)?;
        let last = index.entries.last().unwrap().clone();
        index.entries.push(last);
        index.entry_count = Some(index.entries.len() as u64);
        wal.commit(&namespace, &index, etag)?;
        assert!(wal
            .index(&namespace)
            .unwrap_err()
            .to_string()
            .contains("unordered"));
        Ok(())
    }

    fn request(namespace: &Namespace, append_id: &str, bytes: &[u8]) -> WalAppendRequest {
        WalAppendRequest {
            namespace: namespace.clone(),
            epoch: 1,
            batch: WalBatch {
                operations: vec![WalOperation::Write {
                    offset: 0,
                    bytes: bytes.to_vec(),
                }],
            },
            position: None,
            append_id: append_id.into(),
            timestamp_millis: 7,
        }
    }

    fn load_page<'a>(
        store: &'a LocalObjectStore,
        namespace: &'a Namespace,
    ) -> impl FnMut(&str) -> Result<Vec<u8>> + 'a {
        move |hash| {
            Ok(store
                .get(&namespace.key(&format!("wal/receipt-pages/{hash}"))?)?
                .context("missing WAL receipt page")?
                .bytes)
        }
    }

    fn load_segment(
        store: &LocalObjectStore,
        namespace: &Namespace,
        hash: &str,
    ) -> Result<Vec<u8>> {
        Ok(store
            .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
            .context("missing WAL index segment")?
            .bytes)
    }

    fn load_chunk(store: &LocalObjectStore, key: &str) -> Result<Vec<u8>> {
        Ok(store.get(key)?.context("missing WAL chunk")?.bytes)
    }

    fn load_validated(
        store: &LocalObjectStore,
        namespace: &Namespace,
        index: &[u8],
    ) -> Result<crate::recovery::ValidatedWal> {
        crate::recovery::validate_index_state(
            index,
            namespace,
            |hash| load_segment(store, namespace, hash),
            |key| load_chunk(store, key),
            load_page(store, namespace),
        )
    }

    fn validate_cold(
        store: &LocalObjectStore,
        namespace: &Namespace,
        index: &[u8],
    ) -> Result<Option<WalCommit>> {
        Ok(load_validated(store, namespace, index)?.commit())
    }

    fn validate_append(
        store: &LocalObjectStore,
        namespace: &Namespace,
        before: &[u8],
        candidate: &[u8],
    ) -> Result<crate::recovery::ValidatedWal> {
        let previous = load_validated(store, namespace, before)?;
        crate::recovery::validate_index_append(
            previous,
            candidate,
            namespace,
            |hash| load_segment(store, namespace, hash),
            |key| load_chunk(store, key),
            load_page(store, namespace),
        )
    }

    fn validate_incremental(
        store: &LocalObjectStore,
        namespace: &Namespace,
        before: &[u8],
        candidate: &[u8],
    ) -> Result<Option<WalCommit>> {
        Ok(validate_append(store, namespace, before, candidate)?.commit())
    }

    fn store_record_chunk(
        store: &LocalObjectStore,
        namespace: &Namespace,
        append_id: &str,
        lsn: u64,
    ) -> Result<String> {
        let encoded = EncodedWalRecord::new(WalRecord {
            append_id: append_id.into(),
            namespace: namespace.clone(),
            epoch: 1,
            lsn,
            timestamp_millis: 1,
            batch: WalBatch::default(),
            commit: None,
        })?;
        store.put(
            &namespace.key(&format!("wal/chunks/{}", encoded.hash()))?,
            encoded.bytes(),
            PutCondition::Absent,
        )?;
        Ok(encoded.hash().to_owned())
    }

    fn append_request(namespace: &Namespace, append_id: &str, bytes: &[u8]) -> WalAppendRequest {
        WalAppendRequest {
            namespace: namespace.clone(),
            epoch: 1,
            batch: WalBatch {
                operations: if bytes.is_empty() {
                    Vec::new()
                } else {
                    vec![WalOperation::Write {
                        offset: 0,
                        bytes: bytes.to_vec(),
                    }]
                },
            },
            position: None,
            append_id: append_id.into(),
            timestamp_millis: 1,
        }
    }

    #[test]
    fn validated_wal_keeps_archive_state_constant_sized() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let namespace = namespace()?;
        let baseline = EncodedWalRecord::new(WalRecord {
            append_id: "baseline-1".into(),
            namespace: namespace.clone(),
            epoch: 1,
            lsn: 1,
            timestamp_millis: 1,
            batch: WalBatch {
                operations: vec![WalOperation::Truncate { size: 0 }],
            },
            commit: None,
        })?;
        store.put(
            &namespace.key(&format!("wal/chunks/{}", baseline.hash()))?,
            baseline.bytes(),
            PutCondition::Absent,
        )?;
        let pages = PagedIndex::with_prefix(store.as_ref(), &namespace, "wal/receipt-pages");
        let archived = (0..1_200)
            .map(|ordinal| {
                (
                    format!("archived-{ordinal:04}"),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: 1,
                        digest: Some("a".repeat(64)),
                    })
                    .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let index = WalMetadata {
            epoch: 1,
            last_lsn: 1,
            replay_baseline_lsn: 1,
            entries: vec![Entry {
                lsn: 1,
                hash: baseline.hash().to_owned(),
            }],
            receipts: BTreeMap::new(),
            receipt_digests: BTreeMap::new(),
            receipt_index: Some(pages.insert_many(None, archived)?),
            entry_count: Some(1),
            entry_index: None,
            commit: None,
        };
        let validated = load_validated(&store, &namespace, &serde_json::to_vec(&index)?)?;
        assert_eq!(validated.archive_entries(), 1_200);
        assert_eq!(validated.archive_root(), index.receipt_index.as_deref());
        Ok(())
    }

    #[test]
    fn rolling_append_must_preserve_the_validated_archive() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        assert_eq!(
            wal.append_local(&append_request(&namespace, "archived-old", &[]))?,
            1
        );
        assert_eq!(
            wal.append_local(&append_request(&namespace, "inline-old", &[]))?,
            2
        );
        wal.truncate_with_part_bytes(&namespace, 2, FRAME_SIZE)?;
        let before = index_bytes(&store, &namespace)?;
        assert_eq!(
            load_validated(&store, &namespace, &before)?.archive_entries(),
            1,
            "truncate cold load must recount the archive"
        );
        let (old, _) = wal.index(&namespace)?;
        let old_root = old
            .receipt_index
            .clone()
            .context("missing WAL receipt archive")?;
        let old_root_key = namespace.key(&format!("wal/receipt-pages/{old_root}"))?;
        let old_root_page = store
            .get(&old_root_key)?
            .context("missing WAL receipt archive root page")?
            .bytes;
        let pages = PagedIndex::with_prefix(store.as_ref(), &namespace, "wal/receipt-pages");
        let old_entries = pages.entries(Some(&old_root))?;
        let old_receipt: ArchivedReceipt =
            serde_json::from_value(old_entries["archived-old"].clone())?;
        let inline_lsn = old.receipts["inline-old"];
        let inline_digest = old.receipt_digests["inline-old"].clone();
        let new_hash = store_record_chunk(&store, &namespace, "new-append", 3)?;
        let duplicate_hash = store_record_chunk(&store, &namespace, "archived-old", 3)?;

        let append_candidate = |identity: &str, hash: &str| -> WalMetadata {
            let mut index = old.clone();
            index.entries.push(Entry {
                lsn: 3,
                hash: hash.to_owned(),
            });
            index.entry_count = Some(index.entry_count.unwrap() + 1);
            index.last_lsn = 3;
            index.commit = None;
            index.receipts.insert(identity.to_owned(), 3);
            index
                .receipt_digests
                .insert(identity.to_owned(), hash.to_owned());
            index
        };
        let moved = ArchivedReceipt {
            lsn: inline_lsn,
            digest: Some(inline_digest),
        };
        let moved_value = serde_json::to_value(&moved)?;
        let mut rolling = append_candidate("new-append", &new_hash);
        rolling.receipts.remove("inline-old");
        rolling.receipt_digests.remove("inline-old");

        let rejection = |candidate: &WalMetadata| -> Result<String> {
            let bytes = serde_json::to_vec(candidate)?;
            match validate_incremental(&store, &namespace, &before, &bytes) {
                Ok(_) => bail!("rolling candidate was accepted"),
                Err(error) => Ok(error.to_string()),
            }
        };

        let mut omitted = rolling.clone();
        omitted.receipt_index = Some(pages.insert_many(
            None,
            BTreeMap::from([("inline-old".to_owned(), moved_value.clone())]),
        )?);
        let failure = rejection(&omitted)?;
        assert!(failure.contains("not an exact extension"), "{failure}");

        let mut tampered = rolling.clone();
        tampered.receipt_index = Some(pages.insert_many(
            None,
            BTreeMap::from([
                (
                    "archived-old".to_owned(),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: old_receipt.lsn,
                        digest: Some("0".repeat(64)),
                    })?,
                ),
                ("inline-old".to_owned(), moved_value.clone()),
            ]),
        )?);
        let failure = rejection(&tampered)?;
        assert!(failure.contains("not an exact extension"), "{failure}");

        let mut extra = rolling.clone();
        extra.receipt_index = Some(pages.insert_many(
            None,
            BTreeMap::from([
                (
                    "archived-old".to_owned(),
                    serde_json::to_value(&old_receipt)?,
                ),
                ("inline-old".to_owned(), moved_value.clone()),
                (
                    "extra-archived".to_owned(),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: inline_lsn,
                        digest: Some("1".repeat(64)),
                    })?,
                ),
            ]),
        )?);
        let failure = rejection(&extra)?;
        assert!(failure.contains("not an exact extension"), "{failure}");

        let duplicate = append_candidate("archived-old", &duplicate_hash);
        let failure = rejection(&duplicate)?;
        assert!(
            failure.contains("reintroduced an archived receipt inline"),
            "{failure}"
        );

        let mut accepted = rolling;
        accepted.receipt_index = Some(pages.insert_many(
            Some(&old_root),
            BTreeMap::from([("inline-old".to_owned(), moved_value)]),
        )?);
        let after = serde_json::to_vec(&accepted)?;
        let accepted_state = validate_append(&store, &namespace, &before, &after)?;
        assert_eq!(
            accepted_state.archive_entries(),
            2,
            "rolling must add exactly the removed inline receipt"
        );
        assert_eq!(
            accepted_state.commit(),
            validate_cold(&store, &namespace, &after)?
        );
        let extended = pages.entries(accepted.receipt_index.as_deref())?;
        assert!(extended.contains_key("archived-old"));
        assert!(extended.contains_key("inline-old"));

        let duplicate_bytes = serde_json::to_vec(&duplicate)?;
        let previous = load_validated(&store, &namespace, &before)?;
        store.put(&old_root_key, b"tampered", PutCondition::Any)?;
        let failure = match crate::recovery::validate_index_append(
            previous,
            &duplicate_bytes,
            &namespace,
            |hash| load_segment(&store, &namespace, hash),
            |key| load_chunk(&store, key),
            load_page(&store, &namespace),
        ) {
            Ok(_) => bail!("tampered archive page was accepted"),
            Err(error) => error.to_string(),
        };
        assert!(failure.contains("digest"), "{failure}");
        store.put(&old_root_key, &old_root_page, PutCondition::Any)?;

        let previous = load_validated(&store, &namespace, &before)?;
        store.delete(&old_root_key, None)?;
        let failure = match crate::recovery::validate_index_append(
            previous,
            &after,
            &namespace,
            |hash| load_segment(&store, &namespace, hash),
            |key| load_chunk(&store, key),
            load_page(&store, &namespace),
        ) {
            Ok(_) => bail!("rolling append accepted a missing old archive page"),
            Err(error) => error.to_string(),
        };
        assert!(failure.contains("missing WAL receipt page"), "{failure}");
        Ok(())
    }

    #[test]
    fn rolling_append_cannot_reintroduce_an_archived_identity_inline() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        assert_eq!(
            wal.append_local(&append_request(&namespace, "archived-old", &[]))?,
            1
        );
        assert_eq!(
            wal.append_local(&append_request(&namespace, "inline-old", &[]))?,
            2
        );
        wal.truncate_with_part_bytes(&namespace, 2, FRAME_SIZE)?;
        let before = index_bytes(&store, &namespace)?;
        let (old, _) = wal.index(&namespace)?;
        let old_root = old
            .receipt_index
            .clone()
            .context("missing WAL receipt archive")?;
        let pages = PagedIndex::with_prefix(store.as_ref(), &namespace, "wal/receipt-pages");
        let inline_lsn = old.receipts["inline-old"];
        let inline_digest = old.receipt_digests["inline-old"].clone();
        let duplicate_hash = store_record_chunk(&store, &namespace, "archived-old", 3)?;
        let legal_hash = store_record_chunk(&store, &namespace, "new-append", 3)?;

        let mut candidate = old.clone();
        candidate.entries.push(Entry {
            lsn: 3,
            hash: duplicate_hash.clone(),
        });
        candidate.entry_count = Some(candidate.entry_count.unwrap() + 1);
        candidate.last_lsn = 3;
        candidate.commit = None;
        candidate.receipts.remove("inline-old");
        candidate.receipt_digests.remove("inline-old");
        candidate.receipts.insert("archived-old".into(), 3);
        candidate
            .receipt_digests
            .insert("archived-old".into(), duplicate_hash);
        candidate.receipt_index = Some(pages.insert_many(
            Some(&old_root),
            BTreeMap::from([(
                "inline-old".to_owned(),
                serde_json::to_value(ArchivedReceipt {
                    lsn: inline_lsn,
                    digest: Some(inline_digest),
                })?,
            )]),
        )?);
        let bytes = serde_json::to_vec(&candidate)?;
        let failure = match validate_append(&store, &namespace, &before, &bytes) {
            Ok(_) => bail!("rolled append reintroduced an archived identity inline"),
            Err(error) => error.to_string(),
        };
        assert!(
            failure.contains("reintroduced an archived receipt inline"),
            "{failure}"
        );
        assert!(
            validate_cold(&store, &namespace, &bytes).is_err(),
            "cold validation accepted an identity stored inline and archived"
        );

        let mut legal = candidate;
        legal.entries.last_mut().unwrap().hash = legal_hash.clone();
        legal.receipts.remove("archived-old");
        legal.receipt_digests.remove("archived-old");
        legal.receipts.insert("new-append".into(), 3);
        legal
            .receipt_digests
            .insert("new-append".into(), legal_hash);
        let legal = serde_json::to_vec(&legal)?;
        let validated = validate_append(&store, &namespace, &before, &legal)?;
        assert_eq!(validated.archive_entries(), 2);
        assert_eq!(
            validated.commit(),
            validate_cold(&store, &namespace, &legal)?
        );
        Ok(())
    }

    #[test]
    fn rolling_append_does_not_read_unrelated_archive_pages() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let namespace = namespace()?;
        let baseline = EncodedWalRecord::new(WalRecord {
            append_id: "baseline-1".into(),
            namespace: namespace.clone(),
            epoch: 1,
            lsn: 1,
            timestamp_millis: 1,
            batch: WalBatch {
                operations: vec![WalOperation::Truncate { size: 0 }],
            },
            commit: None,
        })?;
        store.put(
            &namespace.key(&format!("wal/chunks/{}", baseline.hash()))?,
            baseline.bytes(),
            PutCondition::Absent,
        )?;
        let inline_hash = store_record_chunk(&store, &namespace, "inline-old", 2)?;
        let new_hash = store_record_chunk(&store, &namespace, "new-append", 3)?;
        let pages = PagedIndex::with_prefix(store.as_ref(), &namespace, "wal/receipt-pages");
        let archived = (0..1_000)
            .map(|ordinal| {
                (
                    format!("archived-{ordinal:04}"),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: 1,
                        digest: Some("a".repeat(64)),
                    })
                    .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let old_root = pages.insert_many(None, archived)?;
        let old = WalMetadata {
            epoch: 1,
            last_lsn: 2,
            replay_baseline_lsn: 1,
            entries: vec![
                Entry {
                    lsn: 1,
                    hash: baseline.hash().to_owned(),
                },
                Entry {
                    lsn: 2,
                    hash: inline_hash.clone(),
                },
            ],
            receipts: BTreeMap::from([("inline-old".to_owned(), 2)]),
            receipt_digests: BTreeMap::from([("inline-old".to_owned(), inline_hash.clone())]),
            receipt_index: Some(old_root.clone()),
            entry_count: Some(2),
            entry_index: None,
            commit: None,
        };
        let before = serde_json::to_vec(&old)?;
        let mut candidate = old;
        candidate.entries.push(Entry {
            lsn: 3,
            hash: new_hash.clone(),
        });
        candidate.entry_count = Some(3);
        candidate.last_lsn = 3;
        candidate.receipts.remove("inline-old");
        candidate.receipt_digests.remove("inline-old");
        candidate.receipts.insert("new-append".into(), 3);
        candidate
            .receipt_digests
            .insert("new-append".into(), new_hash);
        candidate.receipt_index = Some(pages.insert_many(
            Some(&old_root),
            BTreeMap::from([(
                "inline-old".to_owned(),
                serde_json::to_value(ArchivedReceipt {
                    lsn: 2,
                    digest: Some(inline_hash),
                })?,
            )]),
        )?);
        let candidate = serde_json::to_vec(&candidate)?;

        let mut hot_reads = BTreeSet::new();
        crate::recovery::validate_index_append(
            load_validated(&store, &namespace, &before)?,
            &candidate,
            &namespace,
            |hash| load_segment(&store, &namespace, hash),
            |key| load_chunk(&store, key),
            |hash| {
                hot_reads.insert(hash.to_owned());
                load_page(&store, &namespace)(hash)
            },
        )?;
        let prefix = format!("{}wal/receipt-pages/", namespace.prefix());
        let sibling = store
            .list(&prefix)?
            .into_iter()
            .find_map(|key| {
                let hash = key.rsplit('/').next()?.to_owned();
                (!hot_reads.contains(&hash)).then_some((key, hash))
            })
            .context("every archive page was on the rolling insertion path")?;
        assert_ne!(sibling.1, old_root, "the archive root was not read");
        let sibling_bytes = store
            .get(&sibling.0)?
            .context("missing unread archive page")?
            .bytes;

        let previous = load_validated(&store, &namespace, &before)?;
        store.put(&sibling.0, b"tampered", PutCondition::Any)?;
        crate::recovery::validate_index_append(
            previous,
            &candidate,
            &namespace,
            |hash| load_segment(&store, &namespace, hash),
            |key| load_chunk(&store, key),
            load_page(&store, &namespace),
        )?;
        assert!(
            validate_cold(&store, &namespace, &candidate).is_err(),
            "cold validation accepted a tampered unread sibling page"
        );
        store.put(&sibling.0, &sibling_bytes, PutCondition::Any)?;
        Ok(())
    }

    #[test]
    fn legacy_inline_window_does_not_reject_an_ordinary_append() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        for ordinal in 1..=WAL_RECEIPT_INLINE_LIMIT {
            assert_eq!(
                wal.append_local(&append_request(
                    &namespace,
                    &format!("legacy-{ordinal}"),
                    &[]
                ))?,
                ordinal as u64
            );
        }
        let (mut legacy, etag) = wal.index(&namespace)?;
        assert_eq!(legacy.receipts.len(), WAL_RECEIPT_INLINE_LIMIT);
        legacy.receipt_digests.clear();
        wal.commit(&namespace, &legacy, etag)?;
        let before = index_bytes(&store, &namespace)?;

        let ordinary = append_request(&namespace, "ordinary", &[]);
        let lsn = wal.append_local(&ordinary)?;
        assert_eq!(lsn, (WAL_RECEIPT_INLINE_LIMIT + 1) as u64);
        let after = index_bytes(&store, &namespace)?;
        let metadata = wal.metadata(&namespace)?;
        assert!(
            metadata.receipt_index.is_none(),
            "an append must not archive its own receipt without moving an older one"
        );
        assert_eq!(metadata.receipts.get("ordinary"), Some(&lsn));
        assert_eq!(metadata.receipts.len(), WAL_RECEIPT_INLINE_LIMIT + 1);
        assert_eq!(
            validate_incremental(&store, &namespace, &before, &after)?,
            validate_cold(&store, &namespace, &after)?
        );
        assert_eq!(wal.append_local(&ordinary)?, lsn);
        assert_eq!(wal.metadata(&namespace)?.last_lsn, lsn);
        Ok(())
    }

    #[test]
    fn three_hundred_legacy_receipts_still_accept_an_ordinary_append() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        for ordinal in 1..=WAL_RECEIPT_INLINE_LIMIT {
            assert_eq!(
                wal.append_local(&append_request(
                    &namespace,
                    &format!("legacy-{ordinal}"),
                    &[]
                ))?,
                ordinal as u64
            );
        }
        let (mut legacy, etag) = wal.index(&namespace)?;
        legacy.receipt_digests.clear();
        wal.commit(&namespace, &legacy, etag)?;
        for ordinal in (WAL_RECEIPT_INLINE_LIMIT + 1)..=300 {
            let (mut legacy, etag) = wal.index(&namespace)?;
            legacy.receipt_digests.clear();
            wal.commit(&namespace, &legacy, etag)?;
            assert_eq!(
                wal.append_local(&append_request(
                    &namespace,
                    &format!("legacy-{ordinal}"),
                    &[]
                ))?,
                ordinal as u64
            );
        }
        let (mut legacy, etag) = wal.index(&namespace)?;
        legacy.receipt_digests.clear();
        wal.commit(&namespace, &legacy, etag)?;
        assert_eq!(wal.metadata(&namespace)?.receipts.len(), 300);
        let before = index_bytes(&store, &namespace)?;

        let ordinary = append_request(&namespace, "ordinary", &[]);
        let lsn = wal.append_local(&ordinary)?;
        assert_eq!(lsn, 301);
        let after = index_bytes(&store, &namespace)?;
        let metadata = wal.metadata(&namespace)?;
        assert!(metadata.receipt_index.is_none());
        assert_eq!(metadata.receipts.get("ordinary"), Some(&lsn));
        assert_eq!(metadata.receipts.len(), 301);
        assert_eq!(
            validate_incremental(&store, &namespace, &before, &after)?,
            validate_cold(&store, &namespace, &after)?
        );
        assert_eq!(wal.append_local(&ordinary)?, lsn);
        assert_eq!(
            load_validated(&store, &namespace, &after)?.archive_entries(),
            0,
            "an undigested legacy window must not grow the archive count"
        );
        Ok(())
    }

    #[test]
    fn receipt_window_rolls_digested_receipts_and_preserves_duplicate_identity() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = Namespace::new("receipt", "window")?;
        let mut first = None;
        for ordinal in 1..=(WAL_RECEIPT_INLINE_LIMIT + WAL_RECEIPT_ROLL_BATCH + 1) {
            let request = append_request(&namespace, &format!("append-{ordinal}"), &[]);

            if ordinal == 1 {
                first = Some(request.clone());
            }
            assert_eq!(wal.append_local(&request)?, ordinal as u64);
            if ordinal == 1 {
                let (mut legacy_index, etag) = wal.index(&namespace)?;
                legacy_index.receipt_digests.clear();
                wal.commit(&namespace, &legacy_index, etag)?;
            }
        }
        let index = wal.metadata(&namespace)?;
        assert!(index.receipt_index.is_some());
        assert!(index.receipts.contains_key("append-1"));
        assert!(!index.receipt_digests.contains_key("append-1"));
        assert!(index.receipts.len() < index.entry_count.unwrap() as usize);
        assert!(index.receipts.len() <= WAL_RECEIPT_INLINE_LIMIT);

        let first = first.context("first receipt request missing")?;
        assert_eq!(wal.append_local(&first)?, 1);
        assert_eq!(wal.metadata(&namespace)?.last_lsn, index.last_lsn);
        let mut changed = first.clone();
        changed.batch.operations.push(WalOperation::Write {
            offset: 0,
            bytes: vec![1],
        });
        assert!(wal.append_local(&changed).is_err());

        let bytes = store
            .get(&namespace.key("wal/index")?)?
            .context("WAL index missing")?
            .bytes;
        crate::recovery::validate_index_state(
            &bytes,
            &namespace,
            |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                    .context("WAL index segment missing")?
                    .bytes)
            },
            |key| Ok(store.get(key)?.context("WAL chunk missing")?.bytes),
            load_page(&store, &namespace),
        )?;
        Ok(())
    }

    #[test]
    fn archived_receipt_missing_or_tampered_evidence_fails_closed() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let original = append_request(&namespace, "missing-evidence", &[1, 2, 3]);
        assert_eq!(wal.append_local(&original)?, 1);
        wal.append_local(&append_request(&namespace, "retained", &[4, 5]))?;
        wal.truncate_with_part_bytes(&namespace, 2, FRAME_SIZE)?;
        let root = wal
            .metadata(&namespace)?
            .receipt_index
            .context("missing WAL receipt archive")?;
        let page_key = namespace.key(&format!("wal/receipt-pages/{root}"))?;
        let page = store
            .get(&page_key)?
            .context("WAL receipt page missing")?
            .bytes;
        store.delete(&page_key, None)?;
        assert!(wal.append_local(&original).is_err());
        store.put(&page_key, b"tampered", PutCondition::Any)?;
        let bytes = index_bytes(&store, &namespace)?;
        assert!(
            crate::recovery::validate_receipt_index(&bytes, load_page(&store, &namespace)).is_err()
        );
        store.put(&page_key, &page, PutCondition::Any)?;
        assert_eq!(wal.append_local(&original)?, 1);
        Ok(())
    }
    #[test]
    fn archived_receipt_answers_a_retry_and_rejects_changed_content() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let original = request(&namespace, "archived-retry", &[1, 2, 3]);
        assert_eq!(wal.append_local(&original)?, 1);
        assert_eq!(
            wal.append_local(&request(&namespace, "kept-retry", &[4, 5]))?,
            2
        );

        wal.truncate_with_part_bytes(&namespace, 2, FRAME_SIZE)?;

        let (index, _) = wal.index(&namespace)?;
        assert_eq!(index.replay_baseline_lsn, 1);
        assert!(!index.receipts.contains_key("archived-retry"));
        assert!(index.receipts.contains_key("kept-retry"));
        assert!(index.receipt_index.is_some());

        // The original request is answered from the archive without a second append.
        assert_eq!(wal.append_local(&original)?, 1);
        assert_eq!(wal.index(&namespace)?.0.last_lsn, 2);

        let changed = request(&namespace, "archived-retry", &[9]);
        let failure = wal.append_local(&changed).unwrap_err().to_string();
        assert!(failure.contains("conflict"), "{failure}");
        assert_eq!(wal.index(&namespace)?.0.last_lsn, 2);

        let bytes = index_bytes(&store, &namespace)?;
        crate::recovery::validate_receipt_index(&bytes, load_page(&store, &namespace))?;

        let root = wal
            .index(&namespace)?
            .0
            .receipt_index
            .context("missing WAL receipt archive")?;
        store.delete(&namespace.key(&format!("wal/receipt-pages/{root}"))?, None)?;
        let failure =
            crate::recovery::validate_receipt_index(&bytes, load_page(&store, &namespace))
                .unwrap_err()
                .to_string();
        assert!(failure.contains("missing WAL receipt page"), "{failure}");
        Ok(())
    }

    #[test]
    fn archived_legacy_receipt_without_digest_refuses_replay() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(LocalObjectStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = namespace()?;
        let original = request(&namespace, "legacy-retry", &[1, 2, 3]);
        assert_eq!(wal.append_local(&original)?, 1);
        assert_eq!(
            wal.append_local(&request(&namespace, "kept-retry", &[4, 5]))?,
            2
        );
        let (mut index, etag) = wal.index(&namespace)?;
        index.receipt_digests.clear();
        wal.commit(&namespace, &index, etag)?;

        wal.truncate_with_part_bytes(&namespace, 2, FRAME_SIZE)?;

        let failure = wal.append_local(&original).unwrap_err().to_string();
        assert!(failure.contains("content unavailable"), "{failure}");
        assert_eq!(wal.index(&namespace)?.0.last_lsn, 2);
        Ok(())
    }

    struct BatchedPageStore {
        inner: LocalObjectStore,
        batches: std::sync::atomic::AtomicUsize,
        pages: std::sync::atomic::AtomicUsize,
    }

    impl BatchedPageStore {
        fn open(path: &std::path::Path) -> Result<Self> {
            Ok(Self {
                inner: LocalObjectStore::open(path)?,
                batches: std::sync::atomic::AtomicUsize::new(0),
                pages: std::sync::atomic::AtomicUsize::new(0),
            })
        }
    }

    impl ObjectStore for BatchedPageStore {
        fn get(&self, key: &str) -> Result<Option<crate::object_store::StoredObject>> {
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
            self.inner.put(key, bytes, condition)
        }

        fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
            self.inner.delete(key, expected)
        }

        fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix)
        }

        fn publish_pages(&self, writes: &[crate::object_store::GroupWrite]) -> Result<bool> {
            self.batches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.pages
                .fetch_add(writes.len(), std::sync::atomic::Ordering::SeqCst);
            self.inner.put_group(writes)?;
            Ok(true)
        }
    }

    #[test]
    fn a_receipt_roll_publishes_its_pages_in_one_batch() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(BatchedPageStore::open(directory.path())?);
        let wal = RemoteWal::new(store.clone());
        let namespace = Namespace::new("receipt", "batched")?;
        for ordinal in 1..=(WAL_RECEIPT_INLINE_LIMIT + WAL_RECEIPT_ROLL_BATCH + 1) {
            assert_eq!(
                wal.append_local(&append_request(
                    &namespace,
                    &format!("append-{ordinal}"),
                    &[]
                ))?,
                ordinal as u64
            );
        }
        assert_eq!(
            store.batches.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the roll must reach the store as one batched page publication"
        );
        assert!(
            store.pages.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "the roll must archive at least one page"
        );
        assert!(wal.metadata(&namespace)?.receipt_index.is_some());
        let bytes = index_bytes(&store.inner, &namespace)?;
        validate_cold(&store.inner, &namespace, &bytes)?;
        assert_eq!(
            wal.append_local(&append_request(&namespace, "append-1", &[]))?,
            1,
            "an archived receipt must still answer a duplicate"
        );
        Ok(())
    }
}

#[cfg(test)]
mod replay_window_tests {
    use super::*;
    use crate::object_store::{PutCondition, StoredObject};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const CHILD_ENV: &str = "PERI_WAL_REPLAY_TEST_CHILD";
    const CHILD_TEST: &str = "wal::replay_window_tests::wal_replay_window_follows_the_byte_budget";

    #[derive(Clone, Copy)]
    enum Chunk {
        Present,
        Missing,
        Corrupt,
    }

    /// Fake branch store: serves a hand-built index and chunk bodies, and counts the chunk
    /// reads that overlapped, so the fetch window is observable from outside the replay.
    struct PrefetchStore {
        index: Vec<u8>,
        chunks: BTreeMap<String, Vec<u8>>,
        delays: BTreeMap<String, Duration>,
        missing: BTreeSet<String>,
        completed: Mutex<Vec<String>>,
        live: AtomicUsize,
        peak: AtomicUsize,
    }

    impl ObjectStore for PrefetchStore {
        fn get(&self, key: &str) -> Result<Option<StoredObject>> {
            if key.ends_with("/wal/index") {
                return Ok(Some(StoredObject {
                    bytes: self.index.clone(),
                    etag: "index".into(),
                }));
            }
            let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(live, Ordering::SeqCst);
            // Held past every return path, so a failed read still counts as in flight.
            let _live = Live(&self.live);
            if let Some(delay) = self.delays.get(key) {
                std::thread::sleep(*delay);
            }
            self.completed.lock().unwrap().push(key.to_owned());
            if self.missing.contains(key) {
                return Ok(None);
            }
            Ok(self.chunks.get(key).map(|bytes| StoredObject {
                bytes: bytes.clone(),
                etag: key.to_owned(),
            }))
        }

        fn put(&self, _: &str, _: &[u8], _: PutCondition) -> Result<String> {
            bail!("prefetch fixture store is read-only")
        }

        fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
            bail!("prefetch fixture store is read-only")
        }

        fn list(&self, _: &str) -> Result<Vec<String>> {
            bail!("prefetch fixture store is read-only")
        }
    }

    struct Live<'a>(&'a AtomicUsize);

    impl Drop for Live<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct Fixture {
        store: Arc<PrefetchStore>,
        namespace: Namespace,
        keys: BTreeMap<u64, String>,
    }

    impl Fixture {
        /// Writes the index and the chunk bodies a replay of `plan` would find, in the shape
        /// the append path publishes: `(lsn, read latency, chunk state)`.
        fn new(plan: &[(u64, u64, Chunk)]) -> Result<Self> {
            let namespace = Namespace::new("prefetch", "binding")?;
            let mut entries = Vec::new();
            let mut chunks = BTreeMap::new();
            let mut delays = BTreeMap::new();
            let mut missing = BTreeSet::new();
            let mut keys = BTreeMap::new();
            for &(lsn, delay_ms, chunk) in plan {
                let record = WalRecord {
                    append_id: format!("append-{lsn}"),
                    namespace: namespace.clone(),
                    epoch: 1,
                    lsn,
                    timestamp_millis: 1,
                    batch: WalBatch::default(),
                    commit: None,
                };
                let bytes = serde_json::to_vec(&record)?;
                let hash = blake3::hash(&bytes).to_hex().to_string();
                let key = namespace.key(&format!("wal/chunks/{hash}"))?;
                entries.push(Entry {
                    lsn,
                    hash: hash.clone(),
                });
                delays.insert(key.clone(), Duration::from_millis(delay_ms));
                match chunk {
                    Chunk::Present => {
                        chunks.insert(key.clone(), bytes);
                    }
                    // The index still names the hash of the real body, so only a reader that
                    // verifies the chunk can notice.
                    Chunk::Corrupt => {
                        chunks.insert(key.clone(), b"corrupted body".to_vec());
                    }
                    Chunk::Missing => {
                        missing.insert(key.clone());
                    }
                }
                keys.insert(lsn, key);
            }
            let index = WalMetadata {
                epoch: 1,
                last_lsn: entries.last().map_or(0, |entry: &Entry| entry.lsn),
                replay_baseline_lsn: 0,
                entries,
                receipts: BTreeMap::new(),
                receipt_digests: BTreeMap::new(),
                receipt_index: None,
                entry_count: None,
                entry_index: None,
                commit: None,
            };
            Ok(Self {
                store: Arc::new(PrefetchStore {
                    index: serde_json::to_vec(&index)?,
                    chunks,
                    delays,
                    missing,
                    completed: Mutex::new(Vec::new()),
                    live: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                }),
                namespace,
                keys,
            })
        }

        /// Replays with an explicit window, so the cases below pin one window each instead of
        /// inheriting whatever the environment of the suite happens to hold.
        fn replay(&self, from_lsn: u64, window: usize) -> Result<Vec<u64>> {
            let wal = RemoteWal::new(self.store.clone());
            let mut seen = Vec::new();
            wal.for_each_record_with_window(&self.namespace, from_lsn, window, |record| {
                seen.push(record.lsn);
                Ok(())
            })?;
            Ok(seen)
        }

        fn peak(&self) -> usize {
            self.store.peak.load(Ordering::SeqCst)
        }

        fn completion_order(&self) -> Vec<String> {
            self.store.completed.lock().unwrap().clone()
        }
    }

    #[test]
    fn replay_delivers_records_in_lsn_order_when_chunks_finish_in_reverse() -> Result<()> {
        // Every chunk waits longer than the one after it, so the reads finish in the reverse
        // of LSN order and only the delivery order can make `visit` ascending.
        let plan: Vec<(u64, u64, Chunk)> = (1..=6)
            .map(|lsn| (lsn, (7 - lsn) * 20, Chunk::Present))
            .collect();
        let fixture = Fixture::new(&plan)?;
        let ascending: Vec<String> = (1..=6).map(|lsn| fixture.keys[&lsn].clone()).collect();
        assert_eq!(fixture.replay(0, 4)?, (1..=6u64).collect::<Vec<_>>());
        assert_ne!(
            fixture.completion_order(),
            ascending,
            "the reads finished in LSN order, so this case proves nothing about delivery"
        );
        // The window covers the requested range, not the whole index.
        assert_eq!(fixture.replay(4, 2)?, vec![4u64, 5, 6]);
        Ok(())
    }

    #[test]
    fn replay_reports_the_lowest_lsn_failure_and_stops_delivering() -> Result<()> {
        // LSN 2 fails slowly, LSN 3 fails fast with a different reason: a window that reported
        // whichever chunk answered first would return LSN 3's error instead.
        let plan = [
            (1, 10, Chunk::Present),
            (2, 120, Chunk::Corrupt),
            (3, 10, Chunk::Missing),
        ];
        let fixture = Fixture::new(&plan)?;
        let wal = RemoteWal::new(fixture.store.clone());
        let mut seen = Vec::new();
        let error = wal
            .for_each_record_with_window(&fixture.namespace, 0, 4, |record| {
                seen.push(record.lsn);
                Ok(())
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("WAL chunk checksum mismatch"),
            "{error}"
        );
        assert_eq!(seen, vec![1]);
        assert!(
            fixture.completion_order().contains(&fixture.keys[&3]),
            "the later failure was never fetched, so this case proves nothing about ordering"
        );
        Ok(())
    }

    #[test]
    fn replay_stops_delivering_when_the_visitor_rejects_a_record() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=6).map(|lsn| (lsn, 10, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let wal = RemoteWal::new(fixture.store.clone());
        let mut seen = Vec::new();
        let error = wal
            .for_each_record_with_window(&fixture.namespace, 0, 3, |record| {
                seen.push(record.lsn);
                if record.lsn == 2 {
                    bail!("replay stopped by visitor");
                }
                Ok(())
            })
            .unwrap_err();
        assert!(
            error.to_string().contains("replay stopped by visitor"),
            "{error}"
        );
        assert_eq!(seen, vec![1, 2]);
        Ok(())
    }

    #[test]
    fn replay_bounds_the_window_plus_the_delivery_to_the_budget() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=12).map(|lsn| (lsn, 30, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let window = wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT);
        assert_eq!(fixture.replay(0, window)?, (1..=12u64).collect::<Vec<_>>());
        let peak = fixture.peak();
        assert!(
            peak <= window,
            "read {peak} chunks at once, window {window}"
        );
        assert!(
            peak >= 2,
            "a {window}-wide window never overlapped a read: peak={peak}"
        );
        // The delivered record is the unit the budget spends beyond the window, so the ledger
        // charges `peak + 1`. Charging `peak` alone would describe a replay that never hands a
        // chunk to its visitor.
        assert!(
            (peak + 1) * WAL_CHUNK_CHARGE <= WAL_REPLAY_BYTES_DEFAULT,
            "held {peak} chunks plus one delivery, which the ledger charges as {} bytes, over a \
             budget of {}",
            (peak + 1) * WAL_CHUNK_CHARGE,
            WAL_REPLAY_BYTES_DEFAULT
        );
        Ok(())
    }

    /// The window bounds one replay; the shared permits bound the worker. Two replays at the
    /// widest window would hold twice the cap without them — which is what the compile-time
    /// reservation used to assume away by describing a single replay.
    #[test]
    fn concurrent_replays_share_one_fetch_budget() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=12).map(|lsn| (lsn, 30, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let wal = Arc::new(RemoteWal::new(fixture.store.clone()));
        let replays = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let wal = wal.clone();
                    let namespace = fixture.namespace.clone();
                    scope.spawn(move || -> Result<Vec<u64>> {
                        let mut seen = Vec::new();
                        wal.for_each_record_with_window(
                            &namespace,
                            0,
                            WAL_REPLAY_WINDOW_MAX,
                            |record| {
                                seen.push(record.lsn);
                                Ok(())
                            },
                        )?;
                        Ok(seen)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        for replay in replays {
            assert_eq!(replay?, (1..=12u64).collect::<Vec<_>>());
        }
        let peak = fixture.peak();
        assert!(
            peak <= WAL_REPLAY_FETCH_PERMITS,
            "two replays held {peak} chunks at once, over the {WAL_REPLAY_FETCH_PERMITS} permits \
             the worker shares between them"
        );
        assert!(
            peak >= 2,
            "the replays never overlapped a read: peak={peak}"
        );
        Ok(())
    }

    /// The window is a *derived* quantity: the budget is the setting, the window is what it
    /// buys. Pinning the arithmetic here keeps the byte bound from drifting away from the
    /// window the replay actually uses.
    #[test]
    fn the_window_is_the_budget_less_the_delivery_over_the_fetch_charge() {
        assert_eq!(wal_replay_window_for(1), 1);
        assert_eq!(wal_replay_window_for(WAL_CHUNK_CHARGE - 1), 1);
        assert_eq!(wal_replay_window_for(WAL_CHUNK_CHARGE), 1);
        assert_eq!(wal_replay_window_for(WAL_CHUNK_CHARGE + 1), 1);
        // `n + 1` charges buy `n` fetches: the last charge is the record being handed over.
        for window in 1..=WAL_REPLAY_WINDOW_MAX {
            assert_eq!(
                wal_replay_window_for((window + 1) * WAL_CHUNK_CHARGE),
                window
            );
        }
        assert_eq!(wal_replay_window_for(usize::MAX), WAL_REPLAY_WINDOW_MAX);
        assert_eq!(
            wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT),
            4,
            "the default budget has to buy the window it replaced, not a serial replay"
        );
    }

    #[test]
    fn wal_replay_window_follows_the_byte_budget() -> Result<()> {
        if std::env::var_os(CHILD_ENV).is_some() {
            return child_case();
        }
        // The child re-exec is how the environment reaches `for_each_record`: this suite must
        // not mutate the process environment that its own parallel cases read from.
        let three_fetches = (4 * WAL_CHUNK_CHARGE).to_string();
        for (value, expected) in [
            (Some("1".to_string()), "outcome=single"),
            (None, "outcome=default"),
            (Some(three_fetches.clone()), "outcome=three_fetches"),
            (Some("0".to_string()), "outcome=rejected"),
            (Some("abc".to_string()), "outcome=rejected"),
            (Some("-1".to_string()), "outcome=rejected"),
        ] {
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", CHILD_TEST, "--nocapture"])
                .env(CHILD_ENV, "1");
            match &value {
                Some(value) => command.env(WAL_REPLAY_BYTES_ENV, value),
                None => command.env_remove(WAL_REPLAY_BYTES_ENV),
            };
            let output = command.output()?;
            let report = String::from_utf8_lossy(&output.stderr).into_owned();
            assert!(
                output.status.success(),
                "child for {WAL_REPLAY_BYTES_ENV}={value:?} failed: {report}"
            );
            assert!(
                report.contains("WAL_REPLAY_CHILD") && report.contains(expected),
                "child for {WAL_REPLAY_BYTES_ENV}={value:?} did not report {expected}: {report}"
            );
        }
        Ok(())
    }

    /// Child arm of [`wal_replay_window_follows_the_byte_budget`]: replays 12 chunks and asserts
    /// the window its budget asked for is the one the replay used.
    fn child_case() -> Result<()> {
        let plan: Vec<(u64, u64, Chunk)> = (1..=12).map(|lsn| (lsn, 20, Chunk::Present)).collect();
        let fixture = Fixture::new(&plan)?;
        let wal = RemoteWal::new(fixture.store.clone());
        let mut seen = 0;
        let result = wal.for_each_record(&fixture.namespace, 0, |_| {
            seen += 1;
            Ok(())
        });
        let peak = fixture.peak();
        let configured = std::env::var(WAL_REPLAY_BYTES_ENV);
        match (&configured, &result) {
            (Ok(value), Ok(())) if value == "1" => {
                assert_eq!(seen, 12);
                assert_eq!(
                    peak, 1,
                    "{WAL_REPLAY_BYTES_ENV}=1 replayed more than one chunk at a time"
                );
                eprintln!("WAL_REPLAY_CHILD value=1 outcome=single peak={peak} visited={seen}");
            }
            (Err(std::env::VarError::NotPresent), Ok(())) => {
                assert_eq!(seen, 12);
                let window = wal_replay_window_for(WAL_REPLAY_BYTES_DEFAULT);
                assert!(
                    (2..=window).contains(&peak),
                    "the default budget replayed with peak={peak}, expected 2..={window}"
                );
                eprintln!(
                    "WAL_REPLAY_CHILD value=unset outcome=default peak={peak} visited={seen}"
                );
            }
            (Ok(value), Ok(())) => {
                assert_eq!(seen, 12);
                let budget = value.parse::<usize>()?;
                let window = wal_replay_window_for(budget);
                assert_eq!(
                    window, 3,
                    "four charges have to buy a window of three fetches"
                );
                assert!(
                    (2..=window).contains(&peak),
                    "a window of {window} replayed with peak={peak}"
                );
                // `peak` counts fetches, so the delivery is the unit beyond it.
                assert!(
                    (peak + 1) * WAL_CHUNK_CHARGE <= budget,
                    "{peak} chunks in flight plus one delivery exceed the {value}-byte budget"
                );
                eprintln!(
                    "WAL_REPLAY_CHILD value={value} outcome=three_fetches peak={peak} \
                     window={window}"
                );
            }
            (Ok(value), Err(error)) => {
                assert!(
                    error
                        .to_string()
                        .contains(&format!("invalid {WAL_REPLAY_BYTES_ENV}")),
                    "{error}"
                );
                assert_eq!(seen, 0, "a rejected budget must not have replayed a record");
                assert_eq!(peak, 0, "a rejected budget must not have read a chunk");
                eprintln!("WAL_REPLAY_CHILD value={value} outcome=rejected error={error}");
            }
            (_, outcome) => bail!("child case for {configured:?} ended as {outcome:?}"),
        }
        Ok(())
    }

    /// Manual timing harness: `cargo test --lib -- --ignored --nocapture`.
    ///
    /// Fake-store microbenchmark, not platform evidence: the sleeps stand in for authority
    /// round trips and no authority, object store, TLS or contention is involved, so the
    /// numbers cannot be extrapolated to a real open. Both arms run the same code path and
    /// differ only in the window, and neither reads `PERI_WORKER_WAL_REPLAY_BYTES`.
    #[test]
    #[ignore = "manual fake-store timing comparison; not platform performance evidence"]
    fn wal_replay_wall_clock_against_serial_replay() -> Result<()> {
        const CHUNKS: u64 = 20;
        const READ_MS: u64 = 30;
        let plan: Vec<(u64, u64, Chunk)> = (1..=CHUNKS)
            .map(|lsn| (lsn, READ_MS, Chunk::Present))
            .collect();
        let mut walls = BTreeMap::new();
        for window in [1, 4] {
            let fixture = Fixture::new(&plan)?;
            let started = Instant::now();
            let seen = fixture.replay(0, window)?;
            let wall_ms = started.elapsed().as_millis();
            assert_eq!(seen, (1..=CHUNKS).collect::<Vec<_>>());
            eprintln!(
                "WAL_REPLAY_BENCH chunks={CHUNKS} read_ms={READ_MS} window={window} \
                 wall_ms={wall_ms} peak={}",
                fixture.peak(),
            );
            walls.insert(window, wall_ms);
        }
        eprintln!(
            "WAL_REPLAY_BENCH_SUMMARY serial_ms={} window4_ms={} ratio={:.2}",
            walls[&1],
            walls[&4],
            walls[&1] as f64 / walls[&4] as f64
        );
        Ok(())
    }
}

#[cfg(test)]
mod index_segment_tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn segmented_retry_replay_truncate_and_legacy_normalization() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let wal = RemoteWal::new(store.clone());
        let namespace = Namespace::new("wal", "index-segments")?;
        let first = WalAppendRequest {
            namespace: namespace.clone(),
            epoch: 1,
            batch: WalBatch::default(),
            position: None,
            append_id: "first".into(),
            timestamp_millis: 1,
        };
        wal.append_local(&first)?;
        for _ in 1..514 {
            wal.append(&namespace, 1, WalBatch::default())?;
        }
        assert_eq!(wal.resolved_entries(&namespace)?.len(), 514);
        assert_eq!(wal.read(&namespace, 0)?.len(), 514);
        assert_eq!(wal.append_local(&first)?, 1);
        let head = wal.metadata(&namespace)?;
        assert_eq!(head.entry_count, Some(514));
        assert_eq!(head.entries.len(), 2);
        let root = head.entry_index.clone().unwrap();
        wal.truncate(&namespace, 300)?;
        let truncated = wal.metadata(&namespace)?;
        assert!(truncated.entry_index.is_none());
        assert_eq!(truncated.entry_count, Some(216));
        assert_eq!(wal.append_local(&first)?, 1);
        assert!(store
            .get(&namespace.key(&format!("wal/index-segments/{root}"))?)?
            .is_some());
        assert_eq!(wal.read(&namespace, 0)?.len(), 216);

        let mut legacy = serde_json::to_value(&head)?;
        legacy["entries"] =
            serde_json::to_value(crate::recovery::resolve_entries(&head, |hash| {
                Ok(store
                    .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                    .unwrap()
                    .bytes)
            })?)?;
        legacy.as_object_mut().unwrap().remove("entry_count");
        legacy.as_object_mut().unwrap().remove("entry_index");
        store.put(
            &namespace.key("wal/index")?,
            &serde_json::to_vec(&legacy)?,
            PutCondition::Any,
        )?;
        assert_eq!(wal.metadata(&namespace)?.entry_count, Some(514));
        wal.append(&namespace, 1, WalBatch::default())?;
        assert_eq!(wal.metadata(&namespace)?.entries.len(), 1);
        assert_eq!(wal.resolved_entries(&namespace)?.len(), 515);
        Ok(())
    }
}

#[cfg(test)]
mod segmented_baseline_tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;

    #[test]
    fn baseline_continuation_across_segment_boundary_keeps_total_ordinal() -> Result<()> {
        let store = Arc::new(MemoryObjectStore::default());
        let wal = RemoteWal::new(store.clone());
        let namespace = Namespace::new("wal", "segmented-baseline")?;
        wal.append(
            &namespace,
            1,
            WalBatch {
                operations: vec![WalOperation::Write {
                    offset: 0,
                    bytes: vec![7; 300],
                }],
            },
        )?;
        wal.truncate_with_part_bytes(&namespace, 2, 1)?;
        let before = store.get(&namespace.key("wal/index")?)?.unwrap().bytes;
        let load_segment = |hash: &str| -> Result<Vec<u8>> {
            Ok(store
                .get(&namespace.key(&format!("wal/index-segments/{hash}"))?)?
                .context("missing segment")?
                .bytes)
        };
        let load_chunk =
            |key: &str| -> Result<Vec<u8>> { Ok(store.get(key)?.context("missing chunk")?.bytes) };
        let load_page = |hash: &str| -> Result<Vec<u8>> {
            Ok(store
                .get(&namespace.key(&format!("wal/receipt-pages/{hash}"))?)?
                .context("missing WAL receipt page")?
                .bytes)
        };
        let previous = crate::recovery::validate_index_state(
            &before,
            &namespace,
            load_segment,
            load_chunk,
            load_page,
        )?;
        let truncated = wal.metadata(&namespace)?;
        assert!(truncated.entry_index.is_some());
        assert!(truncated.entries.len() <= WAL_INDEX_INLINE_ENTRIES);
        assert_eq!(truncated.entry_count, Some(300));
        wal.append(&namespace, 1, WalBatch::default())?;
        let after = store.get(&namespace.key("wal/index")?)?.unwrap().bytes;
        crate::recovery::validate_index_append(
            previous,
            &after,
            &namespace,
            load_segment,
            load_chunk,
            load_page,
        )?;
        assert_eq!(wal.metadata(&namespace)?.entry_count, Some(301));
        assert_eq!(wal.metadata(&namespace)?.entries.len(), 1);
        let mut image = Vec::new();
        wal.for_each_record(&namespace, 0, |record| {
            apply_batch(&mut image, &record.batch)
        })?;
        assert_eq!(image, vec![7; 300]);
        Ok(())
    }
}
