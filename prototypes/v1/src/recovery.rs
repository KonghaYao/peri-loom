use anyhow::{ensure, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::authority::paged_index::PagedIndex;
use crate::manifest::WalPosition;
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition, StoredObject};
use crate::wal::{
    apply_batch, valid_entry_index, ArchivedReceipt, Entry, WalBatch, WalCommit, WalIndexSegment,
    WalMetadata, WalOperation, WalRecord, WAL_INDEX_SEGMENT_LIMIT, WAL_RECEIPT_ARCHIVE_BATCH,
    WAL_RECEIPT_PAGE_PREFIX,
};

pub(crate) const WAL_HEADER: usize = 32;
const WAL_FORMAT: u32 = 3_007_000;
const WAL_MAGIC_LITTLE_ENDIAN: u32 = 0x377f_0682;
const WAL_MAGIC_BIG_ENDIAN: u32 = 0x377f_0683;

/// Work the recovery path does that the `wal_durability` timer deliberately excludes: the
/// frame scans it runs before and after each append, and the image copies a validated WAL
/// extension pays. Kept separately observable so a review can tell a removed growth term
/// from one that merely moved out of the timer.
///
/// An accepted append extends its image in place, so the steady state copies no image bytes:
/// `copied_bytes` now counts only the buffer regrowth a batch can still force when it
/// outgrows the capacity the previous append left behind. The allocator may extend in
/// place, so that count is an upper bound, not a measurement of a memcpy.
///
/// Counters are per thread, like `request_reads`: the commit that pays the cost is the one
/// that reads it. A reader on another thread sees zeros until aggregation exists, and this
/// module does not add any: the scan updates the counters once per call, never per byte, so
/// the cost of keeping them is one thread-local read plus a few additions per scan. Every
/// field saturates instead of wrapping, because a diagnostic counter that wraps would
/// report a smaller cost than the work it measured.
///
/// The reset and stop counters name the reasons a verified prefix was abandoned or a scan
/// could not advance. They sit on the rare paths (a reset follows a mutation the prefix
/// cannot describe), so they cost one thread-local read each; the process-wide copy of every
/// count lives in [`crate::work_metrics`], which is off unless asked for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecoveryObservations {
    pub scanned_bytes: u64,
    pub copied_bytes: u64,
    pub scan_calls: u64,
    pub scan_nanos: u64,
    /// Frames the checksum chain walked, as opposed to the header and frame bytes counted
    /// by `scanned_bytes`.
    pub scanned_frames: u64,
    /// Resets per [`crate::work_metrics::ScanReset`] discriminant.
    pub scan_resets: [u64; RESET_REASONS],
    /// Stops per [`crate::work_metrics::ScanStop`] discriminant.
    pub scan_stops: [u64; STOP_REASONS],
}

const RESET_REASONS: usize = 6;
const STOP_REASONS: usize = 3;

impl RecoveryObservations {
    const ZERO: Self = Self {
        scanned_bytes: 0,
        copied_bytes: 0,
        scan_calls: 0,
        scan_nanos: 0,
        scanned_frames: 0,
        scan_resets: [0; RESET_REASONS],
        scan_stops: [0; STOP_REASONS],
    };
}

thread_local! {
    static OBSERVATIONS: std::cell::Cell<RecoveryObservations> =
        const { std::cell::Cell::new(RecoveryObservations::ZERO) };
}

pub fn recovery_observations() -> RecoveryObservations {
    OBSERVATIONS.with(std::cell::Cell::get)
}

pub fn reset_recovery_observations() {
    OBSERVATIONS.with(|observations| observations.set(RecoveryObservations::ZERO));
}

fn observed_scan(scanned: usize, frames: usize, elapsed: Duration) {
    OBSERVATIONS.with(|observations| {
        let counters = observations.get();
        observations.set(RecoveryObservations {
            scanned_bytes: counters.scanned_bytes.saturating_add(scanned as u64),
            scan_calls: counters.scan_calls.saturating_add(1),
            scan_nanos: counters
                .scan_nanos
                .saturating_add(elapsed.as_nanos() as u64),
            scanned_frames: counters.scanned_frames.saturating_add(frames as u64),
            ..counters
        });
    });
    crate::work_metrics::observe_scan(scanned, frames, elapsed);
}

fn observed_reset(reason: crate::work_metrics::ScanReset) {
    OBSERVATIONS.with(|observations| {
        let mut counters = observations.get();
        counters.scan_resets[reason as usize] =
            counters.scan_resets[reason as usize].saturating_add(1);
        observations.set(counters);
    });
    crate::work_metrics::observe_scan_reset(reason);
}

fn observed_stop(reason: crate::work_metrics::ScanStop) {
    OBSERVATIONS.with(|observations| {
        let mut counters = observations.get();
        counters.scan_stops[reason as usize] =
            counters.scan_stops[reason as usize].saturating_add(1);
        observations.set(counters);
    });
    crate::work_metrics::observe_scan_stop(reason);
}

fn observed_copy(bytes: usize) {
    OBSERVATIONS.with(|observations| {
        let counters = observations.get();
        observations.set(RecoveryObservations {
            copied_bytes: counters.copied_bytes.saturating_add(bytes as u64),
            ..counters
        });
    });
    crate::work_metrics::observe_copy(bytes);
}

#[cfg(test)]
mod bench;
#[cfg(test)]
pub(crate) mod fixture;
#[cfg(test)]
mod tests;

pub fn committed_position(bytes: &[u8]) -> Result<Option<WalPosition>> {
    let started = Instant::now();
    let mut scanned = 0;
    let mut frames = 0;
    let result = committed_position_of(bytes, &mut scanned, &mut frames);
    observed_scan(scanned, frames, started.elapsed());
    result
}

fn committed_position_of(
    bytes: &[u8],
    scanned: &mut usize,
    frames: &mut usize,
) -> Result<Option<WalPosition>> {
    if bytes.len() < WAL_HEADER {
        return Ok(None);
    }
    *scanned += WAL_HEADER;
    let magic = word(bytes, 0);
    ensure!(
        matches!(magic, WAL_MAGIC_LITTLE_ENDIAN | WAL_MAGIC_BIG_ENDIAN),
        "invalid WAL magic"
    );
    ensure!(word(bytes, 4) == WAL_FORMAT, "unsupported WAL format");
    let page_size = word(bytes, 8) as usize;
    ensure!(
        (512..=65536).contains(&page_size) && page_size.is_power_of_two(),
        "invalid WAL page size"
    );
    let big_endian = magic & 1 != 0;
    let mut rolling = checksum(&bytes[..24], [0, 0], big_endian);
    ensure!(
        rolling == [word(bytes, 24), word(bytes, 28)],
        "WAL header checksum mismatch"
    );
    let frame_size = page_size + 24;
    if !(bytes.len() - WAL_HEADER).is_multiple_of(frame_size) {
        return Ok(None);
    }
    let salts = [word(bytes, 16), word(bytes, 20)];
    let mut position = None;
    for (frame_index, frame) in bytes[WAL_HEADER..].chunks_exact(frame_size).enumerate() {
        *scanned += frame_size;
        *frames += 1;
        if word(frame, 0) == 0 || [word(frame, 8), word(frame, 12)] != salts {
            return Ok(None);
        }
        let next = checksum(
            &frame[24..],
            checksum(&frame[..8], rolling, big_endian),
            big_endian,
        );
        if next != [word(frame, 16), word(frame, 20)] {
            return Ok(None);
        }
        rolling = next;
        position = (word(frame, 4) != 0).then_some(WalPosition {
            salts,
            frame_count: frame_index as u64 + 1,
        });
    }
    Ok(position)
}

/// Frame-level state of a WAL image as [`committed_position`] reads it. A commit that only
/// appends frames reuses the verified prefix instead of re-deriving the checksum chain over
/// the whole image; a caller that rewrites or truncates bytes the prefix covers resets it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WalFrameScan {
    /// Header the verified prefix belongs to. A different header means the image was
    /// restarted or rewritten, so the prefix describes bytes that no longer exist.
    header: Option<[u8; WAL_HEADER]>,
    frame_size: usize,
    big_endian: bool,
    salts: [u32; 2],
    /// Checksum chain after the `verified_frames` frames.
    rolling: [u32; 2],
    verified_frames: usize,
    /// Header plus every frame examined, including the frame a stop was decided on. Only a
    /// mutation that leaves the first `examined` bytes untouched may keep this state.
    examined: usize,
    position: Option<WalPosition>,
    /// The scan stopped on a frame that cannot yield a position; further appends cannot
    /// change that answer until the bytes it examined do.
    stopped: bool,
}

impl WalFrameScan {
    fn reset(&mut self) {
        *self = Self::default();
    }

    /// Records why the prefix had to go, then rebuilds the state.
    fn reset_for(&mut self, reason: crate::work_metrics::ScanReset) {
        observed_reset(reason);
        self.reset();
    }

    /// Equivalent of [`committed_position`] for an image whose examined bytes the caller
    /// left untouched since the previous call.
    fn position(&mut self, bytes: &[u8]) -> Result<Option<WalPosition>> {
        let started = Instant::now();
        let mut scanned = 0;
        let mut frames = 0;
        let result = self.advance(bytes, &mut scanned, &mut frames);
        observed_scan(scanned, frames, started.elapsed());
        result
    }

    fn advance(
        &mut self,
        bytes: &[u8],
        scanned: &mut usize,
        frames: &mut usize,
    ) -> Result<Option<WalPosition>> {
        // A shrink below what was examined is a mutation the caller did not report, and the
        // prefix cannot survive it.
        if bytes.len() < self.examined {
            self.reset_for(crate::work_metrics::ScanReset::Shrink);
        }
        if bytes.len() < WAL_HEADER {
            self.reset_for(crate::work_metrics::ScanReset::ShortImage);
            return Ok(None);
        }
        *scanned += WAL_HEADER;
        let header: [u8; WAL_HEADER] = bytes[..WAL_HEADER].try_into().expect("header length");
        if self.header != Some(header) {
            self.reset_for(crate::work_metrics::ScanReset::HeaderChanged);
            self.derive(&header)?;
            self.header = Some(header);
            self.examined = WAL_HEADER;
        }
        if self.stopped {
            return Ok(None);
        }
        let available = bytes.len() - WAL_HEADER;
        let complete = available / self.frame_size;
        if complete > self.verified_frames {
            self.extend(bytes, complete, scanned, frames);
        }
        if self.stopped {
            return Ok(None);
        }
        if !available.is_multiple_of(self.frame_size) {
            observed_stop(crate::work_metrics::ScanStop::IncompleteTail);
            return Ok(None);
        }
        Ok(self.position)
    }

    /// Header checks of [`committed_position`], plus the constants its frame loop needs.
    fn derive(&mut self, header: &[u8; WAL_HEADER]) -> Result<()> {
        let magic = word(header, 0);
        ensure!(
            matches!(magic, WAL_MAGIC_LITTLE_ENDIAN | WAL_MAGIC_BIG_ENDIAN),
            "invalid WAL magic"
        );
        ensure!(word(header, 4) == WAL_FORMAT, "unsupported WAL format");
        let page_size = word(header, 8) as usize;
        ensure!(
            (512..=65536).contains(&page_size) && page_size.is_power_of_two(),
            "invalid WAL page size"
        );
        let big_endian = magic & 1 != 0;
        let rolling = checksum(&header[..24], [0, 0], big_endian);
        ensure!(
            rolling == [word(header, 24), word(header, 28)],
            "WAL header checksum mismatch"
        );
        self.frame_size = page_size + 24;
        self.big_endian = big_endian;
        self.salts = [word(header, 16), word(header, 20)];
        self.rolling = rolling;
        Ok(())
    }

    /// Frames the checksum chain already covers are never re-read: the loop resumes at the
    /// first unverified frame and stops where [`committed_position`] would.
    fn extend(&mut self, bytes: &[u8], complete: usize, scanned: &mut usize, frames: &mut usize) {
        for index in self.verified_frames..complete {
            let start = WAL_HEADER + index * self.frame_size;
            let frame = &bytes[start..start + self.frame_size];
            self.examined = start + self.frame_size;
            *scanned += self.frame_size;
            *frames += 1;
            if word(frame, 0) == 0 || [word(frame, 8), word(frame, 12)] != self.salts {
                observed_stop(crate::work_metrics::ScanStop::SaltMismatch);
                self.stopped = true;
                return;
            }
            let next = checksum(
                &frame[24..],
                checksum(&frame[..8], self.rolling, self.big_endian),
                self.big_endian,
            );
            if next != [word(frame, 16), word(frame, 20)] {
                observed_stop(crate::work_metrics::ScanStop::ChecksumMismatch);
                self.stopped = true;
                return;
            }
            self.rolling = next;
            self.verified_frames = index + 1;
            // Assigned for every frame, exactly as the whole-image scan does it: a frame
            // that does not commit leaves the image without a committed boundary, so a
            // trailing uncommitted frame must clear the position rather than keep it.
            self.position = (word(frame, 4) != 0).then_some(WalPosition {
                salts: self.salts,
                frame_count: index as u64 + 1,
            });
        }
    }

    /// A write that starts inside the examined prefix replaces bytes the chain covers.
    fn note_write(&mut self, offset: usize) {
        if offset < self.examined {
            self.reset_for(crate::work_metrics::ScanReset::WriteInPrefix);
        }
    }

    fn note_truncate(&mut self, size: usize) {
        if size < self.examined {
            self.reset_for(crate::work_metrics::ScanReset::TruncateInPrefix);
        }
    }
}

/// The replayed WAL image together with the scan state describing its verified prefix.
/// Every mutation goes through this type, so the prefix can never describe bytes the caller
/// replaced.
#[derive(Clone, Debug, Default)]
pub(crate) struct WalImage {
    bytes: Vec<u8>,
    scan: WalFrameScan,
    /// Test-only control arm: an image set to this drops its verified prefix before every scan,
    /// which reproduces the whole-image scan the prefix reuse replaced. It is instance state, so
    /// a control arm cannot change what another image — or another test running in parallel —
    /// scans: only the caller that owns the image decides, and `clear` keeps the setting.
    #[cfg(test)]
    whole_image: bool,
}

impl WalImage {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Applies a WAL batch, keeping the verified prefix when every operation lands beyond
    /// it. The bytes themselves are still written by `wal::apply_batch`, the single
    /// implementation of write and truncate semantics.
    pub(crate) fn apply(&mut self, batch: &WalBatch) -> Result<()> {
        let mut end = self.bytes.len();
        for operation in &batch.operations {
            match operation {
                WalOperation::Write { offset, bytes } => {
                    let offset = usize::try_from(*offset)?;
                    self.scan.note_write(offset);
                    let write_end = offset
                        .checked_add(bytes.len())
                        .context("WAL write overflow")?;
                    end = end.max(write_end);
                }
                WalOperation::Truncate { size } => {
                    let size = usize::try_from(*size)?;
                    self.scan.note_truncate(size);
                    end = end.max(size);
                }
            }
        }
        // Grow once for the whole batch, the same amortized policy `apply_batch` uses per
        // operation. One reserve per batch means at most one reallocation, so the copy it
        // may cause has a single attributable place; the old capacity is an upper bound
        // because the allocator is allowed to extend the buffer in place.
        let capacity = self.bytes.capacity();
        if end > self.bytes.len() {
            self.bytes.try_reserve(end - self.bytes.len())?;
            if self.bytes.capacity() != capacity {
                observed_copy(capacity);
            }
        }
        apply_batch(&mut self.bytes, batch)
    }

    pub(crate) fn resize(&mut self, size: usize) -> Result<()> {
        if size > self.bytes.len() {
            self.bytes.try_reserve(size - self.bytes.len())?;
        }
        self.scan.note_truncate(size);
        self.bytes.resize(size, 0);
        Ok(())
    }

    /// Drops the image: a verified prefix cannot outlive the bytes it described.
    pub(crate) fn clear(&mut self) {
        self.bytes.clear();
        self.scan.reset();
    }

    /// Test-only: moves this image to the control arm described on the field.
    #[cfg(test)]
    pub(crate) fn force_whole_image(&mut self) {
        self.whole_image = true;
    }

    /// Position of the last commit frame, verifying only the frames added since the
    /// previous call.
    pub(crate) fn committed_position(&mut self) -> Result<Option<WalPosition>> {
        #[cfg(test)]
        if self.whole_image {
            self.scan
                .reset_for(crate::work_metrics::ScanReset::ForcedWholeImage);
        }
        self.scan.position(&self.bytes)
    }
}

pub fn validate_index_commit(
    index_bytes: &[u8],
    namespace: &Namespace,
    load_object: impl FnMut(&str) -> Result<Vec<u8>> + Send,
) -> Result<Option<WalCommit>> {
    let load = Mutex::new(load_object);
    Ok(validate_index_state(
        index_bytes,
        namespace,
        |hash| {
            let key = namespace.key(&format!("wal/index-segments/{hash}"))?;
            let mut load = load
                .lock()
                .map_err(|_| anyhow::anyhow!("WAL object loader lock poisoned"))?;
            load(&key)
        },
        |key| {
            let mut load = load
                .lock()
                .map_err(|_| anyhow::anyhow!("WAL object loader lock poisoned"))?;
            load(key)
        },
        |hash| {
            let key = namespace.key(&format!("wal/receipt-pages/{hash}"))?;
            let mut load = load
                .lock()
                .map_err(|_| anyhow::anyhow!("WAL object loader lock poisoned"))?;
            load(&key)
        },
    )?
    .commit())
}

pub(crate) fn resolve_entries(
    index: &WalMetadata,
    mut load_segment: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<Vec<Entry>> {
    ensure!(
        index.entry_count.is_some() || index.entry_index.is_none(),
        "missing WAL entry count"
    );
    let entry_count = index.entry_count.unwrap_or(index.entries.len() as u64);
    ensure!(
        entry_count >= index.entries.len() as u64,
        "invalid WAL entry count"
    );
    ensure!(
        index.entry_index.is_some() || entry_count == index.entries.len() as u64,
        "missing WAL entry segments"
    );
    let mut segments = Vec::new();
    let mut root = index.entry_index.clone();
    let mut seen = 0u64;
    while let Some(hash) = root {
        ensure!(valid_entry_index(&hash), "invalid WAL index segment root");
        ensure!(
            seen < entry_count - index.entries.len() as u64,
            "WAL index segment chain exceeds entry count"
        );
        let bytes = load_segment(&hash)?;
        let segment = WalIndexSegment::decode(&hash, &bytes)?;
        crate::wal::validate_entry_sequence(&segment.entries, index.replay_baseline_lsn)?;
        seen = seen
            .checked_add(segment.entries.len() as u64)
            .context("WAL index segment count overflow")?;
        segments.push(segment.entries);
        root = segment.previous;
    }
    ensure!(
        seen.checked_add(index.entries.len() as u64) == Some(entry_count),
        "WAL entry count mismatch"
    );
    let mut entries = segments.into_iter().rev().flatten().collect::<Vec<_>>();
    entries.extend(index.entries.clone());
    validate_layout(index, &entries)?;
    Ok(entries)
}

#[derive(Clone, Copy, Debug, Default)]
struct ReceiptArchiveStats {
    entries: u64,
    /// Receipts above the replay baseline still need a retained record to answer a retry.
    retained: u64,
}

fn validate_archived_receipt(
    index: &WalMetadata,
    identity: &str,
    receipt: &ArchivedReceipt,
) -> Result<()> {
    ensure!(
        !identity.is_empty()
            && identity.len() <= 128
            && identity
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
        "invalid archived WAL receipt identity"
    );
    ensure!(
        !index.receipts.contains_key(identity),
        "WAL receipt archived and inline"
    );
    ensure!(
        receipt.lsn > 0 && receipt.lsn <= index.last_lsn,
        "archived WAL receipt outside the WAL"
    );
    if let Some(digest) = &receipt.digest {
        ensure!(
            crate::manifest::valid_hash(digest),
            "invalid archived WAL receipt digest"
        );
    }
    Ok(())
}

/// Validates every archived receipt without retaining the tree in memory.
fn walk_receipt_archive(
    index: &WalMetadata,
    load_page: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<ReceiptArchiveStats> {
    let Some(root) = index.receipt_index.as_deref() else {
        return Ok(ReceiptArchiveStats::default());
    };
    let mut stats = ReceiptArchiveStats::default();
    crate::authority::paged_index::walk_tree_with(Some(root), load_page, |entries| {
        for (identity, value) in entries {
            let receipt: ArchivedReceipt = serde_json::from_value(value.clone())?;
            validate_archived_receipt(index, identity, &receipt)?;
            stats.entries = stats
                .entries
                .checked_add(1)
                .context("WAL receipt archive count overflow")?;
            if receipt.lsn > index.replay_baseline_lsn {
                stats.retained = stats
                    .retained
                    .checked_add(1)
                    .context("WAL receipt archive count overflow")?;
            }
        }
        Ok(())
    })?;
    Ok(stats)
}

/// A read-through page store for replaying a receipt archive update without touching the
/// durable tree: old pages come from `load`, pages the replay rewrites stay in `staged`.
///
/// The write path is the same `PagedIndex::insert_many` the worker uses, so replaying the
/// removed inline receipts over the old root produces the exact content-addressed root the
/// publication must have. Only pages on the insertion paths are read or written; the rest of
/// the archive is never materialized.
struct ReceiptPageReplay<'a, Load> {
    prefix: String,
    load: Mutex<&'a mut Load>,
    staged: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl<'a, Load> ReceiptPageReplay<'a, Load>
where
    Load: FnMut(&str) -> Result<Vec<u8>> + Send,
{
    fn new(namespace: &Namespace, load: &'a mut Load) -> Self {
        Self {
            prefix: format!("{}wal/receipt-pages/", namespace.prefix()),
            load: Mutex::new(load),
            staged: Mutex::new(BTreeMap::new()),
        }
    }

    fn into_staged(self) -> BTreeMap<String, Vec<u8>> {
        self.staged
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<Load> ObjectStore for ReceiptPageReplay<'_, Load>
where
    Load: FnMut(&str) -> Result<Vec<u8>> + Send,
{
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        if let Some(bytes) = self
            .staged
            .lock()
            .map_err(|_| anyhow::anyhow!("receipt replay page lock poisoned"))?
            .get(key)
        {
            return Ok(Some(StoredObject {
                bytes: bytes.clone(),
                etag: blake3::hash(bytes).to_hex().to_string(),
            }));
        }
        let hash = key
            .strip_prefix(&self.prefix)
            .context("receipt replay read outside the archive")?;
        ensure!(
            valid_entry_index(hash),
            "receipt replay read has an invalid page identity"
        );
        let mut load = self
            .load
            .lock()
            .map_err(|_| anyhow::anyhow!("receipt replay page lock poisoned"))?;
        let bytes = (**load)(hash)?;
        Ok(Some(StoredObject {
            bytes,
            etag: hash.to_owned(),
        }))
    }

    fn put(&self, key: &str, bytes: &[u8], _: PutCondition) -> Result<String> {
        self.staged
            .lock()
            .map_err(|_| anyhow::anyhow!("receipt replay page lock poisoned"))?
            .insert(key.to_owned(), bytes.to_vec());
        Ok(blake3::hash(bytes).to_hex().to_string())
    }

    fn delete(&self, _: &str, _: Option<&str>) -> Result<()> {
        anyhow::bail!("receipt replay cannot delete pages")
    }

    fn list(&self, _: &str) -> Result<Vec<String>> {
        anyhow::bail!("receipt replay cannot list pages")
    }
}

/// Replays the removed inline receipts over the validated archive root.
///
/// Candidates use the same `(lsn, identity)` order and archive batch size as
/// `RemoteWal::roll_receipts`, so the expected root is deterministic. The returned staged
/// pages are the pages the publication must have written; callers verify them separately.
struct ArchiveReplay {
    root: Option<String>,
    staged: BTreeMap<String, Vec<u8>>,
}

fn replay_archive_extension<Load>(
    namespace: &Namespace,
    old_root: Option<&str>,
    removed: &BTreeSet<String>,
    old_receipts: &BTreeMap<String, u64>,
    old_digests: &BTreeMap<String, String>,
    load_page: &mut Load,
) -> Result<ArchiveReplay>
where
    Load: FnMut(&str) -> Result<Vec<u8>> + Send,
{
    let mut candidates = removed
        .iter()
        .map(|identity| {
            let lsn = old_receipts
                .get(identity)
                .copied()
                .context("WAL receipt is not inline")?;
            Ok((lsn, identity.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    candidates.sort();
    // See `validate_index_state`: the replay store's address may repeat, so it must not
    // share the outer request's page cache.
    let _reads = crate::authority::request_reads::Scope::new();
    let replay = ReceiptPageReplay::new(namespace, load_page);
    let root = {
        let pages = PagedIndex::with_prefix(&replay, namespace, WAL_RECEIPT_PAGE_PREFIX);
        let mut root = old_root.map(str::to_owned);
        for batch in candidates.chunks(WAL_RECEIPT_ARCHIVE_BATCH) {
            let mut updates = BTreeMap::new();
            for (lsn, identity) in batch {
                let digest = old_digests.get(identity).with_context(|| {
                    format!("cannot archive WAL receipt {identity} without a digest")
                })?;
                updates.insert(
                    identity.clone(),
                    serde_json::to_value(ArchivedReceipt {
                        lsn: *lsn,
                        digest: Some(digest.clone()),
                    })?,
                );
            }
            root = Some(pages.insert_many(root.as_deref(), updates)?);
        }
        root
    };
    Ok(ArchiveReplay {
        root,
        staged: replay.into_staged(),
    })
}

/// Proves a published index's receipt archive is fully present and well formed.
pub fn validate_receipt_index(
    index_bytes: &[u8],
    load_page: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<()> {
    let index: WalMetadata = serde_json::from_slice(index_bytes)?;
    walk_receipt_archive(&index, load_page).map(|_| ())
}

pub struct ValidatedWal {
    index: WalMetadata,
    entries: Vec<Entry>,
    image: WalImage,
    /// Root of the archive this state was validated against. The archive itself is immutable
    /// and content addressed, so a rolling append can replay its update from this root
    /// without retaining every receipt; an unchanged root uses it for path lookups only.
    archive_root: Option<String>,
    /// Number of receipts reachable from `archive_root`, kept as a scalar for the cold
    /// replay's record-evidence check. It does not grow the cache's per-receipt footprint.
    archive_entries: u64,
}

impl ValidatedWal {
    pub fn commit(&self) -> Option<WalCommit> {
        self.index.commit
    }

    #[cfg(test)]
    pub(crate) fn archive_root(&self) -> Option<&str> {
        self.archive_root.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn archive_entries(&self) -> u64 {
        self.archive_entries
    }
}

pub fn validate_index_state(
    index_bytes: &[u8],
    namespace: &Namespace,
    load_segment: impl FnMut(&str) -> Result<Vec<u8>>,
    mut load_chunk: impl FnMut(&str) -> Result<Vec<u8>>,
    mut load_page: impl FnMut(&str) -> Result<Vec<u8>> + Send,
) -> Result<ValidatedWal> {
    namespace.key("wal/index")?;
    let mut index: WalMetadata = serde_json::from_slice(index_bytes)?;
    index.normalize_entries()?;
    let entries = resolve_entries(&index, load_segment)?;
    let stats = walk_receipt_archive(&index, &mut load_page)?;
    let archive_root = index.receipt_index.clone();
    let mut image = WalImage::default();
    let mut inline_hits = 0u64;
    let mut archived_hits = 0u64;
    let mut last_record_commit = None;
    {
        // The replay store is a stack object, so its address can repeat across requests. A
        // fresh request-read scope keeps cached pages from an earlier adapter from answering
        // for this one; the scope is dropped before control returns to the caller.
        let _reads = crate::authority::request_reads::Scope::new();
        let replay = ReceiptPageReplay::new(namespace, &mut load_page);
        let pages = PagedIndex::with_prefix(&replay, namespace, WAL_RECEIPT_PAGE_PREFIX);
        for entry_index in 0..entries.len() {
            let (commit, append_id) = validate_record(
                &index,
                &entries,
                namespace,
                entry_index,
                &mut image,
                &mut load_chunk,
                |identity| {
                    let Some(value) = pages.get(archive_root.as_deref(), identity)? else {
                        return Ok(None);
                    };
                    let receipt: ArchivedReceipt = serde_json::from_value(value)?;
                    validate_archived_receipt(&index, identity, &receipt)?;
                    archived_hits = archived_hits
                        .checked_add(1)
                        .context("WAL receipt archive count overflow")?;
                    Ok(Some(receipt))
                },
            )?;
            last_record_commit = commit;
            if append_id
                .as_ref()
                .is_some_and(|identity| index.receipts.contains_key(identity))
            {
                inline_hits = inline_hits
                    .checked_add(1)
                    .context("WAL append receipt count overflow")?;
            }
        }
    }
    let inline_retained = index
        .receipts
        .values()
        .filter(|lsn| **lsn > index.replay_baseline_lsn)
        .count() as u64;
    ensure!(
        inline_retained == inline_hits,
        "inline WAL receipt has no retained record"
    );
    ensure!(
        stats.retained == archived_hits,
        "archived WAL receipt has no retained record"
    );
    ensure!(
        last_record_commit == index.commit,
        "index and final record commit disagree"
    );
    Ok(ValidatedWal {
        index,
        entries,
        image,
        archive_root,
        archive_entries: stats.entries,
    })
}

/// Extends an accepted image by one record, consuming the accepted state.
///
/// The caller moves the state in, so a rejected or discarded append cannot leave a cached
/// image describing bytes that were never published: the only copy of that state is dropped
/// with the error and the next append re-derives it from the store. Extending in place is
/// what keeps an append from copying the whole image; the scan still pays only for the frames
/// the record adds, and a record that rewrites bytes inside the prefix resets it first.
pub fn validate_index_append(
    previous: ValidatedWal,
    index_bytes: &[u8],
    namespace: &Namespace,
    load_segment: impl FnMut(&str) -> Result<Vec<u8>>,
    mut load_chunk: impl FnMut(&str) -> Result<Vec<u8>>,
    mut load_page: impl FnMut(&str) -> Result<Vec<u8>> + Send,
) -> Result<ValidatedWal> {
    namespace.key("wal/index")?;
    let mut index: WalMetadata = serde_json::from_slice(index_bytes)?;
    index.normalize_entries()?;
    let ValidatedWal {
        index: old,
        mut entries,
        mut image,
        archive_root: old_archive_root,
        archive_entries: old_archive_entries,
    } = previous;
    let mut load_segment = load_segment;
    ensure!(
        index.epoch >= old.epoch
            && index.last_lsn == old.last_lsn.checked_add(1).context("WAL LSN exhausted")?
            && index.replay_baseline_lsn == old.replay_baseline_lsn
            && index.entry_count
                == Some(
                    (entries.len() as u64)
                        .checked_add(1)
                        .context("WAL entry count exhausted")?
                ),
        "WAL append changed validated index prefix"
    );
    let rolled = index.receipt_index != old.receipt_index;
    let mut removed = std::collections::BTreeSet::new();
    for (identity, lsn) in &old.receipts {
        match index.receipts.get(identity) {
            Some(current) => {
                ensure!(
                    current == lsn
                        && index.receipt_digests.get(identity) == old.receipt_digests.get(identity),
                    "WAL append changed a retained receipt"
                );
            }
            None => {
                removed.insert(identity.clone());
            }
        }
    }
    ensure!(
        rolled || removed.is_empty(),
        "WAL append dropped a receipt without rolling the archive"
    );
    ensure!(
        !rolled || !removed.is_empty(),
        "WAL append changed the archive root without archiving"
    );
    let (archive_root, archive_entries) = if rolled {
        let replay = replay_archive_extension(
            namespace,
            old_archive_root.as_deref(),
            &removed,
            &old.receipts,
            &old.receipt_digests,
            &mut load_page,
        )?;
        ensure!(
            replay.root == index.receipt_index,
            "WAL append archive is not an exact extension of the validated archive"
        );
        for (key, expected) in replay.staged {
            let hash = key
                .rsplit('/')
                .next()
                .context("invalid staged WAL receipt page key")?;
            ensure!(
                load_page(hash)? == expected,
                "WAL append archive page missing or changed"
            );
        }
        (
            index.receipt_index.clone(),
            old_archive_entries
                .checked_add(removed.len() as u64)
                .context("WAL receipt archive count overflow")?,
        )
    } else {
        // Root equality is a content-addressed identity: a page reached from this root cannot
        // change without changing the root. The hot path only reads the pages on each new
        // identity's lookup path, and every page read verifies its digest. An out-of-band
        // replacement of an unread sibling page is outside this hot-path trust boundary; the
        // next cold validation re-walks the tree and rejects it.
        if old_archive_root.is_some() {
            let _reads = crate::authority::request_reads::Scope::new();
            let replay = ReceiptPageReplay::new(namespace, &mut load_page);
            let pages = PagedIndex::with_prefix(&replay, namespace, WAL_RECEIPT_PAGE_PREFIX);
            for identity in index.receipts.keys() {
                if !old.receipts.contains_key(identity) {
                    ensure!(
                        pages.get(old_archive_root.as_deref(), identity)?.is_none(),
                        "WAL append reintroduced an archived receipt inline"
                    );
                }
            }
        }
        (old_archive_root.clone(), old_archive_entries)
    };
    // 旧内联格式与截断后的 baseline 可能超过单段上限；仅转换时允许冷校验，
    // 仍须证明原有 prefix 与 receipts 未改变，不能将任意新索引当作追加。
    if old.entries.len() > WAL_INDEX_SEGMENT_LIMIT {
        let cold = validate_index_state(
            index_bytes,
            namespace,
            load_segment,
            &mut load_chunk,
            &mut load_page,
        )?;
        ensure!(
            cold.entries[..entries.len()] == entries,
            "WAL append changed validated entry prefix"
        );
        load_record(
            &cold.index,
            &cold.entries,
            namespace,
            entries.len(),
            &mut load_chunk,
            |_| Ok(None),
            true,
        )?;
        return Ok(cold);
    }
    if index.entry_index == old.entry_index {
        ensure!(
            index.entries.len() == old.entries.len() + 1
                && index.entries[..old.entries.len()] == old.entries,
            "WAL append changed validated entry prefix"
        );
    } else {
        let hash = index
            .entry_index
            .as_deref()
            .context("WAL append removed segment root")?;
        let segment = WalIndexSegment::decode(hash, &load_segment(hash)?)?;
        ensure!(
            segment.previous == old.entry_index
                && segment.entries == old.entries
                && index.entries.len() == 1,
            "WAL append changed flushed entry prefix"
        );
    }
    entries.push(
        index
            .entries
            .last()
            .context("WAL append missing entry")?
            .clone(),
    );
    let record = load_record(
        &index,
        &entries,
        namespace,
        entries.len() - 1,
        &mut load_chunk,
        |_| Ok(None),
        true,
    )?;
    for (identity, lsn) in &index.receipts {
        if old.receipts.get(identity) != Some(lsn) {
            ensure!(
                identity == &record.append_id && *lsn == record.lsn,
                "WAL append introduced an unexpected receipt"
            );
        }
    }
    let commit = apply_record(&record, &mut image, false)?;
    ensure!(
        commit == index.commit,
        "index and final record commit disagree"
    );
    Ok(ValidatedWal {
        index,
        entries,
        image,
        archive_root,
        archive_entries,
    })
}

fn validate_layout(index: &WalMetadata, entries: &[Entry]) -> Result<()> {
    crate::wal::validate_entry_sequence(entries, index.replay_baseline_lsn)?;
    ensure!(
        index.entries.last().map_or(0, |entry| entry.lsn) == index.last_lsn,
        "incomplete WAL tail"
    );
    ensure!(
        index.entry_count.unwrap_or(entries.len() as u64) == entries.len() as u64,
        "WAL entry count mismatch"
    );
    ensure!(
        index.entry_index.as_deref().is_none_or(valid_entry_index),
        "invalid WAL index segment root"
    );
    ensure!(
        index.entry_count.unwrap_or(index.entries.len() as u64) >= index.entries.len() as u64
            && (index.entry_index.is_some()
                || index.entry_count.unwrap_or(index.entries.len() as u64)
                    == index.entries.len() as u64),
        "invalid WAL index tail"
    );
    ensure!(
        entries.last().map_or(0, |entry| entry.lsn) == index.last_lsn,
        "incomplete WAL index"
    );
    ensure!(
        entries.windows(2).all(|pair| {
            pair[0].lsn.checked_add(1) == Some(pair[1].lsn)
                || (index.replay_baseline_lsn > 0
                    && pair[0].lsn == pair[1].lsn
                    && pair[0].lsn == index.replay_baseline_lsn)
        }),
        "noncontiguous WAL index"
    );
    let first_lsn = entries.first().map_or(0, |entry| entry.lsn);
    ensure!(
        if index.last_lsn == 0 {
            index.entries.is_empty() && index.replay_baseline_lsn == 0
        } else if index.replay_baseline_lsn == 0 {
            first_lsn == 1
        } else {
            first_lsn == index.replay_baseline_lsn
        },
        "invalid WAL replay baseline"
    );
    ensure!(
        index
            .receipts
            .values()
            .all(|lsn| *lsn > 0 && *lsn <= index.last_lsn),
        "invalid WAL append receipt"
    );
    ensure!(
        index
            .receipt_digests
            .keys()
            .all(|identity| index.receipts.contains_key(identity)),
        "orphan WAL receipt digest"
    );
    ensure!(
        index
            .receipt_index
            .as_deref()
            .is_none_or(crate::manifest::valid_hash),
        "invalid WAL receipt archive root"
    );
    if let Some(commit) = index.commit {
        ensure!(
            commit.lsn > 0 && commit.lsn == index.last_lsn,
            "index commit must describe the final LSN"
        );
    }
    Ok(())
}

fn validate_record(
    index: &WalMetadata,
    entries: &[Entry],
    namespace: &Namespace,
    entry_index: usize,
    image: &mut WalImage,
    load_chunk: &mut impl FnMut(&str) -> Result<Vec<u8>>,
    archived: impl FnMut(&str) -> Result<Option<ArchivedReceipt>>,
) -> Result<(Option<WalCommit>, Option<String>)> {
    let record = load_record(
        index,
        entries,
        namespace,
        entry_index,
        load_chunk,
        archived,
        false,
    )?;
    let append_id = if index.replay_baseline_lsn > 0 && record.lsn == index.replay_baseline_lsn {
        None
    } else {
        Some(record.append_id.clone())
    };
    let baseline_continuation = entry_index > 0
        && index.replay_baseline_lsn > 0
        && entries[entry_index].lsn == index.replay_baseline_lsn;
    let commit = apply_record(&record, image, baseline_continuation)?;
    Ok((commit, append_id))
}

/// Loads the record an index entry names and checks its identity, receipt and replay
/// baseline, leaving the replay image untouched: a caller that extends a validated image
/// decides from the loaded batch whether it may reuse that image's verified frames.
fn load_record(
    index: &WalMetadata,
    entries: &[Entry],
    namespace: &Namespace,
    entry_index: usize,
    load_chunk: &mut impl FnMut(&str) -> Result<Vec<u8>>,
    mut archived: impl FnMut(&str) -> Result<Option<ArchivedReceipt>>,
    require_digest: bool,
) -> Result<WalRecord> {
    let entry = &entries[entry_index];
    let chunk = load_chunk(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?;
    ensure!(
        blake3::hash(&chunk).to_hex().as_str() == entry.hash,
        "WAL chunk checksum mismatch"
    );
    let record: WalRecord = serde_json::from_slice(&chunk)?;
    ensure!(
        record.namespace == *namespace && record.lsn == entry.lsn && record.epoch <= index.epoch,
        "WAL record identity mismatch"
    );
    // Every part of a split baseline shares the baseline LSN. The first part resets the image;
    // a later part must only extend it, or replay would silently drop the bytes before it.
    let baseline = index.replay_baseline_lsn > 0 && entry.lsn == index.replay_baseline_lsn;
    if baseline {
        if entry_index == 0 {
            ensure!(
                matches!(
                    record.batch.operations.first(),
                    Some(WalOperation::Truncate { size: 0 })
                ),
                "WAL baseline must reset the replay image"
            );
        } else {
            ensure!(
                !record
                    .batch
                    .operations
                    .iter()
                    .any(|operation| matches!(operation, WalOperation::Truncate { .. })),
                "WAL baseline continuation must not reset the replay image"
            );
        }
    } else {
        let inline = index.receipts.get(&record.append_id);
        let archived = if inline.is_none() {
            archived(&record.append_id)?
        } else {
            None
        };
        ensure!(
            inline.is_none() || archived.is_none(),
            "WAL receipt stored inline and archived"
        );
        if let Some(lsn) = inline {
            ensure!(*lsn == record.lsn, "WAL receipt LSN mismatch");
            if require_digest {
                ensure!(
                    index.receipt_digests.contains_key(&record.append_id),
                    "missing new WAL append digest"
                );
            }
            if let Some(digest) = index.receipt_digests.get(&record.append_id) {
                ensure!(
                    digest
                        == blake3::hash(&serde_json::to_vec(&record)?)
                            .to_hex()
                            .as_str(),
                    "WAL receipt request digest mismatch"
                );
            }
        } else if let Some(archived) = archived {
            ensure!(
                archived.lsn == record.lsn,
                "archived WAL receipt LSN mismatch"
            );
            let digest = archived
                .digest
                .as_deref()
                .context("missing archived WAL receipt digest; refusing replay")?;
            ensure!(
                digest
                    == blake3::hash(&serde_json::to_vec(&record)?)
                        .to_hex()
                        .as_str(),
                "WAL receipt request digest mismatch"
            );
        } else {
            anyhow::bail!("missing WAL append receipt");
        }
    }
    Ok(record)
}

fn apply_record(
    record: &WalRecord,
    image: &mut WalImage,
    baseline_continuation: bool,
) -> Result<Option<WalCommit>> {
    image.apply(&record.batch)?;
    if let Some(commit) = record.commit {
        ensure!(
            commit.lsn == record.lsn && commit.position.frame_count > 0,
            "invalid record commit LSN"
        );
        ensure!(
            image.committed_position()? == Some(commit.position),
            "commit does not match replayed WAL boundary"
        );
        // A continued baseline writes only the tail of the replay image, so the commit frame
        // may live in an earlier part. Its identity is still proven by the record LSN and by
        // the replayed boundary; every other record must carry the frame itself.
        ensure!(
            baseline_continuation
                || batch_covers_position(&record.batch, image.bytes(), commit.position)?,
            "commit frame was not written by this record"
        );
    }
    Ok(record.commit)
}

pub(crate) fn batch_covers_position(
    batch: &WalBatch,
    bytes: &[u8],
    position: WalPosition,
) -> Result<bool> {
    ensure!(
        bytes.len() >= 32 && position.frame_count > 0,
        "missing WAL commit frame"
    );
    let frame_size = u64::from(word(bytes, 8)) + 24;
    let start = position
        .frame_count
        .checked_sub(1)
        .and_then(|count| count.checked_mul(frame_size))
        .and_then(|offset| offset.checked_add(32))
        .context("WAL position overflow")?;
    let end = start
        .checked_add(frame_size)
        .context("WAL frame end overflow")?;
    let mut ranges = Vec::new();
    for operation in &batch.operations {
        if let WalOperation::Write { offset, bytes } = operation {
            ranges.push((
                *offset,
                offset
                    .checked_add(bytes.len() as u64)
                    .context("WAL operation overflow")?,
            ));
        }
    }
    ranges.sort_unstable();
    let mut covered = start;
    for (range_start, range_end) in ranges {
        if range_start > covered {
            break;
        }
        covered = covered.max(range_end);
        if covered >= end {
            return Ok(true);
        }
    }
    Ok(false)
}

fn word(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("validated WAL field"),
    )
}

fn checksum(bytes: &[u8], mut rolling: [u32; 2], big_endian: bool) -> [u32; 2] {
    for pair in bytes.as_chunks::<8>().0 {
        let first: [u8; 4] = pair[..4].try_into().expect("checksum word");
        let second: [u8; 4] = pair[4..].try_into().expect("checksum word");
        let first = if big_endian {
            u32::from_be_bytes(first)
        } else {
            u32::from_le_bytes(first)
        };
        let second = if big_endian {
            u32::from_be_bytes(second)
        } else {
            u32::from_le_bytes(second)
        };
        rolling[0] = rolling[0].wrapping_add(first).wrapping_add(rolling[1]);
        rolling[1] = rolling[1].wrapping_add(second).wrapping_add(rolling[0]);
    }
    rolling
}
