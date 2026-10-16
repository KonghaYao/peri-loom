use anyhow::{ensure, Context, Result};
use std::time::{Duration, Instant};

use crate::manifest::WalPosition;
use crate::namespace::Namespace;
use crate::wal::{apply_batch, WalBatch, WalCommit, WalMetadata, WalOperation, WalRecord};

const WAL_HEADER: usize = 32;
const WAL_FORMAT: u32 = 3_007_000;
const WAL_MAGIC_LITTLE_ENDIAN: u32 = 0x377f_0682;
const WAL_MAGIC_BIG_ENDIAN: u32 = 0x377f_0683;

/// Work the recovery path does that the `wal_durability` timer deliberately excludes: the
/// frame scans it runs before and after each append, and the image copies a validated WAL
/// extension pays. Kept separately observable so a review can tell a removed growth term
/// from one that merely moved out of the timer.
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
mod fixture;
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
        for operation in &batch.operations {
            match operation {
                WalOperation::Write { offset, .. } => {
                    self.scan.note_write(usize::try_from(*offset)?);
                }
                WalOperation::Truncate { size } => {
                    self.scan.note_truncate(usize::try_from(*size)?);
                }
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
    load_chunk: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<Option<WalCommit>> {
    Ok(validate_index_state(index_bytes, namespace, load_chunk)?.commit())
}

pub struct ValidatedWal {
    index: WalMetadata,
    image: WalImage,
}

impl ValidatedWal {
    pub fn commit(&self) -> Option<WalCommit> {
        self.index.commit
    }
}

pub fn validate_index_state(
    index_bytes: &[u8],
    namespace: &Namespace,
    mut load_chunk: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<ValidatedWal> {
    namespace.key("wal/index")?;
    let index: WalMetadata = serde_json::from_slice(index_bytes)?;
    validate_layout(&index)?;
    let mut image = WalImage::default();
    let mut last_record_commit = None;
    for entry_index in 0..index.entries.len() {
        last_record_commit = validate_record(
            &index,
            namespace,
            entry_index,
            &mut image,
            &mut load_chunk,
            false,
        )?;
    }
    ensure!(
        last_record_commit == index.commit,
        "index and final record commit disagree"
    );
    Ok(ValidatedWal { index, image })
}

pub fn validate_index_append(
    previous: &ValidatedWal,
    index_bytes: &[u8],
    namespace: &Namespace,
    mut load_chunk: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<ValidatedWal> {
    namespace.key("wal/index")?;
    let index: WalMetadata = serde_json::from_slice(index_bytes)?;
    validate_layout(&index)?;
    let old = &previous.index;
    ensure!(
        index.epoch >= old.epoch
            && index.last_lsn == old.last_lsn.checked_add(1).context("WAL LSN exhausted")?
            && index.replay_baseline_lsn == old.replay_baseline_lsn
            && index.entries.len() == old.entries.len() + 1
            && index.entries[..old.entries.len()]
                .iter()
                .zip(&old.entries)
                .all(|(next, previous)| next.lsn == previous.lsn && next.hash == previous.hash)
            && index.receipts.len() == old.receipts.len() + 1
            && old
                .receipts
                .iter()
                .all(|(id, lsn)| index.receipts.get(id) == Some(lsn))
            && index.receipt_digests.len() == old.receipt_digests.len() + 1
            && old
                .receipt_digests
                .iter()
                .all(|(id, digest)| index.receipt_digests.get(id) == Some(digest)),
        "WAL append changed validated index prefix or receipts"
    );
    let record = load_record(&index, namespace, old.entries.len(), &mut load_chunk, true)?;
    // The candidate extends the accepted image, so it starts as a copy: an append that is
    // rejected must leave the cached state describing exactly the accepted bytes. The scan
    // is what makes the extension cheap: only the frames this record adds are verified.
    let mut image = previous.image.clone();
    observed_copy(image.len());
    let commit = apply_record(&record, &mut image)?;
    ensure!(
        commit == index.commit,
        "index and final record commit disagree"
    );
    Ok(ValidatedWal { index, image })
}

fn validate_layout(index: &WalMetadata) -> Result<()> {
    ensure!(
        index.entries.last().map_or(0, |entry| entry.lsn) == index.last_lsn,
        "incomplete WAL index"
    );
    ensure!(
        index
            .entries
            .windows(2)
            .all(|pair| pair[0].lsn.checked_add(1) == Some(pair[1].lsn)),
        "noncontiguous WAL index"
    );
    let first_lsn = index.entries.first().map_or(0, |entry| entry.lsn);
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
    namespace: &Namespace,
    entry_index: usize,
    image: &mut WalImage,
    load_chunk: &mut impl FnMut(&str) -> Result<Vec<u8>>,
    require_digest: bool,
) -> Result<Option<WalCommit>> {
    let record = load_record(index, namespace, entry_index, load_chunk, require_digest)?;
    apply_record(&record, image)
}

/// Loads the record an index entry names and checks its identity, receipt and replay
/// baseline, leaving the replay image untouched: a caller that extends a validated image
/// decides from the loaded batch whether it may reuse that image's verified frames.
fn load_record(
    index: &WalMetadata,
    namespace: &Namespace,
    entry_index: usize,
    load_chunk: &mut impl FnMut(&str) -> Result<Vec<u8>>,
    require_digest: bool,
) -> Result<WalRecord> {
    let entry = &index.entries[entry_index];
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
    let baseline = entry_index == 0 && index.replay_baseline_lsn > 0;
    if baseline {
        ensure!(
            matches!(
                record.batch.operations.first(),
                Some(WalOperation::Truncate { size: 0 })
            ),
            "WAL baseline must reset the replay image"
        );
    } else {
        ensure!(
            index.receipts.get(&record.append_id) == Some(&record.lsn),
            "missing WAL append receipt"
        );
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
    }
    Ok(record)
}

fn apply_record(record: &WalRecord, image: &mut WalImage) -> Result<Option<WalCommit>> {
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
        ensure!(
            batch_covers_position(&record.batch, image.bytes(), commit.position)?,
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
