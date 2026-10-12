use anyhow::{ensure, Context, Result};

use crate::manifest::WalPosition;
use crate::namespace::Namespace;
use crate::wal::{apply_batch, WalBatch, WalCommit, WalMetadata, WalOperation, WalRecord};

pub fn committed_position(bytes: &[u8]) -> Result<Option<WalPosition>> {
    if bytes.len() < 32 {
        return Ok(None);
    }
    let magic = word(bytes, 0);
    ensure!(
        matches!(magic, 0x377f0682 | 0x377f0683),
        "invalid WAL magic"
    );
    ensure!(word(bytes, 4) == 3007000, "unsupported WAL format");
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
    if !(bytes.len() - 32).is_multiple_of(frame_size) {
        return Ok(None);
    }
    let salts = [word(bytes, 16), word(bytes, 20)];
    let mut position = None;
    for (frame_index, frame) in bytes[32..].chunks_exact(frame_size).enumerate() {
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

pub fn validate_index_commit(
    index_bytes: &[u8],
    namespace: &Namespace,
    mut load_chunk: impl FnMut(&str) -> Result<Vec<u8>>,
) -> Result<Option<WalCommit>> {
    namespace.key("wal/index")?;
    let index: WalMetadata = serde_json::from_slice(index_bytes)?;
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
    let mut bytes = Vec::new();
    let mut last_record_commit = None;
    for (entry_index, entry) in index.entries.iter().enumerate() {
        let chunk = load_chunk(&namespace.key(&format!("wal/chunks/{}", entry.hash))?)?;
        ensure!(
            blake3::hash(&chunk).to_hex().as_str() == entry.hash,
            "WAL chunk checksum mismatch"
        );
        let record: WalRecord = serde_json::from_slice(&chunk)?;
        ensure!(
            record.namespace == *namespace
                && record.lsn == entry.lsn
                && record.epoch <= index.epoch,
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
        apply_batch(&mut bytes, &record.batch)?;
        if let Some(commit) = record.commit {
            ensure!(
                commit.lsn == record.lsn && commit.position.frame_count > 0,
                "invalid record commit LSN"
            );
            ensure!(
                committed_position(&bytes)? == Some(commit.position),
                "commit does not match replayed WAL boundary"
            );
            ensure!(
                batch_covers_position(&record.batch, &bytes, commit.position)?,
                "commit frame was not written by this record"
            );
        }
        last_record_commit = record.commit;
    }
    ensure!(
        last_record_commit == index.commit,
        "index and final record commit disagree"
    );
    Ok(index.commit)
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
