use super::*;
use crate::recovery::fixture::{started, truncate, write_at, WalBuilder, FRAME_SIZE, PAGE_SIZE};

/// Same comparison for images that are expected to be invalid: a header the caller
/// clobbered must fail identically on both paths instead of only on one.
fn assert_same_outcome(image: &mut WalImage) {
    let incremental = image.committed_position();
    let full = committed_position(image.bytes());
    match (incremental, full) {
        (Err(left), Err(right)) => {
            assert_eq!(left.to_string(), right.to_string(), "scan errors disagree")
        }
        (Ok(left), Ok(right)) => {
            assert_eq!(left, right, "incremental scan disagrees with full scan")
        }
        (left, right) => {
            panic!("incremental scan validity disagrees: {left:?} vs {right:?}")
        }
    }
}

/// The contract every incremental result must keep: same outcome as the full scan of the
/// same bytes.
fn assert_matches_full_scan(image: &mut WalImage) -> Result<Option<WalPosition>> {
    let incremental = image.committed_position();
    let full = committed_position(image.bytes());
    match (&incremental, &full) {
        (Err(left), Err(right)) => {
            assert_eq!(left.to_string(), right.to_string(), "scan errors disagree")
        }
        (Ok(_), Ok(_)) => {}
        _ => panic!("incremental scan validity disagrees: {incremental:?} vs {full:?}"),
    }
    let incremental = incremental?;
    assert_eq!(
        incremental, full?,
        "incremental scan disagrees with full scan"
    );
    Ok(incremental)
}

#[test]
fn frames_without_a_commit_have_no_position_and_a_commit_has_one() -> Result<()> {
    let mut builder = WalBuilder::new([11, 12]);
    let mut image = started(&builder)?;
    for page in 1..=4 {
        let frame = builder.frame(page, 0);
        image.apply(&write_at(image.len(), &frame))?;
    }
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    let frame = builder.frame(5, PAGE_SIZE as u32);
    let offset = image.len();
    image.apply(&write_at(offset, &frame))?;
    assert_eq!(
        assert_matches_full_scan(&mut image)?,
        Some(WalPosition {
            salts: [11, 12],
            frame_count: 5
        })
    );
    Ok(())
}

#[test]
fn partial_trailing_frame_reports_no_position() -> Result<()> {
    let mut builder = WalBuilder::new([2, 3]);
    let mut image = started(&builder)?;
    let commit = builder.frame(1, PAGE_SIZE as u32);
    let offset = image.len();
    image.apply(&write_at(offset, &commit))?;
    assert!(assert_matches_full_scan(&mut image)?.is_some());

    // A half-written frame must not be mistaken for a committed boundary even though a
    // commit frame precedes it.
    let next = builder.frame(2, 0);
    image.apply(&write_at(image.len(), &next[..FRAME_SIZE / 2]))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    // Completing a frame that does not commit leaves the image without a committed
    // boundary, exactly as the whole-image scan reports it.
    let rest = next[FRAME_SIZE / 2..].to_vec();
    let offset = image.len();
    image.apply(&write_at(offset, &rest))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    let tail = builder.frame(3, PAGE_SIZE as u32);
    let offset = image.len();
    image.apply(&write_at(offset, &tail))?;
    assert!(assert_matches_full_scan(&mut image)?.is_some());
    Ok(())
}

#[test]
fn appends_verify_only_the_frames_they_add() -> Result<()> {
    let mut builder = WalBuilder::new([5, 6]);
    let mut image = started(&builder)?;
    for page in 1..=64 {
        let frame = builder.frame(page, if page == 64 { PAGE_SIZE as u32 } else { 0 });
        image.apply(&write_at(image.len(), &frame))?;
        image.committed_position()?;
    }
    let image_len = image.len();
    assert_eq!(image_len, WAL_HEADER + 64 * FRAME_SIZE);

    let frame = builder.frame(65, PAGE_SIZE as u32);
    let offset = image.len();
    image.apply(&write_at(offset, &frame))?;
    reset_recovery_observations();
    assert!(image.committed_position()?.is_some());
    let scanned = recovery_observations().scanned_bytes;
    assert!(
        scanned <= (WAL_HEADER + FRAME_SIZE) as u64,
        "one append scanned {scanned} bytes of a {image_len}-byte image"
    );
    Ok(())
}

#[test]
fn rewriting_a_verified_frame_revalidates_the_image() -> Result<()> {
    let mut builder = WalBuilder::new([8, 9]);
    let mut image = started(&builder)?;
    let mut frames = Vec::new();
    for page in 1..=4 {
        let frame = builder.frame(page, if page == 4 { PAGE_SIZE as u32 } else { 0 });
        frames.push(frame.clone());
        image.apply(&write_at(image.len(), &frame))?;
    }
    assert!(assert_matches_full_scan(&mut image)?.is_some());

    // Corrupt the payload of an already verified frame in place: the checksum chain must be
    // rebuilt from the header, and the full scan agrees the image no longer commits.
    let offset = WAL_HEADER + FRAME_SIZE + 24;
    let mut corrupted = image.bytes()[offset..offset + 8].to_vec();
    corrupted[0] ^= 0xff;
    image.apply(&write_at(offset, &corrupted))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    // Restoring the frame restores the position, again in agreement with the full scan.
    let original = frames[1][24..32].to_vec();
    image.apply(&write_at(offset, &original))?;
    assert!(assert_matches_full_scan(&mut image)?.is_some());
    Ok(())
}

#[test]
fn truncated_or_replaced_headers_invalidate_the_prefix() -> Result<()> {
    let mut builder = WalBuilder::new([21, 22]);
    let mut image = started(&builder)?;
    for page in 1..=3 {
        let frame = builder.frame(page, PAGE_SIZE as u32);
        image.apply(&write_at(image.len(), &frame))?;
    }
    assert!(assert_matches_full_scan(&mut image)?.is_some());

    // Shrinking below the examined prefix drops it: the reference scan of the shorter image
    // is the authority again.
    let shorter = image.len() - FRAME_SIZE;
    image.apply(&truncate(shorter))?;
    assert!(assert_matches_full_scan(&mut image)?.is_some());

    // A new header (fresh salts) is a restart, not an append.
    let mut restarted = WalBuilder::new([31, 32]);
    let restart = restarted.header();
    let frame = restarted.frame(1, PAGE_SIZE as u32);
    image.apply(&truncate(0))?;
    image.apply(&write_at(0, &restart))?;
    let offset = image.len();
    image.apply(&write_at(offset, &frame))?;
    assert_eq!(
        assert_matches_full_scan(&mut image)?,
        Some(WalPosition {
            salts: [31, 32],
            frame_count: 1
        })
    );

    // A clobbered header must fail identically on both paths, not only on one of them.
    let garbage = vec![0x7f; WAL_HEADER];
    image.apply(&write_at(0, &garbage))?;
    assert_same_outcome(&mut image);
    Ok(())
}

#[test]
fn corrupted_frames_stop_the_scan_until_their_bytes_change() -> Result<()> {
    let mut builder = WalBuilder::new([41, 42]);
    let mut image = started(&builder)?;
    let first = builder.frame(1, 0);
    let offset = image.len();
    image.apply(&write_at(offset, &first))?;
    let halt_offset = image.len();
    let pristine = builder.frame(2, 0);
    let mut halt = pristine.clone();
    halt[16] ^= 0xff;
    image.apply(&write_at(halt_offset, &halt))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    // Appending behind a stopped scan cannot change the answer, and the full scan agrees.
    let tail = builder.frame(3, PAGE_SIZE as u32);
    image.apply(&write_at(image.len(), &tail))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    // Replacing the frame the scan stopped on resumes it: the pristine bytes are the ones
    // the chain after the first frame expects.
    image.apply(&write_at(halt_offset, &pristine))?;
    assert!(assert_matches_full_scan(&mut image)?.is_some());

    // Growing the image with zero fill is an append of unusable bytes, which both scans
    // report the same way.
    image.apply(&truncate(image.len() + FRAME_SIZE))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);
    Ok(())
}

#[test]
fn incremental_scan_tracks_full_scan_over_mixed_mutations() -> Result<()> {
    let mut builder = WalBuilder::new([61, 62]);
    let mut image = started(&builder)?;
    let mut pending = Vec::new();
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for step in 0..200u32 {
        match next() % 5 {
            0 | 1 => {
                pending = builder.frame(step + 1, if step % 7 == 0 { PAGE_SIZE as u32 } else { 0 });
            }
            2 => {
                // Deliver the pending frame in pieces, so the image sees partial frame
                // tails between complete ones.
                if !pending.is_empty() {
                    let length = 1 + next() as usize % pending.len();
                    let slice = pending[..length].to_vec();
                    pending.drain(..length);
                    let offset = image.len();
                    image.apply(&write_at(offset, &slice))?;
                }
            }
            3 => {
                // Rewrite bytes already inside the frames, which is what a restarted write
                // stream or a checkpoint does. The header is left alone: replacing it is
                // covered separately, and a clobbered header would make every later
                // comparison a trivial equality of two errors.
                if image.len() > WAL_HEADER {
                    let at = WAL_HEADER + next() as usize % (image.len() - WAL_HEADER);
                    let length = 1 + next() as usize % (image.len() - at).min(16);
                    let fill = vec![(next() & 0xff) as u8; length];
                    image.apply(&write_at(at, &fill))?;
                    pending.clear();
                }
            }
            _ => {
                if !pending.is_empty() {
                    let slice = pending.clone();
                    pending.clear();
                    let offset = image.len();
                    image.apply(&write_at(offset, &slice))?;
                }
            }
        }
        if image.len() >= WAL_HEADER {
            assert_matches_full_scan(&mut image)?;
        }
    }
    Ok(())
}

#[test]
fn clearing_or_growing_the_image_keeps_the_reference_agreement() -> Result<()> {
    let mut builder = WalBuilder::new([81, 82]);
    let mut image = started(&builder)?;
    for page in 1..=3 {
        let frame = builder.frame(page, PAGE_SIZE as u32);
        image.apply(&write_at(image.len(), &frame))?;
    }
    assert!(assert_matches_full_scan(&mut image)?.is_some());

    // Growing with zero fill appends bytes that cannot be a frame, which both paths report.
    image.apply(&truncate(image.len() + FRAME_SIZE))?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    // Clearing drops the image and its verified prefix together, so the next scan starts
    // from nothing rather than from a prefix describing bytes that are gone.
    image.clear();
    assert_eq!(image.len(), 0);
    assert_eq!(assert_matches_full_scan(&mut image)?, None);

    // A rebuilt image starts a fresh chain, so it must use frames the builder produced
    // from a fresh chain as well.
    let mut rebuilt = WalBuilder::new([81, 82]);
    let header = rebuilt.header();
    image.apply(&write_at(0, &header))?;
    let frame = rebuilt.frame(1, PAGE_SIZE as u32);
    let offset = image.len();
    image.apply(&write_at(offset, &frame))?;
    assert!(assert_matches_full_scan(&mut image)?.is_some());
    Ok(())
}

/// The same mutation sequence must agree with the reference in both arms, because the
/// benchmark's baseline is only a control if it stays correct.
#[test]
fn the_whole_image_arm_matches_the_reference_over_the_same_mutations() -> Result<()> {
    for incremental in [true, false] {
        INCREMENTAL_SCAN.store(incremental, std::sync::atomic::Ordering::Relaxed);
        let mut builder = WalBuilder::new([72, 73]);
        let mut image = started(&builder)?;
        for page in 1..=8 {
            let frame = builder.frame(page, if page % 3 == 0 { PAGE_SIZE as u32 } else { 0 });
            image.apply(&write_at(image.len(), &frame))?;
            assert_matches_full_scan(&mut image)?;
        }
        let offset = WAL_HEADER + FRAME_SIZE + 24;
        let mut corrupted = image.bytes()[offset..offset + 8].to_vec();
        corrupted[0] ^= 0xff;
        image.apply(&write_at(offset, &corrupted))?;
        assert_eq!(assert_matches_full_scan(&mut image)?, None);
    }
    INCREMENTAL_SCAN.store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

/// The control arm must actually do the whole-image work the benchmark compares against,
/// or the paired measurement would compare a path with itself.
#[test]
fn the_whole_image_arm_rescans_the_prefix() -> Result<()> {
    let mut builder = WalBuilder::new([61, 62]);
    let image_bytes = builder.image(64);
    let next_frame = builder.frame(65, PAGE_SIZE as u32);
    let mut incremental_bytes = None;
    let mut whole_image_bytes = None;
    for incremental in [false, true] {
        INCREMENTAL_SCAN.store(incremental, std::sync::atomic::Ordering::Relaxed);
        let mut image = WalImage::default();
        image.apply(&write_at(0, &image_bytes))?;
        image.committed_position()?;
        let offset = image.len();
        image.apply(&write_at(offset, &next_frame))?;
        reset_recovery_observations();
        assert!(image.committed_position()?.is_some());
        let scanned = recovery_observations().scanned_bytes;
        if incremental {
            incremental_bytes = Some(scanned);
        } else {
            whole_image_bytes = Some(scanned);
        }
    }
    INCREMENTAL_SCAN.store(true, std::sync::atomic::Ordering::Relaxed);
    let incremental = incremental_bytes.context("incremental arm missing")?;
    let whole_image = whole_image_bytes.context("control arm missing")?;
    assert!(
        incremental <= (WAL_HEADER + FRAME_SIZE) as u64,
        "one append scanned {incremental} bytes of an already verified image"
    );
    assert!(
        whole_image >= ((WAL_HEADER + 64 * FRAME_SIZE) as u64),
        "the control arm scanned only {whole_image} bytes, so it is not the whole-image path"
    );
    Ok(())
}
