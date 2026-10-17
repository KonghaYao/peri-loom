use super::*;
use crate::object_store::{LocalObjectStore, ObjectStore};
use crate::recovery::fixture::{started, truncate, write_at, WalBuilder, FRAME_SIZE, PAGE_SIZE};
use crate::wal::RemoteWal;
use std::sync::Arc;

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
/// benchmark's baseline is only a control if it stays correct. Each arm scans its own image, so
/// a control arm cannot change what another arm — or a test running in parallel — scans.
#[test]
fn the_whole_image_arm_matches_the_reference_over_the_same_mutations() -> Result<()> {
    for whole_image in [false, true] {
        let mut builder = WalBuilder::new([72, 73]);
        let mut image = started(&builder)?;
        if whole_image {
            image.force_whole_image();
        }
        for page in 1..=8 {
            let frame = builder.frame(page, if page % 3 == 0 { PAGE_SIZE as u32 } else { 0 });
            let offset = image.len();
            image.apply(&write_at(offset, &frame))?;
            assert_matches_full_scan(&mut image)?;
        }
        let offset = WAL_HEADER + FRAME_SIZE + 24;
        let mut corrupted = image.bytes()[offset..offset + 8].to_vec();
        corrupted[0] ^= 0xff;
        image.apply(&write_at(offset, &corrupted))?;
        assert_eq!(assert_matches_full_scan(&mut image)?, None);
    }
    Ok(())
}

/// The control arm must actually do the whole-image work the benchmark compares against, or
/// the paired measurement would compare a path with itself.
#[test]
fn the_whole_image_arm_rescans_the_prefix() -> Result<()> {
    let mut builder = WalBuilder::new([61, 62]);
    let image_bytes = builder.image(64);
    let next_frame = builder.frame(65, PAGE_SIZE as u32);
    let mut incremental_bytes = None;
    let mut whole_image_bytes = None;
    for whole_image in [false, true] {
        let mut image = WalImage::default();
        if whole_image {
            image.force_whole_image();
        }
        image.apply(&write_at(0, &image_bytes))?;
        image.committed_position()?;
        let offset = image.len();
        image.apply(&write_at(offset, &next_frame))?;
        reset_recovery_observations();
        assert!(image.committed_position()?.is_some());
        let scanned = recovery_observations().scanned_bytes;
        if whole_image {
            whole_image_bytes = Some(scanned);
        } else {
            incremental_bytes = Some(scanned);
        }
    }
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

/// The control arm belongs to the image it was set on: an image that never asked for it keeps
/// paying for the frames it adds. A process-wide switch cannot hold this property, which is why
/// tests of different arms in the same binary used to tread on each other.
#[test]
fn the_control_arm_does_not_leak_between_images() -> Result<()> {
    let mut builder = WalBuilder::new([51, 52]);
    let image_bytes = builder.image(8);
    let next_frame = builder.frame(9, PAGE_SIZE as u32);
    let mut control = WalImage::default();
    control.force_whole_image();
    let mut observed = WalImage::default();
    for image in [&mut control, &mut observed] {
        image.apply(&write_at(0, &image_bytes))?;
        image.committed_position()?;
        let offset = image.len();
        image.apply(&write_at(offset, &next_frame))?;
    }

    reset_recovery_observations();
    assert!(observed.committed_position()?.is_some());
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + FRAME_SIZE) as u64,
        "another image's control arm changed what this image scanned"
    );

    reset_recovery_observations();
    assert!(control.committed_position()?.is_some());
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + 9 * FRAME_SIZE) as u64,
        "the image that asked for the control arm did not rescan"
    );
    Ok(())
}

/// Store-backed index fixture for the append tests: the records the store publishes and the
/// chunk loader the validators read them back through.
struct WalStore {
    _directory: tempfile::TempDir,
    store: Arc<dyn ObjectStore>,
    namespace: Namespace,
    wal: RemoteWal,
}

impl WalStore {
    fn open(database: &str) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::open(directory.path())?);
        Ok(Self {
            _directory: directory,
            wal: RemoteWal::new(store.clone()),
            store,
            namespace: Namespace::new("recovery", database)?,
        })
    }

    /// Appends one record and returns the index that now describes it.
    fn append(&self, batch: WalBatch, position: Option<WalPosition>) -> Result<Vec<u8>> {
        self.wal
            .append_commit(&self.namespace, 1, batch, position)?;
        Ok(self
            .store
            .get(&self.namespace.key("wal/index")?)?
            .context("missing WAL index")?
            .bytes)
    }

    fn load_segment(&self, hash: &str) -> Result<Vec<u8>> {
        self.load(&self.namespace.key(&format!("wal/index-segments/{hash}"))?)
    }

    fn load_page(&self, hash: &str) -> Result<Vec<u8>> {
        self.load(&self.namespace.key(&format!("wal/receipt-pages/{hash}"))?)
    }

    fn load(&self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .store
            .get(key)?
            .context("missing committed chunk")?
            .bytes)
    }
}

/// An accepted append extends the validated image in place: it verifies only the frames the
/// record adds, lands on exactly the bytes a cold replay derives, and copies the image only
/// when the batch outgrows the capacity the previous append left behind.
#[test]
fn an_accepted_append_extends_the_validated_image_in_place() -> Result<()> {
    let store = WalStore::open("prefix")?;
    let mut builder = WalBuilder::new([17, 18]);
    let salts = [17, 18];
    let first = builder.image(4);
    let index = store.append(
        write_at(0, &first),
        Some(WalPosition {
            salts,
            frame_count: 4,
        }),
    )?;
    let validated = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;

    let appended = builder.frame(5, PAGE_SIZE as u32);
    let offset = first.len();
    let index = store.append(
        write_at(offset, &appended),
        Some(WalPosition {
            salts,
            frame_count: 5,
        }),
    )?;

    reset_recovery_observations();
    let incremented = validate_index_append(
        validated,
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    let observed = recovery_observations();
    assert_eq!(
        observed.scanned_bytes,
        (WAL_HEADER + FRAME_SIZE) as u64,
        "the append verified more than the frames it added"
    );
    // The fixture's image was written as one batch, so its capacity equals its length. The
    // growth this append forces is the only copy a steady-state append can still cause; the
    // counter reports the old capacity as an upper bound because the allocator may extend in
    // place. The next append starts with that headroom and copies nothing.
    assert_eq!(
        observed.copied_bytes,
        first.len() as u64,
        "the growth was not attributed to the append that forced it"
    );
    let cold = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    assert_eq!(incremented.commit(), cold.commit());
    assert_eq!(incremented.image.bytes(), cold.image.bytes());

    let second = builder.frame(6, PAGE_SIZE as u32);
    let offset = first.len() + FRAME_SIZE;
    let index = store.append(
        write_at(offset, &second),
        Some(WalPosition {
            salts,
            frame_count: 6,
        }),
    )?;
    reset_recovery_observations();
    let incremented = validate_index_append(
        incremented,
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    let observed = recovery_observations();
    assert_eq!(
        observed.scanned_bytes,
        (WAL_HEADER + FRAME_SIZE) as u64,
        "the second append did not reuse the verified prefix"
    );
    assert_eq!(
        observed.copied_bytes, 0,
        "the appended image was copied instead of extended in place"
    );
    let cold = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    assert_eq!(incremented.commit(), cold.commit());
    assert_eq!(incremented.image.bytes(), cold.image.bytes());
    Ok(())
}

/// A record that writes inside the validated prefix must not keep it: the append after it
/// re-derives the chain from the header, and the chain still agrees with a cold replay.
#[test]
fn an_overwriting_append_forces_the_next_one_to_re_derive_the_prefix() -> Result<()> {
    let store = WalStore::open("overwrite")?;
    let mut builder = WalBuilder::new([31, 32]);
    let salts = [31, 32];
    let first = builder.image(4);
    let index = store.append(
        write_at(0, &first),
        Some(WalPosition {
            salts,
            frame_count: 4,
        }),
    )?;
    let validated = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;

    // Overwrite a byte of an already verified frame. The image it leaves has no committed
    // boundary, so the record carries no commit marker and does not scan at all.
    let damage = WAL_HEADER + FRAME_SIZE + 24;
    let mut corrupted = first[damage..damage + 8].to_vec();
    corrupted[0] ^= 0xff;
    let index = store.append(write_at(damage, &corrupted), None)?;
    let overwritten = validate_index_append(
        validated,
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;

    // Repairing the frame and appending behind it is one record. Its scan must cover the whole
    // image: had the overwriting record left a prefix reusable, only the two new frames would
    // be verified.
    let repaired = first[damage..damage + 8].to_vec();
    let mut batch = write_at(damage, &repaired);
    for (page, db_size) in [(5, 0), (6, PAGE_SIZE as u32)] {
        batch.operations.push(WalOperation::Write {
            offset: (WAL_HEADER + (page - 1) * FRAME_SIZE) as u64,
            bytes: builder.frame(page as u32, db_size),
        });
    }
    let index = store.append(
        batch,
        Some(WalPosition {
            salts,
            frame_count: 6,
        }),
    )?;

    reset_recovery_observations();
    let chained = validate_index_append(
        overwritten,
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    assert_eq!(
        recovery_observations().scanned_bytes,
        (WAL_HEADER + 6 * FRAME_SIZE) as u64,
        "the overwrite did not invalidate the verified prefix"
    );

    let cold = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    assert_eq!(chained.commit(), cold.commit());
    assert_eq!(chained.image.bytes(), cold.image.bytes());
    Ok(())
}

/// The in-place path must accept exactly what a cold replay accepts, derive the same commit
/// and hold the same bytes across a sequence of real appends. A forged candidate is rejected
/// by both, and consuming the accepted state on rejection is part of the contract: every
/// forged attempt below starts from a state re-derived from the store.
#[test]
fn in_place_appends_agree_with_a_cold_replay_and_reject_the_same_forgeries() -> Result<()> {
    let store = WalStore::open("agreement")?;
    let mut builder = WalBuilder::new([51, 52]);
    let salts = [51, 52];
    let first = builder.image(3);
    let mut index = store.append(
        write_at(0, &first),
        Some(WalPosition {
            salts,
            frame_count: 3,
        }),
    )?;
    let mut image_len = first.len();
    let mut previous = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;

    for page in 4..=40u32 {
        let frame = builder.frame(page, PAGE_SIZE as u32);
        let next = store.append(
            write_at(image_len, &frame),
            Some(WalPosition {
                salts,
                frame_count: page as u64,
            }),
        )?;
        image_len += FRAME_SIZE;

        // A commit position the appended frame does not produce: both validators must refuse
        // it, so the incremental path never accepts a state a cold replay rejects.
        let mut forged: serde_json::Value = serde_json::from_slice(&next)?;
        forged["commit"]["position"]["frame_count"] = serde_json::Value::from(page - 1);
        let fresh = validate_index_state(
            &index,
            &store.namespace,
            |hash| store.load_segment(hash),
            |key| store.load(key),
            |hash| store.load_page(hash),
        )?;
        assert!(
            validate_index_append(
                fresh,
                &serde_json::to_vec(&forged)?,
                &store.namespace,
                |hash| store.load_segment(hash),
                |key| store.load(key),
                |hash| store.load_page(hash)
            )
            .is_err(),
            "the incremental validator accepted a forged candidate at page {page}"
        );
        assert!(
            validate_index_state(
                &serde_json::to_vec(&forged)?,
                &store.namespace,
                |hash| { store.load_segment(hash) },
                |key| store.load(key),
                |hash| store.load_page(hash)
            )
            .is_err(),
            "the cold replay accepted a forged candidate at page {page}"
        );

        let extended = validate_index_append(
            previous,
            &next,
            &store.namespace,
            |hash| store.load_segment(hash),
            |key| store.load(key),
            |hash| store.load_page(hash),
        )?;
        let cold = validate_index_state(
            &next,
            &store.namespace,
            |hash| store.load_segment(hash),
            |key| store.load(key),
            |hash| store.load_page(hash),
        )?;
        assert_eq!(
            extended.commit(),
            cold.commit(),
            "commit diverged at page {page}"
        );
        assert_eq!(
            extended.image.bytes(),
            cold.image.bytes(),
            "image bytes diverged at page {page}"
        );
        previous = extended;
        index = next;
    }
    Ok(())
}

/// Geometric growth is the only copy the in-place path still causes. It must stay amortized:
/// the reserved headroom is never larger than the image it extends, and the capacity growth
/// an append sequence forces is bounded by a small multiple of the bytes the appends added.
#[test]
fn image_growth_stays_within_the_amortized_bound() -> Result<()> {
    let mut builder = WalBuilder::new([71, 72]);
    let mut image = WalImage::default();
    reset_recovery_observations();
    let header = builder.header();
    image.apply(&write_at(0, &header))?;
    let mut added = header.len();
    for page in 1..=256u32 {
        let frame = builder.frame(page, if page == 256 { PAGE_SIZE as u32 } else { 0 });
        let before = image.len();
        image.apply(&write_at(before, &frame))?;
        added += image.len() - before;
        image.committed_position()?;
    }
    let observed = recovery_observations();
    assert!(
        observed.copied_bytes <= 4 * added as u64,
        "growing a {}-byte image copied {} bytes for {added} added bytes",
        image.len(),
        observed.copied_bytes
    );
    assert!(
        image.bytes.capacity() - image.len() <= image.len(),
        "the image reserved more headroom than it holds: {} bytes for a {}-byte image",
        image.bytes.capacity(),
        image.len()
    );
    Ok(())
}

/// Which reason abandoned a verified prefix, and which check stopped a scan, must be
/// attributable: a review reads these counters to tell a removed growth term from one that
/// only moved. The thread-local counts are exact because they belong to this test's thread;
/// the process-wide totals are lower-bounded because other tests in the binary may record
/// into the same sink once the switch is on.
#[test]
fn scan_fallback_reasons_are_counted() -> Result<()> {
    use crate::work_metrics::{ScanReset, ScanStop};

    crate::work_metrics::set_enabled(true);
    let global_before = crate::work_metrics::snapshot();
    let mut builder = WalBuilder::new([21, 22]);
    let mut image = started(&builder)?;
    for page in 1..=4 {
        let frame = builder.frame(page, if page == 4 { PAGE_SIZE as u32 } else { 0 });
        image.apply(&write_at(image.len(), &frame))?;
    }
    assert_matches_full_scan(&mut image)?;

    // A write into the verified prefix, then the header the fresh scan does not belong to.
    reset_recovery_observations();
    image.apply(&write_at(0, &builder.header()))?;
    assert_eq!(
        recovery_observations().scan_resets[ScanReset::WriteInPrefix as usize],
        1
    );
    assert_matches_full_scan(&mut image)?;
    assert_eq!(
        recovery_observations().scan_resets[ScanReset::HeaderChanged as usize],
        1
    );

    // A truncate into the prefix, and the frame-aligned and unaligned tails it leaves.
    reset_recovery_observations();
    image.resize(WAL_HEADER + FRAME_SIZE)?;
    assert_eq!(
        recovery_observations().scan_resets[ScanReset::TruncateInPrefix as usize],
        1
    );
    reset_recovery_observations();
    image.resize(WAL_HEADER + FRAME_SIZE - 8)?;
    assert_eq!(assert_matches_full_scan(&mut image)?, None);
    let tail = recovery_observations();
    assert_eq!(tail.scan_stops[ScanStop::IncompleteTail as usize], 1);

    // A frame whose salt no longer matches the header, and one whose checksum no longer
    // matches its payload. Each gets a fresh state so the prefix cannot answer for it.
    // Each image starts from its own header: a builder continues the checksum chain of the
    // frames it already produced, so a fresh image must come from a fresh builder.
    let mut salted = WalBuilder::new([23, 24]).image(3);
    salted[WAL_HEADER + FRAME_SIZE + 8] ^= 0xff;
    let mut salted_scan = WalFrameScan::default();
    assert_eq!(salted_scan.position(&salted)?, None);
    assert_eq!(
        recovery_observations().scan_stops[ScanStop::SaltMismatch as usize],
        1
    );
    let mut corrupt = WalBuilder::new([25, 26]).image(3);
    corrupt[WAL_HEADER + 2 * FRAME_SIZE + 32] ^= 0xff;
    let mut corrupt_scan = WalFrameScan::default();
    assert_eq!(corrupt_scan.position(&corrupt)?, None);
    assert_eq!(
        recovery_observations().scan_stops[ScanStop::ChecksumMismatch as usize],
        1
    );

    // A shrink and an image below a full header are reachable only through the scan type:
    // the image API reports every mutation, so these are the defensive branches.
    let whole = WalBuilder::new([27, 28]).image(2);
    let mut shrink_scan = WalFrameScan::default();
    assert!(shrink_scan.position(&whole)?.is_some());
    assert_eq!(shrink_scan.position(&whole[..WAL_HEADER + 4])?, None);
    assert_eq!(
        recovery_observations().scan_resets[ScanReset::Shrink as usize],
        1
    );
    assert_eq!(shrink_scan.position(&whole[..8])?, None);
    let defensive = recovery_observations();
    assert_eq!(defensive.scan_resets[ScanReset::Shrink as usize], 2);
    assert_eq!(defensive.scan_resets[ScanReset::ShortImage as usize], 1);

    // The same work reaches the process-wide sink the diagnostics endpoint exports. The
    // totals are lower-bounded: the delta covers this thread's increments plus any other
    // test recording into the same sink.
    let observed = recovery_observations();
    let global = crate::work_metrics::snapshot().context("enabled snapshot")?;
    let before = global_before.unwrap_or_default();
    assert!(
        global.wal_workload.scanned_frames - before.wal_workload.scanned_frames
            >= observed.scanned_frames
    );
    for reason in [
        "write_in_prefix",
        "header_changed",
        "truncate_in_prefix",
        "shrink",
    ] {
        assert!(
            global.wal_workload.scan_resets[reason] >= 1,
            "reason {reason} never reached the exported sink"
        );
    }
    Ok(())
}

#[test]
fn segmented_entries_replay_and_validate_only_the_new_segment() -> Result<()> {
    let store = WalStore::open("index-segments")?;
    let salts = [31, 32];
    let mut builder = WalBuilder::new(salts);
    let mut expected = builder.header();
    let mut previous = None;
    let mut before_flush = Vec::new();
    let mut flushed = Vec::new();
    for ordinal in 1..=514u64 {
        let frame = builder.frame(1, PAGE_SIZE as u32);
        let mut bytes = if ordinal == 1 {
            expected.clone()
        } else {
            Vec::new()
        };
        bytes.extend_from_slice(&frame);
        let offset = if ordinal == 1 { 0 } else { expected.len() };
        expected.extend_from_slice(&frame);
        let index = store.append(
            write_at(offset, &bytes),
            Some(WalPosition {
                salts,
                frame_count: ordinal,
            }),
        )?;
        let metadata: WalMetadata = serde_json::from_slice(&index)?;
        assert_eq!(metadata.entry_count, Some(ordinal));
        assert!(metadata.entries.len() <= WAL_INDEX_SEGMENT_LIMIT);
        assert_eq!(metadata.entry_index.is_some(), ordinal > 256);
        let mut reads = 0;
        let validated = if let Some(previous) = previous {
            validate_index_append(
                previous,
                &index,
                &store.namespace,
                |hash| {
                    reads += 1;
                    store.load_segment(hash)
                },
                |key| store.load(key),
                |hash| store.load_page(hash),
            )?
        } else {
            validate_index_state(
                &index,
                &store.namespace,
                |hash| store.load_segment(hash),
                |key| store.load(key),
                |hash| store.load_page(hash),
            )?
        };
        assert_eq!(reads, usize::from(ordinal == 257 || ordinal == 513));
        assert_eq!(validated.image.bytes(), expected);
        if ordinal == 512 {
            before_flush = index.clone();
        }
        if ordinal == 513 {
            flushed = index.clone();
        }
        previous = Some(validated);
    }
    let index = store.load(&store.namespace.key("wal/index")?)?;
    let cold = validate_index_state(
        &index,
        &store.namespace,
        |hash| store.load_segment(hash),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    assert_eq!(cold.image.bytes(), expected);
    assert_eq!(cold.commit(), previous.unwrap().commit());
    let mut inline = cold.index.clone();
    inline.entries = cold.entries.clone();
    inline.entry_index = None;
    let legacy = validate_index_state(
        &serde_json::to_vec(&inline)?,
        &store.namespace,
        |_| anyhow::bail!("inline loaded segment"),
        |key| store.load(key),
        |hash| store.load_page(hash),
    )?;
    assert_eq!(legacy.image.bytes(), cold.image.bytes());
    assert_eq!(legacy.commit(), cold.commit());

    let fresh = || {
        validate_index_state(
            &before_flush,
            &store.namespace,
            |hash| store.load_segment(hash),
            |key| store.load(key),
            |hash| store.load_page(hash),
        )
    };
    for fault in [
        "previous", "entries", "missing", "hash", "count", "version", "empty",
    ] {
        let mut candidate: WalMetadata = serde_json::from_slice(&flushed)?;
        let mut segment: WalIndexSegment =
            serde_json::from_slice(&store.load_segment(candidate.entry_index.as_ref().unwrap())?)?;
        match fault {
            "previous" => segment.previous = None,
            "entries" => segment.entries[0].hash = "0".repeat(64),
            "count" => candidate.entry_count = Some(999),
            "version" => segment.version = 2,
            "empty" => segment.entries.clear(),
            _ => {}
        }
        let mut bytes = serde_json::to_vec(&segment)?;
        candidate.entry_index = Some(blake3::hash(&bytes).to_hex().to_string());
        if fault == "hash" {
            bytes[0] ^= 1;
        }
        let result = validate_index_append(
            fresh()?,
            &serde_json::to_vec(&candidate)?,
            &store.namespace,
            |_| {
                if fault == "missing" {
                    anyhow::bail!("segment missing");
                }
                Ok(bytes.clone())
            },
            |key| store.load(key),
            |hash| store.load_page(hash),
        );
        assert!(result.is_err(), "accepted {fault}");
    }
    Ok(())
}

#[test]
fn cold_segment_resolution_rejects_corruption_and_count_or_order_changes() -> Result<()> {
    let hash = "a".repeat(64);
    let segment = WalIndexSegment {
        version: 1,
        previous: None,
        entries: vec![Entry {
            lsn: 1,
            hash: hash.clone(),
        }],
    };
    for fault in [
        "missing",
        "digest",
        "version",
        "empty",
        "oversized",
        "previous",
        "entry-hash",
        "count",
        "gap",
        "first",
    ] {
        let mut segment = segment.clone();
        let mut index = WalMetadata {
            last_lsn: 2,
            entry_count: Some(2),
            entries: vec![Entry {
                lsn: 2,
                hash: hash.clone(),
            }],
            ..WalMetadata::default()
        };
        match fault {
            "version" => segment.version = 2,
            "empty" => segment.entries.clear(),
            "oversized" => segment.entries = vec![segment.entries[0].clone(); 257],
            "previous" => segment.previous = Some("INVALID".into()),
            "entry-hash" => segment.entries[0].hash = "INVALID".into(),
            "count" => index.entry_count = Some(3),
            "gap" => {
                index.entries[0].lsn = 3;
                index.last_lsn = 3;
            }
            "first" => {
                segment.entries[0].lsn = 2;
                index.entries[0].lsn = 3;
                index.last_lsn = 3;
            }
            _ => {}
        }
        let mut bytes = serde_json::to_vec(&segment)?;
        index.entry_index = Some(blake3::hash(&bytes).to_hex().to_string());
        if fault == "digest" {
            bytes[0] ^= 1;
        }
        assert!(
            resolve_entries(&index, |_| {
                if fault == "missing" {
                    anyhow::bail!("missing segment");
                }
                Ok(bytes.clone())
            })
            .is_err(),
            "accepted {fault}"
        );
    }
    Ok(())
}
