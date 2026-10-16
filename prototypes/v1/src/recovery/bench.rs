//! Paired scan benchmark. Ignored by default: its numbers belong to a dedicated timing
//! slot, and running it beside other owners' measurements would attribute their load to
//! this change. Fixtures — the image of each tier and the frames a sample appends — are
//! generated before the measurement, never inside it.
//!
//! Both arms run in the same binary on the same fixture bytes, and the only difference is
//! the `INCREMENTAL_SCAN` switch: the baseline arm re-derives the checksum chain from the
//! header on every call, which is the whole-image scan this change replaced. The measured
//! span is appends plus one scan per append, so the reported difference is the scan state
//! and nothing else — it is not a commit, a publication or a statement cost.

use super::*;
use crate::recovery::fixture::{write_at, WalBuilder, FRAME_SIZE, PAGE_SIZE};

/// Cumulative WAL sizes to measure, given as frame counts.
const TIERS_ENV: &str = "PERI_WAL_BENCH_TIERS";
/// Rounds per tier; the harness asks for at least three and alternates the arm order so a
/// warm-up trend cannot favour one arm.
const ROUNDS_ENV: &str = "PERI_WAL_BENCH_ROUNDS";
/// Fixed number of single-frame appends measured per sample.
const APPENDS_ENV: &str = "PERI_WAL_BENCH_APPENDS";

const DEFAULT_TIERS: &str = "2000,16000,65536";
const DEFAULT_ROUNDS: usize = 3;
const DEFAULT_APPENDS: usize = 32;

fn tiers() -> Vec<usize> {
    std::env::var(TIERS_ENV)
        .unwrap_or_else(|_| DEFAULT_TIERS.into())
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .collect()
}

fn rounds() -> usize {
    std::env::var(ROUNDS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ROUNDS)
        .max(3)
}

fn appends() -> usize {
    std::env::var(APPENDS_ENV)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_APPENDS)
        .max(1)
}

fn median(values: &mut [u128]) -> u128 {
    values.sort_unstable();
    values[values.len() / 2]
}

/// One measured sample: `frames.len()` single-frame appends, each followed by a scan.
struct Sample {
    wall_us: u128,
    scanned_bytes: u64,
    scan_nanos: u64,
    scan_calls: u64,
    copied_bytes: u64,
}

/// Both arms start from the same image bytes and append the same frames, so the only
/// difference between them is whether a scan may reuse the prefix it already verified.
fn sample(image_bytes: &[u8], frames: &[Vec<u8>], incremental: bool) -> Sample {
    INCREMENTAL_SCAN.store(incremental, std::sync::atomic::Ordering::Relaxed);
    let mut image = WalImage::default();
    image
        .apply(&write_at(0, image_bytes))
        .expect("fixture image");
    // Warm the scan state outside the measurement, so both arms start from a verified
    // prefix exactly as a steady-state commit path does.
    assert!(
        image.committed_position().expect("fixture scan").is_some(),
        "fixture image must commit"
    );
    reset_recovery_observations();
    let before = recovery_observations();
    let started_at = std::time::Instant::now();
    for frame in frames {
        let offset = image.len();
        image
            .apply(&write_at(offset, frame))
            .expect("fixture append");
        assert!(
            image.committed_position().expect("fixture scan").is_some(),
            "an append lost its commit frame"
        );
    }
    let wall_us = started_at.elapsed().as_micros();
    let observed = recovery_observations();
    INCREMENTAL_SCAN.store(true, std::sync::atomic::Ordering::Relaxed);
    // The arm's own conclusion must agree with the whole-image scan of the same bytes.
    assert_eq!(
        committed_position(image.bytes()).expect("reference scan"),
        image.committed_position().expect("final scan"),
        "arms disagree on the fixture"
    );
    Sample {
        wall_us,
        scanned_bytes: observed.scanned_bytes - before.scanned_bytes,
        scan_nanos: observed.scan_nanos - before.scan_nanos,
        scan_calls: observed.scan_calls - before.scan_calls,
        copied_bytes: observed.copied_bytes - before.copied_bytes,
    }
}

#[test]
#[ignore = "WAL 扫描对照基准：显式运行时才计时，避免与其它 owner 的测量重叠"]
fn wal_scan_paired_curve() {
    let rounds = rounds();
    let appends = appends();
    for tier in tiers() {
        // Fixture generation happens once per tier, before any measurement.
        let mut builder = WalBuilder::new([91, 92]);
        let image_bytes = builder.image(tier);
        let image_bytes_len = image_bytes.len();
        let frames = (0..appends)
            .map(|_| builder.frame(1, PAGE_SIZE as u32))
            .collect::<Vec<_>>();
        let mut walls: [Vec<u128>; 2] = [Vec::new(), Vec::new()];
        for round in 1..=rounds {
            let order = if round % 2 == 1 { [0, 1] } else { [1, 0] };
            for index in order {
                let incremental = index == 0;
                let sample = sample(&image_bytes, &frames, incremental);
                walls[index].push(sample.wall_us);
                eprintln!(
                    "WAL_SCAN_BENCH tier_frames={tier} image_bytes={image_bytes_len} \
                     frame_bytes={FRAME_SIZE} arm={} round={round} appends={appends} \
                     wall_us={} scan_us={} scanned_bytes={} per_append_scan_bytes={} \
                     scan_calls={} copied_bytes={}",
                    if incremental {
                        "incremental"
                    } else {
                        "whole-image"
                    },
                    sample.wall_us,
                    sample.scan_nanos / 1_000,
                    sample.scanned_bytes,
                    sample.scanned_bytes / appends as u64,
                    sample.scan_calls,
                    sample.copied_bytes,
                );
            }
        }
        for (index, values) in walls.iter_mut().enumerate() {
            eprintln!(
                "WAL_SCAN_BENCH_SUMMARY tier_frames={tier} arm={} rounds={rounds} median_wall_us={}",
                if index == 0 { "incremental" } else { "whole-image" },
                median(values),
            );
        }
    }
}
