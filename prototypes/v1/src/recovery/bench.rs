//! Paired scan benchmark. Ignored by default: its numbers belong to a dedicated timing
//! slot, and running it beside other owners' measurements would attribute their load to
//! this change. Fixtures — the image of each tier and the frames a sample appends — are
//! generated before the measurement, never inside it.
//!
//! Both arms run in the same binary on the same fixture bytes, and the only difference is the
//! control arm on the image itself (`WalImage::force_whole_image`): the baseline arm re-derives
//! the checksum chain from the header on every call, which is the whole-image scan this change
//! replaced. Each arm owns its image, so the setting is instance state rather than process
//! state and no other test can be affected by it. The measured span is appends plus one scan
//! per append, so the reported difference is the scan state and nothing else — it is not a
//! commit, a publication or a statement cost.

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
    let mut image = WalImage::default();
    if !incremental {
        image.force_whole_image();
    }
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

/// Cumulative image sizes for the append benchmark, as frame counts. The default tiers are
/// about 8, 64 and 512 MiB at the 536-byte frame the fixtures build.
const APPEND_TIERS_ENV: &str = "PERI_WAL_APPEND_TIERS";
const DEFAULT_APPEND_TIERS: &str = "16000,125000,1000000";
/// The accepted append may copy at most this multiple of the smallest tier's per-append wall.
const APPEND_WALL_RATIO: f64 = 1.2;

fn append_tiers() -> Vec<usize> {
    std::env::var(APPEND_TIERS_ENV)
        .unwrap_or_else(|_| DEFAULT_APPEND_TIERS.into())
        .split(',')
        .filter_map(|value| value.trim().parse().ok())
        .collect()
}

/// One measured append: a single committing frame behind an image that already has the
/// headroom its previous append left.
struct AppendSample {
    wall_us: u128,
    copied_bytes: u64,
}

/// `in_place` extends the image; the control arm clones it first, which is the copy the
/// publication path used to pay before it could reuse the accepted state. Both arms append
/// the same frames and end with the same bytes, so the wall difference is the copy.
fn append_sample(image_bytes: &[u8], frames: &[Vec<u8>], in_place: bool) -> AppendSample {
    let mut image = WalImage::default();
    image
        .apply(&write_at(0, image_bytes))
        .expect("fixture image");
    assert!(
        image.committed_position().expect("fixture scan").is_some(),
        "fixture image must commit"
    );
    // Absorb the one growth the first append can force outside the measurement. A
    // steady-state image pays that growth once and then appends into its headroom; the
    // amortized bound of that growth is asserted by the growth test, not measured here.
    let offset = image.len();
    image
        .apply(&write_at(offset, &frames[0]))
        .expect("warm-up append");
    assert!(
        image.committed_position().expect("warm-up scan").is_some(),
        "warm-up frame must commit"
    );
    reset_recovery_observations();
    let before = recovery_observations();
    let started = std::time::Instant::now();
    if in_place {
        for frame in &frames[1..] {
            let offset = image.len();
            image.apply(&write_at(offset, frame)).expect("append");
            assert!(
                image.committed_position().expect("append scan").is_some(),
                "an append lost its commit frame"
            );
        }
    } else {
        for frame in &frames[1..] {
            let mut candidate = image.clone();
            let offset = candidate.len();
            candidate.apply(&write_at(offset, frame)).expect("append");
            assert!(
                candidate
                    .committed_position()
                    .expect("append scan")
                    .is_some(),
                "an append lost its commit frame"
            );
            image = candidate;
        }
    }
    let wall_us = started.elapsed().as_micros();
    let observed = recovery_observations();
    assert_eq!(
        committed_position(image.bytes()).expect("reference scan"),
        image.committed_position().expect("final scan"),
        "arms disagree on the fixture"
    );
    AppendSample {
        wall_us,
        copied_bytes: observed.copied_bytes - before.copied_bytes,
    }
}

/// A-min acceptance (V8): appending one frame must not become slower as the image grows. The
/// in-place arm is the implemented path; the cloning arm is the previous behaviour kept as a
/// control, and its ratio is reported rather than asserted because it is not the shipped
/// path. Only the in-place ratio is a gate: the largest tier's median append must stay within
/// 1.2x of the smallest tier's.
#[test]
#[ignore = "WAL 追加对照基准：显式运行时才计时，且默认最大档需要约 2 GiB 内存"]
fn wal_append_does_not_scale_with_the_image() {
    let rounds = rounds();
    let appends = appends();
    let mut medians: [Vec<(usize, u128)>; 2] = [Vec::new(), Vec::new()];
    for tier in append_tiers() {
        let mut builder = WalBuilder::new([93, 94]);
        let image_bytes = builder.image(tier);
        let image_bytes_len = image_bytes.len();
        // One warm-up frame plus the measured appends, generated before the measurement so
        // the builder's checksum chain is not part of the timed span.
        let frames = (0..=appends)
            .map(|_| builder.frame(1, PAGE_SIZE as u32))
            .collect::<Vec<_>>();
        for round in 1..=rounds {
            let order = if round % 2 == 1 { [0, 1] } else { [1, 0] };
            for index in order {
                let in_place = index == 0;
                let sample = append_sample(&image_bytes, &frames, in_place);
                medians[index].push((tier, sample.wall_us));
                eprintln!(
                    "WAL_APPEND_BENCH tier_frames={tier} image_bytes={image_bytes_len} \
                     frame_bytes={FRAME_SIZE} arm={} round={round} appends={appends} \
                     wall_us={} per_append_us={} copied_bytes={}",
                    if in_place { "in-place" } else { "cloned" },
                    sample.wall_us,
                    sample.wall_us / appends.max(1) as u128,
                    sample.copied_bytes,
                );
            }
        }
    }
    if medians[0].is_empty() {
        eprintln!("WAL_APPEND_BENCH no tiers configured");
        return;
    }
    let ratio = |values: &mut Vec<(usize, u128)>, arm: &str| {
        values.sort_unstable();
        let mut per_tier: Vec<(usize, Vec<u128>)> = Vec::new();
        for &(tier, wall) in values.iter() {
            match per_tier.last_mut() {
                Some((last, walls)) if *last == tier => walls.push(wall),
                _ => per_tier.push((tier, vec![wall])),
            }
        }
        let medians = per_tier
            .into_iter()
            .map(|(tier, mut walls)| (tier, median(&mut walls)))
            .collect::<Vec<_>>();
        let smallest = medians.first().expect("tier").1.max(1);
        let largest = medians.last().expect("tier").1;
        let ratio = largest as f64 / smallest as f64;
        eprintln!(
            "WAL_APPEND_BENCH_SUMMARY arm={arm} rounds={rounds} tiers={} samples={} \
             smallest_wall_us={smallest} largest_wall_us={largest} ratio={ratio:.3}",
            medians.len(),
            values.len()
        );
        ratio
    };
    let in_place = ratio(&mut medians[0], "in-place");
    let cloned = ratio(&mut medians[1], "cloned");
    eprintln!("WAL_APPEND_BENCH_CONTROL clone_ratio={cloned:.3}");
    assert!(
        in_place <= APPEND_WALL_RATIO,
        "the in-place append scaled with the image: ratio {in_place:.3} > {APPEND_WALL_RATIO}"
    );
}
