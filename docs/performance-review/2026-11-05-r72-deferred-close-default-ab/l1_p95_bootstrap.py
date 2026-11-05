#!/usr/bin/env python3
"""Context-only bootstrap for the C4 L1 p95 ratio.

This does NOT change the frozen C4 criterion (a point estimate < 1.10). It
quantifies how much of the observed 1.17471 could be sampling noise, because
the L1 legs only have 125 samples each and the two rounds disagree. The result
is reported as context in the README; the verdict remains FAIL.

Seed 20261105, 10,000 draws, matching the P1 bootstrap seed.
"""
from __future__ import annotations

import json
import pathlib
import random
import statistics
import sys

SEED = 20261105
DRAWS = 10000


def percentile(values, percent):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * percent / 100.0))]


def durations(root, leg):
    path = root / "legs" / "p2" / "l1" / leg / "harness" / "raw.jsonl"
    raw = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
           if line.strip()]
    return [item["duration_ms"] for item in raw if item.get("phase") == "timed"]


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else __file__).resolve()
    if root.is_file():
        root = root.parent
    a_legs = ("r1-a1", "r1-a2", "r2-a1", "r2-a2")
    b_legs = ("r1-b1", "r1-b2", "r2-b1", "r2-b2")
    samples = {"a": {leg: durations(root, leg) for leg in a_legs},
               "b": {leg: durations(root, leg) for leg in b_legs}}

    def merged_p95(side, legs=None):
        legs = legs or (a_legs if side == "a" else b_legs)
        return statistics.median([percentile(samples[side][leg], 95) for leg in legs])

    point_a = merged_p95("a")
    point_b = merged_p95("b")
    lines = []
    lines.append("point: A %.6f B %.6f ratio %.6f" % (
        point_a, point_b, point_b / point_a))
    lines.append("per-leg p95 A: " + ", ".join(
        "%s=%.6f" % (leg, percentile(samples["a"][leg], 95)) for leg in a_legs))
    lines.append("per-leg p95 B: " + ", ".join(
        "%s=%.6f" % (leg, percentile(samples["b"][leg], 95)) for leg in b_legs))

    rng = random.Random(SEED)
    leg_ratios = []
    for _ in range(DRAWS):
        aa = [a_legs[rng.randrange(4)] for _ in range(4)]
        bb = [b_legs[rng.randrange(4)] for _ in range(4)]
        leg_ratios.append(merged_p95("b", bb) / merged_p95("a", aa))
    leg_ratios.sort()
    lines.append("leg bootstrap 95%%: [%.6f, %.6f] share>1.10=%.3f" % (
        leg_ratios[int(0.025 * DRAWS)], leg_ratios[int(0.975 * DRAWS)],
        sum(1 for value in leg_ratios if value > 1.10) / len(leg_ratios)))

    rng = random.Random(SEED)
    sample_ratios = []
    for _ in range(DRAWS):
        def boot(side, legs):
            values = []
            for leg in legs:
                source = samples[side][leg]
                resampled = [source[rng.randrange(len(source))]
                             for _ in range(len(source))]
                values.append(percentile(resampled, 95))
            return statistics.median(values)
        sample_ratios.append(boot("b", b_legs) / boot("a", a_legs))
    sample_ratios.sort()
    lines.append("sample bootstrap 95%%: [%.6f, %.6f] share>1.10=%.3f" % (
        sample_ratios[int(0.025 * DRAWS)], sample_ratios[int(0.975 * DRAWS)],
        sum(1 for value in sample_ratios if value > 1.10) / len(sample_ratios)))
    pooled_a = [value for leg in a_legs for value in samples["a"][leg]]
    pooled_b = [value for leg in b_legs for value in samples["b"][leg]]
    lines.append("pooled-sample p95 (not the frozen aggregation): A %.6f B %.6f ratio %.6f" % (
        percentile(pooled_a, 95), percentile(pooled_b, 95),
        percentile(pooled_b, 95) / percentile(pooled_a, 95)))
    lines.append("note: context only; the frozen C4 verdict uses the point estimate 1.17471 -> FAIL")
    text = "\n".join(lines) + "\n"
    (root / "l1-p95-bootstrap.txt").write_text(text, encoding="utf-8")
    print(text, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
