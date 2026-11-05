#!/usr/bin/env python3
"""Analyze one R71 Gate 1b campaign: A/A check, positive control, or candidate.

Every number comes from the per-block `raw.jsonl` written by block_driver.py.
The merged estimate is the ratio of the two sides' pooled block medians; the
interval is a percentile bootstrap that resamples blocks within each side
(10,000 draws, fixed counts per side), so it measures block-to-block noise, not
leg-to-leg drift. Per-round ratios are reported separately and are the check
the protocol uses for "both rounds same direction".
"""
from __future__ import annotations

import argparse
import json
import pathlib
import random
import statistics

BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20261105


def blocks_by_side(root: pathlib.Path, mode: str) -> tuple[dict, list[str], list[dict]]:
    sides: dict[str, list[float]] = {"a": [], "b": []}
    rounds: dict[str, dict[str, list[float]]] = {}
    positions: dict[str, list[float]] = {"a1": [], "a2": []}
    legs: list[dict] = []
    for directory in sorted(root.glob("legs/*-%s" % mode)):
        summary = json.loads((directory / "summary.json").read_text())
        raw = [json.loads(line) for line in (directory / "raw.jsonl").read_text().splitlines()
               if line.strip()]
        timed = [block["per_request_ms"] for block in raw if not block["warmup"]]
        leg_name = summary["leg"]
        side = summary["side"]
        round_name = summary["round"]
        sides[side].extend(timed)
        rounds.setdefault(round_name, {"a": [], "b": []})[side].extend(timed)
        if side == "a" and leg_name.endswith("a1"):
            positions["a1"].extend(timed)
        if side == "a" and leg_name.endswith("a2"):
            positions["a2"].extend(timed)
        legs.append({
            "leg": leg_name,
            "side": side,
            "round": round_name,
            "blocks": len(timed),
            "median_ms": round(statistics.median(timed), 4) if timed else None,
            "mean_ms": round(statistics.fmean(timed), 4) if timed else None,
            "stdev_ms": round(statistics.stdev(timed), 4) if len(timed) > 1 else None,
            "failures": summary.get("failures", 0),
            "first_failure": summary.get("first_failure"),
        })
    return {"sides": sides, "rounds": rounds, "positions": positions}, [], legs


def ratio_of_medians(left: list[float], right: list[float]) -> float | None:
    if not left or not right:
        return None
    return statistics.median(right) / statistics.median(left)


def bootstrap_ratio(left: list[float], right: list[float]) -> dict | None:
    if not left or not right:
        return None
    generator = random.Random(BOOTSTRAP_SEED)
    left_items = list(left)
    right_items = list(right)
    ratios = []
    for _ in range(BOOTSTRAP_DRAWS):
        left_sample = [left_items[generator.randrange(len(left_items))] for _ in range(len(left_items))]
        right_sample = [right_items[generator.randrange(len(right_items))] for _ in range(len(right_items))]
        ratios.append(statistics.median(right_sample) / statistics.median(left_sample))
    ratios.sort()
    return {
        "ratio": round(statistics.median(right) / statistics.median(left), 5),
        "ci95": [round(ratios[int(0.025 * len(ratios))], 5),
                 round(ratios[min(len(ratios) - 1, int(0.975 * len(ratios)))], 5)],
        "draws": BOOTSTRAP_DRAWS,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, help="evidence directory")
    parser.add_argument("--mode", required=True, choices=("aa", "control", "control2", "candidate"))
    arguments = parser.parse_args()
    root = pathlib.Path(arguments.root)

    data, _, legs = blocks_by_side(root, arguments.mode)
    sides, rounds, positions = data["sides"], data["rounds"], data["positions"]

    report: dict = {
        "mode": arguments.mode,
        "legs": legs,
        "blocks": {side: len(values) for side, values in sides.items()},
        "per_leg": {leg["leg"]: leg["median_ms"] for leg in legs},
    }

    merged = bootstrap_ratio(sides["a"], sides["b"])
    report["merged_b_over_a"] = merged
    report["round_ratios"] = {
        name: round(ratio_of_medians(values["a"], values["b"]), 5) if values["a"] and values["b"] else None
        for name, values in sorted(rounds.items())
    }

    failures = [leg for leg in legs if leg["failures"]]
    report["failed_legs"] = [leg["leg"] for leg in failures]
    report["complete"] = all(leg["blocks"] == 30 for leg in legs) and len(legs) == 8 and not failures

    verdict = "unknown"
    if arguments.mode == "aa":
        # The frozen check: the merged ratio of the two sides, with the same
        # binary on both, must sit inside 1.00 +/- 0.05. The positional split is
        # reported as a drift diagnostic, not as the gate.
        aa = bootstrap_ratio(positions["a1"], positions["a2"])
        report["aa_a1_over_a2"] = aa
        if merged is None:
            verdict = "invalid: missing legs"
        elif 0.95 <= merged["ratio"] <= 1.05:
            verdict = "pass: merged A/A ratio inside 1.00 +/- 0.05"
        else:
            verdict = "fail: merged A/A ratio outside 1.00 +/- 0.05, protocol power insufficient"
    elif arguments.mode in ("control", "control2"):
        if merged is None:
            verdict = "invalid: missing legs"
        else:
            per_round = report["round_ratios"]
            direction = all(value is not None and value > 1.0 for value in per_round.values())
            if merged["ratio"] >= 1.08 and direction:
                verdict = "pass: +100us control detected (>= 8%, both rounds same direction)"
            else:
                verdict = "fail: +100us control not detected at 8% with both rounds same direction"
    else:
        if merged is None:
            verdict = "invalid: missing legs"
        elif merged["ratio"] <= 0.85:
            verdict = "implement: >= 15% improvement"
        elif merged["ratio"] <= 0.90:
            verdict = "borderline: 10-15% improvement, do not implement by default"
        else:
            verdict = "stop: < 10% improvement"

    report["verdict"] = verdict
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    (root / ("analysis-%s.json" % arguments.mode)).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    print("\nVERDICT:", verdict)
    if arguments.mode == "candidate":
        return 0
    return 0 if verdict.startswith("pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
