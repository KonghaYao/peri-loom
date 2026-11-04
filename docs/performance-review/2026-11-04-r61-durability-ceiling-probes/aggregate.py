#!/usr/bin/env python3
"""Aggregate the R61 ceiling-probe legs into the tables used by README.md."""
from __future__ import annotations

import argparse
import json
import pathlib

W1_LEGS = ["w1-a1", "w1-b1", "w1-b2", "w1-a2"]
X1_LEGS = ["x1-c1", "x1-d1", "x1-d2", "x1-c2"]


def load(root: pathlib.Path, leg: str) -> dict:
    return json.loads((root / leg / "summary.json").read_text())


def w1(root: pathlib.Path) -> None:
    print("## W1.insert json-pipeline (A1 baseline, B1/B2 intent probe, A2 baseline)")
    values = {}
    for leg in W1_LEGS:
        document = load(root, leg)
        row = document["rows"][0]
        values[leg] = row
        print(f"{leg}: median={row['median_ms']:.2f}ms "
              f"rounds={[round(value, 2) for value in row['round_medians_ms']]} "
              f"samples={row['samples']} warnings={document['warnings']}")
    a1, b1, b2, a2 = (values[leg]["median_ms"] for leg in W1_LEGS)
    print(f"paired A1→B1: {(b1 / a1 - 1) * 100:+.1f}%   A2→B2: {(b2 / a2 - 1) * 100:+.1f}%")
    print(f"absolute drop: {a1 - b1:.2f}ms / {a2 - b2:.2f}ms")


def x1(root: pathlib.Path, legs: list[str]) -> None:
    print(f"## X1 json-pipeline ({', '.join(legs)})")
    data = {}
    for leg in legs:
        document = load(root, leg)
        data[leg] = {row["level"]: row for row in document["concurrency"]}
        summary = " ".join(
            f"c{row['level']}={row['median_ms']:.2f}/{row['p95_ms']:.1f}/{row['p99_ms']:.1f}"
            for row in document["concurrency"])
        print(f"{leg}: p50/p95/p99 {summary} warnings={document['warnings']}")
    if len(legs) == 4:
        labels = ["A1", "B1", "B2", "A2"]
        print("level | " + " | ".join(f"{label} p50/p99" for label in labels))
        for level in (1, 4, 8, 16):
            cells = [f"{data[leg][level]['median_ms']:.2f}/{data[leg][level]['p99_ms']:.1f}"
                     for leg in legs]
            print(f"c{level} | " + " | ".join(cells))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default="legs")
    parser.add_argument("--x1-legs", nargs="*", default=X1_LEGS)
    parser.add_argument("--only-x1", action="store_true")
    arguments = parser.parse_args()
    root = pathlib.Path(arguments.root)
    if not arguments.only_x1:
        w1(root)
        print()
    x1(root, arguments.x1_legs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
