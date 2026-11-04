#!/usr/bin/env python3
"""Summarize the R56+R57 batch A/B/B/A legs.

Hard gate (frozen in criteria.txt): per round, take the median of the two legs'
median_ms for each side and protocol; pass when candidate <= 0.90 * baseline for
both protocols, same direction across both rounds.
"""
from __future__ import annotations

import json
import pathlib
import statistics
import sys

ROOT = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/peri-perf/batchab")
PROTOCOLS = ("json-cursor", "protobuf-cursor")
EXPECTED_BYTES = {"json-cursor": 20608525, "protobuf-cursor": 20272006}


def main() -> int:
    data: dict[tuple[int, str, str], list[float]] = {}
    meta: dict[tuple[int, str, str], dict] = {}
    for path in sorted(ROOT.glob("r*-*/summary.json")):
        # r{round}-{side}{leg}-H1.content-20mb-{protocol}
        round_no = int(path.parent.name[1])
        side = "base" if path.parent.name.split("-")[1].startswith("base") else "cand"
        document = json.loads(path.read_text())
        row = document["rows"][0]
        protocol = row["protocol"]
        data.setdefault((round_no, side, protocol), []).append(row["median_ms"])
        meta[(round_no, side, protocol)] = {
            "bytes": row["response_bytes"],
            "rows": row["rows"],
            "status": row["status_counts"],
            "outcomes": row["outcomes"],
            "warnings": document["warnings"],
            "fatal": document["fatal"],
            "fixture": document["fixture"],
        }
        print(
            "round=%d side=%s protocol=%s median=%.3fms bytes=%d rows=%d %s warnings=%s"
            % (
                round_no,
                side,
                protocol,
                row["median_ms"],
                row["response_bytes"],
                row["rows"],
                row["status_counts"],
                document["warnings"],
            )
        )

    print("\n== per-round side medians (median of the two legs) ==")
    passed_all = True
    for round_no in (1, 2):
        for protocol in PROTOCOLS:
            base = statistics.median(data[(round_no, "base", protocol)])
            cand = statistics.median(data[(round_no, "cand", protocol)])
            ratio = cand / base
            ok = ratio <= 0.90
            passed_all = passed_all and ok
            print(
                "round=%d %-15s base=%.3fms cand=%.3fms ratio=%.4f improvement=%.2f%% -> %s"
                % (
                    round_no,
                    protocol,
                    base,
                    cand,
                    ratio,
                    (1.0 - ratio) * 100.0,
                    "PASS" if ok else "FAIL",
                )
            )
    print("\nhard gate:", "PASS" if passed_all else "FAIL")

    print("\n== invariants ==")
    for key, info in sorted(meta.items()):
        marker = "OK" if info["bytes"] == EXPECTED_BYTES[key[2]] else "BYTES-DIFF"
        if info["warnings"] or info["fatal"]:
            marker += " WARNINGS"
        fixture = info["fixture"]
        print(
            "round=%d side=%-4s %-15s bytes=%d %s rows=%d status=%s outcomes=%s warnings=%s fatal=%r fixture(t/m/c/l)=%s/%s/%s/%s"
            % (
                key[0],
                key[1],
                key[2],
                info["bytes"],
                marker,
                info["rows"],
                info["status"],
                info["outcomes"],
                info["warnings"],
                info["fatal"],
                fixture.get("threads"),
                fixture.get("messages"),
                fixture.get("content_chars"),
                fixture.get("list_threads"),
            )
        )
    return 0 if passed_all else 1


if __name__ == "__main__":
    raise SystemExit(main())
