#!/usr/bin/env python3
"""Summarize the frozen R62 A/B legs (X1, 50 requests per level)."""
from __future__ import annotations

import argparse
import json
import pathlib
import statistics

ROUNDS = {"1": ("r1a1", "r1b1", "r1b2", "r1a2"), "2": ("r2a1", "r2b1", "r2b2", "r2a2")}
LEVELS = (1, 4, 8, 16)
METRICS = ("median_ms", "p95_ms", "p99_ms", "max_ms")


def load(root: pathlib.Path, leg: str):
    base = root / ("out-r62-" + leg)
    summary = json.loads((base / "results" / "summary.json").read_text())
    before = json.loads((base / "metrics-before.json").read_text())
    after = json.loads((base / "metrics-after.json").read_text())
    stop = json.loads((root / ("run-r62-" + leg) / "launcher-stop.json").read_text())
    return {
        "summary": summary,
        "before": before,
        "after": after,
        "stop": json.loads(stop["payload"]),
    }


def flatten(doc):
    out = {}
    for key, value in doc.items():
        if isinstance(value, dict):
            out.update({key + "." + k: v for k, v in flatten(value).items()})
        else:
            out[key] = value
    return out


def counter(flat, name):
    return flat.get(name, 0) or 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/tmp/peri-perf")
    parser.add_argument("--json-out")
    args = parser.parse_args()
    root = pathlib.Path(args.root)

    legs = {}
    for round_name, names in ROUNDS.items():
        for index, leg in enumerate(names):
            role = "A" if index in (0, 3) else "B"
            legs[leg] = (round_name, role, load(root, leg))

    report = {"legs": {}, "rounds": {}, "pooled": {}, "guards": {}}

    for leg, (round_name, role, data) in legs.items():
        levels = {}
        for item in data["summary"]["concurrency"]:
            levels[str(item["level"])] = {
                key: item.get(key) for key in (*METRICS, "errors", "status_429",
                                               "rejection_reasons", "requests", "verdict")
            }
        after = flatten(data["after"])
        report["legs"][leg] = {
            "round": round_name,
            "role": role,
            "levels": levels,
            "deferred": {
                key: after.get("deferred_durability." + key)
                for key in ("pending", "pending_max", "maximum", "enqueued_total",
                            "inline_total")
            },
            "audit": {
                "queue_count": after.get("audit.queue_count"),
                "queue_age_ms": after.get("audit.queue_age_ms"),
                "fsync_count": after.get("audit.fsync_count"),
                "head_checkpoint_attempts_total": after.get("audit.head_checkpoint_attempts_total"),
            },
            "stop": {
                "outcome": data["stop"].get("outcome"),
                "unresolved_operations": data["stop"].get("unresolved_operations"),
            },
        }

    def level_item(data, level):
        return next(item for item in data["summary"]["concurrency"]
                    if item["level"] == level)

    def side_median(round_name, role, level, metric):
        values = [level_item(legs[leg][2], level)[metric]
                  for leg in ROUNDS[round_name] if legs[leg][1] == role]
        return statistics.median(values)

    def pooled_median(role, level, metric):
        values = [level_item(data, level)[metric]
                  for _, leg_role, data in legs.values() if leg_role == role]
        return statistics.median(values)

    for round_name in ROUNDS:
        report["rounds"][round_name] = {}
        for level in LEVELS:
            report["rounds"][round_name][str(level)] = {}
            for metric in ("median_ms", "p95_ms", "p99_ms"):
                a = side_median(round_name, "A", level, metric)
                b = side_median(round_name, "B", level, metric)
                report["rounds"][round_name][str(level)][metric] = {
                    "A": a, "B": b, "change": (b / a - 1.0) if a else None,
                }

    report["pooled"] = {}
    for level in LEVELS:
        report["pooled"][str(level)] = {}
        for metric in ("median_ms", "p95_ms", "p99_ms"):
            a = pooled_median("A", level, metric)
            b = pooled_median("B", level, metric)
            report["pooled"][str(level)][metric] = {
                "A": a, "B": b, "change": (b / a - 1.0) if a else None,
            }

    errors = sum(item.get("errors", 0) or 0
                 for _, _, data in legs.values() for item in data["summary"]["concurrency"])
    rejected = sum(item.get("status_429", 0) or 0
                   for _, _, data in legs.values() for item in data["summary"]["concurrency"])
    reasons = {}
    for _, _, data in legs.values():
        for item in data["summary"]["concurrency"]:
            for reason, count in (item.get("rejection_reasons") or {}).items():
                reasons[reason] = reasons.get(reason, 0) + count
    inline = {leg: report["legs"][leg]["deferred"]["inline_total"]
              for leg, (_, role, _) in legs.items() if role == "B"}
    report["guards"] = {
        "errors": errors,
        "status_429": rejected,
        "rejection_reasons": reasons,
        "candidate_inline_total": inline,
        "clean_stops": all(item["stop"]["outcome"] == "clean"
                           and item["stop"]["unresolved_operations"] is False
                           for item in report["legs"].values()),
    }

    print(json.dumps(report, indent=1, sort_keys=True, ensure_ascii=False))
    if args.json_out:
        pathlib.Path(args.json_out).write_text(
            json.dumps(report, indent=1, sort_keys=True, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
