#!/usr/bin/env python3
"""Per-window metric deltas and the session-lifecycle share for R70 Gate 0.

Reads the metrics snapshots and harness summaries written by run_gate0.sh and
prints only the nested keys this round depends on. For the c1 windows the
fixture-discovery delta (a `run.py --dry-run` immediately before the window) is
subtracted because every stock run discovers the fixture before the timed
requests; the c8 driver discovers before it opens its own window, so no
subtraction applies there.

`open` and `close` are platform action buckets. The implicit Hrana open fuses
the first pipeline statement, so the bucket also carries that statement's
execution and result encoding; both buckets also carry admission, audit and
session bookkeeping. The share below is therefore an upper bound on the
worker-native open_session + close_session cost, not that cost itself.
"""
from __future__ import annotations

import argparse
import json
import pathlib


def nested(document: dict, path: str) -> int:
    value: object = document
    for key in path.split("."):
        if not isinstance(value, dict):
            return 0
        value = value.get(key, 0)
    return int(value) if isinstance(value, (int, float)) else 0


def has_actions(document: dict) -> bool:
    return isinstance(document.get("actions_total"), dict)


def action_delta(before: dict, after: dict) -> dict[str, dict[str, int]]:
    counts: dict[str, int] = {}
    totals: dict[str, int] = {}
    actions = set(before.get("actions_total") or {}) | set(after.get("actions_total") or {})
    for action in actions:
        counts[action] = nested(after, "actions_total." + action) - nested(before, "actions_total." + action)
        totals[action] = nested(after, "action_latency_ns_total." + action) - nested(before, "action_latency_ns_total." + action)
    return {"counts": counts, "totals": totals}


def subtract(window: dict, discovery: dict | None) -> dict[str, dict[str, int]]:
    if discovery is None:
        return window
    counts = dict(window["counts"])
    totals = dict(window["totals"])
    for action in set(discovery["counts"]) | set(discovery["totals"]):
        counts[action] = counts.get(action, 0) - discovery["counts"].get(action, 0)
        totals[action] = totals.get(action, 0) - discovery["totals"].get(action, 0)
    return {"counts": counts, "totals": totals}


def action_row(actions: dict[str, dict[str, int]], action: str) -> tuple[int, int, float]:
    count = actions["counts"].get(action, 0)
    total = actions["totals"].get(action, 0)
    mean_us = (total / count / 1000.0) if count else 0.0
    return count, total, mean_us


def load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def summary_row(summary: dict, scenario: str) -> tuple[float, list[float]]:
    for row in summary.get("rows", []):
        if row.get("scenario") == scenario:
            medians = [float(value) for value in (row.get("round_medians_ms") or [])]
            return float(row.get("median_ms") or 0.0), medians
    return 0.0, []


def render(name: str, root: pathlib.Path, scenario: str, discovery: bool,
           requests: int) -> dict:
    before = load(root / "metrics-before.json")
    after = load(root / "metrics-after.json")
    summary = load(root / "summary.json")
    p50_ms, round_medians = summary_row(summary, scenario)

    lines = ["[%s]" % name]
    if not has_actions(before) or not has_actions(after):
        lines.append("  metrics: this run captured only the admission subset; no action delta")
        return {"name": name, "lines": lines, "valid": False, "p50_ms": p50_ms}

    window = action_delta(before, after)
    discovery_delta = None
    if discovery:
        discovery_delta = action_delta(load(root / "metrics-discovery-before.json"),
                                       load(root / "metrics-discovery-after.json"))
    corrected = subtract(window, discovery_delta)

    for label, actions in (("window", window), ("corrected", corrected)):
        for action in ("open", "close", "statement", "query", "describe",
                       "cursor_start", "cursor_open", "cursor_fetch"):
            count, total, mean_us = action_row(actions, action)
            if count == 0 and total == 0:
                continue
            lines.append("  %-9s %-9s count=%d total_ns=%d mean_us=%.3f"
                         % (label, action, count, total, mean_us))

    open_count, _, open_us = action_row(corrected, "open")
    close_count, _, close_us = action_row(corrected, "close")
    raw_open_us = action_row(window, "open")[2]
    raw_close_us = action_row(window, "close")[2]
    session_us = open_us + close_us
    raw_session_us = raw_open_us + raw_close_us
    share = (session_us * 1000.0 / (p50_ms * 1e6) * 100.0) if p50_ms else 0.0
    raw_share = (raw_session_us * 1000.0 / (p50_ms * 1e6) * 100.0) if p50_ms else 0.0
    lines.append("  requests issued=%d (open=%d close=%d)"
                 % (requests, open_count, close_count))
    lines.append("  open_mean_us=%.3f close_mean_us=%.3f session_us=%.3f"
                 % (open_us, close_us, session_us))
    lines.append("  raw_open_us=%.3f raw_close_us=%.3f raw_session_us=%.3f"
                 % (raw_open_us, raw_close_us, raw_session_us))
    lines.append("  %s p50_ms=%.4f round_medians_ms=%s"
                 % (scenario, p50_ms, ",".join("%.3f" % value for value in round_medians) or "-"))
    lines.append("  session_share(corrected) = %.3fus / %.4fms = %.2f%%"
                 % (session_us, p50_ms, share))
    lines.append("  session_share(raw)       = %.3fus / %.4fms = %.2f%%"
                 % (raw_session_us, p50_ms, raw_share))
    return {
        "name": name,
        "lines": lines,
        "valid": True,
        "open_us": open_us,
        "close_us": close_us,
        "session_us": session_us,
        "p50_ms": p50_ms,
        "share": share,
        "raw_share": raw_share,
        "open_count": open_count,
        "close_count": close_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--c8-dir", default="f1-c8b")
    arguments = parser.parse_args()
    root = pathlib.Path(arguments.root)

    windows = [
        render("f1-c1  F1 json-pipeline c1 (primary)", root / "f1-c1", "F1.select-1", True, 18),
        render("f1-c1b F1 json-pipeline c1 (stability)", root / "f1-c1b", "F1.select-1", True, 18),
        render("l1-c1  L1 json-pipeline c1 (primary)", root / "l1-c1", "L1.list-20", True, 18),
        render("l1-c1b L1 json-pipeline c1 (stability)", root / "l1-c1b", "L1.list-20", True, 18),
        render("f1-c8  F1 json-pipeline c8 (run A)", root / "f1-c8", "F1.select-1", False, 144),
        render("f1-c8b F1 json-pipeline c8 (run B, metrics-complete)",
               root / arguments.c8_dir, "F1.select-1", False, 144),
    ]
    output = []
    for window in windows:
        output.extend(window["lines"])
        output.append("")

    output.append("[frozen-formula summary]")
    output.append("  window   p50_ms  session_us  share_corrected  share_raw")
    for window in windows:
        if not window.get("valid"):
            output.append("  %-7s %7.4f  %10s  %15s  %9s"
                          % (window["name"].split()[0], window["p50_ms"], "-", "no metrics", "-"))
            continue
        output.append("  %-7s %7.4f  %10.3f  %14.2f%%  %8.2f%%"
                      % (window["name"].split()[0], window["p50_ms"], window["session_us"],
                         window["share"], window["raw_share"]))
    output.append("")

    final_root = root / arguments.c8_dir
    final = load(final_root / "metrics-after.json")
    output.append("[final account]")
    for key in ("active_permits", "unknown_held_permits", "session_seats"):
        output.append("  %s=%r" % (key, final.get(key)))
    text = "\n".join(output) + "\n"
    (root / "metrics-delta.txt").write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
