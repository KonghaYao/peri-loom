"""R63 aggregate: reproduce the c1 tail tables and mechanism counters from the committed evidence.

Usage: python3 aggregate.py        (reads ./legs/ and ./legs-c1/ next to this file)
"""

import json
import math
import pathlib
import statistics

HERE = pathlib.Path(__file__).resolve().parent

OBSERVER_LEGS = [
    ("o1", "B"), ("o2", "A"), ("o3", "A"), ("o4", "B"),
    ("o5", "B"), ("o6", "A"), ("o7", "A"), ("o8", "B"),
]
C1_ONLY_LEGS = [
    ("p1", "B"), ("p2", "A"), ("p3", "A"),
    ("p4", "B"), ("p5", "B"), ("p6", "A"),
]


def load_leg(subdir, leg):
    base = HERE / subdir / leg
    summary = json.loads((base / "summary.json").read_text())
    c1 = next(c for c in summary["concurrency"] if c["level"] == 1)
    before = json.loads((base / "metrics-before.json").read_text())
    after = json.loads((base / "metrics-after.json").read_text())
    return c1, before, after


def timed_samples(subdir, leg):
    out = []
    for line in (HERE / subdir / leg / "raw.jsonl").read_text().splitlines():
        rec = json.loads(line)
        if "request_id" not in rec:
            continue
        if rec.get("concurrency") != 1 or rec.get("phase") != "timed":
            continue
        out.append(rec["duration_ms"])
    return out


def tail_table(subdir, legs, title):
    print(f"\n### {title}\n")
    print("| leg | 侧 | p50 | p95 | p99 | max | >2ms |")
    print("|---|---|---:|---:|---:|---:|---:|")
    grouped = {"A": [], "B": []}
    for leg, side in legs:
        c1, _, _ = load_leg(subdir, leg)
        samples = timed_samples(subdir, leg)
        spikes = sum(1 for s in samples if s > 2.0)
        print(f"| {leg} | {side} | {c1['median_ms']:.2f} | {c1['p95_ms']:.2f} | "
              f"{c1['p99_ms']:.2f} | {c1['max_ms']:.2f} | {spikes} |")
        grouped[side].append((c1, spikes))
    for side in ("A", "B"):
        rows = grouped[side]
        print(f"| **{side} 腿中位** | — | "
              f"{statistics.median(r[0]['median_ms'] for r in rows):.2f} | "
              f"{statistics.median(r[0]['p95_ms'] for r in rows):.2f} | "
              f"{statistics.median(r[0]['p99_ms'] for r in rows):.2f} | "
              f"{statistics.median(r[0]['max_ms'] for r in rows):.2f} | "
              f"{statistics.median(r[1] for r in rows)} |")
    for side in ("A", "B"):
        pooled = sorted(s for leg, sd in legs if sd == side for s in timed_samples(subdir, leg))
        n = len(pooled)

        def rank(p):
            return pooled[min(n - 1, max(0, math.ceil(p * n) - 1))]

        print(f"- 合并 {side}：n={n} p50={rank(0.5):.2f} p95={rank(0.95):.2f} "
              f"p99={rank(0.99):.2f} max={pooled[-1]:.2f} "
              f">2ms={sum(1 for s in pooled if s > 2)}")


def mechanism_table(subdir, legs, title):
    print(f"\n### {title}\n")
    print("| leg | 侧 | journal syncs | journal 等待合计 | journal 等待 max | "
          "audit fsyncs | audit fsync 均值 | audit fsync max | head checkpoint | "
          "`pending_max` | `inline_total` |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    grouped = {"A": [], "B": []}
    for leg, side in legs:
        _, before, after = load_leg(subdir, leg)
        cost_b, cost_a = before["state_operation_cost"], after["state_operation_cost"]
        audit_b, audit_a = before["audit"], after["audit"]
        syncs = cost_a["operation_journal_syncs_total"] - cost_b["operation_journal_syncs_total"]
        wait = cost_a["operation_journal_wait_ns_total"] - cost_b["operation_journal_wait_ns_total"]
        wait_max = cost_a["operation_journal_wait_max_ns"]
        fsyncs = audit_a["fsync_count"] - audit_b["fsync_count"]
        fsync_total = audit_a["fsync_ns_total"] - audit_b["fsync_ns_total"]
        fsync_max = audit_a["fsync_ns_max"]
        checkpoint = (audit_a["head_checkpoint_attempts_total"]
                      - audit_b["head_checkpoint_attempts_total"])
        deferred = after.get("deferred_durability", {})
        pending_max = deferred.get("pending_max", "—")
        inline = deferred.get("inline_total", "—")
        print(f"| {leg} | {side} | {syncs} | {wait / 1e9:.2f}s | {wait_max / 1e6:.1f}ms | "
              f"{fsyncs} | {(fsync_total / fsyncs / 1e6 if fsyncs else 0):.2f}ms | "
              f"{fsync_max / 1e6:.1f}ms | {checkpoint} | {pending_max} | {inline} |")
        grouped[side].append((syncs, wait, wait_max, fsyncs, fsync_total, fsync_max, checkpoint))
    for side in ("A", "B"):
        rows = grouped[side]
        med = lambda i: statistics.median(r[i] for r in rows)  # noqa: E731
        print(f"| **{side} 腿中位** | — | {med(0)} | {med(1) / 1e9:.2f}s | {med(2) / 1e6:.1f}ms | "
              f"{med(3)} | {(med(4) / med(3) / 1e6 if med(3) else 0):.2f}ms | "
              f"{med(5) / 1e6:.1f}ms | {med(6)} | — | — |")


tail_table("legs", OBSERVER_LEGS, "观察器腿 o1–o8（X1 1/4/8/16，每级 50 请求）的 c1 单元")
mechanism_table("legs", OBSERVER_LEGS, "观察器腿机制计数器（本轮窗口前后快照之差）")
tail_table("legs-c1", C1_ONLY_LEGS, "c1-only 腿 p1–p6（X1 仅 c1，50 请求）")
mechanism_table("legs-c1", C1_ONLY_LEGS, "c1-only 腿机制计数器（本轮窗口前后快照之差）")
