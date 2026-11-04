#!/usr/bin/env python3
"""R60 attribution: W1 phase split + durability event stats + intent overlap."""
import json, sys, statistics as st

PHASE_DIR = "/tmp/peri-perf/run-r60-w1attr/platform"
REQ = PHASE_DIR + "/request-phase-observation.jsonl"
DUR = PHASE_DIR + "/durability-observation.jsonl"


def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)


def ms(ns):
    return None if ns is None else round(ns / 1e6, 2)


def load(path):
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def req_summary():
    records = load(REQ)
    by_action = {}
    for r in records:
        by_action.setdefault(r["action"], []).append(r)
    print("== request phase ==")
    for action, rows in sorted(by_action.items(), key=lambda kv: -len(kv[1])):
        totals = [r["total_ns"] for r in rows]
        phases = {}
        for r in rows:
            for span in r["spans"]:
                phases.setdefault(span["phase"], []).append(span["ns"])
        top = sorted(((p, st.median(v)) for p, v in phases.items()), key=lambda kv: -kv[1])[:6]
        print(f"{action:12s} n={len(rows):4d} total p50={ms(pct(totals,0.5)):7.2f} "
              f"p95={ms(pct(totals,0.95)):7.2f} max={ms(max(totals)):7.2f} | " +
              ", ".join(f"{p} {ms(v):.2f}" for p, v in top))
    return records


def dur_summary():
    events = load(DUR)
    by_kind = {}
    for e in events:
        if e.get("kind") == "header":
            continue
        by_kind.setdefault(e["kind"], []).append(e)
    print("\n== durability ==")
    windows = {}
    for kind, rows in sorted(by_kind.items()):
        totals = [r.get("total_ns") for r in rows if r.get("total_ns") is not None]
        syncs = [r.get("timings", {}).get("sync_ns") for r in rows
                 if r.get("timings", {}).get("sync_ns") is not None]
        counts = [r.get("count") for r in rows if r.get("count") is not None]
        waits = [r.get("timings", {}).get("wait_ns") for r in rows
                 if r.get("timings", {}).get("wait_ns") is not None]
        queues = [r.get("timings", {}).get("queue_ns") for r in rows
                  if r.get("timings", {}).get("queue_ns") is not None]
        busy = sum(totals) if totals else 0
        extra = ""
        if counts:
            from collections import Counter
            c = Counter(counts)
            extra = " counts=" + ",".join(f"{k}:{v}" for k, v in sorted(c.items(), key=lambda kv: -kv[1])[:4])
        if waits:
            extra += f" wait p50={ms(pct(waits,0.5))} p90={ms(pct(waits,0.9))}"
        if queues:
            extra += f" queue p50={ms(pct(queues,0.5))}"
        def f(v):
            return "None" if v is None else f"{v:7.2f}"
        print(f"{kind:22s} n={len(rows):5d} total p50={f(ms(pct(totals,0.5)) if totals else None)} "
              f"p95={f(ms(pct(totals,0.95)) if totals else None)} max={f(ms(max(totals)) if totals else None)} "
              f"sync p50={f(ms(pct(syncs,0.5)) if syncs else None)} busy={busy/1e9:.3f}s{extra}")
        if kind in ("audit_batch", "journal_batch", "audit_checkpoint"):
            windows[kind] = [(r["started_unix_ns"], r["finished_unix_ns"], r.get("total_ns", 0)) for r in rows]
    return windows


def overlap(records, windows):
    print("\n== statement intent overlap ==")
    stmts = [r for r in records if r["action"] == "statement"]
    spans = []
    for r in stmts:
        cursor = r["started_unix_ns"]
        loc = {}
        for s in r["spans"]:
            loc[s["phase"]] = (cursor, cursor + s["ns"])
            cursor += s["ns"]
        spans.append((r, loc))
    cats = {}
    for r, loc in spans:
        intent = loc.get("intent")
        if not intent:
            continue
        hits = []
        for kind, wins in windows.items():
            for start, end, _ in wins:
                if start < intent[1] and end > intent[0]:
                    hits.append(kind)
                    break
        key = "+".join(sorted(hits)) or "none"
        cats.setdefault(key, []).append(intent[1] - intent[0])
    for key, vals in sorted(cats.items(), key=lambda kv: -len(kv[1])):
        print(f"{key:40s} n={len(vals):3d} intent p50={ms(pct(vals,0.5))} mean={ms(sum(vals)/len(vals))} max={ms(max(vals))}")
    # gap between an open ending and the next statement starting on the same client stream
    print("\n== serial chain (open finish -> statement start, same stream) ==")
    by_stream = {}
    for r in records:
        by_stream.setdefault(r["stream"], []).append(r)
    gaps = []
    for stream, rows in by_stream.items():
        rows.sort(key=lambda r: r["started_unix_ns"])
        for i, r in enumerate(rows):
            if r["action"] != "statement" or i == 0:
                continue
            prev = rows[i - 1]
            if prev["action"] == "open":
                gaps.append(r["started_unix_ns"] - prev["finished_unix_ns"])
    if gaps:
        print(f"n={len(gaps)} gap p50={ms(pct(gaps,0.5))}us? min={min(gaps)} max={max(gaps)} (ns)")


if __name__ == "__main__":
    records = req_summary()
    windows = dur_summary()
    overlap(records, windows)
