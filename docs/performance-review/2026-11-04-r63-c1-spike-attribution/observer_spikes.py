"""R63 observer attribution: for every c1 sample slower than 2 ms, print the HTTP envelope spans,
the inner records contained in it, and the inter-record gaps.

Join rule: the harness `request_id` appears only on the `hrana_http` envelope record. Inner
records (open / statement / query / close) carry their own ids, so they are attached by time
containment (`started_unix_ns`/`finished_unix_ns`) inside a serial c1 run.

Usage: python3 observer_spikes.py [leg ...]      (default: all eight legs)
"""

import gzip
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
LEGS = [("o1", "B"), ("o2", "A"), ("o3", "A"), ("o4", "B"),
        ("o5", "B"), ("o6", "A"), ("o7", "A"), ("o8", "B")]
SLOW_MS = 2.0


def c1_samples(leg):
    out = []
    for line in (HERE / "legs" / leg / "raw.jsonl").read_text().splitlines():
        rec = json.loads(line)
        if "request_id" not in rec:
            continue
        if rec.get("concurrency") != 1 or rec.get("phase") != "timed":
            continue
        out.append(rec)
    return out


def observations(leg):
    with gzip.open(HERE / "observer" / f"{leg}.request-phase.jsonl.gz", "rt") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def top_spans(record, limit=4):
    spans = record.get("spans") or []
    spans = sorted(spans, key=lambda s: s["ns"], reverse=True)[:limit]
    return ", ".join(f"{s['phase']}.{s['kind']}={s['ns'] / 1e6:.2f}ms" for s in spans)


def main(selected):
    for leg, side in LEGS:
        if selected and leg not in selected:
            continue
        obs = observations(leg)
        envelopes = {r["request_id"]: r for r in obs
                     if r.get("action") == "hrana_http" and "request_id" in r}
        inners = sorted((r for r in obs if r.get("action") not in (None, "hrana_http", "observer")
                         and "started_unix_ns" in r and "finished_unix_ns" in r),
                        key=lambda r: r["started_unix_ns"])
        print(f"\n## {leg}（{side}）\n")
        slow = [r for r in c1_samples(leg) if r["duration_ms"] > SLOW_MS]
        if not slow:
            print("- 无 >2ms 的 c1 样本")
            continue
        for rec in slow:
            env = envelopes.get(rec["request_id"])
            if env is None:
                print(f"- {rec['duration_ms']:.2f}ms 客户端；无匹配 envelope")
                continue
            start, end = env["started_unix_ns"], env["finished_unix_ns"]
            inside = [r for r in inners if r["started_unix_ns"] >= start and r["finished_unix_ns"] <= end]
            print(f"- 客户端 {rec['duration_ms']:.2f}ms；envelope total_ns={env['total_ns'] / 1e6:.2f}ms；"
                  f"envelope spans: {top_spans(env)}")
            if not inside:
                print(f"  - envelope 内没有内层记录：全部 {(env['total_ns']) / 1e6:.2f}ms 在 handler 之前/之后")
                continue
            cursor = start
            for r in inside:
                gap = (r["started_unix_ns"] - cursor) / 1e6
                print(f"  - {r['action']} {r['total_ns'] / 1e6:.2f}ms（前间隙 {gap:.2f}ms）"
                      f"：{top_spans(r, 3)}")
                cursor = r["finished_unix_ns"]
            print(f"  - 末条之后间隙 {(end - cursor) / 1e6:.2f}ms")

    print("\n注：观察器会放大请求计数并压低吞吐；本文件只用于归因，不作为性能数字。")


if __name__ == "__main__":
    main(set(sys.argv[1:]))
