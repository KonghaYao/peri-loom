import glob
import json
import os
import re
import statistics

BASE = "/tmp/peri-perf/r49ab"
SCEN = {
    "R1-json": ("R1.limit-1000", "json-pipeline"),
    "H1-json-cursor": ("H1.content-20mb", "json-cursor"),
    "H1-protobuf-cursor": ("H1.content-20mb", "protobuf-cursor"),
    "F1-json": ("F1.select-1", "json-pipeline"),
}
data = {}
anomalies = []
for path in sorted(glob.glob(BASE + "/*/")):
    name = os.path.basename(path.rstrip("/"))
    matched = re.match(r"(r\d)-(base|cand)(\d)-(.*)", name)
    if not matched:
        anomalies.append((name, "unparsed"))
        continue
    round_name, side, leg_no, tag = matched.groups()
    summary = json.load(open(path + "summary.json"))
    rows = summary.get("rows") or []
    if not rows:
        anomalies.append((name, "no rows"))
        continue
    row = rows[0]
    data.setdefault((tag, round_name), {}).setdefault(side, []).append(
        (int(leg_no), row["median_ms"], row["p95_ms"], row["response_bytes"],
         row["rows"], row["status_counts"], row["error_codes"], row["verdict"])
    )
    if summary.get("warnings") or summary.get("fatal") or any(int(k) != 200 for k in row["status_counts"]):
        anomalies.append((name, summary.get("warnings"), summary.get("fatal"), row["status_counts"], row["error_codes"]))

print("=== per-leg ===")
for (tag, round_name) in sorted(data):
    for side in ("base", "cand"):
        for legs in [sorted(data[(tag, round_name)].get(side, []))]:
            for leg in legs:
                print("%-20s %s %-4s leg%d median=%8.3f p95=%8.3f bytes=%s rows=%s status=%s err=%s verdict=%s" % (
                    tag, round_name, side, leg[0], leg[1], leg[2], leg[3], leg[4], leg[5], leg[6], leg[7]))

print()
print("=== per-round ===")
for tag in SCEN:
    for round_name in ("r1", "r2"):
        base = sorted(data.get((tag, round_name), {}).get("base", []))
        cand = sorted(data.get((tag, round_name), {}).get("cand", []))
        if not base or not cand:
            print("INCOMPLETE", tag, round_name)
            continue
        bm = statistics.median([x[1] for x in base])
        cm = statistics.median([x[1] for x in cand])
        paired = []
        for index in range(min(len(base), len(cand))):
            b = base[index][1]
            c = cand[index][1]
            paired.append((c - b) / b * 100.0)
        print("%-20s %s base_median=%8.3f cand_median=%8.3f delta=%+7.2f%% paired=%s" % (
            tag, round_name, bm, cm, (cm - bm) / bm * 100.0,
            ["%+.2f%%" % value for value in paired]))

print()
print("=== anomalies ===")
for item in anomalies:
    print(item)
