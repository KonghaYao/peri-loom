import json, glob, os, statistics

base = "/tmp/peri-perf/r46f1"
by = {}
for d in sorted(glob.glob(base + "/*/")):
    n = os.path.basename(d.rstrip("/"))
    rnd, sideleg = n.split("-", 1)
    side = sideleg.rstrip("1234")
    leg = sideleg[len(side):]
    s = json.load(open(d + "summary.json"))
    r = s["rows"][0]
    by.setdefault(rnd, {}).setdefault(side, []).append((leg, r["median_ms"], r["p95_ms"], r["response_bytes"], r["status_counts"], s.get("warnings")))

for rnd in sorted(by):
    b = sorted(by[rnd]["base"])
    c = sorted(by[rnd]["cand"])
    bm = statistics.median([x[1] for x in b])
    cm = statistics.median([x[1] for x in c])
    print(rnd, "base", [round(x[1], 3) for x in b], "cand", [round(x[1], 3) for x in c],
          "delta %+.2f%%" % ((cm - bm) / bm * 100))
allb = [x[1] for r in by.values() for x in r["base"]]
allc = [x[1] for r in by.values() for x in r["cand"]]
print("all base median", statistics.median(allb), "all cand median", statistics.median(allc),
      "delta %+.2f%%" % ((statistics.median(allc) - statistics.median(allb)) / statistics.median(allb) * 100))
print("bytes/status/warnings:", set((x[3], tuple(sorted(x[4].items())), tuple(x[5] or [])) for r in by.values() for side in r.values() for x in side))
