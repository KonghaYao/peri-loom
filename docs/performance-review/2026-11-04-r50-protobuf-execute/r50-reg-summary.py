import json, glob, os, statistics

base = "/tmp/peri-perf/r50reg"
data = {}
anom = []
for d in sorted(glob.glob(base + "/*/")):
    n = os.path.basename(d.rstrip("/"))
    rest, sideleg = n.rsplit(".", 1)
    scen, proto = rest.rsplit(".", 1)
    side = sideleg.rstrip("1234")
    s = json.load(open(d + "summary.json"))
    rows = s.get("rows") or []
    if not rows:
        conc = s.get("concurrency") or []
        sample = dict(round=sideleg, median=None, p95=None, samples=None, bytes=None,
                      rows=None, status={}, err={}, verdict="x1",
                      levels=[(c.get("concurrency"), c.get("median_ms"), c.get("status_counts"), c.get("error_codes")) for c in conc])
        data.setdefault((scen, proto), {}).setdefault(side, []).append(sample)
        if s.get("warnings") or s.get("fatal"):
            anom.append((n, s.get("warnings"), s.get("fatal")))
        continue
    r = rows[0]
    data.setdefault((scen, proto), {}).setdefault(side, []).append(
        dict(round=sideleg, median=r["median_ms"], p95=r["p95_ms"], samples=r["samples"],
             bytes=r["response_bytes"], rows=r["rows"], status=r["status_counts"],
             err=r["error_codes"], verdict=r["verdict"])
    )
    if s.get("warnings") or s.get("fatal") or any(int(k) != 200 for k in r["status_counts"]):
        anom.append((n, s.get("warnings"), s.get("fatal"), r["status_counts"]))

for key in sorted(data):
    b = sorted(data[key].get("base", []), key=lambda x: x["round"])
    c = sorted(data[key].get("cand", []), key=lambda x: x["round"])
    if not b or not c:
        print("INCOMPLETE", key)
        continue
    if b[0].get("levels"):
        print("%-24s %-16s X1 levels (conc,median_ms):" % key)
        for x in b + c:
            print("   ", x["round"], [(l[0], round(l[1], 2)) for l in x["levels"]])
        continue
    bm = statistics.median([x["median"] for x in b])
    cm = statistics.median([x["median"] for x in c])
    print("%-24s %-16s base=%8.2f legs=%s cand=%8.2f legs=%s delta=%+7.2f%%" % (
        key[0], key[1], bm, [round(x["median"], 2) for x in b],
        cm, [round(x["median"], 2) for x in c], (cm - bm) / bm * 100))
print("--- anomalies ---")
for a in anom:
    print(a)
