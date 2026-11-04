"""R64 invariant check: raw Hrana JSON bodies of the same SELECT 1 pipeline, one file per binary.

Usage: python3 /tmp/peri-perf/bodycmp.py <connection.json> <out.json>
"""

import json
import pathlib
import sys

sys.path.insert(0, "/Users/kk/code/peri-loom/scripts/perf-reliability/issue28")

import hrana  # noqa: E402

target = hrana.Target.from_file(sys.argv[1])
out = pathlib.Path(sys.argv[2])
client = hrana.HranaClient(target)
requests = [
    {"type": "execute", "sql": "SELECT 1", "want_rows": True, "args": ()},
    {"type": "close"},
]
bodies = []
for _ in range(5):
    raw = client.pipeline_json(requests)
    bodies.append({"status": raw.status, "body": raw.body.decode("utf-8", "replace")})
client.close()
out.write_text(json.dumps(bodies, indent=1))
print("wrote", len(bodies), "bodies to", out)
