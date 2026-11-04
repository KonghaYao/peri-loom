#!/usr/bin/env bash
# R61 measurement leg: fresh stack -> seed fixture -> run one scenario -> clean stop.
set -euo pipefail
leg=$1; port=$2; platform=$3; shift 3
run=/tmp/peri-perf/run-r61-$leg
rm -rf "$run"
cd /Users/kk/code/peri-loom
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform" \
  PERI_RUST_BINARY=/tmp/peri-perf/bin/peri-loom-v1-r58-main \
  python3 prototypes/v1/platform/dev.py --run-dir "$run" --listen 127.0.0.1:$port --no-build \
  > "/tmp/peri-perf/run-r61-$leg.log" 2>&1 &
launcher=$!
cleanup() { pkill -TERM -f -- "--listen 127.0.0.1:$port" || true; wait $launcher || true; }
trap cleanup EXIT
for i in $(seq 1 90); do [ -f "$run/connection.json" ] && break; sleep 1; done
if [ ! -f "$run/connection.json" ]; then
  echo "STACK FAILED $leg"; tail -20 "/tmp/peri-perf/run-r61-$leg.log"; exit 1
fi
python3 /tmp/peri-perf/seed_fixture.py "$run/connection.json" | tail -1
python3 scripts/perf-reliability/issue28/run.py \
  --connection-file "$run/connection.json" --out "/tmp/peri-perf/out-r61-$leg" \
  "$@" --print-table --allow-misses
cleanup
trap - EXIT
python3 - "$run" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1]) / "launcher-stop.json"
doc = json.loads(path.read_text())
stop = json.loads(doc["payload"])
print("stop:", stop["outcome"], "unresolved:", stop["unresolved_operations"])
PY
