#!/usr/bin/env bash
# R63 c1-only leg: fresh stack -> seed -> c1-only X1 (50 req) -> metrics snapshots -> clean stop.
set -euo pipefail
leg=$1; port=$2; platform=$3; label=$4
run=/tmp/peri-perf/run-r63c1-$leg
out=/tmp/peri-perf/out-r63c1-$leg
rm -rf "$run" "$out"; mkdir -p "$out"
cd /Users/kk/code/peri-loom
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform" \
  PERI_RUST_BINARY=/tmp/peri-perf/bin/peri-loom-v1-r58-main \
  python3 prototypes/v1/platform/dev.py --run-dir "$run" --listen 127.0.0.1:$port --no-build \
  > "/tmp/peri-perf/run-r63c1-$leg.log" 2>&1 &
launcher=$!
cleanup() { pkill -TERM -f -- "--listen 127.0.0.1:$port" || true; wait $launcher || true; }
trap cleanup EXIT
for i in $(seq 1 90); do [ -f "$run/connection.json" ] && break; sleep 1; done
[ -f "$run/connection.json" ] || { echo "STACK FAILED"; tail -20 "/tmp/peri-perf/run-r63c1-$leg.log"; exit 1; }
python3 /tmp/peri-perf/seed_fixture.py "$run/connection.json" | tail -1
python3 /tmp/peri-perf/_snap.py "$run/connection.json" "$out/metrics-before.json"
set +e
python3 /tmp/peri-perf/work-c1/issue28/run.py \
  --connection-file "$run/connection.json" --out "$out/results" \
  --scenario X1 --rounds 1 --warmup 0 --samples 1 --concurrency-requests 50 --print-table --allow-misses
status=$?
set -e
python3 /tmp/peri-perf/_snap.py "$run/connection.json" "$out/metrics-after.json"
cleanup
trap - EXIT
echo "$leg label=$label run_status=$status"
python3 - "$run" <<'PY'
import json, pathlib, sys
doc = json.loads((pathlib.Path(sys.argv[1]) / "launcher-stop.json").read_text())
stop = json.loads(doc["payload"])
print("stop:", stop["outcome"], "unresolved:", stop["unresolved_operations"])
PY
