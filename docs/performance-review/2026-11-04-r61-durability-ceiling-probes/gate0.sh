#!/usr/bin/env bash
# R61 Gate 0: one fresh stack, seed, one X1 run, dump the audit metrics, clean stop.
set -euo pipefail
name=$1; port=$2; platform=$3
run=/tmp/peri-perf/run-r61-$name
rm -rf "$run"
cd /Users/kk/code/peri-loom
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform" \
  PERI_RUST_BINARY=/tmp/peri-perf/bin/peri-loom-v1-r58-main \
  python3 prototypes/v1/platform/dev.py --run-dir "$run" --listen 127.0.0.1:$port --no-build \
  > "/tmp/peri-perf/run-r61-$name.log" 2>&1 &
launcher=$!
cleanup() { pkill -TERM -f -- "--listen 127.0.0.1:$port" || true; wait $launcher || true; }
trap cleanup EXIT
for i in $(seq 1 90); do [ -f "$run/connection.json" ] && break; sleep 1; done
if [ ! -f "$run/connection.json" ]; then echo "STACK FAILED $name"; tail -20 "/tmp/peri-perf/run-r61-$name.log"; exit 1; fi
python3 /tmp/peri-perf/seed_fixture.py "$run/connection.json" | tail -1
python3 scripts/perf-reliability/issue28/run.py --connection-file "$run/connection.json" \
  --out "/tmp/peri-perf/out-r61-$name" --scenario X1 --protocol json-pipeline \
  --concurrency-requests 100 2>&1 | grep 'X1 concurrency'
python3 - "$run" > "/tmp/peri-perf/$name-metrics.json" <<'PY'
import json, sys, urllib.request
doc = json.load(open(sys.argv[1] + '/connection.json'))
request = urllib.request.Request(doc['endpoint'].rstrip('/') + '/v1/metrics',
                                 headers={'Authorization': 'Bearer ' + doc['global_api_key']})
print(json.dumps(json.load(urllib.request.urlopen(request))['result']['audit'], indent=1, sort_keys=True))
PY
cleanup
trap - EXIT
echo "metrics: /tmp/peri-perf/$name-metrics.json"
