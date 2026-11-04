#!/usr/bin/env bash
# R65 A/B leg: fresh stack -> seed fixture -> frozen scenario run -> metrics snapshots -> clean stop.
# Usage: run_leg.sh <scenario> <leg> <port> <platform-binary> <samples>
set -euo pipefail
scenario=$1; leg=$2; port=$3; platform=$4; samples=$5
here=$(cd "$(dirname "$0")" && pwd)
legdir=$here/legs/$scenario/$leg
run=/tmp/peri-perf/run-r65-$scenario-$leg
log=/tmp/peri-perf/run-r65-$scenario-$leg.log

case "$scenario" in
  W1.insert) protocols=(json-pipeline protobuf-pipeline) ;;
  F1.select-1) protocols=(json-pipeline protobuf-pipeline json-cursor protobuf-cursor) ;;
  H1.content-20mb) protocols=(json-cursor protobuf-cursor) ;;
  *) echo "unknown scenario: $scenario" >&2; exit 2 ;;
esac
proto_args=()
for p in "${protocols[@]}"; do proto_args+=(--protocol "$p"); done

pkill -TERM -f -- "--listen 127.0.0.1:$port" || true
sleep 1
rm -rf "$run"
mkdir -p "$legdir"

cd /Users/kk/code/peri-loom
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform" \
  PERI_RUST_BINARY=/tmp/peri-perf/bin/peri-loom-v1-r58-main \
  python3 prototypes/v1/platform/dev.py --run-dir "$run" --listen 127.0.0.1:$port --no-build \
  > "$log" 2>&1 &
launcher=$!
cleanup() { pkill -TERM -f -- "--listen 127.0.0.1:$port" || true; wait $launcher || true; }
trap cleanup EXIT
for i in $(seq 1 90); do [ -f "$run/connection.json" ] && break; sleep 1; done
[ -f "$run/connection.json" ] || { echo "STACK FAILED"; tail -20 "$log"; exit 1; }

python3 "$here/seed_fixture.py" "$run/connection.json" | tail -1
python3 "$here/snap_metrics.py" "$run/connection.json" "$legdir/metrics-before.json"
set +e
python3 scripts/perf-reliability/issue28/run.py \
  --connection-file "$run/connection.json" --out "$legdir" \
  --scenario "$scenario" "${proto_args[@]}" \
  --rounds 1 --warmup 1 --samples "$samples" \
  --concurrency-requests 50 --print-table --allow-misses
status=$?
set -e
python3 "$here/snap_metrics.py" "$run/connection.json" "$legdir/metrics-after.json"
cleanup
trap - EXIT
if [ -f "$run/launcher-stop.json" ]; then
  cp "$run/launcher-stop.json" "$legdir/launcher-stop.json"
else
  echo "launcher-stop.json missing"
fi
python3 - "$run" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1]) / "launcher-stop.json"
if path.exists():
    stop = json.loads(json.loads(path.read_text())["payload"])
    print("stop:", stop["outcome"], "unresolved:", stop["unresolved_operations"])
else:
    print("stop: missing")
PY
echo "$scenario/$leg run_status=$status"
exit $status
