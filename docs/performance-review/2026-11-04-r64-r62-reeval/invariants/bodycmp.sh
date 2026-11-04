#!/usr/bin/env bash
set -uo pipefail
run=$1; port=$2; platform=$3; out=$4
rm -rf "$run"; mkdir -p "$(dirname "$out")"
cd /Users/kk/code/peri-loom
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform" \
  PERI_RUST_BINARY=/tmp/peri-perf/bin/peri-loom-v1-r58-main \
  python3 prototypes/v1/platform/dev.py --run-dir "$run" --listen 127.0.0.1:$port --no-build \
  > "/tmp/peri-perf/bodycmp-$port.log" 2>&1 &
launcher=$!
cleanup() { pkill -TERM -f -- "--listen 127.0.0.1:$port" || true; wait $launcher 2>/dev/null || true; }
trap cleanup EXIT
for i in $(seq 1 90); do [ -f "$run/connection.json" ] && break; sleep 1; done
[ -f "$run/connection.json" ] || { echo "STACK FAILED"; tail -5 "/tmp/peri-perf/bodycmp-$port.log"; exit 1; }
python3 /tmp/peri-perf/bodycmp.py "$run/connection.json" "$out"
cleanup
trap - EXIT
