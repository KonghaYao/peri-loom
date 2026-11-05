#!/usr/bin/env bash
# R69 Gate 0: measure F (one operation-journal batch fsync) and A (the gap from
# the previous deferred record submit to the next committing intent submit) for
# W1.insert, then decide whether the intent-barrier reorder can clear 10%.
#
# Usage:
#   WORKER=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7 \
#   PROBE_PLATFORM=/tmp/peri-r69-gate0/bin/peri-platform-probe \
#   ./run_gate0.sh base probe
#
# The base platform is built from the current checkout when BASE_PLATFORM is
# unset. The probe platform must be built from branch probe/r69-intent-gap
# (final commit 7881c38); see probe-branch.txt. Every stack gets a fresh named
# volume and a dedicated port. Connection files stay outside the repository and
# are chmod 600. The scripts never print connection.json or an Authorization
# header.
#
# Environment:
#   REPO            repository root (default: three levels above this script)
#   IMAGE           runtime image (default: ghcr.io/konghayao/peri-loom:v1.2.7)
#   WORKER          linux/arm64 worker binary (required)
#   BASE_PLATFORM   linux/arm64 platform binary built from main
#   PROBE_PLATFORM  linux/arm64 platform binary built from probe/r69-intent-gap
#   OUT             evidence directory (default: /tmp/peri-r69-gate0)
#   BASE_PORT       default 20711
#   PROBE_PORT      default 20713
#   REMOVE_VOLUME   set to 1 to delete the two named volumes at the end
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
OUT=${OUT:-/tmp/peri-r69-gate0}
BASE_PORT=${BASE_PORT:-20711}
PROBE_PORT=${PROBE_PORT:-20713}
WORKER=${WORKER:?WORKER must point at the linux/arm64 peri-loom-v1 binary}
seed_py=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
snap_py=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/snap_metrics.py
mkdir -p "$OUT/bin" "$OUT/base" "$OUT/probe"

if [ -z "${BASE_PLATFORM:-}" ]; then
  BASE_PLATFORM=$OUT/bin/peri-platform-base
  ( cd "$REPO/prototypes/v1/platform" && \
    GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o "$BASE_PLATFORM" ./cmd/peri-platform )
fi
PROBE_PLATFORM=${PROBE_PLATFORM:?PROBE_PLATFORM must point at a binary built from probe/r69-intent-gap}
sha256sum "$BASE_PLATFORM" "$PROBE_PLATFORM" "$WORKER" | tee "$OUT/binaries.txt"

start_stack() { # name volume port platform
  local name=$1 volume=$2 port=$3 platform=$4
  docker volume create "$volume" >/dev/null
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --platform linux/arm64 --network host --init --stop-timeout 60 \
    --security-opt no-new-privileges:true --cap-drop ALL \
    -e PERI_HRANA_EXPERIMENTAL=1 \
    -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform \
    -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 \
    -v "$volume":/data/peri \
    -v "$platform":/usr/local/bin/peri-platform:ro \
    -v "$WORKER":/usr/local/bin/peri-loom-v1:ro \
    "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" --no-build >/dev/null
  for _ in $(seq 1 120); do
    docker exec "$name" test -f /data/peri/connection.json 2>/dev/null && break
    sleep 1
  done
  docker exec "$name" test -f /data/peri/connection.json
  mkdir -p "$OUT/$name"
  docker cp "$name":/data/peri/connection.json "$OUT/$name/connection.json"
  chmod 600 "$OUT/$name/connection.json"
  docker exec "$name" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1 \
    | tee "$OUT/$name/binaries.txt"
}

run_leg() { # name port
  local name=$1 port=$2
  local conn=$OUT/$name/connection.json
  python3 "$seed_py" "$conn" > "$OUT/$name/seed.out" 2>"$OUT/$name/seed.log"
  cat "$OUT/$name/seed.out"
  python3 "$snap_py" "$conn" "$OUT/$name/metrics-before.json"
  python3 "$REPO/scripts/perf-reliability/issue28/run.py" \
    --connection-file "$conn" --container "$name" --out "$OUT/$name" \
    --scenario W1.insert --protocol json-pipeline \
    --rounds 3 --warmup 1 --samples 5 > "$OUT/$name/harness.log" 2>&1
  python3 "$snap_py" "$conn" "$OUT/$name/metrics-after.json"
}

stop_stack() { # name
  docker logs "$1" > "$OUT/$1/container.log" 2>&1 || true
  docker stop -t 60 "$1" >/dev/null 2>&1 || true
  docker rm "$1" >/dev/null 2>&1 || true
}

trap 'stop_stack peri-r69-gate0-base || true; stop_stack peri-r69-gate0-probe || true' EXIT

for stack in "$@"; do
  case "$stack" in
    base)
      start_stack peri-r69-gate0-base peri-r69-gate0-base-data "$BASE_PORT" "$BASE_PLATFORM"
      run_leg peri-r69-gate0-base "$BASE_PORT"
      ;;
    probe)
      start_stack peri-r69-gate0-probe peri-r69-gate0-probe-data "$PROBE_PORT" "$PROBE_PLATFORM"
      run_leg peri-r69-gate0-probe "$PROBE_PORT"
      ;;
    *) echo "unknown stack: $stack" >&2; exit 2 ;;
  esac
done

python3 - "$OUT" <<'PY'
import json, os, statistics, sys
out = sys.argv[1]
def get(d, path):
    for key in path.split('.'):
        d = d[key]
    return d
def q(values, quantile):
    values = sorted(values)
    return values[min(int(quantile * (len(values) - 1)), len(values) - 1)]
for name in ('peri-r69-gate0-base', 'peri-r69-gate0-probe'):
    root = os.path.join(out, name)
    if not os.path.exists(os.path.join(root, 'metrics-after.json')):
        continue
    before = json.load(open(os.path.join(root, 'metrics-before.json')))
    after = json.load(open(os.path.join(root, 'metrics-after.json')))
    syncs = get(after, 'state_operation_cost.operation_journal_syncs_total') - get(before, 'state_operation_cost.operation_journal_syncs_total')
    syncns = get(after, 'state_operation_cost.operation_journal_sync_ns_total') - get(before, 'state_operation_cost.operation_journal_sync_ns_total')
    appends = get(after, 'state_operation_cost.operation_journal_appends_total') - get(before, 'state_operation_cost.operation_journal_appends_total')
    summary = json.load(open(os.path.join(root, 'summary.json')))
    row = [item for item in summary['rows'] if item['scenario'] == 'W1.insert'][0]
    f_ms = syncns / syncs / 1e6
    print('%s: syncs=%d appends=%d syncs/iteration=%.2f F=%.4fms W1.insert p50=%.3fms warnings=%r' % (
        name, syncs, appends, syncs / 18, f_ms, row['median_ms'], summary['warnings']))
    probe = after.get('r69_gap_probe')
    if not probe:
        continue
    before_probe = before['r69_gap_probe']
    run0, run1 = len(before_probe['run_gap_samples_ms']), len(probe['run_gap_samples_ms'])
    last0, last1 = len(before_probe['last_gap_samples_ms']), len(probe['last_gap_samples_ms'])
    int0, int1 = before_probe['intents_total'], probe['intents_total']
    run_gap = probe['run_gap_samples_ms'][run0:run1]
    last_gap = probe['last_gap_samples_ms'][last0:last1]
    waits = probe.get('intent_wait_samples_ms', [])[int0:int1]
    print('  A_run p50=%.3fms (n=%d)  A_last p50=%.3fms (n=%d)' % (
        q(run_gap, .5), len(run_gap), q(last_gap, .5), len(last_gap)))
    if waits:
        print('  intent_wait p50=%.3fms p90=%.3fms (n=%d)' % (q(waits, .5), q(waits, .9), len(waits)))
    w1 = row['median_ms']
    print('  (F-A_run)/W1=%.2f%%  (F-A_last)/W1=%.2f%%  ceiling F/W1=%.2f%%' % (
        (f_ms - q(run_gap, .5)) / w1 * 100, (f_ms - q(last_gap, .5)) / w1 * 100, f_ms / w1 * 100))
PY

if [ "${REMOVE_VOLUME:-0}" = "1" ]; then
  docker volume rm peri-r69-gate0-base-data peri-r69-gate0-probe-data >/dev/null 2>&1 || true
fi
