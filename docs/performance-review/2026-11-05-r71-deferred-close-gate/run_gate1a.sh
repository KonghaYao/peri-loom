#!/usr/bin/env bash
# R71 Gate 1a: measure the `native` phase of the client `/v1/sql/close` action.
#
# Protocol (frozen before the first window):
#   F1 json-pipeline c1, rounds=3 warmup=1 samples=5 (the standard F1 shape).
#   The request-phase profile is on with threshold 0 and sample 1000, so every
#   request in the timed window is written; the observer is a diagnostic and its
#   absolute durations are NOT end-to-end performance.
#   Records written before the timed window (fixture seed, harness dry-run) are
#   excluded with an append-offset marker taken right before the timed run.
#   Decision input: p50 of the `native` span on `action=close` records.
#     native p50 <  60us  -> stop after recording Gate 1a
#                60-80us  -> record, skip Gate 1b (ambiguous band)
#                >= 80us  -> Gate 1b may run
#
# Isolated stack: fresh named volume, dedicated container name, dedicated port.
# Nothing here touches the production `peri-loom` container or `peri-loom-data`,
# and nothing is pruned. The connection file stays under $STATE and is chmod
# 600; it is never printed and never copied into the repository.
#
# Usage:
#   ./run_gate1a.sh
# Environment:
#   REPO      repository root (default: three levels above this script)
#   IMAGE     runtime image (default: ghcr.io/konghayao/peri-loom:v1.2.7)
#   PLATFORM  linux/arm64 platform binary (default: v1.2.7 release artifact)
#   WORKER    linux/arm64 worker binary (default: v1.2.7 release artifact)
#   OUT       evidence directory (default: this script's directory)
#   STATE     private state + connection file (default: /tmp/peri-r71-gate1a)
#   PORT      default 20771
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/peri-release-ab/bin/peri-platform-v1.2.7}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
OUT=${OUT:-$here}
STATE=${STATE:-/tmp/peri-r71-gate1a}
CONTAINER=${CONTAINER:-peri-r71-gate1a}
VOLUME=${VOLUME:-peri-r71-gate1a-data}
PORT=${PORT:-20771}

RUN=$REPO/scripts/perf-reliability/issue28/run.py
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
SNAP=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/snap_metrics.py
CONN=$STATE/connection.json
LEG=$OUT/legs/f1-c1
PHASE_IN_CONTAINER=/data/peri/platform/request-phase-observation.jsonl

mkdir -p "$STATE" "$OUT" "$LEG"

snap() { python3 "$SNAP" "$CONN" "$1"; }

stop_stack() {
  docker logs "$CONTAINER" > "$OUT/container.log" 2>&1 || true
  docker stop -t 60 "$CONTAINER" >/dev/null 2>&1 || true
  docker rm "$CONTAINER" >/dev/null 2>&1 || true
}
trap 'stop_stack' EXIT

# -- evidence and guard rails -------------------------------------------------
{
  echo "R71 Gate 1a: close native phase"
  echo
  echo "Host: $(uname -srm), $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unknown)"
  echo "Image: $IMAGE"
  docker image inspect "$IMAGE" --format '  image id: {{.Id}}'
  echo
  echo "Platform"
  echo "  path:   $PLATFORM"
  echo "  sha256: $(shasum -a 256 "$PLATFORM" | awk '{print $1}')"
  echo "  source: v1.2.7 release artifact (95716e4d...)"
  echo
  echo "Worker"
  echo "  path:   $WORKER"
  echo "  sha256: $(shasum -a 256 "$WORKER" | awk '{print $1}')"
  echo "  source: v1.2.7 release artifact (b17a0708...)"
  echo
  echo "Stack"
  echo "  container: $CONTAINER"
  echo "  volume:    $VOLUME (fresh)"
  echo "  port:      $PORT"
  echo "  harness:   $RUN"
  echo "  params:    F1 json-pipeline c1 rounds=3 warmup=1 samples=5"
  echo "  observer:  PERI_SQL_OPEN_OBSERVER=1 (--sql-open-observer),"
  echo "             PHASE_THRESHOLD_MS=0, PHASE_SAMPLE_PERMILLE=1000"
  echo "  quotas:    platform defaults (not relaxed)"
} > "$OUT/binaries.txt" 2>&1

# -- isolated stack -----------------------------------------------------------
docker volume create "$VOLUME" >/dev/null
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" --platform linux/arm64 --network host --init --stop-timeout 60 \
  --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 \
  -e PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS=0 \
  -e PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE=1000 \
  -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform \
  -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 \
  -v "$VOLUME":/data/peri \
  -v "$PLATFORM":/usr/local/bin/peri-platform:ro \
  -v "$WORKER":/usr/local/bin/peri-loom-v1:ro \
  "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$PORT" \
  --no-build --sql-open-observer >/dev/null

for _ in $(seq 1 120); do
  docker exec "$CONTAINER" test -f /data/peri/connection.json 2>/dev/null && break
  sleep 1
done
docker exec "$CONTAINER" test -f /data/peri/connection.json
docker cp "$CONTAINER":/data/peri/connection.json "$CONN"
chmod 600 "$CONN"
docker exec "$CONTAINER" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1 \
  | tee -a "$OUT/binaries.txt"
# The observer file must exist before the first measurement or the profile did
# not start; failing here is cheaper than discovering a silent default-off run.
docker exec "$CONTAINER" test -f "$PHASE_IN_CONTAINER"

# -- fixture ------------------------------------------------------------------
python3 "$SEED" "$CONN" > "$OUT/seed-verify.txt" 2> "$OUT/seed.log"
cat "$OUT/seed-verify.txt"
snap "$OUT/metrics-after-seed.json"

# -- timed window -------------------------------------------------------------
# Discovery (fixture metadata + workload discovery) runs before the offset is
# taken, so only the F1 window's own requests enter the phase distribution.
snap "$LEG/metrics-discovery-before.json"
python3 "$RUN" --connection-file "$CONN" --container "$CONTAINER" \
  --scenario F1 --protocol json-pipeline --dry-run \
  > "$LEG/discovery.log" 2>&1
snap "$LEG/metrics-discovery-after.json"
sleep 2

OFFSET=$(docker exec "$CONTAINER" stat -c %s "$PHASE_IN_CONTAINER")
echo "$OFFSET" > "$LEG/phase-offset-bytes.txt"

snap "$LEG/metrics-before.json"
set +e
python3 "$RUN" --connection-file "$CONN" --container "$CONTAINER" \
  --scenario F1 --protocol json-pipeline --rounds 3 --warmup 1 --samples 5 \
  --out "$LEG" --print-table > "$LEG/harness.log" 2>&1
echo "$?" > "$LEG/exit-code.txt"
set -e
snap "$LEG/metrics-after.json"

# The sink drains on a 10ms ticker; two seconds leaves no request in flight.
sleep 2
docker cp "$CONTAINER":"$PHASE_IN_CONTAINER" "$LEG/request-phase-observation.jsonl"
docker exec "$CONTAINER" stat -c %s "$PHASE_IN_CONTAINER" > "$LEG/phase-end-bytes.txt"

# -- summary ------------------------------------------------------------------
python3 "$OUT/phase_summary.py" "$LEG/request-phase-observation.jsonl" \
  --after-bytes "$OFFSET" > "$LEG/phase-summary.json"
cat "$LEG/phase-summary.json"

# Keep the evidence volume and the private connection file; remove only the
# container this script started.
stop_stack
trap - EXIT
echo "R71 Gate 1a window complete"
