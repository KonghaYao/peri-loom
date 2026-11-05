#!/usr/bin/env bash
# R70 Gate 0: how much of the clean F1/L1 latency is the session lifecycle
# (the platform `open` and `close` action buckets)?
#
# Protocol (frozen before the first window):
#   F1 json-pipeline c1  x2  (primary + stability)
#   L1 json-pipeline c1  x2  (primary + stability)
#   F1 json-pipeline c8  x2  (run A + run B with the metrics-complete driver)
# Every c1 window: rounds=3 warmup=1 samples=5. The c8 window runs eight
# concurrent harness clients with the same per-client shape (3x(1+5) requests
# each, 120 timed samples).
#
# A `run.py --dry-run` immediately before each c1 window measures the fixture
# discovery delta; it is subtracted from that window's action totals so only
# benchmark requests enter the per-action means. The c8 driver discovers before
# it opens its own window, so no subtraction applies there.
#
# Every stack is isolated: fresh named volume, dedicated container name and
# port. Nothing here touches an existing container or volume. The connection
# file stays under $STATE and is chmod 600; it is never printed and never
# copied into the repository.
#
# Usage:
#   ./run_gate0.sh
# Environment:
#   REPO      repository root (default: three levels above this script)
#   IMAGE     runtime image (default: ghcr.io/konghayao/peri-loom:v1.2.7)
#   PLATFORM  linux/arm64 platform binary (default: release A/B artifact)
#   WORKER    linux/arm64 worker binary (default: v1.2.7 release artifact)
#   OUT       evidence directory (default: this script's directory)
#   STATE     private state + connection file (default: /tmp/peri-r70-gate0)
#   PORT      default 20731
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/peri-release-ab/bin/peri-platform-v1.2.7}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
OUT=${OUT:-$here}
STATE=${STATE:-/tmp/peri-r70-gate0}
CONTAINER=${CONTAINER:-peri-r70-gate0}
VOLUME=${VOLUME:-peri-r70-gate0-data}
PORT=${PORT:-20731}

RUN=$REPO/scripts/perf-reliability/issue28/run.py
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
SNAP=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/snap_metrics.py
CONN=$STATE/connection.json

mkdir -p "$STATE" "$OUT"

snap() { python3 "$SNAP" "$CONN" "$1"; }

stop_stack() {
  docker logs "$CONTAINER" > "$OUT/container.log" 2>&1 || true
  docker stop -t 60 "$CONTAINER" >/dev/null 2>&1 || true
  docker rm "$CONTAINER" >/dev/null 2>&1 || true
}
trap 'stop_stack' EXIT

# -- evidence and guard rails -------------------------------------------------
{
  echo "R70 Gate 0 binaries and stacks"
  echo
  echo "Host: $(uname -srm), $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unknown)"
  echo "Image: $IMAGE"
  docker image inspect "$IMAGE" --format '  image id: {{.Id}}'
  docker image inspect "$IMAGE" --format '  repo digests: {{join .RepoDigests ", "}}'
  echo
  echo "Platform"
  echo "  path:   $PLATFORM"
  echo "  sha256: $(shasum -a 256 "$PLATFORM" | awk '{print $1}')"
  echo "  source: v1.2.7 release artifact; repo diff since v1.2.7 is docs plus"
  echo "          sql_failure.go error-path evidence, no F1/L1 hot-path change"
  echo
  echo "Worker"
  echo "  path:   $WORKER"
  echo "  sha256: $(shasum -a 256 "$WORKER" | awk '{print $1}')"
  echo "  source: v1.2.7 (640ae7e), rustc 1.98.1, linux/arm64"
  echo
  echo "Stack"
  echo "  container: $CONTAINER"
  echo "  volume:    $VOLUME (fresh, kept after measurement)"
  echo "  port:      $PORT"
  echo "  harness:   $RUN"
  echo "  params:    c1 rounds=3 warmup=1 samples=5; c8 = 8 clients x same shape"
  echo "  quotas:    platform defaults (not relaxed)"
} > "$OUT/binaries.txt" 2>&1

# -- isolated stack -----------------------------------------------------------
docker volume create "$VOLUME" >/dev/null
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" --platform linux/arm64 --network host --init --stop-timeout 60 \
  --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 \
  -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform \
  -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 \
  -v "$VOLUME":/data/peri \
  -v "$PLATFORM":/usr/local/bin/peri-platform:ro \
  -v "$WORKER":/usr/local/bin/peri-loom-v1:ro \
  "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$PORT" --no-build >/dev/null

for _ in $(seq 1 120); do
  docker exec "$CONTAINER" test -f /data/peri/connection.json 2>/dev/null && break
  sleep 1
done
docker exec "$CONTAINER" test -f /data/peri/connection.json
docker cp "$CONTAINER":/data/peri/connection.json "$CONN"
chmod 600 "$CONN"
docker exec "$CONTAINER" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1 \
  | tee -a "$OUT/binaries.txt"

# -- fixture ------------------------------------------------------------------
python3 "$SEED" "$CONN" > "$OUT/seed-verify.txt" 2> "$OUT/seed.log"
cat "$OUT/seed-verify.txt"
python3 "$SNAP" "$CONN" "$OUT/metrics-after-seed.json"

# -- c1 windows ---------------------------------------------------------------
run_c1() { # name scenario
  local name=$1 scenario=$2
  mkdir -p "$OUT/$name"
  snap "$OUT/$name/metrics-discovery-before.json"
  python3 "$RUN" --connection-file "$CONN" --container "$CONTAINER" \
    --scenario "$scenario" --protocol json-pipeline --dry-run \
    > "$OUT/$name/discovery.log" 2>&1
  snap "$OUT/$name/metrics-discovery-after.json"
  snap "$OUT/$name/metrics-before.json"
  set +e
  python3 "$RUN" --connection-file "$CONN" --container "$CONTAINER" \
    --scenario "$scenario" --protocol json-pipeline --rounds 3 --warmup 1 --samples 5 \
    --out "$OUT/$name" --print-table > "$OUT/$name/harness.log" 2>&1
  echo "$?" > "$OUT/$name/exit-code.txt"
  set -e
  snap "$OUT/$name/metrics-after.json"
}

run_c8() { # name
  local name=$1
  python3 "$OUT/c8_driver.py" --connection-file "$CONN" --repo "$REPO" \
    --out "$OUT/$name" --concurrency 8 --rounds 3 --warmup 1 --samples 5 \
    > "$OUT/$name/driver.log" 2>&1
  sleep 2
}

run_c1 f1-c1 F1
run_c1 f1-c1b F1
run_c1 l1-c1 L1
run_c1 l1-c1b L1
run_c8 f1-c8
run_c8 f1-c8b
snap "$OUT/metrics-final.json"

# -- deltas and the frozen decision input ------------------------------------
python3 "$OUT/metrics_delta.py" --root "$OUT" | tee "$OUT/metrics-delta.txt"

# Keep the evidence volume and the private connection file; remove only the
# container this script started.
stop_stack
trap - EXIT
echo "R70 Gate 0 windows complete"
