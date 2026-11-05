#!/usr/bin/env bash
# R71 frozen A/B: structural wire probe (supporting P3 evidence).
#
# Starts one short-lived stack per side with the same candidate binary, worker
# and launcher; the only difference is PERI_DEFERRED_SESSION_CLOSE. Sends the
# same F1-shaped pipeline a few times and stores the raw/normalized responses,
# so byte-count differences caused by `query_duration_ms` width can be separated
# from structural changes.
#
# Usage: ./run_wire_probe.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/r71-ab/bin/peri-platform-ab}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
DEV=${DEV:-$REPO/prototypes/v1/platform/dev.py}
SEED=${SEED:-$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py}
OUT=${OUT:-$here}
BASE_PORT=${BASE_PORT:-20990}

for side in a b; do
  if [ "$side" = a ]; then flag=off; port=$BASE_PORT; else flag=on; port=$((BASE_PORT + 1)); fi
  container="peri-r71-ab-wire-$side"
  volume="peri-r71-ab-wire-$side-data"
  state="/tmp/peri-r71-ab/wire/$side"
  legdir="$OUT/legs/wire/$side"
  mkdir -p "$state" "$legdir"
  docker volume inspect "$volume" >/dev/null 2>&1 && {
    echo "volume exists: $volume" >&2; exit 1; }
  docker container inspect "$container" >/dev/null 2>&1 && {
    echo "container exists: $container" >&2; exit 1; }
  (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1 && {
    echo "port busy: $port" >&2; exit 1; }

  env_args=(-e PERI_HRANA_EXPERIMENTAL=1
    -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform
    -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1)
  [ "$flag" = on ] && env_args+=(-e PERI_DEFERRED_SESSION_CLOSE=1)

  docker volume create "$volume" >/dev/null
  docker run -d --name "$container" --platform linux/arm64 --network host \
    --init --stop-timeout 60 --security-opt no-new-privileges:true --cap-drop ALL \
    "${env_args[@]}" \
    -v "$volume":/data/peri \
    -v "$PLATFORM":/usr/local/bin/peri-platform:ro \
    -v "$WORKER":/usr/local/bin/peri-loom-v1:ro \
    -v "$DEV":/app/prototypes/v1/platform/dev.py:ro \
    "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" \
    --no-build >/dev/null
  conn="$state/connection.json"
  for _ in $(seq 1 120); do
    docker exec "$container" test -f /data/peri/connection.json 2>/dev/null && break
    sleep 1
  done
  docker exec "$container" test -f /data/peri/connection.json
  docker cp "$container":/data/peri/connection.json "$conn" >/dev/null
  chmod 600 "$conn"
  python3 "$SEED" "$conn" > "$legdir/seed.txt" 2>&1
  python3 "$here/wire_probe.py" --connection-file "$conn" --repo "$REPO" \
    --out "$legdir" --label "$side" > "$legdir/probe.txt" 2>&1
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  docker inspect "$container" --format '{{.State.ExitCode}}' > "$legdir/stop-exit.txt" \
    2>/dev/null || echo missing > "$legdir/stop-exit.txt"
  docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" \
    2>/dev/null || true
  docker rm "$container" >/dev/null 2>&1 || true
  echo "wire side=$side flag=$flag done $(cat "$legdir/probe.txt")"
done
