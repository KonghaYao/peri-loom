#!/usr/bin/env bash
# R71 frozen A/B, P1: block-mean A/B of the flag-off baseline against the
# candidate with PERI_DEFERRED_SESSION_CLOSE=1. Both sides use THE SAME
# platform binary; the only difference is the environment flag.
#
# One leg: fresh named volume, dedicated container and port, same fixture,
# default quotas, no observer. One warm-up block plus 30 timed blocks of 100
# consecutive [execute SELECT 1, close] pipeline requests; a sample is the
# block's wall clock / 100. Blocks are separated by 0.75 s so the default
# admission bucket refills; the 100 requests inside a block stay consecutive.
#
# Legs: r1-a1 r1-b1 r1-b2 r1-a2 r2-a1 r2-b1 r2-b2 r2-a2
#
# Usage: ./run_p1.sh [leg ...]
# Environment: REPO IMAGE PLATFORM WORKER DEV SEED OUT BASE_PORT
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/r71-ab/bin/peri-platform-ab}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
DEV=${DEV:-$REPO/prototypes/v1/platform/dev.py}
SEED=${SEED:-$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py}
OUT=${OUT:-$here}
BASE_PORT=${BASE_PORT:-20980}
BLOCKS=${BLOCKS:-30}
BLOCK_SIZE=${BLOCK_SIZE:-100}
WARMUP_BLOCKS=${WARMUP_BLOCKS:-1}
LEGS=${*:-"r1-a1 r1-b1 r1-b2 r1-a2 r2-a1 r2-b1 r2-b2 r2-a2"}

[ -x "$PLATFORM" ] || { echo "missing platform binary: $PLATFORM" >&2; exit 2; }
[ -x "$WORKER" ] || { echo "missing worker binary: $WORKER" >&2; exit 2; }
[ -f "$DEV" ] || { echo "missing launcher: $DEV" >&2; exit 2; }
[ -f "$SEED" ] || { echo "missing seed script: $SEED" >&2; exit 2; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "missing image: $IMAGE" >&2; exit 2; }

platform_sha=$(shasum -a 256 "$PLATFORM" | awk '{print $1}')
worker_sha=$(shasum -a 256 "$WORKER" | awk '{print $1}')
dev_sha=$(shasum -a 256 "$DEV" | awk '{print $1}')
mkdir -p "$OUT/legs/p1" "/tmp/peri-r71-ab/p1"

index=0
for leg in $LEGS; do
  case "$leg" in
    *-a*) flag=off; side=a ;;
    *-b*) flag=on; side=b ;;
    *) echo "bad leg name: $leg" >&2; exit 2 ;;
  esac
  port=$((BASE_PORT + index))
  index=$((index + 1))
  container="peri-r71-ab-p1-$leg"
  volume="peri-r71-ab-p1-$leg-data"
  state="/tmp/peri-r71-ab/p1/$leg"
  legdir="$OUT/legs/p1/$leg"
  mkdir -p "$state" "$legdir"

  if docker volume inspect "$volume" >/dev/null 2>&1; then
    echo "volume already exists, refusing to touch it: $volume" >&2
    exit 1
  fi
  if docker container inspect "$container" >/dev/null 2>&1; then
    echo "container already exists, refusing to touch it: $container" >&2
    exit 1
  fi
  if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
    echo "port already in use, refusing to start: $port" >&2
    exit 1
  fi

  cat > "$legdir/leg.txt" <<EOF
campaign=p1
leg=$leg
flag=$flag
port=$port
container=$container
volume=$volume
platform_host=$PLATFORM
platform_sha256=$platform_sha
worker_host=$WORKER
worker_sha256=$worker_sha
launcher=$DEV
launcher_sha256=$dev_sha
image=$IMAGE
blocks='$BLOCKS'
block_size='$BLOCK_SIZE'
warmup_blocks='$WARMUP_BLOCKS'
block_gap_seconds=0.75
initial_refill_seconds=1.2
quotas=platform defaults (not relaxed)
observer=none
EOF

  {
    echo "leg: $leg flag=$flag"
    echo "host: $(uname -srm)"
    echo "image: $IMAGE"
    docker image inspect "$IMAGE" --format '  image id: {{.Id}}'
    docker image inspect "$IMAGE" --format '  repo digest: {{index .RepoDigests 0}}'
    echo "platform host: $PLATFORM sha256=$platform_sha"
    echo "worker host:   $WORKER sha256=$worker_sha"
    echo "launcher:      $DEV sha256=$dev_sha"
    echo "container: $container  volume: $volume  port: $port"
  } > "$legdir/binaries.txt" 2>&1

  env_args=(
    -e PERI_HRANA_EXPERIMENTAL=1
    -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform
    -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1
  )
  if [ "$flag" = on ]; then
    env_args+=(-e PERI_DEFERRED_SESSION_CLOSE=1)
  fi

  echo "== p1 leg $leg flag=$flag port=$port"
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
  if ! docker exec "$container" test -f /data/peri/connection.json 2>/dev/null; then
    echo "stack failed to start: $container" >&2
    docker logs --tail 80 "$container" >&2 || true
    exit 1
  fi
  docker cp "$container":/data/peri/connection.json "$conn" >/dev/null
  chmod 600 "$conn"

  {
    docker exec "$container" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1
    echo "launcher container sha256: $(docker exec "$container" sha256sum /app/prototypes/v1/platform/dev.py)"
    echo "container PERI_DEFERRED_SESSION_CLOSE=$(docker exec "$container" printenv PERI_DEFERRED_SESSION_CLOSE 2>/dev/null || echo '<unset>')"
    docker exec "$container" sh -c '
      flag=unset
      for cmd in /proc/[0-9]*/cmdline; do
        first=$(tr "\0" "\n" < "$cmd" 2>/dev/null | head -1)
        case "$first" in
          */peri-platform)
            pid=${cmd#/proc/}; pid=${pid%/cmdline}
            line=$(tr "\0" "\n" < "/proc/$pid/environ" 2>/dev/null | grep "^PERI_DEFERRED_SESSION_CLOSE=" || true)
            if [ -n "$line" ]; then flag=$line; fi
            ;;
        esac
      done
      echo "platform process env: $flag"
    '
  } >> "$legdir/binaries.txt" 2>&1

  python3 "$SEED" "$conn" > "$legdir/seed.txt" 2>&1
  tail -1 "$legdir/seed.txt" >> "$legdir/binaries.txt"
  python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-before.json"

  set +e
  python3 "$here/block_driver.py" --connection-file "$conn" --repo "$REPO" \
    --out "$legdir" --side "$side" --leg "$leg" --round "${leg%%-*}" \
    --block-size "$BLOCK_SIZE" --blocks "$BLOCKS" --warmup-blocks "$WARMUP_BLOCKS" \
    --block-gap-seconds 0.75 --initial-refill-seconds 1.2 \
    > "$legdir/driver.txt" 2>&1
  status=$?
  set -e
  printf '{"driver": %d}\n' "$status" > "$legdir/exit-code.json"

  set +e
  python3 "$here/wait_settled.py" "$conn" "$legdir/metrics-after.json" \
    "$legdir/settle.json" > "$legdir/settle.txt" 2>&1
  settle_status=$?
  set -e
  printf '{"settle": %d}\n' "$settle_status" > "$legdir/settle-exit-code.json"

  docker logs "$container" > "$legdir/container.txt" 2>&1 || true
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  docker inspect "$container" --format '{{.State.ExitCode}}' \
    > "$legdir/stop-exit.txt" 2>/dev/null || echo "missing" > "$legdir/stop-exit.txt"
  docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" \
    2>/dev/null || echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing.txt"
  docker cp "$container":/data/peri/audit "$legdir/audit" \
    2>/dev/null || echo "audit directory missing" > "$legdir/audit.missing.txt"
  docker rm "$container" >/dev/null 2>&1 || true
  echo "p1 leg $leg done driver_exit=$status settle_exit=$settle_status stop_exit=$(cat "$legdir/stop-exit.txt") volume=$volume"
done

echo "p1 legs complete"
