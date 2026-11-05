#!/usr/bin/env bash
# R71 Gate 2: mechanism verification and eligibility ratio for
# PERI_DEFERRED_SESSION_CLOSE.
#
# One leg is one isolated stack: a fresh named volume, a dedicated container
# and port, the same synthetic fixture, default quotas, the candidate platform
# binary built from the branch tip, and the v1.2.7 release worker on both
# sides. The launcher is the branch-tip dev.py on every leg (its only change
# from the image is the flag passthrough), so A and B differ only in whether
# PERI_DEFERRED_SESSION_CLOSE=1 reaches the platform.
#
# Legs:
#   a-read     flag off, F1.select-1 json-pipeline c1, 5 x 100 requests
#   b-read     flag on,  same workload
#   c-write    flag on,  W1-insert-shaped pipeline, 5 x 20 write requests
#   d-read-c8  flag on,  optional c8 mechanism check, 160 requests
#
# Usage: ./run_gate2.sh [leg ...]
# Environment: REPO IMAGE PLATFORM WORKER DEV SEED OUT BASE_PORT
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/r71-g2/bin/peri-platform-g2}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
DEV=${DEV:-$REPO/prototypes/v1/platform/dev.py}
SEED=${SEED:-$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py}
OUT=${OUT:-$here}
BASE_PORT=${BASE_PORT:-20880}
LEGS=${*:-"a-read b-read c-write d-read-c8"}

[ -x "$PLATFORM" ] || { echo "missing platform binary: $PLATFORM" >&2; exit 2; }
[ -x "$WORKER" ] || { echo "missing worker binary: $WORKER" >&2; exit 2; }
[ -f "$DEV" ] || { echo "missing launcher: $DEV" >&2; exit 2; }
[ -f "$SEED" ] || { echo "missing seed script: $SEED" >&2; exit 2; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "missing image: $IMAGE" >&2; exit 2; }

platform_sha=$(shasum -a 256 "$PLATFORM" | awk '{print $1}')
worker_sha=$(shasum -a 256 "$WORKER" | awk '{print $1}')
dev_sha=$(shasum -a 256 "$DEV" | awk '{print $1}')
mkdir -p "$OUT/legs" "/tmp/peri-r71-g2"

index=0
for leg in $LEGS; do
  case "$leg" in
    a-read)    flag=off; kind=read;  blocks=5;  block_size=100 ;;
    b-read)    flag=on;  kind=read;  blocks=5;  block_size=100 ;;
    c-write)   flag=on;  kind=write; blocks=5;  block_size=20 ;;
    d-read-c8) flag=on;  kind=c8;    blocks=0;  block_size=0 ;;
    *) echo "unknown leg: $leg" >&2; exit 2 ;;
  esac
  port=$((BASE_PORT + index))
  index=$((index + 1))
  container="peri-r71-g2-$leg"
  volume="peri-r71-g2-$leg-data"
  state="/tmp/peri-r71-g2/$leg"
  legdir="$OUT/legs/$leg"
  mkdir -p "$state" "$legdir"

  # Never reuse or delete an existing stack: a name collision means this run
  # would silently measure another volume.
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
leg=$leg
flag=$flag
kind=$kind
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
blocks=$blocks
block_size=$block_size
quotas=platform defaults (not relaxed)
observer=none
EOF

  {
    echo "leg: $leg flag=$flag kind=$kind"
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

  echo "== leg $leg flag=$flag port=$port"
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
    docker logs --tail 60 "$container" >&2 || true
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
  case "$kind" in
    read)
      python3 "$here/f1_driver.py" --connection-file "$conn" --out "$legdir" \
        --leg "$leg" --label "flag $flag" \
        --blocks "$blocks" --block-size "$block_size" \
        --warmup-blocks 1 --block-gap-seconds 0.75 \
        --initial-refill-seconds 1.2 --settle-seconds 2.0 \
        > "$legdir/driver.txt" 2>&1
      ;;
    write)
      python3 "$here/write_driver.py" --connection-file "$conn" --out "$legdir" \
        --leg "$leg" --label "flag $flag" \
        --blocks "$blocks" --block-size "$block_size" \
        --block-gap-seconds 0.75 --initial-refill-seconds 1.2 \
        --settle-seconds 2.0 > "$legdir/driver.txt" 2>&1
      ;;
    c8)
      python3 "$here/read_concurrency_driver.py" --connection-file "$conn" \
        --out "$legdir" --leg "$leg" --label "flag $flag" \
        --requests 160 --concurrency 8 --initial-refill-seconds 1.2 \
        --settle-seconds 2.0 > "$legdir/driver.txt" 2>&1
      ;;
  esac
  status=$?
  set -e
  printf '{"driver": %d}\n' "$status" > "$legdir/exit-code.json"

  # Give the account a last chance to drain, then snapshot the settled state.
  sleep 2
  python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-settled.json"
  python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-after.json"

  docker logs "$container" > "$legdir/container.txt" 2>&1 || true
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  docker inspect "$container" --format '{{.State.ExitCode}}' \
    > "$legdir/stop-exit.txt" 2>/dev/null || echo "missing" > "$legdir/stop-exit.txt"
  if ! docker cp "$container":/data/peri/launcher-stop.json \
      "$legdir/launcher-stop.json" 2>/dev/null; then
    echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing.txt"
  fi
  docker rm "$container" >/dev/null 2>&1 || true
  echo "leg $leg done driver_exit=$status stop_exit=$(cat "$legdir/stop-exit.txt") volume=$volume"
done

echo "gate2 legs complete"
