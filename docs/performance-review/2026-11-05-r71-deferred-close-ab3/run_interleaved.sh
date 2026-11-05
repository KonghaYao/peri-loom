#!/usr/bin/env bash
# R71c short interleaved campaign: start two identical isolated stacks
# (A = flag off, B = PERI_DEFERRED_SESSION_CLOSE=1) and drive them with
# interleaved_driver.py.
#
# Design (frozen in criteria.txt before the first block):
#   1 round x 30 triples; each triple is A1 (20 requests), A2 (20), B (20) in a
#   seeded random order; block gap 0.75 s; initial refill 1.2 s.
#
# Each stack has its own port, run-dir, named volume and container name. The
# two stacks are started sequentially (so the launcher's ephemeral internal
# ports cannot race) and then run concurrently for the whole campaign. Both
# are seeded with the same fixture, use default quotas and no observer.
#
# Usage: ./run_interleaved.sh [p1]
# Environment: REPO IMAGE PLATFORM WORKER DEV SEED OUT BASE_PORT
set -euo pipefail

campaign=${1:-p1}
case "$campaign" in
  p1) ;;
  *) echo "usage: $0 [p1]" >&2; exit 2 ;;
esac

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/r71-ab3/bin/peri-platform-ab3}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
DEV=${DEV:-$REPO/prototypes/v1/platform/dev.py}
SEED=${SEED:-$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py}
OUT=${OUT:-$here}
ROUNDS=${ROUNDS:-1}
TRIPLES=${TRIPLES:-30}
BLOCK_SIZE=${BLOCK_SIZE:-20}
STATE_ROOT=${STATE_ROOT:-/tmp/peri-r71-ab3}
PREFIX=${PREFIX:-peri-r71-ab3}
BASE_PORT=${BASE_PORT:-21200}
expected_platform=3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014
b_env="PERI_DEFERRED_SESSION_CLOSE=1"
a_env=""
probe_var="PERI_DEFERRED_SESSION_CLOSE"
seed_rng=20261105
[ "$ROUNDS" = 1 ] || { echo "p1 requires ROUNDS=1 (frozen short design)" >&2; exit 2; }
[ "$TRIPLES" = 30 ] || { echo "p1 requires TRIPLES=30 (frozen short design)" >&2; exit 2; }
[ "$BLOCK_SIZE" = 20 ] || { echo "p1 requires BLOCK_SIZE=20 (frozen short design)" >&2; exit 2; }

[ -x "$PLATFORM" ] || { echo "missing platform binary: $PLATFORM" >&2; exit 2; }
[ -x "$WORKER" ] || { echo "missing worker binary: $WORKER" >&2; exit 2; }
[ -f "$DEV" ] || { echo "missing launcher: $DEV" >&2; exit 2; }
[ -f "$SEED" ] || { echo "missing seed script: $SEED" >&2; exit 2; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "missing image: $IMAGE" >&2; exit 2; }

platform_sha=$(shasum -a 256 "$PLATFORM" | awk '{print $1}')
worker_sha=$(shasum -a 256 "$WORKER" | awk '{print $1}')
dev_sha=$(shasum -a 256 "$DEV" | awk '{print $1}')
if [ "$platform_sha" != "$expected_platform" ]; then
  echo "platform SHA mismatch: got $platform_sha expected $expected_platform" >&2
  exit 1
fi
if [ "$worker_sha" != "b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff" ]; then
  echo "worker SHA mismatch: $worker_sha" >&2
  exit 1
fi

mkdir -p "$OUT/legs/$campaign" "$STATE_ROOT/$campaign"

declare -a ports containers volumes states legdirs envs sides
sides=(a b)
for index in 0 1; do
  side=${sides[$index]}
  port=$((BASE_PORT + index))
  container="$PREFIX-$campaign-$side"
  volume="$PREFIX-$campaign-$side-data"
  state="$STATE_ROOT/$campaign/$side"
  legdir="$OUT/legs/$campaign/$side"
  ports[$index]=$port
  containers[$index]=$container
  volumes[$index]=$volume
  states[$index]=$state
  legdirs[$index]=$legdir
  if [ "$side" = b ]; then envs[$index]=$b_env; else envs[$index]=$a_env; fi

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
  if [ -d "$state" ] && [ -n "$(ls -A "$state" 2>/dev/null)" ]; then
    echo "state directory already populated, refusing to touch it: $state" >&2
    exit 1
  fi
  if [ -d "$legdir" ] && [ -n "$(ls -A "$legdir" 2>/dev/null)" ]; then
    echo "evidence directory already populated, refusing to touch it: $legdir" >&2
    exit 1
  fi
  mkdir -p "$state" "$legdir"
done

start_stack() {
  local index=$1 side=${sides[$1]}
  local port=${ports[$index]} container=${containers[$index]}
  local volume=${volumes[$index]} state=${states[$index]} legdir=${legdirs[$index]}
  local env_pair=${envs[$index]}

  cat > "$legdir/leg.txt" <<EOF
campaign=$campaign
leg=$side
side=$side
flag=$([ "$side" = b ] && echo on || echo off)
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
rounds=$ROUNDS
triples_per_round=$TRIPLES
block_size=$BLOCK_SIZE
block_gap_seconds=0.75
round_gap_seconds=5.0
initial_refill_seconds=1.2
order_rng_seed=$seed_rng
quotas=platform defaults (not relaxed)
observer=none
EOF

  {
    echo "campaign: $campaign leg: $side"
    echo "host: $(uname -srm)"
    echo "image: $IMAGE"
    docker image inspect "$IMAGE" --format '  image id: {{.Id}}'
    docker image inspect "$IMAGE" --format '  repo digest: {{index .RepoDigests 0}}'
    echo "platform host: $PLATFORM sha256=$platform_sha"
    echo "worker host:   $WORKER sha256=$worker_sha"
    echo "launcher:      $DEV sha256=$dev_sha"
    echo "container: $container  volume: $volume  port: $port"
  } > "$legdir/binaries.txt" 2>&1

  local env_args=(
    -e PERI_HRANA_EXPERIMENTAL=1
    -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform
    -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1
  )
  if [ -n "$env_pair" ]; then
    env_args+=(-e "$env_pair")
  fi

  echo "== $campaign stack $side port=$port env='$env_pair'"
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

  local conn="$state/connection.json"
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
    echo "container $probe_var=$(docker exec "$container" printenv "$probe_var" 2>/dev/null || echo '<unset>')"
    docker exec "$container" sh -c '
      var="$1"
      flag=unset
      for cmd in /proc/[0-9]*/cmdline; do
        first=$(tr "\0" "\n" < "$cmd" 2>/dev/null | head -1)
        case "$first" in
          */peri-platform)
            pid=${cmd#/proc/}; pid=${pid%/cmdline}
            line=$(tr "\0" "\n" < "/proc/$pid/environ" 2>/dev/null | grep "^$var=" || true)
            if [ -n "$line" ]; then flag=$line; fi
            ;;
        esac
      done
      echo "platform process env: $flag"
    ' sh "$probe_var"
  } >> "$legdir/binaries.txt" 2>&1

  python3 "$SEED" "$conn" > "$legdir/seed.txt" 2>&1
  tail -1 "$legdir/seed.txt" >> "$legdir/binaries.txt"
  python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-before.json"
  docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' \
    | tr -d ' ' > "$legdir/audit-count-before.txt"
}

start_stack 0
start_stack 1

a_conn="${states[0]}/connection.json"
b_conn="${states[1]}/connection.json"
driver_dir="$OUT/legs/$campaign/driver"
mkdir -p "$driver_dir"

set +e
python3 "$here/interleaved_driver.py" \
  --a-connection "$a_conn" --b-connection "$b_conn" --repo "$REPO" \
  --out "$driver_dir" --seed "$seed_rng" --rounds "$ROUNDS" \
  --triples "$TRIPLES" --block-size "$BLOCK_SIZE" \
  --block-gap-seconds 0.75 --round-gap-seconds 5.0 \
  --initial-refill-seconds 1.2 \
  > "$driver_dir/driver.txt" 2>&1
status=$?
set -e
printf '{"driver": %d}\n' "$status" > "$driver_dir/exit-code.json"
for index in 0 1; do
  printf '{"driver": %d}\n' "$status" > "$OUT/legs/$campaign/${sides[$index]}/exit-code.json"
done

for index in 0 1; do
  side=${sides[$index]}
  conn="$STATE_ROOT/$campaign/$side/connection.json"
  legdir="$OUT/legs/$campaign/$side"
  set +e
  python3 "$here/wait_settled.py" "$conn" "$legdir/metrics-after.json" \
    "$legdir/settle.json" --timeout 120 > "$legdir/settle.txt" 2>&1
  settle_status=$?
  set -e
  printf '{"settle": %d}\n' "$settle_status" > "$legdir/settle-exit-code.json"

  container=${containers[$index]}
  docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' \
    | tr -d ' ' > "$legdir/audit-count-after.txt"
  docker logs "$container" > "$legdir/container.txt" 2>&1 || true
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  docker inspect "$container" --format '{{.State.ExitCode}}' \
    > "$legdir/stop-exit.txt" 2>/dev/null || echo "missing" > "$legdir/stop-exit.txt"
  docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" \
    2>/dev/null || echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing.txt"
  docker cp "$container":/data/peri/audit "$legdir/audit" \
    2>/dev/null || echo "audit directory missing" > "$legdir/audit.missing.txt"
  docker rm "$container" >/dev/null 2>&1 || true
  echo "$campaign stack $side done driver_exit=$status settle_exit=$settle_status stop_exit=$(cat "$legdir/stop-exit.txt") volume=${volumes[$index]}"
done

echo "$campaign interleaved campaign complete"
