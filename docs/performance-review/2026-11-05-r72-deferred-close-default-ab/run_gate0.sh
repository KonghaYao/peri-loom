#!/usr/bin/env bash
# R72 Gate 0: one short leg per side to verify the mechanism switches.
#
# A = PERI_DEFERRED_SESSION_CLOSE=0 explicitly (synchronous close)
# B = PERI_DEFERRED_SESSION_CLOSE completely unset (R72 default deferred close)
#
# Each leg: fresh volume, dedicated container/port, same fixture, default
# quotas, no observer. Five P1-shaped requests are enough to publish the
# account; check_gate0_r72.py then verifies C1.
#
# Usage: ./run_gate0.sh
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/peri-r72-ab/bin/peri-platform-r72}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
DEV=${DEV:-$REPO/prototypes/v1/platform/dev.py}
SEED=${SEED:-$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py}
OUT=${OUT:-$here}
STATE_ROOT=${STATE_ROOT:-/tmp/peri-r72-ab}
PREFIX=${PREFIX:-peri-r72-ab}
BASE_PORT=${BASE_PORT:-21400}
expected_platform=e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba

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

mkdir -p "$OUT/legs/gate0" "$STATE_ROOT/gate0"
sides=(a b)
declare -a ports containers volumes states legdirs envs
for index in 0 1; do
  side=${sides[$index]}
  port=$((BASE_PORT + index))
  container="$PREFIX-gate0-$side"
  volume="$PREFIX-gate0-$side-data"
  state="$STATE_ROOT/gate0/$side"
  legdir="$OUT/legs/gate0/$side"
  ports[$index]=$port
  containers[$index]=$container
  volumes[$index]=$volume
  states[$index]=$state
  legdirs[$index]=$legdir
  if [ "$side" = a ]; then envs[$index]="PERI_DEFERRED_SESSION_CLOSE=0"; else envs[$index]=""; fi
  if docker volume inspect "$volume" >/dev/null 2>&1; then
    echo "volume already exists, refusing to touch it: $volume" >&2; exit 1
  fi
  if docker container inspect "$container" >/dev/null 2>&1; then
    echo "container already exists, refusing to touch it: $container" >&2; exit 1
  fi
  if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
    echo "port already in use, refusing to start: $port" >&2; exit 1
  fi
  if [ -d "$state" ] && [ -n "$(ls -A "$state" 2>/dev/null)" ]; then
    echo "state directory already populated, refusing to touch it: $state" >&2; exit 1
  fi
  if [ -d "$legdir" ] && [ -n "$(ls -A "$legdir" 2>/dev/null)" ]; then
    echo "evidence directory already populated, refusing to touch it: $legdir" >&2; exit 1
  fi
  mkdir -p "$state" "$legdir"
done

start_stack() {
  local index=$1 side=${sides[$1]}
  local port=${ports[$index]} container=${containers[$index]}
  local volume=${volumes[$index]} state=${states[$index]} legdir=${legdirs[$index]}
  local env_pair=${envs[$index]}

  cat > "$legdir/leg.txt" <<EOF
campaign=gate0
leg=$side
side=$side
flag=$([ "$side" = b ] && echo on || echo off)
switch=$([ "$side" = b ] && echo unset-default || echo explicit-0)
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
requests=5
quotas=platform defaults (not relaxed)
observer=none
EOF

  {
    echo "campaign: gate0 leg: $side"
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
  if [ -n "$env_pair" ]; then env_args+=(-e "$env_pair"); fi

  echo "== gate0 stack $side port=$port env='$env_pair'"
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
  docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' \
    | tr -d ' ' > "$legdir/audit-count-before.txt"
}

start_stack 0
start_stack 1

a_conn="${states[0]}/connection.json"
b_conn="${states[1]}/connection.json"
set +e
python3 "$here/gate0_driver.py" --a-connection "$a_conn" --b-connection "$b_conn" \
  --repo "$REPO" --out "$OUT/legs/gate0/driver" --requests 5 \
  > "$OUT/legs/gate0/driver.txt" 2>&1
driver_status=$?
set -e
printf '{"driver": %d}\n' "$driver_status" > "$OUT/legs/gate0/driver-exit-code.json"

for index in 0 1; do
  side=${sides[$index]}
  conn="${states[$index]}/connection.json"
  legdir=${legdirs[$index]}
  set +e
  python3 "$here/wait_settled.py" "$conn" "$legdir/metrics-after.json" \
    "$legdir/settle.json" --timeout 120 > "$legdir/settle.txt" 2>&1
  settle_status=$?
  set -e
  printf '{"settle": %d}\n' "$settle_status" > "$legdir/settle-exit-code.json"
  docker exec "${containers[$index]}" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' \
    | tr -d ' ' > "$legdir/audit-count-after.txt"
  docker logs "${containers[$index]}" > "$legdir/container.txt" 2>&1 || true
  docker stop -t 60 "${containers[$index]}" >/dev/null 2>&1 || true
  docker inspect "${containers[$index]}" --format '{{.State.ExitCode}}' \
    > "$legdir/stop-exit.txt" 2>/dev/null || echo "missing" > "$legdir/stop-exit.txt"
  docker cp "${containers[$index]}":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" \
    2>/dev/null || echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing.txt"
  docker cp "${containers[$index]}":/data/peri/audit "$legdir/audit" \
    2>/dev/null || echo "audit directory missing" > "$legdir/audit.missing.txt"
  docker rm "${containers[$index]}" >/dev/null 2>&1 || true
  echo "gate0 stack $side done driver_exit=$driver_status settle_exit=$settle_status stop_exit=$(cat "$legdir/stop-exit.txt") volume=${volumes[$index]}"
done

echo "gate0 campaign complete"
