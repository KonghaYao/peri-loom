#!/usr/bin/env bash
# R71c P2 regression gate and sustained c8.
#
# Scenarios:
#   f1  F1.select-1 json-pipeline, 1 warm-up + 125 timed requests per leg
#       (2 rounds x A/B/B/A => 500 timed requests per side)
#   l1  L1.list-20  json-pipeline, same shape
#   x1  standard Issue 28 X1 concurrency scenario (levels 1/4/8/16, 100
#       requests per level; c1/c4 warm up c8). The c16 429 level is the known
#       default-quota artifact and is recorded as a diagnostic only.
#   c8  sustained c8: 8 workers x 500 pipeline requests each (100 warm-up +
#       400 timed), split into 200-request blocks with a 1.2 s admission
#       refill gap, per criteria.txt.
#
# Both sides use THE SAME platform binary; only PERI_DEFERRED_SESSION_CLOSE
# differs. Every leg is a fresh volume, dedicated container/port, same fixture,
# default quotas, no observer.
#
# Usage: ./run_p2.sh <f1|l1|x1|c8> [leg ...]
set -euo pipefail

scenario=${1:-}
shift || true
case "$scenario" in
  f1|l1|x1|c8) ;;
  *) echo "usage: $0 <f1|l1|x1|c8> [leg ...]" >&2; exit 2 ;;
esac

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
PLATFORM=${PLATFORM:-/tmp/peri-r71-ab4/bin/peri-platform-ab4}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
DEV=${DEV:-$REPO/prototypes/v1/platform/dev.py}
SEED=${SEED:-$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py}
RUN_PY=${RUN_PY:-$REPO/scripts/perf-reliability/issue28/run.py}
C8_DRIVER=${C8_DRIVER:-$here/sustained_c8_driver.py}
OUT=${OUT:-$here}
PREFIX=${PREFIX:-peri-r71-ab4}
SAMPLES=${SAMPLES:-125}
CONCURRENCY_REQUESTS=${CONCURRENCY_REQUESTS:-100}
LEGS=${*:-"r1-a1 r1-b1 r1-b2 r1-a2 r2-a1 r2-b1 r2-b2 r2-a2"}

case "$scenario" in
  f1) base_port=21310 ;;
  l1) base_port=21320 ;;
  x1) base_port=21330 ;;
  c8) base_port=21340 ;;
esac
if [ -n "${BASE_PORT:-}" ]; then base_port=$BASE_PORT; fi

[ -x "$PLATFORM" ] || { echo "missing platform binary: $PLATFORM" >&2; exit 2; }
[ -x "$WORKER" ] || { echo "missing worker binary: $WORKER" >&2; exit 2; }
[ -f "$DEV" ] || { echo "missing launcher: $DEV" >&2; exit 2; }
[ -f "$SEED" ] || { echo "missing seed script: $SEED" >&2; exit 2; }
[ -f "$RUN_PY" ] || { echo "missing harness: $RUN_PY" >&2; exit 2; }
[ -f "$C8_DRIVER" ] || { echo "missing sustained c8 driver: $C8_DRIVER" >&2; exit 2; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "missing image: $IMAGE" >&2; exit 2; }

platform_sha=$(shasum -a 256 "$PLATFORM" | awk '{print $1}')
worker_sha=$(shasum -a 256 "$WORKER" | awk '{print $1}')
dev_sha=$(shasum -a 256 "$DEV" | awk '{print $1}')
if [ "$platform_sha" != "a07ec7b874df3d941ea274823857f2a42ed63497f0c76f50795fb8bef8577cca" ]; then
  echo "platform SHA mismatch: $platform_sha" >&2
  exit 1
fi
if [ "$worker_sha" != "b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff" ]; then
  echo "worker SHA mismatch: $worker_sha" >&2
  exit 1
fi
mkdir -p "$OUT/legs/p2/$scenario" "/tmp/peri-r71-ab4/p2/$scenario"

index=0
for leg in $LEGS; do
  case "$leg" in
    *-a*) flag=off; side=a ;;
    *-b*) flag=on; side=b ;;
    *) echo "bad leg name: $leg" >&2; exit 2 ;;
  esac
  round=${leg%%-*}
  port=$((base_port + index))
  index=$((index + 1))
  container="$PREFIX-p2-$scenario-$leg"
  volume="$PREFIX-p2-$scenario-$leg-data"
  state="/tmp/peri-r71-ab4/p2/$scenario/$leg"
  legdir="$OUT/legs/p2/$scenario/$leg"
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

  if [ "$scenario" = x1 ]; then
    shape="X1 standard concurrency levels 1/4/8/16, 100 requests per level (c1/c4 warm c8)"
  elif [ "$scenario" = c8 ]; then
    shape="sustained c8: 8 workers x 500 pipeline requests (100 warm-up + 400 timed), 200-request blocks, 1.2s refill gap"
  else
    case "$scenario" in
      f1) scenario_key=F1 ;;
      l1) scenario_key=L1 ;;
    esac
    harness_args=(--scenario "$scenario_key" --protocol json-pipeline \
      --rounds 1 --warmup 1 --samples "$SAMPLES")
    shape="${scenario} $SAMPLES timed requests"
  fi

  cat > "$legdir/leg.txt" <<EOF
campaign=p2
scenario=$scenario
leg=$leg
side=$side
flag=$flag
round=$round
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
shape=$shape
quotas=platform defaults (not relaxed)
observer=none
EOF

  {
    echo "leg: $scenario/$leg flag=$flag"
    echo "host: $(uname -srm)"
    echo "image: $IMAGE"
    docker image inspect "$IMAGE" --format '  image id: {{.Id}}'
    docker image inspect "$IMAGE" --format '  repo digest: {{index .RepoDigests 0}}'
    echo "platform host: $PLATFORM sha256=$platform_sha"
    echo "worker host:   $WORKER sha256=$worker_sha"
    echo "launcher:      $DEV sha256=$dev_sha"
    echo "harness:       $RUN_PY"
    echo "c8 driver:     $C8_DRIVER"
    echo "shape: $shape"
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

  echo "== p2 $scenario leg $leg flag=$flag port=$port"
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
  docker exec "$container" python3 -c '
import json
envelope = json.load(open("/data/peri/platform/state.json"))
payload = envelope["payload"]
state = json.loads(payload) if isinstance(payload, str) else payload
quotas = {key: value.get("quotas")
          for key, value in (state.get("projects") or {}).items()}
print("project quotas: " + json.dumps(quotas, sort_keys=True))
' >> "$legdir/binaries.txt" 2>&1
  python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-before.json"
  docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' \
    | tr -d ' ' > "$legdir/audit-count-before.txt"

  set +e
  if [ "$scenario" = x1 ]; then
    python3 "$RUN_PY" --connection-file "$conn" --out "$legdir/harness" \
      --container "$container" --scenario X1 \
      --concurrency-requests "$CONCURRENCY_REQUESTS" \
      > "$legdir/driver.txt" 2>&1
  elif [ "$scenario" = c8 ]; then
    python3 "$C8_DRIVER" --connection-file "$conn" --out "$legdir/harness" \
      --leg "$leg" --label sustained-c8 \
      --requests-per-client 500 --warmup-per-client 100 \
      --requests-per-block 200 --block-gap-seconds 1.2 \
      --initial-refill-seconds 1.5 \
      > "$legdir/driver.txt" 2>&1
  else
    python3 "$RUN_PY" --connection-file "$conn" --out "$legdir/harness" \
      --container "$container" "${harness_args[@]}" \
      > "$legdir/driver.txt" 2>&1
  fi
  status=$?
  set -e
  printf '{"driver": %d}\n' "$status" > "$legdir/exit-code.json"

  set +e
  python3 "$here/wait_settled.py" "$conn" "$legdir/metrics-after.json" \
    "$legdir/settle.json" > "$legdir/settle.txt" 2>&1
  settle_status=$?
  set -e
  printf '{"settle": %d}\n' "$settle_status" > "$legdir/settle-exit-code.json"

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
  echo "p2 $scenario leg $leg done driver_exit=$status settle_exit=$settle_status stop_exit=$(cat "$legdir/stop-exit.txt") volume=$volume"
done

echo "p2 $scenario legs complete"
