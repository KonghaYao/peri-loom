#!/usr/bin/env bash
# R73 release A/B: run one leg of one scenario.
# Usage: run_leg.sh <scenario> <leg> <port> [extra docker -e args ...]
set -euo pipefail
scenario=$1; leg=$2; port=$3; shift 3

ROOT=/tmp/r73-evidence
REPO=/Users/kk/code/peri-loom
IMAGE=ghcr.io/konghayao/peri-loom:v1.2.7
PLATFORM_A=/tmp/peri-release-ab/bin/peri-platform-v1.2.7
WORKER_A=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7
PLATFORM_B=/tmp/r73-evidence/bin/peri-platform-56dc914-dirty
WORKER_B=/tmp/r73-evidence/bin/peri-loom-v1-main
DEV=${DEV_OVERRIDE:-$REPO/prototypes/v1/platform/dev.py}
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
RUN_PY=$REPO/scripts/perf-reliability/issue28/run.py
C8=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/sustained_c8_driver.py
WAIT=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/wait_settled.py
SNAP=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/snap_metrics.py

case "$leg" in
  *-a*) side=a ;;
  *-b*) side=b ;;
  *) echo "bad leg name: $leg" >&2; exit 2 ;;
esac
case "$leg" in p*-*) round=${leg%%-*} ;; r*-*) round=${leg%%-*} ;; *) round="" ;; esac
if [ "$side" = a ]; then PLATFORM=$PLATFORM_A; WORKER=$WORKER_A; flag=off; arm=v1.2.7
else PLATFORM=$PLATFORM_B; WORKER=$WORKER_B; flag=on; arm=main; fi

# T1 uses the probe platform mounted by the caller via T1_PLATFORM.
if [ -n "${T1_PLATFORM:-}" ]; then PLATFORM=$T1_PLATFORM; fi

container="peri-r73-release-$scenario-$leg"
volume="peri-r73-release-$scenario-$leg-data"
legdir="$ROOT/legs/p2/$scenario/$leg"
mkdir -p "$legdir"

for v in "$PLATFORM" "$WORKER" "$DEV" "$SEED" "$RUN_PY"; do
  [ -f "$v" ] || { echo "missing input: $v" >&2; exit 2; }
done
if docker volume inspect "$volume" >/dev/null 2>&1; then
  echo "volume exists, refusing: $volume" >&2; exit 2
fi
if docker container inspect "$container" >/dev/null 2>&1; then
  echo "container exists, refusing: $container" >&2; exit 2
fi
if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
  echo "port in use: $port" >&2; exit 2
fi

platform_sha=$(shasum -a 256 "$PLATFORM" | awk '{print $1}')
worker_sha=$(shasum -a 256 "$WORKER" | awk '{print $1}')
dev_sha=$(shasum -a 256 "$DEV" | awk '{print $1}')

cat > "$legdir/leg.txt" <<LEG
campaign=r73
scenario=$scenario
leg=$leg
side=$side
arm=$arm
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
quotas=platform defaults (not relaxed)
observer=none
LEG

{
  echo "host: $(uname -srm)"
  echo "image: $IMAGE"
  docker image inspect "$IMAGE" --format 'image id: {{.Id}}'
  docker image inspect "$IMAGE" --format 'repo digest: {{index .RepoDigests 0}}'
  echo "platform host: $PLATFORM sha256=$platform_sha"
  echo "worker host: $WORKER sha256=$worker_sha"
  echo "launcher: $DEV sha256=$dev_sha"
  echo "harness: $RUN_PY"
  echo "c8 driver: $C8"
} > "$legdir/binaries.txt" 2>&1

docker volume create "$volume" >/dev/null
docker run -d --name "$container" --platform linux/arm64 --network host \
  --init --stop-timeout 60 --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 \
  -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform \
  -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 \
  "$@" \
  -v "$volume":/data/peri \
  -v "$PLATFORM":/usr/local/bin/peri-platform:ro \
  -v "$WORKER":/usr/local/bin/peri-loom-v1:ro \
  -v "$DEV":/app/prototypes/v1/platform/dev.py:ro \
  "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" \
  --no-build >/dev/null

conn="$ROOT/state/$scenario/$leg/connection.json"
mkdir -p "$(dirname "$conn")"
for _ in $(seq 1 120); do
  docker exec "$container" test -f /data/peri/connection.json 2>/dev/null && break
  sleep 1
done
if ! docker exec "$container" test -f /data/peri/connection.json 2>/dev/null; then
  echo "stack failed to start: $container" >&2
  docker logs --tail 100 "$container" > "$legdir/startup-failure.txt" 2>&1 || true
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
          line=$(tr "\0" "\n" < "/proc/$pid/environ" 2>/dev/null | grep -E "^(PERI_DEFERRED_SESSION_CLOSE|PERI_R71_G3_[A-Z0-9_]+)=" || true)
          if [ -n "$line" ]; then flag="$flag,$line"; fi
          ;;
      esac
    done
    echo "platform process env deferred: $flag"
    for name in PERI_DURABILITY_OBSERVER PERI_SQL_OPEN_OBSERVER PERI_COMPLETION_JOURNAL; do
      found=unset
      for cmd in /proc/[0-9]*/cmdline; do
        first=$(tr "\0" "\n" < "$cmd" 2>/dev/null | head -1)
        case "$first" in
          */peri-platform)
            pid=${cmd#/proc/}; pid=${pid%/cmdline}
            line=$(tr "\0" "\n" < "/proc/$pid/environ" 2>/dev/null | grep "^${name}=" || true)
            if [ -n "$line" ]; then found=$line; fi
            ;;
        esac
      done
      echo "platform observer $name: $found"
    done
  '
} >> "$legdir/binaries.txt" 2>&1

python3 "$SEED" "$conn" > "$legdir/seed.txt" 2>&1
tail -1 "$legdir/seed.txt" >> "$legdir/binaries.txt"
if [ "$scenario" = t1 ]; then
  python3 "$WAIT" "$conn" "$legdir/seed-settle-metrics.json" "$legdir/seed-settle.json" >/dev/null 2>&1 || true
fi
docker exec "$container" python3 -c '
import json
envelope = json.load(open("/data/peri/platform/state.json"))
payload = envelope["payload"]
state = json.loads(payload) if isinstance(payload, str) else payload
quotas = {key: value.get("quotas") for key, value in (state.get("projects") or {}).items()}
print("project quotas: " + json.dumps(quotas, sort_keys=True))
' >> "$legdir/binaries.txt" 2>&1
python3 "$SNAP" "$conn" "$legdir/metrics-before.json"
docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' | tr -d ' ' > "$legdir/audit-count-before.txt"

set +e
case "$scenario" in
  f1|l1|w1|h1|t8|smoke*)
    case "$scenario" in
      f1) key=F1; samples=${SAMPLES:-125} ;;
      l1) key=L1; samples=${SAMPLES:-125} ;;
      w1) key=W1.insert; samples=${SAMPLES:-20} ;;
      h1) key=H1; samples=${SAMPLES:-3} ;;
      t8) key=F1; samples=${SAMPLES:-5} ;;
      smoke*) key=F1; samples=${SAMPLES:-5} ;;
    esac
    proto=json-pipeline
    [ "$scenario" = h1 ] && proto=json-cursor
    python3 "$RUN_PY" --connection-file "$conn" --out "$legdir/harness" \
      --container "$container" --scenario "$key" --protocol "$proto" \
      --rounds 1 --warmup 1 --samples "$samples" > "$legdir/driver.txt" 2>&1
    ;;
  x1)
    python3 "$RUN_PY" --connection-file "$conn" --out "$legdir/harness" \
      --container "$container" --scenario X1 --concurrency-requests 100 \
      > "$legdir/driver.txt" 2>&1
    ;;
  t1)
    python3 /tmp/r73-evidence/t1_burst.py "$conn" "$legdir/burst.json" > "$legdir/driver.txt" 2>&1
    ;;
  c8)
    python3 "$C8" --connection-file "$conn" --out "$legdir/harness" \
      --leg "$leg" --label sustained-c8 \
      --requests-per-client 500 --warmup-per-client 100 \
      --requests-per-block 200 --block-gap-seconds 1.2 \
      --initial-refill-seconds 1.5 > "$legdir/driver.txt" 2>&1
    ;;
  *) echo "unknown scenario: $scenario" >&2; exit 2 ;;
esac
status=$?
set -e
printf '{"driver": %d}\n' "$status" > "$legdir/exit-code.json"

if [ "${IMMEDIATE:-0}" = 1 ]; then
  python3 "$SNAP" "$conn" "$legdir/metrics-immediate.json"
fi

settle_status=0
if [ "${SKIP_SETTLE:-0}" = 1 ]; then
  printf '{"settle": "skipped-for-t1"}\n' > "$legdir/settle-exit-code.json"
else
  set +e
  python3 "$WAIT" "$conn" "$legdir/metrics-after.json" "$legdir/settle.json" \
    > "$legdir/settle.txt" 2>&1
  settle_status=$?
  set -e
  printf '{"settle": %d}\n' "$settle_status" > "$legdir/settle-exit-code.json"
fi

docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' | tr -d ' ' > "$legdir/audit-count-after.txt"
docker logs "$container" > "$legdir/container.txt" 2>&1 || true
stop_start=$(python3 -c 'import time;print(time.time())')
docker stop -t 60 "$container" >/dev/null 2>&1 || true
stop_end=$(python3 -c 'import time;print(time.time())')
python3 -c "print(round(($stop_end)-($stop_start),3))" > "$legdir/stop-seconds.txt"
docker inspect "$container" --format '{{.State.ExitCode}}' > "$legdir/stop-exit.txt" 2>/dev/null || echo missing > "$legdir/stop-exit.txt"
docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" 2>/dev/null || echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing.txt"
docker cp "$container":/data/peri/audit "$legdir/audit" 2>/dev/null || echo "audit directory missing" > "$legdir/audit.missing.txt"
docker rm "$container" >/dev/null 2>&1 || true
echo "$scenario $leg done driver_exit=$status settle_exit=$settle_status stop_exit=$(cat "$legdir/stop-exit.txt") stop_s=$(cat "$legdir/stop-seconds.txt") volume=$volume"
