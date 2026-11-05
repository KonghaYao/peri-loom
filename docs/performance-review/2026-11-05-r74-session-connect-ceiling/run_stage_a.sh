#!/usr/bin/env bash
# R74 Stage A: zero-code screen on the v1.2.8 release image.
# Fresh volume, port 21900, default quotas, observers off.
set -euo pipefail

ROOT=/Users/kk/code/peri-loom/docs/performance-review/2026-11-05-r74-session-connect-ceiling/stage-a
REPO=/Users/kk/code/peri-loom
IMAGE=ghcr.io/konghayao/peri-loom:v1.2.8
CONTAINER=peri-r74-probe-f1-stagea
VOLUME=peri-r74-probe-f1-stagea-data
PORT=21900
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
RUN_PY=$REPO/scripts/perf-reliability/issue28/run.py
SNAP=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/snap_metrics.py
WAIT=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/wait_settled.py
VERIFY=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/verify_audit.py
STATE=/tmp/r74-probe/state/stage-a
CONN=$STATE/connection.json

mkdir -p "$ROOT" "$STATE"
chmod 700 "$STATE"

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "container exists, refusing: $CONTAINER" >&2; exit 2
fi
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  echo "volume exists, refusing: $VOLUME" >&2; exit 2
fi
if (echo > /dev/tcp/127.0.0.1/"$PORT") >/dev/null 2>&1; then
  echo "port in use: $PORT" >&2; exit 2
fi

{
  echo "stage=a"
  echo "image=$IMAGE"
  docker image inspect "$IMAGE" --format 'image_id={{.Id}}'
  docker image inspect "$IMAGE" --format 'repo_digest={{index .RepoDigests 0}}'
  echo "host=$(uname -srm)"
  echo "docker=$(docker version --format '{{.Server.Version}}')"
  echo "harness=$RUN_PY sha256=$(shasum -a 256 "$RUN_PY" | awk '{print $1}')"
  echo "seed=$SEED sha256=$(shasum -a 256 "$SEED" | awk '{print $1}')"
  echo "snap=$SNAP sha256=$(shasum -a 256 "$SNAP" | awk '{print $1}')"
  echo "wait=$WAIT sha256=$(shasum -a 256 "$WAIT" | awk '{print $1}')"
  echo "verify=$VERIFY sha256=$(shasum -a 256 "$VERIFY" | awk '{print $1}')"
} > "$ROOT/environment.txt"

docker volume create "$VOLUME" >/dev/null
docker run -d --name "$CONTAINER" --platform linux/arm64 --network host \
  --init --stop-timeout 60 --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 \
  -v "$VOLUME":/data/peri \
  "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$PORT" --no-build >/dev/null

for _ in $(seq 1 120); do
  docker exec "$CONTAINER" test -f /data/peri/connection.json 2>/dev/null && break
  sleep 1
done
if ! docker exec "$CONTAINER" test -f /data/peri/connection.json 2>/dev/null; then
  echo "stack failed to start" >&2
  docker logs --tail 200 "$CONTAINER" > "$ROOT/startup-failure.txt" 2>&1 || true
  exit 1
fi
docker cp "$CONTAINER":/data/peri/connection.json "$CONN" >/dev/null
chmod 600 "$CONN"

{
  docker exec "$CONTAINER" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1
  echo "launcher_sha256=$(docker exec "$CONTAINER" sha256sum /app/prototypes/v1/platform/dev.py | awk '{print $1}')"
  docker exec "$CONTAINER" printenv PERI_IMAGE_VERSION PERI_IMAGE_REVISION PERI_RUST_BINARY PERI_PLATFORM_BINARY 2>/dev/null || true
  docker exec "$CONTAINER" sh -c '
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
      echo "observer $name: $found"
    done'
} > "$ROOT/binaries.txt" 2>&1

python3 "$SEED" "$CONN" > "$ROOT/seed.txt" 2>&1
tail -1 "$ROOT/seed.txt" >> "$ROOT/binaries.txt"
docker exec "$CONTAINER" python3 -c '
import json
envelope = json.load(open("/data/peri/platform/state.json"))
payload = envelope["payload"]
state = json.loads(payload) if isinstance(payload, str) else payload
quotas = {key: value.get("quotas") for key, value in (state.get("projects") or {}).items()}
print("project quotas: " + json.dumps(quotas, sort_keys=True))
' >> "$ROOT/binaries.txt" 2>&1

python3 "$SNAP" "$CONN" "$ROOT/metrics-before.json"
docker exec "$CONTAINER" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' | tr -d ' ' > "$ROOT/audit-count-before.txt"

set +e
python3 "$RUN_PY" --connection-file "$CONN" --out "$ROOT/harness" \
  --container "$CONTAINER" --scenario F1 --protocol json-pipeline \
  --rounds 1 --warmup 1 --samples 125 > "$ROOT/driver.txt" 2>&1
status=$?
set -e
printf '{"driver": %d}\n' "$status" > "$ROOT/exit-code.json"

python3 "$WAIT" "$CONN" "$ROOT/metrics-after.json" "$ROOT/settle.json" > "$ROOT/settle.txt" 2>&1 || true

docker exec "$CONTAINER" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' | tr -d ' ' > "$ROOT/audit-count-after.txt"
docker logs "$CONTAINER" > "$ROOT/container.txt" 2>&1 || true

stop_start=$(python3 -c 'import time;print(time.time())')
docker stop -t 60 "$CONTAINER" >/dev/null 2>&1 || true
stop_end=$(python3 -c 'import time;print(time.time())')
python3 -c "print(round(($stop_end)-($stop_start),3))" > "$ROOT/stop-seconds.txt"
docker inspect "$CONTAINER" --format '{{.State.ExitCode}}' > "$ROOT/stop-exit.txt" 2>/dev/null || echo missing > "$ROOT/stop-exit.txt"
docker cp "$CONTAINER":/data/peri/launcher-stop.json "$ROOT/launcher-stop.json" 2>/dev/null || echo "launcher-stop.json missing" > "$ROOT/launcher-stop.missing.txt"
docker cp "$CONTAINER":/data/peri/audit "$ROOT/audit" 2>/dev/null || echo "audit directory missing" > "$ROOT/audit.missing.txt"
docker rm "$CONTAINER" >/dev/null 2>&1 || true

python3 "$VERIFY" "$ROOT" > "$ROOT/audit-verify.txt" 2>&1 || true
echo "stage A done driver_exit=$status stop_exit=$(cat "$ROOT/stop-exit.txt") stop_s=$(cat "$ROOT/stop-seconds.txt") volume=$VOLUME"
