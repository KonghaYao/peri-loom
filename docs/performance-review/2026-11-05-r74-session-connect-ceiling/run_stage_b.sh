#!/usr/bin/env bash
# R74 Stage B: block-average A/B of the v1.2.8 worker vs the connect-pool probe worker.
# Round 1 order A/B/B/A, round 2 order B/A/A/B. One fresh volume and one container
# per leg, same image, same launcher, default quotas, observers off.
set -euo pipefail

ROOT=/Users/kk/code/peri-loom/docs/performance-review/2026-11-05-r74-session-connect-ceiling/stage-b
REPO=/Users/kk/code/peri-loom
IMAGE=ghcr.io/konghayao/peri-loom:v1.2.8
DEV_R74=$REPO/docs/performance-review/2026-11-05-r74-session-connect-ceiling/dev_r74.py
A_WORKER=/tmp/r74-probe/bin/peri-loom-v1-a
B_WORKER=/tmp/r74-probe/bin/peri-loom-v1-b
DRIVER=$REPO/docs/performance-review/2026-11-05-r74-session-connect-ceiling/probe_block_driver.py
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
RUN_PY=$REPO/scripts/perf-reliability/issue28/run.py
SNAP=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/snap_metrics.py
WAIT=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/wait_settled.py
VERIFY=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/verify_audit.py
STATE=/tmp/r74-probe/state/stage-b

mkdir -p "$ROOT" "$STATE"
chmod 700 "$STATE"

run_leg() {
  round=$1; seq=$2; arm=$3; regression=${4:-0}
  port=$((21900 + seq))
  container="peri-r74-probe-r${round}-${seq}${arm}"
  volume="${container}-data"
  legdir="$ROOT/legs/r${round}/${seq}${arm}"
  conn="$STATE/${container}-connection.json"
  mkdir -p "$legdir"

  if [ "$arm" = a ]; then worker=$A_WORKER; else worker=$B_WORKER; fi
  [ -f "$worker" ] || { echo "missing worker: $worker" >&2; exit 2; }
  if docker container inspect "$container" >/dev/null 2>&1; then
    echo "container exists, refusing: $container" >&2; exit 2
  fi
  if docker volume inspect "$volume" >/dev/null 2>&1; then
    echo "volume exists, refusing: $volume" >&2; exit 2
  fi
  if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
    echo "port in use: $port" >&2; exit 2
  fi

  extra=()
  if [ "$arm" = b ]; then extra+=(-e PERI_R74_CONNECT_POOL=1); fi
  {
    echo "round=$round"
    echo "seq=$seq"
    echo "arm=$arm"
    echo "port=$port"
    echo "container=$container"
    echo "volume=$volume"
    echo "image=$IMAGE"
    docker image inspect "$IMAGE" --format 'image_id={{.Id}}'
    docker image inspect "$IMAGE" --format 'repo_digest={{index .RepoDigests 0}}'
    echo "worker_host=$worker sha256=$(shasum -a 256 "$worker" | awk '{print $1}')"
    echo "driver=$DRIVER sha256=$(shasum -a 256 "$DRIVER" | awk '{print $1}')"
    echo "seed=$SEED sha256=$(shasum -a 256 "$SEED" | awk '{print $1}')"
    echo "run_py=$RUN_PY sha256=$(shasum -a 256 "$RUN_PY" | awk '{print $1}')"
  } > "$legdir/leg.txt"

  docker volume create "$volume" >/dev/null
  docker run -d --name "$container" --platform linux/arm64 --network host \
    --init --stop-timeout 60 --security-opt no-new-privileges:true --cap-drop ALL \
    -e PERI_HRANA_EXPERIMENTAL=1 ${extra[@]+"${extra[@]}"} \
    -v "$volume":/data/peri \
    -v "$DEV_R74":/app/prototypes/v1/platform/dev.py:ro \
    -v "$worker":/usr/local/bin/peri-loom-v1:ro \
    "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" --no-build >/dev/null

  for _ in $(seq 1 120); do
    docker exec "$container" test -f /data/peri/connection.json 2>/dev/null && break
    sleep 1
  done
  if ! docker exec "$container" test -f /data/peri/connection.json 2>/dev/null; then
    echo "stack failed to start: $container" >&2
    docker logs --tail 200 "$container" > "$legdir/startup-failure.txt" 2>&1 || true
    exit 1
  fi
  docker cp "$container":/data/peri/connection.json "$conn" >/dev/null
  chmod 600 "$conn"

  {
    docker exec "$container" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1
    echo "launcher_sha256=$(docker exec "$container" sha256sum /app/prototypes/v1/platform/dev.py | awk '{print $1}')"
    docker exec "$container" printenv PERI_R74_CONNECT_POOL 2>/dev/null || echo "PERI_R74_CONNECT_POOL=unset"
    docker exec "$container" printenv PERI_R74_SEGMENTS 2>/dev/null || echo "PERI_R74_SEGMENTS=unset"
    docker exec "$container" sh -c '
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
  } >> "$legdir/binaries.txt" 2>&1

  docker exec "$container" python3 -c '
import json
envelope = json.load(open("/data/peri/platform/state.json"))
payload = envelope["payload"]
state = json.loads(payload) if isinstance(payload, str) else payload
quotas = {key: value.get("quotas") for key, value in (state.get("projects") or {}).items()}
print("project quotas: " + json.dumps(quotas, sort_keys=True))
' >> "$legdir/binaries.txt" 2>&1

  python3 "$SEED" "$conn" > "$legdir/seed.txt" 2>&1
sleep 2
  tail -1 "$legdir/seed.txt" >> "$legdir/binaries.txt"
  python3 "$SNAP" "$conn" "$legdir/metrics-before.json"
  docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' | tr -d ' ' > "$legdir/audit-count-before.txt"
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' "$container" > "$legdir/stats-before.txt" 2>&1 || true

  set +e
  python3 "$DRIVER" --connection-file "$conn" --out "$legdir/driver.json" \
    --label "r${round}-${seq}${arm}" --blocks 30 --block-size 100 \
    --warmup-blocks 1 --gap-ms 1000 > "$legdir/driver.txt" 2>&1
  status=$?
  set -e
  printf '{"driver": %d}\n' "$status" > "$legdir/exit-code.json"

  if [ "$regression" = 1 ]; then
    set +e
    python3 "$RUN_PY" --connection-file "$conn" --out "$legdir/harness-l1" \
      --container "$container" --scenario L1 --protocol json-pipeline \
      --rounds 1 --warmup 1 --samples 125 > "$legdir/l1.txt" 2>&1
    printf '{"l1": %d}\n' "$?" > "$legdir/l1-exit-code.json"
    python3 "$RUN_PY" --connection-file "$conn" --out "$legdir/harness-x1" \
      --container "$container" --scenario X1 --concurrency-requests 100 > "$legdir/x1.txt" 2>&1
    printf '{"x1": %d}\n' "$?" > "$legdir/x1-exit-code.json"
    set -e
  fi

  python3 "$SNAP" "$conn" "$legdir/metrics-immediate.json"
  python3 "$WAIT" "$conn" "$legdir/metrics-after.json" "$legdir/settle.json" > "$legdir/settle.txt" 2>&1 || true
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' "$container" > "$legdir/stats-after.txt" 2>&1 || true

  docker exec "$container" sh -c 'cat /data/peri/audit/events.json /data/peri/audit/events.json.segment.* 2>/dev/null | wc -l' | tr -d ' ' > "$legdir/audit-count-after.txt"
  docker logs "$container" > "$legdir/container.txt" 2>&1 || true
  for name in worker-a worker-b authority platform; do
    docker cp "$container":/data/peri/logs/$name.log "$legdir/$name.log" 2>/dev/null || true
  done
  cat "$legdir"/worker-*.log 2>/dev/null | grep -E 'R74POOL|R74WORKER' > "$legdir/probe.txt" || true

  stop_start=$(python3 -c 'import time;print(time.time())')
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  stop_end=$(python3 -c 'import time;print(time.time())')
  python3 -c "print(round(($stop_end)-($stop_start),3))" > "$legdir/stop-seconds.txt"
  docker inspect "$container" --format '{{.State.ExitCode}}' > "$legdir/stop-exit.txt" 2>/dev/null || echo missing > "$legdir/stop-exit.txt"
  docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" 2>/dev/null || echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing.txt"
  docker cp "$container":/data/peri/audit "$legdir/audit" 2>/dev/null || echo "audit directory missing" > "$legdir/audit.missing.txt"
  docker rm "$container" >/dev/null 2>&1 || true
  python3 "$VERIFY" "$legdir" > "$legdir/audit-verify.txt" 2>&1 || true

  echo "leg r${round}-${seq}${arm} done driver_exit=$status stop_exit=$(cat "$legdir/stop-exit.txt") stop_s=$(cat "$legdir/stop-seconds.txt")"
}

run_leg 1 1 a
run_leg 1 2 b
run_leg 1 3 b
run_leg 1 4 a
run_leg 2 5 b
run_leg 2 6 a
run_leg 2 7 a 1
run_leg 2 8 b 1
