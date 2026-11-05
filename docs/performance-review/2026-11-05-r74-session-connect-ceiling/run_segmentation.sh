#!/usr/bin/env bash
# R74 Stage B segmentation run (diagnostic only, not part of the A/B judgment).
# B worker, PERI_R74_SEGMENTS=1, pool off. Reuses port 21901 after the A/B legs
# are stopped, with a fresh volume.
set -euo pipefail

ROOT=/Users/kk/code/peri-loom/docs/performance-review/2026-11-05-r74-session-connect-ceiling/stage-b
REPO=/Users/kk/code/peri-loom
IMAGE=ghcr.io/konghayao/peri-loom:v1.2.8
DEV_R74=$REPO/docs/performance-review/2026-11-05-r74-session-connect-ceiling/dev_r74.py
B_WORKER=/tmp/r74-probe/bin/peri-loom-v1-b
DRIVER=$REPO/docs/performance-review/2026-11-05-r74-session-connect-ceiling/probe_block_driver.py
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
SNAP=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/snap_metrics.py
WAIT=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/wait_settled.py
VERIFY=$REPO/docs/performance-review/2026-11-05-r72-deferred-close-default-ab/verify_audit.py
STATE=/tmp/r74-probe/state/stage-b
container=peri-r74-probe-seg3
volume=peri-r74-probe-seg3-data
port=21901
legdir="$ROOT/legs/segmentation"
conn="$STATE/${container}-connection.json"
mkdir -p "$legdir" "$STATE"
chmod 700 "$STATE"

if docker container inspect "$container" >/dev/null 2>&1; then
  echo "container exists, refusing: $container" >&2; exit 2
fi
if docker volume inspect "$volume" >/dev/null 2>&1; then
  echo "volume exists, refusing: $volume" >&2; exit 2
fi
if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
  echo "port in use: $port" >&2; exit 2
fi

{
  echo "kind=segmentation"
  echo "worker=$B_WORKER sha256=$(shasum -a 256 "$B_WORKER" | awk '{print $1}')"
  echo "image=$IMAGE"
  docker image inspect "$IMAGE" --format 'repo_digest={{index .RepoDigests 0}}'
} > "$legdir/leg.txt"

docker volume create "$volume" >/dev/null
docker run -d --name "$container" --platform linux/arm64 --network host \
  --init --stop-timeout 60 --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 -e PERI_R74_SEGMENTS=1 \
  -v "$volume":/data/peri \
  -v "$DEV_R74":/app/prototypes/v1/platform/dev.py:ro \
  -v "$B_WORKER":/usr/local/bin/peri-loom-v1:ro \
  "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" --no-build >/dev/null

for _ in $(seq 1 120); do
  docker exec "$container" test -f /data/peri/connection.json 2>/dev/null && break
  sleep 1
done
docker cp "$container":/data/peri/connection.json "$conn" >/dev/null
chmod 600 "$conn"

{
  docker exec "$container" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1
  docker exec "$container" printenv PERI_R74_SEGMENTS
  docker exec "$container" printenv PERI_R74_CONNECT_POOL 2>/dev/null || echo "PERI_R74_CONNECT_POOL=unset"
} >> "$legdir/leg.txt" 2>&1

python3 "$SEED" "$conn" > "$legdir/seed.txt" 2>&1
sleep 2
python3 "$SNAP" "$conn" "$legdir/metrics-before.json"

set +e
python3 "$DRIVER" --connection-file "$conn" --out "$legdir/driver.json" \
  --label segmentation --blocks 10 --block-size 100 --warmup-blocks 1 --gap-ms 1000 \
  > "$legdir/driver.txt" 2>&1
status=$?
set -e
printf '{"driver": %d}\n' "$status" > "$legdir/exit-code.json"

python3 "$SNAP" "$conn" "$legdir/metrics-immediate.json"
python3 "$WAIT" "$conn" "$legdir/metrics-after.json" "$legdir/settle.json" > "$legdir/settle.txt" 2>&1 || true
docker logs "$container" > "$legdir/container.txt" 2>&1 || true
for name in worker-a worker-b; do
  docker cp "$container":/data/peri/logs/$name.log "$legdir/$name.log" 2>/dev/null || true
done
cat "$legdir"/worker-*.log 2>/dev/null | grep -E 'R74WORKER|R74POOL' > "$legdir/probe.txt" || true
docker stop -t 60 "$container" >/dev/null 2>&1 || true
docker inspect "$container" --format '{{.State.ExitCode}}' > "$legdir/stop-exit.txt" 2>/dev/null || echo missing > "$legdir/stop-exit.txt"
docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" 2>/dev/null || true
docker cp "$container":/data/peri/audit "$legdir/audit" 2>/dev/null || true
docker rm "$container" >/dev/null 2>&1 || true
python3 "$VERIFY" "$legdir" > "$legdir/audit-verify.txt" 2>&1 || true

echo "segmentation done driver_exit=$status stop_exit=$(cat "$legdir/stop-exit.txt")"
