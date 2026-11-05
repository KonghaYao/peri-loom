#!/usr/bin/env bash
# R74 pool smoke: verify the fixed refiller keeps the pool warm before the A/B campaign.
set -euo pipefail

ROOT=/Users/kk/code/peri-loom/docs/performance-review/2026-11-05-r74-session-connect-ceiling/stage-b/smoke
REPO=/Users/kk/code/peri-loom
IMAGE=ghcr.io/konghayao/peri-loom:v1.2.8
DEV_R74=$REPO/docs/performance-review/2026-11-05-r74-session-connect-ceiling/dev_r74.py
B_WORKER=/tmp/r74-probe/bin/peri-loom-v1-b
DRIVER=$REPO/docs/performance-review/2026-11-05-r74-session-connect-ceiling/probe_block_driver.py
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
STATE=/tmp/r74-probe/state/smoke
container=peri-r74-probe-smoke2
volume=peri-r74-probe-smoke2-data
port=21901
conn=$STATE/connection.json
mkdir -p "$ROOT" "$STATE"
chmod 700 "$STATE"

if docker container inspect "$container" >/dev/null 2>&1 || docker volume inspect "$volume" >/dev/null 2>&1; then
  echo "smoke name already used" >&2; exit 2
fi
if (echo > /dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
  echo "port in use: $port" >&2; exit 2
fi

docker volume create "$volume" >/dev/null
docker run -d --name "$container" --platform linux/arm64 --network host \
  --init --stop-timeout 60 --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 -e PERI_R74_CONNECT_POOL=1 \
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
{ docker exec "$container" sha256sum /usr/local/bin/peri-loom-v1; docker exec "$container" printenv PERI_R74_CONNECT_POOL; } > "$ROOT/binaries.txt" 2>&1
python3 "$SEED" "$conn" > "$ROOT/seed.txt" 2>&1
sleep 2
set +e
python3 "$DRIVER" --connection-file "$conn" --out "$ROOT/driver.json" \
  --label pool-smoke --blocks 6 --block-size 100 --warmup-blocks 1 --gap-ms 1000 \
  > "$ROOT/driver.txt" 2>&1
status=$?
set -e
printf '{"driver": %d}\n' "$status" > "$ROOT/exit-code.json"
for name in worker-a worker-b; do
  docker cp "$container":/data/peri/logs/$name.log "$ROOT/$name.log" 2>/dev/null || true
done
cat "$ROOT"/worker-*.log 2>/dev/null | grep -E 'R74POOL' > "$ROOT/pool.txt" || true
docker logs "$container" > "$ROOT/container.txt" 2>&1 || true
docker stop -t 60 "$container" >/dev/null 2>&1 || true
docker rm "$container" >/dev/null 2>&1 || true
echo "smoke done driver_exit=$status"
tail -3 "$ROOT/pool.txt"
