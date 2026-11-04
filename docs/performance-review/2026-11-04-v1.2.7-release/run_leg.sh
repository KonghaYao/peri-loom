#!/usr/bin/env bash
# v1.2.7 release A/B leg: fresh volume + fresh stack -> seed fixture -> frozen
# scenario run -> metrics snapshots -> clean stop. One leg runs one side.
#
# Usage: run_leg.sh <scenario> <leg> <port> <side> <platform-binary> <worker-binary> <samples>
set -euo pipefail
scenario=$1; leg=$2; port=$3; side=$4; platform=$5; worker=$6; samples=$7
here=$(cd "$(dirname "$0")" && pwd)
legdir=$here/legs/$scenario/$leg
key=$(printf '%s' "$scenario" | tr '[:upper:].' '[:lower:]-')
volume=peri-loom-release-v127-$key-$leg
container=peri-loom-release-v127-$key-$leg
image=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
# The connection file carries a private API key. Keep it outside the repository.
conn=/tmp/peri-release-ab/conn-$key-$leg.json
mkdir -p /tmp/peri-release-ab

case "$scenario" in
  W1.insert) protocols=(json-pipeline protobuf-pipeline) ;;
  F1.select-1) protocols=(json-pipeline protobuf-pipeline json-cursor protobuf-cursor) ;;
  X1.concurrency) protocols=(json-pipeline) ;;
  H1.content-20mb) protocols=(json-cursor protobuf-cursor) ;;
  *) echo "unknown scenario: $scenario" >&2; exit 2 ;;
esac
proto_args=()
for p in "${protocols[@]}"; do proto_args+=(--protocol "$p"); done

mkdir -p "$legdir"
rm -f "$legdir/summary.json" "$legdir/summary.md" "$legdir/raw.jsonl" \
      "$legdir/metrics-before.json" "$legdir/metrics-after.json" \
      "$legdir/launcher-stop.json" "$legdir/stop-exit.txt" "$legdir/harness.log"
docker rm -f "$container" >/dev/null 2>&1 || true
docker volume create "$volume" >/dev/null
docker run -d --name "$container" --platform linux/arm64 --network host --init --stop-timeout 60 \
  --security-opt no-new-privileges:true --cap-drop ALL \
  -e PERI_HRANA_EXPERIMENTAL=1 \
  -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform \
  -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 \
  -v "$volume":/data/peri \
  -v "$platform":/usr/local/bin/peri-platform:ro \
  -v "$worker":/usr/local/bin/peri-loom-v1:ro \
  "$image" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" --no-build >/dev/null

cleanup() {
  docker logs "$container" > "$legdir/container.log" 2>&1 || true
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  docker inspect "$container" --format '{{.State.ExitCode}}' > "$legdir/stop-exit.txt" 2>/dev/null || true
  if ! docker cp "$container":/data/peri/launcher-stop.json "$legdir/launcher-stop.json" 2>/dev/null; then
    echo "launcher-stop.json missing" > "$legdir/launcher-stop.missing"
  fi
  docker rm "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 120); do
  docker exec "$container" test -f /data/peri/connection.json 2>/dev/null && break
  sleep 1
done
if ! docker exec "$container" test -f /data/peri/connection.json 2>/dev/null; then
  echo "STACK FAILED for $scenario/$leg" >&2
  docker logs --tail 40 "$container" >&2 || true
  exit 1
fi
docker cp "$container":/data/peri/connection.json "$conn" >/dev/null
chmod 600 "$conn"

python3 "$here/seed_fixture.py" "$conn" > "$legdir/seed.log" 2>&1
tail -1 "$legdir/seed.log"
python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-before.json"

cd /Users/kk/code/peri-loom
set +e
python3 scripts/perf-reliability/issue28/run.py \
  --connection-file "$conn" \
  --container "$container" \
  --out "$legdir" \
  --scenario "$scenario" "${proto_args[@]}" \
  --rounds 1 --warmup 1 --samples "$samples" \
  --concurrency-requests 50 --print-table --allow-misses \
  > "$legdir/harness.log" 2>&1
status=$?
set -e
python3 "$here/snap_metrics.py" "$conn" "$legdir/metrics-after.json"
echo "$scenario/$leg side=$side harness_status=$status"
exit $status
