#!/bin/bash
# Release-level endpoint A/B: v1.1.27 image vs the same image with the R37
# platform binary mounted. The worker binary stays the image's own (Rust source
# is identical between v1.1.27 and the candidate). Fresh volume per side,
# alternated A/B/B/A. Default launch profile: no PERI_COMPLETION_JOURNAL env.
set -u
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.1.27}
B_BIN=${B_BIN:-/tmp/r37/rel-out/peri-platform-v120}
OUT=${OUT:-/tmp/r37/rel-ab}
VOL_PREFIX=${VOL_PREFIX:-peri-loom-rel}
CONTAINER=peri-loom-bench-126
URL=http://127.0.0.1:8092/db/local_default/demo/main
REPO=/Users/mino/code/peri-loom
mkdir -p "$OUT"

start() { # $1 volume, $2 optional platform binary
  docker stop -t 60 "$CONTAINER" >/dev/null 2>&1
  docker rm "$CONTAINER" >/dev/null 2>&1
  docker volume create "$1" >/dev/null || return 1
  if [ -n "${2:-}" ]; then
    docker run -d --name "$CONTAINER" --platform linux/arm64 --network host --init --stop-timeout 60 \
      --security-opt no-new-privileges:true --cap-drop ALL -e PERI_HRANA_EXPERIMENTAL=1 \
      -v "$1":/data/peri -v "$2":/usr/local/bin/peri-platform:ro \
      "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:8092 --no-build >/dev/null
  else
    docker run -d --name "$CONTAINER" --platform linux/arm64 --network host --init --stop-timeout 60 \
      --security-opt no-new-privileges:true --cap-drop ALL -e PERI_HRANA_EXPERIMENTAL=1 \
      -v "$1":/data/peri \
      "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:8092 --no-build >/dev/null
  fi || return 1
  for i in $(seq 1 90); do
    sleep 1
    /tmp/bench-token.sh env BENCH_URL=$URL SCENARIO=probe CONC=1 N=1 node /tmp/conc-bench.mjs 2>/dev/null | grep -q '"ok_queries":1' && return 0
  done
  echo "never became ready"
  docker logs --tail 30 "$CONTAINER"
  return 1
}

stop_clean() { # $1 evidence dir
  docker stop -t 60 "$CONTAINER" >/dev/null 2>&1
  docker inspect "$CONTAINER" --format '{{.State.ExitCode}}' > "$1/stop-exit.txt" 2>/dev/null
  docker rm "$CONTAINER" >/dev/null 2>&1
}

run_side() { # $1 tag, $2 cycle, $3 optional platform binary
  local tag=$1 cyc=$2 bin=${3:-}
  local dir="$OUT/$tag-r$cyc"
  local vol="$VOL_PREFIX-$tag-$cyc"
  mkdir -p "$dir"
  echo "[$(date +%H:%M:%S)] start $tag round $cyc (vol $vol)"
  if [ -n "$bin" ]; then
    cp "$bin" "$OUT/$tag-r$cyc-platform" >/dev/null
  fi
  start "$vol" "$bin" || { echo "start failed for $tag r$cyc"; exit 1; }
  docker inspect "$CONTAINER" --format '{{json .Config.Env}}' > "$dir/container-env.json" 2>/dev/null
  docker exec "$CONTAINER" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1 > "$dir/binaries.txt" 2>/dev/null
  node /tmp/import-threads-loom-126.mjs > "$dir/import.log" 2>&1
  grep -E "^verify messages" "$dir/import.log" | tail -1
  if ! grep -q "^verify messages" "$dir/import.log"; then
    echo "import failed for $tag r$cyc"; tail -20 "$dir/import.log"; exit 1
  fi
  local REV
  REV=$(/tmp/mtr.sh http://127.0.0.1:8092/v1/catalog | python3 -c "import json,sys;print(json.load(sys.stdin)['result']['revision'])")
  /tmp/manage.sh /v1/projects/quotas "{\"request_id\":\"setup-$tag-$cyc\",\"id\":\"default\",\"revision\":$REV,\"quotas\":{\"max_databases\":8,\"max_sessions\":1024,\"max_concurrent\":1024,\"requests_per_second\":100000,\"max_sql_bytes\":65536,\"max_result_bytes\":4194304}}" > "$dir/quotas.json"
  docker cp "$CONTAINER":/data/peri/connection.json "$dir/connection.json" >/dev/null
  chmod 600 "$dir/connection.json"
  cd "$REPO" || exit 1
  python3 scripts/perf-reliability/issue28/run.py \
    --connection-file "$dir/connection.json" \
    --container "$CONTAINER" \
    --out "$dir/results" \
    --rounds 3 --warmup 1 --samples 5 \
    --post-idle-seconds 30 --print-table > "$dir/harness.log" 2>&1
  echo "harness-exit=$?" > "$dir/harness-exit.txt"
  tail -3 "$dir/harness.log"
  stop_clean "$dir"
  echo "[$(date +%H:%M:%S)] done $tag round $cyc stop-exit=$(cat "$dir/stop-exit.txt" 2>/dev/null)"
}

for cyc in 1 2; do
  if [ $((cyc % 2)) -eq 1 ]; then
    run_side base "$cyc" ""
    run_side cand "$cyc" "$B_BIN"
  else
    run_side cand "$cyc" "$B_BIN"
    run_side base "$cyc" ""
  fi
done
echo "release-ab done"
