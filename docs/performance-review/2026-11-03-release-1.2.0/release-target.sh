#!/bin/bash
# Same-window client-path comparison: candidate platform vs sqld, 3 alternating
# rounds x 9 samples x 6 scenarios, matching the #29 methodology.
set -u
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.1.27}
CAND=${CAND:-/tmp/r37/rel-out/peri-platform-v120}
OUT=${OUT:-/tmp/r37/target-ab}
CONTAINER=peri-loom-bench-125
VOL=${VOL:-peri-loom-rel2-target}
URL=http://127.0.0.1:8092/db/local_default/demo/main
mkdir -p "$OUT"
docker stop -t 60 "$CONTAINER" >/dev/null 2>&1
docker rm "$CONTAINER" >/dev/null 2>&1
docker volume create "$VOL" >/dev/null || exit 1
docker start sqld-bench-126 >/dev/null 2>&1
docker run -d --name "$CONTAINER" --platform linux/arm64 --network host --init --stop-timeout 60 \
  --security-opt no-new-privileges:true --cap-drop ALL -e PERI_HRANA_EXPERIMENTAL=1 \
  -v "$VOL":/data/peri -v "$CAND":/usr/local/bin/peri-platform:ro \
  "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:8092 --no-build >/dev/null || exit 1
ready=0
for i in $(seq 1 90); do
  sleep 1
  CTR="$CONTAINER" /tmp/bench-token.sh env BENCH_URL=$URL SCENARIO=probe CONC=1 N=1 node /tmp/conc-bench.mjs 2>/dev/null | grep -q '"ok_queries":1' && { ready=1; break; }
done
[ "$ready" = 1 ] || { echo "never became ready"; docker logs --tail 30 "$CONTAINER"; exit 1; }
docker exec "$CONTAINER" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1 > "$OUT/binaries.txt"
node /tmp/r37/import-threads-125.mjs > "$OUT/import.log" 2>&1
grep -E "^verify messages" "$OUT/import.log" | tail -1
grep -q "^verify messages" "$OUT/import.log" || { echo import failed; tail -10 "$OUT/import.log"; exit 1; }
REV=$(CTR="$CONTAINER" /tmp/mtr.sh http://127.0.0.1:8092/v1/catalog | python3 -c "import json,sys;print(json.load(sys.stdin)['result']['revision'])")
CTR="$CONTAINER" /tmp/manage.sh /v1/projects/quotas "{\"request_id\":\"setup-target\",\"id\":\"default\",\"revision\":$REV,\"quotas\":{\"max_databases\":8,\"max_sessions\":1024,\"max_concurrent\":1024,\"requests_per_second\":100000,\"max_sql_bytes\":65536,\"max_result_bytes\":4194304}}" > /dev/null
for round in 1 2 3; do
  if [ $((round % 2)) -eq 1 ]; then
    SAMPLES=9 BENCH_URL=http://127.0.0.1:8093 node /tmp/real-bench-sqld.mjs > "$OUT/sqld-q$round.log" 2>&1
    cp /tmp/real-bench-sqld.json "$OUT/rb-sqld-q$round.json"
    SAMPLES=9 node /tmp/real-bench-125.mjs > "$OUT/loom-q$round.log" 2>&1
    cp /tmp/real-bench-125.json "$OUT/rb-loom-q$round.json"
  else
    SAMPLES=9 node /tmp/real-bench-125.mjs > "$OUT/loom-q$round.log" 2>&1
    cp /tmp/real-bench-125.json "$OUT/rb-loom-q$round.json"
    SAMPLES=9 BENCH_URL=http://127.0.0.1:8093 node /tmp/real-bench-sqld.mjs > "$OUT/sqld-q$round.log" 2>&1
    cp /tmp/real-bench-sqld.json "$OUT/rb-sqld-q$round.json"
  fi
  echo "round $round done"
done
docker stop -t 60 "$CONTAINER" >/dev/null 2>&1
echo "loom-stop=$?"
docker stop -t 60 sqld-bench-126 >/dev/null 2>&1
echo "sqld-stop=$?"
echo done
