#!/bin/bash
# R52 attribution swap: fresh volumes on new ports; both binaries measured on
# fresh seeds so no stack has history from earlier A/B/gate runs.
# Port 19499 = base binary (main @ 5327333), port 19500 = cand binary (8967ae8).
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r52swap-base/connection.json
CAND=/tmp/peri-perf/run-r52swap-cand/connection.json
OUT=${1:-/tmp/peri-perf/r52swap}
rm -rf "$OUT"; mkdir -p "$OUT"
one() {
  local scen="$1" proto="$2" n="$3" pause="$4"
  local leg side conn
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    local dir="$OUT/${scen}.${proto}.${side}${leg}"
    python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
      --scenario "$scen" --protocol "$proto" --rounds 1 --warmup 1 --samples "$n" \
      --out "$dir" > "$dir.log" 2>&1
    echo "done $scen $proto $side$leg rc=$?"
    sleep "$pause"
  done
}
one R1.limit-1000 json-pipeline 15 2
one B1.batch-20 json-pipeline 15 2
echo SWAPDONE
