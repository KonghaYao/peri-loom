#!/bin/bash
# R52 regression recheck: high-sample alternating run for R1/B1 json-pipeline,
# which showed >10% pooled deltas in the first regression gate.
# Base = main @ 5327333 (peri-platform-r52-base), cand = 8967ae8 (peri-platform-r52-cand).
# 8 legs A,B,B,A,A,B,B,A; 15 timed samples per leg.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r52base/connection.json
CAND=/tmp/peri-perf/run-r52cand/connection.json
OUT=${1:-/tmp/peri-perf/r52reg-recheck}
rm -rf "$OUT"; mkdir -p "$OUT"
one() { # scenario proto samples pause
  local scen="$1" proto="$2" n="$3" pause="$4"
  local leg side conn
  for leg in 1 2 3 4 5 6 7 8; do
    case $leg in
      1|4|5|8) side=base; conn="$BASE" ;;
      2|3|6|7) side=cand; conn="$CAND" ;;
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
echo RECHECKDONE
