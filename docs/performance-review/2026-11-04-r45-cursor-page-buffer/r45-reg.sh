#!/bin/bash
# R45 regression gate: base = main (peri-platform-r43-cand), cand = perf/r45-cursor-page-buffer (2a52f46)
# One scenario at a time, 4 legs A,B,B,A, 1 warmup + 5 timed samples per leg.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r45base/connection.json
CAND=/tmp/peri-perf/run-r45cand/connection.json
OUT=${1:-/tmp/peri-perf/r45reg}
rm -rf "$OUT"; mkdir -p "$OUT"
one() { # scenario proto samples pause
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
one F1.select-1 json-pipeline 5 2
one F1.select-1 protobuf-pipeline 5 2
one L1.list-20 json-pipeline 5 2
one L1.list-20 json-cursor 5 2
one B1.batch-20 json-pipeline 5 2
one B1.batch-20 protobuf-pipeline 5 2
one C1.baton-10 json-pipeline 5 2
one X1.concurrency json-pipeline 5 3
one R1.limit-1000 json-pipeline 5 2
one H3.content-projection json-cursor 5 2
echo REGDONE
