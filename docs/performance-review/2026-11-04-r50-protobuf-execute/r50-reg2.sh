#!/bin/bash
# R50 regression recheck: higher sample count on the two flagged scenarios.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r50base/connection.json
CAND=/tmp/peri-perf/run-r50cand/connection.json
OUT=${1:-/tmp/peri-perf/r50reg2}
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
one B1.batch-20 protobuf-pipeline 15 2
one L1.list-20 json-pipeline 15 2
echo REG2DONE
