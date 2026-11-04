#!/bin/bash
# R47 focused B1 rerun: the single-round +18.39% protobuf reading needs a paired check.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r47base/connection.json
CAND=/tmp/peri-perf/run-r47cand/connection.json
OUT=${1:-/tmp/peri-perf/r47b1b}
rm -rf "$OUT"; mkdir -p "$OUT"
one() { # scen proto
  local scen="$1" proto="$2" round leg side conn dir
  for round in 1 2 3; do
    for leg in 1 2 3 4; do
      case $leg in
        1|4) side=base; conn="$BASE" ;;
        2|3) side=cand; conn="$CAND" ;;
      esac
      dir="$OUT/${scen}.${proto}.r${round}-${side}${leg}"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario "$scen" --protocol "$proto" --rounds 1 --warmup 1 --samples 10 \
        --out "$dir" > "$dir.log" 2>&1
      echo "done $scen $proto r${round} $side$leg rc=$?"
      sleep 2
    done
  done
}
one B1.batch-20 protobuf-pipeline
echo B1DONE
