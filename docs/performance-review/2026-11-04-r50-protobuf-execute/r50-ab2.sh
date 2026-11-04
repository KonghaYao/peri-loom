#!/bin/bash
# R50 follow-up: more repetitions on the JSON control plus the main metric.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r50base/connection.json
CAND=/tmp/peri-perf/run-r50cand/connection.json
OUT=${1:-/tmp/peri-perf/r50ab2}
rm -rf "$OUT"; mkdir -p "$OUT"
for round in 1 2 3; do
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    for spec in "json-pipeline 7" "protobuf-pipeline 5"; do
      proto=${spec% *}; samples=${spec#* }
      dir="$OUT/r${round}-${side}${leg}-${proto}"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario R1.limit-1000 --protocol "$proto" --rounds 1 --warmup 1 --samples "$samples" \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
  done
done
echo ABDONE2
