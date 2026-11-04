#!/bin/bash
# R50 A/B/B/A: base = main @ 4784413 (peri-platform-r50-base),
# cand = perf/r50-protobuf-execute (f73aa1c, peri-platform-r50-cand).
# Main metric: R1.limit-1000 protobuf-pipeline.
# Controls: R1 json-pipeline, H1 protobuf/json cursor, F1 protobuf/json pipeline.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r50base/connection.json
CAND=/tmp/peri-perf/run-r50cand/connection.json
OUT=${1:-/tmp/peri-perf/r50ab}
rm -rf "$OUT"; mkdir -p "$OUT"
for round in 1 2; do
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    dir="$OUT/r${round}-${side}${leg}-R1-protobuf"
    python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
      --scenario R1.limit-1000 --protocol protobuf-pipeline --rounds 1 --warmup 1 --samples 5 \
      --out "$dir" > "$dir.log" 2>&1
    sleep 2
    dir="$OUT/r${round}-${side}${leg}-R1-json"
    python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
      --scenario R1.limit-1000 --protocol json-pipeline --rounds 1 --warmup 1 --samples 5 \
      --out "$dir" > "$dir.log" 2>&1
    sleep 2
    for proto in protobuf-cursor json-cursor; do
      dir="$OUT/r${round}-${side}${leg}-H1-${proto}"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario H1.content-20mb --protocol "$proto" --rounds 1 --warmup 1 --samples 5 \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
    for proto in protobuf-pipeline json-pipeline; do
      dir="$OUT/r${round}-${side}${leg}-F1-${proto}"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario F1.select-1 --protocol "$proto" --rounds 1 --warmup 1 --samples 5 \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
  done
done
echo ABDONE
