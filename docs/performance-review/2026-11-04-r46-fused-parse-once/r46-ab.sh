#!/bin/bash
# R46 A/B/B/A: base = main @ bad41e6 (peri-platform-r46-base), cand = perf/r46-fused-parse-once
# Main metric: R1.limit-1000 json-pipeline. Controls: H1 json/protobuf cursor, F1 json-pipeline.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r46base/connection.json
CAND=/tmp/peri-perf/run-r46cand/connection.json
OUT=${1:-/tmp/peri-perf/r46ab}
rm -rf "$OUT"; mkdir -p "$OUT"
for round in 1 2; do
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    dir="$OUT/r${round}-${side}${leg}-R1-json"
    python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
      --scenario R1.limit-1000 --protocol json-pipeline --rounds 1 --warmup 1 --samples 5 \
      --out "$dir" > "$dir.log" 2>&1
    sleep 2
    for proto in json-cursor protobuf-cursor; do
      dir="$OUT/r${round}-${side}${leg}-H1-${proto}"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario H1.content-20mb --protocol "$proto" --rounds 1 --warmup 1 --samples 5 \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
    dir="$OUT/r${round}-${side}${leg}-F1-json"
    python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
      --scenario F1.select-1 --protocol json-pipeline --rounds 1 --warmup 1 --samples 5 \
      --out "$dir" > "$dir.log" 2>&1
    sleep 2
  done
done
echo ABDONE
