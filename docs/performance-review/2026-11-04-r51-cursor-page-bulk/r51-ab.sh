#!/bin/bash
# R51 A/B/B/A: base = main @ 578c32d (peri-platform-r51-base),
# cand = perf/r51-cursor-page-bulk (9ad2c43, peri-platform-r51-cand).
# Main metric: H1.content-20mb json-cursor and protobuf-cursor.
# Same-path control: H3.content-projection json-cursor / protobuf-cursor.
# Unaffected controls: R1.limit-1000 and F1.select-1 json/protobuf pipeline.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r51base/connection.json
CAND=/tmp/peri-perf/run-r51cand/connection.json
OUT=${1:-/tmp/peri-perf/r51ab}
rm -rf "$OUT"; mkdir -p "$OUT"
for round in 1 2; do
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    for spec in \
      "H1.content-20mb json-cursor" \
      "H1.content-20mb protobuf-cursor" \
      "H3.content-projection json-cursor" \
      "H3.content-projection protobuf-cursor" \
      "R1.limit-1000 json-pipeline" \
      "R1.limit-1000 protobuf-pipeline" \
      "F1.select-1 json-pipeline" \
      "F1.select-1 protobuf-pipeline"; do
      set -- $spec
      dir="$OUT/r${round}-${side}${leg}-$1-$2"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario "$1" --protocol "$2" --rounds 1 --warmup 1 --samples 5 \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
  done
done
echo ABDONE
