#!/bin/bash
# 批次 A/B/B/A：base = main @ fbc0e3d（平台 peri-platform-r56-base2 + worker peri-loom-v1-r53-clean）
# cand = R56 @ d5cf31f + R57 @ 1daea98（平台 peri-platform-r56-cand2 + worker peri-loom-v1-r57）
# 主指标：H1.content-20mb json-cursor 与 protobuf-cursor。
# 两条独立栈、各自全新 run dir 与同批 fixture；每轮 4 腿，每腿 1 warmup + 5 samples。
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-batch-base/connection.json
CAND=/tmp/peri-perf/run-batch-cand/connection.json
OUT=${1:-/tmp/peri-perf/batchab}
rm -rf "$OUT"; mkdir -p "$OUT"
for round in 1 2; do
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    for spec in "H1.content-20mb json-cursor" "H1.content-20mb protobuf-cursor"; do
      set -- $spec
      dir="$OUT/r${round}-${side}${leg}-$1-$2"
      mkdir -p "$dir"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario "$1" --protocol "$2" --rounds 1 --warmup 1 --samples 5 \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
  done
done
echo ABDONE
