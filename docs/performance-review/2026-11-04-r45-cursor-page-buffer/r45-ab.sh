#!/bin/bash
# R45 A/B/B/A: base = main (peri-platform-r43-cand), cand = perf/r45-cursor-page-buffer (2a52f46)
# Design: 2 rounds x 4 legs (A,B,B,A) x 2 protocols x (1 warmup + 5 timed) H1 samples.
set -u
cd /Users/kk/code/peri-loom || exit 1
BASE=/tmp/peri-perf/run-r45base/connection.json
CAND=/tmp/peri-perf/run-r45cand/connection.json
OUT=${1:-/tmp/peri-perf/r45ab}
rm -rf "$OUT"; mkdir -p "$OUT"
for round in 1 2; do
  for leg in 1 2 3 4; do
    case $leg in
      1|4) side=base; conn="$BASE" ;;
      2|3) side=cand; conn="$CAND" ;;
    esac
    for proto in json-cursor protobuf-cursor; do
      dir="$OUT/r${round}-${side}${leg}-${proto}"
      python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
        --scenario H1.content-20mb --protocol "$proto" --rounds 1 --warmup 1 --samples 5 \
        --out "$dir" > "$dir.log" 2>&1
      sleep 2
    done
  done
done
echo ABDONE
