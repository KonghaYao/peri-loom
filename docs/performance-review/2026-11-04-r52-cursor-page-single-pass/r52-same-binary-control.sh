#!/bin/bash
# R52 stack-variance control: BOTH stacks run the SAME base binary
# (peri-platform-r52-base) on fresh, identically seeded volumes.
# Port 19501 = A, port 19502 = B; 8 legs alternating, 15 samples/leg.
set -u
cd /Users/kk/code/peri-loom || exit 1
A=/tmp/peri-perf/run-r52same-a/connection.json
B=/tmp/peri-perf/run-r52same-b/connection.json
OUT=${1:-/tmp/peri-perf/r52same}
rm -rf "$OUT"; mkdir -p "$OUT"
for leg in 1 2 3 4 5 6 7 8; do
  case $leg in
    1|4|5|8) side=a; conn="$A" ;;
    2|3|6|7) side=b; conn="$B" ;;
  esac
  dir="$OUT/R1.limit-1000.json-pipeline.${side}${leg}"
  python3 scripts/perf-reliability/issue28/run.py --connection-file "$conn" \
    --scenario R1.limit-1000 --protocol json-pipeline --rounds 1 --warmup 1 --samples 15 \
    --out "$dir" > "$dir.log" 2>&1
  echo "done $side$leg rc=$?"
  sleep 2
done
echo SAMEDONE
