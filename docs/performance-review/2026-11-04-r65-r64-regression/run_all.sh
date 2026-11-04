#!/usr/bin/env bash
# R65: 3 scenarios x 2 rounds x A/B/B/A on fresh stacks, one leg at a time.
# A = main@1399f15 (pre-merge), B = main@1a6b9dd (post-merge, rebuilt).
# Ports 20301-20308 per scenario; the frozen protocol fixes this range.
set -u
cd "$(dirname "$0")"
BASE=/tmp/peri-perf/bin/peri-platform-r62-main
CAND=/tmp/peri-perf/bin/peri-platform-r65-main
WORKER=/tmp/peri-perf/bin/peri-loom-v1-r58-main
LOG="$PWD/legs.txt"
: > "$LOG"
shasum -a 256 "$BASE" "$CAND" "$WORKER" | tee -a "$LOG"

run_scenario() {
  local scenario=$1 samples=$2
  local -a legs=(r1a1 r1b1 r1b2 r1a2 r2a1 r2b1 r2b2 r2a2)
  local -a sides=(A B B A A B B A)
  local -a ports=(20301 20302 20303 20304 20305 20306 20307 20308)
  local i bin side
  for i in "${!legs[@]}"; do
    side=${sides[$i]}
    bin=$BASE
    [ "$side" = B ] && bin=$CAND
    echo "=== $(date '+%Y-%m-%dT%H:%M:%S%z') scenario=$scenario leg=${legs[$i]} side=$side port=${ports[$i]} ===" >> "$LOG"
    bash ./run_leg.sh "$scenario" "${legs[$i]}" "${ports[$i]}" "$bin" "$samples" >> "$LOG" 2>&1
    echo "leg $scenario/${legs[$i]} exit=$?" >> "$LOG"
  done
}

run_scenario W1.insert 5
run_scenario F1.select-1 5
run_scenario H1.content-20mb 3
echo "ALL_DONE $(date '+%Y-%m-%dT%H:%M:%S%z')" >> "$LOG"
