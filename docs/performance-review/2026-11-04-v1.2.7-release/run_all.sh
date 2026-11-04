#!/usr/bin/env bash
# v1.2.7 release A/B: 4 scenarios x 2 rounds x A/B/B/A, one leg at a time.
# A = v1.2.6, B = v1.2.7. Both sides use the same v1.2.7 image; the platform
# and worker binaries are mounted from each tag. Fresh volume per leg.
set -u
cd "$(dirname "$0")"
BASE_PLATFORM=/tmp/peri-release-ab/bin/peri-platform-v1.2.6
BASE_WORKER=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.6
CAND_PLATFORM=/tmp/peri-release-ab/bin/peri-platform-v1.2.7
CAND_WORKER=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7
LOG="$PWD/legs.txt"
: > "$LOG"
shasum -a 256 "$BASE_PLATFORM" "$BASE_WORKER" "$CAND_PLATFORM" "$CAND_WORKER" | tee -a "$LOG"

run_scenario() {
  local scenario=$1 samples=$2
  local -a legs=(r1a1 r1b1 r1b2 r1a2 r2a1 r2b1 r2b2 r2a2)
  local -a sides=(A B B A A B B A)
  local -a ports=(20401 20402 20403 20404 20405 20406 20407 20408)
  local i side binp binw
  for i in "${!legs[@]}"; do
    side=${sides[$i]}
    binp=$BASE_PLATFORM; binw=$BASE_WORKER
    if [ "$side" = B ]; then binp=$CAND_PLATFORM; binw=$CAND_WORKER; fi
    echo "=== $(date '+%Y-%m-%dT%H:%M:%S%z') scenario=$scenario leg=${legs[$i]} side=$side port=${ports[$i]} ===" >> "$LOG"
    bash ./run_leg.sh "$scenario" "${legs[$i]}" "${ports[$i]}" "$side" "$binp" "$binw" "$samples" >> "$LOG" 2>&1
    echo "leg $scenario/${legs[$i]} exit=$?" >> "$LOG"
  done
}

run_scenario W1.insert 5
run_scenario F1.select-1 5
run_scenario X1.concurrency 1
run_scenario H1.content-20mb 3
echo "ALL_DONE $(date '+%Y-%m-%dT%H:%M:%S%z')" >> "$LOG"
