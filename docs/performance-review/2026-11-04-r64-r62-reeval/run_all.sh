#!/usr/bin/env bash
# R64: 2 rounds x A/B/B/A on fresh stacks. A = baseline main@1399f15, B = perf/r62-deferred-bound@530fcf7.
# Ports 20101-20108; one leg at a time.
set -u
cd "$(dirname "$0")"
BASE=/tmp/peri-perf/bin/peri-platform-r62-main
CAND=/tmp/peri-perf/bin/peri-platform-r62-cand
LOG=/tmp/peri-perf/r64-legs.log
: > "$LOG"
run() { bash ./run_leg.sh "$1" "$2" "$3" >> "$LOG" 2>&1; echo "leg $1 exit=$?" >> "$LOG"; }

shasum -a 256 "$BASE" "$CAND" /tmp/peri-perf/bin/peri-loom-v1-r58-main >> "$LOG"

# round 1
run r1a1 20101 "$BASE"
run r1b1 20102 "$CAND"
run r1b2 20103 "$CAND"
run r1a2 20104 "$BASE"
# round 2
run r2a1 20105 "$BASE"
run r2b1 20106 "$CAND"
run r2b2 20107 "$CAND"
run r2a2 20108 "$BASE"

echo ALL_DONE >> "$LOG"
