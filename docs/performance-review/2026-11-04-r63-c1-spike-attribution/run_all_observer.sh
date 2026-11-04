#!/usr/bin/env bash
set -u
cd /tmp/peri-perf
BASE=/tmp/peri-perf/bin/peri-platform-r62-main
CAND=/tmp/peri-perf/bin/peri-platform-r62-cand
run() { bash /tmp/peri-perf/r63obs.sh "$1" "$2" "$3" "$4" >> /tmp/peri-perf/r63-legs.log 2>&1; echo "leg $1 exit=$?" >> /tmp/peri-perf/r63-legs.log; }
: > /tmp/peri-perf/r63-legs.log
run o1 19801 "$CAND" B
run o2 19802 "$BASE" A
run o3 19803 "$BASE" A
run o4 19804 "$CAND" B
run o5 19805 "$CAND" B
run o6 19806 "$BASE" A
run o7 19807 "$BASE" A
run o8 19808 "$CAND" B
echo ALL_DONE >> /tmp/peri-perf/r63-legs.log
