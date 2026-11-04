#!/usr/bin/env bash
set -u
BASE=/tmp/peri-perf/bin/peri-platform-r62-main
CAND=/tmp/peri-perf/bin/peri-platform-r62-cand
: > /tmp/peri-perf/r63c1-legs.log
run() { bash /tmp/peri-perf/r63c1.sh "$1" "$2" "$3" "$4" >> /tmp/peri-perf/r63c1-legs.log 2>&1; echo "leg $1 exit=$?" >> /tmp/peri-perf/r63c1-legs.log; }
run p1 19901 "$CAND" B
run p2 19902 "$BASE" A
run p3 19903 "$BASE" A
run p4 19904 "$CAND" B
run p5 19905 "$CAND" B
run p6 19906 "$BASE" A
echo ALL_DONE >> /tmp/peri-perf/r63c1-legs.log
