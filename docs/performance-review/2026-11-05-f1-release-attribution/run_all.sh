#!/usr/bin/env bash
# F1.select-1 / json-pipeline release attribution window.
#
# 2 rounds x A/B/B/A = 8 legs. A = v1.2.6 platform+worker, B = v1.2.7
# platform+worker; both sides use the same ghcr.io/konghayao/peri-loom:v1.2.7
# image and mount their own platform/worker binaries over it. Fresh volume per
# leg. No observers, no TLS, default quotas.
#
# Deviation from the frozen release protocol (documented in perf-protocol.md):
# timed samples per leg are 20 instead of the frozen 5. The task asked for
# >=5 because the F1 leg-level spread was large; 20 gives a materially better
# median estimate at negligible runtime cost. The verdict rule is unchanged.
set -u
cd "$(dirname "$0")"
export WINDOW=f1attr
BASE_PLATFORM=/tmp/peri-release-ab/bin/peri-platform-v1.2.6
BASE_WORKER=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.6
CAND_PLATFORM=/tmp/peri-release-ab/bin/peri-platform-v1.2.7
CAND_WORKER=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7
BASE_PLATFORM_SHA=2a4a41d84011021e7c7b4d556f737a9582c5d7a48dec6a6c3887f6fbbae8294d
BASE_WORKER_SHA=61f742362c04d095b6f23754e22bf6aa2d807ac7154feaf4af211406282faeff
CAND_PLATFORM_SHA=95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912
CAND_WORKER_SHA=b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff
SAMPLES=${SAMPLES:-20}
LOG="$PWD/legs.txt"
: > "$LOG"
shasum -a 256 "$BASE_PLATFORM" "$BASE_WORKER" "$CAND_PLATFORM" "$CAND_WORKER" | tee -a "$LOG"
echo "samples_per_leg=$SAMPLES" | tee -a "$LOG"

run_scenario() {
  local scenario=$1
  local -a legs=(r1a1 r1b1 r1b2 r1a2 r2a1 r2b1 r2b2 r2a2)
  local -a sides=(A B B A A B B A)
  local -a ports=(20401 20402 20403 20404 20405 20406 20407 20408)
  local i side binp binw psha wsha
  for i in "${!legs[@]}"; do
    side=${sides[$i]}
    binp=$BASE_PLATFORM; binw=$BASE_WORKER
    psha=$BASE_PLATFORM_SHA; wsha=$BASE_WORKER_SHA
    if [ "$side" = B ]; then
      binp=$CAND_PLATFORM; binw=$CAND_WORKER
      psha=$CAND_PLATFORM_SHA; wsha=$CAND_WORKER_SHA
    fi
    echo "=== $(date '+%Y-%m-%dT%H:%M:%S%z') scenario=$scenario leg=${legs[$i]} side=$side port=${ports[$i]} load=$(sysctl -n vm.loadavg | tr ' ' '_') ===" >> "$LOG"
    bash ./run_leg.sh "$scenario" "${legs[$i]}" "${ports[$i]}" "$side" "$binp" "$binw" "$SAMPLES" "$psha" "$wsha" >> "$LOG" 2>&1
    echo "leg $scenario/${legs[$i]} exit=$?" >> "$LOG"
  done
}

run_scenario F1.select-1
echo "ALL_DONE $(date '+%Y-%m-%dT%H:%M:%S%z')" >> "$LOG"
