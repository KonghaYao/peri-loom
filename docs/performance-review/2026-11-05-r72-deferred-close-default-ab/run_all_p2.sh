#!/usr/bin/env bash
# R72 P2: run all four regression scenarios in the frozen alternating order.
# Round 1 A/B/B/A; round 2 B/A/A/B (per criteria.json / R71d criteria.txt).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
legs="r1-a1 r1-b1 r1-b2 r1-a2 r2-b1 r2-a1 r2-a2 r2-b2"
for scenario in f1 l1 x1 c8; do
  echo "===== R72 P2 $scenario start $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="
  # shellcheck disable=SC2086
  "$here/run_p2.sh" "$scenario" $legs
  echo "===== R72 P2 $scenario done $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="
done
echo "===== R72 P2 all scenarios complete $(date -u +%Y-%m-%dT%H:%M:%SZ) ====="
