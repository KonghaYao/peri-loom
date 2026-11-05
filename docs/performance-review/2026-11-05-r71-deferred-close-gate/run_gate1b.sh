#!/usr/bin/env bash
# R71 Gate 1b: block-mean A/B of "client close waits for the worker" versus the
# unsafe probe that dispatches close after replying.
#
# One campaign is 4 legs A/B/B/A x 2 rounds = 8 legs. Every leg is an isolated
# stack with a fresh named volume, the same fixture, default quotas, one worker
# binary and no observer. A leg runs one warm-up block plus 30 timed blocks of
# 100 sequential [execute SELECT 1, close] pipeline requests; a sample is the
# block's wall clock / 100.
#
# Campaigns (run in this order; each one is judged before the next starts):
#   aa         A and B are both the v1.2.7 release platform binary.
#              Merged ratio must be inside 1.00 +/- 0.05.
#   control    B and A are both the probe binary; B sets
#              PERI_R71_CLOSE_DELAY_US=100. The +100us injection must be
#              detected (>= 8%, both rounds same direction), otherwise the
#              protocol has no power and we stop.
#   candidate  B is the probe binary without the delay. Decision ratio:
#              <= 0.85 implement the safe version; 0.85-0.90 borderline, do
#              not implement by default; > 0.90 stop.
#
# The probe branch is never merged or pushed. Its launcher is mounted from the
# probe worktree for BOTH sides so the launcher is identical across legs; the
# only launcher difference from the image is an inert env passthrough that no A
# leg sets.
#
# Usage:
#   ./run_gate1b.sh aa
#   ./run_gate1b.sh control
#   ./run_gate1b.sh candidate
# Environment:
#   REPO          repository root (default: three levels above this script)
#   IMAGE         runtime image (default: ghcr.io/konghayao/peri-loom:v1.2.7)
#   BASELINE      baseline platform binary (default: v1.2.7 release artifact)
#   PROBE         probe platform binary (default: /tmp/r71-bin/peri-platform-probe)
#   PROBE_DEV     launcher from the probe worktree
#   WORKER        worker binary, identical on both sides
#   OUT           evidence directory (default: this script's directory)
#   LEGS          optional space-separated leg override (smoke runs only)
set -euo pipefail

MODE=${1:-}
case "$MODE" in
  aa)
    A_BIN=${BASELINE:-/tmp/peri-release-ab/bin/peri-platform-v1.2.7}
    B_BIN=$A_BIN
    B_DELAY=""
    ;;
  smoke*)
    # Wiring checks before the frozen campaigns; never analyzed.
    A_BIN=${BASELINE:-/tmp/peri-release-ab/bin/peri-platform-v1.2.7}
    B_BIN=$A_BIN
    B_DELAY=""
    ;;
  control|control2)
    # Both sides are the probe binary; only B carries the +100us injection.
    # Comparing baseline vs probe+delay would confound the injection with the
    # probe's own effect, so the control isolates the known 100us only.
    # control used time.Sleep, which this VM inflated to ~1.1ms; control2 uses
    # the spin implementation and is the valid 100us control.
    A_BIN=${PROBE:-/tmp/r71-bin/peri-platform-probe}
    B_BIN=$A_BIN
    B_DELAY="100"
    ;;
  candidate)
    A_BIN=${BASELINE:-/tmp/peri-release-ab/bin/peri-platform-v1.2.7}
    B_BIN=${PROBE:-/tmp/r71-bin/peri-platform-probe}
    B_DELAY=""
    ;;
  *)
    echo "usage: $0 aa|control|candidate" >&2
    exit 2
    ;;
esac

here=$(cd "$(dirname "$0")" && pwd)
REPO=${REPO:-$(cd "$here/../../.." && pwd)}
IMAGE=${IMAGE:-ghcr.io/konghayao/peri-loom:v1.2.7}
WORKER=${WORKER:-/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7}
PROBE_DEV=${PROBE_DEV:-/tmp/peri-r70-close/prototypes/v1/platform/dev.py}
OUT=${OUT:-$here}
SEED=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py
SNAP=$REPO/docs/performance-review/2026-11-04-v1.2.7-release/snap_metrics.py
DRIVER=$here/block_driver.py
LEGS=${LEGS:-"r1-a1 r1-b1 r1-b2 r1-a2 r2-a1 r2-b1 r2-b2 r2-a2"}
BASE_PORT=${BASE_PORT:-20780}

mkdir -p "$OUT/legs"
snap() { python3 "$SNAP" "$1" "$2"; }

index=0
for leg in $LEGS; do
  case "$leg" in
    *-a*) side=a; binary=$A_BIN; delay="" ;;
    *-b*) side=b; binary=$B_BIN; delay=$B_DELAY ;;
    *) echo "bad leg name $leg" >&2; exit 2 ;;
  esac
  round=${leg%%-*}
  port=$((BASE_PORT + index))
  container="peri-r71-g1b-$MODE-$leg"
  volume="peri-r71-g1b-$MODE-$leg-data"
  state="/tmp/peri-r71-g1b/$MODE/$leg"
  legdir="$OUT/legs/$leg-$MODE"
  mkdir -p "$state" "$legdir"
  index=$((index + 1))

  echo "== leg $leg ($MODE) side=$side port=$port binary=$(basename "$binary") delay=${delay:-none}"
  {
    echo "campaign: $MODE"
    echo "leg: $leg  side: $side  round: $round"
    echo "host: $(uname -srm)"
    echo "image: $IMAGE"
    docker image inspect "$IMAGE" --format '  image id: {{.Id}}'
    echo "platform host path: $binary"
    echo "platform host sha256: $(shasum -a 256 "$binary" | awk '{print $1}')"
    echo "worker host path: $WORKER"
    echo "worker host sha256: $(shasum -a 256 "$WORKER" | awk '{print $1}')"
    echo "close delay env: ${delay:-<unset>}"
    echo "launcher: $PROBE_DEV (mounted for both sides)"
    echo "container: $container"
    echo "volume: $volume (fresh)"
    echo "port: $port"
    echo "driver: $DRIVER block_size=100 blocks=30 warmup=1"
    echo "quotas: platform defaults (not relaxed)"
    echo "observer: none"
  } > "$legdir/binaries.txt" 2>&1

  docker volume create "$volume" >/dev/null
  docker rm -f "$container" >/dev/null 2>&1 || true
  docker run -d --name "$container" --platform linux/arm64 --network host --init --stop-timeout 60 \
    --security-opt no-new-privileges:true --cap-drop ALL \
    -e PERI_HRANA_EXPERIMENTAL=1 \
    ${delay:+-e PERI_R71_CLOSE_DELAY_US=$delay} \
    -e PERI_PLATFORM_BINARY=/usr/local/bin/peri-platform \
    -e PERI_RUST_BINARY=/usr/local/bin/peri-loom-v1 \
    -v "$volume":/data/peri \
    -v "$binary":/usr/local/bin/peri-platform:ro \
    -v "$WORKER":/usr/local/bin/peri-loom-v1:ro \
    -v "$PROBE_DEV":/app/prototypes/v1/platform/dev.py:ro \
    "$IMAGE" --run-dir /data/peri --backend local --listen 127.0.0.1:"$port" --no-build >/dev/null

  conn=$state/connection.json
  for _ in $(seq 1 120); do
    docker exec "$container" test -f /data/peri/connection.json 2>/dev/null && break
    sleep 1
  done
  docker exec "$container" test -f /data/peri/connection.json
  docker cp "$container":/data/peri/connection.json "$conn"
  chmod 600 "$conn"
  docker exec "$container" sha256sum /usr/local/bin/peri-platform /usr/local/bin/peri-loom-v1 \
    >> "$legdir/binaries.txt"
  # Prove whether the positive-control variable reached the launcher process.
  if docker exec "$container" printenv PERI_R71_CLOSE_DELAY_US >/dev/null 2>&1; then
    echo "launcher env PERI_R71_CLOSE_DELAY_US=$(docker exec "$container" printenv PERI_R71_CLOSE_DELAY_US)" \
      >> "$legdir/binaries.txt"
  else
    echo "launcher env PERI_R71_CLOSE_DELAY_US=<unset>" >> "$legdir/binaries.txt"
  fi

  python3 "$SEED" "$conn" > "$legdir/seed-verify.txt" 2> "$legdir/seed.log"
  snap "$conn" "$legdir/metrics-before.json"

  set +e
  python3 "$DRIVER" --connection-file "$conn" --repo "$REPO" --out "$legdir" \
    --side "$side" --leg "$leg" --round "$round" --block-size 100 --blocks 30 --warmup-blocks 1 \
    > "$legdir/driver.log" 2>&1
  echo "$?" > "$legdir/exit-code.txt"
  set -e
  snap "$conn" "$legdir/metrics-after.json"

  docker logs "$container" > "$legdir/container.log" 2>&1 || true
  docker stop -t 60 "$container" >/dev/null 2>&1 || true
  docker rm "$container" >/dev/null 2>&1 || true
  echo "leg $leg done exit=$(cat "$legdir/exit-code.txt")"
done

echo "campaign $MODE complete"
