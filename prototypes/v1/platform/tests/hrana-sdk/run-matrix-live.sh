#!/usr/bin/env bash
set -euo pipefail

sdk_dir="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$sdk_dir/../../../../.." && pwd)"
rust_binary="${PERI_RUST_BINARY:-$repo/prototypes/v1/target/debug/peri-loom-v1}"
platform_binary="${PERI_PLATFORM_BINARY:?set PERI_PLATFORM_BINARY to a built platform executable}"
rounds="${PERI_HRANA_TEST_ROUNDS:-2}"
if [[ ! "$rounds" =~ ^[1-5]$ || ! -x "$rust_binary" || ! -x "$platform_binary" ]]; then
  printf 'Expected 1–5 rounds and executable PERI_RUST_BINARY / PERI_PLATFORM_BINARY\n' >&2
  exit 2
fi

fixture="$(mktemp -d "${TMPDIR:-/tmp}/peri-hrana-matrix-XXXXXX")"
log="$fixture/matrix.log"
port="$(python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    print(listener.getsockname()[1])
PY
)"
printf 'Fixture: %s\nLog: %s\n' "$fixture" "$log"
PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform_binary" PERI_RUST_BINARY="$rust_binary" \
  python3 "$repo/prototypes/v1/platform/dev.py" --tls --no-build --listen "127.0.0.1:$port" --run-dir "$fixture" > "$fixture/launcher.log" 2>&1 &
launcher=$!
trap 'kill "$launcher" 2>/dev/null || true; wait "$launcher" 2>/dev/null || true' EXIT
for ((attempt = 0; attempt < 120; attempt++)); do
  if [[ -f "$fixture/connection.json" ]]; then break; fi
  if ! kill -0 "$launcher" 2>/dev/null; then
    printf 'Launcher stopped before readiness; see %s\n' "$fixture/launcher.log" >&2
    exit 1
  fi
  sleep 0.2
done
if [[ ! -f "$fixture/connection.json" ]]; then
  printf 'Launcher readiness timed out; see %s\n' "$fixture/launcher.log" >&2
  exit 1
fi
export NODE_EXTRA_CA_CERTS="$(node -e 'console.log(require(process.argv[1]).ca_file)' "$fixture/connection.json")"

scripts=(
  read-only-live live auth-live v3-live conditions-live ws-live
  large-cursor-live large-http-cursor-live
  multi-step-large-cursor-live multi-step-large-http-cursor-live
  conditional-large-cursor-live mixed-large-cursor-live
  large-write-cursor-live abort-write-cursor-live
  http-abort-write-cursor-live ws-abort-write-cursor-live
  high-level-ws-live sequence-failure-prefix-live
  high-level-large-live high-level-large-write-live
  discard-large-read-live high-level-mixed-large-batch-live
  http-cached-sql-live high-level-sequence-read-live auth-negative-extended-live
  high-level-result-metadata-live http-v3-simple-live
)
for ((round = 1; round <= rounds; round++)); do
  for index in "${!scripts[@]}"; do
    script="${scripts[index]}"
    printf 'Round %s/%s, script %s/%s: %s.mjs\n' "$round" "$rounds" "$((index + 1))" "${#scripts[@]}" "$script" | tee -a "$log"
    if node "$sdk_dir/$script.mjs" "$fixture/connection.json" >> "$log" 2>&1; then
      continue
    else
      status=$?
      printf 'FAIL round=%s script=%s.mjs exit=%s; do not reuse this fixture or retry an unknown write\n' "$round" "$script" "$status" | tee -a "$log" >&2
      exit "$status"
    fi
  done
done
printf 'PASS: %s rounds x %s scripts\n' "$rounds" "${#scripts[@]}" | tee -a "$log"
