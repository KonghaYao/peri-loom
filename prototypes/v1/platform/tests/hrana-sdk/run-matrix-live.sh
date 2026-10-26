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

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    print(listener.getsockname()[1])
PY
}

launcher=""
trap 'if [[ -n "${launcher:-}" ]]; then kill "$launcher" 2>/dev/null || true; wait "$launcher" 2>/dev/null || true; fi' EXIT

# The launcher preflights every listen address before writing state. A port can be
# claimed between that check and bind; only this pre-startup failure is retried, with
# a fresh fixture and port. No SQL request has run while the launcher is not ready.
start_launcher_attempt() {
  fixture="$(mktemp -d "${TMPDIR:-/tmp}/peri-hrana-matrix-XXXXXX")"
  log="$fixture/matrix.log"
  local port
  port="$(free_port)"
  printf 'Fixture: %s\nLog: %s\n' "$fixture" "$log"
  PERI_HRANA_EXPERIMENTAL=1 PERI_PLATFORM_BINARY="$platform_binary" PERI_RUST_BINARY="$rust_binary" \
    python3 "$repo/prototypes/v1/platform/dev.py" --tls --no-build --listen "127.0.0.1:$port" --run-dir "$fixture" > "$fixture/launcher.log" 2>&1 &
  launcher=$!
}

ready=0
for start_attempt in 1 2 3; do
  start_launcher_attempt
  outcome=timeout
  for ((attempt = 0; attempt < 120; attempt++)); do
    if [[ -f "$fixture/connection.json" ]]; then outcome=ready; break; fi
    if ! kill -0 "$launcher" 2>/dev/null; then outcome=stopped; break; fi
    sleep 0.2
  done
  if [[ "$outcome" == stopped ]]; then
    wait "$launcher" 2>/dev/null || true
    launcher=""
  fi
  if [[ "$outcome" == ready ]]; then ready=1; break; fi
  if [[ "$outcome" == stopped ]] && (( start_attempt < 3 )) \
     && grep -q 'listener unavailable before startup' "$fixture/launcher.log"; then
    printf 'Launcher pre-startup listener collision; retrying with a fresh fixture (attempt %s/3)\n' "$((start_attempt + 1))" >&2
    continue
  fi
  if [[ "$outcome" == stopped ]]; then
    printf 'Launcher stopped before readiness; see %s\n' "$fixture/launcher.log" >&2
  else
    printf 'Launcher readiness timed out; see %s\n' "$fixture/launcher.log" >&2
  fi
  exit 1
done
if [[ "$ready" != 1 ]]; then
  printf 'Launcher did not become ready after 3 attempts\n' >&2
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
# E1：HTTP reader.cancel 没有服务器取消确认；在途 fetch 可能已经提交。
# 保留原用例和断言；显式开启时仍按原始退出码失败，不能计为通过。
known_gaps=(http-abort-write-cursor-live)
include_gaps="${PERI_HRANA_INCLUDE_KNOWN_GAPS:-0}"
if [[ "$include_gaps" != 0 && "$include_gaps" != 1 ]]; then
  printf 'PERI_HRANA_INCLUDE_KNOWN_GAPS must be 0 or 1\n' >&2
  exit 2
fi
if [[ -n "${PERI_HRANA_TEST_SCRIPTS:-}" ]]; then
  read -r -a selected <<< "$PERI_HRANA_TEST_SCRIPTS"
  for script in "${selected[@]}"; do
    valid=0
    for candidate in "${scripts[@]}"; do
      if [[ "$script" == "$candidate" ]]; then valid=1; break; fi
    done
    if [[ "$valid" != 1 ]]; then
      printf 'Unknown matrix script: %s\n' "$script" >&2
      exit 2
    fi
  done
  scripts=("${selected[@]}")
fi
printf 'KNOWN-GAP E1: %s.mjs; HTTP v3 write cursor abort has no acknowledged rollback proof; context cancellation may quarantine the database. include=%s\n' "${known_gaps[0]}" "$include_gaps" | tee -a "$log"
passed=0
skipped=0
for ((round = 1; round <= rounds; round++)); do
  for index in "${!scripts[@]}"; do
    script="${scripts[index]}"
    if [[ "$script" == "${known_gaps[0]}" && "$include_gaps" == 0 ]]; then
      printf 'SKIP known-gap=E1 round=%s script=%s.mjs reason=no-acknowledged-abort-proof\n' "$round" "$script" | tee -a "$log"
      skipped=$((skipped + 1))
      continue
    fi
    printf 'Round %s/%s, script %s/%s: %s.mjs\n' "$round" "$rounds" "$((index + 1))" "${#scripts[@]}" "$script" | tee -a "$log"
    if node "$sdk_dir/$script.mjs" "$fixture/connection.json" >> "$log" 2>&1; then
      passed=$((passed + 1))
      continue
    else
      status=$?
      printf 'FAIL round=%s script=%s.mjs exit=%s; do not reuse this fixture or retry an unknown write\n' "$round" "$script" "$status" | tee -a "$log" >&2
      exit "$status"
    fi
  done
done
printf 'RESULT: passed=%s failed=0 skipped_known_gap=%s rounds=%s; E1 remains unproven, not a full compatibility PASS\n' "$passed" "$skipped" "$rounds" | tee -a "$log"
