#!/usr/bin/env bash
# 证据上传前的最后一道脱敏：用本次 fixture 的 connection.json 取出测试凭据做精确替换，
# 并覆盖所有大小写形式的 `Bearer <token>`（含裸 Bearer，不要求 Authorization 前缀）。
# 只读取、不打印凭据；connection.json 本身不在 artifact 路径中。
set -euo pipefail

fail() { printf '[sdk-matrix] ERROR: %s\n' "$*" >&2; exit 1; }

self_test() {
  local tmp script
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/peri-redact-self-test-XXXXXX")"
  trap "rm -rf -- '${tmp}'" EXIT
  script="${BASH_SOURCE[0]}"
  [[ "${script}" = /* ]] || script="$(pwd)/${script}"

  mkdir -p "${tmp}/fixture"
  cat > "${tmp}/fixture/connection.json" <<'JSON'
{"project_api_key":"unit-test-secret-0123456789abcdef","global_api_key":"unit-test-global-0123456789abcdef"}
JSON
  cat > "${tmp}/fixture/matrix.log" <<'LOG'
key=unit-test-secret-0123456789abcdef
Authorization: Bearer tok_1234567890
other Bearer tok_1234567890
already Bearer <REDACTED>
lower bearer tok_0987654321
LOG
  cat > "${tmp}/fixture/launcher.log" <<'LOG'
bare Bearer tok_abcdefghij
LOG

  bash "${script}" "${tmp}" >/dev/null
  grep -Fq 'Authorization: Bearer <REDACTED>' "${tmp}/fixture/matrix.log" \
    || fail "self-test: Authorization Bearer was not redacted"
  grep -Fq 'other Bearer <REDACTED>' "${tmp}/fixture/matrix.log" \
    || fail "self-test: bare Bearer was not redacted"
  grep -Fq 'lower Bearer <REDACTED>' "${tmp}/fixture/matrix.log" \
    || fail "self-test: case-insensitive bearer was not redacted"
  grep -Fq 'already Bearer <REDACTED>' "${tmp}/fixture/matrix.log" \
    || fail "self-test: existing placeholder changed"
  grep -Fq 'bare Bearer <REDACTED>' "${tmp}/fixture/launcher.log" \
    || fail "self-test: launcher bare Bearer was not redacted"
  if grep -Eq 'unit-test-secret|unit-test-global|tok_1234567890|tok_0987654321|tok_abcdefghij' \
    "${tmp}/fixture/matrix.log" "${tmp}/fixture/launcher.log"; then
    fail "self-test: credential material remained after redaction"
  fi

  cp "${tmp}/fixture/matrix.log" "${tmp}/matrix.before"
  cp "${tmp}/fixture/launcher.log" "${tmp}/launcher.before"
  bash "${script}" "${tmp}" >/dev/null
  cmp -s "${tmp}/matrix.before" "${tmp}/fixture/matrix.log" \
    || fail "self-test: redaction is not idempotent for matrix.log"
  cmp -s "${tmp}/launcher.before" "${tmp}/fixture/launcher.log" \
    || fail "self-test: redaction is not idempotent for launcher.log"

  rm -rf -- "${tmp}"
  trap - EXIT
  printf '[sdk-matrix] redaction self-test passed: exact key, Authorization Bearer, bare Bearer, case-insensitive, idempotent\n'
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

root="${1:?usage: redact-matrix-evidence.sh [--self-test] <matrix-evidence-dir>}"
if [[ ! -d "${root}" ]]; then
  printf '[sdk-matrix] 证据目录不存在，跳过脱敏：%s\n' "${root}" >&2
  exit 0
fi

python3 - "${root}" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
secrets = set()
for connection in root.glob("*/connection.json"):
    try:
        payload = json.loads(connection.read_text())
    except (OSError, ValueError):
        continue
    for name in ("project_api_key", "global_api_key"):
        value = payload.get(name)
        if isinstance(value, str) and len(value) >= 8:
            secrets.add(value.encode())

# Bare Bearer is covered too: a test log must not need an Authorization prefix
# to leak a token. The replacement is idempotent for the placeholder itself.
bearer = re.compile(rb"(?i)\bBearer\s+\S+")
changed = 0
for pattern in ("*/matrix.log", "*/launcher.log"):
    for log in root.glob(pattern):
        try:
            original = log.read_bytes()
        except OSError:
            continue
        updated = original
        for secret in secrets:
            updated = updated.replace(secret, b"<REDACTED>")
        updated = bearer.sub(b"Bearer <REDACTED>", updated)
        if updated != original:
            log.write_bytes(updated)
            changed += 1

print(f"[sdk-matrix] redacted credential material in {changed} evidence file(s)")
PY
