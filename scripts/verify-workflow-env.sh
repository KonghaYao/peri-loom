#!/usr/bin/env bash
# GitHub Actions env-context 静态 guard。
# 本仓库策略（比 GitHub 更严格）：workflow-level 与 job-level env 只允许
# github/secrets/inputs/vars。GitHub 本身允许 job-level env 使用 matrix/needs/
# strategy；本仓库刻意收紧到同一套规则，避免 workflow/job 两级语义漂移。
# guard 是已知 GitHub context 的 denylist：拒绝 runner/steps/job/jobs/matrix/
# needs/strategy/env。step-level env 不受限制（GitHub 允许 runner/steps/matrix 等）。
# 用缩进区分层级（本仓库统一 2-space：0 / 4 / 8），不依赖 PyYAML。
# 这是保守的文本 guard：遇到无法识别的 env 缩进直接失败，要求人工扩展/复核。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW_DIR="${1:-${REPO_ROOT}/.github/workflows}"

fail() { printf '[workflow-env] ERROR: %s\n' "$*" >&2; exit 1; }

self_test() {
  local tmp script
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/peri-workflow-env-self-test-XXXXXX")"
  trap "rm -rf -- '${tmp}'" EXIT
  script="${BASH_SOURCE[0]}"
  [[ "${script}" = /* ]] || script="$(pwd)/${script}"

  mkdir -p "${tmp}/bad-workflow" "${tmp}/bad-job" "${tmp}/bad-jobs" "${tmp}/good"
  cat > "${tmp}/bad-workflow/bad.yml" <<'YAML'
name: bad workflow env
on: push
env:
  BAD: ${{ runner.temp }}/x
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: step-level is allowed
        env:
          STEP: ${{ runner.temp }}/x
        run: echo ok
YAML
  cat > "${tmp}/bad-job/bad.yml" <<'YAML'
name: bad job env
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      BAD: ${{ runner.temp }}/x
    steps:
      - run: echo ok
YAML
  cat > "${tmp}/bad-jobs/bad.yml" <<'YAML'
name: bad jobs context
on: push
env:
  BAD: ${{ jobs.build.outputs.x }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
YAML
  cat > "${tmp}/good/good.yml" <<'YAML'
name: good env
on: push
env:
  ROOT: ${{ github.workspace }}/x
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: step-level is allowed
        env:
          STEP: ${{ runner.temp }}/x
        run: echo ok
YAML

  if bash "${script}" "${tmp}/bad-workflow" >"${tmp}/bad-workflow.out" 2>"${tmp}/bad-workflow.err"; then
    fail "self-test: workflow-level runner context was not rejected"
  fi
  grep -Fq 'workflow-level env references disallowed context `runner`' "${tmp}/bad-workflow.err" \
    || fail "self-test: workflow-level failure did not identify runner"

  if bash "${script}" "${tmp}/bad-job" >"${tmp}/bad-job.out" 2>"${tmp}/bad-job.err"; then
    fail "self-test: job-level runner context was not rejected"
  fi
  grep -Fq 'job-level env references disallowed context `runner`' "${tmp}/bad-job.err" \
    || fail "self-test: job-level failure did not identify runner"

  if bash "${script}" "${tmp}/bad-jobs" >"${tmp}/bad-jobs.out" 2>"${tmp}/bad-jobs.err"; then
    fail "self-test: jobs context was not rejected"
  fi
  grep -Fq 'workflow-level env references disallowed context `jobs`' "${tmp}/bad-jobs.err" \
    || fail "self-test: jobs-context failure did not identify jobs"

  bash "${script}" "${tmp}/good" >"${tmp}/good.out" 2>"${tmp}/good.err" \
    || fail "self-test: valid env sample was rejected: $(cat "${tmp}/good.err")"
  grep -Fq 'checked 1 workflow-level env, 0 job-level env, 1 step-level env' "${tmp}/good.out" \
    || fail "self-test: valid sample classification/count mismatch"

  rm -rf -- "${tmp}"
  trap - EXIT
  printf '[workflow-env] self-test passed: workflow-level runner rejected, job-level runner rejected, jobs context rejected, step-level runner allowed\n'
}

if [[ "${1:-}" == "--self-test" ]]; then
  self_test
  exit 0
fi

shopt -s nullglob
files=("${WORKFLOW_DIR}"/*.yml)
[[ "${#files[@]}" -gt 0 ]] || fail "找不到 workflow 文件：${WORKFLOW_DIR}/*.yml"

awk '
function indent_of(s,   n) {
  n = match(s, /^ +/)
  return n == 0 ? 0 : RLENGTH
}

function report(level, file, line_no, name) {
  printf "[workflow-env] ERROR: %s:%d %s env references disallowed context `%s`; repo policy: workflow/job env is restricted to github, secrets, inputs, vars (GitHub allows matrix/needs/strategy at job-level; this repo intentionally rejects them; step-level is not restricted)\n", \
    file, line_no, level, name > "/dev/stderr"
  violations++
}

function scan_exprs(text, level, file, line_no,   rest, expr, body, names, i, n, name) {
  rest = text
  while (match(rest, /\$\{\{.*\}\}/)) {
    expr = substr(rest, RSTART, RLENGTH)
    body = expr
    sub(/^\$\{\{[[:space:]]*/, "", body)
    sub(/[[:space:]]*\}\}$/, "", body)
    # Ignore quoted literals before looking for context names; a string such as
    # "runner" is data, not a context reference.
    gsub(/\047[^\047]*\047/, "", body)
    gsub(/"[^"]*"/, "", body)
    n = split("runner steps job jobs matrix needs strategy env", names, " ")
    for (i = 1; i <= n; i++) {
      name = names[i]
      if (body ~ ("(^|[^A-Za-z0-9_.-])" name "([^A-Za-z0-9_-]|$)")) {
        report(level, file, line_no, name)
      }
    }
    rest = substr(rest, RSTART + RLENGTH)
  }
}

BEGIN {
  violations = 0
  workflow_env = 0
  job_env = 0
  step_env = 0
  in_env = 0
  env_indent = -1
  env_level = ""
}

FNR == 1 { file = FILENAME }

{
  line = $0
  if (in_env && line ~ /^[[:space:]]*$/) next
  if (in_env && line ~ /^[[:space:]]*#/) next

  ind = indent_of(line)
  if (in_env && ind <= env_indent) {
    in_env = 0
  }

  if (line ~ /^[[:space:]]*-[[:space:]]+env:/) {
    # `- env:` on a step list item is step-level; unrestricted.
    step_env++
    next
  }

  if (line ~ /^[[:space:]]*env:/) {
    rest = line
    sub(/^[[:space:]]*env:[[:space:]]*/, "", rest)
    sub(/[[:space:]]*#.*$/, "", rest)

    if (ind == 0) {
      workflow_env++
      if (rest == "") {
        in_env = 1; env_indent = 0; env_level = "workflow-level"; next
      }
      scan_exprs(line, "workflow-level", file, FNR)
      next
    }
    if (ind == 4) {
      job_env++
      if (rest == "") {
        in_env = 1; env_indent = 4; env_level = "job-level"; next
      }
      scan_exprs(line, "job-level", file, FNR)
      next
    }
    if (ind == 8) {
      # step-level env may use runner/steps/matrix/...; classification is counted
      # and deliberately not restricted.
      step_env++
      next
    }
    printf "[workflow-env] ERROR: %s:%d unsupported env indentation %d; extend guard if this is a container/service env\n", \
      file, FNR, ind > "/dev/stderr"
    violations++
    next
  }

  if (!in_env) next
  scan_exprs(line, env_level, file, FNR)
}

END {
  printf "[workflow-env] checked %d workflow-level env, %d job-level env, %d step-level env (step-level contexts not restricted)\n", \
    workflow_env, job_env, step_env
  if (violations > 0) exit 1
}
' "${files[@]}"
