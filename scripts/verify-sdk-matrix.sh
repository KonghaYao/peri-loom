#!/usr/bin/env bash
# SDK 矩阵覆盖率门禁：脚本清单与 known-gap 清单的唯一来源是 run-matrix-live.sh。
# declared 是声明覆盖；default_executed 是默认门禁真正执行的覆盖。E1 是显式
# known-gap（默认跳过），因此默认是 26/27 per round，不能写成 27/27 或 full PASS。
# known-gap 清单、runner 默认轮数或 include 默认值变化时本脚本失败，要求显式更新
# 本脚本、workflow 步骤名与 release notes，避免覆盖率口径随 runner 静默变化。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="${REPO_ROOT}/prototypes/v1/platform/tests/hrana-sdk/run-matrix-live.sh"
SDK_DIR="$(dirname "${RUNNER}")"

EXPECTED_DECLARED=27
EXPECTED_ROUNDS=2
EXPECTED_INCLUDE_DEFAULT=0
EXPECTED_GAP_NAMES=(http-abort-write-cursor-live)

fail() { printf '[sdk-matrix] ERROR: %s\n' "$*" >&2; exit 1; }

# Workflow/job-level env 不得引用 runner/steps/job/matrix/needs 等 step-only
# context；该 guard 同时检查三个 workflow，避免 PyYAML 通过但 Actions 拒绝。
bash "${REPO_ROOT}/scripts/verify-workflow-env.sh"

contains() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    [[ "$item" == "$needle" ]] && return 0
  done
  return 1
}

[[ -f "${RUNNER}" ]] || fail "找不到矩阵入口：${RUNNER}"

rounds="${PERI_HRANA_TEST_ROUNDS:-2}"
include="${PERI_HRANA_INCLUDE_KNOWN_GAPS:-0}"
[[ "${rounds}" =~ ^[1-5]$ ]] || fail "PERI_HRANA_TEST_ROUNDS 必须是 1-5，当前为 ${rounds}"
[[ "${include}" == 0 || "${include}" == 1 ]] || fail "PERI_HRANA_INCLUDE_KNOWN_GAPS 必须是 0 或 1，当前为 ${include}"
[[ "${rounds}" == "${EXPECTED_ROUNDS}" ]] \
  || fail "矩阵轮数为 ${rounds}，门禁口径是 ${EXPECTED_ROUNDS}；若有意修改，请同步更新本脚本、workflow 步骤名与 release notes"
[[ "${include}" == "${EXPECTED_INCLUDE_DEFAULT}" ]] \
  || fail "阻塞门禁默认不得包含 known-gap（当前 include=${include}）；known-gap 只能放在独立、非阻塞 job 中显式开启"
[[ -z "${PERI_HRANA_TEST_SCRIPTS:-}" ]] \
  || fail "PERI_HRANA_TEST_SCRIPTS 不得用于阻塞门禁；默认矩阵必须按完整声明清单执行"

# 从 runner 读取 "name=( ... )" 数组，兼容单行与多行写法；只做词法拆分，不执行 runner。
matrix_array() {
  awk -v name="$1" '
    index($0, name "=(") == 1 { capture = 1; sub("^" name "=\\(", "") }
    capture {
      if (sub(/\)[[:space:]]*$/, "")) { print; exit }
      print
    }
  ' "${RUNNER}" | tr '\n' ' '
}

read -r -a scripts <<< "$(matrix_array scripts)"
read -r -a known_gaps <<< "$(matrix_array known_gaps)"

[[ "${#scripts[@]}" -eq "${EXPECTED_DECLARED}" ]] \
  || fail "declared 脚本数为 ${#scripts[@]}，期望 ${EXPECTED_DECLARED}；若官方矩阵确有增减，请同步更新本门禁与文案"

unique="$(printf '%s\n' "${scripts[@]}" | sort -u | wc -l | tr -d ' ')"
[[ "${unique}" -eq "${EXPECTED_DECLARED}" ]] \
  || fail "declared 脚本去重后为 ${unique}，期望 ${EXPECTED_DECLARED}（存在重复项）"

for script in "${scripts[@]}"; do
  [[ -f "${SDK_DIR}/${script}.mjs" ]] || fail "矩阵声明的脚本不存在：${script}.mjs"
done

[[ "${#known_gaps[@]}" -eq "${#EXPECTED_GAP_NAMES[@]}" ]] \
  || fail "known-gap 数量为 ${#known_gaps[@]}，期望 ${#EXPECTED_GAP_NAMES[@]}；若清单变化，请显式更新本门禁与文案"

unique_gaps="$(printf '%s\n' "${known_gaps[@]}" | sort -u | wc -l | tr -d ' ')"
[[ "${unique_gaps}" -eq "${#known_gaps[@]}" ]] || fail "known-gap 清单存在重复项"

for gap in "${known_gaps[@]}"; do
  contains "${gap}" "${scripts[@]}" || fail "known-gap ${gap} 不在 declared scripts 中"
  contains "${gap}" "${EXPECTED_GAP_NAMES[@]}" \
    || fail "出现未登记的 known-gap ${gap}；请显式更新本门禁、workflow 步骤名与 release notes"
done
for expected in "${EXPECTED_GAP_NAMES[@]}"; do
  contains "${expected}" "${known_gaps[@]}" || fail "期望的 known-gap ${expected} 未出现在 runner 的 known_gaps 中"
done

# runner 自身的默认值也是口径的一部分：默认轮数或默认 include 改变时，CI 的
# 26/27、52 runs、2 skips 文案会失真，必须在同一变更里显式更新。
runner_rounds="$(sed -n 's/^rounds="\${PERI_HRANA_TEST_ROUNDS:-\([0-9]\)}"$/\1/p' "${RUNNER}" | head -n 1)"
runner_include="$(sed -n 's/^include_gaps="\${PERI_HRANA_INCLUDE_KNOWN_GAPS:-\([0-9]\)}"$/\1/p' "${RUNNER}" | head -n 1)"
[[ "${runner_rounds}" == "${EXPECTED_ROUNDS}" ]] \
  || fail "runner 默认轮数为 '${runner_rounds}'，门禁口径是 ${EXPECTED_ROUNDS}；请显式更新本门禁与文案"
[[ "${runner_include}" == "${EXPECTED_INCLUDE_DEFAULT}" ]] \
  || fail "runner 默认 include 为 '${runner_include}'，门禁口径是 ${EXPECTED_INCLUDE_DEFAULT}；请显式更新本门禁与文案"

executed_per_round=$(( ${#scripts[@]} - ${#known_gaps[@]} ))
runs=$(( executed_per_round * rounds ))
skipped=$(( ${#known_gaps[@]} * rounds ))

printf '[sdk-matrix] declared=%s/%s; default_executed=%s/%s per round; rounds=%s; runs=%s; skipped_known_gap=%s\n' \
  "${#scripts[@]}" "${EXPECTED_DECLARED}" "${executed_per_round}" "${EXPECTED_DECLARED}" \
  "${rounds}" "${runs}" "${skipped}"
printf '[sdk-matrix] known-gap E1: %s (default skipped; include=%s)\n' \
  "${known_gaps[0]}" "${include}"
printf '[sdk-matrix] E1 remains unproven; this is not a full Hrana compatibility claim\n'
