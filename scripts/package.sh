#!/usr/bin/env bash
# 把整个项目打成压缩包（不含依赖与构建产物）。
#
# 打什么：
#   - 全部 git 跟踪的文件（源码、proto、migrations、部署文件、脚本、文档）
#   - .git 目录本身（解压出来就是可直接 git log / git checkout 的仓库）
#
# 不打什么：
#   - target/            Rust 构建产物（本机 40G，由 cargo build 重新生成）
#   - web/node_modules/  前端依赖（由 npm install 重装，国内走 npmmirror）
#   - web/dist/          前端构建产物（由 npm run build 重新生成）
#   - deploy/secrets/    真实凭据目录 —— **整目录排除**，由 scripts/gen-secrets.sh 重建
#   - .env 与 .env.*     本地运行配置（含密码），从 .env.example 复制即可
#   - .claude/           agent 运行产物，与项目本身无关
#
# 用法：
#   ./scripts/package.sh                    # 输出到 ../peri-loom-<commit>-<时间>.tar.gz
#   ./scripts/package.sh /tmp/out.tar.gz    # 指定输出路径
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

COMMIT="$(git rev-parse --short HEAD)"
STAMP="$(date +%Y%m%d-%H%M)"
OUT="${1:-$(dirname "${REPO_ROOT}")/peri-loom-${COMMIT}-${STAMP}.tar.gz}"

if ! git diff-index --quiet HEAD --; then
  echo "警告：工作区有未提交的改动（它们会进包，但不会进 .git 的历史）。" >&2
fi

echo "打包 ${REPO_ROOT} @ ${COMMIT} -> ${OUT}"

# 注意：tar 以 -C 父目录 运行，包内路径形如 peri-loom/deploy/secrets/x.txt。
# 因此排除模式**不能**写成 ./deploy/...（那样一条都匹配不到，凭据会被打进包）。
tar \
  --exclude='target' \
  --exclude='*/target' \
  --exclude='*/*/target' \
  --exclude='node_modules' \
  --exclude='*/node_modules' \
  --exclude='*/*/node_modules' \
  --exclude='dist' \
  --exclude='*/dist' \
  --exclude='*/deploy/secrets' \
  --exclude='*.key' \
  --exclude='*.crt' \
  --exclude='*.pem' \
  --exclude='.env' \
  --exclude='.claude' \
  --exclude='*/.claude' \
  --exclude='*/.local' \
  --exclude='*/.run' \
  --exclude="$(basename "${REPO_ROOT}")/data" \
  --exclude='*.log' \
  -czf "${OUT}" \
  -C "$(dirname "${REPO_ROOT}")" "$(basename "${REPO_ROOT}")"

# ------------------------------------------------------------------ 校验
# 先把清单落到文件再判断。直接把 grep 接进 head 会因为 SIGPIPE 让管道状态失真，
# 结论会反过来（这正是上一版把“含凭据”判成“通过”的原因）。
LIST="$(mktemp)"
trap 'rm -f "${LIST}"' EXIT
tar -tzf "${OUT}" > "${LIST}"

fail=0
check_absent() {
  local desc="$1" pattern="$2"
  local hits
  hits="$(grep -E "${pattern}" "${LIST}" || true)"
  if [ -n "${hits}" ]; then
    echo "  ✗ 仍然包含 ${desc}：" >&2
    printf '%s\n' "${hits}" | head -10 >&2
    fail=1
  fi
}

check_absent "依赖/构建产物目录" '(^|/)(target|node_modules|dist)/'
check_absent "凭据文件"          '/deploy/secrets/[^/]+$'
# 只匹配真正含密钥的 .env（.env.example 是必须随包分发的模板）
check_absent ".env（真实配置）"  '/\.env$'
check_absent "agent 产物"        '/\.claude/'

if [ "${fail}" -ne 0 ]; then
  echo "打包失败：包内含不应分发的文件，已删除 ${OUT}" >&2
  rm -f "${OUT}"
  exit 1
fi

echo "  校验通过：无 target/、node_modules/、dist/、凭据、.env、agent 产物"
echo "  文件数：$(wc -l < "${LIST}")"
echo "  体积：$(du -h "${OUT}" | cut -f1)"
echo
echo "提示：包内含 .git，解压后可直接 git log；构建前请先"
echo "      cp .env.example .env && ./scripts/gen-secrets.sh && ./scripts/docker-pull.sh"
