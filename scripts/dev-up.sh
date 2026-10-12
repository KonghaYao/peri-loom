#!/usr/bin/env bash
# 一键拉起本地开发平台：Rust authority + 两个加密 worker + Go gateway（含内嵌管理控制台）。
#
# 本脚本只是 dev.py 的便捷包装：构建、TLS 生成、readiness 校验与 joined shutdown
# 全部仍由 prototypes/v1/platform/dev.py 负责，脚本只做前置检查、默认参数和引导输出。
# 前台运行；Ctrl+C / SIGTERM 转发给启动器做优雅关闭；默认每次启动使用新的运行目录。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV_PY="${REPO_ROOT}/prototypes/v1/platform/dev.py"

log() { printf '\033[36m[dev-up]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[dev-up]\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
一键启动本地开发平台：Rust authority + 2 个加密 worker + Go gateway（内嵌管理控制台）。

用法：
  scripts/dev-up.sh [dev.py 参数...]

运行目录：
  默认每次启动在 /tmp 下生成新的私有 fixture 目录。owned worker incarnation 护栏
  拒绝同一目录二次启动，脚本不清理、
  不绕过护栏；需要指定固定目录时用 PERI_RUN_DIR 或透传 --run-dir，代价是被 fenced
  的目录会启动失败。

环境变量：
  PERI_RUN_DIR        显式指定运行目录（默认新建一次性目录）
  PERI_TLS            默认 1（开启 TLS）；设为 0 关闭
  PERI_PROJECT_ROOT   仓库根路径（默认按脚本位置推导）
  PERI_RUST_BINARY / PERI_PLATFORM_BINARY   自定义二进制路径（配合 --no-build）

常用透传参数（完整列表：python3 prototypes/v1/platform/dev.py -h）：
  --listen 127.0.0.1:9443   监听地址（默认）
  --backend s3              使用 S3 后端（需 S3_* 环境变量，仅 authority 接收）
  --no-build                跳过构建（要求二进制已存在）
  --writer-admission        启用 writer admission
  --stop-worker worker-a    停掉一个有主 worker（故障演练），需同一运行目录，执行后退出

就绪后启动器会打印一行 {"ready": true, "address": ...}；浏览器打开该 address 即进入
内嵌管理控制台，凭据见 <运行目录>/connection.json（0600，勿公开）。Ctrl+C 优雅关闭。
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

# --------------------------------------------------------------- 前置检查
command -v python3 >/dev/null 2>&1 || fail "找不到 python3"

[ -f "$DEV_PY" ] || fail "找不到启动器：${DEV_PY}"

# Turso 子模块是 Rust 数据面的构建依赖，未初始化时先拉取（幂等）。
if [ ! -e "${REPO_ROOT}/prototypes/p0/turso/core/Cargo.toml" ]; then
  command -v git >/dev/null 2>&1 || fail "子模块未初始化且找不到 git"
  log "初始化 Turso 子模块 prototypes/p0/turso（首次可能较慢）"
  git -C "$REPO_ROOT" submodule update --init prototypes/p0/turso \
    || fail "子模块初始化失败；请先确认 Turso 子模块提交已发布到远端"
fi

# 运行目录：透传的 --run-dir 优先（取最后出现的，与 dev.py 的 argparse 一致），
# 其次 PERI_RUN_DIR；都没有就生成新的一次性 fixture —— owned worker incarnation
# 护栏不允许同一目录二次启动，脚本既不清理旧目录也不绕过护栏。
transmitted_run_dir=""
previous_argument=""
for argument in "$@"; do
  if [ "$previous_argument" = "--run-dir" ]; then
    transmitted_run_dir="$argument"
  fi
  case "$argument" in
    --run-dir=*) transmitted_run_dir="${argument#--run-dir=}" ;;
  esac
  previous_argument="$argument"
done
effective_run_dir="${transmitted_run_dir:-${PERI_RUN_DIR:-}}"
if [ -z "$effective_run_dir" ]; then
  effective_run_dir="$(mktemp -d /tmp/peri-local.XXXXXX)" || fail "无法创建运行目录"
fi

no_build=0
for argument in "$@"; do
  case "$argument" in
    --no-build) no_build=1 ;;
  esac
done
if [ "$no_build" = "1" ]; then
  # --no-build 依赖既有产物：Rust 二进制在仓库内，Go 二进制在运行目录内。
  rust_binary="${PERI_RUST_BINARY:-${REPO_ROOT}/prototypes/v1/target/debug/peri-loom-v1}"
  platform_binary="${PERI_PLATFORM_BINARY:-${effective_run_dir}/peri-platform}"
  [ -x "$rust_binary" ] || fail "--no-build 但缺少 Rust 二进制：${rust_binary}"
  [ -x "$platform_binary" ] || fail "--no-build 但缺少 Go 二进制：${platform_binary}"
else
  command -v cargo >/dev/null 2>&1 || fail "找不到 cargo（可先执行 scripts/install-toolchain.sh）"
  command -v go >/dev/null 2>&1 || fail "找不到 go"
fi

# --------------------------------------------------------------- 启动
launcher_args=(--run-dir "$effective_run_dir")
if [ "${PERI_TLS:-1}" = "0" ]; then
  log "已关闭 TLS（PERI_TLS=0）"
else
  launcher_args+=(--tls)
fi

log "启动本地平台：authority + 2 workers + gateway/控制台"
log "运行目录：${effective_run_dir}"
log "就绪后会打印 ready JSON；浏览器打开其中的 address 即进入内嵌管理控制台"

# 信号语义交给启动器：Ctrl+C 由终端直达进程组；脚本自身忽略 SIGINT 以免提前退出、
# 丢掉收尾提示，只把发往脚本 PID 的 SIGTERM 转发给启动器。
trap '' INT
python_pid=""
trap 'if [ -n "$python_pid" ]; then kill -TERM "$python_pid" 2>/dev/null || true; fi' TERM

python3 "$DEV_PY" "${launcher_args[@]}" "$@" &
python_pid=$!
# 转发 SIGTERM 会中断 wait 并返回 128+n；继续等待直到拿到启动器的真实退出码，
# 否则会把一次正常的 joined shutdown 误报成失败。
status=0
while :; do
  if wait "$python_pid"; then
    status=0
    break
  else
    status=$?
  fi
  if [ "$status" -lt 128 ]; then
    break
  fi
  kill -0 "$python_pid" 2>/dev/null || break
done
trap - TERM

# --------------------------------------------------------------- 收尾
if [ "$status" -eq 0 ]; then
  log "平台已停止（joined shutdown）；本次运行目录保留在 ${effective_run_dir}。"
  log "受 owned worker incarnation 护栏限制，同一目录不能再次启动；重跑本脚本会自动使用新目录。"
else
  log "启动器以退出码 ${status} 结束。常见原因："
  log "  · 监听端口被占用：用 --listen 换端口"
  log "  · 指定的运行目录已被护栏 fenced：换一个新目录；不要删除 owned proof 文件来绕过护栏"
  log "  · 构建失败：先单独执行 cargo build / go build 排查"
fi
exit "$status"
