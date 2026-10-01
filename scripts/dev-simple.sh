#!/usr/bin/env bash
# 开发时由 Vite 提供热更新，API 仍使用同一个 Simple 宿主。
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
api_port="${PERI_LOOM_API_PORT:-18081}"
web_port="${PERI_LOOM_WEB_PORT:-5174}"
data_dir="${PERI_LOOM_DATA_DIR:-$repo_root/data}"

# 提前发现端口冲突，避免先初始化数据再因 bind 失败退出。
python3 - "$api_port" "$web_port" <<'PY'
import socket, sys
ports = [int(value) for value in sys.argv[1:]]
if len(set(ports)) != len(ports):
    raise SystemExit('API 和 Web 端口必须不同')
for port in ports:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', port))
PY

if [ ! -d web/node_modules ]; then
  npm --prefix web ci --no-audit --no-fund
fi
# Simple 二进制仍保留内嵌 UI；开发访问 Vite 可立即看到前端修改。
npm --prefix web run build
cargo build --locked --bin peri-loom

api_pid=''
web_pid=''
cleanup() {
  trap - EXIT INT TERM
  for pid in "$web_pid" "$api_pid"; do
    if [ -n "$pid" ]; then kill -TERM "$pid" 2>/dev/null || true; fi
  done
  for pid in "$web_pid" "$api_pid"; do
    if [ -n "$pid" ]; then wait "$pid" 2>/dev/null || true; fi
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

target/debug/peri-loom serve --mode simple --data-dir "$data_dir" --listen "127.0.0.1:$api_port" &
api_pid=$!
DB_SERVER_URL="http://127.0.0.1:$api_port" node web/node_modules/vite/bin/vite.js web --host 127.0.0.1 --port "$web_port" --strictPort &
web_pid=$!
echo "Simple dev UI：http://127.0.0.1:$web_port"
echo "Simple API：http://127.0.0.1:${api_port}；数据目录：$data_dir"
echo '按 Ctrl+C 同时停止前端与后端。'
while kill -0 "$api_pid" 2>/dev/null && kill -0 "$web_pid" 2>/dev/null; do
  sleep 1
done
echo '开发进程已退出，正在关闭其余进程。' >&2
exit 1
