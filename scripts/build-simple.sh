#!/usr/bin/env bash
# 先生成内嵌前端，再编译可独立运行的宿主机二进制。
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}/web"
npm ci --no-audit --no-fund
npm run build
cd "${repo_root}"
cargo build --release --locked --bin peri-loom
echo "Simple 二进制：${repo_root}/target/release/peri-loom"
