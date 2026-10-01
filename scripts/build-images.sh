#!/usr/bin/env bash
# 构建平台全部镜像（架构 §17.14）。
#
# 为什么用一个脚本串起来：三个 target 共享同一个 builder 阶段，
# 先构建 db-server 会把 Cargo 依赖全部编译进 BuildKit 缓存，
# 后面两个 target 只做增量链接，能省掉大量重复编译。
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

PLATFORM_VERSION="${PLATFORM_VERSION:-dev}"
log() { printf '\033[36m[build]\033[0m %s\n' "$*"; }

log "构建 db-server（首次会编译全部依赖，耗时较长）"
docker build --target db-server \
  -t "db-platform/db-server:${PLATFORM_VERSION}" \
  -f Dockerfile .

log "构建 db-worker（内含 db-runtime 二进制）"
docker build --target db-worker \
  -t "db-platform/db-worker:${PLATFORM_VERSION}" \
  -f Dockerfile .

log "构建 wal-service"
docker build --target wal-service \
  -t "db-platform/wal-service:${PLATFORM_VERSION}" \
  -f Dockerfile .

log "构建 web（Vite + nginx）"
docker build -t "db-platform/web:${PLATFORM_VERSION}" -f web/Dockerfile web/

log "全部镜像构建完成："
docker images --format '{{.Repository}}:{{.Tag}}' | grep '^db-platform/' | sort
