#!/usr/bin/env bash
# 通过国内 registry 镜像拉取 Compose 所需基础镜像，并重打成本地标准 tag。
#
# 设计说明：
#   * 不修改 /etc/docker/daemon.json、不重启 docker daemon（该机器上可能运行着
#     其他业务容器，重启 daemon 会中断它们）。
#   * 拉取时使用镜像加速前缀，成功后 docker tag 为 compose 文件引用的标准名。
#   * 镜像源可用性已实测：daocloud 覆盖本项目所需的全部镜像（含 RustFS）。
#   * 对象存储用 RustFS 而不是 MinIO：后者社区版已下架官方镜像并转向 AGPLv3。
set -euo pipefail

# 镜像加速器
MIRROR_PRIMARY="docker.m.daocloud.io"

# 格式： 标准tag|镜像源|源内引用
IMAGES=(
  "postgres:16-alpine|${MIRROR_PRIMARY}|postgres:16-alpine"
  "nginx:1.27-alpine|${MIRROR_PRIMARY}|nginx:1.27-alpine"
  "node:22-alpine|${MIRROR_PRIMARY}|node:22-alpine"
  "debian:bookworm-slim|${MIRROR_PRIMARY}|debian:bookworm-slim"
  "rust:1.98-slim-bookworm|${MIRROR_PRIMARY}|rust:1.98-slim-bookworm"
  "rustfs/rustfs:1.0.0|${MIRROR_PRIMARY}|rustfs/rustfs:latest"
  "otel/opentelemetry-collector-contrib:0.121.0|${MIRROR_PRIMARY}|otel/opentelemetry-collector-contrib:0.121.0"
  "prom/prometheus:v2.55.1|${MIRROR_PRIMARY}|prom/prometheus:v2.55.1"
  "grafana/grafana:11.4.0|${MIRROR_PRIMARY}|grafana/grafana:11.4.0"
)

log() { printf '\033[36m[pull]\033[0m %s\n' "$*"; }

failed=0
for entry in "${IMAGES[@]}"; do
  IFS='|' read -r target mirror source <<< "${entry}"
  if docker image inspect "${target}" >/dev/null 2>&1; then
    log "已存在：${target}"
    continue
  fi
  log "${mirror}/${source} -> ${target}"
  if timeout 600 docker pull "${mirror}/${source}" >/dev/null 2>&1; then
    docker tag "${mirror}/${source}" "${target}"
    log "OK：${target}"
  else
    log "失败：${target}"
    failed=$((failed + 1))
  fi
done

if [ "${failed}" -gt 0 ]; then
  log "${failed} 个镜像拉取失败。可改用内网仓库（IMAGE_REGISTRY）或导入离线镜像包。"
  exit 1
fi
log "全部基础镜像就绪。"
