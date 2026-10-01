#
# 平台标准交付镜像（架构 §17.14，冻结）。
#
#   docker build --target db-server -t db-platform/db-server .
#   docker build --target db-worker -t db-platform/db-worker .
#   docker build --target wal-service -t db-platform/wal-service .
#
# 说明：
#   * db-worker 镜像必须同时包含 /usr/local/bin/db-worker 与 /usr/local/bin/db-runtime，
#     Worker 以普通子进程方式 exec db-runtime（不启动新容器，不做一 DB 一 container）。
#   * 构建阶段使用国内镜像（apt -> aliyun，cargo -> rsproxy），无需外网直连 crates.io。

ARG RUST_IMAGE=rust:1.98-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

# ============================================================================
# builder：全量构建 4 个二进制
# ============================================================================
FROM ${RUST_IMAGE} AS builder

# rust-toolchain.toml 固定了 1.98.1；若基础镜像版本略有差异，rustup 走国内镜像补齐
ENV RUSTUP_DIST_SERVER=https://rsproxy.cn \
    RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup \
    CARGO_TERM_COLOR=never

# apt 换国内源（基础镜像默认 deb.debian.org 在国内不可用）
RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /etc/apt/sources.list ]; then \
      sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates pkg-config libssl-dev protobuf-compiler git curl; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 国内 crates 镜像配置随仓库进入构建上下文
COPY .cargo/config.toml .cargo/config.toml
COPY rust-toolchain.toml rust-toolchain.toml

# 依赖层与源码层分离，源码变更时尽量复用 registry 缓存
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY services ./services
COPY proto ./proto
COPY migrations ./migrations

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
      --bin db-server --bin db-worker --bin db-runtime --bin wal-service && \
    mkdir -p /out && \
    cp target/release/db-server target/release/db-worker \
       target/release/db-runtime target/release/wal-service /out/

# ============================================================================
# runtime-base：最小运行时
# ============================================================================
FROM ${RUNTIME_IMAGE} AS runtime-base

RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /etc/apt/sources.list ]; then \
      sed -i 's|deb.debian.org|mirrors.aliyun.com|g; s|security.debian.org|mirrors.aliyun.com|g' /etc/apt/sources.list; \
    fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl tini; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd -g 10001 platform; \
    useradd -u 10001 -g platform -m -s /usr/sbin/nologin platform

ENV RUST_LOG=info \
    OTEL_SERVICE_NAMESPACE=db-platform

# ============================================================================
# db-server：公网 HTTP API + Router + Control Plane + DBA
# ============================================================================
FROM runtime-base AS db-server

COPY --from=builder /out/db-server /usr/local/bin/db-server
COPY migrations /opt/db-platform/migrations

USER platform
EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/db-server"]

# ============================================================================
# db-worker：Worker Agent + Data Dispatcher + Process Supervisor
# 必须同时包含 db-runtime（子进程 exec），且需要 cgroup v2 委派权限
# ============================================================================
FROM runtime-base AS db-worker

COPY --from=builder /out/db-worker /usr/local/bin/db-worker
COPY --from=builder /out/db-runtime /usr/local/bin/db-runtime

# Worker 需要 root 才能创建 per-DB cgroup 并管理子进程生命周期
USER root
# 运行期数据（Local NVMe 工作集）与本地 UDS 目录
RUN mkdir -p /var/lib/db-platform /run/db-platform && chmod 0755 /run/db-platform

VOLUME ["/var/lib/db-platform"]
EXPOSE 9100 9101
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/db-worker"]

# ============================================================================
# wal-service：Remote WAL（Raft 3 副本之一）
# ============================================================================
FROM runtime-base AS wal-service

COPY --from=builder /out/wal-service /usr/local/bin/wal-service

RUN mkdir -p /var/lib/db-platform/wal && chown -R platform:platform /var/lib/db-platform

USER platform
EXPOSE 9200 9201
VOLUME ["/var/lib/db-platform/wal"]
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/wal-service"]

# ============================================================================
# db-runtime-standalone：仅用于本地排障（正常路径由 Worker 拉起子进程）
# ============================================================================
FROM runtime-base AS db-runtime

COPY --from=builder /out/db-runtime /usr/local/bin/db-runtime
USER platform
ENTRYPOINT ["/usr/local/bin/db-runtime"]
