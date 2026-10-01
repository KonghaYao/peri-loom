# TursoDB DB Platform

基于自建 TursoDB Engine 的独立 DB Platform。平台自行负责 Server、Worker、调度、路由、DBA、生命周期、可靠性与存储管理，**不依赖 Turso Cloud 控制面**。

架构契约见 [`tursodb-db-platform-architecture.md`](./tursodb-db-platform-architecture.md)（状态：FINAL / Development Contract）。
本仓库的任何实现不得改变该文档第 15 章与第 18.4 节冻结的系统语义。

## 架构一句话

> Server 是全局 HTTP 入口、路由与管理面；Worker 是高密度 DB Process Host。
> 一个 DB 对应一个独立进程，一个 Worker 同时运行多个 DB。
> 热数据工作集位于 Local NVMe；事务必须在 **Synchronous Remote WAL durable 之后** 才能返回 Commit Success；
> Object Storage Snapshot 提供 Base Image / Backup / Cold Restore。
> Catalog / Ownership / Epoch 以及 Panel 系统数据统一以 **PostgreSQL** 为权威事实源。

## 目录结构

```text
proto/                     内部 protobuf 契约（Server->Worker / Worker->DB / Remote WAL）
migrations/                PostgreSQL Catalog migrations（权威事实源 schema）
crates/
  domain/                  领域模型：ID、Epoch、生命周期、错误码
  protocol/                proto 代码生成 + 本地 UDS framing
  catalog/                 PostgreSQL Catalog 访问层（SQLx，无 ORM）
  engine-adapter/          TursoDB 集成唯一入口（PlatformDurableIO / Engine Adapter）
  scheduler/               Resource Budget Packing + Failover Reserve
  routing/                 Route Cache（db_id -> worker_id）
  wal-client/              Remote WAL 客户端（Append / Fence / ReadRange）
  objectstore/             S3-compatible ObjectStore + Snapshot manifest
  observability/           tracing / metrics 统一初始化
services/
  db-server/               公网 HTTP API + Router + Control Plane + DBA Service
  db-worker/               Worker Agent + Data Dispatcher + Process Supervisor + cgroup v2
  db-runtime/              一个 DB = 一个进程（链接 TursoDB Engine）
  wal-service/             Remote WAL Service（Raft 3 副本）
web/                       DBA Panel（Vite + React + Ant Design）
Dockerfile                 多目标镜像（db-server / db-worker / wal-service）
docker-compose.yml         标准部署拓扑
```

## 国内环境（已内置）

本仓库已针对国内网络环境配置，clone 后无需额外设置：

| 组件 | 镜像 | 配置位置 |
|---|---|---|
| crates.io | `rsproxy.cn`（sparse） | `.cargo/config.toml`（随仓库提交） |
| npm | `registry.npmmirror.com` | `web/.npmrc` |
| apt | `mirrors.aliyun.com` | 系统 `/etc/apt/sources.list` |
| Docker Hub | `docker.m.daocloud.io` 等 | `scripts/docker-mirror.sh`（拉取时前缀加速，不重启 daemon） |
| Rust 工具链 | `rsproxy.cn` dist | `rust-toolchain.toml` + `scripts/install-toolchain.sh` |

Rust 工具链固定为 `1.98.1`（见 `rust-toolchain.toml`），CI 与生产不得浮动使用 `latest`。

### 首次安装工具链

```bash
./scripts/install-toolchain.sh   # 通过 rsproxy 安装 rustup + 1.98.1 + protoc 25.1
```

### 对象存储选型

平台只依赖 S3 API 与内部 `ObjectStore` 接口（架构 §17.9），可替换为任意 S3 兼容实现。
Compose 默认使用 **RustFS**（Apache-2.0）：MinIO 社区版自 2025 年起停止发布官方 Docker
镜像并转向 AGPLv3，不再适合作为平台默认依赖。bucket 由平台自身在启动时幂等创建
（`S3ObjectStore::ensure_bucket`），部署侧只需提供 endpoint 与凭据。

## 本地构建

```bash
cargo check --workspace            # 编译校验
cargo nextest run --workspace      # 单元 / 集成测试（cargo-nextest）
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings
```

## 本地启动（Docker Compose）

```bash
./scripts/docker-pull.sh           # 通过国内 registry 镜像预拉基础镜像
docker compose up -d --build
docker compose ps                  # 等待全部 healthy（目标 <= 120s）
```

默认只发布 `web` 的 HTTP 端口；`db-server` / Worker / WAL / PostgreSQL / 对象存储的内部端口不发布到公网（见架构 §17.14）。

## 验收

```bash
./scripts/acceptance.sh            # 验收指标（并发启动 / 故障注入 / 路由 / durability）
```

验收阈值来自架构文档第 16 章，脚本不修改阈值，只报告实测值与目标值差异。
