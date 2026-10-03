# TursoDB DB Platform

基于自建 TursoDB Engine 的独立 DB Platform。平台自行负责 Server、Worker、调度、路由、DBA、生命周期、可靠性与存储管理，**不依赖 Turso Cloud 控制面**。

架构契约见 [`tursodb-db-platform-architecture.md`](./tursodb-db-platform-architecture.md)（状态：FINAL / Development Contract）。
本仓库的任何实现不得改变该文档第 15 章与第 18.4 节冻结的系统语义。

单机部署可使用独立的 **Simple 模式**：一个内嵌管理后台的二进制、一个端口和一个数据目录，使用 SQLite 元数据与本地可靠提交，无需外部服务。启动、备份与迁移见 [Simple 部署指南](deploy/simple-deployment.md)。下述远程 WAL 与 Worker 契约适用于 distributed 模式。

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

## 性能实测（初始版本，2026-10-03）

本次在 macOS ARM64（18 核、48 GiB）上测试 release 构建的 **Simple 单机模式**。客户端与服务同机，通过本地 HTTP 发送真实 SQL；每档运行两轮，读每轮 8 秒、写每轮 5 秒。下表区间表示两轮的观测值，并非稳定容量或置信区间。读负载为单行主键热点，写负载为本地同步提交。完整命令、逐轮数据及复核见[压力测试报告](docs/experiment-serverless-db/01_experiment_report.md)。逐请求原始样本保存在本地 `docs/experiment-serverless-db/raw/`，该目录不纳入 Git；可用报告中的脚本重建。

### 压力测试报告

| 操作 | 并发 | 两轮请求总数 | 成功吞吐 req/s | P95 ms | P99 ms | 观测错误 |
|---|---:|---:|---:|---:|---:|---:|
| 主键点查 | 1 | 1,162 | 70.1–75.0 | 16.30–16.94 | 17.01–17.31 | 0 |
| 主键点查 | 8 | 8,215 | 440.4–585.8 | 19.34–29.40 | 27.46–34.99 | 0 |
| 主键点查 | 32 | 25,823 | 1,156.6–2,061.6 | 28.62–76.36 | 45.58–104.91 | 0 |
| 主键点查 | 64 | 31,731 | 1,214.4–2,747.2 | 46.55–157.75 | 70.15–218.01 | 0 |
| 本地同步写入 | 1 | 611 | 60.8–61.3 | 19.83–20.03 | 20.95–21.62 | 0 |
| 本地同步写入 | 8 | 2,502 | 241.2–256.1 | 40.97–44.09 | 47.06–53.66 | 0 |
| 本地同步写入 | 32 | 2,179 | 172.9–251.7 | 162.89–258.07 | 187.39–262.85 | 0 |

共执行 **72,223 次请求，观测到 0 次错误**。写入后总行数为 5,293，等于 5,292 次成功写入加 1 行预置数据。延迟为客户端 HTTP 端到端耗时，不是平台内部路由附加延迟。

### Serverless 性能

| 场景 | 实测结果 | 范围 |
|---|---|---|
| Simple 显式停库后首查 | 5 个独立库均成功；21.26–25.31 ms | 本地 HTTP 端到端，不能代表分布式冷启动分位数 |
| Simple 新实例启动至 `/readyz` | 306.45 ms | 单次观测 |
| Simple 热读最高观测吞吐 | 2,747.2 req/s | 同档另一轮为 1,214.4 req/s，不能作为稳定容量 |
| Simple 同步写最高观测吞吐 | 256.1 req/s | 8 并发第一轮 |
| 自动回收、Worker 密度、远程 WAL、故障恢复 | 未测 | 本次未运行 distributed 集群 |

本机 Docker Engine 未响应，无法启动 distributed 拓扑。因此本次数据**不能判定**架构第 16 章的 10,000 req/s、平台附加延迟及分布式冷启动目标是否达标。测试只覆盖短时、单库、单行热点；64 并发读吞吐两轮相差 2.26 倍。原始统计已[独立复核](docs/experiment-serverless-db/01_verification.md)。

### Simple 延迟诊断与修复

初始版本的每库执行线程在命令队列暂时为空时固定睡眠 10 ms。现已将**无会话、无活动事务**时的等待改为消息到达即唤醒；有会话时仍定期检查超时。相同隔离脚本、单并发、每次实验两轮各 120 次请求的结果如下（HTTP 端到端 P50）：

| 操作 | 修复前 | 首次空闲唤醒修复后 | 空闲唤醒修复版复测 |
|---|---:|---:|---:|
| 单行主键点查 | 12.895 / 12.732 ms | **0.332 / 0.326 ms** | **2.070 / 0.418 ms** |
| 本地同步单行写 | 16.015 / 15.992 ms | **4.032 / 4.034 ms** | **6.945 / 8.829 ms** |

这是低并发固定延迟的已确认实现问题；修复后绝对延迟仍随实验轮次波动。第一次修复后的另一轮 32/64 并发点查 P95 为 26.65–46.57 ms，88,583 次请求中仍有 79 次达到 100 ms。各阶段归因和原版 TursoDB 对照见[延迟诊断结论](docs/experiment-serverless-db/perf_diagnosis_conclusion.md)。上面的初始版本压力表保留作修复前基线。

### 单行点查尾延迟复核

每个小型读查询通常发送列信息、数据行、结束帧共 3 帧，而原结果通道只能暂存 2 帧。通道满时单库执行线程固定睡眠 5 ms，阻塞后续请求。将容量增为 3 后，按容量 2 → 3 → 2 → 3 的顺序重复短时同机实验：

| 并发 | 容量 2：HTTP P95 | 容量 3：HTTP P95 | 容量 2：成功吞吐 | 容量 3：成功吞吐 |
|---:|---:|---:|---:|---:|
| 32 | 26.28–41.96 ms | **10.75–11.71 ms** | 1,765–2,688 req/s | **4,409–4,546 req/s** |
| 64 | 49.75–81.17 ms | **23.50–29.41 ms** | 1,489–2,823 req/s | **4,073–4,355 req/s** |

四次实验合计 259,291 次点查、0 次观测错误。容量 3 的 64 并发仍有少量 ≥100 ms 请求，且多行结果仍可能触发满队列等待；详情见[尾延迟实验与复核](docs/experiment-serverless-db/frame_capacity_experiment.md)。
