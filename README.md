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

### 与原生 TursoDB 的同机点查对照

使用仓库锁定的 `turso_core 0.8.1`，在同一台 macOS 主机上直接调用引擎，执行与 Simple 延迟诊断相同的 `SELECT v FROM bench WHERE id=1`（预置单行结果 `42`）。使用 `UnixIO`、优化构建、`Full`/`FullFsync`、单线程、预热 100 次，每轮每种方式各 1,000 次，逐次校验返回值；下表是两轮的区间，单位 µs。基准源码见 [bench_native_core.rs](docs/experiment-serverless-db/bench_native_core.rs)，逐轮原始输出保存在本地被忽略的 `raw/native_compare/results.txt`。

| 原生嵌入式调用方式 | P50 | P95 | P99 |
|---|---:|---:|---:|
| 复用连接 | 4.500–5.333 | 5.084–6.083 | 5.709–6.750 |
| 每次新建连接 | 10.625–12.875 | 12.000–14.250 | 15.958–16.292 |

Simple 的单并发 HTTP 点查 P50 在修复后各轮为 0.326–2.070 ms。原生数据只测嵌入式引擎调用，Simple 数据还包含 HTTP、鉴权、元数据检查、排队、结果转换和目录同步；两者**不是同一接口的吞吐或延迟基准**，不能据此计算原生产品与本服务的性能倍数。随后针对目录同步和全局启动锁做了下述优化；每次请求重复读取 Catalog、超过三帧时满队列固定休眠 5 ms，仍是待测热点。

### 持续优化复测

现已让已编译语句判定为只读、无状态且自动提交的查询跳过目录同步；写入、DDL、会话事务继续同步。旧版→新版交错 A/B/A/B、32/64 并发共 313,856 次点查、0 错；各档新版 HTTP P95 均低于对应旧版。单并发点查两轮 HTTP P50 从 0.342/0.379 ms 降至 0.313/0.337 ms；同步写仍约 4 ms。旧版各轮波动较大，**不能据此宣称稳定吞吐提升**。

热库可用性检查也已避免等待全局启动锁，冷库仍在锁内重读并串行启动。此项单独交错复测共 381,526 次请求、0 错，8 组同位比较中 6 组改善、2 组退化，稳定性能收益尚未证实。逐轮 P95、构建指纹、测试条件和局限见[持续优化报告](docs/experiment-serverless-db/continued_optimization.md)；逐请求原始样本仍保存在本地被忽略的 `raw/`。

### Catalog 热路径续测

Simple 模式已将待处理删除/恢复检查与数据库状态读取合为同一条 SQLite 查询；JWT 认证也在同一查询中读取当前用户与角色权限，仍逐请求检查禁用和撤权。两项分别做旧→新→旧→新的同机短时对照：

| 改动 | 请求数 / 错误 | 32 并发阶段平均成功 RPS：旧→新 | 64 并发阶段平均成功 RPS：旧→新 | HTTP P95 结果 |
|---|---:|---:|---:|---|
| 合并 Simple 准入查询 | 424,552 / 0 | 4,895.9 → 5,777.3 | 4,758.9 → 5,771.5 | 8 个对应阶段均下降 |
| 合并 JWT 用户与权限查询 | 533,207 / 0 | 5,544.7 → 7,874.0 | 4,902.2 → 8,311.3 | 8 个对应阶段均下降 |

这些是同机闭环短测的阶段均值，**不是稳定容量承诺**；单并发 JWT 优化结果仍有波动。将 Catalog 连接池从 1 提到 4 曾让并发写测试出现 `database is locked`，已撤回。完整 P95 区间、测试条件、正确性复核及后续实验见[Catalog 优化报告](docs/experiment-serverless-db/optimization_round3.md)。

### 独立只读池与平台期复核

保留 Catalog 写池上限 1，另建上限 4 的 SQLite 只读池，供 JWT、数据库授权及准入快照使用。读池 4 与原共用单连接版按旧→新→旧→新交错短测，共 **1,017,234 请求、0 错误**：

| 并发 | 共用单连接阶段平均 RPS | 独立只读池 4 阶段平均 RPS | HTTP P95：旧→新 ms |
|---:|---:|---:|---:|
| 32 | 8,639.7 | 17,030.8 | 4.22–4.63 → 2.34–2.60 |
| 64 | 8,768.9 | 16,398.4 | 8.17–8.40 → 4.97–5.33 |

8 个对应阶段的吞吐都上升、P95 都下降。读池继续从 4 增到 8 的点查收益很小（32/64 并发阶段平均 RPS +2.0%/+3.9%），一次短时同步写复测还出现回退；写测试不能独立归因，默认保持 4。

随后固定总并发 64，把压测客户端独立进程数按 1→2→4→2→1 交错。关闭分段计时时，成功吞吐依次为 **16,378→15,628→15,516→15,905→16,736 req/s**，共 641,495 请求、0 错。多进程未突破单进程读吞吐，回切又恢复。开启分段计时的另一轮共 612,255 请求、0 错；JWT 用户/权限、数据库授权、准入三段平均耗时之和约 3.2–4.2 ms，接近 query 路由到内联响应创建的平均耗时，host SQL 流仅约 15–19 µs。计时开启会影响吞吐，以上阶段值只用于定位；重复 Catalog 读取随后继续优化。完整复测条件、计时边界、原始数据位置见[Catalog 优化报告](docs/experiment-serverless-db/optimization_round3.md)。

### Simple 查询再合并一次 Catalog 读取

Simple `/query` 在维护读许可内用一个 SQLite 快照同时读取数据库记录与待处理作业，完成租户授权、删除和运行准入；JWT 用户及角色权限仍每请求实时读取。冷库启动仍在锁内重读，结果流仍持有维护许可。旧→新→旧→新交错短测 **1,402,066 次点查、0 错**：

| 并发 | 旧版阶段平均 RPS | 合并版阶段平均 RPS | HTTP P95：旧→新 ms |
|---:|---:|---:|---:|
| 32 | 15,661.3 | 19,379.4 | 2.41–2.93 → 2.08–2.20 |
| 64 | 15,638.1 | 19,406.6 | 4.63–6.10 → 3.87–4.72 |

8 个对应阶段的吞吐上升、P95 下降；旧版有明显轮间漂移，且新版一档 P99 回退，不能当作稳定容量。同步写有轮间波动，定向计时显示目录同步耗时上升时两版都变慢、单库队列随之放大；目前没有证据把写尾延迟归因于合并版，也**不宣称写性能改善**。新增授权错误优先级、JWT 即时撤权及 NDJSON 维护许可测试均通过。

合并版开启计时的 1→2→4→2→1 多进程点读为 **19,205→19,684→19,129→19,621→19,243 req/s**，775,241 次请求、0 错。1 进程时路由到内联响应创建平均约 0.25 ms；2/4 进程时约 3.0–3.15 ms，剩余 JWT 与数据库快照两段 Catalog 计时各约 1.46–1.53 ms。路由计时不含 socket 收发，不能与客户端 HTTP 延迟混用。完整读写交错数据与原始样本见[Catalog 优化报告](docs/experiment-serverless-db/optimization_round3.md)。

### 只读池连接复用续测

将剩余两次 Catalog 调用分段后，在总并发 64、2/4 个客户端进程时，每次取得只读连接平均等待约 1.42–1.50 ms，SQL fetch 本身约 47–58 µs。SQLx 0.8.6 默认在取得 idle 连接时 ping；只对 Catalog 只读池跳过这次 ping，仍保留归还时健康检查及写池默认行为。

开启计时的旧→新→旧→新交错测试共 **1,247,631 次点查、0 错**，六个同位阶段新版 RPS 均提高约 10–13%、P95 均下降约 0.33–0.42 ms。关闭计时的 2→4→2 多进程复测中，旧版为 **20,035→19,386→20,138 RPS**，新版为 **22,330→22,028→22,762 RPS**，合计 633,670 次请求、0 错。同步写 c1/c8 各两轮 8 秒，旧版 RPS 为 233/231、266/259，新版为 231/229、264/260，合计 15,822 次写、0 错；此次未见写吞吐回退。完整条件、计时拆分与风险见[Catalog 优化报告](docs/experiment-serverless-db/optimization_round3.md)。

### Simple JWT 单快照与当前实用上限

Simple `/query` 的 JWT 请求已在同一条 SQLite 语句中读取当前用户、角色权限、数据库状态及待处理作业；每请求仍验证签名并查询最新已提交状态。API Token、distributed、batch、session 和 Hrana 沿用原路径。旧→新→旧→新、总并发 64、2→4→2 个独立客户端进程的无计时交错测试共 **1,449,524 次点查、0 错**：

| 客户端进程 | 旧版 RPS 范围 | 单快照 RPS 范围 | HTTP P95：旧→新 ms |
|---:|---:|---:|---:|
| 2 | 22,019–22,443 | 26,309–26,745 | 3.21–3.34 → 2.80–2.96 |
| 4 | 21,587–21,799 | 25,663–25,793 | 3.26–3.30 → 2.76–2.77 |

六个同位阶段新版吞吐均提高约 17–21%，P95 均下降约 8–17%。同步写 c1/c8 两轮共 13,728 次、0 错，未观察到新版写回退；旧版自身波动，不能宣称写性能提升。新增测试覆盖即时撤权、禁用用户、坏 JSON/非法库 ID 的错误顺序与维护许可。

单快照开启计时的多进程点读为 21,487→26,623→25,680→25,872→21,323 RPS（1→2→4→2→1），968,074 次请求、0 错；2/4 进程时唯一一次只读池获取平均仍约 1.7–2.0 ms，SQL fetch 约 0.11 ms。尝试把只读池 4→8 后，旧→新→旧→新共 **1,323,729 次点查、0 错**，六个同位阶段吞吐反而下降 21–27%、P95 上升 22–34%，服务端 CPU 从约 5.1 核升至 7.2 核；已回退并保持读池 4。

**Astra 的最终判断：这一限定工作负载已接近当前架构的实用极限。**范围是本机 macOS ARM64、Simple 单库、JWT 管理员、单行热点点查、总并发 64、逐请求即时撤权和库状态检查。它不代表原生 TursoDB 的理论上限，也不代表多库、多行、混合读写或 distributed 模式。逐轮数值、二进制指纹、正确性验证和原始数据位置见[Catalog 优化报告](docs/experiment-serverless-db/optimization_round3.md)。
