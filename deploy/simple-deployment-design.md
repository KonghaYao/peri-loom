# Simple 单机部署方案

状态：已按用户后续明确授权实施首版（2026-10-01）。

本文保留原设计目标与架构讨论；实际启动、功能边界与验收结果分别见
[部署指南](simple-deployment.md)、[实施记录](simple-deployment-progress.md) 和
[隔离回归记录](simple-validation.md)。首版省略自动 LRU 与跨模式迁移，使用停机整实例导出、持久任务轮询及显式容量限制。

## 1. 目标与约束

将单机部署收敛为 **一个可执行文件、一个主进程、一个 HTTP 端口、一个数据目录**。
运行时不要求 PostgreSQL、S3、独立 WAL 节点、Worker、Nginx、Node 或观测平台。
容器是可选包装，不是必要依赖；构建阶段仍可使用 Rust、Node 等工具。

这里的 simple 不是把现有所有进程塞进一个容器，也不是隐藏内部 PostgreSQL。
HTTP、管理后台、元数据、数据库执行、备份任务全部由主进程承载。
保留现有 distributed 模式，不降低其 Remote WAL quorum / fencing 契约。

适用：开发、小型自托管、可信用户的单机生产部署。
不适用：跨机器高可用、敌对多租户强隔离、依靠副本承诺节点损坏后不丢数据。

## 2. 当前部署难点与代码证据

| 当前依赖或机制 | 代码位置 | Simple 替代 |
| --- | --- | --- |
| PostgreSQL Catalog | `crates/catalog/src/lib.rs`、`jobs.rs`、`watcher.rs` | 嵌入式 SQLite 元数据库 |
| Server → Worker → UDS → Runtime | `services/db-server/src/router.rs`、`clients.rs`；`services/db-runtime/src/host.rs` | 进程内数据库宿主 |
| Remote WAL / Raft | `services/db-runtime/src/host.rs`；`crates/engine-adapter/src/durable/` | 引擎本地 WAL 与可靠同步 |
| S3 对象存储 | `crates/objectstore/src/store.rs`、`s3.rs` | 同接口的文件系统 Adapter |
| Nginx 静态站点与反向代理 | `web/nginx/default.conf` | 主 HTTP Router 内嵌前端产物 |
| 独立观测栈 | `docker-compose.yml` 的 observability profile | 日志、健康检查和可选指标 |

Compose 目前包含 web、db-server、PostgreSQL、三个 Worker、三个 WAL 节点以及对象存储相关容器。
默认规模可以缩小，但只减少副本不能消除这些依赖。

重要阻碍：Catalog 是持有 `PgPool` 的具体类型，不是可直接替换后端的接口；代码使用
`FOR UPDATE SKIP LOCKED`、PostgreSQL 类型转换、JSONB 和 LISTEN/NOTIFY。
因此不能只替换 `DATABASE_URL`，也不建议把所有查询统一换成通用 SQL 驱动。

## 3. 目标形态

```text
Browser / SDK
      │ HTTP :8080
      ▼
peri-loom（单进程）
├── HTTP API + Hrana + 内嵌 Web
├── 鉴权 / 管理逻辑
├── SQLite Metadata Adapter
├── Local Database Host
│   ├── 每库执行队列 / 引擎线程
│   ├── 会话、事务、超时、取消
│   └── Turso 引擎 + 本地 WAL
├── Local ObjectStore Adapter
└── 持久任务执行器 / 健康检查
      │
      ▼
一个本地数据目录
```

数据库执行不能阻塞 HTTP 的 Tokio executor。每库引擎对象和连接由固定线程或专用执行器
持有，通过有界队列接收请求；先验证引擎线程约束，不靠新增 `unsafe Send/Sync` 强行跨线程。
首版每库串行执行，以低复杂度保证会话与事务正确性；不同数据库可并行。
同库有活动事务时，其他会话不能进入该事务的连接；其等待必须有上限。

## 4. 真正需要的三个 Seam

### 4.1 元数据：按业务语义抽象，不抽象 SQL

提取身份与权限、数据库生命周期、Operations / Jobs、备份记录和 Panel 数据等业务接口，
提供 PostgreSQL 与 SQLite 两种 Adapter。HTTP 逻辑不应拿到 `PgPool`。
不为 simple 强制模拟 Worker ownership、租约或 Raft epoch；分布式协调接口留在 distributed 路径。
现有可共享的校验与领域状态机继续复用，避免两套业务规则漂移。

SQLite 元数据采用独立的嵌入式后端，首选评估 SQLx SQLite；不要先让平台元数据也依赖
尚待验证的 Turso 本地持久化路径。SQLite migration 与 PostgreSQL migration 独立维护，
但遵循同一业务模型和版本管理原则。

- 数据库、用户、权限、令牌、审计、Operations 和 Jobs 必须落盘，不能用内存 Map 代替。
- 元数据写事务短小、有界，设置忙等待与重试上限；禁止持有事务跨外部任务等待。
- Job 用短事务完成状态条件更新与领取；不移植 `SKIP LOCKED`。
- 通知用进程内事件加持久版本号，事件丢失时从元数据重建视图；不移植 PgListener。
- Simple 不依赖心跳发现自己的数据库，不把自身判成失联 Worker。

### 4.2 执行：网络执行与本地执行分离

提取面向调用方的数据执行接口，例如执行 SQL、流式查询、打开/关闭会话、取消请求，
以及数据库打开/关闭等宿主操作。不要把 gRPC Channel 或 UDS frame 暴露到接口。

- Distributed Adapter 保持现有 Worker RPC / Runtime 路径。
- Simple Adapter 直接访问 `Local Database Host`，不启动 Worker、Runtime 子进程或内部 gRPC。
- 从 `db-runtime` 提取可复用的引擎宿主、会话和执行逻辑；UDS、Remote WAL 恢复和 fencing
  仍属于 distributed Adapter，不能整段移入 simple 启动流程。
- 默认懒加载数据库，限制打开数据库数、队列长度、结果集内存和会话数量；LRU 只关闭
  无活动事务、会话和任务的库。进程内执行不提供 cgroup 级的逐库硬隔离。
- 保留参数绑定、事务、流式背压、取消和超时语义，不通过额外 HTTP 自调用来复用网络代码。

### 4.3 备份对象：扩展已有 ObjectStore

新增 `LocalObjectStore`，复用现有 `ObjectStore` 接口与 snapshot manifest，而不是启动本地 S3。
它负责对象存取；数据库一致性快照仍由宿主和备份作业负责。

- 对象 key 映射到受限根目录，拒绝绝对路径、`..` 和符号链接逃逸。
- 临时文件写入、校验、同步后原子发布；对目录项做必要同步。
- 大文件必须流式读写，不能全部加载到内存；保留校验和与 manifest 完整性检查。
- 对象发布并验证成功后才写入完成状态；启动时识别孤立临时文件和未完成作业。

## 5. 持久化：不能把“不连接 WAL”当成方案

`EngineAdapter` 已允许传入 IO，`EngineOpenConfig.durable_io` 也可为空，但现有实现明确
把生产写入的契约绑定到 `PlatformDurableIO`；没有该 IO 时 `durable_lsn()` 返回 0。
这只是本地模式的切入点，不是已证明的生产本地持久化能力。

明确区分两种策略：

- `LocalDurable`：成功响应意味着本地提交记录已经完成必要的可靠同步；恢复只依赖本地文件。
- `RemoteQuorumDurable`：沿用现有 Remote WAL 成功确认、LSN 与 fencing 语义。

第一阶段必须验证现有 Turso IO / WAL 的提交、同步、checkpoint 和恢复路径。
若标准本地 IO 不满足契约，补足本地 durability Adapter，再进入上线阶段；不能默认跳过。
同步失败、磁盘满、只读介质等情况不能返回提交成功。

不要把 simple 的 `durable_lsn = 0` 宣传成有效的远程复制位置。接口暴露部署能力，
远程 LSN / replication 功能在 simple 明确不支持；若现有客户端依赖该字段，先审计兼容性，
用版本化能力约定处理，不能伪造 quorum LSN。

本地可靠同步只承诺在文件系统与硬件正确履行同步语义时恢复成功提交；不承诺磁盘损坏后恢复。
`kill -9` 恢复测试与断电/丢失未同步写模拟需要分别覆盖，前者不能替代后者。

## 6. 目录、生命周期与恢复

```text
data/
├── instance.lock
├── instance.json          # 数据格式版本 / 实例身份
├── secrets/               # 持久化签名密钥等（受限权限）
├── catalog/metadata.db    # 元数据及其 WAL / SHM sidecar
├── databases/<uuid>/      # 主库、WAL、引擎 sidecar
├── objects/               # 快照对象与 manifest
└── tmp/                   # 可清理的临时文件
```

操作系统级排他文件锁覆盖整个实例生命周期；不能仅判断 lock 文件是否存在。
禁止两个进程打开同一 data 目录，不支持把该目录放到共享网络盘用于伪集群。
用户可见数据库名不参与路径拼接，路径使用内部数据库 ID。

启动：读取配置 → 加锁 → 校验数据版本 → 打开元数据与 migration → 恢复未完成状态
→ 初始化身份与宿主 → 注册 API / Web → 标记 ready。大规模用户库按需恢复，损坏库标记
为不可服务并隔离；元数据损坏时整个实例不能 ready。

创建/删除库跨元数据和文件系统，不存在天然单事务：记录持久 Operation，执行幂等步骤，
启动时继续或补偿；删除必须等待活动句柄释放，再清理文件，不能只删 Catalog 行。
Jobs 重启后恢复未完成任务，重试步骤必须幂等，不能因进程重启把 Operation 静默遗失。

停止：停止接收新写入 → 有界排空 → 回滚未完成事务 → 完成必要同步 → 关闭引擎与元数据
→ 释放锁。达到超时可退出，但下次启动必须走恢复流程。

## 7. 备份、前端与默认安全

在线备份必须通过引擎的一致性快照能力，或在暂停写入并满足 checkpoint/句柄约束后生成。
不能运行中直接复制 `.db` 并漏掉 WAL。整实例备份还需协调元数据、数据库和 manifest：
首版采用维护模式排空写操作后导出；简单可靠优先于复杂并发快照。

同盘备份只保护误操作，不保护磁盘故障。备份导出与离机保存是运维能力，不是启动依赖。
恢复使用已校验的暂存目录，关闭目标库、失效旧会话后替换；整实例恢复要求停机。

构建时生成 Web 静态文件并打包到 Rust 二进制；Node 只存在于构建阶段。
同一 HTTP Router 保留 `/api/*`、`/data/*`、`/db/*`，API 未命中不能落入 SPA fallback。
沿用流式输出与背压，不引入响应聚合；静态文件保留合理缓存和安全响应头。

- 本机二进制默认监听 `127.0.0.1:8080`；容器内监听 `0.0.0.0`，宿主默认只绑定回环地址。
- 首次启动创建随机管理员凭据，通过受限的首次设置文件或一次性初始化流程交付，完成后移除。
- JWT 签名密钥首次生成并持久化，重启不随机替换；不使用示例默认密码。
- 提供存活和就绪检查；可选指标放到同端口受保护路径，不默认开放敏感指标。
- TLS 可以通过主进程显式配置；不把外置反代作为必要依赖。未配置 TLS 时不宣称适合公网直连。
- Simple 配置缺失时不能偷偷退回 distributed；不需要任何集群地址或 S3 凭据。

## 8. 用户最终体验

```bash
./peri-loom serve --mode simple --data-dir ./data
```

可选配置：监听地址、数据目录、日志等级、打开库上限、内存预算、初始化凭据、TLS。
采用明确部署模式和独立配置解析，不复用要求 `DATABASE_URL` / `WAL_CLUSTER` 的校验。

```yaml
services:
  peri-loom:
    image: peri-loom:<固定版本>
    command: ["serve", "--mode", "simple", "--listen", "0.0.0.0:8080", "--data-dir", "/data"]
    ports: ["127.0.0.1:8080:8080"]
    volumes: ["peri_data:/data"]
    restart: unless-stopped
volumes:
  peri_data:
```

这里的命名卷只是本地持久存储，不是额外服务。升级替换可执行文件或镜像，不删除 data。
升级前导出备份；migration 版本与二进制版本绑定，不支持任意降级打开新格式。

## 9. 功能取舍与界面

| 能力 | Simple 首版 |
| --- | --- |
| 登录、RBAC、令牌、审计、数据库管理 | 保留 |
| SQL、参数、会话、事务、NDJSON、Hrana HTTP（v2/v3 pipeline、v3 cursor） | 保留，逐项做兼容性验证 |
| 创建/删除、打开/关闭、按需加载 | 保留 |
| 本地备份、恢复、导出、持久 Operations | 保留 |
| 跨 Worker Move、自动故障转移、复制管理 | 明确禁用 |
| Raft quorum、远程 durability LSN、跨节点 fencing | 不承诺、不伪造 |
| 每库 cgroup 与故障进程隔离 | 不提供；进程崩溃影响整个实例 |
| Worker/拓扑页面 | 隐藏或显示“本机实例”，不伪造健康 Worker |

Hrana 兼容端点在 db-server 已落地（`/db/{db_id}/v2/pipeline`、`/v3/pipeline`、`/v3/cursor`，权限与
`/data/v1` 同档 `db:write`）；simple 模式复用同一套 HTTP 逻辑，不另立语义。

提供统一的模式/能力信息，供 Web 和客户端判断；未支持的管理动作返回稳定的
`UNSUPPORTED_IN_DEPLOYMENT_MODE` 或映射到现有等价错误码，不能返回假成功。

## 10. 实施顺序与验收

1. **先验证本地 durability**：引擎本地提交、重启恢复、同步错误、checkpoint、事务和快照。
   这是阻塞项，失败则先补适配层，不在错误承诺上开发 UI。
2. **建立最小纵向链路**：simple 启动入口、SQLite 元数据、进程内宿主，实现创建库、
   执行 SQL、事务、重启读取；全程不需要 PostgreSQL / Worker / WAL 服务。
3. **共享业务接口**：逐步把身份权限、生命周期和执行调用接入双 Adapter；保留 distributed
   行为，复用 domain/protocol 和可复用的 HTTP 逻辑，不复制一整套 db-server。
4. **补齐产品闭环**：持久作业、本地对象存储、一致性备份恢复、Web 内嵌、能力适配和初始化安全。
5. **交付**：单文件与单镜像发布、simple 文档、升级/导出流程；现有 Compose 继续作为 distributed 部署。

验收标准：

- 干净机器不安装数据库、S3、Node 或 Docker，预编译文件可启动；不配置任何外部服务地址。
- 只有一个主进程、一个服务端口，无子服务进程，无 cgroup 特权要求，无运行时外网依赖。
- 创建两个数据库，完成参数 SQL、多会话事务、流式查询；正确处理超时、取消与背压。
- 成功提交后强杀重启可恢复，未提交事务不泄漏；同步失败不报成功；另做断电模型验证。
- 两个实例打开同一 data 目录时第二个明确失败；磁盘满、坏库和权限错误有可诊断错误。
- 重启后用户、令牌、审计、数据库和 Operations 保留；未完成任务可安全续跑或明确失败。
- 备份可以恢复到新目录；并发写入期间快照保持一致，损坏或不完整快照被拒绝。
- UI 不提供无效的集群操作；API/Hrana 兼容用例通过，未知 API 路径不返回 SPA HTML。
- distributed 的 quorum / fencing、Catalog 并发和既有执行链回归测试仍通过。

## 11. 明确不做

不把 PostgreSQL / RustFS / WAL 节点打包进同一镜像假装零依赖；不首先重构全部 Catalog SQL；
不实现第二套通用数据库引擎；不为保持界面完整而模拟一套假的分布式系统；
不承诺无需验证即可把测试用本地 IO 用于生产。

后续由 simple 迁移到 distributed，应提供停写、一致导出、导入和验证工具。
两种模式持久化契约不同，不能只改 `--mode` 原地切换数据目录。

## 12. 架构切面与实施难度讨论（草稿）

### 核心判断

这是一项中高难度的架构调整，不是 Compose 配置调整，但不需要重写项目。
合理方向是共享业务规则、分离部署机制，让 simple 与 distributed 成为两种装配方式，
而不是在当前调用链中遍布 `if simple`。本节记录候选设计，不构成已采纳的架构决策。

### 建议的职责划分

```text
HTTP / Hrana / Web
        │
共享应用逻辑：鉴权、数据库管理、SQL、会话、Operations
        │
面向业务能力的接口
        │
├── Simple：本地元数据、进程内宿主、本地持久化、本地对象存储
└── Distributed：PostgreSQL、Worker / Runtime、Remote WAL、S3
```

共享应用逻辑不应依赖 PgPool、Worker 地址、gRPC Channel 或 Raft。
需要评估的切面为：

1. **元数据**：以业务操作定义接口，不暴露通用 SQL；身份权限、数据库元数据、任务与备份
   按职责划分。Worker inventory、ownership 与租约独立属于分布式协调，不能迫使 SQLite
   Adapter 为接口完整性模拟这些机制。
2. **执行**：共享 SQL、流式查询、会话、事务和取消契约；本地执行与 Worker RPC 分别适配。
   提取 Runtime 的可共享逻辑，但不连同 UDS、远程恢复和 fencing 一并搬入本地宿主。
3. **生命周期**：共享“让数据库可用/关闭”等目标与领域状态转换，隔离具体步骤。
   本地是打开/关闭句柄与本地恢复；分布式是调度、ownership、Runtime 与 Remote WAL 恢复。
   跨节点 Move 不属于必须由两种模式实现的通用能力。
4. **持久化**：在引擎宿主内部区分本地可靠提交与远程 quorum 提交，明确不同承诺。
   共享事务语义不意味着共享复制位置或容灾保证；本地同步和恢复必须先验证。

对象存储已有真实接口，直接增加本地 Adapter，不再增加一层抽象。
以上是职责切面，不要求各自新增独立 crate，也不要求先搭一个包揽所有能力的
`PlatformBackend` 巨型接口。

### 相对难度与风险

| 工作 | 相对难度 | 主要风险 |
| --- | --- | --- |
| 前端内嵌、单文件启动入口 | 低 | 构建产物与启动装配 |
| 本地对象存储 | 中低 | 原子发布、路径安全、流式读写 |
| 进程内执行 | 中高 | 线程归属、队列、事务连接、取消与背压 |
| SQLite 元数据 | 高 | PostgreSQL 特有机制与业务原子性的重新实现 |
| 本地可靠提交与恢复 | 高，需先验证 | 成功响应是否可靠、故障恢复、checkpoint 与快照 |

以上为基于当前讨论的相对评估，不是工期承诺。最大风险不是能否启动，
而是重启、故障、事务和备份后是否仍然正确。

### 若未来推进

先验证本地 durability，再提取最小接口，走通“创建库 → 执行事务 → 重启读回”的纵向链路；
随后让现有 distributed 路径接入共享接口并做回归，再逐步覆盖鉴权、任务、备份和 Web。
不要先重构整个 Catalog，也不要为了未来扩展抽象所有模块。

切面是否有效的判断标准：新增 simple 模式时，变化主要集中在本地 Adapter 和启动装配，
而不是要求大规模修改 HTTP 与共享业务逻辑。当前仅保留该方向，暂不执行这些步骤。
