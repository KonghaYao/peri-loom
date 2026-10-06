# P0 — Core Prototype

## 目标

用最小可运行系统证明 peri-loom 最核心的数据库模型成立：

```text
database != physical file

database = manifest + immutable segments + WAL
```

P0 不追求 Cloud、不追求高可用、不追求性能，唯一目标是证明新的持久化模型可以正确运行。

## 实现范围

### 1. Turso Fork

- Fork Turso OSS。
- 明确 upstream patch 边界。
- SQL Parser / VM / BTree 尽量不改。
- 找到 Pager / Storage / WAL / Checkpoint / Recovery 的最小切入点。

### 2. SegmentedStorage

```text
Turso
  │
Pager
  │
SegmentedStorage
```

要求：

- 固定 Segment 大小，初期建议 128 KiB。
- Page → Segment 映射。
- 支持随机读取逻辑数据库 offset。
- Segment 初期直接存本地目录。
- Pager 不知道 S3 / Object Storage 的存在。

### 3. Manifest / Generation

```text
HEAD
  ▼
Generation N
  ├── segment 0 → A
  ├── segment 1 → B
  └── segment 2 → C
```

Manifest 至少包含：

- generation id
- logical DB size
- page size
- segment size
- segment mapping
- applied WAL position

### 4. Lazy Segment Loading

打开数据库只加载 Manifest。

```text
Pager asks page
    ↓
page → segment
    ↓
load segment on demand
    ↓
return page
```

必须证明 DB 可以在没有完整物理 `.db` 文件的情况下工作。

### 5. WAL / Checkpoint / Recovery

Checkpoint：

```text
WAL dirty pages
    ↓
affected segments
    ↓
rebuild changed segments
    ↓
new Generation
    ↓
new Manifest
    ↓
HEAD
```

Recovery：

```text
Manifest + Segments + WAL tail → Database
```

## 明确不做

- Gateway
- Control Plane
- Worker Pool
- Remote Object Storage
- Remote WAL
- Auth / Tenant
- Branch UI/API
- DBA Panel

## 验收

```text
create DB
  ↓
write enough data to create multiple segments
  ↓
checkpoint
  ↓
kill process
  ↓
restart
  ↓
load only manifest
  ↓
query
  ↓
lazy-load required segments
  ↓
data correct
```

## 阶段产物

一个纯 Rust、单进程、可运行的 segmented Turso prototype。

## 实现入口

P0 原型位于 `prototypes/p0`，作为独立 Cargo workspace 运行，不接入旧平台的 Gateway、Control Plane 或远程存储。

```sh
cd prototypes/p0
cargo test --locked
cargo run --locked -- /tmp/peri-loom-p0 demo 400
cargo run --locked -- /tmp/peri-loom-p0 inspect
cargo run --locked -- /tmp/peri-loom-p0 query 'SELECT count(*) FROM p0_records'
```

Turso 作为 Git submodule 注册在 `prototypes/p0/turso`，使用用户提供的 fork `https://github.com/KonghaYao/turso`。开发分支为 `codex/p0-core-prototype`，P0 修改已提交为 `1a548d14a5be41e32b05bc43cb559cfb8b44dae4`，父仓库 gitlink 跟踪此提交；不使用 vendor 或 patch 重放脚本。尚未执行 push，需先发布 Turso 提交，再由其他机器用 `git submodule update --init --recursive` 获取该 revision。

运行说明见 `prototypes/p0/README.md`，upstream 补丁边界见 `prototypes/p0/UPSTREAM.md`，独立最终验收见 `prototypes/p0/VERIFICATION.md`。`inspect` 只加载 manifest；SQL open 仍需按需读取 header/schema 和恢复本地 WAL，不能将其解释为 SQL 初始化零读页。
