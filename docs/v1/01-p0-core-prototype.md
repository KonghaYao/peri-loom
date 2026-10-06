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
