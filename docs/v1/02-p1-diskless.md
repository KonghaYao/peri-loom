# P1 — Diskless

## 目标

把 P0 的本地 segmented database 升级为真正的远程 Object-Storage-native database。

从本阶段开始：

> **Worker 本地磁盘不再保存 authoritative database state，只允许作为 cache。**

## 1. ObjectStore Abstraction

统一接口支持：

- Local filesystem
- MinIO / S3-compatible
- S3
- R2

能力：

- GET / PUT
- checksum
- retry / timeout
- conditional write
- namespace isolation

## 2. Remote Segments

```text
Pager
  ↓
SegmentedStorage
  ↓
RAM Cache
  ↓ miss
SSD/NVMe Segment Cache
  ↓ miss
Object Store
```

要求：

- Manifest 远端存储。
- Segment 远端读取/上传。
- cold start 不下载整个 DB。
- Segment integrity verification。

## 3. Cache

- RAM page cache
- RAM segment metadata cache
- local SSD/NVMe segment cache
- eviction
- cache size limit
- restart 后可选择复用 SSD cache

## 4. Remote WAL

定义 `DurableWal` 抽象：

```text
append(db, epoch, batch) → LSN
read(db, from_lsn)
truncate(db, before_lsn)
```

P1 重点：

- WAL 可以远程持久化。
- commit 必须等待 durability confirmation。
- 支持 replay。
- 支持 batching。

## 5. Cold Recovery

```text
fresh process
  ↓
load HEAD / manifest
  ↓
lazy fetch required segments
  ↓
replay remote WAL tail
  ↓
serve query
```

## 验收

删除 Worker 本地所有 DB/cache 文件后：

```text
start fresh process
  ↓
remote manifest
  ↓
remote lazy segment fetch
  ↓
remote WAL recovery
  ↓
query succeeds
```

到这里 peri-loom 才真正可以称为 **Diskless Turso**。

## 实施入口

实现位于 `prototypes/v1`，复用 P0 的 Turso fork submodule。运行、S3 环境
配置和质量门见 `prototypes/v1/README.md`；独立验收及真实 RustFS S3-compatible
服务证据见 `docs/v1/P1-VERIFICATION.md`。AWS S3、R2 和 MinIO 品牌服务未实测，
不以协议 mock 代替真实服务验收。分布式所有权与 HEAD/epoch 联合 fencing 留在 P4。
