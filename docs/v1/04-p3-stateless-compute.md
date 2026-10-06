# P3 — Stateless Compute

## 目标

把数据库和运行它的机器彻底解绑：

> **DB belongs to storage, Worker only owns compute + cache.**

## Worker 结构

```text
DB Worker (Rust)
│
├── Multi-DB Runtime
├── Turso Engine instances
├── RAM Page Cache
├── SSD/NVMe Segment Cache
└── Object Storage / WAL clients
```

## 1. Multi-DB Runtime

- dynamic DB open
- dynamic DB close
- DB registry
- active/inactive state
- resource accounting
- idle eviction

## 2. Scale-to-zero

Idle DB 应该做到：

```text
no process
no memory
no attached database volume

only:
manifest + segments + WAL
```

## 3. Worker Disposal

Worker 本地没有 authoritative DB state，因此：

- Worker 可以 kill -9。
- Worker 可以重建。
- SSD cache 可以全部删除。
- 任意新 Worker 都能重新打开 DB。

## 4. Migration

迁移不搬完整数据库：

```text
Worker A stops serving DB
        ↓
Worker B loads manifest
        ↓
lazy segment fetch
        ↓
WAL recovery
        ↓
serve
```

## 本阶段不解决

P3 **不解决多个 Worker 同时争抢 Writer 的分布式正确性**。

该问题由 P4 Ownership Runtime 解决。

## 验收

- 任意 Worker 可以 cold-open 任意 DB。
- Worker 销毁不会造成数据丢失。
- 大 DB 迁移不需要复制完整 DB 文件。
- idle DB 可以完全卸载。
