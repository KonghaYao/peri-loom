# peri-loom 总体路线

> **peri-loom**：基于 Turso OSS fork 构建的 Diskless / Serverless 数据库平台。
>
> 核心演进目标：
>
> `database = file` → `database = manifest + immutable segments + WAL`

## 设计原则

1. **渐进式开发**：每个阶段都必须独立可运行、可验证。
2. **先证明数据库模型，再做平台化**：P0/P1 不引入 Gateway、Control Plane、Auth、Tenant 等外围能力。
3. **架构边界稳定**：SQL / VM / BTree 尽量跟随 Turso upstream；主要 fork 范围集中在 Storage / WAL / Checkpoint / Recovery。
4. **Rust 管数据正确性，Go 管全局控制**：两侧只通过粗粒度 RPC 协作。
5. **Storage 是最终 fencing authority**：即使控制面或 Worker 出错，也不能允许 stale writer 写坏持久状态。
6. **本地磁盘只做 cache**：从 P1 开始，Worker 本地状态必须可丢弃。

## 阶段总览

```text
P0  Core Prototype
│   Turso Fork / SegmentedStorage / Manifest / Lazy Load / WAL Recovery
▼
P1  Diskless
│   Object Storage / Remote WAL / RAM+SSD Cache
▼
P2  Storage Capabilities
│   Branch / Snapshot / Clone / PITR / GC
▼
P3  Stateless Compute
│   Multi-DB Worker / Scale-to-zero / Migration
▼
P4  Ownership Runtime (Rust)
│   Epoch / Lease Enforcement / Fencing / Safe State Machine
▼
P5  Ownership Controller (Go)
│   Placement / Scheduler / Lease Issuance / Failover / Migration Orchestration
▼
P6  Cloud Platform
    Gateway / API / Auth / Tenant / Quota / Panel / SDK
```

## 每阶段一句话

- **P0**：证明 `manifest + segments + WAL` 可以替代完整数据库文件。
- **P1**：证明数据库状态可以完全放在远端，本地只保留 cache。
- **P2**：利用 generation 模型自然获得 branch / snapshot / clone / PITR。
- **P3**：证明任意 Worker 都可以运行任意 DB，Worker 可随时销毁。
- **P4**：证明即使发生网络分区、旧 Worker 复活，也不会出现非法写入。
- **P5**：让 Go 控制面自动决定数据库应该运行在哪里，并安全驱动 failover/migration。
- **P6**：把已验证的数据库架构产品化为完整 Cloud Platform。

## 长期语言边界

```text
                Go Control Plane

placement / scheduler / lease / failover / API
                       │
                  coarse RPC
                       ▼
                Rust Data Plane

worker / ownership enforcement / Turso / WAL
checkpoint / recovery / segmented storage / cache
                       │
                       ▼
             Persistent Storage Layer

      WAL fencing / HEAD CAS / epoch validation
```

**原则：Go 可以暂时做错调度，但不能因此导致数据损坏。**
