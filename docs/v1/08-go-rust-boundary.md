# peri-loom — Go / Rust 实现边界

## 总原则

> **Go 负责全局策略与协调。**  
> **Rust 负责数据库执行与正确性。**  
> **Storage 是最终 fencing authority。**

语言边界不是偏好，而是长期架构契约。

## 总体分层

```text
                 Go Control Plane

Worker Registry / Placement / Scheduler
Lease Issuance / Failover / Migration / API
                       │
                 coarse-grained RPC
                       ▼
                 Rust Data Plane

Worker Runtime / Ownership Enforcement
Turso / Pager / WAL / Checkpoint / Recovery
SegmentedStorage / Cache / Object Store Client
                       │
                       ▼
              Persistent Storage

    epoch validation / WAL fencing / HEAD CAS
```

## Rust 负责

### Database Engine

- Turso fork
- SQL execution
- Pager
- Storage abstraction
- SegmentedStorage

### Durability

- WAL
- remote WAL client
- transaction commit semantics
- checkpoint
- recovery
- generation/manifest runtime

### Cache

- RAM page cache
- RAM metadata cache
- SSD/NVMe segment cache
- lazy loading / prefetch

### Ownership Enforcement

- actual ownership state
- lease deadline
- epoch checking
- fencing
- stale writer rejection
- lifecycle state machine

### Worker Runtime

- multi-DB runtime
- open / close
- idle eviction
- local resource accounting

## Go 负责

### Cluster State

- Worker registry
- heartbeat aggregation
- health state
- capacity view

### Ownership Policy

- DB placement
- scheduler
- desired owner
- epoch allocation
- lease issuance / renewal policy

### Orchestration

- failover
- graceful migration
- worker drain
- rebalance

### Cloud Management

- management API
- tenant / organization
- auth policy
- quota policy
- admin backend

## 两边禁止跨越的边界

### Go 禁止做

- ReadPage
- LoadSegment
- AppendWalFrame
- CheckpointPage
- FlushPager
- ConstructManifest internals
- manipulate Turso transaction state

### Rust 禁止做

- 全局 Worker 选择
- scheduler policy
- cluster-wide load balancing
- region placement policy
- tenant allocation policy

## RPC 原则

只允许粗粒度动作，例如：

```text
AcquireDatabase
RenewLease
RevokeDatabase
DrainDatabase
GetDatabaseState
GetWorkerState
```

不使用跨语言 Pager/WAL/Segment 级别调用。

## State Ownership

```text
Go:
Desired State

DB foo should be on Worker B, epoch 73

-------------------------------

Rust:
Actual State

DB foo is Writer, epoch 73, lease valid
```

Go 通过 reconciliation 驱动 actual state 收敛到 desired state。

## 三层安全链

```text
Go Controller
    │ desired ownership
    ▼
Rust Ownership Runtime
    │ local enforcement
    ▼
Storage / WAL
    │ final fencing
    ▼
Persistent State
```

关键 invariant：

> **Go 可以暂时做错调度，但不能因此导致数据损坏。**

即使 Rust 进程存在 stale task 或网络分区，最终 Storage/WAL 仍必须拒绝旧 epoch mutation。
