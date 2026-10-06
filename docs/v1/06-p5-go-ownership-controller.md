# P5 — Go Ownership Controller

## 定位

P5 属于 Go Control Plane。

它负责回答：

> **哪个 Worker 应该拥有哪个数据库？**

Go 决定 desired ownership；Rust P4 执行 actual ownership safety。

## Go 持有的全局状态

```text
DatabasePlacement
├── db_id
├── desired_worker
├── epoch
├── lease_id
├── lease_deadline
└── desired_state
```

这里是 **desired state**，不是数据库内部真实状态。

## Go 必须负责

### Worker Registry

维护：

- worker id
- address
- region
- capacity
- load
- health
- last heartbeat
- draining state

### Placement

决定：

```text
DB foo → Worker B
```

未来策略可使用：

- load
- locality
- cache affinity
- region
- capacity
- tenant policy

### Epoch Allocation

Epoch 必须全局单调增加。

Worker 不能自行生成新 epoch。

### Lease Issuance / Renewal

Go 决定：

- grant
- renew
- stop renew
- revoke intent

Rust 负责 lease 过期后的真实自我禁写。

### Failure Detection

根据：

- heartbeat timeout
- RPC failure
- worker state

判断 Worker 是否失效。

但“认为 Worker dead”不等于立即允许新 Writer 写入，必须先推进 fencing epoch。

### Failover Orchestration

```text
A heartbeat lost
   ↓
advance epoch
   ↓
grant B
   ↓
B AcquireDatabase
   ↓
B recovery
   ↓
B Writer
```

### Migration Orchestration

正常迁移：

```text
Drain A
  ↓
A stops new tx / flushes state
  ↓
advance epoch
  ↓
Acquire B
  ↓
B recover
  ↓
B Writer
```

Migration 本质上是优化过的 failover。

## Go 明确不负责

禁止进入：

- Pager
- segment read/write
- WAL frame serialization
- checkpoint algorithm
- dirty page tracking
- Manifest content construction
- Turso transaction state
- recovery internals
- page cache

禁止出现这种 RPC：

```text
ReadPage
LoadSegment
AppendWalFrame
CheckpointPage
FlushPager
```

## Reconciliation Model

```text
Go: Desired State
        ↓
   reconciliation
        ↓
Rust: Actual State
```

Go 可以暂时判断错误或状态滞后，但 Rust/Storage 必须保证不会因此产生非法持久写入。

## 验收

- 自动发现 Worker 丢失。
- 自动推进 epoch。
- 自动将 DB 重新分配给新 Worker。
- 旧 Worker 恢复后无法继续写。
- graceful migration 与 failure failover 使用同一套 ownership primitive。
