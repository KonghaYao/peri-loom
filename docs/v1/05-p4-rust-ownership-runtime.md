# P4 — Rust Ownership Runtime

## 定位

P4 属于 Rust Data Plane。

它不决定数据库应该运行在哪个 Worker，而负责保证：

> **当前 Worker 只有在 ownership 有效时才允许修改数据库。**

这是 peri-loom 防 split-brain 的本地执行层。

## 核心状态

```text
OwnershipState
├── db_id
├── epoch
├── lease_id
├── lease_deadline
└── mode
```

建议状态机：

```text
Detached
Acquiring
Writer
ReadOnly
Draining
Revoked
```

## Rust 必须负责

### Epoch Enforcement

所有持久状态 mutation 必须检查 epoch：

- WAL append
- transaction commit
- checkpoint
- generation commit
- Manifest publish
- HEAD CAS
- destructive GC mutation

不能只在 SQL 请求入口检查一次。

### Lease Enforcement

Rust 自己维护 lease deadline。

```text
now >= lease_deadline
    ↓
Writer → Revoked
    ↓
all mutations rejected
```

即使 Go Controller 失联，Rust 也必须自行停止写入。

### WAL Fencing

Remote WAL 请求必须包含：

```text
db_id + epoch + LSN
```

Storage/WAL backend 必须拒绝 stale epoch。

### HEAD / Manifest Fencing

HEAD 更新必须至少校验：

```text
expected generation + current epoch
```

防止 stale Worker 完成旧 checkpoint 后覆盖新 Writer。

### Local DB Lifecycle

Rust 自己负责：

- open
- recover
- enter writer
- drain
- stop new transactions
- finish/abort transaction
- readonly transition
- close

## Rust 明确不负责

- Worker selection
- cluster-wide scheduling
- load balancing
- heartbeat aggregation
- failure detector
- global DB registry
- region policy
- tenant placement policy

## 对外动作接口

Rust 只接受粗粒度 ownership command：

```text
AcquireDatabase
RenewLease
RevokeDatabase
DrainDatabase
GetDatabaseState
GetWorkerState
```

## Safety Invariant

即使：

- Go Controller 出错
- 网络分区
- stale Worker 复活
- 延迟 RPC 到达

也必须保证：

> stale epoch 无法产生新的 durable mutation。

## 验收

```text
Worker A epoch=10 → write OK
new ownership epoch=11
Worker B epoch=11 → write OK
Worker A epoch=10 → WAL/HEAD mutation MUST FAIL
```
