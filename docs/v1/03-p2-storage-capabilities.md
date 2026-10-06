# P2 — Storage Capabilities

## 目标

利用 `Generation + Immutable Segment` 模型，把 branch / snapshot / clone / PITR 从高成本数据库操作降级为廉价 storage metadata 操作。

## 1. Immutable Generations

每次 checkpoint 形成新 generation：

```text
G100 = [A, B, C, D]
G101 = [A, B, C2, D]
```

未修改 Segment 直接复用。

## 2. Branch

```text
main     → G100
branch-A → G100
```

创建 branch 不复制数据。

后续写入：

```text
main     → G101 [A,B,C2,D]
branch-A → G102 [A,B3,C,D]
```

## 3. Snapshot

Snapshot 本质是 pin generation：

```text
snapshot-2026-10-06 → G100
```

要求：

- instant snapshot
- immutable snapshot
- restore
- clone

## 4. PITR

组合：

```text
Generation History + WAL History
```

实现 point-in-time recovery。

## 5. Content-addressed Segments

建议本阶段引入：

```text
segment_id = BLAKE3(segment content)
```

获得：

- immutable object identity
- integrity verification
- natural deduplication
- COW sharing

## 6. Garbage Collection

必须处理：

- reachable generations
- branch refs
- snapshot pins
- orphan manifests
- orphan segments
- WAL retention
- delayed GC safety window

## 验收

- branch creation 不复制完整 DB。
- snapshot creation 仅修改 metadata。
- clone 共享已有 segment。
- 删除 branch/snapshot 后 GC 能安全释放不可达对象。
