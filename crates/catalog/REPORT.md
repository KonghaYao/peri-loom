# crates/catalog 交付报告

PostgreSQL Catalog 访问层（架构 §17.5 / §17.5.1）。SQLx 运行期 API，无 ORM、无编译期宏；
控制面状态变更用显式事务 + 行锁；后台任务用 Job Table + `FOR UPDATE SKIP LOCKED` + lease。
SQL 与 `migrations/0001_init.sql`（已冻结，本 crate 未改动）的列名 / CHECK 取值完全一致。

## 1. 交付模块

| 文件 | 职责 |
| --- | --- |
| `lib.rs` | `Catalog` 本体：`connect` / `migrate` / `pool` / `health_check`，并汇聚全部集成测试 |
| `pg.rs` | 列清单、domain 记录的行映射（手写 `FromRow`）、SQL 组装器、类型转换工具 |
| `error.rs` | `CatalogError`（thiserror）+ SQLSTATE 分类映射到 `domain::error::ErrorCode` |
| `databases.rs` | Database catalog、Ownership/Epoch、Coalesce Wakeup、Route Cache 全量视图 |
| `workers.rs` | Worker inventory、心跳、missed_heartbeats、状态标记 |
| `operations.rs` | 长操作（202 + operation_id）记录 |
| `idempotency.rs` | Idempotency-Key 判定（First / Replay / Conflict） |
| `jobs.rs` | Job 队列：入队、抢占（SKIP LOCKED + lease）、完成/重试、续租 |
| `backup.rs` | Snapshot metadata、Backup / Restore / PITR 作业记录 |
| `rbac.rs` | 用户、API Token（只存哈希）、权限合并、审计日志 |
| `panel.rs` | Panel preferences / Saved SQL / Slow query |
| `watcher.rs` | `LISTEN catalog_changes` + 断线重连（NOTIFY 仅作加速提示） |

## 2. 公共 API 清单

**连接与迁移**
`Catalog::connect(url, max_conns)` / `from_pool` / `migrate()` / `pool()` / `health_check()` / `migrations_dir()`

**全局版本与变更通知（§17.4 / B）**
`current_catalog_version()`；`watch_catalog_changes() -> CatalogWatcher`；
`CatalogWatcher::{recv, try_recv, reconnect, last_version, reconnect_count}`；`CatalogChange::parse`

**Database catalog（C）**
`create_database(CreateDatabaseParams)`（tenant 缺省 `00000000-0000-0000-0000-000000000001`）、
`get_database`、`get_database_by_name`、`list_databases(DatabaseFilter)`（tenant/worker/state/name_prefix + limit/offset）、
`soft_delete_database`、`set_lifecycle_state`、
`bump_ownership` / `bump_ownership_with_reason`（epoch 校验 + 审计行，0 行返回 `EPOCH_MISMATCH`）、
`renew_lease`、`clear_stale_ownership`、`try_begin_wakeup`（→ `WakeupDecision::{Leader, Waiter, AlreadyRunning}`）、
`end_wakeup`、`list_routing_entries`、`list_ownership_events`

**Worker inventory（D）**
`upsert_worker(UpsertWorkerParams)`、`record_heartbeat(...) -> RecordHeartbeatOutcome{request_full_inventory, draining, catalog_version}`、
`mark_missed_heartbeats` / `mark_missed_heartbeats_with_timeout`、`mark_worker_state`、`list_workers`、`get_worker`

**Operations（E）**：`create_operation` / `create_operation_with_id`、`update_operation`、`get_operation`、`list_operations`

**Idempotency（F）**：`begin_idempotent(key, request_hash)`、`attach_idempotent_operation`、`complete_idempotent`、`purge_expired_idempotency`

**Job 队列（G）**：`enqueue_job`、`lease_job`、`complete_job`、`complete_job_fenced`、`extend_job_lease`、`get_job`

**Snapshot / Backup（H）**：`insert_snapshot`、`latest_snapshot`、`list_snapshots`、`mark_snapshot_state`、`create_backup_job`、`update_backup_job_state`、`list_backup_jobs`

**RBAC / Token / Audit（I）**：`find_user_by_username`、`find_user`、`create_user`、`list_users`、`create_api_token`、`find_user_by_token_hash`、`revoke_api_token`、`list_tokens_for_user`、`touch_token_last_used`、`resolve_permissions`、`append_audit`、`list_audit`

**Panel（J）**：`get_preference` / `set_preference` / `list_preferences`、`create_saved_query` / `list_saved_queries` / `delete_saved_query`、`insert_slow_query` / `list_slow_queries`

## 3. 关键实现说明

- **Split Brain 防护**：`bump_ownership` 在单事务内 `SELECT ... FOR UPDATE` 锁行 → 校验 epoch →
  `UPDATE ... WHERE owner_epoch = $2`（0 行即 `EPOCH_MISMATCH`）→ 同事务写 `ownership_events`；
  `clear_stale_ownership` 回收过期租约时同样递增 epoch 并写 `lease_expired` 审计。
- **Coalesce Wakeup（§8）**：单条 `UPDATE ... WHERE wakeup_in_progress = FALSE AND state = 'COLD' RETURNING id`
  完成「检查 + 置位」，命中即 Leader；未命中再区分 Waiter / AlreadyRunning。
- **幂等（§16）**：`INSERT ... ON CONFLICT DO NOTHING RETURNING` + 同事务 `SELECT`；hash 不一致返回 `Conflict`。
- **Job lease**：抢占事务内先回收过期 lease（`attempts >= max_attempts` 直接 FAILED），再
  `ORDER BY priority, run_after FOR UPDATE SKIP LOCKED LIMIT 1` 后置为 LEASED 并 `attempts + 1`。
- **心跳 fencing**：Worker 上报的本地 DB 状态只有在 `(owner_worker_id, owner_epoch)` 与 Catalog 一致时才被接受；
  `state IN ('DRAINING','EMPTY')` 不会被心跳覆盖为 ACTIVE。
- **错误映射**：`RowNotFound` → 实体对应码（DB → `DB_NOT_FOUND` 等）；`23505` → `DB_ALREADY_EXISTS` / `IDEMPOTENCY_CONFLICT`；
  `23503` → Worker 外键为 `WORKER_UNAVAILABLE`、其余 `INVALID_ARGUMENT`；`23514` → `CONSTRAINT_VIOLATION`；
  连接/池/IO 与 `40001`/`40P01` 标记 `retryable`，其余内部错误显式关闭 `retryable`。
- 所有多步写操作都在事务内完成（代码注释写明了原因），SQL 全部使用绑定参数（含 LIMIT/OFFSET）。

## 4. 测试结果

| 命令 | 结果 |
| --- | --- |
| `cargo test -p catalog` | **39 passed / 0 failed / 18 ignored**（无 PostgreSQL 时全部通过） |
| `cargo clippy -p catalog --all-targets -- -D warnings` | **0 警告** |
| `cargo fmt -p catalog -- --check` | 干净 |
| `DATABASE_URL=... cargo test -p catalog -- --ignored` | **18 passed / 0 failed**（PostgreSQL 16，另在空 schema 上复跑通过） |

单元测试（无 DB）覆盖：SQLSTATE 分类与 retryable 语义、`NotFoundAs`/`ConflictAs` 全实体映射、
`SqlBuilder` 占位符顺序、LIKE 元字符转义、LIST 查询构造、epoch/值饱和转换、
`IdempotencyOutcome` 判定、Job 退避、审计过滤条件绑定、枚举白名单与 schema CHECK 取值一致性。

真库测试（`#[ignore]`，需 `DATABASE_URL`）覆盖：迁移与初始版本、DB 建/查/软删与唯一冲突、
epoch 错配拒绝 + 审计、**并发接管仅一个成功**、**并发唤醒仅一个 Leader**、租约过期回收与 Route Cache 快照、
心跳重置/排除/miss 计数/fencing、**并发幂等提交仅一个 First**、Job 幂等入队与独占租约、失败重试回 READY、
Snapshot 注册与审计落库、Operations 生命周期与 idempotency_key 唯一、RBAC 用户/角色合并/token 认证与吊销、
Panel preferences(upsert)/saved queries/slow queries、Backup job 生命周期、LISTEN/NOTIFY 实际送达。

运行真库测试：

```bash
DATABASE_URL=postgres://user:pass@host:5432/db cargo test -p catalog -- --ignored --test-threads=1
```

## 5. 未完成项

无功能性遗留。以下为已知边界（有意为之）：

- `CatalogWatcher` 只做「连接级」重连（3 次退避后上抛）；漏事件的补偿由调用方用
  `current_catalog_version()` 全量 reconcile 完成（§17.4 的设计前提）。
- `resolve_permissions` 对 superuser 追加通配符 `*`，通配语义由调用方解释。
- Panel 的 `saved_filters` 表（schema 已存在）未封装访问方法：本次交付范围未要求，其他模块如需可补。

## 6. 需要的额外依赖

**无**。全部能力用现有依赖（sqlx / tokio / serde / serde_json / uuid / chrono / thiserror / tracing）实现。
仅提示：`crates/catalog/Cargo.toml` 中 `async-trait`、`futures`、`tempfile` 当前未被本 crate 使用，
为避免改动 lock 文件未做增删，如需清理请由仓库维护者统一处理。
