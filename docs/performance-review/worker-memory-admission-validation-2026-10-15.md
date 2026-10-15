# Worker 内存准入增量验证（2026-10-15）

源码：主仓库 `a18d9d3`，Turso 子模块 `aaee58591`。本页只记录本轮 F05 增量的证据，不能作为 T04 或生产内存验收。

## 已验证

- `cargo test --manifest-path prototypes/v1/Cargo.toml --test p6_budget --test recovery --quiet`：8 + 14 项通过。覆盖 `randomblob` 分配前拒绝、`PRAGMA cache_size` 无法超过 16 MiB 对应页数、64 KiB 页重新换算缓存页数及恢复测试。
- `cargo test --manifest-path prototypes/v1/Cargo.toml --test runtime_registry --test p4_runtime --quiet`：18 + 26 项通过。覆盖已有运行时容量、写入及恢复回归。
- `cargo test --manifest-path prototypes/v1/Cargo.toml --lib reservations_limit_512m_and_2g_workers_without_overflow --quiet`：1 项通过。检查 512 MiB 与 2 GiB 算术预算及溢出拒绝。
- `cargo test --manifest-path prototypes/v1/Cargo.toml --lib runtime_rejects_a_second_session_then_releases_its_reservation --quiet`：1 项通过。实际 WorkerRuntime 在预留预算耗尽时拒绝第二个会话，释放后重新准入。

## 容器实测状态

Docker Engine 29.4.0 可用。已由主仓库 `a18d9d3` 与 Turso `aaee58591` 建立干净的临时源码副本，尝试构建 `peri-loom:t04-a18d9d3`。基础镜像拉取约 2 分钟后仍在传输 Rust/Go 镜像层，未进入源码编译；本轮主动停止。构建日志保留于 `/tmp/peri-t04-a18d9d3.NVHhua/docker-build.log`。没有新镜像，也没有 cgroup 峰值、OOM、PSI 或 CPU throttle 数据。

## 限制

页缓存容量在 Turso 中仍是软上限，受 pin/dirty 页影响可短暂超额。16 MiB 会话预留未约束 TEMP、哈希、排序、WAL 和其他标量函数分配；cgroup v2 `memory.max` 不存在或为 `max` 时没有进程预留预算。因此 F05 与 T04 仍未通过。
