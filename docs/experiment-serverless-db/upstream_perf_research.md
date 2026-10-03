# Turso 原生引擎性能资料核对

核对日期：2026-10-03。项目 `Cargo.toml` 锁定 `turso_core = "=0.8.1"`。这里的 Turso 指原生嵌入式数据库引擎（原名 Limbo）；Turso Cloud / libSQL 的 HTTP 服务是另一个测量对象。

| 一手来源与版本 | 工作负载和环境 | 官方数字 | 与本项目 Simple HTTP 压测的关系 |
| --- | --- | --- | --- |
| [Limbo 发布文章，2024-12](https://turso.tech/blog/introducing-limbo-a-complete-rewrite-of-sqlite-in-rust) | MacBook Air M2；`cargo bench`；嵌入式执行 `SELECT * FROM users LIMIT 1`，SQLite 已调优 | Limbo 506 ns，SQLite 620 ns | 只有 SQL 引擎微基准，没有 HTTP、鉴权、Worker 调度，也没有持久化写；不能用作本项目 HTTP 请求的预期延迟。该数字属于 2024 年早期 Limbo，并非 0.8.1。 |
| [Turso 0.8 发布文章，2026-09](https://turso.tech/blog/turso-0.8.0) | 原生嵌入式 `BEGIN CONCURRENT`；每事务插入 100 行、不同键；1–64 个连接，泊松到达；Linux/Fedora 44、Ryzen 9 3900XT、64 GB、Kingston NV3 NVMe、XFS；两引擎 `synchronous=FULL`；Turso commit `fd41c07dc` vs SQLite 3.50.2 | 64 连接 Turso 约 9,500 **事务/s**，SQLite 约 1,370 事务/s；在 1,000 事务/s 的延迟实验中，32 连接 Turso p99.9 为 2.4 ms | 这是嵌入式批量持久化事务，非单行 HTTP 写入。100 行/事务与本项目一请求一行/事务不能按行数或 TPS 直接比较；本项目也未验证启用 `BEGIN CONCURRENT`。 |

官方 [0.8 性能基准源码](https://github.com/tursodatabase/turso/tree/main/perf) 开源，但这里未在同一硬件、数据集、事务设置下复现。2024 年文章说明 Turso Cloud 当时通过 HTTP 提供的是 SQLite/libSQL，与新 Turso 原生引擎不同。对 **原生 0.8.1、单行主键点查、持久化单行写入、相同 macOS ARM64 机器**，目前未找到一手的可直接对照的延迟数据。

因此，项目压测的 10–100 ms 数字表明的是 Simple 模式 HTTP 端到端路径表现，不能据此判断 Turso 原生引擎本身是否慢。要定位差额，需在同机同数据集上分别测 `turso_core` 引擎调用、Simple 服务内部各阶段和 HTTP 客户端耗时；写入须核对事务边界、`synchronous` 设置和 fsync 次数。
