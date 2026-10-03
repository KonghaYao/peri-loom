# Serverless 生命周期与性能验收口径（只读代码审计）

本文件仅分析架构、代码和验收脚本；**没有启动服务、施加负载或取得实测性能值**。以下阈值均为 [架构第 16 章](../../tursodb-db-platform-architecture.md)的设计目标，不代表当前通过。

## 生命周期与可观测点

| 环节 | 代码行为 | 可实测口径 | 第 16 章目标 | 当前结果 |
| --- | --- | --- | --- | --- |
| 冷库唤醒 | [router.rs](../../services/db-server/src/router.rs) 经 Catalog `try_begin_wakeup` 合并请求；Leader 做 placement、ownership、Worker Start；所有请求轮询 Catalog READY，默认 20 ms 间隔、3 s 上限 | 对已确认 COLD 且 Storage Ready 的多库样本，各发一次 SQL；分别从请求进入至 READY、首个 SQL 完成打时间戳，计算分位数；记录 `cold_start_micros`，检查错误 | COLD→READY P50 ≤150 ms、P95 ≤500 ms、P99 ≤1 s；首查询 P95 ≤700 ms、P99 ≤1.5 s；成功率 ≥99.9% | 未测 |
| 同库并发唤醒 | Catalog 原子唤醒标志与租约；Worker 对同 epoch Start 视为幂等 | 每轮先确认 COLD；同步放行 100 请求；从 **Worker** `/metrics` 的 `start_db_total{outcome=...}` 增量、进程 PID/epoch 与全部响应验证每轮恰好一个实际 spawn；多轮重复 | 100 并发只产生 1 次 Start，双 Owner/损坏 0 次 | 未测 |
| 空闲回收 | [background.rs](../../services/db-server/src/background.rs) 每 30 s 扫 WARM；`evictable=true`、`updated_at` 早于 600 s、Worker 预算利用率至少 80% 时 Stop 并置 COLD | 构造资源压力与空闲 WARM 库，观察 Stop、进程数、RSS、Catalog COLD；再查询并量唤醒。无压力时不会自动回收 | 第 16 章没有回收时延阈值；COLD 资源为 0 是第 7 章模型 | 未测 |
| 高密度/资源 | Worker 按预算做 admission；心跳资源采样用 cgroup v2，回退 `/proc`；`db_process_cpu`、`db_process_memory_mib`、`worker_saturation` 在 Worker metrics | 基线 32 vCPU/128 GiB Worker 跑 500 个 WARM 库，稳定采样进程 RSS/CPU、Worker 饱和度；并测 80%/90% 准入与 reserve | ≥500 WARM；Idle 增量 RSS P95 ≤24 MiB/DB；Idle CPU ≤0.1% core；packing 70–75%；80% 停普通 placement、90% 保护；reserve ≥max(1 Worker,20%) | 未测 |
| 并发启动/扩容 | [scheduler](../../crates/scheduler/src/scheduler.rs) 用六维预算及 reserve 选择 Worker；新 Worker 的实际弹性供给不属于该路径 | 同步启动足量不同 DB，按 READY 完成时间算持续 start/s、P95、失败率；并持续采集预算与实测内存、资源水位 | ≥50 DB/s；STARTING→READY P95 ≤1 s；失败率 <0.1%；实际内存不超过估算 10%；90% 后不无界启动 | 未测 |
| 热路由 | [router.rs](../../services/db-server/src/router.rs) 命中 Route Cache 时绕过 PostgreSQL；stale route 最多透明重试一次 | 稳态批次前后读取 Server `route_cache_hit_total/miss_total` 增量；内部 route/dispatcher 分段打点与客户端总延迟分开；故障注入旧路由测 refresh | 命中率 ≥99.9%；stale refresh P95 ≤500 ms；恢复成功 ≥99.9%，泄漏错误 <0.01% | 未测 |
| DB 崩溃恢复 | [supervisor.rs](../../services/db-worker/src/supervisor.rs) 进程监控、清理、指数退避重启；默认最多 3 次、首次退避 100 ms | 对独立测试库注入进程崩溃，记录退出、Route 清理、恢复首个成功查询各时间；同时维持邻居 DB 负载 | 检测 ≤500 ms；Route 清理 ≤100 ms；重启 P95 ≤1 s；邻居错误增量 ≤0.1%、P99 恶化 ≤10% | 未测 |
| Worker 故障恢复 | Worker 默认 1 s 心跳；Server 1 s 检查、3 次 miss 标 Suspect，下一次 miss 标 Unavailable 并逐库 failover | 断开 Worker/容器后持续查询和写入，记录故障、状态变更、epoch fencing、首个成功查询；故障前后核对 committed 数据 | 检测 P95 ≤4 s；路由恢复 P95 ≤10 s；RTO P95 ≤15 s；RPO 0 committed、旧 epoch 写入拒绝率 100% | 未测 |

## 测试环境与已有脚本的口径问题

- 设计比较基线为 Server 8 vCPU/16 GiB、Worker 32 vCPU/128 GiB/NVMe、Server↔Worker RTT ≤1 ms、Worker↔WAL RTT P95 ≤2 ms、≥10 Gbps 网络；热读为 1 GiB DB 的索引点查、结果 ≤1 KiB 且页面已缓存。与这些条件不同的本机结果应单列环境，不直接判定设计验收通过。
- [acceptance.sh](../../scripts/acceptance.sh) 默认只执行 `hot cold crash durability http compose`；第 16 章的高密度、回收、扩容、Worker 故障、路由故障等没有对应自动场景。
- `cold.cold_to_ready_ms` 实际只量 **一次首查询总耗时**，随后与 P99 上限 1 s 比较；这既不是 COLD→READY，也无法推出 P99。100 并发唤醒只比较 Server `start_db_total` 差值 ≤1，未断言请求全部成功，也未读 Worker 的实际 spawn 次数；Server 指标由成功控制调用记录，Worker 指标另在 Worker 端口。
- [hot-query.js](../../deploy/k6/hot-query.js) 的 `platform_latency_ms` 是 `res.timings.duration`，即客户端端到端请求耗时。[acceptance.sh](../../scripts/acceptance.sh) 却把该 P95/P99 对比平台附加延迟 3/8 ms；脚本顶部说明也要求内部指标分段判读。这会产生错误的 FAIL，且脚本未单独检查端到端 10/25 ms。其 20 s/32 VU 固定并发无法保证达到 10,000 RPS，需另做足够发压能力的梯度测试。
- `crash.recovery_ms` 也是单次进程杀死到查询恢复，不能证明 P95；退出检测、Route 清理和邻居影响未被量到。`compose` 场景会重启服务，不应与压测并发运行。
- 指标采集位置不同：Server 指标在 Server 内部 `/metrics`；Worker `start_db_total`、`db_process_cpu`、`db_process_memory_mib`、`worker_saturation` 在 Worker ops `/metrics`；WAL fencing 计数在 wal-service `:9300/metrics`。采集时记录具体端点、实例及计数器重置，不能把不同实例的同名计数器当成一条连续序列。
- [docker-compose.yml](../../docker-compose.yml) 将 `WORKER_MAX_DB_PROCESS` 默认设为 2000，而 [db-worker CLI](../../services/db-worker/src/cli.rs) 单独运行默认 256。500 WARM 容量实验需记录生效配置；在单独运行的默认值下，进程硬上限本身就阻止达到目标。
- 回收代码以 Catalog `updated_at` 判空闲，Worker 本地另有 `last_activity_unix_ms`。测试时应确认查询活动是否刷新 Catalog `updated_at`；否则“空闲 600 s”可能只是元数据未更新，回收正确性有风险。另一次 tick 只列最多 500 个 WARM 库，超过 500 的回收覆盖率应单测。

## 建议报告列

每个场景保存：测试时间、代码 revision、部署模式、生效配置、机器与网络、DB 样本数、并发、持续时间、成功/失败数、P50/P95/P99、吞吐、Server/Worker/WAL 指标快照、原始输出路径、目标值、判定。单样本只写“单次观测”，不标成 P95/P99。
