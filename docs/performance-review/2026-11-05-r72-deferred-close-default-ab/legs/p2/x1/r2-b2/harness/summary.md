# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:21437', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | 56dc9142d62c8ce22445b2b8aa90ed00fa8fbba6 |
| connection_file | provided (redacted) |
| container | peri-r72-ab-p2-x1-r2-b2 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD 56dc914 / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-05T08:07:39.599052+00:00 |
| ended_utc | 2026-11-05T08:07:40.002265+00:00 |
| duration_seconds | 0.403 |
| rounds | 3 |
| warmup | 1 |
| samples | 5 |
| concurrency_requests | 100 |
| resource_before | {'mem_usage': '167.9MiB / 11.73GiB', 'cpu_percent': '0.85%', 'memory_stat': {'anon': 167559168, 'file': 37634048, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 30683136, 'active_file': 6955008, 'slab': 0, 'pgfault': 63774, 'pgmajfault': 168}} |
| resource_after | {'mem_usage': '177.9MiB / 11.73GiB', 'cpu_percent': '1.13%', 'memory_stat': {'anon': 177496064, 'file': 39514112, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 32174080, 'active_file': 7344128, 'slab': 0, 'pgfault': 65893, 'pgmajfault': 177}} |
| resource_after_idle | {} |
| threads | 201 |
| messages | 8,625 |
| content chars | 20,177,069 |
| content bytes | 20,177,069 |
| focus thread messages | 8,625 |
| projection non-null | 0 |
| projection bytes | 0 |
| fixture | PASS |
| quota max_sessions | 16 |
| quota max_concurrent | 100 |
| quota requests_per_second | 500 |
| quota max_result_bytes | 4,194,304 |

## 结果

| 版本 | 场景 | 协议 | 样本 | 中位数 | p95 | 最小 | 最大 | 行数 | 响应字节 | SQL-only | 结果 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|

## 并发

| 并发 | 请求 | 中位数 | p95 | p99 | 429 | 错误 | active_permits | unknown_held_permits | session_seats | 排空 | 结果 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 100 | 1.23ms | 3.94ms | 5.96ms | 0 | 0 | 0 | 0 | 0 | 3.92ms | PASS |
| 4 | 100 | 1.43ms | 6.69ms | 7.82ms | 0 | 0 | 0 | 0 | 0 | 3.42ms | PASS |
| 8 | 100 | 4.10ms | 9.18ms | 11.29ms | 0 | 0 | 0 | 0 | 0 | 3.27ms | PASS |
| 16 | 100 | 6.07ms | 12.28ms | 19.32ms | 52 | 62 | 0 | 0 | 0 | 2.99ms | FAIL |

## 备注

- 单流场景：3 轮 × 每轮 1 次 warmup + 5 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- **并发验收 FAIL；本次运行不计为 PASS。**
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
