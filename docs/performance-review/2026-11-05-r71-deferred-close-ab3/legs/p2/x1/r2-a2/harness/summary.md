# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:21237', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | 596361bedea04b069e18d76faa6698687811af23 |
| connection_file | provided (redacted) |
| container | peri-r71-ab3-p2-x1-r2-a2 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD 596361b / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-05T05:13:03.645138+00:00 |
| ended_utc | 2026-11-05T05:13:04.131723+00:00 |
| duration_seconds | 0.487 |
| rounds | 3 |
| warmup | 1 |
| samples | 5 |
| concurrency_requests | 100 |
| resource_before | {'mem_usage': '162MiB / 11.73GiB', 'cpu_percent': '0.89%', 'memory_stat': {'anon': 167727104, 'file': 146038784, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 145924096, 'active_file': 118784, 'slab': 0, 'pgfault': 62165, 'pgmajfault': 2}} |
| resource_after | {'mem_usage': '172.1MiB / 11.73GiB', 'cpu_percent': '1.18%', 'memory_stat': {'anon': 178614272, 'file': 143175680, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 142770176, 'active_file': 389120, 'slab': 0, 'pgfault': 64741, 'pgmajfault': 5}} |
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
| 1 | 100 | 1.08ms | 5.77ms | 6.49ms | 0 | 0 | 0 | 0 | 0 | 7.26ms | PASS |
| 4 | 100 | 1.52ms | 6.32ms | 7.26ms | 0 | 0 | 0 | 0 | 0 | 2.96ms | PASS |
| 8 | 100 | 2.63ms | 10.83ms | 11.50ms | 0 | 0 | 0 | 0 | 0 | 2.82ms | PASS |
| 16 | 100 | 8.12ms | 33.91ms | 37.08ms | 33 | 46 | 0 | 0 | 0 | 2.61ms | FAIL |

## 备注

- 单流场景：3 轮 × 每轮 1 次 warmup + 5 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- **并发验收 FAIL；本次运行不计为 PASS。**
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
