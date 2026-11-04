# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:20406', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | 271e0962ec12b230bc28aa29774e602aa5a46ed5 |
| connection_file | provided (redacted) |
| container | peri-loom-release-v127-b-w1-insert-r2b1 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD 271e096 / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-04T13:02:25.162458+00:00 |
| ended_utc | 2026-11-04T13:02:25.844119+00:00 |
| duration_seconds | 0.682 |
| rounds | 1 |
| warmup | 1 |
| samples | 5 |
| concurrency_requests | 50 |
| resource_before | {'mem_usage': '160MiB / 11.73GiB', 'cpu_percent': '0.60%', 'memory_stat': {'anon': 166449152, 'file': 61857792, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 61390848, 'active_file': 471040, 'slab': 0, 'pgfault': 54633, 'pgmajfault': 5}} |
| resource_after | {'mem_usage': '161MiB / 11.73GiB', 'cpu_percent': '0.53%', 'memory_stat': {'anon': 167133184, 'file': 67604480, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 67137536, 'active_file': 471040, 'slab': 0, 'pgfault': 55839, 'pgmajfault': 5}} |
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
| HEAD 271e096 / rust v1.2.7 | W1.insert | json-pipeline | 5 | 21.45ms | 37.30ms | 15.92ms | 37.30ms | 0 | 252 | 15.47ms | — |
| HEAD 271e096 / rust v1.2.7 | W1.insert | protobuf-pipeline | 5 | 16.79ms | 20.02ms | 13.43ms | 20.02ms | 0 | 31 | 11.60ms | — |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| W1.insert | json-pipeline | 21.45ms |
| W1.insert | protobuf-pipeline | 16.79ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 5 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 50 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
