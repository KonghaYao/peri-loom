# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:20406', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | 271e0962ec12b230bc28aa29774e602aa5a46ed5 |
| connection_file | provided (redacted) |
| container | peri-loom-release-v127-f1-select-1-r2b1 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD 271e096 / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-04T12:55:12.257493+00:00 |
| ended_utc | 2026-11-04T12:55:12.353848+00:00 |
| duration_seconds | 0.096 |
| rounds | 1 |
| warmup | 1 |
| samples | 5 |
| concurrency_requests | 50 |
| resource_before | {'mem_usage': '165.9MiB / 11.73GiB', 'cpu_percent': '1.09%', 'memory_stat': {'anon': 167084032, 'file': 133734400, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 128880640, 'active_file': 4857856, 'slab': 0, 'pgfault': 52717, 'pgmajfault': 34}} |
| resource_after | {'mem_usage': '165.7MiB / 11.73GiB', 'cpu_percent': '0.95%', 'memory_stat': {'anon': 167194624, 'file': 133840896, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 128987136, 'active_file': 4857856, 'slab': 0, 'pgfault': 53889, 'pgmajfault': 34}} |
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
| HEAD 271e096 / rust v1.2.7 | F1.select-1 | json-pipeline | 5 | 2.36ms | 6.08ms | 1.29ms | 6.08ms | 1 | 323 | 0.04ms | PASS |
| HEAD 271e096 / rust v1.2.7 | F1.select-1 | protobuf-pipeline | 5 | 3.71ms | 7.19ms | 1.20ms | 7.19ms | 1 | 40 | 0.03ms | PASS |
| HEAD 271e096 / rust v1.2.7 | F1.select-1 | json-cursor | 5 | 3.14ms | 6.54ms | 1.14ms | 6.54ms | 1 | 252 | — | PASS |
| HEAD 271e096 / rust v1.2.7 | F1.select-1 | protobuf-cursor | 5 | 1.20ms | 5.81ms | 1.08ms | 5.81ms | 1 | 57 | — | PASS |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| F1.select-1 | json-pipeline | 2.36ms |
| F1.select-1 | protobuf-pipeline | 3.71ms |
| F1.select-1 | json-cursor | 3.14ms |
| F1.select-1 | protobuf-cursor | 1.20ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 5 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 50 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
