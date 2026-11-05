# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:21326', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | af669318b73e763e43385492cb97149a1a73c2c3 |
| connection_file | provided (redacted) |
| container | peri-r71-ab4-p2-l1-r2-a2 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD af66931 / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-05T06:41:24.501213+00:00 |
| ended_utc | 2026-11-05T06:41:24.861930+00:00 |
| duration_seconds | 0.361 |
| rounds | 1 |
| warmup | 1 |
| samples | 125 |
| concurrency_requests | 100 |
| resource_before | {'mem_usage': '162MiB / 11.73GiB', 'cpu_percent': '1.10%', 'memory_stat': {'anon': 166567936, 'file': 115499008, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 113881088, 'active_file': 1622016, 'slab': 0, 'pgfault': 61676, 'pgmajfault': 23}} |
| resource_after | {'mem_usage': '161.2MiB / 11.73GiB', 'cpu_percent': '0.65%', 'memory_stat': {'anon': 166252544, 'file': 116031488, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 114413568, 'active_file': 1622016, 'slab': 0, 'pgfault': 62944, 'pgmajfault': 23}} |
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
| HEAD af66931 / rust v1.2.7 | L1.list-20 | json-pipeline | 125 | 2.18ms | 4.74ms | 0.91ms | 6.94ms | 40 | 15,627 | 0.11ms | PASS |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| L1.list-20 | json-pipeline | 2.18ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 125 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
