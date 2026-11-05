# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:21315', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | af669318b73e763e43385492cb97149a1a73c2c3 |
| connection_file | provided (redacted) |
| container | peri-r71-ab4-p2-f1-r2-a1 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD af66931 / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-05T06:38:44.636195+00:00 |
| ended_utc | 2026-11-05T06:38:44.933382+00:00 |
| duration_seconds | 0.297 |
| rounds | 1 |
| warmup | 1 |
| samples | 125 |
| concurrency_requests | 100 |
| resource_before | {'mem_usage': '163.7MiB / 11.73GiB', 'cpu_percent': '1.10%', 'memory_stat': {'anon': 167100416, 'file': 20328448, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 17031168, 'active_file': 3301376, 'slab': 0, 'pgfault': 63070, 'pgmajfault': 16}} |
| resource_after | {'mem_usage': '163.3MiB / 11.73GiB', 'cpu_percent': '1.19%', 'memory_stat': {'anon': 166395904, 'file': 21065728, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 17571840, 'active_file': 3497984, 'slab': 0, 'pgfault': 64342, 'pgmajfault': 18}} |
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
| HEAD af66931 / rust v1.2.7 | F1.select-1 | json-pipeline | 125 | 1.90ms | 4.88ms | 0.61ms | 8.20ms | 1 | 312 | 0.02ms | PASS |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| F1.select-1 | json-pipeline | 1.90ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 125 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
