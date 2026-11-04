# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:20401', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | 271e0962ec12b230bc28aa29774e602aa5a46ed5 |
| connection_file | provided (redacted) |
| container | peri-loom-release-v127-h1-content-20mb-r1a1 |
| image | ghcr.io/konghayao/peri-loom:v1.2.7 |
| peri_image_version | v1.2.7 |
| peri_image_revision | 640ae7e3ce035f1d2b02d9367dc6aa755a018789 |
| platform_label | HEAD 271e096 / rust v1.2.7 |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-04T12:57:46.576792+00:00 |
| ended_utc | 2026-11-04T12:57:48.043689+00:00 |
| duration_seconds | 1.467 |
| rounds | 1 |
| warmup | 1 |
| samples | 3 |
| concurrency_requests | 50 |
| resource_before | {'mem_usage': '162.2MiB / 11.73GiB', 'cpu_percent': '1.06%', 'memory_stat': {'anon': 168169472, 'file': 126468096, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 126468096, 'active_file': 4096, 'slab': 0, 'pgfault': 52785, 'pgmajfault': 0}} |
| resource_after | {'mem_usage': '172.8MiB / 11.73GiB', 'cpu_percent': '1.27%', 'memory_stat': {'anon': 177864704, 'file': 13172736, 'file_dirty': 139264, 'file_writeback': 0, 'inactive_file': 2748416, 'active_file': 10297344, 'slab': 0, 'pgfault': 59947, 'pgmajfault': 136}} |
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
| HEAD 271e096 / rust v1.2.7 | H1.content-20mb | json-cursor | 3 | 160.10ms | 167.08ms | 158.96ms | 167.08ms | 8,625 | 20,608,525 | — | PASS |
| HEAD 271e096 / rust v1.2.7 | H1.content-20mb | protobuf-cursor | 3 | 121.82ms | 123.93ms | 115.37ms | 123.93ms | 8,625 | 20,272,006 | — | PASS (protobuf vs JSON -23.9% (within noise)) |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| H1.content-20mb | json-cursor | 160.10ms |
| H1.content-20mb | protobuf-cursor | 121.82ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 3 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 50 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
