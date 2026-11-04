# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:20303', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | a46afa22c07ea4ad694692d533e224aab7594d80 |
| connection_file | provided (redacted) |
| platform_label | HEAD a46afa2 / rust unknown |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-04T09:52:02.603414+00:00 |
| ended_utc | 2026-11-04T09:52:02.633854+00:00 |
| duration_seconds | 0.03 |
| rounds | 1 |
| warmup | 1 |
| samples | 5 |
| concurrency_requests | 50 |
| resource_before | {} |
| resource_after | {} |
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
| HEAD a46afa2 / rust unknown | F1.select-1 | json-pipeline | 5 | 0.52ms | 0.60ms | 0.46ms | 0.60ms | 1 | 312 | 0.01ms | PASS |
| HEAD a46afa2 / rust unknown | F1.select-1 | protobuf-pipeline | 5 | 0.47ms | 0.51ms | 0.46ms | 0.51ms | 1 | 40 | 0.01ms | PASS |
| HEAD a46afa2 / rust unknown | F1.select-1 | json-cursor | 5 | 0.76ms | 0.77ms | 0.68ms | 0.77ms | 1 | 252 | — | PASS |
| HEAD a46afa2 / rust unknown | F1.select-1 | protobuf-cursor | 5 | 0.82ms | 0.87ms | 0.80ms | 0.87ms | 1 | 57 | — | PASS |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| F1.select-1 | json-pipeline | 0.52ms |
| F1.select-1 | protobuf-pipeline | 0.47ms |
| F1.select-1 | json-cursor | 0.76ms |
| F1.select-1 | protobuf-cursor | 0.82ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 5 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 50 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
