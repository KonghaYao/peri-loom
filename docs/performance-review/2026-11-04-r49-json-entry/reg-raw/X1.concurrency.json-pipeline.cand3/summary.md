# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:19495', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | c5d2997f123c2fecddbc4317be578ecdab21b52f |
| connection_file | provided (redacted) |
| platform_label | HEAD c5d2997 / rust unknown |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-04T03:47:17.957217+00:00 |
| ended_utc | 2026-11-04T03:47:19.085535+00:00 |
| duration_seconds | 1.128 |
| rounds | 1 |
| warmup | 1 |
| samples | 5 |
| concurrency_requests | 100 |
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

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|

## 并发

| 并发 | 请求 | 中位数 | p95 | p99 | 429 | 错误 | active_permits | unknown_held_permits | session_seats | 排空 | 结果 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 100 | 0.49ms | 7.70ms | 14.90ms | 0 | 0 | 0 | 0 | 0 | 2.54ms | PASS |
| 4 | 100 | 0.60ms | 107.86ms | 145.74ms | 0 | 0 | 0 | 0 | 0 | 4.03ms | PASS |
| 8 | 100 | 3.01ms | 143.45ms | 151.54ms | 0 | 0 | 0 | 0 | 0 | 9.13ms | PASS |
| 16 | 100 | 5.75ms | 158.55ms | 164.80ms | 0 | 0 | 0 | 0 | 0 | 6.65ms | PASS |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 5 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
