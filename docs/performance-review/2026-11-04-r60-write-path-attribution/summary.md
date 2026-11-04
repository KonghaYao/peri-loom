# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| target | {'endpoint': 'http://127.0.0.1:19681', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| python | 3.14.7 |
| platform | macOS-26.5.1-arm64-arm-64bit-Mach-O |
| git_revision | 3671c1c7fe7228a01c2eac8e9cc604a27b62f4ea |
| connection_file | provided (redacted) |
| platform_label | HEAD 3671c1c / rust unknown |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
| started_utc | 2026-11-04T07:44:35.558456+00:00 |
| ended_utc | 2026-11-04T07:44:37.448400+00:00 |
| duration_seconds | 1.89 |
| rounds | 1 |
| warmup | 1 |
| samples | 3 |
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
| HEAD 3671c1c / rust unknown | W1.create | json-pipeline | 3 | 32.01ms | 33.00ms | 31.91ms | 33.00ms | 0 | 253 | 10.31ms | — |
| HEAD 3671c1c / rust unknown | W1.insert | json-pipeline | 3 | 40.70ms | 50.79ms | 38.03ms | 50.79ms | 0 | 252 | 17.04ms | — |
| HEAD 3671c1c / rust unknown | W1.delete | json-pipeline | 3 | 39.91ms | 40.71ms | 38.62ms | 40.71ms | 0 | 262 | 16.35ms | — |
| HEAD 3671c1c / rust unknown | W1.drop | json-pipeline | 3 | 40.91ms | 48.55ms | 38.81ms | 48.55ms | 0 | 253 | 18.14ms | — |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| W1.create | json-pipeline | 32.01ms |
| W1.insert | json-pipeline | 40.70ms |
| W1.delete | json-pipeline | 39.91ms |
| W1.drop | json-pipeline | 40.91ms |

## 备注

- 单流场景：1 轮 × 每轮 1 次 warmup + 3 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 100 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
