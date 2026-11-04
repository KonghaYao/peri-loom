# Issue 28 纯 peri-loom 性能测试结果

## 环境与数据

| 项 | 值 |
|---|---|
| scenario | F1.select-1 |
| protocol | json-pipeline |
| rounds | 1 |
| warmup | 0 |
| samples | 50 |
| concurrency_requests | 50 |
| started_utc | 2026-11-04T11:55:34.418135+00:00 |
| ended_utc | 2026-11-04T11:55:34.600168+00:00 |
| duration_seconds | 0.182 |
| git_revision | 39e8e6cf9b41fbb32190c0b414a77edbe17e6535 |
| platform_binary | /tmp/peri-perf/bin/peri-platform-r66-main |
| platform_sha256 | 44448ed6323ec3eb08c04863f8eef68c6c048b9c5d3ff2f4a7569f09efbff104 |
| worker_binary | /tmp/peri-perf/bin/peri-loom-v1-r58-main |
| worker_sha256 | a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594 |
| target | {'endpoint': 'http://127.0.0.1:20601', 'database': {'namespace': {'tenant': 'local_default', 'database': 'demo'}, 'branch': 'main'}, 'tls': False} |
| quota_max_sessions | 16 |
| quota_max_concurrent | 100 |
| quota_requests_per_second | 500 |
| quota_max_result_bytes | 4194304 |
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
| — | F1.select-1 | json-pipeline | 50 | 0.55ms | 8.52ms | 0.47ms | 16.79ms | 1 | 312 | 0.01ms | PASS |

## 协议轮次中位数

| 场景 | 协议 | 各轮中位数 |
|---|---|---|
| F1.select-1 | json-pipeline | 0.55ms |

## 备注

- 单流场景：1 轮 × 每轮 0 次 warmup + 50 次计时；表中统计各场景实际计时样本数。
- 并发场景每个并发级别 50 次，一次运行；warmup 不计入。
- 所有请求带唯一 `X-Request-ID`；SQL 请求不自动重试。
- `SQL-only` 来自响应中的 `query_duration_ms`；cursor 无该指标，显示 `—`。
- R1 的 `result_limit` 是结果预算拒绝，属于预期行为，不计为错误。
- X1 运行配额：`max_concurrent=100`。
- 当前快照的 `projection` 基本为 NULL；H2/H3 不能代表真实投影载荷。
