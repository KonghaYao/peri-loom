# Serverless DB 压测方法独立复核

复核时间：2026-10-03。范围：仓库现有脚本、服务端契约、本机工具可用性；本复核未启动服务、未发起负载。运行结果应以压测执行记录为准。

## 关键发现

| 项目 | 仓库证据 | 对结果的影响 / 建议 |
| --- | --- | --- |
| 现有 k6 工作负载 | `deploy/k6/hot-query.js` 发 `POST /data/v1/databases/{id}/query`，请求体为 `{"sql":"SELECT 1"}`；`services/db-server/src/api/data/query.rs` 确实将它交给执行器。 | 是一次真实 SQL 执行请求，但不访问用户表、索引或数据页，不能称为 Indexed Point Read。若测索引点查，应先建表、填充数据、建索引，使用绑定参数查询并核验返回行。 |
| 延迟口径混用 | k6 `platform_latency_ms` 取 `res.timings.duration`；架构 §16 分别列出平台附加延迟 3/8 ms 和 Indexed Point Read 端到端 10/25 ms。`scripts/acceptance.sh` 把该 k6 客户端 HTTP 耗时与 3/8 ms 比较。 | k6 值含网络、认证、路由、SQL 执行、序列化，不能证明或否定平台附加延迟。报告中应命名为“HTTP SQL 请求端到端延迟”。平台附加延迟需要明确埋点边界后单独采样。`request_latency_micros` 虽有定义，但全仓搜索未见生产调用点；`query_latency_micros` 来自 Worker dispatch，也不是平台附加延迟。 |
| 目标环境不匹配 | 架构 §16 的 `>= 10,000 request/s` 写明“单 Server（8 vCPU）”；本机报告 18 CPU、48 GiB、Apple Silicon，且 Simple 模式与 distributed 路径不同。 | 本机结果只能标作该机器、该部署模式的观测吞吐，不能直接判定 8 vCPU distributed 契约。应记录 CPU/内存限制、服务拓扑、负载机位置和版本，再与目标比较。 |
| 错误判定偏弱 | k6 仅检查 HTTP 200 和正文不含字面 `"error"`，未解析 `rows`/`columns`、未核验查询值；脚本 `handleSummary` 缺指标时填 0。 | 200 但结果错误、空结果或响应格式错可能算成功；缺失 p95/p99/error 指标会被 0 掩盖。建议验证 JSON `rows` 的预期值和 `request_id`，对缺失指标直接判实验无效。NDJSON 流要逐帧检查尾帧与错误帧。 |
| 脚本退出状态 | `scripts/acceptance.sh` 中 `k6 ... || true` 忽略 k6 非零退出，随后只依赖解析出的数值。 | k6 阈值失败或负载执行异常不能单凭脚本状态识别；应保存原始 stdout/stderr 和 k6 退出码，缺数据时记“未测”而非达标。 |
| 负载形状 | k6 使用 `constant-vus`，默认 64 VU、30 秒；验收脚本传 32 VU、20 秒。 | 闭环模型在服务变慢时自动降低请求到达率，适合观察该并发下性能，不适合证明固定到达率容量。容量测试建议分档固定到达率并同时观察响应时间、成功吞吐和积压。20 秒也不足以称为持续吞吐。 |
| API 与认证 | `/healthz` 是进程存活；`/readyz` 检查元数据与模式相关就绪条件。`/query` 要 `db:write` 权限。Simple `POST /api/v1/auth/login` 可用初始管理员凭据取得 Token（`services/db-server/tests/simple_http.rs`）。 | 开跑前须以 `/readyz` 200、数据库实际 SQL 查询成功作为就绪证据，不能只看 `/healthz`。不要把凭据或响应含敏感数据的原始正文写入报告。 |
| 结果集与写入语义 | 查询响应含 `rows`、`affected_rows`、`elapsed_micros`、`wal_lsn`；`Accept: application/x-ndjson` 可流式返回。 | `elapsed_micros` 是 Worker 执行耗时，并非端到端耗时。写负载要用唯一键和读回校验，区分成功提交、HTTP 错误、数据丢失与重复写入；流式负载不能只检查 HTTP 200。 |

## 本机只读调查

- `docker` 客户端 29.4.2、Compose v5.1.3；`docker info` 在调查时未及时返回，因此不能确认 Docker daemon 可用。未操作现有容器。
- 可见 `node`、`npm`、`bun`、`cargo`、`rustc`、`curl`、`hyperfine`、`ab`；`k6`、`wrk`、`vegeta`、`hey` 未在 PATH 中。`ab` 不能方便地逐请求校验 SQL 响应语义。
- `127.0.0.1:18081` 运行着已有 Simple 开发实例，只读探测 `/healthz` 与 `/readyz` 均返回 200。它使用仓库 `data` 目录；若用于负载实验，其已有数据、debug 构建和其他本机任务会影响数字，应单独标注，不应宣称隔离生产性能。
- 本机报告 `hw.ncpu=18`、`hw.memsize=51539607552`、`arm64`。这些是主机规格，不代表进程可用资源；Docker/容器限额须另测。

## 可复现 SQL API 预检

在**隔离的数据目录和服务实例**上运行；以下是 API 契约示例，`BASE_URL`、`DB_ID`、`TOKEN` 由实际实验环境提供：

```sh
curl -fsS "$BASE_URL/readyz"
curl -fsS -X POST "$BASE_URL/data/v1/databases/$DB_ID/query" \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sql":"SELECT 1 AS probe"}'
```

要求 HTTP 200 且 JSON `rows` 中有数值 `1`，并检查 `request_id`。热路径应先用同一数据库和同一查询预热，记录预热次数；索引点查需另建测试表、索引及固定规模数据，按真实主键或索引键查询。每个正式实验应保存工作负载代码版本、请求数、成功数、错误分类、P50/P95/P99、成功 RPS、CPU/RSS、测试时长、服务模式和负载源配置。冷启动测试另设场景并从停止/冷状态开始，不能混入热查询分位数。

## 推荐的报告判读

将 Simple 与 distributed 分表，热读、索引点读、写入、冷启动分别列行。把现有 k6 的 `SELECT 1` 结果标为“简单 SQL 请求端到端”。`>= 10,000 RPS`、3/8 ms、10/25 ms 仅在对应架构场景和资源约束满足时判达标；其余场景报告测量值与条件，不作跨场景合格结论。所有“未运行/未采集”字段填 `—` 并给原因，不能以 0 代替。
