# R46 — Hrana fused open 的 statement 答案只解析一次（2026-11-04）

分支 `perf/r46-fused-parse-once`，实现提交 `d79500c`。
基线平台 `/tmp/peri-perf/bin/peri-platform-r46-base`（`8b42d4eeb5f2…`，= `main` @ `bad41e6`），
候选平台 `/tmp/peri-perf/bin/peri-platform-r46-cand`（`27d37e3d6634…`，`d79500c` 干净构建），
两侧同一 worker `peri-loom-v1-r43-cand`（`27d8dd468212…`）。两套独立栈
（`run-r46base` :19498 / `run-r46cand` :19499），各自全新卷并导入同一 fixture：
`threads=201 messages=8625 chars=20177069 focus=8625`。
fixture 是合成数据：R1 = 1,000 行、json 响应体 2,369,290 字节；H1 = 8,625 行 × ~2.34KB、
json 20,608,525 / protobuf 20,272,006 字节。

## 方案

R45 归因显示 R1 json-pipeline 单请求 46.5ms 中 `parseFusedStatementAnswer` 被调用 3 次、
合计 20.0ms（43.1%）：gateway open 校验一次、Hrana 适配器发布前校验一次、dispatch 再解析一次。
适配器那次解析结果（`fusedStatementAnswer`）已足以供 dispatch 使用，本轮把它随 marker 传递：

- `hranaOpenResult.FusedAnswer` / `fusedAnswerValid`：`hranaOpen` 把已解析答案留在结果里；
- `fusedStatementResult.Answer` / `AnswerValid`：marker 携带答案；
- `fusedGuardedStatement` 优先使用 marker 的答案，只有 `!AnswerValid` 时才回退解析 `Reply`；
- `hranaFusedStatementResult` 改为接收 `hranaOpenResult`（两个调用点已更新）。

未改变：gateway open 自己的校验、适配器对非法 payload 的 502、结果预算、写语句 intent 边界、
失败分类与 `outcome_unknown` 语义。回退分支保留，marker 缺答案时行为与基线一致。

## Gate 0

临时计数器（`perf_probe_r46.go`，探针二进制见 `probes/`，测量前已移除）：

| 侧 | fused_dispatch | fused_parse | fused_reuse | fused_fallback | parses/dispatch |
|---|---:|---:|---:|---:|---:|
| base | 4 | 12 | 0 | 0 | 3.0 |
| cand | 4 | 8 | 4 | 0 | 2.0 |

运行方式：同一 fixture，`R1.limit-1000 json-pipeline` 1 warmup + 3 timed = 4 次 fused statement。
候选侧 4/4 走复用、回退 0；解析次数由 3.0 降到 2.0，与机制一致。

## A/B/B/A（R1 json-pipeline，1 warmup + 5 timed/leg，2 轮 × A/B/B/A）

| run | n | median ms | p95 ms | bytes | rows | errors |
|---|---:|---:|---:|---:|---:|---:|
| r1-base1 | 5 | 46.78 | 50.99 | 2369290 | 1000 | 0 |
| r1-cand2 | 5 | 39.69 | 48.02 | 2369290 | 1000 | 0 |
| r1-cand3 | 5 | 40.02 | 46.59 | 2369290 | 1000 | 0 |
| r1-base4 | 5 | 48.67 | 51.50 | 2369290 | 1000 | 0 |
| r2-base1 | 5 | 43.00 | 45.12 | 2369290 | 1000 | 0 |
| r2-cand2 | 5 | 37.49 | 40.96 | 2369290 | 1000 | 0 |
| r2-cand3 | 5 | 38.29 | 49.58 | 2369290 | 1000 | 0 |
| r2-base4 | 5 | 47.75 | 53.26 | 2369300 | 1000 | 0 |

| round | base median ms | cand median ms | delta |
|---|---:|---:|---:|
| 1 | 47.72 | 39.86 | **−16.48%** |
| 2 | 45.37 | 37.89 | **−16.49%** |

四个配对腿全部同向：−15.15% / −17.77%（第 1 轮）、−12.81% / −19.81%（第 2 轮）；
候选最慢腿 40.02ms 仍低于基线最快腿 43.00ms。判据（中位 ≥10%、两轮同向）满足。

## 对照（同一次 A/B 运行）

| 场景 | round | base median | cand median | delta |
|---|---|---:|---:|---:|
| H1 json-cursor | 1 | 123.19 | 121.30 | −1.53% |
| H1 json-cursor | 2 | 119.65 | 123.18 | +2.95% |
| H1 protobuf-cursor | 1 | 103.64 | 100.54 | −2.99% |
| H1 protobuf-cursor | 2 | 100.41 | 99.75 | −0.65% |
| F1 json-pipeline | 1 | 0.61 | 0.57 | −6.56% |
| F1 json-pipeline | 2 | 0.61 | 0.67 | +9.72% |

方向不一致且量级落在噪声内（F1 单腿 0.53–0.73ms），不构成可复现回归；
本轮改动只影响 pipeline 的 fused statement dispatch，cursor 路径无 diff。

## 回归门

每场景 4 条腿 A/B/B/A、1 warmup + 5 timed（`reg-raw/`）：

| 场景 | 协议 | base 中位 ms | cand 中位 ms | delta |
|---|---|---:|---:|---:|
| B1.batch-20 | json-pipeline | 2.52 | 2.64 | +4.87% |
| B1.batch-20 | protobuf-pipeline | 2.69 | 2.76 | +2.57% |
| C1.baton-10 | json-pipeline | 0.35 | 0.35 | −0.13% |
| F1.select-1 | json-pipeline | 0.54 | 0.62 | +14.79%（见下） |
| F1.select-1 | protobuf-pipeline | 0.67 | 0.64 | −4.01% |
| H3.content-projection | json-cursor | 119.13 | 118.80 | −0.27% |
| H3.content-projection | protobuf-cursor | 102.53 | 100.83 | −1.66% |
| L1.list-20 | json-cursor | 1.12 | 1.14 | +1.71% |
| L1.list-20 | json-pipeline | 1.30 | 1.36 | +4.26% |
| X1.concurrency | json-pipeline | — | — | 不可确认 |

- **F1 json 的 +14.79% 不可复现**：同一 A/B 窗口内为 −6.56% / +9.72%（方向相反），
  专门复跑 3 轮 × A/B/B/A（12 条腿，`f1-rerun/`）为 −0.04% / +0.51% / −43.42%
  （最后一轮由 base 单腿 1.734ms 离群驱动），12 腿合并中位 0.595→0.601ms（**+0.93%**）。
  效应量 0.006–0.08ms 小于同侧离散度（0.52–0.67ms 常见），按噪声处理。
- **X1 不可确认**：4 档并发中最高档的腿间中位为 base 30.37/28.70ms、cand 22.68/4.92ms，
  各腿 p95 143–160ms，宿主瞬时抖动主导（与 R45 相同现象）；无 429、无错误码，
  且本轮 diff 不进入并发调度路径，机制上不可能引入方向性退化。
- L1 全部 8 条腿 `row_count_mismatch`（合成 fixture `list_threads=100` 而 SQL `LIMIT 40`），
  两侧相同、与本轮无关，延迟仍可比。
- 所有运行 `status=200`、`error_codes={}`、`warnings=[]`、`fatal=false`；结束时两侧
  `session_seats=0`、`active_permits=0`，动作计数逐项相同（open/close 1085、
  statement 1522、cursor_fetch 2412）。

## 不变量

- wire 字节：R1 2,369,290（`r2-base4` 为 2,369,300，差异来自 `query_duration_ms` 数字位数，
  与 R4/R5 记录一致）；H1 json 20,608,525 / protobuf 20,272,006，与 R45 相同。
- 所有 A/B 与回归运行 `status=200`、`error_codes={}`、`warnings=[]`、`fatal=false`；
  fixture 校验 201/8625/20177069 全部通过。
- `query_ms`（worker SQL 时间）R1 约 2.34ms，两侧不变。

## 测试

- `gofmt -l .` 干净；`go build ./cmd/peri-platform ./cmd/peri`；`go vet ./...` 干净。
- 新增 `TestHranaFusedStatementMarkerCarriesTheParsedAnswer`（marker 携带解析答案）。
- `go test -count=1 .`（`PERI_RUST_BINARY` 指向本机 debug worker）：`ok peri-loom/platform 172.806s`。

## 配置与偏离

- 默认配额：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、
  `requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304`。
- 无观测器参与 A/B；测量期间未并行构建或跑其他重负载。
- 平台二进制为本地 macOS/arm64 构建（Go 1.27.3）；worker 为 linux/arm64 候选二进制
  （Rust 1.98.1），两侧相同。
- 主机（macOS 26.5.1 / arm64 / 10 核）同一窗口内完成全部测量；绝对数值只在本窗口可比。

## 剩余风险与未测量项

- 未在 v2 完成日志（`completion_journal_v2`）卷上复测；本改动与耐久路径无关。
- 未做 protobuf pipeline 的 R1 对照（R1 在 harness 中只声明 json-pipeline）；protobuf 单
  statement 路径（`serveHranaProtoSingleExecute`）同样受益于 marker，但未单独计时。
- 回退分支只在单测中构造（`AnswerValid=false`）验证，未在真实请求中触发（Gate 0 回退计数 0）。
