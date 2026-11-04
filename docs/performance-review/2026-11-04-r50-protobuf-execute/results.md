# R50：Hrana protobuf pipeline execute 单遍直转码

- 日期：2026-11-04
- 分支：`perf/r50-protobuf-execute`
- 实现提交：`f73aa1c`（4 文件 +913 / −0）
- 基线：`main` @ `4784413`（R49 文档提交）
- 主指标：`R1.limit-1000` `protobuf-pipeline`（1,000 行 × 1 列，2.35 MB）

## 背景

R49 已把 Hrana **JSON** pipeline 的 execute entry 改成单遍直转码，但 protobuf 单执行
pipeline（`[execute, close]`）仍把 worker 返回的 2.35 MB JSON 结果完整物化：

1. adapter 用 `json.Decoder` 解码成 `hranaNativeStatement`（`[][]any`）；
2. `hranaProtoStatementResultWithin` 转 Hrana 单元格；
3. `hranaProtoPipelineResponseWithin` 再把整个结果编码成 protobuf。

R49 的下一轮提示点名了这条路径，本轮先测上限再实现。

## 上限探针（零代码形状探针）

探针工作树 `/tmp/peri-perf/r50-probe-src` @ `4784413`，平台二进制
`bin/peri-platform-r50-probe` sha256 `418ad3111bbae9b8384bcc945c7640fe2202bc98a853726d1c2221f3a133ba12`，
栈 :19497，R1 protobuf 请求 n=6（含 warmup），探针侧中位：

| 阶段 | 中位 |
|---|---:|
| `hranaOpen`（含 worker 执行与 2.35 MB 融合应答） | 8.61 ms |
| worker `/v1/sql/statement` 调用 | 0.17 ms |
| 解码 worker JSON → `hranaNativeStatement` | 2.99 ms |
| 单元格转换 | 0.07 ms |
| protobuf 编码 `hranaProtoPipelineResponseWithin` | 12.02 ms |
| 响应写出 | 0.33 ms |
| 探针侧合计 | 25.59 ms |

`decode + convert + encode` = **16.36 ms 中位，占客户端中位 26.25 ms 的 64%**；换成单遍
转码的下限是消掉这 16.36 ms，远高于 10% 门槛（即便只兑现 12 ms 也是 ≈46%）。

同一探针顺带确认：R1 的 execute+close 形状走 `serveHranaProtoSingleExecute`，没有
`json.Marshal(input)` 往返；目标就是「物化 + 再编码」，不是 JSON 入参。

## 实现（`f73aa1c`）

新增 `prototypes/v1/platform/hrana_protobuf_execute.go`：

- `encodeHranaProtoStatementResult(result json.RawMessage, budget int)
  (hranaEncodedStatementResult, hranaProtoEncodeStatus)`：复用 R48 的
  `fusedReplyScanner` 单遍严格扫描 worker 结果 span，直接产出 protobuf 结果字节。
  状态为 `complete` / `unsupported` / `overBudget`；重复已知键、大小写变体、缺失或为
  null 的 `cols`/`rows` 一律回退。
- `assembleHranaProtoSingleExecute(result []byte) []byte`：按 pipeline
  `{3: {1: {2: {1: result}}}, 3: {1: {1: {1: (empty)}}}}` 拼装 execute + close 两 entry
  （close entry 为 `1a040a020a00`，16 bit）。
- 逐行预算计费（`resultLimit`），超预算返回 `hranaProtoEncodeOverBudget`。

**刻意复刻的旧路径怪癖**：旧 `map[string]any` 路径把 `decltype` 与 `last_insert_rowid`
存成 `*string`，而旧编码器的 `string` 类型断言不匹配指针，因此这两个字段**只校验、
不输出**；列只输出 `{1: name}`。此点在代码注释中说明，否则差分测试会失败（首轮失败即
`direct=…1207494e5445474552…` vs `legacy=…`）。

`hrana_http.go`（+10）：`executeWithin` 在 `hranaProtoExecuteRequested(ctx)` 且融合标记
命中（session/SQL 与 gateway 检查一致）时尝试直转码；complete 返回空 reply 的
`hranaEncodedStatementResult`，over-budget 走既有 413 `pipeline_result_limit`。

`hrana_protobuf.go`（+14）：`serveHranaProtoSingleExecute` 给语句请求挂
`withHranaProtoExecute`，命中直转码时写 `assembleHranaProtoSingleExecute`，再走
`writeHranaProtoResponse`。只影响 HTTP protobuf 单执行 pipeline；JSON、cursor、WS、
公开 API 均不变。

测试 `hrana_protobuf_execute_test.go`：与旧路径复刻实现（`legacyHranaProtoStatement` /
`legacyHranaProtoSingleExecute` / `directHranaProtoSingleExecute`）差分对拍，覆盖
cols/rows/affected/metrics 变体、`-0.0`（用 `math.Copysign(0,-1)` 构造，Go 常量会把
`-0.0` 折叠成 `+0`）、20 MB 级大载荷、预算边界、拼装一致性。

## Gate 0（机制核验，测前移除探针）

平台二进制 `bin/peri-platform-r50-gate0` sha256
`7cb020cbd5b752e9d8e97a983e4958eb088c6fba12980893477f96bde919ea9e`（`f73aa1c` + 探针），
栈 :19496，R1 protobuf 4 个请求（1 warmup + 3）：

```
r50gate attempt=4 complete=4 limit=0 unsupported=0 equal=4 mismatch=0 legacy_bad=0 detail=2349027
```

4/4 请求都命中直转码，且与旧路径（decode → `hranaProtoStatementResultWithin` 编码）
**逐字节一致**，2,349,027 字节。该探针栈的 smoke 中位 20.76 ms **不是性能数字**
（Gate 0 每请求额外跑一遍旧路径编码）。

## 配对 A/B

两侧独立平台二进制、各自全新同数据卷、同一 fixture、同一 worker（`bin/peri-loom-v1-r43-cand`）：

- base `bin/peri-platform-r50-base` sha256
  `ad327012752f415ea051cc4f955fa81aa9978af31c31e35c842bb1fca6816a73`（`4784413`）
- cand `bin/peri-platform-r50-cand` sha256
  `522bf5ef5b79b34720fd7911a659f405b139c3aeddbe0d7b6b1c9c9b3a85546b`（`f73aa1c`）

### 第 1 批：2 轮 × A/B/B/A（每腿 1 warmup + 5 样本）

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 20.285 / 19.967 | 16.678 / 17.592 | **20.13→17.13（−14.86%）** |
| 2 | 25.056 / 20.688 | 18.551 / 14.732 | **22.87→16.64（−27.24%）** |

配对 −17.78% / −11.89% / −25.96% / −28.79%（4/4 同向），4 腿整体中位
**20.49→17.13ms（−16.36%）**。两侧响应均 2,349,053 字节、1,000 行、仅 200。

### 第 2 批：3 轮 × A/B/B/A 复核（json 7 样本/腿，protobuf 5 样本/腿）

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 24.988 / 16.659 | 14.229 / 16.310 | **20.82→15.27（−26.67%）** |
| 2 | 19.103 / 18.642 | 15.771 / 14.754 | **18.87→15.26（−19.13%）** |
| 3 | 20.776 / 25.845 | 13.463 / 16.572 | **23.31→15.02（−35.58%）** |

全体 6 腿中位 **19.94→15.26ms（−23.46%）**，三轮同向。合计 5 轮全部 ≥14%，满足
「主指标中位 ≥10%、≥2 轮同向」的冻结门槛。

### 对照（同窗口）

| 场景 | 协议 | 第 1 批 | 第 2 批 |
|---|---|---|---|
| H1.content-20mb | protobuf-cursor | +1.36% / +1.57% | — |
| H1.content-20mb | json-cursor | −1.65% / −0.50% | — |
| F1.select-1 | json-pipeline | +8.33% / +9.46%（亚毫秒） | — |
| F1.select-1 | protobuf-pipeline | −15.23% / −7.44%（亚毫秒） | — |
| R1.limit-1000 | json-pipeline | +17.98% / +2.13% | −2.22% / −0.06% / −20.10% |

R1 json 第 1 批聚合 +12.56%，逐腿可见一个离群爬坡腿（样本
13.23/19.50/20.61/17.07/21.07/17.33 的 warm-up/GC 伪影）；第 2 批 3 轮复核聚合
**−11.09%**，逐轮 −2.22% / −0.06% / −20.10%。R50 不触及 JSON 路径，判定该“退化”为
噪声，对照无回归。

## 回归门（10 场景 × 4 腿 A/B/B/A）

| 场景 | 协议 | base 中位 | cand 中位 | 变化 |
|---|---|---:|---:|---:|
| B1.batch-20 | json-pipeline | 2.32 | 2.28 | −1.47% |
| B1.batch-20 | protobuf-pipeline | 2.68 | 5.73 | +113.94%（单腿瞬态，见下） |
| C1.baton-10 | json-pipeline | 0.37 | 0.34 | −8.25% |
| F1.select-1 | json-pipeline | 0.60 | 0.63 | +5.37% |
| F1.select-1 | protobuf-pipeline | 0.66 | 0.62 | −5.23% |
| H3.content-projection | json-cursor | 121.03 | 119.83 | −0.99% |
| H3.content-projection | protobuf-cursor | 98.89 | 97.74 | −1.16% |
| L1.list-20 | json-cursor | 1.21 | 1.13 | −6.71% |
| L1.list-20 | json-pipeline | 0.79 | 0.91 | +16.13%（单腿瞬态，见下） |
| X1.concurrency | json-pipeline | 不可确认（见下） | | |

`warnings=[]`、`fatal=[]`、非 200=0。

**复核**（每腿 15 样本，4 腿 A/B/B/A）：

| 场景 | 协议 | base 两腿中位 | cand 两腿中位 | 变化 |
|---|---|---:|---:|---:|
| B1.batch-20 | protobuf-pipeline | 2.553 / 2.378 | 2.307 / 2.284 | **−6.9%** |
| L1.list-20 | json-pipeline | 0.753 / 0.765 | 0.709 / 0.747 | **−4.1%** |

两处“退化”均来自单腿里的 9–16 ms 宿主瞬态（同侧 p95 也在 9–16 ms），高样本复核不成立。
X1 各档（c1/c4/c8/c16）两侧样本交错且 c16 在 6.22–24.30 ms 间跳动，判定不可确认，
与 R46/R48/R49 一致。L1 两侧均 `row_count_mismatch`：fixture 的 `list_threads=100`
（当前 cwd）大于 SQL `LIMIT 40`，属 fixture 项，与候选无关。

## 验证

- `gofmt -l .` 干净；`go vet ./...` 干净；`go build ./cmd/peri-platform ./cmd/peri` 通过。
- 定向：`go test -run 'TestHranaProto' -count=1 .` → `ok peri-loom/platform 0.780s`。
- 全量：`PERI_RUST_BINARY=…/target/debug/peri-loom-v1 go test -count=1 .` →
  `ok peri-loom/platform 174.556s`。
- fixture（两侧一致，harness 自检）：threads 201、messages 8,625、content_chars
  20,177,069、focus_messages 8,625、quotas max_sessions 16 / max_concurrent 100 /
  max_result_bytes 4,194,304。

## 结论

合入 `main`：主指标 5 轮同向、最差一轮 −14.86%，4 腿整体 −16.36%、第 2 批 −23.46%；
对照与回归门无可复现 >10% 退化，字节与语义经 Gate 0 逐字节对拍。

## 下一轮提示

R1 的两条 pipeline 路径（JSON R49、protobuf R50）都已不再物化/二次编码。剩余成本是
worker 固定开销与响应写出。可继续评估：非融合路径（公开 open、cursor）复用 R49/R50
编码器；C2 对非融合路径的 `validStatementResult` span 校验。任何上限必须在当轮候选上
重新探针。

## 证据

- `gate0.txt`：Gate 0 输出（`/tmp/peri-perf/r50-gate0.log`）
- `probes/`：上限探针与 Gate 0 探针源码、对 `dev.py`/`hrana_http.go`/`hrana_protobuf.go`
  的注入 diff
- `ab-raw/`：两批 A/B 的 `summary.json` 与 `raw.jsonl`
- `reg-raw/`：回归门与复核的原始输出
- 运行脚本：`r50-ab.sh`、`r50-ab2.sh`、`r50-ab-summary.py`、`r50-reg.sh`、
  `r50-reg2.sh`、`r50-reg-summary.py`
- 原始日志仍在 `/tmp/peri-perf/`（非持久）
