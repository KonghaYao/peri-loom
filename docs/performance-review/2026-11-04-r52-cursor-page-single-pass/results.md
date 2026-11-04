# R52：只读 cursor 页单趟严格解码与 span 交接

- 分支：`perf/r52-cursor-page-single-pass`，实现提交 `8967ae8`
  （`perf(v1)：只读 cursor 页单趟严格解码与 span 交接（R52）`），基于 `main` `5327333`。
- 证据目录：本目录（`criteria.txt` 于 A/B 前冻结；`r52-ab.sh` / `r52-ab-summary.py`；
  `r52-reg.sh` / `r52-reg-summary.py`；`r52-reg-recheck.sh`；`probes/`；`gate0.txt`）。

## 方案与改动边界

只读 cursor 的每一页在基线路径上要付出：整页 `json.Valid`（20 MB 页体）、结果 span 复制、
gateway 页校验、fused 首页校验、adapter 二次扫描与 compact/escape 重写；这些阶段在 H1 上
合计约 20.7 ms（见 `probes/upper-bound.md`）。

本轮把只读 cursor 页改成单趟严格解码 + span 交接：

- `hrana_cursor_page.go`（新增）：`hranaCursorPageReply` 以 `allowMetadata` + 页 span 为解码结果，
  `UnmarshalJSON` 仅服务测试夹具的回退路径。
- `sql_upstream_codec.go`：`sqlJSONFields` 拆为保持 `json.Valid` 的版本与只做结构定位的
  `sqlJSONFieldSpans`；`sqlDecode` 对 `*hranaCursorPageReply` 走
  「结构定位 span → 信封骨架 `json.Valid`（结果 span 替换为 `{}`）→ 结果 span 严格页扫描」，
  页体零拷贝交给上层（body 是每请求 `io.ReadAll` 的独立缓冲，永不复用）。
- `gateway_sessions.go`：只读 cursor 用 `pageReply`，预算/行数判断读页元数据，首页另用
  `validCursorFirstPage`；返回 `gatewayResult{value: page.source, validatedResult: true,
  cursorPage: &page.page}`。写 cursor 路径未改。
- `hrana_cursor_start.go`：`validCursorFirstPage` 用短缓冲 `{"cols":…}` 复用既有列校验规则。
- `gateway.go` / `gateway_http.go` / `hrana_gateway_response.go` / `hrana_http.go`：页只在
  `hranaCursorPageSameBytes(page.source, result)` 时保留（compact/escape 复制即丢弃）。
- `hrana_v3_json_cursor.go` / `hrana_http_cursor.go`：优先 `hranaCursorPageFromReply(reply)`，
  保留 `pageScanner.scan` 回退。

改动只影响「只读 cursor 页」路径；pipeline、写 cursor、非页回复仍走原解码。

## 上限探针（写生产代码前）

探针二进制 `bf743deb…`，基线路径阶段计时（H1 json 单查询 75.01 ms）：整页 `json.Valid`
8.04 ms、结果复制 1.32、gateway 页校验 2.20、fused 首页 5.61、adapter 二次扫描 2.40、
compact+escape 0.80、`utf8.Valid` 0.32 ⇒ **可移除 20.70 ms（27.6%）**，远高于 7.57 ms
（10%）门槛。结构定位 `sqlJSONFields` 8.70 ms 不属可移除（新路径仍要一次结构扫描）。
详见 `probes/upper-bound.md`。

## Gate 0（先于 A/B）

探针二进制 `f9dff879…`，H1 json-cursor 连测 3 次：每次 **pagedecode=34 次 / 20.22 MB**、
**pagescan=34**（每页恰好一次严格扫描）、**fallback=0**；`json.Valid` 仅作用于小回复
（895–922 次 / 共约 0.12 MB），`skeleton=34`。wire 字节与基线一致
（20,608,525 / 20,272,006）。详见 `gate0.txt`。

## A/B 验收（2026-11-04）

每轮 4 腿 A/B/B/A，每条腿 1 warmup + 5 samples；每轮取该侧两条腿 `median_ms` 的中位数。
基线 `main@5327333`（`peri-platform-r52-base`），候选 `8967ae8`（`peri-platform-r52-cand`），
同一 Rust worker（`peri-loom-v1-r43-cand`），两侧独立 run 目录与全新卷，默认配额，无观测器。

| 场景 | 协议 | 第 1 轮 base→cand | 第 2 轮 base→cand |
|---|---|---|---|
| H1.content-20mb | json-cursor | 75.04 → 62.17（**−17.15%**） | 75.01 → 61.69（**−17.76%**） |
| H1.content-20mb | protobuf-cursor | 71.65 → 57.29（**−20.04%**） | 69.68 → 58.90（**−15.48%**） |
| H3.content-projection | json-cursor | 78.24 → 63.93（−18.29%） | 74.77 → 60.99（−18.43%） |
| H3.content-projection | protobuf-cursor | 69.73 → 58.39（−16.26%） | 69.69 → 59.50（−14.62%） |
| R1.limit-1000 | json-pipeline | 15.11 → 15.46（+2.29%） | 14.41 → 17.53（+21.71%） |
| R1.limit-1000 | protobuf-pipeline | 16.12 → 18.72（+16.12%） | 17.34 → 16.97（−2.12%） |
| F1.select-1 | json-pipeline | 0.57 → 0.58（+2.66%） | 0.62 → 0.61（−2.46%） |
| F1.select-1 | protobuf-pipeline | 0.58 → 0.59（+1.73%） | 0.57 → 0.61（+7.63%） |

主指标判定：H1 json / protobuf 两轮均 ≥10% 改善且同向（4/4），**硬门满足**。
R1/F1 是 pipeline 路径，机制上不经过本轮改动的游标页函数；单轮出现的 +16.12%（pb 第 1 轮）
与 +21.71%（json 第 2 轮）方向不跨轮复现，进入回归门复测。

## 回归门（4 腿 A/B/B/A，5 samples/腿）

| 场景 | 协议 | base 中位 | cand 中位 | 变化 |
|---|---|---:|---:|---:|
| B1.batch-20 | json-pipeline | 2.29 | 2.85 | +24.11% |
| B1.batch-20 | protobuf-pipeline | 4.91 | 2.39 | −51.38% |
| C1.baton-10 | json-pipeline | 0.35 | 0.36 | +4.87% |
| F1.select-1 | json-pipeline | 0.59 | 0.56 | −5.55% |
| F1.select-1 | protobuf-pipeline | 0.60 | 0.61 | +2.52% |
| H3.content-projection | json-cursor | 75.14 | 61.67 | −17.93% |
| H3.content-projection | protobuf-cursor | 69.80 | 58.26 | −16.53% |
| L1.list-20 | json-cursor | 1.05 | 1.02 | −2.66% |
| L1.list-20 | json-pipeline | 0.85 | 0.78 | −8.06% |
| R1.limit-1000 | json-pipeline | 13.49 | 15.48 | +14.74% |
| R1.limit-1000 | protobuf-pipeline | 17.60 | 17.81 | +1.24% |

- **L1 判定不可用（既有夹具偏差，非本轮）**：本 fixture 的最忙 cwd 有 99 个线程，而场景 SQL 为
  `LIMIT 40`，`rows=40` 与 `expected_rows=99` 不符，四腿全部 `row_count_mismatch`（base 与 cand
  完全一致）。R51 回归门的 L1 同样四腿 FAIL。时间数字仅作记录，不参与回归判定。
- **R1 json-pipeline +14.74% 为单腿离群**：base1 14.181 / base4 12.805 vs cand2 18.155 /
  cand3 12.809 —— 候选两腿自相差 5.3 ms，cand3 与 base4 几乎相同，非可复现退化。
- **B1 两个协议方向相反**：json +24.11%（2.29→2.85，绝对 +0.55 ms）、protobuf −51.38%
  （base1 7.482 为离群，base4 2.335）；p95 在两侧重叠（9.2–11.7 ms）。
- 按冻结判据，对 R1 json-pipeline 与 B1 json-pipeline 做高样本复测
  （8 腿 A,B,B,A,A,B,B,A，15 samples/腿），结果见下。

### 高样本复测（R1 / B1 json-pipeline）

8 腿 `A,B,B,A,A,B,B,A`、15 samples/腿，仍为原两栈（19497/19498）：

| 场景 | base 腿中位（4 腿） | cand 腿中位（4 腿） | 变化 |
|---|---:|---:|---:|
| R1.limit-1000 json-pipeline | 14.354 | 16.266 | +13.32% |
| B1.batch-20 json-pipeline | 2.205 | 2.160 | −2.04% |

- **B1 判定为噪声**：首轮 +24.11%（绝对 +0.55 ms）在 15 samples/腿下变成 −2.04%，方向翻转；
  p95 两侧重叠（9.3–16.3 ms）。与 R51 回归门的 −11.22% 同量级抖动，不构成回归。
- **R1 需要归因**：+13.32% 在腿中位口径上仍 >10%。样本呈双峰（11.9–12.5 ms 与 17–26 ms 交替），
  5–15 样本的中位数会在两峰间跳动。合并 60 样本/侧后：base 均值 15.318、cand 16.306（+6.45%），
  ≥17 ms 样本 15/60 vs 23/60。

### R1 归因：全新卷换位与同二进制对照

为区分「二进制效应」与「栈/卷历史效应」，另起全新卷、同 fixture 的栈做换位测量：

| 窗口 | 栈 | 腿数/样本 | base 腿中位 | cand 腿中位 | 变化 | 合并均值变化 |
|---|---|---|---:|---:|---:|---:|
| 换位 1 | 19499 base / 19500 cand（全新卷） | 2 腿 × 15 | 15.221 | 16.222 | +6.58% | +2.2% |
| 换位 2 | 同上 | 4 腿 × 15 | 13.909 | 14.481 | +4.11% | +8.09% |
| 同二进制对照 | 19501 / 19502 均为 base 二进制（全新卷） | 4 腿 × 15 | 17.343 | 15.937 | **−8.11%** | −3.23% |

- **同二进制对照**：两个完全相同的基线二进制、相同全新卷、相同 fixture，腿中位相差 **−8.11%**、
  合并均值 −3.23%。这就是该场景在本机上的栈间方差下限。
- 候选在全新卷上的 +6.58% / +4.11% 落在该范围内；只有长历史栈上的 +13~15% 超过它，但方向与
  幅度随窗口变化（A/B 第 1 轮 +2.29%、第 2 轮 +21.71%）。
- **机制核对**：`git diff main..HEAD` 对 pipeline 路径的全部影响是 `sqlDecode` 中一次
  `output.(*hranaCursorPageReply)` 类型断言与 `capture` 中一次 `page != nil` 检查；pipeline 输出
  类型不是 `hranaCursorPageReply`，不进入新解码分支，也不产生额外分配。机制上不可能解释
  1.9 ms（13%）的差异。

**判定：R1 json-pipeline 不可确认（栈/窗口效应，不构成回归）**。判据要求的「多轮/多腿可复现」
在消除栈历史后不成立，且同二进制对照给出同等量级的反例；其余场景无 >10% 可复现退化。

## 不变量

- wire 字节与基线一致：H1 json 20,608,525、H1 pb 20,272,006、H3 json 20,746,565、
  H3 pb 20,306,526、F1 312/40、C1 300、L1 16,393/15,636、B1 pb 686、R1 pb 2,349,053；
  R1 json 2,369,290（A/B 中一条候选腿为 2,369,300，差 10 字节来自 `query_duration_ms` 位数，
  历史同现象）；B1 json 4,952–4,971（同理）。
- 全部 A/B 与回归样本 `warnings=[]`、无 `fatal`、非 200 = 0；fixture 校验
  `threads=201 messages=8625 content_chars=20177069 focus=8625` 通过。

## 正确性与测试

- 新增 `hrana_cursor_page_decode_test.go`（603 行）：6,000 条合法页差分、8,000 条信封变异、
  5,000 条任意字节、嵌套深度 1–129；`TestCursorPageFirstPageAcceptsOutOfFloat64Numbers`
  覆盖 `validCursorFirstPage` 对超 float64 数字的接受。
- `gofmt` / `go vet` / `go build ./cmd/peri-platform ./cmd/peri` 干净。
- 全量 `go test -count=1 .`：`ok 175.1s`（连续两次）；定向 `-race` 14.0 s 通过；
  无 `platform.test` 残留。

## 配置与工具链

- 平台二进制：base `/tmp/peri-perf/bin/peri-platform-r52-base`
  sha256 `028abd1533b6e1b16506d1460d8998b357d740fcb5c7c1039b0e73adabba6675`；
  cand `peri-platform-r52-cand` sha256 `a54c9b913c7b81a7d48233d4ac1a275c456c565fecc23149a119a34062ba6948`。
- worker：`/tmp/peri-perf/bin/peri-loom-v1-r43-cand`
  sha256 `27d8dd4682128ff7668999254e068144c878b5fa972357d7dd22a5aac50ce3a4`（两侧相同）。
- go 1.27.3 darwin/arm64；rustc 1.98.1；macOS 26.5.1（M4，10 核）。
- 端口 19497（base）/ 19498（cand），无 TLS；配额 `max_concurrent=100`、`max_sessions=16`、
  `requests_per_second=500`、`max_result_bytes=4194304`；durablity 默认（`audit_file_v1`）。
- 观测器：探针只用于上限探针与 Gate 0，均不在 A/B 二进制中；A/B 与回归门无观测器。

## 剩余风险与未测量项

- 只读 cursor 页的严格性由「信封骨架 `json.Valid` + 结果 span 严格页扫描」共同保证；
  写 cursor 路径与 pipeline 路径未改，未对其做新的差分。
- 未跑 X1 并发档（历史上该档不可确认）；未测 c128/容量。
- 真实崩溃恢复未因本轮改动重跑（本轮不改变耐久时点与提交路径）。
- 页体零拷贝依赖「每请求独立缓冲」这一上游不变量，已在代码注释与测试中固定；
  若未来引入缓冲池，必须重新审查该交接。
