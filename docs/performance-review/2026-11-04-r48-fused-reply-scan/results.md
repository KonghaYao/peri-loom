# R48 — 融合 open 应答改为单遍扫描校验（2026-11-04）

分支 `perf/r48-statement-scan`，实现提交 `1b44553`。
基线平台 `/tmp/peri-perf/bin/peri-platform-r48-base`（`d61d3b70558b…`，= `main` @ `a167ace`），
候选平台 `/tmp/peri-perf/bin/peri-platform-r48-cand`（`ac9137985688…`，`1b44553` 干净构建），
两侧同一 worker `peri-loom-v1-r43-cand`（`27d8dd46…`）。两套独立栈
（`run-r48base` :19496 / `run-r48cand` :19495），各自全新卷并导入同一 fixture：
`threads=201 messages=8625 chars=20177069 focus=8625`。

R1 = 1,000 行、json 响应体 2,369,290 字节（个别 2,369,300，10 字节差来自
`query_duration_ms` 的数字位数）；H1 = json 20,608,525 / protobuf 20,272,006 字节。

## 归因与上限探针

基线 `a167ace` 上对同一份 2.37MB 语句回答做计时探针（n=18 个 R1 请求，测量前已移除）：

| 阶段 | 中位 |
|---|---:|
| `parseFusedStatementAnswer`（信封解码 + 校验） | **6.33ms** |
| ↳ 其中 `validStatementResult`（map + `[][]RawMessage`） | 3.50ms |
| adapter 解码为 `hranaNativeStatement` | 2.93ms |
| 被丢弃的结果预算 `json.Marshal` | 1.43ms |
| 最终 `json.Marshal(response)` | 6.85ms |
| compact + 行分隔扫描 | 0.09ms |
| 单遍扫描下界（`json.Compact` 代理） | **0.92ms** |

D2（本轮的融合应答扫描）上限 = 6.33 − 0.92 = **5.41ms/请求（20.8%）**；
D1（adapter 直转码）上限 **10.29ms**。基线 smoke：`R1.limit-1000 json-pipeline`
中位 **26.06ms**（n=15），环境健康。

## 方案

`parseFusedStatementAnswer` 原本把应答整体 `json.Unmarshal` 进信封结构，再对
`result` 做一次 `validStatementResult`（顶层 map + `[][]RawMessage`），产生多次完整遍历与
大量分配。改为手写严格 JSON 扫描器：

- `fused_reply_scan.go`：单遍扫描应答，直接在原始字节上定位信封字段与 `result` span，
  零拷贝返回 `answer.result`（其后不得修改 reply 字节）；10,000 层嵌套上限；SWAR 8 字节
  字符串扫描；非 ASCII/转义键回退 `json.Unmarshal` + `strings.EqualFold`，与
  encoding/json 的 simple-fold 语义一致（`ſtatus` 命中、`Ktatus` 不命中）。
- `fused_statement_scan.go`：等价复刻 `validStatementResult` 的判定（cols/rows 结构、
  单元格 `{"type":…,"value":…}` 形状、重复键 last-wins、`rows:[null]` 零宽行、
  autocommit 仅接受裸 true/false、数值字段走 `json.Unmarshal` 以保留 Go 精确语义）。
- 语义边界：`result` 缺省为 nil、`result:null` 为 `"null"`；尾随内容拒绝；未知信封键拒绝；
  `validStatementResult` 保持原样作为权威与测试 oracle。
- 只影响融合 open 内部路径；公开 `/v1/sql/open`、cursor、websocket 的响应字节不变。

## Gate 0

探针二进制 `0e3bffd51bac…`（`1b44553` + 调用计数与计时；测前已移除），栈 `run-r48gate0`
:19498，2 rounds × (1 warmup + 5 timed) = 12 个 R1 请求：

| 指标 | 结果 |
|---|---|
| `scan_calls` | 1→12（**1.0/请求**） |
| `scan_ok` | 每次 1 |
| `legacy_calls`（旧校验器） | **全程 0** |
| `scan_ns` | 0.538–0.788ms |
| `reply_bytes` | 2,345,209–2,345,220 |

新路径命中、旧校验器离开请求路径、扫描成本 ≈0.55ms 对基线 6.33ms。机制确认。
（该栈上 smoke 中位 24.62ms，n=10。）

## 配对 A/B

2 轮 × A/B/B/A，1 warmup + 5 timed/leg，主指标 `R1.limit-1000 json-pipeline`。

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 25.864 / 30.615 | 27.908 / 19.823 | **28.24→23.87（−15.49%）** |
| 2 | 31.581 / 29.531 | 22.281 / 27.008 | **30.56→24.65（−19.35%）** |

逐对配对：+7.91% / −35.25% / −29.45% / −8.54%（3/4 同向）；全体 4 腿中位
**30.07→24.64ms（−18.05%）**。实测降幅 ≈5.4ms 与上限探针的 5.41ms 一致。
每 run rows=1,000、响应体 2,369,290 字节、`warnings=[]`、0 非 200。

**对照**（同窗口）：H1 json-cursor −1.13% / −3.76%、H1 protobuf-cursor −1.15% / +0.29%、
F1 json-pipeline −3.48% / +13.94%（亚毫秒场景，绝对差 ≈0.07ms，方向不一致）。

## 回归门

4 腿 A/B/B/A（1 warmup + 5 timed/leg）：

| 场景 | base 中位 | cand 中位 | 变化 |
|---|---:|---:|---:|
| B1 json-pipeline | 2.61ms | 2.30ms | −11.96% |
| B1 protobuf-pipeline | 2.25ms | 2.70ms | **+19.92%**（复测，见下） |
| C1 baton-10 json-pipeline | 0.36ms | 0.35ms | −2.81% |
| F1 json-pipeline | 0.57ms | 0.65ms | +12.98%（亚毫秒） |
| F1 protobuf-pipeline | 0.58ms | 0.64ms | +9.22%（亚毫秒） |
| H3 json-cursor | 122.86ms | 121.24ms | −1.31% |
| H3 protobuf-cursor | 100.28ms | 99.12ms | −1.16% |
| L1 json / json-cursor | 两侧均 `row_count_mismatch`（fixture `list_threads=100` 对 SQL `LIMIT 40`） | — | 不作为判据 |
| X1 4 级并发 | 0.52/0.79/3.26/5.25 与 0.49/0.64/2.37/2.83 | 0.53/0.80/2.39/3.82 与 0.49/0.96/2.43/6.40 | 宿主瞬态主导，记「不可确认」 |

B1 protobuf 单轮 +19.92% 按协议追加 2 轮 × A/B/B/A × 8 采样（`b1-rerun/`）：
protobuf r1 −10.52% / r2 +15.80%，json r1 −0.90% / r2 +13.22%；逐腿看，慢值集中在
单个候选腿（3.139ms / 2.709ms），其余候选腿（2.20–2.376ms）不高于基线腿（2.18–2.79ms）。
合并回归门与复测的 6 腿池化中位：protobuf base 2.332 vs cand 2.369（**+1.6%**）、
json base 2.238 vs cand 2.278（**+1.8%**），方向不一致、量级在噪声内。B1 只有一行
`SELECT 1`，且本轮改动只让融合应答解析更快，不存在可解释的回归机制 ⇒ 判为噪声。
全程 `warnings=[]`、`fatal=[]`、非 200=0。

## 正确性

- 差分测试：约 130 个手写用例 + 5 个真实 R1 变体（原始 2,345,185 字节、rowid 改写、
  duration null、metrics 移除、截断）+ **3,500 个确定性变异**（种子 0x48）在
  `(ok, kind, result bytes, reason, message, executionCompleted)` 上与旧实现完全一致；
  另做一次 20,000 样本（种子 0x1234）抽样对拍，同样一致。
- 微基准（真实载荷）：新实现 **0.52–0.56ms**、4 次分配、39–40 B/op；旧实现
  **6.5–7.7ms**、2,076 次分配、15,868,288 B/op（≈12×，分配 −99.8%）。
- `gofmt -l .` 干净；`go vet ./...`、`go build ./cmd/peri-platform ./cmd/peri` 通过；
  定向测试 `-run 'TestHrana|TestFused|TestGateway|TestStatement|TestScanFused'` 通过。

## 证据

- `gate0.txt`：Gate 0 探针日志；`probe-base.log`：基线计时探针日志。
- `probes/`：`perf_probe_r48.go` 与探针 diff（含 `dev.py` 环境白名单补丁）。
- `ab-raw/`：2 轮 × A/B/B/A 的主指标与对照逐样本数据；`reg-raw/`：回归门逐样本；
  `b1-rerun/`：B1 复测逐样本。
- 运行与汇总脚本：`r48-ab.sh`、`r48-ab-summary.py`、`r48-reg.sh`、`r48-reg-summary.py`、
  `r48-b1-rerun.sh`。
- 原始日志仍在 `/tmp/peri-perf/`（非持久）。
