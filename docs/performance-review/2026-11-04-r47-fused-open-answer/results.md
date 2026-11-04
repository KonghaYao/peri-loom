# R47 — 融合 open 的答案解析跨进程内边界直传（2026-11-04）

分支 `perf/r47-fused-open-answer`，实现提交 `e4b6606`。
基线平台 `/tmp/peri-perf/bin/peri-platform-r47-base`（`d170405fca4f…`，= `main` @ `d05f314`），
候选平台 `/tmp/peri-perf/bin/peri-platform-r47-cand`（`fdb914cf…`，`e4b6606` 干净构建），
两侧同一 worker `peri-loom-v1-r43-cand`（`27d8dd46…`）。两套独立栈
（`run-r47base` :19494 / `run-r47cand` :19495），各自全新卷并导入同一 fixture：
`threads=201 messages=8625 chars=20177069 focus_thread=issue28-focus`。
fixture 是合成数据：R1 = 1,000 行、json 响应体 2,369,290 字节
（个别样本 2,369,300，差 10 字节来自 `query_duration_ms` 的数字位数，已用同请求体 diff 核对）；
H1 = 8,625 行 × ~2.34KB，json 20,608,525 / protobuf 20,272,006 字节。

## 归因与上限探针

R47 开始前，R1 json-pipeline 单请求在进程内对同一份 2.37MB 语句回答完整物化多次。
计时探针（探针二进制见 `probes/`，测量前已移除）在 base 上的每调用均值：

| 阶段 | 每调用 | 每请求 |
|---|---:|---:|
| `parseFusedStatementAnswer`（解码回复 + 校验） | 6.39ms | 2 次 = 12.8ms |
| ↳ 其中 `validStatementResult` | 3.53ms | 2 次 = 7.06ms |
| adapter 解码内部 open 信封 | — | 2.11ms |
| execute 结果解码为 `hranaNativeStatement` | — | 2.99ms |
| 逐单元 `hranaOutputValue` | — | 0.07ms |
| 被丢弃的结果预算 `json.Marshal` | — | ~1.1ms |
| 最终 `json.Marshal(response)` | — | 3.4–5.2ms |

可移除的两段是 adaptor 的解码（2.11ms）+ 第二次完整解析校验（6.39ms）≈ **8.5ms/请求（21%）**，
尚未计入 gateway 把答案编码进内部信封的成本。上限高于 10% 门槛，值得实现。

## 方案

网关在 open 阶段已经把 worker 的语句回答解析并校验过一次；随后它把答案编码进内部信封，
adapter 立刻解码同一份信封并再次完整解析校验——同进程、同一请求、同一份字节。

- `hranaOpenReply{session, fused_statement, fused_close, answer, answerValid}`：
  网关已解析并校验过的融合 open 答案；
- `gatewayResult.hranaOpen` → `hranaGatewayResponse.captureOpen`：进程内 recorder 直接收下
  这个答案，不再编码 2.4MB 信封（与既有 `capture` 快速路径同一形态）；
- `hranaReply.open` → adapter 的 `hranaOpenFromGateway`：直接用答案，跳过
  `json.Unmarshal(reply.result, &opened)` 与第二次 `parseFusedStatementAnswer`；
- 只对携带融合标记的 pipeline open 生效（`fusedOpenStatementFrom(ctx)`）；cursor、
  websocket 与公开 open 仍走信封编码，公开响应字节不变；
- adapter 保留三条拒绝规则：未请求融合却收到语句、有 `fused_close` 无语句、缺少
  `AnswerValid` 校验证明。网关的校验边界（`parseFusedStatementAnswer` + 失败即
  `outcome_unknown`）没有移动，只是不再重复。

未改变：结果预算、写语句 intent 边界、失败分类与 `outcome_unknown` 语义、审计与结算顺序、
公开 `/v1/sql/open` 的响应形状。

## Gate 0

探针二进制（测量前已移除）：base `9f62c6a3e025…`（`d05f314` + 计时探针）、
cand `afba7370…`（`e4b6606` + 同一探针）。同一 fixture，`R1.limit-1000 json-pipeline`
3 rounds × (1 warmup + 5 timed) = 18 请求：

| 侧 | fused_parse | validate | open_unmarshal | typed_open | decode |
|---|---:|---:|---:|---:|---:|
| base（次/请求） | 2.0 | 2.0 | 1.0 | 0.0 | 1.0 |
| cand（次/请求） | 1.0 | 1.0 | 0.0 | 1.0 | 1.0 |

新路径命中 1.0/请求、重复解析与校验消失、信封解码归零，与机制一致。

## A/B/B/A（R1 json-pipeline，1 warmup + 5 timed/leg，2 轮 × A/B/B/A）

| run | n | median ms | p95 ms | bytes | rows | errors |
|---|---:|---:|---:|---:|---:|---:|
| r1-base1 | 5 | 37.99 | 41.93 | 2369290 | 1000 | 0 |
| r1-cand2 | 5 | 24.24 | 26.46 | 2369300 | 1000 | 0 |
| r1-cand3 | 5 | 32.12 | 34.95 | 2369290 | 1000 | 0 |
| r1-base4 | 5 | 42.21 | 46.95 | 2369290 | 1000 | 0 |
| r2-base1 | 5 | 36.20 | 37.72 | 2369290 | 1000 | 0 |
| r2-cand2 | 5 | 24.85 | 26.48 | 2369290 | 1000 | 0 |
| r2-cand3 | 5 | 27.66 | 29.41 | 2369290 | 1000 | 0 |
| r2-base4 | 5 | 38.11 | 40.76 | 2369290 | 1000 | 0 |

| round | base median ms | cand median ms | delta |
|---|---:|---:|---:|
| 1 | 40.10 | 28.18 | **−29.73%** |
| 2 | 37.15 | 26.25 | **−29.34%** |

四个配对腿全部同向：−36.21% / −23.90%（第 1 轮）、−31.35% / −27.42%（第 2 轮）；
候选最慢腿 32.12ms 低于基线最快腿 36.20ms。判据（中位 ≥10%、两轮同向）满足。
实测降幅（≈11.9ms）高于上限探针的 8.5ms，差额来自不再编码 2.4MB 内部信封与随之减少的分配。

## 对照（同一次 A/B 运行）

| 场景 | round | base median | cand median | delta |
|---|---|---:|---:|---:|
| H1 json-cursor | 1 | 124.28 | 125.11 | +0.67% |
| H1 json-cursor | 2 | 124.98 | 126.29 | +1.05% |
| H1 protobuf-cursor | 1 | 99.55 | 101.31 | +1.77% |
| H1 protobuf-cursor | 2 | 101.05 | 99.73 | −1.30% |
| F1 json-pipeline | 1 | 0.60 | 0.62 | +3.43% |
| F1 json-pipeline | 2 | 0.62 | 0.57 | −7.12% |

方向不一致且量级落在噪声内；本轮改动只进入 pipeline 的融合 open，cursor 路径无 diff。

## 回归门

每场景 4 条腿 A/B/B/A、1 warmup + 5 timed（`reg-raw/`）：

| 场景 | 协议 | base 中位 ms | cand 中位 ms | delta |
|---|---|---:|---:|---:|
| B1.batch-20 | json-pipeline | 2.58 | 2.74 | +6.37% |
| B1.batch-20 | protobuf-pipeline | 2.33 | 2.75 | **+18.39%**（单腿 3.22 离群） |
| C1.baton-10 | json-pipeline | 0.34 | 0.34 | −0.36% |
| F1.select-1 | json-pipeline | 0.61 | 0.58 | −5.13% |
| F1.select-1 | protobuf-pipeline | 0.58 | 0.63 | +8.93% |
| H3.content-projection | json-cursor | 119.08 | 124.19 | +4.29% |
| H3.content-projection | protobuf-cursor | 99.70 | 98.85 | −0.85% |
| L1.list-20 | json-cursor | 1.13 | 1.14 | +0.69% |
| L1.list-20 | json-pipeline | 1.14 | 1.01 | −11.31% |
| X1.concurrency | json-pipeline | 不可确认 | 不可确认 | 1/4/16 档腿间离散 0.5–31.4ms |

B1 protobuf 的 +18.39% 单独复核：两轮 2 × A/B/B/A × 8 采样（`b1-rerun/`）得到
+18.65%（配对 +37.39% / −0.71%）与 +8.34%（+9.52% / +7.22%）；第三轮 3 × A/B/B/A × 10 采样
（`b1-rerun2/`）得到 −13.05% / +11.23% / −4.15%，6 条腿中位 **−0.43%**
（base 2.356ms vs cand 2.345ms）。三个批次合并后两侧分布几乎重合
（base 2.20–3.19ms、cand 2.21–3.10ms），方向随轮次翻转；B1 的结果只有一行
`SELECT 1`，本轮改动省下的固定开销在微秒级，不存在可解释的回归机制。按不可复现处理。

X1 与 R46 相同：4 条腿的 16 并发中位 5.74/31.40（base）与 28.38/29.33（cand）ms，
p95 137–198ms，但 0 个错误、0 个 429，分布受宿主瞬态主导，记为「不可确认」。

## 验证

- `gofmt -l .` 干净；`go vet ./...` 通过；
- `go build ./cmd/peri-platform ./cmd/peri` 通过；
- 全量包测试（`PERI_RUST_BINARY` 指向本机 debug worker，`-count=1`）通过；
- 新增 3 个用例：`TestHranaGatewayResponseTakesTheTypedOpenAnswer`（typed 路径不编码信封）、
  `TestHranaOpenWithoutFusionStillReturnsTheEnvelope`（无融合标记仍返回信封——实现过程中
  这条曾是真实回归，cursor/websocket 适配器直接解码 `reply.result`）、
  `TestHranaOpenFromGatewayKeepsTheParsedAnswer`（typed 答案保留校验结果与原始字节）；
- 全部 A/B 与回归运行 `status=200`、`error_codes={}`、`warnings=[]`、
  `session_seats=0`、`active_permits=0`。

## 结论

合入 `main`。主指标两轮同向 −29.7% / −29.3%，4/4 配对腿同向，候选最慢腿低于基线最快腿；
对照与回归门无超过 10% 的可复现退化；B1 protobuf 的单轮高值经三批复测判定为噪声。
