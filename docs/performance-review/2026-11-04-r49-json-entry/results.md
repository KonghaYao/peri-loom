# R49 — Hrana JSON execute entry 单遍直转码与响应字节复用（2026-11-04）

分支 `perf/r49-json-entry`，实现提交 `d84924d`。
基线平台 `peri-platform-r49-base`（`8c96e7fdab09…`，`main` @ `c5d2997`，即 R48 合入后）；
候选平台 `peri-platform-r49-cand`（`0bbbe901910b…`，干净 `d84924d` 构建）。
两侧同一 worker `peri-loom-v1-r43-cand`（`27d8dd46…`），各自全新卷 + 同一 fixture
（201 线程 / 8,625 消息 / 20,177,069 字符）。

## 背景与上限探针

R48 之后 R1 json-pipeline 单请求仍把同一份 2.35MB 语句结果完整物化：adapter 用
`json.Decoder` 解码为 `hranaNativeStatement`（`[][]any`），转成 Hrana 单元格 map，entry 预算
`json.Marshal` 一次后丢弃，最终响应再 `json.Marshal` 一次。

上限探针（`main` @ `c5d2997`，`PERI_R49_PROBE_LOG`，18 个 R1 请求，中位）：

| 阶段 | 中位 | 备注 |
|---|---:|---|
| adapter 解码（UseNumber） | 3.04ms | 尾部 7.2–10.7ms 出现 5 次 |
| 单元格转换 | 0.064ms | |
| entry 预算 marshal | 1.51ms | 结果随即丢弃 |
| 最终响应 marshal | 1.91ms | 尾部 7.2–13.0ms 出现 5 次 |
| 按 entry 字节拼装响应 | 0.119ms | 一次预分配 |
| 写响应 | 0.343ms | |

结论：
- **C3'（只复用 entry 字节，不动解码）上限 1.79ms（6.9%），低于 10% 门槛，不单独立项。**
- **D1（解码/转换/entry 编码合并为单遍）净收益 3.10–4.61ms（11.9–17.7%）。**
- D1 + 响应复用合计 4.89–6.40ms（18.8–24.6%），据此实施合并方向。

## 改动

`prototypes/v1/platform/hrana_encoded_execute.go`（新增）：
- `hranaEncodedEntry` 与 `MarshalJSON`（原样返回字节；失败路径/回退 `json.Marshal` 仍逐字节一致）。
- `encodeHranaExecuteEntry`：单遍严格扫描 worker statement 结果 span，直接产出完整 entry 字节
  `{"response":{"result":{…},"type":"execute"},"type":"ok"}`；等价复刻 `hranaNativeStatement` +
  `hranaOutputValue` + metrics 校验的全部语义（重复键 last-wins、`cols:[null]`、`rows:[null]`、
  整数保原始字面量、float 按 encoding/json 格式化、blob base64、metrics 三者同时存在、
  `last_insert_rowid` null/int64、字符串 HTML 转义与 U+2028/U+2029/非法 UTF-8 处理）。
  任何不支持形状返回 `false`，由旧路径接管分类。
- `assembleHranaResponse`：按 entry 字节拼装最终响应（一次预分配），字节与 `json.Marshal` 一致。

`hrana_http.go`：
- `executeWithin` 在 `/v1/sql/statement` 成功、解码之前，仅当融合标记命中
  （`fusedStatementResultFrom` 且 session/SQL 与 gateway 的检查一致）时尝试直转码；成功直接返回
  entry 字节，失败回退原解码路径。describe/cursor 回退与错误分类不变。
- pipeline 预算块直接使用 entry 字节参与长度核对与 `resultBytes` 累计。
- 最终响应在全部 entry 都有字节时用拼装，否则回退 `json.Marshal(response)`。
- 只影响 HTTP JSON pipeline；protobuf、cursor、WS 与公开 API 不改。

## Gate 0（探针构建，测前移除）

探针二进制 `2b4fd46674dc…`（`d84924d` + 每请求计数 + 现场 `json.Marshal` 对拍），栈 :19498，
R1 12 个请求：

| 计数 | 值 |
|---|---|
| `fast_attempt` / `fast_ok` / `fast_fallback` | 1 / 1 / 0（每请求） |
| `entry_reused` / `entry_marshaled` | 1 / 1（execute 复用，close 走旧 marshal） |
| `assemble_ok` / `assemble_fallback` | 1 / 0 |
| `legacy_equal` / `legacy_mismatch` | **1 / 0（每请求）** |
| `final_bytes` | 2,369,288–2,369,299 |

即直转码命中、旧解码路径不在请求路径上，且现场拼装字节与 `json.Marshal` 逐字节一致。
探针构建的 smoke 中位 19.89ms（n=10；含额外一次 legacy marshal + 比较，不作为性能数字）。

## 配对 A/B

2 轮 × A/B/B/A，每腿 1 warmup + 5 timed：

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 24.266 / 17.806 | 14.848 / 19.353 | **21.04→17.10（−18.71%）** |
| 2 | 21.739 / 20.931 | 16.822 / 15.951 | **21.34→16.39（−23.19%）** |

逐对配对 −38.81% / +8.68% / −22.62% / −23.79%（3/4 同向）；4 腿整体中位
**21.34→16.39ms（−23.2%）**。实测降幅约 5.0ms，与上限探针的 4.9–6.4ms 一致。
全部 R1 响应 2,369,290 字节（一行 2,369,300，`query_duration_ms` 位数差）、1,000 行、
仅 200、无 warning。

对照：H1 json-cursor −1.23% / −3.45%，H1 protobuf-cursor −3.65% / −2.84%（候选更快），
F1 json-pipeline +7.93% / −8.42%（亚毫秒，方向不一致）。

## 回归门

4 腿 A/B/B/A：

| 场景 | 变化 |
|---|---:|
| B1.batch-20 json-pipeline | +3.31% |
| B1.batch-20 protobuf-pipeline | −1.99% |
| C1.baton-10 json-pipeline | +3.55% |
| F1.select-1 json-pipeline | −22.34%（亚毫秒） |
| F1.select-1 protobuf-pipeline | +0.82% |
| H3.content-projection json-cursor | −0.86% |
| H3.content-projection protobuf-cursor | −2.41% |
| L1.list-20 json-cursor | −5.80%（两侧 `row_count_mismatch`，fixture 项） |
| L1.list-20 json-pipeline | −11.36%（同上） |
| X1.concurrency | 该宿主已记录不可确认（各腿跨度大于候选差） |

无候选回归超过 10%；全部样本 `warnings=[]`、非 200=0。

## 测试

- 差分对拍：约 130 手写用例 + R1 真实结果（2,345,185 B）+ 多类型单元格（text 转义/HTML/
  U+2028/U+2029/非法 UTF-8、integer 含 `-0` 与边界、float、blob 边界、null、metrics 变体、
  重复键、未知键、`rows:[null]`、`cols:[null]`、尾部内容），新旧路径逐字节一致。
- 拼装对拍：baton nil/字符串、0/1/N entries 与 `json.Marshal(hranaResponse)` 一致。
- 回退：不可编码形状回退旧路径，错误码不变。
- 微基准（真实载荷）：旧路径 4.53ms → 新编码器 2.49ms、4 allocs、2.5MB。
- `gofmt -l .` 干净；`go vet ./...`、`go build ./cmd/peri-platform ./cmd/peri` 通过；
  `PERI_RUST_BINARY=…/target/debug/peri-loom-v1 go test -count=1 .` **ok 176.567s**。

## 证据

`docs/performance-review/2026-11-04-r49-json-entry/`：`results.md`、`gate0.txt`、`probe-base.log`、
`probes/`（两个探针 diff 与源文件）、`gate0-smoke/`、`ab-raw/`、`reg-raw/`、运行与汇总脚本。
原始日志仍在 `/tmp/peri-perf/`（非持久）。
