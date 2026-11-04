R51：cursor 页文本扫描与转码改为批量定位（2026-11-04）

## 方案与改动边界

H1/H3 的 20MB 文本游标页在网关内被扫描多遍：`hranaCursorPageScanner.string()` 逐字节
`switch`，`hranaCursorPageAppendText`（protobuf 行）与 `hranaJSONAppendPageText`（JSON 行）
逐字节解码/转义。本轮把「定位下一个需要单独处理的字节」变成批量操作：

- `hrana_cursor_page.go`：`string()` 先用 SWAR 跳过普通字节，只在 `"`、`\`、控制字节
  （<0x20）处进入原逐字节分支；两个 append 函数用 `bytes.IndexByte` / SWAR 批量拷贝普通段。
- 新增 `hrana_cursor_page_bulk.go`（83 行）：`hranaCursorPageIndexSpecial`、
  `hranaCursorPageIndexJSONSpecial`、`hranaCursorPageIndexBackslash`。允许提前报候选
  （如控制字节造成的进位），调用方逐字节复查，结果不变。
- 新增 `hrana_cursor_page_bulk_test.go`（223 行）：随机化差分测试。

diff `578c32d..9ad2c43` 仅 3 个文件（2 改 1 新 + 测试）；调用点只在 cursor
（`hrana_http_cursor.go`、`hrana_v3_json_cursor.go`、`gateway_cursor.go`、
`hrana_json_cursor_row.go`、`hrana_proto_cursor_row.go`），pipeline execute 路径不经过。

## 基线与环境

| 项 | 值 |
|---|---|
| 基线 | `main` @ `578c32d`，`peri-platform-r51-base` sha256 `e3745b85f36fae6dc4eb4f32b425d465e19e9b9a1d0e9e85adfe557e8063b364` |
| 候选 | `perf/r51-cursor-page-bulk` @ `9ad2c43`，`peri-platform-r51-cand` sha256 `f7f4f1307383e2ff33fd1818081d87d47cf4412887b53422daac1ce01a27d9eb` |
| worker | 两侧同一 `peri-loom-v1-r43-cand`（Rust 1.98.1 构建） |
| 工具链 | Go 1.27.3；macOS 26.5.1 arm64（Apple M4，10 核） |
| 端口/卷 | 19493（base）/ 19494（cand），各自全新 run 目录与卷 |
| fixture | threads=201 messages=8625 chars=20177069 focus=8625（两侧 verify 一致） |
| 配额 | max_sessions 16、max_concurrent 100、requests_per_second 500、max_result_bytes 4,194,304（默认） |
| 观测器 | 无；A/B 与回归门期间均未开启 |

## Gate 0（先于 A/B，详见 gate0.txt）

探针二进制先于 A/B 在同一候选源码上运行：34,779 次 string 扫描（80,778,969 字节）、
8,625 次 protobuf 文本解码（20,177,069 字节）、8,625 次 JSON 转义（20,194,319 字节），
与旧实现逐字节比较 **mismatch=0**（52,029 行日志全部为 0）；wire 字节与基线一致。
探针未进入测量二进制。

## 主指标（A/B/B/A，2 轮 × 4 腿 × 5 samples/腿）

| 场景 | 协议 | 轮 | base 中位（两条腿） | cand 中位（两条腿） | 变化 |
|---|---|---|---:|---:|---:|
| H1.content-20mb | json-cursor | r1 | 124.14 (126.24/122.04) | 74.65 (75.58/73.72) | **−39.87%** |
| H1.content-20mb | json-cursor | r2 | 123.77 (124.29/123.26) | 75.84 (75.73/75.94) | **−38.73%** |
| H1.content-20mb | protobuf-cursor | r1 | 101.26 (99.91/102.61) | 69.84 (71.89/67.78) | **−31.03%** |
| H1.content-20mb | protobuf-cursor | r2 | 104.70 (102.76/106.65) | 68.81 (69.06/68.57) | **−34.28%** |

判据（A/B 前冻结）：两协议各轮候选中位 ≤ 0.90 × 基线中位，且 2 轮同向。
实际 0.60/0.61、0.69/0.66，两轮同向 → **PASS**。四腿合并（n=4）：json 123.77→75.66
（−38.88%）、protobuf 102.68→68.81（−32.99%）。

## 同路径与对照

| 场景 | 协议 | r1 变化 | r2 变化 | 四腿合并 |
|---|---|---:|---:|---:|
| H3.content-projection | json-cursor | −35.78% | −37.92% | −36.80% |
| H3.content-projection | protobuf-cursor | −34.19% | −30.27% | −32.70% |
| R1.limit-1000 | json-pipeline | −4.47% | −9.78% | −5.63% |
| R1.limit-1000 | protobuf-pipeline | −24.66% | −4.55% | −11.11% |
| F1.select-1 | json-pipeline | +3.89% | −10.88% | −3.39% |
| F1.select-1 | protobuf-pipeline | −12.69% | −1.14% | −4.65% |

R1/F1 是 pipeline 路径，机制上不受本轮 diff 影响；其差异为宿主噪声（R1 protobuf 的
−24.66% 仅是候选更快，不构成回归）。F1 绝对值 0.5–0.6ms。

## 回归门（4 腿 A/B/B/A，5 samples/腿）

| 场景 | 协议 | base 中位 | cand 中位 | 变化 |
|---|---|---|---:|---:|---:|
| B1.batch-20 | json-pipeline | 2.46 | 2.18 | −11.22% |
| B1.batch-20 | protobuf-pipeline | 2.35 | 2.34 | −0.35% |
| C1.baton-10 | json-pipeline | 0.35 | 0.33 | −6.43% |
| F1.select-1 | json-pipeline | 0.52 | 0.59 | +14.02%（0.07ms） |
| F1.select-1 | protobuf-pipeline | 0.55 | 0.66 | +19.73%（0.11ms） |
| H3.content-projection | json-cursor | 119.85 | 74.08 | −38.19% |
| H3.content-projection | protobuf-cursor | 100.00 | 69.18 | −30.82% |
| L1.list-20 | json-cursor | 1.09 | 1.06 | −2.70% |
| L1.list-20 | json-pipeline | 0.75 | 0.75 | −0.64% |
| X1.concurrency | json-pipeline | — | — | 不可确认 |

F1 的 +14%/+19.7% 为亚毫秒绝对值，做了两次高样本复测：

- 第一轮（4 腿，15 samples/腿）：json base 0.5401 → cand 0.6102（+12.98%）；
  protobuf base 0.5216 → cand 0.5225（+0.17%）。
- 第二轮（8 腿交替，15 samples/腿）：json base 0.5284 → cand 0.5397
  （中位 +2.14%，均值 +2.45%），逐腿 0.4867–0.5455 / 0.5126–0.5482。

机制核对：diff 不含 pipeline 路径；F1 的 `SELECT 1` 经 `hrana_http.go` 执行，不调用本轮
改动的游标页函数。两轮复测方向不一致（+13% vs +2%），按噪声处理，不记为回归，
也不宣称候选在 F1 上更快。

X1：c16 档 base 两腿 29.01/4.60ms、cand 22.29/18.26ms，c8 档 base 1.11/3.29ms、
cand 2.14/7.07ms，跨度远大于效应量，**不可确认**；未以此档下结论。

## 不变量

- wire 字节与基线一致：H1 json 20,608,525、H1 pb 20,272,006、H3 json 20,746,565、
  H3 pb 20,306,526、F1 312/40、C1 300、L1 16,393/15,636、B1 pb 686；
  B1 json 4,960–4,982 与 R1 json 2,369,290/2,369,300 的差异来自 `query_duration_ms`
  数字位数（历史同现象）。
- 全部 A/B、回归与复测样本 `warnings=[]`、`fatal` 无、非 200 = 0；两侧平台日志无 error、
  无 `audit_unavailable` / `outcome_unknown` / `session_moved`。
- 探针日志 52,029 行全部 mismatch=0。

## 正确性

- 新增随机化差分测试（固定种子）：
  - `TestHranaCursorPageIndexSpecialMatchesReference` / `...JSONSpecial...` / `...Backslash...`：
    各 2,000 次随机字节串，覆盖 8 字节对齐、不同长度与特殊字节分布。
  - `TestHranaCursorPageAppendTextRandomizedMatchesDecode`：2,000 个随机合法 JSON 文本 token
    （原始段、全部转义、代理对、孤立代理、多字节字符）与 `encoding/json` 解码对拍。
  - `TestHranaJSONAppendPageTextRandomizedMatchesAppendString`：同一批 token 与
    `hranaJSONAppendString` 的转义逐字节对拍。
  - `TestHranaCursorPageRandomPageMatchesLegacyDiff`：200 个随机整页，JSON 与 protobuf
    行编码均与 legacy 解码路径逐字节一致。
- 既有语料测试（`TestHranaCursorPageJSONTranscodeMatchesLegacy` 等）全部通过。
- `gofmt -l .` 干净、`go vet ./...` 干净、`go build ./cmd/peri-platform ./cmd/peri` 通过、
  定向游标测试 `ok peri-loom/platform 1.039s`、全量 `go test -count=1 .`
  `ok peri-loom/platform 179.642s`（`PERI_RUST_BINARY` 指向本机 debug worker）。

## 证据与脚本

- `criteria.txt`：A/B 前冻结的判据。
- `r51-ab.sh` / `r51-ab-summary.py`：主指标与对照（原始结果 `ab-raw/`，128 个目录）。
- `r51-reg.sh` / `r51-reg-summary.py`：回归门（`reg-raw/`，80 个目录）。
- `reg-recheck-f1/round1-15samples/`、`round2-15samples/`：F1 高样本复测。
- `probes/r51-gate0.diff`、`probes/perf_gate0_r51.go`：Gate 0 探针，仅存在于探针 worktree。
- `gate0.txt`：Gate 0 计数与判定。

## 剩余风险与未测量项

- 未测写路径（W1）容器 A/B；本轮 diff 不进入写路径，写路径由全量 Go 测试覆盖。
- 未做发布级 endpoint A/B；本轮只对平台二进制做受控 A/B，worker 与镜像未变。
- SWAR 的“提前候选”只会多一次复查，不改变结果；已在随机化测试中覆盖对齐/控制字节边界。
- 0xe2 字节（部分多字节字符的前导）会提前结束批量段，只影响含此类字符的页；H1/H3 的
  fixture 为 ASCII 文本。若未来内容以 ≥0xE2 多字节字符为主，需要重新测量。
- 未测 E1 known-gap 与 Hrana abort 路径；本轮未触碰写 cursor / abort 逻辑。
