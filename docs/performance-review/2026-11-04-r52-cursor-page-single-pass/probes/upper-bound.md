# R52 上限探针：只读 cursor 页单趟解码可移除成本

- 探针二进制：`/tmp/peri-perf/bin/peri-platform-r52-probe`
  （sha256 `bf743deb4792de8aaf62e1f3a0c4064853fd3ecf6174a437267ab611c064d3d8`）
- 源码：`perf/r52-cursor-page-single-pass` @ `8967ae8` + 本目录 `upper-bound-probe.diff`
  （`perf_probe_r52.go` 累计微秒计数器，1s 脉冲写日志；不改变任何判定逻辑）
- 探针工作的主路径 = **基线路径**（`main@5327333` 的 `sqlDecode` 通用解码 + gateway 页校验），
  因此测得的是「候选改动可以省掉的阶段成本」上限。
- 环境：macOS 26.5.1 / arm64，本地栈 `127.0.0.1:19495`，无 TLS，生产配额
  （`max_concurrent=100`、`max_sessions=16`、`max_result_bytes=4194304`），
  fixture `threads=201 messages=8625 content_chars=20177069`。
- 场景：`H1.content-20mb json-cursor`，单次查询（1 warmup + 1 timed），同窗口基线
  `median=75.01ms`（A/B 基线侧 75.04/75.01）。

## 单次查询的阶段增量（pulse 66，一条查询）

| 阶段 | 调用数 | 耗时 |
|---|---:|---:|
| `json.Valid`（整页 20MB，通用解码） | 47 | **8.043 ms** |
| `sqlJSONFields`（结构定位） | 47 | 8.696 ms |
| 结果 span 复制 | 43 | **1.320 ms** |
| gateway 页校验（非首页） | 35 | **2.200 ms** |
| fused 首页 `validCursorFirstResponse` | 1 | **5.612 ms** |
| adapter 二次扫描 | 34 | **2.397 ms** |
| compact + escape | 35 | **0.802 ms** |
| `utf8.Valid` | 47 | **0.323 ms** |

交叉核对（pulse 16，3 条查询合计折算单条）：`json.Valid 7.914 ms`、`fields 8.539 ms`、
`copy 2.511 ms`、`gwfetch 2.337 ms`、`gwfirst 5.514 ms`、`adapter 2.419 ms`、
`compact+escape 0.784 ms`、`utf8 0.279 ms` —— 同量级、同排序。

## 判定

- 可移除阶段合计：8.043 + 1.320 + 2.200 + 5.612 + 2.397 + 0.802 + 0.323 = **20.70 ms**，
  占同窗口 H1 json 单查询（75.01 ms）的 **27.6%**。
- 结构定位 `sqlJSONFields`（8.70 ms）**不**计入可移除：新路径仍要一次结构扫描来定位 span，
  它变成 `sqlJSONFieldSpans`；新路径新增的严格页扫描成本未在本探针内。
- 门槛：主指标 10% = 7.57 ms（75.66 ms 基线的 10%）。上限 20.70 ms ≥ 7.57 ms，
  方向成立，允许进入实现。
- 事后对照：实现后 A/B 实测 H1 json 75.04→62.17 / 75.01→61.69（−17.2% / −17.8%），
  低于上限（新增严格页扫描约 7–8 ms），但显著高于门槛，方向与探针一致。
