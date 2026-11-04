# R54 上限探针：平台只读 cursor 页路径

**结论：可移除上限 ≤5ms（7.7%），低于 10% 门槛，不实施。**

## 目的

R52 合入后，H1 json-cursor 的 62–64ms 里平台侧还剩多少？R52 用差集推算严格页扫描约 7.9ms，
本轮直接测量 readall / spans / skeleton / scan / transcode / flush，判断「扫描+转码融合」是否值得做。

## 方法

- 平台探针构建：`peri-platform-r54-probe`，sha256 `8d7c0a7c6520a90bb7953b4aded8b51239c12fd06b652f6d11158d7e008e4053`
  （基于 `1a2d2e0`，仅加计时，不改路径；diff 见 `upper-bound-probe.diff`）。
- worker：`peri-loom-v1-r53-clean`，sha256 `27d8dd4682128ff7668999254e068144c878b5fa972357d7dd22a5aac50ce3a4`
  （与 `r43-cand` 逐字节相同，main 当前代码）。
- 栈：`127.0.0.1:19610`，新卷 + 标准 fixture（201 threads / 8,625 messages / 20,177,069 chars）。
- 场景：`H1.content-20mb` json-cursor，1 warmup + 2 samples；平台探针每次 cursor 流结束打一行累计值，
  逐行相减得到单次查询的耗时。

## 结果（单次查询，稳态）

| 段 | 耗时 | 说明 |
|---|---:|---|
| `ReadAll` | 8.53–8.83ms | 只含 body 传输；worker 计算/响应头等待在 `client.Do`，不在本段 |
| `sqlJSONFieldSpans` | 0.70–1.45ms | 顶层结构定位 |
| 信封 skeleton `json.Valid` | 0.016–0.026ms | 结果 span 被替换为 `{}`，几乎为零 |
| 严格页扫描 `sqlDecodeCursorPage` | 2.42ms | **R52 推算为 7.9ms，实测 2.42ms** |
| 页转码（`appendPageRow`/`rowBuffer`） | 4.18–4.21ms | 20.6MB → 4.9GB/s，接近内存带宽 |
| 分页 flush | 1.54–1.57ms | 34 次写客户端 |
| 合计 | 17.3–18.4ms | 占同栈 64.7ms 的 27–28% |

原始输出：`platform-probe.log`；harness：`harness/`。

## 判定

- 融合「严格扫描 + 转码」只能省掉扫描这一趟：**≤2.42ms**。
- `ReadAll` 预分配（`Content-Length` + `bytes.Buffer.Grow`）只能省增长拷贝，估算 ≤2ms（未单独实测）。
- spans 0.7ms 与 skeleton 0.02ms 可忽略；转码/flush 已接近带宽，无空间。
- 合计上限 **≤5ms ≈ 7.7%**，低于 10% 硬门槛（6.5ms）。**不实施**；平台页路径已无 ≥10% 的单一杠杆。

## 备注

- 该栈 H1 json-cursor 中位数 64.73ms，与 R52 A/B 基线 62–64ms 同窗口一致。
- 此前 `R53` 探针栈（每页向 worker stderr 打一行）把 H1 拖到 141–148ms；本探针改为每查询一行，
  未再出现放大（64.73ms）。
