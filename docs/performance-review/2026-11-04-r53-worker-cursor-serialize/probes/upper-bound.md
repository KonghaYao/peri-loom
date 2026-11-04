# R53 上限探针：worker 只读 cursor 页单趟序列化

日期：2026-11-04
基线：`main` = `1a2d2e0`（R52 已合入）
结论：**上限低于 10% 门槛，不实施**（无生产代码改动）

## 假设（来自 R53 反思报告）

worker 每个只读 cursor 页做三趟 20MB 量级文本拷贝：

1. `row.get_values().cloned()`：`Value::Text` 深拷贝进 `Vec<Vec<Value>>`；
2. `json!` 构建 `serde_json::Value` 树（第二趟深拷贝）；
3. `serde_json::to_vec` 逐字节过 `ESCAPE` 表转义。

拟改为只读 cursor 专用借用 `Serialize`（跳过树）+ `memchr` 批量转义，预估 11–28ms。
判据：10% 门槛 = 6.2ms（基线约 62ms）。

## 方法

- 探针 worker 由 `main` 构建（仅加计时，不改输出）：
  - `dispatch.rs`：`FetchReadCursor` 与 `OpenReadCursorFirst` 分四段计时——native
    （`fetch_read_cursor` / `open_cursor_first`）、`json!` 树、`to_vec` 序列化、`transport::reply`；
  - `session/execution.rs`：`run_steps` 的 `get_values().cloned()` 单独计时；
  - `server.rs`：每请求 reset 计时、序列化与写出计时。
- 平台：`peri-platform-r53-base`（`go build ./cmd/peri-platform`，`deaea101…`），与
  `peri-platform-r52-cand`（`a54c9b91…`，同尺寸）行为对照一致。
- 栈：`dev.py --no-build`、`PERI_HRANA_EXPERIMENTAL=1`、全新 run 目录与卷、同一 seed
  （`201 threads / 8,625 messages / 20,177,069 chars`）。
- 场景：`run.py --scenario H1.content-20mb --protocol json-cursor --rounds 1 --warmup 1 --samples 2..3`。

二进制（sha256）：

| 名称 | sha256 | 说明 |
|---|---|---|
| `peri-platform-r53-base` | `deaea1013e84f83797fba53260a0f3a4716dccdece80887021f426d239b91156` | 平台基线（main） |
| `peri-loom-v1-r53-clean` | `27d8dd4682128ff7668999254e068144c878b5fa972357d7dd22a5aac50ce3a4` | main 干净构建，与 `r43-cand` 逐字节一致 |
| `peri-loom-v1-r53-probe2` | `e2be9569cf2faf29aafb8f992e347cb06da5aeb83d5234511cccf256f0a2efcc` | 计时 + 每页 `eprintln!` |
| `peri-loom-v1-r53-probe3` | `15c4592a147d1d87368c3f5a15e885fdce2c5e8691cef4ed878a08258aea1f57` | 同计时、无输出 |

## 端到端对照

| 栈 | worker | H1 json-cursor 中位 |
|---|---|---:|
| 19600/19604 | probe2（计时 + 每页打印） | 141.35 / 147.75 / 141.64ms |
| 19601 | clean（`r43-cand`）+ `r52-cand` 平台 | 64.50ms |
| 19602 | clean + `r53-base` 平台 | 63.80ms |
| 19605 | probe3（计时、无输出） | 64.56ms |

**方法教训**：每页一次 `eprintln!`（34 行/查询）把 H1 拉高 2.2×（64.6 → 141.6ms）；
换成同计时的无输出构建即回到基线。分段计时在打印之前取得，且各段之和与 `total_us` 自洽，
故分段结论有效；但**热路径探针不得逐请求写 stderr**，应累计到原子计数、每查询输出一次。

## 分段实测（probe2，稳态页，n≈34 页/查询 × 2 查询）

H1 实际形状是 **8,625 行 × ≈2.34KB、34 页**，每页 ≈600KB（不是单行 20MB）。

| 段 | 每页 | 每查询（34 页） |
|---|---:|---:|
| native（含 `get_values()`） | 700–870µs | ≈25.5ms |
| `get_values().cloned()` 克隆 | 24–35µs | ≈1.0ms |
| `json!` 树 | 36–71µs | ≈1.9ms |
| `serde_json::to_vec` | 129–206µs | ≈5.4ms（≈3.75GB/s） |
| `transport::reply` | 29–73µs | ≈1.7ms |
| handler 合计（`total_us`） | 955–1206µs | ≈35.5ms |

页大小：fetch 600,437B、首帧 600,501B、末页 415,004B；响应对照 20,608,525B 与基线一致。

## 判定

- 借用序列化能省：树 1.9ms + `to_vec` 的转义部分（实测吞吐已 ≈3.75GB/s，`memchr` 乐观省
  50–60% ≈ 2.7–3.2ms）⇒ **4.6–5.1ms ≈ 7.2–8.0%**。
- 即使（不可实现地）删掉克隆 + 树 + 序列化全部，也只有 8.3ms（13%）；序列化本身不可删除，
  20.6MB 输出必须产生。
- 门槛 6.4ms ⇒ **不实施**；探针 diff 见 `probes/upper-bound-probe.diff`。

## 对上一轮反思假设的修正

- 「三趟 20MB 量级」不成立：每页仅 ≈600KB，克隆 30µs、树 55µs、序列化 160µs。
- 克隆不是大头；worker 剩余成本主要在 native（≈25.5ms/查询，turso 逐行取数与预算记账）。
- 端到端 64ms 中约 35ms 在 worker handler 内，平台与客户端合计约 29ms。

## 下一轮提示

平台侧在 R52 后仍有两趟 20MB 走查：严格页校验（R52 算术 ≈7.9ms）与 Hrana 转码（3–6ms）。
「边校验边编码、整页通过后才放行」的融合 + 文本 span 的 `memchr` 定位可能省 5–9ms（8–14%），
须先做上限探针（Go-only，不改协议）。
