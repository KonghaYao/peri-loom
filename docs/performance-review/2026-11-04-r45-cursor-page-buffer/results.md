# R45 — Hrana cursor 行按页缓冲（2026-11-04）

分支 `perf/r45-cursor-page-buffer`，实现提交 `2a52f46`。
基线平台 `/tmp/peri-perf/bin/peri-platform-r43-cand`（`dfc4ae16a30b…`，= main 代码），
候选平台 `/tmp/peri-perf/bin/peri-platform-r45-cand`（`549f0f68f32a…`），两侧同一 worker
`peri-loom-v1-r43-cand`（`27d8dd468212…`）。两套独立栈（`run-r45base` :19498 /
`run-r45cand` :19499），各自全新卷并导入同一 fixture：
`threads=201 messages=8625 chars=20177069 focus=8625`。
fixture 是合成数据：H1 = 8,625 行 × ~2.34KB（不是单行 20MB），json 响应体 20,608,525 字节、
protobuf 20,272,006 字节。

## Gate 0

临时响应 writer 计数（`gate0.txt`）：H1 json-cursor 8,625 行只产生 **34** 次写出，protobuf **37** 次；
整包字节与基线一致（20,608,525 / 20,272,006）。探针已移除后重建候选二进制。

## A/B/B/A（H1，1 warmup + 5 timed/leg，2 轮 × A/B/B/A）

| run | n | median ms | p95 ms | bytes | rows | errors |
|---|---:|---:|---:|---:|---:|---:|
| r1-base1-json-cursor | 5 | 153.79 | 159.03 | 20608525 | 8625 | 0 |
| r1-base1-protobuf-cursor | 5 | 123.97 | 124.80 | 20272006 | 8625 | 0 |
| r1-base4-json-cursor | 5 | 151.97 | 154.67 | 20608525 | 8625 | 0 |
| r1-base4-protobuf-cursor | 5 | 127.04 | 127.19 | 20272006 | 8625 | 0 |
| r1-cand2-json-cursor | 5 | 122.23 | 124.72 | 20608525 | 8625 | 0 |
| r1-cand2-protobuf-cursor | 5 | 106.69 | 111.91 | 20272006 | 8625 | 0 |
| r1-cand3-json-cursor | 5 | 120.24 | 126.96 | 20608525 | 8625 | 0 |
| r1-cand3-protobuf-cursor | 5 | 97.23 | 97.70 | 20272006 | 8625 | 0 |
| r2-base1-json-cursor | 5 | 153.38 | 153.52 | 20608525 | 8625 | 0 |
| r2-base1-protobuf-cursor | 5 | 128.94 | 131.72 | 20272006 | 8625 | 0 |
| r2-base4-json-cursor | 5 | 178.34 | 184.30 | 20608525 | 8625 | 0 |
| r2-base4-protobuf-cursor | 5 | 123.68 | 129.04 | 20272006 | 8625 | 0 |
| r2-cand2-json-cursor | 5 | 121.82 | 126.83 | 20608525 | 8625 | 0 |
| r2-cand2-protobuf-cursor | 5 | 102.19 | 103.42 | 20272006 | 8625 | 0 |
| r2-cand3-json-cursor | 5 | 127.68 | 131.85 | 20608525 | 8625 | 0 |
| r2-cand3-protobuf-cursor | 5 | 102.91 | 104.09 | 20272006 | 8625 | 0 |

| protocol | round | base median ms | cand median ms | delta | same direction |
|---|---|---:|---:|---:|---|
| json-cursor | 1 | 152.88 | 121.74 | -20.37% | 是 |
| json-cursor | 2 | 158.74 | 125.50 | -20.94% | 是 |
| **json-cursor 合并** | 1–2 | **154.23** | **123.20** | **-20.12%** | 2/2 |
| protobuf-cursor | 1 | 125.57 | 100.04 | -20.34% | 是 |
| protobuf-cursor | 2 | 127.74 | 102.55 | -19.72% | 是 |
| **protobuf-cursor 合并** | 1–2 | **126.44** | **102.03** | **-19.31%** | 2/2 |

门槛：场景中位 >10ms ⇒ 主指标至少 10%。json 与 protobuf 两轮均同向，双双约为门槛的 2 倍。

## 回归门禁（4 legs A/B/B/A，5 timed/leg）

| 场景 | 协议 | base median ms | cand median ms | delta |
|---|---|---:|---:|---:|
| B1.batch-20 | json-pipeline | 2.30 | 2.29 | -0.20% |
| B1.batch-20 | protobuf-pipeline | 6.72 | 2.43 | -63.90% |
| C1.baton-10 | json-pipeline | 0.34 | 0.36 | +5.29% |
| F1.select-1 | json-pipeline | 0.57 | 0.56 | -2.86% |
| F1.select-1 | protobuf-pipeline | 0.59 | 0.59 | +0.30% |
| H3.content-projection | json-cursor | 148.77 | 126.66 | -14.86% |
| L1.list-20 ⚠ | json-cursor | 1.14 | 1.14 | +0.64% |
| L1.list-20 ⚠ | json-pipeline | 1.33 | 1.41 | +6.64% |
| R1.limit-1000 | json-pipeline | 43.08 | 47.46 | +10.16% |

L1 在两侧全部 4 条腿上都是 `row_count_mismatch`：合成 fixture 的主导 cwd 线程数超过 SQL 的
`LIMIT 40`，而 harness 期望恰好 `list_threads` 行。两侧同等失败，与候选无关；延迟数值仍可比较。

### R1 / X1 复测

首轮 R1 json +10.16%、X1 各级方向不一致，且二者都是 pipeline 路径（本次改动只进入
`/v3/cursor`），继续 4 legs 复测：

| 场景 | 首轮 delta | 复测 base / cand median | 复测 delta | 判定 |
|---|---:|---|---:|---|
| R1.limit-1000 json | +10.16% | 46.92 / 44.30 | -5.60% | 不复现，噪声 |
| X1.concurrency json | c4 +15.6 / c8 −3.2 / c16 −52.8 | 见右 | c1 -7.0% / c4 -0.4% / c8 +91.0% / c16 -18.5% | 两轮方向不一致，宿主瞬态不可确认 |

X1 复测的腿内中位数跨 1.4–39.7ms 波动（同一侧同一档），首轮 c16 基线含 26ms 离群腿；
两轮复测在 c4/c8/c16 上方向互换，无法构成可复现退化。

## 结论

- H1 json-cursor **−20.12%**（两轮 −20.37% / −20.94%），protobuf-cursor **−19.31%**
  （−20.34% / −19.72%），两轮同向，均超过 10% 门槛；响应字节与行数逐样本一致。
- H3（另一个大 cursor 场景）−14.86%，为同一机制的旁证。
- F1 / L1 / B1 / C1 / R1 在噪声内；X1 宿主瞬态不可确认，未见可复现 >10% 退化。
- 全程 `warnings=[]`、`fatal=[]`、无 429 / 非 200；两侧结束 `session_seats=0`、`active_permits=0`。

判定：达标，合入 `main`。

