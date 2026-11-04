# R56 A/B 结果：只读 cursor 页预取 + worker 响应体按声明长度预分配

判据见同目录 `criteria.txt`（测量前冻结）：主指标 H1.content-20mb 的 json-cursor 与
protobuf-cursor，每轮 4 腿 A/B/B/A、每腿 1 warmup + 5 samples，同轮候选腿中位数
≤ 0.90 × 基线腿中位数（≥10%）且两轮同向。

## 环境

- 两条独立栈、各自全新 run dir 与同批 fixture，默认配额
  （`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_result_bytes=4,194,304`）。
- 基线栈 `127.0.0.1:19631`（`/tmp/peri-perf/run-r56base`）：
  平台 `/tmp/peri-perf/bin/peri-platform-r56-base2`
  （sha256 `9e003707aeb9ba846f60b7f569a867016226834f2520af68a54ed7b3994c3052`，
  由 worktree `/tmp/peri-r56-base` @ `fbc0e3d` 干净树构建）+ worker
  `/tmp/peri-perf/bin/peri-loom-v1-r53-clean`
  （sha256 `27d8dd4682128ff7668999254e068144c878b5fa972357d7dd22a5aac50ce3a4`）。
- 候选栈 `127.0.0.1:19632`（`/tmp/peri-perf/run-r56cand`）：
  平台 `/tmp/peri-perf/bin/peri-platform-r56-cand2`
  （sha256 `611fa2d8455688691cc65739c7d6d2c75156ffa6bf49f2b6dc37a96a94e358ae`，
  由 `perf/r56-cursor-page-pipeline` @ `20d8db5` 干净树两次构建一致）+ 同一 worker。
  R56 只改 Go 平台，worker 两侧相同。
- fixture（两侧 `summary.json` 的 `fixture` 块一致）：`threads=201`、`messages=8625`、
  `content_chars=20177069`、`focus_messages=8625`、`list_threads=40`。
- harness `git_revision=069d579`（仅文档差异，代码同 `d5cf31f`）。
- 脚本：`r56-ab.sh`（跑腿）、`r56-ab-summary.py`（按判据汇总）；原始数据
  `/tmp/peri-perf/r56ab`（每腿 `raw.jsonl`、`summary.json`、`summary.md`）。

## 逐腿中位数（ms）

| 轮 | 侧 | 腿 | json-cursor | protobuf-cursor |
|---|---|---:|---:|---:|
| 1 | base | 1 / 4 | 63.341 / 66.885 | 57.208 / 61.277 |
| 1 | cand | 2 / 3 | 63.071 / 55.136 | 52.995 / 54.115 |
| 2 | base | 1 / 4 | 62.647 / 59.674 | 56.578 / 59.148 |
| 2 | cand | 2 / 3 | 54.689 / 56.638 | 52.886 / 52.578 |

## 判决（按冻结判据）

| 轮 | 协议 | 基线腿中位数 | 候选腿中位数 | 比值 | 判定 |
|---|---|---:|---:|---:|---|
| 1 | json-cursor | 65.113 | 59.104 | 0.9077 | FAIL |
| 1 | protobuf-cursor | 59.243 | 53.555 | 0.9040 | FAIL |
| 2 | json-cursor | 61.161 | 55.663 | 0.9101 | FAIL |
| 2 | protobuf-cursor | 57.863 | 52.732 | 0.9113 | FAIL |

- 改善幅度 **8.99%–9.60%**，4/4 格同向，无可复现退化；唯一未达标的原因是冻结硬门
  正好是 10%，四格都差 0.4–1.0 个百分点。
- 噪声说明：json 候选第 1 轮有一条腿 63.071ms 落进基线区间（基线该轮 62.647–66.885ms）；
  protobuf 的 4 条候选腿（52.578–54.115ms）与 4 条基线腿（56.578–61.277ms）完全不重叠。
  按冻结口径用「两条腿的中位数」聚合，未剔除任何腿。

## 不变量

- wire 字节与参考值逐字节一致：json `20,608,525`、protobuf `20,272,006`（16 条腿全部）。
- 每条腿 `rows=8625`、`status={"200": 5}`、`outcomes={"ok": 5}`、`warnings=[]`、`fatal=""`。
- 平台严格扫描接受预取页：全部样本 `verdict` 正常，无 `outcome_unknown`/`session_moved`。

## 结论

单独作为一轮，R56 **未达冻结的 10% 硬门（实测 8.99%–9.60%）**，不单独合入。
按 R56 判据中的批次声明，R57（同一链路上的 worker 侧页序列化）作为第二项机制加入同一批次；
批级 A/B 在 R57 目录（`../2026-11-04-r57-worker-cursor-borrow/`）用**两侧全新卷、当轮的
候选/基线二进制重新测量**，不复用本文件数字作为批级结论。

本文件与 `r56-ab.sh`、`r56-ab-summary.py` 一起作为该轮的验收证据保留在分支上。
