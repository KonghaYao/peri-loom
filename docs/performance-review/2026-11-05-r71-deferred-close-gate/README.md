# R71 Gate 1a：close 的 native 段测量

日期：2026-11-05
基线：`main` @ `203f241`（工作树干净）
性质：**单场景 smoke 测量**，只覆盖 F1 `json-pipeline` c1；不是发布级 A/B，
不是容量/SLA 证明。Gate 1b 的块平均 A/B 与探针记录在同目录的下一次提交中。

## 结论

| Gate | 判据 | 实测 | 决定 |
|---|---|---|---|
| 1a | close `native` p50 < 60µs 停止；60–80µs 模糊；≥80µs 进入 1b | **154.875µs**（n=19） | **进入 Gate 1b** |

- close 的 `native` 相位是跨进程调用边界（`gateway_writer.go:53-61`，`native`
  嵌套在 `worker` 内），即客户端 close 等待 worker 的时间。
- 对照：open 的 `native` p50 **416.667µs**；statement 没有 `native`/`worker`
  相位（Hrana 隐式 open 把第一条 execute 融合进 `open_session`），其整个动作
  p50 为 50.667µs。
- 平台动作桶交叉校验：同一窗口 close 均值 882.5µs、open 980.5µs、
  statement 291.6µs。**整个 close 动作**远大于 `native`，差额是平台准入、
  状态读、记录入队与响应编码；Gate 1b 的块均值测的是整个客户端可见成本。
- 观测器绝对值只作判据与机制证据，**不得当作端到端性能**。

## 方法

- 隔离栈：镜像 `ghcr.io/konghayao/peri-loom:v1.2.7`（image id `sha256:8a8ba43b...`），
  挂载 v1.2.7 平台二进制 `95716e4d...` 与 worker `b17a0708...`；全新卷
  `peri-r71-gate1a-data`，容器 `peri-r71-gate1a`，端口 20771；默认配额。
- 场景：F1 `json-pipeline` c1，`rounds=3 warmup=1 samples=5`。
- 观测器：`--sql-open-observer`（`PERI_SQL_OPEN_OBSERVER=1`），
  `PHASE_THRESHOLD_MS=0`、`PHASE_SAMPLE_PERMILLE=1000`；相位流写入
  `<run-dir>/platform/request-phase-observation.jsonl`。
- 计时窗口：先跑一次 `run.py --dry-run` 完成 fixture discovery，再记录相位文件的
  追加偏移（`legs/f1-c1/phase-offset-bytes.txt`），只统计偏移之后的记录。
  偏移恰好落在 discovery 的 `open` 记录行首；第一版切片多丢了一行（18 vs 19），
  修正后相位记录数与 `/v1/metrics` 的动作计数逐项一致。
- 观测器关闭行（卷内文件末尾，平台停止时写入）：`complete=true`、`accepted=508`、
  `dropped=0`、`kept_slow=508`、`slow_threshold_ms=0`、`sample_permille=1000`、
  `capacity=8192`，即窗口内没有记录丢失。副本见 `legs/f1-c1/observer-close.json`。

## 结果

相位窗口：close **19**、open **19**、statement **18**、query **4**，全部 `ok`。
其中 query 4 条与 close/open 各 1 条来自**计时运行自身的 discovery**；18 条
statement + 18 条 close 是 F1 的计时/预热请求。

| 动作 | 相位 | n | p50 | p90 | mean | min | max |
|---|---|---:|---:|---:|---:|---:|---:|
| close | **native** | 19 | **154.875µs** | 1359.335µs | 335.847µs | 115.250µs | 1460.169µs |
| close | 整个动作 | 19 | 302.792µs | 3648.338µs | 887.760µs | 175.959µs | 3788.963µs |
| open | native | 19 | 416.667µs | 612.376µs | 443.536µs | 276.542µs | 1031.751µs |
| open | 整个动作 | 19 | 551.542µs | 3652.005µs | 982.321µs | 346.458µs | 4241.338µs |
| statement | 整个动作 | 18 | 50.667µs | 2089.544µs | 293.112µs | 35.750µs | 2261.712µs |

- 观测器开启时 harness 的 F1 中位为 **2.2266ms**（n=15，p95 7.362ms，exit 0，
  `warnings=[]`）。这只是该窗口的诊断值，**不是端到端性能**。
- fixture 校验通过（`threads=201 messages=8625 chars=20177069 focus=8625`）；
  窗口前后 `session_seats=0`、`active_permits=0`；`errors_total` 为空。

## 证据布局与复现

```
README.md                 本文
run_gate1a.sh             隔离栈 + F1 c1 + 相位窗口
phase_summary.py          相位流按动作/相位汇总（含追加偏移切片）
binaries.txt              镜像/平台/worker sha256 与栈参数
legs/f1-c1/               summary.json、raw.jsonl、harness.log、discovery.log、
                          metrics-*.json、request-phase-observation.jsonl、
                          phase-summary.json、phase-offset/end-bytes、observer-close.json
```

```bash
cd docs/performance-review/2026-11-05-r71-deferred-close-gate
./run_gate1a.sh
```

## 未确认项

- 观测器开启会压低吞吐、放大请求计数；绝对值只用于判据。
- 只测了 F1 形状（`SELECT 1` + close）在 c1、JSON pipeline 下的 close；
  未测 c8/c16、L1、W1、protobuf、fused close。
- Gate 1b 的块平均 A/B、A/A 自检、正对照与候选判定见同目录的下一次提交。
