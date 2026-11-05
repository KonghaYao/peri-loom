# R71 Gate 1a / 1b：close 的 worker 等待与异步派发的上限

日期：2026-11-05
基线：`main` @ `203f241`（Gate 1a 测量时工作树干净；Gate 1b 的探针在独立 worktree 中）
性质：**探针测量，非发布级**。Gate 1a 是单场景 smoke；Gate 1b 使用**不安全的探针分支**
（`probe/r70-deferred-close-ceiling`），该分支只用于测量，**绝不合并、不推送为候选**。
结论只覆盖 F1 形状的小查询 pipeline 在 c1 下的行为，不构成容量或 SLA 证明。

## 结论

| Gate | 判据 | 实测 | 决定 |
|---|---|---|---|
| 1a | close `native` p50 < 60µs 停止；60–80µs 模糊；≥80µs 进入 1b | **154.875µs**（n=19） | 进入 1b |
| 1b A/A | 同一二进制两侧，合并比值 1.00 ± 0.05 | **1.0463**（CI 1.0067–1.0801） | 字面通过，余量 0.4pp |
| 1b 正对照 | probe 侧注入 100µs，检出 ≥8% 且两轮同向 | **+13.3%**（CI +8.3%–+20.9%），两轮 1.136 / 1.128 | 通过 |
| 1b 候选 | 合并中位比值 ≤0.85 实现；0.85–0.90 临界；>0.90 停止 | **0.7265**（CI 0.6935–0.7504），两轮 0.731 / 0.699 | **建议实现安全版** |

- **可移除量**：close 动作均值从 **671.7µs 降到 89.1µs**（−583µs/请求）；块均值从
  **2.55ms 降到 1.86ms**（−27.3%）。候选两轮同向，CI 上界 0.750 仍低于 0.85。
- **协议功效**：同二进制 A/A 的位置拆分（a1 vs a2）为 **0.914**、第二轮 B/A 为 **1.092**，
  说明本宿主上仍有约 9% 的位置/轮次漂移。候选效应（−27%）远大于该漂移，且两轮一致，
  因此判决不受影响；但 10–15% 量级的效应在本协议下不可靠。
- **正对照的实现陷阱**：第一版用 `time.Sleep(100µs)`，容器内实测 **p50 = 2.92ms**
  （见下），把标称 100µs 放大成毫秒级；该轮结果（1.662）不能作为 100µs 对照。
  第二版用忙等，实测 p50 100.04µs，才是有效对照。
- **未实现安全版**：本轮不写生产代码。`close` 异步派发涉及配额/座位、失败重试、
  优雅关闭排空与崩溃恢复，必须按规划文档 §2.2–2.5 另行实现并验证。

## Gate 1a：close 的 native 段

### 方法

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

### 结果

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

- **statement 没有 `native`/`worker` 相位**：Hrana 隐式 open 把第一条 execute 融合进
  `open_session`（`open_guarded_statement`），statement 记录只承担平台侧的结果处理。
- 平台 `/v1/metrics` 的动作桶交叉校验（同一窗口，19/19/18 次）：
  close 均值 882.5µs、open 980.5µs、statement 291.6µs；与相位“整个动作”一致。
- 观测器开启时 harness 的 F1 中位为 **2.2266ms**（n=15，p95 7.362ms，exit 0，
  `warnings=[]`）。这只是该窗口的诊断值，**不是端到端性能**。
- fixture 校验通过（`threads=201 messages=8625 chars=20177069 focus=8625`）；
  窗口前后 `session_seats=0`、`active_permits=0`；`errors_total` 为空。

### 判据与解释

close `native` p50 = **154.875µs ≥ 80µs** → 进入 Gate 1b。

`native` 是跨进程调用边界（`gateway_writer.go:53-61`，`native` 嵌套在 `worker` 内），
因此它就是客户端 close 等待 worker 的时间。**注意**：整个 close 动作的均值
（882.5µs）远大于 native（p50 154.9µs / mean 335.8µs），差额是平台准入、状态读、
记录入队与响应编码；Gate 1b 的块均值测的是**整个客户端可见的 close 成本**，
不是仅 native 段。

## Gate 1b：块平均 A/B

### 冻结协议

- **样本**：连续 100 个
  `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}},{"type":"close"}]}`
  pipeline 请求的总墙钟 ÷ 100。
- **每腿**：1 个 warmup 块 + 30 个计时块；一个 keep-alive 连接。
- **每轮**：4 腿 A/B/B/A；**2 轮**，共 8 腿/campaign。
- **隔离**：每腿独立栈、全新命名卷、专用容器/端口、同 fixture、默认配额、无观测器；
  worker 二进制两侧相同（`b17a0708...`）。
- **默认配额与块间距**：每个 pipeline 请求消耗 3 个准入 token（隐式 open、statement、
  close），项目桶容量 500、500/s。100 个连续请求花 300 token，块间必须补桶，
  否则会出现 429（首次 smoke 腿实测 1542 个 429，见 `legs/r1-a1-smoke/`）。
  协议因此在**块之间**等待 0.75s（首个块前 1.2s），**块内 100 个请求仍连续**，
  样本定义不变，配额未放宽。

### campaign 1：A/A 自检

两侧都是 v1.2.7 平台二进制（sleep 与 spin 探针的改动都不在这条路径上）。

| 腿 | r1-a1 | r1-b1 | r1-b2 | r1-a2 | r2-a1 | r2-b1 | r2-b2 | r2-a2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 中位（ms/请求） | 2.6818 | 2.4467 | 2.6243 | 2.4481 | 2.4864 | 2.5863 | 2.6411 | 2.3320 |

- 合并 B/A = **1.04629**，bootstrap 95% CI **1.00665–1.08013**（10,000 次，按块重采样）。
- 逐轮：r1 **0.99859**、r2 **1.09169**。
- 位置拆分（a1 腿 vs a2 腿）：**0.91407**，CI 0.87739–0.97270。
- 8/8 腿 `failures=0`、`session_seats=0`、`active_permits=0`、`errors_total` 空。

**判定**：按冻结口径（合并比值）通过 1.00 ± 0.05，但只剩 0.4pp 余量；位置/轮次
漂移约 9% 是协议功效的真实限制。候选效应 −27% 且两轮同向，判决仍站得住；
**10–15% 的效应在本协议下不可判**。

### campaign 2：正对照（sleep 版，无效）

- 实现：close 路径 `time.Sleep(100µs)`（probe commit `5403339`，二进制 `51680925...`）。
- 合并 B/A = 1.66238（CI 1.57261–1.78467），两轮 1.75294 / 1.55022。
- **但它不是 100µs**：同一容器镜像内独立测量（n=300）得到
  `sleep100us p50=2.923109ms, mean=2.877749ms, min=118.251µs`；腿级 close 动作均值
  也从 73.7µs 涨到 1196.8µs。该轮只能证明“协议能看见毫秒级延迟”，不能验证 100µs。

### campaign 3：正对照（忙等 100µs，有效）

- 实现：改为 `time.Now()` 忙等（probe commit `f01cd8e`，二进制 `6517583e...`）；
  独立测量 `spin100us p50=100.042µs, p90=100.084µs`。
- 两侧都是 probe；仅 B 设置 `PERI_R71_CLOSE_DELAY_US=100`。

| 腿 | r1-a1 | r1-b1 | r1-b2 | r1-a2 | r2-a1 | r2-b1 | r2-b2 | r2-a2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 中位（ms/请求） | 1.8748 | 2.1537 | 2.1372 | 1.9338 | 1.8595 | 2.2288 | 2.1693 | 2.0067 |

- 合并 B/A = **1.13258**，CI **1.08304–1.20891**；逐轮 **1.13602 / 1.12804**。
- 机制：close 动作均值 85.4µs → 234.8µs（+149.4µs），比 100µs 多出的约 50µs
  是忙等的调度开销。
- **判定：通过**（≥8%、两轮同向）。

### campaign 4：候选

A = v1.2.7 基线（`95716e4d...`），B = probe（`6517583e...`，env 未设置）。

| 腿 | r1-a1 | r1-b1 | r1-b2 | r1-a2 | r2-a1 | r2-b1 | r2-b2 | r2-a2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 中位（ms/请求） | 2.5682 | 1.9407 | 1.7738 | 2.5340 | 2.5672 | 1.8092 | 1.7450 | 2.5148 |

- 合并 B/A = **0.72653**，CI **0.69350–0.75035**（10,000 次重采样）。
- 逐轮：r1 **0.73142**、r2 **0.69852**；8/8 腿同向。
- 机制（同一轮 r1 的 A/B 对）：close 动作均值 **671.7µs → 89.1µs**；
  open 711.2µs → 651.2µs、statement 128.0µs → 101.8µs（无实质变化）；
  块均值 2.55ms → 1.86ms。
- 8/8 腿 `failures=0`、`session_seats=0`、`active_permits=0`、`errors_total` 空；
  fixture 逐腿校验通过。

**判定：0.72653 ≤ 0.85 → 建议实现安全版。**

## 运行性护栏

- 24 条 campaign 腿（aa / control / control2 / candidate 各 8）全部
  `failures=0`、`session_seats=0`、`active_permits=0`、`errors_total` 空；
  每条腿的 fixture 均为 `threads=201 messages=8625 chars=20177069 focus=8625`。
  明细见 `guardrails.txt`。
- 无 429/503、无 `outcome_unknown`、无 `session_moved`、无 `audit_unavailable`
  （`errors_total` 与 driver 逐请求状态均为空/200）。容器 stdout 只有 dev.py 的
  ready 行，平台自身日志落在卷内，未逐条审阅；判定依据是 metrics 与 driver。
- 唯一异常是协议开发期的 `r1-a1-smoke`：1542 个 429（默认配额被块内 3 token/请求
  耗尽）。它促成了块间补桶停顿，**不进入任何 campaign**，保留作证据。
- 所有卷保留：`peri-r71-gate1a-data`、`peri-r71-g1b-<campaign>-<leg>-data`（24 个）。
  未触碰 `peri-loom` / `peri-loom-data` 及 `fenix-*`、`coze-*`、`open-compute-*`；
  未 prune、未重启 Docker。

## 偏离、限制与未确认项

1. **探针是不安全的**：close 的失败被丢弃、不重试、不写异步记录；每个 close 一个
   goroutine（30s 上限）。它只回答“把等待移走的上限是多少”，不回答安全版能否在
   配额/座位/关闭排空/崩溃恢复约束下保住收益。
2. **A/A 漂移**：同二进制两侧的合并比值只有 0.4pp 余量，位置拆分 0.914、r2 1.092。
   判决依赖候选效应（−27%）远大于漂移；不要把本轮协议用于 10–15% 的效应判定。
3. **块均值水平高于规划假设**：本轮 A 侧 2.33–2.68ms、probe 侧 1.71–2.23ms，
   而规划按 0.545–1.095ms 估算功效。默认配额下的块间停顿已写入协议；块内仍是
   100 个连续请求。绝对 ms 只在本窗口内可比。
4. **close 动作差额（583µs）大于 Gate 1a 的 native p50（155µs）**：块协议测的是
   整个客户端可见的 close 成本，平台记账与调度占其余部分；本轮未进一步分解
   “为什么异步派发把动作均值也降到 89µs”，只报告端到端差额。
5. **正对照的实际注入**：忙等 100µs 在 close 动作上表现为 +149.4µs（含调度开销），
   块级表现为 +13.3%；这验证了协议在“约 150µs 量级”的敏感性，不是严格 100µs 的
   校准。
6. **覆盖范围**：只测 v1.2.7 worker、F1 形状的 `SELECT 1` + close、c1、JSON pipeline、
   v1 审计模式；未测 c8/c16、L1、W1、protobuf、fused close、崩溃/恢复、优雅关闭。
7. **探针分支提交使用了 `--no-verify`**（不应如此）；分支从未推送，仅本地保留。
8. **观测器绝对值**：Gate 1a 的相位时长来自开启观测器的诊断窗口，只用于判据
   （native p50）与机制证据，不得当作端到端性能。

## 证据布局与复现

```
README.md                 本文
run_gate1a.sh             Gate 1a 隔离栈 + F1 c1 + 相位窗口
phase_summary.py          相位流按动作/相位汇总（含追加偏移切片）
run_gate1b.sh             Gate 1b campaign 驱动（aa|control|control2|candidate|smoke）
block_driver.py           块平均驱动（100 请求/块、30 块/腿、块间补桶）
analyze_gate1b.py         合并比值、bootstrap CI、A/A 与正对照判定
legs/f1-c1/               Gate 1a：summary.json、raw.jsonl、相位流、观察器关闭行
legs/<leg>-<campaign>/    每腿：summary.json、raw.jsonl、binaries.txt、seed-verify.txt、
                          metrics-before/after.json；driver.log/container.log 等 *.log
                          按仓库 .gitignore 只保留在本地
analysis-{aa,control,control2,candidate}.json
probe-branch.txt          探针分支、commit、二进制 sha
positive-control.txt      sleep/spin 两版正对照与独立睡眠测量
aa-check.txt              A/A 数字与判定
guardrails.txt            24 腿的运行性护栏
```

复现（本机）：

```bash
cd docs/performance-review/2026-11-05-r71-deferred-close-gate
./run_gate1a.sh
# 探针分支（本地，不推送）：
#   git worktree add -b probe/r70-deferred-close-ceiling /tmp/peri-r70-close 203f241
#   # 应用 probe-branch.txt 描述的改动并构建 linux/arm64：
#   GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
#     -o /tmp/r71-bin/peri-platform-probe ./cmd/peri-platform
./run_gate1b.sh aa
python3 analyze_gate1b.py --root . --mode aa
./run_gate1b.sh control2
python3 analyze_gate1b.py --root . --mode control2
./run_gate1b.sh candidate
python3 analyze_gate1b.py --root . --mode candidate
```
