# R63：R62 c1 尾部的归因（测量轮，无生产代码改动）

日期：2026-11-04
状态：测量轮。本记录不含生产代码改动，也不重新裁决 R62 的冻结判据。
结论：R62 的 c1 p95 失败**不是** 256→4096 这个改动造成的。c1 单元上两侧的 5–19ms 尖峰
**形态相同、频次同量级**；c1-only 腿里延后账户的峰值只有 **200–243 条**，从未触及基线的
冻结上限 256，`inline_total=0`，因此该改动在 c1 上**根本不会生效**。尖峰的幅度与该宿主在
同一腿内实测的单次屏障最大值（journal 等待 8.1–12.3ms、audit fsync 7.2–9.8ms）一致。
本记录给出的是**幅度与形态层面的归因**，不是某一次 fsync 事件的因果证明。

R62 的判决维持不变：主指标 PASS、c1 p95 护栏 FAIL、**不合入**。本轮不重跑 R62、不改判据，
R64 只能用**新冻结的判据**重新评估同一候选。

## 问题

R62 的冻结护栏里，c1 p95 合并中位 0.973→6.370ms（+554.9%）判 FAIL，而 c1 p50 与 p99 都
不差于基线（−3.2% / −22.1%）。同一候选在反序诊断里 c1 p95 = 0.54ms。需要回答三件事：

1. 尖峰是不是候选独有？
2. 尖峰落在请求的哪一段？
3. 候选的 256→4096 在 c1 上理论上是否可能生效？

## 方法

两组腿，全部使用 R62 的两个平台二进制（基线与候选），worker 相同，每条腿独立栈、独立
run dir、重新导入同一 fixture：

- **观察器腿 o1–o8**：X1（`json-pipeline`、`SELECT 1`）按合同跑 1/4/8/16 四级、每级 50 请求；
  开启请求相位观察器（`PERI_SQL_OPEN_OBSERVER*`，阈值 0、采样 1000‰），用于把 c1 尖峰
  拆到 span。
- **c1-only 腿 p1–p6**：同一 X1 场景，harness 副本把 `concurrency_levels=(1,)`（补丁
  `harness-c1-only.patch`），只跑 c1、50 请求，**不开观察器**，用于在干净条件下复制 c1 尾部。

顺序：o1–o8 为 B A A B B A A B；p1–p6 为 B A A B B A。端口 19801–19808 / 19901–19906。
每腿跑完都 `stop: clean unresolved: False`；全部腿 `errors=0`、`status_429=0`、
`rejection_reasons={}`、成功请求 `rows=1`。

观察器只用于归因：它会压低吞吐并放大请求计数（本组腿 200 请求/腿），**其绝对值不进入任何
性能结论**。

## 结果一：c1-only 腿（干净条件，无观察器）

| leg | 侧 | p50 | p95 | p99 | max | >2ms |
|---|---|---:|---:|---:|---:|---:|
| p1 | B | 0.43 | 0.55 | 7.69 | 7.69 | 1 |
| p2 | A | 0.45 | 0.59 | 8.70 | 8.70 | 1 |
| p3 | A | 0.44 | 0.56 | 9.81 | 9.81 | 1 |
| p4 | B | 0.46 | 0.55 | 16.67 | 16.67 | 1 |
| p5 | B | 0.52 | 4.65 | 14.42 | 14.42 | 4 |
| p6 | A | 0.44 | 0.53 | 16.72 | 16.72 | 1 |
| **A 腿中位** | — | 0.44 | 0.56 | 9.81 | 9.81 | 1 |
| **B 腿中位** | — | 0.46 | 0.55 | 14.42 | 14.42 | 1 |

- 合并 150 样本：A p50/p95/p99 = 0.44/0.56/9.81ms，>2ms 3 条；B = 0.47/0.77/14.42ms，
  >2ms 6 条。
- 只有 p5 一条腿的 p95 越过 2ms（4.65ms，第 3 高值），其余五条腿的 p95 都在 0.53–0.59ms。
- 两侧的**最大值**同为 7.7–16.7ms 量级；尾部极值不是候选独有。

## 结果二：观察器腿的 c1 单元

| leg | 侧 | p50 | p95 | p99 | max | >2ms |
|---|---|---:|---:|---:|---:|---:|
| o1 | B | 0.46 | 0.90 | 15.23 | 15.23 | 2 |
| o2 | A | 0.48 | 0.66 | 18.56 | 18.56 | 1 |
| o3 | A | 0.44 | 0.62 | 15.55 | 15.55 | 1 |
| o4 | B | 0.47 | 1.12 | 14.78 | 14.78 | 2 |
| o5 | B | 0.48 | 10.72 | 11.59 | 11.59 | 3 |
| o6 | A | 0.44 | 0.58 | 15.52 | 15.52 | 1 |
| o7 | A | 0.47 | 0.59 | 14.63 | 14.63 | 1 |
| o8 | B | 0.46 | 1.42 | 19.51 | 19.51 | 2 |
| **A 腿中位** | — | 0.45 | 0.60 | 15.54 | 15.54 | 1 |
| **B 腿中位** | — | 0.47 | 1.27 | 15.00 | 15.00 | 2 |

- **八条腿每一条都有 ≥1 条 >5ms 的 c1 样本，两侧都有。** 合并 200 样本：A >2ms 4 条、
  B 9 条；max 分别 18.56 / 19.51ms。
- 观察器本身会改变时序，因此本组只用于 span 归因，不能替代 c1-only 组的尾部结论。

## 结果三：尖峰落在哪里（观察器腿，13 条 >2ms 样本）

`observer-spikes.md` 是逐样本明细（`observer_spikes.py` 生成）。形态可归为三类，**两侧都出现**：

| 形态 | 实例 |
|---|---|
| `terminal` 段（延后耐久结算） | o1(B) `open.terminal.io=8.16ms`；o4(B) `open.terminal.io=11.08ms`；o3(A) `open.terminal.io=7.39ms`；o8(B) `terminal.io=11.05ms`、o6(A) `terminal.io=8.06ms`、o7(A) `terminal.io=6.20ms` |
| 结算段之外的 handler 自身时间（`admission` 是包到 handler 结束的外层帧，其“自身时间”是未归入子 span 的部分） | o4(B) `statement.admission=14.27ms`；o3(A) `statement.admission=7.66ms`、`admission=7.66ms`；o6(A) `admission=6.92ms`；o7(A) `admission=7.90ms`；o8(B) `admission=7.95ms` |
| 内层记录之间的空档（不在任何 span 内） | o2(A) 首条内层记录前 **17.95ms**；o5(B) 前间隙 7.83 / 4.05 / 10.19ms；o8(B) 7.00ms；o1(B) `close.intent.io=7.89ms` 且前间隙 6.67ms |

尖峰在腿内的位置分散（o5 三条分别在 +7ms / +28ms / +45ms），不是单次全局停顿把整段
请求一起拖慢；同一腿内其余 47 条样本仍在 0.4–0.5ms。

## 结果四：同一腿内的宿主屏障成本

`metrics-before/after` 差值与 `state_operation_cost` / `audit` 计数器：

| 组 | journal 等待 max | audit fsync 均值 | audit fsync max | head checkpoint |
|---|---:|---:|---:|---:|
| o（A / B 腿中位） | 10.0 / 10.3ms | 4.42 / 4.74ms | 8.6 / 8.0ms | 19.5 / 9.0 |
| p（A / B 腿中位） | 9.8 / 9.1ms | 4.30 / 4.67ms | 8.0 / 8.0ms | 4 / 4 |

- 每条腿（两侧、两组）的单次 journal 等待最大值都在 **8.1–12.3ms**，单次 audit fsync 最大值
  都在 **7.2–9.8ms**——与尖峰幅度同量级。
- c1-only 组的 `head_checkpoint_attempts` 两侧都是 4，**否掉「检查点差异解释 p5」**；
  R61 已独立否证检查点是尾部限速器，本轮不复活该方向。

## 机制核对：256→4096 在 c1 上不可能生效

- c1-only 腿（50 个串行请求）里 `deferred_durability.pending_max` = **243 / 215 / 200**
  （p1 / p4 / p5，基线冻结上限是 256），`inline_total=0`：账户从未达到上限，**内联回退从未触发**，
  未触及候选改动的判定条件。
- 观察器腿里 c1 是每条腿的**第一级**（harness 在一条腿内按 1→4→8→16 顺序执行），c1 阶段
  账户从空开始；`pending_max` 696–780 是后面 c4/c8/c16 级造成的。
- 因此 R62 的 `deferredDurabilityMaximum` 差异在 c1 单元上**结构性地不可见**；c1 的尾部差异
  只能来自宿主抖动或测量噪声。
- diff 核对：本轮无生产代码改动（`git diff` 为空），所有差异都在测量侧。

## 判决与含义

- **归因结论**：R62 的 c1 p95 失败与候选实现无因果关系；它是该宿主上间歇性的屏障抖动
  （单次 fsync 7–12ms）被 c1 的严格串行与"50 样本第 3 高值"的统计口径放大后的结果，
  两侧共有。**这不能推翻 R62 的冻结判决**——R62 的判据在 A/B 前已冻结，事后归因只用于
  **下一轮**的判据设计。
- **不重跑洗判据**：不重跑 R62 的 A/B、不修改 R62 记录中的任何数字、不据此合入
  `perf/r62-deferred-bound`。
- **对 R64 的直接约束**：新判据必须选一个 c1 上的宿主抖动无法触发的形式（例如尖峰计数/形态
  判据，或把 c1 的统计量换成对单次抖动不敏感的度量），并在 A/B 之前冻结。
- **仍未闭合**：`terminal` 段与 handler 外层帧内部没有细粒度 span，本轮不能说"第 X 次 fsync
  造成了第 Y 条样本"；只能说形态、幅度与同腿的屏障最大值一致，且两侧相同。

## 环境与二进制

| 角色 | 二进制 | sha256 |
|---|---|---|
| 平台基线（`main` @ `1399f15`） | `/tmp/peri-perf/bin/peri-platform-r62-main` | `fd959df7a0680f80f7b15e9b86c851b9ca24d555c673e69b2659f0d1f15159a1` |
| 平台候选（`perf/r62-deferred-bound` @ `530fcf7`） | `/tmp/peri-perf/bin/peri-platform-r62-cand` | `98487e5ea6d9e2590a41619ef3848ed8f78072b7aab5909254873985745867a1` |
| worker（两侧相同，v1.2.5） | `/tmp/peri-perf/bin/peri-loom-v1-r58-main` | `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594` |

- 两个平台二进制同一 Go 工具链（go1.27.3）、同模块路径，体积 15,525,650 / 15,525,762。
- 场景 X1：`json-pipeline`、`SELECT 1`、`want_rows`、`expected_rows=1`，每级 50 请求
  （R62 已就 100/级下的 500 rps 令牌桶混杂做过说明，本轮沿用 50）。
- 配额：`max_databases=8`、`max_sessions=16`、`max_concurrent=100`、
  `requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4 MiB`；未放宽。
- 耐久模式：`audit_file_v1`（`fsync_scope=audit_file`）。
- fixture：`threads=201 messages=8625 chars=20177069`，每腿重新导入并校验。

## 证据文件

- `run_leg_observer.sh` / `run_all_observer.sh`：观察器腿驱动与串行编排（端口 19801–19808）。
- `run_leg_c1_only.sh` / `run_all_c1_only.sh`：c1-only 腿驱动与编排（端口 19901–19906）。
- `harness-c1-only.patch`：把 `scripts/perf-reliability/issue28/contract.py` 的
  `concurrency_levels=(1, 4, 8, 16)` 改成 `(1,)` 的完整补丁；其余 harness 代码未改。
- `aggregate.py`：从 `legs/`、`legs-c1/` 复算本文件所有尾部与计数器表格。
- `observer_spikes.py` / `observer-spikes.md`：把每条 >2ms 的 c1 样本按 envelope + 时间包含
  拆成内层记录、span 与空档。
- `legs/oN/`、`legs-c1/pN/`：每腿 `summary.json`、`summary.md`、`raw.jsonl`、
  `metrics-before.json`、`metrics-after.json`。
- `observer/oN.request-phase.jsonl.gz`：观察器腿的原始请求相位记录（每腿约 1.0MB，gzip 后约 105KB）。
- `observer-legs.txt`、`c1-only-legs.txt`：两次编排的完整控制台输出（`.gitignore` 忽略 `*.log`，故用 `.txt`）。

## 复现

```bash
# 观察器腿（每腿独立栈；平台二进制按 A/B 角色切换）
bash run_leg_observer.sh o1 19801 /tmp/peri-perf/bin/peri-platform-r62-cand B
bash run_leg_c1_only.sh  p1 19901 /tmp/peri-perf/bin/peri-platform-r62-cand B

# 复算本文表格
python3 aggregate.py
python3 observer_spikes.py            # 需要 observer/*.jsonl.gz
```

`run_leg_*.sh` 依赖 `/tmp/peri-perf/seed_fixture.py` 与 `/tmp/peri-perf/_snap.py`（与 R62 记录
用的是同一份）；`_snap.py` 只读 `/v1/metrics` 并写成 JSON。

## 未测量 / 剩余风险

- 未做真实磁盘 I/O 隔离：宿主 fsync 延迟（均值 4.1–4.7ms、峰值 7–12ms）是观测事实，
  但没有用 cgroup io 限速或独立磁盘复现尖峰，因此"抖动来自宿主"仍是归因而非受控实验。
- `terminal` 段与 `admission` 外层帧内部没有更细的 span，无法把 8–14ms 精确切到某一次屏障。
- 观察器腿的绝对延迟被观察器抬高，不能用于任何性能比较。
- 本轮没有跑 `go test ./...`：无生产代码改动，无新增测试义务。
