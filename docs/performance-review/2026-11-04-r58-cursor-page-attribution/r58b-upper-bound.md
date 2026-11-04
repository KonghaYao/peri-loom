# R58B 上限探针：移除 run_steps 行级检查（不合入）

目的：在写任何生产代码前，测「去掉行级 budget/操作守卫/step 时钟」的端到端上限。
R58 归因给出行级检查 ≈4.3–5.9ms/查询，但该数字被探针自身的 `Instant::now` 放大，
不能直接当上限；本探针直接把这些调用移出循环，测真实天花板。

## 探针改动

分支 `probe/r58b-row-checks`，commit `14aef4e`，只改
`prototypes/v1/src/session/execution.rs::run_steps`：

- `guard.check()` / `deadline.check()`：由每行一次改为每页一次（循环外）；
- `database.operation_until(deadline, false)`：由每步一次改为每页一次（循环外持有读守卫）；
- `guard.enter_step()` / `StepClock::finish()`：去掉逐步 CPU 时钟记账；
- 循环末尾的 `guard.account_steps(...)`：去掉逐步调用，保留页返回点的调用；
- `guard.account_row`（行/字节预算）与 VM progress handler（步数/墙钟）保持不变。

该分支只用于测量上限，**不得合入 main**。

## 环境与二进制

- 两套独立栈（同一平台二进制、各自全新 run dir 与 fixture）：
  基线 `127.0.0.1:19661`（`/tmp/peri-perf/run-r58b-base`）与
  候选 `127.0.0.1:19662`（`/tmp/peri-perf/run-r58b-cand`）。
- 平台（两侧相同）：`/tmp/peri-perf/bin/peri-platform-r58-main`，
  sha256 `f4210c08d97cb84eb7eaa900af9689c5e9d1a688e909853e26a1927e5d6e626b`
  （`main` @ `0a51ab9` 干净树）。
- 基线 worker：`/tmp/peri-perf/bin/peri-loom-v1-r58-main`，
  sha256 `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594`
  （与 R57 合入二进制逐字节一致）。
- 候选 worker：`/tmp/peri-perf/bin/peri-loom-v1-r58b-nocheck`，
  sha256 `745888eb869e466fd2a0234a1cc58a79712381df16104e618f81d56b9416974b`。
- fixture 两侧一致：`threads=201`、`messages=8625`、`content_chars=20177069`、
  `focus_messages=8625`；默认配额；无观测器。
- harness `scripts/perf-reliability/issue28/run.py`，每腿 `--rounds 1 --warmup 1 --samples 5`，
  腿序 A1 → B1 → B2 → A2（A=基线，B=探针）。

## 结果（H1.content-20mb，中位数 ms）

| 协议 | A1 | B1 | B2 | A2 | 基线均值 | 候选均值 | 上限 |
|---|---:|---:|---:|---:|---:|---:|---:|
| json-cursor | 52.65 | 48.74 | 47.65 | 51.35 | 52.00 | 48.20 | **7.3%** |
| protobuf-cursor | 51.72 | 48.52 | 47.54 | 52.16 | 51.94 | 48.03 | **7.5%** |

配对差值：json 7.43% / 7.21%，protobuf 6.19% / 8.86%；两协议、两条腿同向。

## 判决

- **上限 ≈7.3–7.5%，低于 10% 硬门，不实现该方向。**
  即使把行级检查全部删掉（本探针已是不安全版本），端到端也只省约 3.7–3.9ms。
- 不变量：两协议响应字节与基线逐字节一致（json `20,608,525`、protobuf `20,272,006`），
  行数 `8,625`，`warnings=[]`、`fatal=''`、全部样本 `200`/`ok`。
- 机制：worker 是生产者，平台预取（R56，深度 1）只负责把页转码/发送与下一页生产重叠；
  行级检查确实在关键路径上，但其绝对量不足以单独过门槛。
- 剩余 H1 成本（turso VM stepping ≈17ms、页序列化 ≈6.6ms）属于 worker 引擎/协议层改动，
  按既定授权边界需用户明确批准后才能立项；本轮不越界。
