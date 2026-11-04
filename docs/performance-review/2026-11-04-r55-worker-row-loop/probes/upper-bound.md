# R55 上限探针：worker 只读 cursor 行循环

**结论：除 turso `step` 外的行循环开销 ≤3.1ms（4.7%），低于 10% 门槛，不实施。**

## 目的

R53 实测 worker 每页处理 ~1.0ms（34 页 ≈34ms），其中 `native`（含行循环）约 0.8ms/页。
本轮把行循环拆成四段，判断 fencing/预算/克隆里是否有 ≥10% 的可移除成本。

## 方法

- worker 探针构建：`peri-loom-v1-r55-probe`，sha256 `f447547c7d61234418a04b8f8c1827b9cc6c25b3c8299bd57526fa1ffb2fe92d`
  （基于 `1a2d2e0`，仅计时；diff 见 `upper-bound-probe.diff`）。
- 计时点（`run_steps`，仅 `collect && page_size.is_some()`，即只读 cursor 页）：
  - `pre`：`guard.check` + `deadline.check`
  - `gate`：`operation_until` + `guard.enter_step`
  - `step`：`step_with_admission_check` + `step_clock.finish`
  - `post`：`statement.row` + `account_row` + `get_values().cloned()` + `account_steps`
- 计数用全局原子量，只在最后一页（`done=true`）打印一行并清零，避免逐页 stderr 写放大。
- 栈：`127.0.0.1:19611`，平台 `peri-platform-r53-base`（main），新卷 + 标准 fixture；
  `H1.content-20mb` json-cursor，1 warmup + 2 samples。

## 结果（单次查询，34 页 / 8,625 行）

| 段 | 耗时 | 说明 |
|---|---:|---|
| `pre` | 0.40ms | 每行 guard/deadline 检查 |
| `gate` | 1.63–1.64ms | 每行 `operation_until`（fencing/隔离校验）+ `enter_step` |
| `step` | 14.70–15.87ms | turso VM 执行（含 b-tree 读取与值构造） |
| `post` | 0.92–1.02ms | 行取值、预算计账、`Value` 克隆（克隆本身约 1.0ms/查询，见 R53） |
| 合计 | 17.7–18.9ms | 同一查询约 64ms 的 28–30% |

原始输出：`worker-probe.log`；harness：`harness/`。

## 判定

- 除 `step` 外全部可动空间 = pre + gate + post ≈ **3.0–3.1ms（4.7%）**。
- 其中 `gate` 是每行的 fencing/隔离校验（`operation_until`）与 step 额度，不能去除；
  `pre` 是请求 deadline 与预算检查，不能去除；`post` 里唯一可省的是 `Value` 克隆（≈1ms），
  属于 R53 已判死的单趟序列化方向。
- 即便把 pre+gate+post 全部消掉也只有 4.7% < 10%。**不实施**。
- 剩余主体是 turso `step`（14.7–15.9ms，23–25%）；动它属于存储引擎/SQL 兼容面，不在性能轮范围。

## 备注

- 探针版 H1 中位 70.89ms，比干净 worker（63.8–64.7ms）高约 6ms，来自每行 4 次 `Instant::now()`
  （8,625 行）；分段数值仍有效，结论依赖的「step 占绝对多数」不受影响。
