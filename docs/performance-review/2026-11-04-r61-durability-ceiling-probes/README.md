# R61：两项耐久上限探针（W1 intent 等待、X1 周期 checkpoint）

日期：2026-11-04
结论：W1.insert 的 intent 屏障等待值 **8.5–9.5ms（−20.9% / −23.1%）**，是当前唯一被量化到
≥10% 的剩余方向，但它改的是耐久提交时点/顺序，属授权边界，本轮不实现。X1 的周期 checkpoint
已被 Gate 0 证明可以完全跳过（attempts 54→0），却没有带来一致收益，**不是** X1 尾部限速器；
该方向不再申请授权。

## 目的与背景

R60 把 W1 单条写请求的 37.85ms 拆成 worker 写执行 17.6ms + intent 12.6 + audit 3.8 +
terminal 3.8，并判断 intent 的 12.6ms 是排在上一请求延迟会话记录（R37 deferred durability）
之后的排队；同时把 X1 尾部指向 audit/durability 串行段（`audit_checkpoint` 54 次 × 21ms
≈ 1.19s）。两条 ≥10% 方向都在授权边界上，R60 建议先做两个不安全上限探针，用数字决定是否
申请授权。本轮执行这两个探针。

## 环境与二进制

| 角色 | 二进制 | sha256 |
|---|---|---|
| 平台基线（main） | `peri-platform-r58-main` | `f4210c08…` |
| worker | `peri-loom-v1-r58-main` | `a648ecd9…` |
| W1 intent 探针 | `peri-platform-r61-intent` | `6010408d…` |
| X1 checkpoint 探针（修正版） | `peri-platform-r61-nocheckpoint` | `91259bbc…` |

- 探针分支：`probe/r61-w1-intent-ceiling` @ `9cf4911`、`probe/r61-x1-checkpoint-ceiling`
  @ `c59e917`（第一版 `144d781` 已否证，见下）。均不合入。
- 每条腿独立栈（127.0.0.1:19682/19683）、全新 run dir、重新导入同一 fixture：
  `verify threads=201 messages=8625 chars=20177069 focus=8625`。
- 顺序 A/B/B/A；A/B 期间观测器关闭。所有计时请求非 200 计数为 0、`warnings=[]`。
- 驱动脚本：`run_leg.sh`（A/B 腿）、`gate0.sh`（Gate 0）。

## 探针 A：W1 intent 屏障等待

改动：`beginCorrelatedOperation` 提交意图记录后不再 `wait()`，调用方立即 dispatch。记录仍然
入队、仍然落盘，只去掉「等它 durable 再派发」。

| 腿 | 二进制 | 6 样本中位 | 两轮中位 | 样本 |
|---|---|---:|---|---:|
| A1 | baseline | 40.56ms | 48.17 / 39.83 | 6 |
| B1 | intent 探针 | 32.10ms | 30.16 / 34.05 | 6 |
| B2 | intent 探针 | 31.47ms | 31.05 / 31.89 | 6 |
| A2 | baseline | 40.94ms | 40.52 / 41.15 | 6 |

- 配对：A1→B1 **−20.9%**（−8.46ms），A2→B2 **−23.1%**（−9.47ms）；4 轮逐轮中位全部同向。
- 响应字节 252–256（`query_duration_ms` 位数差异），无错误、无 429。

判决：intent 等待的上限是 W1.insert 的 **约 21–23%**。其中约 9ms 是「排队 + 自身屏障」的
组合；一个保留「意图先 durable 再派发」但把意图排在延迟会话记录之前的实现，收益应在这个
上限以内。该方向改 journal 顺序/耐久提交时点，需用户显式授权；本轮不实现。

## 探针 B：X1 周期 checkpoint

### Gate 0（必须在 A/B 之前）

第一版只跳过 `checkpointAfterBatch` 的 `post_batch`。用 `/v1/metrics` 读
`result.audit.head_checkpoint_attempts_total`：seed + X1 后仍为 **56**，因为真正的周期路径是
`audit.go` `appendBatch` 里的 `pre_batch` guard。**该版探针无效**；它跑出的 4 条腿
（`legs-probe-v1/`）只作为「Gate 0 抓错」的证据，不进入判决。

修正版在 `commitHead` 按 mode 跳过除 `shutdown` 外的全部 checkpoint：

| 栈 | `head_checkpoint_attempts_total` | `head_checkpoint_ns_total` | `fsync_count` |
|---|---:|---:|---:|
| baseline（main） | 54 | 1.1447s | 512 |
| 修正探针 | **0** | 0 | 567 |

Gate 0 通过：整个 seed + X1 窗口内周期 checkpoint 完全消失，关闭时的最终 head 仍保留。

### 结果（修正探针，X1 json-pipeline，100 请求/级）

| 腿 | c1 p50 / p99 | c4 p50 / p99 | c8 p50 / p99 | c16 p50 / p99 | 4 级 elapsed 合计 |
|---|---|---|---|---|---:|
| A1 | 0.51 / 22.6 | 0.65 / 146.9 | 1.73 / 148.4 | 28.28 / 161.5 | 1058ms |
| B1 | 0.49 / 16.7 | 0.59 / 39.3 | 3.16 / 128.3 | 27.21 / 219.8 | 1037ms |
| B2 | 0.46 / 14.7 | 9.92 / 68.8 | 2.22 / 42.4 | 28.81 / 75.4 | 1002ms |
| A2 | 0.47 / 8.6 | 0.52 / 139.3 | 2.03 / 154.6 | 6.90 / 135.7 | 938ms |

- p99 在 3/4 对比较里下降（c4 −73% / −51%，c8 −14% / −73%，c16 B1 +36% / B2 −44%），
  但 c16 B1 反向、elapsed 合计 B1 −2.0% / B2 **+6.8%**；单级 elapsed 跨度 82–557ms，
  方差大于效应。
- 另一次 Gate 0 对照（baseline 1018ms vs 探针 728ms，−28.5%）是单对观测，不用于判决。

判决：**checkpoint 不是 X1 尾部限速器**（至少在当前默认 v1 审计文件 + R37 延迟耐久配置下）。
请求在审计 append/checkpoint 之前就已回复，checkpoint 只占用审计写者；去掉它不改变请求路径。
该方向不再申请授权。

## 方法与教训

- **Gate 0 必须在 A/B 之前**：第一版探针跑了完整 A/B 后才做 Gate 0，先得到的是「无效探针 vs
  基线」的噪声；修正后重跑全部 4 条腿。测量轮的成本应花在 Gate 0 上，而不是花在无效 A/B 上。
- 上限探针量化的是「拿掉这个等待/维护后单请求延迟能到多少」，不是安全实现的收益承诺；W1 的
  安全实现必须保留 intent durable-before-dispatch 不变量。

## 判决

- 不合入任何探针；两个 `probe/r61-*` 分支保留为证据。
- W1 intent 方向：等用户授权（改 journal 顺序/耐久提交时点）。
- X1 checkpoint 方向：否证，不再申请。

## 下一轮提示

- X1 的 100–150ms 固定停顿在 c4/c8/c16 都出现、c1 不出现，且与 checkpoint 无关；下一步应在
  worker 会话 open/close 与 platform admission/deferred queue 之间做单窗口归因。
- 授权内剩余项（worker 页响应缓冲 3–8%、平台页路径 <8%、审计编码 1–2%）都低于 10% 门槛，
  不再单独开轮。
- W1 的 worker 写执行 17.6ms（SQL-only 16–18ms）属存储/引擎，需 worker 内部 span。

## 证据文件

- `legs/w1-a1,b1,b2,a2/`：W1 四条腿的 `summary.json` / `summary.md` / `raw.jsonl`
- `legs/x1-c1,d1,d2,c2/`：修正探针四条腿
- `legs-probe-v1/x1-a1,b1,b2,a2/`：第一版无效探针四条腿（仅作 Gate 0 证据）
- `gate0/baseline-metrics.json`、`gate0/probe-metrics.json`：两条 Gate 0 栈的 audit 计数
- `run_leg.sh`、`gate0.sh`、`aggregate.py`：测量与汇总脚本
