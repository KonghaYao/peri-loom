# R66：三个上限探针（legacy settle / Go GC / F1 close）

日期：2026-11-04（CST；legacy settle 探针协议冻结于 2026-11-03）
状态：**探针轮，没有合入任何生产代码**；三个方向全部被上限探针判停。
全部数字均为 **smoke**，不构成对外性能结论、容量或 SLA 证据。

## 目的与背景

R64 把延后耐久账户的待办上限从 256 提高到 4096，X1 c8/c16 的 p95/p99 在冻结判据下通过；
R65 回归门确认 W1/F1/H1 无 >10% 退化。此后 X1 的 c8/c16 尾部与 F1 的 close 成本是两条
仅存的、可能过 10% 门槛的线索，本轮用三个**授权内**的上限探针判断是否值得实现安全版本：

1. **legacy 全队列 settle 跳过**：R37 的延迟耐久让 `audit_file_v1` 的 legacy 路径仍然在
   `awaitOperationCommit` 里等待整个未确认队列；假设跳过它可以降低 X1 尾部。
2. **GC 目标进一步放宽**：R35 已把 GC 目标设为 400%；假设 400→1000/off 能减少 c8/c16 的
   台阶式停顿，若不能则台阶与 GC 无关。
3. **F1 close 成本**：只读 `/v1/metrics`，测量 close 动作的均值与占 F1 p50 的比例，
   为「是否申请 fused close 授权」提供当前数字。

另一个方向（worker fused open 生命周期去重）已由 R24 的站点探针数据关闭：
`begin.entry`/`begin.image`/`begin.call` 合计 **0.005ms/请求**，只占 X1 c16 p50
（1.8–2.6ms）的 **0.2–0.3%**，远低于 10% 门槛，无需新探针。

## 探针 1：legacy 全队列 settle 跳过（smoke，不安全）

- 分支：`probe/r66-legacy-settle-ceiling` @ `066ba9efeae6bd5802ba8f2e007608ab290ac895`
  （**本地，未 push、未合并**）。基线 `main` @ `39e8e6c`。
- 变更：`prototypes/v1/platform/operation_store.go` **+5/−1**。`awaitOperationCommit` 的
  legacy 分支（`completion == nil`，本栈为 `audit_file_v1`）不再调用 `settleOperations()`；
  `waitObserved` 与 `commit.err` 不变。压缩也因此不再由该路径推进（`compactions_delta=0`）。
- 协议：X1 `json-pipeline`（`open` → `[execute("SELECT 1"), close]`），每级 50 请求；
  c1/c8/c16 判决，c4 仅报告；2 轮 × A/B/B/A，共 8 腿；默认配额、无 TLS、**无观测器**；
  每腿独立栈与全新 run 目录。测量用同源 darwin/arm64 二进制；任务书要求的 linux/arm64
  产物一并构建留档但未运行（宿主 worker 为 Mach-O）。
- 冻结判据：c8/c16 的 p95/p99 四单元两轮同向 <1.0，且 c16 p95 或 p99 至少一项 ≤0.90，
  且不出现相反方向 >1.10；c1 p50 护栏 ≤1.10。

| 轮 | c8 p95 | c8 p99 | c16 p95 | c16 p99 |
|---|---:|---:|---:|---:|
| round1（B/A） | 0.160 | 0.169 | **2.237** | **2.123** |
| round2（B/A） | **2.685** | **2.692** | **3.333** | **2.591** |

c1 p50 护栏：0.934 / 0.890 / 合并 0.900，通过。8/8 腿有效（`errors=0`、`429=0`、
`stop=clean`、`unresolved_operations=false`），1,600 个计时请求。

**机制核对**：改动确实生效——`state_lock_wait_ns_total` 每请求中位 **3.01→1.74ms（−42%）**，
c1 尾部 p95 两轮 **6.83→0.57 / 7.57→0.64ms**；`appends=404`、`compactions=0` 不变，
`deferred.pending_max` 696–830 vs 749–800 同量级。但 c8/c16 没有改善：等待没有消失，
只是从 deferred worker 的 `awaitOperationCommit` 推给了读路径的 `settleOperations()`。

**判决：上限不足，停止该方向，不实现安全版本。** c8 两轮方向相反，c16 p95/p99 两轮
2.12–3.33×；探针同时语义不安全（可能留下未确认编辑）。

## 探针 2：GOGC 400→1000/off（smoke，零代码）

- 只改平台子进程环境变量，同一 platform/worker 二进制、同一配置/配额/fixture：
  A=`GOGC=400`（本机生产默认等价）、B=`GOGC=1000`、C=`GOGC=off`（绝对上限，非判决档）。
  `GOMEMLIMIT` 三档均不设；正式腿不开 gctrace。
- 协议：Gate 0 三档各一次（`GODEBUG=gctrace=1`，判据外），随后 2 轮 × A/B/B/A + 每轮一条 C，
  共 10 条正式腿；X1 `json-pipeline`，每级 50 请求；c1/c8/c16 判决，c4 仅报告。
- Gate 0：A **39 次** GC / 堆目标 16–24MB；B **13 次** / 40MB；C **0 次**。首个堆目标
  16→40MB 与 GOGC 400→1000 的 2.5× 一致，证明变量确实到达平台运行时。
- 冻结判据：四单元两轮同向且至少一个 ≤0.90；出现方向相反或任一单元 >1.10 即判停；
  c1 p50 护栏 ≤1.10。

| 单元 | round1 | round2 | 形态 |
|---|---:|---:|---|
| c8 p95 | 2.763 | 1.036 | 方向相反 |
| c8 p99 | 0.727 | 1.114 | 方向相反 |
| c16 p95 | 1.947 | 0.339 | 方向相反 |
| c16 p99 | 1.826 | 0.393 | 方向相反 |
| c1 p50（护栏） | 0.973 | 1.013 | 通过（合并 0.980） |

**绝对上限档 C**：GC 次数 0、RSS 峰值 490.6/491.1 MiB（未 OOM、未触 3 GiB 护栏），
但 round2 的 c16 p95/p99 仍是 A 的 **1.809×/1.819×**；round1 的“改善”（0.346/0.349）
在 round2 反转。慢请求是 **15.7–32.9ms 的连续样本块**，A/B/C 三档随机出现；
零 GC 的腿仍出现 17.7–18.5ms 的 c16 台阶，反证停顿不是 GC assist。

**判决：GC 尾延迟假设否证/不可确认，停止。** 10/10 腿有效、无重跑；单轮比值不可解读为
“B 变快/变慢”（同侧两腿 c8 p95 可相差 21.53 vs 1.52）。

## 探针 3：F1 close 成本与 fused close 上限（smoke，零代码）

- 当前 `main` @ `39e8e6c`（`17854a8` 已撤销 R43 的适配器 fused close）。主协议 19:51:00
  冻结：F1 `json-pipeline`，形状 `[execute SELECT 1, close]`，2 轮 A/A、每轮 50 个 timed
  请求、warmup=0；`/v1/metrics` 窗口恰好覆盖 50 个请求。
- 补充诊断 20:05:00 冻结（非主判据）：1 warmup + 100 timed，逐请求抓 metrics 前后快照，
  用于区分“典型 close”与“周期性停顿落在 close”的离群值。

| 项目 | 实测（smoke） |
|---|---:|
| F1 p50 / p95 / mean（100 样本） | **0.5449ms / 7.6276ms / 1.2820ms** |
| close 次数 / 累计 / 均值 | **100 / 24.8217ms / 0.2482ms** |
| open / statement / close 占 F1 mean | **51.6% / 19.9% / 19.4%**（residual 9.2%） |
| close 均值 / F1 p50（整个 close 入口消失） | **45.5%** |
| 稳健 close（补充中位 / 截尾均值） | 0.2330ms / 0.2268ms（42.8% / 41.6%） |

**关键限制**：`close` 是一次内部 `/v1/sql/close` 调用，包含 worker `close_session` RPC
与平台 close 处理，但不含 R37 的回复后异步 fsync。fused close 只是把 worker close 工作
移入 open，**不会消除 worker close 工作或平台 close 入口**，只省一次内部 RPC/边界；
`[execute, close]` 本来就是一个外部 HTTP pipeline 请求，外部帧数不会减少。
R65（fused close 生效）的 8 条 F1 腿 p50 为 0.465–0.602ms，当前为 0.545ms，
**没有出现与 0.14ms close 动作差一致的系统性 E2E 差异**；可归因部分的下限约 0.02ms
（≈4% of F1 p50）。因此不能把 23–45% 写成 fused close 的 E2E 预期收益。

**判决：不能证明 ≥20%；实现 fused close 属“减少协议帧数”红线，需用户明确授权。**
若要以 ≥20% 为授权依据，必须先做 close-only 的受控 A/B（同栈、同 fixture、配对交替、
至少 2 轮），并明确候选省掉的是 RPC 边界、平台编排还是整个 close 入口。

## 交叉结论

1. **X1 c8/c16 尾部由宿主级暂停主导。** GC=off（GC 次数 0）仍有同样的 17.7–18.5ms 台阶；
   legacy settle 移除把状态锁等待降了 42%，但 c8/c16 尾部无改善，等待只是转移到读路径。
   n=50 的 p95/p99 由 5–8 个连续样本块决定，单轮方向不能当作系统性快慢。
2. **F1 close 是真实成本（≈0.248ms/次），但 45.5% 只是“整个入口消失”的机械上限。**
   fused close 保留 worker close 工作与平台 close 入口，当前没有 E2E 证据；可归因下限约 4%。
3. **worker fused open 生命周期去重已被 R24 数据关闭**（0.005ms/请求，0.2–0.3%），
   不需要新探针。

## 剩余方向与授权边界

- **W1 intent 屏障重排**：R61 探针实测 **−20.9%~−23.1%**，是唯一已测到 ≥10% 的剩余方向；
  它改变写语句的耐久提交时点/顺序，**必须用户明确授权**。R66 未实施。
- **fused close**：未证明 ≥20%，且属“减少协议帧数”红线；需要 close-only 受控 A/B
  与用户明确授权后才能实现。
- **legacy settle 安全版本**：没有 ≥10% 的上限证据，不实现；读路径的 settle 语义
  在 R29/R30 已反复验证，不应再拆。
- **GC 目标**：停止；更宽的 GC 只增加 RSS（C 档约 490MiB），不改善尾部。

## 证据索引

- `legacy-settle-ceiling/`：`protocol.md`、`report.md`、`aggregate.txt`、`legs.txt`、
  `legs/<level>/<leg>/{summary.json,launcher-stop.json,metrics-before.json,metrics-after.json}`。
- `gc-target/`：`protocol.md`、`report.md`、`aggregate.txt`、`legs.txt`、
  `gate0/{A,B,C}/`（gctrace/rss/platform-env/metrics/stop）、`legs/<level>/<leg>/`
  （summary/stop/fixture/metrics/rss/platform-env/gctrace-lines）。
- `f1-close/`：`protocol.md`、`supplementary-protocol.md`、`report.md`、`aggregate.json`、
  `round{1,2}/` 与 `supp/`（summary/analysis/fixture/stop/metrics/sha256）、
  `capprobe/`（能力与 fused-open 只读探针输出）。
- 复制规则：只复制 protocol/report/aggregate/legs 的 `.json`/`.md`/`.txt`；
  排除 `raw.jsonl`、`*.log`、`per-request/*`、`_raw/`、脚本与二进制。原始证据仍在
  `/tmp/peri-r66-probe`、`/tmp/peri-r66-gc`、`/tmp/peri-r66-f1`（未移动、未修改）。
- 脱敏：复制后共 **543 个文件 / 6.40 MiB**；唯一替换是 `f1-close/capprobe/fused-open-probe.txt`
  中的 worker session id → `<REDACTED>`。全树未发现 `Authorization`/`Bearer` 头值或私钥；
  用 6 份 `connection.json` 的 `project_api_key`/`global_api_key` 做精确比对，**0 命中**。

## 限制

- 三个探针都是 smoke：X1 每级 50 请求、F1 每轮 50/100 请求；p95/p99 由少数连续样本主导，
  单轮数字不能作为系统性结论。
- 单机 macOS 26 / arm64、local backend、无 TLS、默认配额；不代表容量、SLA 或生产部署。
- 探针 1 的分支与提交未 push、未合并，且实现语义不安全；它只测量“移除 deferred worker
  侧全队列 settle”的上限，读路径 settle 仍在。
- 探针 2 通过 `/tmp` 的 `dev.py` 副本转发 `GOGC`（仓库 `dev.py` 不转发该变量，仓库文件零改动）；
  macOS 的 `ps` 不显示他进程环境，故用 gctrace 堆目标/次数与 RSS 分层作为运行时证据。
- 探针 3 的补充诊断每次请求前抓一次完整 metrics，会改变系统状态，其 E2E 中位数（1.18ms）
  明显高于主测量 p50（0.545ms），因此只把 close 的中位数/截尾均值当作稳健性检查。
- F1 主测量两轮都在第 14 个请求附近出现约 16.7ms 的确定性停顿，动作均值会随停顿落在
  哪个动作而跳动；分轮 close 均值 0.314/0.183ms 即由此而来。
