# R70 Gate 0：会话生命周期在小查询延迟中的占比

日期：2026-11-05
基线：`main` @ `5ef1280`（与远端一致，工作树在测量时干净）
轮次性质：**单场景 smoke 测量（Gate 0）**，只覆盖 F1 / L1 两个场景族，不是发布级 A/B，
也不是容量/SLA 证明。测量期间未安装任何会改变吞吐的观测器。

## 结论

**按冻结判据（动作桶口径）所有取到动作桶的窗口都 ≥ 40% → 建议进入「worker 会话复用」的设计阶段。**

| 窗口 | 场景 / 并发 | 计时样本 | p50 | 轮内中位 | open 均值 | close 均值 | 会话合计 | **占比（校正）** | 占比（未校正） |
|---|---|---|---:|---|---:|---:|---:|---:|---:|
| `f1-c1` | F1 json-pipeline / c1（主） | 15 | 1.3958ms | 3.760 / 0.831 / 4.076 | 208.6µs | 534.1µs | 742.7µs | **53.21%** | 96.68% |
| `f1-c1b` | F1 json-pipeline / c1（稳定性） | 15 | 0.7947ms | 0.886 / 0.710 / 0.794 | 399.6µs | 173.3µs | 572.9µs | **72.09%** | 96.18% |
| `l1-c1` | L1 json-pipeline / c1（主） | 15 | 3.1521ms | 4.471 / 2.564 / 3.152 | 1067.6µs | 1600.2µs | 2667.8µs | **84.63%** | 85.14% |
| `l1-c1b` | L1 json-pipeline / c1（稳定性） | 15 | 4.1400ms | 4.140 / 3.843 / 4.143 | 1100.7µs | 1424.0µs | 2524.7µs | **60.98%** | 62.28% |
| `f1-c8` | F1 json-pipeline / c8（run A） | 120 | 2.0683ms | — | 未取到 | 未取到 | 未取到 | — | — |
| `f1-c8b` | F1 json-pipeline / c8（run B） | 120 | 2.4216ms | — | 1501.6µs | 1279.9µs | 2781.6µs | **114.87%** | 114.87% |

- 五个取到动作桶的窗口里没有任何一个低于 40%；即使把发现（discovery）差值完全不做校正，
  占比仍为 62.3%–114.9%。按冻结判据，**方向成立，进入设计**。
- **但这不是「R67 的 341µs 被证实占 64%」**。冻结公式的分子是平台 `open`/`close`
  动作桶，它是 worker 原生 `open_session` + `close_session` 的**上限**，不是该成本本身：
  Hrana 隐式 open 会把 pipeline 的第一条语句融合进同一个 worker 命令
  （`gateway_fused_open.go` + `worker_rpc/protocol.rs:83`），所以 `open` 桶还包含
  首条语句的执行与结果编码；两个桶也都包含平台准入、审计与会话记账。
- R67 的 341µs（251.4 + 89.8）对**本窗口** F1 p50 的占比：干净窗口
  `f1-c1b` 0.7947ms → **42.9%**；被慢峰污染的 `f1-c1` 1.3958ms → **24.4%**；
  L1 两窗口 10.8% / 8.2%；c8 14.1%。R67 的「64%」只对应 F1 p50 ≈ 0.529ms 的历史窗口，
  **不是稳定属性**。
- 因此设计阶段的第一道门必须是 **worker-native 分解**（worker 侧 span 或等价证据），
  用本轮的 p50 重新计算可回收比例；不要把 53%–115% 直接当作复用可回收的收益。

## 方法

### 冻结公式

对每个 c1 窗口：

```
会话生命周期占比 = (open 均值 × open 次数 + close 均值 × close 次数) / 该场景端到端 p50
```

- 动作次数与累计纳秒来自 `/v1/metrics` 的 `actions_total` 与 `action_latency_ns_total`
  （R7 曾用同一接口测出 `describe` 均值 0.391ms/次）。
- F1/L1 的每次 pipeline HTTP 请求恰好产生 1 次 `open`、1 次 `statement`、1 次 `close`：
  隐式 open 走 `/v1/sql/open`，execute 走 `/v1/sql/statement`，pipeline 的 `close`
  entry 走 `/v1/sql/close`。
- c1 窗口的指标窗口里还包含 harness 启动时的 fixture discovery（1 open + 4 query + 1 close）。
  每个窗口前先跑一次 `run.py --dry-run`（只做 discovery）测出其差值，再从窗口差值中扣除；
  `metrics-delta.txt` 同时给出**校正**与**未校正**两种口径。
- c8 由 `c8_driver.py` 驱动：8 个独立 harness client，每个 3 轮 ×（1 warmup + 5 计时），
  合计 144 次请求 / 120 个计时样本；discovery 在驱动打开指标窗口之前完成，因此无需扣除。

### 为什么动作桶是上限

1. **融合首条语句**：`hranaOpen` 把第一条 execute 作为 candidate 交给网关，worker 的
   `open_session` 命令随后执行该语句（`fused_statement`）。所以 `open` 桶包含首条语句的
   执行与结果编码。L1 的响应是 15.6KB（F1 是 312B），其 `statement` 桶只有 56.7–110.9µs、
    `open` 桶却达 1067.6–1100.7µs，正说明行扫描与结果编码落在 open 桶里。
2. **平台侧记账**：认证、准入、会话记录、审计入队都在两个桶内。
3. **后台耐久竞争**：R37 之后非提交记录的屏障在回复后异步执行。窗口内实测：

| 窗口 | audit fsync | 平均 | journal sync | 平均 | journal appends | audit checkpoint |
|---|---:|---:|---:|---:|---:|---:|
| `f1-c1` | 19 | 1.948ms | 22 | 2.115ms | 38 | 2 |
| `f1-c1b` | 11 | 2.620ms | 11 | 2.875ms | 38 | 2 |
| `l1-c1` | 30 | 1.884ms | 34 | 1.837ms | 38 | 2 |
| `l1-c1b` | 38 | 1.712ms | 36 | 1.808ms | 38 | 1 |
| `f1-c8b` | 7 | 2.386ms | 35 | 2.026ms | 285 | 7 |

  `f1-c1` 的 15 个计时样本合计约 38ms（均值 2.55ms/样本），同窗口后台却有
  37ms 的 audit fsync + 46.5ms 的 journal fsync。慢样本成簇出现
  （5.640 / 5.441 / 5.036 / 5.111ms），与这些屏障事件一致；`f1-c1b` 的
  fsync 次数减半、p50 降到 0.7947ms。**这些后台工作不会因为 worker 会话复用而消失**。

### 环境

- 宿主 macOS 26.5.1 / arm64，OrbStack Docker，`--network host`、`--init`、
  `--stop-timeout 60`、`--security-opt no-new-privileges --cap-drop ALL`；无 TLS。
- 镜像：`ghcr.io/konghayao/peri-loom:v1.2.7`
  （image id `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`）。
- 平台：`/tmp/peri-release-ab/bin/peri-platform-v1.2.7`
  `sha256 95716e4d…`（v1.2.7 发布产物；`main` 相对 v1.2.7 的 Go 差异只有
  `sql_failure.go` 的错误路径证据，不改变 F1/L1 成功路径）。
- worker：`/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`
  `sha256 b17a0708…`（rustc 1.98.1，linux/arm64）。
- 隔离栈：容器 `peri-r70-gate0`、卷 `peri-r70-gate0-data`（全新，测量后保留）、端口 `20731`。
  未触碰任何既有容器/卷（生产 `peri-loom` 与 `fenix-*` / `coze-*` / `open-compute-*` 全程未动）。
- 默认配额，未放宽：`max_concurrent=100`、`max_sessions=16`、
  `requests_per_second=500`、`max_sql_bytes=65536`、`max_result_bytes=4194304`。
- fixture 每个栈重新导入并校验：`threads=201 messages=8625 chars=20177069 focus=8625`
  （`seed-verify.txt`；四项检查全部 OK）。

### 运行

- c1 窗口：`run.py --scenario F1|L1 --protocol json-pipeline --rounds 3 --warmup 1 --samples 5`。
- c8 窗口：`c8_driver.py --concurrency 8 --rounds 3 --warmup 1 --samples 5`。
- 每窗口前后各取一次完整 `/v1/metrics`；只用嵌套键差值，证据中不保存任何凭据。
- 证据目录中的连接文件只保留在 `/tmp/peri-r70-gate0/connection.json`（`chmod 600`），
  未复制进仓库，也未打印。

## 实测细节

### 每个窗口的动作桶（校正后）

| 窗口 | open 次数 / 均值 | close 次数 / 均值 | statement 次数 / 均值 |
|---|---|---|---|
| `f1-c1` | 18 / 208.6µs | 18 / 534.1µs | 18 / 194.6µs |
| `f1-c1b` | 18 / 399.6µs | 18 / 173.3µs | 18 / 191.0µs |
| `l1-c1` | 18 / 1067.6µs | 18 / 1600.2µs | 18 / 110.9µs |
| `l1-c1b` | 18 / 1100.7µs | 18 / 1424.0µs | 18 / 56.7µs |
| `f1-c8b` | 144 / 1501.6µs | 144 / 1279.9µs | 144 / 131.1µs |

完整数字（含未校正口径与 discovery 差值）见 `metrics-delta.txt`。

### F1 的双峰

- `f1-c1`：15 个样本排序后中位 1.3958ms；轮内中位 3.760 / 0.831 / 4.076ms。
  5 个样本落在 4.08–5.64ms，其余 0.67–1.65ms。
- `f1-c1b`：轮内中位 0.886 / 0.710 / 0.794ms，只有 1 个 5.6ms 离群点，p50 0.7947ms。
- 两次 F1 窗口的 p50 中位数 = **1.095ms**；这就是本机 clean 条件下 F1 c1 的代表量级。
  历史窗口的 0.529 / 2.433 / 2.891ms 分别对应「更快的稳态」「发布 A/B 的慢窗口」，
  不能当作同一总体的固定值。
- L1 两次窗口 p50 3.1521 / 4.1400ms，中位 3.646ms；两次的 open/close 绝对均值相差
  不大（open 1067.6→1100.7µs，close 1600.2→1424.0µs），说明 L1 的会话成本相对稳定，
  绝对 p50 漂移主要来自 40 行结果与主机状态。

### R67 对照

| 窗口 | p50 | 341µs / p50 |
|---|---:|---:|
| `f1-c1` | 1.3958ms | 24.4% |
| `f1-c1b` | 0.7947ms | 42.9% |
| `l1-c1` | 3.1521ms | 10.8% |
| `l1-c1b` | 4.1400ms | 8.2% |
| `f1-c8b` | 2.4216ms | 14.1% |
| F1 两窗口中位 p50 | 1.095ms | 31.1% |

R67 的「64%」= 341µs / 0.529ms；它只在 F1 p50 处于最快尾部时成立。本轮的干净窗口
（0.7947ms）给出 42.9%，被后台耐久污染的窗口（1.3958ms）给出 24.4%。**该方向不能
建立在 64% 这个数字上**；能建立的是「动作桶占比 ≥ 40%，值得进入设计」。

## 运行性护栏

- 所有窗口的计时样本：`ok` 100%（15/15 或 120/120），无 429、无 `outcome_unknown`、
  无 `session_moved`、无 `audit_unavailable`、无 SQL 错误。
- `summary.json` 的 `warnings=[]`、`fatal=""`；fixture `complete=true`，四项检查全 OK。
- 每个窗口结束：`active_permits=0`、`unknown_held_permits=0`、`session_seats=0`。
- 容器日志无 error/panic；唯一的 `clean_terminal_recovered` 是第二次启动时的正常 launcher 消息
  （`429` 的 grep 命中是 generation 哈希里的子串，不是 HTTP 429）。

## 未确认项与偏离

1. **c8 run A 没有动作桶**：第一版 `c8_driver.py` 误用了只返回准入子集的
   `MetricsApi.snapshot()`，所以 `f1-c8` 只有 `active_permits` / `session_seats` /
   `rejection_reasons_total`，没有 `actions_total`。其 p50 2.0683ms 有效；run B 用修复后的
   驱动重跑（同一卷、同一参数），同时提供 p50 与动作桶。两个 c8 窗口都如实报告，
   没有据此挑选样本。
2. **增加了两个稳定性窗口**：`f1-c1` 的三个轮内中位跨越 4.9×，触发「F1 p50 是否稳定」的
   预设疑问；因此在同一卷、同一参数下补跑 `f1-c1b`。随后 L1 也补跑 `l1-c1b` 以区分
   「L1 会话占比高」与「该窗口被慢峰污染」。四个 c1 窗口全部报告，未丢弃任何运行。
3. **未做 worker-native 分解**：本轮按任务要求只用 `/v1/metrics` 的动作桶，不装 worker
   探针、不改生产代码。因此无法直接把 `open_session` / `close_session` 从 `open` / `close`
   桶里分离出来；R67 的 341µs 仍是外部估计（来自旧窗口的站点探针）。
4. **主机漂移**：两次 c8 窗口的 p50 分别为 2.0683 / 2.4216ms（+17%），两次 L1 为
   3.1521 / 4.1400ms（+31%）。本轮只做同窗口内的占比，不跨窗口比较绝对延迟。
5. **c8 的公式不适配**：分子是均值、分母是 p50，重尾窗口下占比可以 >100%
   （`f1-c8b` 114.87%）。c8 只用于观察排队是否改变占比，不作主判据。
6. **两次平台启动**：第一次脚本跑完 F1/L1/c8-A 后正常停止容器并保留卷；因为 c8-A 缺指标，
   在同一卷上重启一次平台（launcher 报 `clean_terminal_recovered`，正常）完成 c8-B 与
   两个稳定性窗口。重启前后的数据分属两次进程生命周期，但同一个卷、同一份 fixture。

## 复跑

```bash
cd docs/performance-review/2026-11-05-r70-session-lifecycle-gate0
WORKER=/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7 \
PLATFORM=/tmp/peri-release-ab/bin/peri-platform-v1.2.7 \
./run_gate0.sh
```

脚本会新建卷 `peri-r70-gate0-data`、容器 `peri-r70-gate0`、端口 `20731`，导入并校验
fixture，依次跑 `f1-c1`、`f1-c1b`、`l1-c1`、`l1-c1b`、`f1-c8`、`f1-c8b`，最后生成
`metrics-delta.txt`。连接文件写入 `/tmp/peri-r70-gate0/connection.json`（`chmod 600`），
不进入仓库。测量后保留卷、删除容器；不执行 `docker prune`，不触碰既有容器/卷。

证据文件：`binaries.txt`、`seed-verify.txt`、`seed.log`、`metrics-delta.txt`、
`metrics-after-seed.json`、`metrics-final.json`、各窗口的 `summary.json` / `summary.md` /
`raw.jsonl` / `harness.log` / `metrics-*.json`、`container.log`、`container-companion.log`。
