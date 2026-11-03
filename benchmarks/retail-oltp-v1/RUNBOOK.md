# 零售事务固定基准 v1.0 — 运行手册

契约：`PL-RETAIL-OLTP / 1.0.0`（`benchmark.json` 是唯一权威文本，本文件只解释怎么跑）
profile：`local-8c8g-legacy-hrana-v1`
seed：`20261103`，scale：`full`

本目录是 §九 P0 交付项的实现。**当前没有任何一次正式全量运行被执行**，因此
`certified_driver_sha256` 保持 `null`。任何非 `full` 运行都只是 smoke，不能用于验收或对外声明。

---

## 1. 目录内容

| 路径 | 作用 |
| - | - |
| `benchmark.json` | 契约：数据、业务混合、矩阵、SLO、统计口径、环境、状态与验收规则 |
| `pl_retail/` | 驱动 + runner + 严格解析器 + 评分器（入口 `pl_retail.cli`） |
| `tests/` | 离线单元测试（oracle、计划、评分器正负例、退出码、close 策略） |
| `scorecard-template.json` | 评分卡形状；`status` 为 `null`，不是一次运行的结论 |
| `reference/ddl.sql` | 七张表的建表与索引 |
| `reference/oracle.json` | 独立 oracle 的期望值 |
| `reference/historical-v1.2.6.json` | 历史成绩，仅作背景，不是验收来源 |
| `SHA256SUMS` | 本目录内除自身外全部文件的摘要 |

`reference/reference_driver.py` **不在本次交付中**（`driver.legacy_reference_present: false`）。
历史驱动原字节的 SHA256 是
`5201f42520464b1a32e42408c3bb87734207802a5f795ad2a9d5c4429add68c1`。要恢复可核验性，
必须从交付归档取回原字节后放回该路径；在此之前不得声称参考驱动可复现，也不得把任何文件
改名冒充它。

---

## 2. 前置条件

1. 被测实例满足 `benchmark.json.environment`：
   8 CPU 配额、8 GiB cgroup、客户端与服务端同机、local 后端、固定设备/文件系统/挂载布局、
   legacy completion、object pack 关闭、Hrana 实验入口开启、TLS 校验开启。
2. 项目配额：`max_sessions 16`、`max_concurrent 100`、`RPS 500`、`max_databases 8`。
3. `PERI_SQL_MAX_BYTES` 65,536；结果上限 4,194,304 字节；SQL VM 10,000,000 步、执行上限 5 秒；
   segment RAM 8 MiB、segment disk 64 MiB、native pager 16 MiB/连接。
4. 一次只允许一个 fixture 运行；运行前确认没有别的基准实例共用同一个平台。
5. Python ≥ 3.11，无第三方依赖。

> 生效配置由 `pl_retail.config_audit` 在**每一轮**（性能轮、L1、H1、IDLE）前后各读一次并写入
> `config-before.json` / `config-after.json`。读取失败或出现 `gaps` 都判该轮 INVALID，
> 不允许静默放宽。

---

## 3. 连接文件

`--fixture-file` 是 JSON（**含私钥，权限 600，禁止入库、禁止贴进文档或日志**）：

```json
{
  "endpoint": "https://127.0.0.1:8443",
  "tenant": "peri",
  "project": "retail-bench",
  "global_api_key": "<REDACTED>",
  "project_api_key": "<REDACTED>",
  "ca_file": "/path/to/dev-ca.pem",
  "database_prefix": "retail"
}
```

`endpoint` 必须是 `https://`（本地开发 CA 可接受）。抓取或回显任何带
`Authorization` 的输出时一律替换成 `Bearer <REDACTED>`。

`--lifecycle-file`（可选，全量矩阵必需）描述同一卷上的关停与重启：

```json
{
  "stop": ["<command>", "<arg>..."],
  "start": ["<command>", "<arg>..."],
  "ready_path": "/v1/platform/capabilities",
  "clean_proof_path": "/v1/platform/state/clean-proof",
  "clean_proof_contains": "\"clean\":true"
}
```

没有 lifecycle 时，`run_restart` 与 `verify-restart.json` 会被标为
`configured: false`，全量运行因此不能 PASS——这是有意的 fail-closed。

`--platform-descriptor` 是平台事实（pid、cgroup 路径、worker 卸载探针命令等）的 JSON，
用于资源采样与 IDLE 的卸载证明。没有卸载探针时 IDLE 轮判 INVALID（无法从服务状态证明
worker/数据库已卸载）。

---

## 4. 运行

```bash
cd benchmarks/retail-oltp-v1

# 0) 包自检（离线，不需要平台）
python3 -m pl_retail.cli self-check

# 1) smoke：非正式，禁止用于验收
python3 -m pl_retail.cli run \
  --fixture-file /secure/fixture.json --lifecycle-file /secure/lifecycle.json \
  --platform-descriptor /secure/platform.json \
  --mode smoke --scale 8 --window-seconds 10 --warmups 2 --idle-cycles 1 \
  --output /var/tmp/retail-smoke

# 2) 正式全量（3 轮 × [U1,U4,U8,U16,H8] + L1 + H1 + IDLE + close A/B）
python3 -m pl_retail.cli run \
  --fixture-file /secure/fixture.json --lifecycle-file /secure/lifecycle.json \
  --platform-descriptor /secure/platform.json \
  --mode full --scale 1 --output /var/tmp/retail-full

# 3) 评分（可重复执行；写入 scorecard.json 与每轮 scorecard.json）
python3 -m pl_retail.cli score --evidence /var/tmp/retail-full

# 4) 只看结论（永远 exit 0，不具验收效力）
python3 -m pl_retail.cli summary --evidence /var/tmp/retail-full
```

`run` 先执行矩阵、再按同一评分器打分：退出码等于评分卡状态（见 §5 退出码表），
只有真正 PASS 才返回 0；被前置条件阻塞时返回 4。

### 每轮流水线

新 fixture 与生效配置审计 → 全量导入 → preflight（1 次提交级 checkout + 4 个已知负例）
→ 测前核验 → 每 worker 20 次预热（屏障同时放开）→ 120 秒计时窗口 → 停止派发/排空/退役会话
（≤30 s）→ 测后核验 → 干净关停证明（≤30 s）→ 同卷重启（≤30 s 接受请求）→ 重启后核验。

计时窗口内**不重试任何 SQL**；出现 unknown 立即停止派发并按契约隔离。

### 每轮证据（全部写入 `evidence-manifest.json` 的摘要表）

`round.json`、`config-before.json`、`config-after.json`、`initial-data-manifest.json`、
`attempts.jsonl`、`http.jsonl`、`ledger.json`、`verify-pre.json`、`verify-post.json`、
`verify-restart.json`、`shutdown.json`、`restart.json`、`resource.jsonl`、`scorecard.json`。

行数分三个口径分别记录，永不合并：导入后、preflight 后、计时窗口开始时。

---

## 5. 评分与状态

严格解析器先校验 manifest：缺摘要、摘要不符、记录数不符、文件未被 manifest 覆盖、
run 与磁盘上的轮次集合不一致，全部判 INVALID。随后按维度判定：

| 维度 | 判据 |
| - | - |
| performance | 每档每类成功 p50/p95/p99、业务 goodput；SLO 见下 |
| correctness | 四个读类 + checkout 的业务不变量与账本闭合 |
| recovery | 干净关停、同卷重启、重启后核验 |
| load | 导入行数、preflight、全量数据核对 |
| idle | 10 次「静默 ≥120 s → 证明卸载 → 首次业务 ≤5 s」循环 |
| cleanup_ab | 已知拒绝 close 退休策略 A/B 定向对照 |
| evidence | manifest 与文件一致性、driver/contract 摘要、轮次完整性 |

SLO：读 p95 ≤100 ms、p99 ≤250 ms；checkout p95 ≤250 ms、p99 ≤1,000 ms；5 秒业务期限内确认成功；
非预期错误 0；`service_unknown` / `transport_unknown` / `cleanup_unconfirmed` 0；不变量违规 0。

统计：`business_goodput` = 窗口内开始且在窗口结束前完成的成功次数 ÷ 120（单调时钟）；
每类给 n/p50/p95/p99/max（成功/失败/全部）；每轮 + 中位数（从不取最好一轮）；
3 轮合并 p99 = 排序后 `ceil(0.99 × N)`；合并后每类成功样本 <1,000 判 INCONCLUSIVE；
中止轮容量记 `null`。

退出码（fail-closed）：`PASS 0`、`FAIL 1`、`INVALID 2`、`INCONCLUSIVE 3`、`BLOCKED 4`、`NOT_RUN 5`。
`summary` 永远 exit 0，只用于阅读，不签发任何结论。

`run.certifiable` 还要求 `certified_driver_sha256` 非空且等于本次 `driver_sha256`。
在 §九 (a)–(d) 被接受之前它恒为 `false`，评分卡只记录测量结果。

---

## 6. §九 交付项对照

| 项 | 实现在哪 | 怎么验证 |
| - | - | - |
| (a) 严格证据解析与评分器 | `pl_retail/score.py` | `tests/test_score.py`：缺摘要、空成功 trace、缺窗口、缺重复、摘要被篡改、未被 manifest 覆盖、语句数不符、SLO 越界 |
| (b) 已知拒绝 close 退休策略 | `pl_retail/cleanup_ab.py`、`RunnerConfig.close_policy`、`Session.retire()` | `pl_retail.cli ab-cleanup`；每轮 `cleanup-ab.json`；`tests/test_cleanup_ab.py` |
| (c) 正式 runner 与新驱动摘要 | `pl_retail/runner.py`、`contract.driver_digest()` | `run.json.driver_sha256`；`python3 -m pl_retail.cli digest` |
| (d) 数据与生效配置审计 | `pl_retail/generate.py`、`oracle.py`、`config_audit.py` | `self-check` 复算全部 oracle 值；每轮 `config-before/after.json` |
| (e) 可解释资源指标 | `pl_retail/metrics.py` | `resource.jsonl`，按拒绝原因分类并与业务 id/请求 id/操作 id 关联 |
| (f) smoke 与全量矩阵 | `RunnerConfig.mode`、`scale` | smoke 只能跑小矩阵，评分卡标注 `mode` 并禁止认证 |

### close 退休策略 A/B

策略 A = 原策略（不额外关闭会话）；策略 B = 仅在「对端证明语句未开始」的
`known_failure/rolled_back` 上，额外发一次**独立** `close`（新连接、独立 5 秒预算）。
`service_unknown` / `transport_unknown` / `cleanup_unconfirmed` 在两种策略下都走原有隔离路径，
结构上不可能被退休（`should_retire_session` 只接受该 terminal + `execution_proven_not_started`）。

定向对照同时比较会话容量（占用 `max_sessions` 中多少个）、退休次数与延迟代价：

```bash
python3 -m pl_retail.cli ab-cleanup --fixture-file /secure/fixture.json --ops 40
```

判定 PASS 需要：两侧标签正确、A 侧 0 次退休、B 侧每次已知未执行都退休且全部成功、
unknown 上 0 次退休、B 侧 p95 不超过 A 侧的 2 倍且不多于 A 侧 +5 ms、两侧会话容量一致。

---

## 7. 已知缺口（必须随结论一起声明）

1. **没有正式全量运行**：本交付只有代码、自检与合成证据的离线测试。任何"通过"都不成立。
2. **`certified_driver_sha256` 为 null**：§九 (a)–(d) 尚未被接受。
3. **`reference_driver.py` 缺失**：历史摘要已记录，字节无法复核。
4. **IDLE 依赖卸载探针**：平台描述符未提供可证明卸载的观测时，IDLE 判 INVALID。
5. **全量运行需要 lifecycle 命令**：否则 `verify-restart` 缺失，性能轮 INVALID。
6. **import 行数与契约常量比对**：不一致判 INVALID；不要改常量来迁就实现。
7. **smoke 永不认证**：`run.certifiable` 要求 `mode == full`、全部维度 PASS 且驱动已被认证。

---

## 8. 维护约定

- 改动 `pl_retail/*.py` 会改变 `driver_sha256`（按文件名+内容排序后 SHA256），
  `run.json` 与评分卡会记录新值；不要手写这个摘要。
- 改动 `benchmark.json` 会改变 `contract_sha256`，同时使 `SHA256SUMS` 失效：
  先跑 `python3 -m pl_retail.cli self-check --write-sums` 再提交两者。
- 测试：`python3 -m unittest discover -s tests -v`（全部离线，不连接任何平台）。
- 提交信息用中文全角冒号，例如 `feat(retail-bench)：…`。
