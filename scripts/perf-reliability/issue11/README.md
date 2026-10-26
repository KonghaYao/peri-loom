# Issue 11 W0-A：Hrana 合同、客户端证据和 raw gate

状态：**INCOMPLETE / 正式性能 NOT_RUN**。本目录不含 Go/Rust 观测实现，不能宣称 W0 完成、正式性能 PASS 或完整 Hrana 兼容。数值全部是设计目标，不是测量结果。不修改 issue8 frozen 合同或其独立 native 口径。

## 执行范围与 SQL

隔离性能 fixture，不改业务表：

```sql
perf_kv(id INTEGER PRIMARY KEY, value INTEGER NOT NULL, payload TEXT NOT NULL)
perf_events(id INTEGER PRIMARY KEY, value INTEGER NOT NULL, payload TEXT NOT NULL)
```

两档 1,000 / 100,000 行。初始 value=0，id=1..scale，payload 是固定公式的 256 字节 ASCII；seed=20261026，没有在线数据源。`contract.fixture_digest` 对初始 `[id,value,payload]` 的规范 JSONL 计算 SHA256，SQL 文件另受合同 manifest 保护。

- R0：`SELECT 1;`
- R1：`SELECT value, payload FROM perf_kv WHERE id = ?;`
- W0：`UPDATE perf_kv SET value = value + 1 WHERE id = ?;`
- W1：INSERT perf_events 后按 id 回读全部 value/payload。
- R2：`SELECT id, value FROM perf_kv WHERE id >= ? ORDER BY id LIMIT 100;`

| 场景 | 严格语义 | W0-A |
|---|---|---|
| H1 | 持久连接 SELECT 1 | 可执行 |
| H2 | 持久连接 by-id 读 | 可执行 |
| H3 | 持久连接 UPDATE，仅单次写计时；验证读取保留但不计入 H3 duration | 可执行 |
| H4 | 持久连接 INSERT 后独立 SELECT，两个 HTTP 动作分别计时，同时记录合计场景耗时 | 可执行 |
| H5 | open → SELECT 1 → close | 可执行 |
| H6 | open → INSERT → 独立 SELECT → close | 可执行 |
| H7 | 同一 pipeline 写与读，不能替代 H4 | 可执行 |
| H8 | 显式事务 10/100 条写，单列 rows/s；转账余额和/原子性 | 占位 |
| H9 | 同库并发与多库混合 | 占位 |
| H10 | 持续运行与维护边界 | 占位 |
| N1 | native 对应读写，保留 #8 独立口径 | 占位 |

H8/H9/H10/N1 均为 `available_in_w0_a=false`，不得用于 W0-A formal PASS。W0-A 未实现转账或事务原子性 oracle，不能用其他编号伪装这些场景。

## 冻结目标与 formal gate

formal-eligible 必须覆盖 H1–H7，每场景至少 5 组 AB/BA 配对（A=baseline、B=candidate），有 fresh/aged 状态；不能只声明 H1。配对按 scenario/state/pair，环境、持久后端、数据/元数据摘要必须匹配，C=1。按 hash 保护的 run.json 中 started_at_unix_ms/completed_at_unix_ms 校验配对执行次序且不重叠，缺窗口为 INCOMPLETE；这些本身不是外部执行真实性证明。warmup≥200（H4 两动作只算一组）；quick timed≥2000；formal timed≥10000。aged 窗口≥3600 秒。

- H4：配对 p50 改善≥20%，p99 不回退；候选每轮 p50≤20ms、p99<100ms。
- H6：配对 p50 改善≥20%，p99 不回退；候选每轮 p50≤35ms、p99<100ms。
- H1/H2/H5：分别保持 p50/p95 回退≤5%；H5 p50≤20ms 是争取目标，不伪造硬阈值。
- H3/H7 没有权威独立数值目标，仍要求可靠性和完整报告。
- H4/H6 同状态候选轮间 p50 max/min≤1.25，不挑最好一轮。
- 健康负载 failure/Unknown/rejected/permit residual=0；已 ACK 写恢复证明必需。客户端回读不是持久恢复证明。

全量 timed operation 和所有已记录请求均报告 p50/p95/p99，另列成功分位数、成功吞吐、拒绝率、失败/Unknown/拒绝数及正确性。不得删掉慢请求/失败请求只展示成功 p99。容量、排队、超载、配额与置信区间（含配对差值区间）在 acceptance 中声明字段，`available_in_w0_a=false`；容量档位为 25/50/75/90%，配额 max_concurrent=4、超载 C=8。这里只冻结未来证据要求，没有实现这些负载或置信区间计算；缺证据 gate 保持 INCOMPLETE，不据此宣称容量、公平性或正式 PASS。超载允许执行前明确拒绝，健康负载不得拒绝。

## HTTP runner

```bash
python3 scripts/perf-reliability/issue11/http/runner.py --help
python3 scripts/perf-reliability/issue11/gate.py --help
python3 scripts/perf-reliability/issue11/attribution/recompute.py --help
```

真实 HTTPS POST `/db/{tenant}/{database}/{branch}/v2/pipeline`；默认验证 TLS；不提供禁用 TLS 验证选项。明文只允许 loopback 并显式 `--diagnostic-plaintext`。凭据只从 `--credential-env` 或 `--credential-file` 读取，证据 Authorization 固定 `Bearer <REDACTED>`，不写 baton 之外的凭据。连接创建/复用由 transport 实际计数；**SDK 对象复用未验证**，本 runner 是标准库 HTTP transport，不是 SDK 兼容性证据。

SQL no-retry；业务幂等 ID 仅关联/恢复，绝不授权自动重试。Unknown 必须终态记录并停止；close 未确认不得算成功，不把超时当回滚证明。E1 known-gap：HTTP v3 写 cursor abort 无可确认回滚证明，本 runner 不解决 E1。

`http/runtime.py` 管环境/结果类型，`runner_engine.py` 管请求证据/生命周期，`workflows.py` 管 H1–H7 SQL，`runner_cli.py` 管 CLI/落盘。gate 分离 raw checks、观测、pairing、status 和 evaluate；无第三方依赖。

## 证据与信任边界

每 run 保留 environment.json、run.json（scenario/arm/pair_index/state/profile）、operations.jsonl、requests.jsonl、request-attribution.jsonl、periodic-events.jsonl、metrics-snapshot-index.json、resources.json、oracle.json，附 SHA256SUMS.json/MANIFEST.json。summary.json **可选且完全不参与完整性或判定**，删除/篡改不改变结果。单 run 从 run.json 读取身份；缺元数据 fail-closed；单 run 不构成完整 formal campaign。

W0-A 真实 runner 只产客户端归因、客户端资源及服务端不可用记录。没有正式性能数据；gate 应为 INCOMPLETE（证据齐备的 quick 最高 PHASE_PASS），不是 formal PASS。所有 raw 工件中的合成标记（包括 campaign/ledger/run 元数据/environment/operation/span/metrics/resources/oracle）、synthetic image tag 均阻止 PASS，summary 除外。正式证据须非合成，且服务端覆盖从实际 span 复算，不信任 server_evidence 布尔或 process_coverage 自报。manifest 只证明内部一致，不证明来源真实性；测试中完整环境元数据的 fixture 仅测试 gate 合同，不是真实基准。

## 归因与 W0-B 依赖（均未实现）

短归因保留全部请求/事件；正式长测保留所有客户端耗时，服务端成功采样并保留全部慢/异常。只在同一 request/correlation 内拼接；每内部动作保留独立 operation/request 身份。跨进程只传关联 ID，每个进程使用自己的单调时钟算 duration，**禁止跨进程时间戳相减**。

同进程树扣除直接子阶段，跨进程 parent 仅关联，不扣除。client 分母为客户端 total；server 每个 root 分别用本进程 duration 作分母。`.root` 是计时容器，自身的未解释余量不能充当阶段（嵌套容器同样如此）；每进程至少95% 的请求满足所有 root 未归因≤10%，其余列出，不能用大量合格 root 稀释另一个未覆盖 root。最慢1%单独排名，不把阶段 p99 相加，不把 server root 加进 client total。process_coverage 每个声明进程必须有 span。

设计阶段层级（不是现有指标）：

- SDK/HTTP：encode/connect/send(write)/wait/read/decode/classify/validate、HTTP 次数、连接复用、close 完成。
- Gateway：auth/route/classify/guarded/intent/worker/native/terminal/audit/respond/encode。
- Worker：lane/admission wait/prepare/reprepare/engine/I/O。
- Authority：append/wal_pages/pin_extend/lease/publish/CAS。
- ObjectStore：新对象/复用/字节/body fsync/目录刷新/锁等待。
- 周期事件：audit head/journal compact/receipt roll/checkpoint/index compact 开始/结束。

必须补齐：

1. Go intent span 区分 guarded 判定调用、durable intent 注册和正式执行调用。
2. Rust admission wait 的 success、timeout、closed、isolated 四类记录。
3. Hrana dispatch 外层 correlation/step 传播，同时保留每内部动作独立 operation_id/request_id。
4. Authority/ObjectStore 阶段及周期事件、metrics、产品资源、恢复/许可、容量/排队/超载/配额/置信区间证据。

诊断合同：`PERI_WORK_METRICS=1`、`PERI_DIAG_GROUP_PHASE=1`、`PERI_ADMISSION_TRACE=1`、`PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS=20`、`PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE=1000`；launcher 用 `--sql-open-observer`，直接 platform 用 `PERI_SQL_OPEN_OBSERVER=1`。必须检查实际 Worker/Authority 的 pid 与 observed_switches；只有 launcher/env 自报不够。诊断开关需配对，不能拿诊断启用与未启用两臂直接比较。

**PERI_METRICS_SNAPSHOT_PATH 是 stdio Worker 退出快照，不能当作 HTTP 全栈归因。** W0-A 不伪造服务端 spans、周期事件或快照；缺失保持 INCOMPLETE。
