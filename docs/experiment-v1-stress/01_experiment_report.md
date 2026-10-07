# 第 1 轮：架构 baseline — PARTIAL / UNRESOLVED

## 结论边界

本轮仅执行一个经主代理明确授权的真实 Gateway 架构 runner，最终退出码为 **1**，raw 状态为 **UNRESOLVED**，原因 `session_cleanup_not_proven`。实验覆盖为 **PARTIAL**，不能将本轮或完整架构矩阵判为 PASS。

5 个初始化正确性 oracle 均 PASS，`session_cycles` 完成且场景后正确性检查 PASS。随后 `many_db` C1 在 smoke 阶段遇到 HTTP 409；工具将其分类为 unknown，停止 SQL，并拒绝继续使用或关闭该不确定 session。后续场景与 measurement 均未执行。

本轮设置为 **gate-only**：计划 measurement 仅 10s，且实际上没有进入任何 measurement 阶段。warmup 的约 26.43 ACK/s 仅是短阶段原始观测，不是稳态 QPS、容量、饱和点或性能改进结论。本轮没有自动重试，没有第二个 runner，没有 faultprobe 或故障信号。

## 证据与版本冻结

| 项目 | 记录 |
| --- | --- |
| Run ID | `5ef59625c3b44756` |
| Source Git / joint gate | `96303c021a54624a313dc3bb5ec4c699f5ba4869` |
| Harness | `/tmp/peri-stress` |
| Harness SHA256 | `a910edf120e78bb7fb00271e54a7d69b824b4367f113b777ebf7c558838e8613` |
| Native Release SHA256 | `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836` |
| Go platform SHA256 | `f4b568f4f0b8c3ad1fee459ea7e41cc9dbfae9a7b2e69c7edae64047701fc33c` |
| Fixture | `/private/tmp/peri-stress-baseline-h16lzhoI`，provision label `barch` |
| Connection | `/private/tmp/peri-stress-baseline-h16lzhoI/stress-barch.json`，私有配置，不复制内容 |
| Joint gate | `/private/tmp/peri-stress-results-h3wtrdm2/joint-pass.json` |
| Raw directory | `/private/tmp/peri-stress-results-h3wtrdm2/run_5ef59625c3b44756` |

Native 完整 SHA 来自主代理指定的 Release 身份与本地 `/tmp/peri-p6-root-final-binary-after.sha256` 清单；Go 完整 SHA 与本地 `/tmp/peri-p6-platform-bin` 文件摘要一致。它们是本轮指定的二进制身份，不是事后由 source HEAD 推算出的二进制摘要。`run.json` 自身只记录 harness 的二进制摘要，未包含这两个服务二进制的启动时摘要。

启动前确认实际 HEAD 与 joint gate 相符，config/gate 权限为 `0600`，output 为 `0700`，没有已有的全局串行锁。joint gate 的 `joint_pass` 为 true、`confirmed_by` 为 main，阶段为 P0–P6；该先验联合门禁不代表本轮压力 PASS。

Raw provenance 的 `dirty` 为 true。主代理明确允许的用户 `CLAUDE` dirty 内容排除在 immutable code 之外，不作为本轮运行代码变更。raw 仍保留以下指纹，不将工作树描述为完全 clean：

- `status_sha256`: `b4efa7568a2f2c5b463d62276d2aeb1538a80ffaa1c3fe5260600043d2f9a742`。
- `tracked_diff_sha256`: `57729f585882d861fdc385cee10f058c4e7cdc0bcd3f7eff0a87fb9095de1848`。
- `untracked_content_provenance`: status hash only；raw 指明性能比较前需主代理冻结的 source manifest，不能把 status hash 当作 untracked 内容的完整证明。

## 方法、拓扑与安全限额

本轮使用实际 Go Gateway HTTP API，通过 Gateway/controller、两个 Release Rust owned workers、authority 和配置的 S3 后端访问完整项目；不是直接 engine CLI 或 mock。主代理报告启动前两个 worker healthy，已完成 label `barch` 的 provision PASS。Endpoint 为本地 fixture 的 19446 端口。运行期间没有重新 provision、构建、主动迁移、kill 或删除 cache。

主机容量来自 raw `actual_host_capacity`：Darwin / arm64，18 logical CPUs，物理内存 `51539607552` bytes（48 GiB）。启动前剩余 tmp 约 54 GiB 是主代理提供的信息，不是本轮工具逐秒磁盘测量。

| 控制项 | 本轮值 |
| --- | --- |
| Rows / synthetic seed | 每 fixture 1000 / `20261007` |
| Planned concurrency | `1,2` |
| Warmup / smoke / measurement | `2s / 10s / 10s` |
| Configured offered rate | 50 workflows/s |
| Max sampled process RSS | `536870912` bytes（512 MiB，逐进程阈值） |
| Generated request data budget | `268435456` bytes（256 MiB） |
| Artifact budget | `67108864` bytes（64 MiB） |
| Max operations per phase | 50000 |
| Response body cap / request timeout | `8388608` bytes / 10s |

调度 offered 与实际 attempted/ACK 分开记录；客户端有界 admission drop 不是服务端 429，也不证明服务饱和。SQL 无隐式重试，每个并发执行槽使用 opaque Gateway session。unknown、已确认 correctness failure 或 cleanup 无法证明时停止，不能通过重试或额外 SQL 掩盖首失败。

### 实际执行命令

以下为本轮已经执行的命令记录，**不是重跑授权**：

```sh
/tmp/peri-stress \
  -connection /private/tmp/peri-stress-baseline-h16lzhoI/stress-barch.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass.json \
  -cases session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate \
  -rows 1000 -concurrency 1,2 \
  -warmup 2s -smoke 10s -measure 10s \
  -offered-rate 50 -max-process-rss-bytes 536870912
```

外层以 `umask 077`、`pipefail` 和 `tee` 保留 stdout/stderr；检查日志及 exit-code sidecar 不存在才允许首次执行，未覆盖旧证据。只有这一个 runner。

## 实际观察窗口

以下时间戳逐字取自 raw，均为 **host wall-clock observed timestamps**，不推断日历正确性、不转换为推测日期，也不据此计算耗时。

| Raw 字段 | 值 |
| --- | --- |
| Joint gate `observed_at` | `2026-10-07T00:18:36.401231+00:00` |
| Run `started_at_observed` | `2026-10-07T00:20:52.363506Z` |
| Run `finished_at_observed` | `2026-10-07T00:21:16.665369Z` |
| Run monotonic `elapsed_ns` | `24301951666`，即 24.301951666s（约 24.302s） |

耗时、latency 使用 Go monotonic elapsed；总窗口包含 fixture 初始化、校验与阶段切换，不能把整个 24.302s 当作统一 SQL measurement 分母。

## 正确性与覆盖

### 已完成 oracle

5 个初始化 fixture 的 raw oracle 均记录：1000 accounts、4 ledger rows，余额期望值与实际值均为 `100000004`，各自 expected/actual account hash 与 ledger hash 相等，状态 PASS。它们验证初始化与已 ACK 的 preflight ledger，不代表未执行场景的正确性。

`session_cycles` raw 状态 COMPLETE，场景后同样记录 1000 accounts、4 ledger rows、余额及两类 hash 一致，correctness PASS。

### 全部计划项状态

| 计划项 | 实际覆盖 | 状态 |
| --- | --- | --- |
| `session_cycles` | 生命周期场景完成；场景后 oracle PASS | COMPLETE，局部正确性 PASS |
| `many_db` C1 warmup | 完整 2s 短阶段；只有 1 个 independently active database | COMPLETE，gate-only |
| `many_db` C1 smoke | 计划 10s，约 1.040s 后 unknown abort | UNRESOLVED，部分执行 |
| `many_db` C1 measurement | 未进入 | NOT EXECUTED |
| `many_db` C2 全部阶段 | 未进入 | NOT EXECUTED |
| `noisy_tenant` | 未进入任何并发配置或阶段 | NOT EXECUTED |
| `saturation_admission` | 未进入任何并发配置或阶段 | NOT EXECUTED |
| `idle_suspend_resume` | 未进入 | NOT EXECUTED |
| `graceful_migrate` | 未进入；没有观察到 destination-already-owner 情况 | NOT EXECUTED |
| SQL 专用矩阵、30s measurement、faultprobe | 本轮未授权执行 | NOT EXECUTED |

未执行不等于 PASS，也不等于因功能不可用而 SKIP。尤其不能用 C1 的 `many_db` 名称宣称多个 DB 同时活跃。

## `many_db` C1 原始阶段结果

| 指标 | Warmup | Smoke（中止） |
| --- | --- | --- |
| Requested duration | 2s | 10s |
| Actual elapsed ns | `2004977417` | `1040380375` |
| Planned offers / offered | 100 / 100 | 500 / 53 |
| Offers not emitted after stop | 0 | 447 |
| Attempted / completed workflows | 53 / 53 | 27 / 27 |
| ACKed workflows | 53 | 26 |
| Client admission dropped / queue expired | 46 / 1 | 25 / 1 |
| Observed offered workflows/s | 49.98146624754708 | 50.94404075104942 |
| ACK/s including drain | 26.434212949541667 | 24.99085971320826 |
| Unknown / cleanup unknown | 0 / 0 | 1 / 1 |
| Timeouts / confirmed correctness failures | 0 / 0 | 0 / 0 |
| Actual server admission rejection observed | false | false |
| Status | COMPLETE | UNRESOLVED |

`completed_workflows` 包含已经返回错误的 workflow，不等于成功；cleanup unknown 是 unknown query 后的清理状态，不是第二次 SQL 执行。客户端 drop/expiry 均不纳入 ACK。

| ACKed HTTP-workflow latency | Warmup（53 samples） | Smoke（26 samples） |
| --- | --- | --- |
| p50 | 37.914500ms | 38.001042ms |
| p95 | 47.206750ms | 47.167292ms |
| p99 / max | 50.860375ms / 50.860375ms | 54.144125ms / 54.144125ms |

这些 latency 来自 `acked_workflow_latency`，不是单独 native SQL latency，也不包含 client queue 的完整 offered-to-response 窗口。小样本、warmup、提前中止和客户端丢弃均限制解释；**约 26.43 ACK/s 不构成性能结论**。

## 首失败与停止语义

首失败记录位于 `operations.jsonl`：

- Request ID：`stress_5ef59625c3b44756_000000000420`。
- Business operation ID：`stress_5ef59625c3b44756_000000000418`。
- 场景/阶段/槽位：`many_db` / `smoke` / slot 0，C1。
- `POST /v1/sql/query`，HTTP 409，response 124 bytes；RPC elapsed `31915416` ns。
- Harness failure：`class=unknown`、`code=unrecognized_server_code`、`unknown=true`、`timed_out=false`；decoded flags 为 `execution_completed=false`、`may_have_committed=false`。
- 查询发起与返回的 raw observed timestamps：`2026-10-07T00:21:16.633365Z` / `2026-10-07T00:21:16.66528Z`。

这些 decoded false flags 不能用来推断未知响应保证未执行或未提交。该请求不是已知 typed terminal `sql_error`；工具没有尝试 rollback、再次 query、重发 request 或 close 不确定 session。

对应 operation 的 `acked=false`、`correct=false` 与 null-response hash 并不证明返回了错误数据：请求失败，没有成功 rows 可供 oracle 比对；阶段 `correctness_failures=0`。不能将其改写为数据损坏，也不能据此宣称 abort 后所有数据仍完整。

紧随其后的 `session_cleanup` 记录 `unresolved_session_not_closed`，unknown=true。由此阶段和整体 reason 为 `session_cleanup_not_proven`。这是主动保留不确定性并停载，不是一个额外 close 请求超时；阶段 timeouts 为 0。没有执行失败后的全量恢复校验。

## 只读 server audit 诊断与待验证假设

主代理只读 audit 定位原始服务器 outcome 为 **`session_moved`**。报告编写时本地只读复核 `/private/tmp/peri-stress-baseline-h16lzhoI/audit/events.json` 的同一 request event，得到：

- `request_id=stress_5ef59625c3b44756_000000000420`。
- `project=stress_barch`。
- `database=local_stress_barch/stress_barch_main/main`。
- `action=query`，`outcome=session_moved`。

这与 harness operation 的 project、tenant、DB、branch 相同；不是跨租户或目标 DB 不匹配的证据。Harness 的 sanitized error 没有保留该服务器原始 code，因此保留两层描述，不将 raw `unrecognized_server_code` 改写掉。

主代理的源码只读诊断指出：基线 `gateway.route` 只接受 active phase，正常 `renew_intent`、`renew_pending`、`renew_worker_pending` 暂态可能变为 route error，再由 dispatch 永久 mark moved，尽管 lease 身份未发生变化。这是一个可构造复现的明确 bug 假设，解释路径与此次 `session_moved` 一致。

**证据缺口：没有 request 420 路由瞬间的 metadata phase / lease identity 快照。** Audit outcome 本身不证明当时正处于上述任何一个 renew phase。因此不声称已观测到具体 phase、确定发生 owner/epoch 变化，或已完成实际因果重验。

主代理报告 B 已在 bounded dev 修正正常 renewal 路由及暂不可用时永久 moved 的处理，保留 native fence，未要求 harness 将 unknown 当成功。该修复不属于本轮基线，不据此追认本轮 PASS；仍需独立 unit gate 与明确授权后的实际回归检验。

## 实际资源与 metrics

Raw 保留 23 个 `resource_sample`，23 个 management metrics 状态均为 available，并有实际 `native` authority/workers map；不是用虚构 0 填充缺失指标。Map 为进程/global scope，可能包含 fixture 启动及此前活动，不把其累计总量当作本轮专属增量或每 query 成本。

| 采样角色 | 最大实际 process RSS bytes | 最大 `ps` CPU percent |
| --- | --- | --- |
| authority | 112754688 | 9.3 |
| platform | 22593536 | 9.4 |
| worker-a | 35176448 | 1.6 |
| worker-b | 35618816 | 1.8 |

所有采样进程 RSS 均低于 512 MiB 的阈值；这不是连续采样间峰值或全系统内存硬上限证明。本轮 stop reason 不是资源越界。RSS 来自本地 PID 的 OS `ps`，不把 segment/cache payload accounting 当作 RSS。`ps` CPU 为 OS lifetime/window metric，不是测量窗口内的精确 CPU 利用率。

顶层 `measurement_scopes` 与 `service_queue_delay` 在所检查的样本中为 null；native map 自带 scope 描述。保留 unavailable/null 的边界，不伪造队列时延或缺失的独立冷缓存证据。普通 session reopen 不证明 worker cache 为空。

`generated_request_bytes_charged=577247`，是 harness 请求生成预算记账，不是 S3 字节、总存储占用或 process RSS。实际 object-store/SQL/WAL 指标保留在 raw map，本文不作吞吐比较。

## 保留产物与后续门禁

- Summary：`/private/tmp/peri-stress-results-h3wtrdm2/run_5ef59625c3b44756/run.json`。
- Per-operation / resource / metrics：`/private/tmp/peri-stress-results-h3wtrdm2/run_5ef59625c3b44756/operations.jsonl`。
- Raw stdout/stderr：`/private/tmp/peri-stress-results-h3wtrdm2/baseline-architecture-barch.stdout.log`。
- Exit code：`/private/tmp/peri-stress-results-h3wtrdm2/baseline-architecture-barch.exit-code`，值为 1。
- 原始 server audit：`/private/tmp/peri-stress-baseline-h16lzhoI/audit/events.json`，只读诊断，不复制私有配置或凭据。

全部保留首失败，不删改 raw，不以修复后成功覆盖此轮。F 的独立验证报告另行产出；本报告不代写独立验证 PASS。

Round 2 之前需要主代理确认修复的回归 gate、冻结新 source/binary 身份、准备独立 fresh fixture，并明确授权实际 runner。验证应同时覆盖不变 lease 身份下正常 renewal、暂不可用可恢复、不误标永久 moved，以及真实身份变化/native fencing 仍拒绝旧 session。若再次 unknown/correctness/cleanup failure，仍须停止、不自动重试。

截至本报告写入，仅执行本轮授权的一个实际 runner；之后只有本地只读取证及本报告写入。未启动下一轮 load/provision/network SQL 或故障操作。完整项目架构、SQL矩阵、30s 性能、故障恢复和修复收益均仍待验证。
