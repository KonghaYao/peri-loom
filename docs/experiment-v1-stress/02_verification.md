# Round 2 独立验证：renewal 修复后的 architecture gate

## 当前子阶段结论

**已授权 architecture gate 的 raw 一致性、实际覆盖和已记录正确性验证 PASS；run COMPLETE、exit 0。不是性能 PASS，也不是后续完整 measured matrix 的最终验证。** noisy_tenant C1 明确 SKIP，不作为实际运行 PASS。后续作者实验报告/测量矩阵尚未在本验证中验收。

本 verifier 仅只读解析 E finalized raw、已提交 harness/lifecycle 源码、退出日志及指定 artifact hash，并写本文件。没有执行测试/build、负载、SQL/RPC、fixture API、provision 或信号；没有修改已提交 Round 1/FIX 报告、生产源码或用户 dirty `CLAUDE.md`。

## 身份与 raw 不变证明

- Source/gate：`ae6f0db9259630b0e87c5c40474c57b26f90581b`；run source 与 matching main joint gate 的 source_git 相同。git 当前 HEAD 匹配，仅 `CLAUDE.md` dirty；raw 明确保留 dirty=true，不称工作树完全 clean。
- Fresh fixture：`/private/tmp/peri-stress-renewal-ZednExYd`，config `stress-rarch.json`；不读取/复制凭据。raw run：`/private/tmp/peri-stress-results-h3wtrdm2/run_62d4554dd9321f12`。
- `renewal-architecture-rarch.exit-code=0`；stdout 标注 `raw run recorded` 和 `not a performance conclusion`。
- `run.json` SHA256 `f73be3b91e432b5219e4c720dbe2445a2d12e00ade2948e97edfb8b5b571d59d`；`operations.jsonl` SHA256 `b706ffaad6d258a46a50c63229999cf5da3d71baef98eb85fc7556719138e44a`。独立读取前后相同。
- Harness 现存 artifact 与 run 记录均为 `a910edf120e78bb7fb00271e54a7d69b824b4367f113b777ebf7c558838e8613`；指定新 Go artifact 重算 `8636dd84d2157e89ad506697efa912269e1ce598a7f8a871152b8bc8c493ac8f`；指定 host release native 重算 `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`，与 main 的冻结启动身份一致。
- run 本身只直接记录 harness binary SHA，native/Go 的启动绑定依赖 main/E 的冻结启动记录及此前独立 proof；本次 artifact 当前 hash 不是单独证明每个运行 PID 历史启动身份。status/diff hash 也不是完整 untracked source 内容证明。

## 独立重算与实际覆盖

全部 **14,994** JSONL 行可解析：7,056 gateway RPC、6,466 operation、1,228 client admission drop、184 resource sample、36 session cleanup、20 preflight write ACK、3 lifecycle、1 migration source。6,872 个非空 RPC request ID 均唯一；184 个空 ID 是只读 metrics GET，不是 SQL replay。

逐 operation 检查 **6,466 全部 ACK、correct=true、actual/expected rows hash 相同，failure 为 null**。5 个 preflight 加 24 个 scenario correctness oracle 均 PASS，sum/rows hash/ledger hash 的 actual 与 expected 一致。这里验证的是记录的 oracle 和 ACK 证据，未重新查询实际数据库，也不能从未保留的完整 rows 独立重算持久化内容。

对 **21 个 COMPLETE timed phases**，用 observed timestamp labels 关联该 phase 的原始 operation，不用墙钟推算延迟；从记录的 monotonic workflow/queue durations 重算 count/max/nearest-rank p50/p95/p99，与 summary 全部一致。attempted/completed/ACK、per-phase offered 分母及 ACK/elapsed rate 同样一致：

`7,700 offered = 6,466 attempted/ACK + 1,228 client drops + 6 queue expired`

所有 phase 的 unknown/timeouts/cleanup_unknown/correctness_failures 为 0、errors map 为空。36 session cleanup 没有 failure；raw 中没有失败后重试或将未知结果转 ACK 的证据。

| Case | 实际证据 | 覆盖边界 |
| --- | --- | --- |
| session_cycles | COMPLETE，lifecycle failure=null，1 后置 oracle PASS | reopen 不证明 native cache 清空 |
| many_db | C1/C2 各 W2/S10/M10 完成，12 correctness oracle PASS | C1 独立活跃 DB=1，C2=2；不是四 DB 同时执行或 C4/C8 scaling |
| noisy_tenant | C1 SKIP；C2 primary/noise isolated 与 combined 各 W2/S10/M10 完成，6 oracle PASS | offered 与每租户会话数不等，不能直接判干扰 |
| saturation_admission | C1/C2 各 W2/S10/M10 完成，3 oracle PASS | 实际 admission rejection=false；不证明服务端饱和点 |
| idle_suspend_resume | COMPLETE，suspend/phase poll/resume/active poll 后 1 oracle PASS | lifecycle elapsed 1,424,387,250ns；不是 lease-expiry/authority outage test |
| graceful_migrate | COMPLETE，source snapshot 及目标 active convergence 路径后 1 oracle PASS | lifecycle elapsed 1,198,929,125ns；不是 abrupt failover/kill test |

Lifecycle 源码验证 suspend/resume 会等待 suspended/active，migration 会先读取实际 owner、拒绝同 owner 目标并等待目标 active。raw migration source 是 worker-b，配置目标是另一 worker；本次只读验证没有额外调用 status。最终 poll 完整 response 没有逐条保留，因此 convergence 结果依赖该运行 harness 的执行/分类记录与成功后 oracle，不能声称重新独立读取了最终 placement/epoch。

## Gate-only 与 noisy offered 混杂

总 run elapsed **189.382484750s** 包含初始化、多个 sequential case 与 convergence，不是某个 189s steady-state 测量。每个 timed phase 均 `short_gate_not_benchmark=true`；配置 W2/S10/M10、rate 50、C1/2，没有 30s 正式测量或 C4/C8。

noisy 的 measure 原始分母为：primary isolated C2 **500 offered /485 ACK**；noise isolated C2 **500 offered /495 ACK**；combined C2 各租户 **250 offered**，primary **244 ACK**、noise **245 ACK**。前两者各全 rate 50、两 sessions；combined 总 rate 50、每租户约 25、一 session。裸 QPS 下降不能归为干扰或隔离失败；必须控制这些负载/并发差异才讨论因果。

client admission drop 不是 server quota/admission rejection。本子阶段服务端 admission rejection 没有出现；后续 30s saturation 50/200 和 point/mixed C1/2/4/8 未运行，不能 PASS。

## 资源、观测限制与后续

- 184 samples 的 process/management/native reported map 状态均 available。离散 RSS 峰值 bytes：authority **104,497,152**、platform **23,445,504**、worker-a **50,511,872**、worker-b **47,218,688**，均低于单进程 512MiB guard；不排除采样间瞬时峰值，也不是全系统硬内存上限证明。
- generated request charge **1,798,231 bytes**，低于 256MiB；两 raw 文件共 **10,504,725 bytes**，低于 64MiB。请求计费不是 object-store 实际存储占用。host capacity 48GiB/18 logical CPUs，不由此推断系统容量。
- 顶层 measurement_scopes/service_queue_delay 184 次均 null，不伪造零值；native cumulative maps 不能无差分地解释为本 case 独占成本。三 lifecycle 均 `native_empty_cache_proven=false`，不宣称 cold recovery。
- 只执行 main/E 授权的单 runner 是协调记录；raw 不单独证明整个窗口没有其他 heavy workload。此 verifier 没有新增任何测试/build/负载。
- 独立重算输出：`/tmp/peri-round2-architecture-f-raw-check.log`。Round 1 首失败与 renewal unit fix 报告保持不变；Round 2 成功 gate 支持修复后该架构路径能够完成，但不倒填 Round 1 具体 renewal 时序因果，也不形成 performance improvement 结论。
- 下一测量阶段需单独授权、冻结 source/artifact/config、明确可比 offered load 和正确性 oracle；unknown/abort 仍须真实保留并停止。此文件只验收本次 architecture gate 子阶段。

## 后续 satbase 30s 子阶段：PARTIAL / UNRESOLVED

本附录与前面的 architecture gate PASS 分开：新 saturation runner 已停止，**exit 1、UNRESOLVED**，不能把整轮 Round 2 或完整性能矩阵判 PASS。pointbase/mixbase 据执行者报告尚未 provision/运行，不作为已验收 case。此时仍仅只读验证，不新增测试/build/API/负载或信号。

原始目录 `/private/tmp/peri-stress-results-h3wtrdm2/run_f75a4caf1b5c8bc0`；`measure-satbase.{provenance.json,stdout.log,exit-code}` 保留。provenance/run gate 的 HEAD 为同一 `ae6f0db`，harness/native/new Go SHA 分别匹配前述 a910/3c67/8636；provenance 另记录观察到的服务 executable 路径。这是同一独立 service fixture 内的新 `stress_satbase` project/namespace，不描述为再次重启全部服务。

独立核对 raw **31,073 行**：5,715 RPC、5,487 operation、19,708 client drop、130 resource sample、29 cleanup、4 preflight ACK。分母为 `25,216 offered = 5,487 attempted +19,708 client drops +21 queue expired`；attempted 中 **5,479 ACK +8 unknown**。所有 ACK 的 correct/actual/expected rows hash 一致，1 preflight +3 post-concurrency oracle PASS；不能为未完成的 C8 补一个 correctness PASS。

9 个 C1/C2/C4 W2/S10/M30 phases COMPLETE，逐 raw operation 重算 ACK、延迟分位数和 rate 与 summary 一致；配置 offered rate **200**，三次 30s measure 各 offered 6,000：

| Measure | ACK | Recorded elapsed ns | 该片段 ACK/s（含 drain） |
| --- | --- | --- | --- |
| C1 | 746 | 30,011,052,792 | 24.8575 |
| C2 | 1,517 | 30,041,299,875 | 50.4971 |
| C4 | 1,592 | 30,062,779,792 | 52.9558 |

这些是已完成片段的描述，不是全矩阵性能 PASS、服务最大容量或受控性能提升结论。大量 client drop 不能改写成 server admission reject；同一 service 累计 counters 也不是新项目独占增量。raw 的 short_gate flag 为 false 对应这次配置 M30，不能据此把 W2/S10 也叫正式 30s测量。

C8 仅开始 warmup：planned 400、实际 offered 16、attempted 11、ACK 3，约 82ms 后停止；没有 C8 smoke/measure。第 **31,050** 行 query request `stress_f75a4caf1b5c8bc0_000000030788` HTTP **429**、response 121 bytes，被分类为 `unknown/invalid_envelope`。随后 7 个已 dispatch 的请求因停止取消得到 `request_failed` unknown；8 个 poisoned session 的 cleanup 为 `unresolved_session_not_closed`，未盲目关闭。独立检查所有非空 RPC ID 唯一，没有重试记录；后续失败是已开始请求的完成记录，不是恢复运行。

**不能用 `actual_admission_rejection_observed=false` 抹掉这次原始 HTTP429**：客户端没有通过严格 envelope validation，因而没有将其分类为 admission。baseline `platform/gateway_http.go` 在 admission reserve 失败时使用 server-generated identifier 返回429，位于读取/校验 body 并采用 client request_id 之前；`stress/client.go` 则先验证 echoed request_id 后才分类 server code。这是确定存在的 request-ID 契约缺口和一致解释路径；本 raw 未保存首个 response body，不能声称独立捕获了其具体 server ID 或 JSON error code，更不能把 8 unknown 擅自当安全 rejection/ACK。已反馈 main 由作者定位/修复，不自行降级校验或添加 retry。

130 次 sample 的 RSS peaks bytes 为 authority 43,024,384、platform 24,100,864、worker-a 50,741,248、worker-b 47,104,000，均低于采样 512MiB guard；request charge 1,136,643 bytes、两 raw 文件总 13,366,015 bytes 均在预算内。离散采样限制仍适用。

sat raw 前后 SHA 不变：`run.json` 为 `f3cd45a5dfffeb5dd939ae25989ce488bdf99d64784b176a82ddbd59265ab93e`；JSONL 为 `2e499232016162680b2bb392b40b766774235311405f3a3c9d649c6b39efc2f9`。重算日志 `/tmp/peri-round2-satbase-f-raw-check.log`。保留本次首失败及此前所有 raw；完整性能比较、修复后实测和 point/mixed matrix 仍未验收。
