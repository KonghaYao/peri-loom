# Round 1 独立验证：架构 gate 提前停止

## 结论与范围

**方法与 finalized raw 一致性验证 PASS；整轮架构覆盖 PARTIAL，实际运行结果 UNRESOLVED，exit 1。不是系统性能 PASS。**

本验证只读解析 E 已完成的 raw、退出日志、baseline Git 源码及对应审计记录；没有执行 SQL/RPC、fixture API、负载、build、信号或恢复操作，没有修改生产源码或用户 dirty `CLAUDE.md`。不以未运行 case 的测试/实现存在代替实际实验 PASS。

已独立对照 E 的 `01_experiment_report.md`（202 行）：退出状态、已执行覆盖、ACK/unknown 数、资源峰值和因果/性能限制均与 raw 一致，没有发现报告把未执行项写为 PASS。

## 原始证据与冻结身份

- Baseline HEAD：`96303c021a54624a313dc3bb5ec4c699f5ba4869`。`run.json.source.git_hash` 与 joint gate 的 `source_git` 相同；gate 标注 main、P0–P6 和 joint_pass=true。这是主代理提供的 gate 证据，不是本次重新执行全阶段验收。
- Run：`5ef59625c3b44756`；raw 目录 `/private/tmp/peri-stress-results-h3wtrdm2/run_5ef59625c3b44756`。
- `run.json` SHA256：`cbcd587be9d597cabc9327a1c929d2535f3177bede0ba17272d3a558f2265f36`。
- `operations.jsonl` SHA256：`6ee95fd6f9a86282d97b6997733f620cce34eeb8166b314d1f96b8d682bd03f3`。两 raw 文件在独立读取前后 hash 相同。
- `baseline-architecture-barch.exit-code` 为 `1`；stdout 保留 `harness stopped: run stopped: session_cleanup_not_proven`，没有假成功。
- Fixture config：`/private/tmp/peri-stress-baseline-h16lzhoI/stress-barch.json`。报告不复制凭据；run 中 API/management/noisy key 均为 `[REDACTED]`。
- Raw 记录 harness binary SHA256：`a910edf120e78bb7fb00271e54a7d69b824b4367f113b777ebf7c558838e8613`。run 本身不含 native binary SHA；先前 release build 的可用性证明不能单独替代本轮启动路径/运行进程绑定证明。
- 独立只读重算现存 artifact：`/tmp/peri-stress` 为上述 harness SHA；`/tmp/peri-p6-platform-bin` 为 `f4b568f4f0b8c3ad1fee459ea7e41cc9dbfae9a7b2e69c7edae64047701fc33c`；release native 为 `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`，均与 E/先前冻结记录一致。这是指定文件当前身份的证明，不单凭它宣称所有运行 PID 的启动 artifact 绑定已独立证明。
- Raw 标注 source dirty=true，并记录 status/diff hash；首次只读检查工作树仅 `CLAUDE.md` dirty。后续 B 的获批修复进入工作树时，本验证仍使用 `git show 96303c0:...` 审查 baseline，绝不把修复后的文件解释为失败当时源码。
- HEAD 加 dirty/status hash 并非所有 untracked 内容或 native binary 的完整证明；本报告保留这一 provenance 限制，不宣称从 run.json 独立证明了所有启动 artifact 的不可变性。

## 独立解析和计数核对

JSONL 全部 469 行可解析，类型计数为：272 gateway RPC、80 operation、71 client admission drop、23 resource sample、20 preflight write ACK、2 session cleanup、1 lifecycle。

249 个非空 RPC request ID 均唯一。另有 23 次只读 metrics GET 使用空 request ID；它们不是 SQL 重试。失败 query request 420 在 raw 中只出现一次，此后没有新的 gateway RPC。最终仅记录 poisoned session 的本地 cleanup failure，而不是盲目发送 close/replay。

对两已记录 phase，从 operation 行重新汇总 attempted/completed/ACK 数，重算 nearest-rank p50/p95/p99/max 与 ACK/elapsed rate，均与 run.json 一致。也分别验证：

`offered = attempted + client_admission_dropped + client_queue_expired`

`planned_offers = offered + scheduled_offers_not_emitted`

这些等式和延迟重算验证 raw 汇总方法，而不是扩大测量窗口或证明吞吐能力。

## 正确性与 case 覆盖

| 实际项目 | 独立验证结果 | 边界 |
| --- | --- | --- |
| 5 DB preflight | PASS：各 1,000 accounts、4 ledger rows；sum/hash/ledger hash 的 actual 与 expected 全一致 | raw 中 20 preflight write ACK 按 5 DB 各 4 条对应；本验证没有重新查询数据库或从完整 SQL rows 重算持久化内容 |
| session_cycles | COMPLETE/PASS：lifecycle failure=null，后续 correctness sum/hash/ledger hash 一致 | elapsed 957,104,208ns；`native_empty_cache_proven=false`，不能称 cold recovery/cache-reset PASS |
| many_db C1 warmup | 2s phase COMPLETE，53 ACK，全部 ACK 的 row hashes 与 expected 相同 | `independently_active_databases=1`，不是多个 DB 同时活跃的 scaling 证明 |
| many_db C1 smoke | PARTIAL/UNRESOLVED：请求 10s，约 1.040s 后停止；26 ACK +1 unknown | 没有完成 10s smoke、measure、C2 或本 case 的最终 correctness oracle |
| noisy_tenant / saturation_admission / idle_suspend_resume / graceful_migrate | NOT RUN | 即使 listed/configured 或有单测，也不能 PASS |

many_db 已记录 ACK 总计 79，均 correct=true 且 actual/expected row hashes 相等。unknown 行 correct=false 且 actual 是无可用结果的 hash，不能据此声称已证明数据损坏；同样不能把未知请求算 ACK 或 correctness PASS。

## 提前停止与 unknown 分类

JSONL 第 467 行：`stress_5ef59625c3b44756_000000000420`，POST `/v1/sql/query`，HTTP 409，客户端 failure 为 `unknown/unrecognized_server_code`，unknown=true、timed_out=false。第 468 行对应业务 operation 不 ACK；第 469 行 cleanup 为 `unresolved_session_not_closed`。run 原因 `session_cleanup_not_proven` 是 cleanup 不可证明，并不取代最初 query 错误。

Baseline `stress/client.go` classifier 没有 `session_moved`，未知 server code 保守归 unknown；session 被 poison，停止后不 replay/reuse/blind-close。这个保守停止契约在本轮 raw 中成立，但保守 unknown 不等于实际发生了 mutation。

只读审计 `/private/tmp/peri-stress-baseline-h16lzhoI/audit/events.json` 中对应 request 420 的 sequence 277，action=query，outcome=`session_moved`，project/DB 与本轮主 DB 一致。独立重算所读取 277 条 payload SHA256、previous hash 与连续 sequence 全部通过；这是当前文件链的一致性证明，不是独立签名或不可篡改根证明。

Baseline `platform/gateway_sessions.go` 中 `session_moved` 相关分支位于 worker SQL dispatch 之前，支持本请求在 gateway predispatch 被拒绝的解释。审计没有采集该瞬间 placement phase；**不能直接断言本次事件由 renew、migration 或某个具体时序导致**。

Baseline 源码确有可独立验证的机制问题：`gateway_routing.go` 只允许 active，`dispatchLocked` 将 route error 与真正 binding/address 改变合并并永久置 moved。renew phase 因而可能误伤已有 session。B 被指定实现/单测验证该假设；其修复单测和实际 retest 是后续证据，不倒填为本轮因果事实或本轮 PASS。

## 测量方法、资源与比较限制

- 配置 warmup 2s、smoke 10s、measure 10s，C1/2、offered rate 50；所有已记录 phase 均 `short_gate_not_benchmark=true`。run 总 elapsed 24.301951666s 包含初始化等工作，不是完整 steady-state measure。
- Warmup：planned/offered 100，attempted/ACK 53，client drop 46、queue expired 1；smoke：planned 500、offered 53、attempted 27、ACK 26、client drop 25、queue expired 1、not emitted 447。
- 约 26.43/24.99 ACK/s 只是两短片段的原始描述；client queue drop 不是服务端 admission rejection，raw 标注 actual_admission_rejection_observed=false。未证明服务端 saturation 或最大吞吐。
- noisy tenant 未运行。后续 isolated C2 总 rate 50 与 combined C2 每 tenant 25 的 offered 差异必须记录，不能凭裸 QPS 下降判定干扰。30s saturation 50/200、point/mixed C1/2/4/8 仍需单独完整运行与验证。
- Raw host：arm64、18 logical CPUs、51,539,607,552 bytes RAM（48GiB）。23 次 sample 中各进程状态 available，观察到 RSS 峰值：authority 112,754,688；platform 22,593,536；worker-a 35,176,448；worker-b 35,618,816 bytes，均低于配置单进程 536,870,912 bytes（512MiB）。离散 ps samples 不能排除采样间瞬时峰值。
- generated_request_bytes_charged=577,247，低于累计 256MiB；两 raw 文件共 488,504 bytes，低于 64MiB。累计请求计费不是 object-store 实际存储占用或 native RSS。
- 23 samples 有实际 management/native reported maps，但 top-level measurement_scopes 和 service_queue_delay 为 null；缺失范围/queue 数据不可编造为零。native counters 是进程累计值，不直接解释为本 case 增量，也不把 Execute 延迟等同 COMMIT 延迟。
- Raw 本身没有证明整个窗口无其他 heavy build/load；独占 runner/无重型任务由主代理执行协调，不能由短 raw 自动推导。

## 后续验收条件

1. B 的获批 routing/session 修复冻结后，独立审查及定向 normal/race tests；保持 epoch/owner/role/root 等真正 binding 改变仍被 fencing，metadata/非 serving 状态不错误永久污染 session。不编辑生产源码由 verifier 自修。
2. 主代理更新最终 Git HEAD、joint gate、源码和 binary proof；所有重型 build/test 停止后，只有获授权 E 执行新独立 fixture retest。
3. 保留本次 exit 1/UNRESOLVED raw 与作者 partial 报告；新结果独立成轮，不覆盖失败或把未运行 case 算通过。
4. 完整架构及正确性先过，再讨论足够时长、可比 offered load 的性能与干扰结论。
