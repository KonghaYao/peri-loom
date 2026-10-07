# Busy4 DEV 候选：独立静态安全审查

## 结论与严格范围

1. **本次增量 STATIC SAFETY REVIEW PASS。** 六文件最终冻结身份匹配；生产变更仅将新 RemoteSession 的 busy timeout 从 1s 改为 4s，未发现新增 SQL/batch replay、事务重开、错误自动 rollback 或全库串行锁路径。一次授权的文档路径修正已闭环，没有未解决的本次 blocker。
2. 本轮只读生产源码、差异、测试源码与文档，**执行测试数为 0**。不编译、不跑 Cargo/API/SQL/压力/浏览器/Docker；F 独占完整 Cargo/runtime 门禁，本文不复制作者测试结果为自己的实测。
3. 这是 DEV 候选安全边界审查，不是运行门禁、mixed 完成、性能收益或 whole PASS。最终联合对抗验证仍待 R7 与部署完成后另行授权。

## 最终冻结身份

1. 路径均相对仓库根目录；直接读取的 SHA-256 如下：

   | 文件 | 行数 | SHA-256 |
   | --- | ---: | --- |
   | `prototypes/v1/src/session.rs` | 185 | `92900e843667d19135069dd34eda5147c91e4f4595e1e3a12223e8b4152e5bae` |
   | `prototypes/v1/tests/busy_wait.rs` | 385 | `8162aabed033ac051e92187871ebd8ffad068fb85abacc510d329b831b9f54c9` |
   | `prototypes/v1/tests/concurrent_sessions.rs` | 264 | `ea02960734a97327b89c8d402956bd3108ca01cc082c1b685712d6b613742caa` |
   | `prototypes/v1/tests/busy_tuning.rs` | 341 | `f3fc7709d530ea59ce8b3b38e9605d22e8a7749687468d739900c16a17286244` |
   | `docs/v1/NATIVE-BUSY-WAIT.md` | 57 | `4e5a85ccb4f1baaa7d1d269a2f06d9c25b929d862bcbeb5745ce226f48e981f4` |
   | `docs/v1/USER-PATH-CONCURRENCY.md` | 212 | `14355413fb4ebb92d1b10b764e91158c01f2e079a98345f08b1091515747f8e7` |

2. 静态证据 `/tmp/peri-busy4-independent-static/final-source-before.json` 与 `final-source-after.json` 一致，SHA-256 均为 `1c1167cb19c50c65065f9fc12e71cb9bd368da514f01832beafe3dfd96c7bf7f`。这是最终六文件范围，不是全仓库或 compiled artifact 门禁。
3. 辅助只读核对既有 `session/drive.rs`、`session/budget.rs` 与 Turso `core/busy.rs`。session 的生产 diff 只有初始化常量 1→4；drive/budget 无本轮 tracked diff，子模块 `git diff --quiet` 退出 0，不声称本轮重编译了 native。

## 事务、预算与并发语义

1. **原 statement 继续，不重放 batch。** timeout 只在 RemoteSession::new 初始化一次，不在每 statement 的 configure 中重复设置。driver 对同一个 Statement 的 Sleep 等待后继续 step；Busy/Interrupt 仍立即返回 typed native 错误，`statement.step()?` 保留 BusySnapshot，不新增 catch/reprepare/retry SQL 的分支。native 锁获取内部的 busy 重试与客户端 SQL replay 不同，本文不混称二者。
2. **true BusySnapshot 不被四秒 handler 修复。** 新测试先在 contender 明确 BEGIN 并读取旧快照，再由另一 session COMMIT 行 10；只调用一次 INSERT，要求 typed LimboError::BusySnapshot、仍 in_transaction、旧快照仍为空、无额外 store I/O。只有调用者显式 ROLLBACK 后才能看到 10，fresh reader 也不得出现失败行 11。这是 meaningful stale-snapshot 序列，不是把普通 Busy 或 HTTP sql_error 当成 BusySnapshot。
3. **交互事务不自动重开。** 生产 Err 路径没有打开新连接、BEGIN、rollback 或清空旧快照。既有 RemoteSession Drop 在实际销毁时仍有 best-effort rollback/close，不是 Busy4 新增的错误恢复，不能据此说系统所有生命周期都没有清理；也不能在 unknown 后盲目 close 来获取取消证明。
4. **已提交 prefix 不随 suffix 失败撤回。** 新 prefix helper 单次 batch 执行 BEGIN/INSERT 20/COMMIT，再开始下一事务；在第 4 个 statement 的 validate_transaction hook 暂停，由 holder 获取锁。后续 BEGIN 的 typed Busy 或 duration error 不得重放 21、擦除 20；旧 contender 与 fresh reader 只应见 20，failed BEGIN 不应留下 active transaction。
5. 既有 Constraint prefix 测试另要求后续事务仍 active，显式 ROLLBACK 只清理后续行，fresh reader 保留已提交行及 integrity。两种失败语义不同，不据 failed BEGIN 推广所有错误均自动结束事务。单次失败 batch 没有独立的成功 HTTP prefix ACK，不能虚构该 ACK；也不能将先前已 ACK 的持久提交描述为可撤销。
6. **默认五秒是整批共享 SQL budget。** execute_bounded 在 query_runner 前只安装一次 ExecutionBudget；每个 statement 与等待共用同一 started、steps/guard，不逐语句重置。driver 在 Sleep 前后/分片间检查 budget 与 local lease，IO/Yield 保留既有检查。该 SQL budget 从 install 开始，不包括此前 session mutex 排队或整个 Gateway 网络生命周期。
7. 新测试用 250ms budget + 150ms prefix hook delay，以及默认 5s + 1.5s hook delay，要求 prefix 后的 suffix 在原预算耗尽时停止，不能再完整获得新的 4s。40ms write query 和入口过期用例保留短预算优先；这些是作者测试源码中的断言，不是本轮测量。
8. **四秒是累计 backoff，不是 wall/FIFO。** core timeout 按 delay schedule 的累计 prior+delay 决定是否继续同一锁事件，实际 timeout instant 仍基于当前 now；调度、执行、I/O 及不同锁事件不被该数字统一硬限。driver 将请求 sleep 分片为最多 10ms 并复查，但 OS 唤醒不是硬实时；5s guard 同样不是撤回已派发 I/O 的取消机制。不存在 FIFO 或任意负载下必然成功的保证。
9. **并发 holder 仍可释放。** 各 session 有自己的 connection mutex；等待 contender 的独立线程不需要同一 session 来 COMMIT。1.5s 释放用例要求 reader 不见未提交行、holder 先提交、同一个 contender 调用最终提交，不发送第二次 batch。观察 hook 基于本地 validate_local 时间，不是生产服务端锁状态 telemetry。

## 测试源码、mock 与诊断限制

1. 静态审阅 busy_wait 6、concurrent_sessions 2、busy_tuning 5 个测试函数的关键序列与 oracle，合计 13 个作者测试函数；**不是 13 项本轮执行 PASS**。文档的作者 20 项聚焦 PASS 包括另 7 项 p6_budget，本文没有重跑或冒充独立接受该计数。
2. concurrent_sessions 覆盖 Memory/Local、ledger/balance/integrity，held-lock 与 fresh operation 使用不同业务 ID，每个 batch 单次调用；其四个 fresh writer 全 ACK 是该测试的要求，不是普遍的公平性、吞吐或并发成功承诺。
3. lease clock/revoke 用例在 ProbeStore.validate_local 中操作 Atomic mock clock/revoked 标记，统计 wrapper 的 get/put/delete/list 调用差值。它们不是真实 Authority RPC、TTL/epoch takeover、namespace admission barrier 或已派发远端 I/O 的完成/取消证明；本轮未重新认证这些完整 fence 门禁。
4. TestStore 的 prefix 暂停与等待观察也是本地测试 hook；physical counter 没有真实 S3/backend completion oracle。真实网络未知结果仍禁 replay/cleanup，不能借 local 预算到期或无新 counter 推断未提交。
5. typed cause 仅来自 native 错误或实际新安全诊断的准确关联；HTTP sql_error 本身不能区分 Busy、BusySnapshot、Constraint。文档保留 R3/R5 缺原始原因的限制，R6 仅一个已关联 busy 事件不能 retrofill 旧失败，也不能推出所有 mixed 失败根因。
6. 测试中的 3.8–5.5s、500ms、150ms 余量等是特定运行断言，受宿主调度影响，不是产品 SLA。本轮不新增/放宽这些测试、不调整超时，也不声称已验证不旋转、延迟收益或完整 mixed architecture oracle。

## 已修正缺陷、剩余边界与冻结

1. 审查开始时 NATIVE-BUSY-WAIT 为 55 行、SHA `50d3fdca357eee5417608c04d31d26969b426414c0a8619ce9dfbf6b22491498`，仍残留旧产物路径。主控明确授权作者仅修文档，最终 57 行采用上表 SHA：保留旧 1s artifact 不覆盖，4s 由 F 独立 release 门禁后使用新 `/tmp/peri-stress-native-busy4-bin`。不将初读六文件假称全程未变；其余五文件身份不变。
2. 该 doc-only delta 不修改 compiled source；F 是否完成门禁与产物身份由其独立证明，本文不要求重跑、不代作 release 构建或 live 认证。旧五文件用户路径静态报告及早期诊断报告未编辑。
3. 剩余限制是候选尚需实际运行门禁、fresh fixture mixed/SQL oracle 与部署验证；长事务、累积等待和现有 unknown 资源占用仍可能失败。静态无新 blocker 不等于这些限制已解决。
4. 本报告只新增于授权路径，完成后冻结，少于 100 行；不授予 API、压力或部署执行权限，也不提前宣布 R7、Linux 或最终联合 whole PASS。
