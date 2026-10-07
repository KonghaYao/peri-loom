# Native SQL 失败诊断：独立静态安全审查

## 结论与边界

1. **本次增量 SECURITY STATIC REVIEW PASS。** 冻结三文件的新增 stderr 记录具有固定字段与固定类别，typed native 分类优先于字符串兜底；计数并发安全，写入返回错误不传播，不改变原 SQL 错误或 public Gateway 终态语义。未发现本次范围内必须阻止合入的安全问题。
2. 本轮只读源码、差异与作者单元测试源码；**执行测试数为 0**。没有 API、SQL、压力、Docker、Cargo、构建或实际 stderr 故障注入；F 独占完整 Cargo/runtime 门禁，本文不重复、不采作者测试结果为自己的执行。
3. 结论仅覆盖新增诊断路径，不是整栈安全、真实故障、压力矩阵或 mixed 根因已确定的证明。R5 mixed 仍 FAIL、whole matrix 仍 partial；最终整体对抗验收不在本轮执行范围。

## 冻结输入与追溯

1. 文件均位于 ROOT `prototypes/v1/src/`，本轮直接读取并与 A 的冻结值核对：

   | 文件 | 行数 | SHA-256 |
   | --- | ---: | --- |
   | `worker_rpc.rs` | 4 | `f7668b43f275cef5ded7c5395aa6cd7d1172219cda22e5e3f0329b017d37e336` |
   | `worker_rpc/server.rs` | 444 | `0090a4896b3d358acd51a8b0603b9f5206328d7f9f5fc12b1bbfb08927b81548` |
   | `worker_rpc/diagnostics.rs` | 359 | `1e0d2b8909f45ef91a1d0d994772c89ad302cf94b4e25ca48e119b1aae64dc01` |

2. 静态证据位于 `/tmp/peri-native-diagnostics-security-static/`。`frozen-inputs.json` SHA-256 `4962a5eadf13815906d8b90ffbe90921cb67210a07ce30cd434b3d0a65f9dd5f`；`review.json` SHA-256 `25494d67af3bd93e9a0373fbfa8b52f3755bf82570a78f97bd0f96b055a7b949`。这些是文件身份和审阅记录，不是运行门禁日志。
3. 补充只读核对 `ownership/types.rs`、`worker_rpc/protocol.rs`、`worker_rpc/sessions.rs`、`session.rs` 与 handle 路径。`git diff --quiet` 对 internal protocol、public `gateway_http.go` 和 `gateway_sessions.go` 实际退出 0；不据此声称全仓库未变。

## 字段与分类

1. `Record` 只有五个字段：`event`、`action`、`request_id`、`cause_class`、`execution_completed`。event 固定为 `native_sql_terminal_error`，action 为 Execute/Query enum 的固定序列化值，cause_class 只能由代码返回的静态字符串产生，completed 固定为 true。
2. 不把 SQL、session、namespace、token、error、原始 cause/display/debug、行数据或原始请求对象加入记录。未知原因输出 `not_determined`，不能退回输出错误文本；typed payload 和包含秘密的上下文也不直接序列化。
3. request_id 是唯一按请求输入保留的字段。入口与 record 均使用现有 `validate_label`：非空、至多 128 字节、ASCII 字母数字及 `-_.`；换行、路径分隔符等不可进入新记录。
4. **验证形状不是秘密清洗。** 合法字符组成的 API key、token、业务值仍可能通过 label 校验，所以调用者必须只提供不含秘密的 opaque trace ID，不能把 request_id 当作传递秘密的用途。本文不声称任意输入 request_id 都已脱敏，也不把它当幂等键或身份凭据。
5. classify 先遍历 typed native error chain：Busy/BusySnapshot 为对应固定类，Constraint/ForeignKeyConstraint/RAISE 为 constraint，Interrupt/ReadOnly/IO 为固定类别；不会从 Constraint 或 RAISE 的 message 识别 LeaseExpired、ReadOnly 或 budget 原因。SQL error 的用户文本也不参与政策兜底，归为 other_native。
6. native InternalError 只匹配明确枚举的完整政策字符串；无 typed native 时先识别 typed std I/O，再仅检查最底层 cause 的 exact policy。上下文中的相似文字、带额外 payload 的前缀/子串或 mock 描述不匹配，不使用 regex/contains 对用户文本推断原因。
7. 这些类别是诊断，不是 ownership、租约、AEAD、存储健康或原始 HTTP mixed 失败根因证明；true completed 也不表示 SQL 成功、事务已回滚或此前提交前缀不存在。

## 插入点、错误保真与增长控制

1. 入口先认证相应 lane、解码、校验版本/request_id 与 stopping，再 dispatch。新增 emit 只在 SQL Execute/Query 已取得 session handle 后、同步 handle 调用返回 Err 的分支；Open/Close、lookup 失败、dispatch 前字节检查、控制面/ownership/GetMetrics/Shutdown/Checkpoint/Compact 不新增该失败记录。
2. Execute 仍只调用一次 `handle.execute`，失败时 emit 后返回同一个 error；Query 仍只调用一次 `handle.query`，Err 分支 emit 后返回原 error。没有 retry、reprepare、自动 rollback、readback、错误替换或转 Unknown 的新分支。
3. diagnostics 只借用 `&Error`，序列化失败返回 None，writer `write_all` 的 I/O Err 被忽略；没有 `?`、显式 panic/unwrap 或错误传播进入响应路径。这里的 best-effort 指返回错误不改变 SQL 结果，不承诺拦截 OOM、任意恶意 Write 实现 panic 或底层无限阻塞。
4. registry `handle` 返回 cloned Arc 后其 mutex 已释放；execute/query 返回时既有 session/operation guard 已退出，再调用 emit。新增状态只含 AtomicUsize，未新增 DB/session/namespace 锁。stderr 自身仍使用进程标准流锁和同步写入，本文不称其为无锁、异步或具有延迟硬上限。
5. 每个 WorkerServer State 共享同一个 Diagnostics；`fetch_update` 原子地只在 count<64 时预留一个名额。Relaxed 不削弱单个原子计数的唯一预留，前置 load 只是快速路径，不能允许并发超过 64。没有按请求/session 重置计数，重新创建 State 才有新计数生命周期。
6. 名额在序列化/写入前消耗，写失败也不归还。非法 ID 短路且不消耗名额。**64 限制的是该 State 的记录预留次数和新增事件增长，不是整个 stderr/底层磁盘硬容量，也不保证 64 次都是成功落盘。** write_all 可以涉及多次底层写入，不将记录数冒充系统调用次数。
7. 64 次后该诊断不再提供新事件；不能假设后续失败仍可观察、用“无新日志”证明成功，或承诺跨 worker 重启、多个 server State 或其他日志源的全局硬上限。计数不触发控制面动作或 SQL 重试。

## 作者测试源码审阅：不是本轮测试 PASS

1. 本轮完整审阅新增模块的六个 `#[test]` 函数，**仅静态审阅，没有执行**：typed native 保留上下文分类、exact policy 限制、五字段/无 error payload、非法 ID 不消耗、并发上限、writer 失败保真。
2. 分类测试包含 Busy 与带 ReadOnly 上下文、Constraint/ForeignKeyConstraint 的误导文本、SqlError/InternalError、IO 与 AEAD 类别；RAISE 对应同一 constraint match arm 已直接审查，但不声称作者有独立 RAISE live 用例。
3. 并发测试源码为 16 个线程各尝试 16 次，断言只分配 64，并验证旧计数不重置。它不模拟多进程全局上限或真实 stderr 文件总大小。
4. failing writer 测试源码循环 100 次 emit_to，writer 始终返回 BrokenPipe，断言该模拟下恰好 64 次 write、原 error 显示与 typed Busy 保持不变。该断言不是本报告作者实际执行的 100/64 测量，也不是对任意 writer 的系统调用次数保证。

## 既有 trusted wire 与最终冻结

1. WorkerResponse.error 仍是原有 `Some(format!("{error:#}"))`，存在于既有认证内部 worker wire；本次 server diff 没有改变该表达式或 execution_completed 规则。它不是新增 stderr 格式中的字段，也不能据本文称既有内部 raw error 已脱敏或对所有内部观察者无秘密风险。
2. public Gateway 的既有 error 分类/旗标路径未因新增诊断改变；本文只证明本次 diff 没扩大公开响应或用 logging failure 替换原 error，不重新认证整套 Gateway redaction 的运行结果。
3. 完成后核对三文件仍匹配冻结身份；本报告冻结供主线程提交。F 的完整运行门禁与后续实际授权各自独立，本文不批准 API、压力、Docker 或最终 whole PASS。
