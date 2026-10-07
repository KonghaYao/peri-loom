# 并发 Session 候选路径：独立静态核验

## 结论与范围

1. **最终文档 STATIC PASS。** 候选路径的 SDK 方法、SQL schema、两个独立 session、holder 释放顺序、提交前缀与 unknown 停止规则与当前入口一致；主入口新增链接与步骤顺序有效。初始 loader 引用错误由作者修正后复核，没有未解决的本次文档 blocker。
2. 本轮只读源码与候选文档，进行 Go 片段语法解析、链接和散列检查；没有执行 SQL、fixture API、Docker、压力、信号、Cargo 或这些候选函数。STATIC PASS 不是 live proof、HTTP typed Busy 根因结论或性能测量通过。
3. 本报告独立于 faultprobe mock 门禁；不把该模块测试数算作并发候选已执行。没有修改作者文档或冻结的 `USER-PATH-VERIFICATION.md`，只新增本报告。

## 最终输入与增量来源

1. A 的 `docs/v1/USER-PATH-CONCURRENCY.md` 最终 211 行，SHA-256 `64f4270f66c478610762fce7e467df12481e425d7267f9982cac216ff2690c13`。
2. 主入口 `docs/v1/USER-PATH.md` 当前 40 行，SHA-256 `5bf2f6f8316b798cdef1e21cdc25e3ec65d789c15582eb2f2f4b120f31ff27b5`。只新增并发第 5 步及顺延旧 5/6；移除该新增步骤并还原编号，精确恢复历史 SHA `87c3e446078704dc88386e44e9de31fc5ff912a67807424affc7627f141eafb1`，没有把旧冻结报告清单改成当前值。
3. 原候选为 208 行，SHA `eccadd72fe0c770a31510fa6671a2a7383e1abaf444811ed24094acba8cf9ea5`，错误称 DATABASE 篇提供 Go 私有配置加载程序。独立读文档确认完整 Go loader 实在 SDK 篇 §2，反馈作者后只修引用和衔接说明；最终明确保留私有加载/CA、替换原常量 smoke 为候选调用、每组传入新 DB，不能沿用旧 smoke 库或把原 smoke 算并发验收。
4. 离线证据位于 `/tmp/peri-concurrency-doc-independent/`。两份输入的 `docs-before.json` 与 `docs-after.json` 一致，SHA-256 均为 `fa7edb98f64226cf2ff2550ccde6a51ff508bc6dadd28b49d4039fd1fec113e0`；`checks.json` SHA-256 `a4863f4191294af4dca42ce3bbb822395a4eda927747d62f0a00e2aa9b243c5a`。

## 实际静态检查

1. 候选有 23 个有序步骤，包含共同前提、三组操作与停止规则；每组新库、正常 CA、私有 credential、至少两 slots/两 concurrent、独立 context 和结果记录均明确。需要操作员另获部署及创建授权，文档本身不是授权。
2. 抽取 4 个 Go code block（共同 helper 与三个候选函数），添加 package/import 后以 `gofmt` 标准输入合并解析，实际退出 0。只有语法解析，不是类型检查、构建或函数执行；SDK 签名通过直接读当前源码核对。
3. 主入口 7 个 Markdown 链接目标均存在，新增链接指向冻结候选；原有步骤字节恢复验证通过。原完整用户路径报告保持历史冻结，不追补其 manifest 冒充当时已有该候选。

## 操作语义独立审查

1. **方法与数据：静态一致。** `sdk.NewClient(endpoint, credential, options)` 返回 client/error；`Open(ctx, sdk.DatabaseID)` 返回 session/error；`Execute(ctx, sql, requestID)`、`Query(ctx, sql, requestID)`、`Close(ctx)` 与代码匹配。两个独立 clients 开两个独立公开 session，不以 UI 的单活动句柄或跨 CLI 命令假设共享事务。
2. **空 rows 不是 null：静态一致。** Rust `src/session.rs` 的 query/step_rows 从空 Vec 构造 rows，空结果编码为 `[]`；Gateway 要求 query result 为数组。Go client 用 JSON decoder 解码到 `[][]any`，`[]` 是非 nil 空 slice，`Session.sql` 又将 nil query result 视为 unknown。`expectRows` 将有效空 slice marshal 为 `[]`，不是 `null`；故候选的空行 oracle 与路径一致。本轮未执行真实空查询。
3. **HTTP 错误不是 native 类型：静态一致。** Gateway 将已完成 SQL 负终态映射成 `sql_error`，并不向 SDK 暴露 typed Busy/Constraint 原因。`knownSQLFailure` 以 `errors.As(*sdk.Error)` 检查 code、非 unknown、ExecutionCompleted/MayHaveCommitted 两项 true；这是 HTTP 协议结构，不是 `LimboError::Busy` 类型证明。标记缺失时 Go client 转成 unknown，候选不会继续 rollback/新 SQL。
4. **持锁与新业务：静态一致。** 同一 `busy_ledger(id INTEGER PRIMARY KEY)` schema；A 未提交行 1，B 读取已提交快照应为空。B 的包含行 2 batch 只调用一次并等待明确失败；A 显式 rollback 后再用不同业务行 3，一次 COMMIT ACK 后关闭健康 A/B，再用新 reader 查 `[[3]]` 和 integrity。行 2 缺失仅为这条已知序列的预期，不推广为任意 sql_error 无副作用。
5. **holder 可以 COMMIT：静态一致。** B 请求位于独立 goroutine，A 的控制线程先执行 COMMIT，再 join B；两 session 的操作互斥各自独立，不在 B 阻塞后才安排 A 提交。buffered completed channel 可以接收已派发结果，不自动发送第二个请求。`launched` 只证明本地 goroutine 启动，文档准确承认它不是服务端 contention/Sleep hook，也不能保证任何调度和网络下都成功。
6. **提交前缀：静态一致。** 一个 batch 先 `BEGIN/INSERT 20/COMMIT`，再新事务重复插入 21；只有明确 SQL 负终态才显式 rollback 后续事务。新 session 应只见 `[[20]]`，证明预期已提交前缀未被后续 rollback 抹去，而不证明任意 batch 全部提交或全部回滚。该单次 batch 的外部结果是负终态，不把其中 COMMIT 描述成另一个已收到的独立 HTTP 成功 ACK；也不将此前已 ACK 提交解释为可以撤销。
7. **unknown 不 cleanup：静态一致。** 没有网络 `defer Close`、自动 rollback/replay；任一未识别错误、unknown、moved、malformed ACK 或 context 到期即返回并停止新 SQL。已派发 B 调用可以被观察/join，但不重发；错误路径保留句柄/事务风险并交运维，不把超时、读回或重新 open 当取消证明。
8. **证据限制：准确。** helpers 是要合入 loader 的候选函数，不是完整审计工具；操作员仍须安全保留请求关联证据，不记录 credential/Authorization/connection 全文。新 reader 只代表新 session，不能冒充冷 cache、fresh worker、真实 takeover 或故障恢复验收。

## 最终冻结与执行限制

1. 本报告结论仅支持提交候选文档及主入口增量；不授权马上执行候选，正文要求压力结束及运维批准新的隔离数据库。
2. native Busy 策略和已有离线测试仅作为文档所指来源，不是本轮重跑，也不能证明原 Round3 HTTP 失败根因。故障、性能和最终压力对抗验证须另行实际执行与独立验证。
3. 本报告完成后冻结，没有真实 fixture 请求或生产修改；旧用户路径静态报告保持原样。
