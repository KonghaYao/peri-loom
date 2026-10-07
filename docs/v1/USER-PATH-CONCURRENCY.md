# 并发 Session：候选用户验收路径

本页是 DEV 用户验收建议，不是 live PASS、性能报告或 HTTP Busy 根因证明。
只有压力结束、运维明确批准新的隔离数据库后才执行。不要使用现有用户库、
压力库或故障 fixture；不发 signal、不删 cache、不自动清理数据库。

## 前提与 SDK 衔接

1. 复用 `USER-PATH-SDK.md` 第 2 组完整 Go 程序的私有 connection 文件加载与 CA
   验证代码，准备 Gateway endpoint、项目 SQL credential，以及新建的 Writer placement。
   凭据只从私有文件/受控配置载入，不放 argv、源码、日志或本页示例。
2. 每组单独创建 `busy_path_<随机后缀>` 数据库，tenant 使用本部署配置、branch 为
   `main`。必须获得明确创建成功，不能自动重发未知结果的管理 mutation。
   每组使用全新数据库，因此示例中的整数业务 ID 不会复用此前失败操作。
3. 配额至少允许 **2 个 session slots、2 个并发请求**；新 reader 只在 A/B 关闭后
   打开，仍不超过两个 slots。确认没有其他程序占用这些配额。
4. 私有加载程序分别调用 `sdk.NewClient(endpoint, credential, options)` 得到
   `holderClient` 和 `contenderClient`；`options.RootCAs` 使用正常信任链，
   `options.Timeout` 可设 15s。每组调用使用独立的 30s context，并显式检查返回 error。
   客户端期限必须给 1s native busy 策略留 RPC 余量；这些数值不是完成时限保证。
5. 以下代码可合并进该程序，使用 `context`、`encoding/json`、`errors` 和
   `sdk "peri-loom/platform/sdk/go"`。修改其 `run`：保留私有加载与 CA 设置，
   将原常量查询 smoke 替换为两个 client 的初始化和本页对应函数的一次调用，
   传入该组新建数据库的 `sdk.DatabaseID`，不能沿用 connection 中的旧 smoke 数据库。
   每组使用不同新数据库；直接运行原 smoke 不算并发验收，也不是跨独立 CLI `sql`
   命令拼接事务。
6. 当前 UI 的单一活动 SQL session 不足以完成本页并发序列。
   使用下列两个独立 SDK clients/session，不把 UI 切换选择器当作第二个 session。

共同辅助函数仅生成唯一 request ID、检查已知终态和查询 oracle，不做 retry：

```go
func executeOnce(ctx context.Context, session *sdk.Session, sql string) error {
    requestID, err := sdk.RandomID()
    if err != nil { return err }
    return session.Execute(ctx, sql, requestID)
}

func expectRows(ctx context.Context, session *sdk.Session, sql, expected string) error {
    requestID, err := sdk.RandomID()
    if err != nil { return err }
    rows, err := session.Query(ctx, sql, requestID)
    if err != nil { return err }
    encoded, err := json.Marshal(rows)
    if err != nil { return err }
    if string(encoded) != expected { return errors.New("row oracle mismatch") }
    return nil
}

func knownSQLFailure(err error) error {
    if err == nil { return errors.New("expected terminal SQL failure") }
    var failure *sdk.Error
    if !errors.As(err, &failure) || failure.Code != "sql_error" ||
        failure.OutcomeUnknown || !failure.ExecutionCompleted || !failure.MayHaveCommitted {
        return err
    }
    return nil
}

func verifyFresh(ctx context.Context, client *sdk.Client, database sdk.DatabaseID, expected string) error {
    reader, err := client.Open(ctx, database)
    if err != nil { return err }
    if err := expectRows(ctx, reader, "SELECT id FROM busy_ledger ORDER BY id;", expected); err != nil {
        return err
    }
    if err := expectRows(ctx, reader, "PRAGMA integrity_check;", `[["ok"]]`); err != nil {
        return err
    }
    return reader.Close(ctx)
}
```

## 一、持锁一致读、竞争终态与释放后的新业务

1. A 建表，`BEGIN IMMEDIATE` 后插入行 1，不提交。B 纯 `SELECT` 应看不到行 1。
2. 保持 A 的锁，B 一次执行包含行 2 的事务并等待终态，不自动重放。
   此时故意不释放 A；不是测试“B 应成功”。
3. 只在收到符合协议的已知 `sql_error` 后，A 显式 `ROLLBACK`，B 再读应为空。
   然后 B 对**新的业务行 3**调用一次，必须获得 COMMIT ACK；不复用失败行 2。
4. 关闭健康 A/B 后，新 reader 应得到 `[[3]]` 与完整性 `ok`。

```go
func heldLockPath(ctx context.Context, holderClient, contenderClient *sdk.Client, database sdk.DatabaseID) error {
    holder, err := holderClient.Open(ctx, database)
    if err != nil { return err }
    contender, err := contenderClient.Open(ctx, database)
    if err != nil { return err }
    if err := executeOnce(ctx, holder, "CREATE TABLE busy_ledger(id INTEGER PRIMARY KEY);"); err != nil {
        return err
    }
    if err := executeOnce(ctx, holder, "BEGIN IMMEDIATE; INSERT INTO busy_ledger VALUES(1);"); err != nil {
        return err
    }
    if err := expectRows(ctx, contender, "SELECT id FROM busy_ledger ORDER BY id;", "[]"); err != nil {
        return err
    }
    failure := executeOnce(ctx, contender, "BEGIN IMMEDIATE; INSERT INTO busy_ledger VALUES(2); COMMIT;")
    if err := knownSQLFailure(failure); err != nil { return err }
    if err := executeOnce(ctx, holder, "ROLLBACK;"); err != nil { return err }
    if err := expectRows(ctx, contender, "SELECT id FROM busy_ledger ORDER BY id;", "[]"); err != nil {
        return err
    }
    if err := executeOnce(ctx, contender, "BEGIN IMMEDIATE; INSERT INTO busy_ledger VALUES(3); COMMIT;"); err != nil {
        return err
    }
    if err := holder.Close(ctx); err != nil { return err }
    if err := contender.Close(ctx); err != nil { return err }
    return verifyFresh(ctx, holderClient, database, "[[3]]")
}
```

5. **证据/限制**：保留 request ID、终态 flags、各 ACK 与 oracle，不记录凭据。
   SDK 只暴露类型化 `*sdk.Error` 的 `sql_error`，HTTP 不携带 native Busy 原因，
   因而本组不能仅凭 code 证明 `LimboError::Busy`，也不能声称恰好 1s 返回。
   typed native Busy 的证据来自离线 `tests/concurrent_sessions.rs` / `tests/busy_wait.rs`。
   行 2 缺失是本组特定序列的 oracle，不是任意 `sql_error` 都没有副作用的承诺。

## 二、独立控制线程释放 holder，竞争调用可以进展

1. 使用另一新库，A 持锁写行 10；B 先查询，仍只读已提交快照。
2. 在独立 goroutine 中启动 B 的一次事务调用。A 的控制线程执行 `COMMIT`，
   **先释放 holder，再等待 B 的终态**；绝不能同步阻塞在 B 后才计划提交 A。
3. 两个 COMMIT 均 ACK 后关闭健康会话，新 reader 应得到 `[[10],[11]]`。
   单线程 SDK session 本身不能在一个阻塞调用中执行另一条 COMMIT。

```go
func holderReleasePath(ctx context.Context, holderClient, contenderClient *sdk.Client, database sdk.DatabaseID) error {
    holder, err := holderClient.Open(ctx, database)
    if err != nil { return err }
    contender, err := contenderClient.Open(ctx, database)
    if err != nil { return err }
    if err := executeOnce(ctx, holder, "CREATE TABLE busy_ledger(id INTEGER PRIMARY KEY);"); err != nil {
        return err
    }
    if err := executeOnce(ctx, holder, "BEGIN IMMEDIATE; INSERT INTO busy_ledger VALUES(10);"); err != nil {
        return err
    }
    if err := expectRows(ctx, contender, "SELECT id FROM busy_ledger ORDER BY id;", "[]"); err != nil {
        return err
    }
    launched := make(chan struct{})
    completed := make(chan error, 1)
    go func() {
        close(launched)
        completed <- executeOnce(ctx, contender, "BEGIN IMMEDIATE; INSERT INTO busy_ledger VALUES(11); COMMIT;")
    }()
    <-launched
    holderError := executeOnce(ctx, holder, "COMMIT;")
    var contenderError error
    select {
    case contenderError = <-completed:
    case <-ctx.Done():
        return errors.New("candidate stopped with unresolved contender outcome")
    }
    if holderError != nil { return holderError }
    if contenderError != nil { return contenderError }
    if err := holder.Close(ctx); err != nil { return err }
    if err := contender.Close(ctx); err != nil { return err }
    return verifyFresh(ctx, holderClient, database, "[[10],[11]]")
}
```

4. **证据/限制**：`launched` 只证明本地 goroutine 已启动，不证明服务端已进入 Sleep。
   这是可选用户进展烟测，不是确定性的 contention 阶段 hook。不要加入长固定 sleep
   人为耗尽 B 的期限；网络、调度、租约或 holder 的慢 COMMIT 仍可能导致合法失败。
   失败时停下保留两个调用的 request ID/终态，不重放行 11，不将失败归因于全库锁。

## 三、Constraint 之后显式回滚，不抹去已提交前缀

1. 使用第三个新库，健康 A 建表，然后单次 batch：先提交行 20，后开新事务，
   插入行 21 两次触发主键冲突。禁止拆成“失败后自动重试整个 batch”。
2. 已知 `sql_error` 的 `execution_completed=true` 表示调用有明确终态，
   `may_have_committed=true` 是保守提示，既不证明全部已提交，也不证明全部已回滚。
3. A 显式 `ROLLBACK` 后关闭健康会话，新 reader 应保留 20、不含 21，完整性为 `ok`。

```go
func committedPrefixPath(ctx context.Context, holderClient, contenderClient *sdk.Client, database sdk.DatabaseID) error {
    session, err := holderClient.Open(ctx, database)
    if err != nil { return err }
    if err := executeOnce(ctx, session, "CREATE TABLE busy_ledger(id INTEGER PRIMARY KEY);"); err != nil {
        return err
    }
    failure := executeOnce(ctx, session,
        "BEGIN; INSERT INTO busy_ledger VALUES(20); COMMIT; " +
        "BEGIN; INSERT INTO busy_ledger VALUES(21); INSERT INTO busy_ledger VALUES(21); COMMIT;")
    if err := knownSQLFailure(failure); err != nil { return err }
    if err := executeOnce(ctx, session, "ROLLBACK;"); err != nil { return err }
    if err := session.Close(ctx); err != nil { return err }
    return verifyFresh(ctx, contenderClient, database, "[[20]]")
}
```

4. **证据/限制**：HTTP `sql_error` 也没有 Constraint 类型细节；本组使用明确的唯一键
   重复输入及新会话 oracle 验证预期副作用，而不是声称 HTTP 返回了 native Constraint。
   本组没有并行竞争，需要的是提交前缀语义，不要把它当 busy timeout 的测量。

## 全组停止规则与静态依据

1. 示例故意不使用网络 `defer Close` 或 error 后自动 ROLLBACK。
   任何未识别错误、`outcome_unknown`、`session_moved`、context 到期或 malformed ACK
   都停止该组，不继续 SQL、不盲 close、不自动重开或重试。错误路径可能留下会话或事务，
   将证据交运维按既有故障流程处理，不能以超时或读回解释原操作已经取消。
2. 各示例依赖已准备好的 context/clients/database，并非新部署加载器。
   运行者应保存非秘密 request ID 关联证据；不要输出 credential、connection 全文、
   Authorization 或任意用户数据。新 reader 是新 session，不等于冷 cache 或新 worker。
3. 静态接口依据：`prototypes/v1/platform/sdk/go/session.go` 的 Open/Execute/Query/Close，
   `sdk/go/types.go` 的 Error/RandomID/Options，`sdk/README.md` 的 no-replay 合同，
   `platform/gateway_http.go` 与 `gateway_sessions.go` 的终态字段，
   以及 `docs/v1/NATIVE-BUSY-WAIT.md` 的 native policy / mock 边界。
4. 本文没有执行 API、SQL、test、build 或真实 fixture；不替代真实 Authority revoke、
   epoch/physical I/O fence 门禁，不声称原 Round3 HTTP 失败根因已确定。
