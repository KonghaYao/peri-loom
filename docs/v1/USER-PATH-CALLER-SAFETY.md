# 用户行为验收：调用者安全、TLS 与观察凭据

本文是操作与判定清单，不是新一轮运行报告。编写本篇仅查源码，没有调用 API、执行 SQL、build、启动服务或制造故障。
各组的“自动验收范围”引用已有证据；以下用户手工步骤本轮均未运行。未知结果不能用重新打开、关闭会话或读回数据冒充已解决。

## 1. 先决条件与安全记录

1. 使用已获授权的隔离数据库与当前批准的服务/CLI artifact；确认实际 endpoint、tenant、database、branch。
   本文 `<TENANT>/<DATABASE>/<BRANCH>` 是 CLI 身份格式，不是 worker 私有句柄。
   期望：项目 SQL 凭据属于该数据库项目，数据库已 active；数据库 Token 的 incarnation 绑定仍有效。
2. 由部署者提供可信 CA 文件及当前用户控制的私有 credential 文件，建议 mode0600。
   不把 credential 放进命令参数、URL、截图、日志或 Git；不要展示私有 connection/config 文件完整内容。
   下面 `<PRIVATE_CREDENTIAL_FILE>`、`<CA_FILE>`、`<HTTPS_ENDPOINT>` 都是占位符，不能原样运行。
3. 仅记录非秘密证据：artifact/source hash、request ID、错误类型、ACK、退出码与安全状态摘要。
   request ID 只关联请求，不保证 SQL 去重、exactly-once 或失败可重试；query 也可能写入。
   官方 SDK 同发 `X-Request-ID` 和 body ID，调用者不能放宽 response ID 校验来掩盖错误。

源码：`prototypes/v1/platform/cmd/peri/credentials.go`、`prototypes/v1/platform/sdk/go/session.go`、`prototypes/v1/platform/sdk/go/client.go`。
自动验收范围：CLI/SDK mock codegate覆盖凭据/关联与调用路径，不证明当前真实服务可用。

## 2. 正常 SQL ACK 后显式关闭

1. 使用项目 SQL/admin/developer 凭据执行只读示例；不要使用全局观察凭据代替项目 SQL 身份：

   ```sh
   peri sql --endpoint '<HTTPS_ENDPOINT>' --database '<TENANT>/<DATABASE>/<BRANCH>' \
     --credential-file '<PRIVATE_CREDENTIAL_FILE>' --ca-file '<CA_FILE>' \
     --query 'SELECT 1;' --request-id 'caller_read_1' --timeout 10s
   ```

   期望：CLI 打开会话，收到经过 envelope/result 校验的行，再请求 Close；只有 Close ACK 成功才返回成功。
   若已经输出行而随后 Close 失败，退出仍为错误，不能把 stdout 有结果当作成功关闭。
2. 需要事务时使用 `peri shell` 加同样 endpoint/database/credential-file/ca-file 参数。
   输入 `:execute BEGIN;`、`:query SELECT 1;`、`:execute COMMIT;`，逐条等待 ACK，最后 `:quit`。
   期望：这些语句留在同一个会话；健康 `:quit` 或 EOF 关闭一次，不自动重放事务。
3. Go SDK 的真实入口是 `sdk.NewClient(endpoint, credential, sdk.Options{RootCAs: roots})`、
   `client.Open(ctx, database)`、`session.Query(ctx, sql, requestID)` / `Execute`、`session.Close(ctx)`。
   `sdk` import 为 `peri-loom/platform/sdk/go`；`database` 是 `sdk.DatabaseID`，不是自由 URL。
   正常路径逐次检查错误，全部所需 ACK 已确认后才显式 Close，并检查 Close 错误。
   不写无条件 `defer session.Close(...)`：它会把 unknown 路径也变成网络清理请求。
4. Go 行中的数字保留为 `json.Number`；读取大整数时选择 `Int64()` 等明确转换，不隐式转换成 float64。
   TypeScript SDK 的超安全范围整数为 bigint，展示可转十进制字符串，不改变 SQL wire 数值或重试请求。

源码：`prototypes/v1/platform/cmd/peri/main.go`、`prototypes/v1/platform/cmd/peri/shell.go`、`prototypes/v1/platform/sdk/go/session.go`。
自动验收范围：`CLI-CALLER-SAFETY-VERIFICATION.md` 的独立 mock normal/race证据覆盖健康 Close ACK；
`LINUX-CURRENT-RUNTIME.md` 的 Local whole11覆盖实际 SQL、int64、rollback等，但未另执行本组 CLI/SDK命令。

## 3. Unknown 与已确认终态 SQL 错误分开处理

1. 发生超时、断连、丢失/畸形 ACK或响应超限时，先检查 typed SDK错误。
   Go 使用 `errors.As` 读取 `*sdk.Error` 的 `Code`、`RequestID`、`OutcomeUnknown`；
   TypeScript 使用 `PeriError` 的 `code`、`requestID`、`outcomeUnknown`。
   期望：真正未知结果停止该会话业务，不发自动 Close、ROLLBACK、COMMIT重试、query重试或 reconnect Open。
2. 保存 request ID 与非秘密错误摘要，交应用/运维按实际证据解决不确定性。
   timeout/cancel不证明服务端停止；新会话不恢复旧事务；只读回某一值不证明旧 RPC 已取消。
   Gateway未知路径可能保留 active permit并 poison会话，不能用客户端清理假称 quota 已释放。
3. 若收到严格 typed `sql_error`，并且 `ExecutionCompleted=true` / `executionCompleted=true` 与
   `MayHaveCommitted=true` / `mayHaveCommitted=true`，它表示同步 SQL调用返回终态，不表示事务已回滚。
   期望：调用者可在仍可用的 SDK会话上明确发 `ROLLBACK`，检查其 ACK，再决定后续业务或 Close。
   多语句已提交前缀仍可能存在；不自动 rollback/retry。标志缺失或畸形不能冒充该终态。
4. CLI 的 SQL/shell错误路径直接返回错误，不自动 Close；CLI也不会替用户保留一个错误后可继续输入的 shell来自动恢复。
   SDK的 `Close` 方法本身不是“不确定性解决器”，调用者必须避免在 unknown路径无条件调用它。

源码：`prototypes/v1/platform/sdk/go/types.go`、`prototypes/v1/platform/sdk/go/session.go`、`prototypes/v1/platform/sdk/typescript/client.ts`。
自动验收范围：CLI cleanup mock覆盖丢失/畸形响应、error不盲Close；SDK terminal tests覆盖终态标志。
本篇没有手工注入丢包/超时，也不授权为了验收而中断生产请求。

## 4. Typed known session_moved 只允许显式重连，不 replay

1. 在服务确实返回 typed `session_moved` 且 `OutcomeUnknown=false` 时，区分它与真正未知结果。
   期望：旧 session不能继续 SQL；不能仅因错误字符串包含 moved 或一次 metadata不可用就触发重连。
2. CLI shell会显示 `Operation not replayed`；用户明确输入 `:reconnect`。
   期望：本地丢弃已知 moved旧会话，不向它发送 Close，然后 Open一个新会话；未确认 SQL和旧事务均不 replay。
   未输入 `:reconnect` 前再发业务命令会被提示拒绝；known moved下退出也不向旧会话 Close。
3. 新会话的业务由用户重新评估，使用新 request ID；只有确定业务允许的新操作才提交。
   SDK无自动 reconnect方法或自动迁移事务能力；调用者按相同规则显式调用 `client.Open`。
   对 true unknown即使 `Code` 同时看似 moved，也不能使用此 known-moved例外。

源码：`prototypes/v1/platform/cmd/peri/shell.go` 的 `knownSessionMoved` 与 `:reconnect` 分支。
自动验收范围：独立CLI mock保留 opens2/queries2/executes1、closes1；旧 moved句柄 Close0。
Linux whole11验证实际 migration epoch，但不是本篇手工 shell迁移/reconnect验收。

## 5. 正确 CA 与 nonroot Linux 核验

1. CLI用 `--ca-file '<CA_FILE>'` 或 `PERI_CA_FILE`；Go SDK用 `Options.RootCAs`。
   Node/TypeScript在进程启动前配置 `NODE_EXTRA_CA_CERTS='<CA_FILE>'`，浏览器使用其正常信任机制。
   期望：证书链与目标主机/IP正常验证；不使用 `-k`、`InsecureSkipVerify`、broad certificate ignore或修改用户默认浏览器 profile。
2. 部署者如另获运行授权，可对自己的 endpoint验证：

   ```sh
   curl --fail --noproxy '*' --cacert '<CA_FILE>' --max-time 10 \
     -o '<OWN_OUTPUT_FILE>' -w '%{http_code} %{ssl_verify_result}\n' '<HTTPS_ENDPOINT>/'
   ```

   期望：HTTP200及 ssl_verify_result0。这只是 TLS/入口检查，不证明 SQL、SDK或所有服务健康。
3. 对明确归属自己的容器，运行 `docker exec '<OWN_CONTAINER>' id`，核对 UID/GID10001。
   runtime Local验收须明确 `--backend local --tls --no-build --writer-admission`；profile默认OFF，不能假定继承开启。
   不为权限错误改用 root、privileged或777；context文件模式需按受验 manifest修复。
4. 仅在获授权且已知会话已关闭时，对 own容器 `docker stop --time 45 '<OWN_CONTAINER>'`、`docker wait`，
   核验实际 exit0、OOMfalse及 `authority/authority.lock` 精确 `clean\n`。
   shutdown响应ACK不能代替进程 joined退出；不得 KILL他人容器或据超时假定后台 I/O已终止。

源码：`prototypes/v1/deploy/Dockerfile`、`prototypes/v1/platform/dev.py`、`prototypes/v1/platform/cmd/peri/credentials.go`。
自动验收范围：已接纳 Linux Local报告记录实际UID10001、proper-CA、whole11、joined clean退出；不外推 Linux S3。
本篇未启动容器、浏览器或重跑这些命令；scoped leaf-SPKI浏览器例外也不等于浏览器 CA validation。

## 6. SQL / management / metrics 凭据分离

1. SQL及日常项目 lifecycle使用该项目的授权身份；checkpoint等操作须有相应项目权限。
   全局 admin并不自动拥有所有项目 SQL/tool权限；不能把观察 key当作 operational key修复403。
2. `/v1/metrics` 当前仅全局 admin（`Project="*"`）可读。
   项目 admin收到403是安全边界，不应放宽服务端权限以获取跨租户 metrics。
   观察者使用独立控制/观察 credential，只发 GET `/v1/metrics`，不发 SQL或项目管理操作。
3. 若独立获授权使用 stress observer，私有 config中保持 `api_key`、`management_key` 与可选 `metrics_key` 分离。
   `metrics_key` 需要显式 `management_endpoint`；字段缺省时 metrics标记 unavailable，不回退 management key，也不把缺失计数写成0。
   完整 credential/config保持私有文件，不输出 stdout/log/git；仅分享 redacted配置与可用性摘要。
4. 核对 metrics响应的实际 measurement scopes、null/缺失说明和权限结果。
   功能 ACK/TLS通过不是性能结论；本清单不授权 stress、S3、压力或跨租户观察。

源码：`prototypes/v1/platform/management.go` 的 `/v1/metrics`；`prototypes/v1/stress/config.go`、`runner.go`、`observe.go`。
自动验收范围：metrics credential mock验证独立 GET路由、缺省不fallback及redaction；Local whole11检查实际 native metrics。
本篇没有发任何观察请求，不能据本篇宣称当前部署 metrics/perf验收完成。

## 7. 证据入口与验收边界

1. 参照 `docs/experiment-v1-stress/CLI-CALLER-SAFETY-VERIFICATION.md` 的冻结源码及独立 mock范围，
   与 `docs/experiment-v1-stress/LINUX-CURRENT-RUNTIME.md` 的实际 Local image/UID/TLS/11项/退出证据。
2. 验收新 artifact时逐项重新记录该 artifact的版本与结果，不把旧 hash或旧PASS贴到后来 observer/P4/UI版本。
   本文不消除历史失败、不宣布最终生产 Ready或联合全阶段PASS。
