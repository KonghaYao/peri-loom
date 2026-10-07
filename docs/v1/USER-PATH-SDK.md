# SDK 用户验收路径

本文从仓库根目录执行，面向已经按 [运维路径](USER-PATH-OPERATIONS.md) 启动的
独立验收实例。命令是用户执行步骤，不表示本文编写时已执行新的 fixture 调用。
三个 SDK 要分别记录结果；单元测试、UI 成功或一个语言成功不代替另两个语言的实际调用。

## 1. 私有连接与执行前提

1. 使用实例生成的 `connection.json`，不要在命令行、源码或报告中写明文凭据。
   文件应为当前用户所有、普通文件、非符号链接、权限 `0600`，父目录应为私有目录。
   只使用自己控制的 launcher 输出，不把 SDK 示例当作任意不可信文件的沙箱。
   配置至少包含 `endpoint`、`project_api_key`、`database` 和 `ca_file`；
   `database` 是包含 `namespace.tenant`、`namespace.database`、`branch` 的对象。
2. 在终端只设置文件路径，关闭 shell 调试输出，不 `cat` 连接文件：

   ```sh
   set +x
   export PERI_CONNECTION='/absolute/private/run/connection.json'
   ```

   替换为真实私有路径。本文使用项目 Key 打开 SQL，不使用全局管理 Key 替代它。
3. 本文实际 smoke 入口要求 HTTPS。开发实例的 `ca_file` 应指向真正的开发根 CA；
   服务端叶证书应由它签发，具有正确 SAN 和 serverAuth 用途，不能把 CA 当作叶证书。
   Go 配置 `RootCAs`，Rust 配置 `trusted_ca_pem`，Node 启动前配置
   `NODE_EXTRA_CA_CERTS`。不得使用 `InsecureSkipVerify`、
   `NODE_TLS_REJECT_UNAUTHORIZED=0` 或其他证书绕过。
4. SDK 的非 TLS HTTP 仅允许 literal-loopback IP，不允许以 `localhost` 或外部主机名
   代替 HTTPS；这不是本文 TLS 验收通过的依据。保持正常证书链和服务端名字校验。
5. 三组命令按顺序执行，期间不要迁移、恢复或关闭实例。正常 smoke 只查询常量和状态，
   不建表、不创建用户数据、不压测、不自动重试。记录提交、工具链、脱敏日志和 PASS/FAIL。

## 2. Go：完整最小程序

1. 确认 Go 满足 `prototypes/v1/platform/go.mod` 的 Go 1.23 要求。
   从仓库根目录进入模块并准备临时源码目录；以下路径不包含凭据：

   ```sh
   cd prototypes/v1/platform
   SDK_EXAMPLE_DIR=$(mktemp -d ./user-sdk-go.XXXXXX)
   ```

2. 将以下完整程序保存为 `$SDK_EXAMPLE_DIR/main.go`。它直接读取私有连接文件，
   验证文件身份与权限，使用 SDK 默认请求/回复上限和五秒请求超时，不输出配置内容。

   ```go
   package main

   import (
       "context"
       "crypto/x509"
       "encoding/json"
       "errors"
       "fmt"
       "io"
       "os"
       "strings"
       "syscall"
       "time"

       sdk "peri-loom/platform/sdk/go"
   )

   type connectionConfig struct {
       Endpoint string `json:"endpoint"`
       ProjectAPIKey string `json:"project_api_key"`
       Database sdk.DatabaseID `json:"database"`
       CAFile string `json:"ca_file"`
   }

   func readConnection(path string) (connectionConfig, error) {
       var connection connectionConfig
       before, failure := os.Lstat(path)
       if failure != nil {
           return connection, errors.New("connection unavailable")
       }
       owner, valid := before.Sys().(*syscall.Stat_t)
       if !valid || !before.Mode().IsRegular() || before.Mode().Perm() != 0600 ||
           owner.Uid != uint32(os.Getuid()) || owner.Nlink != 1 || before.Size() > 65536 {
           return connection, errors.New("private regular connection required")
       }
       file, failure := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|
           syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
       if failure != nil {
           return connection, errors.New("connection unavailable")
       }
       defer file.Close()
       after, failure := file.Stat()
       if failure != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() ||
           after.Mode().Perm() != before.Mode().Perm() || after.Size() != before.Size() {
           return connection, errors.New("connection changed")
       }
       payload, failure := io.ReadAll(io.LimitReader(file, 65537))
       if failure != nil || len(payload) > 65536 || json.Unmarshal(payload, &connection) != nil {
           return connection, errors.New("invalid connection")
       }
       return connection, nil
   }

   func run() error {
       connection, failure := readConnection(os.Getenv("PERI_CONNECTION"))
       if failure != nil {
           return failure
       }
       if !strings.HasPrefix(connection.Endpoint, "https://") {
           return errors.New("verified HTTPS required")
       }
       certificate, failure := os.ReadFile(connection.CAFile)
       if failure != nil || len(certificate) > 1<<20 {
           return errors.New("CA unavailable")
       }
       roots := x509.NewCertPool()
       if !roots.AppendCertsFromPEM(certificate) {
           return errors.New("invalid root CA")
       }
       client, failure := sdk.NewClient(connection.Endpoint, connection.ProjectAPIKey,
           sdk.Options{RootCAs: roots, Timeout: 5*time.Second})
       if failure != nil {
           return errors.New("client configuration rejected")
       }
       session, failure := client.Open(context.Background(), connection.Database)
       if failure != nil {
           return failure
       }
       rows, failure := session.Query(context.Background(),
           "SELECT 9223372036854775807, -9007199254740993", "go-user-path-integers")
       if failure != nil {
           return failure
       }
       if len(rows) != 1 || len(rows[0]) != 2 {
           return errors.New("unexpected rows")
       }
       for position, expected := range []int64{9223372036854775807, -9007199254740993} {
           number, valid := rows[0][position].(json.Number)
           if !valid {
               return errors.New("numeric cell lost its type")
           }
           integer, failure := number.Int64()
           if failure != nil || integer != expected {
               return errors.New("integer precision mismatch")
           }
       }
       if failure := session.Close(context.Background()); failure != nil {
           return failure
       }
       fmt.Println("Go SDK verified-TLS exact-i64 explicit-close PASS")
       return nil
   }

   func main() {
       if failure := run(); failure != nil {
           var remote *sdk.Error
           if errors.As(failure, &remote) {
               fmt.Fprintf(os.Stderr, "Go SDK FAIL code=%s request_id=%s unknown=%t\n",
                   remote.Code, remote.RequestID, remote.OutcomeUnknown)
           } else {
               fmt.Fprintln(os.Stderr, "Go SDK FAIL: local precondition or result check")
           }
           os.Exit(1)
       }
   }
   ```

3. 用户确认实例允许此次调用后运行，完成后返回仓库根目录：

   ```sh
   go run "$SDK_EXAMPLE_DIR/main.go"
   cd ../../..
   ```

   期望仅打印安全 PASS。Go 泛型数值单元格是 `json.Number`，应显式 `Int64()`；
   不能先转 `float64` 再比较。失败即停止，不用重新运行程序当作 SQL 重试。
4. 此程序只在正常路径显式关闭会话；未知结果不自动 rollback 或重新打开。
   若本地精度断言失败但请求已明确成功，应人工处理该次已打开会话；不能标记通过。

## 3. TypeScript / Node：现有真实 smoke 入口

1. 使用支持原生 fetch、Web Crypto 和 ES2022 bigint 的 Node 环境以及已安装的 TypeScript
   编译器。先检查版本并构建 SDK；不要用未固定版本的临时联网安装替代工具链记录：

   ```sh
   node --version
   tsc --version
   tsc -p prototypes/v1/platform/sdk/typescript/tsconfig.json --noEmit
   tsc -p prototypes/v1/platform/sdk/typescript/tsconfig.json
   ```

2. 从私有 launcher 配置读取 CA 路径，不输出项目 Key。以下命令只读取本地文件，
   输入必须是第一组已确认的私有、可信 launcher 配置：

   ```sh
   export NODE_EXTRA_CA_CERTS="$(python3 -c 'import json,os; print(json.load(open(os.environ["PERI_CONNECTION"]))["ca_file"])')"
   ```

   必须在启动 Node **之前** 设置；既有 Node 进程不会靠改环境动态加载新 CA。
   若环境设置了禁用 TLS 校验的变量，先修正配置；真实 smoke 会拒绝该绕过。
3. 用户确认后执行仓库内的现有入口：

   ```sh
   node prototypes/v1/platform/tests/sdk-live.mjs "$PERI_CONNECTION"
   ```

   入口加载 `sdk/typescript/dist/client.js`，检查私有连接文件和 CA 路径，
   实际依次调用 management `status`、SQL `open`、常量 `query` 和显式 `close`。
   期望打印 `TypeScript SDK live verified-TLS SELECT exact-bigint/status/explicit-close PASS`。
4. 两个返回值必须分别为 `9223372036854775807n` 和 `-9007199254740993n`。
   SDK 将超出安全整数范围的 JSON 整数字面量解析成 `bigint`；小整数以及小数/指数
   仍为 `number`，引号中的数字仍为字符串。不要用普通 `response.json()` 绕过 SDK。
   若应用要展示结果，使用以下显示转换，不改变服务器 wire 格式：

   ```ts
   const display = JSON.stringify(rows, (_key, value) =>
     typeof value === "bigint" ? value.toString() : value);
   ```

5. 该 smoke 的 `finally` 会尝试一次显式 close，包括 query 失败时；这是示例的释放动作，
   不是 query 重试，也不能证明未知 query 已回滚。失败信息的 stage 可能因此显示 close。
   若需要审计首次 query 失败，保留其错误码、请求 ID 与服务端证据，不能只看最终 stage。

## 4. Rust：现有真实 smoke 入口

1. 使用仓库规定的 Rust 工具链并初始化仓库所需的 Turso fork；不要自行升级 image pin
   或修改 fork。协调好共享构建门禁后构建现有 example，不同时启动多套完整 Cargo：

   ```sh
   cargo build --locked --manifest-path prototypes/v1/Cargo.toml --example p6_sdk_smoke
   ```

2. 用户确认后运行构建结果，默认 target 路径如下；设置 `CARGO_TARGET_DIR` 时应替换路径：

   ```sh
   prototypes/v1/target/debug/examples/p6_sdk_smoke "$PERI_CONNECTION"
   ```

   入口为 `prototypes/v1/examples/p6_sdk_smoke.rs`，使用
   `peri_loom_v1::sdk::{Client, ClientOptions, DatabaseId, ManagementRequest}`。
   它读取私有文件及 CA，调用真实 management `status`、`open`、常量 `query`、显式 `close`。
   期望打印 `Rust SDK live verified-TLS SELECT exact-i64/status/explicit-close PASS`。
3. `serde_json::Value::as_i64()` 必须精确得到两个测试整数；不要先转浮点数。
   Rust 的 `ClientOptions.trusted_ca_pem` 加入正常信任根，不关闭名字或证书链校验。
4. 当前 example 在 status 失败时另发一次只读 status 诊断请求，报告脱敏的 HTTP 状态或
   证书错误类别。它不是 SQL 重试，也不能替代首次失败或作为未知 mutation 的结果证明。
   query 发出后 example 会尝试一次 close，即使 query 失败；close ACK 不证明 query 未执行。

## 5. 事务、明确错误与未知结果

1. 正常程序使用同一个 opaque session 执行事务。若要额外验收显式 rollback，必须使用
   独立 writer 验收数据库；用同一 session 依次执行 `BEGIN`、`SELECT 1`、`ROLLBACK`、
   `SELECT 1`、`close`，每步确认成功才进行下一步。这只验收事务控制，不写业务数据。
   三个 SDK 的调用形状分别为：

   ```go
   failure := session.Execute(ctx, "ROLLBACK", "go-user-path-rollback")
   ```

   ```ts
   await session.execute("ROLLBACK", "ts-user-path-rollback");
   ```

   ```rust
   session.execute("ROLLBACK", Some("rust-user-path-rollback"))?;
   ```

2. 终止的 `sql_error` 只有携带严格校验的 `execution_completed=true` 和
   `may_have_committed=true` 才保留可用会话。调用者可明确决定在该会话执行 `ROLLBACK`。
   这些标志不证明多语句前缀没提交，SDK 不自动 rollback；不要在未知结果后照此继续。
3. Go 用 `errors.As` 检查 `*sdk.Error`，TypeScript 用 `PeriError`，Rust 返回 `SdkError`。
   记录安全的 code、request_id 和 unknown/执行标志，不 dump 客户端、连接文件或 Authorization。
   `outcome_unknown`、`session_moved` 必须停止后续 SQL；重新 open 不恢复原事务。
   query 也可能写数据，不能因方法名而自动重试。
4. 三个 SDK 都在 HTTP `X-Request-ID` 与 JSON `request_id` 使用同一个实际请求 ID，
   并严格校验响应 ID。已关联的早期 401/429 能成为 `unauthenticated`/`quota_exceeded`
   明确错误，而不是因随机回复 ID 误判成未知。Header 是关联标识，不是幂等保证或提交证明。
   丢失 ACK、超时、取消后在途请求、错误 ID、截断或无效 envelope 仍可能未知；不重放 SQL。
5. 不靠压力制造 429 或使用真实 Key 的错误认证来验收协议。可先运行隔离的专属客户端测试：

   ```sh
   (cd prototypes/v1/platform && go test ./sdk/go -count=1)
   (cd prototypes/v1/platform/sdk/typescript && node --test *.test.mjs)
   ```

   Rust 协议测试入口为 `tests/p6_sdk.rs`，由统一 Cargo 门禁运行
   `cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test p6_sdk`。
   这些 mock 协议测试核对请求 body/header 一致、401/429 明确错误、错 ID fail-closed
   及无自动 SQL 重试，不冒充实际部署/性能验收。
6. 对每个实际 smoke 独立记录 PASS/FAIL/NOT RUN。正常显式 close 成功后结束该组；
   未知结果停止并交人工核实，不自动 readback 作提交证明、不 replay COMMIT、不恢复会话。
   本文没有要求破坏性故障、压力负载、修改配额或关闭 TLS 校验。
