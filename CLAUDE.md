# CLAUDE.md

面向在本仓库工作的 AI agent。项目介绍、目录结构、环境安装、本地启动见 `README.md`；
架构设计与冻结决策见 `tursodb-db-platform-architecture.md`（第 15 / 18.4 章的冻结条款
不要自行修改，需要改就先问人）。

本文只写**你不会自然发现、但一定会绊倒你**的事。

## 命令

```bash
cargo check -p <crate>                                  # 快速编译校验，先跑这个
cargo test -p engine-adapter                            # 不依赖外部服务
cargo test -p db-server                                 # 需要 PostgreSQL 可达
cargo test -p db-runtime                                # 见「已知红灯」
cargo clippy -p <crate> --all-targets -- -D warnings    # CI 门：警告即错误

# 端到端（需平台已启动）
cd test && npm install && npm test                      # 真实 tursodb TS 客户端，36 条
node scripts/hrana-v3-sdk-check.mjs                     # Hrana v3 验收，14 条
```

## 环境陷阱

1. **macOS 宿主编译不了 `db-worker`** —— 它用到 `libc::SYS_pidfd_open` /
   `SYS_pidfd_send_signal`，macOS 没有。改了 db-worker 或它依赖的 crate，必须在容器里验证：
   ```bash
   docker compose -f docker-compose.yml -f .run/compose.dev.yml build db-worker-1
   ```
   `Dockerfile` 的 builder 阶段**一次构建全部二进制**（db-server / db-worker / db-runtime /
   wal-service），所以构建任一 target 都会把全部一起编好，切换 target 基本走缓存。

2. **compose 必须同时给两个 `-f`** —— `.run/compose.dev.yml` 提供本地覆盖（如 db-server 的
   直连端口）。只给 `docker-compose.yml` 会得到一个和开发环境不一样的拓扑。

3. **前端是烤进镜像的** —— `web/Dockerfile` 里跑 `vite build`，没有挂 volume。改完前端
   必须重建镜像才生效：
   ```bash
   docker compose -f docker-compose.yml -f .run/compose.dev.yml build web
   docker compose -f docker-compose.yml -f .run/compose.dev.yml up -d --no-deps web
   ```

4. **`cargo clippy --workspace` 在宿主机会失败** —— `raft-proto` 的构建脚本与宿主 protoc
   不兼容（既有问题，与你的改动无关）。**按 crate 跑**，不要用 workspace 全量结果判断自己
   改坏了什么。

5. **容器一律用 compose 管**：重启单个服务用 `up -d --no-deps <svc>`，不要 `kill` 容器内进程
   或手工 `docker rm` —— 后者会让 compose 的状态与实际不一致。

## 已知红灯（不是你弄坏的，不要顺手"修"）

- `cargo test -p db-runtime` 的 `host::tests::rss_is_reported_on_linux` 在 macOS 上必然失败
  —— 没有 `/proc`。
- `cargo test` 会往 Catalog 里插 `test-worker-*` 假 worker，之后 db-server 日志会持续刷它们的
  心跳超时 ERROR。这是单测残留数据，不是故障。
- 宿主机 `cargo test -p db-server` 的部分用例需要 PostgreSQL 可达；连不上时的失败先查
  `docker compose ps postgres`，别怀疑代码。

## 架构红线

- **控制面不进 SQL 热路径**：Catalog（PostgreSQL）是权威事实源，但不参与查询执行。
- **对外有两套契约，别混淆**：
  - 平台自有 —— `/api/v1/*`（控制面）、`/data/v1/*`（数据面，NDJSON 流式）。这两个进 OpenAPI。
  - 客户端兼容 —— `/db/{db_id}/v2/pipeline`、`/v3/pipeline`、`/v3/cursor`（Hrana over HTTP）。
    不进 OpenAPI，权限与 `/data/v1` 同档（`db:write`）。
- **两个 Hrana 版本都不能删**：`@libsql/client` 全程只打 v2；`@tursodatabase/serverless`
  **自发布起（0.1.0）**就只打 v3，从不请求 v2、不探测版本、不降级。删任何一个都会直接打断
  一整类客户端。
- **本地协议不是 gRPC**：Server→Worker 是 gRPC/HTTP2；Worker→DB Process 是
  **UDS + length-delimited protobuf**（`proto/platform/runtime_local.proto`）。不要给后者在
  UDS 上再套一层 HTTP2。
- **别为某个协议污染平台契约**：`domain::value::ResultSet` 是平台自己的结果集契约。Hrana
  特有的字段（`last_insert_rowid` 之类）由兼容层自己携带，不要塞进去。
- **会话不进控制面**：db-server 的会话是进程内本地状态，重启即失效（客户端收
  `SESSION_NOT_FOUND` 后重开）。这是刻意的降级，不要把会话写进 Catalog。

## 代码约定

- **注释用中文，讲「为什么」**，不复述「是什么」。现有代码对这条执行得很严格 —— 跟上，
  别写「本函数用于处理请求」这种。
- 错误统一用 `domain::error::{ErrorCode, PlatformError}`，不要自造错误类型。
- 提交信息用中文全角冒号：`feat(hrana)：…`、`fix(compose)：…`。

## 协议层的坑（都真实踩过）

- **proto3 `optional` 不是锦上添花**：当「字段缺失」与「字段为 false/0」语义不同时**必须**
  用 `optional`。例：`StreamEnd.is_autocommit` 缺失表示「DB Process 没上报」，与「上报了
  false（仍在事务中）」完全是两回事 —— 裸 `bool` 会把前者读成后者，而后者会让客户端以为
  事务还在。取值方向危险时，宁可多一层存在性。
- **Hrana 值编码不对称**：`integer` 的 `value` 是**字符串**（JSON 数字承载不了完整 i64），
  `float` 的 `value` 是 JSON 数字，`blob` 的字段名是 **`base64`** 而不是 `value`。
- **Hrana 响应字段是蛇形**：客户端解码器读的是 `is_explain` / `is_readonly`，之后才在 SDK
  内部转成驼峰。服务端写成驼峰会静默读成 `undefined`。
- **`@tursodatabase/serverless` 用朴素字符串拼接 URL**（`${url}/v3/cursor`）：给它的地址
  **不能带任何 query** —— `?tls=0` 会被当成路径的一部分吃掉。`?tls=0` 只对 `@libsql/client`
  有效。
- **步骤级失败不能升级成 HTTP 错误**：Hrana 客户端生成的批处理里，回滚靠 `error` /
  `not(ok,…)` 条件触发；整个请求失败会让回滚步骤永远不执行，事务就留在半途。用 `step_error`
  表达。
- **被条件跳过的步骤不发任何条目**：`@tursodatabase/serverless` 正是靠「探测步有没有条目」
  判断连接是否在事务中。

## 凭据

- `deploy/secrets/` 下的文件**不要提交、不要打印内容**。管理员密码在
  `deploy/secrets/bootstrap_admin_password.txt`。
- 抓取或回显任何含 `Authorization` 的输出时，替换成 `Bearer <REDACTED>`。
