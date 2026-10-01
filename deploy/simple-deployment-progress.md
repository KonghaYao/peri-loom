# Simple 部署实施记录

本次实施已由用户明确授权；`simple-deployment-design.md` 的历史草稿状态不再阻止本次实现。
现有 distributed 契约继续适用。Simple 是独立装配，只有本地可靠持久化保证。

## 验收清单

- [x] 本地 WAL 提交、同步失败、checkpoint、强杀恢复及丢失未同步写模型验证
- [x] SQLite 元数据：身份权限、数据库、审计、幂等、Operations、Jobs、备份、Panel
- [x] 有界进程内宿主：会话、事务、取消、超时、流式背压、关闭与恢复
- [x] 共享 HTTP / NDJSON / Hrana v2、v3 pipeline / v3 cursor
- [x] 本地对象存储、一致性备份、恢复、整实例导出
- [x] 单文件启动、实例锁、版本检查、初始化凭据、持久签名密钥
- [x] 内嵌 Web、能力信息、不支持动作明确失败、同端口健康检查
- [x] 文档、发布包装、Simple 验收、distributed 构建与选定回归（范围见下文）

## 实施前基线

- Git 起点：`0e5fa43`；工作分支：`feat/simple-deployment`。
- `cargo test -p engine-adapter --lib --locked --offline`：61 passed。
- macOS 全 workspace 构建受既有 Worker Linux pidfd 依赖限制。
- `raft-proto` 旧构建器仅识别 protoc 3.x；缓存的 protoc 3.9 可用于它的构建。
- 本机 Turso 0.8.1 源码提供 interrupt、progress handler、step、checkpoint；
  Runtime 旧注释不能作为引擎不支持取消的依据。

## 提交与验证

实现按元数据、宿主、存储、服务装配与验收分阶段记录。下方只记录实际完成的验证；
未运行、被环境阻断或仅有代码检查的项目不能标成通过。

## 已执行的局部检查（2026-10-01）

- `cargo test -p objectstore --lib`：36 passed；本地对象存取、路径逃逸、大文件流式与损坏快照用例通过。后续增加了本地快照解压字节上限及 `main.db` 清单要求，其聚焦用例再次通过。
- `cargo check -p db-server`：通过；`simple::export::tests::offline_roundtrip_and_corruption_rejected`：通过，覆盖身份密钥与对象文件往返、覆盖目标拒绝、篡改拒绝。
- `npm run typecheck`、`npm run build`：通过，Simple 能力 UI 可构建。
- 早期上述局部验证之后，已完成下述集成与故障验证。

## 集成与故障验证

- `cargo test --locked -p db-server -p database-host -p objectstore -p catalog --lib`：Server 98、Host 11、ObjectStore 36、Catalog 51 项通过（SQLite 专项 8 项）。Catalog 的 32 项 PostgreSQL 测试在本机默认跳过，随后已在隔离 Linux + 新 PostgreSQL 中显式运行且全部通过。
- `cargo test -p db-server --test simple_http`：真实子进程验收通过，覆盖 SQL 参数、事务隔离、Hrana v2/v3 pipeline、NDJSON、SIGKILL 恢复、元数据与认证持久化、实例锁、坏库隔离、备份恢复、损坏快照拒绝及停机导出导入。
- `cargo test -p engine-adapter --test local_sync_failure`：5 项通过，覆盖自动/显式提交同步、同步失败、WAL 写入磁盘满、只读写入错误，以及丢弃未同步字节的断电模型。此处为确定性 IO 故障注入，不是物理硬件断电测试。
- 真实 `@tursodatabase/serverless` SDK：36/36 通过，包含 v3 cursor。另有 mock 帧测试验证 cursor 在 trailer 前输出首帧，丢弃响应触发取消。
- 使用临时自签证书并由客户端验证证书的 HTTPS readiness、内嵌 Web、正常停止均通过。
- `npm run typecheck`、`npm run build` 通过；未做浏览器像素或全页面交互验收。
- 修改涉及的 5 个 Rust crate 的 `cargo clippy --all-targets -- -D warnings` 通过；Shell 语法、Compose 配置与 `git diff --check` 通过。
- Linux 分布式构建、WAL/quorum/fencing 单元回归、PostgreSQL 契约和 Simple 镜像验证见 [隔离回归记录](simple-validation.md)。现有运行中的 `db-platform` 集群没有被替换。
- 最终 Simple 镜像 `peri-loom-simple:validation-final` 重新构建并通过独立容器的 readiness、能力信息、Web 与未知 API 验证。分布式回归使用此前代码快照；没有对最终版本做完整多节点端到端故障演练。

## 首版取舍

单机省略远程 quorum/fencing、Worker/cgroup 隔离、Move/自动故障转移。远程位置字段返回 `null`，不支持的动作明确失败。保留打开库、会话、队列与结果帧上限；暂不做自动 LRU。整实例导出要求停机，跨模式迁移工具不在本次范围内。坏库可显式启动或恢复，不能通过普通请求反复自动打开。

## 原生 Release / mise 交付调整

按后续要求，Simple 发布改为 Linux x64、Linux ARM64、macOS ARM64 三平台原生二进制 GitHub Release，取消 macOS Intel、独立 Compose 与 Simple GHCR 发布，保留 Dockerfile。工作流包含打包、解压后启动验证、SHA-256 校验及发布后 mise 安装验证。

本地已用现有 macOS ARM64 debug 二进制验证打包、权限、校验和及独立目录启动；`cargo check -p db-server`、Actionlint 1.7.12、Python/Shell 语法及 `git diff --check` 通过。完整三平台 release 构建和远端 mise 安装需在 GitHub 工作流实际运行后确认；这里不将它们记录为已通过。原 Simple 演示容器已移除，命名卷保留。

## 数据库连接与单库 Token（2026-10-01）

建库成功后展示 libSQL / TursoDB 地址与端口，详情首屏提供一键申请本库 Token，明文仅在本次弹窗显示。每库最多一个未吊销 Token，轮换原子撤销旧 Token；管理 JWT 负责发证与生命周期管理，SDK 只接受绑定本库的 API Token。SQLite / PostgreSQL 均新增独立 migration，旧版未绑定 Token 不再接受认证。

- `cargo test -p catalog --lib`：53 项通过，33 项 PostgreSQL 用例默认跳过；新增的 PostgreSQL 并发创建/轮换用例另在临时独立 PostgreSQL 16 容器通过，随后移除容器。SQLite 覆盖旧 schema 升级、重复 Token 归并、跨库不受影响、重开后约束及轮换失败回滚。
- `cargo test -p db-server --lib`：102 项通过；`cargo test -p db-server --test simple_http` 通过，覆盖跨库 Data/Hrana/session 拒绝、Token 禁止发证及本库生命周期管理、JWT 禁止用于 SDK、重复签发、轮换与吊销、敏感响应 `no-store`，并保留崩溃恢复与导入导出验证。
- 独立临时 Simple 实例的真实 TursoDB SDK 套件：37/37 通过，包含数据库 Token 日志脱敏回归。
- Playwright + 本机 Chrome 使用临时实例验证账号登录、建库后展示地址、详情一键申请、关闭清空且不写 localStorage、列表不含明文、轮换旧 Token 失效；真实 `@libsql/client` 与 `@tursodatabase/serverless` 均使用页面生成的地址与单库 Token 查询成功。测试没有读取或修改用户现有数据库。
- Web 类型检查和生产构建、`cargo clippy -p catalog -p db-server --all-targets -- -D warnings` 与 `git diff --check` 通过；本次未重跑完整 distributed 多节点部署。
