# v1 用户验收路径

本目录的用户路径以真实可运行入口为准。每组使用有序操作步骤，供人工逐项验收；
功能验收、故障演练和性能测量分开记录。测试通过不等于生产 HA 或无限资源保证。

## 最新已观察入口（2026-10-08）

1. 最近已观察的运行入口为 `https://127.0.0.1:19457/`；本节不执行新 health probe，
   不保证阅读时服务仍在线，也不表示用户已按本页完成手工验收。
2. 使用私有 `0600` 连接文件：
   `/private/tmp/peri-sql-open-diagnostic-6kd1nb3v/peri-stress-693cbb766368/connection.json`。
   按其中 proper CA 正常验证 TLS；项目 Key 用于项目 SQL/管理，不 dump Key、不用 S3 后台 Key 或全局管理员 Key 替代。
3. 此 launcher 配置的数据库是 `local_default/demo/main`；SDK/UI 使用该配置，不混用 stress parent config 或旧 Unknown 库。
4. 已接受的实际范围见 [SDK 四 case / UI 功能](FINAL-SDK-UI-GO-OBSERVATION-VERIFICATION.md)、
   [common 13 scenarios](FINAL-COMMON-OBSERVED-VERIFICATION.md) 与 [Linux Local](FINAL-LINUX-GO-OVERLAY-VERIFICATION.md)；
   仅限各报告的实际版本与范围，不把历史 proof 冒充新版本调用证据，不表示全覆盖或 Linux S3 部署通过。
5. 后阶段 [UI 19457 own-profile cleanup](UI-19457-PROFILE-CLEANUP-VERIFICATION.md) 已独立接受，不再 pending；
   旧 Unknown、原 supervisor exit1 及其原始证据保留，不洗绿，不对旧会话盲 Close、重开或重放。
6. 后续操作按 [运维](USER-PATH-OPERATIONS.md)、[平台](USER-PATH-PLATFORM.md)、[数据库](USER-PATH-DATABASE.md) 路径逐组授权执行；
   写入、恢复、迁移、故障演练使用另行授权的独立目标，不因本节入口可用而自动执行。
7. 三种 SDK 与 UI 分别按 [SDK](USER-PATH-SDK.md)、[平台](USER-PATH-PLATFORM.md) 和 [调用者安全](USER-PATH-CALLER-SAFETY.md) 路径记录；
   Unknown/transport 失败立即停止，不自动重试、清理或读回作为取消证明。
8. 并发步骤继续见 [并发](USER-PATH-CONCURRENCY.md) 与 [Writer admission](USER-PATH-WRITER-ADMISSION.md)；
   [最新限定联合验收](FINAL-JOINT-DELIVERY-VERIFICATION.md) 为 prototype `FUNCTIONAL ACCEPT`，远端发布未证明、cold/HA 覆盖仅部分、非 production READY；
   当前边界仍为 `PERF NOT PASS`，不抵消旧 Unknown，也不等于用户手工验收通过。

## 阅读与执行顺序

1. 阅读 [运维路径](USER-PATH-OPERATIONS.md)，构建并启动独立的测试实例，确认
   TLS、私有连接配置和健康状态。不要复用生产数据、已有 S3 bucket 或其他实例的目录。
2. 阅读 [平台路径](USER-PATH-PLATFORM.md)，验收项目、数据库、权限、Token、
   管理界面和 SQL 会话。每组使用独立测试标签，保留可见结果。
3. 阅读 [数据库路径](USER-PATH-DATABASE.md)，验收 SQL、事务、快照、分支、
   PITR 和存储一致性。特别注意托管数据库与底层原生 CLI 的不同权限边界。
4. 阅读 [SDK 路径](USER-PATH-SDK.md)，分别验收 Go、TypeScript 和 Rust 客户端，
   核对 TLS、整数精度、请求关联及错误处理；不要把 UI 成功当作三个 SDK 均通过。
   继续阅读 [调用者安全路径](USER-PATH-CALLER-SAFETY.md)，区分正常 ACK、未知结果与明确迁移，
   核对显式关闭/重连的前提、TLS 与独立观察凭据；该清单不代表用户已手工执行。
5. 阅读 [并发路径](USER-PATH-CONCURRENCY.md)，使用独立客户端验收持锁竞争、
   holder 提交进展和已提交前缀；这些是候选人工步骤，不代表已经执行或性能通过。
   需要显式 writer profile 时，继续阅读 [Writer admission 路径](USER-PATH-WRITER-ADMISSION.md)；
   其中 8 组有序操作是尚未按该页执行的手工验收说明，不代表性能通过。
6. 回到运维路径，按明确前提执行重启、故障恢复和压力测试；这些步骤不能在未知
   SQL 结果后靠自动重试继续，也不能对正在使用的数据库直接运行原始存储命令。
7. 为每组记录实际版本、入口、操作结果和证据；失败时保留第一次失败，不覆盖或
   删除原始日志。只有执行并核对预期结果的组才能记为通过。

## 验收记录模板

1. 记录父仓库提交、Turso submodule 提交、操作系统和实际二进制版本或 SHA256。
2. 记录本组标题、独立测试项目/数据库标签、配置文件路径和服务地址；不得抄录
   API key、Token、S3 secret、私钥或本地 KEK 文件内容。
3. 逐步执行所属路径的有序列表，记录实际返回、界面可见状态或脱敏日志路径。
4. 检查本组预期结果与限制；标注 `PASS`、`FAIL` 或 `NOT RUN`，不要将跳过记为通过。
5. 如请求结果未知，停止本组，保留 session、请求 ID 和证据；先确认持久业务状态，
   不重放事务、不盲目关闭或把连接中断当作执行取消证明。

## 范围与证据

1. 实施状态见 [IMPLEMENTATION.md](IMPLEMENTATION.md)，跨阶段功能证据见
   [JOINT-VERIFICATION.md](JOINT-VERIFICATION.md)。它们不是要求人工重复所有测试。
2. 压测报告见 `../experiment-v1-stress/`；以各报告明确的实际执行范围和判定为准，
   部分测量、未运行场景和未知结果都不能算完整性能验收通过。
3. 本地原型为单 Authority、单平台、两个 Worker 的部署形态；加密、资源限制、
   故障恢复和启动关闭边界以各路径的说明为准，不假设已经具备生产级高可用。
