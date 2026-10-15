# 第 2 轮：修正权限验收合同后的联合测试复核（2026-10-14）

## 假设与边界

第 1 轮的全局 bootstrap key SQL 拒绝断言与已发布的权限合同冲突：全局管理密钥可对既有数据库执行 SQL；修正测试期望后，重新进行平台包级验证。本轮仍非负载基准，不用任何测试耗时外推 ACK/s 或 p99。源代码含未提交变更，不签发 P0–P6 联合 PASS。

## 可核查观测

| 项目 | 观测 |
| --- | --- |
| 修复范围 | `prototypes/v1/platform/p6_verify_security_test.go` 保留租户密钥隔离断言，改用跨项目全局管理员允许读取的断言；`prototypes/v1/platform/tests/user-api-live.py` 同步更新合同（在线脚本未运行）。 |
| 首次重试 | 仓库内 `prototypes/v1/target/debug/peri-loom-v1` 在核查时已不存在；依赖该路径的 native 集成测试报 `PERI_RUST_BINARY must be an actual built executable`；日志 `trial-ground/private/platform-go-recheck-2026-10-14.log`。日志不能证明何时/由谁清理或是否另有校验原因，此轮不能作为产品回归证据。 |
| 独立二进制 | 隔离 Rust debug 二进制 `/private/tmp/peri-p6-isolated-target/debug/peri-loom-v1`，本地 SHA256 `ef01a1fd58a7c45ce2dab7e3bc17fa5f25e7f7d43009aef97da2fe8454eaba74`；尚未将其视为冻结发行产物。 |
| 全量复核 | `GOMAXPROCS=2 PERI_RUST_BINARY=/private/tmp/peri-p6-isolated-target/debug/peri-loom-v1 go test -p 1 -timeout 10m ./...` 在 `peri-loom/platform` 包 FAIL（152.087 秒）；子包 `cmd/peri` 与 `sdk/go` PASS。原始日志 `trial-ground/private/platform-go-isolated-recheck-2026-10-14.log`。 |
| 新的首败 | `TestPublishImageInvalidatesAlreadyLoadedWorker`：已 ACK 的 HEAD 发布之后，旧会话及新会话查询 `imported` 都返回 400 `sql_error`，schema API 返回空列表；测试在 `prototypes/v1/platform/publish_image_visibility_test.go:45` 判 FAIL。相同测试文件的 fresh-worker-migration 场景未在本次日志中报告失败。 |

修订权限用例的单独运行结果由执行协作者报告为通过（12.531 秒），但未保存逐项原始计时日志，须独立复核，且不能证明完整联合门禁通过。新增失败用例来自工作区内既有未跟踪测试；它触及发布后同一 Worker 的可见性，尚须进一步定位是否为实现缺陷、缓存失效或测试夹具问题，不能抢先称已修复或已确认丢失持久化数据。

## 性能与停止条件

| 项目 | 状态 |
| --- | --- |
| ACK/s，延迟 p50/p95/p99，错误与 Unknown，资源、恢复 | NOT MEASURED；没有使用正式 stress runner |
| S01/S04 端到端正确性与 S06/S08 容量 | SKIP/NOT RUN；联合 PASS、独占 S3 与源代码/产物冻结仍未就绪 |
| 商用可用性、SpaceX 审阅/认可 | NOT PROVEN；不得对外声称 |

保留两次原始日志，不重置快照或绕过失败。先修复/排除新可见性问题、重测本条并扩大到完整门禁，独立验证后才准入 S01 与后续吞吐实验；前一次准入报告 `01_experiment_report.md` 不覆盖。

## 夹具准备补记（复核失败之后）

通过离线 CLI 检查固定本机 RustFS/MinIO 客户端镜像 digest，并使用 `trial-ground/trialctl.py infra-up` **仅**创建独立回环 S3 对象存储和独占桶；`trial-ground/private/infra-ready.json` 已留存。这是基础设施准备，不是数据库运行、联合 PASS、S01 正确性或性能测量。运行实例需保留供后续独占夹具使用；失败时不擅自清理卷。
