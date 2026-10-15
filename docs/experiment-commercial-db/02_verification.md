# 第 2 轮实验报告：独立验证（2026-10-14，Asia/Shanghai）

核验对象：`docs/experiment-commercial-db/02_experiment_report.md`。仅静态读取两份私有复测日志、测试与生产代码、接口文档，并核对现存二进制摘要和文件时间；未运行 SQL、Go/Rust 测试、夹具或负载，未改原报告。下文 PASS 仅指特定陈述与可见证据相符，不代表联合门禁 PASS。

## 证据判定

| 报告陈述 | 判定 | 独立核对结果与限制 |
| --- | --- | --- |
| 全局管理密钥 SQL 拒绝断言与接口合同冲突；已修改权限测试 | **PASS（测试合同修改）** | `prototypes/v1/platform/README.md` 明确允许全局管理密钥在现有数据库执行 SQL；`git diff` 显示 `p6_verify_security_test.go` 删除全局密钥拒绝断言，新增本项目查询及跨项目 open/close；`tests/user-api-live.py` 改为全局密钥 open/query/close。原有 scoped/project-key 跨租户拒绝断言仍在。在线 Python 脚本无执行证据。改的是测试，不是生产权限实现；不宜称已修复生产缺陷。 |
| 首次重试因 Rust 二进制路径不可用而失败 | **PARTIAL** | `trial-ground/private/platform-go-recheck-2026-10-14.log` 多处报告 `PERI_RUST_BINARY must be an actual built executable`，`peri-loom/platform` FAIL（51.016s）；`p6_verify_fixture_test.go:72-79` 的校验也会在文件缺失、非普通文件或不可执行时给出同一消息。日志**不能证明**文件是在命令开始后由外部清理、谁清理、何时清理，也不能确定只有路径缺失一种原因。该轮 native 集成用例在前置二进制校验处失败，不能用于判定 SQL 功能好坏或权限修订是否通过。 |
| 隔离二进制 SHA256 为 `ef01a1fd58a7c45ce2dab7e3bc17fa5f25e7f7d43009aef97da2fe8454eaba74` | **PASS（当前文件摘要） / NEEDS_MORE（运行产物身份）** | 对当前 `/private/tmp/peri-p6-isolated-target/debug/peri-loom-v1` 重新执行只读 SHA256 计算，结果与报告逐字一致；其当前文件时间为 2026-10-14 21:50:23 CST，早于隔离复测日志创建。没有运行前后产物摘要、冻结源码/构建清单或本轮命令的完整执行记录，不能证明运行时使用的字节与**当前**文件完全相同，更不能据此宣称 release 产物或可复现构建。 |
| 隔离复测 `go test -p 1 -timeout 10m ./...` 平台包 FAIL 152.087s，两个子包 PASS | **PASS（日志结果） / NEEDS_MORE（完整命令溯源）** | `trial-ground/private/platform-go-isolated-recheck-2026-10-14.log` 末尾确为 `FAIL peri-loom/platform 152.087s`；`cmd/peri`、`sdk/go` 显示 `ok (cached)`，`cmd/peri-platform` 无测试。日志没有命令行、环境和完整构建摘要，故报告所写的 GOMAXPROCS、二进制路径、`-p 1`、`-timeout 10m` 只能据报告转述；`(cached)` 不等同本轮子包重新实测。 |
| 新的失败出自既有未跟踪平台测试 | **PASS（当前工作树及日志） / NEEDS_MORE（历史时序）** | `git status` 显示 `prototypes/v1/platform/publish_image_visibility_test.go` 为未跟踪文件；隔离日志**唯一显式的 `--- FAIL:`** 是其中 `TestPublishImageInvalidatesAlreadyLoadedWorker (4.34s)`，第 37 行记录旧/新会话均 400 `sql_error`、schema 为 200 且结果 `[]`，第 45 行判失败。Go 默认 `go test ./...` 会编入包内 `*_test.go`，故这条观测确由该测试的断言报告，不是修订后的 P6 权限断言。未跟踪状态不能单独证明测试首次添加的时间或生产代码引入缺陷的时间；“首败”只限本次日志中显式失败。 |
| HEAD 发布已 ACK 后的旧/新会话及 schema 不可见 | **PARTIAL** | 测试 `stageAndPublishImportedImage` 检查 `publish-image` CLI 返回 `ok=true` 与非空 manifest ID；`prototypes/v1/src/import_cli.rs:233-247` 表明该 CLI 先写 HEAD 再打印 ACK。日志支持在这条测试路径下读取失败。尚无独立存储 HEAD/manifest/ledger 核查或重启读回；不能据此证明数据丢失、持久化失败，亦不能断言是缓存失效还是夹具/语义问题。测试含两种情形（同 Worker 已载入、新会话），当前生产运行时存在按数据库复用实例的代码路径（`prototypes/v1/src/runtime.rs`）；可能需要失效机制，但仅凭这些材料不能确诊根因。 |
| P6 权限用例已通过，耗时 12.531s；fresh-worker-migration 测试通过 | **NEEDS_MORE** | 隔离日志是默认非 verbose `go test` 输出，未列该 P6 用例或 fresh-worker 用例的逐项 PASS/耗时；仅能说日志**没有报告它们失败**，不可独立核实“12.531 秒”或声称 fresh-worker 用例明确 PASS。首次重试中 P6 用例因二进制前置校验 0.00s FAIL。应保存 `go test -json` 或 `-v` 的逐项事件、退出状态并固定源/产物后重核。 |
| 已解决首轮合同问题、修复发布后可见性、达到 P0–P6 联合 PASS | **FAIL（作为完整修复或门禁结论）** | 权限测试合同修订可见，但没有生产逻辑修复 diff 或冻结产物的完整通过记录；新测试失败且首轮用例逐项通过缺明确日志证据。原报告**没有**宣称发布问题已修复或联合 PASS，这一点谨慎且正确。两轮包级结果均为 FAIL，不能以局部无失败代替联合门禁。 |

## 时间、指标与结论范围

- 文件系统记录（本机 CST）：首次重试日志创建 **21:54:22**、末次修改 **21:55:17**；隔离复测日志创建 **21:59:50**、末次修改 **22:02:23**；报告文件末次修改 **22:03:30**。日志创建/修改时间与包级 51.016s、152.087s **大致相容**，但不是可信的进程起止时间戳；日志中 21:54:39、22:00:11 仅是局部事件。不能从文件时间推出二进制“运行中被外部清理”，也不能从 152.087s 推出单用例 12.531s。
- 本次可准确核对的耗时是日志的首次包级 **51.016s**、隔离包级 **152.087s** 和新失败测试 **4.34s**；P6 权限用例 **12.531s：NEEDS_MORE**（未见逐项计时原始证据）。当前隔离二进制 SHA256 **PASS（仅当前文件）**，运行时 SHA256 **NEEDS_MORE**。
- **NOT MEASURED**：ACK/s、offered/admitted/actual、p50/p95/p99/max、client drops/Unknown 分母、吞吐拐点、磁盘/CPU/RSS、网络/对象存储、恢复与租户干扰；测试/包耗时不等于业务延迟。S01/S04 正确性和 S06/S08 容量均 **NOT RUN / NOT MEASURED**；S01 仍为准入 **SKIP**，不是性能或事务 PASS。
- 后续须保存独立 fixture 的源码与二进制运行前/后摘要、逐项 JSON 测试事件、可核对的命令与 UTC 起止时间、发布前后 HEAD/manifest 及新 Worker oracle。先复核测试合同与隔离复现，再判定是否存在生产实现缺陷、修复范围及重跑结果；不可把“测试期望修改”或“新增测试暴露失败”写成已修复数据库问题，亦无 SpaceX 审阅/认证证据。

私有日志只用于内部索引；本文没有摘录凭据、私有配置或日志全文。
