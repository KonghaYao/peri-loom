# 第 10 轮独立只读复核（2026-10-15）

## 判定

**单项隔离 S3 SQL 正确性复测的留存证据相互吻合；不是正式联合门禁或性能 PASS。** 本次只读审阅 `trial-ground/private/local-s3-smoke/run-1e684b90602a1d976de437ec/` 六份已有证据及已提交的 `10_plan.md`；没有重跑 workload、访问 S3、读取对象字节或清理桶。

## 独立重算

- 桶清单各条均为 `success`/`folder` 且桶名无重复：before **11**，after **12**；集合差集仅新增 `peri-p1-test-50721-18de8fa259cc20e0-6770ef1631b9/`，删除 **0**。新增桶与 `runner-result.json` 的 `bucket` 及日志保留桶名称一致。
- `objects-after.jsonl` 有 **12** 条不重复的 `success`/`file` 条目，URL 均指向本次桶：`HEAD` 1、manifests 4、segments 3、WAL chunks 3、WAL index 1；与执行器记录的 12 一致。这是对象**清单**计数，不是对象内容校验。
- 原始 `test.log` 中目标用例行 **1** 条、`PASS real S3 SQL` 标记 **1** 条、`test result: ok. 1 passed; 0 failed; 0 ignored` 结果行 **1** 条；执行器记录 `runner_exit_code=0`。日志标注 11 filtered out、用例耗时 1.41 s；用例耗时不是 SQL 延迟或业务性能数据。日志不能单独证明没有外部重复执行。
- 本地重算 `test.log` SHA-256 为 `34bd1280f67bc8dfcf41f6119f30749b24bb2a12050d2fb1a5c2ec51210b3c00`，与执行器记录一致。当前 `10_plan.md` 及 `HEAD:docs/experiment-commercial-db/10_plan.md` SHA-256 均为 `e2609bc7a721b5bc8168062d26937b59255a48686b49e43ef82ff45ca9d4a65a`，与意图中的计划哈希一致。
- `intent.json` 与 `runner-result.json` 均记录源码 HEAD `02664de196693066258ad7f8d9a83f800399a6e0`，等于复核时的 `git rev-parse HEAD`。首次检查时 `git status --porcelain` 为空；复核期间出现另一未跟踪文件 `docs/experiment-commercial-db/10_experiment_report.md`，因此**随后检查的工作树不干净**；本次没有修改该文件。当前状态不证明运行当时源码干净，也不冻结当时编译二进制。
- 私有目录 `trial-ground/private/`、`local-s3-smoke/` 和本次运行目录在检查时均由当前用户 `konghayao` 持有且为 **0700**；六份证据文件均为同一用户持有的 **0600**。这是检查时的本地权限状态，非运行全程的权限证明。

## 边界与局限

意图记录 120 秒上限、S3 客户端允许 2 次网络重试，以及容器 ID/镜像 digest；只读证据不能独立证实进程组超时机制、实际请求重试次数、运行全程容器身份/资源限额/回环映射或凭据匹配。本复核按要求未触碰凭据、未对容器或 S3 做在线复查，也未自行重新列桶；桶和对象结论仅是对已留存清单的离线一致性复算。`runner_exit_code=0` 与日志相符，但哈希和清单一致性不等于远端字节正确性、跨时间持久性或完整进程现场。没有端到端 Gateway/TLS、并发吞吐、ACK/s、p99、资源峰值、业务 SLO 或 P0–P6 联合门禁数据；均不得推断为通过。
