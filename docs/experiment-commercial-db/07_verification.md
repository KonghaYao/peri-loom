# 第 7 轮独占 S3 正确性报告：独立核验（2026-10-15）

核验对象：`docs/experiment-commercial-db/07_experiment_report.md`。逐份**只读**核对五份 `trial-ground/private/s3-*.log`、`trial-ground/private/compose.env` 中非凭据配置、`trial-ground/compose.yaml`、`prototypes/v1/tests/minio.rs` 的桶创建/保留逻辑，以及 Docker 容器只读元数据。未读取/输出私有凭据、未打印日志全文，未发起 SQL、S3 操作或负载。PASS 仅指对应的**有限正确性用例**。

## 五份原始结果

| 日志（`trial-ground/private/`） | 本项 `test result` | 文件创建时间（CST） |
| --- | --- | --- |
| `s3-conformance-2026-10-14.log` | **1 passed; 0 failed; 9 filtered**，3.63s | 10 月 14 日 23:54:17 |
| `s3-p2-recovery-2026-10-14.log` | **1 passed; 0 failed; 9 filtered**，1.54s | 10 月 15 日 00:00:21（文件名沿用前一日） |
| `s3-p3-multidb-2026-10-15.log` | **1 passed; 0 failed; 9 filtered**，1.76s | 10 月 15 日 00:01:06 |
| `s3-p4-fencing-2026-10-15.log` | **1 passed; 0 failed; 9 filtered**，2.05s | 10 月 15 日 00:01:08 |
| `s3-fresh-cache-2026-10-15.log` | **1 passed; 0 failed; 9 filtered**，0.31s | 10 月 15 日 00:01:11 |

**PASS（求和口径）：** 五份日志各只有一个 `test result: ok`，故为 **5 次单项运行、5 条通过记录、0 条失败记录**，不是一次全套测试 5/5；九项 filtered/次不是 PASS。日志的测试标识与报告列出的五个用例相符。文件创建/末次修改时间显示预期顺序、窗口无明显重叠，但秒级文件时间不证明严格串行、每次开跑前完成容器检查或完整 `cargo test --locked --test minio … -- --ignored --exact --test-threads=1` 命令。日志没有单独保存 shell 退出码、环境变量、产物 SHA256、运行前后源码身份。

## 桶、后端与资源边界

- **PASS（五份通知唯一） / PARTIAL（实际保留）：** 每份日志有一条 `retained isolated S3 test bucket` 通知，五个标识经内部去重为 **5 个不同值**，本文不披露标识。`minio.rs:46-59,248-284` 要求显式 `PERI_S3_TEST_RETAIN=1` 才打印该通知并使 `Drop` 跳过清理；桶名由进程 ID、纳秒时间和随机临时路径哈希组合，先 HEAD 检查不存在再 PUT 创建。**仅凭通知**不能证明五桶后来仍存在、持久化对象完整或没有其他桶；后续新增的事后只读清单仅能补证其采样时刻的列出状态，见文末追加核验。
- **PASS（当前本机配置/容器） / PARTIAL（逐项后端归属）：** Compose 配置的服务镜像为报告所列 digest-pinned RustFS，客户端镜像也为 digest-pinned；服务端口仅绑定回环，声明内存 `512m`、CPU `0.5`、PID 64。Docker 只读 inspect 核对**当前**运行容器的 Compose project/service label 为本试炼场 object-store、镜像 digest 与报告一致、`running=true`、内存限额 536870912 字节、NanoCPUs 500000000、9000/TCP 映射回环。`minio.rs` 采用 S3Config 网络接口且需 opt-in；若未显式给 endpoint/认证可退回另一个本地默认值。提供的五份日志不含每轮 endpoint/容器 ID/环境摘要，所以**不能独立证明每一轮网络请求都命中了这个当前容器**，也不能将今天的 inspect 当成五个历史时间点的健康状态证明。`bucket-init` 镜像固定及创建夹具桶，不等于上述测试程序的五个动态桶。
- **NEEDS_MORE（原先的资源数值）：** 配置及 Docker 当前 inspect 能证实 **512 MiB/0.5 CPU 的容器上限**，不证实测试期间实际占用。先前报告所称测试后单点 `181.3 MiB / 0.06%` 不在五份测试日志或所读预检中，仍**无法独立复核该旧数值**；后续另存的原始快照记录的是不同时间点的 **184.9 MiB / 0.05%**，见文末。任一单点都不是峰值、平均值或数据库 RSS。
- **FAIL（若外推为联合/性能 PASS）：** 五个有限网络正确性用例不包括上一轮仍 ignored 的四个 `p4_completion` 真 S3/故障用例，也不覆盖独立 release TLS Authority＋两 Worker＋Controller＋Gateway＋Key Broker 的联合门禁。缺冻结源码与产物、正式 S01/S04 独立 oracle 和 S06/S08 负载；**P0–P6 联合 PASS 未证实**。3.63s/1.54s/1.76s/2.05s/0.31s 是整项测试的 wall time，**不是业务 p99、SQL 查询时延或恢复耗时**。ACK/s、p50/p95/p99/max、Unknown、错误率、租户公平性、S3 请求放大、数据库 CPU/RSS 峰值及恢复时间均 **NOT MEASURED**；没有 SpaceX 商用验收证据。

**总体：PARTIAL。** “五个单项日志分别通过”与“当前夹具为受限、回环、固定镜像的 RustFS”有可核查证据；逐项后端身份、实际保留桶状态、瞬时资源数值、精确命令/退出码与完整联合环境仍缺原始证明。保留私有失败/运行现场，不把有限正确性结果冒充真实性能。

## 追加核验：P4 故障夹具保全限制

- **PASS（代码路径）：** `prototypes/v1/tests/p4_completion.rs:5-7` 显式加载 `p4_verify/process_support.rs`；其 `Fixture` 持有 Authority 和两 Worker 的 `Process` 及 `tempfile::TempDir`（`p4_completion.rs:38-48,55-57`）。`prototypes/v1/tests/p4_verify/process_support.rs:177-184` 的 `Process::drop` 尝试对**本测试持有的子进程**执行 `kill`/`wait` 并结束日志线程。`TempDir` 默认在正常 drop 时删除其临时目录。故若测试失败后发生正常栈展开，进程/临时目录的默认清理与“原地保全首次失败的本地状态”目标存在真实冲突；这类清理也可能在成功退出时发生，不能说只在失败时才 kill。它不等于对共享服务或本轮五个 `minio` 保留桶的清理。
- **NEEDS_MORE（保全结论）：** `p4_completion.rs:57-87` 的真 S3 分支使用显式授权的独立已有桶，所查片段未证明测试会删除该远端桶；`TempDir` 清理的是**本地**路径。失败可能以 panic、提前返回、进程中止等不同方式发生，默认 Drop 并不保证每类失败都会执行，也没有逐项失败现场/dirty marker 的保全演练证据。因此可认定**当前夹具未展示满足首败保全的契约，选择暂不运行合理**；不能断言所有故障都会丢失远端 S3 证据、已发生清理事故或已验证故障安全。四个 `p4_completion` 真 S3 用例仍是 **NOT RUN / NOT PROVEN**，不计入前述 5/5，也不据此签发联合 PASS。

## 追加核验：事后桶清单与资源快照

- **PASS（2026-10-15 00:09 CST 的桶可列出状态）：** 只读解析 `trial-ground/private/s3-retention-audit-2026-10-15.jsonl` 得到 **6 条 `folder`/`success` 记录**；内部以键名分别与五份 `s3-*.log` 的五个不同保留桶标识比对，每个**恰好匹配一次**。剩余一条恰好对应私有 `trial-ground/private/fixture.json` 所声明的试炼场桶，**在这份清单内没有第七个意外桶**。未披露任一桶名、URL 或凭据。清单文件创建约 **00:09:41**、修改约 **00:09:42 CST**；这是五份测试日志的最后修改（最后一份约 **00:01:11 CST**）之后的**单次可见性快照**，不证明每个对象完整、测试时各自连接的后端身份、现在仍存在或长期保留。
- **PASS（事后资源原始单点） / NOT MEASURED（性能峰值）：** `trial-ground/private/s3-posttest-resource-2026-10-15.txt` 明记 **2026-10-14 16:09:40 UTC = 2026-10-15 00:09:40 CST**，与文件创建约 00:09:40 CST 相容；记录的容器 ID/名称与本次只读 Docker 身份核查的**当前独占 object-store 容器**相符，并记录约 **184.9 MiB / 512 MiB**、**0.05% CPU**。这是测试全部结束数分钟后的**一个时刻**，不能证明五项测试执行期间的 CPU/RSS 峰值、均值或业务延迟，也不能从当前容器同一身份推断每项历史请求的 endpoint 归属。旧 `181.3 MiB / 0.06%` 没有相应原始快照，不能以新快照追证旧值。
- **总体边界保持 PARTIAL：** 事后清单强化“该时点五个桶仍可列出”，资源快照支持新的**事后**观测值；不改变四项 P4 故障用例 NOT RUN、P0–P6 联合 PASS 未证明和所有数据库性能指标 **NOT MEASURED** 的结论。
