# 第 8 轮：P4 真 S3 故障用例准入与首败保全（2026-10-15）

## 问题与边界

四项此前忽略的 `p4_completion` 真 S3 测试能否在试炼场自有对象存储上逐项验证，并完整保存失败证据？本轮**不是**正式 challenger、生产相似全栈、商业吞吐压测或联合 P0–P6 门禁。S06/S08 ACK/s、p95/p99、资源峰值等仍 **NOT MEASURED**。不把用例完成时间充当 SQL 请求时延。

## 方法和所有权

`trial-ground/p4_s3_preflight.py` 每次最多执行一项允许的真 S3 用例；先只读核对私有 fixture、当前运行容器的 Compose project/service/oneoff 标签、digest 固定镜像、回环端口、卷、512 MiB/0.5 CPU 限额及凭据与私有配置的一致性，再用一次性 `bucket-init` 为本次用例创建新的随机唯一桶。Rust 测试只接收本桶的 S3 变量，独立 `p4-s3-*` 0700 本地目录、子进程 stderr、输出和桶均保留。`private/p4-s3-preflight/` 有持久化锁文件；成功或失败均不自动删锁、桶、卷、dirty marker 或证据。若前一项存在未裁定结果，后续用例拒绝运行。源码工作树仍脏，意图记录仅保存 Git HEAD 和脏文件状态摘要，**不是**冻结构建溯源或 Main 签字。

## 第一项：用例通过，但执行器失败

在专属 RustFS 上运行 `actual_s3_close_scope_and_grant_completion_gate`：测试原始日志显示精确一项 **1 passed; 0 failed; 0 ignored**、约 5.06 秒（整个测试用时），本次用例自有 Authority/Worker 的 5 份 stderr 保留。本轮 `p4_s3_preflight.py` 最终退出码 **1 / envelope FAIL**：Rust `tempfile::tempdir_in` 在当前进程环境下创建的保留目录实际为 `0755`，而执行器要求 `0700`。外层私有父目录为 `0700`、日志为 `0600`；根因是测试夹具没有显式将新目录设为 `0700`。**不能因测试绿而把该次执行器判为 PASS。** 保留原始意图、`test.log`、建桶日志和失败现场于 `trial-ground/private/p4-s3-preflight/`，不重跑同一项覆盖证据。

独立只读复核确认测试摘要与本地权限异常，不将其外推为完整持久性证明；单凭 `bucket-init.log` 不能证明桶创建。后续独立核对当前 Docker 身份未改变，通过 mc **只读**记录当前桶清单（原 6 个＋该项唯一新桶＝7 个）、该桶 24 条成功列出的对象；这仅证明**检查时**桶与对象条目可列出，不证明对象字节完整或远端的长期保留。`adjudicated.json` 在校验上述原始文件及 SHA256 后只将该项标为“SQL 用例通过、执行器因目录模式失败”，不是追认该执行器 PASS，也不是联合验收。

## 定向修复与局部回归

`prototypes/v1/tests/p4_completion.rs` 的保留目录助手现显式设置 `0700`，再开启失败也不清理；新增回归验证目录在夹具 drop 后仍在且权限为 `0700`。原始失败目录保持 `0755` 不被改写，仍被私有 `0700` 父目录隔离。`trial-ground/p4_s3_preflight.py` 增强卷归属核查，拒绝无裁定的先前失败；11 条离线试炼场测试 PASS。新助手定向测试 PASS，原始原样首次失败见私有 `p4-s3-preflight/<first>/`。这些修复仅针对证据夹具/执行器，不是宣称提升数据库性能。

模式修复后的 `p4_completion` **默认本地**全项复测见 `trial-ground/private/p4-completion-full-after-modefix-2026-10-15.log`：16 passed、0 failed、4 ignored，35.77 秒；四项真 S3 的单独运行不能计入这个默认全项的 16 条通过。

## 同轮续测：其余三项独立预检

第一项失败裁定为“测试通过、执行器因目录权限失败”后，后续每项均重新核查容器身份，用**全新独占桶/独立运行目录**串行运行；没有重试或覆盖首项失败记录：

| 用例 | 单项测试结果 | 执行器结果 | 目录 / 子进程 stderr 数 | 事后只读桶清单 / 对象条目 |
| --- | --- | --- | --- | --- |
| `actual_s3_close_scope_and_grant_completion_gate` | 1/1 PASS，测试整体 5.06s | **FAIL，已裁定目录模式异常** | 首项 `0755`（外层 `0700`），5 个 stderr | 7 个桶 / 24 条对象 |
| `actual_s3_lost_backend_response_and_dirty_restart_gate` | 1/1 PASS，6.25s | 退出 0 | `0700`，7 个 stderr | 8 个桶 / 29 条对象 |
| `actual_s3_grant_reply_loss_full_identity_gate` | 1/1 PASS，4.44s | 退出 0 | `0700`，3 个 stderr | 9 个桶 / 21 条对象 |
| `actual_s3_native_checkpoint_acknowledged_prefix_gate` | 1/1 PASS，4.56s | 退出 0 | `0700`，3 个 stderr | 10 个桶 / 24 条对象 |

四个原本 ignored 的真实 RustFS 子集现在各有一份单项 `test result: ok. 1 passed; 0 failed; 0 ignored`；**四项测试通过、仅后三次执行器完整退出 0，首项执行器 FAIL 被保留并裁定**。只读桶清单相邻两次差集均恰好为当前随机新桶，最后一次列出共 10 桶（试炼场基础桶、前轮 5 个保留桶、本轮 4 个新桶）。每桶读到的对象条目分别 24、29、21、24 条，列表返回 success；这些是事后目录项计数，**不是**对象字节校验、SQL 持久性独立 oracle、S3 请求量或生产吞吐。完整原始测试、桶清单、对象目录、子进程 stderr、意图/裁定文件在 `trial-ground/private/p4-s3-preflight/`，模式 0600/0700；首项 `0755` 临时目录不得静默更改。测试无额外网关、TLS、Controller、Key Broker，debug 工作树仍脏。此前默认 Rust workspace ignored 不能凭回顾测试自动变为“全套绿色含 S3”；这四项仅作为独立逐项预检。

四项后只读资源单点 `trial-ground/private/p4-posttest-resource-2026-10-15.txt` 记录于 **2026-10-14 16:58:05 UTC**：RustFS 容器 194.9 MiB / 512 MiB、0.03% CPU、18 PID。这是事后瞬时值，**不是**测试期间峰值或数据库服务资源。

私有索引 `trial-ground/private/p4-s3-preflight/evidence-index-2026-10-15.json` 汇总四次运行的 42 项当前文件 SHA256 与保留目录模式，供审阅者定位/检查原始证据；它是事后生成的**同一工作站自报索引**，不是第三方签名、运行时二进制冻结或远端对象内容校验。

对抗审阅后，执行器新增获取本地锁之后、建桶之后及单项测试之后的容器身份重复核查，任何失配不产生 `passed.json`。这仅改善**未来**同类预检的失配检测；四次历史运行只有运行前的一次核查，不能把新增逻辑倒灌成历史请求归属或冻结证明。

任何新故障、未分类 Unknown 或身份失配均停其后的实际 S3 用例。尚无正式 TLS Gateway、项目隔离 oracle、发行产物/环境冻结或业务 SLO；S01/S04 与 S06/S08 未运行，**ACK/s、p95/p99 和性能改进效果 NOT MEASURED**，不可把这轮提交给客户作 SpaceX 性能认证。
