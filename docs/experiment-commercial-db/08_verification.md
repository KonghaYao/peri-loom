# 第 8 轮独立验证：P4 真 S3 有限预检

## 判定

**PARTIAL（四个单项有限正确性用例通过；首项执行器失败，不构成联合门禁或性能验收）。** 只读核对 `08_experiment_report.md`、`trial-ground/p4_s3_preflight.py`、`prototypes/v1/tests/p4_completion.rs` 及私有预检目录下四份意图、测试摘要、逐项事后桶/对象清单、裁定与通过元数据；未运行 S3/SQL 测试，未接触容器、桶、锁或凭据。以下数量均为核对时所见，不代表之后的远端状态。

## 分项核验

四个意图各指向不同测试和**不同桶**。每份 `test.log` 均有对应测试 `ok`，且恰好 `1 passed; 0 failed; 0 ignored`。逐项核对结果如下；“桶增量”相对前一次事后清单（首项相对第 7 轮核验的 6 桶基线），“对象差值”仅为**不同独立桶**快照的数目差，绝非同一桶的新增/删除对象。

| 用例（均为 `actual_s3_*`） | 测试总时长 | 事后桶清单（增量） | 本桶事后对象清单（数目差） | 本地保留目录 / stderr | 执行器证据 |
| --- | ---: | ---: | ---: | --- | --- |
| `close_scope_and_grant_completion_gate` | 5.06 s | 7（+1） | 24（首项） | `0755` / 5 份 | **envelope FAIL**；`adjudicated.json`，无 `passed.json` |
| `lost_backend_response_and_dirty_restart_gate` | 6.25 s | 8（+1） | 29（+5） | `0700` / 7 份 | **PASS**；`passed.json` 退出码 0 |
| `grant_reply_loss_full_identity_gate` | 4.44 s | 9（+1） | 21（−8） | `0700` / 3 份 | **PASS**；`passed.json` 退出码 0 |
| `native_checkpoint_acknowledged_prefix_gate` | 4.56 s | 10（+1） | 24（+3） | `0700` / 3 份 | **PASS**；`passed.json` 退出码 0 |

四份 `read-only-buckets.jsonl` 分别有 **7/8/9/10** 条 `folder`/`success` 记录，且各自用例的唯一桶在相应清单中恰有一条；逐对比较相邻清单的键集合，后一次仅比前一次多出本项桶，没有消失的桶。四份 `read-only-objects.jsonl` 分别有 **24/29/21/24** 条 `file`/`success` 记录，均归属各自桶。这些本地留存的只读**事后**快照支持检查时桶和对象条目可列出，不是本验证者发起的远端复查，更不能证明对象字节完整、跨时间持久性或运行期间身份。`bucket-init.log` 存在本身不证明建桶成功。

私有预检根目录和四个运行目录均为 `0700`；每份 `test.log`、两份只读快照、裁定/通过元数据及所有 18 份子进程 stderr 均为 `0600`。首项唯一保留的本地 `p4-s3-*` 目录实际为 `0755`，不符合执行器要求的 `0700`；其 `adjudicated.json` 状态为 `adjudicated_envelope_failure_not_runner_pass`，测试和桶与意图相符，所记日志及两份只读清单 SHA256 均与现存文件匹配。首项用例绿**不能**追认执行器 PASS；第 8 轮实验报告记载执行器退出码为 1，私有裁定未独立存储该退出码。后三项各自的 `passed.json` 记录退出码 0、范围 `one ignored correctness test only`，测试和桶匹配意图，所记日志 SHA256 与现存文件一致；这些通过记录未绑定各自事后清单的哈希。

**定向代码检查：** Rust 保留目录助手显式设置 `0700`，并在关闭清理前保留目录；新增测试检查夹具 drop 后目录仍存在且模式为 `0700`。四个入口依次调用 `completion_gate(true, false, false)`、`completion_gate(true, true, false)`、`grant_reply_loss(true)`、`native_checkpoint_gate(true)`；第二项的断言覆盖丢失后端响应、Grant 隔离和脏状态重启拒绝。执行器一次只允许运行一项，校验自有对象存储身份/卷/资源限制，拒绝未裁定的先前尝试，验证恰好一项测试通过和本地日志证据后才写入 `passed.json`。这些是代码与留存元数据的核验，并非本验证者重跑测试。首项原始 `0755` 现场未被修复追溯覆盖。

## 溯源与外推边界

私有意图只记录 Git HEAD 和工作树脏状态的摘要；不能据此恢复或冻结当时参与构建的源码/二进制，也没有独立构建制品证明或 Main 签署。`passed.json` 与其引用的日志哈希是在同一执行链写入的自述证据；哈希匹配证明当前文件与记录一致，不是外部见证。首项裁定和四组只读快照也不代替持续远端审计。四项测试均为隔离环境中的单用例正确性预检，不能推断 P0–P6 联合 PASS、生产相似全栈行为或商业吞吐；S06/S08 ACK/s、p95/p99、资源峰值和业务 SLO **NOT MEASURED**。表中时间是用例总时长，不能作 SQL 延迟。第 8 轮实验报告现已列出四项结果；本验证独立核对相应留存元数据与当前代码，后三项执行器 PASS 不会追认首项的 envelope FAIL。
