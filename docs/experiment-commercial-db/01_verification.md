# 第 1 轮实验报告：独立证据核验（2026-10-14）

核验对象：`docs/experiment-commercial-db/01_experiment_report.md`。本次仅静态读取该报告、私有测试日志与预检、对应提交的测试源码及仓库场景/夹具文档；未运行测试、SQL、夹具或压力负载，未修改原报告。以下是**报告证据的核验**，不是数据库验收签字。

## 分项判定

| 主张 / 方法 | 判定 | 核验依据与边界 |
| --- | --- | --- |
| 已进行离线预检，未进行数据库压力测试 | **PARTIAL** | `trial-ground/private/challenger-preflight-2026-10-14.md` 记录未启动独立存储、S01 SKIP、S04 及其余场景 NOT RUN；报告没有给出负载运行记录。可以支持“本轮没有提供负载实测证据”，但仅凭这两份文档无法审计全局是否有人在别处发压。 |
| 夹具 Python 单测 7/7、provisioner 19/19 + 1/1、stress Go 单测 PASS | **PARTIAL** | 这些计数及命令见私有预检；没有分别提供上述三项的完整原始 stdout、退出码、执行时间与源码/产物摘要供独立复算。即使完全属实也仅说明工具离线自测通过，不等于联合 P0–P6 PASS 或 SQL 负载。 |
| 平台 `go test -p 1 -timeout 180s ./...` FAIL | **PASS（日志结论）** | `trial-ground/private/platform-go-test-2026-10-14.log` 记录 `TestP6VerifyScopedTokensAndNativeReadonly` 失败（12.51s），`/v1/sql/open` 在一次预期拒绝的调用中返回 200；随后 3m0s 整包超时，`peri-loom/platform` FAIL（180.584s）。其他子包的 PASS 不能覆盖平台包失败。日志不能证明线上越权、数据外泄或被超时中断的第二测试存在业务缺陷。 |
| “无法判断命中哪一个拒绝断言” | **PARTIAL（报告表述过于保守）** | **只看日志**只能看到 `p6_verify_security_test.go:100`，没有凭据类别；但 `git show c1eea11caffd893390001517149380e824aeb6a2:prototypes/v1/platform/p6_verify_security_test.go` 的第 100 行恰为 `fixture.deny(fixture.config.BootstrapKey, "/v1/sql/open", …)`。若日志对应这一版测试文件，则命中的是**全局 bootstrap 管理密钥**的拒绝断言，不是第 97–98 行的跨租户 scoped/project-key 断言。当前未提交工作树已把第 100 行改为允许全局密钥访问；缺当时完整源码清单，故版本关联为高可信推断、非产物级证明。 |
| 全局密钥拒绝断言与平台接口合同 | **FAIL（原测试期望与文档冲突）** | `prototypes/v1/platform/README.md` 明确允许全局管理密钥对已有数据库执行 native SQL（使用该项目配额）。旧第 100 行却要求拒绝，HTTP 200 **与文档一致**，因此不能将该条记录直接定性为跨租户安全漏洞；仍需按合同验证实际授权、配额与审计边界。该冲突及平台包超时意味着不能签发联合 PASS；修正测试期望也不能追认本轮 PASS，需对冻结版本重新完整执行。 |
| S01 / S04 / S06 / S08 已通过场景验证 | **FAIL（没有该类证据）** | 报告没有宣称这些场景 PASS，明确写 S01 SKIP、其余未运行；`challenger/ORDERED_SCENARIOS.md` 是待测场景清单而非成绩单，`trial-ground/p0/README.md` 规定独立夹具与 oracle。S04/S06/S08 的状态应写 **NOT RUN / NOT MEASURED**，而不是把 S01 的 SKIP 自动外推到它们。 |
| 当前满足独立夹具与联合门禁，可开始商用基准测试 | **FAIL** | 私有预检报告 `images_pinned=false`、`main_ready_file_present=false`、独立存储未启动、源码未冻结及无联合 PASS；`trial-ground/README.md` 明确 Main 二进制摘要声明亦不能代替联合门禁。预检属历史快照，不能代替今后的实时复检。 |
| 可凭本轮数据证明吞吐、尾延迟、生产适用性或 SpaceX 认证 | **FAIL（证据不足）** | 无独占 fixture 的负载轨迹、冻结合同/阈值、逐请求 ledger、样本、资源采样、R1/R2 复测或企业验收；原报告本身明确禁止作此类主张。 |

**总体：PARTIAL。** 第 1 轮报告作为“未获准发压、平台测试失败的预检记录”基本与证据一致；它不是商用压力测试报告，也没有任何数据库性能 PASS。唯一可独立从私有原始日志核对的执行结果是平台测试失败/超时；三组离线自测仅有预检摘要，不能独立复算。

## NOT MEASURED 与方法缺口

- **NOT MEASURED：** ACK/s、offered/planned/emitted 与实际尝试/完成/准入、client queue drop/expiry、typed 429、各类错误及 Unknown 分母、p50/p95/p99/max（ACK-only 与 all-completed）、队列等待、吞吐拐点、受害租户 offered/ACK 进展。
- **NOT MEASURED：** 数据库各角色 CPU/RSS、磁盘增长、网络、对象存储请求/失败/延迟、native queue、WAL/segment/checkpoint/GC/root CAS、恢复时间及任何受控故障后的读写恢复。预检中“磁盘约 99%”仅是主机准入观察，不能冒充数据库阶段资源测量。
- **NOT MEASURED / NOT RUN：** S01 的 SQL/事务/重连 ledger 与 TLS 校验；S04 的两项目密钥隔离；S06 容量曲线与 S08 noisy-neighbor 干扰。S01 的准入结论是 **SKIP**，并非正确性 FAIL 或 PASS。
- `docs/experiment-commercial-db/00_plan.md` 要求冻结源码/二进制身份、拓扑、预算、SLO、种子、阈值、采样窗口和独立 oracle，并在正确性门禁后逐项运行、保留首次失败；本轮无 `run.json`/`operations.jsonl`、测量区间或可复算分母，方法只能评价为**准入预检**，不能评价为压力测试。Rust debug 二进制与未提交源码一致性未证实，整包 180s 超时也不是完成的联合测试。

## 超出证据的表述及后续核验

- 原报告没有给出商用性能数字、SpaceX 背书或生产可用 PASS，**未见此类越证据的正面主张**；“无网络调用”与三组离线自测计数来自预检转述而非本次可重算原始轨迹，应注明证据层级。
- 原报告称失败的具体拒绝调用尚未定位：对**日志单独**成立；与该提交第 100 行交叉比对后，可定位为旧测试的 bootstrap-key 拒绝断言（以当时文件同版本为前提）。不要把此线索夸大为已证明代码无安全问题，亦不要把平台包超时解释为另一项测试断言失败。
- 下一次先保存冻结源码/产物摘要与完整门禁原始结果，解决合同与测试期望冲突并完整重跑 P0–P6；再实时检查独占夹具及安全预算，按 `challenger/ORDERED_SCENARIOS.md` 从 S01 起依次留存原始 oracle 与资源证据。R1/R2/R3 未完成前不得给出修复提升幅度或对外商用达标声明。

私有日志、预检只作内部证据索引；本文不转录原始日志全文、认证头、密钥或私有配置。
