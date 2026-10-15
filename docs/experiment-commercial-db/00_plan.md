# 商用数据库压力测试：实验与交付计划

状态：**计划，不是测试结果或认证**。面向 SpaceX 等严格企业的技术审阅材料；不得写成“SpaceX 已审阅/认可/认证”，也不得在没有对应实测证据时宣称商用或生产级 PASS。最终报告应提供可复算的数字、首次失败、修复前后对照、适用边界与未测项，而不是营销性能声明。

## 当前事实与范围

- `challenger/ORDERED_SCENARIOS.md` 是按风险排序的待测清单，不是完成记录。先处理 S01、S04、S03；S02、S28 等需要安全故障夹具，不因优先级高就擅自注入。正确性门禁通过后才考虑 S06（单库吞吐）、S07（热点）、S08（租户干扰）等有限容量场景。每项按自己的合同报 `PASS`、`FAIL`、`SKIP` 或 `NOT MEASURED`；不能把 S10–S40 当作当前 runner 已支持的 case。
- `prototypes/v1/stress/README.md` 自述为实现与单元验证状态，尚无该工具执行真实 SQL 负载的证据。工具现有数据量仅 1,000/10,000 行、并发 1/2/4/8，默认 5s 预热、10s 冒烟、30s 测量及全局 offered 50 ops/s；这些是初始限定覆盖，不是高规模商用结论。更短阶段标记 `short_gate_not_benchmark`。当前明确 SKIP 的冷/热 native cache 清零、突发 Worker 故障、租约/存储故障、GC pin 和分支/PITR 交错，不得从近似场景外推。
- 已使用 `go run ./cmd/peri-stress -list` **仅列出能力、不连接/发压**：S06 最初限定覆盖可选择实际存在的 `capacity_read` 与 `capacity_short_writer`，S08 的干扰维度需要预先配置独立项目/key 的 `noisy_tenant` 或 `capacity_competition`；这些 runner case 不是 challenger S 编号的自动等价证明。每轮独占一个 case，先写独立 oracle 与预冻结参数，禁止以默认 `-cases all` 把多个风险维度混作一次实验。
- 现有私有预检 `trial-ground/private/challenger-preflight-2026-10-14.md` 记录 S01 为 SKIP、后续未执行、性能指标 NOT MEASURED；当时缺独立对象存储/固定镜像、匹配源码的 P0–P6 联合 PASS，且平台测试出现需定位的失败和超时。预检是历史快照，不代替执行前实时检查；离线单测通过不等于联合 PASS。本文不引用私有凭据或把旧观察当成当前性能数字。

## 角色、证据与预冻结合同

Main 独占执行权和门禁签发权。并行分工仅限非发压工作：场景/威胁模型、独立 oracle 实现与验算、资源监测/原始证据归档、修复设计和对抗审阅可由不同协作者承担；**发压、故障注入、管理变更及复测必须串行、由同一个 Main 管控**。审阅人不依赖负载器的 PASS 位或 Main 的总结，独立重算原始 ledger/hash 与分位数。

每轮开始前冻结并记录：Git commit、dirty/untracked 源文件清单及内容摘要、构建命令和二进制/镜像 digest、负载器摘要、依赖版本、拓扑与进程/容器身份、各角色实际资源限额、专用数据库/tenant/branch/桶身份、TLS/CA、种子、SQL/业务操作分布、数据规模、并发、offered 阶梯、预热/测量/排空窗口、采样频率、恢复定义和停止阈值。变更任何一项均产生新轮次/新版本，不能覆写原轮证据。事先填写目标环境和业务 SLO：ACK/s 下限、ACK 与全部请求 p95/p99 上限、typed 429/错误/Unknown 上限、受害租户最小进展、CPU/RSS/磁盘/对象存储预算、恢复时间；缺预冻阈值只报告数字，不给性能 PASS。先做容量和磁盘余量预检，资源不足时不得借用现有进程或共享用户数据库。

执行必须满足与当前冻结源码相匹配、由 Main 确认的 P0–P6 **联合 PASS** 证据（attestation 不是密码学证明）；定位并修复联合门禁失败后重新验证，不得由本计划自动伪造 PASS。使用新建、独占且可丢弃的 release-mode S3 fixture：Authority、至少两 Worker、Controller、Gateway、Key Broker 与独占 S3 兼容桶；真实 S3 凭据仅给 Authority，保持 TLS 校验。独占 fixture/namespace/project/branch 与预算须由 Main 核实，不连接现有用户数据库。私有 `connection.json` 和输出目录 0700、证据文件 0600，认证头和私钥不进公开报告。provisioner 先 plan 后经批准 execute；其 reservation、部分结果及不确定管理调用需保留，不自动重试或删除。

注意：`prototypes/v1/tests/minio.rs` 的 `TestBucket` 析构器会删除该测试临时创建的 bucket；在需要保全**首次 S3 集成测试失败现场**的验证轮次，不得直接把 `--ignored` 测试指向本试炼场作为联合 PASS 证据，除非先提供测试级失败保全契约并证明不会删除不确定结果。普通 cargo 单测忽略的 S3 用例也不能计入联合门禁。

每次运行先验证全局 `/tmp/peri-loom-stress.global.lock` 和输出根 `.stress.lock` 独占，确认进程身份、CA、空余资源、原始结果写入能力。不能因锁“看起来过期”自动删锁；只能在证明旧 runner 已终止、夹具归属和现场保全后按批准流程处理。runner 自带请求/输出安全上限、独立会话、无 SQL 自动重试；不能以这些上限冒充 native RSS 硬限。每次只跑一个测量 case，先小规模冒烟及 oracle 校验，再逐级放大；一次只改变一个主要维度。所有管理/故障操作遵循已批准的夹具合同，不对共享 PID 发信号、不删桶、不清 dirty marker。

## 独立正确性 oracle 与停止规则

测试前由服务外独立生成期望主键/行 hash、账户总额、事务业务 ID 和 `(business_op_id, ordinal)` ledger；持久化签名/摘要及种子，和负载器记录分开保存。每阶段前后由**新会话**在正确 DB/branch 身份上读回，另一个只读验算程序独立对比已确认写入集合、重复/缺失项、约束回滚及结果 hash；从数据库导出的 ledger 不能反过来定义“预期已确认集合”。对已发送未获确认请求单列 Unknown，不能当失败回滚或自动重试；先以 durable ledger 核查并在仍不确定时人工归类，保留 request ID、时间窗和相关原始响应。独立审阅者复核 oracle 实现、权限隔离和样本，不用负载器自身成功计数代替持久化校验。

任何已确认写入丢失/重复、跨租户读取、旧 epoch 写成功、无法归类的 Unknown、初始正确性失败、夹具/锁归属异常、监测身份丢失或资源/预算越线，即停**后续发压**，固定首次失败的日志、ledger、响应和环境身份；仅按批准合同收尾，不清场覆盖。不论修复是否成功，失败轮在报告中仍为 FAIL；无法安全满足前置条件则 SKIP（指标 NOT MEASURED）。故障注入必须有安全钩子和独立恢复 oracle，否则只记录 SKIP。

## 串行实验（至少两轮）

1. **R1 基线/暴露问题。** 在门禁及资源合同通过后，单独跑 S01 冒烟并读回；再按 `challenger/ORDERED_SCENARIOS.md` 风险顺序尝试可安全运行的 S04、S03。每项不具备 hook/oracle 时 SKIP 并记录缺口。正确性稳定后，逐项执行可覆盖的 S06/S07/S08：依工具的实际 case、offered、并发、规模与资源限制做有限曲线，保留未达到拐点的 `NOT FOUND`，不得把客户端 queue drop 当服务器饱和，也不得把“无 429”写成达到 admission knee。R1 即使失败也保留首发现场，不为凑数字改阈值重跑。
2. **问题定位与定向修复（不发压）。** 根据 R1 原始 request/ledger、服务日志和资源采样形成问题单：复现条件、根因假设、影响面、最小代码/配置修复、回归测试及新源码/二进制摘要。安全/语义缺陷先修正确性；没有证实缺陷不虚构问题或宣称“修复带来提升”。修复后的门禁、独立夹具及预算重新核验。
3. **R2 修复后复测。** 新 fixture、新种子/运行 ID，按照预冻结的与 R1 相同硬件档位、配额、负载矩阵和统计口径串行重跑失败场景及必要对照；先证明 oracle 正确，再展示 ACK/s、p95/p99、错误/Unknown 与资源差值及原始分母。若 R1 没有可修复问题，R2 做独立重复性/环境敏感性实验，明确标注“未执行修复比较”；不能暗示存在提升。新出现失败依停止规则终止，不用 R1 的 PASS 掩盖 R2 的 FAIL。
4. **R3 对抗复核。** 审阅人对 R1/R2 原始文件、构建/夹具身份和 SHA256 做独立重算；用未参与调参的预冻结种子或单一扰动进行一次独立有限复测（只有全部门禁仍成立时），重点检查持久化 ledger、越权、尾延迟、饱和与恢复。若没有安全 hook 或独立 oracle，则不运行对应故障挑战，保留缺口为 SKIP；发现分歧保留原结论与新的反证，进入新修复轮次而非改写历史。

## 指标口径与可审计交付

按轮次、场景、阶段、project/DB/Worker 输出 offered/planned/emitted、client queue drop/expiry、实际尝试/完成、业务 ACK、typed 429、其它已知错误、超时和 Unknown 的**计数及分母**。ACK/s = 测量阶段经持久性核对的已确认业务工作流数 / 实际测量及排空的单调时钟秒数；不把 offered、HTTP 超时、事务内部语句或重试计作 ACK。工作流从计划、排队、HTTP 到确认分别记录：ACK-only 与 all-completed 延迟的 p50/p95/p99/max（明确单调计时、nearest-rank 和样本数）、client queue p95/p99、分阶段总耗时；不能用成功请求 p99 遮蔽失败尾部。记录容量拐点/未找到、受害租户 offered 与 ACK 进展及实际 admission 结果。

每阶段保留按角色的 CPU/RSS 原始采样（注明 host `ps` 与 Linux `/proc` 不同口径、PID/start 身份）、磁盘使用/增长、网络及对象存储请求/失败/时延、WAL/segment/checkpoint/GC/root CAS 与 native queue 等**实际暴露**的原始指标、采样覆盖和缺口；`ps` 不冒充 native cache，管理 map 存在不代表各 worker 指标均可用。可控恢复演练记录故障起止、重新可读/可写及 ledger 验证的时间点和恢复耗时；未演练写 `NOT MEASURED`。缺采样/无法可靠归属的值一律 `NOT MEASURED`，不是 0，也不是 PASS。

私有证据包保留每轮 `run.json`、`operations.jsonl`、冻结 source/artifact manifests、门禁证据、预冻结合同、采样日志、oracle 输入/输出、Unknown 处置记录、首次失败及修复 diff、复测与对抗签字、清理/留存记录及文件摘要。公开报告仅提供脱敏的可复算汇总、环境/日期/局限、问题单与修复前后表格，原始凭据和客户数据不外流。验收逐场景给出阈值判定和证据索引：未填阈值、缺数据、共享主机或仅单测通过都不能上升为商用容量/可靠性或 SpaceX 认证结论。
