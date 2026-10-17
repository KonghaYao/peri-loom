# 本地性能 harness 与 runner 静态复核

日期：2026-10-17。复核者为本 harness/runner 的原作者；本文是设计者静态复核，**不充当独立实验验证或最终对抗验证**。未构建、未执行性能测试、未修改产品代码。已阅读主线程保存的 baseline smoke v2 原始输出，仅用于核对记录与方法。

结论：修正后的固定工作量、请求计时、完整内容 oracle 和合法冷重开流程符合规划；runner 已补充三个 fail-fast 检查，可供正式 AB/BA 测量使用。尚无正式配对收益结论；来源归档仍须由独立验证者核对。

## 来源与已排除的无效产物

- 基线固定 `1198b18611601f7417f7b9ae4bbb3c370b41ff03`，Turso 固定 `24a3a4609683b73935888f8bc30b47727d100aa8`。
- 当前 [测试源码](../../prototypes/v1/tests/local_performance.rs)、[正式归档源码](evidence/local_performance.rs)、identity 与两臂 binary metadata 一致引用修正版 harness SHA-256：`99083bf52c60d2c0861a2140f2ef693001cd8bcdd7a1077b345f181036785fef`。
- 注意：[frozen-local_performance.rs](evidence/frozen-local_performance.rs) 仍是首次失败版本，hash 为 `6a64fb10937e9ab5a61b47a3c265e8e953b457eca23c9f6d1c15a7edb9ff4d48`。该文件名容易误导，复现时必须以 identity 的 hash 和 `evidence/local_performance.rs` 为准，不得混用。
- 首次旧 harness smoke 返回 `Fenced unknown namespace`；保留 `baseline-smoke.*` 和 failed-v1 证据，不能当作性能样本。
- 主线程发现 tar 源码 mtime=0 导致 Cargo 复用旧产物，保存 [invalid-candidate-mtime-binary.json](evidence/invalid-candidate-mtime-binary.json)。其中所谓 candidate 与 baseline 的 SHA 同为 `a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3`，该 candidate 无效，不能支持任何前后比较。
- 已重编 candidate 的 metadata SHA 为 `ce3744e8b9a5d5f7159f1cfe151d2c06bfe8b9dd86dedb4e8e0bac580a4e9d28`，不同于 baseline。hash 不同仅排除“完全相同二进制”问题；源码来源仍要核对 source manifest、patch、build 日志和复制流程，环境变量 source 标签不能证明编译内容。

## 完整工作量与 oracle

| 检查 | 结果 |
| --- | --- |
| 实际持久路径 | 真实 LocalObjectStore、OwnershipAuthority、fenced branch store、RemoteDatabase；没有 MemoryObjectStore 或模拟 WAL |
| 初始状态 | 512 行、三列；512 B 确定性完整 payload；表创建和初始插入事务计入单独 fixture_ns |
| 热读 | 32 次预热在计时外；256 个 query 在计时内；点查/16 行范围交替，总返回 2,176 行，逐项比对 |
| 有效写入 | 32 早期 + 512 历史 + 32 晚期单行提交，另 8 个各更新 32 行的事务；共 584 个被测 SQL 提交、832 次内容确实改变的行更新 |
| 历史保留 | 512 次历史提交真实执行且计入总量；早晚提交独立报告。configure 禁用自动 WAL maintenance，第一次显式 checkpoint 在历史和首次冷重开后 |
| checkpoint | 每个 32 行事务后调用真实 native Full checkpoint，共 8 次，事务与 checkpoint 分开计时 |
| oracle | 模型在 SQL 之外确定性生成；完整结果按 id 排序，严格检查行/列数、id、version、TEXT 类型和全部 512 B payload；不是只比较 COUNT/SUM |
| 逐阶段验证 | 初始、早期提交、历史增长、晚期提交、最终 checkpoint 各检查完整 512 行；另有两次冷进程完整检查；同一写入阶段没有覆盖先前更新的重复 id |
| 错误路径 | query/execute/checkpoint 和子进程失败均立即返回错误；成功 summary 在所有 oracle 与冷重开比对之后输出，没有 SQL 自动重试 |

SQL 字符串和模型在计时外构造，避免把格式化成本误报为数据库改善。每条 UPDATE 的 version 和 payload 都改变；`73` 与 `512` 互质，使 512 次历史更新遍历全部行，32/256 次更新阶段内部也无重复 id。阶段末 oracle 因而能核对该阶段的每次目标行变更。

## 计时范围与总和

九个固定 phase 为 `hot_reads`、`small_commits`、`history_commits`、`long_history_commits`、`cold_wal_reopen`、`writer_reopen`、`checkpoint_updates`、`checkpoints`、`cold_checkpoint_reopen`。单调 `Instant` 测量实际请求完成时间；热读包括结果物化，所有 execute 返回才结束提交计时。所有 phase 的 u64 ns 相加生成 `total_request_ns`，无重复或漏掉历史提交/检查点事务。

不计入请求总和的部分包括：建库和初始数据、预热、SQL/model 构造、验证用全表 SELECT、oracle 比对、进程 spawn/退出、drop/close、oracle.json 读写、临时目录清理。冷 phase 从子进程读完 oracle 后开始，包含新 LocalObjectStore/Authority 的加载、深校验、boot term、重新 grant、数据库打开和全表结果物化。故总量是固定业务请求集合的 wall 之和，**不是进程端到端 wall**。

读取 [baseline-smoke-v2.stderr.log](evidence/baseline-smoke-v2.stderr.log) 得到九阶段之和与 `total_request_ns` 严格同为 **25,166,108,457 ns**；stdout 确认唯一测试通过。三个重开阶段占该次请求总量 **59.12%**，历史增长占约 29.85%。这是 smoke 的组成检查，不是正式性能样本或优化收益证明。总体结论会受真实冷启动/恢复成本支配，不能用单个热提交倍数代替。

## 冷重开与 fencing 证明

父进程先 drop 原 RemoteDatabase，再生成私有 oracle，删除 cold-cache，调用 `current_exe` 的精确 ignored 测试且只设置 child 分支标记。子进程从 `objects` 读取实际 SQL 数据；oracle.json 只提供期待值和旧 lease 身份，不作为 SQL 数据后端。

每个新 Authority 通过 `state` 完成真实 namespace bootstrap，核对持久 epoch floor，要求旧 binding 的 `validate_lease` 返回错误，再以 `floor+1` challenge/grant；获取新只读 session 后用新 RemoteDatabase 查询。通过 oracle 后才写回新 binding。父进程要求 epoch 恰好加一、root incarnation 不变，再用新 Authority 获取 writer lease 继续。全流程 epochs 为 1→2→3→4；两个冷 phase 的 `prior_binding_rejected=true` 只在上述 `ensure!` 已实际通过之后输出。

这证明正常重开后的数据可读、旧 lease 被拒绝和新 lease 合法，不证明所有旧绑定失败分支都返回某个特定错误，也不证明任意并发 fencing 交错。父进程 graceful close 可能执行额外 WAL sync，因此该 oracle **不是断电或提交后立即崩溃耐久性实验**。产品的 Full sync 路径必须保持，故障/耐久性 correctness 门禁仍须另外通过。

## Runner 比较与门槛

`workload()` 只从顶层去掉 `run`、`source`、`fixture_ns`、`total_request_ns` 和待递归处理的 `phases`；每个 phase 只去掉 `wall_ns`。请求计数、行数、payload、返回行数、两个完整 digest、epoch、旧绑定拒绝标记和 phase 顺序全部参与两臂/跨轮等值比较。run/source 差异虽然不参与工作量比较，但分别必须精确匹配本次调用的期望值。

正式调用至少两轮，奇数 A→B、偶数 B→A，每个 arm 串行独立创建夹具。门槛为：每轮 `A.total/B.total ≥ 1.5`，配对倍数中位数 ≥1.5，各 phase 的配对 speedup 中位数 ≥`1/1.10`。后一个门槛等价于该阶段允许的中位延迟回退不超过 10%；这是阶段的跨轮门槛，不要求每个阶段在每轮都没有噪声回退。未通过返回 2，保留全部样本；不选最快轮次。

本次复核发现 runner 原先不能主动拦截来源相同或 summary 总和错误，已按主线程要求仅修改 runner，新增：

1. 测量前拒绝两臂完全相同的 binary SHA-256，避免再次使用 Cargo stale artifact。
2. 测量前要求当前 harness hash 与 identity 冻结 hash 完全一致。
3. 每条结果要求 phase wall_ns 全为正整数，且 total_request_ns 严格等于所有 phase 之和。

更新后 runner SHA-256：`244f59c9ed7dbde6fe4f75b7fb999953677aade7175638ee2386a06feee2fc98`，Python AST 语法检查通过。未动冻结 harness、plan 或产品源码。

## 外推边界

该工作量刻意固定单线程、本地文件系统、512×512 B 数据和 512 次真实历史增长，覆盖热读、实际提交、长历史、checkpoint 和冷重开。native pager 热缓存可能绕过 File；无 HTTP/Gateway、网络、外部 S3、审计队列、并发维护/读写、公平性、尾延迟或容量结论。宿主 OS page cache 和设备缓存保持暖态，只有应用进程与 cache 冷态。即使正式门槛通过，结论仍应限定为这组固定本地 SQL 请求，不应泛化为所有部署和读写场景至少提高 1.5×。
