# 验证报告 02：外层读锁、增量 WAL 复制、段发布与 memo

## 总体评估

**PARTIAL，可进入综合结论；不需要扩大本轮实验。** 原始记录、表格和冻结源码相符，未发现推翻所述局部机制的数据错误。H3 的实际 File 门闩对照、H4 的真实校验函数计数、H5 的去重反例和 H6 的既有断言均支持报告收窄后的结论。它们不支持当前完整请求性能、优化优先级或并发补丁完整正确性。PARTIAL 来自研究覆盖与复现入口的实际限制，不能解读为已测机制失败。

本审查只读取代码、日志、版本对象并独立计算数据，没有构建、重跑实验、修改业务代码或派发 agent。审查对象为 `identity.json` 中的 c8990c406588a23a203a46033cf10103681d188c；Turso 身份为 24a3a4609683b73935888f8bc30b47727d100aa8。

## 逐维度评估

### 数据完整性：PASS

- 从 `02-frozen-raw.log` 与 `02-patched-raw.log` 独立提取全部 RW02 JSON，添加 raw_file 后与 `02-results.json` **逐条全等**：21 条，H3 为 11 条（维护夹具 1、旧/新各 5），H4 为 6 条，H5 为 4 条。H6 不在这 21 条中，其证据是初始构建及既有对照日志中的原测试 PASS，报告没有把它伪装成新的数值样本。
- H3 旧版 Shared 不可得、同 IO 两臂 Timeout、独立 IO 与直接 storage 完成；补丁版 Shared 可得、四臂完成。started 均 true。释放后 join、旧阻塞臂最终返回成功由测试中的断言和整体 PASS 支持，没有单独 JSON 事件。维护夹具亦为 PASS。
- H4 按 `32 + frames × 536` 重算得到 34,336 / 548,896 / 4,390,944 B，与 image_bytes、copied_bytes 及表格完全一致；每次新增 scanned_frames=1、scanned_bytes=568。三个 index 为 584/586/586 B，entries、receipts、receipt_digests 均为 2。no_new 三条的 copied/scanned 都是 0。它们是三种确定输入的计数，不是统计重复、时间样本或总分配观测。
- 对 H5 全部逐对象 events 重新按 class/op 计算次数和字节，所有 totals 与两张表一致。四臂新段 PUT 为 **8/1/0/8**，body 为 1,048,576/131,072/0/1,048,576 B；segment GET 均 8 次，返回字节为 **0/917,504/1,048,576/0**。不同段 key 为 8/1/8/8；命中为 0/7/8/0。manifest GET 各 1 次、返回 0 B；PUT 各 1 次，前三臂 808 B、满段臂 809 B；HEAD 无 GET、各 PUT 79 B。32/4/0/1 的比值计算正确。
- 日志记录初始 memo 1 test、冻结新夹具 4 tests、补丁夹具 1 test、既有三个测试各 1 test 全部通过。没有发现日志之间的数值矛盾。时间线披露了从未编译的草稿修改，未将草稿当作运行源码。

### 方法正确性：PARTIAL

**H3 的锁归因对夹具成立。** GateStore 在目标段 GET 进入时发送信号并停在 release 通道，try_lock 直接观察真实 Shared。独立 RemoteIO 用不同缓存目录且 `Arc::ptr_eq` 排除同 Shared；直接 storage 臂绕开 File 外层。这两项正控制与最小 pread 补丁共同排除了“整个 backing/store 都无法前进”的解释。新旧臂测试源码相同，业务差异为归档的小补丁，环境变量只改变预期和观察窗口。旧源码的 guard 包含实际 MutexGuard，其生命周期与 GET 内的 try_lock 观测相符。

started 信号在调用前发送，不能证明接收时线程已经执行到 mutex；1 秒 Timeout 不能单独证明锁等待。这里有持锁直接观测、两个能推进的控制和补丁配对，足以支持这个确定场景的阻塞域归因；只有每臂一次，不能估计一般调度概率或延迟分布。报告正确地将该窗口写为判定窗，并未用测试总时间计算加速比。

另有一项读成功的边界应在综合结论明示：`02-io-tests.rs` 的 read helper 只检查 `pread(...)?` 与 Completion.finished，**没有断言返回字节数或 buffer 内容**；直接 storage 也丢弃返回计数。冻结 `finish` 在业务错误时会返回 Err，因此本夹具并非将已知业务错误当成成功，但它证明的是 API 完成且未返回错误，不能证明读出了预期 32 B。WAL 本来为空，完成的 WAL pread 是 EOF 路径；合成主库也不是 SQLite 文件。它仍真实竞争 File Shared，可以验证调度，不能验证有效 WAL/SQL、pager 缓存或读取一致性。

maintenance 测试取得真实 `maintenance_operation` 返回的写 guard，检查同 operations 的 try_read 被阻塞，并让真实 WAL File 完成，释放后读锁恢复。它证明两种锁域独立；并未运行 checkpoint 或前台 SQL，也没有验证前台等待/超时实现。这一范围与报告的持锁夹具声明一致。

**H4 的真实函数与控制变量成立，覆盖不包含完整提交和 index 曲线。** fixture 使用实际 LocalObjectStore/RemoteWal 生成 index/chunk，然后先调用真实 validate_index_state，再显式调用真实 validate_index_append。冻结实现为 Vec 深 clone，紧接着 observed_copy(image.len())，因此 copied_bytes 是源码明确执行的镜像复制口径；不是系统 allocator、峰值内存或整次提交成本。每臂清零计数后才调用增量函数，在保存 observed 后再全量重建，避免把冷重放计数混入；image/commit 与 cold 相等、previous 未变均有断言。

裸 RemoteWal 的 append 自身没有走 Authority clone；数据只能归因于随后显式调用的 Authority 校验函数。新扫描固定一帧是纯尾追加且保留已验证前缀的结果。既有 overwrite 测试实际修改前缀、修复再追加，断言重新扫描 header+6 frames，并与 cold 比较，足以限制“所有 append 都固定一帧”的外推。no_new 违反新增 LSN/entry/receipt 条件，是 clone 前的早期拒绝，不是合法空 sync 或提交的优化反例。上述区分报告均已明确。

三种规模只改变一个初始 record 所含帧数，新增后 index 的三种计数始终 2，JSON 大小几乎不变。因此它隔离了活跃镜像复制量，**没有完成累计 receipts/index 规模增长的运行验证**，也没有覆盖 BranchStore 增量分支命中率、cold/CAS retry、compact 周期或真实 Authority session。保留源码推理即可，不需要为本报告补跑这些实验。

**H5 对低层 publish 的边界反例成立。** 独立 store/namespace、同样 8 个脏索引、最终 publish 前清零事件，隔离了最终段发布与前期构造。相同新内容的 1 PUT 加 7 次整段 GET 命中，以及已有内容的 0 PUT 加 8 次整段 GET 命中，确实反驳“脏段数等于新上传段数”。已有内容臂没有实际变化，报告没有把 0× 冒充有效业务变化的放大。计数只覆盖 MemoryObjectStore 的逻辑调用和 body，不含 native checkpoint、真实 RPC、Authority 内部 group、网络或 S3。事件是在同步调用返回后记录，没有进入/退出门闩；段间串行保证应继续只引用同步 publish 源码。

**复现入口存在具体限制。** 源码/patch/raw 足以人工复核，现存 lab 可供脚本重放；但 `02-run.sh` 依赖 identity 中已存在的临时绝对路径，不创建 c899/Turso 快照，不验证全树身份。它只重置 files.rs，`02-prepare.py` 发现 rw02_/mod experiment02 标记就保留现有模块，因此被修改过的 lab 测试可能被静默保留；离线依赖与工具链也需预先存在。这是现存 lab 的重放入口，尚非干净环境的一条命令完整重建。综合文档补充前置条件与该边界即可，不要求运行或改脚本。

### 结论合理性：PASS

报告保留的四项结论均有直接证据：冻结 File 冷段 GET 持 Shared；指定增量函数复制量随当前镜像变大而增大；新内容稀疏写的 32× 段 body/input 可被相同/已有内容显著削弱；c899 memo 已排除稳定文档每次 validation 重复 decode。没有把这些机制排为当前主要瓶颈，也没有从 debug 构建、超时窗或固定计数推出吞吐、p99、CPU 占比或百分比收益。

H6 原测试源码与 c899 字节一致，测试通过隔离子进程返回状态证明其断言通过：预热后 64 次验证 decode=0，新有效文档 64 次 decode=1，畸形文档拒绝。日志没有单独输出计数，不能把 PASS 当作新采样曲线。测试仍读取底层 head；不能由此推断没有 GET、commands 不增长、冷 decode 无规模成本或整个 ownership 已无成本。报告对此限制充分。

H3 最小补丁改善了合成静态场景的 File 推进，没有改变维护 operations gate，也没有测试 read/write/truncate、撤权、revision 重试、故障发布与完整 SQL 的正确性。H4 的 compact/空 sync/receipts 保留以及 H5 的 native latest-frame/writev/内部 group 均明确归为源码事实，没有伪装为本轮新实验。未见需要追加实验才能撤回的过度结论。

### 报告质量：PASS

结构、单位、计数清零位置、失败/拒绝对照、实测与源码证据、版本身份和排除项清楚；21 条数据可回到完整日志和测试源码。DONE_WITH_CONCERNS 与证据强度相符。建议将上文的“完成未检查读字节”和“脚本依赖保留的 lab”两点写进综合结论，避免读者扩大“成功”与“复现”的含义，不必改动原始日志。

## 冻结版本与后续 HEAD/WIP 边界

独立核对 02-environment 的七个源码 SHA-256，全部与完整归档及现存 lab 一致；业务 Cargo.lock 与归档 hash、c899 字节一致，实际日志编译 serde_json 1.0.151。io.rs/database.rs/recovery/tests.rs 恰为 c899 原文加归档测试，fixture 和 ownership 原测试未改；pread patch 与 `git show 1198b18 -- .../io/files.rs` 字节一致。

进一步只读比较 c899 跟踪的 165 个 V1 src/manifest/lock 文件与 lab，仅 database.rs、io.rs、io/files.rs、recovery/tests.rs 四处不同，符合挂载和补丁说明；新 experiment02 模块另有完整归档。此检查提高当前源码身份可核查性，不能替代运行当时完整构建清单。

本审查时共享 HEAD 为 1198b18611601f7417f7b9ae4bbb3c370b41ff03，并存在 recovery/Authority/io 等多处 WIP。原 e8beae4、冻结 c899、c899+局部 pread 补丁、1198b18 完整提交和当前 WIP 必须分别表述。H3 配对不能验证完整 1198b18；本轮 frozen recovery 数据不能用来证明后续 recovery/Authority 修改后的成本或正确性。

## 关键问题与改进建议

1. **覆盖限制，非数据失败：** 累计 receipts/index 增长、完整 session/SQL/checkpoint 和并发正确性未运行验证。综合结论保留“源码支持/尚未实测”标签，勿恢复生产排序。无需扩大本轮实验。
2. **读取断言限制：** 将“reader 成功”解释为本夹具 API 完成且无错误，明示没有校验内容和长度、WAL 为空。未来若该夹具用于数据正确性验收，再补内容/长度断言及真实有效输入。
3. **复现前置条件：** 将脚本称为保留 lab 的重放入口，列出固定临时快照、离线依赖、工具链和未被后续修改的测试前提。若未来需要跨机器复现，再增加快照创建与 hash 拒绝检查；当前无需重跑。

## 上一轮反馈的处理情况

第 01 轮关于依赖身份、运行源码归档与时间外推的反馈在本轮得到实质处理：本轮使用业务锁文件/features，归档完整最终测试和补丁，时间线说明编译前草稿变化，未把 debug 时长当性能结果。真实 RemoteIO File 入口、GET 进入事件、Shared 直接观测、独立 IO 和直接 storage 对照也已提供。第 01 轮自身 serde_json/harness/APFS 文案缺口不因此自动消失，综合结论仍应分别保留各轮限制。
