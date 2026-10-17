# 实验 02：外层读锁、WAL 增量复制、段发布与 ownership memo

## 基本信息

- **状态：DONE_WITH_CONCERNS**。既定最小机制实验成功；完整 SQL、Authority session 提交、native checkpoint、网络后端和优化正确性矩阵未测。没有性能计时结论，不据此重排生产瓶颈。
- **日期**：2026-10-17。
- **基线**：独立 lab 冻结 `c8990c406588a23a203a46033cf10103681d188c`，Turso `24a3a4609683b73935888f8bc30b47727d100aa8`，位置见 [identity.json](identity.json)。H3 配对只叠加从 `1198b18` 提取的 pread 小补丁，没有合并该提交其余文件或当前 WIP。
- **环境**：Apple arm64 / Darwin 25.5，rustc/Cargo 1.98.1，`CARGO_BUILD_JOBS=2 GOMAXPROCS=2`，离线 `--locked --offline`，独立 `lab/target-02`，dev test 构建（unoptimized，debug=1），单测试线程、各 cargo 串行。真实使用冻结业务 Cargo.lock（serde_json 1.0.151，保留业务 features），没有新增或安装依赖。完整版本与源码 hash 见 [02-environment.json](evidence/02-environment.json)。
- **前序反馈**：已读正式 [01_verification.md](01_verification.md)。遵守“机制不等于主瓶颈”、依赖身份和运行源码版本边界；这里不使用 debug 时间比较优化速度。

## 方法与原始数据

### H3：实际 RemoteIO → File::pread 的受控 GET

[测试源码](evidence/02-io-tests.rs) 在 lab 的 `io` 内部测试模块运行，真实调用 `RemoteIO::open_file` 返回的 NativeFile/RemoteFile。直接 storage 预先发布两个不同的 128 KiB 合成二进制段；随后使用全新空缓存打开 RemoteIO。**输入不构成 SQLite 数据库，WAL 镜像为空**；实验验证 File 调度与锁，不验证 SQL、WAL 内容或 native pager 缓存行为。

ObjectStore 在第一个段的 GET **进入时**发送 bounded 通道信号，再等待另一个 bounded 通道释放。收到 GET 进入信号后，以 `io.shared.gate.try_lock()` 直接检查实际 Shared 可用性，并启动同 IO WAL pread、同 IO 另一主库段 pread、独立 RemoteIO 另一段 pread、直接 `io.storage().read_at`。独立 IO 使用同 backing、另一个空缓存目录，并以 `Arc::ptr_eq` 确认 Shared 不同。每个 reader 在调用前发 started 信号。

负向观察为 bounded `recv_timeout(1s)`，没有 sleep 判断进度；正向/GET 事件的保护上限为 10s。**1s 只是失败判定窗，不是延迟样本**。锁是否持有由 try_lock 确认，负向超时本身并不证明线程已到达 mutex 指令。每臂各一次；释放门闩后 join 全部线程，并确认旧版先前等待的两个 reader 最终成功。RAII finally release 和 GET 的 10s 保护避免门闩永久悬挂。

| 门闩停在主库段 GET 时 | c899 冻结版 | c899 + pread 小补丁 |
| --- | --- | --- |
| Shared try_lock 可得 | 否 | 是 |
| 同 IO WAL pread 在释放前完成 | 否，1s Timeout | 是 |
| 同 IO 另一段主库 pread 在释放前完成 | 否，1s Timeout | 是 |
| 独立 RemoteIO、独立缓存完成 | 是 | 是 |
| 同 IO 的直接 storage.read_at 完成 | 是 | 是 |
| 释放后全部 reader 成功、join 完成 | 是 | 是 |

旧版的 Shared guard 没有被编译器提前释放；低层 storage 的锁外获取不会自动解除 File 外层锁。该补丁消除了本夹具的 File 阻塞域。它**没有证明任意 SQL 可并发完成，也没有证明补丁在读/写/truncate/revision 重试、撤权、故障发布交错中的完整正确性**。新的预读重试错误表面仍需单独验收。

另一个内部测试真实调用 `RemoteDatabase::maintenance_operation`，持其返回的 operations 写 guard 时，`operations.try_read()` 为 WouldBlock，但同数据库的真实 WAL File pread 可完成；释放后 operations 读锁恢复可得。这确认 operations gate 是独立充分的前台排队原因。它只是确定性持锁夹具，**不是实际 checkpoint→前台 SQL 交错**，不能说缩短 Shared 已允许 checkpoint 时 SQL 并发。

原始旧版、补丁版数据见 [02-frozen-raw.log](evidence/02-frozen-raw.log)、[02-patched-raw.log](evidence/02-patched-raw.log)。补丁原文见 [02-main-pread.patch](evidence/02-main-pread.patch)，5 added / 2 removed lines（共 7 行变更，含注释）。

### H4：真实增量校验函数，固定新增一帧

[新增 recovery 测试](evidence/02-recovery-test.rs) 复用冻结 `recovery/tests.rs` 的 WalStore + WalBuilder：LocalObjectStore/RemoteWal 先产生实际 index/chunk，再 `validate_index_state` 得到 previous，新增一个 record 后调用真实 `validate_index_append(&previous, ...)`。这就是冻结 Authority 增量分支调用的校验函数，直接记录 thread-local `RecoveryObservations`，不是只测 Worker `image.apply`。

fixture 为 512 B 页、536 B WAL frame，分别预建 64/1024/8192 帧，固定只新增一帧。每次只在增量校验前重置计数，采集后再以全量重建逐字节对比 image/commit，并检查 previous 未被改变。

| 前镜像帧数 | 前镜像字节 | copied_bytes | 新扫描帧数 | scanned_bytes | 新 index JSON B | entries / receipts / receipt_digests |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 64 | 34,336 | 34,336 | 1 | 568 | 584 | 2 / 2 / 2 |
| 1,024 | 548,896 | 548,896 | 1 | 568 | 586 | 2 / 2 / 2 |
| 8,192 | 4,390,944 | 4,390,944 | 1 | 568 | 586 | 2 / 2 / 2 |

扫描的 568 B 包含 32 B header + 536 B 新 frame。**复制依赖当前镜像大小，新增扫描仍固定**；浅 clone 反例被实际计数及冻结 Vec 实现排除。copied_bytes 记录的是明确的 image clone 字节，**不等于进程总分配/峰值内存，也不包括所有 Vec regrowth、JSON、record 编解码或对象存储复制**。

同样对 previous 再传完全相同的旧 index，三个规模均被“必须新增一条 entry/receipt”校验拒绝，copied/scanned 都为 0。它是非法增量调用的早期拒绝对照，**不是合法空 sync 的优化收益，也不是“每次提交不复制”的运行反例**。既有 `a_rejected_append_leaves_the_validated_prefix_reusable` 与 `an_overwriting_append_forces_the_next_one_to_re_derive_the_prefix` 另行复跑均 PASS：拒绝候选不污染 previous，覆盖前缀后后续扫描回到全量。后者说明扫描并非所有 append 都固定一帧。

**范围限制**：这里由裸 LocalObjectStore RemoteWal 构造 fixture，它自身的 append 没有 Authority clone；复制数据只归属于随后**显式调用的真实 Authority 校验函数**。没有测 Worker→Authority session、BranchStore 增量分支命中率、冷会话、CAS retry、完整 append 时间、compact 周期。三个规模 entries/receipts 始终各 2，不能据此声称已验证累计 index/receipts 增长曲线。compact 可清空镜像、回执保留及无 pending sync 不 append 等属于 [源码审查](source-challenge.md)，本轮未做新运行实验。

### H5：真实 RemoteStorage::publish，8 段的去重反例

计数 ObjectStore 包裹 MemoryObjectStore，真实调用 `RemoteStorage::write_at/publish`。四臂各用独立 store/namespace：每段一个不同 4 KiB 新 payload、8 个相同 4 KiB 新 payload、先发布后再次写入相同已有内容、8 个连续完整段（每段不同）。后者用 8 次段对齐满段 write 构造，不代表 native pwritev 的调用分布。

**计数在完成构造/写入、紧邻最终 publish 前清零**，因此只算最终 publish，不含前期 seed、storage open、脏段构造 GET。记录所有 ObjectStore GET/PUT 的 key、命中与返回/body 字节；计数口径是逻辑 ObjectStore 调用，不是 Worker RPC 或 S3/Authority 内部对象请求。数据不含 HTTP/header/base64/wire 开销。

| 臂（脏索引均 8） | 写入 payload B | distinct 段内容 | segment GET 数 / 返回 B | segment PUT 数 / body B | PUT body / 输入 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 稀疏、不同、新内容 | 32,768 | 8 | 8 / 0（均 miss） | 8 / 1,048,576 | 32× |
| 稀疏、相同、新内容 | 32,768 | 1 | 8 / 917,504（7 hits） | 1 / 131,072 | 4× |
| 再写相同已有内容 | 32,768 | 8 | 8 / 1,048,576 | 0 / 0 | 0× 输入；无实际内容变化 |
| 连续 8 满段 | 1,048,576 | 8 | 8 / 0（均 miss） | 8 / 1,048,576 | 1× |

| 最终 publish 的其他对象 | 前三臂 | 连续满段臂 |
| --- | ---: | ---: |
| manifest GET 数 / 返回 B | 1 / 0 | 1 / 0 |
| manifest PUT 数 / body B | 1 / 808 | 1 / 809 |
| HEAD GET 数 | 0 | 0 |
| HEAD PUT 数 / body B | 1 / 79 | 1 / 79 |

**最终对抗复核补记（未重跑）**：由上表原始计数相加，三种稀疏臂的 segment GET 返回字节 + PUT body 均为 1,048,576 B，对本轮 32,768 B 写入调用输入均为 32×；连续满段臂为 1×。去重改变上/下行方向和 PUT 数，本夹具中并未降低最终 publish 的双向段 body 总量。这里不含构造期、元数据或 wire 编码，也不是实际业务变化字节的写放大。

32× 只在指定“每段一个 4 KiB 不同且未存在内容”的 PUT body/input 定义下成立。相同新内容与已有内容明确推翻“8 个脏段必定上传 8 段”的推算；去重仍支付逐段 GET 与整段下载。已有臂没有真正变化的业务字节，不能把 0× 叫作真实业务写放大。

事件顺序与同步 publish 循环一致；**本轮没有新增段请求门闩或网络计时，所以段间串行的通用保证仍归属源码证据**。没有运行 native SQL checkpoint、测实际 distinct page 分布、Authority put_group 或 S3 成本，不能将此表当作真实 SQL 负载去重率、checkpoint 字节分布或部署 RTT 倍数。

### H6：既有 ownership decode memo 断言

冻结 `a_head_is_decoded_once_per_document` 原样复跑两次均 PASS（首次原冻结构建，随后补丁构建再跑；pread 补丁不涉及 Authority）。该测试自行隔离子进程，确定性断言：预热同一 root 后 **64 次 lease validation 为 0 次 decode**；替换成一个有效新文档后 **64 次共 1 次 decode**；畸形新文档仍拒绝。

因此原报告基线的“每次 lease validation 都重新 parse ownership root”**不能作为 c899 的当前事实保留**。测试仍每次读取底层 head；没有测 commands 数量、单次解析随文档大小的增长、缓存容量、全局 CPU 或端到端占比。结果见 [初始构建日志](evidence/02-build-initial.log) 与 [既有对照日志](evidence/02-existing-controls-raw.log)；断言原文归档于 [02-final-src-authority-store_scaling_tests.rs](evidence/02-final-src-authority-store_scaling_tests.rs)。

## 证据、复现与身份

- [02-results.json](evidence/02-results.json)：从旧/新原始日志提取的 21 条机器可读观察，含 H5 全量逐对象事件。
- [02-run.sh](evidence/02-run.sh)：从仓库根执行 `sh docs/experiment-read-write-adversarial/evidence/02-run.sh`；默认新建临时日志目录，避免覆盖本次证据。会重置独立 lab 的 pread baseline、挂载测试、跑旧/新与既有对照，只使用现成离线依赖。
- [02-prepare.py](evidence/02-prepare.py)、[02-lab-tests.patch](evidence/02-lab-tests.patch)：测试挂载与完整测试源码补丁；单独的 02-main-pread.patch 是唯一业务候选补丁。
- [02-timeline.md](evidence/02-timeline.md)：实际命令、退出状态和未编译草稿→冻结测试→最小补丁配对的时间线。首次实际编译后测试源码未改；全部测试成功，无挑选或丢弃失败样本。
- [02-archive.py](evidence/02-archive.py) 与 [02-environment.json](evidence/02-environment.json)：完整最终模块/冻结业务文件/fixture、工具与 hash。后续只改报告和复现脚本，不重写 raw 行号。共享业务 WIP 没有进入实验二进制。

## 结论与后续范围

| 判断 | 本轮实测支持 | 源码支持但未实测 | 尚不能宣称 |
| --- | --- | --- | --- |
| H3 | 冻结真实 File 主库 GET 持 Shared；最小 pread 补丁使四个对照推进；operations gate 独立排他 | 完整 SQL 的各调用点、native 热页绕 File、各种并发状态关系 | 所有 SQL 都受慢 GET 阻塞；补丁解决 checkpoint 前台停顿；补丁完整并发正确性或当前生产速度收益 |
| H4 | 真实 validate_index_append 克隆随活跃镜像增长，新扫描固定；拒绝/overwrite 夹具 PASS | BranchStore 分支选择、cold/retry/compact/空 sync 行为 | 每次 SQL/提交均全量复制；终身历史单调增长；本轮已测累计 receipts 或 Authority 全提交 |
| H5 | 指定稀疏新内容 32×；相同/已有段只 1/0 PUT，但仍 GET 下载 | 同步 publish 段调用、native latest frame/writev、Authority put_group | 所有 checkpoint 固定 32×；脏段数等于新 PUT 数；真实 S3 请求成本或当前主要瓶颈 |
| H6 | memo 已使稳定文档重复 validation 无重复 decode | 单次 decode 与文档规模关系 | 每次 validation 重复 parse；本轮测过 commands 增长或 CPU 占比 |

本轮成功找到约束宽泛判断的反例，机制事实与全服务收益仍需分开。c899+pread 补丁只复现 `1198b18` 相关局部效果；共享 HEAD 后续对 recovery/Authority 的修改没有在这里测试。若要声称公开 API 收益或安排优化优先级，后续需要冻结版本的真实请求工作量与分段等待/CPU 证据；本轮无需扩大计时压测。
