# 实验 01：日志缓冲与 RPC 双解析的对抗验证

## 基本信息

- **状态：DONE**。完成要求的机制微测和契约对照；没有进行实际 HTTP 或公开 API 端到端 A/B，不能声称它们的吞吐、p99 或百分比收益。
- **日期**：2026-10-17。
- **冻结版本**：`c8990c406588a23a203a46033cf10103681d188c`；路径见 [identity.json](identity.json)。原报告基线为 `e8beae4`，没有混入共享工作区已有的 operation buffer / parser 修改。
- **环境**：Apple M5 Pro，Darwin 25.5 arm64，Go 1.23.4，Rust/Cargo 1.98.1。Go `GOMAXPROCS=2`；Rust `CARGO_BUILD_JOBS=2`。只使用现成离线依赖，均正常优化构建，串行运行。临时文件所在文件系统类型未获有效证据，不能称已验证为 APFS。详细环境与冻结源码 SHA-256 见 [01-environment.json](evidence/01-environment.json)。
- **待挑战判断**：H1 固定 1 MiB 缓冲的分配可能被编译器消除，或合批/持久化使其时间收益很小；H2 双解析对小响应不重要，且单解析可能放宽契约。

## 方法与适用边界

### H1：实际文件路径与独立 buffer 模型

[Go 源码](evidence/01-go-bench_test.go) 新增在独立 lab 的 `platform` 测试包中，冻结业务文件未改。`frozen` 臂直接调用原 `operationJournal.writeBatchAt`；`exact` 臂复制该方法，仅增加批次长度求和并改变 `make` 容量。两臂均执行真实 `verifyPathIdentity`、payload `WriteAt`、`file.Sync`、`writeWitnessFor`。每次调用各含一次 Sync；witness 在该 Sync 之后写入，保留原顺序，未增加第二次 Sync。

两种模式：

1. `actual_sync`：通过真实 `openOperationJournal` 在本地临时目录创建文件，循环覆盖同一批次保留区；包含身份检查、持久化调用和 witness 处理，排除首次创建/锁定和最后校验。验证完整文件逐字节等于“最后一个 commit 的 witness + 按序连接的 batch”。文件长度、记录顺序和 witness 均检查。该模式直接测方法，不包含 submit、flush 队列、JSON 编码、state 应用、压缩或整个请求；重复覆盖不代表持续增长的追加负载，也没有模拟掉电。
2. `buffer_model`：仅按对应容量分配并连接相同 line；全局 sink 强制结果存活并验证逐字节顺序。包含 GC/分配与复制，排除身份检查、文件写、Sync 和 witness。它是隔离模型，不能视作完整日志路径。

批次 1 为 172 B；32 条小记录合计 7,511 B（仅约上限的 0.72%，并非字节满载）。另测接近上限的 32 条、1,047,959 B（99.94%）。后者在 mutation 的 `remove` 数组加入合成大字符串，属于写方法的容量边界数据，不代表正常业务会生成如此长的 removal 标识。所有输入通过实际 journalMutation/journalEntry JSON 编码与 hash 构造。

每种场景各运行 6 个 Go 进程，奇数轮 frozen→exact，偶数轮 exact→frozen；`-benchmem -benchtime=250ms -count=1`。默认优化，无 race/debug/禁止内联参数。统计单位是每批调用，不是每条 operation；Go B/op 为累计分配字节/调用。小批与容量边界分两次顺序实验，不能互相作为严格同时配对的速度比较。小批日志引用源码第 78 行、最终归档源码对应第 82 行，是补充容量边界场景后增加了 4 行；原始小批测试文件/二进制及其 hash 没有单独归档。执行记录中的改动位于 import 和计时前的输入/场景选择，未改计时循环，但此说明不能补足原始构建身份的审计缺口。详见 [01-evidence-notes.md](evidence/01-evidence-notes.md)。

### H2：复制真实结构与校验逻辑的独立 Rust 微测

[冻结 RpcResponse](evidence/01-frozen-RpcResponse.rs) 从协议文件自动原样截取；[冻结客户端校验](evidence/01-frozen-client-validation.rs) 留存供逐项对照。[Rust harness](evidence/01-rust-main.rs) 仅依赖 serde/serde_json，以 release 离线编译。**依赖并非冻结生产锁文件的原样版本**：冻结 `prototypes/v1/Cargo.lock` 使用 serde_json **1.0.151**，harness 首次离线解析得到 **1.0.154**；两者 serde 都为 **1.0.229**；业务启用 serde_json `raw_value` feature，独立 harness 未启用。下面的耗时和反例结果仅是 1.0.154 上复制结构/逻辑的微测，不能冒充冻结生产 parser 的原样成本或证明 1.0.151 的所有行为相同。本轮不重跑不同版本，保留此限制。

- 双解析：`Value::from_slice` → 必须是 object 且含 result → `RpcResponse::from_slice`，然后保留版本/请求身份、ok/error/precondition_failed/result 一致性约束。
- 严格单解析：相同字段结构与 `deny_unknown_fields`，对 `result: Option<Value>` 使用 `deserialize_with` 强制字段存在；显式 null 仍为 None。其余校验共用同一函数。该实验实现独立于共享工作区未提交的 parser 修改；本轮不证明现有未提交修改的契约正确性。
- 实际校验结果映射成 `Success(Value)`、`Precondition`、`KnownRejection`、`Invalid`，去掉 anyhow 文本、观察器回调与真实错误对象。这保留成功/失败以及 precondition 与 known rejection 的类别，但不声称逐字错误消息等价；例如同时缺 result 又含其他畸形字段时错误优先顺序可能不同。
- 请求 ID 固定为 `request-01`，版本 1；不包含 HTTP、连接池、body 读取/限长、请求构造、随机 ID、provider 后续类型转换及 base64 解码。大响应为单个无转义 ASCII data 字符串，接近编码后 base64 的字符串解析形态，但不是对象存储或 HTTP 实测。
- 112 B、1,024 B 各 50,000 次/臂/轮，1 MiB 各 100 次；各 6 轮交替先后顺序，并每臂先热身 50 次。`black_box` 保留输入输出，包含返回值释放。计时阶段关闭分配计数；另一个相同迭代循环开启全局 allocator 计数。自定义 allocator 即使关闭计数也有开关读取，绝对时间与生产 allocator 会有偏差。
- Rust `allocated_B/op` 统计 alloc 请求大小及 realloc 新大小的累计和，`allocs/op` 含 realloc，均非峰值存活内存或 RSS。它不能与 Go 内存语义直接等同，也没有在此测峰值。

## 原始证据与复现

所有结果均保留，包括不利结果。六轮每项的中位数、均值、样本标准差、Go 最小/最大值及分配指标在 [01-summary.json](evidence/01-summary.json)。

| 内容 | 文件 |
| --- | --- |
| Go 小记录批次原始 benchmem | [01-go-raw.log](evidence/01-go-raw.log) |
| Go 接近容量上限原始 benchmem | [01-go-full-raw.log](evidence/01-go-full-raw.log) |
| Rust 构建、契约与逐轮结果 | [01-rust-raw.log](evidence/01-rust-raw.log) |
| 精确 Rust harness 依赖版本 | [01-rust-Cargo.lock](evidence/01-rust-Cargo.lock) |
| 补丁版本、Go 归档时间线与文件系统证据限制 | [01-evidence-notes.md](evidence/01-evidence-notes.md) |
| Go 冻结方法 | [01-frozen-writeBatchAt.go](evidence/01-frozen-writeBatchAt.go) |
| Go 可复现运行脚本 | [01-run-go.sh](evidence/01-run-go.sh) |
| Rust 生成脚本、运行脚本 | [01-prepare-rust.py](evidence/01-prepare-rust.py)、[01-run-rust.sh](evidence/01-run-rust.sh) |
| 统计脚本 | [01-summarize.py](evidence/01-summarize.py) |

从仓库根目录执行（日志重定向应改用新文件，避免覆盖本次原始证据）：

```sh
sh docs/experiment-read-write-adversarial/evidence/01-run-go.sh
RW01_FULL_ONLY=1 sh docs/experiment-read-write-adversarial/evidence/01-run-go.sh
sh docs/experiment-read-write-adversarial/evidence/01-run-rust.sh
python3 docs/experiment-read-write-adversarial/evidence/01-summarize.py
```

Rust harness 依赖已冻结在 evidence 的 Cargo.lock，运行脚本会先复制该文件以固定离线解析选择；这固定的是 serde_json 1.0.154 微测，不是生产锁文件的 1.0.151。禁止并行执行两种构建/计时。

## 结果

以下均为六轮中位数。每臂 n=6；均值±样本标准差单列用于展示噪声，不是置信区间或 p99。

### H1 实际文件路径

| 批次与 payload | 臂 | ns/op 中位数 | ns/op 均值 ± SD | B/op 中位数 | allocs/op 中位数 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 / 172 B | frozen | 4,429,636 | 4,548,176 ± 1,085,066 | 1,050,068 | 14 |
| 1 / 172 B | exact | 4,924,443 | 4,895,886 ± 1,041,683 | 1,352 | 12 |
| 32 / 7,511 B | frozen | 4,968,322 | 5,031,708 ± 1,418,538 | 1,050,041 | 14 |
| 32 / 7,511 B | exact | 3,777,733 | 4,221,641 ± 1,123,855 | 9,368 | 12 |
| 32 / 1,047,959 B | frozen | 4,535,675 | 4,634,186 ± 631,237 | 1,049,885 | 12 |
| 32 / 1,047,959 B | exact | 4,491,809 | 4,758,805 ± 681,429 | 1,049,888 | 12.5 |

- 小批实际分配确认在优化构建中存在，编译器没有消除固定 1 MiB 缓冲。exact 明显减少每批分配字节。
- 单条批次 exact 的中位时间**反而更慢**（4.92 ms vs 4.43 ms）；32 条小记录六轮配对均是 exact 较快，这是局部有利信号；标准差约 1.1–1.4 ms、轮间范围重叠本身并不是无效应证明。因此本轮不能确认稳定的实际方法时间收益，更不能输出“降低日志延迟 X%”的结论。
- 接近满载时两臂 buffer 都进入 1 MiB 分配级，节省分配的理由消失；真实路径时间约 4.5 ms，无可据此确认的改善。exact 多一次长度遍历也有成本。
- 每个稀疏满数批只有一次大缓冲，若可合并 32 个调用，固定缓冲按每条摊销为 32 KiB，而不是每条 1 MiB；这只是给定合批率的机制算术，未测实际队列合批分布。

### H1 隔离 buffer 模型

| 批次与 payload | 臂 | ns/op 中位数 | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| 1 / 172 B | frozen | 94,579.5 | 1,048,576 | 1 |
| 1 / 172 B | exact | 35.875 | 176 | 1 |
| 32 / 7,511 B | frozen | 89,855 | 1,048,576 | 1 |
| 32 / 7,511 B | exact | 1,204.5 | 8,192 | 1 |
| 32 / 1,047,959 B | frozen | 59,787.5 | 1,048,576 | 1 |
| 32 / 1,047,959 B | exact | 57,931 | 1,048,576 | 1 |

隔离结果显示分配/清零/复制及其 GC 成本，但不能用其倍数放大完整路径收益。真实方法仍包含身份 stat、payload 写、Sync 和 witness JSON/hash/写入，整体在毫秒级。两模式不是对同一调用逐阶段打点，不能把时间差精确归给 Sync，也没有单独量化 witness 的占比。

### H2 解析模型

| 响应字节 | 臂 | ns/op 中位数 | ns/op 均值 ± SD | allocated_B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| 112 | dual | 605.669 | 613.033 ± 35.963 | 1,975 | 15 |
| 112 | single | 197.4045 | 196.328 ± 5.362 | 647 | 4 |
| 1,024 | dual | 800.9025 | 794.307 ± 22.319 | 3,799 | 15 |
| 1,024 | single | 288.133 | 286.990 ± 11.925 | 1,559 | 4 |
| 1,048,576 | dual | 194,952.5 | 195,222.223 ± 6,045.710 | 2,098,903 | 15 |
| 1,048,576 | single | 93,872.5 | 96,383.890 ± 6,489.932 | 1,049,111 | 4 |

在 serde_json 1.0.154 的该 harness 中，小响应绝对差仅约 **0.408 / 0.513 µs**；1 MiB 差约 **101.08 µs**。大响应减少约一份大字符串的累计分配，小响应也减少 object/key 构造分配。因此源码成本成立，但实际收益取决于响应大小与频率；未测 HTTP/Authority 工作，无法据此称其为主要瓶颈。

在 harness 的 serde_json 1.0.154 下，25 个契约对照全部通过（逐例见 raw log），覆盖：显式 result:null 成功、对象成功、缺 result、缺 error 的成功仍允许、缺必需字段、畸形 JSON、额外尾随 JSON、非 object、未知字段、版本/身份错误、null 身份、重复 result（两种 null 顺序）、重复 version/error、成功携带 error/precondition、错误携带非空 result、缺错误原因、known rejection 和 precondition rejection。

顶层重复字段被两臂拒绝，**result 内部重复对象键仍被两臂接受并保留相同解析结果**；实验没有偷偷收紧这一现有行为。测试是有针对性的反例集，不是对所有 JSON 的形式证明；原始错误文本与优先级未纳入等价保证。

## 结论

1. **H1 成本确认，主瓶颈推断受反证约束。** 固定分配在实际优化构建中存在，小记录按实际长度分配可减少分配量；但单条真实持久化路径未呈现改善，接近满载也无分配优势。这足以反驳“源码存在 1 MiB 分配，所以必然是当前主要延迟瓶颈或有相同比例请求收益”的外推。
2. **H2 在不同补丁版模型中确认成本，小响应优先级需要按实测体量判断。** 严格单解析在 serde_json 1.0.154 的本模型中可保留所列契约；生产冻结依赖为 1.0.151，未测其原样成本，不能把数字直接归给生产 parser。模型的 1 MiB 有约 0.10 ms 解析节省；约百字节/1 KiB 仅约半微秒，不能仅凭双解析代码把它排为当前端到端性能主因。
3. 原报告已写明“端到端收益未测”，本实验没有推翻这一谨慎表述。可以将两项列为减少分配/CPU 的局部候选，**不能从本轮验证其相对其他瓶颈的排序**。真实请求吞吐、队列等待、GC 峰值、p99、网络等待及真实响应体量分布仍未测。

## 异常与下一步

离线构建均成功。共享主机上的 Go 持久化轮间噪声较大，已保留不利数据，未为得到肯定结论扩大压测。若准备宣称用户可见收益，应另做冻结构建下的实际请求 A/B，并记录 batch 字节分布、Sync/队列等待、RPC 响应体量及累计分配；本轮不需要追加全栈实验来完成机制验证。
