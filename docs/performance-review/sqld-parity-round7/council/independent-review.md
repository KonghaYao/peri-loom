# R7 独立代码与测量审查

日期：2026-10-29。基线：`3703ca696d55e333aff746667fd39125579c4b26`。

本审查不修改生产代码，不执行性能负载。范围是本轮 Go 成功结果交接、诊断 runner，以及恢复 R6 摘要复用的连续性检查。下述结论不是六项真实业务、H4/H6 尾延迟或 sqld 正式验收结论。

## Go 成功结果交接

静态审查和新增定向测试未发现阻塞问题。

- `ServeHTTP` 的控制流程没有移动。交接位于原 `writeResponse` 中，终态结算和持久化审计都在它之前；`result.finish`、`permit.finish` 仍由原 defer 在返回前完成。每次 dispatch 构造私有 recorder，没有全局捕获对象或跨请求共享状态。
- 只有 `code==""`、status 为 0/200、非 stream、动态类型为非 nil `json.RawMessage` 的结果可交接。外部 REST writer 不能触发私有具体类型断言；失败、202、其他状态和 map 结果继续走旧 envelope 与 `sqlcontract`。
- `json.Compact` 保留与 `Encoder.SetEscapeHTML(false)` 一致的 RawMessage 字节表示，包括数字拼写、转义、字符串内空白；它另持有一份自有缓冲区。`executeCursor` 的结果字节预算没有因为移除 envelope 而改变计数字节。
- 配额、原生结果结构验证、fencing、Unknown、close ACK、审计行和原生 RPC 次数均没有新增旁路。新测试实际通过 handler.call 覆盖审计阻塞、审计失败与终态写入失败，并对 cursor-first 形状/配额拒绝和单 RPC 作断言。
- 任意约 10000 层 RawMessage 与外部 envelope 的 JSON 深度上限可能相差一层，但生产 native 边界 `sqlDecode` 已限制 128 层并验证 UTF-8、重复字段。128 层等价测试通过；没有为不可达输入增加额外生产扫描。

独立运行：

```text
GOTOOLCHAIN=local /workspace/scratch/6757c94bd47c/toolchains/go/bin/go test -count=1 -run '^TestHranaGatewayResponse' .
ok peri-loom/platform 0.033s
```

定向测试包括成功/失败逐字节等价、HTML/U+2028/U+2029/非 UTF-8 RawMessage、nil/null/数组/标量/非法 JSON、202、stream、缓冲所有权、审计与终态屏障、cursor-first 配额。全包、race 和实际服务验证由主任务另行记录。

## 诊断 runner

发现并修正了一项测量偏差：原草稿在响应 clone 读取结束时立即 JSON.parse，可能在 SDK all()/close() 完成前占用事件循环。现已将解析和 proof 校验移至业务动作结束后的 `settle()`。业务时钟取 issued→最后必要 SDK 动作完成；另保存 evidence_settled 时刻，后者没有回填进业务时长。

仍存在公开声明的观察器开销：clone/tee、正文收集、请求摘要和时间戳会与 SDK 执行重叠。两侧使用相同采集器并不证明该干扰完全等量，因此本轮仍是诊断证据。

已复核：

- SDK 锁定 1.4.1，使用完整 all() 与实际 close ACK；同步 connect 的对象构造时间也包含在 issued 时钟后。
- 缺行、错值、行序错误通过完整类型化摘要拒绝；BigInt、数字、字符串、NULL 和 BLOB 不混为一类。
- 第二次 fetch 在进入网络前被阻断；SDK 吞掉 fallback 异常时，call 结束前仍检查 blocked_retries 并停止，避免继续 close 后误记成功。
- baton 链在结算阶段校验；SDK close 吞错不等于 close 已确认。失败样本留存，warmup 失败同样停止。
- 生成数据显式标记 synthetic-proxy，输出恒为 formal_pass=false/recovery_proven=false；没有把 issue 中五次均值或本轮代理 p99 混合。

独立执行 `npm test`：12 项通过，0 失败，0 skipped，耗时约 1.25 秒。测试覆盖真实固定 SDK 的 mock Hrana 交互、close 吞错、结果摘要、隐藏 fallback、时钟边界及 fixture 确定性；这不是数据库性能测试。

## R6 Rust 连续性移植

R6 摘要复用是已接受优化的恢复，不应计入 R7 Go 增量收益。A/B 必须使用同一最终 native 二进制。

静态对照显示该优化将祖先路径每一位重复 blake3 改为每个 key 一次；bucketed 路径仍分别计算 pin 摘要和完整 key 摘要。完整 prefix 每位仍验证，Hashed 64 位与 Bucketed 4+64 位边界明确；空 prefix、缺 slash、非法 nibble 均保持原语义。页大小、内容 digest、key 验证、层级校验、pending 优先顺序与缓存身份不变。新三项测试覆盖每种深度/错误祖先、叶形状/层级与 whole-tree 错路由。

独立检查曾发现第一份 overlay 工件不完整：主文件仍为 main 字节，而 patch 只新增测试文件。已阻止将其标记为有效 R6 移植，并通知环境与主任务；环境已确认问题。该尝试不应作为通过证据，必须以修复后的实际源文件、完整 patch 和二进制身份重新冻结。

修复后已再次独立核对实际源文件：

| 文件 | SHA-256 | 结果 |
| --- | --- | --- |
| `authority/paged_index.rs` | `fef2d54929a8e6df0638635b76a9af5e8dfae78f68c6b8fa21f3c61f42c9558a` | 与恢复的 R6 逐字节一致；实际 diff 为 91 加/12 删 |
| `authority/paged_index_validation_tests.rs` | `9caa9905ef87367d06ec706bd6febc68541c66f1a6048887f2e049e39d2b2ed3` | 与恢复的 R6 逐字节一致 |

`git diff --check` 通过，`routing_digest` 及两个祖先前缀检查替换均实际存在。仅这两个 Rust 文件进入移植范围；不覆盖 main 的其余新功能。

最终证据再次只读复核完成：

- 两文件 patch 实际 SHA-256 为 `4351475479b14fe757531535e396f785bb5267d7bcb260ad469b8bbb40a51efe`，文件清单确实包含生产主文件和新测试文件；isolated index apply 记录为 PASS。
- 三个 exact 测试日志各为 1 passed / 0 failed，名字与新增测试逐项对应；Authority 全集日志为 164 passed / 0 failed / 5 ignored，并包含上述三个测试。没有把空过滤结果算作通过。
- release 构建记录为 PASS、exit=0；Rust/Turso 输入前后摘要均为 `79c13a49086e3ae333e31bfbc0ba803d3c8df4103c23bf23fbe798b0b08e3dae`。
- 对磁盘 native 二进制独立重新求 SHA-256 得 `5875b5969f9236d424c625f9dc81d8284df763d492b974d7741392a0f15a78f1`，与最终 manifest 一致。A/B 仍须共用它。

证据位于 `performance-round7/environment/r6-overlay/`；初次无效 overlay 不作有效构建依据。

## 后续采集器完整性复核

测量 agent 又发现并修正空结果的证据漏洞：仅有 cursor 头和 baton 的截断响应可能被 SDK 接受，而空结果摘要仍相同。现 `settle()` 额外要求业务 step 0 的 `step_begin→step_end` 完整结束，并将 `primary_step_complete` 写入 raw；缺少结束事件不能成为成功样本。该校验仍在业务时钟之外，分析器也要求此字段为 true。

追加独立运行 collector `npm test`：13/13 通过，包含截断空结果的新反例，0 skipped。

外部 `analyze-comparison.py` 也已静态复核并独立运行其构造记录测试，13/13 通过。分析器先验证所有 warmup/timed 记录的 SHA、结果摘要、ACK、NoRetry、step 完整性、时钟顺序与样本数，再计算逐腿及 pooled nearest-rank 分位数；失败不删除后继续出成绩，而是拒绝统计。六腿顺序必须 A/S/B/B/S/A，所有客户端运行区间不能重叠；各臂跨场景的源码/二进制/鉴权/TLS 身份固定，A/B 的 native 摘要还必须相同。分位说明使用真实 N，不把单阶段 p99 相加，也不声称容量或正式置信区间。显式 setup 写入单独列出，不进入只读样本统计。

本审查未读取未完成的真实性能数据，也未据 mock 测试推断性能收益。
