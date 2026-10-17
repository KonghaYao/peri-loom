# Boot 自有 root 的 admission 复用候选

日期：2026-10-17。此项为独立 candidate02，基于 candidate01 后继续修改工作区。只修改 `authority/migration.rs` 并新增 `authority/boot_admission_tests.rs`；不叠加 request scope、Walker 去重或其他优化。没有运行构建、测试或压测，交由主线程串行执行。

## 问题与最小改动

namespace 初始化的 `Authority::from_loaded` 会完整 deep verify 并 remember 原 root R；boot 再从已验证 snapshot 克隆并持久发布 R+1。原成功路径没有 remember R+1，初始化后首次 `Service::state` 的 snapshot 因 digest 不匹配，再做一次完整 deep verify。

修改只把 boot 的 `codec::encode` 换为 `encode_with_digest`，沿用普通 `transact_inner` 的自有 publication admission 模式。在 `put_group` 返回成功并取得最后一个 root etag 后，使用同一次编码的 digest 记忆 candidate，再返回原 RootSnapshot。正常读取仍 GET root、校验 envelope、检查 digest，并执行既有 head/reference 校验。

## 为何可以继承验证

每一轮 boot 的 `previous = self.snapshot()` 只会返回已完整验证过的 head，或从它自身构造并验证过的 head。未知 digest 仍先走 deep verify，CAS retry 也会重新 snapshot 并完成相同检查。

candidate 是该 previous root 的 owned clone。boot 只改 ownership catalog（term、revocation、epoch floor 等）、version、revision、operation receipts，以及 registry 的持久表示；没有增删或替换 blobs 的语义条目、objects 映射、branch refs、history refs、snapshots、pins。registry.publish 把现有内容转换成 inline 或内容寻址 pages，seal 检查没有未发布变更；原有 `validate_head_with`、floor index 验证和 publication CAS 全保留。因此所有继承引用来自该轮已验证的 previous，新增 ownership/schema/receipt 信息也仍由既有校验覆盖。

admission digest 来自实际被加入这轮 durability group 的精确编码，不来自 etag，也不重新编码 candidate 推测。只有整个 group 已成功、root etag 可用后才登记。CAS 拒绝、后端异常、丢 ACK 即便实际 bytes 已落盘，均保持原 BootFailure 结果且不提前登记；Unknown 不转成成功，不增加重试。失败路径后续 snapshot 如遇外来或未记忆 head，继续完整验证。

本候选没有扩大 admitted 表的容量或跨进程作用域。表满或 mutex 失败仍允许复用失败并回退原深校验，正确性不依赖 cache 命中。

## 新回归

新增独立 module，不使用 process-wide 性能探针，避免其他单测污染计数。fixture 建立一份只有 recovery history 引用的合法 WAL index，epoch 为 7，current HEAD/WAL 为空；计数该 index 的真实 physical GET：cheap current-reference 校验不会读它，完整 deep verify 和 boot floor 才会读取。

- `first_state_after_boot_reuses_verified_references_and_retains_the_epoch_floor`：新 OwnershipAuthority 经真实 `state` 初始化，期望历史 index 仅 deep verify 一次、boot floor 一次，共两次；随后再 state 不增加历史 GET，epoch floor 仍为 7。原实现会多一次 deep verify 的 GET。
- `an_external_root_is_deep_verified_and_invalid_history_still_fails_closed`：成功 boot 后直接外部发布合法新 digest，state 必须增加一次历史 GET；随后将新 root 的 history 引用 hash 改成未注册身份，state 与新的 Authority::open 都必须拒绝，坏 digest 不得进入 admission。
- `a_rejected_boot_candidate_is_not_admitted_before_its_successful_retry`：第一次 root CAS 被拒绝，第二次写入前由 store 回调检查所有失败候选尚未进入 admitted；最终仅成功 candidate 可命中 admission。
- `an_unknown_boot_never_admits_its_candidate_even_when_the_bytes_landed`：分别在写入前和实际持久化后注入非 CAS 错误；原 BootFailure::Unsettled 保持，candidate 不进入 admission，原已验证 digest 继续保留，后端是否落盘与故障位置一致。

测试源码已格式化，相关文件 `git diff --check` 通过；尚未编译或执行。既有 namespace 初始化不确定性/lease/fencing 回归也应由主线程继续执行。

## 性能验证边界

静态路径能证明此项避免 boot 成功后紧接着的重复 deep verify，不能据此认定此前每次 reopen 的约 5 s 均由它造成。没有新的时间、吞吐或 1.5× 结论。主线程应先做 candidate02 独立 smoke，再按相同冻结负载配对，并保留两个 deep verify、boot floor 与 RemoteIO replay 的分段边界；其他候选需另轮实验。
