# 源码反证记录

本记录由主线程收集；实验与独立审查另见各轮报告。代码快照 c8990c4，原报告 e8beae4。

## H6：当前性攻击成立

`792f546` 已合入 root decode memo。快照的 `authority/codec.rs::decode_with_digest` 对当前读到的完整字节计算摘要，命中 `decode_cache` 时复用 `Arc<CatalogRoot>`；缓存上限 32 个 head / 16 MiB 编码字节。`ownership_view` 仍调用 decode，但不能由函数名推断每次都执行 parse。

所以原报告“本次基线没有 memo”对 e8beae4 的描述成立，对 c8990c4 已过期。当前只读租约校验的剩余成本包括实际 root GET、完整字节哈希、身份/迁移规则检查；更新或缓存失效时仍解码。写入 snapshot 为 attach registry store 还需获取自有 root。commands 的存储和 prepare 遍历没有因此分页。

现有 `a_head_is_decoded_once_per_document` 用例能够区分“调用 decode”与“实际 parse”；实验 02 可复跑确定性计数断言。它不能证明当前读取成本或端到端收益的占比。

## H7：从旧占比推断当前主因的攻击成立

59% 来自旧构建的一轮分段数据，分母为 14.357 ms，分子为 terminal 4.112 + audit 4.292 ms。当前源码仍有串行终态与审计等待，只能保留“关键路径存在两类持久化等待”这一结构结论。缓存、后端、主机队列和合批情况变化均可改变占比。此次没有当前公开 API 的同条件分段数据，因此不把 59% 用于当前性能排序或收益预测。

旧连接复用实验未给出跨轮稳定突破，不能仅由“协议调用多”推断 TCP/TLS 是主瓶颈。九臂观测的 offered/派发率与配额也限制容量解释。

## 其他要避免的外推

- 每批次 1 MiB 分配不等于每请求 1 MiB，也不等于该改动改善整个请求的延迟。实验 01 分开缓冲构造与真实文件 Sync。
- checkpoint 的 32 倍是“128 KiB 新互异段 / 每段改变 4 KiB”的 payload 模型；已存在相同内容、连续写满段、元数据和编码成本均会改变实际比值。
- WAL memcpy 是进入该验证路径的候选 append 的成本；并非凭源码就证明所有 SQL 请求或所有提交均恰好执行一次。compact、WAL 重用及 full-validation 回退改变路径。
- Shared 锁持有范围需经真实 File 入口验证，不能以直接 storage 的并行测试当成整个 SQL 路径已经并行；同样，File 层确定性阻塞也不自动证明该锁在热查询中占主要时间。
- 当前工作区的 operation buffer 和响应解析改动不进入冻结快照；若实验对照仅重建相同优化，其结果也不是当前工作区端到端 A/B。

## H3/H5：更高层互斥与已有去重限制结论

独立源码审查见 [source-challenge.md](source-challenge.md)。checkpoint_once / compact_wal_once 先取得 maintenance_operation 的 operations 写锁，普通 operation_until 取得同一把锁的读锁。这是独立于 Shared/storage 的前台等待原因。主库 File 层并行改善不能单独推出维护期间 SQL 并行；native pager 已加载页也可能完全不调用 File。

put_immutable 的 GET 比较可使新段 PUT 为 0 或 1；native checkpoint 已按页保留最新帧并合并连续 writev，Authority 每次事务已有 put_group。段间循环仍串行，但不能把全部脏段数当作新 PUT 数、把所有内部对象都当作独立 fsync。

## 实验期间版本推进

观察到 HEAD 从冻结 c8990c4 推进至 1198b18：94534c5 按批次实际长度分配；df93605 单解析；1198b18 主库 pread 移出 Shared 锁。这些由并行工作合入，本调查没有修改或完整验证这些实现。当前还有 Authority/recovery 等未提交改动，不能用它们覆盖冻结实验身份。版本记录见 [version-followup.json](version-followup.json)，后续若再推进以综合结论的最终观察为准。
