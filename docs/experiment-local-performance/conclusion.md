# 本地 SQL 固定工作量优化结论

日期：2026-10-17。最终候选 source ID `e4620e51fa8e987a644aaab867cf4fe19099d662d4e0de2c7fe56b066ea3e3c2`，基线固定 `1198b18611601f7417f7b9ae4bbb3c370b41ff03`，Turso固定版本。本文结果来自冻结二进制，不代表仍由其他工作流修改中的整个最新工作区。

## 研究问题与结果

在不改变耐久发布、SQL 行为、fencing、Unknown 或 GC 保护的前提下，固定本地真实 SQL 测试工作量能否至少快 1.5×？

**实验03通过预设性能门槛。** A→B / B→A 两对总倍数为 **2.370838406× / 2.421294477×**，配对倍数中位数 **2.396066442×**；所有九个固定阶段均未出现超过10%的配对中位回退。请求阶段合计约24秒降至10秒，完整内容oracle、请求数量和两次独立进程冷重开结果完全一致。基线强制重编补证、逐轮独立验证和最终独立对抗审查均完成。最终裁定为 CONCLUSION_STANDS，仅限本文明确的固定本地工作量。

| 实验 | 候选范围 | 两对整体倍数 | 配对中位数 | 裁定 |
| --- | --- | --- | --- | --- |
| 01 | 已有IO/维护/WAL状态复用，加存储COW、同次publish重复body检查复用、WAL单次编码、丢ACK视图GC pin修复 | 1.027 / 0.886 | 0.956× | 未达标，history阶段触发回退门槛 |
| 02 | 01加boot成功后记忆自有已验证root | 1.508 / 1.404 | 1.456× | 未达标；保留第二对不利结果 |
| 03 | 02加同一不可变root遍历内已验证WAL chunk前缀复用 | 2.371 / 2.421 | 2.396× | 性能PASS |

全部样本、包括最初harness授权错误和mtime导致的未计时stale候选，都保留而未混入有效结果。见[规划](00_plan.md)、[实验01](01_experiment_report.md)/[验证01](01_verification.md)、[实验02](02_experiment_report.md)/[验证02](02_verification.md)、[实验03](03_experiment_report.md)/[验证03](03_verification.md)、[最终对抗审查](adversarial_review.md)。

## 哪些猜想被推翻、哪些获得支持

1. **减少复制和编码足以获得整体1.5×，被本轮证据否定。** 实验01未达标，不将它的局部机制收益说成可见整体收益；不能从这两对噪声样本断定其中每一项单独有害。
2. **boot后重复deep verify确实存在且可避免。** 真实state回归的历史独有index GET计数确认避免重复深扫，失败/外来root仍重新验证。实验02组合候选的整体中位数为1.456×，三个重开阶段约1.96–2.00×。这些是阶段整体结果，没有为每个内部函数单独计时。
3. **同历史前缀的注册图重复解析可以在一次root遍历中复用。** 正向缓存仅在lookup、hash、registered和墓碑检查全部成功后插入，最多4096项，FIFO淘汰后重新检查；每个新Walker为空，不跨root或请求缓存。GC首次检查已收集reachable，不因淘汰而移除。实验03的共享前缀物理page GET回归、损坏引用/新root/浅深校验隔离及容量测试均通过。
4. **不能说所有读写都快2.4×。** 实验03热读、历史提交、checkpoint阶段配对中位倍数分别为0.980×、1.019×、0.989×；主要改善在cold WAL reopen、writer reopen、cold checkpoint reopen，分别为16.688×、17.869×、15.927×。

## 正确性与安全边界

- 实验01针对性release门禁77项通过；最初wal::tests零匹配已明确补运行receipt_tests两项，未虚计。
- 实验02本轮52项通过（4 boot、5 namespace过滤、8 ownership、35 p4_authority）。
- 实验03本轮63项通过（4 Walker、4 boot、14 recovery、35 p4_authority、6 branch），包括真实零窗口GC后读取和继续提交、CAS失败、丢ACK后选中后来状态、Unknown落盘前后、旧lease重启失效与恢复。不是全项目或SQL兼容全绿声明。
- 未减少sync、持久屏障、fencing或内容校验，没有SQL自动重试、持久schema改动或Turso变更。boot admission只发生在整组发布成功且取得root etag之后；未见新root仍deep验证。
- 独立代码审查找到并修复了丢ACK后采纳较晚HEAD/WAL但未持久pin其依赖的缺口；最终视图采用补pin交易闭包捕获的已保护点，不递归采纳另一更晚返回root。见[独立代码审查](candidate-code-review.md)。

## 置信度与局限

固定工作量的同机release结果置信度高于局部微基准，但只有两对最终样本，共享主机存在负载漂移，不能推导生产吞吐、p99或任意读写比例。基线中三个重开约占六成，此权重预先固定且全程未减少；持续热连接的业务不会自动得到相同倍数。对最终原始样本做权重敏感性计算：排除三个重开后其余六阶段合计倍数仅为1.043769× / 1.001178×，未证明常驻连接读写达到1.5×；该计算不替换预先固定的验收总量。冷态仅为新进程、新Authority和空应用cache，不是清除OS/设备cache。

总量是九个请求阶段计时之和，包含历史增长、checkpoint和真实重启授权，但排除夹具、预热、比对、进程启动和句柄销毁；不能称进程全生命周期wall。每次重启走正常boot并以新epoch重新授权，明确验证旧binding被拒绝。graceful close后的oracle不代替断电/立即崩溃耐久测试。此测试不包括HTTP/Gateway、审计排队、外部S3 RTT或并发公平性。

源码/二进制/harness hashes及构建日志全部归档，source标签只是一项记录，不能单独证明binary来源。候选03与候选02之间另有外部已提交cfg(test)维护测试变化；最终工作区还在被独立维护/platform工作流推进，它们不属于冻结结果的性能声明。

## 后续建议

保留本固定负载作为性能验收入口。若目标是持续连接的读写吞吐，再分别采Authority持久提交、操作/审计等待、root/index规模和后端往返；当前结果不支持把这些热点宣布已解决。无需为已通过的本地目标继续堆叠未经需要的优化。

## 基线来源补证

[强制重编记录](evidence/baseline-rebuild/verification.json)：从固定1198的git archive重新安装完整V1源码（archive SHA与初始冻结相同），加入同一修正harness，刷新所有源文件mtime后在同一工具链/release/locked/offline路径构建。产品lib与integration产物均fresh=false。新二进制SHA-256为`a8fdfb33849581d77d9a39e06bce056b054f618b04f2579d0cbf95b2eb2811f3`，与全部正式实验使用的A逐字节一致。这直接闭合先前cache复用造成的源码来源缺口；补证在所有正式计时结束后进行，不是新增或替换性能样本。Turso HEAD固定且工作树干净。

## 最终对抗审查处理

最终审查独立复算三轮12个正式样本，确认所有非计时工作量完全一致。采纳重开权重敏感性计算；补齐强制重编源码清单遗漏的rust-toolchain.toml，最终719项完整覆盖baseline archive718项加同harness，逐项摘要验证通过。没有通过删样本、改变工作量或门槛达标。最终独立裁定CONCLUSION_STANDS；本地固定用例至少1.5×目标已完成，不宣称生产热读写或全部SQL门禁通过。
