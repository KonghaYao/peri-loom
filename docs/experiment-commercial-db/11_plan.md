# 第 11 轮预注册：本机正式压测准入与按 issue 修复（2026-10-15）

研究问题：当前机器是否具备运行**正式** S01→S04→S03 正确性前置阶段，再运行有界 S06 容量阶段的安全条件？用户已经授权在本机开展压测，机器是否专用不再是本轮的硬前提；负载的冻结源码、P0–P6 Main 联合门禁、独占 TLS 全栈夹具、私有独立 oracle 和资源/停止条件仍是硬前提。

步骤：先只读核对源码/夹具/门禁；若缺项，标 HOLD、建立 issue、派 subagent 修复可修代码，**不伪造联合 PASS，不启动 `trialctl launch`/stress workload，不触及已有进程/业务库**。缺项修复后用离线单测独立验证；重新审核门禁；只有在完整同版本证据齐备时，先独占 fixture 串行做 S01，确认新会话 ledger 后 S04/S03，再经预注册 offered/资源阶梯做 S06。任一已 ACK 丢失、越权、Unknown、锁/所有权变更或预算超限停止并保留第一次现场。无预先业务 SLO 时可以报告实测，但不能宣称性能达标。执行和复核不得并行发压。

本轮已知阻断项与修复分别见 `issues/ISSUE-001-stress-dirty-source.md`、`issues/ISSUE-002-formal-entry-prerequisites.md`。本轮输出 `11_experiment_report.md` 和独立只读验证 `11_verification.md`；若门禁仍缺，则如实标注 `HOLD / NOT MEASURED`，不把准入预检写成压力结果。
