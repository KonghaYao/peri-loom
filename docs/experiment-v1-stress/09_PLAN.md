# R9：已验收配额修复后的共享主机功能压力计划

这是待执行计划，不是结果或隔离性能结论。顺序为 mixed → 独立 verify → architecture →
独立 verify → common SQL → 独立 verify；同一时间只有一个压力进程。

## 冻结前提

1. 平台配额修复已独立 normal/race 各 114 root PASS、两个 SKIP，九组 actual Native 均通过，
   fmt/vet/build 全部退出零；父提交 `77af2fc956b7337bd5568fcde7478dff7349a466`。
2. Native SHA256 `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`；
   新 Go SHA256 `e6c9dc3060646fe64b4c016f43de56f3b87dda2b88f186b06f11ef106e217af8`。
   新 soft-stop CLI SHA256 `f16dcd05b43afbcbe79835f5f702a37f18a0488379de40538944bd43c9a54918`。
3. 主控已用新唯一 RustFS bucket、新私有目录启动 S3/TLS 完整实例，正确 CA 的 whole smoke
   十一项实际通过。旧 R8 数据与外部进程不修改；本计划不抄录私有连接配置或凭据。
4. Guard 的有限 hard-stop join 与显式矩阵参数 allowlist 必须独立验收、提交后才放行。
   对外部 builder 使用显式 `record-confound` 和 `functional_only=true`：不终止用户进程，
   始终 `performance_pass=false`。己方 builder/并行负载仍 STOP。

## 小矩阵与预算

1. Mixed：`mixed_oltp`，concurrency 1,4，warmup 2s、smoke 10s、measure 30s、offered 50/s。
2. Architecture：`session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate`，
   concurrency 1,2，warmup 2s、smoke 10s、measure 10s、offered 50/s。
3. Common：`point_read,index_range,pagination,mixed_oltp,hot_keys,bulk_tx,sort_aggregate_join,index_plan,rollback,constraint_error_reuse,large_values,sustained_wal,checkpoint_reopen`，
   concurrency 1,2，warmup 1s、smoke 2s、measure 5s、offered 25/s。
4. 所有组 rows 1000、seed 20261007、request deadline 10s；RSS 512MiB 为采样阈值，
   generated 256MiB 为 charged 请求字节，artifact 64MiB 为 harness 范围，不冒充存储总量硬限。
5. 仅接受场景契约预声明的配置 SKIP，并逐项保留原因；没有性能 PASS 或所有场景全覆盖承诺。

## 首失败规则与验收

1. typed429 admission 独立统计；首 unexpected negative/unknown/oracle/resource/clock failure
   立即停止新 offers，不自动 SQL/HTTP retry，不重放事务、换 label 绕过失败或盲 Close。
2. 已 admission 请求保留原上下文/deadline 自然 drain；hard stop/identity refusal 超有限 grace
   记录 retained PID、may-have-unknown、HOLD，不假造 clean join、不杀错进程。
3. 每组保留原始输出、operations/ledger、guard 前中后样本、源码/二进制身份与实际 exit。
   独立 verifier 核对 oracle、ACK/错误/unknown 分母及 guard 完整性后，主控才授权下一组。
4. 压力全部停止后才构建最新 Linux 并运行 SDK/UI；最后联合大 verify 与用户路径验收，
   留完整健康项目运行。R7/R8 历史失败与性能无效记录不因新通过而删除。
