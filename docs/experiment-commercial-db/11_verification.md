# 第 11 轮独立核验（2026-10-15）

## 结论

**HOLD / NOT MEASURED；不能写作“正式压测已开始”。** 本轮留存的是准入快照、两次 `trialctl preflight` 诊断及离线代码/单测，不是 S01→S04→S03 或 S06 的运行证据。没有提供本轮 Gateway 请求记录、ACK/s、p95/p99、错误率或容量判定；也不能仅凭这些快照独立证明历史上绝无其他调用。当前证据只支持“本轮未提供已发压证据、准入未放行”，不支持性能、安全性或商用 PASS。

## 证据交叉核对

- `11_plan.md` 要求冻结源码、Main P0–P6 联合 PASS、独占 TLS 全栈夹具和停止/资源边界；`11_experiment_report.md` 将结果标为 HOLD、正式场景 SKIP，与留存状态一致。
- 仅按预期布尔字段读取 `trial-ground/private/formal-admission-20261015.json`：`decision=HOLD`、`source_clean_at_snapshot=false`、`main_ready_present=false`、`prebuilt_platform_present=false`、`formal_joint_gate_located_in_fixture=false`、`formal_connection_located_in_fixture=false`，仅 `owned_loopback_s3=true`。这些是取样时/约定位置的检查，不是全盘不存在证明，也不是一次压力结果。
- `trial-ground/private/formal-preflight-20261015.json` 与 `formal-preflight-postfix-20261015.json` 的布尔检查一致：`fixture_valid`、`owned_loopback_object_store`、`rust_binary` 为 true；`main_ready`、`source_clean_head`、`source_matches_main`、`platform_binary`、两项 binary-matches-main 为 false；两份均 `status=HOLD`。快照没有记录命令退出码；报告称退出码 1 与代码行为一致，但不能仅凭 JSON 独立验证当时进程退出码。
- 核对 `prototypes/v1/stress/runner.go`/`safety_test.go` 的 Git diff：新增 `source.Dirty` 在 `ReadGate`、锁与运行目录前拒绝；回归覆盖 clean→缺联合门禁、tracked 修改、untracked 文件三种路径。`gate.go` 之前只按 HEAD 校验联合门禁，原静态缺口和当前局部修复均成立；没有模拟已持有有效联合门禁的真实请求，不应称已做正式发压复现。
- `trial-ground/` 是独立 Git 仓库；以其 HEAD `26006f5` 为基线执行 `git -C trial-ground diff -- trialctl.py tests/test_trialctl.py`，**可独立确认 ISSUE-002/003 的代码前后差异**：原 `launch` 只核对 Running/project，现于创建 `runs` 前调用新增 `preflight_store`，核对 project/service、镜像引用、回环端口、卷、配额及凭据；同时新增只读 `preflight` 与相应 mock 回归。`tests/test_trialctl.py` 覆盖六类错误归属及成功路径。此 diff 尚未提交，测试证明的是离线代码行为，不能证明真实 Docker 身份在启动全过程不变，也不解除正式准入 HOLD。

## 离线复核与界限

在临时测试夹具中执行 `go test -run '^TestSerialRunnerRejectsDirtySourceBeforeGateAndOutput$' -count=1 ./...`：PASS；`go test -race -p 2 ./...`：PASS（本次 Go 输出为 cached）；`python3 -m unittest discover -s tests -q`：18/18 PASS，其中 `test_trialctl.py` 单独为 11/11。未执行 `trialctl launch`/`preflight` 实时探测、Docker 操作、服务调用、SQL 或 stress runner；没有读取或打印私有凭据。

**状态完整性限制：** `trialctl.preflight()` 无条件输出 `HOLD` 并返回 1，即使其全部检查为 true，也不能把检查布尔值解释为正式准入 PASS。它只诊断本地 Main 声明/二进制/HEAD 和对象存储，不验证 P0–P6 联合门禁、TLS `connection.json`、Gateway/Worker 独占归属、私有 oracle、资源/停止阈值或预注册业务 SLO。`ISSUE-001` 与 `ISSUE-003` 的“已修复”仅指当前代码及离线回归；`ISSUE-002` 仍 HOLD。源树本轮未冻结，`CollectSource` 的单次 Git 状态检查也不是构建产物/运行期间身份的连续证明。原报告对缺项与 SKIP 的措辞基本克制；凡称“正式压力已开始”“联合 PASS 已具备”“全部准入完成”或“性能达标”均为过度主张。

**后续放行条件：** 保留当前 HOLD；待可审计的同版本冻结源码和 Main 联合 PASS、预构建摘要、独占 TLS 全栈连接与 oracle、资源/停止预算齐备后，再独立逐阶段复核。不得用对象存储预检或离线单测代替正式场景证据。
