# ISSUE-001: 正式压测器允许脏工作树进入发压路径

- 状态：代码已修复并通过离线 race 测试；**待冻结/提交与正式联合复验**（2026-10-15，尚未运行正式负载）。
- 严重性：P0／性能证据版本溯源与安全门禁。
- 位置：`prototypes/v1/stress/gate.go` 的 `CollectSource` 填写 `Source.Dirty`；`prototypes/v1/stress/runner.go` 的 `SerialRunner.Run` 直接进入 `ReadGate`、锁、fixture/SQL，未要求 `Dirty == false`。`JointGate.Validate` 仅校验 HEAD，不校验 dirty 内容。
- 可观察问题：在 HEAD 相同而工作树被修改时，合法的 `joint_pass.json` 仍可以与该 HEAD 匹配；报告事后虽然记录脏状态，却不能把实际参与构建/执行的源码视作经联合验证的冻结版本。**未实际发压复现**，以上是静态路径核查，不是已发生的数据丢失。
- 预期修复：在发压路径创建锁、产生日志或触及网络前拒绝脏源；错误不得暴露凭据。新增离线测试证明脏源被拒绝、无锁/运行目录/请求，干净源行为不变。保留 `Source` 的观测字段；不得合成 `joint_pass`、降低 Main 门槛或改动既有私有证据。
- 委派范围：`prototypes/v1/stress/runner.go`、该 Go 模块的相关测试；勿调用真实 Gateway/压测。
- 处理记录：subagent 在 `ReadGate`/锁之前拒绝 `source.Dirty`；离线测试核对干净源码的正常门禁、已跟踪改动和未跟踪新增均无运行目录/锁且明确拒绝。主流程独立执行 `go test -race -p 2 ./...` PASS；工作树修改仍未冻结，不代表正式准入完成。
