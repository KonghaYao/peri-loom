# ISSUE-002: 本机正式全栈压测入口缺联合门禁与独占夹具

- 状态：**HOLD**（2026-10-15；只读诊断已修复，外部联合门禁/二进制/夹具仍缺；不是负载中的错误）。
- 严重性：阻断正式 stress harness/`challenger` S01→S04→S03 的真实发压。
- 已核对：隔离回环 RustFS 容器与私有配置已存在，根工作树预检时干净；`trial-ground/private/main-ready.json` **不存在**，预构建的 `prototypes/v1/platform/peri-platform` **不存在**；仅有历史 debug Rust 二进制，非冻结产物。没有与当前 HEAD 对应的 Main 核准 P0–P6 联合 PASS 文件，也没有独立全栈的专用 `connection.json`/TLS CA、Gateway/两 Worker 控制身份及正式业务 SLO。此前本机 Rust/Go 包测试通过、单项真 S3 正确性用例 1/1，**均不等于联合 PASS**。
- 操作影响：`trialctl.py launch` 的 `main_ready` 与 `prototypes/v1/stress/gate.go` 的 `ReadGate` 分别 fail-closed；不可生成假 Main 声明、借用既有用户服务或绕过锁/门禁。不因用户允许在本机测试就擅自宣称联合门禁已通过。
- 可自动化修复：增加只读 `trialctl.py preflight`（或等价）清晰列出文件/二进制/HEAD/容器归属等缺项并提供稳定机器可读状态；不得创建 ready/gate/connection 文件、启动服务或读出凭据。实质解除 HOLD 仍需 Main 完成同版本联合验证、预构建与真实独占夹具，然后按预注册工作流逐项运行。
- 委派范围：`trial-ground/trialctl.py`、`trial-ground/tests/` 与 `trial-ground/README.md`；不得更改已保留的 `private/` 证据。
- 处理记录：subagent 加入只读 `preflight`；实际运行返回退出码 1 和 `HOLD`，自有回环容器检查为 true，Main ready 与平台二进制为 false；同时因本轮 issue/修复未提交，源 clean 为 false。Python 单测 18/18 PASS。诊断不会生成任何 Main 签署证据，HOLD 只能按原合同真实解除。
