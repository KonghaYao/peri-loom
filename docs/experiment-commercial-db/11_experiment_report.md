# 第 11 轮：正式压测准入预检（2026-10-15）

## 状态与范围

**HOLD，尚未执行正式发压；ACK/s、Gateway p95/p99 与错误率 NOT MEASURED。** 用户授权在当前机器而不是先等专用机器进行压测，但 `stress` 的 Main P0–P6 联合门禁和独占全栈夹具不能因此跳过。预注册方法见 `11_plan.md`，本轮只做实时只读准入检查和离线代码审查；没有调用 `trialctl launch`、`stress` runner、删除桶、启动新数据库或对现有进程发信号。

## 检查事实

| 条件 | 本轮观察 | 判定 |
| --- | --- | --- |
| 当前机器 | Python 3.12.8、Go 1.23.4；自有回环 RustFS 的 Compose project/image/卷/端口/配额和凭据匹配 | 仅对象存储预检通过，非独占全栈 |
| 冻结源码 | 准入时 HEAD `0efe692e94b0fb3c1c2d04e51df16ca0a5c3b7b7`；创建本轮 issue/报告后工作树出现本轮未提交文件 | 不能以 HEAD alone 认证参与构建的源码 |
| `trialctl launch` | 没有 `trial-ground/private/main-ready.json`，没有目标 Go 平台预构建二进制；存在旧 debug Rust 二进制但无本版本构建摘要 | HOLD；不得制造 Main 声明或借用进程 |
| 正式 stress harness | 未提供与 HEAD 对应的 Main 核准 P0–P6 联合 PASS 路径、独占 TLS 夹具的 `connection.json` | HOLD；不会拿前几轮单项/离线 PASS 冒充联合门禁 |
| 商业判定 | 业务 ACK/s 与 p95/p99 SLO 尚未事前给定 | 即使以后有小样本，也不得给商用达标结论 |

首个只读快照保存在 0600 `trial-ground/private/formal-admission-20261015.json`；其 `source_clean_at_snapshot` 是**创建本轮 issue/报告之后**的取样值，不是此前源码干净的证明，也不是运行中每个请求的身份序列。没有在私有夹具约定位置找到联合门禁/连接文件，不能排除其他未知位置存在文件，但未被提供给本轮 runner，故不作为放行依据。

补充只读 `trialctl.py preflight` 实际返回退出码 **1／HOLD**，0600 原始 JSON 在 `trial-ground/private/formal-preflight-20261015.json`：夹具配置和自有回环对象存储为 `true`，Main ready 和平台二进制为 `false`；旧 Rust 可执行文件存在但因无 Main 摘要不能匹配；本轮 issue/测试代码正在修改，`source_clean_head=false`。预检只输出布尔值，不输出凭据；状态 HOLD 不是发压结果。

## 发现、issue 与修复约束

- `issues/ISSUE-001-stress-dirty-source.md`：只读代码检查发现 `CollectSource` 记录脏工作树后，`SerialRunner.Run` 未据此 fail-closed；这会让 HEAD 匹配的门禁不能证明实际源码已经冻结。派 subagent 添加发压前拒绝与离线回归；**静态发现，不是实际攻击/线上事故**。
- `issues/ISSUE-002-formal-entry-prerequisites.md`：Main 联合验证、预构建平台二进制、main-ready 和真实独占 Gateway/TLS 连接缺席；派另一 subagent 改进只读准入诊断。程序可修复诊断与守卫，但**不能凭代码补出外部联合 PASS 或客户业务 SLO**。
- `issues/ISSUE-003-launch-store-identity.md`：启动路径只检查项目/运行态，未复用更严格的 service/image/回环端口/卷/资源/凭据归属检查；派 subagent 使 `launch` 在创建运行目录前 fail-closed，并以纯 mock 单测验证。不声称发生了真实容器篡改。

修复和独立复核后若仍缺必备输入，本轮仍保持 HOLD，正式 S01/S04/S03/S06 均 SKIP；不能用第 9 轮 16/64 原生 SQL 样本或第 10 轮两行 S3 SQL 用例冒充本轮 Gateway 发压。

## 第一轮 issue 处理

ISSUE-001 的 subagent 在 `SerialRunner.Run` 获取 Git source 后、读取联合门禁和获取锁之前显式拒绝 `Source.Dirty`，并新增离线测试：清洁源码进入正常门禁检查、修改已跟踪文件时被拒绝且不产生运行目录。是否能启动正式发压仍取决于 ISSUE-002 中不可伪造的外部门禁和夹具；修复前后均未执行真实 workload。后续由不同审阅者复核 source 缺口、单测和本报告。

ISSUE-001 的复核又加入未跟踪文件的拒绝断言；主流程运行 Stress Go 全包 `go test -race -p 2 ./...` PASS。ISSUE-002 的 subagent 加入只读 `trialctl.py preflight`，并将 ISSUE-003 的弱 `launch` 容器检查改为严格的夹具归属校验；主流程在严格存储校验中补上 512 MiB/0.5 CPU/64 PID 配额，独立执行试炼场 Python 单测 **18/18 PASS**。修复后的预检仍为退出码 **1／HOLD**（原始 JSON：`trial-ground/private/formal-preflight-postfix-20261015.json`），没有任何 Gateway SQL 负载、自动签署门禁或性能数字。本轮改动未提交，不属于历史第 10 轮已提交版本。
