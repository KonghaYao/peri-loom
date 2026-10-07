# v1 用户路径：独立文档核验

## 最终结论与责任范围

1. **用户路径文档 STATIC PASS。** 独立核验数据库篇、平台篇、SDK 篇与总入口的有序步骤、真实入口、预期结果和安全边界；更新后的平台篇与冻结 UI 契约一致，没有未解决的本次文档 blocker。
2. 运维篇由本报告作者编写，不自证独立通过。其结论来自 B 的独立报告 [USER-PATH-OPERATIONS-VERIFY.md](USER-PATH-OPERATIONS-VERIFY.md)，本轮只核对其冻结文件身份与跨篇衔接。
3. 本轮只读源码、文档及离线证据，执行语法解析、链接存在和文件散列检查；未执行正文命令、构建、fixture 网络/API/SQL、负载、信号或缓存删除。只新增本报告，没有编辑其他作者文档、生产代码、`CLAUDE.md` 或冻结的 P6/JOINT 报告，没有提交。
4. STATIC PASS 表示说明与当前入口一致，不表示这些用户操作已经实测通过。当前代码完整门禁另由 F 负责，首次失败及修复复验不能被本文替代；本轮不宣布测量、性能提升、最终压力对抗验证或生产 HA 通过。

## 冻结输入与来源

1. 观察到父仓库 HEAD 为 `9819a47f88fd3f51a551f1c01f328563cd5d14c3`。工作树包含其他作者未提交内容，因此以直接读取的文件 SHA-256 为核验身份，不把 HEAD 当作全部内容证明。
2. 独立审阅输入如下；平台篇最终值包含本轮作者修订，不复用旧文档散列冒充当前版本：

   | 文件（位于 `docs/v1/`） | 行数 | 最终 SHA-256 |
   | --- | ---: | --- |
   | `USER-PATH-DATABASE.md` | 298 | `4f974ad9cbb68d4698b668bcafcb9e7ffcb93d18c5b629d949f0158e35f8f3d8` |
   | `USER-PATH-PLATFORM.md` | 256 | `e2ee2383c2f806da0765b8caa17bcab7bd5e969c5f0fbab5ac6b7ad1fab5d544` |
   | `USER-PATH-SDK.md` | 287 | `4c30e6fffb694b355e10f94824003c7d42b3352cba540fb5e9d1ef76ca0ed697` |
   | `USER-PATH.md` | 38 | `87c3e446078704dc88386e44e9de31fc5ff912a67807424affc7627f141eafb1` |

3. 首次完成静态核验时，运维篇为 257 行，SHA-256 `d767bafd9adca9c96f35bcebe0d92102c6f1ee771d9c2ca74c3cae2b9bbb7c7d`；B 的独立报告为 83 行，SHA-256 `cd0b7f9a5292796e93e0e11efd75068e4df3001ce1bbd2c2023763d6a8ce796f`。当时均与冻结值一致；后续唯一发布限制增量的最终身份见下节。
4. 离线检查证据保存在 `/tmp/peri-user-path-static-verify/final/`。首次六份文档的 `docs-before.json` 与 `docs-after.json` 字节一致，均为 SHA-256 `2826fea57cc7ceef4d662b89c0b67032bd6fde8c7eab72f31dd744d3dcb391de`。该记录保留原检查范围，不将后续增量伪称为旧清单下未变。
5. 本轮选取的 39 个入口/契约源码锚点记录于 `source-before.json`、`source-after.json`；两份清单一致，均为 SHA-256 `07dda66b0ddf685bad0a0456e888cefa79690d9e65795a7cedece2fe08fd6e56`。这只是明确列出的源码范围，不是全仓库冻结或二进制一致性门禁。
6. C 的 `/tmp/peri-ui-rpc-freeze.json` 中 16 个文件逐一与当前字节一致。直接核对 `web/rpc.js`、`web/app.js` 的实际行为，而非只采信作者测试数。

## 本轮实际离线检查

1. 数据库篇有 39 个有序步骤、平台篇 64 个、SDK 篇 24 个、总入口 14 个。检查组内的前提、操作、预期与限制，以及先部署、后平台/数据库/SDK、再授权恢复/压力的顺序。
2. 抽取数据库篇 16 个、SDK 篇 9 个 shell block，以 `/bin/zsh -n` 解析，**25/25 PASS**。未执行 block，语法通过不是外部工具、服务或权限已经可用。
3. SDK 篇 1 个完整 Go 程序以 `gofmt` 标准输入解析，**1/1 PASS**；平台篇 1 个 JSON 示例以 `json.loads` 解析，**1/1 PASS**。没有 Go 类型检查/构建，没有执行该程序，也没有编译 TypeScript/Rust 片段；方法签名通过直接读源码核对。
4. SDK 篇和总入口的 7 个 Markdown 链接目标存在，**7/7 PASS**。`git diff --check -- docs/v1/USER-PATH*` 实际退出 0；不将链接存在等同于对应功能通过。
5. 最终检查记录为 `checks.json`，SHA-256 `455876d8860a0328f9d2f90298dd0f0f352fed8ca1a17de5999e85b6b0c32a2b`，无未解决问题。第一次抽取器漏识别缩进 fence，产生零 block；该不充分记录保留为 `checks-fence-missed.json`，修正抽取器后才得到上述实际解析数，不把零检查称为语法通过。
6. B 对运维篇独立完成的 13 个 shell block、2 个 Python heredoc 解析等证据是复用其报告，**不是本报告作者本轮重跑数**。P6、JOINT 和真实部署报告用于边界说明，也不是本轮重新执行功能、故障或压力证明。

## 数据库篇独立核验

1. **入口和隔离：静态一致。** 对照平台 CLI、管理 API 和权限代码，项目 tenant 与数据库身份必须对应；项目凭据、CA 路径经私有文件提供，全局管理 Key 不能用于 SQL。一次性 SQL 命令和持久 shell 的连接生命周期有区分，不跨进程命令假设同一事务。
2. **SQL 与事务：静态一致。** 同一 session 的 BEGIN/COMMIT、显式 ROLLBACK 与重开后查询分开记录。query 方法名不能保证只读；原生 ReadOnly 是实际连接属性，不是正则过滤。完整 SQL 错误的两个终态标记不证明事务已自动回滚或多语句无提交前缀。
3. **恢复：静态一致。** 对照 `prototypes/v1/platform/management_tools.go` 与 `prototypes/v1/src/capability_cli.rs`，托管 `/pitr` 是查找点，`/restore-point` 才恢复；可信底层 `pitr`/`pitr-time` 命令具有直接恢复语义，不能混用。恢复前后的 incarnation、旧 token/session 和当前 ACK 验证有明确限制。
4. **分支、存储与观察：静态一致。** 克隆保留加密 domain，不宣称创建独立 DEK；只读分支不能由默认 Writer UI 按钮代替。refs 图有展示上限，不是完整物理存储遍历；GC inspection/collect、保留窗口与授权区别明确，不能删除用户 bucket。status 的可选 refs/diagnostics 不作为 ownership 或提交证明，资源 payload 指标不是 RSS。

## 平台篇及新 UI 独立核验

1. **管理契约：静态一致。** 对照 management、auth、model 和 gateway，组织/项目/Key 创建、tenant 生成、默认配额、shown-once 凭据、Token TTL/发行者撤销和跨项目拒绝均有真实入口。示例身份是占位/隔离标签，不提供真实凭据；新 namespace 只初始化 Writer。
2. **JSON 成功：静态一致。** `web/rpc.js` 严格校验 version、请求 ID、完整 envelope 与 SQL action 结果形状；UI 与 SDK 使用相同 header/body ID。成功面板显示的是经过校验的 result，不是完整 envelope 或成功 ID。要保存相关性证据，只摘录安全 ID/状态/必要结果，不导出 Authorization、凭据或完整 SQL 网络记录。
3. **NDJSON：静态一致。** UI 要求唯一 complete、相同 ID、声明行数等于收到行数，并读取 EOF 确认没有 complete 后的额外帧，才返回并显示本次 rows。缺终态、截断、非法帧、错 ID/count 不显示本次成功；这是已受限、已物化结果的输出，不宣称 native 增量游标。
4. **未知和迁移：静态一致。** unknown/session_moved 使本地句柄不可用，UI 显示“会话不可继续”；同一句柄的 query/execute/close 本地拒绝，不自动重试或清理 close。此 poison 不是服务端回滚、原生调用返回、物理 I/O 取消或完成证明，刷新/重开也不恢复旧事务。
5. **明确 SQL 错误：静态一致。** 完整 `sql_error` 必须带 `execution_completed:true` 和 `may_have_committed:true`；这种负终态不 poison 会话，可以由用户用新请求 ID 显式 ROLLBACK。缺终态则停止，不放宽校验。
6. **早期拒绝兼容边界：已保留。** Gateway 在读 body 前可能认证失败或拒绝配额，仅 body 有 ID 的旧/外部客户端仍可能不能得到该 ID 回显。关联失败不能当作已确认终态；新 UI 发 header/body 同 ID，不将此旧客户端限制错误套用到新 UI，也不把 ID 当作 SQL 去重保证。
7. **观察与审计：静态一致。** status 的不可用可选观察不等于 ownership 通过；metrics 累计延迟不提供因果归因或百分位。认证前匿名占位不冒充主体，真正 audit 失败仍 fail closed，不能把执行后 unknown 解释为 rollback。

## SDK、运维与总入口交叉核验

1. **SDK：静态一致。** Go 的 RootCAs、Node 启动前 NODE_EXTRA_CA_CERTS、Rust trusted_ca_pem 与实际客户端入口一致；不绕过 TLS。私有连接文件、精确 i64、请求 ID、session 生命周期和安全错误记录有可检查预期，不用 UI 成功代替三个 SDK 的实际结果。
2. **程序生命周期：静态一致。** Go 完整程序的临时目录与返回根目录步骤可解析；TS smoke 的 finally close、Rust 示例先 query 后 close 的限制已明确，不把 close ACK 当未知 query 的取消证明，不隐瞒可能改变最终错误 stage 的处理顺序。
3. **运维：独立结论复用且衔接一致。** B 核验过 launcher、S3 凭据仅 Authority、proper CA、精确小写 `authority/authority.lock`、joined 退出及 dirty restart 拒绝。release 构建显式 host target/target-dir，避免指向旧产物。通用 faultprobe 的 management/topology 选择不是实时 Authority lease oracle，不能借用专用 joint 的强证据。
4. **历史修正保留。** OPS 初版锁名、隐式 Cargo target 产物路径与故障 oracle 过强三项由作者修正后经 B 复验；平台篇旧冻结 SHA `cc4e6661…` 不含新 UI 行为，本轮反馈作者增加终态/poison/result-only 描述，再增加 body-only 兼容句，最终采用上表新 SHA。不把最终文档通过写成所有旧版本均正确。
5. **总入口：静态一致。** 先独立部署与私有凭据，再分别操作平台/数据库/SDK；重启、故障和压力需单独满足授权条件。验收模板区分 PASS/FAIL/NOT RUN，保留首次失败，不强行关闭未知会话；单 Authority 原型不宣称 HA、Kubernetes 实际集群通过或未测性能。

## 子模块发布限制：最终增量

1. 主线程后续仅授权 OPS 源码准备步骤增加发布限制；作者新增 4 行，其他字节未变。当前运维篇为 261 行，最终 SHA-256 `2cc655b2b7cc9dd67593b6eec375dd3144088c3594458936312cc935a0846305`，取代首次完成时的 OPS 身份。
2. B 已独立增量 STATIC PASS，其报告只追加发布限制复核，现为 91 行，最终 SHA-256 `02365ffcf87abed19432ed6a888d078664e37a1f2d1753694242cba270753862`。不是重跑原语法、功能或部署门禁；本报告不自证作者 OPS。
3. 新限制明确：维护者声明父仓库与 Turso 对应提交目前仅本地、尚未 push；已有完整本机子模块不受影响，全新网络 checkout 能否获取固定 gitlink 取决于维护者发布对应 fork 提交，不能保证现在全新 clone 可重现。fetch 失败须停止联系维护者，不能 vendor、改 gitlink、换提交掩盖，也不自行 push。
4. 本轮没有联网检查远端可达性。B 只读确认本机 gitlink 与子模块 HEAD 一致；“尚未 push”是维护者声明，不包装成独立 fetch 证明。作者与 B 均验证删除新增段后精确恢复原 OPS SHA，证据另存 `ops-delta.json`。
5. 四份其他篇与 39 个源码锚点再次字节核对未变，复用其上述静态检查，不重复正文执行。六份最终文档身份记录于 `docs-delta-final.json`，SHA-256 `5235487606bcdd7c0c10676bb9ee517c43ac4a5c4a25d118cb393afed9e9d515`；只说明这次文档增量，不扩大为全树新门禁。
6. 最终文档结论仍为 STATIC PASS，无未解决的文档 blocker；原报告 65 行版本的证据保留，本次仅更新 OPS 身份并追加该限制及 B 复核来源，其他篇结论不变。

## 冻结与后续执行限制

1. 本报告完成后冻结，供主线程按限定文件提交；本轮未改变 P6/JOINT 报告。核对其原 SHA 分别为 `4eb2a0ea74c60820659242fbab3f36674a00109f76d0cb0c7d1f92b8edefa5c0`、`598c1bac0ba31691ae1dba9e12de50f40c8d65ab8670c6a4476c7679d473534c`。
2. 本次结论可用于提交用户路径文档，不是批准对现有 fixture 发 RPC、SQL、负载、信号或清理，也不授权把历史 fixture 参数直接复制为当前故障演练目标。
3. 最终压力对抗验证仍等待主线程通知与实测/独立验证/修复复测报告；本文没有开始该任务，没有新增或推断任何性能结论。
