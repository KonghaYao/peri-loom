# 数据库用户操作路径

本文是按真实 CLI、Gateway API 和浏览器控制台源码核对的操作说明，不是
这些步骤已经在当前环境执行通过的验收记录。编写本文未启动服务、执行 SQL、
发送 fixture RPC、运行负载或删除缓存。所有操作只应针对自己新建的隔离数据库。

## 共同前提与隔离

1. 先由部署负责人提供一个健康、已授权的开发部署及其 Gateway HTTPS 地址、
   CA 文件、项目 API Key 和已有 `peri` 二进制。部署入口参见
   `prototypes/v1/platform/README.md`；不要为跟随本文另起服务占用现有端口。
   S3 部署必须使用专用 bucket，真实 S3 凭据仅交给 authority。
2. 示例采用 launcher 创建的项目 `default`、tenant `local_default`。若不是此部署，
   必须改为自己获授权项目及它对应的 tenant，不能仅改数据库名称跨租户访问。
   使用项目 admin/developer 权限执行管理与 SQL；全局管理 Key 不能用于 SQL。
3. 在私人 shell 配置以下变量。路径示例必须替换为负责人给出的真实路径；
   `PERI_CREDENTIAL_FILE` 是仅包含项目凭据的本人所有私有普通文件，建议 `0600`，
   不能是符号链接。不要把密钥放进参数、命令历史、文档或截图；不要开启 `set -x`。

   ```sh
   export PERI_ENDPOINT='https://127.0.0.1:9443'
   export PERI_CA_FILE='/absolute/private/path/dev-ca.crt'
   export PERI_CREDENTIAL_FILE='/absolute/private/path/project-api-key'
   PERI_BIN='/absolute/path/to/prebuilt/peri'
   RUN_SUFFIX=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')
   DATABASE="local_default/userpath_${RUN_SUFFIX}/main"
   SNAPSHOT="before_change_${RUN_SUFFIX}"
   CHILD="fork_${RUN_SUFFIX}"
   CLONE="clone_${RUN_SUFFIX}"
   ```

4. 新建独立数据库，不使用现有 `demo`、压力 fixture 或任何用户数据库：

   ```sh
   "$PERI_BIN" create --database "$DATABASE" --project default \
     --role writer --request-id "up_${RUN_SUFFIX}_create"
   "$PERI_BIN" status --database "$DATABASE"
   ```

   预期 placement 为 `active`，有实际 worker 与 grant。如果创建失败或结果未知，
   停止后续步骤，保留 request ID 交运维检查；不要改名称掩盖问题、自动重发创建，
   或把 status 读回当作原请求已经取消的证明。
5. UI 从该 Gateway 页面进入，输入项目 Key、连接/刷新目录并选中刚建的数据库。
   CA 只信任此测试部署，不关闭 TLS 校验。浏览器 Key 仅存在当前页面内存，但响应
   面板可能显示签发凭据，不应分享整个面板或 connection 文件。
6. 统一证据至少包括：所用隔离数据库标识、非秘密 request ID、操作终态、placement
   的 worker/epoch、必要的查询结果。只保存在私有目录；不记录 Authorization、Key、
   DEK、连接配置全文或无关用户数据。request ID 是关联标识，不是 SQL 幂等保证。

## 一、SQL 读写与事务

1. **前提**：新数据库已 active，使用具有 SQL 权限的项目 Key。CLI 单次 `sql`
   命令打开并关闭自己的会话，不能用多个独立 `sql` 命令拼接一个事务。
   UI 支持同一会话的 Execute/Query，CLI 持久会话入口如下：

   ```sh
   "$PERI_BIN" shell --database "$DATABASE"
   ```

2. 在同一 shell 内依次输入，每一步成功后才继续：

   ```text
   :execute CREATE TABLE path_ledger(id INTEGER PRIMARY KEY, note TEXT NOT NULL);
   :execute BEGIN;
   :execute INSERT INTO path_ledger VALUES(1,'first');
   :execute INSERT INTO path_ledger VALUES(2,'second');
   :execute COMMIT;
   :query SELECT id,note FROM path_ledger ORDER BY id;
   ```

   预期提交终态成功，返回两行 `1/first`、`2/second`。UI 操作对应先“打开 SQL 会话”，
   在同一会话 Execute `BEGIN`、INSERT、`COMMIT`，再 Query；不要切换数据库选择器
   来假定现有 session 已改绑，必须先关闭再打开。
3. 验证显式回滚，同样保持这一个 session：

   ```text
   :execute BEGIN;
   :execute INSERT INTO path_ledger VALUES(999,'not_committed');
   :execute ROLLBACK;
   :query SELECT id FROM path_ledger ORDER BY id;
   :quit
   ```

   预期仍只有 `1、2`。INSERT 返回成功不等于事务已 COMMIT；关闭健康会话会中止未完成
   事务。网络超时、close 超时或进程退出不能推断已经取消一个在途写入。
4. 新建独立会话检查已提交数据与完整性：

   ```sh
   "$PERI_BIN" sql --database "$DATABASE" --query 'SELECT id FROM path_ledger ORDER BY id;'
   "$PERI_BIN" sql --database "$DATABASE" --query 'PRAGMA integrity_check;'
   ```

   预期分别为 `[[1],[2]]`、`[["ok"]]`。这是新 session，不保证一定使用冷 SSD/RAM cache；
   不能把它称作 worker 重启或真实远程冷恢复验收。
5. **证据/限制**：保留显式 COMMIT 的终态与之后新会话结果。Query 也可以执行写语句，
   不是安全只读接口；只读必须来自整个数据库 placement 的原生 ReadOnly 权限。
   viewer 不能对 Writer placement 任意执行 SQL。unknown outcome 不重放 SQL；
   `sql_error` 的终态字段可能提示已提交前缀，不能假定多语句错误自动全回滚。

## 二、快照、分支与 Clone

1. **前提**：已完成上组 COMMIT，结束自己的所有事务/会话。snapshot、branch、clone
   都有 UI 按钮及管理 API；创建分支所需项目数据库配额也计算在内。
2. 建立命名快照并检查真实 refs：

   ```sh
   "$PERI_BIN" snapshot --database "$DATABASE" --name "$SNAPSHOT" \
     --request-id "up_${RUN_SUFFIX}_snapshot"
   "$PERI_BIN" refs --database "$DATABASE"
   ```

   预期 `snapshots` 含该名称与实际 HEAD/WAL、sequence/timeline。UI 名称框用于新快照名；
   “分支图 / 快照”显示 branch/snapshot 到对象的引用，不是完整分支血缘图或 segment 图。
3. 分别从当前分支和快照创建两条新分支：

   ```sh
   "$PERI_BIN" branch --database "$DATABASE" --name "$CHILD" --role writer \
     --request-id "up_${RUN_SUFFIX}_branch"
   "$PERI_BIN" clone --database "$DATABASE" --snapshot "$SNAPSHOT" --name "$CLONE" \
     --role writer --request-id "up_${RUN_SUFFIX}_clone"
   CHILD_DATABASE="local_default/userpath_${RUN_SUFFIX}/${CHILD}"
   CLONE_DATABASE="local_default/userpath_${RUN_SUFFIX}/${CLONE}"
   "$PERI_BIN" sql --database "$CLONE_DATABASE" --query 'SELECT id FROM path_ledger ORDER BY id;'
   ```

   预期新分支 active，clone 为 `[[1],[2]]`。引用共享不可变对象，不进行 segment 全量复制；
   并不意味着分支已经独立存放在另一个 namespace/bucket 或独立加密域。
4. 在 fork 写入一行，检查原分支没有改变：

   ```sh
   "$PERI_BIN" sql --database "$CHILD_DATABASE" --execute "INSERT INTO path_ledger VALUES(3,'fork_only');"
   "$PERI_BIN" sql --database "$DATABASE" --query 'SELECT id FROM path_ledger ORDER BY id;'
   "$PERI_BIN" sql --database "$CHILD_DATABASE" --query 'SELECT id FROM path_ledger ORDER BY id;'
   ```

   预期原分支 `[[1],[2]]`，fork `[[1],[2],[3]]`。证据是数据隔离结果和 refs 中的真实对象引用，
   不是 UI 图形位置或进程 RAM 占用。
5. **限制**：CLI 可显式 `--role read_only` 创建 reader 分支；UI 当前未提供分支 role
   选择，默认 Writer。UI Clone 当前用同一个名称框同时发送 snapshot 与新分支 name，
   无法分别填写两者；不同源快照/目标分支名称请使用上面的 CLI 或 API。
   大 refs 的 UI 图有 200 个 reference 节点限制；API 当前不是已实现
   分页接口，不能根据 UI 的提示假定存在分页参数。未知管理结果需人工处理，不改 request
   ID 自动重试。共享 snapshot、history、pin 会影响后续 GC 保留集合。

## 三、PITR 与恢复

1. **前提**：使用自己这一个隔离数据库。`refs` 返回原生已确认提交的 history，记录
   一个仍保留的 recovery point 的 `sequence`、`timestamp_ms`、`timeline`，而非凭空估算
   SQL 语句次数。可以选择快照创建前后实际返回的 point，但没有原生 commit 的空数据库
   不会凭空产生 PITR 点。以下 `RECOVERY_SEQUENCE` 必须填写该真实 sequence：

   ```sh
   RECOVERY_SEQUENCE='替换为真实保留的十进制sequence'
   "$PERI_BIN" refs --database "$DATABASE"
   ```

2. **只查找，不恢复**：平台 CLI/API 的 `pitr` 返回 recovery point。

   ```sh
   "$PERI_BIN" pitr --database "$DATABASE" --sequence "$RECOVERY_SEQUENCE"
   ```

   按时间查找改用 `--timestamp-ms` 的实际 UTC 毫秒整数，不能同时传 sequence 与时间。
   UI 支持 sequence 查找，没有时间选择器。UI 先查找、再“恢复所选 PITR 点”；实际按钮
   调用 `restore-point`，不是先创建一个中间快照，页面旧提示不应覆盖真实请求语义。
3. **改变当前状态**：关闭自己的会话/结束事务并核准恢复目标，再显式执行以下二选一。
   不能把两个命令连续执行当作同一流程：

   恢复精确提交点：

   ```sh
   "$PERI_BIN" restore-point --database "$DATABASE" --sequence "$RECOVERY_SEQUENCE" \
     --request-id "up_${RUN_SUFFIX}_restorepoint"
   ```

   或恢复第二组创建的命名快照：

   ```sh
   "$PERI_BIN" restore --database "$DATABASE" --snapshot "$SNAPSHOT" \
     --request-id "up_${RUN_SUFFIX}_restore"
   ```

   预期返回 restored 与重新收敛的 placement，branch incarnation 前进，旧 session/旧
   数据库 token 被拒绝，epoch 高水位不倒退；sequence 高水位与 current_sequence 是
   不同字段，恢复后的 current 指向目标旧状态。不得用旧 session 自动重放未确认 SQL。
4. 显式打开新 session，查询目标时点应该存在的数据并检查完整性；例如恢复到上述
   命名快照后应为 `[[1],[2]]`。UI Restore 不自动替你丢弃已有 SQL session；先关闭健康
   session 再恢复，完成后重新打开。若旧 session 结果已经未知，交运维解决，不盲 close
   当作取消证明。恢复后若业务使用数据库 token，需要重新签发绑定新 incarnation 的 token。
5. **证据/限制**：记录所选 point、restore request ID、恢复前后 incarnation/epoch、新会话
   数据。时间解析先返回不晚于该时间的实际保留点，执行恢复仍需要明确目标；API 恢复只
   接受保留的精确 sequence。已 GC 的历史不能恢复。PITR 不是存储介质备份，也不证明
   所有历史对象健康。新 OwnershipView 的纯 lease proof 仅验证租约身份，历史健康仍由
   full snapshot/clone/GC/实际读取等边界校验。
6. **不要混用旧 CLI**：Rust trusted CLI 的 `pitr BRANCH SEQUENCE` 和 `pitr-time`
   会直接恢复，而平台 `peri pitr` 只查找。运行中的 authority namespace 禁止直接执行
   trusted raw-store CLI；本文全部业务步骤使用 Gateway，不绕过 ownership fence。

## 四、Checkpoint、保留策略与回收

1. **前提**：只处理自己新建 namespace，关闭不再需要的健康会话。先理解哪些分支、
   snapshot、history、服务 pins、未决 upload intents 仍需保留；GC 不是“删除整个 bucket”。
2. 可执行 checkpoint 或 compact，并记录 status：

   ```sh
   "$PERI_BIN" checkpoint --database "$DATABASE"
   "$PERI_BIN" compact --database "$DATABASE"
   "$PERI_BIN" status --database "$DATABASE"
   ```

   UI 有两个对应按钮。checkpoint 持久化脏 segment；compact 改写当前 WAL replay 基线。
   两者都不会自动删除被历史快照/PITR 引用的旧 WAL，也不是清空所有 reader pins。
3. 默认先只检查，不删除：

   ```sh
   "$PERI_BIN" gc --database "$DATABASE"
   ```

   此命令映射 `gc-inspect`，查看 known/reachable/unreachable blobs、unresolved uploads、
   pins、pending/completed deletions。UI “GC inspection”也是检查入口。坏 root 或 retained
   metadata 导致校验失败时必须停止，不应切换成 raw S3 删除绕过去。
4. **可选且不可逆**：核准不再保留的快照与 history 后，运维可通过 CLI/API 调整保留策略：

   ```sh
   "$PERI_BIN" drop-snapshot --database "$DATABASE" --name "$SNAPSHOT" \
     --request-id "up_${RUN_SUFFIX}_dropsnapshot"
   "$PERI_BIN" retain-after --database "$DATABASE" --sequence "$RECOVERY_SEQUENCE" \
     --request-id "up_${RUN_SUFFIX}_retain"
   ```

   两个操作的路由已存在，CLI 实现可接受，但其 help 摘要未列出；UI 未提供对应按钮。
   不做 PITR 示例时不要执行 retain-after。共享 clone/分支仍可保护同一物理实例，删除
   snapshot 不等于删除其所有物理 blob。删除实验分支可用 `peri delete --database`
   指向自己的 CHILD_DATABASE 或 CLONE_DATABASE；UI 删除的是所选分支，不是全 bucket。
5. 显式核准后收集，窗口建议由运维确定，不照抄 UI 的固定短窗口：

   ```sh
   "$PERI_BIN" gc --database "$DATABASE" --collect --safety-window-ms 60000 \
     --request-id "up_${RUN_SUFFIX}_gc"
   "$PERI_BIN" gc --database "$DATABASE"
   ```

   首次可能只 mark，窗口未满时 `deleted` 为 0 是正常结果。后续需要收集时是在窗口
   之后发起一个新的明确维护操作，而不是把未知的上次操作自动重试。持久 tombstones
   不删除；后续 sweep 可再清理晚到的同一唯一物理 ID，不能伤及同内容的新实例。
6. **证据/限制**：记录检查统计、mark/delete 结果与窗口，以及保留分支查询仍成功。
   UI 有 GC collect 按钮，但当前固定 `safety_window_ms:1000`，没有窗口配置及分阶段
   展示；实际回收优先走参数明确的运维 CLI/API。崩溃遗留 pins/未决 intents 可保守泄漏，
   不能把它们手工删掉来追求 `unreachable=0`。cache payload 指标不是 RSS。

## 五、故障、迁移与恢复

1. **前提**：业务 session 结果可分类，操作者只拥有自己的独立部署；准备剩余健康 worker
   与容量。本文不授权 SIGKILL、删除 cache/metadata、清理 dirty marker 或操作现有 fixture。
   故障注入必须另有精确进程/目录授权与独立脚本核验，不属于常规用户操作。
2. 正常迁移或暂停前先结束事务和健康 SQL session，然后显式执行所需的一项：

   ```sh
   "$PERI_BIN" migrate --database "$DATABASE" --worker-id '实际健康目标worker-id'
   "$PERI_BIN" suspend --database "$DATABASE"
   "$PERI_BIN" resume --database "$DATABASE"
   ```

   UI 有 migrate/suspend/resume。graceful drain 等待现有会话结束，不因用户超时就保证
   旧物理 I/O 已结束。不要将此三行不加判断地连续执行；观察每项终态后再决定下一操作。
3. 若 worker 已故障，只做有界只读观察：`peri status --database "$DATABASE"`，确认
   placement 重新 active、实际 worker 已改变、epoch 更高且 grant 身份一致；若容量不足、
   authority unavailable/uncertain 或 pending 不收敛，停止并交运维，不自动重试 SQL。
   Controller 先完成 durable fence 再让替代 worker 获得数据库；心跳故障本身不是旧 I/O
   已停止的证明。launcher 仅将 worker 退出报告 degraded，authority/platform 退出是 fatal。
4. 收敛后显式新建 session，先查询业务幂等键/已确认账本并检查完整性，再决定新的写入。
   旧 session 的 `session_moved` 不应被 SDK/CLI 自动重放；`:reconnect` 仅创建新 session。
   读回数据能辅助业务判定，却不能取消未知在途请求或证明旧写没有提交。
5. Authority 的非干净停止属于运维恢复，不是 UI “Resume”。不能删除 dirty marker 强行
   启动。仅在确认旧 backend 请求全部终止、无重叠 authority 且完成外部核验后，授权运维
   才可采用原服务配置运行 `peri-loom-v1 authority STORE LISTEN STATE_DIR --reconciled`。
   必须保留原 metadata、epoch floor、KEK/keyring、audit 与专用 S3 身份；不能换 bucket
   或回滚所有本地文件后宣称 fencing 单调性。该流程没有 UI 一键入口，没有 HA 共识保证。
6. **证据/限制**：记录旧/新 owner 与 epoch、原请求 ID、新会话结果、服务私有日志中的
   shutdown/restart 终态。SIGTERM/退出请求 ACK 不等于 clean proof，需要实际 joined 退出
   成功及 authority 清洁状态；unknown Go/native exit 不能当成 clean。metrics 必须用全局
   管理 Key，worker unavailable 应显示不可用而非伪零；UI 支持展示 metrics，但不是证明
   fence 已完成的授权来源。普通用户不能从 UI 发起 reconciliation 或修复 dirty startup；
   全局运维可使用 `/v1/reconcile` API，没有对应 `peri reconcile` 命令或 UI 按钮。

## 核对来源与验收边界

1. 入口/凭据：`prototypes/v1/platform/README.md`、`platform/sdk/README.md`、
   `platform/cmd/peri/{main,credentials,shell}.go`；示例 CLI flags 按真实解析器核对。
2. UI：`platform/web/index.html` 与 `platform/web/app.js`；有按钮不等于有完整配置表单，
   也不等于有自动恢复未知结果的流程。
3. API：`platform/management.go` 的 dispatch、`management_databases.go` 的 create/fork/
   placement、`management_tools.go` 的 maintenance/nativeTool/inspect、`gateway_http.go`。
4. 恢复/回收：`src/authority/ownership_maintenance.rs`、`src/capabilities.rs`、`src/gc.rs`、
   `src/capability_cli.rs`、`src/service_cli.rs`、`controller/reconcile_loop.go`。
5. 全部相对 `platform/`、`src/`、`controller/` 路径均位于 `prototypes/v1/`。本文只做源码
   核对，联合真实 S3、重启、故障注入与性能 before/after 的 PASS 需另附独立执行证据，
   不能从这份操作说明或纯 lease proof 推导。
