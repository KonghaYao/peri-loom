# 平台用户行为验收路径

本文是依据当前源码整理的**待执行验收步骤**，不是已通过的验收报告。
本次编写只读源码，不启动部署、fixture、SQL 请求或压测。
执行者应逐项记录实际结果；未执行、环境不足和失败分别标记，不用预期结果代替证据。

## 1. 先决条件与证据约定

1. 使用主线程提供的已启动、已确认版本的独立验收环境。
   记录平台、Rust 服务版本和公开 Gateway 地址 `<GATEWAY_URL>`；本文不要求重新启动环境。
   本地部署入口见 `prototypes/v1/platform/README.md` 的 `dev.py`，默认公开地址是
   `127.0.0.1:9443`，UI 位于公开地址 `/`。外部监听要求 TLS；开发 CA 仅用于该环境，不能关闭证书校验。
2. 确认 authority、worker、平台和 controller 已就绪，有健康且区域匹配的 worker。
   操作独立验收项目，不使用真实业务数据库。先获得全局管理 Key，再创建项目级 Key。
   `<GLOBAL_KEY>`、`<PROJECT_KEY>`、`<DB_TOKEN>`、`<PUBLIC_SESSION>` 都是占位符，不是可用凭据。
3. 使用浏览器 UI 或经批准的 API 客户端。API 使用 `Authorization: Bearer <相应凭据>`；
   POST 使用 `Content-Type: application/json`。管理 POST 的 `request_id` 使用
   1–64 位 ASCII 字母、数字、`_`、`-`，例如 `user_path_001`。
   SQL POST 同时发送合法 `X-Request-ID` 与相同的 JSON `request_id`。
4. 管理与普通 SQL JSON 响应均检查 `version:1`、`ok`、`result`、`error`；
   SQL 请求还应严格核对 `request_id`。管理 GET 的响应 ID 可以为空。
   当前 UI 成功面板只显示经过校验的 `result`，不显示完整成功 envelope 或成功请求 ID；
   若需保存关联证据，使用脱敏客户端记录或 DevTools 仅摘录安全请求 ID、状态及必要结果，
   不导出含 Authorization、凭据或业务 SQL 的完整网络记录。错误面板显示安全错误码、请求 ID 和终态/未知标记。
   保留脱敏的状态码、请求 ID、错误码、必要结果和时间，不保存 Bearer、Token、私钥、DEK 或业务 SQL。
   创建 Key 和 Token 的响应含秘密，不能直接截图、入报告或提交仓库。
5. 所有破坏性操作先确认目标与权限。本文不要求杀进程、删除缓存、修改 dirty marker 或访问原始存储。
   任何未知结果都停止该会话的后续 SQL，不自动重试；重新连接不代表此前 SQL 已取消。

## 2. UI 连接与目录隔离

1. 打开 `<GATEWAY_URL>/`，确认页面包含“数据库控制台”“SQL”“DBA 与恢复”“项目管理”和“真实响应”。
   API Key 输入框为密码字段；当前页面 JS 使用输入框内存，不把凭据写入浏览器持久存储。
   页面本身能加载不代表已经认证成功。
2. 输入项目 API Key，点击“连接 / 刷新目录”，对应 `GET /v1/catalog`。
   期望连接状态显示“已连接 · authenticated”，数据库下拉列表仅来自该项目可见目录。
   API 返回 `revision`、`projects`、`databases`；已删除数据库不出现在 UI 下拉选项中。
3. 换用另一项目 Key 并刷新，确认目录不包含前一项目的数据库。
   全局管理 Key 可看全局目录，但不能用它打开 SQL 会话；不要把全局可见性等同于 SQL 权限。
4. 记录目录中的真实 `database.id`，后续请求以它为准，不从 UI 文本猜测 namespace。
   示例验收身份为 `{"namespace":{"tenant":"qa_org_qa_project","database":"qa_db"},"branch":"main"}`；
   下文用 `<DB>` 表示这一 JSON 对象，不表示一个字符串。

## 3. 创建 Organization、Project 与项目 Key

1. 前提：使用全局管理 Key，准备未被占用的 `qa_org`、`qa_project`。
   UI 输入 Organization ID，点击“创建 Organization”；API 为
   `POST /v1/organizations/create`，字段为 `request_id`、`id`、`name`。
   期望成功结果包含创建的组织身份；名称不可为空。
2. UI 输入 Project ID、Organization ID、Region，点击“创建 Project”；API 为
   `POST /v1/projects/create`。项目 tenant 由源码生成 `organization + "_" + id`，不是自由输入。
   Region 必须与实际可用 worker 匹配；项目创建成功不证明该区域有可用容量。
3. 若要验收低额度，使用 API 在创建项目时传入完整 `quotas`，而不是寻找不存在的 UI 配额编辑按钮。
   请求示例如下，所有名字均为隔离验收用的非秘密标识：

   ```json
   {"request_id":"user_path_project","id":"qa_project","organization":"qa_org","region":"local","quotas":{"max_databases":4,"max_sessions":2,"max_concurrent":1,"requests_per_second":10,"max_sql_bytes":4096,"max_result_bytes":4096}}
   ```

   期望响应及 catalog 中的配额一致。未提供或为零的字段使用默认值：数据库 8、会话 16、并发 4、
   每秒请求 100、SQL 65536 字节、结果 4194304 字节。当前没有通用项目配额修改 API。
4. UI 点击“创建项目 API Key”，当前 UI 固定请求项目 `admin`、subject `console`。
   API `POST /v1/keys/create` 可明确传 `project`、`subject`、`role`、`service_account`；
   项目角色是 `admin`、`developer`、`sql`、`viewer`，服务账户仍使用这些授权规则。
   全局 admin 或该项目 admin 可创建 Key。
5. 期望返回 `key`、`subject`、`project`、`role`、`credential`、`shown_once:true`。
   在私有凭据管理位置保存 credential，仅记录非秘密 Key ID。
   切换到项目 Key 后继续 SQL 验收，不能沿用全局 Key；源码不提供再次查询明文 Key 的接口。

## 4. 创建加密数据库与查看状态

1. 前提：项目已存在，项目 tenant 和目标数据库 namespace 完全一致；使用项目 admin/developer
   或有相应创建资格的全局 admin。UI 填写 Organization、Project、数据库名称后点击“创建加密数据库”。
   UI 根据这两个 ID 构造 tenant，数据库分支固定 `main`、角色固定 `writer`。
2. API 为 `POST /v1/databases/create`，字段为 `request_id`、`project`、`database:<DB>`、`role:"writer"`。
   新 namespace 的初始化只支持 Writer；不要声称可以直接创建 ReadOnly 空数据库。
3. 成功后刷新 catalog，期望数据库 `state:"active"`、`encrypted:true`，
   `root_incarnation` 非空、`branch_incarnation` 大于零。
   UI “数据库 / WAL / generations”对应 `POST /v1/databases/status`，body 带 `database:<DB>`。
   检查实际 placement、worker、refs 与 diagnostics；创建错误可能返回 `409 operation_incomplete`，
   不能解释为所有前置操作已经回滚。
4. 安全边界：新数据库使用原生页/WAL 加密，不代表已有明文数据库被追溯加密。
   LocalKMS 用 AES-GCM 包装 DEK，但 KEK 存在私有文件中，KEK 文件本身未加密；
   不是 HSM，不能承诺防御宿主机 root/磁盘被控制后的密钥恢复。

## 5. 打开、查询与关闭 SQL 会话

1. 前提：使用项目 admin/developer/sql Key，选择 active 数据库。
   UI 点击“打开 SQL 会话”，对应 `POST /v1/sql/open`，body 为 `database:<DB>` 加 request ID。
   期望 `result.session` 为 32 位小写十六进制公开句柄，`result.readonly` 表示实际原生只读属性。
   UI 显示“Writer 会话”或“原生 ReadOnly 会话”；不能提交 worker 的私有 session 句柄代替它。
2. 输入 `SELECT 1;` 并点击“Query / 返回行”，对应 `/v1/sql/query`，字段为 `session`、`sql`、`request_id`。
   期望 `result` 为行数组并在表格出现 `1`。
   UI 的 `web/rpc.js` 校验响应版本、请求 ID、完整 envelope 及该操作的结果形状，校验后才返回结果。
   UI 同时发送 `X-Request-ID` 与相同 body ID，不是只发 body ID 的旧客户端。
   仅在 body 提供 ID 的旧/外部客户端，认证或配额等 body 读取前的拒绝仍可能无法回显该 ID，
   客户端不能将关联校验失败当作已确认终态；请采用 header/body 同 ID，不放宽校验或自动重试。
   每次业务操作使用新请求 ID；相关性 ID 不是 SQL 去重或 exactly-once 保证。
3. 查询 `SELECT 9223372036854775807, -9007199254740993;`。
   期望 JSON 数字字面量和 UI 单元格保留这两个整数，不变成四舍五入的浮点数。
   普通 API 客户端也必须采用保整数精度的解析器，不能先用有损解析再比较结果。
4. 勾选“NDJSON 输出”后再次 Query，客户端发送 `Accept: application/x-ndjson`。
   期望响应为 row 事件和唯一的 complete 事件；complete 的请求 ID 必须匹配，rows 必须等于已收到行数。
   UI 直到读取 EOF、确认 complete 后无后续帧才接受成功；缺少 complete、截断、ID/行数不符或多余帧不能显示为本次成功结果。
   这是已受限、已物化 worker 结果的 NDJSON 输出，不是原生逐行执行游标或无限结果流。
5. UI 点击“关闭会话”，对应 `POST /v1/sql/close`，字段为 `session`、`request_id`。
   健康关闭后可见 UI “无会话”，重新打开得到新公开句柄。
   未提交事务的正常连接关闭与未知 RPC 的终止证明不是一回事；不能用关闭 ACK 消除未知结果。
6. 若当前会话出现 `outcome_unknown` 或 `session_moved`，UI 显示“会话不可继续…”，
   并在本地将该句柄标为不可用；之后对同一句柄的 SQL/close 在本地拒绝，不自动重试或发送清理 close。
   保留证据并按运维流程处置，不用刷新页面或重新打开来推断旧操作已经取消。
   本地 poison 是客户端保护，不是服务端事务回滚、原生调用返回或物理 I/O 终止证明。

## 6. 事务、约束错误与显式恢复

1. 前提：同一个 Writer 会话，仅使用验收表。依次 Execute
   `CREATE TABLE user_path_items(id INTEGER PRIMARY KEY, value TEXT);` 和
   `BEGIN; INSERT INTO user_path_items VALUES(1,'accepted');`。
   `/v1/sql/execute` 的成功结果是执行确认，不应一律称为 commit ACK。
2. 在同一会话 Query `SELECT id,value FROM user_path_items ORDER BY id;`，
   期望看到本事务写入；再 Execute `COMMIT;`。
   正常关闭后重新打开并查询，期望保留已提交行。记录提交和重开查询的实际响应，不能据此声称做过进程崩溃验收。
3. 同一会话 Execute `BEGIN; INSERT INTO user_path_items VALUES(1,'duplicate');`，
   验证主键冲突的完整终态负响应为 `sql_error`，包含
   `execution_completed:true`、`may_have_committed:true`。
   它只证明同步 SQL 调用已返回，不保证事务已经回滚，也不保证多语句没有提交前缀。
4. 收到上述明确终态后，使用**新的操作请求 ID**显式 Execute `ROLLBACK;`，再 Query 原行。
   期望会话仍可用、原已提交值不变；不能自动重放刚才的 INSERT。
   UI 的完整 `sql_error` 终态校验要求两个标记都为 true；此错误不使本地会话 poison，因此可显式 rollback。
   若错误没有终态标记或出现 EOF/timeout/`outcome_unknown`，停止这一路径并记录未知，不继续复用该会话。
5. Query 也可能执行写 SQL；只读性不能由按钮名称或 SQL 正则推断。
   此路径只用有限、已知 SQL，不执行资源压力 SQL；不可把事务成功路径当作吞吐率结论。

## 7. 数据库 Token、过期与发行者撤销

1. 前提：加密数据库 active、密钥 ready、incarnation 已配置；使用可签发的项目 Key。
   UI “签发 2 分钟数据库 Token”固定发 Writer Token。
   API `POST /v1/tokens` 的字段为 `database:<DB>`、`role:"writer"` 或 `"read_only"`、
   `expires_ms` 和 `request_id`；有效期必须在当前时间之后且不超过五分钟。
2. Writer Token 可由项目 admin/developer/sql 签发；ReadOnly Token 还允许 viewer 签发。
   Token 绑定发行 Key、subject、项目、具体数据库/分支、root/branch incarnation 和加密 domain。
   只记录脱敏的签发成功与到期时间，不记录 Token 内容。
3. 用 `<DB_TOKEN>` 直接调用 SQL open/query/close 验证授权。
   不将 Token 当成管理 API Key：管理接口拒绝数据库 Token，当前 UI 先读 catalog 的连接流程也不适合用 Token 导航。
4. 等待到期后，对原会话发送一个新的有限 Query，期望 `401 unauthenticated` 且不执行 SQL。
   每次请求都会重新认证；已经打开过会话不表示它永久保有权限。
5. 用有权管理发行 Key 的 admin 调用 `POST /v1/keys/revoke`，body 带 `id:<发行KeyID>`、`request_id`。
   UI 没有专用撤销按钮，走 API。随后发行 Key 与其未到期 Token 都应认证失败。
   Token 不可自行延长或跨数据库使用；恢复变更 incarnation 后，旧 Token 也不能自动重新绑定。

## 8. 权限、跨项目与原生 ReadOnly

1. 使用另一项目 Key 对 `<DB>` 尝试 SQL open，期望拒绝且不取得公开句柄。
   用不同 subject 的 Key 对已知公开句柄尝试 Query，期望 `403 invalid_session`；
   随机合法格式句柄也应 `403 invalid_session`，不泄露句柄是否存在。不把不可猜测句柄替代逐请求授权。
2. 项目 viewer 可查看授权数据库的 status/schema/refs，但不能直接打开 SQL 会话。
   项目 admin/developer 可做管理和 checkpoint；项目 sql 可执行 SQL，但不具备数据库管理权限。
   全局管理 Key 不能执行 SQL或签发数据库 Token；拒绝响应记录实际错误码，管理拒绝不统一等于 SQL 403。
3. 使用 API `POST /v1/databases/branch` 创建独立只读分支，传
   `database:<DB>`、`name:"qa_readonly"`、`role:"read_only"`、`request_id`。
   UI 的“创建分支”没有角色选择，默认 Writer，不能拿它替代这一步。
4. 对原 Writer placement 签发的 ReadOnly Token 尝试 open，期望
   `403 readonly_placement_required`；不能允许只读 Token 接入 Writer 再靠过滤 SQL 防写。
   对实际 `qa_readonly` 分支签发 ReadOnly Token、打开会话，期望 `readonly:true`，SELECT 成功，写入被原生拒绝。
5. 即使使用能写的项目 SQL Key打开整个 ReadOnly placement，写 SQL 也必须失败。
   不对外公开 `/v1/sql/checkpoint`、`/compact`、`/diagnostics` 或任意 worker command；
   这些公共 SQL 路径应拒绝，管理工具只能经各自授权后的管理入口使用。

## 9. 配额与传输关联的有限验收

1. 前提：第 3 组创建的隔离项目有小而合法的 quotas。一次只进行必要的有限请求，禁止将本文当成压测脚本。
   打开到 `max_sessions` 后再 open，期望 `429 quota_exceeded`，不获得新会话；关闭健康会话后可再打开。
2. 对大于 `max_sql_bytes` 的有限 SQL 请求，期望 `413 sql_limit`，无 SQL dispatch。
   对超出 `max_result_bytes` 的实际返回结果，期望 `result_limit`；若已经执行，则响应带
   `execution_completed:true`。结果超限不是“SQL 一定没有执行”证明。
3. 并发/速率限制的具体触发使用独立定向验收或批准的现有证据，本文不安排高并发或负载注入。
   范围是单 Gateway 进程、每项目；项目 A 的许可不应占用项目 B 的配额。
   `max_databases` 也统计未删除分支，超过数据库/分支额度的管理创建应失败。
4. 早期 401/429 请求携带合法单个 `X-Request-ID` 时，期望响应精确回显该 ID。
   重复、非法 header，或已解析 body ID 与 header 不一致，期望 `400 invalid_request`，无 SQL dispatch。
   非法 header 使用安全生成 ID，不回显非法字节；header 仅用于 correlation，不授予任何权限。
5. 不带 header 的正常 body-ID 请求仍兼容；但若 body 尚未读取便因认证/额度拒绝，响应使用生成 ID。
   不声称 body-only 的早拒绝保证 echo，不放宽客户端 envelope 校验，不修改分类器掩盖原始失败。
   未知上游结果保留许可并 poison 会话，不能手工清零计数来“恢复配额”。

## 10. DBA、快照、分支、Clone 与 PITR

1. 前提：项目 admin/developer，已有已提交验收数据；管理动作前先关闭本页 SQL 会话。
   UI “Schema browser”对应 `POST /v1/databases/schema`；期望结果来自实际 `sqlite_schema`。
   “Checkpoint”“Compact WAL”分别对应 `/v1/databases/checkpoint`、`/v1/databases/compact`，
   body 带数据库与 request ID，期望成功结果；ReadOnly/fence 仍由原生层校验。
2. 在快照输入框填唯一名字 `qa_snap`，点击“创建快照”，对应 `/v1/databases/snapshot` 的 `name`。
   点“分支图 / 快照”，对应 `/v1/databases/refs`，期望看到已创建快照的实际引用。
   图展示 branch/snapshot 到不可变 head/WAL 对象的关系，不声称推导完整分支血缘；
   快照创建是不可覆盖的，不可用同名创建把旧快照修改为新状态。
3. 创建 Writer 分支可点“创建分支”，对应 `/v1/databases/branch` 的 `name`。
   从快照 Clone 使用 API `/v1/databases/clone`，分别指定 `snapshot:"qa_snap"` 与新 `name:"qa_clone"`。
   当前 UI 将同一个输入值同时当 snapshot 与新 name；两者需要不同名字时走 API，不假设有两个 UI 输入框。
4. 新分支/Clone 在目录中出现后分别打开，核对快照中的提交数据。
   在 Writer Clone 中提交一条新行，再查原分支应未受影响；共享引用不等于共享可变写入。
   记录实际 SQL 与 refs 的脱敏结果，不仅凭图形推断 COW 已通过。
5. Restore 快照使用 `/v1/databases/restore` 的 `snapshot`；这是破坏性状态切换，先确认目标并关闭会话。
   期望恢复数据，incarnation/fencing 推进，调用方重新打开会话并重新签发需要的 Token。
   不把恢复等同于 root revision 回滚，不声称旧会话自动续用。
6. 从真实 refs 历史取得保留的 sequence，UI “查找恢复点”对应 `/v1/databases/pitr`。
   查询只返回恢复点，不恢复数据；“恢复所选 PITR 点”调用 `/v1/databases/restore-point` 的 `sequence`。
   API 也可使用 `timestamp_ms` 查询最新不晚于指定时间的保留点；历史之外、真实未来或未保留 sequence 应失败。
   PITR 不承诺任意 SQL 语句的中间位置，也不允许根据估算时间伪造提交点。
7. “GC inspection”对应 `/v1/databases/gc-inspect`；先检查再决定是否执行 `/v1/databases/gc`。
   删除快照与保留裁剪只能分别走 `/drop-snapshot`、`/retain-after` API；UI 没有对应专用按钮。
   有引用的快照/历史对象应保留，不把删除分支称为密码学擦除；本组不要求原始对象删除操作。

## 11. 迁移、Suspend、Resume 与删除

1. 前提：已记录目标数据库与健康目标 Worker ID，先显式结束事务并关闭 SQL 会话。
   UI 目标输入框与“迁移”对应 `/v1/databases/migrate` 的 `worker_id`。
   期望最终 status 中实际 owner/epoch 收敛到目标，而不是只看到按钮返回就断言完成。
2. 正常同一 binding 的 `renew_intent`、`renew_pending`、`renew_worker_pending` 不应使 SQL 会话永久迁移。
   暂态 placement/worker 不可用应在 dispatch 前报告 `placement_unavailable`，不是证明身份改变。
   成功路由证实 worker/address/epoch/root/branch 等身份改变后，旧会话为终态 `session_moved`，必须重新打开。
   不通过主动杀 worker 制造本组验收；需要的时序证据使用批准的独立定向结果。
3. 点击“Suspend”，对应 `/v1/databases/suspend`；期望 catalog 状态 suspended，SQL open 不再可用。
   graceful drain 等待已有连接/事务结束，不承诺强制中止未知执行。
   再点“Resume”，对应 `/v1/databases/resume`；收敛后可新开会话继续读已提交数据。
4. “删除当前分支”对应 `/v1/databases/delete`，先确认且仅对隔离验收分支执行。
   期望 catalog 标为 deleted、UI 不再列出；namespace 身份永久保留，不承诺同名新建或物理/密码学立即擦除。
   任意 `operation_incomplete`/未知回复都应记录并交由运维检查，不自行擦除持久标记或盲目重试。

## 12. 指标、审计与验收结论

1. 前提：使用全局管理 Key。UI “Gateway metrics”对应 `GET /v1/metrics`；
   项目 admin、普通项目 Key 和数据库 Token 不应获得跨项目全局指标。
   返回包含 Gateway 指标，并在 telemetry 已配置时包含 `native` 下实际 authority/worker 进程指标。
2. 对照有限操作检查 `actions_total`、`errors_total`、`action_latency_ns_total`、
   `active_permits`、`unknown_held_permits`、`projects` 与 `observed_workers`。
   计数包括错误请求，累计延迟不是百分位，execute 延迟语义为 `execute_not_all_commits`。
   不能拿这些聚合值单独判定 Authenticate、状态复制、网络或 audit fsync 的耗时归因。
3. 用数据库 status 的 `diagnostics` 核对实际 `generation`、`manifest_hash`、`logical_db_size`、
   `segment_count`、`applied_lsn`、`applied_wal_position`、`resources`。
   `snapshot_consistency:"diagnostic_only_not_authority"` 表示诊断快照，不是提交权威证明。
   cache 命中率是 RAM/SSD segment cache 指标，不是 native pager 命中率；缺失或 unavailable 就如实记录。
4. 请有权限的运维只读检查持久审计，核对请求 ID、主体、项目、动作、结果，不索取密钥或 SQL 文本。
   认证前事件用明确 `unauthenticated` 占位，不冒充用户；真实 audit 写失败仍 fail closed。
   执行后的 audit 失败可能返回 `outcome_unknown`，不能解释成事务回滚；审计存储有 64 MiB 上限，需要运维归档计划。
5. 最终逐组给出“已通过 / 未执行 / 失败 / 环境不足”和证据位置。
   本文不替代 F 的独立门禁、原始压力失败记录或 S3 整栈结果，不据此宣布所有阶段联合 PASS。

## 源码核对入口

1. UI 与路由：`prototypes/v1/platform/service.go`、`admin.go`、`web/index.html`、`web/app.js`、`web/rpc.js`、`web/refs.js`。
2. 管理与权限：`prototypes/v1/platform/management.go`、`management_tenancy.go`、`management_databases.go`、
   `management_tools.go`、`auth.go`、`auth_keys.go`、`model.go`。
3. SQL 与安全边界：`prototypes/v1/platform/gateway_http.go`、`gateway_sessions.go`、`gateway_routing.go`、
   `sql_upstream.go`、`admission.go`、`gateway_stream.go`；部署说明见该模块 `README.md`。
4. 观察与诊断：`prototypes/v1/platform/metrics.go`、`telemetry.go`、`audit.go`，
   `prototypes/v1/src/runtime/handle.rs`、`resources.rs`；密钥边界见平台 `kms.go`、`state_disk.go`。
