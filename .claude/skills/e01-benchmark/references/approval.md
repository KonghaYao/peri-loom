# E01 审批、决策与证据记录

更新：2026-10-16。本轮工作目录 `/Users/konghayao/code/ai/peri-loom`，未提交。

## 1. 已获用户批准

### 1.1 表与索引（2026-10-16）

批准范围：**仅限新建隔离 E01 fixture**。本轮在技能自有临时目录中按此创建过小型 fixture
（96 行）做正确性验证，未触碰任何真实业务库。

```sql
CREATE TABLE e01_records (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, created_at INTEGER NOT NULL, payload TEXT NOT NULL);
CREATE INDEX e01_records_group_time_id ON e01_records(group_id, created_at, id);
```

理由：主键覆盖详情点查；复合索引支持同组过滤与稳定时间/id 排序，并暴露大量相同时间的边界。
正文在查询中实际返回并核验，不用无关大字段虚增字节；不建额外覆盖索引以保留回表成本。
两端结构一致。SQL 支持与计划已在原生引擎上核验（见 3.3），平台侧待真实端点验证。

### 1.2 规模选择

用户选择"使用独立合成规模"：由本技能提出具体 N/payload/工作集/资源预算供确认，
**不宣称真实 Peri 2×覆盖**。具体数字见 [synthetic-scale.md](synthetic-scale.md)，
其中 2/4/8× 一律表示*合成基准倍数*。合成下限与预算**尚未批准**。

### 1.3 仍待批准

正式规模（方案 A 或 B）、资源/磁盘上限、SLO 探索口径、导入速率标定、
平台隔离 fixture 的 tenant/database/branch 与端点、凭据注入方式。

## 2. 2026-10-16 用户纠正（已落实）

**接触点是完整 peri-loom 平台，不是内部模块**：必须用公开 URL + 凭据，
建表/导入/查询/oracle 全走公开接口；不导入内部模块、不直接访问 Worker/Authority/
存储或管理状态文件、不要求内部 fixture 控制文件；公开 API 缺能力时如实记为缺口。
计时覆盖请求到完整结果接收；连接/会话策略固定；SQL 不自动重试；凭据只从环境变量或
私有文件读取，不进 argv/日志/报告；不得从非公开遥测推断冷缓存、扫描量或内部工作集。

落实位置：

- `scripts/platform_client.py`：新增黑盒公开 HTTP 客户端，协议形状取自公开证据
  `platform/sdk/go/{client,session,types}.go` 与 `cmd/peri/main.go`（Bearer、
  `X-Request-ID`、`version=1` 信封、`/v1/sql/open|execute|query|close`）。
  凭据仅经 `PERI_API_KEY`/`PERI_CREDENTIAL_FILE`；`repr` 与错误不回显凭据；
  传输失败/信封异常一律 `outcome_unknown`；不重定向、不走代理、不重试。
- `scripts/run_platform.py`：新增平台运行器（open → DDL → 导入 → 查询 → close），
  只使用公开接口；无端点/凭据时在任何请求前拒绝，且不搜索仓库内私有凭据。
- `scripts/verify.py`：把 oracle 比较与证据组装抽成目标无关的共享逻辑。
- `native-harness/` 与 `scripts/run_native.py`：保留并明确标注为**原生基线/跨边界对照**，
  不是平台验证。
- `scripts/sql.py`：默认 `literal` 渲染，使两个目标消费同一 SQL 文本；
  `parameter` 模式保留（仅原生路径已核验）。
- `tests/mock_gateway.py` + `tests/test_platform.py`：本地模拟服务的适配器机制测试，
  文件内明确标注**不是 peri-loom 行为验证**。

未删除任何内部实现依赖：技能从未导入平台内部模块；原生基线使用的是 TursoDB fork
（与被测 peri-loom 相同提交），这是设计要求的原生对照，不属于平台内部路径。

## 3. 本轮执行证据（可复核）

### 3.1 工具与正确性测试

```bash
python3 -B -m unittest discover -s .claude/skills/e01-benchmark/tests -v
```

28 项通过（约 6 s），含：合同/阶梯校验、四族 oracle 与时间并列边界、完整 OFFSET 与
keyset 翻页（重漏检测）、资源上限、formal 拒绝生成、Unknown/错误/缺失/摘要错配不被掩盖、
离线 CLI 全链路与拒绝覆盖、原生 smoke（literal 与 parameter 两种绑定各一次）、
平台客户端机制（信封违规、传输失败不重试、会话停用、预派发拒绝无时间戳、凭据不回显）、
平台运行器全链路（目标返回错误行时必须判 FAIL_STOP 且不伪造成功）。

其中一次真实缺陷：keyset 参数模式的占位符与绑定顺序不一致（会把 group_id 绑成时间值），
由单元测试发现并修复（`scripts/sql.py`），修复后 parameter 模式端到端 oracle 通过。

### 3.2 原生 smoke（真实 TursoDB，本地持久库）

引擎身份：`prototypes/p0/turso` 子模块当前检出 `24a3a4609683b73935888f8bc30b47727d100aa8`
（分支 `perf/blob-constructor-budget`，2026-10-15）。注意 `CLAUDE.md` 记录的 gitlink 是
`d0c23d6a…`，两者不一致；**本技能没有移动 gitlink**，正式运行前必须重新确认并冻结引擎 commit。

- 96 行 / 118 次操作（含四族各自完整的首页→空尾页遍历）全部 `oracle` 通过，
  `correctness=ORACLE_VERIFIED`，`verified=118/118`。
- 两进程（import / query）证明数据可从磁盘重开读取；`sync_mode=full` 记录在证据中。
- 结论仅限工具与 oracle 正确性：`performance/overload_safety/capacity = NOT MEASURED`，
  `scale_coverage = SMOKE_ONLY`。
- 输出文件权限：脚本写出的证据/计划/结果均为 `0600`；`fixture.db*` 由引擎 IO 创建为
  `0644`，但运行目录始终以 `0700` 创建，目录权限已隔离其内容。

### 3.3 原生计划证据（EXPLAIN QUERY PLAN，118/118 成功）

```
16x  SEARCH e01_records USING INTEGER PRIMARY KEY (rowid=?)
16x  SEARCH e01_records USING INDEX e01_records_group_time_id (group_id=? AND created_at>=?)
86x  SEARCH e01_records USING INDEX e01_records_group_time_id (group_id=?)
```

全部为索引检索，无全表扫描。注意：keyset/offset 的谓词只下推到 `group_id=?`，
`created_at > ?` 未形成范围约束——深分页仍需从组起点走索引。这是后续优化与
E01 深度阶梯的直接输入，不是性能结论。

### 3.4 平台路径

**未对真实 peri-loom 执行任何请求**：用户尚未提供隔离端点与凭据。
所有平台侧结论目前只覆盖适配器机制（模拟服务），不覆盖平台行为、性能或容量。

## 4. 能力缺口与后续实现

1. 流式生成与分批导入（正式规模前置）；当前生成器仅 smoke（≤2048 行、全量驻留）。
2. 导入速率标定（先 1 M 行），据此确认 8× 档预算。
3. 开环调度、并发/到达率阶梯、目标进程资源采集、发生器饱和度检测。
4. 平台侧待验证假设：DDL 经 `/v1/sql/execute`、会话内事务、批量 INSERT 的
   `sql_limit`/`sql_budget_exceeded` 边界、`count(*)` 计数、只读会话角色。
5. 引擎内部扫描量/缓存驻留/冷缓存证据：公开 API 不发布，保持 `NOT MEASURED`。

## 4bis. 2026-10-16 真实本地平台证据（用户授权方案 A + 本地测试环境）

**环境**：`dev.py --tls --no-build --backend local`，新唯一 run 目录（`~/.peri-e01/stack-*`）、
空闲端口 `127.0.0.1:19443`、专用分支；凭据只经环境变量从 `connection.json` 注入，未打印、未落盘到工件。
引擎身份：仓库 HEAD `1faafd05…`（工作树含并行工作流修改，22 个文件）、turso 子模块
`24a3a460…`（与原生 harness 相同）、平台二进制 sha256 `869b75df…` 前 16 位。
**注意：平台侧 revision 未冻结（工作树被并行修改），原生基线为干净子模块 → 严格来说两侧不完全可比。**

**公开协议真实 smoke（通过）**：96 行、118 次操作，`ORACLE_VERIFIED 118/118`，
四条轨迹分页 `complete`，DDL/`BEGIN`/`COMMIT`/批量 INSERT/`count(*)`/会话 close 全部经
公开 `/v1/sql/*` 完成；总耗时 7.3 s。

**有界导入标定**（共享机器，另有并行工作流在跑测试）：

| 项 | 实测 |
| --- | --- |
| 导入速率 | 3,363 行/s（250 行/批，50k 行提交点；含 HTTP 往返） |
| 生成速率 | 207k 行/s（流式，非瓶颈） |
| 存储成本 | **362 字节/行**（300k 行 → +108.7 MB；逻辑 173.33 B/行 → ≈2.1×） |
| 单语句 SQL 上界 | 61,257 B 通过 / 71,417 B 被拒 `sql_limit` → 约 64 KiB |
| 提交点 | 50k 行提交 4.9–9.3 s；第 5 次提交跨过网关请求超时 → `outcome_unknown` |
| Unknown 处理 | 会话停用、回滚被拒并记录、停止导入、保留证据、未重试（符合契约） |
| 2,000 行完整标定 | `count(*)` = 2000 匹配，8/8 抽样点查与 `row_at` oracle 一致 |

**未完成**：300k/100k 完整跑未收尾（前者被提交超时 Unknown 截断，后者因新建分支不可用返回
`forbidden`）；新建数据库操作返回 `operation_incomplete`，未再追查。

**方案 A 阶梯预算（由上表外推，共享机器噪声，非容量结论）**：导入 8M≈40 min / 16M≈79 min /
32M≈159 min；存储 2.9 / 5.8 / 11.6 GB；生成可忽略。整条阶梯单目标 ≈6 h（含每档验证与轻量测量轮次），
两目标 ≈12 h。建议先只跑 8M 首档，硬停止 2 h / 5 GB，再决定是否继续。

**正式运行前必须冻结的部署参数**：`--sql-max-duration-ms`（默认 5000）与
`--dev-sql-timeout-seconds`（默认 30）会直接决定深分页查询是否被平台预算拒绝；
原生基线没有对应预算 → 两侧可比性必须显式记录。

## 5. 续接入口

保持当前负责人。获批准后按序：冻结批准版本 → 实现第 4 节 1–3 项 →
在用户提供的隔离端点做平台 smoke（同 SQL、同数据、同 oracle）→
核验真实规模与工作集覆盖 → 正式两目标轮换阶梯。
不执行全局 cache 清理、不接外部 fixture、不自动重试 Unknown、不绕过端口/身份边界。

## 4ter. 2026-10-16 修正：精确核对既有证据（未重跑、未重启平台）

### 行数口径（禁止把 INSERT ACK 当 durable commit）

| 运行 | attempted | INSERT-ACK | COMMIT-ACK | durability 未定 | 独立回读 |
| --- | --- | --- | --- | --- | --- |
| `cal-small-run2`（2,000 行） | 2,000 | 2,000 | 1,000（记录点）+最终提交未单独记账 | 0 | **2,000，`count(*)` 相符，8/8 抽样点查相符** |
| `cal-1m-run`（1M 配置） | 1,000,000 | 0（首条 INSERT 即 `sql_limit`） | 0 | 0（ROLLBACK 有回执） | 未回读 |
| `cal-1m-run2`（1M 配置，300k 后停） | 1,000,000 | 300,000（1,200 批 × 250） | 250,000（5 个提交点：50k/100k/150k/200k/250k） | **最多 50,000**（`commit-5` 无回执，`outcome_unknown`） | **NOT MEASURED**（会话停用，探针阶段未执行；平台已停止，未重启回读） |

- 300k 运行的 `rollback_failed:session_unusable`：回滚与 close 都没有回执 →
  这部分事务既未证明提交也未证明回滚；不得声称"已回滚"。
- 唯一有独立回读证明的规模：**2,000 行**。300k 与 1M 均为 NOT MEASURED（durability 未证明）。

### 速率口径

- `rows_per_second_import = INSERT-ACK 行数 / 仅批量 INSERT 请求的墙钟`：
  300,000 / 89.2136 s = 3,362.7；2,000 / 1.3559 s = 1,475.1。
  分母**不含**生成（1.4495 s）、提交（30.4461 s，5 次 COMMIT+BEGIN）、建表建索引（未计时）、
  会话 open/close、探针与校验；同时**不含**失败的 `commit-5` 耗时。因此它不是端到端导入吞吐。
- 含提交的派生值（仍不含 DDL/open/close/校验）：300,000 / (89.2136+30.4461) ≈ 2,506 行/s，仅作派生参考。

### 磁盘与预算（撤回）

- `storage_growth_bytes` = 108,685,158 B：范围是**整个栈运行目录**
  `/Users/konghayao/.peri-e01/stack-20261016T082945Z`（authority 对象、worker 缓存、平台状态、
  日志、密钥），时刻为运行开始前 vs 失败退出后；其中含**未决事务数据**与非数据文件。
  → 362 B/行是**单点、含义混杂的观测**，不是稳定放大比，不得用于外推。
- `disk_free_delta_bytes` = +332,550,144（自由空间反而增加）→ 共享机器下不可用。
- **撤回**此前"8M ≈ 40 分钟 / 2.9 GB"的预算：它建立在该单点比值与 INSERT-ACK 速率上，
  未包含提交、DDL、校验与未知事务，属不可靠外推。重新给出预算前必须先有
  **一次完整、经回读证明、分阶段计时（DDL/INSERT/COMMIT/校验分开）**的导入。

### 平台侧未决事项（不重试）

- `peri create --database local_default/e01_cal/main` 与 `.../e01_cal1m/main` 均返回
  `operation_incomplete`（各带回执 request id），随后 `status` 与 `operations` 查询均为 `forbidden`。
- `peri branch ... --name e01cal2` 的调用输出被丢弃、结果未记录；该分支随后 `open` 返回 `forbidden`，
  故两次后续标定（`cal-300k-run`、`cal-100k-run`）只留下空目录、无报告。
- **可能有部分创建残留**（未回读、无法排除）；按要求**不再探测、不重试**。
  这两个目录名保留为空尝试的痕迹：`control-20261016T082844Z/e01/cal-300k-run`、`.../cal-100k-run`。

### `--reset-fixture` 破坏范围（已收紧）

- 只发出一条语句：`DROP TABLE IF EXISTS e01_records`（技能自有、已批准的 E01 表），不触碰其他对象。
- 仅在本技能标定开始时显式启用；**失败、Unknown、回滚路径一律不重置、不重放**。
- 新增硬性守卫 `reset_guard`：库标识中必须包含 `e01`，否则拒绝
  （`reset_requires_dedicated_e01_fixture`），避免误伤共享/业务库；已有单测固定该语句与守卫。

### 证据路径与凭据（不输出内容）

- 证据根：`/Users/konghayao/.peri-e01/control-20261016T082844Z/e01/`
  （`smoke/`、`cal-small-run2/`、`cal-1m-run/`、`cal-1m-run2/`、配置 `cal-*.json`）。
- 控制目录：`/Users/konghayao/.peri-e01/control-20261016T082844Z/`（launcher 日志、pid、平台二进制）。
- **含凭据的路径**：`/Users/konghayao/.peri-e01/stack-20261016T082945Z/connection.json`
  （`project_api_key`、`global_api_key`）与 `.../stack-20261016T082844Z/keys/`（dev CA 私钥）。
- 已按字节核对：`control-*/e01/` 下**没有任何**证据文件包含凭据（检查方法：读取 connection.json
  的两个凭据字符串后逐文件字节匹配，只输出文件名，未打印内容）。
- 清理必须逐路径单独确认；不得笼统建议删除 `~/.peri-e01`，也不得删除上述任一目录而不先确认。

### 运行时冻结摘要

- 仓库 HEAD `1faafd05d8c27f08da96a979ccff0e03052924d2`（工作树含并行工作流修改，未清理、未覆盖）。
- turso 子模块 `24a3a4609683b73935888f8bc30b47727d100aa8`（与原生 harness 相同）。
- 平台服务二进制 `prototypes/v1/target/debug/peri-loom-v1` sha256 `46bcc143524d26497ccd393b92d4198697eb9022ec5d1d75054e50014dc58400`。
- 本地网关二进制（我私有构建，不在仓库内）sha256 `99e3e9aaccd20c6e14677f5e5d328465c4f1706bce71ac35ec714627f1e7ef0b`。
- 原生 harness 二进制 sha256 `fa6a0ad4b501b7117f2d16c32e13f462255a7bb4bb4cfbe4c74d4722ec4cb9ce`。
- 结论：原生基线与平台共享同一引擎提交；**平台侧 peri-loom 源码 revision 未冻结**（并行脏树），
  因此两侧"完全可比"不成立，须在正式结论中显式标注。

### 正式 E01 查询能力（本轮补齐）

- 新增 `scripts/run_platform_queries.py`：只读、公开 API、可选并发（≤4）与派发速率整形、
  计划时间/派发滞后、Unknown 即停止派发且不重试、事件脱敏（不落原始行）。
- 有界 oracle：点查 O(1)；列表/分页用有界堆流式窗口，深度上限 100,000、单次扫描上限 5,000,000 行，
  超预算的操作执行但判定 `not_checked`；内存上界为 `depth_limit+page_size` 行，不驻留数据集。
- 判定分离：`ORACLE_VERIFIED_SUBSET` / `INCOMPLETE` / `FAIL_STOP` / `NOT MEASURED`；
  `performance`、`capacity` 恒为 `NOT MEASURED`，`scale_coverage` 为 `NOT VERIFIED`，`slo` 标 `NOT_APPROVED_EXPLORATION_ONLY`。
- 仍缺：8M 规模的真实执行（未运行）、开环到达率的高精度整形、每族分位统计的正式采样、
  以及"数据规模已核验"的证据链。


## 6. 2026-10-16 收尾修正（软件层）

### 6.1 删除路径移除

`--reset-fixture`、`RESET_STATEMENT`、`reset_guard` 与全部 `DROP` 语句已从技能中删除；
"名称含 e01" 不是所有权或删除授权。既有对象现在直接 fail closed（`CREATE` 不带 `IF NOT EXISTS`）。
第 4ter 节中关于 `--reset-fixture` 的段落**已作废**（当时将其描述为受守卫保护的重置手段）。
单测固定：全部脚本不得出现 `DROP TABLE`、不得出现 `reset-fixture`，两条 DDL 必须为裸 `CREATE`。

### 6.2 可扩展 oracle（替代"逐请求扫描 + not_checked"）

新增 `scripts/oracle_manifest.py`：一次流式扫描（按组索引命中，内存上界为各操作
`depth+limit+1` 行之和）算出**全部计划操作**的期望行；`index_list` 只保留最小 limit 行，
与深度无关。深度超预算直接拒绝构建，不产出半成品。
清单含模型版本、config 摘要、扫描量、算法与内容摘要；`verify_manifest` 先校验清单自身摘要
（防篡改），再用重算结果比对来源。小模型上与暴力 oracle 穷举交叉核对一致（2/4/8 每族，0 差异）。

### 6.3 统计（测量与 SLO 分离）

新增 `scripts/stats.py`：每族 `planned_ops`/`reported_ops`/`missing_ops`、分类计数、
`service_ms`（dispatched→completed，覆盖请求至完整结果）、`end_to_end_ms`（planned→completed）、
`dispatch_lag_ms`；分位采用最近秩，样本不足（p95<100、p99<1000）显式 `sufficient=false` 且值为 null。
未获批 SLO 时 `slo.approved=false`、`comparison=null`，只输出探索测量。

### 6.4 正式导入入口与计数定义

新增 `scripts/run_platform_import.py`（全量、不截断，仅供正式执行；本轮**未运行**）。
计数定义：`planned_rows` = 配置目标行数；`attempted_rows` = 已实际派发的语句覆盖行数
（含失败语句）；`insert_ack_rows` = 收到回执的 INSERT 覆盖行数；`commit_ack_rows` = 已收到回执的
提交所覆盖的最大行数；`readback_rows` = 结束后 `count(*)` 回读值（null 表示未回读）。
安全预算：单语句 ≤61,257 B（实测上界，硬守卫）、`--min-free-disk-bytes` 起始与逐批检查、
`--max-seconds` 到界停止；失败/Unknown 后不自动恢复、不重放、不尝试回滚猜测。

### 6.5 状态与证据口径修正

- 撤回"8M ≈ 40 分钟 / 2.9 GB"：该外推基于失败的 300k 运行与混杂的单点磁盘观测，不作承诺。
- 速率口径已更正为 INSERT-ACK 吞吐（分母仅批量 INSERT 墙钟），非端到端导入吞吐。
- 真实高压（8M/16M/32M 导入与查询）**尚未运行**；技能可完整准备→生成清单→导入→测量→结论。
- 缺 SLO 批准不妨碍实现与报告统计；本技能不因未获批 SLO 而隐藏已有样本。


## 7. 2026-10-16 并发默认与对比能力（用户已批准）

- **并发默认改为 100 真实会话**。依据由用户提供并核对：
  PostgreSQL 官方 `runtime-config-connection` 文档说明 `max_connections` 通常为 100
  （https://www.postgresql.org/docs/current/runtime-config-connection.html）；
  `pgbench` 文档说明其客户端默认并发为 1
  （https://www.postgresql.org/docs/current/pgbench.html）。
  用户通过 Ask 明确批准"100 并发会话（推荐）"，并区分了三个概念：
  100 是**连接规模参考**，不等于 active queries，也不等于 pgbench 默认。
- 语义：100 是真实独立会话，每会话唯一 worker 串行独占 + 显式关闭；`--concurrency 1` 保留；
  请求数少于并发时只开等同请求数的会话；准入不足时报告 requested/opened/peak/open_failures
  与 `partial_admission`，不静默降级。未修改服务端连接配置或项目 quota。
- 计时：新增 `scripts/timing.py`（互斥阶段计时，禁止重叠相加）与阶段
  `prepare/open/count_readback/query/oracle_compare/close` + `total_wall_ms`/`unaccounted_ms`。
- 效率：每族 `success_ops_per_second`（分母固定为查询阶段墙钟）、`throughput.overall`、
  `per_session_efficiency`；`resources`/`cpu_seconds_per_operation` 无实测即为 null/NOT MEASURED。
  导入侧新增 `durable_rows_per_second`：仅当 `readback == planned == commit_ack == insert_ack`
  时计算，否则 null 并给出原因。
- 对比：新增 `scripts/compare.py` 与 `e01.py compare`；不可比时不产出倍数，
  跨目标恒标 `cross_boundary`。
- **未运行**：本轮只改工具与单测（全部走 mock），未启动真实本地栈、未做 100 并发实测、
  未运行 8M 及以上导入或查询；无常驻进程遗留。原 800 万等规模的执行仍未发生。
