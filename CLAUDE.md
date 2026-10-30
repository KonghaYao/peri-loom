# CLAUDE.md

面向在本仓库工作的 AI agent。项目介绍、目录结构、环境安装、本地启动见 `README.md`；
V1 的运行与安全边界见 `prototypes/v1/README.md` 和 `prototypes/v1/platform/README.md`。

## 协作与汇报约定

- **观察者不是数据库专家**：用户没有数据库与调优经验，不负责裁决技术要点和关键实现决策。
  这类问题应依据仓库证据、领域约束与测试结果自行判断并执行；只有目标、授权范围、
  不可逆结果或对外承诺存在歧义时才询问。
- **汇报百分比与关键信息**：任务完成度、测试通过率、性能与容量变化用百分比或明确数字表达；
  只陈述影响决策的事实、证据位置和剩余风险，不复述过程细节。

## 核心工程原则 v1.8

1. **架构与领域优先**：编码前明确目标、领域边界、职责、依赖方向和数据流。复用型抽象等第二个真实用例再提取；协议隔离、依赖反转和确定的可替换边界可在首个用例建立接口，须说明当前需要。
2. **模块封装复杂度**：接口精简、稳定，文件按职责拆分；规模限制及验证遵循 `STD-SIZE-001`。
3. **显式表达领域语义。** 命名与类型表达职责、身份、状态和错误；所有权和副作用可见，避免字符串约定与隐式共享。
4. **边界与数据流清晰**：协议、领域、持久化与视图模型各守边界；在边界校验和转换，避免跨层共享可变状态。
5. **保留维护上下文**：记录决策原因、影响与风险，临时方案注明移除条件；技术债务关联任务，关键决策同步文档，不留无上下文的 `TODO`。
6. **删除优于兼容**：内部重构删除过时实现，不新增兼容层、deprecated shim 或双写；对外兼容义务按协议评估。
7. **业务规则单一权威**：同一领域规则只维护一份实现；允许多个存储后端和协议 adapter 封装适配差异，共用业务规则与契约。
8. **在授权范围内主动闭环。** 依据仓库证据处理常规选择；澄清目标、权限或不可逆结果的歧义。异议须说明代价和替代方案。
9. **快速胜利法**：
   1. **优先广度，而非深度**：优先完成简单的，可以并发的任务或者测试，提高广度覆盖面。如果并发允许，同时研究比较难的问题，先出规划，后续看看如何执行，提升并发度。
   2. **优先完成，而非完整**：先验证用户可观察的行为，再按风险补齐回归、失败和生命周期测试；。
   3. **杜绝过早、过度测试与验证**
10. **保持热点路径中对外部资源消耗的敏感性**：考虑 CPU 是否在计算中有问题，内存、数据库查询等热点场景中的性能
11. **修复一类问题，而不是一个问题**
12. **暴露异常优于简单掩盖**：如果没有任何说明，默认的错误处理方式都是抛出，并在日志中留下记录。
13. **数据库表结构须经用户明确批准**：变更前说明表及结构的合理性，由用户裁决。
14. **日志/通知信息层级化**：日志标准化层级，错误留痕

## 项目质量红线

1. 底层 tursodb sql 兼容性一定要保证好
2. 平台对外接口的 turso serverless sdk 兼容一定要做好
3. 平台自身的 serverless 能力，diskless 能力一定要保证好

> v1 已包含默认关闭的实验性 Hrana HTTP/WS 适配；E1（HTTP v3 写 cursor abort 无可确认回滚证明）是显式
> known-gap，默认矩阵每轮执行 26/27、2 轮共 52 runs + 2 个 skip。已测试的 SDK/协议范围及未证实的边界见
> `prototypes/v1/platform/README.md`，不要宣称完整 Hrana 兼容。

## 命令

```bash
# Rust 数据面（prototypes/v1 是独立 workspace，仓库根没有 workspace）
cargo check  --manifest-path prototypes/v1/Cargo.toml --locked
cargo test   --manifest-path prototypes/v1/Cargo.toml --locked
cargo fmt    --manifest-path prototypes/v1/Cargo.toml -- --check
cargo clippy --manifest-path prototypes/v1/Cargo.toml --locked --all-targets --no-deps -- -D warnings

# Go 平台（Gateway / Broker / Controller）
cd prototypes/v1/platform && go build ./cmd/peri-platform ./cmd/peri
PERI_RUST_BINARY=<Rust 二进制绝对路径> go test ./...                 # 需先构建 Rust
PERI_RUST_BINARY=<Rust 二进制绝对路径> PERI_P6_VERIFY_RACE_CLI=1 go test -race ./...
cd prototypes/v1/controller && go test ./...

# 全栈本地启动（Authority + 两台加密 Worker + Controller + Gateway + Key Broker）
python3 prototypes/v1/platform/dev.py --run-dir /tmp/peri-local --tls

# P0 发布门禁（每次上线必需；CI 的 push/PR 与 tag 镜像发布共用）
scripts/p0-gate.sh
```

## 环境陷阱

1. **仓库根没有 Rust workspace** —— 所有 cargo 命令必须带 `--manifest-path prototypes/v1/Cargo.toml`
   （或 `prototypes/p0/Cargo.toml`）。直接跑 `cargo check` 会报 no manifest。
2. **Turso fork 是固定 submodule** —— 当前 gitlink 为 `24a3a4609683b73935888f8bc30b47727d100aa8`
   （`ee6df32` 提升到该 commit，其内容为「允许非提交准入读取内置只读虚拟表」），远端已可获取。
   首次构建前 `git submodule update --init prototypes/p0/turso`，不要自行移动 gitlink。
3. **Go 测试依赖已构建的 Rust 二进制** —— 只跑 Go 侧测试要显式给 `PERI_RUST_BINARY`；
   省略时按默认路径 `prototypes/v1/target/debug/peri-loom-v1` 查找，未构建则失败。
4. **不要清除 ownership dirty marker 强启** —— 当前 launcher 即使 stop proof 完整也拒绝复用旧 run 目录；
   dirty marker 存在时服务 fail-closed 是设计行为，不是故障。只允许在独占文件锁下前滚已验证的 commit intent
   （见 `prototypes/v1/platform/README.md`）。

## 架构边界

1. **Go 禁止**：ReadPage、LoadSegment、AppendWalFrame、CheckpointPage、构造 manifest 内部结构、
   操作 Turso 事务状态。Go 只持 desired state（placement / lease 策略），Rust 持 actual state。
2. **Rust 禁止**：全局 worker 选择、scheduler policy、集群级负载均衡、tenant allocation policy。
3. **Storage 是最终 fencing authority**：即使控制面调度错误、Worker 复活或网络分区，
   持久层必须拒绝旧 epoch 的写入。Go 可以暂时做错调度，但不能因此导致数据损坏。
4. **本地磁盘只做 cache**：从 P1 起 Worker 本地状态必须可丢弃；没有本地主库、没有持久本地 WAL。
5. **单一受信任 Authority**：只有 Authority 持有 raw `ObjectStore`；SQL 引擎只能用 branch-bound
   adapter，绝不构造替代 root。S3 凭据只注入 Authority，Worker 与 Go 平台不得接收。

## 数据与协议约定

1. **SQL 请求绝不自动重试**：丢失的回复可能已提交。Unknown 结果用业务幂等键 + durable 状态查询
   处理，不能把超时 / 关闭 / 回读当作取消证明。
2. **会话绑定 placement**：迁移会让旧 session 失效，客户端必须显式重开，不要透明重放事务。
3. **JSON 保留原生整数字面量**：Go 侧用 `json.Number`，TS / 浏览器用 `bigint`；
   NDJSON 是已物化行的有界流，不是增量执行 cursor。
4. **不要禁用 TLS 验证**：外部监听必须 TLS；开发 CA 只对本地 fixture 可信。

## 测试与验收口径

- **P0 发布门禁**：`scripts/p0-gate.sh` 校验 `prototypes/p0/turso` 停在父仓库锁定的 gitlink 后，
  运行 `prototypes/p0` 全部测试与固定 Turso commit 的 SQL 冒烟。`.github/workflows/v1-baseline.yml`
  同时包含 P0 与官方 JS SDK 矩阵；tag 发布时 `v1-image` 通过 `workflow_call` 调用它，`build` 依赖
  `[check, baseline]`，P0 每个 tag 只跑一次（`v1-baseline` 的独立 push 触发以 `tags-ignore: v*` 排除
  发布 tag，避免重复）。矩阵 declared 27/27，默认每轮执行 26/27（E1 known-gap 跳过），2 轮共
  52 runs + 2 个 known-gap skips；证据经 `scripts/redact-matrix-evidence.sh` 脱敏后作为 artifact 留存。
  门禁失败时 `v1-image` 不会构建镜像、发布 manifest 或创建 release。不得在发布流程中绕过或裁减该测试集。
- **SQL 兼容标准**（2026-10-10 用户改定）：停止官方 SQLite 原始测试，以 TursoDB 自带测试为准
  （`prototypes/p0/turso` 的 `sqlite-sqltests` / `turso-sqltests`，Rust 后端）。固定 64 MiB hash
  预算、禁用快照更新；跑门禁时不要自行改预算或启用快照更新，结果不可与既有门禁数字混用。
- **SQL 门禁不可宣称全绿**：R21 与 view rename 已有修复代码和回归用例，仍须按固定 64 MiB
  hash 预算重测；分别记录 SQL 结果、约束与 plan snapshot 差异，不自行更新快照。
- **v1.1.8 修复与 Hrana 已知缺口**：R1——close 失败不再覆盖首个终止语句失败，close 失败以独立
  `cleanup_error` 表达；D1——CLI/SDK 展示脱敏 `error.message`，`outcome_unknown` 展示 `operation_id`/
  `may_have_committed`/`execution_proven`；D2——已结算错误路径关闭 session，`outcome_unknown`/
  `session_moved` 保持不透明，不重连、不重放。E1 是显式 known-gap：HTTP v3 写 cursor abort 无
  可确认回滚证明；默认矩阵每轮执行 26/27，2 轮 52 runs + 2 skips，
  `PERI_HRANA_INCLUDE_KNOWN_GAPS=1` 可执行但不保证通过，不是完整 Hrana 兼容。
- **当前交付缺口**：可能提交的 Unknown 仍会隔离命名空间，同一持久卷的*正常*重启在隔离或未决记录存在时仍被 launcher
  拒绝（clean 门禁未放宽）；原数据库在 Unknown 后已有有证明条件的隔离释放路径（`operations/decision` →
  `operations/retire-local` → `isolation/release`，也可由 `peri recover` 一步执行；由
  `dev.py --recover` 在 SQL 关闭的恢复实例中执行，需 owned worker stop proof 与 authority fencing attestation，
  且不删除 ownership marker），但“释放后正常重启”的端到端验证尚未跑通。网关 `open` 的 `outcome_unknown`
  已按不可提交操作处理：记录 `may_have_committed=false` 且不隔离命名空间、不阻止 clean restart；写语句的
  Unknown 仍按隔离处理。审计已经按 64 MiB 分段并验证连续
  hash chain，但磁盘空间耗尽前的并发安全准入预留仍未实现。
  可信 SQL 回执不再被审计失败改写为 Unknown。针对性验证：`cd prototypes/v1/platform && go test ./... -run 'TestAudit|TestDurableRecovery|TestGatewayRealAudit' -count=1`。
  最小 SQL／官方 JS SDK 矩阵已纳入 `.github/workflows/v1-baseline.yml` 发布门禁；E1 known-gap 如上，
  远端 CI 结果以 tag run 为准，不宣称完整支持矩阵。
  下一项：正常重启的 clean proof、lease fencing 与持久身份轮换；launcher 并发锁已有针对性回归。
- **性能验收尚未证明 PASS**：功能测试不等于容量或公平性保证；
  运行与安全边界以 `prototypes/v1/platform/README.md` 和 `prototypes/v1/stress/CAPACITY.md` 为准。
  单机 Issue 28 基线与实验协议见下节，仍不构成容量或 SLA 证明。

## Issue 28 性能基线（2026-10-30）

纯 peri-loom 黑盒基线：不经 peri 客户端、TS SDK 或本地 SQLite，直接驱动 Hrana v3 JSON/protobuf
pipeline 与 cursor。Harness、场景契约和复跑说明见 `scripts/perf-reliability/issue28/`；
离线回归 `python3 -m unittest scripts/perf-reliability/test_issue28_support.py`（16/16）。

```bash
python3 scripts/perf-reliability/issue28/run.py --connection-file <connection.json> \
  --container <container> --rounds 3 --warmup 1 --samples 5 --out <dir>
```

环境：Go platform HEAD `96c1372`（Linux/arm64 本地构建）+ Rust worker `v1.1.24`；隔离容器、
localhost、无 TLS；201 threads / 8,625 messages / 20,177,069 chars；配额 `max_concurrent=16`、
`max_sessions=32`、`max_result_bytes=4,194,304`。3 轮 ×（1 warmup + 5 计时），中位数：

| 场景 | 协议 | 中位数 | p95 | 响应字节 | 结果 |
|---|---|---:|---:|---:|---|
| F1.select-1 | json-pipeline | 9.00ms | 13.15ms | 312 | PASS（13ms 目标，跨轮边缘） |
| F1.select-1 | protobuf-pipeline | 7.37ms | 16.68ms | 40 | PASS |
| F1.select-1 | json-cursor | 6.26ms | 14.46ms | 252 | PASS |
| F1.select-1 | protobuf-cursor | 4.86ms | 9.01ms | 57 | PASS |
| L1.list-20 | json-pipeline | 8.83ms | 10.51ms | 10,744 | PASS |
| L1.list-20 | protobuf-pipeline | 8.09ms | 10.73ms | 4,981 | PASS |
| C1.baton-10（复用） | json-pipeline | 1.52ms | 6.25ms | 300 | — |
| H1.content-20mb | json-cursor | 636.57ms | 676.88ms | 25,162,268 | PASS |
| H1.content-20mb | protobuf-cursor | 652.43ms | 687.13ms | 23,600,883 | PASS |
| H2.projection | protobuf-cursor | 78.86ms | 87.48ms | 60,440 | —（projection 基本为 NULL） |
| H3.content+projection | json-cursor | 664.40ms | 708.42ms | 25,300,319 | PASS |
| B1.batch-20 | json-pipeline | 43.53ms | 49.21ms | 4,968 | **FAIL**（目标 20ms） |
| B1.batch-20 | protobuf-pipeline | 33.80ms | 43.56ms | 686 | **FAIL**（目标 20ms） |
| R1.limit-1000 | json-pipeline | 93.19ms | 122.64ms | 3,725,223 | — |
| R1.limit-2000 | json-pipeline | 110.08ms | 143.60ms | 143 | —（`result_limit`） |
| X1.concurrency=16 | json-pipeline | 61.30ms | 73.55ms | — | PASS（1→16 吞吐 121→251 req/s） |

- **B1 是稳定失败项**：三次正式运行 JSON 41.4/53.3/43.5ms、protobuf 39.8/51.9/33.8ms，均超 20ms；
  SQL-only 仅 3.1–3.7ms，瓶颈在平台/传输与 20 条语句的串行处理。
- **F1 处于目标边缘**：formal3 为 13.2ms（FAIL），本次 9.0ms（PASS）；不得宣称稳定达标。
- **protobuf 的稳定收益是字节数**：F1 −87%、L1 −54%、H1 −6.2%、H2 −82%；大结果延迟差异在 ±3% 内。
- **4MiB 结果预算**：1,000 行时 JSON 3.73MB 通过，2,000 行即 `result_limit`；合同预登记的 5,000 只是下界。
- **20MB 读取约 0.64s**（约 39.5MB/s）；H2/H3 受 projection 全 NULL 限制，不代表真实投影载荷。
- **并发在 8 以后饱和**：1→16 吞吐仅 2.07x；无 429、无租约/会话泄漏。
- **写路径**：端到端 20–30ms，SQL-only 11.5–16.7ms；W1.create 跨轮 19.8–38.9ms，噪声大。
- **边界**：单机 macOS + Docker VM、无 TLS，不是容量/SLA 证明；容器内存 151.9→202.6MiB
  （idle 212MiB），尚未证实是否泄漏。

### 性能实验协议（2026-10-30 用户指定）

- 每轮从 `main` 开独立分支 `perf/issue28-<round>-<slug>`；改动单独 commit，提交信息用中文全角冒号。
- 先由反思 subagent 给出方向，再由执行 agent 实现；实现后构建并部署到同一隔离靶场。
- 验收用同容器、同数据、同一轮 A/B：主指标中位数提升 ≥10%（F1 等 <10ms 场景 ≥20%），
  且其余关键场景（B1/F1/H1/X1 吞吐）不回归 >10%，并至少重复 2 次同向，才算“有提升”。
- 有提升：记录到本节并合入 `main`；无提升：`main` 保持基线，放弃该分支（保留分支与提交哈希作证据）。
- 每轮结束在新 `main` 上重新开分支。

### 实验记录

| 轮次 | 分支 / 提交 | 方向 | 受控 A/B 结果 | 决策 |
|---|---|---|---|---|
| R1 | `perf/issue28-r1-cursor-encoding` / `1112133`（merge `fa270fc`） | cursor 行编码直接写入复用缓冲 | H3 protobuf 665.29→590.66ms（−11.2%）；H1 protobuf 644.19→589.45ms（−8.5%）；H2 JSON 120.95→110.48ms（−8.7%）；H1 JSON −2.1%；H2 protobuf +3.2%（噪声） | 合入 `main` |
| R2 | `perf/issue28-r2-large-result-decode` / `2d21be5`（merge `0b28db9`） | 大结果解码按顶层 span 读取、跳过重复压缩 | H1 JSON 634.90→437.31ms（−31.1%）；H1 protobuf 600.47→400.96ms（−33.2%）；H3 JSON −26.7%、protobuf −29.7%；R1-1000 −20.1%；B1 受控 −8.0%；X1 16 并发 −12.8%（8 并发 +9.7%，噪声内） | 合入 `main` |

- R1 的 H1 主指标为 −8.5%，未达 10%；同路径 H3 达 −11.2%，且消除了“protobuf 比 JSON 慢”的回归。
  wire 字节 H1 25,162,268/23,600,883、H2 336,595/60,440、H3 25,300,319/23,635,410 与基线逐字节一致；
  全量运行 `warnings=[]`、无 429、`session_seats=0`、`active_permits=0`。按“明确收益且零回归”接受并记录例外。
- 全量运行中 pipeline/write 小场景出现 ±30–80% 波动，但该改动只触及 cursor 行编码，且 A/B 交替未复现；
  不作为本轮结论，也不据此宣称小场景性能变化。
- R2 的 H1/H3/R1 改善在 A/B/B/A 中两次同向；wire 字节 H1 25,162,268/23,600,883、H2 336,595/60,440、
  H3 25,300,319/23,635,410 不变；新增 6 个解码对拍测试通过。
- R2 全量运行期间主机 load 3.5、`StorageManagementService` 占 62% CPU，B1/X1 的全量差值被同环境 R1
  自身退化（X1 16 并发 65→99ms）证伪；B1/X1 结论以受控交替复测为准。
- 目标为 v1 耐久模式（`/v1/metrics`：`durability_source=audit_file_v1`、`operation_journal_batch_max=1`、
  `coalesced_total=0`）；B1 的 20 条只读语句仍对应 20 次审计 fsync，这是 B1 未达 20ms 的主要原因。
  后续若要改变耐久时点（批提交/延后 flush）必须先获得用户明确授权，不能作为普通性能轮次合入。
- R2 的已知测试失败 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode` 与
  `TestRegistryEntriesStillExist` 在 `main` 上同样失败，不是 R2 引入。

## 代码约定

- 提交信息用中文全角冒号：`feat(v1)：…`、`fix(authority)：…`、`docs(capacity)：…`。
- 注释讲「为什么」、不复述「是什么」；v1 现有代码几乎无注释、文档为英文，新增注释沿用所在文件风格。
- 错误不得静默吞掉：无说明时默认抛错并留日志（原则 11）。

## 凭据与密钥

- `dev.py` 产出的 `connection.json` 含私有测试 API Key 与开发 CA 路径，**不可公开**、
  不可写入文档或日志。
- 抓取或回显任何含 `Authorization` 的输出时，替换成 `Bearer <REDACTED>`。
