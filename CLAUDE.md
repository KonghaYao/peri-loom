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
- **失败启动后的 authority 封印恢复缺口（2026-11-01 观测）**：一次 startup 失败可留下 clean marker +
  dirty backend seal；随后 `dev.py --recover` 会因 clean marker 放行，但 authority 在 `startup.rs:101`
  先写入 dirty marker，再在 `seal.rs:99` 因 dirty seal 拒绝启动，marker 随之保持 dirty。此后 `--recover`
  要求 S05 evidence；无 evidence 时 recovery plan 为 `Unclassified`，`--reconciled` 对 dirty seal 也不可
  接管；`recover_interrupted_startup` 又因 `dev.py:1155` 的「teardown_completed + services_started」
  短路返回 False，于是该卷没有受支持的就地恢复路径。本次先保留旧卷 `peri-loom-issue28-data` 取证，随后在
  独立新卷重建测试环境；清理阶段两个测试卷均已删除，证据保留在 `/tmp/peri-issue28/recovery-inspect` 与
  失败记录中。修复需重评 marker/seal 写入顺序或失败回滚，属耐久时点变更，未实施。
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
| R3 | `perf/issue28-r3-pipeline-row-encoding` / `44c14be`（未合并） | pipeline 大结果直接编码 | R1-1000 JSON 69.33→63.25ms（−8.8%）；protobuf 65.52→66.97ms（+2.2%）；编码层微基准 JSON −42%、protobuf −44% | 未达 10%，不合入；保留分支 |
| R5 | `perf/issue28-r5-worker-read-batch` / `d739a3d`（merge `e31543a`） | pipeline 连续只读 execute 合并为 worker 批命令（≤8 条/批） | B1 配对中位 json −10.5%（4/4 同向）、protobuf −23.4%（4/4）；候选 B1 首轮 15.2–16.0ms（<20ms）；F1/X1/H1 无 >10% 回归 | 合入 `main` |
| R6 | `perf/issue28-r6-cursor-page-transcode` / `e062d0b`（merge `fffd305`） | 只读 cursor 页 span 校验后直接转码 Hrana JSON/protobuf，去掉页级物化 | H1 json 474.0/465.7→333.2/331.9ms（−28.7%~−29.7%）；H1 protobuf −34.1%~−35.0%；H3 json −30.4%~−30.7%、protobuf −33.0%~−34.3%；W1/B1/X1 健康环境复测无候选回归 | 合入 `main` |
| R7 | `perf/issue28-r7-cursor-describe-elide` / `67e5d4b`（未合入） | 只读 cursor 首步省略 `describe`，写语句由 worker 返回新错误后回退 | 候选无法构建（见 R9 环境事件，宿主仅 8.1 GiB 可用而镜像构建需 6–8 GiB）；改用 `/v1/metrics` 的 `action_latency_ns_total` 直接实测被省略的 `describe`：均值 0.391ms/次（101 次），占串行单查询 16.4ms 的 **2.4%** | 不合入；分支与提交保留作证据 |
| R8 | 无代码改动（归因轮） | 拆解 11–12ms 小查询的 8 次屏障 | 4 次 audit + 4 次 journal fsync；8 次屏障语义上全部必需，严格口径可省 0；即使 8 个屏障全消失仍 5.5ms vs sqld 3.7ms ≈ 1.49× | 小查询差距是耐久模型的价格，不再做屏障削减 |
| R9 | 无代码改动（测量轮） | 单库并发小查询的扩展性与错误分类 | conc=8 时 143–161 qps / p50 48.7–51.5ms，`operation_journal_coalesced_total` 恒为 0、`batch_max=1`、`queue_max=1`；同机 sqld conc=8 为 1510 qps / p50 4.83ms | 单命名空间无合并是结构性上限；并发下另有 502 误分类缺陷 |
| R11 | `perf/issue28-r11-session-record-outcome` / `6f8b35d` | 会话 open/close 只写结果记录，去掉派发前的 intent | 同镜像同卷交错 A/B（每档 A,B,B,A 共 4 样本/侧，每样本 320 次查询）：串行 32.2→41.3 qps（p50 30.1→24.1ms，**+28%/−20%**）、conc=8 183.9→233.6 qps（**+27%**）、conc=32 205.1→374.9 qps（**+83%**）、conc=64 256.8→504.8 qps（**+97%**）；journal appends 4.02→2.02、syncs 4.04→2.02 每查询，conc=64 时 journal+audit 屏障合计 0.82→0.25 次/请求 | 合入 `main` |
| R12 | `perf/issue28-r12-completion-journal` / `c43d48c` | 统一完成日志：命名空间 fence 在等屏障前释放，完成记录按审计顺序提交 | v2 卷（`completion_journal_v2`）同二进制交错 A/B（2 轮 × 4 档 × 4 样本/侧）：串行 50.2→53.8 qps（+7%）、conc=8 115.7→177.4（**+53%**）、conc=32 193.6→443.6（**+129%**）、conc=64 140.4→546.2（**+289%**）；`journal_sync` 0.74–0.77→0.10–0.17 次/请求，`coalesced` 40–92→1000–1120，`batch_max` 3–4→16–32；同窗口 v1 卷 r11 vs r12 为 +1.5%~+5.4%（无回归） | 合入 `main` |
| R13 | 无代码改动（测量轮） | 复核 R12 遗留的 conc=8 缺口，改用多采样协议 | 两格式各 10–15 样本（同一容器连续 5 次 320 查询 × 2–3 组，取中位）：v2 vs v1 串行 38.35→45.91 qps（**+20%**）、conc=8 278.77→285.38（+2.4%，v2 分布 250–297、v1 112–322）、conc=32 403.44→530.20（**+31%**）、conc=64 488.00→658.42（**+35%**）；R12 的「conc=8 −25%」不复现 | 仅记录，无代码改动 |
| R14 | 无（原型已回退：sha256 `37261d5a`） | 统一完成日志的审计 drainer 不再在循环内等屏障（多批流水线） | v2 卷同卷 A/B（r12 vs r14，2 组 × 5 样本/侧，每样本 320 查询）conc=64：中位 574.08→428.50 qps（**−25%**）、p50 100.1→136.8ms | **失败，已回退**：更深的未决提交栈让 `settleOperations()`（在全局 mutation 状态锁上按序等待所有未决提交）阻塞更久，抵消了批次间重叠 |
| R15 | `perf/issue28-r15-state-snapshot-reads` / `48c42b5`（不合入，留证） | 认证/路由元数据读改为原子快照，不再取全局状态锁 | v2 卷同卷 A/B（r12 vs r15，2 组 × 5 样本/侧，每样本 320 查询）：c64 中位 569.11→479.31 qps（**−15.8%**，两轮同向）、c32 393.88→463.99（+17.8%，cycle1 +7.4%/cycle2 +34.3% 不稳）、c8 163.18→167.05（+2.4%）、c1 41.91→42.34（+1.0%）；状态锁获取 10.0→2.8 次/请求 | **失败，已 reset 出 main**：低并发锁等待确实塌缩（c1 0.066→0.0004ms/请求、c32 4.22→2.20ms），但 c64 等待被拉长（首样本 7.04→9.88ms、峰值 261.6ms、p95 +13.8%），吞吐反降 |
| R16 | `perf/issue28-r16-settle-off-lock` / `9316044`（不合入，留证） | 落定等待移出全局状态锁：写者只回收已确认前缀、读者锁外等尾部提交 | v2 卷同卷 A/B（r12 vs r16，2 组 × 5 样本/侧，每样本 320 查询）：c64 中位 594.08→478.99（**−19.4%**）、c32 435.07→334.32（**−23.2%**）、c8 210.88→150.10（**−28.8%**），三档同向退化 | **失败，已 reset 出 main**：锁外等尾部提交 + 反复复查的路径比锁内 settle 更贵；D1 方向在两种实现（R14 流水线、R16 锁外等待）下都退化 |
| R17 | `perf/issue28-r17-confirm-durable-prefix` / `6a98baa`（不合入，留证） | 落定只发布自身耐久前缀：写者确认到自己的 revision 就返回，不再等队列尾部屏障 | 每侧全新 v2 卷 A/B（r12 vs r17，4 组 × 3 样本/侧，每样本 320 查询，conc=64）：r12 中位 510.0 qps、r17 507.3（**−0.5%**），逐组配对中位 −0.3%（−4.4%/+17.0%/+0.9%/−1.6%）；p50 115–132→110–117ms；锁等待/请求 7.74→7.71ms | **中性，已 reset 出 main**：状态锁累计等待 11.34→8.54s（−25%）、单次最大 20.66→16.43ms，但 c64 吞吐不变——状态锁等待不是当前吞吐上限 |
| R18 | `perf/issue28-r18-offlock-completion-maintenance` / `383818e`（merge `a4f11f6`） | 统一完成日志的 maintenance 移出状态锁：settle+捕获在锁内，归档、编码、快照落盘、fsync 与截断在锁外 | 每侧全新 v2 卷 A/B（r12 vs r18，4 组 × 3 样本/侧，每样本 320 查询，conc=64）：逐组中位 +6.3%/+14.2%/+19.0%/+8.9%（4/4 同向），配对中位 **+11.5%**，全体 12 样本中位 452.86→488.71 qps（**+7.9%**）；p50 128→115ms；锁等待/请求 8.74→6.64ms（−24%）；compaction 次数与墙钟不变 | 合入 `main`：收益来自把约 10% 墙钟的维护移出状态锁；与 R15/R16 对照说明状态锁的**持有时间**才是限制项 |
| R19 | `perf/issue28-r19-compact-budget` / `755829a`（不合入，留证） | 操作日志压实预算 256→1024，摊薄每查询分摊的快照/归档/屏障固定成本 | 每侧全新 v2 卷 A/B 两次：run1（r18 先、r19 后，4 组 × 3 样本/侧）逐组中位 −3.9%/−7.9%/−4.9%/−1.3%，整体 493.25→491.54（**−0.3%**）；run2 交换顺序（3 组 × 3 样本/侧）整体 481.59→480.21（**−0.3%**）。机制：compactions 5→1/样本，压实墙钟 83.0→29.4ms（快照 30.1→7.4、fsync 7.9→2.2），appends 1284 不变 | **中性，已 reset 出 main**：压实工作量减少约 54ms/320 查询（≈8% 墙钟）但吞吐两次测量都持平——writer 独占的压实时间不是剩余吞吐上限 |
| R20 | `perf/issue28-r20-batch-audit-submit` / `4217994`（merge `0dfbed9`） | 完成日志 drainer 一批记录共用一次操作视图锁提交（逐条仍是独立 commit、独立屏障、独立拒绝） | 每侧全新 v2 卷 A/B 两轮（c64，320 查询/样本）：run1（r18 先，4 组 × 3 样本/侧）整体中位 482.37→526.89 qps（**+9.2%**，逐组 +13.6/−3.6/+0.9/+22.6%，配对中位 +7.3%）；run2 交换顺序（3 组 × 3 样本/侧）整体 474.13→555.07（**+17.1%**，逐组 +22.6/+7.9/+21.2%）。机制：屏障/样本 150→126（−16%）、锁等待/请求 7.19→6.08ms、p50 124.8→110.3ms | 合入 `main`：审计成员等待（观测均值约 18ms）主要来自逐条取锁排队；批量提交同时让屏障合并更多 |
| R21 | `perf/issue28-r21-audit-pipeline` / `92faad4`（不合入，留证） | 完成日志 drainer 双批次流水：先提交下一批，再结清上一批屏障 | 每侧全新 v2 卷 A/B（r20 vs r21，4 组 × 3 样本/侧，c64，320 查询/样本）：逐组中位 −25.5%/−24.3%/−20.3%/−22.1%，配对中位 −24.9%，全体 12 样本中位 569.04→431.63 qps（**−24.2%**）；p50 100.6→134.1ms；屏障/动作 0.077→0.132（+71%，每次屏障覆盖提交 10.4→6.1），appends 15408 不变；锁等待/动作 5.59→10.50ms（+88%，获取次数 9.31→9.34 不变）；journal 等待/动作 0.161→0.264ms | **失败，已 reset 出 main**：双批次在途让 `settleOperations()` 在持锁状态下遍历的未决栈翻倍，一次等待连带付下一批的屏障；且 drainer 不再被屏障限速，取到的批次更碎，屏障合并反而变差（与 R14/R16 同构） |
| R22 | `perf/issue28-r22-batch-settle` / `193be6f`（不合入，留证） | 完成日志批内结清改为「批级一次等待 + 一次 settle」，去掉每条一次取锁 | 每侧全新 v2 卷 A/B 两轮（r20 vs r22，4 组 × 3 样本/侧，c64，320 查询/样本）：run1（r20 先）整体中位 592.25→525.68（**−11.2%**）、run2 交换顺序 591.41→562.30（**−4.9%**）；两轮均值 −5.0%/−5.5%；p50 95.6→109.6ms、96.3→102.6ms；机制：取锁次数/动作 9.31→8.55（−8%），sync/动作 0.080→0.084，锁等待/动作 5.07→6.09ms | **失败，不合入**：批内逐条结清本来只在首个等待者持锁时真正等待，其余条目面对的是空未决栈；改成批级一次 settle 后持锁时刻与最年轻提交对齐，等待变成等在更晚的屏障上，收益被抵消 |
| R23 | 测量轮（无代码变更） | 请求相位 + 会话生命周期分解：定位剩余延迟在 worker 原生执行与会话建立/关闭 | 无观测器干净 A/B（r20 二进制，每侧全新 v2 卷）：c64 connect→查询→close 123.4ms/468.6 qps，同会话复用 52.2ms/**911.3 qps**；c8 48.3→16.4ms（135.8→407.5 qps）；c1 18.0→10.8ms；`SELECT 1` 的 c64 仍 97.1ms；connect+close 无查询 0.1ms（SDK 惰性）。durability 观测：`journal_prepare` encode p50 9µs、`journal_batch` 1.58ms 中 sync 1.56ms、`audit_member_wait` p50 5.55ms（队列 0.93ms） | **结论**：Go 侧审计提交已达 fsync 下限，R21/R22 证明批处理方向已用尽；c64 下 58% 延迟在会话建立/关闭，其余主要是 worker 每查询约 12ms 固定原生时间（与并发无关）。下一步必须改 Rust worker（需 linux/arm64 构建） |
| R24 | `perf/issue28-r24-worker` / `4b8f82b`（merge `b78189f`） | worker 按库镜像刷新闸门由 2ms `try_lock` 轮询改为 `Condvar` 交接，互斥与 deadline 语义不变 | 每侧全新 v2 卷 A/B 两轮（c64，320 查询/样本，逐轮交替顺序）：run1 全体中位 582.05→667.69 qps（**+14.7%**，逐轮 +11.7/+27.5/+19.4/+11.1%）、p50 99.85→90.39ms；run2 交换顺序 622.54→735.21（**+18.1%**，逐轮 +7.4/+14.2/+21.8/+16.0%）、p50 91.59→83.12ms；两轮 fails=0；机制：探针实测 `begin.refresh.lock` 1.75–1.91ms/请求，sleep 1980 次（均 2.9ms）压在 1553 次 `begin` 上，改交接后消失 | 合入 `main`：`begin`/`open`/`close`/`health` 共用同一按库闸门，闸内工作只有微秒级，2ms 轮询等于把固定睡眠放进请求路径；持闸者 Drop 立即唤醒下一个等待者，超时者把唤醒转交出去 |
| R25 | `perf/issue28-r25-auth-dedup` / `ad4d141`（merge `f294a44`） | `authorizeQuota` 复用已读授权视图，去掉每请求 2 次重复 `authorizationState`（适配器与 handler 各一次 authorize 仍保留） | 每侧全新 v2 卷 A/B（r20 vs r25a，4 组 × 3 样本/侧，c64，320 查询/样本，逐组交替）：run1 逐组 +17.4/+16.2/+3.6/+9.9%（中位 +13.0%，全体中位 690.83→744.66），run2 逐组 +3.3/+7.2/+3.5/+9.4%（中位 +5.4%，725.76→761.62）；8/8 同向，fails=0；状态锁获取 9.32→6.90 次/请求，lockwait/req 5.83→4.91ms、5.54→4.82ms | 合入 main |
| R26 | `perf/issue28-r26-credential-atomic` / `e198d33`（merge `0027a50`） | `credentialByDigest` 改读不可变摘要快照（`atomic.Pointer`），`durableFile.closed/poisoned` 改原子，认证窄读不再进状态锁 | 每侧全新 v2 卷 A/B（r25a vs r26a，4 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替，worker 二进制两侧相同）：逐轮 +5.3 / +0.8 / +13.2 / +29.3%，12 样本/侧中位 694.75→779.82 qps（**+12.2%**），均值 697.49→759.12（+8.8%）；p50 中位 88.2→77.6ms；lockwait/req 中位 5.42→4.51ms（−16.8%）；fails=0。探针：锁获取 6.90→**5.49 次/请求**，`credentialByDigest` 调用点消失 | 合入 main |
| R27 | `perf/issue28-r27-auth-scope` / `8651766`（merge `386feff`） | 请求级 `authorizationScope`：适配器认证后挂到上下文，`open`/`dispatchLocked` 的授权检查复用同一状态快照，跨外部等待（激活/placement/writer profile RPC）后失效重读 | 每侧全新 v2 卷 A/B（r26a vs r27a，6 轮 × 3 样本/侧，c64）：逐轮 +8.3 / +15.6 / −2.9 / −4.6 / +9.2 / +14.8%（4/6 同向），18 样本/侧中位 715.60→752.31 qps（**+5.1%**），均值 717.22→744.43（+3.8%）；探针：`authorizationState` 2.40→**1.40 次/请求**、总获取 5.49→4.50、lockwait/req 4.51→4.10ms；fails=0 | 合入 main（噪声大，见下） |
| R28 | `perf/issue28-r28-request-state` / `9baa692`+`7cdfd35`（merge `3e9ac39`） | 准入读（Projects+Databases）与授权读（APIKeys+Projects+Databases+Keys）合并为一次 `requestState`，请求级快照按 (keyID, project, database) 命中 | 每侧全新 v2 卷 A/B（r27a vs r28b，6 轮 × 3 样本/侧，c64）：逐轮 −5.8 / +23.9 / +19.1 / −3.2 / +4.1 / −4.2%（3/6 同向），18 样本/侧中位 810.61→829.71 qps（**+2.4%**），均值 800.39→838.61（+4.8%），p50 中位 75.1→72.9ms；探针：`requestState` 2.40→**0.40 次/请求**、总锁获取 4.51→**2.50**、lockwait/req 4.52→3.73ms；fails=0 | 合入 main |
| R29 | `perf/issue28-r29-offlock-settle` / `63ced1a`（不合入，留证） | 把 `readOperations` 的 journal 屏障等待移出状态锁：`settling`+`sync.Cond`，锁外 `awaitOperations`，最多 3 轮后回落 | 每侧全新 v2 卷 A/B（r28b vs r29a，2 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：run1 中位 799.79→623.46（−22.1%）、run2 800.29→609.72（−23.8%），合并 12 样本/侧中位 800.04→595.52 qps（**−25.6%**），均值 781.82→588.42（−24.7%）；p50 中位 75.4→95.8ms（+27%）；lockwait/req 3.5–4.1→**0.25–0.51ms**（等待确实挪走），但 sync/q 0.06→**0.10–0.12**；fails=0 | **失败，不合入**：持锁串行化正是屏障合并的限速器；等待挪到锁外后，屏障窗口内每个到达的请求都去等一份包含自己之后写入的「最年轻」未决栈，等待互相堆叠，且 journal 批次变碎使 sync 次数翻倍（与 R22 同构） |
| R30 | `perf/issue28-r30-admission-fastpath` / `54e4059`+`02dc958`（merge `83ad91b`） | `readOperations` 的锁内 settle 只对「回滚会重新加回隔离」的未决编辑执行：阻塞判据抽成 `operationBlockingFor` 与 `isolatedOperation` 共用，读操作带 `operationReadScope`（gateway 不把进行中的 gateway 记录算占位，management 算；零值保守）；未决计数不再触发等待，容量留待提交时 `validate` 兜底 | 每侧全新 v2 卷 A/B（r28b vs r30b，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：逐轮 +6.6 / +19.7 / +2.6%（**3/3 同向**），9 样本/侧中位 836.98→912.88 qps（**+9.07%**），均值 828.73→908.99（+9.68%）；p50 中位 73.3→67.2ms；lockwait/req 均值 3.56→**1.08ms**（−70%）；sync/q 0.06→0.05；fails=0。探针：每读平均 2.86 条未决编辑，其中回滚会重新加回阻塞的 **0.0%**、只改未决计数的 3.2%。回归门 c8（1 轮 × 3 样本/侧）：中位 205.07→317.97 qps（**+55.1%**），p50 中位 35.7→22.8ms，lockwait/req 1.23→**0.09ms**，sync/q 0.34→0.25，fails=0 | 合入 main |
| R31 | `perf/issue28-r31-ready-settle` / `f1a2383`+`f377ecc`+`04fb53d`（merge `96c64fe`） | 完成提交只在状态锁内确认「屏障已返回」的前缀（`settleReadyOperations`，旧版日志仍走完整 settle）；drain 入队整批期间 `holdCompletionBatch` 挡住 committer，一个审计批只付一次 fsync；`awaitOperationBatch` 先等最后一个成员的屏障、再整批结算一次视图 | 相位窗口（单样本，c64，`--sql-open-observer`）：r30 1155 qps / `audit_batch` p50 3.41ms / duty 99% / journal:audit=649:486（1.33:1）；逐步 r31a 1179 qps、3.24ms → r31b 1232 qps、2.82ms、527:527 → r31c 1271 qps、2.72ms、523:523。配对 A/B（r30 vs r31c，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：逐轮中位 +7.65 / +11.25 / +6.25%（**3/3 同向**），9 样本/侧中位 **953.39→1019.59 qps（+6.94%）**，均值 953.28→1029.13（+7.96%）；p50 中位 63.15→59.43ms；`lockwait/req` 均值 0.99→0.67ms；sync/q 0.06→0.05；fails=0。回归门 c8（1 轮 × 3 样本/侧）：中位 288.66→280.33（−2.9%）、均值 277.59→283.55（+2.1%），lockwait 0.11→0.09ms，sync/q 0.26 不变 | 合入 main |
| R32 | `perf/issue28-r32-compaction-period` / `4158f1f`（merge `71c3f3d`） | 维护压缩周期从每 256 次追加放宽到 1024 次（重放仍由周期与 8 MiB 日志上限双重约束） | 相位窗口（c64，`--sql-open-observer`）：R31c 下审计批 2.98ms 中屏障 1.97ms、提交 0.33ms、**压缩 gate 0.56ms**；窗口内 36 次维护压缩每次 11.4ms（归档 5.8 / 快照 3.3 / 同步 1.3），合计 410ms ≈ **21% 墙钟**。改 1024 后压缩 36→10 次、停顿 410→187ms，单跑 qps 1330→1473（+10.7%）。配对 A/B（r31c vs r32，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：逐轮中位 +11.8 / +23.4 / +13.1%（**3/3 同向**），9 样本/侧中位 **979.47→1169.42 qps（+19.39%）**、均值 986.05→1150.99（+16.73%）；p50 中位 62.57→49.96ms、p99 90.77→75.44ms；fails=0。回归门 c8（1 轮 × 3 样本/侧）：中位 379.34→410.60（**+8.2%**），无退化 | 合入 main |
| R33 | `perf/issue28-r33-upstream-idle-pool` / `86865d1`（不合入，留证） | 平台到 worker 的空闲连接上限按并发宽度放宽：`MaxIdleConnsPerHost` 16→64、`MaxIdleConns` 64→128 | c64 窗口内 `/proc/net/tcp` 采样（300 样本，10ms）：worker 侧端口累计 **2,036 个 TIME_WAIT**、约 1,000 次重连/秒，ESTABLISHED 16–43；改后同一采样 **10 个 TIME_WAIT**、ESTABLISHED 稳定 63。吞吐未动：配对 A/B（r32 vs r33，3 轮 × 3 样本/侧，c64）：逐轮 −5.61 / +4.14 / +3.51%（2/3 同向），9 样本中位 1131.78→1167.86（+3.19%）、均值 1138.41→1128.73（**−0.85%**）；p99 中位 80.19→74.19ms；fails=0 | **中性，不合入**：重连确实被消除，但每次重连只落在约 16% 的调用上、单次代价远小于 2.9ms 的原生等待，吞吐落在噪声内 |
| R34 | `perf/issue28-r34-compaction-4096` / `1da88f0`（merge `4c3ce8e`） | 维护压缩周期从每 1024 次追加放宽到 4096 次（同一手段的第二个台阶；重放仍由周期与 8 MiB 日志上限双重约束） | 分段探针（c64 窗口 1.690s，1514 qps）：审计 drainer **98.3% 忙**、fsync 链只 71% 忙；单批 2.90ms = 屏障 2.18ms + 提交 0.45ms + 状态锁 0.26ms，settle 与 maintenance ≈0；窗口内 10 次 `journal_compaction` 每次 18.4ms，合计 184ms = **10.9% 墙钟**。改 4096 后压缩 10→2 次、停顿 184→82ms（**10.9%→4.9%**），单次 18.4→41.0ms（亚线性）。配对 A/B（r32 vs r34，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替，候选二进制 `5efb05b2…`）：逐轮中位 −2.40 / +17.13 / +12.36%（2/3 同向），9 样本/侧中位 **1098.74→1204.33 qps（+9.61%）**、均值 1102.80→1200.91（+8.90%）；p50 中位 51.77→46.51ms（−10.2%）、p99 84.12→81.62ms（−3.0%）；fails=0。回归门 c8（1 轮 × 3 样本/侧）：中位 287.38→286.58（−0.28%），fails=0 | 合入 main |
| R35 | `perf/issue28-r35-collector-target` / `f215bbf`（merge `799d7d2`） | 平台进程启动时把 GC 目标放宽到 400%，`SetMemoryLimit` 取 cgroup 内存上限的 70%；`GOGC`/`GOMEMLIMIT` 显式设置时不介入，cgroup 无上限时不设上限 | 分配画像（c64 负载窗 6,400 查询）：每查询分配 **573 KB**（累计 3.67 GB），in-use 堆只有 **7.3 MB**；默认 GOGC=100 下堆目标≈14 MB，收集器每秒运行数十次只为回收几 MB，assist 时间摊在分配路径。分配前三：Hrana JSON 编解码（`jsontext.fetch`/`ReformatString`/`UnmarshalJSON`）、`bytes.Clone`、`io.ReadAll`，cursor 路径占 52%。诊断臂（同一探针二进制，仅差 `SetGCPercent(400)`，2 轮 × 3 样本/侧）：p50 −14.3 / −9.9%、mean −6.5 / −15.6%、qps +5.8 / +17.0%。配对 A/B（r34 vs r35，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替，候选二进制 `d2dec5f1…`）：逐轮中位 +16.34 / +14.05 / +10.12%（**3/3 同向**），9 样本/侧中位 **1270.06→1444.73 qps（+13.75%）**、均值 1231.68→1372.10（+11.40%）；p50 中位 46.14→41.38ms（−10.3%）、p95 69.68→65.29ms（−6.3%）、p99 72.78→66.85ms（−8.2%）；两侧各 `verify messages: 8625`、`fails=0`。回归门 c8（3 轮 × 3 样本/侧交替）：中位 318.67→363.33（+14.0%）、均值 320.12→356.61（+11.4%），2/3 轮同向；该档由 fsync 抖动主导（`sync_avg` 1.73–2.72ms），按无退化处理 | 合入 main |
| R36 | 测量轮（无代码变更，探针 `d2877e4`、pprof 探针 `a3b6655`，均不合入） | 定位 R35 之后每查询 34.1ms 的归属：worker 调用 15.0ms、平台侧审计屏障 ~10ms、其余 ~9ms | 无插桩分层（r35 二进制，c64）：每次查询 4 条审计追加、0.27 次 sync（c1 是 4.1 次），sync 链 70% 忙。httptrace 传输分层（每命令一组）：连接复用 **94%**，`wait` 0.03ms、`write` 0.02ms、`tail` 0.02ms，**全部成本在 worker 的 server 时间**——describe 2.03ms、statement 1.22ms、cursor 5.91ms、open_session 4.34ms、close_session 1.47ms。把 SQL 换成 `SELECT 1` 后各命令 server 时间不变（1.59/0.97/7.09/4.11/1.16ms），说明这是**与 SQL 无关的每请求固定开销 + 排队**，不是查询执行；worker 自报 `query` 0.48ms/次。CPU 实测（1,538 qps）：platform 0.30 core、worker **0.10 core**，两者都远未饱和 → 仍是等待。c8→c64 同一命令从 ~0.3ms 涨到 2–7ms（8 倍并发换 6.7 倍膨胀），指向 worker 侧按库串行（`RefreshGate` 覆盖 `open`/`begin`/`close`/`health`，注释称守卫区内工作为微秒级） | 留证，未改代码 |
| R37 | `perf/issue30-r37-deferred-durability` / `8684987`+`1dfa9f4` | 非提交操作的 completion/audit 在回复后按 FIFO 异步持久化（8 worker 池、上限 256，饱和回退内联等待）；completion 槽位改为入队即释放；close 在审计预算耗尽时仍 dispatch 并关闭 worker 会话。写语句 intent/terminal 仍同步 durable（用户授权见 issue #30） | 分层实测（v1.1.27，c1，单连接 connect→all→close）：只发 **2 个 HTTP 请求**（open+execute、close），却产生 **3 条耐久记录**（open / 只读 audit / close），`appends==syncs`、每次 fsync 2.36ms，合计 9.67ms = E2E 的 46%；worker 自报 SQL 0.53ms。首版（8684987）在 c64 失败 656–1192/1280（429/502/503）：异步等待仍持有 64 个 completion 槽 → 503 → close 未执行 → `session_seats=190` → `worker session capacity exceeded`；1dfa9f4 把槽位改为入队释放、饱和回退同步后通过。c1 配对 A/B（v1.1.27 vs r37b，3 轮 × 3 样本/侧，每样本 20 次，逐轮交替，两侧全新卷）：cellp **19.89→5.81ms（−70.8%）**、peri **21.36→5.68ms（−73.4%）**、by-id **19.99→4.84ms（−75.8%）**，27/27 样本 fail=0；同 harness 的 sqld 对照为 cellp 1.09×、peri **1.37×**、by-id 1.23×，达到 #30 的 ≤1.5× 目标。c64 回归门（3 轮 × 3 样本/侧，64 并发 × 20 次/worker）：qps cellp +4.6%、peri +1.3%、by-id +5.2%，p50 −1.0% / −0.2% / −4.0%，27/27 样本 fail=0 | 合入 main（崩溃恢复 E2E 通过；掉电丢 close 尾记录会触发隔离，见 R37 节） |
| R38 | `fix/worker-gate-wakeup` / `1b66f58`（merge `0bca5d0`）+ `fix/issue28-verdict` / `f42cfeb`（merge `ef2502f`）；归因探针 `probe/gate-attribution` / `1679f85`（不合入） | G0：`RefreshGate` 过期等待者被唤醒后越过 deadline 时把唤醒转交给下一位（`expire`），超时仍不进临界区；G1：Issue28 并发验收把异常路径、缺测、账本请求数不符与并发 FAIL 纳入 verdict 与退出码。D1 用站点探针归因闸门排队/持有/分段；D2 用 Go benchmark 归因剩余编解码分配 | G0：回退修复的负向对照 8/8 稳定失败，修复后 8/8 通过；clippy 退出码 0；`cargo test --lib` 675 passed / 0 failed。G1：issue28 32 tests OK；`scripts/perf-reliability` 21 个测试文件共 330 tests 全 OK。D1（单库热库 c1–c64，探针 worker `29fe45d4…`）：open 786.7µs = 排队 517.9 + open_session 251.4 + prepare 5.7 + image_check 0.8 + registry 0.3；close 497.2µs = 排队 387.0 + native 89.8 + remove 8.2；begin 283.6µs = 排队 276.4；闸内可移出的 `prepare`（只读检查+ID+Entry）占每查询闸门服务时间 **1.5%**。D2：两候选点合计 7.5µs/请求（json v1，占 5ms 固定成本 **0.15%**），整段删除 `callVersionInto` 上限也只有 1.65µs | D1/D2 都低于 10% 门槛，按方向文档停止、不合入；G0/G1 合入 `main` 并随 v1.2.5 发布 |
| R39 | `perf/issue28-r39-image-gate-shared` / `0b879c9`（merge `446d3fb`） | worker 镜像刷新闸门由互斥改为共享/独占租约：数据面 open/begin/close 持共享租约并发，镜像替换持独占租约（写者优先、双边 deadline、释放即唤醒） | 上限探针（闸门直通、未合入，`3b4b85c2…`，两轮交替）：X1 c16 p50 **7.63→3.54 / 7.29→4.17ms**（−42.8%~−53.6%），c1 不变。配对 A/B（8 轮/侧，json-pipeline，100 请求/档，两侧全新同数据卷，基线 worker `3f2a5175…` vs 候选 `13129acb…`）：c1 0.82→0.79（−3.6%，噪声）、c4 **1.32→0.96（−27.6%）**、c8 **3.06→2.05（−32.9%）**、c16 **9.83→5.36（−45.5%）**；p25 同向 −26.7%~−37.4%。同龄新栈对照（`run-base3` 基线 worker）：c4 1.37/1.40 vs 0.88/1.18、c16 6.29/7.33 vs 4.59/4.71 | 合入 main：回归门 F1 −8.2%、L1 0%、B1 −5.7%、C1 +1.4%、H1 −6.3%、H3 −2.0%，无 >10% 回归；Rust lib 677 passed / 0 failed，clippy/fmt 干净；测量期间 0×429、0 错误、`session_seats`/`active_permits` 归零 |
| R40 | 无代码改动（探针轮，未合入） | 审计头检查点（witness）频率：把 `state_disk.go` 的 `auditBatchMaximum` 32→256 构建探针平台二进制（`aefcb903…`，源码已还原）；动机是 R39 窗口 `head_checkpoint_attempts_total` 1,111 次 / 24.2s（均值 21.8ms） | 机制核验：X1 每轮检查点 **39→5（−87%）**；B1+F1 窗口 11→1（209→20ms）。端到端：F1 p50 0.78/0.87→1.01/0.91ms、B1 2.11/2.17→2.30/2.24ms（均未改善）；X1 c16 基线 3.72/2.96/4.07/6.19ms vs 探针 3.49/190.70/33.11/38.55ms（B2 起 429/错误后劣化；B3/B4 无 429 仍 33–39ms）。归因：检查点停顿同时充当审计批的合并延迟，去掉后 X1 每轮追加 fsync **50→170**，B3/B4 审计总 I/O ≈1.1s vs 基线 1.06s | 上限不成立（无 ≥10% 改善，小查询不改善），不实现、不申请耐久语义授权；分支与提交均无，探针二进制留证。附带发现：X1 在候选明显更快时会耗尽 500 rps 配额出现 429（B1 c4 100 请求 86ms），运行间需留间隔 |
| R41 | `perf/r41-session-open-local-validate` / `e4b39ac`（未合入，留证） | `RemoteDatabase::session()` 与 `is_readonly()` 不再为打开会话/探测只读发 authority `validate_session`/`validate_transaction` 往返：`ObjectStore` 新增带默认实现的 `validate_session_local`/`validate_transaction_local`（无权威的存储回退完整校验），`ContextStore` 只跑 generation + `context.check`（保留 draining 拒绝新事务），`MetricsObjectStore` 透传 | 上限探针（在 Validate RPC 前直接返回 Ok，源码已还原，`5819171f…`，A/B/B/A 20 样本）：F1 json 0.77/0.79→0.47/0.46ms（−39%）、pb 0.66/0.69→0.42/0.44ms（−36%）。Gate 0：20 条 pipeline 的 authority `validate_session` **40→20**（每请求恰少一次）。配对 A/B（6 轮，两侧独立预热栈，A/B 交错，组间 ≥2s）：F1 json 中位 **0.764→0.629ms（−17.6%，配对中位 −17.5%）**、pb **0.714→0.582ms（−18.4%，配对中位 −19.1%）**，6/6 轮同向 | 未达门槛、不合入：F1 属 <10ms 场景，仓库协议要求 ≥20%，实测 −17.6%/−18.4%；回归门无 >10% 可复现退化（L1/C1/X1 c1 反而改善，B1 有单点抖动、H1 持平），分支与提交留证 |
| R42 | `perf/r42-fused-open-statement` / `0bc488b`（未合入，留证） | Hrana 隐式 open 与首条顶层只读 execute 融合为一个私有 worker 命令 `open_guarded_statement`（capability 探测、旧 worker 400 回退、写语句返回 `requires_intent` 后走原提交路径）：每请求 authority `validate_session` 2→1、worker RTT 3→2 | Gate 0：20 条 pipeline 的 `validate_session` **40→20**。配对 A/B（3 轮 A/B/B/A，两侧独立平台+worker 二进制、各自全新同数据卷）：F1 json 中位 **0.766→0.635ms（−17.1%，配对中位 −17.4%，6/6 同向）**、pb **0.819→0.593ms（−27.5%，配对中位 −29.1%，6/6 同向）**；响应字节 312/40 不变 | 未达门槛、不合入：F1 属 <10ms 场景，协议要求 ≥20%；pb 达标、JSON −17.1% 未达（与 R41 的 −17.6% 同量级）。回归门无 >10% 可复现退化（L1 −8.0%、B1 json −4.3%/pb −1.1%、H1 +3.2%/+2.0%、C1 首轮单腿瞬时停顿复测不复现、X1 c1/c4 候选更快、c16 环境敏感，全程 429=0）。分支留证 |

| R43 | `perf/r43-fused-lifecycle` / `6d5c9da`（merge `99a2361`；含 R42 cherry-pick `d41ca9d`） | Hrana `[execute, close]` 融合会话关闭：worker `open_guarded_statement` capability 1→2，level≥2 在同一请求/租约窗口内关闭会话（仅在语句已结算时）；平台 close 入口仍走原 handler（准入、记录、R37 回复后异步耐久），仅跳过 worker close RPC；旧 worker/`requires_intent`/close 失败全部回退普通 close | 上限探针（零代码形状探针，基线栈）：`[execute, close]` 0.752ms vs `[execute]` 0.608ms，close 入口 ≈0.143ms，投影 −20%~−24%。Gate 0（临时平台探针 + authority diag）：20 条 pipeline → `open_guarded_statement` **20**、`close_session` **0**（R42 级 worker 对照 20+20；`validate_session` 20 vs 基线 40）。配对 A/B（3 轮 A/B/B/A，两侧独立预热栈，samples 20/30/50）：F1 json 中位 **0.794→0.510ms（−35.7%，配对中位 −39.3/−34.6/−30.2%，6/6 同向）**、pb **0.768→0.513ms（−33.3%，配对 −32.6/−33.5/−33.8%，6/6 同向）**；响应字节 312/40 不变、warnings=[]、无 429 | 合入 main：F1 <10ms 场景 ≥20% 门槛双协议达标。回归门无 >10% 可复现退化（L1 −11.3%、B1 json +3.9%/pb −3.2%、C1 −3.2%、H1 cursor json 149.59 vs 145.13ms/pb −4.5%；X1 c1 候选更快、c8/c16 宿主瞬态不可确认；全程 429=0、errors=0、seats/permits 归零）。未做崩溃恢复 E2E 与真实 close 失败 Rust 集成测试 |
| R44 | 测量轮（探针分支 `probe/r44-validate` @ `0a65f65`、`probe/r44-rpc-cost` @ `a576fb8`，均不合入） | 在 R43 之后的 0.50ms 底上找剩余方向：D1 close 每次线程 spawn、D3 pipeline 每 entry 内部 HTTP 跳转、authority lease 校验往返 | D1 零代码微基准 `spawn+condvar+join` 0.021ms/次（F1 的 4%）；D3 每 entry 可省 **2.6µs**（marshal 0.61 + NewRequest 1.16 + reply 0.69 + 响应编码 0.16；F1 ≈1%、B1 ≈2.4%）；lease 校验：R41 部分本地校验 0.50→0.46/0.49ms（−2~−8%），全量本地校验 **0.50→0.34/0.36ms（−28%~−32%）**；成本拆分 worker 侧 RPC **207µs/次**、authority 侧 `check_binding` **≈84µs/次**（其中 `ownership_view()` = `store.get(authority/root)`+解码 ≈85µs，两次 `live()` 合计 ≈2µs） | 仅记录，不合入：D1/D3 远低于 20% 门槛；lease 校验是唯一过门槛的方向，但去掉它等于放宽安全语义（读路径再无其它 authority 交互），需用户明确授权，本轮询问被拒绝；authority 侧只缓存 root 视图可省 ≈85µs ≈17%，仍不够 |
| R45 | `perf/r45-cursor-page-buffer` / `2a52f46`（merge `a69b51e`） | Hrana 流式游标（JSON NDJSON 与 protobuf）不再逐行 `writer.Write`：页内累积、页尾一次写出；帧编码、`fatal`/错误路径前先冲刷缓冲，页面 `Flush` 边界与字节流不变 | Gate 0（临时计数 writer，`ad8def88…`，测前已移除）：H1 8,625 行 json **34** 次写出 / protobuf **37** 次，整包 20,608,525 / 20,272,006 字节与基线逐字节一致。配对 A/B（2 轮 × A/B/B/A，两侧独立栈 + 各自全新同数据卷，1 warmup + 5 timed/leg）：H1 json **154.23→123.20ms（−20.12%）**，逐轮 −20.37% / −20.94%（2/2 同向）；protobuf **126.44→102.03ms（−19.31%）**，−20.34% / −19.72%（2/2）；每样本 rows=8,625、响应字节一致、0 错误 | 合入 main：>10ms 场景 10% 门槛双协议达标（约 2×）。回归门 H3（同机制 cursor）**−14.86%**、F1 −2.9%/+0.3%、C1 +5.3%、B1 json −0.2%、R1 复测 −5.6%；L1 两侧均 `row_count_mismatch`（合成 fixture 主导 cwd 超过 SQL `LIMIT 40`，与候选无关）；X1 两轮方向互换、宿主瞬态不可确认，未见可复现 >10% 退化；warnings=[] / fatal=[] / 429=0 / 非 200=0，两侧结束 seats/permits 归零 |
| R46 | `perf/r46-fused-parse-once` / `d79500c`（merge `924f50e`） | Hrana fused open 的 statement 答案只解析一次：适配器发布前已解析的 `fusedStatementAnswer` 随 marker 交给 dispatch，去掉第三次 decode+validate | Gate 0（临时计数，测前移除）：每次 fused statement 的 `parseFusedStatementAnswer` **3.0→2.0**、复用 4/4、回退 0。配对 A/B（R1 json-pipeline，2 轮 × A/B/B/A，两侧独立栈 + 各自全新同数据卷，1 warmup + 5 timed/leg）：逐轮中位 **47.72→39.86ms（−16.48%）/ 45.37→37.89ms（−16.49%）**，4/4 配对腿同向（−12.8%…−19.8%），合并四腿中位 **47.27→38.99ms（−17.51%）**；候选最慢腿 40.02ms 仍低于基线最快腿 43.00ms；H1 json/protobuf 在 ±3% 内、F1 −6.56%/+9.72% 方向相反；响应字节 2,369,290 / 20,608,525 / 20,272,006 不变 | 合入 main：>10ms 场景 10% 门槛达标（约 1.65×）。回归门 B1 json +4.87% / pb +2.57%、C1 −0.13%、H3 json −0.27% / pb −1.66%、L1 +1.71%/+4.26%（两侧均 `row_count_mismatch`）、F1 json 单轮 +14.79% 但 12 腿复跑合并 +0.93% 不复现、X1 宿主瞬态不可确认；warnings=[] / fatal=[] / 429=0 / 非 200=0，两侧结束 seats/permits 归零 |
| R47 | `perf/r47-fused-open-answer` / `e4b6606`（merge `c0b2ab2`） | 融合 open 的答案解析跨进程内边界直传：网关已解析校验的答案随 `hranaOpenReply` 交给 recorder，适配器跳过内部信封 `json.Unmarshal` 与第二次 `parseFusedStatementAnswer`（仅 `fusedOpenStatementFrom(ctx)` 的 pipeline open；cursor/websocket/公开 open 仍编码信封，字节不变） | Gate 0（计时探针，测前移除，18 请求，次/请求）：fused_parse **2.0→1.0**、validate 2.0→1.0、open_unmarshal 1.0→0、typed_open 0→1。配对 A/B（R1 json-pipeline，2 轮 × A/B/B/A，两侧独立栈 + 各自全新同数据卷，1 warmup + 5 timed/leg）：逐轮中位 **40.10→28.18ms（−29.73%）/ 37.15→26.25ms（−29.34%）**，4/4 配对腿同向，候选最慢腿低于基线最快腿；对照 H1/F1 方向不一（−7.1%~+3.4%）；回归门 B1 pb 单腿 +18.39% 经三批复测（6 腿中位 −0.43%）判噪声，L1 json-pipeline −11.31%（两侧 `row_count_mismatch`）、X1 不可确认 | 合入 `main` |
| R48 | `perf/r48-statement-scan` / `1b44553`（merge `fca36a6`） | 融合 open 应答改为单遍扫描校验：手写严格 JSON 扫描器直接定位信封字段与 `result` span（零拷贝），等价复刻 `validStatementResult`（cols/rows、重复键 last-wins、`rows:[null]` 零宽行、autocommit 裸布尔、数值字段走 `json.Unmarshal`），去掉信封整体 `Unmarshal` 与顶层 map/`[][]RawMessage` 物化；`validStatementResult` 保留为 oracle | 上限探针（base `a167ace`，n=18）：`parseFusedStatementAnswer` 6.33ms（其中 `validStatementResult` 3.50ms）、单遍扫描下界 0.92ms ⇒ D2 上限 **5.41ms（20.8%）**。Gate 0（`1b44553` + 计数/计时探针，测前移除，12 请求）：`scan_calls` **1.0/请求**、`legacy_calls` **0**、`scan_ns` 0.538–0.788ms。配对 A/B（2 轮 × A/B/B/A，两侧独立栈 + 各自全新同数据卷，1 warmup + 5 timed/leg）：R1 json-pipeline 逐轮中位 **28.24→23.87ms（−15.49%）/ 30.56→24.65ms（−19.35%）**，全体 4 腿中位 −18.05%（3/4 配对同向）；H1 json −1.1%/−3.8%、H1 protobuf −1.2%/+0.3%、F1 −3.5%/+13.9%（亚毫秒）。回归门 B1 json −11.96%、B1 pb +19.92%（2 批复测池化 +1.6%，噪声）、C1 −2.81%、H3 −1.3%/−1.2%；L1 两侧 fixture mismatch、X1 不可确认。微基准 6.5–7.7ms→0.52–0.56ms、分配 −99.8%；3,500 变异 + 20,000 抽样对拍一致；全量 Go 测试 ok 173.7s | 合入 main |
| R49 | `perf/r49-json-entry` / `d84924d`（merge `d40c1f3`） | Hrana JSON pipeline 的 execute entry 单遍直转码：扫描 worker statement 结果 span 直接产出完整 entry 字节（等价复刻 `hranaNativeStatement` + `hranaOutputValue` + metrics 校验：重复键 last-wins、`cols:[null]`/`rows:[null]`、整数保留原始字面量、float 按 encoding/json、blob base64、字符串 HTML 转义/U+2028/U+2029/非法 UTF-8），并让最终响应按 entry 字节拼装（`assembleHranaResponse`），不再二次 `json.Marshal`；仅融合标记命中的 HTTP JSON pipeline，其余路径与 protobuf/cursor/WS 不变 | 上限探针（`c5d2997`，n=18，R1 中位 26.0ms）：解码 3.04ms、转换 0.064ms、entry 预算 marshal 1.51ms、最终 marshal 1.91ms、拼装 0.119ms ⇒ C3' 单独 1.79ms（6.9%）低于门槛不立项；D1 3.10–4.61ms、D1+C3' 4.89–6.40ms（18.8–24.6%）。Gate 0（`d84924d` + 计数/现场对拍，测前移除，12 请求）：`fast_attempt/ok/fallback` **1/1/0**、`entry_reused` 1、`assemble_ok` 1、`assemble_fallback` 0、**`legacy_equal` 每请求 1**。配对 A/B（2 轮 × A/B/B/A，两侧独立栈 + 全新同数据卷）：R1 json-pipeline 逐轮中位 **21.04→17.10ms（−18.71%）/ 21.34→16.39ms（−23.19%）**，4 腿整体 −23.2%（3/4 配对同向）；对照 H1 json −1.2%/−3.4%、H1 pb −3.7%/−2.8%、F1 +7.9%/−8.4%（亚毫秒）。回归门 B1 +3.31%/−1.99%、C1 +3.55%、H3 −0.86%/−2.41%；L1 两侧 fixture mismatch、X1 不可确认。微基准 4.53→2.49ms、4 allocs；全量 Go 测试 ok 176.6s | 合入 main |
| R50 | `perf/r50-protobuf-execute` / `f73aa1c`（merge `f902721`） | Hrana protobuf pipeline 的 execute entry 单遍直转码：复用 R48 扫描器在 worker statement 结果 span 上直接产出 protobuf 结果（等价复刻 `hranaNativeStatement` + `hranaProtoStatementResultWithin`：重复键 last-wins、`cols`/`rows` 必填非 null、整数保留原始字面量、float/字符串/null 语义；`decltype` 与 `last_insert_rowid` 在旧路径因 `*string` 断言不匹配从不输出，故只校验不输出），`assembleHranaProtoSingleExecute` 直接拼装 `[execute, close]` 两 entry；仅融合标记命中的 HTTP protobuf 单执行 pipeline，JSON/cursor/WS/公开 API 不变 | 上限探针（`4784413`，n=6，客户端中位 26.25ms）：解码 2.99ms + 转换 0.07ms + protobuf 编码 12.02ms = **16.36ms（64%）**。Gate 0（`f73aa1c` + 探针，测前移除，4 请求）：`complete=4 unsupported=0 limit=0 equal=4 mismatch=0`，与旧路径逐字节一致（2,349,027 字节）。配对 A/B（两侧独立栈 + 各自全新同数据卷、同一 worker）：R1 protobuf-pipeline 第 1 批逐轮 **20.13→17.13ms（−14.86%）/ 22.87→16.64ms（−27.24%）**、4 腿整体 −16.36%；第 2 批 3 轮 **−26.67% / −19.13% / −35.58%**（5/5 同向，6 腿整体 19.94→15.26）。对照 H1 pb +1.4~1.6%、H1 json −0.5~−1.7%、F1 亚毫秒 ±15%；R1 json 第 1 批 +12.56% 经 3 轮复核 −11.09%（噪声）。回归门 B1 pb 单腿 +113.94%、L1 json +16.13% 经 15 样本/腿复核为 −6.9%/−4.1%（宿主瞬态），其余 ≤8%；L1 两侧 `row_count_mismatch`（fixture 项）、X1 不可确认。全量 Go 测试 ok 174.556s | 合入 main |
| R51 | `perf/r51-cursor-page-bulk` / `9ad2c43`（merge `3b4b9cb`） | cursor 页文本扫描与转码改为批量定位：`scanner.string()` 用 SWAR 跳过普通字节、只在引号/反斜杠/控制字节处逐字节判定；protobuf 的 `hranaCursorPageAppendText` 与 JSON 的 `hranaJSONAppendPageText` 用 `bytes.IndexByte`/SWAR 批量拷贝普通段；新增 83 行 helper 与随机化差分测试，pipeline/write 路径不变 | Gate 0（探针 `7ba35a7e…`，测前移除）：H1 json/protobuf 各 1 样本，`string_calls=34,779 string_bytes=80,778,969 string_mismatch=0`、`text_calls=8,625 text_bytes=20,177,069 text_mismatch=0`、`json_calls=8,625 json_bytes=20,194,319 json_mismatch=0`，52,029 行全 0，wire 字节不变。配对 A/B（2 轮 × A/B/B/A，两侧独立栈 + 各自全新同数据卷，1 warmup + 5 timed/leg）：H1 json **124.14→74.65ms（−39.87%）/ 123.77→75.84ms（−38.73%）**，protobuf **101.26→69.84ms（−31.03%）/ 104.70→68.81ms（−34.28%）**，两协议两轮 4/4 同向；同路径 H3 json −35.78%/−37.92%、pb −34.19%/−30.27%；对照 R1 json −4.47%/−9.78%、R1 pb −24.66%/−4.55%、F1 亚毫秒 | 合入 main：主指标两协议两轮均远超 10% 门槛（约 1.6×）。回归门 B1 json −11.22%、B1 pb −0.35%、C1 −6.43%、H3 json −38.19%、H3 pb −30.82%、L1 cursor −2.70%、L1 pipeline −0.64%；F1 json 单批 +14.02%（0.07ms）经 4 腿×15 样本 +12.98% 与 8 腿×15 样本 +2.14% 两批复测方向不一致，机制上 pipeline 不经过本轮 diff，判噪声；F1 pb +19.73% 复测 +0.17% 不复现；X1 不可确认。warnings=[]、fatal 无、非 200=0，wire 字节一致；全量 Go 测试 ok 179.642s |
| R52 | `perf/r52-cursor-page-single-pass` / `8967ae8`（merge `cbf99f5`） | 只读 cursor 页单趟严格解码与 span 交接：结构定位结果 span → 信封骨架 `json.Valid` → 结果 span 严格页扫描，页体零拷贝交给 Hrana 适配器；去掉整页 `json.Valid`、结果复制、gateway 页校验、fused 首页二次校验、adapter 二次扫描与 compact/escape 重写；写 cursor/pipeline 路径不变 | 上限探针（基线路径阶段计时）：可移除 20.70ms = H1 json 单查询 75.01ms 的 27.6%，远高于 7.57ms 门槛。Gate 0（探针 `f9dff879…`）：H1 json 每查询 `pagedecode=34/20.22MB`、`pagescan=34`、`fallback=0`，`json.Valid` 只剩约 0.12MB 小回复，wire 字节不变。配对 A/B（2 轮 × A/B/B/A × 5 samples/leg，两侧独立栈 + 各自全新同数据卷）：H1 json **75.04→62.17（−17.15%）/ 75.01→61.69（−17.76%）**、protobuf **71.65→57.29（−20.04%）/ 69.68→58.90（−15.48%）**，4/4 同向；同路径 H3 json −18.29%/−18.43%、pb −16.26%/−14.62% | 合入 main：主指标两协议两轮均过 10% 硬门。回归门 11 组：H3 json −17.93%、H3 pb −16.53%、C1 +4.87%、F1 json −5.55%、F1 pb +2.52%、R1 pb +1.24%、L1 cursor −2.66%/pipeline −8.06%（L1 verdict 为夹具 `LIMIT 40` vs 99 行的既有不符，四腿全 FAIL，只记录时间）；B1 json 首轮 +24.11% 经 8 腿×15 样本复测 −2.04% 翻转；R1 json 首轮 +14.74%、复测 +13.32%，但全新卷换位为 +6.58%/+4.11%，且**同二进制对照**（两台相同基线二进制、全新卷）腿中位相差 −8.11%，判「不可确认（栈/窗口效应）」；机制上 pipeline 只多一次类型断言与 nil 检查。140 份 summary 全 `warnings=[]`、非 200=0；全量 Go 测试 ok 175.1s ×2 |

| R56 | `perf/r56-cursor-page-pipeline` / `d5cf31f`（merge `ee9d553`） | 只读 cursor 页预取 + worker 响应按声明长度预分配：当前页扫描通过后、写出前发出下一页 fetch（`context.WithoutCancel` + 30s 超时，缓冲 `chan`），非首页消费在途回复；`readWorkerReply` 在 `Content-Length` 合法时 `make` + `io.ReadFull`，否则有界回退；写 cursor 不预取 | Gate 0：H1 每查询 issued=33 consumed=33（json/native 同），单页 L2 0/0。单独 A/B（2 轮 × A/B/B/A × 5 samples/leg，两侧独立栈 + 各自全新同数据卷）：H1 json **−9.23% / −8.99%**、protobuf **−9.60% / −8.87%**，4/4 同向但**均低于 10% 硬门**，不单独合入 | 与 R57 作为同一批次合入 main（merge `ee9d553`）；单独轮次按判据记为未达门槛 |
| R57 | `perf/r57-worker-cursor-borrow` / `1daea98`（merge `5492e05`） | worker 只读 cursor 页直接借用序列化：`WorkerResult::ReadCursorPage` 把引擎行直接写进响应编码器（手写 `Serialize`），去掉每格 `serde_json::Value` 分配与整页 `to_vec`；映射逐条对齐 `sessions::value`（Null/Integer/有限 Float/Text 转义/Blob；非有限浮点→null），写 cursor 与 execute 保持 Value 树 | Gate 0：H1 json 每查询 34 页 = 1 first + 33 fetch；protobuf 34 页全部走 fetch 形状（native cursor 无融合首页）；L2 单页 fetch=0；写 cursor `UPDATE … RETURNING` 返回 200 且探针 0 命中。**批次 A/B（R56+R57 vs main，2 轮 × A/B/B/A × 5 samples，两侧全新卷）**：H1 json **−10.33% / −17.55%**、protobuf **−12.52% / −16.14%**，4/4 过 10% 硬门；H3 json −16.63%、pb −11.10% | 合入 main（与 R56 同批验收、同批合入） |
| R58 | 测量轮（探针分支 `probe/r58-cursor-page-timing` @ `da2d8c1`、`probe/r58b-row-checks` @ `14aef4e`，均不合入） | H1 20MB cursor 剩余成本归因 + 行级检查上限探针：平台侧页等待/转码/upstream/dispatch 分段，worker 侧 turso/序列化/行级检查分段；再把 `run_steps` 的每行 budget 检查、操作读守卫与 step 时钟整体移出循环测端到端上限 | 归因（H1 json ≈53ms）：worker ≈31ms（turso VM stepping ≈17–18ms、页序列化 ≈6.6ms、行级检查 ≈4.3–5.9ms 且被探针 `Instant::now` 放大）、平台自身 ≈6.9ms，其余为客户端读取 20MB。上限 A/B（A/B/B/A × 5 samples，两侧独立栈 + 同批 fixture）：json 52.00→48.20ms（**7.3%**）、protobuf 51.94→48.03ms（**7.5%**），两条腿同向，响应字节/行数（8,625）/`warnings` 不变 | 仅记录，不实施：行级检查上限 7.3–7.5% < 10% 硬门；剩余 H1 大头（turso VM stepping、页序列化/协议帧）属 worker 引擎与协议层，需用户显式授权 |
| R59 | 探针 `probe/r59-deferred-workers` / `fbf7b98`（不合入） | 延迟耐久账户 worker 8→64，验证 X1 并发档尾部是否由该池饱和导致；不改耐久时点、审计链语义与配额 | 归因（基线 + `PERI_DURABILITY_OBSERVER=1` + `--sql-open-observer`，独立栈）：`audit_batch` 506 批中 466 批 `count=1`、`sync_ns` p50 3.84ms；`audit_checkpoint` 55 次 p50 **21.0** / p90 31.2 / max 38.1ms，合计 1.204s；`audit_member_wait` 排队 p50 46.7ms、屏障等待 p99 115.8ms；慢 open/close 148ms = `audit` 126.2 + `terminal` 21.7。上限 A/B/B/A（X1 json-pipeline，4 档 × 100 请求，1 warmup + 3 samples/腿）：c16 中位基线 3.75 / 16.56ms → 探针 **165.62 / 124.39ms**（回归 33–44×，两条 B 腿同向），c4 p95 97.6→29.6–34.7ms 但被 c16 抵消；0 错误 / 0×429 / verdict 全 PASS | 不合入；分支与提交保留作证据：账户宽度不是限速器，加并发只让串行写者的屏障更碎（与 R29/R31 同构） |
| R60 | 测量轮（无性能改动；harness protobuf `Error` 字段修正单独提交） | W1 写路径归因：用请求相位 + 耐久观察器拆解单条写请求（open→statement→close）的 40ms 构成，并复核 R1 smoke | 窗口 1.89s、48 statement（16 测量 + 32 harness setup/cleanup）：statement p50 **37.85ms** = native 17.59 + **intent 12.63** + audit 3.81 + terminal 3.79（open 1.08、close 0.45）；native 仅 1.3–1.6ms 的 4 条语句 intent 仍 10.9–12.8ms（p50 11.27）；open→statement 间隔 5–39µs（p50 13µs，n=48）；44/48 intent 与 audit+journal 批重叠、4 条与 checkpoint 重叠，三类 intent 均值 12.78/12.48/12.38ms ⇒ 等待不是 checkpoint CPU。窗口耐久：`audit_batch` 143（142×count=1，sync p50 3.87ms，合计 669ms）、`journal_batch` 145（98×count=1，sync p50 6.64ms，合计 867ms）、两链合计 1.54s/1.89s；`audit_checkpoint` 全run 18 次 p50 **21.0ms**（4 次 fsync：state+floor 两个 `atomicPrivate`，每 32 条审计记录一次） | 仅记录，不合入：W1 成本 = worker 写执行（native 17.6ms）+ 耐久屏障（≈20ms），intent 是上一条请求延迟会话记录的排队；两条 ≥10% 方向（提交意图/延迟记录的屏障顺序、checkpoint 频率与成本）属耐久提交时点与审计链语义，需用户显式授权 |
| R61 | 探针 `probe/r61-w1-intent-ceiling` / `9cf4911`、`probe/r61-x1-checkpoint-ceiling` / `c59e917`（均不合入） | 两项不安全上限探针：① W1 提交意图不等自己的屏障就 dispatch；② 跳过 `appendBatch` 的 `pre_batch` 与 `post_batch` checkpoint（保留 shutdown） | ① W1.insert json-pipeline A/B/B/A（4 条独立栈 + 全新同数据卷，2 轮 × 1 warmup + 3 timed/腿）：基线 40.56 / 40.94ms → 探针 32.10 / 31.47ms（**−20.9% / −23.1%**，4 轮逐轮中位同向，−8.46 / −9.47ms）。② X1 json-pipeline（4 条腿，100 请求/级）：Gate 0 `head_checkpoint_attempts_total` **54→0**（1.1447s→0，fsync 512→567）；c4/c8 p99 多数下降但 c16 B1 +36%、4 级 elapsed 合计 B1 −2.0% / B2 +6.8%，单级跨度 82–557ms，方差大于效应 | 不合入：W1 上限 21–23% 指向「意图排在延迟会话记录之后」，但改的是耐久提交时点/顺序，需用户显式授权；X1 checkpoint 被否证，不是尾部限速器（请求在 checkpoint 前已回复）。第一版 checkpoint 探针只跳 `post_batch`，Gate 0 发现 56 次 `pre_batch` 仍在，已作废并重跑 |
| R62 | `perf/r62-deferred-bound` / `861eb8a`+`530fcf7`（不合入，留证） | 延后耐久账户待办上限 256→4096，与审计队列自身的 4096 条 / 8 MiB 准入上限对齐；新增 `/v1/metrics` 的 `deferred_durability`（pending / pending_max / maximum / enqueued_total / inline_total） | Gate 0：100 请求/级时候选把固定 500 rps 桶耗尽（16 次 `rate_limit` vs 基线 0），拒绝记录排在延后积压之后约 500ms；成功请求 p99 仅 20.37ms。改用 50 请求/级（配额不变）后两侧干净。A/B：2 轮 × A/B/B/A，8 条独立栈 + 重新 seed，X1 c8/c16 p95/p99 两轮四单元全部 −31.1%~−79.3%（合并 −33.1% / −67.6% / −62.1% / −49.2%）；护栏 c1 p95 0.973→6.370ms（**+554.9%**）失败，c1 p50 −3.2%、c1 p99 −22.1%、c4 p50 −23.6%、c4 p95 −93.0%；8 腿 0 错误 / 0×429 / `inline_total=0` / clean stop | 不合入：主指标通过，但 A/B 前冻结的 c1 p95 护栏失败。分支与两份提交保留作证据；下一轮在 c1 上做请求级相位归因（open/statement/close 的 journal/audit/native 分段），先确认 5–9ms 尖峰归属，再决定修实现还是修测量方式 |
| R63 | 无代码改动（归因轮） | R62 c1 尾部归因：观察器腿 o1–o8（X1 1/4/8/16，相位观察器）+ c1-only 腿 p1–p6（harness 补丁 `concurrency_levels=(1,)`，无观察器），两侧仍是 R62 的两个平台二进制 | c1-only 腿：A/B 腿中位 p95 **0.56 / 0.55ms**、p50 0.44 / 0.46、>2ms 3/150 vs 6/150，只有 p5 一条腿越过 2ms（4.65ms）；两侧 max 同为 7.7–16.7ms 量级。观察器腿：**每条腿两侧都有 ≥1 条 >5ms 的 c1 样本**（A 4/200、B 9/200 >2ms；max 18.56 / 19.51ms），形态为 `terminal` 6.2–11.1ms、handler 外层帧自身时间 6.9–14.3ms、内层记录间空档 4.0–18.0ms。机制：c1-only 腿 `pending_max` 200–243 < 基线冻结上限 256、`inline_total=0`，且 c1 是观察器腿的第一级 ⇒ 256→4096 在 c1 结构性不可见；同腿 journal 等待 max 8.1–12.3ms、audit fsync max 7.2–9.8ms，与尖峰同量级；c1-only 组 `head_checkpoint_attempts` 两侧都是 4 | 仅记录，无代码改动：c1 尖峰是两侧共有的宿主屏障抖动，与 R62 候选无因果关系；**不推翻 R62 冻结判决、不重跑洗判据、不合入该分支**。R64 须用新冻结的、c1 抖动无法触发的判据重新评估同一候选 |
| R64 | 测量轮（无代码改动；候选仍为 `perf/r62-deferred-bound` / `861eb8a`+`530fcf7`，merge `8500b5a`） | 在新冻结判据下重新评估 R62 候选：c1 尾部（p95/p99/max/>2ms）按 R63 的归因降级为报告项，c1 中位数保留护栏；其余主指标与护栏沿用 R62 口径；判据在首条腿之前提交并推送（`ad1d113`） | Gate 0：四条候选腿 `maximum=4096`、`pending_max` 741/770/661/800（>256）、`inline_total=0`；基线腿无该指标。A/B（2 轮 × A/B/B/A，8 条独立栈 + 重新 seed，X1 50 请求/级，无观测器）：主指标 8 个轮内比较全部 ≤90%，合并 c8 p95 **0.108** / c8 p99 **0.076** / c16 p95 **0.320** / c16 p99 **0.314**；护栏 c1 p50 0.983（R1）/1.040（R2）/1.014（合并）、c4 p50 0.754/0.737/0.709、c4 p95 0.067/0.037/0.038，全部 ≤110%；c1 尾部报告项 A 6/200、B 8/200 >2ms，两侧最大值 16.60 / 16.38ms；8 腿 0 错误 / 0×429 / 无拒绝 / `rows=1` / clean stop；掩码 `query_duration_ms` 后原始响应体逐字节相同。机制（解释变量）：journal appends 8 腿均为 404，候选 audit fsync 17–20 次 vs 基线 30–32，同步 head checkpoint B 8–10 次 vs A 19–20 次 | **合入 main**（merge `8500b5a`）：新冻结判据下主指标与全部护栏通过；R62 的原冻结判决不变（当时不合入）。候选扩大的只是回复后待办上限，耐久时点、审计链顺序、隔离与 fail-closed 未改 |
| R65 | 测量轮（无生产代码改动；A `main`@`1399f15` sha256 `fd959df7…`，B `main`@`1a6b9dd` sha256 `9214cd0c…`，worker 两侧 `a648ecd9…`） | R64 合入后的回归门：W1/F1/H1 主协议（W1/F1 `json-pipeline`、H1 `json-cursor`），确认延后耐久待办上限 256→4096 未造成 >10% 退化；2 轮 × A/B/B/A，每场景 8 腿，共 24 腿 | 24/24 腿有效：fixture complete、352/352 请求 `ok`/HTTP 200、warnings/fatal 空、F1 rows=1、H1 rows=8625、samples 5/5/3、clean stop 且 `unresolved_operations=false`、无 429/expected_limit/audit_unavailable。主指标（候选/基线）：W1 1.101（33.705→37.125ms；round1 0.972、round2 1.153）、F1 0.987（0.532→0.525ms；两轮均 0.987）、H1 1.022（53.171→54.357ms；0.950/1.078）。**W1 边界 PASS**：round2 1.153 与合并 1.101 均 >1.10，仅因 round1 0.972 未超阈值；W1 round2 `query_ms` 从 9–11ms 阶跃到 16.96/20.41/16.14ms（A `r2a2` 同样受影响），`duration_ms − query_ms` 比值 round1 0.977 / round2 1.031 / 合并 0.977 ⇒ 平台侧无回归信号。机制快照：12 条 B 腿 `deferred_durability.maximum=4096`、`inline_total=0`，`pending_max` W1=4 / F1=91 / H1=18–21，远低于 4096，未逼近新上限；12 条 A 腿无该指标 | 无代码改动：冻结判据下三场景均无回归 PASS；W1 为边界 PASS，不是稳健无回归。后续 R43 adapter 级 fused close 撤销会改变 F1 绝对延迟，本轮判决仅代表 `a46afa2` 上的 R64 变更 |
| R66 | 探针轮（无生产代码）；`probe/r66-legacy-settle-ceiling` @ `066ba9e` 未合入 | legacy settle / GC / fused close 三个上限探针 | legacy settle 移除：c8/c16 尾部无改善（c16 p95/p99 两轮 2.12–3.33×反向）；GOGC=off 仍有同样 15.7–32.9ms 停顿；F1 close 均值 0.248ms 但 fused close 当前仅约 4% 且属红线 | 全部判停，不合入；授权内无可过 ≥10% 门槛的方向 |
| R67 | 测量轮（无代码改动） | 复测 v1.2.7 发布 A/B 的 F1 FAIL：v1.2.6 vs v1.2.7，2 轮 × A/B/B/A、每腿 20 个 timed 样本 | 两轮 0.800 / 1.276（方向相反），合并 2.631→2.413ms（**0.917**）；样本级 Mann-Whitney p=0.997，原窗口 40 样本 p=0.989 | 判为低功效估计量导致的噪声，不改写原 FAIL 记录；验收改用样本级合并统计 |
| R68 | 无代码改动（只读反思轮） | 复核「现有授权内是否还剩 ≥10% 方向」，并定位 c128 的 `audit_unavailable` 与 `deferred_durability` 上限行为 | 逐候选上限：适配器→网关内部 HTTP 转发 25µs/条（F1 4.7%、L1 2.4%、C1 7.0%），而其中可移除部分仅 2.6µs/条（F1≈1%、B1≈2.4%）；worker 会话生命周期 341µs/请求（F1 64%、L1 33%）需会话语义授权；H1 三条路径上限均 <10%；X1 尾部由宿主级暂停主导；编码/分配/GC ≤2–8%，且 c64 时 CPU 仅 0.30 core；唯一实测 ≥20% 的方向（W1 intent 屏障重排，R61 −20.9%~−23.1%）需耐久语义授权 | 无合入；结论记录为 R68 |
| R71 | `perf/r71-deferred-session-close` / `af2e542`（merge `1d0185c`） | close 的 worker 等待移出回复路径（延迟会话关闭，kill switch 默认关闭） | R71d：P1 0.76577（−23.4%，CI 0.66563–0.87018，26/30 同向）；F1/L1/标准 X1/持续 c8 全 PASS（持续 c8 1.02246，8/8 legs 零错误）；P3 34/34；Gate 3 T1–T9 PASS | 合入 main |

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
- R3 的 R1-1000 受控 A/B/B/A 为 JSON 69.33→63.25ms（−8.8%）、protobuf 65.52→66.97ms（+2.2%）；
  编码层微基准 JSON 3.60→2.09ms、protobuf 2.78→1.55ms，但端到端未达 10% 门槛。按协议不合入，
  `main` 保持 R2；分支、commit `44c14be` 与二进制 `/tmp/peri-issue28/peri-platform-r3`
  （父级重建 SHA `487113f…`）保留作证据。
- R3 表明 R1-1000 的剩余成本主要在 worker/传输，不在 Go 编码层；若要继续过 10%，
  需要 buffer 复用或改变耐久时点（后者必须先获得用户明确授权）。
- R4 前置诊断（`main`/R2、无代码改动，直接读 `/v1/metrics` 计数器增量）：
  - X1（800 请求，1/4/8/16 各 100）：每请求 2.01 次 operation journal fsync（每次约 1.96ms）
    + 1.33 次 audit fsync（每次约 1.30ms）；fsync 总耗时 2,610ms / 墙钟 4,512ms = 58%。
    `state_lock_wait_ns_total` 仅 170ms（3.8%，单次最大 7.5ms）。X1 饱和由持久化屏障主导，
    方向 3（worker refresh 门 Condvar）与方向 5（Go state 锁读写分离）的预期收益都到不了 10%。
  - H1 json-cursor（单次 436ms）：38 次 audit fsync（约 70.3ms）+ 5.3 次 journal fsync（约 18.0ms），
    fsync 合计约 88ms = 20%；其余为 worker 扫描与传输。方向 6（页 256→512）只省约 17 次
    audit fsync ≈ 31ms ≈ 7%，低于 10% 门槛；1024 页约 10.5%，但单页预算（4MiB）对宽行有回归风险。
  - 结论：不改变持久化时点/数量的前提下，剩余方向的主场景预期收益均 <10%；唯一可达标的是
    pipeline 内只读审计批提交（B1 25–40%），属耐久语义变更，须用户明确授权后才能开轮。

#### R2 后全量复测（2026-10-30，n=15，容器 `peri-loom-issue28`）

| 场景 | 协议 | 基线（`fa1d4a6`） | R2 后（`e861265`） | 变化 |
|---|---|---:|---:|---:|
| H1.content-20mb | json-cursor | 636.57ms | 447.61ms | −29.7% |
| H1.content-20mb | protobuf-cursor | 652.43ms | 398.59ms | −38.9% |
| H3.content+projection | json-cursor | 664.40ms | 461.48ms | −30.5% |
| H3.content+projection | protobuf-cursor | 643.27ms | 415.29ms | −35.4% |
| R1.limit-1000 | json-pipeline | 93.19ms | 72.97ms | −21.7% |
| R1.limit-1000 | protobuf-pipeline | 90.83ms（R1 基线） | 71.95ms | −20.8% |
| B1.batch-20 | json-pipeline | 43.53ms | 46.43ms | +6.7%（仍 FAIL） |
| X1.concurrency=16 | json-pipeline | 61.30ms | 65.07ms | +6.2%（跨轮噪声内） |
| F1.select-1 | json-pipeline | 9.00ms | 11.59ms | +28.8%（跨轮噪声） |
| L1.list-20 | json-pipeline | 8.83ms | 10.00ms | +13.3%（跨轮噪声） |

- 本次全量 76.7s，`warnings=[]`、无 429、`session_seats=0`、`active_permits=0`；
  响应字节与基线逐字节一致（H1 25,162,268/23,600,883；H3 25,300,319/23,635,410；
  R1-1000 3,725,223/3,545,845）。
- 小场景单次全量差值在 ±30% 量级（历史 F1 9.0–13.2ms、B1 protobuf 33.8–51.9ms），
  不作为 R1/R2 的结论；B1 仍是唯一稳定 FAIL。
- 容器内存 206.2→259.2MiB（idle 259.1MiB），仍未证实是否泄漏；H2 仍受 projection 全 NULL 限制。

#### R4：pipeline 只读审计批提交（2026-10-30）

分支 `perf/issue28-r4-pipeline-audit-batch`，commit `e82e36d`，merge `c815711`（已合入 `main`）。

- **方案**：pipeline 内只读成功语句的 completion audit 先 prepare，每 8 条一次批量提交、
  共用一次 fsync；pipeline 对外响应前 flush；写/open/close/outcome_unknown 保持同步路径，
  committing step 后 Seal。
- **耐久语义变更（协调者批准）**：只读成功 completion 的持久化时点从“执行后立即”改为
  “pipeline 响应前”。已确认响应仍满足“响应前 durable”；未响应/崩溃时可能丢失已执行只读
  step 的审计记录（无数据效果、无 sequence 缺口）。不得宣称“所有已执行语句必有审计”。
- **受控 A/B/B/A（同容器同数据，n=15 中位数）**：
  - B1 json-pipeline：base1 57.742ms、base2 37.427ms；cand1 22.592ms、cand2 23.322ms
    → 相对基线 −37.7%~−60.9%，两轮同向。
  - B1 protobuf-pipeline：base1 45.661ms、base2 37.799ms；cand1 19.946ms、cand2 22.084ms
    → −41.6%~−56.3%。
  - B1 仍高于 20ms 目标（约 22.6–23.3ms），但过 ≥10% 验收门槛。
- **机制证据**：B1 每请求 audit fsync 22 → 5（扣 fixture discovery）；operation journal fsync
  保持 4/请求不变；F1 JSON 12.794→7.822ms（−38.9%）、protobuf 12.721→5.868ms（−53.9%）；
  X1 16 并发 91.84→61.33ms（−33.2%），4 级均 −11%~−33%；
  H1 JSON 438.99→446.75ms（+1.8%，噪声内）、protobuf 410.05→409.86ms（−0.04%）。
- **wire 字节**：B1 JSON 参考 4,968（实测 4,973–4,988，差异仅 `query_duration_ms` 数字位数）、
  protobuf 686；F1 312/40；H1 25,162,268/23,600,883。
- **质量门**：所有运行 `warnings=[]`、无 429、无 `audit_unavailable`、`session_seats=0`、
  `active_permits=0`；审计链 111,185 条记录连续、无重复/缺口。
- **剩余风险 / 未验证**：
  1. v2 `completionReservedMaximum=64`，单 pipeline 最多持 7 条未 flush completion，
     约 10 条并发长 pipeline 可能吃满 slot 导致无关请求 503；本轮容器为 v1，v2 未做并发容器验证。
  2. 反序边界（写之前只读批次 flush 失败）未被 `TestPipelineAuditBatchMixedWriteDoesNotMask`
     覆盖，verifier 判为非阻塞但需记录。
  3. 跨请求审计顺序按 flush 顺序，不再严格等于执行顺序（v1 单请求内顺序保持）。
  4. 未跑 W1 写场景容器 A/B；写路径由全量 Go 测试与混合写测试覆盖。
- **测试**：`go test ./...` 仅 2 个已知基线失败
  （`TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`、`TestRegistryEntriesStillExist`）；
  新增 9 个测试与 `-race` 通过；harness 离线测试 16/16。
- 容器最终恢复 r2（SHA `c35ce353…`）。

#### R5：pipeline 连续只读 execute 的 worker 批命令（2026-10-31）

分支 `perf/issue28-r5-worker-read-batch`，commit `acc2745`（测试补充 `d739a3d`），merge `e31543a`（已合入 `main`）。

- **方案**：worker 新增私有命令 `capabilities`（`{"readonly_batch_protocol":1}`）与
  `guarded_readonly_batch`（需 session；≤8 条语句、≤128KiB）；Go 平台把 pipeline 中连续顶层只读
  execute 规划为一个 block，一次 worker RPC 取回全部结果，再逐条走原有 `dispatchLocked`
  （鉴权、placement/session_moved、permit、审计、settlement）。批内逐条重新判定只读；遇写、事务、
  连接控制、`RETURNING`、已知写语句或非法参数即截断回退单条。worker 不支持时以精确 400 回退，
  负缓存 60s，旧 worker（v1.1.24）行为不变。
- **不变式**：每条语句仍有独立 operation/request ID 与审计证据；审计记录数与 flush 时点不变
  （每 8 条一次、响应前 flush），journal 不变；批命令纳入 durable digest，嵌套整数字面量归一化一致。
- **验收（两轮 A/B，同容器同数据，n=15 中位数）**：
  - 第一轮 A/B/B/A：B1 json base 27.021/17.590ms、cand 15.202/15.952ms；protobuf base
    23.445/18.547ms、cand 15.194/16.455ms。相对基线中位数 −28.5%/−21.6%；相对最快基线轮次
    json −9.3%（差 0.121ms，未过严格阈值）、protobuf −11.3%。候选四轮全部 <20ms。
  - 第二轮配对交错（base→cand→cand→base→base→cand→cand→base；该窗口基线整体偏慢，B1
    23.6–27.0ms）：B1 json 配对改进中位 **+10.46%**（8.83/34.77/10.67/10.24，4/4 为正）；
    protobuf **+23.42%**（25.80/27.89/21.04/15.23，4/4 为正）。该窗口候选 json 3/4 轮 ≥20ms
    （21.2–23.9ms）、protobuf 1/4 轮 20.019ms；<20ms 仅作报告项，不作为合并门槛（与 R4 口径一致）。
  - 回归：F1 json/protobuf、X1 1/4/8/16、H1 json/protobuf 均无 >10% 回归。配对轮中 F1/X1/H1
    的“候选更快”与验证轮方向相反，归因主机漂移，不计入 R5 收益。
- **机制证据**：B1 每请求 audit fsync 5.167 次（基线/候选/回退完全一致，未下降），journal sync
  4.111 次/请求；候选 B1 15.2–23.9ms vs 版本 skew 回退（r5 平台 + 原 worker）25.7–26.1ms，
  收益依赖候选 worker；回退路径功能与 wire 字节正常。
- **wire 字节**：B1 json 4,980–5,037（参考 4,968，差异仅 `query_duration_ms` 位数）、protobuf 686；
  F1 312/40；H1 25,162,268/23,600,883。
- **质量门**：34 轮运行（18 验证 + 16 配对）全部 fixture PASS、`warnings=[]`、无 SQL 错误/429/
  `outcome_unknown`/`session_moved`/`audit_unavailable`，结束 `session_seats=0`、`active_permits=0`；
  审计链 122,435 / 130,759 条连续无缺口。
- **剩余风险 / 未验证**：
  1. 候选 worker 用 Rust 1.99.1 构建（仓库固定 1.98.1），二进制与发布镜像不可比；结论仅对该
     候选二进制成立，发布前需用固定工具链重建并复测。
  2. 仅 v1 受益；v2/writer profile 不启用批路径。v2 `completionReservedMaximum=64` 下块级预保留
     最多 8 槽，约 10 条并发长 pipeline 可能 503（与 R4 同类风险；v1 上限 1024，风险低）。
  3. capability 正缓存 60s 不主动失效；worker 升级需缓存过期或平台重启。降级依赖精确 400 + 负缓存。
  4. >512KiB 请求体由传输层拒绝为 `invalid HTTP request`（非精确 400），平台保守判 ambiguous；
     Go 侧 128KiB 上限使正常路径不触发。
  5. 未测 C1（10 executes）与 W1 写场景的容器 A/B；批路径由单测、混合写截断测试与回退对照覆盖。
- **测试**：`cargo fmt/check/clippy` 通过；Rust lib 672 通过（2 个既有 flaky 单独跑通过）；
  `statement_worker` 34/34；Go `go test ./...` 仅 2 个已知基线失败；新增 14 个 Go 测试与
  9 个审计批测试；harness 离线 16/16。
- 容器最终恢复 v1.1.25 平台（`f5d2d15` 构建，SHA `d7c26cab…`）+ 发布镜像原 worker（`79e058bd…`）。

#### R6：只读 cursor 页直接转码 Hrana JSON/protobuf（2026-11-01）

分支 `perf/issue28-r6-cursor-page-transcode`，commit `c777f76`（span 校验）、`e062d0b`（直接转码），
merge `fffd305`（已合入 `main`）。

- **方案**：只读 cursor 页不再物化为 `[][]any` 或页级 `RawMessage`；`hranaCursorPageScanner` 在页字节上做
  span 校验（`validCursorFetchResponse` 语义不变），Hrana JSON 与 protobuf 按 span 直接转码进复用缓冲，
  fused first page 同路径。diff 仅 8 个 cursor 相关文件；pipeline 处理器 `hrana_http.go`/`hrana_protobuf.go`
  不在其中。
- **验收（同容器同数据，n=15 中位数，两轮基线/候选）**：
  - H1（20MB 单行）json base 473.993/465.679ms → cand 333.225/331.893ms（同轮配对 −28.7%~−29.7%）；
    protobuf 421.143/422.912 → 277.476/275.060ms（−34.1%~−35.0%）。
  - H3（content+projection）json 470.949/466.744 → 327.868/323.436ms（−30.4%~−30.7%）；
    protobuf 430.074/416.078 → 282.581/278.872ms（−33.0%~−34.3%）。
  - 主指标 PASS：`max(cand) ≤ 0.90 × min(base)`——json 333.225/465.679=0.716，protobuf
    277.476/421.143=0.659；对最快基线轮次同样达标（0.747/0.680）。
- **回归门（健康环境复测）**：首次在退化环境中出现的 W1 insert +49.6% 未能复现。新卷 A/B/B/A：
  base 25.137/26.056ms、cand 23.154/30.991ms（运行中位 25.6→27.1ms，+5.8%，两次运行不重叠且单轮噪声
  ±30–50%）；随后 8 轮配对（B C C B ×2）base 中位 38.41ms vs cand 37.44ms（−2.5%，base 含 74.097ms
  离群轮）。B1 候选 25.127/25.337ms vs 基线 35.418/20.562ms；X1 候选 13.180/12.800ms vs 基线
  19.287/11.802ms——均无候选回归；两组首个运行均为基线且显著偏慢，存在冷启动/主机漂移，不宣称
  pipeline 收益。机制上 R6 diff 不进入写或 pipeline 处理路径，无法解释 W1 回归。
- **正确性**：新增 10 个测试/104 子测试；4000+2000 随机差分；JSON/protobuf 输出与旧路径逐字节一致；
  微基准页校验 −77%、转码 −49%/−57%。
- **测试**：`cargo fmt --check`、`go build ./cmd/peri-platform ./cmd/peri`、cursor 定向 `go test`、
  harness 离线 16/16 通过；Go 全量仍只有 2 个已知基线失败。
- **环境事件（不归因 R6）**：原 `peri-loom-issue28` 卷在一次 startup 失败后留下 clean marker + dirty
  seal；后续 `--recover` 被 clean marker 放行，authority 在写入 dirty marker 后因 dirty seal 拒绝启动，
  使卷进入需要 S05 evidence 的状态。R6 复测改在新卷 `peri-loom-issue28b-data` 完成；实验期间旧卷、marker 与
  失败证据保持原样，清理阶段两个测试卷已删除（证据见 `/tmp/peri-issue28/recovery-inspect`，详见“当前交付缺口”）。

#### R7：只读 cursor 首步省略 describe（2026-11-01，未合入）

分支 `perf/issue28-r7-cursor-describe-elide`，commit `67e5d4b`（基于 `9c922f4`），23 文件 +609/−91，
新增 `hrana_cursor_describe_elision_test.go`。只读 cursor 首步不再发 `describe`；worker 对写语句返回
新错误 `RequiresWriteCursor`，gateway 归为 `cursor_write_required`（400）后回退 `describe` + 写 cursor；
同步 `sqlcontract` 谓词、`web/sqlcontract.js`、`sdk/typescript/client.ts`。

- **未测候选（环境限制）**：宿主在磁盘事故后只剩 8.1 GiB，构建 linux/arm64 候选镜像需 6–8 GiB。
  改测「被省略动作本身的成本」：`/v1/metrics` 的 `action_latency_ns_total`（v1.1.26 镜像，100 次串行
  `session_by_id`）给出 `describe` 均值 **0.391ms/次**（`n=101`）。即该改动的**上限**是单查询
  **16.4ms 的 2.4%**，低于协议 10% 门槛，也低于 A/B 噪声下限。
- **决策：不合入**。2.4% 的收益对应 609 行改动、一个新的对外错误码和一个新的审计抑制条件；
  分支与提交保留作证据。
- 自测（交付时）：`cargo fmt --check`、`cargo check`、`clippy -D warnings`、`go build` PASS；
  `cargo test` 674 passed / 1 failed（`object_store::local::pack::tests::actual_store_lock_serializes_two_independent_processes`，
  磁盘满导致无法在基线复跑确认，列为已知 flaky）；`node --test` 30/30 PASS。
- 两处与设计不符已记录：实测屏障为 3 audit / 4 journal（在 Go mock 路径，与 R8 的 4/4 不是同一流程，
  以 R8 的 `/v1/metrics` 口径为准）；未实现「新标志」（旧 worker 遇未知字段返回 400 会让网关走
  `outcome_unknown`，反而污染会话）。

#### R8：小查询 8 次屏障的归因（归因轮，无代码改动）

- 11–12ms 的串行小查询中 **7.94ms 是 8 次 fsync**：audit 4 次 × 0.942ms + journal 4 次 × 1.042ms，
  占 75–81%。归属：`open` 1 audit + 2 journal、`close` 1 audit + 2 journal、`cursor_start` 1 audit、
  `statement` 1 audit。`operation_journal_coalesced_total=0`、`batch_max=1`。
- **判定：8 次屏障语义上全部必需，严格口径可省 0。** 依据：`open` intent 必须在 dispatch 前 durable
  （否则 worker 已建会话而平台无记录）；合并成单条 pre-dispatch 记录会让每次正常重启都隔离命名空间
  （`state.go:75-98` + `gateway_operations.go:121-129`）；4 条 audit 分属 4 个独立事实；`/v3/cursor`
  是流式（`hrana_v3_json_cursor.go:416-443`/`700-723` 先 flush baton 与行），无法把 `statement`
  探针审计提前合批。
- 三条需要契约变更的方向（只读会话省略 open/close journal 省 4.17ms；v2 unified completion 省 1.88ms；
  `/v3/cursor` 首帧前 audit collector 省 0.94ms）**均未实施**，代价分别是丢失 durable session 清单 /
  失败分类变 Unknown（与 `prototypes/v1/platform/README.md:486-491` 冲突）/ 需新设计与故障测试。
- **核心结论**：即使 8 个屏障全消失，peri 仍 5.5ms vs sqld 3.7ms ≈ **1.49×**，恰在 1.5× 门禁线上。
  小查询差距是持久化模型的价格，不是编码效率问题。

#### R9：单库并发下的扩展性与错误分类（2026-11-01，测量轮）

**方法**：同机（macOS 26.6.2 / arm64 / 10 vCPU）、Docker host network、固定镜像 digest
（peri `sha256:c7614c0b…`、sqld `sha256:6dd3eb27…`），数据为 `/tmp/threads-subset.db` 重新导入全新卷
（导入 11.6s，验证 8,625 messages）。每个「查询」= `connect → all → close`（与串行基准同一 SDK 流程）；
每档 320 次查询，`CONC` = 1/8/32/64。bench 项目的 `max_concurrent`/`max_sessions` 临时提到 256
（仅限一次性 bench 卷，与产品默认值无关）。

| 并发 | loom qps | loom p50 | loom 失败 | sqld qps | sqld p50 | sqld 失败 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 34.3–53.9 | 18.4–31.7ms | 0 | 157.4 | 5.93ms | 0 |
| 8 | 143–161 | 48.7–51.5ms | 0 | 1,510.5 | 4.83ms | 0 |
| 32 | 113.2 | 221.2ms | 114/320（35.6%） | 2,168.6 | 13.06ms | 0 |
| 64 | 70.6 | 427.0ms | 126×502 + 114×429 | 2,049.3 | 27.21ms | 0 |

- **并发下屏障完全不合并**：`operation_journal_syncs_total` 恒为 4.04 次/查询（8 路并行时也一样），
  `coalesced_total=0`、`batch_max=1`、`queue_max=1`；audit 侧确实合并（每请求 fsync 0.80→0.59），
  所以瓶颈在 journal 侧。
- **结构性原因**（仓库内已有记录）：`operation-journal-commit-queue-experiment.md` §2——按命名空间的
  fence（`lockOperationNamespace`，64 条带）包裹每次 operation commit，且**横跨屏障持有**，
  因此同一命名空间的两个 commit 永不重叠、无法共享一次 fsync；合并只对跨命名空间或 fence 外的追加生效。
- **对照**：同机 sqld 在 8 路并发即 1,510 qps（loom 的 10.6 倍），32 路 2,168 qps（19 倍）且零失败。
  loom 的串行成本 65% 是 fsync（100 次串行：journal 5.94ms + audit 4.69ms = 10.63ms / 平台侧 14.32ms），
  并发下这部分只能排队，所以吞吐上限 ≈ 1 条 fsync 链（约 143 qps）而非 CPU 上限。
- **同期发现的缺陷（2026-11-05 已修复）**：worker 在**执行前**拒绝 `open_session`
  （`database session capacity exceeded`，`src/runtime.rs:196`）时，回复不携带 `execution_completed`
  证据，平台按 `sqlWorkerFailureFromReply` 只能判为 ambiguous，于是 `/v1/sql/open` 返回
  **502 `outcome_unknown`**（日志证据：
  `SQL outcome unknown: ... action=open stage=open_session ... observation=database session capacity exceeded`）；
  客户端无法区分「可能已提交」与「明确未执行」。
  **修复**：分支 `fix/r9-pre-execution-evidence` / `d4ef13d`，PR #33，merge commit
  **`d1d1b7d`**。worker 的 `open_until` 与 dispatch prelude 补 `SqlEvidence::NotExecuted`，
  复用既有 `WorkerResponse.execution_completed` 字段（**未新增字段、未改命令、未改状态码集合**）；
  平台短语表加两条容量文本 → **503 `sql_unavailable`**。执行前拒绝现在给出确定性「未执行」
  （`execution_completed=false`、`may_have_committed=false`），记录为 `terminal/not_executed`，
  不再 `holdOperation`。**明确未改**：`CloseSession`（close 失败仍 502 `outcome_unknown` 并隔离）、
  配额 16/128/16、耐久时点、隔离规则、Turso gitlink `24a3a460…`。
  CI：PR runs `41843969071`（baseline）、`41843968853`（Issue 8）、`41843932858`（push），
  post-merge `41847588930`、`41847588721`，全部 success（Issue 8 的 `quick-native`/`formal-native`
  是 manual-only，PR 上 skip 属设计行为）。
  **原保留的已知缺口（①② 已修复，③ 保留）**：① worker 侧 **close 没有回归测试覆盖**——把
  `CloseSession` 也包上证据后，68 个 `worker_rpc` 测试与新增的 Go 负向用例**都不会转红**，
  当时 close 安全只靠「分支 diff 为空」这一结构事实。
  **已修复**（`18e4d77`，merge `0ccd0c2`）：新增
  `tests/statement_worker.rs::a_failed_close_carries_no_execution_evidence`，用真实 HTTP worker
  关闭一个该 worker 从未签发过的合法 64-hex 会话（`sessions.close` 在 registry 处以
  `unknown worker session` 拒绝，位于准入之后、任何 SQL 之前），断言失败回复不含
  `execution_completed` 字段；给 `CloseSession` 临时包上 `not_executed` 会让该用例转红
  （`42 passed; 1 failed`），还原后 43/43——close 安全不再只靠 diff 为空。
  ② `not_executed` helper 会覆盖内层 `SqlEvidence::Settled`（实测 `downcast=Some(NotExecuted)`）。
  **已修复**（`18e4d77`，merge `0ccd0c2`）：helper 先 `downcast_ref::<SqlEvidence>()`（`anyhow`
  的 `downcast_ref` 搜索整条 context 链），错误链任意层已有证据就原样返回，只在完全没有证据时
  补 `NotExecuted`（与 `session::ExecutionEvidence::failure` 的保守写法一致）；
  `prepare_guarded_batch` 改用同一 helper（对今日可达输入逐字等价）。新增
  `a_wrapped_settled_failure_keeps_its_execution_evidence` 与
  `a_wrapped_refusal_keeps_a_single_proof_prefix` 两条用例，并用变异证明：回退实现后
  `11 passed; 2 failed`。
  ③ `OpenOwnedSession` 的 binding 失败会按「未执行」释放配额，而 worker 已插入会话——极小概率
  孤儿会话，实际不可达，**仍为已知缺口**。
  本轮**明确未改**：`CloseSession` 行为（close 失败仍 502 `outcome_unknown` 并隔离）、配额
  16/128/16、耐久时点、隔离规则、协议字段、Turso gitlink `24a3a460…`。
  测试口径：`cargo test --lib` 4 次全量中 1 次 686 passed / 0 failed，另 3 次 685/1 为既存 flaky
  `authority_rpc::server::batch_tests::authenticated_batches_are_registered_only_by_live_writers`
  （单跑 3/3 通过），**不宣称全量稳定全绿**；`statement_worker` 43/43、`writer_worker_rpc`
  10 passed / 2 ignored，fmt/clippy 干净。CI：PR #34 runs `41857097197`（baseline）、
  `41857096878`（Issue 8），post-merge `41860190838`、`41860190480`，全部 success。
  另需注意网关 `max_sessions` 配额可以高于 worker 每库上限（默认都是 16，属一致；本次把网关提到 256
  才暴露不一致）——现在这类拒绝被如实分类为 503 `sql_unavailable`，但配额本身的不一致仍未消除。
- **逐 action 分解**（串行 100 次，v1.1.26）：`open` 5.276ms、`close` 4.596ms、`cursor_start` 2.044ms、
  `statement` 1.868ms、`describe` 0.391ms，合计 14.32ms。`open`+`close` 占 69%，几乎全由 fsync 决定。
- **待观察**：同期串行 100 次查询的延迟从 11.5ms 漂到 30.4ms；journal compaction 2 次（19.3ms）、
  audit head checkpoint 13 次（79.3ms，0.79ms/查询）不足以解释全部漂移，未归因。
- **候选方向（需授权）**：
  1. 把屏障移出命名空间 fence，使同库 commit 可共享一次 fsync。预期：journal 5.94ms/查询 → 约 1.5ms，
     并发吞吐数倍提升。代价：改变 fencing 语义（后续同库操作可能早于前一条记录 durable 就被 dispatch），
     仓库文档已明确列为「需评审」。
  2. 仅修上述 502 误分类（Rust 证据携带），风险低，不改变任何耐久时点。
     **已于 2026-11-05 实施**（`d4ef13d` / PR #33 / merge `d1d1b7d`，见上一条「已修复」）。

#### R10：SQL 提交在屏障前释放命名空间 fence（2026-11-01，已合入 `main`）

**改动**（分支 `perf/issue28-r10-fence-barrier-split` / `b79419f`，合并 `a9c81bf`）：

- `operation_store.go`：`updateOperationsAudit` 拆成 `enqueueOperationsAudit`（应用编辑、定序，返回 commit）
  与 `awaitOperationCommit`（付屏障、结算、compaction、poison 判定）。后者逐字保留原尾部逻辑与错误分支。
- `operation_state.go`：新增 `submitOperationRecords`，仅 `beginCorrelatedOperation`（intent）与
  `updateOperation`（settle）在 `wait()` 之前释放 fence。控制面 `release` / `retirement` / `transport`
  仍然横跨屏障持有 fence，未改。
- 不变量：定序仍由提交时的状态锁决定（revision 链不变）；客户端在 `wait()` 返回后才收到回复，
  即「先 durable 再应答」不变；屏障失败仍 poison 并 fail-closed，同一批次内的多条记录原子生效。
- 测试：`TestOperationNamespaceFenceReleasesBeforeBarrier` 取代旧的
  `TestOperationNamespaceFenceStillSerializesCommits`（旧测试断言的正是被推翻的语义）；
  新增 `TestSameNamespaceSettlesShareOneBarrier`——同命名空间两笔 settle 在 gate 下排队，
  要求恰好 1 次 fsync、coalesced=1，并在 `OpenStateStore` 重开后可恢复出两条 terminal 记录。

**测量**：同一镜像 digest、同一 bench 卷（`/tmp/threads-subset.db` 重新导入）、同一导入校验，
只切换 `/usr/local/bin/peri-platform`（候选为本地交叉编译的 linux/arm64 静态二进制，Rust worker 仍用镜像内版本）。
A/B 交替 3 轮（conc=64 为 2 轮），每档 320 次查询，`connect → all → close`。

| 并发 | 基线 qps（中位） | 候选 qps（中位） | 变化 | 基线 p50 | 候选 p50 | 变化 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 54.8 | 60.9 | 无回归（噪声内） | 17.5ms | 14.4ms | 噪声内 |
| 8 | 150.5 | 178.6 | **+19%** | 50.7ms | 43.6ms | −14% |
| 32 | 74.2 | 151.7 | **+104%** | 422.3ms | 212.3ms | **−50%** |
| 64 | 260.3 | 344.7 | **+32%** | 233.7ms | 168.4ms | **−24%** |

- 机制指标（conc=32）：`journal_sync` 0.80→0.48 次/请求，`coalesced` 0→442，`batch_max` 1→15，
  `queue_max` 1→15；conc=64 时 `batch_max` 触顶 32、`queue_max` 62，说明队列上限而不是 fence 成为新边界。
- 三轮全部同向：conc=32 时候选最差一轮（132.3 qps / p50 246ms）仍优于基线最好一轮（87.4 qps / p50 367ms）。
- Go 全量套件（`PERI_RUST_BINARY` 指向不存在的路径）失败集合与基线**逐条一致**：18 项，全部是缺 Rust
  二进制的 native 测试 + 已知既存失败 `TestHranaGatewayResponseMatchesEnvelope`。
- **未覆盖**：native 集成与 crash-recovery 端到端（需要 linux/arm64 Rust 二进制，本机磁盘不足无法构建）。
  该缺口由 Go 侧提交队列/恢复单测覆盖，但不等价于真实进程崩溃演练。

**顺带发现**：`dev.py` 的 `experimental_environment` 只转发固定名单的 `PERI_*`，`PERI_WORKER_MAX_DB_SESSIONS`
不在其中，因此 worker 每库会话上限固定 16，而网关 `max_sessions` 配额可被调到更高——本次压测为了
conc=32/64 干净可比，仅在 bench 侧挂载了改过的 `dev.py` 副本（未进仓库）。产品侧的「配额可高于 worker
上限 → 执行前拒绝被记为 502 outcome_unknown」已于 2026-11-05 修复（`d4ef13d` / PR #33 /
merge `d1d1b7d`，现为确定性 503 `sql_unavailable` 且 `may_have_committed=false`）；但
「`dev.py` 不转发 `PERI_WORKER_MAX_DB_SESSIONS`、网关配额可高于 worker 每库上限」这个不一致本身仍是待修项。

**环境事件（2026-11-01 23:03，不归因任何轮次）**：宿主磁盘耗尽（曾低至 473 MiB / 228 GB），
OrbStack VM 出现 BTRFS 写 I/O error（`Transaction 19922 aborted`）并被强制停机。
`peri-cf-peri-loom-1`（v1.1.24，另一个项目的容器）因此变成 `Exited (255)`，其卷
`peri-cf_peri-loom-data-v9` 的最后一次事务存在丢失可能——**未触碰、未重启、未校验**。
后续只清理本项目构建缓存（`cargo clean` + `go clean -cache`，回收 8.4 GiB），并仅启动 OrbStack
（未重启 daemon、未 prune）。事故后可用空间 8.1 GiB / 97% 占用，这是 R7 候选无法构建、R9 只能用
已发布镜像的原因。

#### R11：会话开关只写结果记录（2026-11-01，已合入 `main`）

**归因**：相位剖面（`--sql-open-observer`，采样 1000‰、阈值 0）把串行一次
`connect → all(SELECT …) → close` 的 5 个请求拆开，每查询网关侧约 22ms：

| 动作 | 合计 | intent | terminal | audit | native |
|---|---:|---:|---:|---:|---:|
| open | 8.04ms | 2.43 | 2.35 | 2.05 | 1.08 |
| statement | 2.86ms | — | — | 2.08 | 0.62 |
| cursor_start | 3.20ms | 0.04 | 0.00 | 2.08 | 0.94 |
| describe（探针） | 0.72ms | — | — | — | 0.55 |
| close | 7.30ms | 2.23 | 2.32 | 2.30 | 0.37 |

即 open/close 各付 **2 次 journal 屏障**，而它们都不可能改数据：open 不下发 SQL，
close 只拆除会话。R8 当年把 8 次屏障判为「语义上全部必需」，是把这两条记录当成
会话账目的一部分；本轮把「账目」与「派发前 intent」拆开。

**改动**（分支 `perf/issue28-r11-session-record-outcome` / `6f8b35d`）：

- 新增 `deferredRecordOperation`（open、close）：不写 intent，由
  `settlePendingOperation` 在调用后写**一条已结算**的记录。
- `beginSessionOpen`、`closeTrackedSession`、`beginDispatchOperation` 走 pending 路径，
  准入仍复用 `verifyNonCommittingAdmission`（close 带 retirement 豁免，隔离库内仍可关会话）。
- `settleNonCommittingOperationUsing` 改为「入队 → 释放命名空间 fence → 等屏障」，
  否则同库两条结果记录会串行（R10 修过的形态）；新增
  `TestSameNamespacePendingSettlesShareOneBarrier` 固定该性质，并要求重开后未解除的 open
  仍按原规则被隔离。
- 完工路径 `needsOperation` 现在也覆盖 deferred 记录，因此在 v2 完成日志
  （`PERI_COMPLETION_JOURNAL=1`）下审计行与结果记录共享一个屏障。**本 bench 卷是
  `audit_file_v1`，该收益未实测**。
- 不变：未知 open 仍不隔离、未知 close 仍隔离、close 仍在同一批次里把 open 的
  `cleanup_outcome` 置 completed、记录仍绑定命令摘要（digest）、恢复期把未解除的 open
  视为可能存活的会话。**放弃的只有**：open/close 在「派发后、回复前」崩溃时不再留下
  in-flight 标记，会话资源转由 worker 空闲回收。
- 测试期望更新：`gateway_probe_test.go` 的三处 sidecar revision 计数（open 由 2 → 1，
  含 close 的用例 4 → 2）。

**测量**：同一镜像 digest、同一 bench 卷，只切换 `peri-platform`。本轮改为**交错** A/B：
同一并发档内按 A,B,B,A 连续起停两侧容器，使宿主噪声落在两侧而不是某一整块扫描上；
2 轮 × 4 档 × 4 样本/侧，每样本 320 次查询。

| 并发 | 基线 qps（中位） | 候选 qps（中位） | 变化 | 基线 p50 | 候选 p50 | 变化 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 32.2 | 41.3 | **+28%** | 30.1ms | 24.1ms | **−20%** |
| 8 | 183.9 | 233.6 | **+27%** | 40.6ms | 32.8ms | **−19%** |
| 32 | 205.1 | 374.9 | **+83%** | 150.7ms | 80.7ms | **−46%** |
| 64 | 256.8 | 504.8 | **+97%** | 232.3ms | 118.2ms | **−49%** |

- 每档 4 个样本同向：conc=32 候选最差 292.7 qps 仍高于基线最好 240.7 qps；
  conc=64 候选最差 462.2 高于基线最好 263.2。
- 机制：`journal_appends` 4.02→2.02、`journal_syncs` 4.04→2.02 次/查询（串行实测），
  conc=64 时 `journal_sync` 0.44→0.15 次/请求、`audit fsync` 0.38→0.10 次/请求
  （少一次屏障后审计行也更容易合批）、`batch_max` 26→27。
- 绝对数字比 R10 低（宿主 load 3.1–3.7，串行基线从 55 qps 掉到 32 qps），
  因此只比较同窗口内的比值。
- Go 全量套件失败集合与基线**逐条一致**（27 项，均为 `PERI_RUST_BINARY` 指向不存在路径的
  native 测试 + 既存 `TestHranaGatewayResponseMatchesEnvelope`）；`gofmt`/`go vet`/`go build` 干净；
  写路径冒烟 `{"ok":true,"rows":40,"sum":2340}`。
- **未覆盖**：native 集成与 crash-recovery 端到端（同 R10 缺口，需 linux/arm64 Rust 二进制）；
  v2 完成日志下的 unified 完工路径未实测。

#### R12：v2 完成日志不再逐条独占屏障（2026-11-02，已合入 `main`）

**归因**：补 R11 留下的「v2 unified 完工路径未实测」缺口。先在 `completion_journal_v2` 卷上做
交错 A/B：v2 串行 +31.7%，但 conc=8/32/64 分别 −38.7%/−55.4%/−65.2%，`journal_sync` 恒
0.76–0.80 次/请求（v1 为 journal 0.40 + 审计 0.80，且随并发下降）。

打开 durability observer（`PERI_DURABILITY_OBSERVER=1`，需同时 `--sql-open-observer`；8 秒高并发
窗口、7,190 次完成提交）读到：

| 观测项 | 值 |
|---|---|
| journal 批次 | 1.05 条/次 fsync（`batch_max` 4） |
| committer 忙碌占比 | 86.5%（858 次 fsync/s，sync 均值 1.00ms） |
| prepare 并发 | 最多 4（p50 12.9µs，state lock 均值 124µs） |
| 请求相位 | open/close 的 `audit` 段均值 284ms；intent/native 各约 1ms |
| 记录数 | 4 条/查询（1,284 条 / 320 查询）；v1 为 journal 2 条 + 审计 4 条 |

即 v2 的完工记录在 `unified` 分支里由 `persist` 闭包**在命名空间 fence 内**完成「提交 + 等屏障」。
同库所有请求共用一个 fence，于是每个请求要独占一次 fsync 才轮到下一个：队列永远只有 1 条，
吞吐 = 1/fsync 延迟，与并发无关。v1 没有该问题——同库 SQL 完成走审计文件（不持 fence），
journal 只承载 open/close。

**改动**（分支 `perf/issue28-r12-completion-journal` / `c43d48c`）：

- `auditReservation.RecordOperation` → `RecordOperationDeferred`：只做
  `prepareCompletionOperation` + 入队（新增 `fileAudit.enqueueCompletion`），把屏障作为返回值
  交给调用方。
- `gateway_http.go` 的 unified `submit` 直接返回该屏障；`settleOperationUsingCaller` 改为
  「入队 → 释放 fence → 等屏障」，与 R10/R11 的 `settleNonCommittingOperationUsing` 同形。
  v1 路径包装为 `return nil, updateOperationRecords(...)`，语义不变。
- `recordUnifiedBatch` 改为**按审计队列顺序**逐条提交 delta、收集屏障后并行等待（原来每条一个
  goroutine，各自提交、各自等待）。顺序即 fence 顺序：`audit.pending` 是 FIFO 前缀，记录在
  fence 持有期间入队，因此视图推进与审计链顺序不变。
- 新增 `TestUnifiedCompletionDropsNamespaceFenceBeforeBarrier`：同库两个 close，用 committer
  闸门把第一个关在屏障里，断言第二个仍能完成 admitted 入队；把等待挪回 fence 内该用例即超时失败
  （已做变异验证）。
- 语义：fence 只保证「同一命名空间的记录按 fence 顺序入队」，不再要求上一条持久化后才放行；
  读取路径仍先 `settleOperations` 等未确认提交，因此读不到未持久化的视图。
- 顺带闭合 R11 的「v2 未实测」：v2 下 open/close 结果记录与审计行共享一个屏障，本轮实测生效。

**测量**（同一 `completion_journal_v2` 卷、同一镜像 digest、只切换 `peri-platform`；交错
A,B,B,A，2 轮 × 4 档 × 4 样本/侧，每样本 320 次查询）：

| 并发 | 基线 qps（中位） | 候选 qps（中位） | 变化 | 基线 p50 | 候选 p50 | 变化 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 50.2 | 53.8 | +7.1% | 19.3ms | 18.0ms | **−6.7%** |
| 8 | 115.7 | 177.4 | **+53%** | 62.7ms | 37.5ms | **−40%** |
| 32 | 193.6 | 443.6 | **+129%** | 156.6ms | 66.5ms | **−58%** |
| 64 | 140.4 | 546.2 | **+289%** | 430.0ms | 105.6ms | **−75%** |

- conc=32/64 两侧样本完全不重叠（基线最好 233.6/169.5 低于候选最差 335.9/499.2）；
  conc=8 有一枚基线样本 191.2 高于候选最低 173.6，中位仍 +53%。
- 机制：`journal_sync` 0.74–0.77→0.10–0.17 次/请求（c32/c64）、0.75→0.42（c8），串行仍 0.80；
  `coalesced` 40–92→1000–1120；`batch_max` 3–4→16–32。
- 同窗口跨格式（两侧都用 r12；v1 `audit_file_v1` vs v2 `completion_journal_v2`）：
  串行 38.2→45.9 qps（+20%）、conc=32 349.0→398.7（+14%）、conc=64 433.3→550.1（+27%），
  **conc=8 仍是 247.2 vs 185.5（−25%）**：该档 v2 的 journal 批次只有 5–6 条，每条完成仍各等
  一次屏障，差距留作下一轮（R13 复核判定该缺口不复现，见下）。
- v1 卷同二进制对照（`audit_file_v1`，r11 vs r12）：c8 +1.5%、c32 +1.1%、c64 +5.4%
  （4 样本/侧），无回归。
- Go 全量套件失败集合与基线**逐条一致**（27 项）；`gofmt`/`go vet`/`go build` 干净；
  v2 卷写路径冒烟 `{"ok":true,"rows":40,"sum":2340}`。
- **未覆盖**：native 集成与 crash-recovery 端到端（同 R10/R11 缺口，需 linux/arm64 Rust 二进制）。

#### R13：复核 conc=8 的 v2 遗留缺口（2026-11-02，测量轮，无代码改动）

**背景**：R12 记录同窗口跨格式 conc=8 为 v1 247.2 / v2 185.5 qps（−25%），列为下一轮目标。

**方法**：改用多采样协议——同一容器内连续 5 次 320 查询为一组、两格式交替 2–3 组，比较中位数与
分布，而不是「每档 4 样本、每样本一次容器重启」的交错 A/B。后者在本宿主上的方差与格式差异同量级：
同一配置同一卷可测出 112–322 qps。

| 并发 | v1 中位 qps | v2 中位 qps | 变化 | v1 p50 | v2 p50 | 样本数 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 38.35 | 45.91 | **+20%** | 24.7ms | 20.6ms | 10 / 10 |
| 8 | 278.77 | 285.38 | +2.4% | 27.7ms | 26.5ms | 15 / 15 |
| 32 | 403.44 | 530.20 | **+31%** | 72.9ms | 56.4ms | 10 / 10 |
| 64 | 488.00 | 658.42 | **+35%** | 119.1ms | 86.4ms | 10 / 10 |

- conc=8 两者中位接近，但 v2 分布稳定（250.2–297.1 qps），v1 有 4 个 ≤200 的样本（112.0–196.6）。
- R12 的「−25%」是宿主噪声叠加 v1 的持续负载退化：c8 每样本 v1 要 1,420 次 fsync
  （审计 756 + journal 590，两路并行），v2 只需 709 次（单路）；同一容器连续 5 次测量时
  v1 的 fsync 均值从 0.86ms 涨到 3.05ms（qps 312→112），v2 稳定在 1.03–1.28ms（qps 285–297）。
  写回积压随 fsync 次数累积，这是 v1 每查询 6 条记录 vs v2 4 条的直接代价。
- 试过 `fdatasync`（Linux 数据屏障；`syncAppendData`，非 Linux 回退 `Sync`）：微基准
  grow+fsync 1.42ms → grow+fdatasync 0.84ms、prealloc+fsync 0.66ms，但生产路径 sync 均值
  1.12→1.14ms、c8 qps 280.4→272.4（8 样本/侧，噪声内），无收益，已回退，未提交。
- 协议修正：宿主噪声大时不要用「每样本一次容器重启」的 4 样本 A/B 定论；用同容器连续采样 +
  中位数 + 分布，并记录每样本的 `sync/q` 与 sync 均值，便于识别写回积压。
- **未覆盖**：与 R12 相同的 native 集成与 crash-recovery 端到端缺口。


#### R14：把审计 drainer 的屏障等待移出循环（2026-11-02，失败，已回退）

**假设**：c64 观测到每个动作的 `audit` 段 11ms，其中 journal member wait 只有 2.7ms；
`audit_batch` 每批 11 条、批间隔 9.2ms。drainer 在批内同步等屏障，导致下一批要等上一批屏障
返回才成形，每个完成记录额外付一次批次间隔。计划：drainer 只按审计顺序提交，把屏障等待交给
批次 goroutine，准入额度在屏障返回后才归还（反压不变）。

**结果**：c64 中位 **574.08→428.50 qps（−25%）**、p50 100.1→136.8ms（同卷、r12 vs 原型、
2 组 × 5 样本/侧）。c8 及以上未再测。

**原因**：流水线变深后 `store.unconfirmed` 里同时挂着的未决提交变多，而每次读状态与每次
`awaitOperationCommit` 都会在全局 mutation 状态锁下调用 `settleOperations()`、按序等待**全部**
未决提交；更长的未决栈把状态锁的持有时长放大，抵消了批次重叠的收益。要让这个方向成立，必须
先把「视图只在提交落定后可见」改成不需读者等待的实现（应用到视图的时点后移），那是操作存储的
结构性改动，不在本轮范围内。

**结论**：回退，无代码合入。后续若重试，验收门槛是 c64 中位 ≥ 574 qps 且 c8/c1 不退化。


#### R15：认证与路由元数据读脱离全局状态锁（2026-11-02，失败，不合入）

**假设**：c64 每动作 7–9 次全局状态锁读（`state_lock_wait_ns_total` 显示 auth 相位 2.2–3.6ms 基本都是锁等待）。registry 部分只在 commit 时整体替换，因此可发布 `atomic.Pointer[registrySnapshot]`，让 `credentialByDigest`/`authorizationState`/`gatewayRequestState` 无锁读。

**实现**（`48c42b5`，13 文件）：`state_registry.go` 新增不可变快照与 digest 索引；`state.go` 三个窄读改原子读；`state_disk.go` 的 `closed`/`poisoned` 改 `atomic.Bool` 以保持 fail-closed 语义；新增 `state_registry_test.go`（吊销/配额即时可见、并发无竞争、关闭拒绝）。基线失败集 `BASELINE-SET-MATCH`（27 项），`go vet`/`gofmt` 干净，`-race` 相关用例通过。

**实测**（v2 卷，同二进制对 r12；2 组 × 5 样本/侧，每样本 320 查询）：

| 并发 | r12 中位 qps | r15 中位 qps | 变化 | p50 中位 |
|---:|---:|---:|---:|---:|
| 64 | 569.11 | 479.31 | **−15.8%**（两轮 −15.97%/−15.24%） | 104.3→115.7ms |
| 32 | 393.88 | 463.99 | +17.8%（逐轮 +7.4%/+34.3%，不稳） | 76.9→64.3ms |
| 8 | 163.18 | 167.05 | +2.4% | 46.8→45.9ms |
| 1 | 41.91 | 42.34 | +1.0% | 23.4→23.4ms |

**机制核对**：状态锁获取 **10.0→2.8 次/请求（−72%）**，c1 等待 0.066→0.0004ms、c32 4.22→2.20ms，说明改动确实生效。但 c64 等待反而被拉长（首样本 7.04→9.88ms、峰值 261.6ms、p95 中位 +13.8%、max 465ms），吞吐同向退化——减少锁获取次数并不等于减少锁持有时间，高并发下剩余的 settle 持锁窗口被更长排队放大。

**结论**：不满足「c64 ≥10% 且两轮同向」，不合入，分支保留作证据（`48c42b5`）。绝对 qps 与本轮窗口外（R13）不可比，只可用同窗口比值。**未归因本轮**的环境事实：本次窗口 r12 的 c8 中位 163 qps 明显低于 R13 窗口的 278 qps，宿主状态随测量窗口变化。

**下一轮提示**：同一批代码里 c32 的正向信号说明锁竞争确实是成本，但必须与 D1（把 settle 的等待移出状态锁，见 R14 归因）一起做才能在高并发兑现；单独做窄读只会在 c64 把等待挤到 settle 窗口。


#### R16：落定等待移出全局状态锁（2026-11-02，失败，不合入）

**假设**：R14 与 R15 都指向 `settleOperations()` 在全局状态锁内按序等待全部未决提交。改为写者只回收已确认前缀（journal 顺序保证更早 append 已持久），读者锁外等尾部 commit 后重取锁复查，锁内只做非阻塞回收。

**实现**（`ef6a214` 写者捷径 + `e0e46b7` 读者锁外等待 + `9316044` 测试修正）：`reclaimSettledOperationsLocked`、`rollbackFailedOperationsLocked`、mutation 入口回收、Close 过滤自身拒绝的 `ErrStateClosed`；新增 4 个用例（写闸门、无 settler 的失败回滚、无读者时 compaction 仍触发、已确认前缀回收）。`gofmt`/`vet`/`build` 干净，失败集 `BASELINE-SET-MATCH`（27 项），受影响用例 `-race -count=10` 通过。

**实测**（v2 卷，同二进制对 r12；2 组 × 5 样本/侧）：

| 并发 | r12 中位 qps | r16 中位 qps | 变化 | p50 中位 |
|---:|---:|---:|---:|---:|
| 64 | 594.08 | 478.99 | **−19.4%**（两轮 −16.5%/−19.0%） | 100.4→120.0ms |
| 32 | 435.07 | 334.32 | **−23.2%**（两轮 −31.1%/+/-11.9%） | 66.8→88.6ms |
| 8 | 210.88 | 150.10 | **−28.8%**（两轮 −33.3%/−21.1%，r16 仅 8 样本） | 34.2→48.9ms |

**结论**：三档同向退化，不合入。机制判断：读者「锁外等尾部提交 → 重取锁复查」把一次锁内有序等待拆成多次跨锁往返，尾部提交往往晚于读者真正需要的那个，等尾部等于等最年轻的屏障；写者捷径省下的 settle 又不足以抵消。**D1 方向在两种实现下（R14 drainer 流水线、R16 锁外等待）都退化**，说明「锁内按序 settle 全部未决」不是可轻易移除的成本，而是当前读写可见性模型的承重结构。分支保留作证据（`9316044`）。

**下一轮提示**：若还要碰这条路径，先建立未决提交与读者所需提交的**最小集合**（读者只需要「最后一次影响可见视图的编辑」之前的提交），而不是等尾部；或改为「视图在落定时才对外发布」的结构性重写。两条都需先出设计再实现，不宜直接改热路径。

#### R17：落定只发布自身耐久前缀（2026-11-02，中性，不合入）

**假设**：`settleOperations()` 在每次提交的屏障返回后，于状态锁内按序等待**全部**未决提交（含比自己更年轻的），把单个请求的耐久成本分摊给同批次其它请求。journal 的屏障按提交顺序完成，所以一个提交耐久就蕴含它之前的全部编辑耐久；写者只需确认**自己的前缀**，读者仍需等全部（读不得看见未确认编辑）。

**实现**（`6a98baa`）：`confirmOperationsLocked(through)` 只修剪「已返回屏障且序号 ≤ through」的连续前缀，遇到在途提交即停止、从不等待；`awaitOperationCommit` 以自身 `commit.revision` 为界；`settleOperations` 改为「等全部 + 统一修剪」；Close 不再重复上报自身拒绝的 `ErrStateClosed`（由该提交的等待者承接）。新增用例 `TestOperationCommitConfirmsOnlyTheSettledPrefix`（前者已返回、后者卡在屏障时：确认前者不等待、未确认计数保留后者、读者仍被后者阻塞；旧语义下 5s 超时失败）。`gofmt`/`vet`/`build` 干净，全量失败集 `BASELINE-SET-MATCH`（27 项，连续两次），`-race` 相关包通过。

**实测**（每侧全新 v2 卷 + 同批导入 + 配额，4 组 × 3 样本/侧，每样本 320 查询，conc=64，A/B 逐组交替）：

| 组 | r12 中位 qps | r17 中位 qps | 变化 |
|---:|---:|---:|---:|
| 1 | 522.8 | 499.7 | −4.4% |
| 2 | 439.9 | 514.9 | +17.0% |
| 3 | 517.8 | 522.5 | +0.9% |
| 4 | 498.7 | 490.9 | −1.6% |
| 全体（n=12/侧） | 510.0 | 507.3 | **−0.5%** |

配对中位 −0.3%；p50 逐组 115–132→110–117ms；锁等待/请求 7.74→7.71ms；全部样本 `failed=0`。

**观察器对照**（同协议、每侧新卷、c64、320 查询）：r12 549.5 qps vs r17 587.2；audit 相位 −1.4~−2.4ms/动作；状态锁累计等待 11.34→8.54s（−25%）、单次最大 20.66→16.43ms。首轮同卷 A/B 曾测到 −10%，复核为宿主噪声（同侧样本 280/713/625 qps），已弃用。

**结论**：机制生效（锁等待 −25%、尾部减半），但 c64 吞吐中性，不合入。**状态锁等待不是当前吞吐上限**：平均每次获取只排队 0.7ms，吞吐 ~510 qps 由其它成本决定（audit 屏障 9.5–12ms/动作、native 3–8ms、admission+auth 2–6ms）。分支保留作证据。

**下一轮提示**：v2 完成日志 maintenance（`completion_journal.go:208-231`）在状态锁内做 settle+快照+fsync+truncate，每次 ~12ms、每 320 查询 5 次（`compaction_ns_total` 59.6ms/0.57s ≈ 10% 墙钟）；这 5 次长持有正好解释 c64 的锁等待体量（11.3s 累计）。按 v1 操作日志 `beginOperationCompactionLocked`/`finishOperationCompaction` 的既有拆分，把编码+I/O 移出状态锁，是唯一还没试过的结构性方向。另：320→4000 查询的稳态复核为 513→506 qps（p50 109→119ms、p99 185→218ms），短样本协议有效、无明显累积退化。

#### R18：统一完成日志的 maintenance 移出状态锁（2026-11-02，合入）

**假设**：R17 测到状态锁累计等待 11.34s，而 v2 完成日志的 maintenance 把 settle、旧审计归档、快照 `json.Marshal`、`atomicPrivate`、`fsync` 与 `Truncate` 全放在状态锁内；c64 下每 320 查询 5 次 compaction、`compaction_ns_total` 59.6ms/0.57s ≈ **10% 墙钟**。若沿用 v1 操作日志已有的「锁内捕获、锁外编码+I/O」拆分，长时间锁持有应塌缩为仅 journal writer 独占。

**探针（先验证方向）**：把 `operationJournalCompactRecords` 256→2048 编译成探针（源码已还原），每侧新卷、3 组 × 3 样本/侧、c64、320 查询：逐组 +41.4%/−0.9%/+10.5%，配对中位 +10.5%，全体中位 501.5→569.3（**+13.5%**），p50 109–122→93–110ms，锁等待/请求 7.31→6.25ms。说明压缩的锁内成本确实是吞吐限制之一。

**实现**（`383818e`）：`beginOperationCompactionLocked` 不再排除 completion 日志；维护 goroutine 用 `beginCompletionMaintenance()` 在状态锁内 settle 并捕获压缩任务，再由 `finishOperationCompaction()` 在锁外完成归档、编码、快照落盘、fsync 与截断；`archiveCompletionPrefix` 在 finish 持有 journal 元数据锁、压缩任务仍独占 writer 时执行，保证归档与截断之间不会插入 append；`maintenancePending` 改由 journal 互斥量保护，消除维护 goroutine 在锁外清标志与调度读取之间的数据竞争；删除已无调用者的 `compactOperationsLocked`。新增 `TestUnifiedCompletionMaintenanceDoesNotHoldTheViewLock`（在 `after_snapshot` 闸门挂起维护，读者仍能完成 `readOperations`）。`gofmt`/`vet`/`build` 干净，全量失败集 `BASELINE-SET-MATCH`（27 项），`-race`（`TestUnifiedCompletion*`、`TestOperationCommit*` 等）通过。

**实测**（每侧全新 v2 卷 + 同批导入 + 配额，4 组 × 3 样本/侧，每样本 320 查询，conc=64，A/B 逐组交替）：

| 组 | r12 中位 qps | r18 中位 qps | 变化 |
|---:|---:|---:|---:|
| 1 | 453.87 | 482.48 | +6.3% |
| 2 | 435.49 | 497.36 | +14.2% |
| 3 | 399.70 | 475.68 | +19.0% |
| 4 | 474.99 | 517.17 | +8.9% |
| 全体（n=12/侧） | 452.86 | 488.71 | **+7.9%** |

配对中位 **+11.5%**（4/4 组同向）；p50 逐组 127.9/134.1/146.4/125.4 → 124.7/114.1/120.1/112.3ms；锁等待/请求中位 8.74→6.64ms（−24%）；compaction 次数（5 次/320 查询）与墙钟（73–90ms）不变，全部样本 `failed=0`、无 compaction failure。

**结论**：合入。收益来自把约 10% 墙钟的维护移出状态锁，而不是减少工作。与 R15（减少锁获取次数，−15.8%）、R16（把锁内有序等待拆成跨锁往返，−19~−29%）对照，**状态锁的持有时间**才是 c64 吞吐的限制项；同样的工作放到锁外即可见效。

**下一轮提示**：maintenance 仍独占 `journal.write` 73–90ms，期间新的 append 只能在队列里等 writer——这是 R18 之后剩下的锁外串行段。可先试提高 `operationJournalCompactRecords`（探针 2048 为 +13.5%，但需评估恢复时间与保留上限），或做「活动段轮转 + 冷却段压缩」让 append 与压缩真正并行。前者便宜、后者是耐久结构改动，均须先出设计与崩溃恢复论证。

#### R19：操作日志压实预算 256→1024（2026-11-02，中性，不合入）

**假设**：R18 之后压实已不在状态锁内，但仍独占 journal writer 完成归档、快照编码、文件屏障与截断；c64 下每 320 查询 5 次、合计 73–90ms（约墙钟 11–13%）。把 `operationJournalCompactRecords` 从 256 提到 1024，可把每查询分摊的压实固定成本降到约 1/4，代价是恢复最多重放 1024 条记录。

**实现**（`perf/issue28-r19-compact-budget` / `755829a`）：常量改为 1024，注释补充「预算同时是恢复重放上限」；`TestUnifiedCompletionArchiveTamperRefused` 原先依赖 256 条预算自动触发压实，改为 prime 预算后由下一条审计记录触发，覆盖意图不变。`gofmt`/`vet`/`build` 干净，全量失败集 `BASELINE-SET-MATCH`（27 项），`-race` 通过。

**实测 1**（每侧全新 v2 卷 + 同批导入，A=r18 先测、B=r19 后测，4 组 × 3 样本/侧，每样本 320 查询，conc=64）：

| 组 | r18 中位 qps | r19 中位 qps | 变化 |
|---:|---:|---:|---:|
| 1 | 497.24 | 477.67 | −3.9% |
| 2 | 532.82 | 490.57 | −7.9% |
| 3 | 524.77 | 498.91 | −4.9% |
| 4 | 469.71 | 463.67 | −1.3% |
| 全体（n=12/侧） | 493.25 | 491.54 | **−0.3%** |

机制确认：compactions 5→1/样本；压实墙钟 83.0→29.4ms（快照 30.1→7.4、fsync 7.9→2.2）；appends 1284 不变；锁等待/请求 6.62→7.20ms；全部样本 `failed=0`。

**实测 2（交换顺序）**：A=r19 先测、B=r18 后测，3 组 × 3 样本/侧：整体中位 r19 480.21 vs r18 481.59（**−0.3%**）；逐组 −9.3%/+11.3%/−4.4%，组间噪声大于效应。

**结论**：中性，不合入。压实工作量减少约 54ms/320 查询（≈8% 墙钟），吞吐两次独立测量都持平——**writer 独占的压实时间不是剩余吞吐上限**（与 R15 同构：减少工作量/获取次数 ≠ 提升吞吐）。恢复重放上限从 256 提升到 1024 是实打实的耐久代价，没有收益就不应承担。main 保持 256，分支留证。

**下一轮提示**：连续两轮「减少压实成本」无效，说明瓶颈已不在 compaction。先用一次性观测器拿到 R18 之后 c64 的相位分解（state_gate、queue_lock、encode、audit、native、admission），再决定动哪一段；拿到分解前不再猜。

#### R20：完成日志批次共用一次操作视图锁（2026-11-02，合入）

**观测依据**（R18 之后的一次性观测器，c64，320 查询；观察器会压低吞吐，只看相位占比）：
- 请求相位均值：close 28.3ms（audit 19.4）、open 26.1（audit 15.3）、cursor_start 32.2（native 15.4、audit 11.7）、statement 18.0（audit 11.6）、describe 6.4（无 audit）；
- durability 流：`audit_member_wait` 均值 17.9ms vs `journal_member_wait` 4.65ms，而 `journal_batch` 的 sync 均值只有 2.97ms——审计成员等待远高于屏障本身；
- drainer 一批平均 13.5 条、批次墙钟 8.26ms，`journal_prepare.state_lock` 均值 143µs/条 → 逐条取锁约占批次 2ms。

**假设**：drainer 每批逐条调用 `submitOperationsAudit`，每条都重新取操作视图锁；c64 下该锁被请求流量占用，一批要排队十几次。整批一次取锁、按序逐条入队（每条仍是独立 commit、独立屏障、独立拒绝）应能缩短批次提交段，并让相邻提交更集中、屏障合并更多。

**实现**（`perf/issue28-r20-batch-audit-submit` / `4217994`）：抽出 `enqueueOperationsAuditLocked`（假定视图锁已持有，单条路径行为不变）；新增 `submitOperationsAuditBatch`（整批一次取锁，返回逐条 wait 与逐条 refusal）；`recordUnifiedBatch` 改用批量提交，等待仍在锁外并行完成；`stateStore` 增加仅测试用的 `testOperationLock` 钩子；新增 `TestUnifiedCompletionBatchSubmitsUnderOneViewLock`。`gofmt`/`vet`/`build` 干净，全量失败集 `BASELINE-SET-MATCH`（27 项），`-race` 通过。

**实测 1**（A=r18 先、B=r20 后，每侧全新 v2 卷，4 组 × 3 样本/侧，320 查询/样本，c64）：

| 组 | r18 中位 qps | r20 中位 qps | 变化 |
|---:|---:|---:|---:|
| 1 | 482.17 | 547.82 | +13.6% |
| 2 | 533.85 | 514.43 | −3.6% |
| 3 | 514.70 | 519.44 | +0.9% |
| 4 | 440.34 | 540.03 | +22.6% |
| 全体（n=12/侧） | 482.37 | 526.89 | **+9.2%** |

**实测 2（交换顺序，排除顺序偏差）**（A=r20 先、B=r18 后，3 组 × 3 样本/侧）：

| 组 | r18 中位 qps | r20 中位 qps | 变化 |
|---:|---:|---:|---:|
| 1 | 468.23 | 574.05 | +22.6% |
| 2 | 510.96 | 551.50 | +7.9% |
| 3 | 458.09 | 555.07 | +21.2% |
| 全体（n=9/侧） | 474.13 | 555.07 | **+17.1%** |

机制确认：journal 屏障/样本 150→126（−16%）、sync/请求 0.093→0.079、锁等待/请求 7.19→6.08ms（−15%）、p50 124.8→110.3ms；compactions 5/样本与失败数（0）不变。

**结论**：合入。批量提交把 drainer 的提交段从「每条一次锁往返」压成一次，并顺带让相邻提交更集中、屏障合并更多；两轮方向一致（+9.2% / +17.1%）。

**下一轮提示**：审计相位仍是最大项（观测约 12–19ms/动作），当前 drainer 提交完一批后同步等待整批屏障，等待期间不再提交下一批。可评估「提交与等待流水化」：drainer 提交后立即取下一批，屏障等待、受理释放与 `done` 投递放进批次自己的 goroutine，Close 需 join 所有在途批次；顺序由提交时分配的审计序号保证，反向压力仍由准入计数约束。须先出设计与关闭/错误路径论证。

#### R21：完成日志 drainer 双批次流水（2026-11-02，失败，不合入）

**假设**：沿用 R20 的下一轮提示——drainer 提交一批后同步等整批屏障，等待期间不再提交，屏障等待与下一批提交没有重叠。

**实现**（`perf/issue28-r21-audit-pipeline` / `92faad4`）：`drain()` 拆出 `takeBatch()`；统一日志路径走新增的 `drainUnified()`——`submitUnifiedBatch(batch)` 按序入队并返回 `*auditUnifiedBarrier`，在途保留上一批，先 `submitUnifiedBatch(下一批)` 再 `finishUnifiedBatch(上一批)`（结清屏障、释放准入额度、投递 `done`、`checkpointAfterBatch`、观测收尾）；`audit.writing` 为两批合并视图；删除 `recordUnifiedBatch`，`recordBatch` 只保留旧 v1 路径。新增 `TestUnifiedCompletionQueuesTheNextBatchBehindTheBarrier`（`testPopGate` 停住 committer，直接塞 40 条已准入请求，要求 `operation_journal_queued` 达到 40；串行实现停在 32）——串行化变异实测 3 连失败，回归有效。`gofmt`/`vet`/`build` 干净，全量失败集 `BASELINE-SET-MATCH`（27 项），`-race` 通过，新用例 `-race -count=10` 通过。

**实测**（每侧全新 v2 卷，A=r20 先、B=r21 后交替，4 组 × 3 样本/侧，320 查询/样本，c64；24 个样本 `failed=0`）：

| 组 | r20 中位 qps | r21 中位 qps | 变化 |
|---:|---:|---:|---:|
| 1 | 584.92 | 435.54 | −25.5% |
| 2 | 568.93 | 430.89 | −24.3% |
| 3 | 552.62 | 440.38 | −20.3% |
| 4 | 549.46 | 427.86 | −22.1% |
| 全体（n=12/侧） | 569.04 | 431.63 | **−24.2%** |

配对 12 组中位 −24.9%（区间 −29.2%…−9.0%），四组同向；p50 100.6→134.1ms。

**机制**（同一批样本的 before/after 快照差，每侧 12 样本合计 19,260 个动作）：`operation_journal_appends_total` 完全不变（15,408），而 `operation_journal_syncs_total` 1,483→2,534（**+71%**），即每次屏障覆盖的提交 10.4→6.1；`operation_journal_coalesced_total` 13,925→12,874；`state_lock_wait_ns_total` 107.65s→202.32s（**+88%**），但 `state_lock_waits_total` 179,268→179,821 几乎不变——单次取锁排队时长翻倍；`operation_journal_wait_ns_total` 3.11s→5.09s（+64%）。compactions 57→57、压实时长 966→1,035ms、编码 194→223ms，均非主因。

**归因**：两批同时在途使 `unconfirmed` 栈翻倍，而 `settleOperations()` 在操作视图锁内按序等待栈中全部未决提交——结清上一批时连带等下一批的屏障，正是 R14/R16 见过的机制（R20 的批内一次取锁没有这个问题，因为提交后立即结清）。同时 drainer 不再被屏障限速，取批节奏改为跟随到达，批次更碎，屏障合并反而变差。

**结论**：失败，不合入。R20 下一轮提示中的「提交与等待流水化」方向被否证：**屏障重叠的前提是不加深未决栈**；drainer 一旦在途保留两批，`settleOperations()` 的持锁等待就是代价。main 保持 R20 的实现，分支留证（`92faad4`），证据 `/tmp/r21fresh/`、`/tmp/r21/fail-r21.txt`。

**下一轮提示**：剩余成本仍在「结清」段——`finishUnifiedBatch` 对批内每条各调一次 `awaitOperationCommit`，每次都重新取视图锁并跑一遍 `settleOperations()`（首次还要在锁内等一个屏障）。同一批的提交在队列里连续，等最后一条的屏障即可覆盖全批，因此可把批内逐条结清改成「批级一次等待 + 一次 settle + 逐条读 `commit.err`」，在不改变耐久时点、审计顺序与失败语义的前提下把每批的取锁次数从 ~13.5 降到 1。

#### R22：批内结清改为批级一次等待 + 一次 settle（2026-11-02，失败，不合入）

**假设**：R21 记录中的下一轮提示——批内每条 `awaitOperationCommit` 都要重新取视图锁并跑一遍 `settleOperations()`，把这 13.5 次取锁压成 1 次应缩短批次墙钟。

**实现**（`perf/issue28-r22-batch-settle` / `193be6f`）：`submitOperationsAuditBatch` 改为返回 `operationAuditBatch`（批内有序 commit + 逐条提交失败）；新增 `awaitOperationsAuditBatch`：按序 `waitObserved` 整批、随后一次 `settleOperationBatch()`（抽出的共享锁内收尾：settle、调度 maintenance/compaction、读取 poison），最后逐条按 `settleErr` / `commit.err` / poisoned 归因；`recordUnifiedBatch` 不再为每条起 goroutine。单条路径 `awaitOperationCommit` 复用 `settleOperationBatch`。`TestUnifiedCompletionBatchSubmitsUnderOneViewLock` 增加断言：提交 1 次取锁、结清再 1 次。`gofmt`/`vet`/`build` 干净，全量失败集 `BASELINE-SET-MATCH`，`-race` 通过。

**实测**（每侧全新 v2 卷，4 组 × 3 样本/侧，320 查询/样本，c64；24 个样本/轮 `failed=0`）：

| 轮次 | 顺序 | r20 全体中位 qps | r22 全体中位 qps | 变化 |
|---|---|---:|---:|---:|
| run1 | r20 先 | 592.25 | 525.68 | −11.2% |
| run2 | r22 先（交换） | 591.41 | 562.30 | −4.9% |

两轮均值 −5.0%/−5.5%，逐组 −23.0%…+4.2%；p50 95.6→109.6ms、96.3→102.6ms。机制：取锁次数/动作 9.31→8.55（**−8%**，符合预期），但 `sync`/动作 0.080→0.084、锁等待/动作 5.07→6.09ms。

**归因**：逐条结清在 R20 里本来就不是「每条各付一次等待」——同批提交共用一个屏障，只有首个等待者真正持锁等待，其余条目面对的是已清空的未决栈。改成批级一次 settle 后，持锁时刻只能对齐到整批最年轻的提交，且同一次 `settleOperations()` 还会连带等待批后新提交，等待搬到了更晚的屏障上；减少的取锁次数（−8%）不足以抵消。**「锁获取次数」再次被证明不是正确的优化变量**（与 R15 同构）。

**结论**：失败，不合入。main 保持 R20 的逐条结清实现，分支留证（`193be6f`），证据 `/tmp/r22fresh/`、`/tmp/r22swap/`。

**下一轮提示**：两种结清写法都不改变吞吐，说明批次墙钟不在结清段。改用一次性观测器（`PERI_DURABILITY_OBSERVER=1` + `PERI_SQL_OPEN_OBSERVER_PHASE_*`）取 R20 之后 c64 的 `journal_prepare` 相位与 `audit_member_wait`，先确认批次墙钟落在提交（`edit_setup`/`edit`/`validate`/`delta`）、屏障（`sync`）还是准入，再决定 R23 动哪一段。

#### R23：测量轮——小查询延迟分解与会话生命周期（2026-11-02，无代码变更）

**动机**：R20 之后 c64 平台侧只剩小幅空间，需要判断剩余延迟落在哪里，避免继续在已用尽方向上迭代。

**方法**：
- durability 观测器（c64，单次 320 查询）：只看提交/屏障相位，不引入请求相位开销；
- 请求相位观测器（c8/c64）：只看相位占比，其请求计数约为客户端查询数的 1.9–2.8 倍，绝对值不可用于对外汇报；
- 无观测器干净对照（r20 二进制、每侧全新 v2 卷、同一导入）：把客户端改成三种模式——`baseline`（每查询 connect→查询→close，与服务端 SDK 默认一致）、`reuse`（每个 worker 一个会话跑 N 次查询）、`ping`（connect→`SELECT 1`→close）、`noop`（只 connect+close，不查询）。脚本：`/tmp/r23clean.sh`、`/tmp/r23split.sh`、`/tmp/conc-split.mjs`；证据 `/tmp/r23/`。

**结果**（p50，客户端视角）：
| 场景 | 每查询总量 | 其中查询段 / close | qps |
|---|---|---|---|
| c64 baseline（connect→查询→close） | 123.4ms | 93.8 / 31.8 | 468.6 |
| c64 reuse（同会话 5 次查询） | 52.2ms | 52.2 / 摊薄 | **911.3** |
| c64 ping（`SELECT 1`，仍每查询新会话） | 97.1ms | 70.9 / 21.0 | 596.6 |
| c64 noop（只 connect+close） | 0.1ms | — | — |
| c8 baseline | 48.3ms（另一次 35.1） | — | 135.8（另一次 211.5） |
| c8 reuse | 16.4ms | — | 407.5 |
| c1 baseline | 18.0ms | 12.8 / 5.6 | 52.6 |
| c1 reuse（热会话） | 10.8ms | — | 93.8 |

**机制侧事实**：
- 提交路径已到下限：`journal_prepare` 的 encode p50 9µs、validate/edit/delta ≤1µs；`journal_batch` p50 1.58ms 中 sync 1.56ms；`audit_member_wait` p50 5.55ms（其中排队 0.93ms）。继续压缩 Go 侧准备/编码没有空间；
- worker 原生时间与并发无关：请求相位观测下 `cursor_start` 的 `native/wait` p50 在 c8 是 16.2ms、c64 是 12.4ms，说明它是每查询固定成本而非排队；同期 native 对象存储只做约 1 次 put、508–978 次 get（51–79µs/次），所以固定成本不在对象存储 I/O；
- 会话生命周期占主导：c64 下 connect→查询→close 比同会话复用多 71ms（58%），c8 下多 32ms（66%）；即使 SQL 换成 `SELECT 1`，c64 仍要 97.1ms，而纯 connect+close 是 0.1ms（SDK 惰性，成本全在服务端）。

**结论与下一步**：Go 侧审计批处理方向（R18/R20 成功、R21/R22 失败）已用尽；c64 剩余延迟里约 58% 是会话建立/关闭（每个会话两次耐久屏障 + worker 会话工作），其余主要是 worker 每查询约 12ms 的固定原生时间。两者都不在 Go 可改范围：下一步必须能构建并运行 linux/arm64 的 Rust worker（仓库根 `rust-toolchain.toml` 钉 1.98.1，容器内需镜像源安装），否则只能接受当前平台侧水位。

#### R24：worker 镜像刷新闸门改为条件变量交接（2026-11-02，合入）

**动机**：R23 把剩余延迟指到 Rust worker 后，先用一次性探针取得请求相位（c64，每请求均值）：`idle`（等请求体）5.1–6.2ms、`prepare` 1.33–1.75ms、`dispatch` 3.3–4.4ms、`dispatch` 内 `cursor.call` 6.0–10.5ms、`reply` 0.04ms。其中 `Sessions::begin` 一段 **1.75–2.05ms** 与 `cursor.call` 同级，明显是固定等待而非排队：`begin.entry`/`begin.image`/`begin.call` 合计仅 0.005ms，成本全在 `refresh.lock`，其中 `refresh.sleep` 在 1553 次 `begin` 上触发 1980 次、均值 2.9ms。

**假设**：`RefreshLocks` 是「按库串行化初始镜像刷新」，被 `open`/`begin`/`close`/`health` 共用；实现是 2ms `try_lock` + `thread::sleep` 轮询。闸门内真正的工作（`initial_image_replaced` 在 generation≠0 时直接返回）只有微秒级，所以每次重叠都要在请求路径上付一整个 2ms 睡眠。改成条件变量交接应当直接消掉这一段。

**实现**（`src/worker_rpc/sessions.rs`）：
- `RefreshLocks` 的 `Weak<Mutex<()>>` 换成 `Weak<RefreshGate>`；`RefreshGate { held: Mutex<bool>, released: Condvar }`，Drop 守卫置 `held=false` 并 `notify_one`；
- `lock_until(deadline)` 用 `wait_timeout(remaining)` 取代轮询：拿到守卫即返回，超时且仍持闸时**把唤醒转交给下一个等待者**（否则被唤醒者若因自己的 deadline 先放弃，其余睡眠者会空转到 deadline）；
- 健康巡检保持非阻塞语义（`try_lock` 返回 `Option`，持闸即 `continue`）；`clear_database` 无请求 deadline，用 60s 上限而非无限等待；互斥与 deadline 语义、fail-closed 行为都不变。

**结果**（每侧独立 platform+worker 二进制、每侧全新 v2 卷 + 同导入 + 同配额，c64，320 查询/样本，逐轮交替顺序；`/tmp/r24ab.sh`）：
| 轮次 | 全体中位（base → r24） | 逐组中位 | p50 |
|---|---|---|---|
| run1（4 组 × 3 样本/侧） | 582.05 → 667.69 qps（**+14.7%**） | +11.7 / +27.5 / +19.4 / +11.1% | 99.85 → 90.39ms |
| run2（交换顺序，r24 先测） | 622.54 → 735.21 qps（**+18.1%**） | +7.4 / +14.2 / +21.8 / +16.0% | 91.59 → 83.12ms |
两轮 `fails=0`；`sync/q` 0.07–0.08 → 0.06–0.07。逐组 8/8 同向，交换顺序无回归 → 判定为真实提升而非顺序偏差。

**验证**：`cargo test --release --lib` 674 passed / 0 failed / 10 ignored；`worker_rpc::sessions` 7 passed；全量 `--no-fail-fast` 的失败目标（`import_control_cli`、`import_image`、`remote_database`、`stage_image_cli`）在基线上同样失败，`p4_authority::maintenance::gc_safety_window_starts_at_each_tombstone_not_namespace_boot` 单跑失败、no-fail-fast 下通过（既有 flaky），非本轮引入。

**副作用**：探针值来自观测器开启时的运行，观测器会压低吞吐并把请求计数放大 1.9–2.8 倍，相位绝对值不可对外汇报，只用于定位。

**下一轮提示**：探针把 c64 剩余固定成本指到 `cursor.call` 6.0–10.5ms（`dispatch_cursor_first` → `open_cursor_first`）与 `prepare` 剩余约 1.3–1.8ms；`idle` 5–6ms 需先判定是客户端间隔还是 `transport.rs` 的 `IDLE_SLICE` 读超时分片。R23 已明确禁止在拆分数据前改协议，所以 `cursor.call` 只能先加内部 span 再决定动哪一段。

**构建能力（后续轮次前置条件）**：现在可以构建并运行 linux/arm64 Rust worker。`rust:1.98-bookworm` 可直接 pull（宿主机/CI 的 `rustup target add` 与容器内 rustup 下载都极慢，不可用）；容器 `peri-rust-build2` 挂 `/src:ro` + 目标卷 `peri-rust-target` + registry 卷 `peri-v1-cargo-registry`，**必须带 `-e RUSTUP_TOOLCHAIN=1.98.1-aarch64-unknown-linux-gnu`** 否则 cargo 会触发 rustup 联网同步并卡住；构建 `cargo build --release --locked --offline --bin peri-loom-v1` 首次约 1 分钟、增量约 40s。registry 由宿主 `~/.cargo/registry` 复制并硬链接到 `rsproxy.cn-*`（仓库 `.cargo/config.toml` 把 crates-io 指向 rsproxy）。

#### R25：授权读取复用已持有视图（2026-11-02，合入）

**动机**：R24 之后先做容量归因，确认瓶颈不在 CPU——c64、约 800 qps 时整机仅 **1.03 核**（平台 0.88、worker 0.08，每查询约 1.4ms CPU），而每查询墙钟约 72ms。运行中对平台进程发 SIGQUIT 取 goroutine 栈：21 个 goroutine 停在 `fileAudit.waitObserved`，11 个停在 `stateStore.lockState`。给 `lockState` 加调用点标签后（临时探针，未合入；`sitelock_{waits,wait_ns,hold_ns}.<site>`）测得 9.3 次获取/请求、等待 6.1ms/请求，其中窄读（`credentialByDigest` 1.4、`authorizationState` **4.8**、`gatewayRequestState` 1.0 次/请求）只持锁 4–6µs 却排掉约 86% 的等待——典型 convoy，被 `readOperations`（等待 journal 屏障，均 107µs/次）和 `compactionAcquire` 的长持有拖住。

**假设**：`authorizeQuota` 先读一次 `authorizationState` 做项目/库校验，紧接着 `Identity.Authorize` 用同一 `(keyID, project, database)` 再读一次；Hrana 适配器与内层 handler 各调一次 authorize，于是每个请求在同一把状态锁上读 **4 次相同视图**。把判定改成接受已持有视图即可减半，且不改变任何判定条件。

**实现**（`prototypes/v1/platform/auth.go`、`gateway_routing.go`）：
- 抽出 `authorizeInState(state, principal, database, action)` 纯判定；`(*identity).Authorize` 仍是「读 + 判定」；
- `authorizeWithin(identity, state, …)` 供已持视图的调用方使用：只有能接受视图的 sink 走捷径，否则回落到 `identity.Authorize`，注入的 Identity 仍是权威；
- `authorizeQuota` 改用已读视图。判定条件、错误返回、权限表均未改动。

**结果**（每侧独立二进制、每侧全新 v2 卷 + 同导入 + 同配额，c64，320 查询/样本，逐轮交替顺序）：
| 轮次 | 全体中位（r20 → r25a） | 逐组中位 | 逐组同向 | lockwait/req 中位 |
|---|---|---|---|---|
| run1（4 组 × 3 样本/侧，r20 先测） | 690.83 → 744.66 qps（**+7.8%**） | +17.4 / +16.2 / +3.6 / +9.9%（中位 **+13.0%**） | 4/4 | 5.83 → 4.91ms |
| run2（交换顺序，r25a 先测） | 725.76 → 761.62 qps（**+4.9%**） | +3.3 / +7.2 / +3.5 / +9.4%（中位 **+5.4%**） | 4/4 | 5.54 → 4.82ms |
两轮 `fails=0`，逐组 **8/8 同向**（合并逐组中位 +8.3%）。机制核验（探针版）：状态锁获取 9.32 → **6.90 次/请求（−26%）**，`authorizationState` 4.80 → **2.40 次/请求**，与「每请求去掉 2 次重复读取」一致。

**对照实验（先排除客户端瓶颈）**：同一服务器、同一总并发下，1 进程 × c64 = 539 qps，2 进程 × c32 = 544 qps（均 `fail=0`）——总量不变，说明瓶颈在服务端而非 Node 客户端，服务端优化才会体现在 qps 上。c128 单进程会出现 `audit_unavailable`（503）与 502，说明当前审计准入在 128 并发下不足，属独立问题。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`；`go test ./...`（`PERI_RUST_BINARY` 指向本机 debug worker）失败集与基线一致，均为此前的环境敏感项（`TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`、`TestP6Verify*` 等，在 main 上同样失败）。

**副作用/风险**：`authorizeWithin` 的类型断言让自定义 Identity 自动回落到自身读取，行为不变；单请求内的判定现在基于同一份视图，比原来「两次独立读取」更一致，不会更宽松（两次读取本来就来自同一状态锁保护的状态）。

**下一轮提示**：探针显示改后剩余等待占比 `credentialByDigest` 约 49%、`authorizationState` 23%、`compactionAcquire` 13%、`gatewayRequestState` 12%。`credentialByDigest` 仍是每请求第一把锁，占比最大；下一步是让它不经过状态锁（不可变摘要索引 + 原子健康位），其次是去掉 Hrana 适配器与 handler 之间的重复授权。

#### R26：凭证视图原子发布，认证窄读不再排队（2026-11-02，合入）

**动机**：R25 的调用点探针把剩余等待排了序：`credentialByDigest` 占 **49%**（23.76s/48.70s），但每次持锁只有 4.4µs。它的代价不在临界区，而在于它是每个请求排进状态锁的**第一站**：一旦被 `readOperations`（等 journal 屏障，均 107µs/次）或 `compactionAcquire`（31µs）的长持有顶住，后面所有窄读都跟着排队。

**假设**：认证只需要一次 digest→key 查找，且 key 表是低频变更（发/撤 key 均为控制面操作）。把它换成「持锁重建的不可变快照 + 原子指针发布」，这一站就从锁队列里消失；陈旧快照不会放行已撤销的 key，因为授权步骤仍读加锁视图并拒绝 revoked descriptor。

**实现**（`prototypes/v1/platform/state.go`、`state_disk.go`）：
- `stateStore.credentials atomic.Pointer[map[string]APIKey]` 取代 `digestIndex`；`commitLocked`/`OpenStateStore` 持锁重建整张表后 `Store` 指针，`Update` 尾部不再增量维护索引（`Update` 只服务控制面，不在 SQL 请求路径上）。
- `credentialByDigest` 只做 `available()` + 原子读；`durableFile.closed/poisoned` 由 `bool` 改为 `atomic.Bool`，`available()` 完全无锁。此前这两个字段在锁外读、锁内写，是真实的数据竞争，顺带修掉。
- 判定条件、错误语义（`ErrStateClosed`/`ErrStateUncertain`）、撤销行为均未改动。

**结果**（每侧独立二进制、每侧全新 v2 卷 + 同导入 + 同配额，c64，320 查询/样本，4 轮 × 3 样本/侧）：
| 轮次 | 逐样本中位（r25a → r26a） | 逐轮幅度 | lockwait/req |
|---|---|---|---|
| ab1 c1（r25a 先测） | 728.53 → 767.27 | +5.3% | 4.93 → 5.03（含一个 545 样本） |
| ab1 c2（r26a 先测） | 741.13 → 746.86 | +0.8% | 5.28 → 4.62 |
| ab2 c1（r25a 先测） | 694.21 → 785.91 | +13.2% | 5.48 → 4.38 |
| ab2 c2（r26a 先测） | 611.89 → 791.34 | +29.3% | 6.30 → 4.51 |
合并 12 样本/侧：qps 中位 **694.75 → 779.82（+12.2%）**、均值 697.49 → 759.12（+8.8%），4/4 轮同向；p50 中位 88.2 → 77.6ms；`fails=0`。

**机制核验**（在 R26 之上重放探针，c64）：状态锁获取 6.90 → **5.49 次/请求（−20%）**，`credentialByDigest` 调用点整体消失；剩余等待占比 `authorizationState` 41–44%、`gatewayRequestState` 36–37%、`compactionAcquire` 15–16%、`readOperations` 4–6%。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`；`go vet ./...` 干净；`go test ./...`（`PERI_RUST_BINARY` 指向本机 debug worker）失败集与基线一致，仅 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`。

**下一轮提示**：`authorizationState` 仍是 **2.4 次/请求**（R25 只去掉了 quota 路径上的重复读取，Hrana 适配器与内层 handler 各调一次 authorize 仍保留），是当前最大的单一等待来源（约 2.0ms/请求），下一轮应从这里去掉一次；`compactionAcquire` 0.9 次/请求、均持锁 33–37µs 排掉 15%，需要先拆内部 span 再动（R19/R21/R22 已否证其批处理变体）。

#### R27：每请求一次授权状态读取（2026-11-02，合入）

**动机**：R26 的探针把剩余等待第一项指向 `authorizationState`（41–44%，2.40 次/请求）。一次请求读同一份视图 2.4 次：协议适配器认证后读一次，`open` 内读三次（入口、路由后、writer profile 后），`dispatchLocked` 内读两次（入口、路由后）。这些检查之间大多只有内存操作，真正需要重读的只有跨外部等待之后。

**实现**（新增 `gateway_authorization.go`；改 `gateway_routing.go`、`gateway_sessions.go`、`gateway_writer.go`、`hrana_http.go`、`hrana_http_cursor.go`、`hrana_protobuf.go`、`hrana_v3_json_cursor.go`、`gateway_http.go`）：
- `authorizationScope` 挂在请求上下文，按 `(principal, database)` 命中；命中时只跳过状态读取，`authorizeQuotaFrom` 的判定与拒绝原因逐条不变。
- 适配器（Hrana pipeline / protobuf / JSON cursor / 原生 API）在认证后挂 scope；WS 连接不挂（长连接里一个快照太粗），仍逐消息读。
- `open` 在激活等待/placement settle 与 writer profile RPC 之后 `invalidateAuthorizationScope`，下一次检查重新读取状态。
- `authorizeQuotaFrom` + `authorizationTargetValid` 拆出纯判定；`authorizeQuota` 的校验顺序、错误文案保持原样。

**结果**（每侧独立二进制、每侧全新 v2 卷 + 同导入 + 同配额，c64，320 查询/样本，6 轮 × 3 样本/侧）：
| 轮次 | 逐样本中位（r26a → r27a） | 幅度 |
|---|---|---|
| ab1 c1 | 720.23 → 780.04 | +8.3% |
| ab1 c2 | 705.99 → 816.38 | +15.6% |
| ab2 c1 | 749.10 → 727.67 | −2.9% |
| ab2 c2 | 750.45 → 715.61 | −4.6% |
| ab3 c1 | 705.00 → 769.77 | +9.2% |
| ab3 c2 | 683.72 → 784.78 | +14.8% |
合并 18 样本/侧：中位 **715.60 → 752.31（+5.1%）**、均值 717.22 → 744.43（+3.8%，含一个 445.91 异常样本，剔除后 +6.2%），4/6 轮同向，`fails=0`。**单轮结论不可用**——同一对二进制在不同轮次可以相差 −4.6% 到 +15.6%，这也是本轮先做三轮再做汇总的原因。

**机制核验**（在 R27 之上重放探针，c64）：`authorizationState` **2.40 → 1.40 次/请求（−42%）**，状态锁总获取 5.49 → 4.50，lockwait/req 4.51 → 4.10ms；剩余等待 `gatewayRequestState` 42%、`authorizationState` 36%、`compactionAcquire` 15%、`readOperations` 7%。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`；`go vet ./...` 干净；`go test ./...`（`PERI_RUST_BINARY` 指向本机 debug worker）失败集与基线一致，仅 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`。

**副作用/风险**：同一请求内的多次检查共用一个快照，撤销只在请求开始（以及上述外部等待之后）被观察到；跨一次 pipeline 的多个步骤也共用，上限是一次 HTTP 请求的生命周期。判定函数本身未改，拒绝原因与顺序不变。

**下一轮提示**：`gatewayRequestState`（1.00 次/请求，只读 Projects+Databases）与 `authorizationState`（1.40 次/请求）是同一次请求里两次形状不同的窄读，合并成一次读是下一轮最直接的方向；`compactionAcquire`（0.9 次/请求、均持锁 25µs、15% 等待）需要先拆内部 span。

#### R28：准入与授权合并为一次窄读（2026-11-02，合入）

**动机**：R27 之后探针显示 `gatewayRequestState`（1.00 次/请求，只读 Projects+Databases）与 `authorizationState`（1.40 次/请求，多带 APIKeys+Keys）是同一把状态锁上的两次窄读。同一次请求为回答两个问题排两次队。

**实现**（`state.go`、`gateway_authorization.go`、`gateway_http.go`、`state_test.go`）：
- 新增 `stateStore.requestState(keyID, projectID, database *DatabaseID)`：一次持锁返回并集视图，带空的项目/库/凭据/描述符条目，不带 Envelope。`authorizationState` 与 `gatewayRequestState` 改为委托，调用方判据与错误文案不变。
- `authorizationScope` 改为按 `(keyID, project, database)` 命中，并允许「库视图回答同项目的纯项目问题」（库视图是前者的超集）。
- **第一次实测暴露的 bug**：`gateway_http.go` 的入口无条件 `withAuthorizationScope(ctx, &authorizationScope{})`，把协议适配器已经挂好的快照覆盖成空对象，于是适配器读到的那次视图传不进 handler。改为仅在没有作用域时挂载（`7cdfd35`），`requestState` 从 2.40 直接掉到 0.40 次/请求。**这是本轮最值得记住的方法教训：机制复核必须在 A/B 之前做**——第一版（未修复）的 6 轮 A/B 中位 +5.4%，而它其实一个读都没省下来，测到的全是噪声。

**结果**（每侧独立二进制、每侧全新 v2 卷 + 同导入 + 同配额，c64，320 查询/样本，6 轮 × 3 样本/侧）：
| 轮次 | 逐样本中位（r27a → r28b） | 幅度 |
|---|---|---|
| b1 c1 | 858.90 → 808.90 | −5.8% |
| b1 c2 | 749.71 → 928.78 | +23.9% |
| b2 c1 | 745.52 → 887.78 | +19.1% |
| b2 c2 | 826.93 → 800.80 | −3.2% |
| b3 c1 | 786.72 → 818.83 | +4.1% |
| b3 c2 | 820.90 → 786.78 | −4.2% |
合并 18 样本/侧：中位 **810.61 → 829.71（+2.4%）**、均值 800.39 → 838.61（+4.8%），p50 中位 75.1 → 72.9ms，3/6 轮同向，`fails=0`；A/B 内 lockwait/req 中位 4.16 → 3.37ms（−19%）。

**机制核验**（在 R28 之上重放探针，c64）：`requestState` **2.40 → 0.40 次/请求（−83%）**，状态锁总获取 **4.51 → 2.50（−45%）**，lockwait/req 4.52 → 3.73ms。剩余等待结构已经变了：`readOperations` 51–53%（1.20 次/请求、均持锁 116µs）、`compactionAcquire` 19–22%（0.89 次/请求、25–30µs）、`requestState` 27%（0.40 次/请求、18–24µs）。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`；`go vet ./...` 干净；`go test ./...` 失败集与基线一致，仅 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`（新增的 `requestState` 需在 `state_test.go` 的 `narrowAuthProbe` 上转发，否则窄读探针会落到 `Snapshot`）。

**下一轮提示**：`readOperations` 现在是最大等待来源（1.20 次/请求、116µs 持锁、51–53%），主要成本是等 journal 屏障；`gateway_sessions.go:76` 与 idle 路径对它的调用是否可以移出请求路径，需要先确认它保护的 settle 语义。`compactionAcquire`（0.89 次/请求、25–30µs）此前已被 R19/R21/R22 否证其批处理变体，若要动必须先拆内部 span。

#### R29：把操作视图的屏障等待移出状态锁（2026-11-02，失败，不合入）

**假设**：R28 探针把 `readOperations` 的持锁时间拆开了：settle（等 journal 屏障）0.13–0.14ms/请求约占 91%，`visit` 只有 0.02ms。只要把这 91% 挪到锁外，屏障期间其他请求就能继续读写状态视图，吞吐应当上升。

**做法**：`stateStore` 加 `settling int` + `settleDone *sync.Cond`（绑 `disk.mutex`）；`readOperations` 改成循环——锁内查 `available()`；`settling>0` 时 `Wait()` 主动释放状态锁；`unconfirmed` 为空则 `visit` 返回；否则摘出 pending、`settling++`、解锁、`awaitOperations(pending)`、重锁、`settling--`、`Broadcast()`，失败则 `rollbackOperations`。最多 `operationReadSettleAttempts = 3` 轮，之后回落到原来的锁内 settle（保持有界）。`settleOperations` 拆成锁外可调的 `awaitOperations` 与调用方持锁的 `rollbackOperations`。

**结果**：2 轮 × 3 样本/侧 A/B（r28b vs r29a，c64，320 查询/样本，逐轮交替）：run1 中位 799.79→623.46（−22.1%），run2 800.29→609.72（−23.8%）；合并 12 样本/侧中位 **800.04→595.52 qps（−25.6%）**，均值 781.82→588.42（−24.7%）；p50 中位 75.4→95.8ms（+27%）；`fails=0`。机制侧：lockwait/req 3.5–4.1→**0.25–0.51ms**（等待确实被挪出锁），但 sync/q 0.06→**0.10–0.12**（journal 屏障次数近乎翻倍）。

**结论与原因**：持锁串行化恰好是屏障合并的限速器。锁内结清时，一个等待者付完屏障，后来者在锁内看到的未决栈已空，直接拿已提交视图；等待挪到锁外后，屏障窗口内每个到达 `readOperations` 的请求都会去 `awaitOperations`，等的还是**包含自己之后入队写入**的未决栈（更年轻的屏障），于是等待互相堆叠——排队放大到 +20ms p50，远超省下的锁等待。同时结清者不再被锁排序，journal 取到的批次更碎，sync 次数翻倍。这与 R22（批级一次 settle）同构：**一次有序等待必须保持串行**，拆成多个跨屏障往返必然退化。

**验证**：`go build ./cmd/peri-platform ./cmd/peri` 通过；`go vet ./...` 干净；`go test ./...` 仅剩基线既有 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`；`go test -race`（`TestOperation|TestDurable|TestState|TestSettle|TestGatewayRequest|TestAuth`）72.5s 无 DATA RACE。分支保留为证据，不合入。

**下一轮提示**：不要再动 settle 的顺序性。`readOperations` 仍有 1.20 次/请求，下一步应先查这两次从哪来（`gateway_sessions.go:76` 与 `gateway_idle.go:24`）：若其中一次只是要读「已提交视图」而不需要自己触发 settle，可用 R26 的原子快照发布模式做成无锁读（`pending==0` 时直接读不可变快照，`pending>0` 时走现有锁路径）。`compactionAcquire`（0.89 次/请求）仍需先拆内部 span。

#### R30：只有回滚会重新加回隔离的未决编辑才等屏障（2026-11-02，合入）

**假设**：R29 证明等待不能挪出锁。换一个方向：不是把等待挪走，而是让读**不必等**。`readOperations` 的 settle 保护的是「读者不要在一个可能被回滚的编辑上做决定」；但如果该编辑回滚后准入答案不变，等待就是纯开销。非提交请求（只读语句、describe、游标）每次都写一条**已经终结**的记录，且不改变隔离集合。

**探针（先定方向再实现）**：场景拆分：`session_by_id`（每次查询 open→describe→cursor_start→statement→close）c64 735–817 qps，`probe`（`SELECT 1` 单步）738–858 qps —— 瓶颈在会话生命周期不在执行；`reuse`（复用会话）百万级。相位观测（`--sql-open-observer`）c64 单样本：`cursor_start` p50 23.00ms（native 17.22 / audit 2.01 / intent 1.69）、`open` 9.82ms（audit 5.49 / native 3.63）、`close` 8.76ms（audit 6.69 / native 1.82）。耐久观测（`PERI_DURABILITY_OBSERVER=1`）：`audit_member_wait` p50 1.99ms / p99 22.61ms。上限探针：`readOperations` 直接跳过 settle（不安全，仅测上限）3 轮 × 3 样本 c64，中位 786.98→916.81 qps（+16.4%），lockwait/req 3.3–4.2→0.8–1.7ms。机制成立，值得实现安全版本。

**第一版（`54e4059`，+2.1%，不足以合入）**：入队时计算 `admissionRelevant`：累加编辑前/后未终结的触及记录数，并检查触及记录是否阻塞；任一触发即等待。9 样本/侧 A/B（r28b vs r30safe）中位 876.26→894.33（+2.06%）、均值 +0.99%，`lockwait/req` 3.41→2.83ms。临时探针（`state_cost` 原子计数，未提交）给出原因：每读平均遇到 **2.86 条**未决编辑，但其中「回滚会重新加回阻塞」的 **0.0%**、只改未决计数的 **3.2%** —— 97% 的读已走快路径，剩下的 3.2% 每次持锁等待一批屏障，其余请求全排在它们后面。

**第二版（`02dc958`，合入）**：把等待条件收到「回滚会重新加回一条占住命名空间的记录」。未决计数不再触发等待：加阻塞/加记录都是读者已经看到的更严答案，回滚只会让当时的拒绝显得多余；计数只用于容量，容量由应用编辑的那次提交在 `validate` 里兜底（越过上限的编辑被拒绝），读者少算一条未决终结化，最多放进一个随后自己的持久写会被拒绝的请求——不会让 SQL 在隔离命名空间上执行，因为所有可能提交的路径都在派发前先持久登记（登记失败即拒绝），而失败的追加会毒化存储使后续登记全部失败。阻塞判据按准入模式拆开并与 `isolatedOperation` 共用 `operationBlockingFor`：gateway 准入不把进行中的 gateway 记录算占位（隔离与 management 记录算），management 准入两者都算；`operationReadScope` 的零值是保守的 management 范围，忘记传参不会放松。

**回归门（c8）**：1 轮 × 3 样本/侧（r28b vs r30b，c8，320 查询/样本）：中位 205.07→317.97 qps（**+55.1%**），均值 201.34→314.55（+56.2%），p50 中位 35.7→22.8ms，`lockwait/req` 1.23→**0.09ms**，sync/q 0.34→0.25，`fails=0`；两侧各 `verify messages: 8625`。低并发下每次查询的 journal 批次更小（sync/q 0.34），锁内等待原本被放大得更明显，快路径把这段等待整个去掉，所以收益比 c64 更大。

**结果**：3 轮 × 3 样本/侧 A/B（r28b vs r30b，c64，320 查询/样本，逐轮交替）：逐轮中位 +6.6 / +19.7 / +2.6%（3/3 同向），合并 9 样本/侧中位 **836.98→912.88 qps（+9.07%）**，均值 828.73→908.99（+9.68%）；p50 中位 73.3→67.2ms；`lockwait/req` 均值 3.56→**1.08ms（−70%）**；sync/q 0.06→0.05；`fails=0`。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`、`go vet ./...`、`gofmt` 干净；`go test ./...` 仅剩基线既有 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`；`go test -race`（`TestOperation|TestDurable|TestState|TestSettle|TestGatewayRequest|TestAuth`）77.6s 无 DATA RACE。新增/改写用例：`TestOperationGatewayReadSkipsAnUndecidedGatewayTerminalization`（同一未决 close，gateway 读立即返回、management 读必须等）、`TestOperationReadWaitsForAnUndecidedQuarantineRelease`（未决释放的屏障返回前，gateway 读不得判命名空间自由）、`TestOperationReadAnswersAroundAnUndecidedTerminalEdit`（终结记录不阻塞读且读后未决栈不变）、`TestOperationNarrowReadIsNotBlockedByASlowBarrier`（改用 management 记录终结化作为必须等待的编辑）。

**下一轮提示**：`lockwait/req` 已到 1.08ms，剩余锁等待主要来自提交本身（sync/q 0.05、sync_avg 2.2–2.7ms），从读侧继续挤压空间有限。下一轮应转向相位表里占比最大的部分：`cursor_start` 23.00ms（native 17.22ms，worker 侧 `dispatch_cursor_first`）与 `close` 8.76ms（audit 6.69ms，`audit_member_wait` p99 22.61ms），前者需要先加 worker 内部 span 才能定位。c128 下 `audit_unavailable`(503) 仍是独立问题。

#### R31：一个审计批只付一次屏障、只结算一次视图（2026-11-02，合入）

**假设**：R30 之后 `lockwait/req` 降到 1.08ms，但 c64 相位表显示审计 drain 仍占 **99%** 的窗口时间（`audit_batch` 3.41ms/批、约 21 成员/批、fsync 约 2.0ms），说明限速器已经从读侧移到审计批本身。耐久观测给出两个结构性浪费：journal 批 649 个对审计批 486 个（**1.33:1**），即 drain 形成的批被 committer 拆成两次 fsync；以及每个成员各拿一次状态锁做 settle（19 次串行加锁/批）。

**第一版（`f1a2383`，前缀确认）**：`awaitOperationCommit` 在状态锁内调用 `settleOperations`，它会按顺序 await 队列里**所有**未确认编辑的屏障——包括别的请求刚入队、尚未落盘的那些。改成 `settleReadyOperations`：只确认「提交结果已经发布」的连续前缀，其余留在队列里由各自的提交收尾；前缀规则保持视图顺序，回滚仍按同样逆序执行，fail-closed/poison/`ErrStateClosed` 判据不变；旧版 operation 日志（compaction 由该调用驱动）仍走完整 settle。相位窗口 1155→1179 qps、`audit_batch` 3.41→3.24ms。配对 A/B 仅 +3.2%（1 轮 3 样本，噪声内）——机制成立但等待不再是主要成本。

**第二版（`f377ecc`，一批一次屏障）**：drain 把一批成员逐条提交给 journal，第一条入队就唤醒 committer，committer 立刻拿走当时可见的几条写盘（约 2ms），其余成员在这段时间才排进来，只能等下一批。新增 `holdCompletionBatch`/`releaseCompletionBatch`：drain 在 state 锁内入队整批期间持有，committer 选批前在条件变量上等待，整批入队完再唤醒。hold 只覆盖一次有界的入队（21 成员约 0.2ms），不覆盖任何等待，用 `defer` 成对释放。相位窗口 1232 qps、`audit_batch` 2.82ms、journal:audit 收敛到 **1:1**（527:527）。

**第三版（`04fb53d`，一批一次结算）**：journal 提交在同一批内按序发布，最后一个屏障返回时前面的都已落盘。`awaitOperationBatch` 先等最后一个成员（含 observation），再持锁一次完成整批 settle 与 maintenance；成员错误仍取自各自的 `commit.err`。相位窗口 1271 qps、`audit_batch` 2.72ms、523:523。

**结果（配对 A/B）**：3 轮 × 3 样本/侧（r30 vs r31c，c64，320 查询/样本，逐轮交替）：逐轮中位 +7.65 / +11.25 / +6.25%（**3/3 同向**），9 样本/侧中位 **953.39→1019.59 qps（+6.94%）**，均值 953.28→1029.13（+7.96%）；p50 中位 63.15→59.43ms（−5.9%）；`lockwait/req` 均值 0.99→**0.67ms**；sync/q 0.06→0.05；`fails=0`。

**回归门（c8）**：1 轮 × 3 样本/侧（r30 vs r31c，c8）：中位 288.66→280.33（**−2.9%**）、均值 277.59→283.55（+2.1%），两侧样本跨度 249–295 / 266–305 qps，落在噪声内；`lockwait/req` 0.11→0.09ms，sync/q 0.26 不变。低并发下批只有 1–2 个成员，hold 的 0.2ms 入队窗口与整批结算都不改变屏障次数。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`、`go vet ./...`、`gofmt` 干净；`go test ./...` 仅剩基线既有 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`；`go test -race`（`TestOperation|TestUnified|TestCompletion|TestAudit|TestDurable|TestState|TestGatewayRequest`）131.3s 无 DATA RACE。新增用例：`TestOperationCompletionDoesNotWaitForALaterAppend`（前一批屏障被 gate 住时完成提交立即返回，队列里只剩后一批；已验证旧实现会让该测试挂起）、`TestOperationReadySettleRollsBackOnlyItsFailedPrefix`（就绪前缀只回滚其中失败的编辑）、`TestUnifiedCompletionJournalHoldsOneDrainBatchTogether`（持有期间断言 committer 一个屏障都没写、释放后恰好一个；已验证去掉 hold 时失败）、`TestUnifiedCompletionBatchSubmitsUnderOneViewLock` 增加断言「N 成员批 submit+settle 总共只进两次状态锁」。

**下一轮提示**：drain 仍占 98% 窗口时间，`audit_batch` 2.72ms 里 fsync 已占约 2.2ms（`journal_batch` duty 62%），本架构下再压缩的空间约 20%。继续提升需要减少每个动作的 fsync 次数或降低单次 fsync 成本（R13 的 `fdatasync` 已证无效，不要重复）；另一条线是 worker 的 native 路径（`cursor_start` native p50 14.29ms 在 c1 呈双峰，p10 2.25ms）与 c128 的 `audit_unavailable`(503)，后者是审计准入容量问题，与本次屏障合并无关。

#### R32：维护压缩周期从每 256 次追加放宽到 1024 次（2026-10-30，合入）

**假设**：R31 之后 c64 的审计批平均 2.98ms（p50 2.28ms），相位探针拆出提交 0.33ms、屏障 1.97ms、**压缩 gate 0.56ms**、锁内结算 ~0；同时段 36 次 `journal_compaction` 每次 11.4ms（归档 5.8 / 快照 3.3 / 编码 0.9 / 同步 1.3），合计 410ms，占 1.93s 窗口的 **21%**。压缩期间 `operationCompacting` 冻结日志 gate，正在结算的审计批只能等它结束；`settle` 的 10–20ms 尾延迟与压缩事件一一对齐（每 ~18 批一次）。

**改动（`4158f1f`）**：`operationJournalCompactRecords` 256→1024。归档与快照的两次 `atomicPrivate`（各含文件 fsync 与目录 fsync）是每次压缩的固定成本，周期放宽后由 4 倍记录摊薄；归档写入按记录数线性增长，快照大小不变。崩溃重放仍由 1024 条记录与 `operationJournalMaximumBytes`（8 MiB）双重约束，审计内容与顺序、屏障次数、fail-closed 语义均未改动。`TestUnifiedCompletionArchiveTamperRefused` 改为跨过生产阈值（`operationJournalCompactRecords+16`）而不是写死 270，测试意图仍是篡改归档必须拒绝。

**结果**：相位窗口压缩 36→10 次、停顿 410→187ms，归档每次 5.8→11.3ms，gate 每批 0.56→0.31ms，单跑 qps 1330→1473（+10.7%）。配对 A/B（r31c vs r32，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：逐轮中位 +11.8 / +23.4 / +13.1%（**3/3 同向**），9 样本/侧中位 **979.47→1169.42 qps（+19.39%）**，均值 986.05→1150.99（+16.73%）；p50 中位 62.57→49.96ms（−20.2%）、p99 90.77→75.44ms（−16.9%）；两侧各 `verify messages: 8625`、`fails=0`。

**回归门（c8）**：1 轮 × 3 样本/侧（r31c vs r32）：中位 379.34→410.60（**+8.2%**）、均值 345.84→380.86（+10.1%）；样本跨度 271–387 / 301–431 qps，无退化。

**验证**：`go build ./cmd/peri-platform ./cmd/peri`、`go vet ./...`、`gofmt` 干净；`go test ./...`（152.9s）仅剩基线既有 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`；`go test -race . -run 'TestOperation|TestUnified|TestCompletion|TestAudit|TestDurable|TestState|TestGatewayRequest'` 136.6s 无 DATA RACE。

**下一轮提示**：c64 审计批仍 ~3.1ms，其中屏障 2.35ms 是大头，压缩停顿已降到窗口的 10.8%。继续压维护成本只剩两条路：把归档与快照的两次目录 fsync 合并，或让归档复用日志已耐久的字节（压缩时把日志文件轮转为归档，而不是重写一遍）。屏障侧若要再降，需要减少每查询的审计记录数（当前每查询约 4 条，来自 session open/execute/close 的完整生命周期）或降低单次 fsync 成本。

#### R33：上游空闲连接上限按并发宽度放宽（2026-10-30，中性，留证）

**假设**：平台到 worker 的 HTTP 连接池 `MaxIdleConnsPerHost` 只有 16，而 c64 下同时在飞的调用约 22 个；返回的连接超过空闲上限就被关闭，下一次调用重新拨号，既付握手又留下 TIME_WAIT。

**证据**：压测窗口内每 10ms 抓一次容器 `/proc/net/tcp`（300 个样本）。worker 侧端口在 2 秒窗口里累计 **2,036 个 TIME_WAIT**、ESTABLISHED 只在 16–43 之间；改成 64/128 后同一采样只剩 **10 个 TIME_WAIT**、ESTABLISHED 稳定在 63。机制修复确定。

**结果**：配对 A/B（r32 vs r33，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：逐轮中位 −5.61 / +4.14 / +3.51%（2/3 同向），9 样本中位 1131.78→1167.86 qps（+3.19%），均值 1138.41→1128.73（**−0.85%**），p50 中位 49.14→48.31ms，p99 中位 80.19→74.19ms，fails=0。增益与均值方向不一致，落在噪声内。

**结论**：重连确实被消除，但约 1,000 次/秒的重连只落在 16% 的调用上，单次代价远小于这些调用 2.9ms 的原生等待，因此吞吐不动。分支保留作「连接重连不是剩余瓶颈」的证据，不合入。

#### R34：维护压缩周期从 1024 次追加放宽到 4096 次（2026-10-30，合入）

**假设**：R32 之后压缩停顿仍占窗口 10.9%（1.690s 里 10 次、每次 18.4ms）。压缩在 committer 内串行执行并冻结日志 gate，期间正在结算的审计批只能等它结束。

**分段探针（`b7ee5cf`，仅诊断）**：c64 窗口内审计 drainer **98.3% 忙**，fsync 链只 71% 忙——drainer 自身是限速器；单批 2.90ms = 屏障等待 2.18ms（75%）+ 提交/编码 0.45ms（16%）+ 状态锁 0.26ms（9%），`settleReadyOperations` 与 `scheduleCompletionMaintenanceLocked` 合计 ≈0；单条记录 20.1µs 中 `edit` 5.2µs、三次 `json.Marshal` 合计 9.0µs、两次 SHA-256 1.2µs、journal 互斥锁 1.5µs。同窗口 10 次 `journal_compaction` 合计 184ms。

**改动（`1da88f0`）**：`operationJournalCompactRecords` 1024→4096。归档写入按记录数线性增长，快照大小与两次目录屏障固定，周期放宽后同一份归档字节摊到更少的停顿上：压缩 10→2 次、停顿 184→82ms，单次 18.4→41.0ms（亚线性）。崩溃重放仍由记录周期与 `operationJournalMaximumBytes`（8 MiB）双重约束；审计内容与顺序、屏障次数、fail-closed 语义均未改动。

**结果**：配对 A/B（r32 vs r34，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替）：逐轮中位 −2.40 / +17.13 / +12.36%（2/3 同向），9 样本/侧中位 **1098.74→1204.33 qps（+9.61%）**，均值 1102.80→1200.91（+8.90%）；p50 中位 51.77→46.51ms（−10.2%）、p95 78.86→79.74ms（+1.1%）、p99 84.12→81.62ms（−3.0%）；两侧各 `verify messages: 8625`、`fails=0`。

**回归门（c8）**：1 轮 × 3 样本/侧（r32 vs r34）：中位 287.38→286.58 qps（**−0.28%**），样本跨度 237.84–319.08 / 284.72–303.97，fails=0。c8 单跑只追加约 1.5k 条记录，4096 周期意味着窗口内根本不触发压缩，因此该项在本场景机制上不可能变差，实测差异按噪声处理。

**口径说明**：第一臂 A/B 用到的二进制里还带着未提交的分段探针代码（观测关闭时是 6 次 `time.Now()`），结果 +4.29%；把探针移到 `perf/issue28-r34-probe`（`b7ee5cf`）后从干净提交重建（`5efb05b2…`）复测得 +9.61%。两臂都是 2/3 同向且中位为正，同一份代码的两轮中位相差 5 个百分点，说明这台机器上的轮间离散度仍有这个量级；本节数字取干净二进制那一臂。

**验证**：`go build ./...`、`go vet ./...`、`gofmt` 干净；`go test ./...` 仅剩基线既有 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode`（U+2028/U+2029 转义差异，与本轮无关）。首跑曾出现 3 个 `TestOperationPreparation*` / `TestOperationObservation*` 失败，已确认是探针代码留在工作区所致，探针移走后全绿。

**下一轮提示**：drainer 98% 忙、fsync 链 71% 忙，剩下的串行预算按大小排在屏障 2.18ms（75%）、提交 0.45ms（16%）、状态锁 0.26ms（9%）。提交侧最肥的是三次 `json.Marshal`：`journalEntry` 会把已经算过摘要的 mutation 再整体编码一遍，可用已编码字节拼接省掉（约 3µs/条，估算 +2–4%）；也可把 audit payload 的 event 部分提到请求 goroutine 里预编码（受审计链顺序约束，需要先证明不破坏链序）。屏障侧若要再降，只能减少每查询的审计记录数（当前约 4 条，来自 session open/describe/statement/cursor_start/close 的完整生命周期）或降低单次 fsync 成本（R13 已证明 `fdatasync` 无效）。

#### R35：GC 目标按容器内存承诺放宽到 400%（2026-10-30，合入）

**先否掉了上一轮的提示**：干净 A/B 快照里 `operation_journal_encode_ns_total` 每样本 9–24ms，负载窗（4.4s、6,400 查询）累计 257ms = **5.8% 墙钟**，编码是 `journal.mutex` 内的串行时间。省掉 `journalEntry` 的第二次编码最多 **+1–2%**，不是 R34 估的 2–4%——低于这台机器的轮间噪声，不做。

**pprof 探针（`a3b6655`，仅诊断）**：c64 负载下 CPU 只占 0.67 核，成本在等待而非计算；fsync 链 1,356 次 × 3.07ms = 3.08s / 4.4s 窗口（**70%**），c1 下每个被审计动作恰好付一次 3.1ms 屏障；分配画像给出决定性事实——每查询分配 **573 KB**（6,400 查询累计 3.67 GB），而 in-use 堆只有 **7.3 MB**。存活集远小于分配速率，意味着 GOGC=100 的堆目标≈14 MB，收集器每秒运行数十次只为回收几 MB，回收成本几乎全落在 assist 与写屏障上。分配前三：Hrana JSON 编解码（`jsontext.fetch` 14.5%、`ReformatString` 9.7%、`UnmarshalJSON` 10.5%）、`bytes.Clone` 9.4%、`io.ReadAll`；cursor 路径占 52%，其中 `json.Marshal` 669MB、`Gateway.ServeHTTP` 1.4GB。

**改动（`f215bbf`）**：`cmd/peri-platform/collector.go` 在进程启动时 `debug.SetGCPercent(400)`，并用 cgroup 内存上限（v2 `memory.max` / v1 `memory.limit_in_bytes`）的 70% 作为 `debug.SetMemoryLimit`。`GOGC`/`GOMEMLIMIT` 显式设置时完全不介入——部署自己配的收集器策略优先；cgroup 报 `max`、哨兵值或不存在时不设上限，行为回到默认。审计内容与顺序、屏障次数、fail-closed 语义、任何配额均未改动。

**诊断臂**：同一探针二进制，只差 `debug.SetGCPercent(400)`（2 轮 × 3 样本/侧，c64）：p50 **−14.3% / −9.9%**、mean −6.5% / −15.6%、qps +5.8% / +17.0%，两轮同向——先把「GC 成本存在」坐实，再动生产代码。

**结果**：配对 A/B（r34 vs r35，3 轮 × 3 样本/侧，c64，320 查询/样本，逐轮交替，候选二进制 `d2dec5f1…`）：逐轮中位 **+16.34 / +14.05 / +10.12%（3/3 同向）**，9 样本/侧中位 **1270.06→1444.73 qps（+13.75%）**，均值 1231.68→1372.10（+11.40%）；p50 中位 46.14→41.38ms（−10.3%）、p95 69.68→65.29ms（−6.3%）、p99 72.78→66.85ms（−8.2%）；两侧各 `verify messages: 8625`、`fails=0`。

**回归门（c8）**：3 轮 × 3 样本/侧交替，中位 318.67→363.33 qps（+14.0%）、均值 320.12→356.61（+11.4%），2/3 轮同向。这一档由磁盘 fsync 抖动主导（各样本 `sync_avg` 1.73–2.72ms、同侧 qps 跨度 279–419）；首轮未交替顺序的门禁曾读到 −8.5%，属顺序偏差。机制上 c8 的分配速率只有 c64 的 1/8，放宽 GC 目标在本档预计接近零效果，据此按无退化处理。

**校准**：连续三轮在 journal 串行预算里找零头之后，本轮改从运行时全局成本入手。同一份证据也划出了上限：每查询 4 条审计记录、5 个串行动作、每次 fsync 3.1ms 是这台机器上的物理下限，编码只占 5.8%，提交侧微秒级收益已低于噪声。

**代价**：同一容器同一负载下平台进程 RSS（`/proc/<pid>/status` 采样）r34 p50 33.7MB / 峰值 60.3MB，r35 p50 55.1MB / 峰值 93.0MB——峰值 +33MB（+55%），绝对值仍不到 100MB，符合「堆目标 5× 存活集、再受 cgroup 上限约束」的预期。

**下一轮提示**：分配结构才是剩余的结构性杠杆——cursor 路径占分配 52%，`callVersionInto` 0.45GB（12.4%）。减少每次查询的分配量（例如 rows 先解码再重编码的往返）会同时降低 GC 频率与 assist，本轮只是把代价摊薄、没有消除。另：`state_lock_wait_ns_total` 每样本仍有 0.3–0.6ms/请求，远超 drainer 持有状态锁的时间，值得单独定位。

#### R36：延迟归属测量——worker 每请求固定开销 + 排队（2026-10-30，留证，不改代码）

**问题**：R35 之后 c64 每查询 34.1ms（p50 38.7ms），按动作：open 9.61 / describe 3.66 / statement 6.31 / cursor_start 8.14 / close 6.35ms。哪一段还能动？

**平台侧先被排除**：干净 A/B 快照显示每查询 1,284 次追加里只有 0.27 次 sync（c1 是 4.1 次），fsync 链 70% 忙——R34/R35 已把这条链上的收益吃掉；`operation_journal_encode_ns_total` 只占墙钟 3.6–5.8%，提交侧微秒级优化低于轮间噪声。

**传输探针（`d2877e4`，仅诊断）**：在 `sqlHTTPUpstream` 的请求上挂 `httptrace`，按命令分组累计 `GotConn`/`WroteRequest`/`GotFirstResponseByte` 与读完响应的时间。c64 预热后每命令 500–1,000 次调用的均值：连接复用 **94%**，`wait`（取连接）0.03ms、`write` 0.02ms、`tail`（读响应）0.02ms，**全部时间在 server**——describe 2.03ms、statement 1.22ms、cursor 5.91ms、open_session 4.34ms、close_session 1.47ms。把查询换成 `SELECT 1` 后各命令 server 时间几乎不变（1.59 / 0.97 / 7.09 / 4.11 / 1.16ms），而 worker 自报 `query` 只有 0.48ms/次：**这是与 SQL 无关的每请求固定开销加排队，不是执行**。同一命令在 c8 约 0.3ms、c64 涨到 2–7ms（8 倍并发换 6.7 倍膨胀）。

**CPU 复核（1,538 qps 窗口，8.1s）**：platform 2.40 core-s（0.30 core）、worker 0.81 core-s（**0.10 core**）。两者都远未饱和，进一步确认瓶颈是等待而不是算力，也排除了「worker 是 debug 构建所以算得慢」的解释。

**指向**：worker 的 `RefreshGate` 按数据库串行 `open`/`begin`/`close`/`health`（`src/worker_rpc/sessions.rs`），注释称守卫区内工作是微秒级；若守卫区实际包含镜像刷新或健康探测，40 个在途请求就会排出 4ms 量级的队列，与 open_session 4.34ms 吻合。cursor 6–7ms 需要 worker 侧分段计时才能定性。`rpc_lanes` 与 owner 授权计数在采样时刻全为 0，可先排除车道准入。

**结论**：平台侧剩余空间只剩「每动作约 1.8ms 的请求/JSON 开销」与「4 次审计屏障等待（下限是该机器 1.5ms 裸 fsync，实测批 2.3–3.1ms）」，都不足以支撑一轮 ≥5% 的改动。下一轮若要再拿双位数，必须在 worker 侧做分段计时（`RefreshGate` 守卫区、cursor 执行到首字节之间），那就需要可复现的 Rust 构建路径；本轮先留下这组可复用的探针与数字。

#### R37：非提交操作回复后异步持久化——短查询降到 sqld 1.1–1.4×（2026-11-03）

**问题**：R36 把 c1 短查询的剩余成本定位到「每条命令一次 fsync」：单连接 connect→all→close 只发 **2 个 HTTP 请求**，却产生 **3 条耐久记录**（open / 只读 audit / close），`appends==syncs`、每次 fsync 2.36ms，合计 **9.67ms = E2E 的 46%**；worker 自报 SQL 0.53ms。issue #30 要求把 20 行 cellp、0 行 peri、按 id 单行三类短查询压到 sqld 的 1.5× 以内。

**授权边界（issue #30 评论 6593598631）**：用户批准「放松非提交耐久点」——open、只读 statement、只读 cursor、close 的 completion+audit 改为回复后按提交顺序异步持久化；写语句 intent（dispatch 前）与 terminal（ack 前）仍同步 durable；隔离/Unknown/clean gate 规则不变。明确接受：崩溃时尾部已确认的只读/open/close 记录可能丢失；若 close 丢失，残留 pending open 仍按现有 fail-closed 规则阻止 clean restart（不放宽）。

**机制（`8684987` + `1dfa9f4`）**：`durability_deferred.go` 新增延迟耐久账户——固定 8 worker 池、队列上限 256，`settle` 在池满/已停止/无账户时回退内联等待（背压变延迟、绝不返回 503）；失败只记第一条 ERROR 并让后续请求得到 503 `audit_unavailable`；`Gateway.Close` 在优雅关闭时排空。completion 准入槽位改为**入队即释放**，drain 跳过已释放记录；completion 预算耗尽时 close 仍 dispatch，worker 会话在回复前关闭。首版 `8684987` 在 c64 失败 656–1192/1280（429/502/503）：异步等待仍占用 64 个 completion 槽 → 503 → close 未执行 → `session_seats=190` → `worker session capacity exceeded`；`1dfa9f4` 修复后通过。

**A/B（c1，v1.1.27 vs r37b，3 轮 × 3 样本/侧，每样本 20 次，逐轮交替，两侧全新卷）**：cellp **19.89→5.81ms（−70.8%）**、peri **21.36→5.68ms（−73.4%）**、by-id **19.99→4.84ms（−75.8%）**，27/27 样本 fail=0。同 harness 的 sqld 对照：cellp **1.09×**、peri **1.37×**、by-id **1.23×**，达到 #30 的 ≤1.5× 目标。c64 回归门（3 轮 × 3 样本/侧，64 并发 × 20 次/worker）：qps cellp +4.6%、peri +1.3%、by-id +5.2%，p50 −1.0% / −0.2% / −4.0%，27/27 样本 fail=0。

**崩溃恢复 E2E（2026-11-03，`a353454d…`）**：证据与命令见 `docs/performance-review/2026-11-03-r37-crash-recovery/`。
- **对照 A**：写一行并 ack → 等延迟屏障排空 → SIGSTOP `dev.py`、SIGKILL 平台进程 → 用 r37b 直接按同一配置重启 → 行仍可读、平台正常服务；随后优雅关闭，`verify_no_unresolved_operations` **pass**。证明已 ack 写不丢、journal/审计链可重放。
- **进程崩溃 B**（探针 `b7f0d0d`，把 close 屏障延迟 5s 后 SIGKILL）：journal 中 close 记录**仍在**。写记录在回复前已 `write()` 进页缓存，SIGKILL 不丢页缓存——延迟窗口真正暴露的是**机器崩溃/掉电**，不是进程崩溃。
- **丢失尾部 D**（探针 `055d1ee`，在统一提交处跳过 close 记录以确定性模拟掉电丢失尾记录）：崩溃前 journal 只有 open（revision 13，`close_session` 计数 0）。用 r37b 重启：journal 链正常重放，pending open 被启动恢复**隔离**（`state=quarantined`、`may_have_committed=true`、`cleanup_outcome=unknown`），`SELECT 1` 返回 **HTTP 409**；`verify_no_unresolved_operations` 报 `unresolved platform operation prevents clean restart`。这正是授权中接受的 fail-closed 结果。
- **限制**：未做真实掉电/机器崩溃测试；D 是确定性故障注入。A/D 的重启是直接启动平台二进制（绕过 `dev.py` clean gate），clean gate 单独在优雅关闭后的物化状态上执行。D 中无写入，写不丢由 A 覆盖。

**已知风险与下一杠杆**：① 进程崩溃不丢尾部，掉电才可能丢；close 尾记录丢失会被启动恢复隔离，命名空间在操作员释放前不可用（已授权，需在发布说明中写明）。② unified journal 的 64 个 completion 槽仍限制 dispatch 并发；瞬时超过 64 个在途请求仍可能 503，下一个杠杆是槽位数量本身。③ close 审计行在 completion 预算耗尽时回退 request 预算；两者都满时 close 可能在会话已退役后返回 503（不泄漏会话，但客户端看到歧义重试）。④ 池饱和转为延迟，高负载 p99 会上升；`dropped_by_class.completion` 可能增加，需改用 `queue_count`/`queue_bytes` 加 journal status 计费。

#### R39：worker 镜像刷新闸门改为共享/独占租约（2026-11-03，合入）

**问题**：worker 的 `RefreshGate`（`src/worker_rpc/sessions.rs`）按数据库串行化 open/begin/close/health。
每次状态命令在临界区内做原生工作：`open_session` 约 250µs、`close` 约 90µs。R38 D1 已量到 open/close 的
66–78% 时间花在排队（517.9µs / 387.0µs），闸内可移出的准备只占 1.5%。会话生命周期又占每请求大头，
于是同库并发被一把本应只保护「镜像替换」的闸门限速：X1 的 p50 从 c1 的 0.78ms 涨到 c16 的 7.3–7.6ms，
16 个并发包的理论排队（15 × 350µs ≈ 5.1ms）与实测 c1→c16 的增长 6.6–6.9ms 同量级。

**上限探针（未合入）**：把闸门临时改为直通（保留 `refresh_initial_image` 调用，只去掉互斥），
在基线栈与探针栈（同数据、同平台二进制、worker sha256 `3b4b85c2…`）两轮交替：
c16 p50 7.63→3.54 / 7.29→4.17ms（−42.8%~−53.6%），c4 1.71→1.15 / 1.41→0.81ms，
c1 0.78→0.77 / 0.74→0.74ms。探针源码已还原，二进制保留在 `/tmp/peri-perf/bin/peri-loom-v1-probe`。

**实现（`0b879c9`）**：闸门改为共享/独占租约。
- 数据面 `open`/`begin`/`close` 持共享租约并发执行；`pin_image` 先在共享侧检查
  `initial_image_replaced`，为真才释放共享、取独占、重查并执行替换，随后的命令在独占侧执行（罕见路径）。
- 镜像替换（`clear_database`、`close_owned`）持独占租约；维护清扫用非阻塞的 `try_exclusive`。
- 等待中的写者挡住后来的读者（`writers_waiting`），避免持续数据流饿死替换；两侧都遵守 deadline，
  任何状态变化 `notify_all`，超时者不会把其他等待者留到各自的 deadline。

**Gate 0（机制核验）**：候选第一次运行 c16 p50 27.76ms（刚导入数据后的冷态瞬态），第二次 3.64ms，
落在探针区间；c8 1.25–1.76ms 低于基线 2.2–4.0ms——共享路径确实生效。正式 A/B 只用预热过的栈。

**A/B（8 轮/侧，A/B/B/A 与 B/A/A/B 交替，json-pipeline，100 请求/档）**：基线 worker `3f2a5175…`
（从 `main` 重建，与原始基线二进制逐字节同哈希）vs 候选 `13129acb…`；平台二进制不变（`80caaec…`）；
两侧独立卷、同一导入（201 threads / 8,625 messages / 20,177,069 chars），v1 耐久（`audit_file_v1`）。

| 并发 | 基线 p50 中位（范围） | 候选 p50 中位（范围） | 变化 | p25 中位变化 |
|---:|---:|---:|---:|---:|
| 1 | 0.82ms（0.74–0.84） | 0.79ms（0.75–0.85） | −3.6%（噪声） | −2.5% |
| 4 | 1.32ms（1.21–2.15） | 0.96ms（0.83–1.40） | **−27.6%** | −30.3% |
| 8 | 3.06ms（2.03–4.04） | 2.05ms（1.47–13.94） | **−32.9%** | −26.7% |
| 16 | 9.83ms（4.97–43.35） | 5.36ms（3.97–26.79） | **−45.5%** | −37.4% |

- c16 单轮存在「快/慢态」翻转（同一侧跨轮 4.97–43.35ms，p95 恒在 130–160ms），按 8 轮中位判定；
  三条并发档同向，c1 不变。
- **同龄新栈对照**：新建 `run-base3`（与候选栈同龄、同样刚导入、跑基线 worker）：c4 1.37/1.40 vs 候选
  0.88/1.18ms、c16 6.29/7.33 vs 4.59/4.71ms；c8 2.06/3.36 vs 2.44/2.83（小样本内重叠）。排除「新栈更快」
  的混淆，方向与 8 轮结论一致。

**回归门（1 warmup + 5 samples，A/B/B/A，各 2 次运行取中位）**：F1.select-1 0.98→0.90ms（−8.2%）、
L1.list-20 1.47→1.47ms（0%）、B1.batch-20 2.30→2.17ms（−5.7%）、C1.baton-10 0.360→0.365ms（+1.4%，噪声）、
H1.content-20mb（json-cursor）151.96→142.35ms（−6.3%）、H3.content-projection 149.6→146.65ms（−2.0%）。
无 >10% 回归。

**质量门**：Rust lib 677 passed / 0 failed / 10 ignored；sessions 模块 10/10（新增共享并发、写者优先、
超时不拖累等待者三个用例）；`clippy --all-targets --no-deps -D warnings` 退出码 0；`cargo fmt --check`
干净。测量运行 429=0、错误 0；结束后 `session_seats`/`active_permits`/`unknown_held_permits` 均为 0，
`peak_sessions` 16（每库上限，符合预期），候选累计 `session_limit` 拒绝 0。

**剩余风险 / 未验证**：
1. c16 的测量被一个与本轮无关的 ~150ms 尾延迟污染：基线栈生命周期内 `head_checkpoint_attempts_total`
   1111 次、`head_checkpoint_ns_total` 24.2s（均值 21.8ms），叠加 fsync 均值 3.66ms/次；它同时作用于
   两侧，是运行级「慢态」的来源。单轮 c16 差值不可作结论，只能看多轮中位；下一轮应查审计头检查点的
   触发频率与锁内成本。
2. `sweep` 的 `try_exclusive` 在有读者时跳过该库一轮；持续高负载下会话过期/回收可能延后，空闲后仍会执行。
   未做长时高负载下的会话回收验证。
3. `close_owned`（控制面按 owner 关闭）现在要等在途读者排空；60s deadline 下未做极端压测。
4. 同一数据库现在允许并发 `open_session`。功能由 Rust 单测、全量测试与 8+ 轮压测覆盖，但未做对抗性
   并发/故障注入。
5. 本轮环境是重建的合成 Issue 28 数据集 + 本地 release 二进制（127.0.0.1、无 TLS），绝对数字只在本窗口内
   可比，不与历史 Issue 28 行比较。

**环境记录**：历史 Issue 28 容器/数据集在本机不存在，本轮重建了确定性夹具
（`/tmp/peri-perf/seed_fixture.py`，201 threads / 8,625 messages / 20,177,069 chars）与两个同源基线栈；
空 A/B 校准（同二进制两栈，2 轮 × 5 样本）F1/B1/H1 离散 <1%，证明栈间可比后才进入正式测量。

#### R40：审计头检查点放宽的上限探针（2026-11-03，探针轮，未合入）

**动机**：R39 的测量窗口里，基线栈累计 `head_checkpoint_attempts_total=1,111`、
`head_checkpoint_ns_total=24.2s`（均值 21.8ms/次），而单次审计追加 fsync 均值 3.66ms；
检查点是 4 次屏障（payload/floor 各 temp-write + fsync，再 rename + 目录 fsync）。先例是
operation journal 压实预算 256→1024→4096：R19 中性、R32/R34 在限速器变化后分别 +19.39%/+9.61%。
若同构，把检查点周期从 32 条记录放宽 8 倍应当摊薄固定成本。

**探针（未合入）**：`prototypes/v1/platform/state_disk.go` 的 `auditBatchMaximum` 32→256，
构建一次性平台二进制 `/tmp/peri-perf/bin/peri-platform-r40-probe`（sha256 `aefcb903…`）；
源码构建后立即还原（`git checkout -- prototypes/v1/platform/state_disk.go`），无分支、无提交。
基线平台从 `main`（`2c9a8f3`）重建（`909a6453…`），两侧 worker 同为 R39 的 `13129acb…`。
两栈独立卷、同一导入、v1 耐久；探针同时放宽了追加批上限（arm C），不单独做“只放宽 witness 步长”的
arm B——碎片化来自去掉检查点停顿，而不是批上限，C 已是更宽松的上限。

**机制核验（Gate 0）**：
- X1 每轮检查点 **39→5（−87%）**，单次成本两侧相当（约 21–30ms）；
- B1+F1 窗口（双方各 324 成功请求）检查点 11→1、209.3→19.9ms；追加 fsync 两侧同为 42 次。

**端到端结果（两侧交替，A/B/B/A）**：

| 场景 | 基线 p50（两次运行） | 探针 p50（两次运行） | 判定 |
|---|---:|---:|---|
| F1.select-1 | 0.78 / 0.87ms | 1.01 / 0.91ms | 未改善（偏慢） |
| B1.batch-20 | 2.11 / 2.17ms | 2.30 / 2.24ms | 未改善（+6~9%） |
| X1 c1 | 0.78 / 0.78 / 0.75 / 0.73ms | 0.73 / 0.79 / 0.78 / 0.73ms | 噪声内 |
| X1 c4 | 0.99 / 1.03 / 1.27 / 0.86ms | 0.83 / 4.10 / 1.87 / 1.07ms | B1 略好，B2/B3 劣化 |
| X1 c8 | 1.74 / 1.52 / 2.28 / 1.98ms | 1.39 / 87.69 / 2.59 / 1.66ms | B2 429 风暴；B3 劣化 |
| X1 c16 | 3.72 / 2.96 / 4.07 / 6.19ms | 3.49 / 190.70 / 33.11 / 38.55ms | B1 相当（7×429）；B2–B4 劣化 |

- B2 的 `status_429=21/6`、`errors=39/8`：探针在 c4 上用 86ms 完成 100 请求（约 1,160 rps），
  耗尽项目 500 rps 令牌桶；B1 的 c16 也吃到 7 次 429，此后 B3/B4 虽无新 429，c16 仍 33–39ms。
- 归因：基线侧检查点停顿本身在“顺带”合并审计批——每轮追加 fsync 稳定在 48–51 次（约 31 条/批）；
  探针去掉停顿后，drain 面对的是刚到达的零散记录，每轮追加 fsync 升到 84/371/170/172 次，
  B3/B4 审计总 I/O ≈1.1s，与基线≈1.06s 相当甚至更高。**检查点成本被它的合批副作用部分抵消，
  单纯放宽周期不改变串行审计 I/O 总量。**

**判定**：上限不成立（无 ≥10% 改善，小查询不改善且 X1 多次劣化），不实现、不申请耐久语义授权。
本方向不再重试；若未来审计链需要降低检查点成本，应同时设计“显式合批窗口”，不能只改周期。

**协议经验**：X1 的 100 请求/档没有速率限制，候选一旦明显变快（本例 c4 86ms）就会触发 429，
使该档失去可比性；后续 X1 A/B 需在运行间留 ≥2s 间隔或降低每档请求数，且 429>0 的样本不得计入中位。

**证据**：`/tmp/peri-perf/{r40-probe.log,r40-probe-metrics.csv,r40-probe2.log,r40-probe2-metrics.csv,r40-actions.py,r40-h1.py}`、
`/tmp/peri-perf/r40-{A1..A4,B1..B4}`、`/tmp/peri-perf/r40p2-*`；探针脚本 `r40-probe.sh`、`r40-probe2.sh`。

#### R41：会话建立与只读探测改用本地校验（2026-11-03，未达门槛，不合入）

**归因**：F1（`execute SELECT 1` + `close`）基线中位 0.76–0.88ms，worker 自报 `query_ms` ≈0.139ms；
平台相位 span 的 `close` p50 0.115ms（native 0.075、terminal 0.022），未归属 ≈0.001ms——平台侧每请求
开销不足 0.1ms。worker RPC 无操作 `capabilities` RTT p50 0.074ms；`Database::session()` 分段探针
`validate1` p50 686µs（有负载时漂到 700–1400µs），其余阶段 ≤2µs；authority RPC 链 `validate_session`
p50 497µs（稳态窗口 143µs）、`open_session` 142µs、`validate_lease` 271µs、`close_session` 84µs。
直接计数：`main` 上一条 F1 形态 pipeline（execute + close）恰好花 **2 次** authority `validate_session`
——一次来自 `Database::session()`/`is_readonly()` 的开库，一次来自 execute 路径；`close` 为 0。
可移除的是前一半（约 0.14ms / 0.88ms ≈16%）。

**改动**（分支 `perf/r41-session-open-local-validate` / `e4b39ac`，5 文件 +43/−4）：
- `ObjectStore` 新增带默认实现的 `validate_session_local`/`validate_transaction_local`，默认回退到完整
  校验，权威侧存储（LocalStore/BranchStore/FencedStore 等）行为不变；
- `ContextStore` 实现本地版：`generation()` + `context.check(...)`（事务版保留 draining 拒绝新事务的
  规则），`MetricsObjectStore` 透传；
- `src/database.rs::session()` 两处 `validate_transaction(false)`、`is_readonly()` 与
  `src/session/lane.rs::is_readonly()` 改用本地版。
边界：打开会话与只读探测不读写任何存储对象；权威端会在真正读写的操作上重新校验绑定，头部 CAS 仍是
最终 fence。execute 路径上的那次 `validate_session` **保留**——它守的是数据操作，移除会改变安全语义，
本轮未尝试。

**上限探针**（未合入，源码已还原）：候选 worker 里在 Validate RPC 前直接返回 Ok（`5819171f…`），
A/B/B/A 20 样本：F1 json 0.767/0.788→0.466/0.461ms（−39%）、pb 0.664/0.687→0.421/0.440ms（−36%）。
上限远高于门槛，先证明方向成立再写生产代码。

**Gate 0（机制核验）**：同一 diag 栈上 20 条 pipeline 的 authority `validate_session` 计数 **40→20**
（候选 `a89bff00…`；基线同法测 `13129acb…` 为 40），即每请求恰少一次，与改动一一对应。

**A/B 结果**（两侧独立预热栈：基线 worker `13129acb…`、候选 worker `a89bff00…`，同一平台
`peri-platform-r41-base` `c8cf19fe…`，同一导入 fixture，每轮 A/B/B/A，组间 ≥2s）：

| 轮次 | 每侧样本 | 基线 json / pb（ms） | 候选 json / pb（ms） | json 变化 | pb 变化 |
|---|---:|---:|---:|---:|---:|
| 1 | 20 | 0.750 / 0.676 | 0.660 / 0.536 | −12.0% | −20.7% |
| 2 | 20 | 0.830 / 0.745 | 0.646 / 0.583 | −22.2% | −21.7% |
| 3 | 30 | 0.778 / 0.682 | 0.705 / 0.527 | −9.3% | −22.6% |
| 4 | 30 | 0.784 / 0.663 | 0.572 / 0.582 | −27.1% | −12.2% |
| 5 | 50 | 0.689 / 0.832 | 0.571 / 0.687 | −17.1% | −17.5% |
| 6 | 50 | 0.746 / 0.757 | 0.613 / 0.635 | −17.9% | −16.1% |

- 6 个 run 中位数取中位：json **0.764→0.629ms（−17.6%）**、pb **0.714→0.582ms（−18.4%）**；
  **6/6 轮同向**（json 最弱一轮 −9.3%，pb 最弱一轮 −12.2%）。
- 同一轮内配对差值的中位：json **−17.5%**、pb **−19.1%**。

**判定：未达门槛、不合入。** F1 属 <10ms 场景，`CLAUDE.md` 性能实验协议（2026-10-30 用户指定）
要求主指标 ≥20%；实测 −17.6%/−18.4%（配对中位 −17.5%/−19.1%），低于门槛。分支与提交 `e4b39ac`
保留作证据，`main` 保持 `3cce186`。本轮未在 A/B 前另写判据，采用上述既存协议门槛。

**回归门**（5 样本/轮，A/B/B/A，候选 vs 基线）：
- L1.list-20：1.42/1.47→1.36/1.10、1.46/1.13→1.16/0.96（改善，p95 噪声大）；
- B1.batch-20：2.19/2.16、2.16/2.25→2.49/2.43、1.94/5.22（protobuf 单点 5.22ms、p95 6.60，
  按抖动处理；B1 不作为本轮收益声明）；
- C1.baton-10：0.37/0.36→0.33/0.33（−10%）；
- X1：c1 0.79/0.80→0.61/0.61（−23%）、c4 1.11/0.89→0.72/0.80、c8 2.35/3.26→2.16/2.16、
  c16 3.47/2.63→3.49/3.67（持平/噪声，p95 两侧 100–160ms）；全程 429=0；
- H1.content-20mb：json 144.81/147.02→149.16/145.79ms、pb 120.26/125.29→123.38/122.67ms（持平）。
无 >10% 且可复现的退化。一次更早的 X1 基线 c16 曾出现 20.08ms / p95 150ms，当时 load average 2.15、
另有无关 `peri` 进程占 9.6% CPU；随后的 A/B/B/A 未复现，c16 按环境敏感项处理。

**配置**：fixture 201 threads / 8,625 messages / 20,177,069 chars；v1 耐久（`audit_file_v1`）；
配额 max_databases 8、max_sessions 16、max_concurrent 100、rps 500、max_sql_bytes 65536、
max_result_bytes 4194304；127.0.0.1、无 TLS；A/B 期间无观测器。Rust 1.98.1 / Go 1.27.3 / macOS arm64。
worker 两侧各从对应源码 release 构建；平台两侧同为 `c8cf19fe…`。

**正确性 / 测试**：`cargo fmt --check` 干净；`cargo clippy --all-targets --no-deps -- -D warnings` 通过；
`cargo test --release --locked --lib`（`ulimit -n 92160`）**677 passed / 0 failed / 10 ignored**。
不加 fd 上限时曾报 1 个 `Too many open files`
（`authority::local_migration_tests::near_8mib_v1_root_migrates_on_a_real_local_store`），
属 shell 软 fd 上限（256）导致的环境失败，与 diff 无关。

**剩余风险 / 未测量**：
1. 该改动把 open/readonly 探测上的权威绑定校验推迟到第一次真实操作；改变的是校验时点而非授权结果
   （权威仍在数据操作上校验、头部 CAS 兜底）。若未来要合入，需在记录中明确这一语义。
2. 未做崩溃恢复 E2E（不涉及耐久时点/fencing）；若合入需补。
3. B1 protobuf 的单点 5.22ms 未复跑；X1 c16 环境敏感。
4. 证据在 `/tmp/peri-perf/`（`r41probe-F1-*`、`r41ab*`、`r41reg-*`、`r41x1b-*`、`r41h1-*`、
   `r41-diagcount*`；采集与计数脚本 `r41-rpc-count.py`、`r41-phases.py`、`r41-rtt.py`），非持久目录；
   需长期留档时应复制进 `docs/performance-review/`。

**下一轮提示**：F1 剩余的 authority 校验在 execute 路径（每请求 1 次），移除它会改变数据操作前的
安全语义，不在性能轮范围内。更大的结构性杠杆仍是会话生命周期：每请求 open+close 约 387 qps /
p50 2.15ms，而复用一条 baton 约 1,650–1,697 qps / p50 1.34ms（≈0.8ms/op），但改动面远大于本轮。

#### R42：Hrana 隐式 open 融合首条只读语句（2026-11-03，protobuf 达标、JSON 未达 20%，不合入）

**归因**：R41 记录显示 F1 形态 pipeline（`execute SELECT 1` + `close`）在 `main` 上恰好花 **2 次**
authority `validate_session`（open 一次、execute 一次），open 侧那次约 0.14ms / 0.88ms ≈16%。
本轮不删校验，而是把「隐式 open」与「首条顶层只读 execute」合并成一次 worker 请求：在同一 lease
window 内先开会话再执行语句，authority 校验只付一次，同时省掉一次 worker RTT（无操作
`capabilities` RTT p50 0.074ms）。

**改动**（分支 `perf/r42-fused-open-statement` / `0bc488b`，14 文件 +1134/−28）：
- Rust：新增 `WorkerCommand::OpenGuardedStatement`（`src/worker_rpc/protocol.rs`）；`dispatch.rs`
  按 `open_session` 打开会话，再在同一请求的 lease window 内 `begin` + `guarded_readonly_statement`，
  返回 `{session, readonly, statement}`；语句由 worker 引擎判定只读，写语句在步进前返回
  `requires_intent`，因此该命令自身不可能提交；`capabilities` 增加 `{"open_guarded_statement":1}`
  （仅 v1）；`sessions.rs::result_row_count` 读取融合结果行数。
- Go：`gateway_fused_open.go` 承载请求上下文标记、私有命令构造与严格校验、capability 探测
  （独立负缓存 TTL）、应答解析与 `fusedGuardedStatement`（重跑准入、结果预算 413、写语句走
  `committingStatement` 的 durable intent、同样的失败分类与 unknown 语义）；`hrana_fused_open.go`
  只对「隐式 open + 首条 execute」提供候选并校验回包；`hrana_http.go`/`hrana_protobuf.go` 只在
  隐式 open 分支融合，批计划从下一条开始；`gateway_sessions.go` 在 open 内计算候选、发送融合命令，
  旧 worker 返回 400 时重建普通 open 并重试一次。
- 边界：融合语句仍是只读；open 仍在派发前通过 `verifyNonCommittingAdmission`；open/close 记录仍按
  R11 的「回复后写一条已结算记录」；公共协议不变（`fused_statement` 只出现在进程内适配器拼接的
  open 结果里，外部 `/v1/sql/open` 不携带）。

**Gate 0（机制核验）**：同一 diag 栈（`PERI_DIAG_SLOW=1 PERI_DIAG_RPC_LINK=1`）上 20 条
`SELECT 1` + `close` pipeline 的 authority `validate_session` 计数 **40→20**
（`{"delta":{"validate_session":20},"requests":20}`），即每请求恰少一次。

**A/B 结果**（两侧独立预热栈：基线平台 `c8cf19fe…` + worker `13129acb…`，候选平台 `ac976bc7…` +
worker `b48b8b55…`；同一导入 fixture；3 轮 × 每轮 A/B/B/A，样本 20/30/50，组间 ≥2s）：

| 轮次 | 每侧样本 | 基线 json / pb（ms） | 候选 json / pb（ms） | json 变化 | pb 变化 |
|---|---:|---:|---:|---:|---:|
| 1 | 20 | 0.796 / 0.879 | 0.711 / 0.643 | −10.7% | −26.9% |
| 2 | 30 | 0.824 / 0.780 | 0.635 / 0.609 | −22.9% | −22.0% |
| 3 | 50 | 0.727 / 0.814 | 0.560 / 0.563 | −23.0% | −30.8% |

- 6 个 run 中位数取中位：json **0.766→0.635ms（−17.1%）**、pb **0.819→0.593ms（−27.5%）**；
  **6/6 轮同向**（配对差值区间 json −8.7%~−31.6%、pb −15.5%~−36.1%）。
- 同一轮内配对差值的中位：json **−17.4%**、pb **−29.1%**。
- 候选侧 `query_ms` 0.006–0.013ms vs 基线 0.14–0.17ms：融合命令走 guarded statement 的引擎计时
  口径，属口径差异，不作为收益。
- 响应字节 json 312 / pb 40，与基线一致。

**判定：protobuf 达标、JSON 未达 20%，不合入。** F1 属 <10ms 场景，`CLAUDE.md` 性能实验协议
（2026-10-30 用户指定）要求主指标 ≥20%。pb −27.5%（配对 −29.1%，6/6 同向）达标；JSON −17.1%
（配对 −17.4%，6/6 同向）未达，且与 R41 独立测得的 −17.6% 同量级。按「判据先冻结、例外须用户批准」
的规则，本轮整体不合入；分支与提交 `0bc488b` 保留作证据，`main` 保持 `d88c2a1`。若用户批准例外，
可直接 `git merge --no-ff perf/r42-fused-open-statement`（Gate 0、回归门、质量门均已通过）。

**回归门**（5 样本/轮，A/B/B/A，同一对二进制）：
- L1.list-20 json：1.414/1.374→1.247/1.317ms（侧中位 −8.0%，改善）；
- B1.batch-20 json：2.708/2.458→2.620/2.326ms（−4.3%）；protobuf：2.338/2.479→2.430/2.334ms
  （−1.1%，噪声）；
- C1.baton-10 json：首轮 B3 腿出现 0.63–0.91ms 的瞬时停顿（p50 0.63 vs 其他腿 0.34–0.37），
  复测 A1/B2/B3/A4 = 0.551/0.366/0.371/0.371ms，**不复现**；
- X1 json：首轮 c16 = 18.11/15.33/3.92/3.20ms（前两腿受宿主瞬时影响），复测 3.22/2.76/11.52/3.57ms；
  c1/c4 候选更快（c1 0.74/0.75→0.60/0.61，复测 0.79/0.77→0.59/0.68）；全程 **429=0、errors=0**；
- H1.content-20mb（cursor）：json 148.21/145.36→150.12/152.90ms（侧中位 +3.2%）、
  pb 126.87/124.44→126.55/129.68ms（+2.0%），均在噪声内；响应字节 20,608,525 / 20,272,006 两侧一致。
- 无 >10% 且可复现的退化；X1 c16 仍为环境敏感项。

**配置**：fixture 201 threads / 8,625 messages / 20,177,069 chars；v1 耐久（`audit_file_v1`）；
配额 max_databases 8、max_sessions 16、max_concurrent 100、rps 500、max_sql_bytes 65536、
max_result_bytes 4194304；127.0.0.1、无 TLS；A/B 期间无观测器。Rust 1.98.1 / Go 1.27.3 / macOS arm64。
平台基线 `c8cf19fe…`、候选 `ac976bc7…`；worker 基线 `13129acb…`、候选 `b48b8b55…`。
harness 的 `platform_label` 记录的是检出 HEAD（`0bc488b`），两侧相同，实际二进制以 SHA 为准。

**正确性 / 测试**：`cargo fmt --check` 干净；`cargo clippy --all-targets --no-deps -- -D warnings`
通过；`cargo test --release --locked --lib`（`ulimit -n 92160`）**677 passed / 0 failed / 10 ignored**；
`cargo test --release --locked --test statement_worker fused_open` 4/4、`capabilities_answer…` 1/1；
`go build ./cmd/peri-platform ./cmd/peri`、`go vet ./...` 干净；`go test -count=1 .`（platform 包，
由执行会话运行）通过（171.7s）；本轮复核定向融合用例 `go test -run 'TestHranaFused|TestFusedOpen…' .`
通过。新增 11 个 Go 用例覆盖 JSON/pb 融合、旧 worker 400 回退 + 负缓存、`requires_intent` 提交路径、
语句错误保留会话、unknown 不重放不隔离、批计划起点、结果预算、私有命令校验；Rust 侧 4 个融合用例。

**剩余风险 / 未测量**：
1. 本轮**没有做写生产代码前的上限探针**（直接采用反思估算），是本轮对单轮流程的唯一偏离；
   Gate 0、A/B、回归门与质量门均已完成。
2. 融合语句在 open 请求内执行，早于 `dispatchLocked` 的逐语句准入（隔离/配额复查）；open 本身在
   派发前已通过 `verifyNonCommittingAdmission`，且 worker 在步进前证明只读，最坏情况是一条只读
   语句在随后被拒的请求中执行过（无数据效果）。
3. capability 正缓存 60s 不主动失效；worker 升级需缓存过期或平台重启（与 R5 的只读批相同）。
4. 适配器首条语句与融合语句不匹配时会重发该只读语句（不产生重复的可见效果）。
5. 未做崩溃恢复 E2E：本轮不改变耐久时点或 fencing（open 记录仍按 R11 在回复后写），但融合命令
   把语句执行提前到 open 请求内。
6. 证据在 `/tmp/peri-perf/`（`r42ab-*`、`r42reg2-*`、`r42reg3-*`、`r42warm-*`、`r42-diag*`；
   脚本 `r42-ab.sh`、`r42-reg2.sh`、`r42-reg-h1.sh`、`r42-reg-c1x1.sh`、`r42-rpc-count.py`），
   非持久目录。

**下一轮提示**：融合后 F1 json 仍有 0.635ms，剩余成本主要在 close 路径（1 次 worker RTT +
`close_session` + 审计）与平台侧两次 HTTP 编解码；pb 已到 0.593ms，JSON 的额外开销来自 JSON
编解码本身，继续压 JSON 不在 worker RTT。若用户批准 R42 例外合入，下一步可在其之上评估
「close 与最后一条语句融合」的上限；否则从 `main` 重新选向。

#### R43：Hrana `[execute, close]` 融合会话关闭（2026-11-03，合入）

**归因与上限探针**：R42 把 open 与首条只读 execute 融合后，F1 剩余成本仍在 close 路径。
零代码形状探针（`/tmp/peri-perf/r43-shapes.py`，基线栈，120 组交错样本、0 错误）给出 p50：
完整 `[execute, close]` 0.752ms、仅 `[execute]` 0.608ms、复用 baton 的 `[execute]` 0.342ms、
带 baton 的 `[close]` 0.218ms；分解为 open ≈0.266、execute ≈0.342、close 入口 ≈0.143ms。
可融合的 close 入口开销估计 0.05–0.07ms（在 R42 已测 −0.131ms 之上），投影 0.752→0.57–0.60ms
（−20%~−24%），高于 20% 门槛，因此实现。

**方案与改动边界**：
- worker：`open_guarded_statement` capability level 1→2；level≥2 时接受 `close_after_statement`，
  在语句结算后于同一请求/租约窗口内关闭会话（`dispatch.rs`、`protocol.rs`）。只在语句结果
  **已结算**时关闭（成功，或带 `execution_completed` 的 SQL 错误）；`requires_intent`、未结算错误、
  `begin` 失败保留会话。close 失败返回 `close:{"status":"error"}` 并保留注册会话，平台回落普通 close。
  `deny_unknown_fields` 使 R42 级 worker 对整条命令返回 400，平台安全降级。
- platform：`gateway_fused_open.go`/`hrana_fused_open.go` 解析并校验 `fused_close`，
  `hrana_http.go` 只对精确的 `[execute, close]` 形态启用；`gateway_sessions.go` 在会话上记录
  `fusedClose`，close 入口**仍运行原 handler**（准入、open/close 记录、R37 回复后异步耐久、
  `dischargePendingOpenSessions`、配额、retirement），仅跳过 worker close RPC；记录写失败保留标记，
  重试仍跳过 worker。`gateway_operations.go` 的空闲清理与 `Gateway.Close` 排空共用同一分支。
- 一处必要的窄行为变化：open 已融合 close 的条目，`result_limit`/`sql_error` 的 describe→cursor
  回退被抑制——已退役的 worker 会话会把已结算答案变成 502 `outcome_unknown` 并污染会话；只读、
  无数据副作用，由 `TestHranaFusedCloseDoesNotReProbeTheRetiredSession` 覆盖。
- 回退矩阵（全部回落普通 close）：open 回复 unknown、`requires_intent`、`close.status!=ok`、
  `close` 缺失/畸形、旧 worker `unsupportedCommand`。

**Gate 0**：临时平台探针（`sql_upstream.go` 加一行 `println`，未提交、源码已还原，探针二进制
`96673a3e…`）+ authority diag（`PERI_DIAG_SLOW=1`）：20 条 `[execute, close]` pipeline →
worker `open_guarded_statement` **20**、`close_session` **0**；同平台配 R42 级 worker（level 1）
对照为 20 + **20**（退化为普通 close）——即「关掉改动」后 close RPC 回归。diag 侧
`validate_session` 候选 **20/20**、基线 **40/20**。

**验收（A/B/B/A）**：两侧独立预热栈（base = `main` `db96753` 代码：平台 `peri-platform-r41-base`
`c8cf19fe…`、worker `peri-loom-v1-cand` `13129acb…`；cand = `6d5c9da`：平台
`peri-platform-r43-cand` `dfc4ae16…`、worker `peri-loom-v1-r43-cand` `27d8dd46…`），
3 轮 × 4 腿，样本 20/30/50，组间 ≥2s，无观测器。逐轮中位（ms，base A1/A4 → cand B2/B3）：
- json：r1 0.790/1.214 → 0.513/0.704；r2 0.799/0.807 → 0.547/0.503；r3 0.700/0.701 → 0.470/0.508。
- pb：r1 0.769/0.768 → 0.506/0.530；r2 0.772/0.798 → 0.519/0.524；r3 0.737/0.752 → 0.494/0.491。

6 样本中位：json **0.794→0.510ms（−35.7%）**、pb **0.768→0.513ms（−33.3%）**；同轮配对中位
json −39.3/−34.6/−30.2%、pb −32.6/−33.5/−33.8%，**6/6 同向**。响应字节 312/40 与基线一致，
全部运行 `warnings=[]`、无 429/错误。对 R42 候选（0.635ms）本轮 close 融合单独贡献约 −0.125ms，
约为探针投影的 2 倍；未进一步归因。

**回归门**（5 样本/腿，A/B/B/A，≥2s）：L1 **1.388→1.231ms（−11.3%）**、B1 json 2.249→2.337
（+3.9%）、B1 pb 2.375→2.300（−3.2%）、C1 0.373→0.361（−3.2%）、H1 cursor json 149.59 vs
145.13ms（A4 的 233.59ms 为宿主离群，A1 对候选 −3.0%）、pb 127.40→121.64（−4.5%）。
X1 复测 2 轮 A/B/B/A：c1 候选 0.52–0.61 vs 基线 0.73–0.83ms，c4 接近，c8/c16 两侧均出现宿主瞬态
（候选 2.35–6.52 / 4.93–23.84ms，基线 1.95–2.54 / 2.57–20.79ms），方向不可确认；全部运行
429=0、errors=0、verdict PASS，结束时 `session_seats=0`、`active_permits=0`。

**正确性/测试**：新增 Go `gateway_fused_close_test.go`（+377，8 个融合 close 用例 + 表驱动形态/资格）
与 `gateway_fused_open_test.go` 更新；Rust `statement_worker.rs` +168（融合 close 成功、`requires_intent`
不关闭、SQL 错误仍关闭、关闭后会话不可用且 close 幂等、未知字段 → 400）。质量门：`cargo fmt --check`
干净；clippy `-D warnings` 退出码 0（仅 9 条上游 `turso_core` 警告）；`statement_worker`
**42 passed / 0 failed**；`cargo test --release --locked --lib`（`ulimit -n 92160`）
**677 passed / 0 failed / 10 ignored**；`go build ./cmd/peri-platform ./cmd/peri`、`go vet ./...` 干净；
平台 `go test -count=1 .` **PASS（180.3s，运行两次）**；定向 `-race` PASS（69.6s）。本轮复核：
`go build`/`go vet`/`fmt`、定向 Go 融合用例（ok 1.494s）、Rust `fused_close` 3 passed / 0 failed。

**配置与二进制**：默认配额（`max_sessions=16`、`max_concurrent=100`、`rps=500`、
`max_result_bytes=4MiB`），`audit.durability_source=audit_file_v1`（未启用 v2 完成日志），
local backend、无 TLS；fixture 201 threads / 8625 messages / 20177069 chars。两侧均未开观测器。
harness 的 `metadata.git_revision` 对两侧都写 `6d5c9da`（读取工作区 HEAD），两侧身份以二进制
SHA256 与端口区分。

**剩余风险/未测量**：
1. 未做真实 `sessions.close` 失败的 Rust 集成测试（Go mock 覆盖平台回退，Rust 分支仅代码审查）。
2. 未做崩溃恢复 E2E：融合把 worker 会话退役提前到语句回复内，平台若在 close 记录持久化前崩溃，
   恢复仍按现有 fail-closed 规则把 pending open 视为未解决并隔离——与 R37 已接受的「掉电丢 close
   尾记录」同类，窗口约等于被省去的 close 入口时间；本轮未实测该窗口。
3. 只覆盖精确 `[execute, close]` 形态；cursor close、批量读、写路径不变。
4. capability 正缓存不主动失效（与 R42 相同），worker 降级需缓存过期或平台重启。
5. 融合 close 条目抑制 describe→cursor 回退（只读、无数据副作用），属有意的窄行为变化。
6. X1 c8/c16 环境敏感，本轮不宣称并发收益；无容量/SLA 结论。
7. 本分支包含 R42 的 cherry-pick `d41ca9d`：R42 单独轮次未达 20% 的判决不变，本轮的 A/B 基线是
   不含 R42 的 `main`，收益为 R42+R43 合计；close 融合单独贡献约 −0.125ms（R42 候选 0.635→0.510）。
8. 证据在 `/tmp/peri-perf/`（`r43ab-*`、`r43reg*`、`r43warm-*`；脚本 `r43-ab.sh`、`r43-reg.sh`、
   `r43-rpc-count.py`、`r43-shapes.py`），非持久目录。

**下一轮提示**：F1 json 已到 0.510ms、pb 0.513ms；剩余成本是平台两次 HTTP 编解码 + close 入口的
记录/准入，以及 worker 侧每条语句的固定原生时间。继续压 F1 需要减少每请求的 HTTP 往返或复用会话
（会改变会话语义）；更高价值的方向转向 c64 容量路径（R37 之后的 completion 槽位、审计批），
或按 R23 的结论处理 worker 每查询固定原生时间。

#### R44：R43 之后剩余方向的上限探针（2026-11-03，测量轮，无代码合入）

**背景**：R43 合入后 F1 json 0.510ms、pb 0.513ms；`<10ms` 场景的验收门槛是 20%（=0.102ms）。
本轮先对三条候选做零/低代码上限探针，只有上限过门槛才实现。

**D1：close 的每次线程 spawn（否掉）**
- `worker_rpc/session_entry.rs` 的 `Entry::close` → `start_close_when` 为每个 close spawn 一个 OS 线程。
- 零代码微基准（`/tmp/peri-perf/spawn_bench.rs`，rustc -O，n=2000）：`spawn+condvar+join`
  mean 0.021ms / p50 0.021 / p95 0.029 / p99 0.034。
- 上限 0.021ms = F1 的 **4%**，远低于 20% 门槛；固定清理执行器方案作废。

**D3：pipeline 每个 entry 的内部 HTTP/JSON 跳转（否掉）**
- `hrana_http.go` 的 `dispatch`：每条 entry 都 `json.Marshal` → `httptest.NewRequest` →
  `Gateway.ServeHTTP` → 解码 reply。
- 零代码微基准（`/tmp/peri-perf/hop_bench.go`，与实际 request/reply 编码同形）：
  marshal 0.61µs、`httptest.NewRequest` 1.16µs、reply 解码 0.69µs、响应编码 0.16µs，
  合计 **2.6µs/entry**。
- F1（2 entry）≈5µs ≈1%；B1（21 entry）≈55µs ≈2.4%；直调 handler 的重构不做。
  reflect 预估的 10–25µs/entry 高了 4–10 倍。

**authority lease 校验（唯一过门槛，但属安全语义变更）**
- lease window 已把一轮请求内的多次校验压到**每请求 1 次**（实测一轮 40 请求 = 40 次 `check_binding`）。
- 探针 A（`probe/r44-validate` @ `eb11608`，R41 的部分本地校验补丁 cherry-pick 到 main）：
  F1 0.50 → 0.46/0.49ms（−2~−8%）——只去掉 open 路径的几处，第一个校验仍会落到 authority。
- 探针 B（同分支 `0a65f65`，把 `ContextStore::validate_session`/`validate_transaction` 全改本地）：
  F1 **0.50 → 0.34/0.36ms（−28%~−32%）**。
- 探针 C（`probe/r44-rpc-cost` @ `a576fb8`，worker 侧 + authority 侧计时；按一轮 F1 40 请求的
  计数/均值增量计算）：worker 侧往返 **207µs/次**；authority 侧 `check_binding` **≈84µs/次**，
  其中 `ownership_view()`（`store.get(authority/root)` + 解码）≈85µs，两次 `live()` 合计 ≈2µs。
  即 authority 侧几乎全部成本是每次校验都重读并解码 authority root。
- 判决：去掉这次校验可拿 −28%~−32%，但读路径此后没有任何 authority 交互，迁移/失联后的陈旧读
  要到下一次写或对象访问才暴露——属安全语义变更，需用户明确授权；本轮询问被拒绝，**不实现**。
  只做 authority 侧安全优化（缓存并失效 root 解码视图、保留全部检查）省 ≈85µs = F1 的 17%，
  仍低于 20% 门槛，不单独成轮。

**本轮结论**：无代码合入 `main`。F1 在「不改变安全语义」的前提下已到结构下限；探针分支与二进制留证：
- `probe/r44-validate`（`eb11608`、`0a65f65`）、`probe/r44-rpc-cost`（`a576fb8`），均不合入。
- 二进制：`peri-loom-v1-r43-cand` `27d8dd46…`（main）、`peri-loom-v1-r44-validate` `f385f510…`、
  `peri-loom-v1-r44-novalidate` `991075ca…`、`peri-loom-v1-r44-rpcprobe` `ed1b7f40…`。
- 证据 `/tmp/peri-perf/`：`r44check/`、`r44v-A1/A2`、`r44v-B1/B2`、`r44nv-B1/B2`、`r44-shapes.py`、
  `hop_bench.go`、`spawn_bench.rs`、`run-r44*.log`，非持久目录。

**下一轮提示**：`<10ms` 场景（F1/B1）在安全边界内已无 ≥20% 的空间。下一轮转向门槛 10% 的更大场景
（H1 约 145ms、R1-1000 约 70ms 的大结果路径），或 c64 容量路径（R37 之后的 completion 槽位、
deferred durability 池饱和）；若用户授权放宽 lease 校验，可直接实现 −28%~−32% 的候选。

#### R45：Hrana cursor 行按页缓冲（2026-11-04，合入）

**背景**：R44 后 `<10ms` 场景已到结构下限，转向 >10ms 的大结果路径。R45 归因（临时探针
`probe-e`，相对干净二进制 +1.1–3.5%）把 H1 json-cursor 154.6ms 拆为：worker RPC 46.5ms（30.1%）、
平台 `sql_decode` 复制 13.5ms（8.8%）、gateway 页校验 17.9ms（11.6%）、adapter `pageScanner.scan`
12.9ms（8.3%）、行编码 + `writer.Write` **58.3ms（37.7%）**，其中逐行写约占 27ms。上限探针
（只改读路径、页级缓冲）152.09→127.30ms（−16.3%），响应字节逐样本一致。

**改动**（`2a52f46`）：`hrana_v3_json_cursor.go` 与 `hrana_http_cursor.go` 的流式写路径不再对每行
调用 `writer.Write`，改为页内累积、页尾一次写出；所有帧编码（start/step_begin/results/step_end/
error）与 `fatal`、写游标错误路径之前先冲刷缓冲，行顺序与「已写出的行不回滚」语义不变；页面
`flusher.Flush()` 边界不变。写游标的 `maxWrites` 测试改为按页粒度（第 2 页中断，语义不变），新增
`TestHranaV3JSONCursorPageBufferKeepsRowBytes` 固定缓冲后的行字节与 step 顺序。

**Gate 0**：临时计数 writer（`r45_probe.go`，测前删除；探针二进制 `ad8def88…`）：H1 json 8,625 行
→ **34** 次响应写出，protobuf **37** 次；整包 20,608,525 / 20,272,006 字节与基线逐字节一致，
rows=8,625。移除探针后重建候选 `549f0f68…`。

**配对 A/B**（2 轮 × A/B/B/A，每 leg 1 warmup + 5 timed；base `peri-platform-r43-cand`
`dfc4ae16…` = main，cand `peri-platform-r45-cand` `549f0f68…`，两侧同一 worker `27d8dd46…`，
各自全新卷 + 同一合成 fixture：8,625 行 / 20,177,069 字符）：

| 协议 | 第 1 轮 | 第 2 轮 | 合并 |
|---|---:|---:|---:|
| json-cursor | 152.88→121.74ms（−20.37%） | 158.74→125.50ms（−20.94%） | **154.23→123.20ms（−20.12%）** |
| protobuf-cursor | 125.57→100.04ms（−20.34%） | 127.74→102.55ms（−19.72%） | **126.44→102.03ms（−19.31%）** |

两协议各 20 样本/侧、2/2 轮同向；每样本 rows=8,625、响应字节逐样本一致、fails=0。按 >10ms 场景
10% 门槛双协议达标，约为门槛的 2 倍。

**回归门禁**（4 legs A/B/B/A，5 timed/leg）：H3.content-projection json-cursor
148.77→126.66ms（**−14.86%**，同一机制）；F1 json −2.86% / pb +0.30%；C1 baton-10 +5.29%；
B1 json −0.20% / pb −63.90%（基线腿含 8.97/15.27ms 瞬态，方向为改善）；R1-1000 首轮 +10.16%，
复测 46.92→44.30ms（−5.60%）不复现。L1 两侧 4 条腿均 `row_count_mismatch`：合成 fixture 主导
cwd 的线程数超过 SQL `LIMIT 40`，而 harness 期望恰好 `list_threads` 行，与候选无关。X1 两轮
（各 4 legs）在 c4/c8/c16 上方向互换、腿内中位跨 1.4–39.7ms，属宿主瞬态，不可确认。全程
`warnings=[]`、`fatal=[]`、429=0、非 200=0，两侧结束 `session_seats=0`、`active_permits=0`。

**测试**：`gofmt`、`go vet ./...`、`go build ./cmd/peri-platform ./cmd/peri` 干净；
`PERI_RUST_BINARY=…/target/debug/peri-loom-v1 go test -count=1 .` 全包 **ok 183.2s**。

**证据**：`docs/performance-review/2026-11-04-r45-cursor-page-buffer/`（`results.md`、`gate0.txt`、
`r45-ab.sh`、`r45-reg.sh`、`ab-raw/` 逐样本 raw + summary）。原始日志仍在 `/tmp/peri-perf/`（非持久）。

**下一轮提示**：H1 剩余 123ms 中行编码仍约 31ms；gateway `validCursorFetchResponse`（17.9ms）与
adapter `pageScanner.scan`（12.9ms）重复校验同一页，合计约 25ms（16%），但单独任一项都 <10%，
需合并归属才过门槛。R1-A（融合语句答案解析 3 次 → 1 次，上限 13.1ms / 27.6%）是下一条明确
过门槛的方向。

#### R46：Hrana fused open 的 statement 答案只解析一次（2026-11-04，合入）

**背景**：R45 归因把 R1 json-pipeline 单请求 46.5ms 拆开，其中 `parseFusedStatementAnswer`
被调用 **3 次**、合计 **20.0ms（43.1%）**：gateway open 校验一次、Hrana 适配器发布前校验一次、
dispatch 再解析一次；单次约 6.7ms（decode 9.3ms + `validStatementResult` 10.8ms 分摊到三次）。
R1-A 的上限是只解析一次省 13.1ms（27.6%）；只去掉 dispatch 那一次的上限约 6.7ms（14%）。

**改动**（`d79500c`）：`hranaOpenResult` 新增 `FusedAnswer` / `fusedAnswerValid`，`hranaOpen`
在发布前解析后保留答案；`fusedStatementResult` 携带 `Answer` / `AnswerValid`；
`hranaFusedStatementResult` 改为接收 `hranaOpenResult`（`hrana_http.go`、`hrana_protobuf.go`
两个调用点）；`fusedGuardedStatement` 直接使用 marker 里的答案，只有 `!AnswerValid` 时才回退
解析 `Reply`。gateway open 自身的校验、适配器对非法 payload 的 502、结果预算、写语句 intent
边界与失败分类均未改动。新增 `TestHranaFusedStatementMarkerCarriesTheParsedAnswer`。

**Gate 0**：临时计数器（`perf_probe_r46.go`，测前移除；探针二进制 `8c6787e7…` / `406114c0…`，
diff 见证据目录）：同一 fixture 跑 `R1.limit-1000 json-pipeline`（1 warmup + 3 timed = 4 次
fused statement）——base 12 次解析 / 4 次 dispatch = **3.0**；cand 8 / 4 = **2.0**，
复用 4/4、回退 0。移除探针后从干净提交重建候选 `27d37e3d…` 再测量。

**配对 A/B**（2 轮 × A/B/B/A，每 leg 1 warmup + 5 timed；base `peri-platform-r46-base`
`8b42d4ee…` = `main` @ `bad41e6`，cand `peri-platform-r46-cand` `27d37e3d…` = `d79500c` 干净构建，
两侧同一 worker `27d8dd46…`，各自全新卷 + 同一合成 fixture：201 线程 / 8,625 消息 / 20,177,069 字符）：

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 46.78 / 48.67 | 39.69 / 40.02 | **47.72→39.86（−16.48%）** |
| 2 | 43.00 / 47.75 | 37.49 / 38.29 | **45.37→37.89（−16.49%）** |

合并四腿中位 **47.27→38.99ms（−17.51%）**；4/4 配对腿同向（−15.15% / −17.77% / −12.81% /
−19.81%），候选最慢腿低于基线最快腿。每 run 5 样本、rows=1,000、响应体 2,369,290 字节、0 错误。

**对照与回归门**：同窗口 H1 json −1.53% / +2.95%、protobuf −2.99% / −0.65%；F1 json
−6.56% / +9.72%（方向相反）。回归门：B1 json +4.87% / pb +2.57%、C1 −0.13%、
H3 json −0.27% / pb −1.66%、L1 json-cursor +1.71% / json-pipeline +4.26%。F1 json 单轮
+14.79% 触发复核：专项 3 轮 × A/B/B/A 得 −0.04% / +0.51% / −43.42%（末轮由 base 单腿 1.734ms
离群驱动），12 腿合并中位 0.595→0.601ms（**+0.93%**），判定噪声、不复现。L1 两侧 8 条腿均
`row_count_mismatch`（合成 fixture `list_threads=100` 对 SQL `LIMIT 40`，与候选无关）。
X1 最高并发档腿间中位 base 30.37 / 28.70ms、cand 22.68 / 4.92ms、各腿 p95 143–160ms，
宿主瞬态不可确认；本轮 diff 不进入并发调度路径。全程 `warnings=[]`、`fatal=[]`、429=0、
非 200=0；结束时两侧 `session_seats=0`、`active_permits=0`，动作计数逐项相同
（open/close 1,085、statement 1,522、cursor_fetch 2,412）。

**测试**：`gofmt -l .` 干净；`go vet ./...`、`go build ./cmd/peri-platform ./cmd/peri` 通过；
`PERI_RUST_BINARY=…/target/debug/peri-loom-v1 go test -count=1 .` **ok 172.806s**。

**证据**：`docs/performance-review/2026-11-04-r46-fused-parse-once/`（`results.md`、`gate0.txt`、
`r46-ab.sh`、`r46-reg.sh`、`probes/`、`ab-raw/`、`reg-raw/`、`f1-rerun/` 逐样本 raw + summary）。
原始日志仍在 `/tmp/peri-perf/`（非持久）。

**下一轮提示**：R1 仍有 2 次解析（gateway open + 适配器），每次约 6.7ms；把其中一次也复用
需要在两层之间传递同一解析结果，契约改动大于本轮。另一个已量化方向是 R1-B：pipeline 大结果
span 直通（去掉 decode → convert → re-encode，9.2–10.7ms / 19–23%），与 R45 的 cursor 页直通同源。

#### R47：融合 open 的答案解析跨进程内边界直传（2026-11-04，合入）

**背景**：R46 之后 R1 json-pipeline 单请求仍对同一份 2.37MB 语句回答完整物化多次。计时探针
（基线 `d05f314`）每调用：`parseFusedStatementAnswer` 6.39ms（×2/请求），其中
`validStatementResult` 3.53ms（×2）；adapter 解码内部 open 信封 2.11ms（×1）；
execute 结果解码 2.99ms；逐单元转换 0.07ms；被丢弃的结果预算 `json.Marshal` ~1.1ms；
最终 `json.Marshal(response)` 3.4–5.2ms。可移除的是「adapter 解码信封 2.11ms +
第二次完整解析校验 6.39ms」≈ **8.5ms/请求（21%）**，高于 10% 门槛，值得实现。

**改动**（`e4b6606`，8 文件 +144/−4）：`hranaOpenReply{session, fused_statement, fused_close,
answer, answerValid}`；`gatewayResult.hranaOpen` 由 `writeResponse` 交给 recorder
（`captureOpen`），不再把 2.4MB 信封编码进内部响应；adapter `hranaOpenFromGateway` 直接使用
答案，跳过 `json.Unmarshal(reply.result, &opened)` 与第二次 `parseFusedStatementAnswer`。
只对携带融合标记的 pipeline open 生效（`fusedOpenStatementFrom(ctx)`）——cursor、websocket
与公开 open 仍编码信封，公开响应字节不变。adapter 保留三条拒绝规则（未请求融合却收到语句、
有 `fused_close` 无语句、缺少 `AnswerValid` 校验证明）。网关的校验边界、结果预算、写语句
intent 边界与 `outcome_unknown` 语义均未移动。

**Gate 0**：计时探针（测前移除；base `9f62c6a3…` = `d05f314` + 探针，cand `afba7370…` =
`e4b6606` + 同一探针），同一 fixture 跑 `R1.limit-1000 json-pipeline`
3 rounds × (1 warmup + 5 timed) = 18 请求，次/请求：

| 侧 | fused_parse | validate | open_unmarshal | typed_open | decode |
|---|---:|---:|---:|---:|---:|
| base | 2.0 | 2.0 | 1.0 | 0.0 | 1.0 |
| cand | 1.0 | 1.0 | 0.0 | 1.0 | 1.0 |

**配对 A/B**（2 轮 × A/B/B/A，1 warmup + 5 timed/leg；base `peri-platform-r47-base`
`d170405f…` = `main` @ `d05f314`，cand `peri-platform-r47-cand` `fdb914cf…` = `e4b6606`
干净构建；两侧同一 worker `27d8dd46…`、各自全新卷 + 同一 fixture：201 线程 / 8,625 消息 /
20,177,069 字符）：

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 37.99 / 42.21 | 24.24 / 32.12 | **40.10→28.18（−29.73%）** |
| 2 | 36.20 / 38.11 | 24.85 / 27.66 | **37.15→26.25（−29.34%）** |

4/4 配对腿同向（−36.21% / −23.90% / −31.35% / −27.42%），候选最慢腿 32.12ms 低于基线最快腿
36.20ms。实测降幅 ≈11.9ms 高于上限探针的 8.5ms，差额来自不再编码 2.4MB 内部信封与随之减少的
分配。每 run 5 样本、rows=1,000、响应体 2,369,290 字节（个别 2,369,300，10 字节差来自
`query_duration_ms` 的数字位数，已逐请求核对）、0 错误。

**对照与回归门**：同窗口 H1 json-cursor +0.67% / +1.05%、H1 protobuf-cursor +1.77% / −1.30%、
F1 json-pipeline +3.43% / −7.12%（方向不一致、量级在噪声内；cursor 路径无 diff）。回归门
（4 腿 A/B/B/A）：B1 json +6.37%、B1 pb **+18.39%**（单腿 3.22ms 离群）、C1 −0.36%、
F1 json −5.13%、F1 pb +8.93%、H3 json +4.29% / pb −0.85%、L1 json-cursor +0.69% /
json-pipeline −11.31%。B1 protobuf 追加两批复测：2 × A/B/B/A × 8 采样得 +18.65% / +8.34%；
3 × A/B/B/A × 10 采样得 −13.05% / +11.23% / −4.15%，6 腿中位 **−0.43%**（base 2.356 vs
cand 2.345ms），两侧分布几乎重合（base 2.20–3.19、cand 2.21–3.10）⇒ 不可复现噪声；B1 只有
一行 `SELECT 1`，不存在可解释的回归机制。L1 两侧均有 `row_count_mismatch`（合成 fixture
`list_threads=100` 对 SQL `LIMIT 40`），json-pipeline −11.31% 记为 fixture/宿主噪声。X1 4 腿
16 并发中位 5.74/31.40（base）与 28.38/29.33（cand）ms、p95 137–198ms、0 错误 0 个 429，
分布受宿主瞬态主导，记「不可确认」。全程 `warnings=[]`、`fatal=[]`、非 200=0、结束
`session_seats=0`、`active_permits=0`。

**测试**：`gofmt -l .` 干净；`go vet ./...`、`go build ./cmd/peri-platform ./cmd/peri` 通过；
`PERI_RUST_BINARY=…/target/debug/peri-loom-v1 go test -count=1 .` **ok 175.894s**。新增 3 个
用例：`TestHranaGatewayResponseTakesTheTypedOpenAnswer`、
`TestHranaOpenWithoutFusionStillReturnsTheEnvelope`（实现过程中曾是真回归——cursor/websocket
适配器直接解码 `reply.result`，因此 typed 直传必须以融合标记为门）、
`TestHranaOpenFromGatewayKeepsTheParsedAnswer`。

**证据**：`docs/performance-review/2026-11-04-r47-fused-open-answer/`（`results.md`、`gate0.txt`、
4 个运行脚本、2 个汇总脚本、`probes/`、`ab-raw/`、`reg-raw/`、`b1-rerun/`、`b1-rerun2/` 逐样本
raw + summary）。原始日志仍在 `/tmp/peri-perf/`（非持久）。

**下一轮提示**：R1 剩余成本里 execute 结果仍被整段物化（解码 2.99ms + 最终 `json.Marshal`
中属于结果的部分 3.4–5.2ms），方向是 JSON-pipeline 大结果 span 直通（R1-B）；但本轮已吃掉
约 12ms 基线，上限必须在本轮候选上重新探针，不能沿用 9.2–10.7ms 的旧估计。其次是
`validStatementResult` 的 span 校验（C2）与计数写者预算核对（C3）。

#### R48：融合 open 应答改为单遍扫描校验（2026-11-04，合入）

**背景**：R47 之后 R1 json-pipeline 每次融合 open 仍对同一份 2.37MB 语句回答做「信封整体
`json.Unmarshal` + `validStatementResult`（顶层 map + `[][]RawMessage`）」的完整物化。上限
探针（base `a167ace`，n=18）每调用：`parseFusedStatementAnswer` **6.33ms**，其中
`validStatementResult` 3.50ms；adapter 解码 2.93ms；被丢弃的结果预算 `json.Marshal` 1.43ms；
最终 `json.Marshal` 6.85ms；单遍扫描下界（`json.Compact` 代理）**0.92ms**。D2 可移除
6.33−0.92 = **5.41ms/请求（20.8%）**，高于 10% 门槛。基线 smoke 26.06ms（n=15）。

**改动**（`1b44553`，4 文件 +1770/−44）：新增 `fused_reply_scan.go`（严格 JSON 扫描器：
10,000 层嵌套上限、SWAR 8 字节字符串扫描、非 ASCII/转义键回退 `json.Unmarshal` +
`strings.EqualFold` 以复刻 encoding/json simple-fold、尾随内容与未知信封键拒绝）与
`fused_statement_scan.go`（复刻 `validStatementResult`：cols/rows 结构、单元格形状、重复键
last-wins、`rows:[null]` 零宽行、autocommit 仅裸布尔、数值字段走 `json.Unmarshal` 保留
`-0`/溢出/null 语义）。`parseFusedStatementAnswer` 改为委托 `scanFusedStatementReply`；
`answer.result` 是应答字节的 span，零拷贝（应答字节此后不可修改）。`validStatementResult`
保持不变，是权威与测试 oracle。只影响融合 open 内部路径；公开 `/v1/sql/open`、cursor、
websocket 响应字节不变。

**Gate 0**：探针二进制 `0e3bffd51bac…`（`1b44553` + 调用计数/计时，测前移除），栈 :19498，
2 rounds × (1 warmup + 5 timed) = 12 个 R1 请求：`scan_calls` 1→12（**1.0/请求**）、
`scan_ok=1`、`legacy_calls` **全程 0**、`scan_ns` 0.538–0.788ms、`reply_bytes`
2,345,209–2,345,220。新路径命中、旧校验器离开请求路径。

**配对 A/B**（2 轮 × A/B/B/A；base `peri-platform-r48-base` `d61d3b70558b…` = `main` @
`a167ace`，cand `peri-platform-r48-cand` `ac9137985688…` = `1b44553` 干净构建；两侧同一
worker `27d8dd46…`、各自全新卷 + 同一 fixture：201 线程 / 8,625 消息 / 20,177,069 字符）：

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 25.864 / 30.615 | 27.908 / 19.823 | **28.24→23.87（−15.49%）** |
| 2 | 31.581 / 29.531 | 22.281 / 27.008 | **30.56→24.65（−19.35%）** |

逐对配对 +7.91% / −35.25% / −29.45% / −8.54%（3/4 同向），全体 4 腿中位 **30.07→24.64ms
（−18.05%）**；实测降幅 ≈5.4ms 与上限探针 5.41ms 一致。rows=1,000、响应体 2,369,290 字节、
`warnings=[]`、0 非 200。

**对照与回归门**：同窗口 H1 json-cursor −1.13% / −3.76%、H1 protobuf-cursor −1.15% / +0.29%、
F1 json-pipeline −3.48% / +13.94%（亚毫秒，绝对差 ≈0.07ms、方向不一致）。回归门（4 腿
A/B/B/A）：B1 json −11.96%、B1 pb **+19.92%**（单腿离群）、C1 −2.81%、F1 json +12.98% /
pb +9.22%（亚毫秒）、H3 json −1.31% / pb −1.16%；L1 两侧均 `row_count_mismatch`（fixture
`list_threads=100` 对 SQL `LIMIT 40`），不作判据；X1 4 级并发两侧分布受宿主瞬态主导，记
「不可确认」。B1 protobuf 追加 2 轮 × A/B/B/A × 8 采样：pb r1 −10.52% / r2 +15.80%、
json r1 −0.90% / r2 +13.22%，慢值集中在单个候选腿（3.139ms / 2.709ms），其余候选腿
（2.20–2.376ms）不高于基线腿（2.18–2.79ms）；合并 6 腿池化中位 pb **+1.6%**、json **+1.8%**
⇒ 不可复现噪声（B1 只有一行 `SELECT 1`，改动只让解析更快）。全程 `warnings=[]`、`fatal=[]`、
非 200=0。

**测试**：`gofmt -l .` 干净；`go vet ./...`、`go build ./cmd/peri-platform ./cmd/peri` 通过；
`PERI_RUST_BINARY=…/target/debug/peri-loom-v1 go test -count=1 .` **ok 173.746s**。
差分测试：约 130 个手写用例 + 5 个真实 R1 变体 + 3,500 个确定性变异（种子 0x48）在
`(ok, kind, result bytes, reason, message, executionCompleted)` 上与旧实现完全一致，另做
20,000 样本（种子 0x1234）抽样对拍一致。微基准（真实载荷）：**6.5–7.7ms → 0.52–0.56ms**、
分配 2,076→4、15,868,288→39 B/op（≈12×）。

**证据**：`docs/performance-review/2026-11-04-r48-fused-reply-scan/`（`results.md`、`gate0.txt`、
`probe-base.log`、`probes/`、`ab-raw/`、`reg-raw/`、`b1-rerun/`、运行与汇总脚本）。原始日志
仍在 `/tmp/peri-perf/`（非持久）。

**下一轮提示**：R1 剩余成本里 adapter 仍把 `result` span 解码为 `[][]any` 再重新编码；
D1 上限 10.29ms 中本轮已吃掉 parse+validate 5.41ms，剩余 adapter 解码 2.93ms + 被丢弃的预算
`json.Marshal` 1.43ms + compact 0.09ms ≈ **4.45ms/请求**。下一步是 adapter JSON 直转码复用
statement span；上限必须在本轮候选上重新探针，不能沿用本轮数字。之后才是 C3 预算长度核对与
C2 的非融合路径 span 校验。

#### R49：Hrana JSON execute entry 单遍直转码与响应字节复用（2026-11-04，合入）

**背景**：R48 之后 R1 json-pipeline 仍把 2.35MB 结果完整物化：adapter `json.Decoder` 解码为
`hranaNativeStatement`（`[][]any`，3.04ms）、转 Hrana 单元格（0.064ms）、entry 预算 marshal
（1.51ms，字节随即丢弃）、最终响应再 marshal（1.91ms）。按 entry 字节拼装响应只需 0.119ms。

**上限探针**（`main` @ `c5d2997`，n=18，R1 中位 26.0ms）：C3'（只复用 entry 字节）1.79ms
（6.9%）低于 10% 门槛，不单独立项；D1（解码/转换/entry 编码合并为单遍）3.10–4.61ms；
D1+C3' 4.89–6.40ms（18.8–24.6%）。据此实施合并方向。

**改动**（`d84924d`，3 文件 +1181/−3）：新增 `hrana_encoded_execute.go`——`hranaEncodedEntry`
（`MarshalJSON` 原样返回字节，失败路径仍逐字节一致）与 `encodeHranaExecuteEntry`（单遍严格扫描
worker 结果 span，直接产出 `{"response":{"result":{…},"type":"execute"},"type":"ok"}`；等价复刻
`hranaNativeStatement`/`hranaOutputValue`/metrics 语义，含重复键 last-wins、`cols:[null]`、
`rows:[null]`、整数保留原始字面量、float 按 encoding/json 格式化、blob base64、字符串 HTML 转义
与 U+2028/U+2029/非法 UTF-8；不支持形状返回 false 回退旧路径）；`assembleHranaResponse` 按
entry 字节拼装最终响应（一次预分配）。`hrana_http.go`：`executeWithin` 在融合标记命中
（session/SQL 与 gateway 检查一致）且 statement 成功时走直转码，失败回退；预算块直接使用
entry 字节；全部 entry 有字节时拼装、否则回退 `json.Marshal(response)`。只影响 HTTP JSON
pipeline，protobuf/cursor/WS/公开 API 不变。

**Gate 0**（探针构建 `2b4fd46674dc…`，测前移除，栈 :19498，12 个 R1 请求）：`fast_attempt`
1、`fast_ok` 1、`fast_fallback` 0；`entry_reused` 1、`entry_marshaled` 1（close 走旧路径）；
`assemble_ok` 1、`assemble_fallback` 0；**`legacy_equal` 每请求 1、`legacy_mismatch` 0**——现场
拼装字节与 `json.Marshal(response)` 逐字节一致；`final_bytes` 2,369,288–2,369,299。

**配对 A/B**（2 轮 × A/B/B/A；base `peri-platform-r49-base` `8c96e7fdab09…` = `main` @
`c5d2997`，cand `peri-platform-r49-cand` `0bbbe901910b…` = `d84924d` 干净构建；同一 worker
`27d8dd46…`、各自全新卷 + 同一 fixture）：

| 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|
| 1 | 24.266 / 17.806 | 14.848 / 19.353 | **21.04→17.10（−18.71%）** |
| 2 | 21.739 / 20.931 | 16.822 / 15.951 | **21.34→16.39（−23.19%）** |

逐对配对 −38.81% / +8.68% / −22.62% / −23.79%（3/4 同向），4 腿整体中位 **21.34→16.39ms
（−23.2%）**；实测降幅约 5.0ms 与上限 4.89–6.40ms 一致。全部 R1 响应 2,369,290 字节
（一行 2,369,300，`query_duration_ms` 位数差）、1,000 行、仅 200、无 warning。

**对照与回归门**：同窗口 H1 json-cursor −1.23% / −3.45%、H1 protobuf-cursor −3.65% / −2.84%、
F1 +7.93% / −8.42%（亚毫秒）。回归门 B1 json +3.31%、B1 pb −1.99%、C1 +3.55%、F1 json −22.34%
（亚毫秒）、F1 pb +0.82%、H3 json −0.86%、H3 pb −2.41%；L1 两侧 `row_count_mismatch`
（fixture 项）、X1 不可确认。全部样本 `warnings=[]`、非 200=0。

**测试**：差分对拍（约 130 手写用例 + R1 真实结果 + 多类型单元格/重复键/未知键/尾部内容）新旧
路径逐字节一致；拼装对拍与 `json.Marshal` 一致；不可编码形状回退且错误码不变；微基准
4.53ms→2.49ms、4 allocs、2.5MB。`gofmt`/`go vet`/`go build` 通过；全量
`go test -count=1 .` **ok 176.567s**。

**证据**：`docs/performance-review/2026-11-04-r49-json-entry/`（`results.md`、`gate0.txt`、
`probe-base.log`、`probes/`、`gate0-smoke/`、`ab-raw/`、`reg-raw/`、运行与汇总脚本）。
原始日志仍在 `/tmp/peri-perf/`（非持久）。

**下一轮提示**：R1 的 2.35MB 结果在 Go 侧已不再整体物化或重复编码（解码、预算 marshal、最终
marshal 全部消失），剩余是 worker 固定成本与响应写出。protobuf pipeline 的 execute 仍走
adapter 解码与二次编码，先探针其成本与可复用边界；非融合路径（公开 open、cursor）可复用本轮
编码器但需先证明收益。任何上限必须在本轮候选上重新探针。

#### R50：Hrana protobuf pipeline execute 单遍直转码（2026-11-04，合入）

**背景**：R49 之后 JSON pipeline 已不再整体物化，但 protobuf 单执行 pipeline
（`[execute, close]`）仍把 2.35MB 结果完整物化：adapter 解码为 `hranaNativeStatement`
（`[][]any`，2.99ms）、转 Hrana 单元格（0.07ms）、`hranaProtoPipelineResponseWithin`
再编码整包（12.02ms）。

**上限探针**（`4784413` 零代码形状探针，R1 protobuf n=6，客户端中位 26.25ms）：
`hranaOpen`（含 worker 执行与 2.35MB 融合应答）8.61ms、worker `/v1/sql/statement`
0.17ms、解码 2.99ms、转换 0.07ms、protobuf 编码 12.02ms、写出 0.33ms；
`decode+convert+encode` = **16.36ms（64%）**。同探针确认 R1 的 execute+close 形状走
`serveHranaProtoSingleExecute`，没有 `json.Marshal(input)` 往返。

**改动**（`f73aa1c`，4 文件 +913/−0）：新增 `hrana_protobuf_execute.go`——
`encodeHranaProtoStatementResult` 复用 `fusedReplyScanner` 单遍严格扫描（重复已知键、
大小写变体、缺失或 null 的 `cols`/`rows` 一律回退），直接产出 protobuf 结果字节，
逐行预算计费；`assembleHranaProtoSingleExecute` 拼装 execute + close 两 entry
（close entry `1a040a020a00`）。`hrana_http.go` 在 `executeWithin` 融合标记命中时尝试
直转码（complete 返回空 reply 的编码结果，over-budget 走既有 413）；`hrana_protobuf.go`
的 `serveHranaProtoSingleExecute` 写出拼装结果。**刻意复刻**旧路径怪癖：`decltype` 与
`last_insert_rowid` 旧路径因 `*string` 断言不匹配从不输出，故只校验不输出；列只输出
`{1: name}`。只影响 HTTP protobuf 单执行 pipeline，JSON/cursor/WS/公开 API 不变。

**Gate 0**（`7cb020cbd5b7…`，测前移除，栈 :19496，4 个 R1 请求）：`complete=4`、
`unsupported=0`、`limit=0`、`equal=4`、`mismatch=0`、`legacy_bad=0`、`detail=2349027`
——4/4 命中直转码且与旧路径 decode→encode 逐字节一致。该探针栈 smoke 中位 20.76ms
不是性能数字（Gate 0 每请求额外跑一遍旧路径编码）。

**配对 A/B**（两侧独立平台二进制、各自全新同数据卷、同一 worker）：base
`ad327012752f…` = `4784413`，cand `522bf5ef5b79…` = `f73aa1c` 干净构建。

| 批次 | 轮次 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|---|
| 1 | 1 | 20.285 / 19.967 | 16.678 / 17.592 | **20.13→17.13（−14.86%）** |
| 1 | 2 | 25.056 / 20.688 | 18.551 / 14.732 | **22.87→16.64（−27.24%）** |
| 2 | 1 | 24.988 / 16.659 | 14.229 / 16.310 | −26.67% |
| 2 | 2 | 19.103 / 18.642 | 15.771 / 14.754 | −19.13% |
| 2 | 3 | 20.776 / 25.845 | 13.463 / 16.572 | −35.58% |

第 1 批 4 腿整体 **20.49→17.13ms（−16.36%）**、4/4 配对同向；第 2 批 6 腿整体
**19.94→15.26ms（−23.46%）**。5 轮全部 ≥14%，满足冻结门槛；两侧响应 2,349,053 字节、
1,000 行、仅 200。

**对照与回归门**：H1 pb +1.36%/+1.57%、H1 json −1.65%/−0.50%、F1 json
+8.33%/+9.46%（0.55→0.60ms）、F1 pb −15.23%/−7.44%（亚毫秒）。R1 json 第 1 批
+12.56% 来自一个离群爬坡腿，第 2 批 3 轮复核 −2.22%/−0.06%/−20.10%（聚合 −11.09%），
判噪声。回归门 B1 json −1.47%、B1 pb 单腿 +113.94%（15 样本/腿复核 2.47→2.30ms，
−6.9%）、C1 −8.25%、F1 json +5.37%、F1 pb −5.23%、H3 json −0.99%、H3 pb −1.16%、
L1 cursor −6.71%、L1 json +16.13%（复核 0.76→0.73ms，−4.1%）；L1 两侧
`row_count_mismatch`（fixture `list_threads=100` vs SQL `LIMIT 40`）、X1 不可确认。
全部样本 `warnings=[]`、非 200=0。

**测试**：8 个测试函数差分对拍（cols/rows/affected/metrics 变体、`cols:[null]`/
`rows:[null]`、重复键与大小写变体回退、`-0.0` 用 `math.Copysign` 构造、20MB 级长文本、
预算边界、拼装一致性）；`gofmt`/`go vet`/`go build` 通过；全量
`go test -count=1 .` **ok 174.556s**。

**证据**：`docs/performance-review/2026-11-04-r50-protobuf-execute/`（`results.md`、
`gate0.txt`、`probes/`、`ab-raw/`、`ab2-raw/`、`reg-raw/`、`reg2-raw/`、运行与汇总脚本）。
原始日志仍在 `/tmp/peri-perf/`（非持久）。

**下一轮提示**：R1 的 JSON（R49）与 protobuf（R50）两条 pipeline 路径都已不再物化或
二次编码，剩余是 worker 固定成本与响应写出。可评估非融合路径（公开 open、cursor）复用
R49/R50 编码器，或对非融合路径做 `validStatementResult` 的 span 校验；任何上限必须在
当轮候选上重新探针。

#### R51：cursor 页文本扫描与转码批量定位（2026-11-04，合入）

**背景**：R45 把 cursor 行改为按页缓冲后，H1/H3 的 20MB 页仍逐字节处理：网关校验
`hranaCursorPageScanner.string()` 每个字节走一次 `switch`，protobuf 行
`hranaCursorPageAppendText` 逐字节解码，JSON 行 `hranaJSONAppendPageText` 逐字节转义。
R45 对 H1 json 154.6ms 的归因中，gateway page validate 17.9ms、adapter
`pageScanner.scan` 12.9ms、行编码与写出 58.3ms，文本扫描/转义是可批量化的一部分。

**上限探针**（Apple M4，`-benchtime=3x -count=3`）：

- 已提交的转义密集语料（`hranaCursorPageBenchmarkCorpus`，24.2 MiB）：base
  validate/span 13.7–13.8ms、json-span 47.1–50.3ms、proto-span 28.4–28.8ms；
  批量路径 validate 12.6–12.8ms（−7%）、json 36.9–39.9ms（−21%）、proto 无变化。
- 与 fixture 同形的纯文本页（仅探针文件，未入库）：base validate 14.3–16.2ms、
  json-span 48.6ms、proto-span 26.6–29.0ms；批量路径 validate **2.6–4.4ms**、
  json-span **6.5–8.1ms**、proto-span **3.6–3.8ms**。

真实 H1/H3 载荷近乎纯文本（fixture 为 `issue28/%05d/` 串，无转义），每个校验遍省约
10–12ms、每个 JSON 转码遍省约 40ms，端到端上限明显高于 10%，方向成立后实现。

**改动**（`9ad2c43`，3 文件 +323/−5）：`scanner.string()` 先用 SWAR
（`hranaCursorPageHasZeroByte`/`hranaCursorPageHasLessThan`）跳过普通字节，只在
`"`、`\`、控制字节（<0x20）处进入原逐字节分支；`hranaCursorPageAppendText` 用
`bytes.IndexByte`、`hranaJSONAppendPageText` 用 SWAR 定位特殊字节并批量拷贝普通段。
新增 `hrana_cursor_page_bulk.go`（83 行）与 `hrana_cursor_page_bulk_test.go`（223 行）。
批量定位允许“提前候选”（如在控制字节造成的进位处），调用方逐字节复查，结果不变。
diff 只触及 cursor 页路径（`hrana_http_cursor.go`、`hrana_v3_json_cursor.go`、
`gateway_cursor.go`、`hrana_json_cursor_row.go`、`hrana_proto_cursor_row.go`），
pipeline execute 与写路径不经过。

**Gate 0**（`7ba35a7e…`，测前移除，栈 :19492）：H1 json/protobuf 各 1 样本，
`string_calls=34,779 string_bytes=80,778,969 string_mismatch=0`、
`text_calls=8,625 text_bytes=20,177,069 text_mismatch=0`、
`json_calls=8,625 json_bytes=20,194,319 json_mismatch=0`；52,029 行日志全部
`mismatch=0`，wire 字节 20,608,525 / 20,272,006 与基线一致。探针开启时延时不作数字。

**配对 A/B**（两侧独立平台二进制与全新同数据卷、同一 worker）：base `e3745b85…`
= `578c32d`，cand `f7f4f130…` = `9ad2c43`。

| 场景 | 协议 | 轮 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---:|---:|---:|
| H1.content-20mb | json-cursor | 1 | 126.24 / 122.04 | 75.58 / 73.72 | **124.14→74.65（−39.87%）** |
| H1.content-20mb | json-cursor | 2 | 124.29 / 123.26 | 75.73 / 75.94 | **123.77→75.84（−38.73%）** |
| H1.content-20mb | protobuf-cursor | 1 | 99.91 / 102.61 | 71.89 / 67.78 | **101.26→69.84（−31.03%）** |
| H1.content-20mb | protobuf-cursor | 2 | 102.76 / 106.65 | 69.06 / 68.57 | **104.70→68.81（−34.28%）** |

两协议两轮 4/4 同向，满足冻结判据（≤0.90 × 同轮基线）。四腿合并：json
123.77→75.66（−38.88%）、protobuf 102.68→68.81（−32.99%）。同路径 H3 json
−35.78%/−37.92%、H3 pb −34.19%/−30.27%。对照 R1 json −4.47%/−9.78%、R1 pb
−24.66%/−4.55%、F1 −3.39%/−4.65%（均亚毫秒或候选更快；pipeline 不经过本轮 diff）。

**对照与回归门**：B1 json −11.22%、B1 pb −0.35%、C1 −6.43%、H3 json −38.19%、
H3 pb −30.82%、L1 cursor −2.70%、L1 pipeline −0.64%。F1 json 5 样本单批
+14.02%（0.52→0.59ms），4 腿 × 15 样本复测 +12.98%、8 腿 × 15 样本复测
**+2.14%**（0.5284→0.5397），两批方向不一致，且 diff 不含 pipeline 路径，判噪声；
F1 pb 单批 +19.73% 在 15 样本复测为 +0.17%，不复现。X1 c16 两腿 4.60–29.01ms /
18.26–22.29ms、c8 1.11–3.29ms / 2.14–7.07ms，跨度远大于效应量，**不可确认**。
全部样本 `warnings=[]`、`fatal` 无、非 200=0，两侧平台日志无 error / 
`audit_unavailable` / `outcome_unknown` / `session_moved`；wire 字节与基线一致
（B1 json 4,960–4,982 与 R1 json 2,369,290/2,369,300 的差异仅 `query_duration_ms`
位数）。

**正确性**：新增随机化差分测试（固定种子）——三个 index helper 各 2,000 次随机
字节串；2,000 个随机合法 JSON 文本 token 对 `encoding/json` 解码与
`hranaJSONAppendString` 转义对拍；200 个随机整页对 JSON/protobuf 行编码与 legacy
路径逐字节对拍。既有语料测试全部通过。`gofmt -l .`、`go vet ./...`、
`go build ./cmd/peri-platform ./cmd/peri` 干净；定向游标测试 `ok 1.039s`；全量
`go test -count=1 .` **ok 179.642s**。

**证据**：`docs/performance-review/2026-11-04-r51-cursor-page-bulk/`（`criteria.txt`、
`results.md`、`gate0.txt`、`probes/`、`ab-raw/`、`reg-raw/`、`reg-recheck-f1/`、
`r51-ab.sh`、`r51-ab-summary.py`、`r51-reg.sh`、`r51-reg-summary.py`）。

**剩余风险与未测量项**：未测 W1 写路径容器 A/B（diff 不进入写路径，由全量测试覆盖）；
未做发布级 endpoint A/B；`0xe2` 字节（部分多字节字符前导）会提前结束批量段，只影响
此类字符密集的页，fixture 为 ASCII；SWAR 的提前候选只多一次复查，已在随机化测试中
覆盖控制字节与对齐边界。

**下一轮提示**：R51 之后 H1 json 约 75ms，剩余主要是 worker RPC（R45 归因约 46.5ms）
与响应写出；继续在网关侧扫描/转义里找零头已接近噪声。若要再拿双位数，需要 worker 侧
cursor 页生成的 span/计时，或评估 cursor 响应的 `Content-Length` 预分配（D2，需先探针）。

#### R52：只读 cursor 页单趟严格解码与 span 交接（2026-11-04，合入）

**背景**：R51 之后 H1 json 约 75ms。基线路径对每个只读 cursor 页要付出：整页 `json.Valid`
（20MB 页体）、结果 span 复制、gateway 页校验、fused 首页校验、adapter 二次扫描与
compact/escape 重写。

**上限探针**（`bf743deb…`，测前移除；基线路径阶段计时，H1 json 单查询 75.01ms）：
整页 `json.Valid` 8.04ms、结果复制 1.32、gateway 页校验 2.20、fused 首页 5.61、
adapter 二次扫描 2.40、compact+escape 0.80、`utf8.Valid` 0.32 ⇒ **可移除 20.70ms
（27.6%）**；结构定位 `sqlJSONFields` 8.70ms 不属可移除（新路径仍要一次结构扫描）。
门槛 7.57ms（10%），方向成立后实现。

**改动**（`8967ae8`，13 文件 +903/−40）：新增 `hranaCursorPageReply`（`allowMetadata` + 页
span）；`sqlDecode` 对页回复改为 `sqlJSONFieldSpans` 结构定位 → `sqlJSONEnvelopeSkeleton`
后的小体 `json.Valid` → 结果 span 严格页扫描 → 零拷贝交接（body 是每请求独立
`io.ReadAll` 缓冲，永不复用）；`gateway_sessions.go` 只读 cursor 走 `pageReply`，首页另用
`validCursorFirstPage`；`capture`/`hranaReply` 只在字节同一性成立时保留页（compact/escape
复制即丢弃，回退 adapter 自扫）；写 cursor 与 pipeline 路径不变。

**Gate 0**（`f9dff879…`，栈 19496）：H1 json 连测 3 次，每次 `pagedecode=34 calls/20.22MB`、
`pagescan=34`（每页恰好一次严格扫描）、`fallback=0`；`json.Valid` 只覆盖小回复
（895–922 次、约 0.12MB），`skeleton=34`；wire 字节 20,608,525 / 20,272,006 与基线一致。

**配对 A/B**（base `028abd15…` = `5327333`，cand `a54c9b91…` = `8967ae8`，同一 worker
`27d8dd46…`；两侧独立 run 目录与全新同数据卷，1 warmup + 5 samples/leg）：

| 场景 | 协议 | 轮 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---|---|---|
| H1.content-20mb | json-cursor | 1 / 2 | 75.04 / 75.01 | 62.17 / 61.69 | **−17.15% / −17.76%** |
| H1.content-20mb | protobuf-cursor | 1 / 2 | 71.65 / 69.68 | 57.29 / 58.90 | **−20.04% / −15.48%** |
| H3.content-projection | json-cursor | 1 / 2 | 78.24 / 74.77 | 63.93 / 60.99 | −18.29% / −18.43% |
| H3.content-projection | protobuf-cursor | 1 / 2 | 69.73 / 69.69 | 58.39 / 59.50 | −16.26% / −14.62% |
| R1.limit-1000 | json-pipeline | 1 / 2 | 15.11 / 14.41 | 15.46 / 17.53 | +2.29% / +21.71% |
| R1.limit-1000 | protobuf-pipeline | 1 / 2 | 16.12 / 17.34 | 18.72 / 16.97 | +16.12% / −2.12% |
| F1.select-1 | json-pipeline | 1 / 2 | 0.57 / 0.62 | 0.58 / 0.61 | +2.66% / −2.46% |
| F1.select-1 | protobuf-pipeline | 1 / 2 | 0.58 / 0.57 | 0.59 / 0.61 | +1.73% / +7.63% |

主指标两协议两轮 4/4 同向且均超 10%（硬门满足）。R1/F1 是 pipeline 路径，机制上不经过
游标页函数；R1 的单轮 +16.12% / +21.71% 进入回归门复测。

**回归门**（11 组 × 4 腿 × 5 samples）：H3 json −17.93%、H3 pb −16.53%、C1 +4.87%、
F1 json −5.55%、F1 pb +2.52%、R1 pb +1.24%、L1 cursor −2.66%、L1 pipeline −8.06%。
三处需要说明：

- **L1 verdict 为既有夹具偏差**：本 fixture 最忙 cwd 有 99 线程而场景 SQL 为 `LIMIT 40`，
  四腿均 `row_count_mismatch`（base/cand 一致），R51 回归门同样四腿 FAIL；只记录时间，
  不参与回归判定。
- **B1 json +24.11%**（2.29→2.85，绝对 +0.55ms，p95 重叠）：8 腿 × 15 samples 复测
  2.205→2.160（**−2.04%**），方向翻转，判噪声。
- **R1 json +14.74%**：8 腿 × 15 samples 在长历史栈上复测 +13.32%（合并 60 样本/侧
  均值 +6.45%）；改用**全新卷换位**后为 +6.58%（n=2/侧）与 +4.11%（n=4/侧）；再做
  **同二进制对照**（两台全新卷都跑基线二进制）得腿中位 **−8.11%**、合并均值 −3.23%
  ——即该场景的栈间方差下限。候选差异落在该范围内，且 diff 对 pipeline 的全部影响
  只是一次 `output.(*hranaCursorPageReply)` 断言与一次 `page != nil` 检查（不进入新分支、
  不产生分配），**判「不可确认（栈/窗口效应），不构成回归」**。

**正确性**：新增 `hrana_cursor_page_decode_test.go`（603 行）：6,000 条合法页差分、
8,000 条信封变异、5,000 条任意字节、嵌套深度 1–129；`validCursorFirstPage` 接受超
float64 数字（worker 的 int64/有限 f64 不会产生，属防御）。`gofmt`/`go vet`/
`go build ./cmd/peri-platform ./cmd/peri` 干净；全量 `go test -count=1 .` **ok 175.1s**
（连续两次）；定向 `-race` 14.0s 通过；无 `platform.test` 残留。

**证据**：`docs/performance-review/2026-11-04-r52-cursor-page-single-pass/`（`criteria.txt`、
`results.md`、`gate0.txt`、`probes/`、`ab-raw/`、`reg-raw/`、`reg-recheck-raw/`、
`swap-raw/`、`swap2-raw/`、`same-raw/`、`r52-*.sh`、`r52-*-summary.py`）。

**剩余风险与未测量项**：页体零拷贝依赖「每请求独立缓冲」这一上游不变量（若未来引入缓冲池
必须重审）；未测 X1/c128/容量；未因本轮重跑崩溃恢复 E2E（不改变耐久时点与提交路径）。

**下一轮提示**：R52 之后 H1 json 约 62ms，剩余主要是 worker RPC（R45 归因约 46.5ms）与
响应写出；网关侧解码/校验的零头已接近噪声。方向 B（worker 直接产出 cursor 页协议）需
Rust+Go 双侧协议与两个 worker 二进制，是下一轮唯一可能再拿双位数的方向；方向 C（去掉
`serde_json::Value` 树）预期低于门槛，需先探针。worker 预取下一页会改变审计/日志语义，
需用户授权。

#### R53：worker 只读 cursor 页单趟序列化的上限探针（2026-11-04，未实施）

**假设（上一轮反思）**：worker 每页对 20MB 量级文本做三趟拷贝——`row.get_values().cloned()`
（`Value::Text` 深拷贝）、`json!` 构建 `serde_json::Value` 树、`serde_json::to_vec` 逐字节转义；
改成借用 `Serialize` + `memchr` 批量转义可省 11–28ms。**探针否证了这个量级。**

**方法**：`main` `1a2d2e0` 上构建探针 worker（`e2be9569…`），分五段计时（native、克隆、树、
`to_vec`、`reply`）；平台用同源 `peri-platform-r53-base`（`deaea101…`）。全新卷 + 同一 seed，
H1 json-cursor 1 warmup + 2–3 samples。

**事实**：H1 是 **8,625 行 × ≈2.34KB、34 页**（每页 ≈600KB），不是单行 20MB。稳态每页：
native 700–870µs、克隆 24–35µs、树 36–71µs、序列化 129–206µs（≈3.75GB/s）、写出 29–73µs；
每查询合计 native ≈25.5ms、克隆 ≈1.0ms、树 ≈1.9ms、序列化 ≈5.4ms、写出 ≈1.7ms
（端到端 64ms 中约 35ms 在 worker handler 内）。

**上限**：借用序列化最多消掉树（1.9ms）与序列化的转义部分（乐观 50–60%，≈2.7–3.2ms），
合计 **4.6–5.1ms ≈ 7.2–8.0%**；即使删掉克隆+树+序列化全部也只有 8.3ms（13%），而 20.6MB
输出必须产生，序列化不可删除。门槛 6.4ms ⇒ **不实施**，无生产代码改动。

**方法教训**：探针每页一次 `eprintln!` 把 H1 从 64.6ms 拉到 141.6ms（2.2×）；同计时的
无输出构建为 64.56ms（干净 worker 63.80ms）。分段计时在打印前取得且与 `total_us` 自洽，
结论不受影响；但**热路径探针不得逐请求写 stderr**，应累计到原子计数、每查询输出一次。

**证据**：`docs/performance-review/2026-11-04-r53-worker-cursor-serialize/probes/`
（`upper-bound.md`、`upper-bound-probe.diff`）。

**下一轮提示**：平台侧 R52 后仍有两趟 20MB 走查（严格页校验 ≈7.9ms、Hrana 转码 3–6ms）；
「边校验边编码、整页通过后才放行」的融合 + 文本 span 的 `memchr` 定位可能省 5–9ms（8–14%），
须先做 Go-only 上限探针，不改协议。

#### R54：平台只读 cursor 页路径的上限探针（2026-11-04，未实施）

**假设（R53 的下一轮提示）**：R52 之后平台侧仍有两趟 20MB 走查（严格页校验 ≈7.9ms、Hrana 转码
3–6ms），融合或 `memchr` 化可省 5–9ms。**直接测量否证了 7.9ms 这个推算。**

**方法**：平台探针（`peri-platform-r54-probe`，sha256 `8d7c0a7c…`，基于 `1a2d2e0`，仅计时）在
`ReadAll`、`sqlJSONFieldSpans`、信封 skeleton `json.Valid`、`sqlDecodeCursorPage`、页转码
（`appendPageRow`/`rowBuffer`）与分页 flush 上计时；worker 为干净构建 `27d8dd46…`（与 `r43-cand`
逐字节相同）；新卷 + 标准 fixture，H1 json-cursor 1 warmup + 2 samples。探针每查询输出一行累计值，
逐行相减得到单次查询：readall 8.53–8.83、spans 0.70–1.45、skeleton 0.016–0.026、**scan 2.42**、
transcode 4.18–4.21、flush 1.54–1.57ms，合计 17.3–18.4ms（同栈 H1 64.73ms 的 27–28%）。

**判定**：融合扫描+转码最多省扫描这一趟 **2.42ms**，`ReadAll` 预分配估算 ≤2ms，spans 0.7ms，
转码/flush 已接近内存带宽；合计上限 **≤5ms ≈7.7%** < 10%（6.5ms）⇒ **不实施**。注意 readall 只含
body 传输，worker 计算与响应头等待在 `client.Do`，因此 8.5ms 不能当作可省空间。平台页路径已无
≥10% 的单一杠杆。

**证据**：`docs/performance-review/2026-11-04-r54-platform-page-path/probes/`
（`upper-bound.md`、`upper-bound-probe.diff`、`platform-probe.log`、`harness/`）。

#### R55：worker 只读 cursor 行循环的上限探针（2026-11-04，未实施）

**假设**：R53 测得 worker 每页 ~1.0ms（34 页 ≈35ms），其中 `native` 含行循环约 0.8ms/页；把
fencing/预算/克隆拆开，看是否有 ≥10% 的可移除成本。

**方法**：worker 探针（`peri-loom-v1-r55-probe`，sha256 `f447547c…`）在 `run_steps`
（仅 `collect && page_size.is_some()`）记录 `pre`（guard/deadline 检查）、`gate`（`operation_until`
+ `enter_step`）、`step`（`step_with_admission_check` + `step_clock.finish`）、`post`（行取值、
预算计账、`Value` 克隆）；原子计数只在 `done=true` 打印一行并清零。H1 json-cursor 1 warmup +
2 samples，34 页 / 8,625 行：pre 0.40、gate 1.63–1.64、**step 14.70–15.87**、post 0.92–1.02ms。

**判定**：除 turso `step` 外全部空间 ≈3.0–3.1ms（4.7%）；`gate` 是每行 fencing/隔离校验与 step 额度，
`pre` 是 deadline/预算检查，均不能去除；`post` 里唯一可省的是克隆（≈1ms，属 R53 已否证方向）。
**不实施**。剩余主体是 turso VM（14.7–15.9ms，23–25%），动它属存储引擎/SQL 兼容面，不在性能轮范围。

**证据**：`docs/performance-review/2026-11-04-r55-worker-row-loop/probes/`
（`upper-bound.md`、`upper-bound-probe.diff`、`worker-probe.log`、`harness/`）。

**下一轮提示**：worker 与平台两侧的页处理都已到带宽/VM 下限。继续要 ≥10% 只剩「只读 cursor 页
一页流水线」：平台在转码/发送第 N 页时已把第 N+1 页的 fetch 发出（worker 会话在同一时刻仍只有
一个在途 RPC，不需改协议），重叠 worker 执行与平台本地处理，估算省 0.25ms/页 × 33 ≈ 8ms（13%）。
该方向属预取类，需用户显式授权后才能开轮。

#### R56：只读 cursor 页预取与 worker 响应按声明长度预分配（2026-11-04，未达单独门槛，随 R57 同批合入）

**背景**：R55 的下一轮提示指出：平台在转码/发送第 N 页时可以同时发出第 N+1 页的 fetch，
把 worker 执行与平台本地处理重叠。该方向属预取类，记录里标注「需用户显式授权后才能开轮」。

**授权缺口（必须留档）**：协调者用 AskUserQuestion 请求授权时用户未作答（工具被拒绝），
随后按用户固定的「实现完代码再说」继续实现。因此 R56 **没有**用户对预取语义的显式授权原文，
只有「按既定流程继续」的指令。此缺口不得在后续轮次被当作先例：涉及审计/耐久/协议语义的
改动仍需引用用户原文。

**改动**（`d5cf31f`，13 文件 +882/−3，代码集中在 `prototypes/v1/platform/` 的 6 个文件）：
新增 `hrana_cursor_prefetch.go`——`cursorFetchAhead` 在 `context.WithoutCancel` + 30s 超时下
发出下一页 fetch（`max_rows=256`、`write_cursor=false`），回复进缓冲 `chan`；
`hrana_v3_json_cursor.go` 的 `servePagedJSONCursor` 与 `hrana_http_cursor.go` 的
`serveNativeHTTPReadCursor` 在非首页分支消费在途回复，首页扫描成功后、写出当前页之前启动下一页，
外层 defer 在清理关闭前 join 在途预取；写 cursor 分支从不预取。
`sql_upstream.go` 的 `readWorkerReply` 在 `0 ≤ Content-Length ≤ sqlWireLimit+1` 时按声明长度
一次分配 + `io.ReadFull`，否则有界增长回退。
测试：`hrana_cursor_prefetch_test.go`（写 cursor 保持串行、中止流 join 预取且 `handler.streams` 不泄漏）、
`TestReadWorkerReplyPrefersTheDeclaredLength`；反向对照（只回退两个 cursor 文件）使读 cursor 用例
失败于「写当前页时下一页 fetch 不在途」。

**Gate 0**（`069d579`，栈 19614，探针测前移除）：H1 json/native 每查询 `issued=33 consumed=33`
（34 页 − 1）；`L2.list-empty` 单页 `0/0`（关掉对照）。探针曾把消费点 `Add(1)` 写两遍
（`consumed=66`），修正后重测；采用二进制 `peri-platform-r56-gate0c`（sha `6a49d992…`）。

**单独 A/B**（base `9e003707…` = `fbc0e3d` 干净 worktree，cand `611fa2d8…` = `20d8db5` 干净树
两次构建一致；worker 两侧 `27d8dd46…`；两侧独立栈 + 各自全新同数据卷，每轮 A/B/B/A、5 samples/leg）：

| 场景 | 协议 | 轮 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---:|---:|---:|
| H1.content-20mb | json-cursor | 1 / 2 | 63.341 / 66.885 | 63.071 / 55.136 | **−9.23% / −8.99%** |
| H1.content-20mb | protobuf-cursor | 1 / 2 | 57.208 / 61.277 | 52.995 / 54.115 | **−9.60% / −8.87%** |

4/4 同向、无退化，但四格都差冻结的 10% 硬门 0.4–1.0 个百分点 ⇒ **单独不合入**。
按 R56 判据里的批次声明与 R57 组成同一批次，由批级 A/B 决定（见 R57）。

**正确性**：Go 全量 `go test -count=1 .`（`PERI_RUST_BINARY` 指向 R57 release worker）
**ok 176.059s 全绿**；`gofmt`/`go vet`/`go build ./cmd/peri-platform ./cmd/peri` 干净；
16 条腿 wire 字节全部等于 20,608,525 / 20,272,006。

**夹具修正（A/B 前）**：重建脚本原先把 100 个线程放进 `/workspace/issue28`，而 L1 场景 `LIMIT 40`
与契约 `expected_rows=list_threads` 不符，每条 L1 都 `row_count_mismatch`。改为 39 个普通线程 +
focus 线程（共 40）落在 list cwd，其余 161 个在另一个 cwd；线程/消息/字节总数不变。
脚本随记录提交（`fixture/seed_fixture.py`，sha256 `092c2ad7…`）。

**记录更正**：R57 判据的「上限依据」引用 R56 为一组「4 轮」数字，来自一次输出目录已被覆盖、
无原始数据的早期窗口；可复算值为上表的 9.23%/9.60%/8.99%/8.87%。冻结判据未因此改变。

**证据**：`docs/performance-review/2026-11-04-r56-cursor-page-pipeline/`（`criteria.txt`、
`gate0.txt`、`results.md`、`probes/gate0-probe.diff`、`fixture/seed_fixture.py`、`r56-ab.sh`、
`r56-ab-summary.py`）。

**剩余风险**：预取每次多读一页（256 行）在内存中短暂并存两份页字节，未做长稳压测；
写 cursor 明确不预取（由平台写 cursor 测试覆盖）；收益只在 20MB 大结果上验证。

#### R57：worker 只读 cursor 页借用序列化（2026-11-04，与 R56 同批合入）

**背景**：R53 探针测得每页 `json!` Value 树 36–71µs、`to_vec` 129–206µs，34 页/查询
⇒ 可移除 ≈1.2–2.4ms（2–4%，低于单独门槛）。R55 之后平台侧已到带宽下限，剩下能动的就是
「不要先造树」。

**改动**（`1daea98`，10 文件 +561/−35，代码集中在 `prototypes/v1/src/worker_rpc/` 的 5 个文件）：
新增 `WorkerResult::{Json, ReadCursorPage}`；`ReadCursorPage { cols, rows, done, autocommit }`
用手写 `Serialize` 把引擎行直接写进编码器（Null→null、Integer→i64、有限 Float→f64、
非有限→null、Text 走同一 serde_json 转义、Blob→u8 序列），键序与 `json!` 排序一致以保证逐字节等价；
`dispatch_read_cursor_first`/`FetchReadCursor` 返回借用页，写 cursor/execute/batch 仍返回 Value 树；
`server.rs`/`sessions.rs` 的行数与 `done` 判定改为按 `WorkerResult`。

**Gate 0**（`22b1373`，栈 19634，探针 `probes/gate0-probe.diff` 测前移除）：计分点在
`impl Serialize for ReadCursorPage`，按 `cols.is_some()` 区分 first/fetch——
H1 json 每查询 **1 first + 33 fetch**（34 页全命中）；protobuf 每查询 **34 fetch、0 first**
（native cursor 先 `cursor/open` 取列，再逐页 fetch，无融合首页，属既有协议差异，不是本轮引入）；
`L2.list-empty` 单页 **fetch=0**；写 cursor（`UPDATE … RETURNING id`）返回 200、`step_begin` 带
`cols`、`step_end` 0 行，**探针 0 命中**（仍走 Value 树）。采样式探针（每 25 页一行）读数不可
分解，已作废；测量二进制由干净树重建（`a648ecd9…`，两次一致）。

**批次 A/B（R56 + R57 vs main）**（base：平台 `9e003707…` + worker `27d8dd46…`；
cand：平台 `611fa2d8…` + worker `a648ecd9…`；两侧全新栈/卷/同批 fixture，2 轮 × A/B/B/A、5 samples/leg）：

| 场景 | 协议 | 轮 | base 两腿 ms | cand 两腿 ms | 中位变化 |
|---|---|---|---:|---:|---:|
| H1.content-20mb | json-cursor | 1 / 2 | 61.255 / 60.995 | 53.988 / 55.634 | **−10.33% / −17.55%** |
| H1.content-20mb | protobuf-cursor | 1 / 2 | 58.626 / 61.387 | 52.733 / 52.250 | **−12.52% / −16.14%** |

4/4 过冻结的 10% 硬门（保守读法用第 1 轮：10.33% / 12.52%）。第 2 轮基线整体偏慢
（json 65.251/63.950ms），候选侧两轮稳定（json 53.0–55.6ms、pb 50.8–52.7ms），
因此收益区间按 **10.3%–17.6%** 记录，不取上界作代表值。

**回归门**（12 组 × 4 腿 × 5 samples）：**无一格 >10% 退化**。最大退化 R1 protobuf **+6.6%**
（18.72→19.95ms）；R1 json −3.6%、F1 json −5.5%、F1 pb −5.0%、L1 cursor −9.9%、
L1 pipeline +4.0%、C1 +1.0%、X1 c=1/c=4/c=8/c=16 = +2.2%/−12.1%/−52.7%/−2.1%
（快场景，只按无退化处理）；同路径 H3 json **−16.6%**、pb **−11.1%**。
B1 json/pb 测得 −42%/−50%，属 2.4–6.6ms 的小绝对值快场景，单腿抖动可达数十个百分点，
**不认领为本轮收益**。字节不变量：R1 2,369,290/2,349,053、H3 20,746,565/20,306,526、
F1 312/40、C1 300 全部一致；B1 json 4,947–4,987（`query_duration_ms` 位数）、pb 686；
全部 `verdict=PASS`、无 429、`warnings=[]`。

**正确性**：pinned 单元测试 `borrowed_page_encodes_exactly_like_the_value_tree` 与
`borrowed_first_page_encodes_exactly_like_the_value_tree` 覆盖转义/空串/unicode/blob/±inf/null，
与 Value 树逐字节相等；`cargo check`/`fmt`/`clippy -D warnings` 干净；全量
`cargo test --lib`（`ulimit -n 10240`）**679 passed / 1 failed / 10 ignored**，
失败项 `authority_rpc::server::batch_tests::authenticated_batches_are_registered_only_by_live_writers`
单跑通过，且**在 `main` 上同样失败**（main：675 passed / 2 failed，另一项是既有的 object_store
锁用例）⇒ 既有 flaky，不是本轮引入。

**机制核对**：R56 diff 只落在 `prototypes/v1/platform/` 的 cursor 读取路径，R57 只落在
`prototypes/v1/src/worker_rpc/`；写 cursor、pipeline、baton 复用路径未被修改，回归门也未出现
>10% 退化。

**剩余风险**：① 收益只在 20MB 单行大结果验证，5KB 级 cursor（L1）虽测得 −9.9% 但绝对值 1ms，
不作结论；② 两侧二进制为宿主本地构建（macOS arm64），发布镜像的工具链一致性需在发版 A/B 中复测；
A/B 候选 `611fa2d8…` 内嵌 `vcs.modified=true`，事后在同一路径复现确认：干净树构建为
`c95d9efc…`，仅凭一个未跟踪的非代码文件即逐字节复现 `611fa2d8…` ⇒ 二进制就是 20d8db5 的
提交代码（Go 会嵌入绝对源码路径与脏标记，跨目录/脏树的 sha 差异不能推出源码差异）；
③ R57 改变了 worker 内部回复的构造方式（对平台是字节等价的 JSON），由平台严格扫描与
16 条腿的字节核对守门。

**证据**：`docs/performance-review/2026-11-04-r57-worker-cursor-borrow/`（`criteria.txt`、
`gate0.txt`、`results.md`、`probes/gate0-probe.diff`、`batch-ab.sh`、`batch-ab-summary.py`、
`batch-reg.sh`、`batch-reg-summary.py`）。

**下一轮提示**：H1 20MB cursor 已从 R52 后的约 62ms（json）/57ms（pb）降到约 53ms / 51ms；
剩余主要是 worker 每页执行（turso VM，R55 实测 14.7–15.9ms/查询）与传输。再要 ≥10% 只能动
worker 的页执行或减少协议帧数；两条都需用户显式授权。

#### R58：H1 只读 cursor 页成本归因与行级检查上限（2026-11-04，测量轮，不实施）

**动机**：R56/R57 之后 H1 已到约 53ms（json）/51ms（pb），下一步必须先回答「剩余时间花在哪里」，
否则只能在已用尽的方向上迭代。两轮探针都不进生产代码：`probe/r58-cursor-page-timing`（归因）
与 `probe/r58b-row-checks`（上限），均不合入 `main`。

**归因探针**（平台 + worker 两侧同时打点，原生 macOS 单机栈，默认配额，H1 20MB）：

- 平台侧（`R58CURSOR`，每次查询 34 页，json 稳定样本）：`wait_ms≈38.6`（prefetch=33、
  direct=0，R56 预取后每页暴露等待约 0.17ms）、`process_ms≈6.0`（页转码）、`first_ms≈1.6`；
  upstream `do≈32.5ms / read≈6.3ms / decode≈5.0ms`（35 次调用）；`dispatch_serve≈45.6ms`、
  `marshal≈0.21ms`、`reply≈0.001ms` ⇒ 平台自身约 **6.9ms/查询**。
- worker 侧（`R58WORKER`，每次查询 33 页）：`total≈30.8–33.6ms`，其中
  `turso_step≈16.5–18.3ms`、`post≈6.4–7.1ms`（页序列化）、`row_work≈0.52–0.60ms`、
  `account≈0.31–0.33ms`、`lane_*` ≈ 0；行级检查（`R58WORKER2`，17,303 次/样本）
  `guard≈0.41–0.56 + operation_until≈0.85–1.12 + clock_enter≈1.23–1.67 +
  thread_cpu≈1.75–2.48ms`，合计 **≈4.3–5.9ms**，但含探针自身的 `Instant::now` 开销。
  `lane_settle≈0.002ms`（`R58WORKER3`），准入 settle 不是成本。
- 结论（假设级）：worker 生产是限速器（平台预取深度 1 足以把页转码藏到下一页生产后面），
  其中 turso VM ≈17ms、页序列化 ≈6.6ms、行级检查 ≈4–6ms；平台自身 ≈6.9ms；
  其余 ≈7ms 是客户端读取 20MB。

**上限探针**（`probe/r58b-row-checks` @ `14aef4e`，只改 `execution.rs::run_steps`）：
把每行的 `guard.check()`/`deadline.check()`、`operation_until` 读守卫、`enter_step`/`finish`
时钟与循环末尾 `account_steps` 全部提到每页一次（不安全版本），测「移除行级开销」的真实天花板。
`account_row` 与 VM progress handler 保持不变。两侧平台二进制相同
（`f4210c08…`，`main` @ `0a51ab9` 干净树），基线 worker `a648ecd9…`（与 R57 合入二进制逐字节一致），
探针 worker `745888eb…`；两侧独立栈、各自全新 run dir 与同批 fixture；腿序 A1→B1→B2→A2，
每腿 1 warmup + 5 samples，无观测器。

| 协议 | A1 | B1 | B2 | A2 | 基线均值 | 探针均值 | 上限 |
|---|---:|---:|---:|---:|---:|---:|---:|
| json-cursor | 52.65 | 48.74 | 47.65 | 51.35 | 52.00 | 48.20 | **7.3%** |
| protobuf-cursor | 51.72 | 48.52 | 47.54 | 52.16 | 51.94 | 48.03 | **7.5%** |

配对差值 json 7.43%/7.21%、protobuf 6.19%/8.86%，两协议两条腿同向；响应字节
（json 20,608,525 / pb 20,272,006）、行数 8,625、`warnings=[]`、全部 200/ok 不变。

**判决**：**上限 ≈7.3–7.5% < 10% 硬门，不实现**。即使把行级检查全部删掉（已是不安全版本），
端到端也只省约 3.7–3.9ms；安全的「降频/批量化」版本只会更少，不值得引入预算语义变更。
剩余 H1 大头是 turso VM stepping（引擎）与页序列化/协议帧（worker-平台协议），
按既定授权边界需用户显式批准后才能立项。

**证据**：`docs/performance-review/2026-11-04-r58-cursor-page-attribution/`
（`README.md`、`r58b-upper-bound.md`、平台/worker 原始探针日志与 8 份 harness `summary.json`）。
探针二进制仅用于归因，A/B 期间不开启任何观测器；探针构建的端到端绝对值高于干净构建，
不作为验收或对外数字。

**下一轮提示**：H1 的两条剩余路径都在授权边界外。若继续在授权内迭代，应先对其它场景做
smoke 剖面（F1/L1/B1/X1/W1）找新的限速器，而不是继续在 20MB cursor 上做 <10% 的局部优化。

#### R59：延迟耐久账户 worker 8→64 的上限探针（2026-11-04，否证，不合入）

**动机**：R58 的下一轮提示要求先找新限速器。X1.concurrency 在 c16 的 p95 长期停在
144–190ms，而中位只有 2–4ms——尾部是突发停顿。先用观测器把停顿归因，再用最小探针验证
「延迟耐久账户（`deferredDurabilityWorkers=8`、队列 256）饱和后内联回退」是否就是尾部成因。

**归因**（独立栈 `127.0.0.1:19671`，基线二进制 `f4210c08…`，`PERI_DURABILITY_OBSERVER=1` +
`--sql-open-observer`，相位阈值 20ms、采样 0；同一 fixture、同一 X1 json-pipeline）：

- 审计写者是单线程串行段：`audit_batch` 506 批中 **466 批 `count=1`**（92.1%），34 批
  `count=32`；`total_ns` p50 3.90 / p99 8.06 / max 42.19ms，`sync_ns` p50 3.84 / p99 7.23ms，
  写者忙 2.114s。
- `audit_checkpoint` 55 次，p50 **21.01** / p90 31.19 / p99 37.08 / max 38.12ms，合计
  **1.204s**（≈ 审计写者忙时的 57%）。
- `audit_member_wait` 1,661 次：总等待 p50 56.9 / p99 119.5 / max 126.3ms，其中排队
  p50 46.7 / p99 115.5ms；`journal_batch` 965 批中 924 批 `count=1`、`sync_ns` p50 3.90ms，
  写者忙 4.121s。
- 请求相位（只记录 >20ms）：慢 open/close 148ms = `audit` 126.0–126.3 + `terminal` 20.6–21.7，
  `native` 仅 0.5–0.8ms；慢查询 73.6ms = `native` 50.2 + `intent` 16.2；快查询 p50 28.2ms
  中 `native` 15.4ms（纯 worker 成本）。慢请求成**突发**出现，不是长尾分布。

机制假设：突发时延迟耐久账户饱和 → `settle` 内联等待 → 请求侧看到 126ms `audit` 相位。
若账户宽度是限速器，把 worker 从 8 提到 64 应显著改善 c16。

**探针**（`probe/r59-deferred-workers` @ `fbf7b98`，二进制 `56aa4aba…`）：只改
`deferredDurabilityWorkers = 8 → 64`（`durability_deferred.go`，两行探针注释），
`deferredDurabilityMaximum` 保持 256；不改耐久时点、审计链语义、配额。基线二进制
`f4210c08…`（`main` @ `0a51ab9` 干净树；`main` 其后只有文档提交 `325207c`），worker 两侧同为
`a648ecd9…`。两侧独立栈、各自全新 run dir 与同批 fixture；腿序 A1→B1→B2→A2，
每腿 1 warmup + 3 samples/档、每档 100 请求，无观测器。

| 腿 | 二进制 | c1 中位/p95 | c4 | c8 | c16 |
|---|---|---:|---:|---:|---:|
| A1 | 基线 | 0.53 / 7.56 | 0.70 / 97.59 | 2.08 / 156.02 | 3.75 / 186.96 |
| B1 | 探针 | 0.51 / 7.58 | 0.86 / 34.71 | 8.96 / 142.09 | **165.62** / 250.42 |
| B2 | 探针 | 0.50 / 7.87 | 1.65 / 29.56 | 3.04 / 146.54 | **124.39** / 231.37 |
| A2 | 基线 | 0.46 / 7.37 | 0.64 / 97.77 | 1.98 / 144.31 | 16.56 / 180.02 |

- c16 中位反向崩坏：基线 3.75 / 16.56ms → 探针 165.62 / 124.39ms（相对较快基线腿回归
  33–44×，相对较慢基线腿 7.5–10×），两条 B 腿同向变差，没有任何档位出现 ≥10% 改善。
- c4 的 p95 确有改善（97.6–97.8 → 29.6–34.7ms），但同档 c16 崩塌，不能算收益；
  c1 不变；c8 探针 8.96 / 3.04ms 对基线 1.98–2.08ms。
- 全部腿 `errors=0`、`status_429=0`、verdict 全 PASS，fixture `complete=true`，响应字节 312。

**判决：方向否证，不合入**。账户宽度不是限速器；限速器是**单线程审计写者的屏障与 checkpoint
停顿**。64 个 worker 让更多记录同时涌向同一写者，屏障被拆得更碎。两栈累计计数器因工作量不等
（基线栈另承接一次未计分的预备运行）不能直接相比；按 session-open 归一后
`operation_journal_syncs_total / open` 从 0.93 升到 1.57，方向与「屏障碎片化」一致，仅作方向性证据。
这与 R29/R31 同构：屏障合并依赖串行化，增加并发只会让批更碎。

**下一轮提示**：要继续压 X1 尾部只剩两条路——降低 `audit_checkpoint` 的单次成本/频率，
或合并每条记录的独立屏障；两者分别触及审计链语义与耐久提交时点，需用户显式授权，本轮不实施。
授权前优先转向授权内场景（H1 平台页路径、W1 写路径归因）。

**证据**：`docs/performance-review/2026-11-04-r59-deferred-workers/`
（`README.md`、4 腿 `summary.json`、`durability-aggregate.json`、
`durability-observation.jsonl.gz`、`request-phase-aggregate.json`、`request-phase-observation.jsonl`、
`probe.txt`、`r59run.py`/`r59sum.py`/`metrics.py`）。

#### R60：W1 写路径的耐久等待归因（2026-11-04，测量轮，无性能改动）

**动机**：R59 的下一轮提示——授权前先做 W1 写路径归因。R59 已把 X1 c16 尾部归到单线程审计
写者的屏障与 checkpoint；W1（写场景，中位 32–41ms）是否同一机制？

**方法**：main 二进制（platform sha256 `f4210c08…`、worker `a648ecd9…`；仓库 `main` @ `3671c1c`，
二进制构建后只有 docs 提交），`dev.py` 独立栈（127.0.0.1:19681、全新 run dir、无 TLS、默认配额），
`PERI_DURABILITY_OBSERVER=1` + `--sql-open-observer`（sample 1000‰、threshold 0），harness
`--scenario W1 --rounds 1 --warmup 1 --samples 3`，窗口 1.89s、16 个测量请求全部 200/ok。
观察器会放大请求计数并压低吞吐，绝对数值只用于归因。

**请求相位**（窗口内 48 open / 48 statement / 48 close = 16 个测量请求 × 3 动作
+ 32 条 harness 自身 setup/cleanup 语句）：

| action | n | total p50 / p95 / max | 主要相位（p50） |
|---|---:|---|---|
| statement | 48 | 37.85 / 46.68 / 48.12ms | native 17.59、**intent 12.63**、audit 3.81、terminal 3.79 |
| open | 48 | 1.08 / 2.23 / 2.71ms | native 0.93（延迟记录：回复前不等屏障） |
| close | 48 | 0.45 / 1.08 / 1.17ms | native 0.29 |

**关键证据**：

1. **intent 与 SQL 无关**：4 条语句 native 只有 1.3–1.6ms，intent 仍 10.9–12.8ms（p50 11.27、
   total p50 21.26）；native >10ms 的 42 条为 native 17.95、intent 12.65、audit 3.79、terminal 3.81。
2. **客户端严格串行**：同连接 open 结束到 statement 开始只有 5–39µs（p50 13µs，n=48），两个请求
   没有重叠；R37 的延迟耐久账户使 open 的 completion/audit 在回复后才持久化，statement 到达时
   这些记录仍在途。
3. **不是 checkpoint CPU**：44/48 的 intent 同时与 `audit_batch`+`journal_batch` 重叠，4 条与
   `audit_checkpoint` 重叠；三类 intent 均值 12.78 / 12.48 / 12.38ms——与 checkpoint 重叠的没有更慢。
4. **窗口内两条屏障链几乎占满**：`audit_batch` 143 批（142 批 count=1，sync p50 3.87ms，合计
   669ms）、`journal_batch` 145 批（98 批 count=1，sync p50 6.64ms，合计 867ms），合计
   1.54s / 1.89s 窗口。全run：`audit_batch` 601（600 count=1）、`journal_batch` 1027（980 count=1）、
   `journal_member_wait` p50 2.70ms、`audit_member_wait` p50 3.92ms、`journal_prepare` p50 35µs。
5. **checkpoint = 4 次 fsync**：全run 18 次、p50 **21.00ms**（18.98–24.16）、合计 386ms；窗口内
   4 次合计 87ms（4.6%）。`checkpointAfterBatch` 在 `sequence - head.revision >= auditBatchMaximum(32)`
   时 `commitHead` → `commitAuditBatch` → `persist`；`persist` 写 `head.path` 与 `head.path+".floor"`
   两个文件，各自 `atomicPrivate`（tmp 写 + fsync + rename + 目录 fsync）。

**机制**：`statement` 的 `intent` 覆盖 `beginDispatchOperation` → `registerOperation` →
`beginCorrelatedOperation`：写「可能提交」意图并等它 durable。journal 单写者、按命名空间有序，
队列里还有上一条 open 的延迟记录（audit 与 journal 两条链都在跑），intent 的屏障落在它们之后；
12.63ms ≈ 3 个屏障单位（窗口 `journal_batch` sync p50 6.64 + audit 3.87），与排队一致。

**判决：仅记录，不合入**（本轮无性能改动）。W1 成本 = worker 写执行（native 17.6ms，SQL-only
16–18ms，属存储/引擎）+ 耐久屏障（≈20ms）。两条 ≥10% 方向都在授权边界：① 让提交意图不再排在
上一条请求的延迟会话记录之后（journal 顺序/耐久提交时点）；② 降低 `audit_checkpoint` 单次成本或
频率（4 次 fsync/次、每 32 条审计记录一次，属审计链与恢复语义）。未授权时不改。

**工具修正**（与性能无关，单独提交）：harness 解析 protobuf `Error` 时把 field 1 当 code、field 2
当 message；Hrana 3 规范是 `message = 1`、`code = 2`（平台 `hranaProtoError` 一致）。修正
`hrana.py`（pipeline 与 cursor 两处）与 `test_issue28_support.py` 后，R1-5000 protobuf 由误判
`error` 恢复为 `expected_limit`；离线 32 tests OK。修正后的 R1 smoke（1 轮 × 3 samples，仅 smoke、
不可认证）：limit-100 1.92/1.64ms、limit-500 12.69/11.53ms、limit-1000 18.35/19.28ms，
limit-2000/5000/8625 均 `expected_limit`（4MiB 预算）。

**下一轮提示**：W1 与 X1 的剩余成本都落在同一条单线程耐久写者上，授权内只剩 <10% 的零散项。
若继续，先做两个**不安全上限探针**（不改 main）：跳过 intent 等待以量化 W1 上限、跳过 checkpoint
以量化 X1 尾部上限，用数字换授权决定。

**证据**：`docs/performance-review/2026-11-04-r60-write-path-attribution/`
（`README.md`、`summary.json`/`md`、`raw.jsonl`、`durability-observation.jsonl.gz`、
`request-phase-observation.jsonl.gz`、`aggregate.py`、`smoke-r1-prefix/`、`smoke-r1-postfix/`）。

#### R61：两项耐久上限探针——W1 intent 等待与 X1 周期 checkpoint（2026-11-04，测量轮，不合入）

**目的**：R60 把两条 ≥10% 的方向都指向授权边界（意图/延迟记录的屏障顺序、checkpoint 频率与成本）。
本轮按 R60 的建议先做两个**不安全上限探针**（只在 probe 分支，绝不合并），用数字决定是否值得申请授权。

**探针 A（W1 intent）**：`beginCorrelatedOperation` 提交意图记录后不再 `wait()`，调用方立即
dispatch；记录仍入队、仍落盘，只去掉「等它 durable 再派发」。A/B/B/A，4 条独立栈 + 全新同数据卷，
2 轮 × 1 warmup + 3 timed/腿：

| 腿 | 二进制 | 6 样本中位 |
|---|---|---:|
| A1 | 基线 `f4210c08…` | 40.56ms |
| B1 | 探针 `6010408d…` | 32.10ms |
| B2 | 探针 | 31.47ms |
| A2 | 基线 `f4210c08…` | 40.94ms |

配对 A1→B1 **−20.9%**（−8.46ms）、A2→B2 **−23.1%**（−9.47ms），4 轮逐轮中位全部同向；
响应字节 252–256、0 错误、0×429、`warnings=[]`。

**探针 B（X1 checkpoint）与 Gate 0 事故**：第一版只跳过 `checkpointAfterBatch` 的 `post_batch`；
Gate 0 读 `/v1/metrics` 发现 `head_checkpoint_attempts_total` 仍 **56**——真正的周期路径是
`audit.go` `appendBatch` 的 `pre_batch` guard。该版 4 条腿（`legs-probe-v1/`）只作 Gate 0 证据，
不进入判决。修正版（`c59e917`）的 `commitHead` 只保留 `shutdown`：

| 栈 | `head_checkpoint_attempts_total` | `head_checkpoint_ns_total` | `fsync_count` |
|---|---:|---:|---:|
| baseline | 54 | 1.1447s | 512 |
| 修正探针 | **0** | 0 | 567 |

修正探针 X1 json-pipeline（4 条腿，100 请求/级）：c4/c8 p99 多数下降（c4 −73%/−51%、c8 −14%/−73%），
但 c16 一条腿 +36%、另一条 −44%；4 级 elapsed 合计 B1 −2.0% / B2 **+6.8%**，单级跨度 82–557ms，
方差大于效应。

**判决：两项探针均不合入**，分支 `probe/r61-w1-intent-ceiling` @ `9cf4911`、
`probe/r61-x1-checkpoint-ceiling` @ `c59e917` 保留为证据。

- W1：intent 屏障等待的上限是 **21–23%**，是当前唯一被量化到 ≥10% 的剩余方向；安全实现必须保留
  「意图先 durable 再派发」，只能把意图排在延迟会话记录之前——属 journal 顺序/耐久提交时点，
  **需用户显式授权**，本轮不实现。
- X1：checkpoint 可以被完全跳过却不改变请求路径（请求在审计 append/checkpoint 之前已回复），
  **不是尾部限速器**；该方向不再申请授权。

**方法与教训**：Gate 0 必须**先于** A/B。第一版 checkpoint 探针跑了完整 A/B 后才核机制，先得到的是
「无效探针 vs 基线」的噪声；修正后重跑全部 4 条腿。上限探针量化的是「拿掉等待后的单请求延迟」，
不是安全实现的收益承诺。

**下一轮提示**：X1 的 100–150ms 停顿在 c4/c8/c16 都出现、c1 不出现且与 checkpoint 无关；下一步在
worker 会话 open/close 与 platform admission/deferred queue 之间做单窗口归因。授权内剩余项
（worker 页响应缓冲 3–8%、平台页路径 <8%、审计编码 1–2%）均低于 10% 门槛，不再单独开轮。

**证据**：`docs/performance-review/2026-11-04-r61-durability-ceiling-probes/`（`README.md`、`legs/`、
`legs-probe-v1/`、`gate0/`、`run_leg.sh`、`gate0.sh`、`aggregate.py`）。

#### R62：延后耐久账户待办上限 256→4096（2026-11-04，主指标通过、c1 p95 护栏失败，不合入）

**机制**：R37 的 `deferredDurability` 账户按 FIFO 支付非提交操作的 completion/audit 屏障。上限
256 时，第 257 条待办会在回复刷出之前**内联**支付屏障，账户饱和直接变成客户端时延。R59 排除
池宽，R61 排除周期 checkpoint；本轮把上限对齐审计队列自身的准入上限（4096 条 / 8 MiB），内联
路径保留为账户停止时的 fail-closed 守卫。

**改动**（`861eb8a`、`530fcf7`）：`deferredDurabilityMaximum` 256→4096；`/v1/metrics` 增加
`deferred_durability`（`pending`、`pending_max`、`maximum`、`enqueued_total`、`inline_total`）；
新增 X1 尾部时间线探针。顺序、`outcome_unknown`、关闭排空语义未改。

**Gate 0 与令牌桶混杂**：100 请求/级（X1 合同值）时候选 c16 出现 16 次拒绝（13×429 + 3× pipeline
内 `quota_exceeded`），基线 0 次。原因是候选在 c1–c8 快约一个数量级，把固定 **500 rps** 令牌桶
提前耗尽；拒绝路径的审计记录又排在延后积压之后，单条约 500ms。同一次 c16 的成功请求 p50
6.46ms / p99 20.37ms，证明不是查询执行问题。改用 50 请求/级（配额不变，只让窗口落在桶的突发
预算内）后两侧都干净；100/级下的拒绝行为作为**配额边界副作用**单独记录，不进入时延判决。

**A/B**：2 轮 × A/B/B/A，8 条独立栈 + 重新 seed，X1 `json-pipeline`，50 请求/级。主指标
c8/c16 p95、p99 两轮四单元全部改善 −31.1%~−79.3%；合并中位 c8 p95 −33.1%、c8 p99 −67.6%、
c16 p95 −62.1%、c16 p99 −49.2%。运行性护栏全过：0 错误、0×429、`inline_total=0`、fixture 与
clean stop 均通过；候选 `pending_max` 645–770，基线无该指标。

**护栏失败**：冻结的 c1 p95 合并中位 0.973→6.370ms（**+554.9%**）。c1 p50 −3.2%、c1 p99 −22.1%；
5–9ms 尖峰出现在 3/4 候选腿、1/4 基线腿，合并原始样本 >2ms 为 8/200（基线）与 13/200（候选）。
反序诊断（候选先跑）c1 p95=0.54ms，说明候选在干净宿主状态下可与基线一致，尖峰是间歇性的，
尚未归因。

**判决**：按 A/B 前冻结的判据，**主指标 PASS、护栏 FAIL，不合入**。分支
`perf/r62-deferred-bound` 与两份提交保留作证据；下一轮在 c1 上做请求级相位归因
（open/statement/close 的 journal/audit/native 分段），先确认 5–9ms 尖峰归属，再决定修实现
还是修测量方式。

**证据**：`docs/performance-review/2026-11-04-r62-deferred-bound/`（`README.md`、`legs/`、
`legs-diag/`、`gate0/`、`run_leg.sh`、`aggregate.py`）。

#### R63：R62 c1 尾部的归因（2026-11-04，测量轮，无生产代码改动）

**问题**：R62 的冻结护栏 c1 p95 合并中位 0.973→6.370ms（+554.9%）判 FAIL，而 c1 p50/p99 都不
差于基线；同一候选在反序诊断里 c1 p95 = 0.54ms。需要回答尖峰是否候选独有、落在哪一段、
256→4096 在 c1 上是否可能生效。

**方法**（两侧仍是 R62 的两个平台二进制，worker 相同，每腿独立栈 + 重新 seed）：
- 观察器腿 o1–o8：X1 1/4/8/16、50 请求/级，开启请求相位观察器（阈值 0、采样 1000‰），
  顺序 B A A B B A A B；只用于 span 归因，绝对值不进性能结论。
- c1-only 腿 p1–p6：harness 副本 `concurrency_levels=(1,)`（补丁随记录提交），只跑 c1、50 请求、
  **不开观察器**，顺序 B A A B B A。
- 全部 14 条腿 `errors=0`、`status_429=0`、`rejection_reasons={}`、`rows=1`、
  `stop: clean unresolved: False`。

**结果**：
- c1-only：A/B 腿中位 p50 **0.44 / 0.46ms**、p95 **0.56 / 0.55ms**、>2ms **3/150 vs 6/150**；
  只有 p5 一条腿 p95 越过 2ms（4.65ms，4 条 >2ms 样本）。两侧最大值同为 7.7–16.7ms 量级。
- 观察器腿：**八条腿每一条都有 ≥1 条 >5ms 的 c1 样本，两侧都有**（A 4/200、B 9/200 >2ms；
  max 18.56 / 19.51ms）。13 条慢样本的形态：`terminal`（延后耐久结算）6.20–11.08ms、
  handler 外层帧（`admission`，包到 handler 结束）自身时间 6.92–14.27ms、
  内层记录之间的空档 4.05–17.95ms；尖峰在腿内分散（o5 在 +7 / +28 / +45ms），不是一次全局停顿。
- 宿主屏障：每条腿（两组、两侧）journal 等待 max **8.1–12.3ms**、audit fsync 均值 4.1–4.7ms、
  max **7.2–9.8ms**，与尖峰同量级。c1-only 组 `head_checkpoint_attempts` 两侧都是 4，
  否掉检查点差异解释 p5（R61 已独立否证检查点是尾部限速器）。

**机制核对（决定性）**：c1-only 腿 `deferred_durability.pending_max` = **243 / 215 / 200**，
低于基线冻结上限 **256**，`inline_total=0` ⇒ 内联回退从未触发；且观察器腿里 c1 是每条腿的
第一级，账户从空开始（`pending_max` 696–780 来自后面的 c4/c8/c16）。因此
`deferredDurabilityMaximum` 的 256→4096 差异在 c1 单元上**结构性不可见**。

**判决**：仅记录，无代码改动。c1 尖峰是两侧共有的宿主屏障抖动，与 R62 候选无因果关系；
**不推翻 R62 的冻结判决、不重跑 R62 洗判据、不合入 `perf/r62-deferred-bound`**。
R64 若要重新评估该候选，必须在 A/B 前冻结一个 c1 宿主抖动无法触发的判据
（例如尖峰计数/形态，或换掉"50 样本第 3 高值"这种对单次抖动敏感的口径），
并且仍需重新跑完整 A/B，不能引用本轮数字。

**未闭合**：`terminal` 与 handler 外层帧内部没有更细 span，"第 X 次 fsync 造成第 Y 条样本"
未被证明；宿主的 fsync 抖动也没有用 io 限速或独立磁盘做受控复现。

**证据**：`docs/performance-review/2026-11-04-r63-c1-spike-attribution/`（`README.md`、
`legs/`、`legs-c1/`、`observer/`、`run_leg_observer.sh`、`run_leg_c1_only.sh`、
`harness-c1-only.patch`、`aggregate.py`、`observer_spikes.py`、`observer-spikes.md`）。

#### R64：在新冻结判据下重新评估 R62 候选（2026-11-04，测量轮，合入）

**背景**：R62 主指标通过、c1 p95 护栏失败（0.973→6.370ms）。R63 已证明该护栏度量的是宿主
fsync 抖动（两侧共有、形态相同），且 256→4096 在 c1 的 50 请求配置下结构性不可见
（`pending_max` 200–243 < 256、`inline_total=0`）。R64 在**首条腿之前**冻结新判据并推送
（`ad1d113`）：c1 的尾部（p95/p99/max/>2ms）降为报告项，c1 中位数保留护栏；主指标与其余护栏
沿用 R62 口径。R62 自己的冻结判决不变。

**方法**：候选与基线仍是 R62 的两个平台二进制（sha256 `fd959df7…` / `98487e5e…`），worker
两侧相同（`a648ecd9…`，v1.2.5）。2 轮 × A/B/B/A 共 8 条腿，端口 20101–20108；每条腿独立栈 +
全新 run dir + 重新导入同一 fixture（threads=201 messages=8625 chars=20177069）。场景 X1
（json-pipeline、`SELECT 1`、`want_rows`），**每级 50 请求**；配额不变（`max_sessions=16`、
`max_concurrent=100`、`rps=500`、`max_result_bytes=4 MiB`）。A/B 期间不开启任何观测器；
`/v1/metrics` 只在计时窗口之外各取一次快照。

**结果**：
- **主指标**（X1 c8/c16 的 p95、p99；每轮四单元 ≤90%，两轮同向）：8 个轮内比较全部一次通过，
  合并比值 c8 p95 **0.108**、c8 p99 **0.076**、c16 p95 **0.320**、c16 p99 **0.314**。
  候选侧离散度小（c8 p95 9.52–17.27ms、c16 p95 9.73–20.24ms），基线侧跨轮跨度大
  （c8 p95 30.83–136.68ms）。
- **护栏**（≤110%）：c1 p50 0.983（R1）/1.040（R2）/1.014（合并）；c4 p50
  0.754/0.737/0.709；c4 p95 0.067/0.037/0.038。全部通过。
- **c1 尾部（报告项）**：A 6/200、B 8/200 >2ms；两侧最大值 16.60 / 16.38ms；候选只有 r2b2
  一条腿越过 2ms（4 条样本，1 条 15.49ms），形态与 R63 记录的宿主屏障抖动一致。

**Gate 0**：四条候选腿 `maximum=4096`、`pending_max` 741/770/661/800（均 >256，证明窗口内越过
基线上限）、`inline_total=0`（新上限未被触达，内联回退未走）；四条基线腿不含该指标。
运行性：8 腿 0 错误、0 ×429、`rejection_reasons={}`、所有样本 `rows=1`、fixture 完整、
`launcher-stop.json` 全为 `clean` 且 `unresolved_operations=false`。

**机制（解释变量，非合入理由）**：`operation_journal_appends_total` 8 条腿都是 **404**
（2.02/请求，工作量不变）；候选 audit fsync 17–20 次 vs 基线 30–32，journal syncs 27–36 vs
36–39；同步 head checkpoint B 8–10 次（191–238ms）vs A 19–20 次（392–423ms）。宿主屏障量级：
每条腿 audit fsync max 7.84–9.03ms、journal 等待 max 9.19–11.23ms，与 c1 尖峰同量级。
**不变量**：响应字节 308–325（`query_duration_ms` 浮点位数导致 ±11–12 字节）；掩码该字段后
5 对原始 pipeline 响应体逐字节相同。

**测试**：候选提交（`530fcf7`）上 `gofmt -l .` 无输出、`go vet ./...` 退出 0、
`go build ./cmd/peri-platform ./cmd/peri` 通过、`go test ./...`（`PERI_RUST_BINARY` 指向
v1.2.5 worker）**全绿**（`peri-loom/platform` 176.9s，其余包 ok，退出码 0）。

**剩余风险**：① 待办上限提高意味着掉电时可能丢失的「已确认回复的只读/open/close 记录」更多，
仍属 R37 用户授权范围，隔离与 fail-closed 规则未变；② 未测 4096 满载时的内存峰值
（本窗口 `pending_max` 661–800）；③ 只测 X1，未复测 W1/H1/H3/F1/L1；④ 单机 macOS + 本机卷、
无 TLS，不代表容量或 SLA；⑤ c1 尾部按新判据不设门槛，若要真正降低尾部只能改宿主/屏障成本。

**证据**：`docs/performance-review/2026-11-04-r64-r62-reeval/`（`protocol.md`、`README.md`、
`legs/`、`launcher-stop.json`、`invariants/`、`legs.txt`、`aggregate.py`、`run_leg.sh`、
`run_all.sh`、`seed_fixture.py`、`snap_metrics.py`）。

#### R65：R64 合入后的回归门（2026-11-04，测量轮，无代码改动）

**目的**：R64 的冻结协议只覆盖 X1；W1、F1、H1 在合入判决时没有复测，合入记录已写明这是缺口。
本轮确认延后耐久账户待办上限 256→4096 没有让任何主场景的端到端中位数退化 >10%。判据在第一条腿之前
冻结（`protocol.md`）：同一场景两轮比值都 >1.10 且合并（4 腿中位）>1.10 才判回归；否则记「无回归」。

**方法**：A = `main` @ `1399f15`（sha256 `fd959df7…`），B = `main` @ `1a6b9dd` 重新构建
（sha256 `9214cd0c…`）；worker 两侧相同（`a648ecd9…`，v1.2.5）。2 轮 × A/B/B/A，每场景 8 腿，
共 24 腿，端口 20301–20308。主协议 W1/F1 `json-pipeline`、H1 `json-cursor`；每腿 1 warmup +
5 timed（H1 为 3 timed）。每条腿独立栈 + 全新 run dir + 重新导入同一 fixture
（threads=201、messages=8625、chars=20177069）；默认配额、无 TLS、无观测器，
`/v1/metrics` 只在计时窗口之外各取一次快照。

**结果**：
- **有效性**：24/24 腿有效；fixture `complete=true`、352/352 请求 `ok`/HTTP 200、
  warnings/fatal 空、F1 rows=1、H1 rows=8625、samples 5/5/3、clean stop 且
  `unresolved_operations=false`、无 429/`expected_limit`/`audit_unavailable`；
  窗口后 `dropped_total`、`head_checkpoint_failures_total`、compaction failures、
  queue/in_flight/session_seats/active_permits/rejected 均为 0。`aggregate.py` 退出码 0、
  三次输出 sha256 一致；独立 verifier 重算 64 个协议行，0 mismatch。
- **主指标**（候选/基线）：W1 round1 **0.972**（33.657→32.708ms）、round2 **1.153**
  （36.178→41.696ms）、合并 **1.101**（33.705→37.125ms）；F1 两轮均 **0.987**、
  合并 **0.987**（0.532→0.525ms）；H1 round1 **0.950**、round2 **1.078**、
  合并 **1.022**（53.171→54.357ms）。按冻结判据，三场景均判**无回归 PASS**。
- **W1 边界 PASS**：round2 1.153 与合并 1.101 都超过 1.10，只因 round1 0.972 未超阈值
  才判 PASS；**不是稳健无回归**。W1 round2 的 `query_ms` 从约 9–11ms 阶跃到
  16.96 / 20.41 / 16.14ms（A 侧最后一条腿 `r2a2` 同样受影响）；用
  `duration_ms − query_ms` 的腿中位再取中位，round1 **0.977**、round2 **1.031**、
  合并 **0.977** ⇒ 平台侧没有回归信号，W1 边界由 worker SQL 时间抬升主导。
- **p95（报告项）**：F1 出现 A/B 两侧 8–18ms 单样本离群（A `r1a1` 17.819、`r1a2` 8.513、
  `r2a1` 13.578、`r2a2` 12.946/8.659；B `r1b1` 15.480），中位数稳定；
  F1 p95 比值 0.064 / 0.095 不能解读为真实 p95 改善。

**机制快照 caveat**：12 条 B 腿 `deferred_durability.maximum=4096`、`inline_total=0`；
`pending_max` W1 **4**、F1 **91**、H1 **18–21**，远低于 4096，说明本轮没有逼近/验证新上限的
饱和行为；12 条 A 腿无该指标。二进制 `go version -m` 的 `vcs.revision` 为 `1399f15` /
`1a6b9dd`、均 `vcs.modified=false`；两侧 `metadata.platform_label` 都是 `a46afa2`，
不能用于区分 A/B，身份判定依赖 sha256 与机制指标。

**数据质量限制**：① `run_all.sh` 只在全部腿开始前复核一次 sha256，而协议措辞是「每次运行前」；
② `aggregate.py` 在某一侧缺一条腿时会静默少用一条腿仍计算比值（本轮 24 腿齐全，不影响结论）；
③ 审计链连续性没有直接的链头/连续性 artifact，间接证据是 24/24 clean stop 与
checkpoint failures=0；④ 未做观测器，`concurrency_requests=50` 只属于 X1，本场景为串行。

**结论**：冻结判据下 W1/F1/H1 均无 >10% 回归；W1 为边界 PASS；无代码改动。R65 测于 `a46afa2`
（即 `main` @ `1a6b9dd` 的生产代码）；之后为修复 CI 回归，R43 的 adapter 级 fused close 被撤销
（保留 fused open），这会改变 F1 的绝对延迟。本轮判决只代表 R64 的 deferred durability
256→4096 变更在 `a46afa2` 上的表现，不代表修复后的 F1 绝对值。

**证据**：`docs/performance-review/2026-11-04-r65-r64-regression/`（`protocol.md`、`README.md`、
`legs/`、`legs.txt`、`aggregate.py`、`run_leg.sh`、`run_all.sh`、`seed_fixture.py`、
`snap_metrics.py`）。

#### R66：三个上限探针（2026-11-04，探针轮，无可合入改动）

**目的**：R64 合入、R65 回归门通过后，用三个授权内的上限探针判断 X1 尾部与 F1 close 是否还有
值得实现安全版本的方向。三个探针的判据都在首条腿之前冻结；全部数字为 **smoke**，
不构成对外性能结论。**没有合入任何生产代码；唯一代码改动在未 push 的探针分支上。**

**探针 1——legacy 全队列 settle 跳过**（`probe/r66-legacy-settle-ceiling` @ `066ba9e`，
基线 `main` @ `39e8e6c`；`operation_store.go` +5/−1）：`awaitOperationCommit` 的 legacy
（`audit_file_v1`）分支不再调用 `settleOperations()`，读路径的 settle 仍在。X1 `json-pipeline`，
每级 50 请求，2 轮 × A/B/B/A，8 腿，无观测器、默认配额。结果：round1 c8 p95/p99
**0.160/0.169**、c16 **2.237/2.123**；round2 c8 **2.685/2.692**、c16 **3.333/2.591**；
c8 两轮方向相反，c16 p95/p99 两轮 2.12–3.33×；c1 p50 护栏 0.934/0.890/合并 0.900 通过。
机制：`state_lock_wait_ns_total` 每请求 **3.01→1.74ms（−42%）**、c1 尾部 p95
6.83→0.57 / 7.57→0.64ms，但 `appends=404`、`compactions=0` 不变，c8/c16 无改善——
等待没有消失，只是推给读路径的 `settleOperations()`。**判决：上限不足，停止；不实现安全版本**
（探针语义本身不安全，可能留下未确认编辑）。

**探针 2——GOGC 400→1000/off**（零代码，仅平台子进程环境变量；A=400、B=1000、C=off）：
Gate 0 gctrace 为 **39 / 13 / 0** 次 GC，首个堆目标 16/40/—MB，RSS 42.7/60.2/490.8MiB，
证明变量到达运行时。正式 2 轮 × A/B/B/A + 每轮一条 C，共 10 腿。B/A：round1 c8 p95
**2.763**、c8 p99 0.727、c16 p95 **1.947**、c16 p99 **1.826**；round2 c8 1.036/**1.114**、
c16 **0.339**/**0.393**——四单元两轮方向全部相反，无单元两轮 ≤0.90；c1 护栏 0.973/1.013/
合并 0.980 通过。绝对上限档 C（GC 次数 0、RSS 490.6/491.1MiB、未 OOM）在 round1 看似改善
（c16 0.346/0.349），round2 仍为 A 的 **1.809×/1.819×**。慢请求是 **15.7–32.9ms 的连续样本块**，
A/B/C 随机出现；零 GC 的腿仍出现 17.7–18.5ms 的 c16 台阶。**判决：GC 尾延迟假设否证/不可确认，
停止**；c8/c16 尾部由宿主级暂停主导，不是 GC assist。

**探针 3——F1 close 成本与 fused close 上限**（零代码，只读 `/v1/metrics`；主协议 19:51 冻结，
补充诊断 20:05 冻结）：当前 `main` 的 F1 `json-pipeline` 2×50 合并 p50 **0.5449ms**、
p95 7.6276ms、mean 1.2820ms；close **100 次 / 24.8217ms / 均值 0.2482ms**；
open/statement/close 占 mean **51.6%/19.9%/19.4%**（residual 9.2%）。若整个 close 入口消失，
机械上限为 close 均值 / F1 p50 = **45.5%**；补充诊断的稳健 close 中位 0.2330ms、截尾均值
0.2268ms（42.8%/41.6%）。但 fused close 只把 worker close 工作移入 open，不消除 worker close
工作与平台 close 入口，外部 HTTP 帧数也不变；R65（fused close 生效）8 条 F1 腿 p50
0.465–0.602ms 与当前 0.545ms 无系统性差异，可归因部分下限约 0.02ms（≈4%）。
**判决：不能证明 ≥20% E2E；实现 fused close 属“减少协议帧数”红线，需用户明确授权，
且先要 close-only 受控 A/B。** 两轮都在第 14 个请求附近出现约 16.7ms 停顿，
动作均值随停顿落在哪个动作跳动（分轮 close 均值 0.314/0.183ms）。

**交叉结论**：① X1 c8/c16 尾部是宿主级暂停主导（GC=off 仍有同样台阶；legacy settle 移除
只把等待推给读路径）；n=50 的 p95/p99 被 5–8 个连续样本块决定，单轮方向不能当系统性快慢。
② F1 close 是真实成本（≈0.248ms/次），但 45.5% 只是“整个入口消失”的机械上限，不能当预期收益。
③ worker fused open 生命周期去重已被 R24 数据关闭（`begin.entry/image/call` 合计 0.005ms/请求，
占 X1 c16 p50 1.8–2.6ms 的 0.2–0.3%，远低于 10%）。

**剩余方向与授权边界**：唯一已测到 ≥10% 的剩余方向是 **W1 intent 屏障重排**
（R61 探针 −20.9%~−23.1%），它改变写语句的耐久提交时点/顺序，**必须用户明确授权**，R66 未实施。
fused close 未证明 ≥20% 且属红线；legacy settle 安全版本没有 ≥10% 证据；GC 方向停止。

**证据**：`docs/performance-review/2026-11-04-r66-probes/`（`README.md`；`legacy-settle-ceiling/`、
`gc-target/`、`f1-close/` 下的 `protocol.md`、`report.md`、`aggregate.txt|json`、`legs/`、
`gate0/`、`round{1,2}/`、`supp/`、`capprobe/`）。复制只含 `.json/.md/.txt`，
排除 `raw.jsonl`、`*.log`、`per-request/*`、`_raw/`、脚本与二进制；原始证据仍在
`/tmp/peri-r66-*`。复制后 543 个文件 / 6.40MiB；唯一脱敏是
`f1-close/capprobe/fused-open-probe.txt` 的 worker session id → `<REDACTED>`，
全树无 `Authorization`/`Bearer` 头值，6 份 `connection.json` 的凭据精确比对 0 命中。

**限制**：smoke（X1 每级 50 请求；F1 每轮 50/100 请求）；单机 macOS 26/arm64、local backend、
无 TLS、默认配额；探针 1 用 darwin/arm64（linux 产物仅构建留档）；探针 2 用 `/tmp` 的 `dev.py`
副本转发 `GOGC`（仓库文件零改动），macOS `ps` 不能读他进程环境，故用 gctrace/RSS 证明；
探针 3 的补充诊断逐请求抓 metrics 会改变系统状态，其 E2E 中位数 1.18ms 不可作为 F1 基线。

#### R67：发布 A/B 的 F1 FAIL 未被复现（验收功效问题）

**背景**：`docs/performance-review/2026-11-04-v1.2.7-release/README.md` 把
`F1.select-1 / json-pipeline` 判为 **FAIL**（基线 2.433ms → 候选 2.891ms，比值 1.188，
两轮 1.228 / 1.541，每腿 5 个 timed 样本）。本轮用更高功效的独立窗口复测同一对比，
判断该 FAIL 是否可复现。

**方法**：沿用上位冻结协议（同运行镜像 `sha256:8a8ba43b…`；A = v1.2.6 平台 `2a4a41d8…`
/ worker `61f74236…`，B = v1.2.7 平台 `95716e4d…` / worker `b17a0708…`；每腿全新卷、
无观测器、默认配额、每腿重新导入 fixture），把每腿 timed 样本由 5 提到 **20**；
2 轮 × A/B/B/A = 8 腿、端口 20401–20408。样本数是相对冻结协议的唯一参数偏离，已在
测量前预注册。8/8 腿二进制 sha256、镜像 digest、fixture、`launcher-stop` clean 与
`stop-exit=0` 全部匹配；160 个 timed 样本全部 `outcome=ok`、`rows=1`、`response_bytes=312`。

**结果**：

| 轮 | 基线中位 | 候选中位 | 比值 |
|---|---:|---:|---:|
| round1 | 2.631ms | 2.106ms | 0.800 |
| round2 | 2.384ms | 3.042ms | **1.276** |
| 合并 | 2.631ms | 2.413ms | **0.917** |

- 两轮方向相反；合并 0.917 < 1.10，未满足冻结的 FAIL 判据。
- 样本级（80/侧）：中位比值 0.939，Mann-Whitney 双侧 **p=0.997**，
  permutation p=0.386 / 0.484 / 0.787，bootstrap 95% CI 均跨 0；10% trimmed mean
  比值 0.912；>5ms 样本占比 A 21.2% / B 16.2%。
- 腿内 sd 1.6–2.4ms、CV 52–74%；中位差 0.41–0.75ms 远小于 3×SD（约 6.5ms）。
- **原发布窗口自身也分不开**：40 个样本 Mann-Whitney p=0.989、permutation p=0.599。
  1.188 来自「每轮 2 条腿中位再取中位」这个低功效估计量——在腿内 CV 45–91% 的分布上，
  它的方差远大于要检测的 0.24–0.5ms 效应。
- 结论：**发布报告里的 F1 FAIL 未被复现，判为噪声主导**；原 FAIL 记录按当时冻结规则
  保留，不改写。

**机制证据**：

- F1 请求确实含 close：`[execute(SELECT 1), close]`，编码后 **102 bytes**，与 8 腿
  `raw.jsonl` 的 `request_bytes=102` 一致。
- 两侧 actions 调用次数完全相同：`open=22`、`statement=21`、`close=22`，
  `describe` 与 `cursor_*` 全 0——没有证据表明 v1.2.7 在 F1 路径上多做了 open/close RPC。
- journal `appends=2.095/请求`、audit fsync `1.238/请求` 两侧一致；
  `operation_journal_syncs` 1.381/1.357、coalesced 0.714/0.738，差异在噪声内。
- **R64 未生效**：v1.2.7 的 `deferred_durability.pending_max` 为 17/35/18/22，
  远低于旧上限 256，`inline_total=0`。
- **三条事实更正**：① v1.2.6 tag（11-03 15:51）没有 fused close，也没有
  `hrana_fused_open.go`——R43 `6d5c9da`（11-04 00:06）引入、`17854a8`（11-04 18:00）
  撤销，因此「撤销 fused close」不是 v1.2.6→v1.2.7 的行为差异；② v1.2.6→v1.2.7 实际有
  **17 个非 merge 代码提交**（R39/R42/R46–R50/R51/R52/R56/R57/R64 等），若差异未来
  复现，**R39（worker 每库 open/close 闸门，`0b879c9`）**比 R64 更值得先测；
  ③ fused close 的引入与撤销都发生在 v1.2.7 开发窗口内部。

**方法结论（可复用判据）**：

- **亚 5ms 场景不能用「腿中位的中位」做验收**。按 σ≈2ms 估算，检测 0.5ms 差异、
  80% power 需要约 **248 样本/侧**；每腿 5 样本、每轮 2 条腿的估计量远低于此，
  判决接近掷硬币。
- 后续这类场景改用**样本级合并统计**（Mann-Whitney / permutation / bootstrap，
  并给出最小样本量）或**请求级交错 A/B**（同一栈内交替发出两侧请求，抵消腿间
  宿主漂移）。

**证据**：`docs/performance-review/2026-11-05-f1-release-attribution/`（`README.md`、
`perf-protocol.md`、`aggregate.py|txt`、`invariants.py|txt`、`legs/`、`mechanism/`）。
提交前扫描无 `Bearer`/API Key/launch token/`connection.json`；`run_leg.sh` 产出的
`harness`/`seed`/`container` 日志由 `.log` 改名为 `.txt`（`.gitignore` 含 `*.log`），
内容未改。

**限制**：单宿主、单窗口；未做 P0/P1/P2 提交级归因（预注册规则：两轮方向相反即结案）；
20 样本/腿仍不足以在 0.5ms 量级上给出 80% power；F1 的双峰慢尾（~1ms 与 ~5–7ms 两簇）
来源未定位。

#### R68：只读反思——现有授权内已无 ≥10% 方向（无代码改动）

**目的与方法**：R66 已判定「现有授权内没有能过 10% 门槛的方向」；本轮以只读方式复核该结论，
并顺带关闭两条长期挂起的容量疑问（c128 的 `audit_unavailable` 归属、`deferred_durability`
4096 上限的饱和行为）。方法是既往轮次实测记录 + 本轮代码阅读。**本轮没有任何 A/B 测量：
所有数字要么来自既往轮次的实测记录，要么来自本轮代码阅读，不得被引用为新的性能结论。**

**候选与上限（逐条否证）**：

| 候选 | 关键位置 | 上限 / 已有证据 | 判定 |
|---|---|---|---|
| 适配器→网关内部 HTTP 转发 | `hrana_http.go:995-1006`、`hrana_protobuf.go:1160-1175`、`hrana_v3_json_cursor.go:845-860` | 转发本身 25µs/条（F1 4.7%、L1 2.4%、C1 7.0%）；R44 D3 实测其中**可移除**部分只有 2.6µs/条（`json.Marshal` 0.61 + `httptest.NewRequest` 1.16 + reply 0.69 + 响应编码 0.16µs），折算 F1≈1%、B1≈2.4% | 上限不足 10% |
| worker 会话生命周期 | `src/worker_rpc/sessions.rs:233-300`、`:417`；平台入口 `gateway_http.go:423-431` | `open_session` 251.4µs + `close_session` 89.8µs = 341µs/请求，占 F1 p50 64%、L1 33%、c1 X1 约 76% | 减少它只有会话复用 / fused close（协议帧）/ 去掉 authority 校验三条路，均需授权或已被否证 |
| W1 intent 屏障重排 | `operation_state.go:454-475`、`gateway_operations.go:222-267` | R61 上限探针实测 −20.9%~−23.1% | 唯一实测 ≥20% 的方向，但改变耐久提交时点，需授权 |
| H1 剩余成本 | worker `turso_step`、页序列化、行级检查 | worker `turso_step` 16.5–18.3ms、页序列化 6.4–7.1ms（R58）；平台页路径 ≤5ms≈7.7%（R54）；worker 序列化 ≤7.2–8.0%（R53）；行级检查 ≤7.3–7.5%（R58） | 均 <10%，且大头是 Turso VM 与协议帧序列化（授权红线） |
| X1 c8/c16 尾部 | deferred 账户、checkpoint、GC | R59 deferred workers 8→64 反向崩坏（c16 3.75→124–166ms）；R61 checkpoint 全跳过无改善；R66 GC=off 仍有 17.7–18.5ms 台阶 | 尾部由宿主级暂停主导，平台侧无杠杆 |
| 编码 / 分配 / GC | journal 编码、堆目标 | R34/R36 编码 ≤1–2%，R35 仅摊薄 GC 代价，R66 探针否证 GC 尾部假设；R36 实测 c64 时 platform 0.30 core、worker 0.10 core | 瓶颈是等待而非算力 |

**结论**：现有授权内没有能同时满足「≥10%（<10ms 场景 ≥20%）」与「不触碰耐久/协议/审计/配额
红线」的单一改动。R66 的判停在本轮复核成立。

**c128 的 `audit_unavailable`(503) 定位（代码阅读结论，未实跑）**：

- 默认配额下 c128 先被会话座位 16（`admission.go:199-205`）与并发 100（`admission.go:201-203`）
  挡住，返回 **429 `quota_exceeded`**（原因 `session_limit` / `concurrency_limit`），不会先到 503。
- 配额放宽后，**v1 `audit_file_v1`**（`completionStore == nil`）在 c128 下容量型 503 不可达：
  完成槽 1024/4 MiB、全局 4096/8 MiB、请求 1024/2 MiB（`audit.go:44-58`）。
- **v2 `completion_journal_v2`** 下 `completionReservedMaximum=64`（`completion_journal.go:12`，
  判定 `audit.go:322-326`，`limits.completion_slots` 在 v2 被覆盖为 64，`audit.go:824-828`）：
  同时处于「已预留、未入队」的**非-close** 在途请求超过 64 个 → 第 65 个起 **503
  `audit_unavailable`**，metrics 拒绝原因 `audit_capacity`。
- close 永不因槽位被拒（`gateway_http.go:418` 显式 `action != "close"`），避免座位泄漏。
- 其余 `audit_unavailable` 是 fail-closed 而非容量：`gateway_http.go:283-286`（`deferred.refusal()`）、
  `hrana_http.go:280-289` + `:853-862`（pipeline collector Flush 失败）、
  `gateway_http.go:212-231`（无 reservation 的 close 在完成预算耗尽时）。
- **精确化**：R25/R30/R31 记录的「c128 下 `audit_unavailable` 仍是独立问题」应读作
  「**v2 完成日志 + 超过 64 个并发在途非-close 请求**才会触发；默认配额先撞 429；
  v1 默认配置下不可达」。原记录保持原样，本节只作补充。以上为代码阅读结论，**没有实跑验证**。

**`deferred_durability` 上限 4096 的饱和行为**：

- `settle` 在 `pending >= maximum` 或账户已停止时不解引用队列，直接 `inline.Add(1)` 并同步
  `wait()`（`durability_deferred.go:81-86`）——语义是「回复后异步」退化为「回复前内联等待」，
  **不返回 503**。
- 调用点在请求 defer 内、`writeResponse` 之前（`gateway_http.go:133-161`、`:247-257`），
  不持有命名空间 fence、不占 completion 槽，因此不会级联成容量拒绝。
- 计数器：`/v1/metrics` 的 `deferred_durability.pending/pending_max/maximum/enqueued_total/inline_total`
  （`durability_deferred.go:124-139`、`metrics.go:98`）。
- 现状：确定性单测 `TestDeferredQueueSaturationWaitsInline`（`durability_deferred_test.go:132-160`，
  `maximum=1`）覆盖行为；生产值 4096 从未在 E2E 触达（R65 `pending_max` W1=4 / F1=91 / H1=18–21；
  R64 的 X1 c1–c16 为 661–800，`inline_total=0`）。
- **判定**：不是正确性缺陷，是**容量标定缺口**——高并发长窗口下有逼近 4096 的结构可能，
  届时表现为 p95/p99 台阶而非 503。补测方式（未实施）：c128 容量实验（非默认配额，需标注）
  抓 `pending_max`/`inline_total`。

**需授权才能做的方向（本轮不推荐实施、未实施，仅备查）**：

| 方向 | 越界原因 | 需要的授权 | 需要的 E2E 证据 |
|---|---|---|---|
| W1 intent 屏障重排 | 改变写语句耐久提交时点/顺序（`operation_state.go:454-475`、`gateway_operations.go:222-267`） | 耐久语义变更授权 | W1 受控 A/B + 崩溃恢复（已 ack 写不丢、Unknown 分类不变） |
| worker 会话复用 | 会话语义与生命周期变更（`src/worker_rpc/sessions.rs:233-300`、`:417`） | 会话绑定 placement、客户端显式重开的语义授权 | 长会话 + 迁移/失效路径 E2E |
| fused close | 减少协议帧数（红线）；且 R66 已证明当前不到 20% | 协议/帧语义授权，并先有 close-only 受控 A/B | adapter 级 close-only A/B + 协议兼容矩阵 |
| authority 本地校验 / lease 缓存 | Go 不做持久层权威，storage 是最终 fencing authority（架构边界 3） | fencing 与权威边界授权 | 旧 epoch 拒写、分区/复活演练 |
| audit checkpoint 频率 | 审计链耐久与顺序语义；R61/R66 已先否证其收益 | 审计语义授权 | 崩溃恢复 + 链连续性校验 |

以上均**未实施**、**本轮不推荐实施**：前两项只有上限证据、没有安全实现的设计；后三项或属红线、
或已被探针否证。

**未闭合项**：

- c128 的 v2 槽位结论来自代码阅读，未实跑；
- `deferred_durability` 4096 的上限逼近未标定；
- 现有授权内已无 ≥10% 方向，继续迭代需要用户授权上表任一项。

#### R71：延迟会话关闭——close 的 worker 等待移出回复路径（2026-11-05，合入）

**机制与改动边界**：Route A 延迟会话关闭——把 close 的 worker 等待移出回复路径。kill switch
`PERI_DEFERRED_SESSION_CLOSE`（R71 合入时默认关闭；R72 起**默认启用**，显式 `=0` 回退到同步关闭，
行为与改动前完全一致）；每库最多 4 个
pending close、全局队列 256 / 8 worker；队列饱和时回退为内联等待（**绝不返回 503**）；
`removeSession` 在入队前执行，seat 立即归还；仅 **pristine 只读会话**可延迟关闭（提交型语句、
`RequiresIntent`、`autocommit=false`、写游标、`outcome_unknown` 一律不走该路径）；
`/v1/sql/execute` 与 `/v1/sql/query` 始终同步关闭（native query 缺 `autocommit` 字段，属设计
边界）。Route B（worker 会话池化 / 连接复用）为 **HARD NO-GO**：Turso `Connection::close()`
是终态、无会话状态复位，且复用 upstream session ID 会破坏 `dischargePendingOpenSessions`。

**证据目录**：`docs/performance-review/2026-11-05-r71-deferred-close-gate/`、`…-gate2/`、
`…-ab/`、`…-ab2/`、`…-ab3/`（早期轮次，含失败结论，按规则留档）、
`docs/performance-review/2026-11-05-r71d-deferred-close-ab4/`（R71d 验收）、
`docs/performance-review/2026-11-05-r71-g3-crash-adversarial/`（Gate 3）。

**验收数字（R71d）**：判据 SHA256 `4a04203e1f9e4e9cf2ad598fd1a2829cbf779b00f5615970eaadf6c86fb2fd29`，
冻结于 `2026-11-05T06:32:23Z`。P0.1 零对照 1.01602（CI 0.88079–1.16322）；P0.2 正对照
1.09317（引自 R71b）；主指标 P1 **0.76577（−23.4%）**，CI 0.66563–0.87018，配对 26/30 同向，
配对差值 −1.0359 ms；F1 p50 0.40600 / p95 0.82209；L1 p50 0.61364 / p95 0.83476（两侧响应
字节 15,627）；标准 X1 c8 1.11257；持续 c8 **1.02246 且 8/8 legs 零错误**；P3 **34/34**。

**Gate 0 机制核验**：`operation_settle` 新路径在 34/34 legs 命中，`retry_total=0`，0 个 502 /
`outcome_unknown`；候选 platform SHA256
`a07ec7b874df3d941ea274823857f2a42ed63497f0c76f50795fb8bef8577cca`、worker
`b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff`、镜像 digest
`sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`、launcher
`80cd2a3c…`。

**Gate 3 崩溃/对抗（PASS，`…-g3-crash-adversarial/`）**：

- **T1 优雅 drain**：`pending=0`、`unresolved=0`、`failed=0`、2407 条 close 审计记录、干净停止；
  注入 2000 ms 时 4/4 在 1907 ms 内排空。
- **T2 确定性丢弃 close 记录**：启动隔离 + HTTP 409 `database_isolated` + 干净门拒绝；经
  `operations/decision` → `operations/retire-local` → `isolation/release`（或 `peri recover`）
  恢复服务，且 ownership marker 哈希不变。
- **T3 SIGKILL 平台**：已 ack 插入仍可读、审计链正常。
- **T4 worker SIGSTOP**：close 仍 3.28 ms 应答、一次 `cleanup_timeout` 重试、SIGCONT 后
  `pending` 归 0。
- **T5 200 ms close 延迟注入**：A 臂 close 203.422 ms vs B 臂 0.091 ms，请求 p50 3.31 ms；
  饱和后退化为内联延迟（p50≈204 ms）且无 429/502。
- **T6 close 后复用会话**：403 `invalid_session` ×15。
- **T7 c8/c16 各 65 s soak**：7,800/7,799 请求、0 失败、seats/queue 平稳，c16 无节流仅 429
  `quota_exceeded`。
- **T8 kill-switch 等价**：无 `deferred_session_close` 键、同步 close。
- **T9 队列上限注入** `PERI_R71_G3_OPERATION_QUEUE_MAX=2`：1,199/1,199 OK、`retry_total=90`、
  `retry_success_total=58`、`exhausted_total=0`、`refused_total={}`、零 `outcome_unknown`。
- Gate 3 探针分支 `probe/r71-g3-fault-injection` @ `090aad5`，探针二进制 SHA256
  `5c3e8f2a…71152`，**未合并**。

**早期轮次失败（未修正，证据保留）**：R71 / R71b / R71c 均未通过验收；R71c 为 P1 0.77194
（−22.8%）、3/8 legs 各出现一个 502 `outcome_unknown` → P3 31/34。该 502 的根因是**本地操作
日志队列满（`ErrOperationQueueFull`）在写任何记录之前被误判为 SQL unknown**；修复在 PR #35
（`c9b522c` + `1c93103`，merge `b1e8ae9`），与 R71 机制无关，但 R71d 在修复后的基线上通过。

**测试**：Go 定向测试（新增 `session_close_deferred_test.go` 等）、`go vet`、`gofmt`、Gate 3
故障注入矩阵；Rust worker 二进制未改（SHA 见上）。

**剩余风险与未测量**：① 真实掉电/机器崩溃未测，用确定性丢记录探针替代；② worker 崩溃丢会话
状态的恢复未端到端验证（依赖 cleanup 预算与 idle 回收）；③ `deferred_session_close.flushed_total`
的计数口径是「按库 flush 等待次数」而非 drained jobs，Gate 3 T1 判据措辞已按实际口径更正，
drain 由 `pending`/`unresolved`/`failed` + 审计条数 + 干净停止共同证明；④ 饱和时退化为内联
延迟（p50≈204 ms @200 ms 注入）而非拒绝，属设计行为但应视为容量边界；⑤ native
`/v1/sql/query` 无 `autocommit` 字段，该路径保持同步关闭。

## 代码约定

- 提交信息用中文全角冒号：`feat(v1)：…`、`fix(authority)：…`、`docs(capacity)：…`。
- 注释讲「为什么」、不复述「是什么」；v1 现有代码几乎无注释、文档为英文，新增注释沿用所在文件风格。
- 错误不得静默吞掉：无说明时默认抛错并留日志（原则 11）。

## 凭据与密钥

- `dev.py` 产出的 `connection.json` 含私有测试 API Key 与开发 CA 路径，**不可公开**、
  不可写入文档或日志。
- 抓取或回显任何含 `Authorization` 的输出时，替换成 `Bearer <REDACTED>`。
