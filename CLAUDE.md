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
- **同期发现的缺陷（未修）**：worker 在**执行前**拒绝 `open_session`（`database session capacity exceeded`，
  `src/runtime.rs:196`）时，回复不携带 `execution_completed` 证据，平台按 `sqlWorkerFailureFromReply`
  只能判为 ambiguous，于是 `/v1/sql/open` 返回 **502 `outcome_unknown`**（日志证据：
  `SQL outcome unknown: ... action=open stage=open_session ... observation=database session capacity exceeded`）。
  客户端无法区分「可能已提交」与「明确未执行」。修复点在 Rust：让该拒绝携带 rejected/not-executed 证据；
  另需注意网关 `max_sessions` 配额可以高于 worker 每库上限（默认都是 16，属一致；本次把网关提到 256
  才暴露不一致）。
- **逐 action 分解**（串行 100 次，v1.1.26）：`open` 5.276ms、`close` 4.596ms、`cursor_start` 2.044ms、
  `statement` 1.868ms、`describe` 0.391ms，合计 14.32ms。`open`+`close` 占 69%，几乎全由 fsync 决定。
- **待观察**：同期串行 100 次查询的延迟从 11.5ms 漂到 30.4ms；journal compaction 2 次（19.3ms）、
  audit head checkpoint 13 次（79.3ms，0.79ms/查询）不足以解释全部漂移，未归因。
- **候选方向（需授权，均未实施）**：
  1. 把屏障移出命名空间 fence，使同库 commit 可共享一次 fsync。预期：journal 5.94ms/查询 → 约 1.5ms，
     并发吞吐数倍提升。代价：改变 fencing 语义（后续同库操作可能早于前一条记录 durable 就被 dispatch），
     仓库文档已明确列为「需评审」。
  2. 仅修上述 502 误分类（Rust 证据携带），风险低，不改变任何耐久时点。

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
上限 → 执行前拒绝被记为 502 outcome_unknown」仍是待修项。

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

## 代码约定

- 提交信息用中文全角冒号：`feat(v1)：…`、`fix(authority)：…`、`docs(capacity)：…`。
- 注释讲「为什么」、不复述「是什么」；v1 现有代码几乎无注释、文档为英文，新增注释沿用所在文件风格。
- 错误不得静默吞掉：无说明时默认抛错并留日志（原则 11）。

## 凭据与密钥

- `dev.py` 产出的 `connection.json` 含私有测试 API Key 与开发 CA 路径，**不可公开**、
  不可写入文档或日志。
- 抓取或回显任何含 `Authorization` 的输出时，替换成 `Bearer <REDACTED>`。
