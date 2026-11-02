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

## 代码约定

- 提交信息用中文全角冒号：`feat(v1)：…`、`fix(authority)：…`、`docs(capacity)：…`。
- 注释讲「为什么」、不复述「是什么」；v1 现有代码几乎无注释、文档为英文，新增注释沿用所在文件风格。
- 错误不得静默吞掉：无说明时默认抛错并留日志（原则 11）。

## 凭据与密钥

- `dev.py` 产出的 `connection.json` 含私有测试 API Key 与开发 CA 路径，**不可公开**、
  不可写入文档或日志。
- 抓取或回显任何含 `Authorization` 的输出时，替换成 `Bearer <REDACTED>`。
