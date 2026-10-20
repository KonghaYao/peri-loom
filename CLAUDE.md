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

> v1 已包含默认关闭的实验性 Hrana HTTP/WS 适配；已测试的 SDK/协议范围及未证实的边界见 `prototypes/v1/platform/README.md`，不要宣称完整 Hrana 兼容。

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
  运行 `prototypes/p0` 全部测试与固定 Turso commit 的 SQL 冒烟。`.github/workflows/p0-gate.yml`
  在每次 push / pull request 与 tag 发布时调用它；P0 未通过时 `v1-image` 不会构建镜像、发布
  manifest 或创建 release。不得在发布流程中绕过或裁减该测试集。
- **SQL 兼容标准**（2026-10-10 用户改定）：停止官方 SQLite 原始测试，以 TursoDB 自带测试为准
  （`prototypes/p0/turso` 的 `sqlite-sqltests` / `turso-sqltests`，Rust 后端）。固定 64 MiB hash
  预算、禁用快照更新；跑门禁时不要自行改预算或启用快照更新，结果不可与既有门禁数字混用。
- **SQL 门禁不可宣称全绿**：R21 与 view rename 已有修复代码和回归用例，仍须按固定 64 MiB
  hash 预算重测；分别记录 SQL 结果、约束与 plan snapshot 差异，不自行更新快照。
- **当前交付缺口**：同一持久卷的*正常*重启仍被 launcher 拒绝（clean 门禁未放宽）；原数据库在 Unknown 后已有
  有证明条件的隔离释放路径（`operations/decision` → `operations/retire-local` → `isolation/release`，由
  `dev.py --recover` 在 SQL 关闭的恢复实例中执行，需 owned worker stop proof 与 authority fencing attestation，
  且不删除 ownership marker），但“释放后正常重启”的端到端验证尚未跑通。审计已经按 64 MiB 分段并验证连续
  hash chain，但磁盘空间耗尽前的并发安全准入预留仍未实现。
  可信 SQL 回执不再被审计失败改写为 Unknown。针对性验证：`cd prototypes/v1/platform && go test ./... -run 'TestAudit|TestDurableRecovery|TestGatewayRealAudit' -count=1`。
  最小 SQL／官方 JS SDK CI 见 `.github/workflows/v1-baseline.yml`；尚未运行远端 CI 或完整支持矩阵。
  下一项：正常重启的 clean proof、lease fencing 与持久身份轮换；launcher 并发锁已有针对性回归。
- **性能验收尚未证明 PASS**：功能测试不等于容量或公平性保证；
  运行与安全边界以 `prototypes/v1/platform/README.md` 和 `prototypes/v1/stress/CAPACITY.md` 为准。

## 代码约定

- 提交信息用中文全角冒号：`feat(v1)：…`、`fix(authority)：…`、`docs(capacity)：…`。
- 注释讲「为什么」、不复述「是什么」；v1 现有代码几乎无注释、文档为英文，新增注释沿用所在文件风格。
- 错误不得静默吞掉：无说明时默认抛错并留日志（原则 11）。

## 凭据与密钥

- `dev.py` 产出的 `connection.json` 含私有测试 API Key 与开发 CA 路径，**不可公开**、
  不可写入文档或日志。
- 抓取或回显任何含 `Authorization` 的输出时，替换成 `Bearer <REDACTED>`。
