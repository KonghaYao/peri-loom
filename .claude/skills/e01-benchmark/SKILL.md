---
name: e01-benchmark
description: 为 E01 点查、索引列表、OFFSET 与 keyset 深分页准备确定性场景、按公开 URL+token 黑盒驱动完整 peri-loom、并按本地 oracle 核验结果。涉及 E01、平台对照、原生 TursoDB 基线、2/4/8 倍压力阶梯或性能结论时使用。当前只实现 smoke 正确性验证；正式规模、性能与容量均未测量。
---

# E01 确定性场景与证据

## 接触点边界（2026-10-16 用户纠正）

peri-loom 目标是**完整平台的黑盒**：只用公开 URL + 凭据，建表/导入/查询/oracle 全走
公开 `/v1/sql/*`。不导入平台内部模块，不读 Worker/Authority/存储或管理状态文件，
不依赖内部 fixture 控制文件，也不从非公开遥测推断扫描量、缓存驻留或内部工作集。
公开 API 缺什么就如实记为能力缺口，不用内部手段补足。
仓库代码/文档只用于确认公开协议，不作为执行依赖。

原生 TursoDB 是**独立基线**（跨边界对照），不是平台验证：其结论不得当作平台结论，
反之亦然。技能同时提供两条适配器，只有一条是平台路径。

## 当前交付边界

已实现：

- 输入合同与 2/4/8× 阶梯计划（`scripts/contract.py`）；当前真实 Peri 规模未知，
  一律 `REFERENCE_MISSING` 或 `SYNTHETIC_REFERENCE`。
- 确定性生成、四族独立 oracle、完整分页无重漏检查（`scripts/model.py`）。
- SQL 渲染（`scripts/sql.py`）：已批准 DDL；`literal` 模式让两个目标消费**同一 SQL 文本**，
  `parameter` 模式使用 `?` 绑定（仅原生路径已核验）。
- 原生基线：`native-harness/`（Rust，嵌入式 `turso_core`，本地持久库，两个进程证明重开）
  + `scripts/run_native.py`。
- 平台黑盒：`scripts/platform_client.py`（Bearer + `X-Request-ID` + v1 信封，
  不重试、不确定即停用会话、凭据只从环境/私有文件读取）+ `scripts/run_platform.py`。
- 共享核验与证据：`scripts/verify.py`、`scripts/evidence.py`、`scripts/jsonio.py`。

未实现或未验证：

- **未经真实 peri-loom 端点验证过任何一次**：用户尚未提供隔离端点与凭据。
  适配器机制只用本地模拟服务验证（见 `tests/mock_gateway.py`，明确不是平台行为）。
- 正式规模：流式生成、分批导入、开环调度、资源采集、导入速率标定均未实现。
- 平台侧假设未验证：DDL 可经 `/v1/sql/execute`、会话内多语句事务、批量 INSERT 的
  `sql_limit`/`sql_budget_exceeded` 边界、`count(*)` 计数。
- 引擎内部扫描量/缓存状态/冷缓存证据：公开 API 无法获得，一律 `NOT MEASURED`。

**没有 `run` 之上的性能结论：`performance`、`overload_safety`、`capacity` 恒为
`NOT MEASURED`；`scale_coverage` 恒为 `SMOKE_ONLY`。** 不能把计划、逻辑字节或
一次性 smoke 当作足压或容量证据。

## 命令（仓库根）

原生基线：

```bash
cargo build --manifest-path .claude/skills/e01-benchmark/native-harness/Cargo.toml --offline
python3 -B .claude/skills/e01-benchmark/scripts/e01.py template --output /tmp/e01/config.json
python3 -B .claude/skills/e01-benchmark/scripts/run_native.py \
  --config /tmp/e01/config.json --out-dir /tmp/e01/run-native
```

平台黑盒（凭据只从环境变量或私有文件读取，绝不进入 argv/日志/报告）：

```bash
export PERI_ENDPOINT="https://<gateway-host>"        # http 只允许字面 loopback IP
export PERI_API_KEY="<私有凭据>"                      # 或 PERI_CREDENTIAL_FILE=<私有文件>
export PERI_CA_FILE="<开发 CA，仅本地 fixture>"        # 可选
python3 -B .claude/skills/e01-benchmark/scripts/run_platform.py \
  --config /tmp/e01/config.json --out-dir /tmp/e01/run-platform \
  --database "<tenant>/<database>/<branch>"          # 隔离 E01 fixture，非机密
```

输出目录必须不存在；脚本以 `O_EXCL` 创建，不覆盖用户文件。第二次运行同一目录会被拒绝
（不静默复用旧 fixture）。缺少端点/凭据时在发出任何请求前拒绝，且不搜索仓库内私有凭据。

本地完整平台（部署准备；新唯一 run 目录 + 空闲端口 + 专用分支/DB）：

```bash
# 1) 自有私有目录（连接资料 0600，绝不进入 argv/日志/报告）
mkdir -p ~/.peri-e01/control-<stamp> && chmod 700 ~/.peri-e01/control-<stamp>
go build -o ~/.peri-e01/control-<stamp>/peri-platform ./cmd/peri-platform   # 在 prototypes/v1/platform
# 2) 启动（--no-build 需要预置二进制；run 目录必须全新）
PERI_PLATFORM_BINARY=~/.peri-e01/control-<stamp>/peri-platform \
PERI_RUST_BINARY=$PWD/prototypes/v1/target/debug/peri-loom-v1 \
nohup python3 dev.py --run-dir ~/.peri-e01/stack-<stamp> --listen 127.0.0.1:<port> \
  --tls --no-build --backend local > ~/.peri-e01/control-<stamp>/launcher.log 2>&1 &
echo $! > ~/.peri-e01/control-<stamp>/launcher.pid
# 3) 环境变量从 connection.json 读取（不打印）；数据库身份从其中 database 字段拼 tenant/database/branch
# 4) 停止：kill -TERM $(cat launcher.pid)，然后确认无进程残留；SIGTERM 后 launcher 可能报
#    "without a clean proof"，按仓库约定不得清理 dirty marker，该 run 目录视为已消耗。
```

导入标定（有界，最多 100 万行 / 600 秒）：

```bash
python3 -B .claude/skills/e01-benchmark/scripts/calibrate_platform.py \
  --config <formal-config>.json --out-dir <new-dir> --database <tenant/db/branch> \
  --batch-size 250 --commit-rows 10000 --max-seconds 240 --samples 8 \
  --reset-fixture --storage-dir ~/.peri-e01/stack-<stamp>
```

期望清单（离线，一次流式扫描；可独立重算与防篡改）：

```bash
python3 -B .claude/skills/e01-benchmark/scripts/e01.py manifest \
  --config <config>.json --per-family 8 --depth-limit 100000 --output <manifest>.json
python3 -B .claude/skills/e01-benchmark/scripts/e01.py manifest \
  --config <config>.json --verify-manifest <manifest>.json --output <verify-output>.json
```

深度预算不足时**构建阶段直接拒绝**（`manifest_depth_budget_exceeded`），不产出半成品清单；
清单内每个操作都有期望行，运行期不再扫描全量，也没有 `not_checked` 兜底。

查询测量（清单驱动、只读、有界）：

```bash
python3 -B .claude/skills/e01-benchmark/scripts/run_platform_queries.py \
  --config <config>.json --manifest <manifest>.json --out-dir <new-dir> \
  --database <tenant/db/branch> [--concurrency 100] [--rate 0] [--max-seconds 120]
```

**并发默认 100**，依据是 PostgreSQL 官方文档给出的典型连接规模
（`max_connections` 通常 100，用户 2026-10-16 已核对该证据并批准；见
`references/approval.md`）。100 表示**真实独立会话数**：每个会话由唯一 worker 线程串行独占、
结束后显式关闭，不使用多线程共享单会话；`--concurrency 1` 仍可用于单并发对照。
该数字**不等价于 active queries，也不是 pgbench 默认值**（pgbench 客户端默认 1）。
会话未全部准入时如实报告 `sessions_requested`/`sessions_opened`/`peak_in_flight`/`open_failures`
与 `partial_admission`，不把它当作"已测 100"；请求数少于并发时只开与请求数相同的会话。

硬预算：并发 ≤256、总时长 ≤600 s、请求数 ≤`--request-budget`；速率为**尽力整形**
（非高精度开环），派发滞后逐条记录可核对。输出 `ORACLE_VERIFIED` / `INCOMPLETE` /
`FAIL_STOP` / `NOT MEASURED`，每族成功 ops/s（分母固定为查询阶段墙钟）、
`service_ms`/`end_to_end_ms`/`dispatch_lag_ms` 的样本量与 p50/p95/p99（样本不足标
`sufficient=false` 且值为 null）、成功/拒绝/超时/错误/Unknown 分类。
`timing` 为互斥阶段计时：各阶段由同一计时器累加、互不重叠，并给出 `unaccounted_ms`；
**不得把阶段相加当作总时长**。`resources`/`cpu_seconds_per_operation` 只有操作者提供
`--resource-json` 时才计算，否则为 NOT MEASURED（不填 0）。`performance`/`capacity`
恒为 `NOT MEASURED`；`scale_coverage` 仅在同一次 `count(*)` 回读相符时为 `VERIFIED_BY_COUNT`；
未获批 SLO 不给 PASS。

对比产物（真正可运行）：

```bash
python3 -B .claude/skills/e01-benchmark/scripts/e01.py compare \
  --base <baseline-queries.json> --candidate <candidate-queries.json> --output <compare.json>
```

先验证模型版本、config 摘要、清单摘要、绑定、适配器、计时边界与命中/准入情况：
不匹配 → `NOT_COMPARABLE`（不给倍数）；缺字段/部分准入/存在 Unknown → `EXPLORATION_ONLY`
（同样不给倍数）；跨目标（原生嵌入式 vs 平台 E2E）恒标 `cross_boundary`，
说明差值含协议/会话/网络/审计/部署边界，**不是纯引擎收益**；缺目标 → `NOT_MEASURED`。

正式导入（8M/16M/32M，全量不截断；本技能不删除既有对象）：

```bash
python3 -B .claude/skills/e01-benchmark/scripts/run_platform_import.py \
  --config <config>.json --out-dir <new-dir> --database <tenant/db/branch> \
  --batch-size 250 --commit-rows 10000 --max-seconds <budget> \
  --min-free-disk-bytes <budget> --storage-dir <自有运行目录>
```

计数分列 `planned_rows`/`attempted_rows`/`insert_ack_rows`/`commit_ack_rows`/`readback_rows`；
单语句超过实测上界 61,257 B、磁盘余量低于预算、时间到界都会停止；失败与 Unknown 停止后
**不自动恢复、不重放、不猜测回滚**。**8M 及以上的真实导入与查询均未执行**；
≤1M 的标定证据见 `references/approval.md`（含各计数的精确定义与撤回的预算）。

本技能**不删除任何对象**：没有 `DROP`、没有 `--reset-fixture`、没有 `IF NOT EXISTS`。
`CREATE TABLE`/`CREATE INDEX` 遇到既有对象即失败退出（fail closed），污染不会被掩盖；
需要干净 fixture 时由用户提供新分支或新库。

工具测试（低成本，不需要生产环境）：

```bash
python3 -B -m unittest discover -s .claude/skills/e01-benchmark/tests -v
```

## 正确性与证据

- 固定版本与 seed；SHA-256 域分离生成分组、长度、时间与查询参数，SHAKE-256 生成正文；
  不依赖运行时随机数、时钟或真实用户内容。整数保持 JSON 原生整数字面量。
- oracle 在本地对逻辑行过滤排序，不执行被测 SQL；核验身份、全部列值、顺序与边界。
  返回行形状异常记为 mismatch，不跳过、不降级。
- 完整分页：对每个组分别做 OFFSET 与 keyset 的首页→空尾页遍历，拼接必须等于该组全序，
  且不得重复或缺失。`report.traversal[*].verdict == "complete"` 才是通过。
- 计时从请求发出到**完整结果接收**；连接/会话策略固定（每轮一个会话，先 open 后 close）。
  SQL 只派发一次，绝不自动重试；Unknown 后会话停用，剩余操作计为缺失（`INCOMPLETE`）。
- 失败分类：`success` / `rejected` / `timeout` / `known_failure` / `unknown` / `not_dispatched`
  分别计数；拒绝、超时与 Unknown 不计入成功吞吐，也不改写为成功或取消。
- 每轮保留原始 JSON 与 SHA-256，按目标/阶梯/轮次隔离；不与 SQL 64 MiB 正确性门禁结果混用。
- 报告字段 `evidence_trust` 只声明"目标自报行经本地 oracle 比对"，
  不声称独立采集链或平台内部可观测性。

## 停止规则

正确性不符、分页缺失或重复、隔离/身份变化、凭据或端点不合规、预算硬限、
发生器饱和、持续积压或 SLO 超限：停止该批并保留证据，不自动升档、不重放写入。
禁止全局 cache 清理、外部 fixture 与隐含费用。正式规模与 SLO 必须先获用户批准。

## 文档

- [审批与决策记录](references/approval.md)：已批准 DDL、纠正、证据与剩余阻塞。
- [合成正式规模提案](references/synthetic-scale.md)：待确认的 N/payload/预算/SLO 探索口径。
