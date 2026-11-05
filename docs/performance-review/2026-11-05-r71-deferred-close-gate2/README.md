# R71 Gate 2：close 异步派发的机制核验与资格比例

日期：2026-11-05
分支：`perf/r71-deferred-session-close` @ `ac5e0c7`（父 `02a05a7`，根 `9e30afa`）
性质：**Gate 2 测量轮**。只构建、启动隔离栈、跑定向工作负载、采集证据；
不修改生产代码，不做冻结 A/B，不推送远端。结论只覆盖下列四条腿和默认配额。

## 结论

**Gate 2 PASS，可进入冻结 A/B。** 20/20 条预登记检查通过（`analysis.json`）。

| 判据 | 实测 | 结果 |
|---|---|---|
| flag 开时 `deferred_session_close` 命中 > 0 | B 腿窗口 `enqueued_total=500` | PASS |
| 目标只读场景资格比例 ≥ 90% | B 腿 **500/500 = 100%** | PASS |
| 写场景 0% 异步 | C 腿 **0/100 = 0%** | PASS |
| flag 关时计数键不存在 | A 腿窗口与整腿 metrics 均无 `deferred_session_close` | PASS |
| close action 延迟下降（方向） | **588.784µs → 95.698µs（−83.75%）** | PASS |
| 无新错误 | 四条腿 `errors_total`/`rejection_reasons_total` 全空，driver 全部 HTTP 200 | PASS |
| 结束状态干净 | 四腿 `session_seats=0`、`active_permits=0`、`pending=0`、`unresolved_total=0`、`unresolved_operations=false` | PASS |
| 干净停止 | 四腿 `stop-exit=0`，launcher outcome `clean`，各 stage exit 0 | PASS |

- B 的 500 个 close 里 **500 个走异步、0 个内联、0 个失败/重试/未决**；
  窗口 `pending_max=2`、每库上限 4，未触顶。
- C 的 100 个写会话 close **全部保持同步**（`enqueued_total=0`），
  `failed_total/retried_total/unresolved_total` 全 0。
- 可选 D（c8）证实每库上限生效：160 次 close 中 **158 异步 + 2 内联**，
  `pending_max=4` 恰为 `per_database_maximum`，无 429/503。

## 环境

| 项 | 值 |
|---|---|
| 宿主 | macOS 26.5.1 / Darwin 25.5.0 arm64；Docker 29.4.0（OrbStack，服务端 aarch64 29.4.0） |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7` |
| 镜像 id / digest | `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff`（id 与 repo digest 相同） |
| worker（四腿相同） | `/tmp/peri-release-ab/bin/peri-loom-v1-v1.2.7`，SHA256 `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 候选 platform | `ac5e0c7` 交叉编译，SHA256 `3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014` |
| 构建命令 | `cd prototypes/v1/platform && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o /tmp/r71-g2/bin/peri-platform-g2 ./cmd/peri-platform` |
| Go | `go1.27.3 darwin/arm64`（`go version`） |
| launcher | 分支 `ac5e0c7` 的 `prototypes/v1/platform/dev.py`，SHA256 `80cd2a3c5ecd01d604861368de9bb208bf792e18eaae22ce8db72854c43c2129`；挂载到 `/app/prototypes/v1/platform/dev.py`，四腿相同 |
| launcher 与镜像差异 | 镜像内 `dev.py` 与挂载文件仅差 `platform_process_environment()` 的 `PERI_DEFERRED_SESSION_CLOSE` 透传（diff 22 行），无其他改动 |
| 耐久模式 | `audit_file_v1`（`/v1/metrics.audit.durability_source`，四腿一致） |
| 配额 | **平台默认，未放宽**：`MaxDatabases=8`、`MaxSessions=16`、`MaxConcurrent=100`、`RequestsPerSecond=500`、`MaxSQLBytes=65536`、`MaxResultBytes=4,194,304`（`management_tenancy.go:defaultQuotas`） |
| 观测器 | **未开启**相位/耐久观测器；只用 `/v1/metrics` 与 driver 自身计时 |
| 数据集 | `docs/performance-review/2026-11-04-v1.2.7-release/seed_fixture.py` 重建的合成 Issue 28 形状；每腿启动后校验 `threads=201 messages=8625 chars=20177069 focus=8625`（`binaries.txt` 末行） |

### 腿、端口、卷

四条腿按 **A → B → C → D** 顺序执行，任一时刻只有一个栈在运行。

| 腿 | flag | 场景 | 容器 | 卷（保留） | 端口 |
|---|---|---|---|---|---|
| A `a-read` | 关（不设置） | F1 select-1，json-pipeline，c1，5×100 | `peri-r71-g2-a-read` | `peri-r71-g2-a-read-data` | 20880 |
| B `b-read` | 开（`=1`） | 同 A | `peri-r71-g2-b-read` | `peri-r71-g2-b-read-data` | 20880 |
| C `c-write` | 开 | W1 insert 形状，json-pipeline，c1，5×20 | `peri-r71-g2-c-write` | `peri-r71-g2-c-write-data` | 20880 |
| D `d-read-c8` | 开 | 同 A，c8，160 次（可选机制检查） | `peri-r71-g2-d-read-c8` | `peri-r71-g2-d-read-c8-data` | 20880 |

- 本轮逐腿单独调用 `./run_gate2.sh <leg>`，前一条腿已 `docker stop` 并删除自建容器后端口释放，
  因此 `leg.txt` 记录的都是 20880；脚本一次跑全部腿时按序分配 20880–20883。
- 只删除脚本自己创建、且已干净停止的腿容器；**四个卷全部保留**。
- 未触碰 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*`；未重启 Docker、未 prune。
  既存的 `peri-r71-diag`（20799）只做了 inspect，未改变其状态。

### flag 到达证据

`legs/*/binaries.txt` 同时记录容器环境与平台进程环境：

- A：`container PERI_DEFERRED_SESSION_CLOSE=<unset>`，`platform process env: unset`。
- B/C/D：`container PERI_DEFERRED_SESSION_CLOSE=1`，`platform process env: PERI_DEFERRED_SESSION_CLOSE=1`
  （从 `/proc/<pid>/environ` 读取 `argv[0]` 为 `/usr/local/bin/peri-platform` 的进程）。

镜像内与容器内 platform/worker 的 SHA256 也与宿主一致（`binaries.txt`）。

## 方法

### 请求形状

- 读腿（A/B/D）：一次请求 =
  `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","want_rows":true}},{"type":"close"}]}`，
  无 baton。即 Issue 28 `F1.select-1` json-pipeline 形状：隐式 open 把语句融合进
  `open_session`，close 条目关闭它自己的会话。每条请求恰好产生 1 个 `open`、
  1 个 `statement`、1 个 `close`（窗口计数逐项核对）。
- 写腿（C）：同样一条 pipeline，但 `execute` 是
  `INSERT INTO "issue28_g2_write_c_write" (payload) VALUES ('issue28')`；
  每条请求的 `affected_row_count` 必须为 1。建表与删表在计时窗口外，也是写会话。

### 计数窗口与资格比例

- driver 先跑 1 个 warmup 块（丢弃），等待 1.2s 补默认准入桶，然后取 `/v1/metrics`
  全量快照 `window-before.json`；跑完计时块后再等 deferred 账户 `pending` 归零，
  取 `window-after.json`。**资格比例只用这两个快照之间的增量**。
- 分子：`deferred_session_close.enqueued_total` 增量。
- 分母：`actions_total.close` 增量（同一窗口），并用 driver 的请求数与
  `actions_total.open`/`statement` 交叉核对。
  B：500 请求 = 500 open = 500 statement = 500 close，`enqueued=500` → **100%**。
  C：100 请求 = 100 open = 100 statement = 100 close，`enqueued=0` → **0%**。
  D：160 请求 = 160 close，`enqueued=158`、`inline=2` → **98.75%**。
- **整腿交叉核对**（含 warmup，不含 seed，因为 seed 在 `metrics-before.json` 之前）：
  A 600 close / 无计数键；B 600 close / 600 enqueued；C 103 close / 0 enqueued；
  D 168 close / 162 enqueued + 6 inline。与窗口数字逐项自洽。

### 干净停止

每腿 `docker stop -t 60`，记录 `stop-exit.txt`；从卷内取 `launcher-stop.json`。
四腿 payload 均为 `outcome=clean`、`reason=terminal_proof_written`、
`unresolved_operations=false`，`platform/worker-a/worker-b/authority` 的 `exit_code` 全为 0。

## 每腿结果

| 腿 | flag | 请求 | 失败 | close 动作 | close 均值 | open 均值 | statement 均值 | 请求 p50 | 请求均值 | wire/请求 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A `a-read` | 关 | 500 | 0 | 500 | **588.784µs** | 609.731µs | 135.202µs | 1.6148ms | 2.1911ms | 314.518B |
| B `b-read` | 开 | 500 | 0 | 500 | **95.698µs** | 699.110µs | 92.968µs | 1.0367ms | 1.9078ms | 314.106B |
| C `c-write` | 开 | 100 | 0 | 100 | 300.769µs | 671.658µs | 23.304ms | 23.184ms | 25.2395ms | 256.170B |
| D `d-read-c8` | 开 | 160 | 0 | 160 | 683.639µs | 2.811ms | 200.335µs | 4.3869ms | 5.3625ms | — |

### `deferred_session_close` 窗口计数

| 腿 | enqueued | inline | failed | retried | unresolved | flushed | pending | pending_max | max / per-db | 资格比例 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| A | 键不存在 | — | — | — | — | — | — | — | — | — |
| B | **500** | **0** | 0 | 0 | 0 | 0 | 0 | 2 | 256 / 4 | **100%** |
| C | **0** | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 256 / 4 | **0%** |
| D | **158** | **2** | 0 | 0 | 0 | 0 | 0 | 4 | 256 / 4 | **98.75%** |

### close 延迟

- 直接判据复用 Gate 1a 的同一口径：`/v1/metrics` 的
  `action_latency_ns_total.close / actions_total.close`。
- A → B：**294.392ms/500 → 47.849ms/500**，均值 **588.784µs → 95.698µs**
  （比值 0.1625，**−83.75%**）。
- **工作没有消失，只是换了归属**：B 的 open 均值从 609.731µs 升到 699.110µs
  （+14.66%），statement 均值从 135.202µs 降到 92.968µs。串行单客户端下，
  下一请求的 open 会与上一会话尚未落地的 worker close 相遇，部分成本转移到这里。
- 端到端（整条 pipeline）方向仍为正：均值 2.1911ms → 1.9078ms（**−12.93%**）、
  p50 1.6148ms → 1.0367ms（**−35.80%**）、p95 5.9804ms → 5.9673ms（−0.22%）。
  本轮的判决只取 close action；端到端数字仅作方向说明，不作为 Gate 2 判据。
- wire 字节：A 157,259、B 157,053（每请求 314.518 → 314.106，−0.13%），
  与 issue28 记录的 F1 参考 312B 同量级；差异来自 `query_duration_ms` 位数，
  响应结构未变。

### 干净度

| 项 | A | B | C | D |
|---|---|---|---|---|
| `session_seats`（窗口后） | 0 | 0 | 0 | 0 |
| `active_permits`（窗口后） | 0 | 0 | 0 | 0 |
| `unknown_held_permits` | 0 | 0 | 0 | 0 |
| `cleanup_ledger.pending_close.count` | 0 | 0 | 0 | 0 |
| `errors_total` 增量 | 空 | 空 | 空 | 空 |
| `rejection_reasons_total` 增量 | 空 | 空 | 空 | 空 |
| driver 非 200 | 0 | 0 | 0 | 0 |
| `audit.dropped_total` | 0 | 0 | 0 | 0 |
| `audit.in_flight_count` | 0 | 0 | 0 | 0 |
| `operation_journal_compaction_failures_total` | 0 | 0 | 0 | 0 |
| `stop-exit.txt` | 0 | 0 | 0 | 0 |

**worker 会话归零的证据链**：平台不暴露 worker 会话计数。可证明的是：
账户只有在 worker 回复 `closed=true` 时才完成一个 job；B 的 `enqueued=500`、
`pending=0`、`failed=0`、`retried=0`、`unresolved=0` 意味着 500 次 close 都在第一次
尝试收到 `closed=true`。此外 500 个新 open 全部成功——若 close 没有落地，worker
每库 16 个会话的上限会在第 17 个请求前拒绝。D 的 `pending_max=4` 与外层上限一致，
`inline=2`（整腿 6）说明触顶时确实回退为同步等待，而不是拒绝。

## 与 Gate 1a / 1b 的口径差异

1. **launcher 来自分支**：Gate 1a/1b 的 A 侧用镜像内 `dev.py`；本轮四腿统一挂载
   `ac5e0c7` 的 `dev.py`（仅多 `PERI_DEFERRED_SESSION_CLOSE` 透传），以证明 flag
   确实进入平台进程。两侧 launcher 完全相同。
2. **close 延迟口径相同**：都取 `/v1/metrics` 的 close action 均值。Gate 1a 的相位
   观测器会改变吞吐，本轮未开启；因此绝对值只能在本窗口内比较。
3. **单对 A/B，无重复**：Gate 2 只判机制与资格，不做冻结 A/B。−83.75% 的效应远大于
   Gate 1b 记录的约 9% 位置/轮次漂移，且 `enqueued=500/500` 是确定性机制证据；
   但本轮不声称已测出稳定的端到端收益。
4. **C 腿的写负载**：用一张表 + 100 次 `INSERT`（W1 insert 形状、每请求独立会话），
   而不是 issue28 的每样本建表/删表；请求形状与 `execute` 写语义相同。
5. **端口**：逐腿单独调用，20880 在前一栈停止并移除后复用；不是并发端口。

## 未测与剩余风险

**未测**：

- L1（列表 20 行）未测。它与 F1 同为「只读 statement + close」形状，机制上应同样
  100% 合格，但本轮没有数据。
- protobuf pipeline、cursor（只读游标按设计不可延迟）、fused close、writer protocol。
- `failed_total` / `retried_total` / `flushed_total` 路径：四条腿均为 0，未制造 worker
  失败、容量拒绝或重试。
- 打开遇 worker 会话上限后的 **flush + 单次重试**：D 的 c8 只到 `pending_max=4`，
  未触达 worker 每库 16 的上限，`flushed_total=0`。
- 优雅关闭时账户非空、崩溃恢复、c16/c64、跨库并发。
- worker 会话计数无公开指标，只能按上面的回复证据链推断。
- 端到端冻结 A/B 与置信区间（下一阶段）。

**风险**：

1. **open 变慢 14.66%**：串行循环里 close 省下的等待有一部分转移到下一次 open。
   下一阶段必须以端到端（p50/均值/吞吐）判决，不能只看 close action；本轮的
   end-to-end p50 −35.80%、均值 −12.93% 只是方向。
2. **内联比例随并发上升**：c8 整腿已有 6/168 次内联（4 次在 warmup、2 次在窗口），
   内联即客户端同步等待。更高并发下的内联比例未测，冻结 A/B 应记录 `inline_total`
   与 p99。
3. **worker 可能比网关多 4 个会话**：网关回复前归还座位，worker 仍持有待关闭会话；
   `max_sessions=16` 时最坏 20。容量拒绝的 flush+retry 是现有安全阀，但本轮未触发。
4. **flag 的语义边界**：只覆盖「整段生命周期可证明只读、autocommit=true、无游标、
   无 unknown、非 writer」的会话；C 腿只证明写会话不进入该账户，不覆盖所有管理面/
   清理路径。
5. **单窗口绝对延迟**：close 588.8µs / 95.7µs 只在本宿主本窗口可比；Gate 1a 的
   882.5µs、Gate 1b 的 671.7µs→89.1µs 来自不同窗口/探针，不能与本轮逐值对齐。

## 证据布局与复现

```
README.md                      本文
run_gate2.sh                   隔离栈 + seed + driver + 快照 + 干净停止（四腿）
gate2_common.py                metrics 快照/差值/百分位
f1_driver.py                   F1 形状读负载（窗口前后快照、raw.jsonl）
write_driver.py                W1 insert 形状写负载
read_concurrency_driver.py     可选 c8 机制检查
analyze_gate2.py               20 条检查、资格比例、close 延迟对比
snap_metrics.py                整腿 /v1/metrics 快照
analysis.json                  汇总判定（20/20 PASS）
legs/<leg>/leg.txt             flag、容器、卷、端口、两侧 SHA
legs/<leg>/binaries.txt        宿主/容器 SHA、镜像 id/digest、flag 到达证据、fixture 校验
legs/<leg>/summary.json        driver 窗口计数与请求统计
legs/<leg>/window-before.json  计时窗口起点 metrics 全量
legs/<leg>/window-after.json   计时窗口终点 metrics 全量
legs/<leg>/metrics-before.json 整腿起点（seed 之后）
legs/<leg>/metrics-settled.json 停止前稳定态
legs/<leg>/metrics-after.json  整腿终点
legs/<leg>/raw.jsonl           每请求：时长、HTTP 状态、响应字节、结果
legs/<leg>/driver.txt          driver 输出（块中位、窗口摘要）
legs/<leg>/container.txt       dev.py 启动行（平台日志在卷内，未逐条审阅）
legs/<leg>/launcher-stop.json  优雅停止证明（outcome clean、unresolved_operations=false）
legs/<leg>/stop-exit.txt       docker stop 后容器退出码
legs/<leg>/seed.txt            fixture 重建与校验输出
```

复现（候选二进制需先按上面的构建命令生成）：

```bash
cd docs/performance-review/2026-11-05-r71-deferred-close-gate2
./run_gate2.sh                     # a-read b-read c-write d-read-c8
./analyze_gate2.py                 # 重新生成 analysis.json 与检查表
```

实际执行命令与关键输出：

```text
$ GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
    -o /tmp/r71-g2/bin/peri-platform-g2 ./cmd/peri-platform
$ shasum -a 256 /tmp/r71-g2/bin/peri-platform-g2
3059c47a116b51ed034621d594f4d5eea34e146937dcbf5f755286d68e6cd014

$ ./run_gate2.sh a-read
== leg a-read flag=off port=20880
leg a-read done driver_exit=0 stop_exit=0 volume=peri-r71-g2-a-read-data
$ ./run_gate2.sh b-read
== leg b-read flag=on port=20880
leg b-read done driver_exit=0 stop_exit=0 volume=peri-r71-g2-b-read-data
$ ./run_gate2.sh c-write
== leg c-write flag=on port=20880
leg c-write done driver_exit=0 stop_exit=0 volume=peri-r71-g2-c-write-data
$ ./run_gate2.sh d-read-c8
== leg d-read-c8 flag=on port=20880
leg d-read-c8 done driver_exit=0 stop_exit=0 volume=peri-r71-g2-d-read-c8-data

$ ./analyze_gate2.py
| leg | flag | kind | requests | failures | close actions | close mean µs | enqueued | inline | failed | unresolved | pending_max | eligibility | seats | permits |
| a-read | off | read | 500 | 0 | 500 | 588.784 | — | — | — | — | — | None | 0 | 0 |
| b-read | on | read | 500 | 0 | 500 | 95.698 | 500 | 0 | 0 | 0 | 2 | 1.0 | 0 | 0 |
| c-write | on | write | 100 | 0 | 100 | 300.769 | 0 | 0 | 0 | 0 | 0 | 0.0 | 0 | 0 |
| d-read-c8 | on | c8 | 160 | 0 | 160 | 683.639 | 158 | 2 | 0 | 0 | 4 | 0.9875 | 0 | 0 |
...
close mean: off=588.784us on=95.698us ratio=0.1625 (-83.75%)
20/20 checks passed
```
