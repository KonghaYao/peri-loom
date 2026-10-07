# 第 2 轮：架构回归完成；30s 饱和矩阵 PARTIAL / UNRESOLVED

## 总结与判定边界

本轮包含两个严格串行、分别授权的实际 runner：

1. **Architecture gate**：`run_62d4554dd9321f12`，exit 0、raw COMPLETE；实际执行的架构 gate 子阶段完成，F 独立验收该子阶段 PASS。`noisy_tenant` C1 明确 SKIP，不能被 summary COMPLETE 掩盖。Timed phases 配置为 W2/S10/M10，均为 gate-only，不是性能 PASS。
2. **satbase 30s sweep**：`run_f75a4caf1b5c8bc0`，exit 1、raw UNRESOLVED，reason `session_cleanup_not_proven`。C1/C2/C4 各完整执行 30s measure；C8 warmup 首个 HTTP429 未通过严格 response-ID/envelope 校验，被分类 unknown，停止并取消已在途请求。该 sweep 及本轮整体覆盖为 **PARTIAL / UNRESOLVED**。

随后计划的 `pointbase`、`mixbase` 未 provision、未运行；common SQL functional matrix 未执行。不得宣称完整压力或性能矩阵 PASS、最大吞吐已测得、unknown 为安全 admission rejection，或修复带来已量化性能提升。

首轮 `session_moved` 失败及本轮首个 429 均保留。工具没有 SQL retry、自动重跑或在失败后追加实际 SQL。后续 header 工具修改仅经过源码/unit gates，未用于任何新实测，不追认先前 unknown 为成功。

## 冻结版本、fixture 与授权

| 项目 | 本轮实际使用身份 |
| --- | --- |
| Source HEAD / joint gate | `ae6f0db9259630b0e87c5c40474c57b26f90581b` |
| Native Release SHA256（与首轮相同） | `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836` |
| Go platform binary | `/tmp/peri-stress-platform-renewal-bin` |
| Go platform SHA256 | `8636dd84d2157e89ad506697efa912269e1ce598a7f8a871152b8bc8c493ac8f` |
| 实测 harness / SHA256 | `/tmp/peri-stress` / `a910edf120e78bb7fb00271e54a7d69b824b4367f113b777ebf7c558838e8613` |
| Joint gate | `/private/tmp/peri-stress-results-h3wtrdm2/joint-pass-renewal.json` |
| 完整 service fixture | `/private/tmp/peri-stress-renewal-ZednExYd` |
| Output directory | `/private/tmp/peri-stress-results-h3wtrdm2` |

主代理在首轮后完成 B 的 bounded renewal routing 修复，报告独立 F normal/race 84 PASS、2 SKIP，并提交上述 HEAD。修复正常 renewal route 及暂不可用时错误永久 mark moved，保留 native fencing，不改变 harness unknown 分类。主代理确认旧 baseline launcher 经 SIGTERM 退出 0，并启动本轮 fresh service fixture、proper CA、19446 Gateway、两个 healthy workers、新 unique S3 bucket。执行者没有再次发信号或改 S3 配置。

Architecture 项目/DB label 为 `rarch`，由主代理 provision PASS，私有 config 为 `stress-rarch.json`。随后 satbase 使用同一 service fixture，但在没有 active runner 时执行一次新 label `satbase` provision，创建独立 project/DB config `stress-satbase.json`；**不是重启完整服务或使用一份全新 worker/cache 历史**。不同项目/DB隔离业务数据，但进程/global metrics 历史仍共享。

两次 run 的 raw source 均保留 `dirty=true`，允许的用户 `CLAUDE.md` dirty 排除在 immutable code 之外。Architecture 的 status SHA 为 `b4efa7568a2f2c5b463d62276d2aeb1538a80ffaa1c3fe5260600043d2f9a742`；satbase 的 status SHA 为 `be6e76ac68a9e5be6f0d557644af5bf137a0a0c4f1702b5e103b2fba44cdd6f8`。两者 tracked diff SHA 均为 `57729f585882d861fdc385cee10f058c4e7cdc0bcd3f7eff0a87fb9095de1848`。不称工作树完全 clean，不把 status hash 当作所有 untracked 内容的证明。satbase provenance 另保留实际 service PID/executable 路径、完整 binary SHA 与精确 argv，不包含 private key。

每个 runner 前确认 HEAD/gate 匹配、config/gate `0600`、fixture/output `0700`、全局串行 lock absent；satbase 前还核对三个二进制 SHA 与 production source 无额外 diff。只有一个 actual runner owner，没有测量中 provision、tests/build 或故障信号。

## 实际执行命令与资源边界

下面是已执行命令的可复现记录，**不是再次执行授权**。

### Architecture gate

```sh
/tmp/peri-stress \
  -connection /private/tmp/peri-stress-renewal-ZednExYd/stress-rarch.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-renewal.json \
  -cases session_cycles,many_db,noisy_tenant,saturation_admission,idle_suspend_resume,graceful_migrate \
  -rows 1000 -concurrency 1,2 -warmup 2s -smoke 10s -measure 10s \
  -offered-rate 50 -max-process-rss-bytes 536870912
```

### satbase provision 与 runner

```sh
python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-renewal-ZednExYd \
  --output /private/tmp/peri-stress-renewal-ZednExYd/stress-satbase.json \
  --label satbase

/tmp/peri-stress \
  -connection /private/tmp/peri-stress-renewal-ZednExYd/stress-satbase.json \
  -output /private/tmp/peri-stress-results-h3wtrdm2 \
  -joint-pass /private/tmp/peri-stress-results-h3wtrdm2/joint-pass-renewal.json \
  -cases saturation_admission -rows 1000 -concurrency 1,2,4,8 \
  -warmup 2s -smoke 10s -measure 30s -offered-rate 200 \
  -max-process-rss-bytes 536870912
```

provision 退出 0；没有重复 mutation。其默认 project quota 由冻结 provisioner 配置，包括 max_concurrent=4；C8 是有意施加 admission 压力，不是假设 8 个请求都获准进入 native engine。

Host raw capacity 为 Darwin/arm64、18 logical CPUs、48 GiB；每 fixture 1000 rows、seed `20261007`，单进程 RSS guard 512 MiB、生成请求预算 256 MiB、artifact 预算 64 MiB。Request timeout 10s、response cap 8 MiB。所有配置、凭据仅从私有文件读取，不在报告复制 credential。

## 观察窗口

| Run | `started_at_observed` | `finished_at_observed` | Monotonic elapsed |
| --- | --- | --- | --- |
| Architecture | `2026-10-07T00:53:33.908736Z` | `2026-10-07T00:56:43.291929Z` | `189382484750` ns = 189.382484750s |
| satbase | `2026-10-07T01:03:07.507694Z` | `2026-10-07T01:05:20.683554Z` | `133175361875` ns = 133.175361875s |

以上是 raw **host wall-clock observed timestamps**，不据此推断日期正确性或用墙钟差计算 latency。耗时与分位数使用 monotonic durations。总 run 窗口包括 setup、oracle、多个阶段，不能当作统一 SQL steady-state 分母。

## Architecture gate：完成范围

Raw 6 个 scenario summaries 均 COMPLETE；21 个 timed phases COMPLETE，另有 noisy C1 SKIP。5 个初始化加 24 个 scenario oracle PASS：1000 accounts、4 ledger rows、余额 `100000004`，expected/actual account hash、ledger hash 均一致。

`7700 offered = 6466 attempted/ACK + 1228 client admission drops + 6 queue expired`。6466 条 operation 均 ACK、correct=true、rows hash 一致；unknown、cleanup unknown、timeouts、correctness failures 为 0。36 session cleanup 正常。非空 RPC ID 唯一，未见 SQL retry。

| 场景 | 实际完成 | 限制 |
| --- | --- | --- |
| `session_cycles` | 生命周期完成，1 后置 oracle PASS | Reopen 不证明 native cache 已清空 |
| `many_db` | C1/C2 各 W2/S10/M10 完成，12 oracle PASS | Independently active DB 为 1/2，不是四 DB 同时活跃 |
| `noisy_tenant` | C1 SKIP；C2 primary/noise isolated + combined 完成，6 oracle PASS | 租户 offered/session 不等，不可据裸 QPS 判干扰 |
| `saturation_admission` | C1/C2 各 W2/S10/M10 完成，3 oracle PASS | 未观察到 server admission rejection，不证明已测饱和点 |
| `idle_suspend_resume` | Suspend/resume convergence 路径完成，1 oracle PASS | 不是 authority outage 或 lease-expiry 测试 |
| `graceful_migrate` | 实际 source worker-b 到 target worker-a 路径完成，1 oracle PASS | 不是 abrupt kill/failover；本次没有同 owner SKIP |

Lifecycle elapsed 分别为 idle `1424387250` ns、migration `1198929125` ns。Convergence 依赖 harness 的实际 poll/分类和成功后 oracle，最终 poll 完整响应未逐条保留；本报告没有再次查询 status。所有 lifecycle `native_empty_cache_proven=false`，不称为 cold recovery PASS。

### Noisy tenant 必须保留的混杂因素

| C2 的 10s measure | Primary offered / ACK | Noise offered / ACK | 每租户 session 数 |
| --- | --- | --- | --- |
| Primary isolated | 500 / 485 | 未运行 | Primary 2 |
| Noise isolated | 未运行 | 500 / 495 | Noise 2 |
| Combined | 250 / 244 | 250 / 245 | 各 1 |

Isolated 每租户 rate 50，combined 总 rate 50、每租户约 25。直接比较每租户 ACK/s 会同时改变 offered rate 和 concurrency，不能把降低解释为 noisy tenant 干扰，也不能从近似总吞吐声称严格隔离。

架构 gate 的每个 timed phase 均 `short_gate_not_benchmark=true`。本次成功支持修复后的被测架构路径能完成，但不倒填首轮路由瞬间的 renewal phase，不能作受控性能提升比较。

## satbase：三个完整 30s measure 与中止

C1/C2/C4 各 W2/S10/M30 的 9 个阶段 COMPLETE。1 preflight 和 3 个 post-concurrency oracle PASS；C8 没有后置 oracle，不补造 PASS。全 raw 分母为：

`25216 offered = 5487 attempted + 19708 client drops + 21 queue expired`

Attempted 中 5479 ACK、8 unknown。所有 ACK 的 actual/expected rows hash 一致。Client drops 是未进入 HTTP/native 的客户端有界排队结果，不能算 server quota 拒绝。

| 完整 measure | Offered | Attempted / ACK | Client drop / expired | Actual elapsed ns | ACK/s，含 drain |
| --- | --- | --- | --- | --- | --- |
| C1 | 6000 | 746 / 746 | 5253 / 1 | `30011052792` | 24.85750850429546 |
| C2 | 6000 | 1517 / 1517 | 4481 / 2 | `30041299875` | 50.497149135095476 |
| C4 | 6000 | 1592 / 1592 | 4405 / 3 | `30062779792` | 52.955848095712255 |

上述三个 measure 是完整 30s 片段，可以报告片段观测；W2/S10 不因 run 配置 M30 而成为完整性能测量。不能据 C2/C4 近似 ACK/s 直接归因于 native engine 瓶颈、服务最大容量或某项修复；客户端 admission、项目 quota、网络/存储链路与顺序 cache 历史都可能影响。

| ACKed HTTP-workflow latency | C1（746 samples） | C2（1517 samples） | C4（1592 samples） |
| --- | --- | --- | --- |
| p50 | 39.109416ms | 36.171208ms | 72.912709ms |
| p95 | 54.200542ms | 63.099167ms | 105.746583ms |
| p99 | 62.204000ms | 79.842250ms | 120.920292ms |
| Max | 99.927250ms | 127.869167ms | 149.764542ms |

这三个完整 measure 没有 workflow failure，因此 `all_workflow_latency` 与成功 ACK 分布一致；不是 native SQL-only latency，也不包含 client queue 的完整 offered-to-response 时间。C8 中止阶段成功与全部分布不是同一集合，不合并进上述表。

### C8 首失败、取消与 cleanup

C8 只开始 warmup：planned 400、offered 16、attempted 11、ACK 3；monotonic elapsed `82003834` ns（约 82ms）后停止。没有 C8 smoke/measure。

首个失败 RPC：`stress_f75a4caf1b5c8bc0_000000030788`，`POST /v1/sql/query`，HTTP429，response 121 bytes，RPC elapsed `58740333` ns，failure `unknown / invalid_envelope`。发起/返回 raw observed timestamp 为 `2026-10-07T01:05:20.62163Z` / `2026-10-07T01:05:20.680371Z`。

工具立即取消该阶段；随后 7 个已经 dispatch 的请求结束为 `request_failed` unknown，属于在途取消完成，不是 7 次重试或恢复运行。合计 8 unknown，8 poisoned sessions 的 cleanup 均 `unresolved_session_not_closed`；没有对不确定 session 再发 SQL/ROLLBACK/close，最终 reason `session_cleanup_not_proven`。Timeouts 与已确认 correctness failures 均为 0。

失败 operation 的 `correct=false`/null-response hash 不证明成功返回了错误数据；它们没有成功 rows 可比对。也不证明所有未知请求未执行或未提交。

Raw 的 `actual_admission_rejection_observed=false` 是严格 envelope 未通过、未进入 admission 分类的结果，**不能抹掉实际 HTTP429**。原始 429 response body 没有 captured，不能声称观测到某个随机 server request ID、JSON code 或安全拒绝保证。

### Request-ID gap：源码与 unit 证据边界

Baseline Gateway 的 early admission reserve 位于读取 body request_id 之前；该路径可使用 server-generated ID 返回429。旧 stress/probe 客户端 public request 没有关联 header，而严格 response-ID 校验在 error classifier 之前执行。这是源码中确定存在的 early request-ID gap，且与本轮 `invalid_envelope` 路径一致。

后续无网络 wire unit 通过构造“请求 ID 与响应 ID 不同的429”复现严格 `invalid_envelope`、unknown poison、禁止后续 SQL/close；构造 correlated quota429则保持现有 known admission 分类。它们确定验证该契约差异的可复现行为，但不是捕获当时原始 response body，不能反推出此次实际 server ID/code。B 的真实 early header capture/server negative gates 由 B/F 另行验收，本报告不代报尚未取得的独立跨层 PASS。

## 全部后续执行状态

| 项目 | 状态 |
| --- | --- |
| Architecture gate 的 C1/C2 被测路径 | COMPLETE；noisy C1 明确 SKIP |
| satbase C1/C2/C4 W2/S10/M30 | COMPLETE，三个有效完整 30s measure 片段 |
| satbase C8 warmup | PARTIAL / UNRESOLVED |
| satbase C8 smoke/measure | NOT EXECUTED |
| pointbase：point_read C1/C4 W2/S10/M30 rate50 | NOT PROVISIONED / NOT EXECUTED |
| mixbase：mixed_oltp C1/C4 W2/S10/M30 rate50 | NOT PROVISIONED / NOT EXECUTED |
| Common functional SQL / faultprobe | NOT EXECUTED |

停止后只读取证与另行授权的工具开发，没有继续已取消的后续 runners。Mixed workload 没有实测，不能报告 write-throughput；未来写入历史增长是时变因素，必须保留 ledger/write counts，不能用不等价写入数量推导性能提升。

## 实际资源与 metrics

| Run / samples | Authority peak RSS | Platform peak RSS | Worker-a / worker-b peak RSS |
| --- | --- | --- | --- |
| Architecture / 184 | 104497152 | 23445504 | 50511872 / 47218688 |
| satbase / 130 | 43024384 | 24100864 | 50741248 / 47104000 |

单位均为 bytes，实际 PID `ps` process RSS，不是 cache payload RSS。样本低于 512MiB guard，stop reason 非 resource abort；离散样本不证明采样间峰值或系统 RAM 硬上限。`ps` CPU 是 OS lifetime/window metric，不是精确 phase CPU percent。

Architecture / satbase generated request charge 分别 `1798231` / `1136643` bytes；两 raw 文件总量分别 `10504725` / `13366015` bytes，均在配置预算内。不将请求记账当作 object-store 存储量。

实际 management/native authority/workers maps 保留于 JSONL。进程/global累计量包含旧活动，不无差分地解释为当前项目独占成本；missing metrics 保持 null/unavailable。顶层 scope/queue-delay缺观测时不编造 0，也不从 reopen 推断空 cache。没有使用合成 metrics 值替代实际观测。

## 产物与独立验证

Output base 为 `/private/tmp/peri-stress-results-h3wtrdm2`：

- Architecture：`run_62d4554dd9321f12/{run.json,operations.jsonl}`，`renewal-architecture-rarch.stdout.log`、`renewal-architecture-rarch.exit-code`（0）。
- satbase：`run_f75a4caf1b5c8bc0/{run.json,operations.jsonl}`，`measure-satbase.provenance.json`、`measure-satbase.provision.stdout.log`、`measure-satbase.provision.exit-code`（0）、`measure-satbase.stdout.log`、`measure-satbase.exit-code`（1）。
- F 的独立只读验收：`docs/experiment-v1-stress/02_verification.md`；architecture gate PASS 与 satbase PARTIAL 分开判定，不将其合并为 full performance PASS。

| Raw SHA256 | run.json | operations.jsonl |
| --- | --- | --- |
| Architecture | `f73be3b91e432b5219e4c720dbe2445a2d12e00ade2948e97edfb8b5b571d59d` | `b706ffaad6d258a46a50c63229999cf5da3d71baef98eb85fc7556719138e44a` |
| satbase | `f3cd45a5dfffeb5dd939ae25989ce488bdf99d64784b176a82ddbd59265ab93e` | `2e499232016162680b2bb392b40b766774235311405f3a3c9d649c6b39efc2f9` |

作者工具 gates 前后校验首轮、architecture、satbase 的全部 6 个 raw 文件摘要不变；F 亦保留独立 before/after 检查。没有覆盖失败证据、重发 SQL 或写入 private config 内容到报告。

## 后续工具修改：不是新增实测

用户在无负载时另行授权仅修改两个 Go stdlib 客户端和新增两个专属 test：

- `prototypes/v1/stress/client.go`：非空 requestID 添加 `X-Request-ID`，值与公开 API body ID 相同；空 ID metrics GET 不添加。
- `prototypes/v1/stress/faultprobe/client.go`：中央 public request 添加 generated ID header，与写入 body 的 ID 相同。
- 两个模块的 `client_header_test.go`：内存 RoundTripper 检查 header/body、唯一 header、各 public path、严格错 ID 的 unknown poison/no retry 和正确 correlated quota 分类，不进行真实 fixture RPC。

Response-ID 校验、classifier、unknown poison、no retry、TLS verify 和 CA loader 均未改变；不触碰 B/C scope、CLAUDE 或 native Rust。作者 focused gates 通过；两个模块 normal/race/vet/build/fmt 通过，parent 为 34 top +24 sub，probe 为 20 top +18 sub，无 skip。这是作者验证，独立 F 的跨层结论另行报告。

保留一个环境失败：最初 parent full normal 在 shell `umask 077` 下，既有 world-readable credential negative fixture 被创建为0600，导致 `TestPrivateConnectionFileAndFixtureBounds` 报“world-readable credential accepted”。未改测试/配置源码；显式测试子进程 `umask 022` 重跑 normal/race 后通过，初始 JSONL 保留。另外 nested focused 初次构建因新 test 使用 `ID` 而结构字段为 `id` 失败，已在冻结前仅改新增 test 字段；最终 focused/full gates均通过。

Exact 4-file manifests：`/tmp/peri-correlation-e-source-before.sha256` 与 `/tmp/peri-correlation-e-source-after.sha256` 一致。可解析日志为 `/tmp/peri-stress-correlation-{normal,normal-standardumask,race}.jsonl`、`/tmp/peri-faultprobe-correlation-{normal,race}.jsonl`，focused/vet logs 同前缀；初始失败不覆盖。

新工具使用各模块 `go build -o <path> ./cmd/<name>` 默认 flags：

| 未实跑新工具 | SHA256 |
| --- | --- |
| `/tmp/peri-stress-correlation` | `c186fd905c34f9d68d2f899e162fdae2bc0539e1a290fffa0cf64341b0e54886` |
| `/tmp/peri-faultprobe-correlation` | `ee89898f9c15355e57990cbb87aad2a4c9439bf5447063a0759e41c68224e892` |

这些工具不是上述两次实验使用的 harness，不形成修复后实际 saturation/SQL PASS。下次执行必须等待主代理冻结并独立验收完整 server/client correlation 契约、匹配新 joint gate、fresh label/provision 与明确 runner 授权；任何 unknown/correctness/resource/cleanup abort 仍停止全部后续，不自动重跑。
