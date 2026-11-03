# v1.2.5 轮次记录：G0/G1 正确性前提与 D1/D2 归因（2026-10-30）

基线 v1.2.0（`34d2810`）。本轮按
`/Users/mino/Desktop/peri-loom-performance-directions.md` 的第一批方向执行：
先补正确性与测量前提（G0、G1），再对 D1（Worker 同库闸门持有范围）与 D2
（Go 剩余分配成本）做归因；两个候选都按方向文档的停止规则在测量后停止，
本轮没有把低于门槛的改动合入。

## 环境与可复现基线

- 运行镜像 `ghcr.io/konghayao/peri-loom@sha256:de8b50e2…`（v1.2.0，本地
  arm64 manifest `d4f6f0a7…`），容器默认 `audit_file_v1`、默认配额档之外只
  调整实验配额（见下）。
- 二进制（sha256）：
  - 平台 `9085f03554bc81e1469e441f23c3c31a0601675b71d65b752b24f907861635e0`
    （v1.2.0 镜像自带，与本轮发布候选相同：本轮没有改 Go 平台产品代码）。
  - worker 基线 `89580e1c6825e0cde57ed94e30d1d3dab015e395ce2cdd09ab15b53403b19727`
    （v1.2.0 镜像自带）。
  - worker 候选 `7fe99014ede9edeafefffde927f1f55fd4dcf201eeb46e7ba6d3cb2c6985eea3`
    （`main` 的 G0 修复 + v1.2.5 版本号，rust:1.98-bookworm、`cargo build --locked --release`；
    与仅含 G0 的 `3a12c667…` 只差 `CARGO_PKG_VERSION` 写入的退出快照）。
  - 归因探针 worker `29fe45d48f7dddb3042eebf6d797d252c709034a35038cd39bbdb68af5c76b76`
    （`probe/gate-attribution` / `1679f85`，**不合入**）。
- 数据集（每次全新卷导入，`/tmp/import-threads-loom-126.mjs`）：
  `verify machines: 1`、`verify projects: 39`、`verify workspaces: 46`、
  `verify threads: 201`、`verify messages: 8625`。
- 实验配额（每个新卷导入后通过 `POST /v1/projects/quotas` 设置）：
  `max_databases=8`、`max_sessions=1024`、`max_concurrent=1024`、
  `requests_per_second=100000`、`max_sql_bytes=65536`、`max_result_bytes=4194304`。
- 容器：`--network host --init --stop-timeout 60 --security-opt no-new-privileges
  --cap-drop ALL`，`-e PERI_HRANA_EXPERIMENTAL=1`，未设置
  `PERI_COMPLETION_JOURNAL`（默认 legacy v1）。

## G0 刷新闸门过期等待者的唤醒交接

缺陷（`worker_rpc/sessions.rs` 的 `lock_until`）：等待者被 `notify_one` 唤醒后，
若重新取得 mutex 时已越过自己的 deadline，旧代码直接返回 `Deadline` 而不通知下
一个睡眠者。此时闸门已空闲，后续等待者却可能一直睡到下一次 release。

修复：新增 `expire()`，只在持锁期间观察到 gate 空闲时把唤醒转交给下一位，再把
`Deadline` 返回给过期调用者；超时不进入临界区的语义不变。测试
`an_expired_waiter_hands_its_wakeup_to_the_next_sleeper` 用确定性交错复现
（先 notify，再持锁 250ms 越过短等待者 deadline），8 次尝试。

验证：

- 负向对照：把修复回退后同一测试稳定失败（`Error: timed out waiting on
  channel`）；恢复修复后 8/8 通过。
- `cargo clippy --locked --all-targets --no-deps -- -D warnings`：退出码 0。
- `cargo test --locked --lib`：**675 passed / 0 failed / 10 ignored**（88.3s）。
- `cargo fmt --check` 在未改动的 pristine `sessions.rs` 上也报差异（既有测试的
  格式问题），本轮只对触碰过的文件做了 `cargo fmt`。

## G1 Issue28 高并发验收的判定缺口

缺陷：`issue28/report.py:155` 的 `_concurrency_verdict` 只看 429、资源未归零与
rejected 计数——传入 100 个 HTTP 500、100 个 SQL error 或 100 个 transport
error，在那些计数为零时仍返回 PASS；`run.py:136` 的退出判定不看并发 verdict；
`runner.py:314` 每并发档固定 100 次且异常只进 warnings。

修复（`fix/issue28-verdict` / `f42cfeb`，merge `ef2502f`）：

- 异常路径同样生成 `Measurement`（`driver_error` / `transport_error`）并进入该
  档账本，不再只写 warnings。
- verdict 检查每条记录的 status/outcome、账本请求数与计划数（新增
  `--concurrency-requests`，默认 100）；缺测、HTTP/SQL/解码/传输错误、未归零
  资源、并发 FAIL 都阻断正式 PASS。
- 并发 FAIL 时退出码 1（即使带 `--allow-misses`），fatal 仍为 2。
- 新增 6 类离线回归测试。

验证：`python3.12 -m unittest discover -s scripts/perf-reliability -p
'test_issue28*.py'` → **32 tests OK**；`scripts/perf-reliability` 全部 21 个
`test_*.py` 逐文件运行 → **330 tests，全部 OK**（含 `test_gate.py` 64 项、
`test_ci_entrypoint.py` 76 项）。
（宿主 `python3` 为 3.9，`hashlib.file_digest` 等 API 不可用，须用 3.12+。）

## D1 同库刷新闸门归因（候选停止）

方法：在 `probe/gate-attribution` 上给 `RefreshGate` 加站点级探针（`PERI_GATE_PROBE=1`），
记录 wait、hold、`open` 分段（image-check / open-session / prepare / registry）与
`close` 分段（native / remove），以及 open/begin/close 命令总时长；每 5s 向 worker
stderr 输出快照。关闭时每次 span 只有一次原子读。为让探针与实验会话上限真正到达
worker，探针运行挂载了一份只在 `safe_environment` 里转发 `PERI_GATE_PROBE` 与
`PERI_WORKER_MAX_DB_SESSIONS` 的 `dev.py`（sha256 `a38f6c67…`，**不随产品代码提交**）。

负载：单库热数据集，`connect→all→close` 小查询，档位 c1/c8/c16/c32/c64
（1,200/2,400/2,400/3,840/5,120 次查询，0 失败）。

| 档位 | qps | p50 | p95 |
|---|---:|---:|---:|
| c1 | 196.3 | 4.53ms | 7.89ms |
| c8 | 831.5 | 7.66ms | 14.12ms |
| c16 | 750.0 | 11.58ms | 71.31ms |
| c32 | 595.6 | 21.65ms | 142.39ms |
| c64 | 464.9 | 98.77ms | 418.63ms |

累计分段（14,968 次 open、14,967 次 close、45,759 次 begin；`hold` 含排队）：

| 站点 | 总计 | 其中排队 | 持有 | 持有分段 |
|---|---:|---:|---:|---|
| open | 786.7µs | 517.9µs | 268.8µs | open_session 251.4、prepare 5.7、image_check 0.8、registry 0.3 |
| close | 497.2µs | 387.0µs | 110.2µs | close_native 89.8、close_remove 8.2 |
| begin | 283.6µs | 276.4µs | 7.2µs | — |

低负载（首个快照，420 次 open）：排队 0µs、持有 227µs（open_session 219µs）；
高负载快照里 open 排队升到约 900µs、close 约 585µs。

结论与停止理由：

- 闸内可证明冗余、且不依赖镜像与会话生命周期的准备工作是 `prepare`（只读检查 +
  `/dev/urandom` + ID 格式化 + `Entry::new`），实测 **5.7µs**，占 open 持有
  2.1%、占每查询闸门服务时间（open 268.8 + close 110.2 + begin 7.2 ≈ 386µs）
  **1.5%**，远低于 10% 的验收门槛 → 按方向文档「移出的成本很小，停止该候选」。
- 持有时间的主体是 `runtime.open_session`（97% 的 open 持有）与 native close
  （92% 的 close 持有），移出它们需要先证明代际一致性，属于方向文档中更后一步的
  「热代际快路径 + 替换慢路径」读写协调，本轮不做。
- 排队在高并发下确实显著（open 66%、close 78% 的时延），说明下一轮的机制假设应
  针对「闸门串行 + 每查询 5 次闸门获取」，而不是闸内微秒级工作。

## D2 Go 剩余分配成本归因（候选停止）

方法：`/tmp/peri-d2` 独立 worktree，用测试内镜像的 v1 信封路径与真实
`decodeGatewayRequest`/内部 dispatch 做 `go test -bench -benchmem`，同时跑
Go 1.27 的 json v2 与 `GOEXPERIMENT=nojsonv2`（对齐发布用 Go 1.23 的 json v1）。

| 段（发布对齐的 json v1） | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `callVersionInto`（解码 + 分类 + 信封） | 1,654 | 1,176 | 20 |
| Hrana `json.Marshal(input)` | 640 | 512 | 12 |
| Hrana 内部请求构造（`httptest.NewRequest`） | 851 | 5,616 | 14 |
| gateway `decodeGatewayRequest` | 4,369 | 3,984 | 96 |
| 合计（两候选点） | 7,514 | 11,288 | 142 |

两候选点合计占客户端约 5ms 固定成本的 **0.15%**（json v2 为 0.10%），低于
20µs 绝对门槛；即使整段删除 `callVersionInto` 也只有 1.65µs（0.03%）。逃逸与
分配画像显示最大项是 `map[string]json.RawMessage` 解码与 4KB bufio，均无法在
不改变既有 wire/digest 边界的前提下零风险移除 → 按停止规则停止，未做产品改动。

附带发现（不是本轮结论）：本机 Go 1.27 默认 json v2 下既有 golden
`TestHranaGatewayResponseMatchesEnvelope/html_and_unicode` 失败（`\u2028` 转义
差异），`GOEXPERIMENT=nojsonv2` 通过；发布用 Go 1.23 不受影响。

## 发布级 endpoint A/B（v1.2.0 vs v1.2.5）

见同目录 `release-ab-summary.md`（A/B/B/A 四侧、每侧全新卷、默认审计格式）；
本轮产品差异只有 worker 的 G0 修复，平台二进制逐字节相同。

## 剩余风险

- 归因探针运行修改了 `dev.py` 的环境转发以便让 `PERI_GATE_PROBE` 与
  `PERI_WORKER_MAX_DB_SESSIONS` 到达 worker；该修改只存在于实验挂载，不进入仓库。
  默认部署下每库 16 会话上限没有改动，c32/c64 数据只在实验档可比。
- G0 的验证是锁算法级交错测试（8/8 负向对照），不是完整服务的故障注入。
- D1/D2 的停止不表示没有收益空间：D1 的下一步是「热代际快路径 + 替换慢路径」的
  读写协调，需要先给出代际一致性证明与替换/close/begin/sweep/旧 owner 恢复的
  交错验证。

## 发布级 endpoint A/B（v1.2.0 → v1.2.5）

- A 侧：`peri-loom:v1.2.0` 镜像自带平台 `9085f035…` 与 worker `89580e1c…`。
- B 侧：同镜像，仅把 worker 换成 `7fe99014…`（`v1.2.5` 源码构建，平台二进制不变）。
- 每个侧全新卷，A/B/B/A 两轮；`--rounds 3 --warmup 1 --samples 5`；默认 `audit_file_v1`；
  `max_concurrent=1024`、`requests_per_second=100000`；数据集 8,625 messages / 201 threads / 46 workspaces。
- 原始摘要：`summary-{base,cand}-r{1,2}.json`；逐行对比：`release-ab-compare.txt`。
- 四侧容器内 `/usr/local/bin` 哈希逐一核对：base 两轮 = 平台 `9085f035…` + worker
  `89580e1c…`，cand 两轮 = 平台 `9085f035…` + worker `7fe99014…`。
- 审计不变量：四侧 `events.json` 各 10,605 条，hash/sequence 逐条连续（`verify-audit.py`
  在容器内校验，任一处不符即非零退出），四侧 harness 日志中 `audit_unavailable` 与
  `outcome_unknown` 均为 0。

### 稳定场景（12–270ms）无回归

| 场景 | 协议 | A 中位 (ms) | B 中位 (ms) | 变化 |
|---|---|---:|---:|---:|
| H1.content-20mb | json-cursor | 268.44 | 265.19 | −1.2% |
| H1.content-20mb | protobuf-cursor | 216.75 | 216.51 | −0.1% |
| H2.projection | json-cursor | 32.10 | 31.49 | −1.9% |
| H2.projection | protobuf-cursor | 31.12 | 30.07 | −3.4% |
| H3.content-projection | json-cursor | 269.10 | 265.81 | −1.2% |
| H3.content-projection | protobuf-cursor | 218.00 | 216.25 | −0.8% |
| R1.limit-8625 | json-pipeline | 59.25 | 61.94 | +4.5% |
| R1.limit-5000 | json-pipeline | 60.69 | 61.76 | +1.8% |
| R1.limit-1000 | json-pipeline | 77.87 | 76.56 | −1.7% |
| R1.limit-100 | json-pipeline | 11.98 | 12.40 | +3.5% |
| B1.batch-20 | json-pipeline | 2.87 | 2.47 | −13.9% |
| C1.baton-10 | json-pipeline | 0.46 | 0.49 | +8.3% |

### 亚 5ms 场景：跨侧差异小于同侧重复抖动

`F1/F2/L1/L2/P1` 两侧中位差异在 −23%～+135% 之间，但同一侧两轮的跨度达到 17%～207%
（例：F1 json-pipeline base 4.83/1.75ms、cand 2.26/6.22ms；P1 protobuf-pipeline base
1.35/2.80ms）。该量级低于测试台的可分辨度，不能归因于只改动超时唤醒交接的 worker。
逐行 PASS/FAIL 与细节见 `release-ab-compare.txt`。

### 并发（X1，各级 100 次，逐侧一轮）

| 级别 | A p50 | B p50 | A p95 | B p95 | B 错误/429 |
|---|---:|---:|---:|---:|---:|
| c1 | 1.94 | 1.35 | 3.60 | 4.39 | 0/0 |
| c4 | 4.38 | 4.26 | 10.31 | 30.65 | 0/0 |
| c8 | 6.00 | 4.94 | 11.34 | 14.49 | 0/0 |
| c16 | 8.36 | 7.58 | 36.14 | 19.54 | 0/0 |

c4 的 p95 变化来自 cand-r2 单轮离群（48.23ms；cand-r1 为 13.08ms），同侧 base 两轮为
10.67/9.95ms、c8 中位数同侧跨度为 7.25/4.74ms，同样是测试台噪声主导。四侧全部并发档
verdict PASS、0 error、0 个 429、`active_permits`/`session_seats` 归零。

### 唯一 FAIL 行

`cand-r2` 的 `F1.select-1 / json-pipeline` 触发场景预算 `SQL-only 0.545ms > 0.500ms`；
同一侧另一轮为 0.249ms，base 两轮为 0.310/0.223ms。该行是单场景 SQL-only 预算校验，
不是跨侧比值；出现于整体变慢的那一轮（该轮 F1 客户端中位 6.22ms，base 同侧跨度为
1.75–4.83ms）。按原样记录，不重跑挑选结果。
