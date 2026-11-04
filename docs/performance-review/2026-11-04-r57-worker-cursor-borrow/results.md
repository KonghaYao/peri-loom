# R57 批次结果：R56 + R57 只读 cursor 页链路（批级 A/B、回归门）

判据见同目录 `criteria.txt`（测量前冻结）：把 R56（平台预取 + 按声明长度预分配）与
R57（worker 借用序列化）作为一个发布批次，主指标 H1.content-20mb 的 json-cursor 与
protobuf-cursor；每轮 4 腿 A/B/B/A、每腿 1 warmup + 5 samples；同轮候选腿中位数
≤ 0.90 × 基线腿中位数（≥10%）且两轮同向，两侧同批合入或都不合入。

## 环境

- 两条**全新**栈（新 run dir、新卷、同批重建 fixture），默认配额
  （`max_sessions=16`、`max_concurrent=100`、`requests_per_second=500`、
  `max_result_bytes=4,194,304`、`max_sql_bytes=65,536`）。
- 基线 `127.0.0.1:19641`（`/tmp/peri-perf/run-batch-base`）：
  平台 `/tmp/peri-perf/bin/peri-platform-r56-base2`
  （sha256 `9e003707aeb9ba846f60b7f569a867016226834f2520af68a54ed7b3994c3052`，
  worktree @ `fbc0e3d` 干净树）+ worker
  `/tmp/peri-perf/bin/peri-loom-v1-r53-clean`
  （sha256 `27d8dd4682128ff7668999254e068144c878b5fa972357d7dd22a5aac50ce3a4`）。
- 候选 `127.0.0.1:19642`（`/tmp/peri-perf/run-batch-cand`）：
  平台 `/tmp/peri-perf/bin/peri-platform-r56-cand2`
  （sha256 `611fa2d8455688691cc65739c7d6d2c75156ffa6bf49f2b6dc37a96a94e358ae`，
  `perf/r56-cursor-page-pipeline` @ `20d8db5` 干净树）+ worker
  `/tmp/peri-perf/bin/peri-loom-v1-r57`
  （sha256 `a648ecd9f4a327c41950627c45326f7c3a22b4b4a1b21163f0f56ca2c4be1594`，
  `perf/r57-worker-cursor-borrow` @ `1daea98` 干净树，`cargo build --release --locked`）。
- 工具链：`cargo 1.98.1`（`rust-toolchain.toml` 锁定）、默认 release profile；
  go 平台由 `go build ./cmd/peri-platform` 构建。
- fixture：两侧每条腿的 `fixture` 块一致
  `threads=201 messages=8625 content_chars=20177069 list_threads=40`；
  种子脚本 sha256 `092c2ad78024084cc9364de6b7710276e2fd04a03a552cc61dd1699f86626210`
  （R56 目录 `fixture/seed_fixture.py`）。
- 脚本：`batch-ab.sh`（跑腿）、`batch-ab-summary.py`（按冻结判据汇总）；
  原始数据 `/tmp/peri-perf/batchab`（16 条腿 × `raw.jsonl`/`summary.json`/`summary.md`）。
- 无观测器、无 TLS、localhost；测量窗口内没有并行构建/CI。

## 主指标：逐腿中位数（ms）

| 轮 | 侧 | 腿 | json-cursor | protobuf-cursor |
|---|---|---:|---:|---:|
| 1 | base | 1 / 4 | 61.255 / 60.995 | 58.626 / 61.387 |
| 1 | cand | 2 / 3 | 53.988 / 55.634 | 52.733 / 52.250 |
| 2 | base | 1 / 4 | 65.251 / 63.950 | 60.640 / 61.450 |
| 2 | cand | 2 / 3 | 53.529 / 53.002 | 51.568 / 50.815 |

## 判决（按冻结判据）

| 轮 | 协议 | 基线腿中位数 | 候选腿中位数 | 比值 | 改善 | 判定 |
|---|---|---:|---:|---:|---:|---|
| 1 | json-cursor | 61.125 | 54.811 | 0.8967 | 10.33% | PASS |
| 1 | protobuf-cursor | 60.007 | 52.492 | 0.8748 | 12.52% | PASS |
| 2 | json-cursor | 64.601 | 53.266 | 0.8245 | 17.55% | PASS |
| 2 | protobuf-cursor | 61.045 | 51.192 | 0.8386 | 16.14% | PASS |

- **4/4 格 ≥10% 且同向，批级硬门 PASS**。
- 保守读法用第 1 轮：10.33% / 12.52%，仍高于 10%。第 2 轮基线整体偏慢
  （json 65.251/63.950ms vs 第 1 轮 61.255/60.995ms），候选侧两轮都很稳
  （json 53.002–55.634ms、pb 50.815–52.733ms），因此第 2 轮的 17.55%/16.14% 含宿主漂移，
  不作为收益上界；本轮收益区间按 **10.3%–17.6%** 如实记录。
- 分项归属：R56 单独为 8.99%–9.60%（见 `../2026-11-04-r56-cursor-page-pipeline/results.md`），
  R57 的上限依据为 R53/R55 探针（每页 `json!` 树 36–71µs + `to_vec` 129–206µs，
  34 页/查询 ⇒ 2–4%）。批次实测高于「两者相加」的解释是同一链路上两次改动重叠生效
  （预取把页间等待移出，借用序列化把每页 CPU 压低），不再单独归因到某一半。

## 不变量

- wire 字节与冻结值逐字节一致（16 条腿全部）：
  json `20,608,525`、protobuf `20,272,006`。
- 每条腿 `rows=8625`、`status={"200": 5}`、`outcomes={"ok": 5}`、`warnings=[]`、`fatal=""`；
  平台严格扫描接受每一页；无 `outcome_unknown`/`session_moved`/429。
- `R57GATE` 计数（`gate0.txt`）：json 每查询 34 页 = 1 first + 33 fetch；protobuf 每查询 34 页
  全部经 fetch 形状（native cursor 无融合首页）；单页 `L2.list-empty` fetch=0；
  写 cursor 0 命中。

## 回归门（同两条栈、同批 fixture，4 腿 A/B/B/A、5 samples/腿）

| 场景 / 协议 | base 中位 | cand 中位 | 变化 | 判定 |
|---|---:|---:|---:|---|
| R1.limit-1000 json-pipeline | 19.19ms | 18.50ms | −3.6% | OK |
| R1.limit-1000 protobuf-pipeline | 18.72ms | 19.95ms | **+6.6%** | OK（<10%，最大退化项） |
| F1.select-1 json-pipeline | 0.59ms | 0.56ms | −5.5% | OK |
| F1.select-1 protobuf-pipeline | 0.56ms | 0.53ms | −5.0% | OK |
| L1.list-20 json-pipeline | 0.75ms | 0.78ms | +4.0% | OK |
| L1.list-20 json-cursor | 1.03ms | 0.93ms | −9.9% | OK |
| B1.batch-20 json-pipeline | 4.20ms | 2.42ms | −42.4% | OK（不认领，见下） |
| B1.batch-20 protobuf-pipeline | 6.64ms | 3.31ms | −50.1% | OK（不认领，见下） |
| C1.baton-10 json-pipeline | 0.35ms | 0.35ms | +1.0% | OK |
| H3.content-projection json-cursor | 63.91ms | 53.28ms | −16.6% | OK（同路径证据） |
| H3.content-projection protobuf-cursor | 59.41ms | 52.81ms | −11.1% | OK（同路径证据） |
| X1.concurrency c=1 / c=4 / c=8 / c=16 | 0.51 / 0.75 / 7.52 / 15.42ms | 0.52 / 0.66 / 3.56 / 15.09ms | +2.2% / −12.1% / −52.7% / −2.1% | OK |

- 没有任何一格 >10% 的退化；最大退化是 R1 protobuf **+6.6%**（18.72→19.95ms），
  其余退化都在 ±4% 内。
- B1 的 −42%/−50% 属**小绝对值的快场景**（2.4–6.6ms），单腿抖动可达数十个百分点，
  不作为本轮收益认领；方向与「worker 响应体按声明长度预分配」一致，但没有单独实验支持。
- X1 的 c=8 差异同样落在该场景已知抖动区间（同侧两腿 p50 1.3–12ms），只按「无退化」处理。
- 字节不变量：R1 json `2,369,290` / pb `2,349,053`、H3 json `20,746,565` / pb `20,306,526`、
  F1 json `312` / pb `40`、C1 `300`；B1 json 4,947–4,987（`query_duration_ms` 位数差异）、
  pb `686`；全部 `verdict=PASS`、无 429、无 `warnings`。

## 机制核对

- `git diff --stat fbc0e3d..perf/r56-cursor-page-pipeline`：13 文件、+882/−3，
  代码改动集中在 `prototypes/v1/platform/` 的 6 个文件（cursor 预取、v3 JSON cursor、
  native HTTP cursor、`sql_upstream.go` 的按长度读取）。
- `git diff --stat fbc0e3d..perf/r57-worker-cursor-borrow`：10 文件、+561/−35，
  代码改动集中在 `prototypes/v1/src/worker_rpc/` 的 5 个文件与 2 个测试文件。
- 两侧改动都只落在只读 cursor 页的读取/序列化链上；写 cursor、pipeline、baton 复用的
  代码路径未被修改，回归门中 R1/F1/L1/B1/C1/X1 也没有出现 >10% 的退化。

## 剩余风险与未测量项

- 收益只在 **20MB 单行大结果**（H1/H3）上验证；5KB 级 cursor（L1）测得 −9.9%，
  绝对值 1ms，不作为结论。没有对「宽行 × 大页数」的其他形状做参数化实验。
- 预取每次多读一页（256 行）在内存里短暂并存两份页字节；20MB 场景下未观测到内存异常，
  但没有做长稳压测。写 cursor 明确不预取。
- 两侧二进制均来自宿主 `cargo`/`go` 本地构建（macOS arm64），不是发布镜像构建；
  发布时的工具链一致性与镜像内行为需在发版 A/B 中复测。
- R57 改变了 worker 内部回复的构造方式（`WorkerResult`），对平台是**字节等价**的
  普通 JSON；仍由平台的严格扫描与 16 条腿的字节核对守门。

## 验证（测试与格式）

- `cargo check --locked`、`cargo fmt --check`、
  `cargo clippy --locked --all-targets --no-deps -- -D warnings`：通过（仅既有 turso_core 警告）。
- `cargo test --lib -- worker_rpc`：通过（含两条 pinned 逐字节对拍）。
- 全量 `cargo test --lib --locked`（`ulimit -n 10240`，R57 树）：
  **679 passed / 1 failed / 10 ignored**（76.24s）。失败项
  `authority_rpc::server::batch_tests::authenticated_batches_are_registered_only_by_live_writers`
  单跑 0.63s 通过；同一套件在 **`main` @ `fbc0e3d` 上也失败**（675 passed / 2 failed：
  同一项 + 既有的 `object_store::local::pack::tests::actual_store_lock_serializes_two_independent_processes`），
  因此判定为**既有 flaky**，不是 R57 引入。
- `go test -count=1 .`（`prototypes/v1/platform`，R56 树，
  `PERI_RUST_BINARY=/tmp/peri-perf/bin/peri-loom-v1-r57`）：**ok 176.059s，全绿**
  （此前的既有失败 `TestHranaGatewayResponseMatchesEnvelope/html_and_unicode` 本次未复现）。
- R56 只改 Go、R57 只改 Rust，两套测试分别覆盖各自的改动面；批级 A/B 覆盖两者的端到端交互。
