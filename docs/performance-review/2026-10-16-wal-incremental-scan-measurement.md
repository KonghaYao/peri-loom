# 2026-10-16 WAL 增量扫描配对测量

测量对象：commit `f4bcee2`（`perf/wal-incremental-20261016-33a7654d-68f0-4277-9d72-f530ce0e4ba5`，
base `997abbd`）相对同一二进制内 `INCREMENTAL_SCAN=false` 的对照臂。
设计与正确性证据见 [设计文档](./2026-10-16-wal-incremental-validation-design.md)。

## 1. 环境与主机条件（共享、非独占）

- 主机：macOS，18 逻辑核，同一台机器上还有其它 owner 的工作负载，**非独占**。
- 另有一份外力负载在同一窗口内运行：另一 owner 的**主树 Rust 门禁**
  （`cd /Users/konghayao/code/ai/peri-loom && cargo fmt/check/clippy/test`，日志
  `/tmp/peri-b2-rust-gate.log`）在 **21:05:26–21:09:26** 完成，与 release 构建及扫描级测量起始重叠。
- load average（1/5/15 分钟）：release 构建后 7.11/6.73/5.57；扫描级测量 6.86→6.55；
  commit 级（3 轮）6.49→8.82；commit 级（9 轮）期间主机仍有其它进程（结束后 load 1 分钟 8.47）。
- release 构建：`cargo test --release --lib --no-run`，**232 秒**（单独报告，不计入下面的测量预算）。
- 测量命令均为 `CARGO_BUILD_JOBS=2 CARGO_NET_OFFLINE=true`，独立 target，`--release`，`--test-threads=1`。

## 2. 冻结配置与命令

```
# 扫描级：累计 WAL 三档 = 2000 / 16000 / 65536 帧，固定 32 次单帧追加，3 轮交替
PERI_WAL_BENCH_TIERS=2000,16000,65536 PERI_WAL_BENCH_APPENDS=32 PERI_WAL_BENCH_ROUNDS=3 \
  cargo test --release --manifest-path prototypes/v1/Cargo.toml --locked --lib \
  -- --ignored --exact recovery::bench::wal_scan_paired_curve --nocapture --test-threads=1

# commit 级：累计行数三档 = 1000 / 4000 / 12000，固定 16 次单行 INSERT 提交，先 3 轮后补 9 轮
PERI_COMMIT_BENCH_ROWS=1000,4000,12000 PERI_COMMIT_BENCH_COMMITS=16 PERI_COMMIT_BENCH_BATCH=200 \
PERI_COMMIT_BENCH_ROUNDS=3|9 cargo test --release ... \
  -- --ignored --exact authority::store_scaling_tests::commit_incremental_scan_paired_curve \
  --nocapture --test-threads=1
```

两臂在同一二进制、同一夹具字节上交替运行，唯一差异是对照开关；夹具（镜像与追加帧）在计时外生成；
每样本结束断言该臂结论与整幅扫描一致；commit 级另对每档每臂做冷重放校验（两臂 `lsn`/帧数一致）。

## 3. 扫描级结果（32 次追加/样本，release，原样输出见 `/tmp/peri-wal-scan-bench.log`）

| 档（帧） | 镜像字节 | 臂 | 每轮 wall µs（r1/r2/r3） | 中位 µs | 每追加扫描字节 |
| --- | --- | --- | --- | --- | --- |
| 2000 | 1,072,032 | incremental | 138 / 31 / 31 | **31** | 568 |
| 2000 | | whole-image | 6023 / 5506 / 4984 | **5506** | 1,080,876 |
| 16000 | 8,576,032 | incremental | 53 / 10 / 8 | **10** | 568 |
| 16000 | | whole-image | 48775 / 51744 / 50847 | **50847** | 8,584,876 |
| 65536 | 35,127,328 | incremental | 314 / 8 / 8 | **8** | 568 |
| 65536 | | whole-image | 167105 / 174723 / 175476 | **174723** | 35,136,172 |

- 增量臂扫描字节恒为 568 B/追加（header 32 + 1 帧 536），与镜像大小无关；对照臂按镜像线性增长。
- 中位加速比：**178× / 5085× / 21840×**（2000 / 16000 / 65536 帧）。
- 增量臂首轮偏高（138 / 53 / 314 µs）是夹具首次触碰内存的效应，r2/r3 稳定在 31 / 10 / 8 µs；
  对照臂三轮稳定（±5%）。
- 复杂度：`scan_calls` 两臂相同（32）；步进量从 O(镜像) 降为 O(新增帧)。`copied_bytes` 为 0
  （该臂不复制镜像）。

## 4. commit 级结果（16 次单行 INSERT 提交/样本，release）

主结果取 9 轮（原样输出 `/tmp/peri-commit-bench-9.log`），3 轮首跑见 `/tmp/peri-commit-bench.log`。

| 档（行） | 臂 | 每提交中位 µs | 每提交范围 µs | scan_us/16提交 中位 | scanned_bytes/16提交 中位 | copied_bytes/16提交 | gets | groups |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | incremental | 25973 | 8473–36469 | 24 | 132,864 | 7,338,232 | 372 | 16 |
| 1000 | whole-image | 28057 | 10112–39659 | 2064 | 14,833,024 | 7,338,232 | 372 | 16 |
| 4000 | incremental | 15616 | 11116–35443 | 24 | 132,864 | 13,678,912 | 376 | 16 |
| 4000 | whole-image | 18477 | 10988–36342 | 3578 | 27,489,664 | 13,678,912 | 374 | 16 |
| 12000 | incremental | 33829 | 16032–111793 | 32 | 132,864 | 31,015,872 | 387 | 16 |
| 12000 | whole-image | 22692 | 19303–54956 | 9714 | 62,163,584 | 31,015,872 | 389 | 16 |

3 轮首跑（同档同臂）给出另一组中位：1000 档 11617 vs 11243；4000 档 22465 vs 17283；
12000 档 39396 vs 64092 µs——与 9 轮的同臂中位差异可达 2–3 倍。

**机制项（scan）确认、总量（commit wall）不可区分：**

- 机制项差异巨大且方向一致：同一 16 提交样本内 `scan_us` 中位 24–32 µs（增量臂）
  对 2064 / 3578 / 9714 µs（对照臂），即 86× / 149× / 304×；`scanned_bytes` 恒为 132,864 B
  （= 16 提交 × 2 站点 × 568 B + 头）对按镜像增长的 14.8 / 27.5 / 62.2 MB。
- 提交总量**不可区分**：每提交中位差在 1000 档 −7%、4000 档 −15%、12000 档 **+49%**（增量臂更慢），
  方向随档位翻转；每档两臂范围大幅重叠（如 12000 档 16032–111793 µs 对 19303–54956 µs）。
  按机制该项在 12000 档只占提交的约 0.6 ms/23–34 ms ≈ 2%，落在本环境噪声（同臂跨轮 2–7 倍离散）之内。
- 两臂 `copied_bytes`、`gets`、`groups` 完全相同：本改动没有减少镜像复制，也没有新增/减少任何
  durable 写或读——它只减少扫描字节与 CPU。
- 每档两臂冷重放校验一致（`COMMIT_BENCH_VERIFY lsn/frames` 相同），正确性未退化。

## 5. 结论与适用范围

1. **扫描级（成立）**：在 1.07–35.1 MB 合成镜像上，单次追加的扫描从 O(镜像) 降为 O(新增帧)，
   中位 wall 31→5506 / 10→50847 / 8→174723 µs。这是可直接展示的候选突破，规模上限受测试夹具限制。
2. **提交级（未证明）**：在 1000/4000/12000 行、每提交镜像约 0.18/0.58/1.94 MB 的规模上，
   提交总耗时两臂不可区分；机制项被证实存在但占比约 2% 或更低，被其它提交开销
   （整幅镜像复制约 1.7–2.2 MB/提交、WAL index 单文档 O(条目数) 编解码与发布）淹没。
3. **E01（570k 行 / 8M 目标）规模未测**：扫描项随镜像线性增长，在更大镜像上占比会提高，
   但本报告没有任何该规模的提交级或端到端数据，不得据此宣称平台突破。
4. **未测**：Go 平台、全栈、E01 基准、网络与并发；单进程单线程、LocalObjectStore、无 Gateway。
   本环境（共享主机，load 6.5–8.8，另有 owner 的主树门禁并发）也无法分辨个位数百分比的提交级差异。

## 6. 异常与预算

- 异常：测量窗口内有另一 owner 的主树 Rust 门禁（21:05:26–21:09:26）；主机 load 在 commit 级
  测量期间由 6.49 升到 8.82；同臂跨轮离散最高 7 倍（12000 档增量臂 16.0–111.8 ms）。
  以上都记录在案，不影响“提交总量不可区分”的结论方向，但足以说明该环境的分辨率上限。
- 预算：release 构建 232 s（单独报告）；扫描级测量 < 1 s（测试自报 0.81 s）；
  commit 级 3 轮 17.24 s、9 轮 32.41 s；测量总耗时约 1 分钟，远低于 30 分钟预算，未做 Stage 2、
  E01、8M 或全栈。

## 7. 复现

```
cd /Users/konghayao/code/ai/peri-loom-worktrees/wal-incremental-0011ed2f-9c66-4f1f-bc66-d26f691e5c69
CARGO_BUILD_JOBS=2 CARGO_NET_OFFLINE=true cargo test --release --manifest-path prototypes/v1/Cargo.toml \
  --locked --lib --no-run            # 232 s（首次，构建 turso_core release）
# 然后执行第 2 节的两条命令；把 PERI_*_ROUNDS 调大可得更窄的范围
```
