# Release Simple 压测独立复核

复核时间：2026-10-03。对象：`bench_simple.py`、`bench_lifecycle.py`、`raw/release/results.json`、`raw/release/lifecycle.json` 与 14 份 `latency_*.csv.gz`。本次只读复核，不启动服务或负载。

## 判定：PARTIAL

**数据计算 PASS；SQL 工作负载和实验口径 PARTIAL；distributed serverless 性能结论暂不可据此判定。** 14 档原始延迟样本与 JSON 中的请求数、P50/P95/P99、最大值、均值、RPS 全部一致；读写均记录 0 错误。脚本走真实 SQL API，读为主键点查，写为 INSERT。另有 5 次 Simple 显式停库后首查询样本，均成功。实验仍只覆盖单进程 Simple、很小的数据集，不能外推到 distributed 的热路由、冷启动、缩容/唤醒或架构第 16 章的 8 vCPU/1 GiB 基线。

## 原始数据复算

用独立 Python 读取 gzip CSV，逐档以 `ceil((N-1)×p)` 的秩重新计算分位数；CSV 仅保留 6 位小数，比较容差为 `1e-5 ms`。14 档全部一致；`requests = success + errors`、`rps = requests / duration_s`、`success_rps = success / duration_s` 也全部一致。

| 工作负载 | 档数 | 请求合计 | 错误合计 | 原始 CSV 与 JSON | 最高单档成功 RPS |
| --- | ---: | ---: | ---: | --- | ---: |
| 主键点读 | 8 | 66,931 | 0 | 全部匹配 | 2,747.2（第 2 轮、并发 64） |
| 单行 INSERT | 6 | 5,292 | 0 | 全部匹配 | 256.1（第 1 轮、并发 8） |

`results.json` 中的 SHA-256 与当前 `target/release/peri-loom` 文件一致；该文件的修改时间早于结果文件。记录显示 macOS arm64、18 个逻辑 CPU、48 GiB RAM、Simple / `local_fsync` 模式。Python 环境当前可见 `aiohttp 3.13.5`，与元数据所写版本相同。

## Simple 显式停库后首查询复核

`lifecycle.json` 有 5 个独立库的单次样本，耗时依次为 **21.313、21.375、23.489、25.313、21.262 ms**；范围 **21.262–25.313 ms**，中位数 **21.375 ms**。五次均为 HTTP 200、`rows == [[1]]`、`error == null`，与[实测报告](01_experiment_report.md)所列 21.26–25.31 ms、中位数 21.37 ms 相符。另有单次新 Simple 进程启动至 `/readyz` 的 **306.451 ms**，这是实例启动指标，不能并入停库首查询样本。

脚本对每个库先执行一次 `SELECT 1` 并核验返回值，再调用 `/stop`、等待关联 operation 为 `SUCCEEDED`，之后立即计时并查询。服务端 `DB_STOP` Simple job 的实现先 `close_db` 再将 Catalog 状态降为 COLD；下一次查询通过 `ensure_available` 恢复本地库。因此“**Simple 本地库显式 Stop 完成后的首个 SQL 请求端到端耗时**”是准确口径。脚本未在停库后额外 GET 数据库详情以记录 COLD 状态，原始 JSON 也未保存 stop operation ID/状态；状态确认依赖脚本等待成功和服务端代码，而非独立的状态快照。脚本没有记录本次生命周期实验的二进制哈希。

五个库各一次是受控功能与延迟观测，不能估计可靠的 P95/P99、长期 idle 自动回收、同库并发唤醒或 distributed Worker spawn/远程恢复。实例末尾 RSS **38.906 MiB** 仅是一个时点的进程值。

## 工作负载与校验

| 检查项 | 复核结果 |
| --- | --- |
| 隔离与准备 | 脚本新建临时数据目录、随机空闲端口，启动指定 release 二进制；等 `/readyz` 200、确认部署模式为 `simple`，登录后建库并等操作 `SUCCEEDED`。存在端口绑定释放到服务监听之间的常规竞态；本次运行显然成功越过预检。 |
| 读请求 | 建表 `bench(id INTEGER PRIMARY KEY, v INTEGER)` 并写入 `(1,42)`；压测执行 `SELECT v FROM bench WHERE id = 1`，逐次要求 HTTP 200 且 `rows == [[42]]`。这确实是 SQL 主键点查；数据集仅一行，不能代表 1 GiB 数据集或复杂索引查询。 |
| 写请求 | 每次执行 `INSERT INTO bench (v) VALUES (42)`，逐次要求 HTTP 200 且 JSON 顶层无 `error`；压测后要求 `count(*) == 1 + 成功写请求数 = 5,293`。聚合数量核对有实际价值，但结果文件未保存最终 `count(*)` 响应；不能从原始文件独立重放逐请求成功判定。未在重启后核对持久性，也未用唯一请求 ID 检查单次提交恰好一次。 |
| 请求失败 | HTTP 非 200、读值错误、写响应带 `error`、异常均记入 `errors`，相应延迟仍计入分位数；本次所有档位 0 错误。CSV 只存延迟、不存状态码/成功标志，错误明细仅有最多 5 例，故原始数据不能独立审计完整错误分布。 |
| 延迟定义 | 每个请求从客户端发起到完整 JSON 解析后计时，含 localhost HTTP、认证、执行和客户端解码；不是数据库引擎耗时或平台附加延迟。使用同进程 Python asyncio 闭环固定并发、零思考时间。 |
| 资源测量 | `server_cpu_cores` 是服务进程 CPU 时间差除以阶段总时长；不含客户端 CPU。`server_rss_end_mib` 实际取开始和结束两次 RSS 的较大值，阶段中没有持续采样，因此既不是严格的结束 RSS，也不能保证是真正峰值。 |

## 限制结果判读的关键问题

1. **重复性差异明显。** 读并发 64：第 1 轮 1,214.4 RPS / P95 157.8 ms；第 2 轮 2,747.2 RPS / P95 46.5 ms。读并发 32：1,156.6 与 2,061.6 RPS。写并发 32：172.9 与 251.7 RPS。报告应逐轮列值，或用明确的聚合规则和误差范围；不能只取最佳一轮作为稳定容量。现有数据不能确定变化来自缓存、客户端、后台负载或其他因素。
2. **客户端与服务同机，未记录客户端 CPU/事件循环负荷。** 最高读档服务进程 CPU 仅约 0.88 core；瓶颈位置无法仅凭这些数据断定。闭环负载在延迟增加时自动降速，最高观察 RPS 不等于服务容量上限。
3. **每档只有 8 秒读、5 秒写，且单库、单行读热点。** 这是短时热路径观测；缺少固定到达率、长稳态、更多数据库与真实数据规模。未记录完整执行命令、Git revision、客户端 CPU、系统负载和文件系统条件；二进制哈希可锁定当前文件，但不足以还原构建过程。
4. **serverless 生命周期只测了 Simple 显式 Stop 后首查询。** 没有分离测量 COLD→READY、同库并发唤醒去重、自动空闲回收、崩溃恢复或多库密度。Simple 的 `workers=false`、`per_database_hard_isolation=false` 也不能代表 distributed 路径。

## 可用于报告的结论

可报告“本机 release Simple 模式、单库单行主键热点、短时闭环压测”的各轮实测值及 0 个观测错误，也可报告 5 个 Simple 显式停库后首查询的原始值。可报告写压测结束时脚本的总行数断言通过，但需标注原始计数响应未保存。第 16 章的 distributed 架构目标和其他 serverless 生命周期指标保持“未测”；10,000 RPS、平台附加延迟 3/8 ms、1 GiB 索引点查目标均不得据此判 PASS/FAIL。
