# #15 离线阶段与失败账本

`analyze-phases.py` 只读取已经停止或冻结的 ABBA 原始目录，不启动服务、不执行 SQL、不改变客户端。它补充分阶段报告，**不替代 `audit-goal-abba.py`，不计算性能 PASS，不证明持久化或故障恢复通过**。

## 输入与运行

需要 Python 3.11 或更高版本，无第三方依赖。当前明确只支持冻结的 timing schema 2、`@tursodatabase/serverless@1.4.1`、H4/H6、1000 行 fixture、256 字节 INSERT payload，和 A1/B1/B2/A2 的 ABBA 目录；不猜测其他历史 schema 的含义。

```bash
python3 scripts/perf-reliability/issue15/analyze-phases.py \
  /absolute/path/to/abba-verified \
  --out /absolute/path/to/new-phase-report

python3 -m unittest discover \
  -s scripts/perf-reliability/issue15 -p 'test_analyze_phases.py' -v
```

输入须有 `abba-settings.json`、`abba-progress.json`、冻结的 `helpers/`，以及已观察场景的 `client/{run.json,summary.json,operations.jsonl,requests.jsonl,SHA256SUMS.json}`。输出目录必须不存在，且与输入目录不能互相包含。程序以只读方式打开输入，在读取及完成分析后核对 SHA，禁止覆盖旧报告。

退出 0 仅表示生成了有效的离线报告；其中 campaign 可以是 `COMPLETED`、`INTERRUPTED` 或 `INCOMPLETE`。已记录非零退出/客户端 STOPPED 属于 INTERRUPTED；只有部分完整场景但没有明确终止记录的快照属于 INCOMPLETE。两种情况均不补齐缺失臂。输入损坏、身份冲突或不支持的 schema 退出 2；若已创建输出目录，仅留下 `invalid-input.json`，不生成可误用的指标报告。

## 输出

| 文件 | 内容 |
|---|---|
| `report.json` | 每场景、每阶段及每轮 A/B 合并指标；250 样本窗口；全阶段账本；所有读取文件的 SHA；身份与边界声明。 |
| `slow-operations.jsonl` | 所有已观察阶段中业务耗时 >100ms 的操作，含完整动作、可观察客户端边界、精确 request ID 与是否缺阶段。没有择优保留 top-N。 |
| `failed-operations.jsonl` | 所有失败/Unknown 操作记录，包括 timed=0 时的 prelude 失败。setup/cleanup 没有 operation 行，其 HTTP 结果仍进入全阶段账本及原始 SHA 索引。 |
| `README.md` | 本次输出状态和简表，明确性能验收未评估。 |

合并只在同一轮、同一场景的 A1+A2 或 B1+B2 内进行；成员不完整时显式 `members_complete=false` 并列计划/观察数量。没有跨轮、跨 campaign、跨机器或 trace 配置混池。缺少整个 client 的状态为 NOT_OBSERVED，**不能从文件不存在推断该场景绝对没有发 SQL**；总账保留未观察 client 数。

## 计时口径

保留既有 H4 `issued→read SDK return`、H6 `issued→close SDK return` 的业务边界。write SDK 指实际 SDK 调用至返回，包含业务语句和 SDK 自动提交探针；H6 首次写还含实际逻辑会话 open。`issued_to_return_confirmed_ms` 是从操作发起累计至该 action 返回，**不是可再次相加的阶段**。`connect_ms` 仅为对象创建，不是 TCP/TLS 握手。

- `business_success_ms` 只统计证据完整成功的操作；同时必须读取旁边的 issued/failed 账本。
- `attempt_elapsed_ms` 保留所有已观察尝试的经过时间，失败返回不能冒充成功响应延迟。
- `sdk_confirmed_ms` 只包含 request 与 action 都确认成功的调用；即使后续 read/close 失败，该 write ACK 仍保留，但业务结果保持失败。
- SDK attempt elapsed 和三个客户端边界列统计全部已观察调用。未知请求未到 headers 时，相应值为 `null`，分位数使用已有值并显示 `missing` 数量，绝不填 0。
- `client_gaps_ms = 业务经过时间 − 所有实际 action 经过时间之和`，只含实际不重叠动作之间/之前的空隙；不再减 issued-to-write 这类累计值。仅容忍 1e-6ms 浮点误差。
- 分位数按原 nearest-rank 精确计算，不能相加。fetch→headers 包含网络和服务端工作；headers→SDK 包含 body 读取、网络与客户端调度；最长分段不等于已定位 fsync/GC 根因。
- clone 校验、证据写入在业务计时后单列。clone 消费结束不等于原始响应 EOF。实际发起率受 C1/50ms 及校验节奏限制，不是服务容量。

>100ms 的次数、比例、超出 100ms 的累计时间及每个已观察样本的平均超额同时保留。零样本时分位数/比例为 `null`；没有擅自新增通过阈值。原目标仍由原始 Goal 审计判断。

## 失败与身份校验

账本按 setup、prelude、prelude_cleanup、warmup、timed、cleanup 分开，记录计划请求、已观察 HTTP、write ACK、精确读回、close ACK、完整成功、失败/Unknown、429 及“write 已确认但业务失败”。setup/cleanup 原格式没有 SDK operation 记录，相关业务操作数为 `null`，不伪造动作时延。全 campaign 计划数与已观察数分开，未启动/未观察部分不会被合并成成功样本。

校验包括：原 raw 清单 SHA、冻结 helper SHA、SDK/runtime/client 身份一致、唯一 run/op/request ID、动作及阶段序列、sample 索引、同会话 baton 链、请求与动作的精确顺序、固定 SQL 与自动提交探针、严格整数 cursor step 和显式 close ACK、预期递增 INSERT 与 payload SHA/读回、计时等式、无重叠、前一次校验及证据写入的可证下界、无 SQL 重放、summary 与 raw 一致。请求顺序依据原始记录及发起时间，不按 clone 完成时间排序。setup 必须是同会话的五次写入及一次 close 前缀，cleanup 必须关闭对应会话。H6 的 prelude 按实际 H4 持久会话处理，不误认身份冲突。已声明完成但缺臂/缺样本/有 Unknown 的输入直接拒绝；STOPPED 也不允许跳过前置阶段或在失败后继续新操作。

settings 中源码和 binary 身份只作为原始记录保留，**本工具没有重新打开外部源码、二进制、私钥、服务端卷或恢复证明**；不能代替原审计的 quota、clean/joined、MAC、持久化和恢复验证。原 raw 本身不是密码学签名凭证，SHA 用于发现内容变化与支持复算，不证明记录真实可信。Unknown 的提交状态仍未知，不推断回滚、不执行重试或恢复 SQL。

测试使用合成的完整/中断记录，覆盖 write ACK 后 read/close 失败、prelude Unknown 且 timed=0、缺 headers、损坏 SHA、错 ID/序列/时钟/读回/ACK、NaN、缺字段、虚假完成、输出覆盖与输入不变。真实 campaign 的验证结果应单独保存，不混入这些合成测试数据。
