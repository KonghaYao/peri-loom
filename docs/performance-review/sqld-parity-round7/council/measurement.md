# 第七轮测量与证据审查

日期：2026-10-29。审查范围：恢复的R6源码、当前main 3703ca696d55e333aff746667fd39125579c4b26、issue15最新十条评论和已接受的sqld追平计划。未启动任何负载、未编译、未修改产品。

## 关键结论

当前可以诚实完成产品机制优化的微基准及明确标注的代理端到端诊断；尚不能重现原六项真实业务，更不能宣布sqld追平。

- issue列出的 /tmp/threads-subset.db、/tmp/real-bench.mjs、/tmp/real-bench.json 是用户测试机器上的路径，不在本轮环境中。
- 对恢复R6和当前main检索 threads-subset、real-bench、history_content、history_projection、threads建表等，未找到对应真实fixture或脚本。
- Library对三个精确文件名检索无结果，并以 threads subset / real bench 简化名称重试仍为空。这里只能说“未定位到可用上传文件”，不能证明文件从未上传或不存在。
- 不能按201 threads、8625 messages、20.2MB等总量自行造数据后标记为真实业务。摘要规模无法恢复索引、SQL、字段分布、行顺序和内容。
- issue的六项为5次平均；不能把这些均值与本轮微基准p99相除，也不能把R6 Linux自身A/B与M4 sqld结果拼接。

## 现有工具可复用之处和缺口

| 入口 | 已有能力 | 本轮限制 |
| --- | --- | --- |
| scripts/perf-reliability/issue11/sdk/runner.mjs | 固定serverless1.4.1；H1/H3/H4/H6；记录HTTP、baton、实际close ACK；无SQL重试 | 使用get/run而非all；没有六项真实查询；连接配置强制/db/tenant/database/branch路径和token |
| 同目录 observe.mjs | 监视实际Hrana cursor/pipeline；捕获SDK close吞错；禁止第二次fetch重试 | 基础版本在await observer.pending之后才结束action计时，包含观察器解析/落盘；不能称为R6最终校正的SDK边界时钟 |
| Python issue11/http/runner.py | 冻结H1至H7 raw合同 | 使用标准库v2 HTTP，不是serverless1.4.1的v3/cursor，与当前客户端不同 |
| tools/sqld-relative-gate/gate.py | 原五场景、至少三轮、pooled p50≤1.5×、错误账检查 | 只消费数据，不启动sqld，不测六业务，不认证p99/容量；不能放宽其输入把本轮代理数据冒充PASS |
| platform/tests/hrana-sdk | 现有Hrana功能矩阵和大结果/错误测试 | package固定@libsql/client0.15.15；该SDK不是issue最新六场景的serverless，不能互换 |

基础Observer并不要求peri专用proof字段，因此协议层并非天然无法对sqld运行；主要限制是配置、场景和时钟。应保留旧runner/旧证据合同，另建有明确schema的新诊断工具，避免直接改旧计时后误比历史结果。

## 本轮可完成的最小测量

1. **机制微基准**：对计划修改的内部dispatch/结果转换路径，固定相同输入和语义输出，统计ns/op、B/op、allocs/op以及实际编码/解析/内部调用次数。覆盖0、1、20行以及256行页和大量窄行。比较修改前后同toolchain二进制，预先固定运行顺序、次数，报告全部轮次。
2. **真实Gateway与fake upstream集成基准**：可以保留Gateway鉴权、配额、错误映射和审计调用接口，使用确定性上游回复隔离Go成本。但不能称为真实Rust/存储/持久化性能，内存fixture不代表磁盘同步成本。
3. **可选真实服务代理端到端诊断**：若根准备好真实Rust+Go和sqld，使用独立新数据库与确定性只读合成fixture，明确标记synthetic-proxy；保持同SDK和完整all/close，测试0/1/20行及8625窄行/长行。它可验证优化对相似形状的影响，不复现用户数据库，不能代替六场景目标或旧gate。
4. **真实业务正式对照**：等待取得原数据及SQL/参数/脚本版本，或一份经授权且校验内容等价的fixture；固定镜像digest和SDK lock、认证/TLS/持久性、资源、fresh/aged条件，再按已接受计划至少三轮交错配对。没有数据时直接标HOLD/NOT_MEASURED。

微基准中的Go testing常规ns/op是迭代均值，不能改名为业务p99。若另采每次时间分布，应明确计时本身的开销；跨机器/不同mock与真实服务不能求倍率。

## 通用只读诊断runner建议

新工具从manifest读取场景name、SQL、参数、expected_row_count、expected_result_sha256、dataset_ref/digest与dataset_origin。它不负责导入生产数据库，也不默认执行建表/写入。目标URL明确分peri完整/db路径和sqld根URL；无鉴权必须显式标记，仅用于声明该配置的sqld轨道。TLS验证不关闭，loopback明文必须显式diagnostic且不作为正式gate。

每个业务严格执行connect→all完整物化→close。捕获SDK返回时刻sdk_resolved_ms和observer证据完成时刻evidence_settled_ms，后者不能挪入业务p99。对真实close ACK分别记录HTTP响应时间、确认成功与SDK close返回；不能因SDK吞错给成功样本。实现中已把JSON解析/证据核验移到业务时钟结束后；clone/tee及正文收集仍可干扰SDK，要两侧一致并标注测量开销。

保留逐次operations与requests、完整请求/结果摘要、版本/锁文件/源码身份、首次失败及未完成账；Unknown、隐藏重试、close失败均停止后续测量，不悄悄重跑。凭据、SQL敏感值和原结果正文不写入公开工件。固定formal_pass=false、recovery_proven=false；结果不能直接送旧sqld gate伪装全场景完成。

建议必测的工具边界：SDK close吞错；第二fetch重试在网络前被阻断；SDK返回与证据结算时间不同；缺行/错值/顺序不一致；不合法URL/重定向/TLS；输出已存在拒绝覆盖；失败样本留存。Mock测试只证明采集器行为，不构成数据库性能结果。

## 本轮实现及验证

根代理授权后新增 `peri-loom-r7/scripts/perf-reliability/sqld-read-diagnostic/`，未修改旧runner、旧gate或产品代码。

- `fixture.mjs`：明确合成的0/1/20/全部窄行/全部文本五个代理形状；默认8625行、2048字节ASCII内容，可用32字节降低本轮seed成本。manifest记录所有完整结果摘要、数据摘要和setup文件摘要。
- `runner.mjs`：固定serverless1.4.1；支持peri与sqld独立URL、显式sqld无认证、逐次all→close、raw、结果摘要、版本身份和输出文件摘要；一旦错误/Unknown/重试/缺ACK/错结果即停。独立setup模式只接受显式授权的合成写入及manifest所匹配文件。
- `evidence.mjs`：客户端SDK时钟与观察器结算分离；所有解析proof和结果摘要在计时后执行；实际closeACK及baton链核验；每个action阻断第二fetch；SDK吞掉隐藏fallback异常后仍强制失败。
- `runner.test.mjs`：13项mock HTTP/真实固定SDK测试全部通过，覆盖两端URL、close吞错/无ACK、错值/错序/缺行、HTTP未知、真实autocommit fallback、时钟边界、typed digest、nearest-rank及fixture确定性/拒绝覆盖。测试首次暴露“HTTP404不会触发该SDK fallback”的假设，改用源码明确的probe step_error触发路径后验证了真实阻断；未把模拟重试误报为真实SDK行为。另验证SDK会接受只有baton头的截断空结果，因此proof额外要求主SQL step0有begin→end，不能仅靠空结果digest判断执行成功。

本轮轻量运行约定由根冻结为每侧每场景200个计时样本、20预热，交错A/S/B和B/S/A；量测只能称代理诊断。N=200的p99是第198条，尾部仅约2条样本，不能证明稳定业务尾延迟或容量。根负责真实服务和全部性能运行，本代理没有自行运行性能负载。

另新增外部 `performance-round7/analyze-comparison.py`，只接受显式列出的正式腿目录和冻结manifest。它先校验全部raw文件SHA、暖身/计时样本N与索引、每条完整结果hash、closeACK、cursor主步骤、NoRetry、请求关联及单调时钟边界；再校验六腿A/S/B/B/S/A、跨run串行区间、同臂全部场景binary/source身份一致及A/B共享native binary一致及SDK/manifest/平台/节拍/超时一致。任一失败则withhold统计，不能静默丢弃样本。每腿和pooled raw分别给业务/read/close的p50/p95/p99/max/>100ms及B/A、B/S倍率，不把阶段分位相加，不平均分位，不用两腿样本伪造正式bootstrap置信区间。`analyze-comparison.test.py`以fabricated raw验证13项边界全部通过，这些记录仅用于采集器测试，不属于数据库性能证据。实际数据须根完成测量后通知才解析。

## 首次完整诊断的产物完整性失败及修复

首次六腿driver退出0、摘要称完成，但冻结分析器发现 `comparison/04-B/proxy_one` 的operations/requests两份JSONL与其自身SHA清单不符。其余23个run全部通过逐条验证；坏run的raw完整停在timed index193，缺194–199的6条业务、12条请求。现存raw总计4794 timed、480 warmup、10548请求、5274条closeACK，不能替代计划4800/480/10560/5280。根独立复核了同样截尾；原因尚未确定。首次comparison判为 **INVALID_ARTIFACT**，保留原始文件、失败分析及调查报告，不输出完整性能统计，也不按摘要重建缺失值。

根随后授权collector存储加固并使用fresh数据库重做全部六腿。collector-only commit `398bfb776cc03ef47842c08bcf929736e76dbcd7` 保留逐条append诊断，同时在内存保留真实operation/request observations；所有业务时钟结束后，从这些原始记录重发完整JSONL，采用临时文件写入→fsync fd→close→atomic rename→fsync目录→字节readback核对。run/summary也采用同样发布；SHA清单最后发布，summary记录原始账N/字节/hash。未改变SDK操作、请求或业务计时，不从summary生成任何观测。原13项回归加2项存储测试共15项通过。新runner SHA为 `50d49f68525a0560810049f072f40c39467f7138e041fbee5b3d2d1f0002eb5b`，新storage模块SHA为 `8512ad8e58f9b4036f8713cbbd23f8ebbe8dc636ef36f87243854880d89fda8c`。

第二次完整运行 `comparison-sealed` 的封存包SHA为 `d73bd693af5241b166a84360bc5fc8409cc2606f9495a49bfb0de027583e13fb`，与completed登记一致；165项封存成员逐字匹配loose文件，含24个读run全部120个证据文件。然而包内自身校验仍失败：`02-S/proxy_twenty` 与 `03-B/proxy_narrow` 各自raw停在timed196，缺197–199的3条业务/6请求；各published_raw登记220/440，实际217/434。22个其他run通过；总量仍为4794 timed/480 warmup/10548请求/5274ACK。第二次也判INVALID_ARTIFACT；封存一致只能证明包与当前文件一致，不能修复内部SHA/N矛盾。故未输出完整per-leg/pooled性能数字。根因仍未确立，前述fsync/atomic方法不能称为已经证明有效的根因修复。独立报告见 `comparison-sealed-verification.json`、`comparison-sealed-analysis.json`、`comparison-sealed-integrity-investigation.json`。

根再授权最后一次独立文件名加固：collector-only commit `c7680fdf338b181594c77792264c08ac1085fd62` 将增量append移到 `operations.partial.jsonl` / `requests.partial.jsonl`，正式raw只在最终首次发布，拒绝覆盖；partial仅供中断诊断，不进入有效业务证据。collector现16项测试通过；外部分析器增加published_raw的N/字节/hash实际一致性，14项测试通过。新runner SHA `1373972b95ff4de71438f61f20edabbf013363a497a481e3e636c29a6d8e5135`，storage SHA `9497b42a46084d287f16b46551498830b1b3cb582243b7e343bce8d114364e01`，analyzer SHA `d19967804d405ea5384f5a0e06198fe234a9f73a9b57ccccebb1638ac5cabc08`。原两轮不改；根计划以fresh DB执行第三个完整六腿，封存前再次核验每个run及封存内部摘要。若仍不能确保完整性，应报告环境阻塞并继续隐藏业务统计，不能再降低校验门槛。

## 业务p99报告契约

第三轮 `comparison-final` 已通过独立完整性审查：封存包SHA `ffb9064800f40f85e415947a938283dedd156d984cb301f1f83f972af83a46e9` 与completed一致，165个成员与loose逐字相同；24run的SHA、published_raw N/bytes/hash和逐条语义/时钟/身份校验全部通过。有效计时4800条、warmup480条、含warmup10560请求及5280closeACK，错误/Unknown/重试/未确认close为0。正式达标、恢复、容量仍false；前两轮无效数据不参与合并。

有效pooled每侧每场景N=400：B的0/1/20行p50分别11.347/12.204/12.245ms，p99分别23.152/26.620/26.436ms；sqld对应p50为4.082/4.202/4.424ms、p99为9.415/8.470/9.075ms，小读未追平。8625窄行B为120.844/166.735ms（p50/p99），sqld为200.450/274.444ms；但相对A的122.551/149.339ms，B虽然中位稍快，p99恶化11.65%，且两配对组都恶化。完整每腿及倍率见 `comparison-final-measurement.md`，机器可读结果见 `comparison-final-analysis.json`。这是一次完整有效代理诊断，不把文件名分离描述为已经证实的环境同步根因解释。

- 场景：说明新建还是复用逻辑会话、读/写/独立读回、是否包括已确认close。逻辑会话不等于新的TCP/TLS。
- 时钟：客户端单调时钟issued→最后必要SDK完成；完整all不能用首字节或首屏替代。ACK验证边界与历史脚本不同则双列，不静默重算旧成绩。
- 数字：每侧每轮N、p50/p95/p99的实际ms、最大值、错误/Unknown/重试/close未确认；超时阈值按场景开测前固定。约1秒的20MB读取不应仅用>100ms作尾部质量指标。
- p99采用nearest rank；N=2000取第1980条。它不是平均、最大、最慢1%的平均，也不能由阶段p99相加或由5次平均推算。
- 1.1×是研发目标；样本/轮次/停止规则/区间算法须预先冻结。点估计达标但不确定区间跨阈值，应报“稳定性未证明”。容量另外测，50ms节拍的约20ops/s不代表最大吞吐。

## 证据定位

- restored/r6/peri-loom-code/source/scripts/perf-reliability/issue11/sdk/config.mjs：URL、token、场景约束。
- 同目录runner.mjs：get/run及H1/H3/H4/H6操作；observe.mjs：真实close ACK、隐藏重试与当前旧边界。
- scripts/perf-reliability/issue11/README-extension-v2.md：明确诊断而非v2精确复现、formal_pass=false。
- tools/sqld-relative-gate/gate.py及docs/performance-review/sqld-1.5x/GATE.md：原五场景和1.5×合同。
- performance-round7/sources/issue15-comments.json：真实六场景仅五次均值与缺失的本地数据/脚本路径。
