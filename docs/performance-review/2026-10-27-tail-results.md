# 验证与性能结论

日期：2026-10-27。结论：**RAW_EVIDENCE_AUDIT_PASS；protocol_complete=true；goal_latency_criteria_met=false。**

## 身份、环境与验收

- 上轮权威 ZIP SHA-256：`365e4285b053f256ed070e519295c15a8d47f88ead17424286330206296c7c35`。恢复时 1306 个父仓库文件与交付逐字节一致。
- A：`3daef7c3900d497ea704005f7e1d5dbc21c53d78`，上一轮完整候选加双方共有的默认关闭诊断。
- B 测量代码：`c56a6e99da687cb6bf925d74e947e73f5d93f18b`。最终源码仅可能追加本报告等文档；准确差异列于 MANIFEST。
- Turso：`24a3a4609683b73935888f8bc30b47727d100aa8`，未修改。
- Linux x86_64，同一云环境、8 CPU quota、相同 SDK/Node/helper hash、验证 TLS、默认配额。未验证原 issue 的 Apple M4/OrbStack/用户旧卷。
- 两轮各 A1→B1→B2→A2；每臂 H4/H6 各 2000 timed；每场景独立新卷，1000 行 fixture，512 次 prelude、50 warmup、256 字节 payload、C1 串行，实际 issued 间隔至少 50 ms。
- pack/completion v2/writer admission/observer 均 OFF；GODEBUG/GOGC/GOMEMLIMIT/GOMAXPROCS/NODE_OPTIONS 清除。测量期间无编译、测试、并行负载或数据分析，仅轻量进度读取。
- 每轮 pooled B/A p99≤0.8，所有相邻配对 p99 不回退、p50≤1.05，>100 ms 比例严格下降；两场景两轮全部满足才达标。阈值未根据结果调整。
- 计时止于真实 SDK Promise 返回；证据 clone 校验和写文件在计时之后且下个样本之前。close Promise 吞错由独立严格 ACK/null baton 校验兜底。不与上一轮包含证据写入开销的旧口径混合。

## 每轮汇总

| 轮次 | 场景 | p99 A → B（ms） | p99 变化 | p50 A → B（ms） | >100 ms 次数 A → B |
|---|---|---:|---:|---:|---:|
| 1 | H4 | 100.670 → 74.817 | -25.68% | 30.101 → 27.151 | 41/4000 → 23/4000 |
| 1 | H6 | 92.120 → 86.557 | -6.04% | 34.711 → 32.923 | 32/4000 → 26/4000 |
| 2 | H4 | 86.501 → 81.325 | -5.98% | 29.080 → 26.959 | 28/4000 → 21/4000 |
| 2 | H6 | 84.702 → 79.071 | -6.65% | 33.400 → 32.254 | 22/4000 → 14/4000 |

所有 timed 32,000 个及 prelude/warmup 共 40,992 个操作逐个精确核验 ID、值、hash；98,504 个总 HTTP 请求（80,000 timed）成功，16,000 个 timed close ACK。0 Unknown、0 429、0 SQL replay。16 个场景均 clean/joined/exit 0，身份、配额和原始停止证明 hash 通过。完整审计见 `交付ZIP/evidence/current/abba-final/goal-audit.json`。

## 全部实验臂（不隐藏回退）

| 轮 | 臂 | 场景 | n | p50 ms | p95 ms | p99 ms | max ms | >100 ms |
|---|---|---|---:|---:|---:|---:|---:|---:|
| 1 | A1 | H4 | 2000 | 28.813 | 48.050 | 96.843 | 307.664 | 18 |
| 1 | B1 | H4 | 2000 | 27.111 | 40.287 | 74.817 | 304.569 | 11 |
| 1 | B2 | H4 | 2000 | 27.168 | 39.646 | 74.623 | 218.765 | 12 |
| 1 | A2 | H4 | 2000 | 31.440 | 53.128 | 105.378 | 326.257 | 23 |
| 1 | A1 | H6 | 2000 | 34.203 | 50.926 | 90.235 | 346.744 | 16 |
| 1 | B1 | H6 | 2000 | 33.467 | 51.210 | 90.061 | 259.420 | 15 |
| 1 | B2 | H6 | 2000 | 32.294 | 52.075 | 82.705 | 247.747 | 11 |
| 1 | A2 | H6 | 2000 | 35.279 | 59.397 | 94.263 | 338.751 | 16 |
| 2 | A1 | H4 | 2000 | 29.320 | 47.973 | 89.761 | 332.544 | 16 |
| 2 | B1 | H4 | 2000 | 26.270 | 39.044 | 74.632 | 235.288 | 9 |
| 2 | B2 | H4 | 2000 | 27.706 | 43.219 | 85.049 | 897.785 | 12 |
| 2 | A2 | H4 | 2000 | 28.851 | 45.163 | 84.049 | 271.569 | 12 |
| 2 | A1 | H6 | 2000 | 33.198 | 49.160 | 85.392 | 262.479 | 11 |
| 2 | B1 | H6 | 2000 | 32.679 | 53.143 | 78.918 | 385.825 | 6 |
| 2 | B2 | H6 | 2000 | 31.858 | 49.165 | 79.695 | 251.904 | 8 |
| 2 | A2 | H6 | 2000 | 33.500 | 47.269 | 83.903 | 259.411 | 11 |

## 相邻配对

| 轮 | 场景 | A/B 配对 | p99 变化 | p50 变化 |
|---|---|---|---:|---:|
| 1 | H4 | A1/B1 | -22.74% | -5.91% |
| 1 | H4 | A2/B2 | -29.18% | -13.59% |
| 1 | H6 | A1/B1 | -0.19% | -2.15% |
| 1 | H6 | A2/B2 | -12.26% | -8.46% |
| 2 | H4 | A1/B1 | -16.85% | -10.40% |
| 2 | H4 | A2/B2 | +1.19% | -3.97% |
| 2 | H6 | A1/B1 | -7.58% | -1.56% |
| 2 | H6 | A2/B2 | -5.01% | -4.90% |

第二轮 H4 A2/B2 p99 +1.19%，违反“稳定、不回退”。最大候选尾点 R2/B2/H4 index 965 为 897.785 ms（write 740.571 ms、read 157.151 ms），高于两轮 A 最大值；保留原样，不归为无证据的宿主噪声。B 的 84 个 >100 ms 样本最长 action 均为 write；A 的 123 个中 120 为 write、3 为 read。这只是 SDK action 边界，不能证明 server 内部或 fsync 因果。

## 不确定性

按臂 circular moving-block bootstrap，block=50、1000 次、固定 seed，以下为 pooled p99 B/A 的 95% 区间：

| 轮 | 场景 | 比值区间 |
|---|---|---:|
| 1 | H4 | 0.629–0.904 |
| 1 | H6 | 0.800–1.057 |
| 2 | H4 | 0.796–1.047 |
| 2 | H6 | 0.868–0.990 |

这些区间仅提示序列不确定性，不是随机对照试验的因果证明，也不保证降低20%。独立 reviewer 的 block=25/100 敏感性结果（每种500次）另存 reviews；不按有利区间选结论。每臂250样本窗口全部保留。

## 慢请求归因与仍未解决部分

独立 trace ON 的 A/B 各400 timed/场景显示：基线每65写 receipt roll 对应12个最慢样本；精确 Authority request ID、pid/publication ID 将 roll、root、group关联。Gateway→Authority 仍有跨进程时间窗口推断，报告明确区分。

请求内 decoded cache 使 roll 解码页中位 H4 767.5→99.5、H6 780.5→102，约减少87%；root mutate 中位减少约28%/26%。但基线 group 持久化占 root约64%–80%，这部分并未因缓存消失。大组1336次写入中1334个unique，简单去重不构成大幅收益来源。

短诊断本身并未改善整体：H4 p99 73.499→74.120 ms（+0.84%），p50 +6.93%；H6 p99 76.914→74.866 ms（−2.66%），p50 +3.73%，>100 ms 0→2。这些不利结果保留，不能用后续正式结果抹去。

- 候选 H6 index133：总145.797 ms，write127.478 ms，Authority group110.958 ms、单body fsync wall104.199 ms，audit1.211 ms。定位到同步调用的墙钟区间，但未证明设备、内核、调度或底层云存储中的具体成因。
- 候选 H6 index157：总135.667 ms，close107.969 ms；HTTP handler仅1.527 ms，headers约4.943 ms，SDK之后仍等约103 ms。不能把它归因于audit或fsync；缺少客户端event-loop、GC、实际response read、内核socket与宿主调度的同时间轴证据。
- H4 index198：HTTP outer与内部覆盖之间约26.949 ms未归因；保留adapter/调度盲区。
- 正式 observer OFF 的尾点只有客户端阶段，不能把400次诊断的原因外推为897.785 ms尾点的原因。该尾点仍未解决。
- cgroup采样未记录throttle增量不等于排除宿主竞争；/proc进程CPU/schedstat/io与pressure不可读，缺失值没有当成零。Go GC有生命周期遥测，但runtime相对时钟不足以逐请求精确归因；没有据此宣称GC是根因。

已补跑同样2000采样深度诊断（见下文）；原M4/OrbStack环境仍需复测，并为极慢样本同时记录客户端event-loop/GC/response消费、Go adapter子阶段、每body fsync与rename/目录sync、OS调度/块设备延迟和虚拟化宿主事件。若要减少receipt/pin registry扇出或改变持久化布局，须先证明旧View、alias删除、held generation、GC与恢复验证不变，再单独实验。当前证据不支持安全地省掉必要屏障或通过提前ACK获得20%收益。

## 正确性与安全验证

| 验证 | 结果与限制 |
|---|---|
| Rust release all-targets | 57 targets：1085 passed、0 failed、26 opt-in ignored；8项新增缓存测试全通过。嵌套子进程不重复计数。 |
| Rust fmt / Clippy | exit0；Clippy --all-targets --no-deps -D warnings。固定上游Turso有已存在编译警告，未修改上游。 |
| Go 全包 | 初次572顶层+747子测试通过，1项因缺TS dist失败；生成dist后该项单独复测通过。合计573顶层覆盖；不是声称全包重新跑一次全绿。5项内置skip，另显式省略AF_UNIX相关TestOwnedStopProofDefaultCLINativeResponseLoss。 |
| 审计/completion race | 26顶层+41子测试通过，含head/floor真实rename失败窗口、下一批阻塞、Close join、v2隔离。 |
| HTTP observation / Node | Go observer普通与race通过；Node严格Observer12项通过；Python新增whitelist测试通过。 |
| TypeScript | 构建成功，30项测试通过。 |
| 原P0 gate | exit0，42 Rust +15固定原生SQL smoke通过；Rust1.88、64MiB预算、snapshot-mode=no。不是全量Turso SQL corpus。 |
| SDK | 原用例52 PASS、0 FAIL、2已知E1 skip；哈希重算一致，TLS校验开启。TCP-only wrapper不是原完整release-gate。 |
| 正常重启 | H4/H6各175 ACK，共350/350精确恢复、0差异；same volume/cache graceful restart，不宣称断电或冷缓存验证。 |

初始编译目录损坏、Go VCS stamping错误、一次race命令拼写错误、依赖构建缺失、Python环境/旧mock失败和会话中断日志全部保留。Rust改用/tmp构建、Go两臂统一-buildvcs=false，最终成功产物才用于测量。完整Python observer测试不是全绿，已明确保留失败；原AF_UNIX环境限制没有伪装为产品通过。

独立审查覆盖持久化/缓存、基准方法、原始数据复算、SDK/恢复、剩余瓶颈和归因边界。未发现本轮代码的P0/P1，不等于已证明性能目标或所有故障情形。参见 `交付ZIP/evidence/current/reviews/`、`correctness-independent-review.md`、`reviews/final-causal-conclusion.md`。

## 交付完整性

所有有效、失败、中断、不利测量和上一轮历史证据均保留。三份补丁用独立Git index做apply --check；源码、固定Turso和测量候选二进制身份核对；复制前后hash不变；实际运行凭据扫描；ZIP CRC及逐entry SHA-256核验。完整私有卷和密钥不随包提供，签名证明只能离线查结构/hash，不能重验MAC。打包验证结果在ZIP旁的verification JSON及包内MANIFEST/SHA256SUMS中。


## 补充完整深度诊断（正式ABBA之后，独立保留）

独立复核发现正式207个>100ms样本中131个（63.29%）在各臂最后500次，仅占样本量25%。因此又补跑 A/B × H4/H6 ×2000 timed，共8000次trace ON，512 prelude/50 warmup/默认配置与冻结脚本保持一致。此轮是顺序诊断，不是新的ABBA，不代替或混入正式验收。

| 场景 | p50 A→B ms | p99 A→B ms | p99变化 | >100 ms A→B | max A→B ms |
|---|---:|---:|---:|---:|---:|
| H4 | 29.427 → 26.460 | 76.276 → 75.588 | -0.90% | 10 → 6 | 279.771 → 295.079 |
| H6 | 36.204 → 33.778 | 97.950 → 85.788 | -12.42% | 20 → 10 | 318.340 → 226.744 |

8000 timed /10248全操作精确读回，24626总HTTP ACK、20000 timed HTTP、4000 timed close；0 Unknown/429/retry，四组clean joined stop。Go phase四文件均complete，accepted与实际条数一致（H4各20525、H6各26672），drop/filter/truncated均0，每个HTTP request_id有精确外层匹配。Rust slow status虽无drop/error/cap，仍标为non_harness complete=false，不能据此声称完整Rust请求账本。

后段退化有直接结构证据：候选receipt roll首500→末500样本窗口中位数如下，均来自实际roll事件与Authority请求关联，而不是只用mod65推断。

| 场景 | 新receipt页数 | 最终group unique对象 | 解码页数 | parsed拒绝次数 | root mutate ms |
|---|---:|---:|---:|---:|---:|
| H4 | 19 → 63 | 88 → 236 | 102.5 → 1115 | 0 → 1047 | 5.847 → 24.061 |
| H6 | 19 → 78.5 | 92 → 281 | 104.5 → 1467 | 11.5 → 1400.5 | 6.516 → 25.853 |

500次之后已观察到的roll请求都发生缓存拒绝，普通请求没有。parsed charge接近4MiB是保守计费预算，不是测得实际RSS；满额后正常回退到原解码/校验路径，保证正确性。缓存无法覆盖后段工作集，与持久化对象数量增长同时存在，解释了为何短工作集收益不能稳定延伸到整轮p99。不能把这类退化简单归为宿主噪声，也不能把所有正式极值直接归为这一个原因。

要扩大有效覆盖，需要验证新的准入/淘汰策略或缓存预算在并发与内存限制下的收益；缓存全命中也不会消除236/281个持久化对象。任何新候选都需重新完整ABBA，不能只挑后续有利样本。完整页增长、阶段与最慢请求证据在 `交付ZIP/evidence/current/rust-tail/diagnostic2000-depth.json` 和独立归因补充报告。

124次实际timed roll均完成内部ID匹配，最后500中的32/32也匹配；独立审核四场景各top20共80条边界。候选H4最大295.079ms样本的group为200.519ms/341个unique对象、单body最大fsync79.296ms；候选H6最大226.744ms样本的group为122.386ms/303个unique、单body最大fsync6.540ms。另一个基线H6最大318.340ms主要落在Go intent191.586ms、terminal49.922ms、audit31.415ms，说明长尾有多条持久化路径，不能用一个storage原因覆盖全部样本。详细证据见 `交付ZIP/evidence/current/rust-tail/diagnostic2000-depth-review.md`。

正式最坏样本R2/B2/H4 #965的write dispatch→headers为699.656ms、headers→SDK返回40.730ms；read分别155.132ms/1.888ms。它仅证明读写都长时间等待，正式测量没有server trace，仍不能区分server计算、锁、存储、网络、客户端或宿主调度。需要在后续复现中捕捉同一极慢请求的跨层时间轴，不能宣称本轮已修复897.785ms尾点。
