# D05/Q02 冻结功能容量、公平性与 D06 rate 接口

这是一套工具和验收合同，不是实测 PASS。只有 Main 在新自有 fixture 串行运行、保留原始证据并独立复验后，才能接受 D05 或批准 D06 rate。共享 host/container 的结果始终是限定功能容量曲线，`performance_pass=false`、`G02=NOT PASS`、`no_starvation_guarantee=false`。

## Main 入口

从 repo 根目录运行。要求 Python >=3.12；执行阶段复用现有 macOS `run_guarded.py`，不引入第二套 load generator、重试或 PID 清理。容器适配复用原 `provision.py` 的管理确认合同，但用真实 container.proc 身份检查替代 HostPID 检查。`--template`、`--schema`、`--freeze`、`--analyze` 都不发送负载。

```sh
umask 077
python3.12 prototypes/v1/deploy/capacity-matrix.py --template > /private/capacity/matrix-config.json
python3.12 prototypes/v1/deploy/capacity-matrix.py --schema > /private/capacity/result-schema.json

python3.12 prototypes/v1/deploy/capacity-matrix.py --freeze \
  --config /private/capacity/matrix-config.json --plan /private/capacity/matrix-plan.json

python3.12 prototypes/v1/deploy/capacity-matrix.py --next-job \
  --plan /private/capacity/matrix-plan.json

python3.12 prototypes/v1/deploy/capacity-matrix.py --analyze \
  --plan /private/capacity/matrix-plan.json --report /private/capacity/candidate-001.json
```

`/private/capacity` 及所有 connection/gate 文件父目录必须是真实、当前用户拥有的 0700 目录；JSON 是 0600，CLI 是绝对路径、0500、SHA256 冻结。Main 先建空 0700 `config.output`，填完模板占位路径、真实 artifact/source hashes、launcher/services 的完整 `ps` identity 和 P0-P6 joint gate，再 freeze。不要把 credentials 放进 CLI、sources、artifact manifest 或审批 capsule。

Main 批准的压缩覆盖通过 `--next-job` 逐项显式 orchestration，或 Python `run(plan, invoke=Main_owned_callback)`；默认 `--run` 不启动 collector，不能单独作为完整交付运行命令。freeze 生成 63 个基础 guarded jobs / 127 个 measure cells，另预冻最多 16 个条件性 hot knee 扩展 jobs / cells。已经有原始证据的 job 拒绝重跑。第一次 guard/正确性/公平性失败或必要证据未测写 `matrix-stop.json`，保留 exit 和 raw，停止整个批次，不自动继续、重放或清理 Unknown。`--analyze` 在必需覆盖未完成时 exit 1；这不意味着允许把 partial candidate 改写成 PASS。结果文件与 `.sha256.json` 都只新建，不覆盖。root READY 只指 selected coverage 可供独立复验，不是全 Cartesian PASS。

### 显式容器适配与单 job 控制

1. Main 先启动一个新自有、proper-CA、writer-admission/trace 都开启的 Linux fixture。冻结实际 image、source、owner label、container ID、publication port 和五角色；macOS guard 的 launcher 使用实际持续存活的 Main 控制进程，不填入 container PID。只有独立确认的当前联合来源才可签发 joint gate。
2. 建立空 0700 provisioning 输出目录；给 `provision-capacity.py` 提供实际 bootstrap connection、launcher report 形状的 monitor config、私有 0600 monitor 源码及其 SHA256。不带 `--execute` 只校验计划；显式执行会用原管理 API 一次性创建专用项目/数据库、私有保留 reservation 及六种 connection。每次管理调用前后核对实际五角色资源；失败保留 partial、不重放，不对 container PID 调用 host `os.kill`。

   ```sh
   python3.12 prototypes/v1/deploy/provision-capacity.py \
     --connection "$BOOTSTRAP_CONNECTION" --monitor-config "$MONITOR_CONFIG" \
     --monitor-script "$MONITOR_SCRIPT" --monitor-sha256 "$MONITOR_SHA256" \
     --output "$PROVISION_OUTPUT" --label "$NEW_LABEL" --execute
   ```

3. 将六份实际 connection 填入模板并 freeze。`guard.sources` 必须同时绑定 monitor、monitor config、provisioning helper、`run-capacity-job.py` 以及实际 Main 控制程序的源码；不靠启动后的临时 hash 替代事先冻结。输出含 credential 的 connection 只用于私有输入，不发布到公共 evidence。
4. `--next-job` 取得实际 `JOB` 后，仅显式运行下面的单 job。控制工具核对 fresh proc 和 collector-ready，再启动原 guard；运行期间持续检查身份、原始 prefix 和 sidecar 健康。任一失败立即停止自有负载组，保留 Unknown/首次 raw 和 matrix STOP，不自动运行下一项。

   ```sh
   python3.12 prototypes/v1/deploy/run-capacity-job.py \
     --plan "$PLAN" --job "$JOB" \
     --monitor-config "$MONITOR_CONFIG" --monitor-script "$MONITOR_SCRIPT"
   ```

5. 正常结束顺序为 guard 结束、collector 最终采集并 join、monitor 停止并 join 固定 raw、首次 finalize、首次 verify。正常 monitor 最终追加纳入首次 manifest，不以反复 finalize/verify 追写旧证据。核对单 job `capacity-verification.json` 后再取下一项；全批结束另做 `--analyze` 和非作者验收。46 项控制 mock 和12项 provisioning mock 的通过仅批准工具，不是实际容量或 D06 安全档位批准。

### 已批准最小覆盖与时长

| 组 | 冻结组合 | jobs | measure cells |
| --- | --- | ---: | ---: |
| quiet envelope | single_hot / mixed / C1 / 5,10,25 | 3 | 3 |
| hot knee | single_hot / 四 workload / C1,C8 / 25,100,250 | 24 | 24 |
| fairness | equal_hot/equal_many/victim_hot/victim_many / 四 workload / safe=5 与 interference=25；safe hot C2、many C4，interference C8；每点三 phase | 32 | 96 |
| multi DB | single_many / 四 workload / C2 / safe=5 | 4 | 4 |
| 可选 knee 扩展 | 八条 hot 曲线各最多两轮 500,1000，仅所有前点 MEASURED 且 knee NOT FOUND 才执行 | <=16 | <=16 |

正式 measure 不变：基础 **63m30s**；加每 phase 2s warmup + 10s smoke 的阶段下限 **88m54s**。全部预冻扩展均执行则阶段下限 **100m06s**，最多 79 jobs / 143 measure cells。setup、seed、fresh-session durable oracle、drain、guard 启停额外耗时未测，不把下限冒充真实总耗时。3 rates 旧 Cartesian 可测 567 cells 的阶段下限为 6h36m54s；旧默认 6 rates 可测 1134 cells 为 13h13m48s。

覆盖 certificate 显式记录六 topology × 四 workload、真实 C1/2/4/8、两档公平性与三 phase；未选 Cartesian 组合逐项 `NOT MEASURED`，不存在 fullMatrixPASS。首版不共享 baseline；每个点保留独立原始 ledger oracle。safe/interference 不按结果更换档位，不以 overload 或 knee 目标豁免进展下限。未找到 knee 输出 NOT FOUND，扩展耗尽也不宣称找到或性能 PASS。

只有八条 hot knee 曲线全部实测 OBSERVED 且所选必需门禁完整，root 才能 READY FOR INDEPENDENT ACCEPTANCE；扩展耗尽仍有 NOT FOUND 时 root NOT MEASURED，单点实测状态仍原样保留。quiet mixed 局部 safe 候选不表示 D05 已闭合。

## 新自有 launcher 与 writer profile

Main 的 launcher orchestration 可用 wrapper 导出的 `new_verification(arguments)`：它复制 Namespace 并**显式设置 `writer_admission=True`**，然后构造现有 `verify-image.Verification`，不自动调用 `prepare/run/cleanup`。Main 仍独占实际 `prepare/ready/smoke`、资源创建、串行生命周期和 Unknown 保留。CLI 等价入口是现有 `verify-image.py --writer-admission`，但不要调用其会退出/清理 fixture 的完整 `run()` 来代替一个仍需运行 capacity 的 fixture。

把 Main 的实际 launcher report 保存为模板的 `launcher_report`：必须有 `writer_admission:true`、不可变 `image.Id`、`resources.Memory/NanoCpus`。模板的 container limits 是 2 CPU/2GiB，与现有 Verification 对齐；guard 的独立实际进程 RSS safety 上限仍是 512MiB，原 256MiB generated/64MiB raw/50000 offers/最大并发 8 不扩大。storage/IO budgets 是冻结的 operator 预算，不能冒称 wrapper 提供了 native 实时 IO hard limiter。部署/config/provenance 输入必须列入 `guard.sources`。

每个 phase 必须记录真实 `management_metrics.projects[project].writer_admission` 和 `management_metrics.native.workers[worker].writer_profile`。关闭是 FAIL，缺失/类型不正确是 NOT MEASURED。启用旗标本身不证明 owner reserved lane 或 FIFO。

## 冻结矩阵

六个 private connection 配置都复用 peri-stress 原 schema；不运行 `provision.py`：

| topology | project | primary DB | noise DB | demand |
| --- | --- | --- | --- | --- |
| single_hot | 单 project | 1 | 无 | quiet |
| single_many | 单 project | 2 个独立 DB | 无 | quiet |
| equal_hot | 两个独立 project/key | 1 | 1 | 各 50% |
| equal_many | 两个独立 project/key | 2 | 1 | 各 50% |
| victim_hot | 两个独立 project/key | 1 | 1 | victim 10%、noise 90% |
| victim_many | 两个独立 project/key | 2 | 1 | victim 10%、noise 90% |

`databases` 只填额外的第二个 primary DB，不重复主 DB；dual 配置的 `noisy_tenant` 必须使用不同 project、key 和 database。DB/keys 必须属于本轮 Main 自有 fixture，不使用旧 Unknown namespace。

每种拓扑都有 `read/short_writer/held_writer/io_checkpoint`；每个点单独冻结 `1/2/4/8` 中一个 matrix **总 session 数**，不突破原 max8。hot 三档可在任何执行前冻结三个合法递增 rate，默认 25/100/250；safe、interference、envelope 和两轮扩展参数固定如表。各点分开 2s warmup、10s smoke、30s measure，每段独立 session 和原始记录。dual 的 quiet baseline/noisy alone/combined phase 分列；baseline 与 combined 该 tenant 的 offered demand **及有效 session slots 均匹配**，包括 victim 0.5 ops/s。isolated 实际使用 matrix C/2 slots，combined 每 tenant C/2；记录 matrix concurrency 与 `per_project_effective_slots` 并用 raw session_slot 交叉核验。各配置 DB 必须出现在实际 operation 和本 DB 的 durable proof，不能只相信声明的 active count。仍不是隔离性能因果结论。

- `read`：原 `point_read` 独立 rows hash。
- `short_writer`：原 `sustained_wal`，事务 ledger 与 account sums。
- `held_writer`：实际 BEGIN IMMEDIATE、ledger/update、持 writer 100ms，每个 worker 的已执行 workflow 交替 COMMIT/ROLLBACK；明确 terminal ACK。确定性取消用独立 5s cleanup deadline；Unknown 不 rollback/Close/replay，保留 poisoned session。
- `io_checkpoint`：实际写 ACK 后调用原 management checkpoint，并要求 completion ACK；同一 combined phase 所有独立 DB 都有 checkpoint ACK。必须另有本 fixture 的真实物理 IO counter delta，不能仅把请求洪峰/checkpoint ACK 当 IO 竞争证明。

额外 `single_hot/mixed/C1` jobs 复用原 seeded `mixed_oltp`（70% reads、30% transactional writes），用于 D06 safe envelope，不充当 noisy 通过。

combined C1 无法激活两个独立 project，明确 NOT MEASURED，旧 `noisy_tenant` C1 SKIP 不回写/不继承为 PASS。两 primary DB 的 isolated C1，以及三独立 DB 的 combined C2，也明确结构性未测；最低必要点分别是 C2/C4，必须确有实际独立活跃 DB/project。所有结构性边界仍出现在完整表中。

## 证据与辅助观测

每个 cell 保存 offered/planned/missed、client drop/expiry、sent、ACK、typed429、other determinate、Unknown、pending，以及两条守恒式：

```text
offered = client_drop + client_expiry + sent
sent = ACK + typed429 + other_determinate + Unknown + pending
```

typed429 来自真实 typed HTTP failure；未知 429 仍是 Unknown，不从 aggregate admission 数推造。缺失已 sent 的 raw completion 计 pending；expiry 是原 scheduler 的未执行队列残量，不是虚构服务 expiry。raw/summary 数量必须相符。各 project 分别记录计数、ACK/offered、all/ACK/queue 的 p50/p95/p99/max、30s offering window 的固定 5s ACK windows 与最大 observed progress gap。每个 measure 后复用原 fresh-session durable rows/account/ledger oracle，附 project/phase/concurrency 的真实证明。

CPU/RSS 来自有 provenance 的实际 process samples，至少两次；container 内不可见的 host PID 不得伪装为 native 资源证明。Main 可在 job output 新建 0600 `capacity-observations.json`，与原 run_id、launcher report 的 container_name 绑定，提供自己的 fixture-scoped、只读采集证据。缺少观测不会由 wrapper 推造。

辅助文件由本工具的只读 sidecar 生成，根字段为 `run_id/container_name/collector/resource_samples/phases`。Linux image 必须使用下述 `container.proc.status` 五角色真实 OS 证据，不能用 host ps 或缓存 bytes 替代。collector 不虚构缺失的 OS resource 数据；原 mature host observer 与现有资源门禁仍保留。collector journal、状态、规范化结果都加入 evidence manifest；oracle 从原始 trace 与 stress RPC 重新 normalize，拒绝手填或不一致的观测。

`phases` 每项用 `phase/concurrency` 对齐：

- `tickets`：实际 native DB queue stream，每项 `ticket/project/database/queue_id/queue_epoch/session/enqueue_order/valid/invalidated/admitted_order`。按真实 DB + queue + epoch 分组，剔除失效 ticket，admission 必须按实际 enqueue_order 排序，不按 ticket 数字、不跨独立 DB 排全局 FIFO。C8 held combined 每个活跃 DB 至少两个不同 session 的有效 admission；缺失为 NOT MEASURED。safe 档不冒称 contention FIFO 通过，不替代 C8 的真实竞争证明。
- `owner_cleanup`：实际 owner reserved cleanup trace，含 `project/database/reserved_slots>=1/terminal=COMMIT|ROLLBACK/terminal_acked=true/elapsed_seconds<=5`；combined 每个实际 DB 两种 terminal 均出现。client deadline 或启用 profile 不能代替 native 证明。
- `io_samples`：每个实际 DB 的 `project/database/source=cgroup.io.stat|proc.io|native.io_counter/before_bytes/after_bytes/sample_count/started_at/finished_at`；至少两次真实采样、phase 内正 delta，总 delta 不超过冻结 per-phase IO 观测预算。共享 worker/cgroup counter 若不能真实归因到 DB，不得复制分摊为各 DB 证据，只能 NOT MEASURED。Main 独立核对 counter 定义、owner、目标与竞争窗口。

竞争 trace 还必须提供以下字段（并与真实 native/raw 数据绑定，不能手造）：

- FIFO ticket 另有 `holder_session/holder_release_at/enqueued_at/admitted_at`。实际 holder 已先获 admission，waiter 与 holder 不同；同 DB 必须有 `waiter_enqueue < holder_release <= waiter_admit`，串行的“两 session 顺序正确”只能 NOT MEASURED。
- C8 `owner_cleanup` 另有 `session/started_at/finished_at/released_at`。release 是真实队列释放时刻，允许它早于 cleanup_finish trace（不能改成相同时刻）；实际 elapsed 对应 cleanup start/finish。按 DB 关联尚待 admission 的 waiter，证明 reserved cleanup 在真实等待期间完成。低 rate 没有实际等待不会补成竞争 PASS。
- IO sample 另有实际 `counter_scope/pid/process_start_identity/activities`；同一 shared counter scope/PID/start 不得重复标签化。activity 项为 `business_operation_id/kind=write|checkpoint/before_bytes/after_bytes/started_at/finished_at`，每项匹配原 ACK workflow、位于该 workflow 与 counter sample 窗口内并有真实正 byte delta。combined 必须有跨 project write/checkpoint 活动时间的实际交叠；串行 IO、无 attribution 或只有正 delta 为 NOT MEASURED。

### 显式只读 collector

`deploy/collect-capacity.py` 不启动 load、fixture、Docker、Open/Close/SQL 或 provision。它只对冻结 connection 的 proper-CA HTTPS management endpoint 发 GET `/v1/metrics` 与 GET `/v1/capacity-trace`；后者与 Hypatia 的 metrics-auth bridge 对齐，参数是 `role=authority|worker`、worker-only `worker_id`、`stream_id`、`after_sequence`、`max_events=128`。私有 control bearer 留在服务端。连接无代理、无 redirect、验证 CA/hostname、每响应 2s、page <=64KiB；每 poll 最多 8s，每 stream <=16 pages，poll 间隔 0.25s。journal <=32MiB，**与原 stress output 共用原 64MiB 总预算**，不扩大 budget。

Maxwell schemaVersion1 的目录 `events:null`，streamId/queueInstance/namespace/DB/epoch 与分页 source/window 都核对；worker 只按冻结 `worker_ids`，新 DB queue 只按 actual runtimeMappings 选中，历史 queue 仅继续读先前已采集的尾部。开始前 baseline cursor，之后所有新增 seq 连续、去重并拒改写。已 durable 捕获的旧事件被 ring 淘汰不算 collector 丢失；新 seq gap、dropped 增长、source generation/epoch 变化、export truncation 或 source eviction 都 fail closed。

source 的 PID/startUnixMicros/generation 是真实 Native **trace source 建立身份，不是 OS process start**；同进程 clock anchor 跨 snapshot 固定，映射到实际 RPC/phase window。physical_inner PUT start/finish 的 `bytesBefore/bytesAfter` 使用真实 per-namespace cumulative put_payload_bytes，`bytes` 是实际成功 payload；同 namespace 并发 delta 可以包含其它真实 PUT，不能伪装 delta==payload。GET 仅使用 producer 累计计数，不伪造 IO interval。IO join 优先真实 requestId，否则必须唯一 DB/RPC 时间包含关系，明确 `unique_DB_RPC_window`，有歧义则未测。cleanup 还必须确有原 stress terminal RPC ACK，不把 Native success 独自当 client ACK。

Main 每个 job 显式串行：`--next-job` 获取冻结 job/spec/output，启动 sidecar，等 `collector-ready.json` 后调用原 `run_guarded.py`；guard 结束后显式停止 sidecar、finalize、verify。不要先用原 `--run` 再补证据，也不要与其它压力并行。

Native Event 与 GET 累计更新通过同一有界 handoff 传递；producer 捕获原 context/timestamp，observer 按成功入队顺序应用消息、分配 Event sequence 并更新 PUT `bytesBefore/bytesAfter`，GET 不增加 Event sequence。stream 的 `pendingMessages` 是发送中或尚未应用的消息计数；无 loss/gap 时，sequence 与 counters 描述已应用前缀，拒绝的 Event 会推进 sequence 并标记缺口，不能视为完整证据。collector 严格要求相关目录与分页计数为非负整数，缺失或 bool 均拒绝；仅在既有最后一次 finishing poll 要求相关 stream 全部为零，不增加 RPC 或 retry。

handoff 每 stream 最多1024消息、1MiB逻辑消息字节，单消息最多4KiB，全进程 queued 逻辑消息字节最多16MiB；预算包含 Message、boxed Record及其 String capacity。channel 满、断开或预算拒绝均产生真实 sticky loss，不能归为安全历史淘汰。原1024条 history、256个 registry source、64KiB page和256KiB Native export边界不变。16MiB不是总RSS上限或证明，channel固定存储/管理开销、history字符串、page clone与JSON临时内存仍须单列并通过实际512MiB进程/2GiB容器门禁。`try_send` 是容量非等待接口，不宣称 lock-free 或硬实时。

### Linux 原生资源 adapter

Main 的 frozen launcher report 还必须含 `image.Os:linux`、`container_id/container_name/source_hash/required_native_roles`（精确 `['authority','worker-a','worker-b','platform','launcher']` 五角色）。Main 在 collector 前启动同一 owned fixture 的只读 `/proc` monitor。Python callback `capture(..., resource_reader=Main_reader)` 返回以下单条实际 snapshot；CLI 由 Main 原子发布 `$OUT/container-resources.jsonl`（0700 父目录/0600 文件，不写半行，严格追加原始内容）。两个来源不可同时使用。monitor 在 guard 后、collector 最后一次 GET/资源读取完成后停止。Main 必须持续监视 collector/process 退出并停止该批负载；ready 文件不是持续健康保证。

```json
{"schema_version":1,"source":"container.proc","observed_at":"ACTUAL_RFC3339",
 "container_id":"FROZEN_ID","container_name":"FROZEN_NAME","image_id":"FROZEN_IMAGE_ID","source_hash":"FROZEN_SOURCE_HASH",
 "running":true,"oom_killed":false,"exit_code":null,"boot_id":"ACTUAL_BOOT_ID",
 "page_size_bytes":4096,"clock_ticks_per_second":100,
 "processes":[{"role":"FROZEN_ROLE","pid":123,"start_ticks":123456,"rss_pages":123,"cpu_ticks":123,"state":"S","uids":[10001,10001,10001,10001]}]}
```

这是形状模板，不是证据；`processes` 必须含五个冻结角色的真实且不同 PID，每 PID 的 `uids` 按 ruid/euid/suid/fsuid 顺序实读且全部 10001，page size/CLK_TCK 必须实读。每角色 RSS=`rss_pages*page_size_bytes` <=512MiB；容器/image/source、boot/PID/start_ticks、单位稳定，无 OOM/退出/僵尸；每个 measure phase 内至少两条全角色采样。CPU 由相邻 proc ticks/CLK_TCK 的差值除实际 interval，输出 `cpu_seconds_proc/cpu_percent_proc_interval`，不冒充 ps。Native trace 仅 authority/worker-a/worker-b；显式 `worker:worker-a→worker-a`、`worker:worker-b→worker-b`、`authority→authority` 映射逐 phase 绑定 proc PID。Native `startUnixMicros` 是 producer 时间，不冒充 OS start_ticks。Linux 缺 monitor 即 NOT MEASURED；启动时和持续采样不得陈旧超过 2s。

host guard 的 launcher/services 只列 Main 实际 macOS monitor/CLI 的 ps identity；容器 Native PID 不得填入 host ps，docker client 也不是 Native 服务。properCA、producer、proc monitor、guard workload 必须绑定同一 frozen owned fixture，不能跨容器拼接。

Linux 的 `collect_job` 无条件要求原始 collector status、trace journal、observations 并重新 normalize；缺任一文件不能退回 host ps 生成 MEASURED 或 safe envelope。移除全部 proc，即使重新封 journal hash 也无法通过 phase 门禁。Native 三个 producer（authority/worker-a/worker-b）都必须实际开启，connection `worker_ids` 包含 worker-a 和 worker-b；每 phase 从只读 poll 的源身份绑定三个实际 proc PID，即使 quiet phase 无 queue 事件也不跳过绑定。host/container 相同数字 PID 的 CPU/RSS 按 source/container/role/PID 分开，不跨 namespace 合并。纯 proc snapshot 不携带管理 profile，不能污染真实管理 metrics 的 writer profile 判定；仅有 proc 而无 profile metrics 仍为 NOT MEASURED。

```sh
python3.12 prototypes/v1/deploy/capacity-matrix.py --next-job --plan "$PLAN"
# JOB/SPEC/OUT 取自上一步；connection 要有 proper ca_file、metrics_key、worker_ids。
python3.12 prototypes/v1/deploy/collect-capacity.py --capture \
  --plan "$PLAN" --job "$JOB" --stop-file "$OUT/collector-stop.json" &
COLLECTOR_PID=$!
# Main 等 ready 文件；若 sidecar 退出则 HOLD，禁止发负载。
guard_exit=0
python3.12 prototypes/v1/stress/run_guarded.py --spec "$SPEC" || guard_exit=$?
touch "$OUT/collector-stop.json"
collector_exit=0
wait "$COLLECTOR_PID" || collector_exit=$?
python3.12 prototypes/v1/deploy/collect-capacity.py --finalize --plan "$PLAN" --job "$JOB"
python3.12 prototypes/v1/deploy/capacity-matrix.py --verify-job --plan "$PLAN" --job "$JOB"
# 任一非零/未测即停止本批，保留首次原始证据。不能重跑；所有 jobs 完毕再 --analyze。
```

必须在 `umask 077` 下建立 stop file；父目录/原始证据严格 0700/0600。collector-ready 只证明只读采集开始，不是容量 PASS。Main 仍独占 startup/stop 与原 guard 的 load；工具没有隐式 sidecar 或 load 启动。

raw/report/guard/aux/spec 的 hashes 留在 evidence manifest，凭据不归档。frozen plan/source/helper/schema hashes、首次 guard STOP、clean join、时钟漂移与外部 busy 标记一并审查。`external_busy=true` 禁止 wrapper 开始实际 load；guard 发现 builder/confound，现有 raw 只有限保留，fairness/容量归因 NOT MEASURED，绝无性能 PASS。

## 完整 D05 接受条件

1. 先冻结版本、image、build provenance、容器/host、resource limits、dataset/seed、所有 CLI/阶段/停止参数、矩阵和本 schema；以真实自有 fixture 和 P0-P6 joint gate 执行。禁止看到结果后改默认阈值、budget、范围或洗掉第一次失败。
2. Main 批准的 63 基础 jobs / 127 cells 覆盖实际完成，条件扩展只按预冻规则触发；raw、资源、各实际 DB 的 durable ledger 合格，Unknown/pending/cleanup Unknown/correctness failure 为零，计数守恒。未选/未执行/缺测点必须 NOT MEASURED，不能填 PASS；完整 Cartesian 仍 NOT MEASURED。
3. 最小 fairness：每个 offered project 在每个完整 5s window 至少 1 ACK，最大 gap <=5s；等权 normalized Jain >=0.90；low-rate victim ACK/offered >=0.80。有效 FIFO 与实际 owner reserved cleanup trace 另过门禁，不以一次 ACK 或平均吞吐替代。
4. noisy actual 跨 project >=2，quiet/noisy-alone/combined 明确分列；held writer 两种 terminal 各 project 都出现；IO/checkpoint 有真实 completion 与物理 IO，不能只是 flood。观测无从归因或 source/profile 缺失即 FAIL/NOT MEASURED，不接受 noisy。
5. Knee 判据冻结为**连续两次增 offered 档，ACK/s 增幅各 <10%，且每次 queue p95 或 typed429 上升**。输出三档 bracket、两次实际增幅、client overload vs 服务 typed429、资源 before/after。只涨 client drop 不单独判 knee；资源相关不冒充 limiter 因果。未找到是 NOT FOUND，缺测是 NOT MEASURED，二者都不是容量 PASS。
6. Main/非作者独立复验完整 hashes、首次失败、owner/cleanup、actual profiles、资源变化和上述边界。`READY FOR INDEPENDENT ACCEPTANCE` 仅表示工具证据齐备；不代表 G02/G03 已通过或无饥饿保证。

## Safe envelope 与 Main 审批接口

冻结 schema 在 `capacity-result.schema.json`；plan 对其 SHA256 绑定。`safe_envelope` 的 status 只可能 `CANDIDATE FOR MAIN APPROVAL` 或 `NOT MEASURED`，不会生成 PASS。它要求 quiet mixed C1 原 raw、guard、resource、profile、ledger 均合格，ACK/offered >=0.99，drop/expiry/rejection/Unknown/pending 都为零。仅取冻结档从最低档开始的连续合格前缀。

RPC rate **不是 business ops/s 换算**。它是至少两次真实 `management_metrics.actions_total(open+execute+query+close)` 的 delta / observed elapsed，`errors_total` delta 为零，且和同窗口原始 `gateway_rpc` completion 相符（固定最多 2 条边界容差）。definition 明确不含 management/native-internal calls；Main 必须核对 D06 的 RPC 定义/维护负载是否可比。安全 bound 是合格实测 RPC/s 的 0.50；没有实测 counter、raw 或 image/source binding 就是 null/NOT MEASURED。

固定字段：`image_id`、`writer_admission=true`、`source_hash/source`（原 report.source.git_hash）、`safe_rpc_per_second/rpc_rate_safe`（同值）、`rpc_definition`、`qualified_business_rate_points`、`points`、`soak_default`、`d05_full_matrix_status`。完整 result 另绑定 `frozen_plan.path/sha256`。Source Git 本身不是 image build provenance，Main 必须独立核对 image/artifact manifest。

默认 Soak 是 0.2 tx/s × 8 RPC/tx + 0.3 observation/maintenance RPC/s = **1.9 RPC/s**，再留冻结 20% margin，批准的 safe rate 必须至少 **2.28 RPC/s**。partial quiet gate 不意味着完整 D05/FIFO/noisy/G02 都通过；是否批准限定 D06 由 Main 决定，matrix FAIL 不能批准。

只有 Main 独立核验真实原始结果后，才写 approval capsule。七个核心字段为 `image_id/writer_admission/rpc_rate_safe/source/result_sha256/verified_by/status`，其中 `status=PASS` 仅由 Main 写，rate 不得超过实测 bound，`result_sha256` 是最终 result 字节 SHA256（CLI 另写 `.sha256.json`）。可额外严格绑定 `rpc_definition/source_sha256/independent_review`；Soak 的 Hegel 接口负责要求 Main reviewer 和这些 source bindings。不要从工具候选拷一个 PASS，更不要用 mock 结果审批。

```python
from capacity_envelope import validate_capacity_capsule

approved = validate_capacity_capsule(
    result_path, capsule_path,
    image_id=immutable_image_id,
    source=approved_product_source_git,
    writer_admission=True,
    tx_per_second=0.2,
    source_sha256=frozen_soak_and_helper_source_hash,
)
```

`approve_capacity` 是同一验证函数的别名入口。验证是离线的：读 private result/capsule，核对 SHA、frozen plan/seal/source/helper/schema，再从原 quiet mixed C1 run/operations/guard 重新 collect 并复算 envelope，拒候选冒充批准、篡改、缺 raw、错 image/source、关闭 profile、超过实际 safe bound 或 rate+margin。它不新建批准 capsule，不做 API/Docker/S3/provision，也不以 generic source token代替 Main 的独立 image provenance 审核。

## 快速离线验证

```sh
python3.12 -m unittest discover -s prototypes/v1/deploy/tests -p test_capacity_matrix.py -v
python3.12 -m unittest discover -s prototypes/v1/stress -p 'test_*.py'
(cd prototypes/v1/stress && go test ./...)
```

这些仅使用 Python mock/private 临时文件及 Go mock transport/现有 unit fixtures；不是实际容量实测，也不会给 Main 写真实 PASS capsule。
