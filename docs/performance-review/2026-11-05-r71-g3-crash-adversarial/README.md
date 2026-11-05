# R71 Gate 3：崩溃恢复 E2E 与对抗性验证

日期：2026-11-05
候选：分支 `perf/r71-deferred-session-close`，HEAD `faa1ea8`（Go 源码与合并提交 `af66931` 完全相同）
性质：**独立验证轮**。只启动隔离栈、注入故障、采集证据；不修改候选分支产品代码、不合并、不推送。
本目录只保留证据摘要与关键日志；全部卷、容器日志与原始 `raw.jsonl` 保留在 Docker 卷与
`/tmp/peri-r71-g3/` 中。连接文件与 `platform.json`（含私有 token）**未入库**。

## 结论

**总判定：PASS。** T1–T9 全部通过；未发现阻断合入的缺陷。

R71 的三条核心语义均经实测成立：

1. **优雅关闭会排空待处理的 deferred close**：4 个待处理 close（每个注入 2s worker 延迟）时
   `SIGTERM`，platform 阶段等待 **1907.3ms** 后 `exit 0`，`launcher-stop` clean，无 unresolved。
2. **丢失 close 尾记录必须 fail-closed**：确定性注入「客户端已收到 close 成功、但 close 耐久记录
   不存在」后，命名空间在重启时进入 `state=quarantined`、`may_have_committed=true`、
   `cleanup_outcome=unknown`；`SELECT 1` 返回 **HTTP 409 `database_isolated`**；
   `verify_no_unresolved_operations` 报告 `unresolved platform operation prevents clean restart`。
   受支持的释放路径（`dev.py --recover` + `peri recover` 的 decision → retire-local → release）
   执行后命名空间恢复，`SELECT 1` 返回 200，ownership marker 未被删除。
3. **200ms worker close 延迟不再进入客户端 close 延迟**：A 臂 close 动作均值
   **203.42ms**，B 臂 **0.091ms**（−99.96%）；B 臂请求 p50 3.31ms。饱和时每库上限 4 生效，
   `pending_max=4`、超出的 close 转为 inline（设计即「上限变成延迟，不变成拒绝」），无 429/502。

## 候选与探针

| 项 | 值 |
|---|---|
| 候选 platform（任务给定） | `a07ec7b874df3d941ea274823857f2a42ed63497f0c76f50795fb8bef8577cca` |
| 本次从 `af66931` 重建 | `a07ec7b874df3d941ea274823857f2a42ed63497f0c76f50795fb8bef8577cca`（逐字节一致） |
| 本次从 `faa1ea8` 重建 | `e3349abef49e4e1fe5ab39a8d84ef99505431f11c8caa8d31d6be91aec563e75` |
| `af66931..faa1ea8` Go 源码差异 | **0 行**（该区间只新增 R71d 证据文档；差异来自 Go 的 VCS stamping） |
| worker（两侧相同） | `b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff` |
| 镜像 | `ghcr.io/konghayao/peri-loom:v1.2.7`，digest `sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff` |
| 分支 launcher `dev.py` | `80cd2a3c5ecd01d604861368de9bb208bf792e18eaae22ce8db72854c43c2129` |
| 探针 launcher | `/tmp/peri-r71-g3/dev-probe.py`，`74ef196b764371acba0c9571c64946e5811156d46bc8122edada32dc228f975d`（只增加 3 个探针环境变量透传） |
| 探针 platform | `5c3e8f2a9a290620de39d56ffcb02292381ba0616873efa04f03cfa0b9c71152` |
| 探针分支 / commit | `probe/r71-g3-fault-injection` @ `090aad5`（基于 `faa1ea8`，**不合入**） |
| 候选 CLI `peri` | `eabcc85bafeb6d97526f9151ad17cd9b4eb128fd526b2f966b03a4e43e5965b8` |
| Go | `go1.27.3 darwin/arm64` |
| 宿主 | macOS 26.5.1 / Darwin 25.5.0 arm64；Docker 29.4.0（OrbStack） |

探针只做三件事，默认全部关闭：`PERI_R71_G3_CLOSE_DELAY_MS`（在 close 的 worker 调用前
`sleep`）、`PERI_R71_G3_DROP_CLOSE_RECORD=1`（丢弃 close 的 operation 记录与 audit 行，客户端仍收
成功回复，确定性模拟掉电丢尾记录）、`PERI_R71_G3_OPERATION_QUEUE_MAX`（压低 operation journal
准入上限，用于触发结算队列满重试）。探针源码见附录。

## 卷、端口、证据

每项一个全新卷 `peri-r71-g3-<test>-data`，容器 `peri-r71-g3-<test>`，`--network host`，默认配额，
`--stop-timeout 60`；全部卷保留，未执行 prune，未重启 Docker。连接文件只在 `/tmp` 与
`/tmp/peri-r71-g3/state/`，权限 600，未打印。

| 测试 | 卷 | 端口 | 容器 | 运行时二进制 |
|---|---|---:|---|---|
| T1a | `peri-r71-g3-t1a-data` | 20901 | `peri-r71-g3-t1a` | 候选，flag on |
| T1b | `peri-r71-g3-t1b-data` | 20902 | `peri-r71-g3-t1b` | 探针，flag on，close delay 2000ms |
| T2 | `peri-r71-g3-t2-data` | 20903 | `peri-r71-g3-t2`、`t2-final` | 探针（drop close record）→ 候选（直接启动 / 恢复 / 正常启动） |
| T3（第一次，保留） | `peri-r71-g3-t3-data` | 20904 | `peri-r71-g3-t3` | 候选，flag on；SIGSTOP 目标选错（见 T3 说明） |
| T3b | `peri-r71-g3-t3b-data` | 20905 | `peri-r71-g3-t3b` | 候选，flag on；SIGSTOP dev.py 后 SIGKILL platform，再直接启动 |
| T4（第一次，保留） | `peri-r71-g3-t4-data` | 20906 | `peri-r71-g3-t4` | 候选，flag on；native `/v1/sql/query` 不带 autocommit，close 走同步路径（见 T4 说明） |
| T4b | `peri-r71-g3-t4b-data` | 20907 | `peri-r71-g3-t4b` | 候选，flag on |
| T5 A0 | `peri-r71-g3-t5a0-data` | 20908 | `peri-r71-g3-t5a0` | 探针，flag off，无延迟 |
| T5 A200 | `peri-r71-g3-t5a200-data` | 20908 | `peri-r71-g3-t5a200` | 探针，flag off，close delay 200ms |
| T5 B200 | `peri-r71-g3-t5b-data` | 20909 | `peri-r71-g3-t5b` | 探针，flag on，close delay 200ms |
| T6 | `peri-r71-g3-t6-data` | 20910 | `peri-r71-g3-t6` | 候选，flag on |
| T7 | `peri-r71-g3-t7-data` | 20911 | `peri-r71-g3-t7` | 候选，flag on |
| T7b | `peri-r71-g3-t7b-data` | 20912 | `peri-r71-g3-t7b` | 候选，flag on（unpaced c16 配额观测） |
| T8 | `peri-r71-g3-t8-data` | 20912 | `peri-r71-g3-t8` | 候选，flag off |
| T9 | `peri-r71-g3-t9-data` | 20913 | `peri-r71-g3-t9` | 探针，flag on，queue max 2 |

未触碰生产容器 `peri-loom`、`fenix-*`、`coze-*`、`open-compute-*`；未重启 Docker daemon。

## 通用检查（构建 / 静态 / 单测）

**命令**

```bash
cd prototypes/v1/platform
go build ./cmd/peri-platform ./cmd/peri
go vet ./...
gofmt -l .
PERI_RUST_BINARY=/Users/kk/code/peri-loom/prototypes/v1/target/debug/peri-loom-v1 \
  go test ./... -run 'TestDeferredSessionClose|TestSessionCloseDeferred|TestSettlementRetries|TestOperationSettle|TestUnifiedCompletionBatchSubmits' -count=1
```

**输出**

```
build-ok
vet-ok
gofmt: 无输出（干净）
ok  peri-loom/platform            1.270s
ok  peri-loom/platform/cmd/peri   1.702s [no tests to run]
ok  peri-loom/platform/cmd/peri-platform 0.941s [no tests to run]
ok  peri-loom/platform/sdk/go     2.110s [no tests to run]
ok  peri-loom/platform/sqlcontract 1.299s [no tests to run]
```

完整 `go test ./...` 在后台执行，结果见文末「完整测试套件」。

**结果：PASS**

## T1 优雅关闭排空

### T1a：c8 持续负载后停止（候选，flag on）

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t1a 20901 /tmp/peri-r71-g3/bin/peri-platform-g3-af66931 on
python3 scripts/driver.py pipeline --connection-file state/t1a/connection.json \
  --out evidence/t1a/load --label t1a-c8 --concurrency 8 --duration 20 --target-rps 120
python3 scripts/wait_settled.py state/t1a/connection.json evidence/t1a/metrics-settled.json ...
/tmp/peri-r71-g3/scripts/stack.sh stop t1a
python3 scripts/verify_audit.py evidence/t1a
```

**输出（关键）**

```
load: requests=2399 ok=2399 failures=0, 119.63 req/s, p50=2.26ms
deferred_session_close:
  enqueued_total=2403, inline_total=4, failed_total=0, retried_total=0,
  unresolved_total=0, pending=0, pending_max=4, per_database_maximum=4
session_seats=0, active_permits=0, unknown_held_permits=0
operation_settle: retry_total=0, retry_success_total=0, exhausted_total=0, refused_total={}
launcher-stop: outcome=clean, reason=terminal_proof_written, unresolved_operations=false
stages: platform exit 0 (waited 38.0ms), worker-a 0, worker-b 0, authority 0
audit: OK records=7231 seq=1..7231 head=True errors=0
audit actions: open=2407, statement=2407, close=2407（close outcome 全部 ok）
```

- 8 个 warmup + 2399 计时请求 = 2407 个 close；`enqueued(2403) + inline(4) = 2407`，逐条闭合。
- **`flushed_total` 不是「已排空任务数」**：它只统计因每库上限触发的 `flush()` 等待，因此这里为 0；
  排空由 `pending=0`、`unresolved_total=0`、`failed_total=0`、close 审计 2407 条与 clean stop
  共同证明。任务描述中的「`flushed_total` 与 enqueued 一致」与实现字段语义不一致，见「缺陷与
  口径问题」。
- 审计链连续，head 与尾记录一致。

**结果：PASS**

### T1b：关闭时仍有待处理 close（探针，close 延迟 2000ms）

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t1b 20902 /tmp/peri-r71-g3/bin/peri-platform-probe on PERI_R71_G3_CLOSE_DELAY_MS=2000
# 连续 4 个 [execute SELECT 1, close]，然后立即：
/tmp/peri-r71-g3/scripts/stack.sh stop t1b
```

**输出（关键）**

```
burst: 4/4 HTTP 200，客户端延迟 17.34 / 7.53 / 2.90 / 5.01ms
stop 前 metrics: enqueued_total=4, pending=4, pending_max=4, inline_total=0, unresolved_total=0
launcher-stop: outcome=clean, reason=terminal_proof_written, unresolved_operations=false
stages: platform exit 0, waited_ms=1907.3（等待 4 个 2s 延迟的 close）
audit: OK records=22, open=4, statement=4, close=4
container log: 0 条 "deferred session close unresolved"
```

客户端已收到 4 个 close 成功回复，但平台在 `SIGTERM` 后仍等待约 1.9s 把 4 个 worker close 排空，
然后才写 clean terminal。**结果：PASS**

## T2 丢失 close 尾记录（核心授权语义）

**注入**：探针 `PERI_R71_G3_DROP_CLOSE_RECORD=1`，丢弃 close 的 operation 记录与 audit 行；
客户端仍收到 HTTP 200 `closed:true`。随后优雅停止（此时 close 耐久记录不存在）。

**命令与输出**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t2 20903 /tmp/peri-r71-g3/bin/peri-platform-probe on PERI_R71_G3_DROP_CLOSE_RECORD=1
# 1 个 Hrana pipeline [execute SELECT 1, close]
# -> HTTP 200, execute rows=1, close ok
# -> deferred_session_close: enqueued_total=1, pending=0
/tmp/peri-r71-g3/scripts/stack.sh stop t2
```

**停止记录**

```
outcome = unclean
reason  = terminal_proof_unavailable: unresolved platform operation prevents clean restart
unresolved_operations = not_verified
stages: platform exit 0, worker-a/b exit 0, authority exit 0, authority_lock=clean
```

停止后的 state（`t2/operations-direct.json`）：

```json
{"action":"open","state":"terminal","business_outcome":"completed",
 "cleanup_outcome":"pending","may_have_committed":false,
 "session":"159e...a38","worker_id":"worker-a"}
```

正常重启被拒绝（`t2/restart.log`）：

```
project stopped: clean launcher terminal proof missing or invalid
```

并单独调用 launcher 的判定函数：

```bash
python3 - <<'EOF'
import sys,json,pathlib; sys.path.insert(0,'prototypes/v1/platform'); import dev
run=pathlib.Path('/tmp/peri-r71-g3/evidence/t2')
cfg={'generation': json.load(open(run/'platform.json'))['owned_stop_proof']['generation']}
try: dev.verify_no_unresolved_operations(run,cfg); print('PASS')
except Exception as e: print('REFUSED:',e)
EOF
```

```
verify_no_unresolved_operations: REFUSED: unresolved platform operation prevents clean restart
```

候选二进制直接启动（`-config /data/peri/platform.json`）后：

```json
{"action":"open","state":"quarantined","business_outcome":"completed",
 "cleanup_outcome":"unknown","may_have_committed":true,"execution_proven":true}
```

```text
Hrana [execute SELECT 1, close] -> HTTP 409
{"error":{"code":"database_isolated","operation_id":"ae87...af71",
          "possible_commit":true,"retry_allowed":false,"stage":"open_session"}}
```

直接启动的 platform 在 SIGTERM 时以 `exit 1` 结束：
`platform stopped: gateway drain: gateway has unresolved upstream outcomes; terminal proof required`。
这是 unresolved operation 下的 fail-closed 预期，不是缺陷；释放后正常启动即恢复 200。

受支持释放路径（`dev.py --recover` 启动 SQL-disabled 实例，再执行 `peri recover`）：

```bash
docker exec ... /usr/local/bin/peri recover --endpoint http://127.0.0.1:20903 \
  --database local_default/demo/main
```

```text
step 1 operations/decision: decision=retain_isolation_new_database_required
step 2 operations/retire-local: local_resources_retired=true, retirement_request_id=...
step 3 isolation/release: release_request_id=..., release_authority_mode=revoked, release_epoch=1,
                           recovery=none
```

释放后正常启动成功（`clean_terminal_recovered`），`SELECT 1` → **HTTP 200**。ownership marker
未删除：

```text
launcher.lock                    e3b0c442...（空文件，前后一致）
owned-proof/worker-a.identity    9f3299f5...（前后一致）
owned-proof/worker-a.stop        233bd4c7...（前后一致）
owned-proof/worker-b.identity    733d6640...（前后一致）
owned-proof/worker-b.stop        887e3c2d...（前后一致）
authority/authority.lock         clean
```

**结果：PASS**。这是**预期内的失败模式**，不是缺陷：close 记录缺失时命名空间被隔离、clean restart
被阻止；释放路径存在且有效，不删除 ownership marker。若平台在记录缺失时仍放行 clean restart 才是 FAIL，
本次未发生。

## T3 SIGKILL 平台 + 已确认写不丢

### 第一次尝试（保留，作为测试方法说明，不归因候选）

第一次按「SIGSTOP PID 1」执行（`peri-r71-g3-t3`）。容器 PID 1 是 `/sbin/docker-init`，dev.py 是
PID 6；SIGSTOP 打到了 docker-init，launcher 仍存活。随后 SIGKILL platform 触发 launcher 检测到
「platform exited without a clean proof」，整个栈退出（`exit 1`），normal restart 被 clean gate 拒绝。
这**不是候选缺陷**，是验证脚本选中了错误的 PID；证据保留在 `t3/`（含崩溃后 journal 中 insert
的 intent + terminal 记录）。

### T3b：正确流程（候选，flag on）

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t3b 20905 /tmp/peri-r71-g3/bin/peri-platform-g3-af66931 on
python3 scripts/driver.py native-write --connection-file state/t3b/connection.json \
  --out evidence/t3b/write --table r71_g3_rows --value r71-g3-acked-b
# topology.json: dev.py pid=6, platform pid=16, worker-a=25, worker-b=30
docker exec peri-r71-g3-t3b python3 -c 'os.kill(6, SIGSTOP)'
docker exec peri-r71-g3-t3b python3 -c 'os.kill(16, SIGKILL)'
docker exec -d -u 10001:10001 -e PERI_HRANA_EXPERIMENTAL=1 peri-r71-g3-t3b \
  /usr/local/bin/peri-platform -config /data/peri/platform.json
python3 scripts/driver.py native-read --connection-file state/t3b/connection.json \
  --out evidence/t3b/read-after-restart --table r71_g3_rows
```

**输出（关键）**

```
write: open 200 / create 200 / insert 200 / close 200
insert result: affected_row_count=1, autocommit=true, last_insert_rowid=1
kill: platform 进程消失；worker-a/worker-b/authority 仍存活；dev.py 处于 T (stopped)
direct restart: platform pid 206，端口 20905 就绪
read: open 200 / query 200 / close 200
rows = [["r71-g3-acked-b"]]
```

崩溃后 journal（`t3b/volume/...`）：

```text
statement request=c71d1f00... state=in_flight  business_outcome=not_proven execution_proven=false
statement request=c71d1f00... state=terminal  business_outcome=completed  execution_proven=true
open      request=fd4685...  state=terminal  cleanup_outcome=completed
close     request=77ebd7...  state=terminal  cleanup_outcome=completed
```

- operation 记录按 request ID 去重后各一条 terminal，无重复 operation；审计链
  `OK records=22 seq=1..22 head=True errors=0`。
- 直接重启后的 platform 以 SIGTERM 优雅退出，`verify_no_unresolved_operations` **PASS**；
  全部 operation 为 terminal/known，未出现 `outcome_unknown`。
- **口径说明**：崩溃发生在 B 臂；直接重启的 platform 探针未设置 `PERI_DEFERRED_SESSION_CLOSE`
  （flag off），但写语句的 intent/terminal 在 R71 中仍走同步路径，写不丢与 close 是否 deferred
  无关。读会话的 close 是否 deferred 不影响行可见性。

**结果：PASS**

## T4 worker 停止/恢复

### 第一次尝试（保留，说明资格条件）

第一次使用 native `/v1/sql/open` + `/v1/sql/query` 打开只读会话，然后 SIGSTOP 两个 worker 再 close。
`/v1/sql/query` 的响应是 `[[1]]`（纯行数组），**不带 `autocommit`**；平台因此把会话标为
`pristine=false`，close 保持同步，等待 30s 超时后连接被服务端关闭，session seat 仍为 1。
这是**保守资格规则的预期行为**，不是缺陷：deferred close 只对能证明 autocommit 的会话开放。
证据保留在 `t4/`。

### T4b：使用带 autocommit 的 statement 路径（候选，flag on）

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t4b 20907 /tmp/peri-r71-g3/bin/peri-platform-g3-af66931 on
# native open + /v1/sql/statement SELECT 1 -> result.autocommit=true
docker exec -i peri-r71-g3-t4b python3 - <<'PY'
import json,os,signal
t=json.load(open('/data/peri/topology.json'))
for n in ('worker-a','worker-b'): os.kill(t['processes'][n]['pid'], signal.SIGSTOP)
PY
# POST /v1/sql/close -> 200; 等待 32s; SIGCONT; 继续轮询 metrics
```

**输出（关键）**

```text
close: HTTP 200, 3.28ms（worker 已 SIGSTOP）
deferred: enqueued_total=1, pending=1, pending_max=1, unresolved_total=0, session_seats=0
32s 后: failed_total=1, retried_total=1, pending=1, unresolved_total=0
platform.log: "deferred session close failed; retrying: ... failure=cleanup_timeout"（仅 1 条）
SIGCONT 后: pending=0, failed_total=1, retried_total=1, unresolved_total=0
observed workers: worker-a/worker-b healthy=true
native.workers: sessions=0, pending_close=0, orphan=0（两侧）
post-resume load: 60/60 OK, p50 5.40ms
launcher-stop: clean, unresolved_operations=false
audit: OK records=199 seq=1..199 head=True errors=0
```

- 只有 1 次失败尝试、1 次重试，没有忙等；失败通过 cleanup ledger 的 2s/4s/8s… 退避控制，
  上限 12 次 / 10 分钟。
- worker 恢复后 pending 回落、worker sessions 与 seat/许可归零；平台继续正常服务。

**结果：PASS**

## T5 200ms close 延迟注入（对抗）

### A 臂对照

| 腿 | 配置 | close 动作均值 | 请求 p50 |
|---|---|---:|---:|
| A0 | 探针，flag off，无延迟 | **0.490ms** | 1.91ms |
| A200 | 探针，flag off，`CLOSE_DELAY_MS=200` | **203.422ms** | 206.64ms |
| B200 | 探针，flag on，`CLOSE_DELAY_MS=200` | **0.091ms** | 3.31ms |

A 臂注入后 close 均值上升约 415 倍，证明 200ms 注入确实生效且同步路径会支付它；B 臂同一注入下
close 动作均值仅 0.091ms（−99.96%），请求 p50 3.31ms。

### B 臂饱和界限

**命令**

```bash
python3 scripts/driver.py pipeline --connection-file state/t5b/connection.json \
  --out evidence/t5b/bound --label t5b-bound-c8 --concurrency 8 --duration 8 --target-rps 60
```

**输出（关键）**

```text
requests=347, ok=347, failures=0
latency: min=1.70ms, p50=203.95ms, p90=206.96ms, max=229.66ms
deferred: enqueued_total=130, inline_total=235, pending_max=4, per_database_maximum=4,
          pending=0, unresolved_total=0, failed_total=0
errors_total={}, rejection_reasons_total={}, session_seats=0
native.workers.*.sessions=0
launcher-stop clean; audit OK records=1105 seq=1..1105
```

- **没有拒绝**：347/347 成功，无 429/502；延迟期间到达的请求仍被受理。
- 单库 close 速率 60/s 远高于 200ms 延迟下的排空速率（约 5/s），每库上限 4 被占满，超出的 235 个
  close 按设计转为 inline，因此 p50 升到约 204ms。**这是「上限变成延迟，不变成拒绝」的设计行为**，
  不是全局阻塞；未饱和时（顺序采样）客户端 close 约 3ms。
- 延迟结束后所有会话关闭、seat/许可归零、审计链连续。

**结果：PASS（含设计边界的明确说明）**

## T6 已关闭会话的后续请求

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t6 20910 /tmp/peri-r71-g3/bin/peri-platform-g3-af66931 on
for i in 1 2 3 4 5; do
  python3 scripts/driver.py native-sequence --connection-file state/t6/connection.json \
    --out evidence/t6/run$i --sql "SELECT $i"
done
```

**输出（关键，5 次运行一致）**

```text
open       200
statement  200
close      200（0.28–0.48ms，deferred）
reuse_statement 403 invalid_session
reuse_cursor    403 invalid_session
reuse_query     403 invalid_session
```

```
deferred: enqueued_total=5, pending=0, failed_total=0, unresolved_total=0
errors_total: {"invalid_session": 15}（5 次 × 3 个复用请求，预期）
session_seats=0, active_permits=0
native.workers.*.sessions=0
launcher-stop clean; audit OK records=40 seq=1..40
```

无 500/panic/悬挂，未重新创建会话（open 计数未因复用请求增加）。**结果：PASS**

## T7 c8/c16 soak 稳定性

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t7 20911 /tmp/peri-r71-g3/bin/peri-platform-g3-af66931 on
# c8: 65s × 120 req/s
python3 scripts/driver.py pipeline ... --concurrency 8 --duration 65 --target-rps 120
# c16: 65s × 120 req/s
python3 scripts/driver.py pipeline ... --concurrency 16 --duration 65 --target-rps 120
```

**输出（关键）**

| 阶段 | 请求 | 失败 | p50 | p99 | 速率 |
|---|---:|---:|---:|---:|---:|
| c8 65s | 7800 | 0 | 2.25ms | 7.68ms | 119.87 req/s |
| c16 65s | 7799 | 0 | 2.30ms | 7.64ms | 119.75 req/s |

```
actions: open=statement=close=15623
deferred: enqueued_total=15619, inline_total=4, pending=0, pending_max=4,
          failed_total=0, retried_total=0, unresolved_total=0
session_seats=0, active_permits=0, unknown_held_permits=0
errors_total={}, rejection_reasons_total={}
operation_settle: retry_total=0, retry_success_total=0, refused_total={}
native.workers.*.sessions=0, pending_close=0, orphan=0
launcher-stop clean; audit OK records=46879 seq=1..46879 head=True errors=0
```

Paced 120 req/s 低于默认 500 token/s 桶（每请求 3 token，约 166 req/s 上限），因此无 429；
c8/c16 两档的全部计数不随请求数增长，结束时归零。

补充 unpaced c16（`t7b`，同候选）：

```text
requests=21811, ok=981, 429=20830（quota_exceeded / rate_limit）, 其它错误=0
actions: open=21827, statement=3532, close=3532
deferred: enqueued_total=3505, inline_total=27, pending=0, unresolved_total=0
native.workers.*.sessions=0; session_seats=0
launcher-stop clean; audit OK records=28901
```

unpaced c16 约 2724 req/s 的尝试速率远超默认桶，429 `quota_exceeded` 是**设计内的限流拒绝**；
没有 502/`outcome_unknown`，且每个被打开的会话都被 close 记录/回收，无泄漏。

**结果：PASS**

## T8 kill switch 等价性（A 臂）

**命令**

```bash
/tmp/peri-r71-g3/scripts/stack.sh start t8 20912 /tmp/peri-r71-g3/bin/peri-platform-g3-af66931 off
python3 scripts/driver.py native-sequence --connection-file state/t8/connection.json --out evidence/t8/sequence
```

**输出（关键）**

```text
/v1/metrics 中没有 deferred_session_close 键（metrics-keys.json: false）
open 200 / statement 200 / close 200（0.48ms）
reuse_statement/reuse_cursor/reuse_query -> 403 invalid_session
launcher-stop clean; audit OK records=16
```

与 `t5a200` 对照：同一 close 路径在 flag off + 200ms worker 延迟下 close 动作均值 **203.42ms**，
且 metrics 仍无 deferred 键——证明 A 臂 close 是同步路径，延迟由客户端承担。

代码级核对（`git merge-base HEAD main = b1e8ae9`）：

- `deferredSessionCloseEnabled()` 为 false → `config.DeferredSessionClose=false` →
  `Gateway.deferredClose=nil`（`gateway.go:217-221`）；
- close 分支 `deferred := gateway.deferredClose != nil && ...` 为 false，直接走
  `gateway.callSession` 后才回复；
- `callOpenSession` 在 `gateway.deferredClose == nil` 时直接返回原始错误，不做容量 flush/重试；
- `session.pristine` / `noteStatementAutocommit` 的写入只被 `deferredCloseEligible()` 读取，
  而后者只在 `deferredClose != nil` 时调用。

**结果：PASS**

## T9 与结算队列满修复的交互

**代码核对**（`session_close_deferred.go`、`operation_settle_retry.go`）：

- close 的 settlement 走 `Gateway.settlePendingOperation` → `settlePendingOperationUsing` →
  `settleNonCommittingOperationUsing(ctx, &gateway.settle, ...)` → `account.submit`；
- `submit` 只对 `ErrOperationQueueFull` 重试：250µs 起、20ms 上限、总预算 300ms，退避前释放
  namespace fence，SQL 从不重放；
- 预算耗尽产生 `settleExhaustedError`，它 `Unwrap()` 为 `ErrOperationQueueFull`，由
  `recordRefusal` 记录 `refused_total{reason=queue_full}` 与一条 `operation settlement refused`
  ERROR 日志；只有到这一步才会把结果改写为 `outcome_unknown`。

**运行时证据**（探针 `PERI_R71_G3_OPERATION_QUEUE_MAX=2`，B 臂，c8 10s × 120 req/s）：

```text
requests=1199, ok=1199, failures=0
operation_settle: retry_total=90, retry_success_total=58, exhausted_total=0, refused_total={}
errors_total={}, rejection_reasons_total={}
deferred: enqueued_total=1207, pending=0, failed_total=0, unresolved_total=0
platform.log: 0 条 "operation settlement refused"
launcher-stop clean; audit OK records=3631
```

queue max 2 使 settlement 在 90 次提交上遇到队列满，全部在预算内重试；没有 `outcome_unknown`，
没有 `refused_total`，也没有 ERROR 拒绝日志。deferred close 的 settlement 使用同一账户，因此也被
这组重试覆盖。

**结果：PASS**

## 发现的缺陷与严重度

| 项 | 严重度 | 结论 |
|---|---|---|
| 无阻断合入的问题 | — | T1–T9 全部通过 |
| `flushed_total` 字段语义与 T1 判据措辞不一致 | 低（文档/可观测性） | 该字段只统计因每库上限触发的 `flush()` 等待，不是「已排空任务数」。排空由 `pending=0`、`unresolved=0`、`failed=0`、close 审计计数与 clean stop 证明。建议文档改口径，或增加 `completed_total` 计数器 |
| deferred 资格依赖权威 autocommit | 信息（设计限制） | native `/v1/sql/query` 的响应不带 `autocommit`，该路径的 close 保持同步；Hrana/statement 路径可 defer。保守规则，安全但会让部分客户端拿不到 R71 收益 |
| 饱和时 inline close 支付 worker 延迟 | 信息（设计边界） | 单库上限 4、200ms 延迟、60 close/s 时 p50 约 204ms；设计明确「上限变成延迟，不变成拒绝」，无 429/502。不是全局阻塞 |
| T2 丢失 close 记录时停止为 unclean | 信息（预期 fail-closed） | `terminal_proof_unavailable: unresolved platform operation prevents clean restart`；释放路径可恢复。符合授权语义 |

**预期内的失败模式 vs 真实缺陷**：

- **预期**：T2 的 quarantine/409/clean-gate 拒绝；T4 第一次 close 同步（native query 无 autocommit）；
  T3 第一次因 SIGSTOP 选错 PID 导致整栈退出；T5 饱和时 inline 延迟；T7b 的 429 quota_exceeded。
- **真实缺陷**：未发现。

## 未验证 / 限制

1. **未做真实掉电/机器崩溃**（无法在容器内可靠丢弃页缓存）；T2 使用确定性的「丢弃 close 记录」
   探针，模拟的正是授权中接受的「close 尾记录丢失、pending open 仍在」状态。
2. T2 的直接平台启动与 T3b 的直接重启分别使用候选/probe 二进制；T3b 的直接重启未设置
   `PERI_DEFERRED_SESSION_CLOSE`。写语句 intent/terminal 在 R71 中保持同步，写不丢与 close
   是否 deferred 无关；读会话 close 不影响行可见性。若要求「B 臂直接重启」的严格复现，可补跑。
3. T4 使用 SIGSTOP/SIGCONT 暂停 worker 进程，而非杀死并重启 worker。进程存活时 worker 的
   `closed` 注册表仍在，重试 close 可幂等成功；**worker 进程崩溃且会话状态丢失**时，重试会
   依赖 cleanup budget 与 worker idle reaping，未做端到端验证（时间上界：12 次 / 10 分钟，
   idle sweep 900s）。
4. 未做容量/SLA 证明；T5 的 200ms 注入仅用于对抗性验证，结论不外推为性能数据。
5. c16 unpaced 的 20,830 个 429 是默认配额限流；如需在 c16 持续压测中零 429，需客户端限速或
   调整配额（本测试未放宽配额）。

## 完整测试套件

**命令**

```bash
cd prototypes/v1/platform
PERI_RUST_BINARY=/Users/kk/code/peri-loom/prototypes/v1/target/debug/peri-loom-v1 \
  go test ./... -count=1
```

**输出**

```
ok  peri-loom/platform              176.174s
ok  peri-loom/platform/cmd/peri       0.669s
ok  peri-loom/platform/cmd/peri-platform 0.793s
ok  peri-loom/platform/sdk/go         0.919s
ok  peri-loom/platform/sqlcontract    0.930s
exit=0
```

完整日志见本目录 `go-test-full.log`。**结果：PASS**（含 `TestP6Verify` 等 native 集成测试）。

## 附录：探针源码

探针分支 `probe/r71-g3-fault-injection` @ `090aad5`（基于 `faa1ea8`，不合入）：

```go
// probe_r71_g3.go：三个环境变量开关，默认全部关闭
func probeCloseDelay() time.Duration        // PERI_R71_G3_CLOSE_DELAY_MS
func probeDropCloseRecord() bool            // PERI_R71_G3_DROP_CLOSE_RECORD=1
func probeOperationQueueMaximum() int       // PERI_R71_G3_OPERATION_QUEUE_MAX
```

补丁点：

- `gateway_writer.go` `callSession`：`close_session` 调用前按 `probeCloseDelay()` 睡眠；
- `gateway_operations.go` `settlePendingOperationUsing`：drop 探针开启时跳过 close 的 operation 记录；
- `gateway_http.go` `recordAuditEvent`：drop 探针开启时跳过 close 的 audit 行并释放 reservation；
- `operation_commit.go` `submitAuditPrepared`：用探针值覆盖 `queueMaximum`。

探针 launcher `/tmp/peri-r71-g3/dev-probe.py` 只把上述三个环境变量加入
`platform_process_environment()`，其余与分支 `dev.py` 相同（SHA256 见上）。
