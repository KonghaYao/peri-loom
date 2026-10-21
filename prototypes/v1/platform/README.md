# P6 native cloud platform

This is the Go management, SQL gateway, key broker, and ownership controller for
the native Rust database services. It is a standalone Go module with a local
dependency on `../controller`. Initialize the pinned Turso submodule before building.

## 发版卷兼容性（v1.1.9 起）

从 v1.1.9 起创建的**本地 backend** 卷支持跨后续版本干净重启；这是完整证明链
成立时的兼容约定，不是任意崩溃都能自动恢复的承诺。S3 backend 的启动中断不提供
自动恢复，必须人工处置。已有旧卷/已 brick 卷不自动迁移，需重建。
本次不提供 ≤v1.1.4 checkpoint 迁移，也不提供旧版 preserved proof 导入工具。

- `launch.json` 在单一入口进行内存归一化，新增可选字段必须提供历史安全默认值。
  `wal_replay_bytes` 缺失时为 0（使用 worker 默认值）；不改写被 terminal 哈希绑定的文件。
- 配置/端口 rebind 拒绝发生在服务启动前，不创建阻塞性的 failure record。
  新卷在写入 `launch.json`/`platform.json`、密钥或 Authority marker 之前预检所有监听地址；
  端口占用时直接退出，释放端口后同一 run-dir 可直接重试。已有卷在复用分支预检。
  预检与实际 bind 之间仍存在竞态，服务启动后的错误不能按无副作用处理。
- staging 复制 terminal/stop/state/floor/worker proof、本地 Authority root 和 seal，
  ready 前不移走唯一 canonical 证据。签名 `launcher-startup-attempt.json` 在每次
  `Popen` **之前**持久化 `starting` 服务、唯一 attempt nonce 和完整 `commands` argv；
  `Popen` 成功后立即原子写回 PID 和服务名，再继续身份文件/readiness。任一 SIGKILL 点
  都由 intent、PID 或 `starting` + 完整 argv 加状态证明覆盖；ready 后归档旧 terminal。
  staging 后、尚未尝试启动服务的 SIGKILL，可在原目录重新启动：必须通过完整门禁，且
  Authority/控制面/state/floor/证明及本地 seal 快照哈希一致。不会回写 dirty marker。
- Authority/platform/worker 已尝试启动的中断，正常启动一律拒绝并保留 canonical 证据。
  先停止并确认存活服务退出；不得把超时或 kill 当作 clean stop 证明。随后显式运行原配置
  加 `--recover`，可按签名 intent 做**证明式启动回滚**（仅本地 backend）：
  - **spawn 窗口**（`starting:"authority"`、`services_started:[]`、`pids:{}`）：按 Authority
    可能已 `Popen` 处理，用 intent 中记录的完整 argv 与 `ps` command 做字符串全等匹配，
    仅对全等进程做有界 SIGKILL join；path-only、path+`authority` token 等诱饵和 PID 复用
    都不会匹配。缺失记录 argv 或无法读取进程表时 fail-closed，不回退子串匹配。
    再校验 preserved terminal HMAC、preserved stop、owned proof HMAC、state/floor、
    operation journal、本地 backend 清单、Authority root 数据字段、live marker，以及
    live seal 是否为本 attempt 写入（`seal_before_spawn` + 新 owner，或已记录的 attempt seal）。
    未写入任何 marker/state 的干净空窗口由正常启动直接通过，不要求 operator。
    若 platform 在 PID 写回前仍有存活进程，直接 `--recover` 拒绝且**不杀** platform/authority：
    先用 SIGTERM 优雅停止两者并等待 joined 退出，使 shutdown checkpoint 物化 operation view，
    再重跑原配置 `--recover`。仅当 joined 停止后 checkpoint 仍缺失时才需重建卷；
    不得清 marker/proof，也不自动回写 state/floor。
  - 校验 preserved terminal HMAC、preserved stop、owned proof HMAC、state/floor、operation
    journal、本地 backend 清单（除 seal/root）、Authority root 数据字段，以及 live seal
    等于本次 attempt 记录的 seal、marker 为 dirty/本次尝试的 clean、readiness 未宣告、
    PID 已退出、端口空闲。
  - 通过后只恢复 preserved 的 owned proof、seal、Authority root 和 clean marker 的原字节，
    回到上次 clean 点；不生成或伪造 terminal/proof/marker。未 ready 前平台产生的
    checkpoint 物化控制面状态（例如 worker 注册）可保留，但数据字段变化仍拒绝。
  - 恢复实例以 SQL 禁用模式启动；SIGTERM 干净停止时写新 terminal，之后正常启动并读回数据。
  - 任一证明不匹配、Authority 已服务持久写入、数据字段变化、S3 backend 或进程/lease
    状态不明，`--recover` 仍 fail-closed，并输出具体失败检查与 operator 指令。
    当前 S3 不提供证明式启动回滚；不得删 marker 强启，需保留卷并按 S3/Authority 证据人工处置。
- **readiness 已宣告后的 SIGKILL** 不进入 startup rollback：客户端可能已执行未知 SQL，
  普通启动和 `--recover` 都拒绝把这次尝试回滚到旧 terminal，按普通 unclean 语义处理。
  另有一条独立的 Authority 恢复 profile：仅在可机器检查的 `--recovery-evidence` 计划
  通过后以受限模式启动，不会回滚旧 terminal；见 `## v1.1.10` 的 S05 小节。
- 启动后的失败保留签名 `requires_operator:true` 记录。完整 clean 门禁通过并宣告 ready 后，
  仅当当前记录仍为 `requires_operator:true` 时写一条 `requires_operator:false` 的取代记录，
  包含 `superseded_by_terminal_sha256` 和时间戳；原始字节保存在
  `launcher-restart-failure-history/`，最多保留 32 份。已取代记录不会再次重写。
  显式 `--recover` 必须在干净停止与全部 terminal 条件成立后才能取代失败记录。
  失败记录缺失签名/不可验证仍 fail-closed；不会自动重试或重放 Unknown SQL。

## Run the whole local project

From the repository root:

```sh
git submodule update --init prototypes/p0/turso
python3 prototypes/v1/platform/dev.py --run-dir /tmp/peri-local --tls
```

The launcher builds the React/TypeScript web console, native Rust service, and
Go platform in that order, then starts one
authority, two encrypted workers, and the platform, and prints a readiness
record only after authenticated provisioning. It seeds an organization, a
project, and an encrypted demo database in a new private directory. The
`connection.json` file contains synthetic API keys and a development CA path;
never publish it. Open its gateway endpoint in a browser and trust the disposable
development CA only for this fixture. Do not disable TLS verification in clients.
The browser console has project/database navigation, SQL editor, refs, operations,
access, and observability; see `frontend/README.md` for direct Vite development
and the required frontend-before-Go build order. Project API keys remain in
page memory only; if a user explicitly opts in, the console verifies the
global management API key and saves it in this browser's localStorage until
cleared. Such a key is readable by same-origin scripts/extensions and anyone
using the browser profile; do not enable this on shared devices. The global
key can open project-scoped native SQL sessions and preview rows; project keys remain available for application access.

The listener defaults to `127.0.0.1:9443`. `--listen` changes the endpoint;
`--tls` generates a disposable local certificate unless an existing private
certificate/key pair is supplied via `PERI_TLS_CERT` and `PERI_TLS_KEY`.
External listeners require TLS. SIGTERM performs joined shutdown; an uncertain
gateway operation makes process termination unsuccessful rather than proving
clean cancellation. A fully acknowledged, joined worker shutdown writes a
signed stop proof for that exact worker incarnation. 启动器允许通过完整 clean
门禁的原目录复用，而不是无条件拒绝所有已有目录。门禁包含 worker incarnation
与 joined-exit 签名、terminal 对配置/state/floor/proof 的哈希绑定、Authority clean
marker、clean stop record、failure record 和无未决操作检查。独占 run-directory
锁覆盖启动和停止，但锁本身不证明旧进程已停止，也不替代 storage fencing。
`operation_storage_version=1` 必须满足 `operations_checkpoint == revision`；
version 0 的完整内联 operation view 不受 checkpoint 字段限制，仍须通过其余门禁。
不得删除 proof、ownership 或 dirty marker 强行启动。

`--recover` reopens an existing run directory to end an isolation or to roll back an
interrupted startup from its signed intent and preserved snapshot. It is not a second
restart gate. The isolation-release path requires the owned worker stop proofs and the
clean Authority shutdown marker the previous stop left behind. The startup-rollback path
is described in the release-volume compatibility section and is limited to the local
backend. It refuses a directory that already passes the unchanged clean gate, and archives
the failing `launcher-stop.json` as `launcher-stop.pre-recovery.json` before
starting the recovery instance. The recovery instance starts only the Authority and the platform
with `PERI_RECOVERY_ONLY=1`: no workers, no reconcile loop, no Hrana, and
`/v1/sql/`, `/db/` and `/` answer 503 `sql_unavailable` with `recovery:true`.
Only `operations`, `operations/decision`, `operations/retire-local`,
`isolation/release` and `status` are routed; every other management path answers
409 `recovery_restricted`, and capabilities report `recovery:true` and
`sql_enabled:false`. `isolation/release` still requires the stop proof, the local
retirement and the Authority's fencing attestation, and never clears the
ownership marker. The run becomes reusable only after a clean stop of that
instance followed by a normal start that passes the unchanged clean gate.
For a fresh fixture only, `PERI_BOOTSTRAP_KEY` can reuse a previously protected global key;
other fixture credentials and database contents are new. Never put the key in a command argument or log.

Private state, KMS, and audit heads use a bounded complete commit intent before
the floor/payload renames. Under the exclusive file lock, restart only rolls
forward exact validated parent/next-revision prefixes. Missing or unsafe files
on an established store, mismatched intent, and rollback without intent remain
fail-closed. Each business payload remains limited to 16 MiB; the temporary
two-payload intent has a separate physical limit of 32 MiB plus 16 KiB.
Audit-head recovery never creates or replays an append: the existing append
chain must still match the recovered sequence/hash, or opening the audit fails.
Persistence uncertainty remains an error until verified reopen; recovering a
file commit does not prove SQL execution, permit replay, or clear quarantine.

For an existing S3-compatible service, create a **dedicated bucket** first and
inject `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`, and
`S3_SECRET_ACCESS_KEY` through a private environment mechanism, then add
`--backend s3`. Only the Rust authority receives these variables. Workers and
the Go platform receive no raw S3 credentials. Never save real credentials in
source, argv, run configuration, or diagnostic logs.

## API and credentials

- `/v1/sql/open`, `/execute`, `/query`, `/close` manage opaque, principal-bound sessions.
- `GET /v1/platform/capabilities` reports the public gateway origin and whether experimental Hrana HTTP and WebSocket are actually enabled. With `PERI_HRANA_EXPERIMENTAL=1`, project SQL API keys can use `https://<host>/db/<tenant>/<database>/<branch>/` through the tested JS client's Hrana HTTP v2 transport, the Rust `turso_serverless` 0.1.3 client's v3 JSON pipeline/cursor transport, or `wss://<host>/db/<tenant>/<database>/<branch>/` through the low-level JS client's WebSocket v1/v2 JSON and v3 protobuf. The adapter defaults off; HTTP v3 JSON read cursors, and v3 protobuf read-only and write cursors on HTTP and WebSocket, fetch bounded native pages. A write cursor no longer requires `RETURNING`: without it the reply carries no columns or rows, and `step_end` reports `affected_row_count` and `last_insert_rowid`. HTTP v3 JSON read cursors have a 10,001-row regression; Rust SDK reads and a disposable transactional write batch passed against an isolated loopback HTTP gateway. TLS development certificates must be trusted by the client independently; the Rust SDK does not accept a custom CA through its public builder. An HTTP v3 write-cursor abort records `cursor_client_stopped`, `cursor_dispatch_stopped` and a proof-bound `cursor_native_outcome` (`rolled_back`, `committed` or `unknown`) on its operation; a close that proves neither outcome keeps the namespace isolated. Full protocol and remote session-store schema compatibility, as well as crash-restart recovery, are not yet proven.
- Query may execute SQL writes: native read-only mode, not SQL text filtering, enforces reader roles.
- `/v1/databases/*` exposes provisioning, lifecycle, refs, snapshots, clone, restore, PITR, checkpoint, and GC.
- `/v1/organizations/create` and `/v1/projects/create` require the global management key.
- Project keys manage only their project; the global management key may execute native SQL against any existing database using that database's project quotas, but cannot mint database tokens.
- `/v1/tokens` issues bounded-lifetime tokens tied to issuer, project, root, branch incarnation, and encryption domain.
- `/v1/keys/revoke` invalidates issuing keys and their database tokens.
- `/v1/metrics` requires a global management key and combines gateway and actual native process observations.

SQL clients should send optional `X-Request-ID` alongside the same JSON
`request_id`. The header must occur once and contain 1–128 ASCII letters,
digits, `-`, `_`, or `.`. It is transport correlation only, not authentication
or an idempotency guarantee. Valid headers correlate even early authentication
or quota denials without reading the body; malformed or duplicate headers fail
closed with a safe generated ID. A nonempty body ID must match the header or
the request fails before SQL dispatch. Headerless body-ID success remains
compatible, but early denials before body reading use a generated fallback ID
and cannot guarantee echoing a body-only ID. Admission still precedes body
parsing; clients must not weaken response-ID checks or retry SQL automatically.

SQL requests are never automatically retried. A lost reply may have committed;
use business idempotency keys and inspect durable state before reconnecting.
Worker SQL failures are answered with a machine-readable `error.code` and a
sanitized `error.message`; the Worker text stays in the Gateway log. `sql_error`
(400) is the statement itself failing, `sql_timeout` (504) the Worker
per-statement deadline, `sql_budget_exceeded` (400) the per-statement compute
budget, `result_limit`/`sql_limit` (413) the Worker result or SQL size budget,
`sql_conflict` (409) writer admission held by another writer, `sql_unavailable`
(503) a refused admission or lost ownership, and `readonly_session` (403) a write
on a read-only session. Settled failures carry
`execution_completed=true` plus `may_have_committed`; a statement the Worker
proved it never entered execution carries `execution_completed=false` and
`may_have_committed=false`. Only the Worker's own SQL reply states which of the
two happened: an answer the Gateway could not decode as that reply, including any
non-200 status on the Worker `/sql` call, is answered as `outcome_unknown` with
`possible_commit` rather than as a proven rejection. The session can issue an
explicit rollback, but a multi-statement committed prefix is not rolled back
automatically. Unknown
outcomes retain admission permits and poison the session instead of treating
timeout, close, or readback as cancellation proof; that transition is logged with
the request id, stage, database and the session it had, and with the Worker's own
verdict (`rpc=ambiguous=…,rejected=…,execution_completed=…`, or `rpc=none` when
the platform holds no verdict) and its failure text or the transport error
(`observation=`, `none` when neither exists), while the isolated-namespace
refusals it causes are audited without a second log line. Hrana pipeline errors
use that same contract: an unproven session or autocommit probe failure is
`outcome_unknown` with `outcome_unknown: true`, `retry_allowed: false`,
`possible_commit`, `execution_proven` and whatever operation id and stage the
platform holds, never a transport-specific code that hides the platform's
verdict.
Every client — the Hrana
adapters, the Go and TypeScript SDKs, the browser console and the stress
harnesses — decides from `peri-loom/platform/sqlcontract`, which owns the code
vocabulary, the meaning of the two evidence fields and the rule that a failure
proving what happened to a statement is never an unknown outcome. The two fields
are unexported and decoded by that package alone, and
`TestHranaAgreesWithTheFailureContract` in the platform package drives each
consumer over every code, evidence shape and status, so a code added to the
vocabulary cannot fall out of a consumer's table. Migration invalidates old
session bindings; reopen explicitly against the current placement.

Native SQL sessions idle for 5 minutes are closed by the Gateway's background
cleanup loop. Each completed request refreshes the idle deadline. Cleanup waits
for a confirmed Worker close before returning the session quota; an uncertain
close retains the session and quota for explicit recovery instead of growing the
durable operation log: a retained session is retried on a bounded account of
twelve attempts or ten minutes, with backoff. Every failed attempt is logged with
the session identity and counted in the cleanup ledger. A spent account has no
in-process exit — the session keeps its seat and its quota until an operator
retires it, and while it exists the launcher refuses a clean stop rather than
hiding the retained session. Clients should close sessions when finished and must
not rely on an idle session to keep an open transaction alive. Hrana streams keep
their separate idle timer.

JSON retains native integer literals. Go generic results use `json.Number`;
TypeScript and the browser console use `bigint` for unsafe integer literals.
NDJSON streams bounded, already materialized native rows; it is not an
incremental native-execution cursor.

## Security and persistence boundaries

The authority owns the only raw storage path. Private authenticated loopback
HTTP carries control, SQL, and key-broker traffic with distinct credentials.
New databases use native AEGIS-256 page encryption, including WAL images.
Every native session connects with the resolved key. Wrapped DEKs are bound to
namespace, immutable root incarnation, encryption domain, and key revision;
the broker checks fresh Rust ownership before and after unwrapping. There is
no plaintext fallback and no retroactive encryption claim for old plaintext databases.

The local KMS wraps DEKs with AES-GCM and stores its KEKs in a private,
file-backed development keyring, **not an HSM or external cloud KMS**. The KEK
file itself is not encrypted; host disk/root access can recover the KEKs and
unwrap DEKs. Database encryption protects stored page/WAL images without
promising protection from compromise of the key-management host.
Rotate a KEK and rewrap descriptors without changing
DEKs or the database domain. Keep old KEK versions required by retained envelopes.
DEKs remain available for reachable historical snapshots; deleting a branch is
not cryptographic erasure. Keys can exist in process memory; swap and hostile
container administrators are outside this prototype's threat boundary.

Controller/platform metadata use separately locked, checksummed, fsynced
atomic local stores with anti-rollback floors. Audit records exclude SQL text
and credentials and use a fsynced hash-chain head. Corruption, missing state,
and unresolved persistence outcomes fail closed. This is a single-authority,
single-platform prototype, not a consensus-backed or replicated HA deployment.
Coordinated rollback of every persistence file is not prevented by local floors.

Admission and rate limits are per gateway process/project. Native SQL has
cooperative VM/time/row/result budgets. Result accounting is not an RSS cap;
a large native scalar can allocate before a result check, and cooperative
timeouts cannot preempt a blocked storage operation or a single native function.
Audit storage rotates at 64 MiB per segment, retaining older segments on the
same volume. Startup verifies the full sequence and hash chain, including
archived segments, and resumes only validated interrupted rotations. Disk space
remains finite: there is no safe pre-dispatch audit-capacity reservation or
operator retention/backup policy yet, so disk exhaustion still fails closed.
After a trusted native SQL completion, an audit append failure returns
`audit_unavailable` without reclassifying the durable operation as Unknown or
poisoning its session. The reply includes `execution_completed` and forbids
retries; the audit outage itself remains a fail-closed service error. A genuinely
ambiguous native outcome remains quarantined. This does not solve disk-space
exhaustion or prove that the client received an earlier reply.

Two durable operation formats upgrade one way, and both refuse before serving
rather than after: a compacted operation snapshot now carries an envelope version
and a digest of its own content, and durable command digests are written as
`logical_command_v1:<hex>`. Binaries released before v1.1.4 do not know either
field vocabulary and refuse to open such a state directory at startup. Rolling
back therefore needs an operator who removes the operation snapshot (which drops
the operations it materialized) or restores a pre-upgrade copy. In the other
direction the upgrade is one way: a snapshot written by an older release stays
readable and is re-sealed in place on its first open, while earlier binaries
refuse this field vocabulary at open. Because the snapshot has no anchor outside
its own directory, an edited snapshot that keeps a legal chain position is only
detectable through its content digest: a writer who can rewrite the directory can
also recompute that digest.

A clean stop materializes the operation view. Operation commits append to the
sidecar log without rewriting the registry payload, so a reader of `state.json`
alone can trail the durable log by an arbitrary number of operations. Shutdown
therefore commits the joined view into the payload and freezes both write paths,
and that commit stamps `payload.operations_checkpoint` with its own revision. A
reader may treat `payload.operations` as the durable operation view exactly when
`operations_checkpoint != 0 && operations_checkpoint == payload.revision`
(equivalently `== envelope.revision`); an absent or older value means the payload
may lag the sidecar, and a clean-stop judgment must not be based on it. A
checkpoint that cannot commit leaves the marker behind, keeps both write paths
open and makes the process exit non-zero, so no clean-terminal proof can be
recorded for that stop.

v1.1.5 adds `payload.operations_checkpoint`. `operation_storage_version=1`
的 payload 若缺少 checkpoint 或 checkpoint 不等于 revision，启动器拒绝复用；
version 0 的 operation view 全部内联，因此不会仅因缺少 checkpoint 被拒绝。
本次不提供旧卷迁移，已有旧卷或已 brick 卷需重建；不要手工补 checkpoint 或签名。
The owned-session
lifecycle and shutdown budget protocols are additive RPC as well: the Worker and
the Authority advertise `lifecycle_protocol` and `shutdown_protocol` on
`get_metrics`, a caller that does not send the new commands keeps the legacy
`open_session` path, and the platform's own consumption of both is not wired yet.

The whole teardown — HTTP shutdown, gateway drain, operation checkpoint and audit
tail — shares one deadline, reported as `platform shutdown incomplete` when a step
does not finish inside it, instead of waiting for a step that may never return.
`PERI_SHUTDOWN_TIMEOUT_MS` lowers that deadline (1s to 60s, default 10s) and an
unusable value refuses to start. A budget above the default only starts next to
`PERI_SHUTDOWN_STAGE_CAP_MS`, the per-stage stop cap the launcher holds this
process to, and it has to stay strictly below it: a process still tearing down
when the cap expires is killed mid-stop, which is the dirty stop this shared
deadline exists to prevent. Raising without that declaration refuses to start,
and so does a declared cap the default budget does not fit under.
`PERI_SHUTDOWN_OUTER_GRACE_MS` is the deployment's statement about the whole
stop, the grace the orchestrator enforces around the process, and a declared cap
that reaches it refuses to start as well: a cap larger than the real kill is a
check that cannot fail, not a covered stop. The process cannot observe either
deadline, so both stay declarations by the launcher, and an unusable value
refuses to start rather than dropping the comparison. The operation
checkpoint is only half of a reuse judgment: it proves the sidecar view reached
the payload, never that the stop itself was clean, and a checkpoint step the
deadline abandoned can still commit after the stop was reported unclean.

v1.1.7 fixes how an ambiguous SQL outcome is reported and recovered. A Hrana
HTTP or WebSocket request that ended with an unknown outcome now returns the
platform failure as structured fields (`operation_id`, `possible_commit`,
`retry_allowed`, `execution_proven`) instead of a plain-text error, including
the autocommit probe on a session already poisoned by an unknown write. A
request counts as dispatched only once its body was written; a connection that
was opened but never carried a written request is not ambiguous. Write cursors
without `RETURNING` now report `affected_row_count` and `last_insert_rowid`
with no rows, so the JavaScript SDK's `run()` no longer fails on ordinary
writes. The Authority staging path also stops cloning every body payload while
encoding and caps staging threads at 4; this is an internal performance change
and does not alter the `put`/`put_group` durability contract.

Recovery from a quarantined database is now reachable without weakening the
restart gate. `isolation/release` is exposed through the management API, the Go
and TypeScript SDK path allowlists and the `peri` CLI; an operation record that
is still quarantined without a release reports
`isolation_release_or_new_database_required`, and management calls outside the
recovery surface answer 409 `recovery_restricted` when the platform runs with
`PERI_RECOVERY_ONLY=1`. `dev.py --recover` starts only the Authority and a
SQL-disabled platform against an existing run directory, requires a clean
Authority startup marker and owned stop proofs, and refuses when the run already
passes the clean restart gate. The normal gate, stop proofs and ownership dirty
marker are unchanged; a released quarantine counts as resolved history but is
never permission to clear the marker.

This release also makes the P0 test set a publish gate. `.github/workflows/p0-gate.yml`
runs `scripts/p0-gate.sh` on every push and pull request and before any tagged
image build. The script verifies the pinned Turso gitlink, runs every
`prototypes/p0` test, and runs the pinned Turso SQL smoke with the fixed 64 MiB
hash budget; a failure blocks the image build, the manifest list and the GitHub
Release.

v1.1.6 changes how the Authority RPC carries object bytes: `Put` bodies,
`UploadSegments` batches and `Get` replies use base64 strings instead of JSON
integer arrays, and a new `put_group` session operation makes a group of
immutable segment bodies durable, so a 4 MiB object takes about 5.6 MB on the
wire instead of about 15 MB; the 32 MiB message cap is unchanged. The new
decoder still accepts the integer-array form and can read a message produced by
an older peer, but an older decoder cannot read base64 and an older Authority
rejects `put_group` before dispatch. `PROTOCOL_VERSION` remains 1, so this
release's Authority and Worker binaries must be deployed together: a mixed pair
is not rejected by a version check, and fails at the first object the older side
has to decode from the new encoder. A decode failure is an RPC failure, never
permission to replay SQL.

The ownership root advances to schema version 5, which adds the paged command
index, and the WAL index gains its receipt index and entry bounds on the first
append or truncate. Boot rewrites a version 1, 2 or 4 root to version 5, and
v1.1.5 and earlier reject these documents at open: they accept only root
versions 1, 2 and 4 and deny unknown fields. Opening a database with v1.1.6 is
therefore a one-way disk upgrade; rolling back to an earlier release needs the
pre-upgrade object store, not just the earlier binary. Command receipts beyond
the inline window and WAL receipt pages written by truncation are
content-addressed and verified on read, with a missing or altered page failing
closed. Legacy WAL receipts without a digest stay inline and are never archived;
once their chunks are truncated, a retry is refused with "WAL receipt content
unavailable" instead of being served from the baseline.

The maintenance scheduler is still off unless `PERI_MAINTENANCE_SCHEDULE=1`;
with the switch unset, maintenance cadence is unchanged and a tick is one static
read per loaded database. Enabling it opts into checkpoint and compaction lanes,
watermarks, cooldowns and budgets whose defaults are experiment inputs, not
measured production thresholds. One maintenance change is unconditional: a
passive checkpoint no longer takes the writer admission or the operations gate,
so admin, scheduled and pre-compaction checkpoints can run beside foreground
SQL; compaction still takes exclusive admission, and maintenance units remain
mutually exclusive. Per-database diagnostics always expose the maintenance
ledger and named block reasons, and a failed scheduled unit is logged to stderr.
`PERI_WORK_METRICS=1` adds WAL/index, lock, execution, result-size, maintenance
and authority publication/snapshot counters; with the switch off the field is
omitted rather than reported as zero. The `/v1/metrics` endpoint always reports
admitted, successful and rejected totals, named rejection reasons, a
successful-only latency account and operation-journal stage timings;
`action_latency_ns_total` still includes refusals. The existing default-off
diagnostics remain diagnostics: `PERI_SQL_OPEN_OBSERVER=1` streams phases for
slow requests, failures and a sampled share of fast successes; the phase threshold, sample
rate, window and ring capacity are the optional `PERI_SQL_OPEN_OBSERVER_PHASE_THRESHOLD_MS`,
`PERI_SQL_OPEN_OBSERVER_PHASE_SAMPLE_PERMILLE`, `PERI_SQL_OPEN_OBSERVER_PHASE_WINDOW_MS` and
`PERI_SQL_OPEN_OBSERVER_PHASE_CAPACITY` knobs documented in
`request-phase-observation.md`. `PERI_DIAG_SLOW=1` requires
`PERI_DIAG_DIR`, and `PERI_ADMISSION_TRACE=1` or `PERI_DIAG_GROUP_PHASE=1` adds
trace detail. None of these switches changes SQL, WAL, publication, audit or
recovery decisions.

The Go SDK and CLI add `Options.ConnectionReuse` and `--connection-reuse`, both
default off. Reuse cuts connection churn. A request whose cancellation or write
fails before the request body was written is answered as a proven non-dispatch
rather than an unknown outcome; a request that completed the write and then lost
its reply remains `outcome_unknown`, so callers must resolve that state with
durable operation records or business idempotency. Shutdown retries the worker
retirement sample within its bounded budget, and a failed launcher restart
preserves prior clean evidence and failure records instead of overwriting them.
The restart gate itself is not relaxed: it still requires the signed clean stop
record, the worker stop proofs, a clean terminal proof, a materialized operation
view and no unresolved operations (a quarantine ended by `isolation/release`
counts as resolved history, not as an open isolation). Experimental Hrana remains default-off with
its documented gaps, and this release claims neither an SP01 pass nor a capacity
result.

## v1.1.10

v1.1.10 raises the default request rate of a newly created project from 100 to
500 requests per second, so a first browser-host startup (wasm host, session
creation and session listing) no longer exhausts the token bucket and turns into
`429 quota_exceeded`. The other defaults are unchanged: 8 databases, 16
sessions, 4 concurrent requests, 64 KiB SQL and 4 MiB result. A project persists
the quotas it was created with; this release does not migrate or rewrite an
existing project's stored quotas.

Hrana failures now carry the platform's sanitized `message` instead of repeating
`code` in both fields. HTTP v2/v3 pipelines, HTTP protobuf and v3 JSON cursors,
and the WebSocket adapters share the classification, including condition and
autocommit probes and cursor describe/open. Worker prose remains in the Gateway
log and is never forwarded. Codes, statuses, execution evidence and the
`outcome_unknown` abort rule are unchanged, and SQL is still never retried
automatically.

### Proof-based post-ready recovery (S05)

The authority now has a separate recovery state machine for a crash after
readiness was announced. `peri-loom-v1 authority ... --recovery-evidence PATH`
evaluates a strict, versioned JSON attestation, and `--recovery-plan-out PATH`
writes the machine-checkable plan: path, per-step evidence and outcome,
operator actions, pending unknown operations, and replay policy `never`. The
evidence schema is `RecoveryEvidence` in `src/authority_rpc/recovery.rs`;
unknown fields are rejected. A dirty marker is still fail-closed. Without
evidence the authority refuses; `--reconciled` remains an explicit legacy
confirmation, cannot be combined with evidence, and cannot take over a dirty
seal.

The plan separates three cases. `readiness: not_announced` is an interrupted
startup: the authority refuses and directs the operator to the launcher
`--recover` path; it never rolls back a marker itself. `readiness: unknown` is
not treated as pre-ready. `readiness: announced` is a post-ready crash and may
start only in a restricted recovery profile after the exclusive state-directory
lock and the launcher's exact argv/PID record establish the old process stop,
and after the evidence binds the old seal owner/etag, the recovery journal and
terminal, the launcher-verified audit head, read-back of every confirmed write,
and the state floor/root revisions. Local process evidence is accepted only for
the local backend. S3 additionally requires a provider read-back bound to the
old seal owner/etag; that remote record is an operator attestation, not a
cryptographic proof that no in-flight data write can still land.

The recovery profile admits only metrics/inspection, database state, session
state, close/retire and shutdown RPCs. Challenges, grants, renew/revoke,
opens, session work, maintenance and imports are rejected with
`recovery_restricted`; unknown operations stay pending and are never replayed.
It is an authority-only gate: it does not start or admit the platform, workers,
or SQL, and their clean gates are unchanged. A dirty seal is replaced only by a
conditional write that matches the evidence-bound owner/etag, and the
replacement is always written dirty, so a late old owner cannot publish a seal.
The operator verifies the restricted state, stops it cleanly, and then starts
normally.

Not covered: automatic evidence generation by `dev.py`, real SIGKILL injection
at the intent/commit/terminal/audit/response boundaries (the tests use
state-machine fixtures), cryptographic S3 quiescence, and multi-authority/HA.
Rust validates the evidence record's internal consistency and the live
seal/lock; it does not re-read the platform state, journal or audit files, so
the launcher/operator must still produce and verify those digests. The
launcher's clean restart gate, ownership markers and fencing rules are
unchanged.

## v1.1.9

v1.1.9 makes a local-backend volume created by this release survive normal stop,
restart and version roll-forward without being locked out by launcher state. The
launcher normalizes optional `launch.json` fields at one entry point
(`wal_replay_bytes` defaults to 0 when absent) and no longer writes a sticky
restart-failure record for configuration, runtime-identity or listener refusals
that happen before any service starts. A new volume checks every listener before
writing `launch.json`, `platform.json`, keys or the Authority marker, so releasing
a busy port and retrying the same run directory succeeds.

An interrupted startup is covered by a signed startup intent persisted before each
`Popen` (`starting`, a unique nonce and the complete argv), with PIDs written back
immediately after spawn. Normal start refuses an attempted startup and preserves
canonical evidence. `--recover` can roll an interrupted local-backend startup back
to the preserved clean point only when every proof (terminal HMAC, stop record,
owned proofs, state/floor, journal, backend manifest, Authority root data fields,
live marker and seal) matches. It writes back only preserved bytes, starts with
SQL disabled, and otherwise fails closed with an explicit operator instruction.
Residual processes are matched by exact argv, so a lookalike command line is not
killed and PID reuse does not authorize a signal.

A platform spawn window with an unmaterialized operation view still fails closed:
stop the surviving platform and Authority gracefully, let the shutdown checkpoint
materialize the view, then rerun `--recover`. Rebuild the volume only if the
checkpoint is still absent after a joined stop.

Volumes created before v1.1.9, including volumes already locked by an older
launcher, are not migrated and need a rebuild. Interrupted startup on S3, a
SIGKILL after readiness was announced, and changed data fields remain outside
startup rollback. The security boundary is unchanged: SQL is never retried
automatically, a timeout, close or read-back is not cancellation proof, and no
marker, proof or quarantine record is cleared to force startup. This release
claims neither a capacity nor a race result.

## v1.1.8

v1.1.8 fixes failure reporting on the terminal path and makes the full SDK
matrix a release gate. A statement failure is no longer overwritten by a later
close failure: R1 keeps the first terminal failure and reports a failed close as
a separate `cleanup_error`. D1 renders the redacted `error.message` in the CLI
and SDKs; an `outcome_unknown` result also shows `operation_id`,
`may_have_committed` and `execution_proven`. D2 closes the session on settled
error paths; `outcome_unknown` and `session_moved` remain opaque, with no
reconnect and no replay.

E1 remains an explicit known gap: an HTTP v3 write-cursor abort does not provide
an automatic rollback proof, and there is still no end-to-end protocol
acknowledgement before the client may read again, so the default matrix executes
26 of 27 scripts per round. The adapter now records a
queryable cursor lifecycle on the operation journal:
`cursor_client_stopped` and `cursor_dispatch_stopped` describe what the adapter
stopped doing, and `cursor_native_outcome` is `rolled_back` only when the worker
acknowledged that a live cursor was aborted, `committed` only when a done page
was delivered, and `unknown` when neither proof exists. An `unknown` close
quarantines the namespace, and the resulting `409 database_isolated` names the
`operation_id` and `possible_commit` that must be queried; the adapter never
reads a fetch abort as a rollback and never reconnects or replays. Two rounds
still produce 52 passed runs and 2 known-gap skips.
`PERI_HRANA_INCLUDE_KNOWN_GAPS=1` executes E1 but does not guarantee a pass.
This release is not a full Hrana compatibility claim. A failing
`http-abort-write-cursor-live.mjs` can leave its fixture quarantined; do not
reuse that fixture or retry an unknown write.

CI runs the P0 gate and the full SDK matrix before an image build, and retains
the redacted matrix evidence as an artifact. The security boundary is unchanged:
SQL is never retried automatically, a timeout, close or read-back is not treated
as cancellation proof, ownership and quarantine markers are not cleared, and TLS
verification is not disabled.

## Build, test, and SDKs

```sh
cd prototypes/v1/platform/frontend
bun install --frozen-lockfile
bun run build
cd ..
go build ./cmd/peri-platform ./cmd/peri
PERI_RUST_BINARY=/absolute/path/to/peri-loom-v1 go test ./...
PERI_RUST_BINARY=/absolute/path/to/peri-loom-v1 PERI_P6_VERIFY_RACE_CLI=1 go test -race ./...
node --test tests/ui-json.test.mjs
cd sdk/typescript
PATH="../../frontend/node_modules/.bin:$PATH" bun run build
bun run test
```

The TypeScript build expects `tsc` on PATH; generated `dist/` is not committed.
See `sdk/README.md` for Go/TypeScript and `../src/sdk.rs` for the Rust SDK.
For a live tenant/role/quota/revocation API check, run
`python3 tests/user-api-live.py --connection /private/fresh/connection.json --output /private/fresh/user-api-results.json`
from this directory. The input must be the launcher's private TLS connection file;
the output must not exist. This creates two test projects and databases, never
replays an uncertain request, and leaves first-failure state for inspection.
The management CLI reads credentials from a private configuration file or
environment, never command-line secret arguments. SQL shell reconnection is
explicit and does not replay transactions. Deployment manifests live in
`../deploy`; their one-container/private-loopback topology is intentional.
