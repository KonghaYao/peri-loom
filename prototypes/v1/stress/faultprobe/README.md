# Bounded architecture fault probe — source-only implementation

Standalone Go standard-library module. The parent stress harness remains frozen.
Only mock HTTP/unit source gates may run during development. **No real probe,
process signal, storage mutation, load or benchmark has been executed.** Main
must confirm the actual P0–P6 joint PASS before a future real invocation.

## Capabilities and limitations

- `owner_crash`: one persistent Gateway writer, 1–32 acknowledged business-ledger
  transactions, an explicit ACK barrier with **zero in-flight SQL**, SIGKILL of
  the actual Management-confirmed owner, removal of only that worker's private
  fixture cache, bounded Management status observations until a different owner
  has a strictly newer epoch, a new opaque Gateway session, full ACK ledger and
  account-balance comparison, `PRAGMA integrity_check`, then one fresh ledger
  write and another verification. This is an ACK-boundary architecture probe,
  **not an in-flight crash or throughput benchmark**.
- `metadata_starvation`: exactly three supplied DBs; explicit active transaction
  and uncommitted update on DB0; one migration request each for DB0/DB1; bounded
  third-DB ledger writes, reads/integrity checks and placement observations.
  Records client-side migration-workflow pending intervals and third-DB ACKs.
  A pending HTTP workflow is not proof that the controller is blocked inside
  its global operation mutex. No SQL or migration request is retried.
- Placement `grant.binding.renewal_sequence` is recorded losslessly, with scope
  **controller placement metadata**, not proof of applied Authority lease
  renewal. No public typed terminal migration-refusal or applied-lease oracle
  is currently available. The scenario cannot claim a whole starvation PASS:
  generic `operation_incomplete`/lost replies are UNRESOLVED; missing oracles
  explicitly SKIP if no uncertain failure occurs. Successful migration of DB0
  while its transaction is held fails the blocking expectation.
- Old-worker revival under the original grant: explicit **SKIP**; no original
  grant replay route exists in Gateway/Management. The tool does not launch
  processes, call private worker RPCs, invent a grant or pretend this passed.
- In-flight crash ambiguity, Authority failure/HA and user RustFS faults: not
  exercised. Authority/platform/launcher are never signal targets.

## Main-owned private fixture

Use the **latest ROOT `prototypes/v1/platform/dev.py` launcher**. Its single
worker exit must keep Authority, platform and the remaining worker alive for
Controller reconciliation; Authority/platform loss is still fatal, not HA.
`launcher_pid` is accepted and excluded from all signal targets.

Main provisions a new canonical private directory whose basename starts with
`peri-stress-` and is at least 24 characters, containing:

- The actual fresh launcher `topology.json` (0600): `launcher_pid`, `processes`
  with exactly authority/platform/worker-a/worker-b PIDs, `gateway`, `authority`,
  `broker`, and the two actual worker URLs.
- A private probe connection JSON (0600), using launcher connection fields:
  `endpoint`, `project_api_key`, `global_api_key`, `database`, `ca_file`.
  Optional `databases` contains **two additional** distinct DatabaseIDs for
  starvation. Endpoint equals `topology.gateway`. Each DB's namespace database
  name starts with `<fixture-directory-basename>-`; branch is `main`. Main must
  explicitly provision these dedicated DBs. The launcher's default `demo` DB
  is intentionally rejected. The tool never provisions/deletes databases or
  modifies launcher/bucket/config state.
- Existing private `cache-a`, `cache-b` directories for the two workers.
- Main's private joint PASS evidence file, with version1, `joint_pass:true`,
  `confirmed_by:"main"`, exact actual Git HEAD in `source_git`, P0…P6 stages,
  and RFC3339 `observed_at`. This is a trusted operational gate, not a signed
  attestation of all dirty/untracked sources or binary provenance.

The run directory, both input JSON files and cache directories must be canonical
and private; root/cache ownership must match the caller. Input files sit directly
in the fixture directory. HTTPS verifies system roots plus the optional fixture
CA; HTTP is accepted only for literal loopback. All service URLs are loopback.

The runtime serial lock interoperates with the parent harness. A permanent
`.faultprobe.used` O_EXCL marker and fresh `faultprobe/` output directory prohibit
reusing a fixture, even after failure. Never remove these to reuse a run.

Before any API work, both workers' actual PID/UID/start identity and complete
argv must match exactly:

```text
<exact-rust-peri-loom-v1-path> owned-worker <topology-authority-url> <exact-fixture-cache-path> worker-a|worker-b <topology-listen-host:port>
```

Both identities are inspected; only the current owner is eligible for SIGKILL.
Identity is rechecked immediately before signalling. Removal follows proven
actual PID absence proven by an ESRCH probe, an unchanged cache-directory identity and
a bounded scan rejecting symlinks/special files (>10,000 entries or >256MiB
is rejected). Cache deletion never touches Authority/backend/platform paths.
Portable PID/start checks have a residual signal-boundary TOCTOU and `ps` start
time granularity limitation; this is a trusted private fixture tool, not a
hard defense against a malicious same-UID process or filesystem mutator.

### Post-signal disappearance observation

The two pre-signal checks still require complete PID, caller UID, process start
and exact owned-worker argv. An incomplete identity at either boundary prevents
the signal. After the one authorized signal, observation has a five-second
deadline inherited from the parent context; it never sends another signal.
Transient `ps` failures, missing argv and zombies are observations to wait on,
not permission to signal or delete. Available PID/UID/start metadata and argv
prefixes are checked on every observation; a visible mismatch stops immediately.
A complete live identity must match all four identity components exactly.

`ErrProcessGone` is reserved by the Host contract for an actual PID probe returning
ESRCH. A failed `ps`, an empty result, EPERM and zombie status do not prove absence.
Zombie metadata remains available for reuse checks, but its defunct command text
is not treated as live argv. Observed metadata is retained even if a subsequent
probe returns ESRCH, so a visible reuse cannot be hidden by that later absence.

Only proven absence can advance to the existing canonical path, cache size,
no-symlink and directory-identity checks. The same wait deadline and parent
cancellation are checked immediately before removal. Timeout, cancellation,
identity mismatch or evidence failure leaves the cache untouched; it does not
prove that the worker survived or that the earlier signal was canceled. Do not
retry the signal or rerun a used fixture to turn an uncertain result into success.

Evidence distinguishes `fault_signal_sent`, `fault_process_wait` (same identity,
incomplete identity, unavailable inspection or zombie), `fault_process_gone`
(Host ESRCH proof), and `fault_exit_uncertain`. No new evidence contains raw argv,
environment values or inspection error details. `fault_completed` remains the
separate record after actual cache removal; signal success alone is not completion.
The safety decision retains the existing trusted-fixture check/use TOCTOU limits;
these observations are not a portable pidfd or an atomic process/filesystem fence.

The wait-regression gates use synthetic Host observations and temporary fixture
caches only. They never execute `LocalHost`, send an OS signal or invoke a deployed
Gateway/Authority API. Run the complete standalone module mock gates with
`go test ./... -count=1`, `go test -race ./... -count=1`, and `go vet ./...`.

## Evidence and stop semantics

Private `faultprobe/events.jsonl` records actual source HEAD/dirty indication,
request IDs, monotonic durations, observed wall timestamps, typed failures,
actual Management snapshots, ACK business IDs and deltas, expected/actual
ledger hashes, balances and integrity output. Responses are bounded to 1MiB,
SQL to64KiB, requests to512 per credential client, samples to32, evidence to8MiB,
with bounded HTTP/recovery/observation deadlines and a three-minute total run
deadline. This is not a process RAM cap or a hard OS/filesystem deadline.

Credential/token/password/lease/grant/session fields and matching known secret
strings are redacted recursively. JSON numbers retain int64/uint64 precision.
Raw argv/environments/request SQL/headers are not logged. Missing native
diagnostics are accepted explicitly for recovery, never fabricated as success.

Unknown SQL or management results stop subsequent SQL, rollback and close;
already-dispatched calls are cancelled and recorded, not retried. Known terminal
HTTP400 `sql_error` requires execution_completed=true + may_have_committed=true
and keeps the session usable without implying rollback or no committed prefix.
After intentional owner kill, the old Gateway session is abandoned without an
unsafe close; no permit-reclamation guarantee is made. An uncertain starvation
run may leave its transaction open; main owns teardown of that isolated fixture.
No helper silently rolls back an uncertain session or signals other services.

## Build gates and future execution

```sh
go test -race ./...
go vet ./...
go build -o peri-faultprobe ./cmd/peri-faultprobe
```

Compiled tools and mock tests are **not** executed architecture evidence.
Only main, after joint PASS, may invoke with `-execute`, `-run-dir`, `-connection`,
`-topology`, `-source-root`, `-joint-pass`, `-rust-binary`, `-case owner_crash`,
and explicit `-allow-worker-sigkill`. Choose one case per fresh launcher fixture.
For observation-only starvation use `-case metadata_starvation` and three DBs;
no worker signal authorization is needed and no worker is killed.

CLI `-list` is network-free. No automatic readiness SQL, reconcile mutation,
restart, retry, grant replay, benchmark or performance conclusion is provided.
