# P6 launcher independent security review

## Scope and evidence

**Latest disposition:** the final TLS/signal-session closure at the end of this report supersedes earlier source snapshots and frozen dispositions. B1–B4, duplicate-field rejection, development CA/leaf generation and child signal-session isolation have narrow closure evidence. This reviewer actually reran runtime **8 PASS** and protocol **10 PASS**. Wider S3/SDK/Docker results are attributed to existing main/C artifacts, not new runs by this reviewer. No new blocker was found in this bounded review; this document is finally frozen.

- Read-only review of `/Users/konghayao/code/ai/peri-loom/prototypes/v1/platform/dev.py` and the adjacent service/controller/startup code. No production changes, services, builds, heavy gates, real credentials, or network requests.
- Reviewed launcher SHA256: `204d067f792d1a311fd9b53e9f3c721fe7be3a83f7ad4fea9a56cba87d465c56`.
- Main changed supervision and live-platform shutdown handling during this review. Findings below refer to the hash above, not the earlier implementation.
- Evidence includes small in-memory Python fake-process/HTTP-response probes. These are not full project lifecycle or live failover acceptance tests.

## Initial verdict (superseded for the three fixes below)

Not ready for a clean-shutdown/security acceptance claim. The worker-degraded supervision policy and checked live-platform wait are now present, but already-exited control-plane processes can still escape cleanup validation. S3 identity, redirect handling, and RPC-response identity also require attention.

## Blockers

### B1 — P1: already-exited platform can still produce launcher exit 0

Locations: `dev.py:193`, `dev.py:202`, `dev.py:205`, `dev.py:216`, `dev.py:250`.

The new `platform.wait(timeout=15) != 0` check only applies when `platform.poll() is None`. A platform already exited with status 1 is skipped. Authority and worker processes are likewise skipped when already exited.

Concrete race: SIGTERM/SIGINT sets `stop`; platform exits 1 before cleanup. The next `stop.wait()` returns true, so the supervision loop exits without checking the platform status. Cleanup skips the already-dead platform, and otherwise-successful cleanup allows `main()` to return 0. This loses the Go shutdown/outcome-unknown error that the launcher is supposed to preserve.

Bounded reproduction against the reviewed source:

```text
FakeProcess.poll() = None; FakeProcess.wait() = 1 -> cleanup raises RuntimeError
FakeProcess.poll() = 1; FakeProcess.wait() = 1    -> cleanup returns normally
```

Required policy: inspect/reap every started platform/authority regardless of its current liveness, preserve any nonzero exit or missing terminal proof, and never classify an unexpected pre-shutdown control-plane exit as clean merely because its code is zero. Record worker crashes separately from control-plane fatal exits: continuing in degraded mode is correct, but an already-dead worker is not itself evidence of a clean worker shutdown. Do not clear authority dirty state or manufacture a terminal proof.

### B2 — P1: persistent S3 run identity is not bound to its backend

Locations: `dev.py:82`, `dev.py:90`, `dev.py:158`; actual S3 configuration inputs: `src/config.rs:21`.

`launch.json` retains only the backend kind (`s3`), not the endpoint/bucket/region identity. Every restart forwards current `S3_*` environment values while reusing the old authority marker, controller placements/epochs, platform catalog, key descriptors, and caches. Changing `S3_ENDPOINT`, `S3_BUCKET`, or effective `S3_REGION` silently points the same local control state at a different durable store. The launch-config comparison does not detect it. Root-incarnation checks may reject later operations, but that is not a pre-start backend identity check and does not justify starting against a different target.

Required policy: persist and validate a canonical non-secret backend identity before starting any child, including endpoint/path, bucket, and effective region. Credential rotation for the same backend must remain possible; do not persist or compare plaintext access/secret/session tokens as identity. Existing S3 launch records lacking this binding must fail closed pending explicit operator confirmation, not auto-infer whichever current environment happens to exist. No unknown mutation retries.

Related operator prerequisite: first-run S3 bootstrap always provisions `local_default/demo` (`dev.py:181`) in the supplied bucket without establishing that this namespace has never been used. A new private run directory also supplies a new local authority lock; it does not prove exclusive authority ownership of a shared remote backend. P6 requires operator proof of fresh namespace, and P4 requires external exclusivity/quiescence. The launcher must not present `ready` as proof of either, nor automatically use `--reconciled` or reclaim an existing namespace.

### B3 — P1: default HTTP redirects can disclose service/API credentials

Locations: `dev.py:42`, `dev.py:48`, `dev.py:51`.

The opener disables proxies but retains urllib's default redirect handler. A 302/303 can convert a dispatched POST to GET at another destination while retaining Authorization. This affects bootstrap/global/project credentials and shutdown control tokens. TLS validation of the new destination is not a substitute for destination identity.

Bounded, no-network reproduction using `HTTPRedirectHandler.redirect_request`:

```text
POST http://127.0.0.1:9001/v1/reconcile
302 -> http://127.0.0.1:9002/other
redirected request retains Authorization: True
```

Required policy: reject all redirects explicitly and treat the outcome of a previously dispatched mutation as uncertain. Do not follow it, retry at the redirected URL, or replay the command under a fresh request ID.

### B4 — P2: request helper accepts mismatched RPC replies and truncated oversized bodies

Locations: `dev.py:42`, `dev.py:54`, `dev.py:57`.

`request()` sends a request ID but never validates the returned ID or version; it checks only the truthiness of `ok`. It also does not enforce `error == null` on success or a boolean `ok`, and ordinary `json.loads` accepts duplicate fields. The read is capped at 32 MiB + 1, but there is no explicit length rejection or EOF proof. A valid small JSON object followed by enough spaces can therefore hide additional bytes beyond the cap.

No-network mocked opener reproduction:

```text
request_id sent: expected
reply: {"version":99,"request_id":"wrong","ok":true,"result":"accepted","error":null}
request() returns: accepted
```

A mismatched reply cannot establish that reconciliation/bootstrap/shutdown completed. Required policy: strict size/complete-body checks, duplicate/extra-field policy for the correct envelope, exact version and request ID, boolean success, and internally consistent result/error. Preserve historical uncertainty on malformed responses; no automatic retry of an unknown outcome.

## Additional risks and readiness gaps

### R1 — P2: readiness is a stdout hint, not a verified identity

Locations: `dev.py:132`, `dev.py:145`, `dev.py:149`, `dev.py:174`, `dev.py:185`.

`start()` accepts the first JSON line with truthy `ready`; it does not compare service identity, bound address, or process liveness after parsing. A child that emits readiness and immediately dies can be accepted temporarily. The final project check performs reconcile and database status, not an encrypted SQL/key/native-session round trip. This should be described as topology/control readiness, not full database readiness.

Minimum recommended bounded checks: require the expected service/address and a still-running process, then verify the relevant authenticated non-mutating readiness/state interfaces. Be precise about the proof obtained; do not add a speculative SQL/bootstrap retry to compensate for an ambiguous startup command.

### R2 — P2: private launcher artifacts lack complete crash/ownership checks

Locations: `dev.py:18`, `dev.py:26`, `dev.py:67`, `dev.py:82`, `dev.py:104`, `dev.py:128`, `dev.py:175`, `dev.py:188`.

- Newly written JSON/signing files use exclusive create, 0600 and file fsync, which is good. Their parent directory entries are not fsynced. `private_json` writes directly to the final pathname, so an interrupted write leaves a partial file rather than an atomic published record.
- Existing `launch.json`/`connection.json` are loaded without regular-file, ownership, private-mode, size, or symlink checks. Go validates `platform.json` and the signing/TLS keys, but that does not validate these other files.
- `run_dir.resolve()` dereferences the supplied symlink before the later directory symlink check. Ancestors and existing log-file symlinks are also not checked; log open lacks `O_NOFOLLOW` and does not revalidate existing modes.
- There is no launcher-wide lock or genuine-empty-directory requirement before initial configuration creation. Concurrent launches can race during preparation/build/artifact creation even though downstream stores have their own locks.
- A crash after creating an API key or database but before persisting `connection.json` leaves bootstrap outcome ambiguous. A later startup can issue another fresh API-key creation. This is not a durable single-operation bootstrap workflow, and must not be advertised as automatically safe recovery.

Recommendation: private no-follow ownership checks and atomic durable artifact publication, exclusive preparation lock, and explicit recovery-required diagnostics for partial/bootstrap-uncertain state. Do not reset state, regenerate identity in a nonempty run directory, delete preexisting keys, or introduce automatic unknown-operation retries.

## Confirmed positive properties

- Current supervision treats authority/platform death as fatal while reporting each worker death once as degraded (`dev.py:193`). It does not automatically restart a failed worker. This permits the existing controller to reconcile remaining capacity without replaying SQL.
- Controller `Run` survives individual reconciliation errors unless context cancellation or metadata failure is terminal (`controller/reconcile_loop.go:123`); active placement selection supports replacement of an unhealthy worker (`controller/reconcile_loop.go:71`). This is source evidence only, not a live failover result.
- Live platform nonzero wait now propagates an error (`dev.py:208`); live Rust children have checked wait results and bounded kill fallback. Platform is stopped before workers and authority.
- `safe_environment` limits inherited environment; only authority receives S3 credentials. Worker authority/control/SQL/key credentials are separately generated. Readiness/topology output contains paths/PIDs/endpoints, not these credentials.
- Service configuration validates private files, distinct credentials, loopback broker binding, and TLS except explicitly insecure loopback (`service_config.go:32`). TLS files are not silently recreated if explicitly configured but missing.
- Authority startup preserves the dirty marker and requires explicit quiescence confirmation after an unclean shutdown (`src/authority_rpc/startup.rs:53`); launcher does not pass `--reconciled`. Clean marking is gated by quiescent authority shutdown (`src/authority_rpc/server.rs:126`).
- Launcher generic request failure explicitly warns against blind retry. Preserve that behavior while closing the validation/redirect gaps.

## Acceptance recommendations for main

Small isolated fake-process/HTTP-handler tests are sufficient for the launcher defects; no heavy joint gates are needed for this review:

1. Stop-event + already-dead platform status 1 must yield nonzero launcher outcome; unknown Go shutdown error must never become clean.
2. Authority death is fatal; worker death reports degraded once without killing platform/remaining worker. Final cleanup distinguishes a crash from proven clean exit.
3. Restart with different non-secret S3 identity fails before any process spawn; same-target credential rotation is allowed.
4. Redirect responses are not followed and credentials never reach the second destination.
5. Wrong version/ID, duplicate/contradictory fields and oversized padded replies cannot count as acknowledgements.
6. Partial private state/bootstrap requires explicit operator recovery, never automatic reset or unknown mutation replay.

No production edit or heavy gate was performed. Findings apply to the recorded source hash; later main patches need their own narrowly scoped confirmation.

## Final read-only recheck after main's fixes

Rechecked launcher SHA256: `4d6d7d2d5cb961316852d04592aaf4f5d9d6aaf77087f400bc716207bf303cf9`.

The requested three fixes are present and their original failure reproductions no longer succeed:

- **Platform nonzero exit propagation: PASS.** `dev.py:218` now handles both live wait and already-exited platform status. Independent fake-process probes reject status 1 in both cases. The original B1 platform-specific failure is closed.
- **Redirect credential forwarding: PASS.** `dev.py:18` implements `NoRedirect`; `request()` installs it at `dev.py:59`. The independent cross-destination redirect-handler probe returns no redirected request. No network request or credential forwarding occurred.
- **Persistent S3 identity: PASS.** `dev.py:23` records endpoint/bucket/effective region without access/secret/session credentials. `dev.py:97` rejects changed target or missing identity on an existing S3 launch. Independent in-memory `prepare()` probes rejected endpoint, bucket, region, and legacy-missing-identity cases before any child spawn; same-target secret rotation was accepted. Exact string equality can conservatively reject equivalent URL spellings; that does not silently rebind the store.

Additional actual focused validation: `PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-runtime.test.py` completed **5 tests, OK** in 0.004s. These use synthetic credentials and temporary private files; they perform no build or service startup. The independent probes above used no filesystem writes or network.

### Remaining blocker: already-exited authority is still skipped

`dev.py:230` still uses `if not process or process.poll() is not None: continue` for authority and workers. An already-exited authority with code 1 is therefore not inspected by cleanup. Independent fake `authority.poll() = 1` / `returncode = 1` still makes cleanup return normally.

The same stop-event race described in B1 remains possible for **authority**, although it is fixed for platform: stop ends supervision before its fatal-child check, authority exits with an unsettled-operation/dirty error, cleanup skips it, and otherwise-successful cleanup can yield launcher 0. Preserve authority nonzero/unknown terminal proof regardless of whether it exited before cleanup. The worker-degraded policy need not be reversed; do not treat a worker crash as reason to kill remaining serving capacity.

### Remaining validation gap and operator prerequisites

- **B4 remains open:** `request()` still accepts mismatched version/request ID and lacks strict explicit oversized-body/duplicate-envelope rejection. The earlier mocked wrong-ID/version reproduction still applies to its unchanged decode/check path. These responses cannot establish a shutdown or mutation acknowledgement.
- R1/R2 and the external fresh-namespace/exclusive-authority prerequisites remain as documented. The S3 identity fix prevents accidental target rebinding, but is not proof of freshness, backend quiescence, or HA exclusivity.
- Supervision remains correctly worker-degraded and control-plane-fatal (`dev.py:206`). This review confirms that source policy; it does not claim a live worker-death failover run or full encrypted SQL readiness.

**Final outcome:** the three requested fixes have actual narrow PASS evidence, but this is not an unconditional clean-shutdown/security approval while already-exited authority errors and malformed-response acknowledgement remain unhandled. No production source, shared security code, or runtime retry policy was changed by this reviewer.

## Final B1–B4 closure recheck and freeze

Latest reviewed `dev.py` SHA256: `4112a5ec5911d86ee095072ddac0ab308e36278d19bdf8867ab4d59940feb418`.

This section supersedes the preceding residual-blocker assessment:

| Finding | Latest disposition | Actual evidence |
| --- | --- | --- |
| B1 exit-code/terminal-proof loss | Closed for identified nonzero-exit scenarios | Live/already-dead platform nonzero is rejected; `dev.py:239` now rejects already-dead authority and both workers with nonzero status. Runtime tests cover these identities. |
| B2 S3 backend identity rebound | Closed for identified restart-rebind scenarios | Endpoint/bucket/effective region binding remains persisted and checked; prior independent probes covered changed values, missing legacy binding and same-target credential rotation. Credentials are not part of identity. |
| B3 redirect credential forwarding | Closed | `NoRedirect` remains installed in the real helper; independent fake-response tests verify its presence and no destination request for 301/302/303/307/308. A redirect error causes exactly one dispatch and an unknown-outcome error, not retry. |
| B4 mismatched/oversized acknowledgement | Closed for requested protocol scenarios | `dev.py:65` rejects bodies over 32 MiB; exact expected request ID/version and `ok is True`/null error checks are present. Independent tests reject wrong version/ID, success-type/error inconsistencies, oversized valid JSON plus padding, nonobject/trailing JSON replies. |

Actual commands and results:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-runtime.test.py
Ran 6 tests in 0.006s — OK

PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-protocol.test.py
Ran 8 tests in 0.008s — OK
```

The newly authorized independent test file is `prototypes/v1/platform/tests/dev-protocol.test.py`, 87 lines, SHA256 `c981cc4e89dc5987a4306bc8d900fac0c67b1df4ab2c57641bcc7ee3e71cd174`. It uses an in-memory HTTP-response stream and mocked opener; it does not bind a port, issue network requests, launch processes, build binaries, or use real credentials. Every success/failure helper path asserts one opener call, preserving the no-unknown-retry boundary.

### Nonblocking hardening suggestion: reject duplicate JSON object fields

The helper still uses standard `json.loads`, which accepts duplicate keys with the last value winning. A no-network probe of `{"version":2,"version":1,"request_id":"review-request","ok":true,"result":"accepted","error":null}` is accepted by the current helper. Recommend `object_pairs_hook` that rejects duplicate fields, recursively, if main elects to make the development helper as strict as the production services. This is reported as a hardening suggestion, not a new acceptance blocker in this bounded closure review. No production change or failing duplicate-rejection regression was introduced by this reviewer.

The earlier freshness/external exclusivity, artifact durability/ownership and exact readiness-scope limitations remain documented; these passes do not prove HA, fresh remote namespace, encrypted SQL readiness, or live worker failover. Worker supervision still reports degraded without terminating remaining workers/controller. No automatic unknown-operation retry was added.

**Frozen:** B1–B4 requested failure scenarios are closed; no new blocker found in this final bounded review. Only this report and the explicitly authorized independent test were written. Production launcher and original security-owned sources were not edited by this reviewer, and no heavy gate was rerun.

## Duplicate-field hardening closure — final freeze

Latest reviewed launcher SHA256: `33fec8f75f5fc9be375ccd1760cb13c4d94853983dfc6dacd80e908e5f8fcfe5`.

Main added `unique_object` and passes it to `json.loads` as `object_pairs_hook`. The previous nonblocking duplicate-field suggestion is now closed: all object nesting levels reject duplicate names as unknown outcomes.

Only the owned `tests/dev-protocol.test.py` received two additional test methods, with nine duplicate-field subcases: envelope version (including identical values), request ID, ok, error; nested ok; deeper nested ok; objects inside arrays; and equivalent names encoded with a Unicode escape. Every failure is checked for `RuntimeError` with duplicate/unknown wording, and the existing fake-opener fixture asserts exactly one dispatch, no retry. Existing positive and redirect/identity/size cases remain passing.

Actual focused runs on the updated production source:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-protocol.test.py
Ran 10 tests in 0.009s — OK

PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-runtime.test.py
Ran 6 tests in 0.006s — OK
```

Latest independent protocol test: 110 lines, SHA256 `7ddc9a154cc69e5ec938de809bc43255f9f0263bc884c28a85aa1aa7c5219007`.

**Final frozen outcome:** B1–B4 closure remains supported, duplicate-field rejection is independently verified, and no new blocker was identified in this bounded recheck. No production/security source edit, real network request, service start, heavy gate, or unknown-operation retry was performed by this reviewer. Earlier deployment/operator limitations remain documented and are not expanded into HA/freshness/live-failover claims.

## Final TLS and terminal-signal closure

This section is the latest source disposition and supersedes the earlier 6-test runtime results. Only this report was edited in this final review; production/security sources, tests and README were read-only.

### Development TLS: proper trust anchor and server leaf

Source inspected: `prototypes/v1/platform/dev.py:145`.

Main replaced the old single self-signed gateway certificate recipe with a separate disposable development CA and signed server leaf. The CA has critical `CA:TRUE` and `keyCertSign,cRLSign`; the leaf has critical `CA:FALSE`, digital signature/key encipherment usage, `serverAuth`, and localhost/loopback/listener-IP SANs. CA and leaf private keys are chmod 0600 inside the private runtime keys directory. Generated trust identity is stored in `launch["ca_file"]`, used by the launcher's verified HTTPS bootstrap calls, and copied into `connection.json` for clients (`dev.py:166`, `dev.py:215`, `dev.py:225`). Explicitly supplied certificates use `PERI_TLS_CA` when provided.

The earlier Rust SDK failure artifact `/tmp/peri-p6-sdk-live-rust-failed-ca-endentity.log` records `CaUsedAsEndEntity`: a certificate acting as a CA was being used as the server end entity. This is a certificate-role error, not grounds for disabling verification. The current recipe establishes the two distinct roles instead of bypassing chain/hostname checks.

Actual runtime test execution in this review invokes OpenSSL to generate the chain and verify the signed leaf with its CA. It checks leaf `CA:FALSE`, TLS server-auth usage, loopback IP SAN and both private-key modes. This is a real local certificate-generation/verification test, not a mocked TLS result; it does not start the application cluster or establish a live SDK HTTPS connection.

Existing launch/certificate material is not automatically rewritten by this review. These tests establish the new-generation recipe, not migration of old certificate artifacts, indefinite certificate validity, arbitrary external PKI compatibility, or a production certificate-rotation system. Verification remains enabled.

### Terminal Ctrl-C: isolate child sessions, preserve ordered cleanup

Source inspected: `dev.py:179`, `dev.py:244`, `dev.py:284`, `dev.py:302`.

`Popen(..., start_new_session=True)` places each child service in a separate session instead of inheriting the launcher's terminal foreground process group. Terminal-generated SIGINT can therefore be handled by the launcher without simultaneously interrupting the Rust/Go children in the same group. The launcher handler requests stop; cleanup then terminates/joins platform, issues authenticated bounded worker shutdowns, and shuts down authority last. Live and already-exited nonzero statuses remain errors; timeout kills are not labeled clean. Child stdout streams are closed after process shutdown/join, followed by the private logs.

The actual new runtime test starts a tiny Python child through `Project.start`, verifies `child SID == child PID` and `child SID != launcher SID`, waits for exit and runs cleanup. This is kernel-observed child-session isolation, not a fake `Popen` assertion. No application service, container, volume or port was created by this reviewer.

**Why old Ctrl-C could look falsely clean:** older launcher versions kept services in the same terminal process group, so interactive Ctrl-C could hit authority/workers directly before ordered RPC cleanup and authority quiescence/clean-marker handling. Earlier cleanup also skipped already-dead children, and discarded platform's nonzero wait result. The launcher stop flag could end supervision before its child-status check, allowing errors or dirty authority shutdown to be reported as launcher success. Those historical success labels are not valid clean-shutdown evidence. Current child-session isolation addresses the signal-delivery cause; checked terminal results address the false-clean reporting cause. The session-ID test is not a full interactive-PTY Ctrl-C cluster acceptance run, and Docker SIGTERM evidence is a different signal scenario.

Worker death remains a degraded event without shutting down the healthy worker/controller. Ordered cleanup still rejects nonzero worker exits rather than manufacturing a clean proof. No unknown SQL, control mutation or uncertain shutdown request is automatically retried.

### Actual narrowly rerun tests

```text
PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-runtime.test.py
Ran 8 tests in 0.110s — OK

PYTHONDONTWRITEBYTECODE=1 python3 -B tests/dev-protocol.test.py
Ran 10 tests in 0.009s — OK
```

The runtime suite includes actual OpenSSL verification and one actual child-session test in addition to bounded mocks/private synthetic state. The independent protocol suite remains fake HTTP responses only and verifies single dispatch/no retry, envelope identity/size/boolean/error rules, nested duplicate rejection and credential-safe redirect rejection. No heavy module, benchmark or whole-cluster gate was rerun here.

### Wider acceptance artifacts: inspected, not rerun here

- `/tmp/peri-p6-sdk-live-rust-proper-ca.log`: recorded Rust SDK verified-TLS SELECT, exact i64, status and explicit close PASS.
- `/tmp/peri-p6-sdk-live-ts-proper-ca.log`: recorded TypeScript SDK verified-TLS SELECT, exact bigint, status and explicit close PASS.
- `/tmp/peri-p6-s3-proper-ca-whole-smoke.json`: inspected `pass:true`, `tls:"certificate_verified"`, `backend:"external_s3_fixture"`, with 11 checks: encrypted native SQL ACK, exact int64, rollback, integrity, checkpoint, snapshot/restore, stale-token denial, native readonly clone, migration epoch, suspend/status/resume and actual native metrics. Its accompanying log records `credentials_logged:false`. Main additionally reports audit scan findings zero; this reviewer did not independently rescan the live audit corpus.
- `/tmp/peri-p6-docker-live-rust-sdk.log` and `/tmp/peri-p6-docker-live-ts-sdk.log`: recorded Docker verified-TLS SDK success.
- `/tmp/peri-p6-docker-live-shutdown.log`: records own-container real SIGTERM joined exit 0, persisted exact authority clean marker, unchanged production-source aggregate hash before build/after runtime, and cleanup of only the owned labeled container/named volume. This is main/C's live encrypted-TLS Docker evidence. The logged source aggregate is not interchangeable with the individual launcher SHA below. This reviewer did not operate Docker or inspect/delete any container/volume.

These artifacts support the specific recorded runs, not HA, universal crash recovery, automatic backend quiescence proof, fresh remote namespace discovery, arbitrary terminal signal delivery, or DEK-forgetting guarantees. Prior deployment/operator limitations remain documented.

### Final source identity and document freeze

Hashes were sampled after the 8+10 runs against the same inspected source. Prefixes are included for quick comparison; full values identify the precise files:

| Source | SHA256 prefix | Full SHA256 |
| --- | --- | --- |
| `prototypes/v1/platform/dev.py` | `bc2d66ec371abd5c` | `bc2d66ec371abd5c2cbbed2ab1238dadc0dd72682572253ad41cc3b26823c659` |
| `prototypes/v1/platform/tests/dev-runtime.test.py` | `e19399d0599f8718` | `e19399d0599f8718fa82b11f0de08cb51da45fcd6a35d5caae3497513eca1c6d` |
| `prototypes/v1/platform/tests/dev-protocol.test.py` | `7ddc9a154cc69e5e` | `7ddc9a154cc69e5ec938de809bc43255f9f0263bc884c28a85aa1aa7c5219007` |

**Final frozen outcome:** TLS trust-role and child-session isolation changes are present, the requested runtime 8/protocol 10 tests actually pass, and no new blocker was identified in this bounded review. This document is the only source write in this final task. Original security production sources and README remain unchanged by this reviewer. Existing live SDK/S3/Docker claims retain their precise artifact scope and attribution.
