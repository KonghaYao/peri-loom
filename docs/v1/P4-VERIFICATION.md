# P4 independent verification — PASS

## Status

**Final PASS on the newly frozen, corrected source: 254 ordinary tests and five
actual real-backend tests passed; formatting and strict all-targets Clippy passed;
source manifests remained identical.** The first frozen-source round failed and
was explicitly revoked for a production regression fix. That evidence remains
below; no failed run is relabeled as successful or hidden by a longer timeout.
P3 baseline is commit `bd2bfed` (165 ordinary + 4 live tests); those reported
results are not reused as newly executed P4 evidence.

Independent ownership is limited to `tests/p4_verify.rs`, its new helpers under
`tests/p4_verify/`, and this document. No production/Cargo/P3 verification/other
documentation edits, commit, push or subagent is performed.

## Final acceptance — corrected frozen source

All commands below completed, with actual exit codes recorded in their logs.
Final independent gate/evidence directory:
`/tmp/peri-loom-p4-verifier/final-fixed/`.

| Gate | Actual result | Log |
| --- | --- | --- |
| `env PERI_P4_VERIFY_TRACE=0 cargo test --locked --manifest-path prototypes/v1/Cargo.toml --no-fail-fast` | Exit 0; **254 top-level ordinary passed, zero failed, five ignored**; doc-tests completed | `all-test.log` |
| `cargo fmt --manifest-path prototypes/v1/Cargo.toml --check` | Exit 0 | `fmt.log` |
| `cargo clippy --locked --manifest-path prototypes/v1/Cargo.toml --all-targets --no-deps -- -D warnings` | Exit 0 | `clippy.log` |
| Unchanged isolated P3 SIGKILL/cold recovery/PITR test, locked with `--nocapture --test-threads=1` | Exit 0; one passed, five filtered; **11.04s whole test**, every worker request retains its original 8s deadline | `p3-isolated.log` |
| Direct no-proxy independent suite, locked with `--nocapture --test-threads=1` | Exit 0; **14 passed, zero failed/ignored/filtered, 8.75s** | `direct-14.log` |
| Main's post-fix private-environment real RustFS execution, independently inspected | **Five passed, zero failed, three filtered, 14.10s** | `/tmp/peri-p4-rustfs-postfix.log` |

Ordinary counts are read from completed results, not estimated from earlier
suite sizes: the package has 13 unit tests; P4 authority 35, compatibility 3,
remote 11, runtime 20 and independent 14 cases. The full log contains a nested
one-test child for cross-process cache locking. Summing all result lines gives
255 passes; removing that duplicate child gives **254 ordinary tests**, not 255.
The five ignored network cases execute separately in main's real-backend run.
Private credentials were neither acquired nor saved by this verifier.

The P3 isolated log records actual SIGKILL signal 9, compact sequence 3, ACK
sequence 4, cold opening segment loads 1, WAL payload 4,152 bytes, and preserved
old PITR sequence 2. Its 11.04s is total test duration, not an individual request
duration or a claimed latency SLA. The full stateless migration target passed
all six cases in 12.78s, versus 45.46s in the failed round. Exact first-round and
corrected-round manifest entries for every P3 verifier file match: **no P3 test
or timeout was edited**.

Reviewed the correction: `ObjectStore::validate_local()` defaults conservatively
to full validation; only the trusted raw branch and scoped lease wrappers opt
into local checks. The worker wrapper checks generation/mode/clock locally;
authority `FencedStore` checks known live binding/deadline without root I/O.
Native cached page reads and RAM writes use those checks, while statement
ingress, physical WAL/durable publication and authority RPC stages retain full
validation. The corrected A suite proves 1,000 local checks perform no root GET,
ingress and durable I/O still do, and expiry/takeover/uncertainty/restore/drop
invalidate known local bindings. The eight-round renewal/native-commit and
Snapshot/pin-cleanup proofs also pass. Own actual stale cached SQL/COMMIT,
checkpoint/compact/HEAD, partition, ReadOnly escalation and fragmented HTTP
regressions pass after this correction.

### Stable source evidence

The 4,274-file manifest includes production/native source, package Cargo files,
README, all 65 package test/fixture files, and other `docs/v1` documents. Only
build targets, Git internals and this verification report are excluded. All
fourteen actual frozen old-store objects are included byte-for-byte.

- Combined manifest SHA-256:
  `4d371d542860d719a599ffdad20c38f958a4a02d125904e5b21510e06100174c`.
- Production/native/docs subset, 4,209 files:
  `0683878e0ab0abbbc789c03fedbc656ad0c32661864870733fcb2a39c5b2937b`.
- **All tests**, 65 files:
  `523a1bfb9b1e485060e7d2978bed50c7564d006f3ff28eb5974b12757ffb6f9b`.
- Legacy fourteen-object fixture subset:
  `7f583586196ef5f7a8cfa32e6788f2b5a9b37804671188693e607b4bf26aed22`.
- Owned independent suite, 11 source files / 14 cases:
  `9dd34de13941145debf5e675568bc4d003380153f7349776e63cc897374a5357`.

`before.sha256` equals `after-gates.sha256` and `after-final.sha256`, both
comparisons exit 0. The latter follows full gates and both focused reruns.
Subset manifests are retained in the final evidence directory. These are hashes
of sorted content-hash/path manifests, not Git commit IDs. This report is
finalized and frozen before completion notification/staging; no late evidence
additions are planned after main stages it.

### Acceptance limits

- One credential-bearing authenticated loopback authority with exclusive startup
  state, not overlapping authority HA or automatic recovery after dirty restart.
  Dirty restart requires explicit external reconciliation; that does not by itself
  prove a remote backend has become quiescent.
- No physical deadline cancellation claim. Valid already-dispatched I/O may
  finish after expiry before a completed barrier; unknown outcomes retain intents
  and block the namespace. A shutdown reply means stopping, not a clean fence;
  successful runner exit and quiescence are required to mark clean.
- Destructive transitions explicitly close admission before waiting for admitted
  I/O. Benign renewal/pin cleanup/Snapshot/Clone admission waits outside its
  ticket; this is not a promise of arbitrary scheduler fairness or HA availability.
- Crashed workers may conservatively leak durable reader pins; unresolved intents
  and durable receipt/catalog growth can increase storage use. Distributed pin
  expiry, receipt compaction, quotas and broad deployment/performance hardening
  are not accepted as implemented by these tests.
- Independent paused/unknown physical I/O tests exercise PUT. Reviewed physical
  DELETE's final guard and namespace-uncertainty path; A's full passing suite
  supplies paused GC deletion, unknown-delete/pending-command and tombstone
  safety-window evidence. No separate independent paused-DELETE case is claimed.
- No interrupted process or teardown cancellation is counted as PASS, no proxy
  buffering substitutes for the direct process tests, and no host-clock-derived
  calendar review date is invented.

## Early review and execution

- Read `05-p4-rust-ownership-runtime.md`, the Astra architecture contract,
  shared ownership types/RPC protocol, authority stages and admission/lease
  implementation, migration/codec, native role hooks and ownership runtime.
- Candidate issue reported promptly to main: the authority's `FencedStore`
  initially inherited `ObjectStore::access_role()`'s Writer default even when
  opened with a ReadOnly permission. A now supplies the explicit override;
  execution of the retained regression test now passes. It checks the advertised
  role, actual native read-only database/session flags, and writes attempted via
  `query()` and `PRAGMA query_only=OFF`; it does not rely on SQL regexes.
- First targeted command, `cargo test --locked --manifest-path
  prototypes/v1/Cargo.toml --test p4_verify -- --nocapture --test-threads=1`,
  exited 101 before compiling the test: production `src/lib.rs` declared
  `authority_rpc`, but neither `src/authority_rpc.rs` nor its `mod.rs` existed
  yet. This is expected integration incompleteness, **not a test pass**.
  Actual log: `/tmp/peri-loom-p4-verifier/targeted-01.log`.

## First frozen-source round — FAILED

- Captured a sorted SHA-256 manifest of 4,273 files: package production,
  Cargo manifest/lock, README, **all package tests including the fourteen actual
  legacy fixture objects**, native Turso source tree, and other `docs/v1`
  documentation. Build targets and Git internals are excluded; this evolving
  verification report is explicitly excluded. Manifest SHA-256:
  `485ac746f6b1c50382488669f57f0d3d637e111ddd1b60cb8c8eb20ac72931a5`.
  Files: `/tmp/peri-loom-p4-verifier/final/before.sha256`,
  `after-first.sha256`, and `after-all.sha256` under the same directory.
  Both comparisons to the original manifest returned zero during this round.
- `cargo test --locked --manifest-path prototypes/v1/Cargo.toml` failed,
  exit 101. Existing P3
  `sigkill_after_compacted_ack_excludes_dirty_tail_and_preserves_old_pitr`
  emitted actual SIGKILL signal 9, compact sequence 3 and ACK sequence 4, then
  exceeded its unchanged eight-second worker-response deadline. Its target
  recorded five passes and one failure, 19.41s. Log:
  `/tmp/peri-loom-p4-verifier/final/all-test.log`.
- Isolated unchanged P3 reproduction also failed, exit 101, one failure,
  five filtered out, 21.05s; same actual SIGKILL/sequence evidence and timeout.
  Log: `/tmp/peri-loom-p4-verifier/final/p3-isolated.log`.
- Complete locked `--no-fail-fast` execution failed, exit 101: **246 top-level
  ordinary passes, one failure, five ignored**. Do not count the successful
  nested cache-lock child invocation as another ordinary test: naively summing
  every result line yields 247 passes plus one failure. All P4 authority 31,
  compatibility 3, remote 11, runtime 18 and independent 14 cases passed.
  Log: `/tmp/peri-loom-p4-verifier/final/all-test-no-fail-fast.log`.
- `cargo fmt --manifest-path prototypes/v1/Cargo.toml --check` completed exit 0;
  `cargo clippy --locked --manifest-path prototypes/v1/Cargo.toml --all-targets
  --no-deps -- -D warnings` completed exit 0. Logs: `fmt.log` and `clippy.log`
  in `/tmp/peri-loom-p4-verifier/final/`. These successful partial gates do not
  turn the failed full-test round into PASS.
- Independently inspected main's combined private-environment RustFS log:
  `/tmp/peri-p4-rustfs-all-final.log`, **five passed, zero failed, three filtered
  out, 11.12s**. All five named P1/P2/P3/P4 network cases completed. This is
  main's actual execution, not an independent credentialed run; no private
  credentials were read or saved. It does not override the ordinary failure.
- An eight-second diagnostic probe observed the fresh-cache legacy worker at
  100% CPU, not a zero-CPU execution stall. The larger stateless migration case
  subsequently passed in 45.46s. Working root-cause inference, reported to main:
  added per-native-page `validate_session()` calls repeatedly reread/decode the
  full catalog through raw `BranchStore`, making cached-page work expensive.
  Main revoked the freeze to separate local lease checks from authoritative
  statement-ingress and physical-durability validation. At that point correction
  and remeasurement were pending; final corrected evidence is recorded above.
  No timeout increase or P3 test edit is made.
- The frozen A suite includes actual eight-round concurrent renewal/native
  commit and Snapshot/pin-cleanup tests. Reviewed the updated benign-transition
  mode: wait on a Condvar **outside** the admission ticket, then retry admission;
  destructive transitions still close admission fail-fast before waiting for
  admitted physical I/O. These passing tests do not excuse the cold-path failure.

No gate process from the failed round remained running when its freeze was
revoked. The newly authorized corrected freeze and its completed gates are
recorded above; the original stable failed-round hash remains truthful.

## Executed targeted evidence

- Compiler-ready first direct run executed 13 cases: 10 passed and three process
  cases failed. Log: `/tmp/peri-loom-p4-verifier/targeted-02.log`. These failures
  are retained as failures, not superseded silently or counted as passes.
- Captured actual authority stderr before teardown identified
  `Resource temporarily unavailable (os error 35)` during HTTP parsing, rather
  than a malformed typed RPC body. Log:
  `/tmp/peri-loom-p4-verifier/targeted-authority-diag-06.log`. The accepted sockets
  inherited nonblocking behavior on this Mac; main corrected shared request and
  reply handling to explicitly set blocking mode. No production fix was made
  by this verifier.
- An opt-in diagnostic proxy buffered full requests and produced a process-case
  pass in `/tmp/peri-loom-p4-verifier/targeted-trace-07.log`. That buffering masks
  request-arrival gaps, so this is diagnostic evidence only, not direct process
  acceptance. The subsequent direct run still failed one case with EAGAIN:
  12 passed, one failed, 8.11s in `targeted-direct-08.log` under the same log
  directory. Actual partition/dirty-restart and ReadOnly-grant cases passed in
  that run.
- Added one bounded, valid fragmented-HTTP regression for both actual authority
  and worker processes. It sends the request line, headers/partial body, then
  remaining body with deliberate arrival gaps and requires HTTP 200 plus typed
  successful responses. Focused run: one passed, 13 filtered out, 2.47s;
  `/tmp/peri-loom-p4-verifier/targeted-fragment-09.log`.
- After main's shared HTTP fix, direct execution with
  `PERI_P4_VERIFY_TRACE=0` completed successfully: **14 passed, zero failed,
  zero ignored, zero filtered out; exit 0; 11.34s**. Actual log:
  `/tmp/peri-loom-p4-verifier/targeted-direct-10.log`. Command:
  `env PERI_P4_VERIFY_TRACE=0 cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test p4_verify -- --nocapture --test-threads=1`.
  This includes actual epoch-10/11 workers, authority SIGKILL/partition/dirty
  restart, remote role-amplification denial, fragmented HTTP, and all ten local
  adversarial cases below. No authority EAGAIN diagnostic appeared in this run.
- These are targeted results against evolving production, not final frozen-source
  gates. No interrupted run, teardown kill or cancellation is counted as PASS.
  At that stage, all-package tests, formatting, strict Clippy, source stability
  and combined five-case real-backend evidence were still pending.
- Independently inspected main's actual focused RustFS log
  `/tmp/peri-p4-rustfs-focused.log`: the new P4 case passed, one executed,
  seven filtered out, 3.30s. It covers retry-free authority storage, remote
  epoch-10/11 workers, stale SQL/COMMIT/checkpoint refusal, restored snapshot
  integrity and native ReadOnly epoch 12. This is main's execution, not this
  verifier's credentialed run; it is not evidence that all five ignored tests
  executed together. No private credentials were read or saved.
- Inspected main's `/tmp/peri-p4-main-all-clippy-final.log`, which completed
  the package check without a package error. This is reported main evidence,
  not the independent post-freeze Clippy gate.
- Reviewed A's explicit admission-transition flag: close via atomic
  compare-exchange before waiting for the write barrier; nonblocking read-ticket
  acquisition checks closure both before and after acquisition; transition
  cleanup releases the barrier before reopening. Independently executed A's
  focused deterministic proof, rather than duplicating it: one passed,
  27 filtered out, exit 0, 0.06s in
  `/tmp/peri-loom-p4-verifier/targeted-admission-11.log`. The proof leaves the old
  lease valid, pauses its physical PUT, observes closure, rejects 100 rounds
  of new reads/mutations/session/lease/competing-grant admissions without another
  raw mutation, then releases physical I/O before takeover completes. This
  distinguishes explicit closure from expiry rejection or assumed RwLock
  fairness. The test is A-owned, not an additional independent-suite case.
- Reviewed shutdown handling: stopping is set before the quiescence check;
  a stopping response is not clean-fence completion. Persistent clean state is
  marked only when the runner succeeds and authority is quiescent. No uncertain
  operation is credited as cancelled or safely fenced merely by shutdown.

## Independent adversarial cases

- Refuse a backend whose default strict-fencing support is false before any
  durable boot dispatch; verify S3 advertises support only with retries zero
  using dummy configuration without making any network request. Independent
  fault proxies explicitly advertise support because they expose uncertainty
  instead of concealing a mutator retry.
- Commit a boot root CAS and report a lost ACK. Even though readback finds the
  new V2 term/receipt, namespace initialization remains error/blocked and no
  challenge/grant or implicit retry dispatches.
- TTL boundary rejection, intent-bound nonce mismatch, payload-digest receipt
  collision, exact expiry, stale nonce at its five-second boundary.
- Renewal anti-replay after revoke, out-of-order renewal rejection, epoch
  high-water retention and changed authority term after sequential trusted boot.
  This in-process sequential test is not evidence of exclusive RPC startup,
  crash recovery or authority HA.
- Monotonic-clock regression permanently blocks the affected namespace while
  another namespace continues.
- Pause a valid already-dispatched physical PUT, expire its old lease, request
  takeover, and prove the takeover does not complete before the physical call
  settles. Old next-stage publication fails after expiry; no later stale valid
  immutable PUT dispatches. Another namespace remains usable.
- Return a socket-close/unknown error while a real delayed physical PUT is still
  waiting to execute. An absent readback does not clear uncertainty or permit
  revoke/takeover. Release that delayed request and prove it actually arrives;
  root/epoch/intent remain unchanged and namespace stays blocked, not reset.
- Scoped-store refusal of root replacement, unconditional mutable publication,
  raw deletion and other-namespace mutation, plus real native ReadOnly behavior.
- Same-CAS restore/revoke/incarnation change, receipt replay without another
  incarnation change, payload collision, branch deletion/recreation and epoch
  tombstone retention.
- Copy the actual frozen 14-object old fixture; alter embedded JSON whitespace
  without altering the parsed value and prove the original checksum rejects it
  without resetting storage. Restore original bytes, boot V2, preserve physical
  blob bytes/current sequence 2/high-water 3 and inspect the old PITR.
- Actual authority and two owned-worker processes with worker environments
  cleared of inherited variables and no backend location/credential arguments.
  Exercise principal/scope/control-versus-SQL authorization; takeover 10→11;
  reject old cached SQL/COMMIT/checkpoint/compact/HEAD and worker GC/root/delete
  abuse, then check B's recovered committed row and ability to write.
- Actual authority SIGKILL, partitioned cached SQL refusal, default restart
  refusal from persistent dirty state, local expiry and no renewal resurrection.
- ReadOnly-grant role amplification using an authenticated malicious worker:
  request Writer `OpenSession` and `ValidateLease`, forge the binding role,
  attempt a valid scoped immutable PUT, and attempt native query/pragma writes.
  All fixture grants explicitly specify their durable role; a Writer grant's
  ReadOnly narrowing remains covered by the native-role case.
- Valid authenticated authority and worker HTTP requests fragmented across
  request-line, header and body arrivals; no nonblocking EAGAIN parse failure.

## Guarantee ledger

| Boundary | Required evidence / precise limitation |
| --- | --- |
| Credential-bearing authority | One exclusive process behind authenticated loopback RPC; persistent dirty startup marker; default refusal after unclean restart. No overlapping authority HA claim. |
| Final durable stages | Exact term, root/branch incarnation, epoch, lease identity and monotonic deadline before each root CAS, physical PUT and destructive DELETE; revalidate CAS retries. |
| Namespace barrier | Wait for admitted physical I/O to become terminal before completing takeover/revoke; no later old-stage dispatch. Valid already-dispatched I/O may finish after expiry but before a successful takeover barrier. |
| Unknown outcome | Timeout/socket close/absent readback is not terminal quiescence; retain intent and refuse completed barrier/reset. No claim of backend cancellation or no physical completion after deadline. |
| Worker boundary | Remote provider has no raw backend path/keys; authenticate principal and role, reject coarse-RPC abuse/root replacement/raw DELETE; actual two worker processes at epochs 10/11 reject old cached SQL/commit/checkpoint/HEAD/GC. |
| Lease protocol | Durable same-CAS payload-bound receipts, single-use intent-bound 5s nonce, TTL 100..60000, renewal sequence anti-replay and no expired grant resurrection. |
| Native role | Genuine native read-only flag on every connection; query-write and pragma bypass attempts cannot write. |
| Root compatibility | Checksum original embedded V1 bytes before typed decode; migrate actual frozen old 14-object fixture explicitly; restored current sequence 2 stays distinct from high-water 3; refs/PITR/physical blobs preserved. |
| Refs / ownership | Restore/delete atomically revoke ownership and update branch incarnation while preserving permanent epoch tombstones. |
| Final gates | Execute only after explicit source freeze; capture stable source manifests and real counts/logs; no cancellation or interrupted run counted as PASS. |

All fourteen independent cases passed in both the failed first round and the
corrected final round. Only the corrected frozen round satisfies all final
acceptance gates. Final PASS is bounded by the explicit limits above; earlier
failures, original source hashes and actual execution logs remain retained.
