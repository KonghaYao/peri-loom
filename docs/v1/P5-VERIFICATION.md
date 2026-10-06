# P5 independent verification — PASS

## Status and scope

**Final P5 PASS on the integrated, explicitly refrozen source.** Both full Go
ordinary and race runs completed: **62 unique top-level passes, zero failures,
one intentional subprocess-helper skip, plus 51 passing subcases per run**.
Vet, formatting and CLI build passed; before/after source and Git/native state
match. The earlier failed formatting round and development harness failures
remain below. P4 commit `5119187` and its frozen report are preserved, not edited
or presented as newly executed P5 Rust/S3 gate evidence.

Independent ownership is limited to `p5_verify_test.go`,
`p5_verify_helpers_test.go`, other `p5_verify_*_test.go` files first developed in
that sidecar, and this document. Main imported the 38-file module into
`prototypes/v1/controller`; final commands execute from that integrated module.
No A/B/C/main production, P3/P4 tests or reports, Cargo,
other documentation, commit, push or subagent changes are made. No benchmark,
OS-daemon/security modification, or real cloud credential acquisition occurs.

## Final integrated frozen-source gates

Final evidence directory: `/tmp/peri-loom-p5-verifier/final-formatted/`.
Go version: `go1.23.4 darwin/arm64`. Commands execute in
`prototypes/v1/controller`, without a Rust-binary environment override; the
actual process helpers resolve the built binary via repository ancestors.
Every gate below actually completed; none is inferred from an agent summary.

| Gate | Actual result | Evidence |
| --- | --- | --- |
| `go test -count=1 -json ./...` | Exit 0; module 21.864s; **62 top-level PASS / 0 FAIL / 1 helper SKIP**, 51 passing subcases | `go-test.jsonl`, `go-test.exit`, `go-test-summary.log` |
| `go test -race -count=1 -json ./...` | Exit 0; module 26.484s; **62 top-level PASS / 0 FAIL / 1 helper SKIP**, 51 passing subcases; no race failure | `go-race.jsonl`, `go-race.exit`, `go-race-summary.log` |
| `go vet ./...` | Exit 0 | `go-vet.log` |
| `gofmt -l` over every module Go file | Empty output; exit 0; no formatting writes by verifier | `gofmt.log`, `gofmt.exit` |
| `go build -o /tmp/peri-loom-p5-verifier/final-formatted/peri-controller ./cmd/peri-controller` | Exit 0 | `go-build.log` |
| Content manifests before/after all gates | Identical; `cmp` exit 0 | `before.sha256`, `after.sha256`, summary logs |
| Frozen P4 Rust/native baseline and Git state | Unchanged; Rust diff exit 0; captured Git state comparison exit 0 | `git-before.log`, `git-after.log` |

Counts use unique structured `(Package, Test)` terminal events, not matching
stdout `PASS` strings or adding child-process output. Top-level names contain
no `/`; subcases are reported separately rather than added to the ordinary
count. There are 63 top-level descriptors, **not 63 passes**:
`TestStoreProcessLockChild` intentionally skips when invoked directly without
its parent-only environment. `TestStoreLockExcludesOtherProcess` passes only
after executing that same helper in a real child and observing successful exit;
the child is not counted twice. `cmd/peri-controller` has no test files and its
package-level skip is not another skipped test.

All **eight independent genuine Rust-process cases** pass in both integrated
runs, including SIGKILL failover, active-transaction migration, durable restart,
real socket lost ACK, allocation failure, native ReadOnly/API, suspend/resume
and endpoint/principal swap. Main's `TestControllerCLIActualServicePlacementAndMigration`
also runs independently here and passes: **2.05s ordinary / 2.02s race**. It
builds and starts an actual Go CLI controller, acknowledges SQL row 7, migrates
epoch 1→2 and recovers it on the second worker, then cleanly exits and reopens
metadata. Neither mock controller suites nor isolated sidecar results substitute
for these final integrated process tests.

### Stable source and executable provenance

The sorted SHA-256 content/path manifest covers **4,316 files**: all 38 module
files (Go production/tests, CLI, README and `go.mod`), Rust production and all
65 Rust test/fixture files, the native Turso tree, package Cargo/README files,
other `docs/v1` documents, `.gitmodules`, and the executed Rust binary. Build
targets/Git internals are excluded except the explicitly included Rust binary;
this finalized verification report and the pre-existing user `CLAUDE.md` change
are outside the source manifest. P6's separate temporary development tree is
outside this closure.

- Combined source/native/binary manifest SHA-256:
  `3b29f23fbb92733a24f5bc0b895e2445775f6b5ae4eba147726efd956576fa26`.
- Integrated Go module, 38 files:
  `345b50cea98236762bc684cfcefdfbe673a4dc90278b6ee0695c00b04b4fd60b`.
- Native tree, 4,112 files:
  `3384e165ab7898f9f307f4e88e3267e477d9f20583fb4a537c979778edb2fe35`.
- All Rust tests including actual fourteen-object migration fixtures, 65 files:
  `523a1bfb9b1e485060e7d2978bed50c7564d006f3ff28eb5974b12757ffb6f9b`.
- Executed Rust binary SHA-256:
  `45997fc62a10769163f5f50ee6e4215e8ea7bb7414fb8666b0d19ffd468d5397`.
- Separately built final Go CLI artifact SHA-256:
  `ce0e3514f4bb39fcb82419d172d16dcaf6ffd0755c103e0b15de642c3c1d2c3d`.

Captured repository baseline HEAD:
`51191870dc903327688fc3672ed8dde31932e016`; tracked tree:
`30059ca843d62c26df3dcd2fa4896e634cb7d05f`.
Native submodule commit:
`1a548d14a5be41e32b05bc43cb559cfb8b44dae4`; native tree:
`357756b78ad255e788243921aae026ec21c07ed6`; its worktree is clean.
The new module is captured by the content manifest even though it is not yet
part of baseline HEAD. No P5 commit ID is invented.

`git diff --exit-code 5119187` over Rust `src`, package Cargo files/README,
all Rust tests and `prototypes/p0/turso` returns 0 before and after. Therefore,
as authorized, the already independently passed P4 Rust gates are not repeated.
This is explicit unchanged-baseline provenance, not a claim of newly rerunning
254 Rust tests or five live S3 cases for a Go-controller deployment.

### Retained initial integrated formatting failure

The first integrated freeze's `gofmt -l` gate failed on only
`store_validation_test.go` (A-owned): exit 1 in
`/tmp/peri-loom-p5-verifier/final/gofmt.log` / `gofmt.exit`. Reported promptly;
the verifier did not edit it. That round's ordinary run completed exit 0,
62 top-level passes / one helper skip / 51 passing subcases, module 23.497s;
vet/build also passed, but the round **did not satisfy all gates**.

Main formatted that single file and explicitly authorized a new freeze. Actual
old/new manifest comparison identifies only
`prototypes/v1/controller/store_validation_test.go` as changed. Initial combined
manifest SHA-256:
`ec27f5243b03dcba43450f2ad06a1f49594542051a9c5acd0def7cc9a99e7a62`;
initial module SHA-256:
`f69f929a17780a8577cc0c7315418f67f10706fca36397ab07c18bdd33f654a4`.
Every final Go gate was rerun against the new manifest, rather than crediting
old-format passes as new-source acceptance.

Owned tests and this report are finalized and frozen before completion
notification/staging. No late evidence additions after staging are planned.

## Public seams and genuine process harness

- Read the P5 specification, shared `model.go`, metadata/registry APIs, real
  `ControlTransport`, public controller methods, management API, and frozen Rust
  authority/owned-worker line-independent HTTP protocols.
- Exercise public `NewController`, `Place`, `Migrate`, `Suspend`, `Resume`,
  `Reconcile`, `Run`, `Status`, `OpenStore`, registry APIs and `NewTransport`.
  The Go controller runs in the test process; **every independent case starts
  an actual Rust authority and two actual owned-worker OS processes**. The
  management API case uses a real loopback HTTP server, not a response mock.
- Use a fresh Local backend, separate authority state, controller metadata and
  worker cache directories for each case. Authority startup receives exactly
  two explicit auth configuration environment variables. Each worker receives
  exactly its three scoped auth variables and authority URL/cache/worker/listen
  arguments: no inherited environment, backend location argument, backend keys,
  or S3 credentials. This is an argument/environment boundary assertion, not an
  OS filesystem sandbox claim.
- Generate fresh synthetic tokens. The private 0600 temporary worker-token
  configuration is test data, not a saved real credential. Readiness and exit
  waits are bounded; captured stderr is bounded and fixture-token-redacted.
  Logs record actual PIDs, endpoints, environment **names**, and executable
  digest, never token values. Teardown kills are cleanup, not acceptance proof.
- Find the actual Rust binary via `PERI_P5_RUST_BINARY` or repository ancestors.
  Absence is fatal with build/invocation guidance, **not Skip**. There is no
  hardcoded user-home fallback. Observed executable SHA-256 in current logs:
  `45997fc62a10769163f5f50ee6e4215e8ea7bb7414fb8666b0d19ffd468d5397`.
  This identifies the executed input, not a final P5 source-stability manifest.
- Test configuration: lease TTL 3s, heartbeat timeout 500ms, reconcile interval
  100ms. Durations below describe actual test execution, not a latency SLA.

## Eight independent adversarial cases

1. **Automatic actual SIGKILL failover:** A acknowledges rows 1/2 after
   checkpoint/compaction, retains dirty transaction row 999, then actually dies
   by signal 9. Delete A's whole cache. Only automatic `Run` drives RPC failure
   detection, registry health updates and epoch 1→2 placement on B. B recovers
   only ACK rows, passes integrity and writes row 3. Restart A with fresh cache:
   its saved old grant returns a parsed negative ownership response, actual
   state is not Writer, and SQL open is refused. No old cached session is
   claimed to survive an OS process restart.
2. **Graceful migration with active native transaction:** initial migration
   fails without advancing actual authority epoch or making B SQL available.
   Explicit ROLLBACK and session close settle A; migration then advances 1→2,
   recovers ACK row 7, excludes dirty row 999, passes integrity and writes.
3. **Durable metadata restart without deadline resurrection:** close and reopen
   the real metadata store after 3.2s without renewal. Persisted epoch/revision
   load unchanged; actual expired lease remains expired. Expiry watchdog or
   explicit cleanup releases all sessions. Reconciliation requires a fresh
   lease/epoch; another database demonstrates global epochs 1<2<3. Loaded
   metadata contains none of the fixture RPC credentials. This reconstructs the
   public controller over durable metadata, not a Go CLI SIGKILL experiment.
4. **Genuine grant lost ACK:** a bounded loopback proxy forwards a real grant to
   authority, observes actual positive commit, then closes the client socket
   before returning an ACK. Transport reports ambiguity and does not retry
   automatically. Durable pending ID/payload/epoch survives store/controller
   reopen. Reconciliation sends identical full wire bytes exactly once again,
   recovers the receipt without replacing epoch 1, and acquires working SQL.
   This tests a real socket close, not a fabricated positive backend reply.
5. **Epoch metadata allocation failure:** a public MetadataStore wrapper rejects
   the first epoch-changing callback before commit. No durable placement is
   partially published and real Rust ownership remains epoch 0/unowned. The
   next successfully persisted allocation acquires epoch 1. This is injected
   pre-commit failure, not a claim of reproducing physical disk/fsync failure.
6. **Authenticated API to native ReadOnly:** seed ACK row 7, snapshot/clone via
   authority control RPC, and place the clone ReadOnly through actual Go HTTP.
   Authority credentials cannot authenticate the management API. Worker Writer
   `OpenSession`/`ValidateLease` amplification is denied; opened native session
   reports readonly. Query write injection and `query_only=OFF` bypass fail;
   row 7/integrity remain correct. Credential-bearing malformed registry input
   is rejected without token echo or durable registry changes.
7. **Durable suspend/resume:** suspend settles actual Rust revocation at epoch 1
   and rejects the original authority write capability/SQL ingress. Metadata
   reopen plus reconciliation remains suspended; `Place` cannot implicitly
   resume. Explicit `Resume` obtains a fresh epoch 2/lease, recovers ACK row 6,
   passes integrity and writes row 7.
8. **Registry endpoint/principal swap:** real worker A state is readable through
   A's scoped transport credential. Advertising B's endpoint under A's identity
   yields actual HTTP 403, not a successful wrong-worker control call; safe
   transport errors contain no fixture token.

## Retained sidecar development execution evidence

Evidence directory: `/tmp/peri-loom-p5-verifier/`.

| Execution | Actual result | Log |
| --- | --- | --- |
| Initial direct endpoint/principal case | One PASS, 0.09s | `direct-wire-01.log` |
| Corrected actual automatic failover | One PASS, 2.62s | `automatic-failover-03.log` |
| Migration/lost ACK/allocation group | Three PASS, one restart assertion FAIL | `migration-restart-receipt-05.log` |
| Corrected expiry/restart case | One PASS, 4.72s | `restart-06.log` |
| Native ReadOnly/HTTP API case | One PASS, 1.84s | `readonly-api-07.log` |
| First seven cases, `-race` | Seven PASS, exit 0, Go total 15.910s | `own-seven-race-08.log` |
| Suspend/resume, `-race` | One PASS, 1.52s | `suspend-race-09.log` |
| **Consolidated all eight, `-race -count=1`** | **Eight PASS, exit 0, Go total 15.536s** | `own-eight-race-10.log` |

The consolidated command is `go test -race -run '^TestP5Verify' -count=1 -v .`,
with `PERI_P5_RUST_BINARY` explicitly selecting the built Rust executable.
It executes only the independent cases, not all Go production/developer tests.

Retained non-passing development evidence:

- `automatic-failover-02.log`: the scenario reached a genuine negative old-grant
  reply, but the initial verifier incorrectly demanded `RPCError.Rejected`.
  The transport intentionally treats generic operation failures conservatively
  as ambiguous. Corrected the assertion to require a parsed negative reply,
  non-Writer actual state and SQL refusal, rather than accepting a mere network
  failure. No production change fixed this harness assertion.
- `migration-restart-04.log`: build failed before tests while C was adding
  `validatePlacementProtocol`; zero executed tests, not PASS.
- In `migration-restart-receipt-05.log`, the first restart assertion demanded
  explicit close success even though Rust's expiry watchdog had already removed
  the session. The corrected case checks observable zero active sessions and
  actual expired ownership instead of requiring a redundant close to succeed.

Independently inspected main's `/tmp/peri-p5-main-cli.log`: its main-owned test
built and started an **actual Go controller CLI process** with authority/two
workers, acknowledged row 7, migrated epoch 1→2, recovered `[[7]]`, then cleanly
SIGTERM-exited and reopened metadata; one PASS, 1.97s. Main's focused
configuration/API/ID log completed successfully in 0.974s. These are separately
attributed main executions, not extra independent cases. Mock controller tests
are not substituted for any genuine process proof above.

## Acceptance limits

- Final acceptance applies to the integrated, refrozen manifest and completed
  gates above, not the earlier evolving sidecar or failed formatting round.
  Sidecar results remain attributed development evidence, not additional final
  ordinary passes.
- Single controller/private metadata directory and one credential authority;
  no HA consensus, coordinated multi-file rollback protection, automatic dirty
  authority restart, or backend cancellation proof. Rust, not heartbeat health
  or stored TTL fields, supplies actual fencing safety.
- The lost-ACK proxy demonstrates uncertainty/receipt replay; it does not claim
  that socket close cancelled a physical operation. ACKed SQL is not retried.
  Crashed-worker durable pins and metadata/receipt growth retain P4's conservative
  availability/storage limitations.
- Synthetic Local-backend process tests do not establish a new real-S3 P5
  deployment run. No calendar review date is inferred from the host clock.
