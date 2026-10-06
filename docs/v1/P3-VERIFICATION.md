# P3 independent final verification

## Verdict

**PASS for the frozen P3 implementation.** All three required final gates actually
completed with exit 0: locked package tests, formatting check, and strict
all-targets/no-deps Clippy. The ordinary run passed **165 distinct top-level
tests**, with **4 explicitly ignored network tests**. Main separately ran those
four against real RustFS: **4 passed, 0 failed**, in 6.16s. The independent
verifier inspected the actual live log and its P3 test source; it did not handle
or save credentials, and does not claim to have personally launched that run.

Baseline is P2 commit `2bf3c1e867d2db9bba318432a8dfb5da0629aa98`
(121 ordinary + 3 live tests). Production freeze was explicitly authorized
before the final gates. No production, Cargo, other documentation, user
`CLAUDE.md`, daemon or security configuration was modified by this verifier.
No commit, push or subagent was made. Independent changes are confined to
`tests/p3_verify.rs`, `tests/p3_verify/*.rs`, and this document.

No blocking P3 finding remains from the reviewed boundaries and executed tests.
This verdict does not assert P4 distributed-writer correctness.

## Frozen source evidence

Two content manifests were captured immediately before final testing and again
after all three final gates. Both `cmp` commands completed with exit 0.
Paths in each manifest are repository-relative, sorted with `LC_ALL=C`; each
line is `shasum -a 256 FILE`. Digests below hash those manifest files themselves:

| Frozen input | SHA-256 manifest digest, identical before/after |
| --- | --- |
| `prototypes/v1/src/**`, `Cargo.toml`, `Cargo.lock`, `README.md` | `eb06227d26292a969e1f3f5bea01f3acc8200939dc507f0a394ab7ae66c4e76f` |
| All files under `prototypes/v1/tests/**` | `499a6ad97d37bea2a4cf6fe08d453dac8ae40f1ca21d52cc313cb139222c48e4` |

Manifests are `/tmp/peri-loom-p3-verifier/production.before.sha256`,
`production.after.sha256`, `tests.before.sha256`, and `tests.after.sha256`.
This is a content freeze of the named P3 package inputs, not a claim that the
whole working tree is clean or that later edits retain this verdict.
The Turso dependency was also inspected at commit
`1a548d14a5be41e32b05bc43cb559cfb8b44dae4`; its `git status --short` was empty.

## Actually completed gates

Commands ran from the repository root:

```sh
cargo test --locked --manifest-path prototypes/v1/Cargo.toml
cargo fmt --manifest-path prototypes/v1/Cargo.toml --check
cargo clippy --locked --manifest-path prototypes/v1/Cargo.toml --all-targets --no-deps -- -D warnings
```

| Gate | Actual outcome | Log |
| --- | --- | --- |
| Locked ordinary tests | Exit 0; 165 passed, 0 failed; 4 ignored | `/tmp/peri-loom-p3-verifier/final-cargo-test.log` |
| Format check | Exit 0; `FMT_EXIT=0` | `/tmp/peri-loom-p3-verifier/final-fmt.log` |
| Strict Clippy | Exit 0; `CLIPPY_EXIT=0` | `/tmp/peri-loom-p3-verifier/final-clippy.log` |
| Real RustFS, launched by main | Exit 0 reported by main; inspected log: 4 passed, 0 failed, 0 ignored | `/tmp/peri-p3-rustfs-live.log` |

The ordinary count is 121 baseline + 2 protocol unit + 18 registry + 7 session
+ 5 resource + 6 stateless acceptance + 6 independent adversarial tests = 165.
The existing cross-process cache-lock test launches a filtered child harness
which prints an additional `1 passed` summary; that nested execution is not an
additional distinct top-level test. Doc-tests contain zero tests. Ignored cases
were not counted as ordinary passes.

The pinned Turso path dependency emits nine existing compiler warnings.
`--no-deps` strict Clippy completed successfully for the v1 package and all its
targets; this is not a claim that the upstream dependency is warning-free.

Main's real-backend command, with access/secret keys supplied only through its
private user environment, was:

```sh
PERI_S3_TEST=1 PERI_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
  cargo test --manifest-path prototypes/v1/Cargo.toml --locked --test minio \
  -- --ignored --nocapture --test-threads=1
```

Live evidence includes object-store CAS/conditional delete/pagination, native
fresh-cache WAL recovery, P2 refs/PITR/GC, and
`p3::network_worker_multi_db_unload_and_fresh_cache_recovery`. Inspection of the
last test confirms real S3 through `LocalAuthorityProvider` and `WorkerRuntime`,
two DBs, unfinished writer drop, compaction, eviction to zero loaded DBs, full
old-cache deletion, a fresh runtime, both DBs' results, and integrity checking.
The live P3 case is an in-process runtime test; actual SIGKILL process evidence
comes from the ordinary local-authority worker CLI tests, not from that live
case. No endpoint cleanup outside the unique test bucket is claimed.

## Independent adversarial evidence

The independent suite uses only the frozen public provider/runtime API and
the line-JSON `worker STORE CACHE` binary. Its provider gate delegates to
`LocalAuthorityProvider::new(ArcObjectStore)` rather than recreating authority
logic. Native session/IO implementations and developer test helpers are not
imported. Each subsidiary file is under 500 lines.

| Test | Counterexample exercised and actual result |
| --- | --- |
| `coalesced_loading_drain_keeps_native_retirement_reserved_until_drop` | Coalesced opens followed by drain while loading; gate native object-store destruction; reject new epoch and different-DB admission while retiring, retain capacity, and complete `status()` outside the blocked destructor. Both canceled opens return errors, one provider call occurs, reservation reaches zero, and a new epoch subsequently opens. |
| `failed_coalesced_open_does_not_consume_other_tenant_capacity` | Inject one gated provider failure with a same-key waiter and another tenant admitted concurrently. Both failed opens complete, provider call count is one, other tenant's session survives, no loading reservation remains, and retry creates a real SQL table. |
| `tiny_cache_caps_measure_payload_and_drop_writer_releases_capacity` | Real SQL blobs exceed one-segment RAM/disk caps; disk metric equals recursively summed actual `.seg` file lengths, dirty/pending payload is zero after compact, and current WAL is bounded. A separate reader cannot see an unfinished writer; compact fails while that writer is live; drop aborts it and restores session capacity/native writer access. Full cache deletion followed by cold reopen retains committed rows only. |
| `tenant_and_delimiter_names_do_not_alias_after_full_cache_disposal` | `a-b/c`, `a/b-c`, and another tenant's `c` hold different real SQL markers across unload/full-cache deletion/reopen. Forged empty, traversal, slash, dot, Unicode and backslash tenant fields fail open/close/drain/cache-key validation. |
| `eof_aborts_writer_with_live_reader_and_protocol_errors_do_not_leak_sessions` | Actual CLI worker has a read transaction and a separate dirty write transaction. Invalid protocol arguments, traversal identifier and unknown field do not create sessions. EOF exits successfully; full-cache deletion and a fresh process recover only the committed row, allow a new write, and pass integrity checking. |
| `sigkill_after_compacted_ack_excludes_dirty_tail_and_preserves_old_pitr` | Actual CLI worker writes 320 random 8192-byte blobs, snapshots sequence 2, compacts current WAL, commits an ACKed two-row tail, then leaves an unfinished writer. OS exit status is signal 9, not graceful disposal or task cancellation. Delete whole old cache, start another worker with nonexistent cache, recover the ACKed rows without dirty row 999, reopen the old P2 PITR on a separate branch, and perform a further write/integrity check. |

The targeted pre-freeze run actually passed 6/6 (12.25s). Its `--nocapture`
output records SIGKILL signal 9, compact sequence 3, ACK sequence 4, and cold
startup **1 segment load**, **131072 B RAM segment payload**, **131072 B disk
segment payload**, **4152 B current WAL**, zero pending WAL/dirty segment
payload. Old PITR has 320 rows/max ID 319; current main has 323 rows. The final
all-package run independently completed the same six tests, 6/6, in 9.16s.
After the final gates, the two independent lifecycle tests ran ten more times:
**20/20 completed executions**, zero failures. This repeat count is not added
to the 165 distinct tests. The command in each iteration was:

```sh
cargo test --locked --manifest-path prototypes/v1/Cargo.toml --test p3_verify lifecycle:: -- --nocapture --test-threads=1
```

The scale-to-zero metrics are loaded engine/session/payload accounting, not
process RSS or all native allocator usage. Segment payload caps are exercised;
they are not advertised as a total WAL/dirty/native-memory cap. The independent
startup-load number alone is not a wire-traffic proof: the developer acceptance
suite additionally measures raw segment bodies and archived/current WAL fetches
in its counted-store migration case, and those cases passed in the final run.

Loading cancellation here means explicit runtime drain and completed error
returns, not canceling the test harness and calling that a pass. All final gate
processes completed; no timeout, interrupted test, or OS execution stall was
counted as passing.

## Code review boundaries and limitations

- Registry reservations precede remote I/O; same-key completion is shared; I/O,
  resource reads and native destruction occur outside the global registry lock.
  `Retiring` prevents cache/native teardown collisions, including canceled
  successful loads. Active handles reject close/idle eviction; drain releases
  capacity only after final session cleanup.
- Handles own distinct native connections. Session drop restores native sync
  protections, rolls back unfinished work, and closes its connection before
  registry release. Other-session transaction/native writer behavior is tested,
  not inferred from the existence of separate Rust wrappers.
- Compaction requires confirmed durability/no pending operations and native
  truncate-checkpoint success. Busy/failed paths return errors. Old authority
  recovery points retain immutable WAL views. Existing P2 regressions and P3
  old-PITR-after-current-compaction tests actually passed.
- Namespace/branch identifiers are checked at public runtime/provider
  boundaries; tenant separation here is storage-key/cache isolation, not
  authentication. The local provider deliberately has trusted authority access
  and does not validate a distributed ownership lease/epoch. P4/P5 must replace
  that boundary for untrusted SQL-worker deployment.
- Registry/loading/drop gates are synthetic adversarial timing controls backed
  by real native databases. They do not prove arbitrary thread interruption,
  provider panic recovery, or every possible scheduler interleaving. SIGKILL,
  EOF, cache disposal and real S3 tests provide separately labeled evidence.

## Log integrity

SHA-256 values captured after final gate completion:

| Log | SHA-256 |
| --- | --- |
| `targeted-01.log` | `48c6891e73ff577127ebec5bf853d5307cceb4e59f2f3f9f4375efa6fe339e74` |
| `final-cargo-test.log` | `2e81f415e62320cdcd484f4a87f63db5f09cbd5c014c68fd68ba4c692e2cc085` |
| `final-fmt.log` | `1a670175a9a78732a00f898faab898a42e64616999d9999c0d52f80649d64f8a` |
| `final-clippy.log` | `a1deceb3f42347422174a841fbc4a7a238dd758dcc9a3026381c1fa29270fc8b` |
| `lifecycle-repeat.log` | `2db04077952438b766445e1b709bd2875f25ea57006a6c8c4289f53756b0b07c` |
| `/tmp/peri-p3-rustfs-live.log` | `d5725ce97d5b6c4adaf9dcee884d4e4c2ab5c7a4119fd6c3f96cfa8faa3c367c` |

The first five paths are under `/tmp/peri-loom-p3-verifier/`. These logs are
ephemeral local evidence, not committed artifacts or secret files. No guessed
calendar review date is assigned.
