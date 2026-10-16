# S3 `put_group`: bounded parallel upload of immutable bodies

Base commit: `997abbdeeb5cbd6b827677a3e39046acc6acf86e` (v1.1.4).
Branch: `perf/s3-group-cba85c2d-531f-44b3-a15e-73eb1ce1955f` (independent worktree, not merged).
Turso gitlink for that base: `24a3a4609683b73935888f8bc30b47727d100aa8`
(`prototypes/p0/turso`, cloned locally into the worktree for the build; the superproject gitlink was not moved).

## Scope

`src/object_store/s3.rs` (`put_group` plus one helper), `src/object_store/http_mock.rs`
(test-only helper), `tests/object_store.rs`, this note.

Not touched: `src/object_store/local.rs` (single-barrier semantics), `transport.rs` retry
policy, WAL / recovery / io / authority semantics, the Go platform, SDK/E01, the root
publication protocol.

## What the base actually did

At the base commit `S3ObjectStore` had no `put_group` override, so S3 inherited the trait's
portable fallback (`src/object_store.rs:159`): one `put` per write, in order, one request at a
time. The bodies of an authority commit are already-materialized payloads, so that fallback
pays one network round trip per body, serially, on the durability path.

## Design

* The adapter executes the group. Every non-final write goes out concurrently, bounded by
  `GROUP_UPLOAD_PARALLELISM = 4`; the final write is then issued by the calling thread, after
  every body has been confirmed. The group's visible write therefore never goes out while a
  body it could reference is unconfirmed, and the returned etags stay aligned with `writes`.
* Eligibility is checked, not assumed. `group_bodies_are_independent` requires every non-final
  write to carry `PutCondition::Any` or `PutCondition::Absent`, to target a key no other write
  of the group targets, and not to share the final write's key. A group that fails the check
  takes the trait's serial, order-preserving path verbatim, so no caller that relies on write
  order can be reordered by this adapter.
* Failure is fail-closed. The first body error stops new dispatch, the writes already
  dispatched are joined, and the error is returned: the final write is not attempted, and an
  Unknown network outcome is never reported as success. Nothing is retried beyond
  `S3Config::retries`.
* `group_rejection_publishes_nothing()` stays `false`: S3 has no cross-object conditional
  write, so a rejected group cannot promise that nothing became visible.

## Contract audit

Callers in this tree:

| site | bodies | final write |
|---|---|---|
| `src/authority.rs:410-420` | page/blob bodies, `PutCondition::Any` | `authority/root`, `Match(current.etag)` |
| `src/authority/migration.rs:93-99` | page bodies, `PutCondition::Any` | `authority/root`, `Match(previous.etag)` |
| `src/authority/namespace_backend.rs:169`, `src/object_store/metrics.rs:62` | pass-through wrappers (fencing/accounting), no reordering of their own | same |

Deferred page writes are created at `src/authority/paged_index.rs:455` with
`PutCondition::Any`; blob bodies at `src/authority.rs:410` with `PutCondition::Any`.

Key namespaces are disjoint by construction — `authority/index-pages/{hash}`,
`authority/blobs/{blob}`, `authority/root` — so a body cannot collide with the CAS key, and
content addressing makes a repeated body key identical-bytes idempotent in the caller's usage.

What the adapter cannot verify is the caller's claim that an earlier write is "harmless to
publish on its own"; that stays the caller contract. What it can verify (no conditional body,
no repeated key, no key shared with the final write) is enforced, and everything else falls
back to the serial order.

`src/authority/namespace_backend.rs:176-190` reads `group_rejection_publishes_nothing()` and
`writes.len() > 1` to mark an initialization attempt uncertain. That declaration must stay
`false` and does; the fallback path also keeps `writes.len()` and per-write conditions
unchanged.

## Evidence

Commands, run offline with `CARGO_BUILD_JOBS=2`, `CARGO_NET_OFFLINE=true`, and a
worktree-local `CARGO_TARGET_DIR=prototypes/v1/target`:

| command | result |
|---|---|
| `cargo check --manifest-path prototypes/v1/Cargo.toml --locked --offline` | exit 0 (the only warnings are 9 pre-existing ones in `turso_core`) |
| `cargo test --manifest-path prototypes/v1/Cargo.toml --locked --offline --test object_store` | exit 0 — 26 passed, 0 failed, 1.02s |
| `cargo clippy --manifest-path prototypes/v1/Cargo.toml --locked --offline --lib --test object_store --no-deps -- -D warnings` | exit 0 |

Test matrix (all six new tests are in `tests/object_store.rs`, mock HTTP S3, no credentials):

| requirement | test |
|---|---|
| bounded concurrency, peak observed exactly at the bound | `s3_group_uploads_immutable_bodies_with_bounded_parallelism` — the mock holds each connection until the batch of bodies the client still owes is connected, so a serialized upload fails the peak assertion instead of passing by timing |
| success only when every write succeeded; etags in `writes` order | same test (9 bodies + CAS, per-index etag and body bytes, signed requests) |
| body bytes and identity | same test asserts body bytes, `if-none-match`, `x-amz-meta-peri-sha256` per body, plus `verify_signature` on every request |
| single body failure | `s3_group_body_failure_never_publishes_root` (HTTP 500, mock accepts a CAS but none is sent) |
| truncated / partial reply | `s3_group_unknown_body_response_fails_closed` (Content-Length 64, 7 bytes delivered, connection closed) |
| root condition conflict | `s3_group_root_condition_conflict_is_typed_and_not_atomic` (typed `PreconditionFailed`, bodies allowed to have landed, `false` declaration pinned) |
| repeated key and conditional body keep serial order | `s3_group_dependent_bodies_keep_serial_order` |
| empty and single-write groups | `put_group(&[])` in the first test; `s3_group_single_write_is_one_conditional_put` |

## Not measured

* No real S3 endpoint, no credentials, no paid traffic, no performance run. Mock timings are
  not evidence of S3 benefit.
* The 26-test run covers the `object_store` target only; the wider `cargo test` suite and
  `--all-targets` clippy were not run in this window.
* A worker panic is converted into `Err` by design (never success, never a CAS); no test
  injects a panic into a worker.

Environment needed for an independent baseline measurement on real S3: an S3-compatible
endpoint plus `S3_ENDPOINT` / `S3_BUCKET` / `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`
(optionally `S3_REGION`, `S3_SESSION_TOKEN`) supplied through the environment, never the
tree; a round-trip time of at least a few milliseconds for the gain to be visible; and two
arms of the same commit (`GROUP_UPLOAD_PARALLELISM = 1` vs `4`) over the same commit workload,
recording group wall time, PUT count, bytes, and p50/p99. `trial-ground/local_s3_smoke.py`
and `trial-ground/p4_s3_preflight.py` in the repository root drive local-endpoint checks.

## Risks

1. The bound is **per group, not global**. `N` groups committing at once (several branches or
   authorities against one store) can reach `4·N` threads and `4·N` in-flight PUTs. This tree
   has no global admission limiter around `put_group` (`src/authority_rpc/client.rs` bounds RPC
   sockets only). A store-wide cap is an architecture decision, so it is deliberately not
   invented here; the exposure grows with commit concurrency and should be re-checked against
   the platform's admission policy before this rides on a busy multi-tenant store.
2. Per-commit thread cost: `thread::scope` creates up to four threads per group, and there is
   no pool to reuse (blocking `reqwest`, no async runtime in this crate). Against S3 round
   trips this is small, but it is unmeasured.
3. The durable path configures `retries = 3`, so one body can cost up to four requests.
   Concurrency stays at four because retries are serial inside a worker, but worst-case
   request volume per commit grows.
4. `src/object_store/metrics.rs` still records `writes.len()` puts and divides elapsed time by
   that count, so per-write latency becomes an average once writes overlap. Counts and totals
   stay comparable; single-write latency semantics do not.
5. Worker panics inside the scope are joined and reported as `Err("S3 group upload worker
   panicked")`; a successful return is impossible without every body etag recorded.
6. Depends on the caller contract for the visible write being last. The adapter enforces
   everything it can observe; it cannot read a body's content to prove unreachability.
