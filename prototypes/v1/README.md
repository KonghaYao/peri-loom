# Segmented v1 runtime

This standalone workspace extends P0 against the tracked Turso fork in
`../p0/turso`. P1 stores authoritative database generations and WAL in an
`ObjectStore`; workers retain only disposable segment caches and an in-memory
WAL image. No local main database or persistent local WAL is used.

## Run

```sh
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-store /tmp/peri-cache tenant database demo 400
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-store /tmp/peri-cache tenant database inspect
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-store /tmp/peri-cache tenant database query 'SELECT count(*) FROM records'
```

The first path is the authoritative local-object backend, **not** a worker cache.
For an S3-compatible service replace that argument with `s3`, and supply
`S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, optionally
`S3_REGION` (default `us-east-1`) and `S3_SESSION_TOKEN` through the environment.
Do not put credentials in this repository. Provision the bucket separately.

`exec`, `query`, `checkpoint`, `inspect`, `demo`, `crash-write`, and
`checkpoint-wait` commands are available. Inspection reads remote metadata but
loads no segments. Separate workers use separate cache directories.

## Validate

```sh
cargo test --manifest-path prototypes/v1/Cargo.toml --locked
cargo fmt --manifest-path prototypes/v1/Cargo.toml -- --check
cargo clippy --manifest-path prototypes/v1/Cargo.toml --locked --all-targets --no-deps -- -D warnings
```

Real S3 service tests are deliberately ignored by default. Set `PERI_S3_TEST=1`
and `PERI_S3_TEST_ENDPOINT`, `PERI_S3_TEST_ACCESS_KEY`,
`PERI_S3_TEST_SECRET_KEY`, then run:

```sh
cargo test --manifest-path prototypes/v1/Cargo.toml --locked --test minio -- --ignored --test-threads=1
```

These tests create uniquely named buckets and normally delete their own objects
and buckets on exit, including when a test fails. For first-failure forensics,
set `PERI_S3_TEST_RETAIN=1` before running: every bucket created by that run is
retained on success or failure, without deleting any objects or buckets. Before
the first bucket HEAD/PUT request, the test prints only the generated bucket name
as a retention identifier, not the endpoint or credentials. Even if setup fails
or PUT has an unknown outcome before the fixture exists, use this identifier to
inspect the exclusive endpoint; the notice does not prove creation succeeded.
Archive and inspect retained buckets, then clean them up manually when the
investigation is complete. Use these tests, especially
retention mode, **only** with an exclusive disposable S3 endpoint and credentials;
never point them at shared or production storage. Do not count ignored service
tests as passing in a local-only run.

The four ignored `p4_completion` real-S3 fault tests use a different fixture:
they do not provision or delete buckets. Run one at a time only after independently
confirming an exclusive endpoint and a separately provisioned bucket per test,
with `PERI_P4_S3_TEST=1` and an existing private (0700), non-symlink
`PERI_P4_S3_EVIDENCE_DIR`. Each run retains its own 0700 local directory and
prints its path even if startup fails; spawned service stderr is saved there in
0600 files (up to the most recent 64 KiB per process). A test still shuts down
or kills only its own child services on failure; do not interpret that as a
successful recovery, and never delete the retained directory or remote bucket
after an unknown result. The test opt-in checks neither Docker project
ownership nor whether the endpoint/bucket is exclusive, and is **not** a
substitute for the P0–P6 joint gate or a license to run on a shared service.

### Historical SQLite semantic comparison

The 15-case `sqlite_quick_compat` differential test previously invoked the
installed `/usr/bin/sqlite3` executable and compared typed rows with native
Turso. Its historical source is retained, but the test is explicitly ignored
by default because the project retired SQLite reference and differential
workflows. Do not count this ignored test as v1 or Turso native coverage.
Current verification uses the no-reference Turso suite and the v1 contract regressions.

## Boundaries

P1 provides remote durability, conditional metadata updates, integrity checks,
bounded clean-page caching, and cold reconstruction after worker loss. A single
cache directory has an exclusive lifetime lock, including retained native file
handles. Native SQL sync settings cannot disable remote commit confirmation.
The WAL protocol is documented in `src/wal/PROTOCOL.md`.

P1 alone is **not** distributed writer ownership: its WAL epoch check does not
fence database HEAD publication, and direct object-store credentials are trusted.
P4 must provide one authoritative fencing boundary. Remote calls are synchronous;
this workspace prioritizes correctness evidence over production performance.

## P2 storage capabilities

The branch-aware path uses a durable namespace root, unique immutable physical
blob instances, and one root CAS for refs, WAL publication, commit history,
reader pins, upload intents and GC tombstones.
The P1 commands remain a compatibility harness for the original physical layout;
the branch-aware commands below use the new root domain. They do not implicitly
import a P1 namespace. Subsequent runtime services must use the authority path.

```sh
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-main tenant database branch-exec main 'CREATE TABLE items(id INTEGER PRIMARY KEY); INSERT INTO items VALUES(1)'
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-main tenant database snapshot main before
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-main tenant database branch main feature
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-feature tenant database branch-exec feature 'INSERT INTO items VALUES(2)'
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-copy tenant database clone before copy
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-copy tenant database branch-query copy 'SELECT * FROM items'
cargo run --manifest-path prototypes/v1/Cargo.toml -- /tmp/peri-root-store /tmp/peri-main tenant database refs
```

`restore BRANCH SNAPSHOT`, `pitr BRANCH COMMIT_SEQUENCE`, and
`pitr-time BRANCH TIMESTAMP_MS` atomically change recovery references and require
reopening the engine. `refs` lists retained native commit sequences and times.
Restoring an older state does not rewind commit-sequence or timestamp high-water
marks. Recovery points carry immutable HEAD/WAL references, including checkpoints
and WAL resets. Snapshots and clones share segments within one namespace; this
does not promise cross-namespace cloning.

`drop-branch NAME`, `drop-snapshot NAME`, `retain-after BRANCH MIN_SEQUENCE`,
`gc-inspect`, and `gc SAFETY_WINDOW_SECONDS` manage retention and collection.
GC first CAS-tombstones unreachable physical instances; a later invocation after
the safety window deletes them. A zero window is intended only for controlled
tests. Tombstone identities remain recorded, and sweeps also remove late physical
reappearances without touching a newer instance of identical content.

Reader pins are durable and released when the final branch-store/native handle
drops. Worker crashes can conservatively leak pins. Unresolved upload intents are
not automatically expired or deleted: unknown in-flight operations must never be
declared safe merely because a timeout elapsed. These trade availability/storage
usage for safety; distributed pin expiry and large-scale metadata compaction are
not production claims. The whole root is a prototype CAS document, not a scalable
catalog implementation.

## P3 multi-database worker

Start the trusted local-development worker with a storage directory (or `s3`)
and a disposable cache directory:

```sh
cargo run --manifest-path prototypes/v1/Cargo.toml -- worker /tmp/peri-root-store /tmp/peri-worker-cache
```

The process accepts one JSON request per stdin line and flushes one JSON response
per stdout line. Request IDs are strings or unsigned integers. For example:

```json
{"id":"open","op":"open","database":{"namespace":{"tenant":"tenant","database":"database"},"branch":"main"},"epoch":0}
{"id":"create","op":"exec","session":"s1","sql":"CREATE TABLE items(id INTEGER PRIMARY KEY)"}
{"id":"insert","op":"exec","session":"s1","sql":"INSERT INTO items VALUES(1)"}
{"id":"read","op":"query","session":"s1","sql":"SELECT * FROM items"}
{"id":"compact","op":"compact","session":"s1"}
{"id":"close","op":"close","session":"s1"}
{"id":"evict","op":"evict","idle_ms":0}
{"id":"status","op":"status"}
```

Each open creates an independent native SQL connection; explicit transactions
can span requests. Closing a session or reaching EOF aborts its unfinished
transaction. `close` releases a session; `unload` removes an idle database, while
`drain` rejects new sessions and unloads after its final existing session closes.
`checkpoint` persists dirty segments; `compact` additionally replaces the current
WAL replay image with a small checkpoint baseline. Historical snapshots/PITR
retain their original immutable WAL objects. Cold-open fetches segments lazily.

Default admission bounds are 32 database instances, 128 total sessions and 16
sessions per database. Loading and retiring instances count against capacity;
slow backend work and native teardown do not hold the global registry mutex.
The library's `WorkerLimits` also configures each database's clean RAM/SSD segment
cache (8 MiB/64 MiB by default). `status` reports cache payload, dirty segment,
WAL/pending WAL bytes and segment loads; these are not process RSS or native
Pager-allocation measurements. Unload removes instance accounting, not the
disposable SSD cache files. The entire worker cache directory may be deleted
after stopping its process.

This P3 command is a trusted development adapter: its epoch identifies a local
loaded instance, not a distributed ownership grant. It retains access to the
configured storage backend. P4 must put credentials behind the authenticated
storage authority and enforce grants at every durable boundary; do not expose
this stdin protocol as a production network service.

## P4 ownership services

The distributed-ownership path is `authority` plus `owned-worker`, not the trusted
P1/P2/P3 backend commands. Build once, then start services with separate process
environments:

```sh
cargo build --manifest-path prototypes/v1/Cargo.toml --locked
prototypes/v1/target/debug/peri-loom-v1 authority /tmp/peri-root-store 127.0.0.1:9150 /tmp/peri-authority-state
prototypes/v1/target/debug/peri-loom-v1 owned-worker http://127.0.0.1:9150/ /tmp/peri-worker-a worker-a 127.0.0.1:9151
```

Authority requires `PERI_AUTHORITY_CONTROL_TOKEN` and
`PERI_AUTHORITY_WORKERS_FILE` (a private 0600 JSON file mapping worker IDs to
distinct authority credentials). Its newly created state directory is private;
existing directories must be 0700. Replace its STORE argument with `s3` and set
the S3 environment described earlier **only for the authority process**. This
path disables backend mutation retries so unknown outcomes cannot be hidden.

Each owned worker requires `PERI_WORKER_AUTHORITY_TOKEN`,
`PERI_WORKER_CONTROL_TOKEN` and `PERI_WORKER_SQL_TOKEN`. Credentials are distinct,
32..256 non-whitespace ASCII bytes. Never supply raw S3 credentials/backend paths
to these workers; use clean environments or separate service configurations.
Each prints its actual bound address as a ready JSON line. Listeners currently
accept loopback addresses only, with explicit internal authentication.

Authority HTTP `/` uses versioned `RpcRequest` from
`src/authority_rpc/protocol.rs`. Worker `/control` and `/sql` use `WorkerRequest`
from `src/worker_rpc/protocol.rs`, with separate bearer credentials. The control
path has only the six coarse ownership commands; SQL sessions are random opaque
IDs. P5 supplies placement/epoch allocation and lease orchestration. Required
grant roles prevent Reader-to-Writer amplification; native ReadOnly connections
also reject writes submitted through query APIs.

Do not concurrently run legacy raw-backend CLI commands against an active
authority namespace. A completed worker takeover fences the old epoch at storage
publication, not merely SQL ingress. Unknown backend outcomes block that namespace.
An unclean authority stop keeps a durable dirty marker and refuses automatic
restart; only confirmed old-backend quiescence permits explicit `--reconciled`
startup. A Shutdown reply announces stopping; successful process exit proves the
clean-stop marker. This stage does not claim overlapping authority HA or deadline
cancellation of already dispatched physical requests. See
the P4 authority and platform documentation for exact compatibility boundaries.
