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

These tests create uniquely named temporary buckets and clean only their own
fixtures. Never point them at a production service without bucket-creation
permission. Evidence and service-specific limitations are recorded in
`../../docs/v1/P1-VERIFICATION.md`.

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
reader pins, upload intents and GC tombstones. See `../../docs/v1/P2-DESIGN.md`.
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
