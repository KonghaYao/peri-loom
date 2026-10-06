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
