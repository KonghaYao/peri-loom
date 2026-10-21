# Go ownership controller

This standalone Go 1.23 module implements desired placement. Native Rust workers
and the credential-bearing Rust authority remain responsible for database I/O,
transaction state, durability and final fencing. Go has no page/WAL/segment RPC.

## Run

Build the Rust service from the repository root:

```sh
cargo build --manifest-path prototypes/v1/Cargo.toml --locked
cd prototypes/v1/controller
go build ./cmd/peri-controller
./peri-controller -config /private/controller.json -state /private/controller-state -listen 127.0.0.1:9100
```

The configuration must be a private regular file, mode `0600`; directories must
be private. Substitute independently generated service credentials of at least
32 graphic ASCII characters; never reuse an authority or worker credential:

```json
{
  "authority_url": "http://127.0.0.1:9002",
  "authority_token": "GENERATE_A_DISTINCT_CONTROL_CREDENTIAL",
  "api_token": "GENERATE_A_DISTINCT_CONTROLLER_API_CREDENTIAL",
  "workers": [
    {
      "worker": {
        "id": "worker-a",
        "address": "http://127.0.0.1:9003",
        "region": "local",
        "capacity": 32,
        "load": 0,
        "healthy": false,
        "last_heartbeat_ms": 0,
        "draining": false
      },
      "control_token": "GENERATE_A_DISTINCT_WORKER_CONTROL_CREDENTIAL"
    }
  ]
}
```

The authority's worker credential file contains **different** worker-to-authority
capabilities. The worker's SQL token is also distinct from its control token.
Go does not receive S3 credentials; workers do not receive S3 credentials or the
raw backend path. Listeners are authenticated literal-loopback HTTP boundaries;
external TLS termination belongs to the platform layer.

## Management boundary

Send `Authorization: Bearer <api_token>`:

- `GET /v1/state`: durable desired placement and observed worker registry.
- `POST /v1/workers/register`: `{"worker":{...}}`.
- `POST /v1/workers/drain`: `{"worker_id":"worker-a","draining":true}`.
- `POST /v1/databases/place`: database ID, region and `writer`/`read_only` role.
- `POST /v1/databases/migrate`: database ID and destination `worker_id`.
- `POST /v1/databases/suspend` or `/resume`: database ID.
- `POST /v1/reconcile`: drive a reconciliation cycle explicitly.

A database ID is `{"namespace":{"tenant":"tenant","database":"db"},"branch":"main"}`.
Each action returns convergence or an explicit error, not an invented SQL commit
receipt. SQL sessions use the selected worker's independently authenticated SQL
boundary. An ambiguous SQL write must never be automatically retried.

## Durability and availability limits

One controller owns one private metadata directory under an exclusive advisory
lock. Each update appends one checksummed delta record to `metadata.journal`,
fsyncs it, and only then publishes the new in-memory revision; the journal is the
single durable revision authority. Records carry the revision, the previous
record checksum and a payload checksum, so replay rejects gaps, duplicate
revisions, broken chains and checksum failures. A trailing record without its
terminating newline is a torn append and is discarded; a complete corrupt record
refuses startup. `metadata.json` is a derived snapshot, replaced when journal
entry/byte thresholds are reached. The journal is truncated only after the new
snapshot rename and directory fsync are durable. Recovery starts from the
snapshot and replays a bounded journal (currently 8192 records or 256 MiB, with
compaction every 256 records or 8 MiB); stale journal prefixes left by an
interrupted compaction are checked against the snapshot anchor and skipped. A
directory containing only the lock marker is never treated as a fresh store.
Stores created before this format (version 1 snapshot plus `epoch-floor.json`)
are validated and migrated atomically. Unsupported versions, checksum/floor
mismatches, ambiguous legacy/journal mixes and a legacy floor ahead of the
snapshot refuse startup. Uncertain metadata persistence poisons the store and
refuses further allocation until reopening verifies the durable state.
`UpdateAtRevision` rejects stale conditional commits, and `StoreMetrics` exposes
journal bytes/entries, replayed entries, and compaction count/duration.
Do not reset/delete these files to recover a deployment. Coordinated rollback of
both snapshot and journal is **not** prevented by a local embedded store: preserve
the authoritative Rust epoch floor and use external monotonic consensus for HA.

Desired epochs are globally allocated and persisted before mutating Rust RPC.
Pending mutators retain exact payload/request identity for receipt replay, never
inferred cancellation from timeout. Stored wall timestamps diagnose heartbeat
health; they do not reconstruct Rust lease deadlines. After restart the controller
queries actual ownership. Rust alone issues fresh nonce-bound grants/renewals and
enforces suspend-inclusive monotonic deadlines.

Worker capacity/health/draining/region govern placement. A failed heartbeat is a
policy signal, not proof that old I/O stopped. Failover first advances the durable
Rust fence and then acquires the replacement. Graceful migration first drains the
old native sessions; active transactions must finish or be explicitly revoked.
This deployment is single-controller/single-authority, not an HA consensus cluster.

## Test

```sh
go test ./...
go test -race ./...
go vet ./...
```

Independent process tests require the real Rust binary and do not substitute mocks
for accepted-write recovery, cold reopen, stale-owner rejection or migration.
