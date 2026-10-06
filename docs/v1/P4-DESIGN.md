# P4 implementation boundary

This stage builds on committed P3 `bd2bfed`. It implements a single, exclusive
credential-bearing Rust storage authority and independent credential-free SQL
workers. Go will allocate desired placements/epochs in P5; it must not send page,
frame or manifest-construction commands. This document describes the implemented
contracts, not a final verification verdict.

## Services and principals

- `authority STORE LISTEN STATE_DIR [--reconciled]` exposes authenticated
  loopback HTTP at `/`. Only this process receives raw S3 credentials or the
  writable local backend directory.
- `owned-worker AUTHORITY CACHE WORKER_ID LISTEN` receives its own authority
  credential, a separate controller credential, and a separate gateway SQL
  credential. `/control` accepts the six coarse ownership commands; `/sql`
  opens independent native sessions and executes SQL against granted databases.
- Authority control and individual worker identities have distinct credentials.
  Only control can issue nonce challenges, grant/renew/revoke, run named P2
  maintenance operations, or stop the authority. Workers cannot replace roots,
  pass physical keys, delete backend objects, or mint epochs.
- Worker requests for unknown/ungranted database identities are rejected before
  lazy namespace initialization. Authority sessions are opaque random IDs bound
  to one principal, lease, database/branch and access role.
- Required role fields in grant intent, durable ownership and lease binding
  prevent a ReadOnly grant being amplified to Writer. Writer grants may narrow
  individual sessions to ReadOnly. SQL clients cannot select a stronger role.

HTTP parsing rejects duplicate headers, transfer-encoded requests, unknown JSON
fields, wrong methods/paths, oversized headers/bodies and expired request reads.
Listeners are nonblocking for bounded admission, but accepted streams are
explicitly switched to blocking mode before deadline-bound reads and writes.
Real macOS process tests exposed inherited nonblocking sockets returning EAGAIN;
the fragmented-request regression exercises the repaired path directly.

## Final durability fence

Every scoped raw upload/delete and every root-CAS retry/publication checks the
live lease and exact durable namespace/authority term, branch incarnation, epoch,
worker, lease/grant identity and role. Root serialization precedes the final
dispatch check. WAL chunks/indexes must carry the current writer epoch; HEAD
publication checks generation lineage as well as the conditional root update.
The same root CAS contains ownership, engine publication, refs, pins, upload
reservations, history and GC state. No worker can perform maintenance deletion.

Namespace admission tickets cover the whole synchronous backend operation.
Ownership transitions close old admission and settle admitted operations before
publishing the new owner. Physical requests dispatched during a valid lease may
complete later; following stages must revalidate. **There is no claim that a
generic S3 server cancels already-dispatched work at the lease deadline.** A
completed takeover barrier admits no unresolved old mutation. Timeouts, socket
closure or one absent-object readback are not terminal proof: uncertainty closes
that namespace, prevents new grants, and retains upload intents/GC identities.

Raw ObjectStore implementations explicitly opt into the strict-outcome contract;
the default is refusal. Local/Memory stores opt in. S3 opts in only with zero
transport retries; the authority CLI selects that configuration and disables
HTTP protocol retries. Legacy P1 retrying S3 clients remain compatibility tools,
not an eligible P4 authority backend. This prevents a hidden earlier timed-out
PUT from being mistaken for a settled operation after a later successful retry.
Initialization and ownership boot also fail closed on unknown publication ACKs.

## Leases and runtime

Authoritative deadlines use a suspend-inclusive monotonic clock, not wall-clock
timestamps or persisted lease deadlines. Linux uses CLOCK_BOOTTIME; macOS uses
continuous Mach time. Clock failure/regression/overflow invalidates admission.
Grant challenges bind the exact intent and expected root/branch ownership and
expire after five seconds. Lease TTL is bounded to 100..60000 ms. Single-use
nonces, same-CAS payload-bound command receipts, exact renewal sequences and
strictly higher takeover epochs prevent delayed or replayed commands reviving
expired/revoked ownership. Renewal sequence is anti-replay metadata: an older
session on the same lease remains valid after a successful renewal, but cannot
issue an out-of-order renewal or change identity/role.

Workers measure fresh authority validation from request start, subtract a safety
margin, and reject responses that arrive too late. They guard cached SQL and each
native statement, IO mutation, WAL sync, checkpoint and publication. Acquiring
and teardown reservations/generation checks prevent delayed cold-open or cleanup
callbacks clobbering a newer owner. Draining stops new transactions while allowing
already-started transactions to finish within the lease; COMMIT ACK is not turned
into a false error by a guard after the final statement. Revoke/expiration stops
durability immediately; unfinished native transactions are aborted, not flushed.

ReadOnly uses actual Turso native ReadOnly open flags on every connection and IO
path, not a SQL classifier or resettable PRAGMA. ATTACH and extension loading are
disabled. A worker-server watchdog releases expired/revoked network-session
handles outside global registries; native teardown and remote calls do not hold
registry mutexes.

Cached page/RAM-WAL IO uses `ObjectStore::validate_local`: cheap known-generation,
role, monotonic lease and local fencing checks, without fetching/deep-validating
the catalog on every page. The conservative trait default still performs full
session validation; trusted legacy BranchStore and guarded runtime/service
wrappers explicitly opt into the cheap path. New SQL statements, real remote
GET/PUTs, WAL durability, checkpoint publication, HEAD/root CAS and GC retain
authoritative checks. Named restore/delete reconcile known in-memory grants
before reopening admission, including idempotent command replay.

The first frozen all-package P4 run exposed an actual regression: the unchanged
P3 post-SIGKILL cold-recovery test repeatedly exceeded its eight-second response
deadline at 100% CPU. Per-page full catalog validation was the cause. Separating
local non-durable checks from final authority checks restored that same test
without raising its timeout. Instrumentation requires zero root GETs during
repeated cached/RAM IO, while actual sync/publication still increases full
validation and root-read counters. These are acceptance/regression observations,
not the post-P6 load-test results or a general throughput claim.

## Root compatibility and refs

The codec hashes the original embedded raw JSON before deserializing the extended
type. Exact P2/P3 writer output remains valid. The captured 14-object fixture is
from the already-built P3 CLI before P4 rebuild: restored current sequence 2,
high-water 3, real snapshot and native WAL history. Externally rewritten embedded
JSON is not silently normalized to conceal checksum disagreement.

Exclusive boot explicitly migrates schema V1 to V2, advances the authority term,
revokes prior grants and derives epoch floors from retained WAL evidence. It
preserves permanent root/physical identities, refs, current restored counters,
history, intents, tombstones and receipts. Restore/delete revoke the affected
ownership in the same root CAS; deletion retains epoch tombstones. Final pin
release is a narrowly authorized service-maintenance action under the namespace
barrier, not permission for a stale writer to publish.

## Restart and availability limits

One private state directory and an exclusive lifetime process lock are required
for this deployment. Its marker is made dirty and fsynced before service/backend
startup. A normal stop joins admitted requests, releases sessions and verifies
quiescence before marking clean. A Shutdown RPC response means **stopping**, not
proof of a completed clean shutdown: wait for successful process exit. Unknown
outcomes leave the marker dirty. Default restart after an unclean stop refuses
service. `--reconciled` is an explicit operator assertion that the old issuer is
gone and its backend operations have actually settled, not an automatic timeout.

This is worker failover behind one authority, **not overlapping authority HA**.
Another state directory/host does not establish exclusive backend ownership.
Raw legacy writers must be removed before activating P4; don't run old trusted
backend commands concurrently with the authority. TLS/external identity lifecycle,
large-scale catalog/receipt compaction and platform quotas belong to P6 or the
post-joint-verification performance work, not current P4 completion claims.
