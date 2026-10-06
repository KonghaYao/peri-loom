# P5 — durable Go desired ownership

Implementation: `prototypes/v1/controller`, a standalone Go 1.23 module without
an external database dependency. Rust P4 remains the actual lease/epoch and final
storage fence. The controller never implements pages, segment reads, WAL frames,
transaction state, checkpoint algorithms or manifest construction.

## Independent batch boundaries

- Metadata and registry: private checksummed embedded state, lifetime process
  lock, epoch/revision floor, atomic persistence and placement policy.
- RPC boundary: bounded authenticated literal-loopback HTTP, strict version and
  command identity, credential separation, no transport retries.
- Reconciliation: durable desired phases, global epoch allocation, grants and
  renewals, heartbeat failure detection, failover, graceful drain and suspend.
- Integration: actual Go HTTP service/CLI, private configuration and API gates.
- Independent verification: real authority and two real native worker processes,
  SIGKILL, complete cache deletion, cold recovery, loss of a committed grant ACK,
  metadata reopen, active transactions and stale owner revival.

## Durable allocation and reconciliation

The embedded state holds worker addresses/regions/capacities and observed health,
plus desired database placements. Epoch allocation uses a global high-water mark
larger than both controller state and the queried Rust floor. The allocation and
desired intent are fsynced **before** the challenge/grant mutation. The exact
nonce-bearing request and request ID are persisted before dispatch. Ambiguous
mutators retain the original request for receipt replay before readback or a new
mutation; a transport timeout is not proof of cancellation.

The Rust grant completes its namespace takeover barrier before the replacement
worker is asked to acquire. The replacement validates fresh authority TTL; Go's
saved wall-clock diagnostics never reconstruct or extend native deadlines.
Renewals advance exactly one sequence, persist the request, and reconcile an
already-applied worker response without replaying an equal-sequence renewal.

Graceful migration records a drain phase and waits for the old worker to report
detached. An unfinished transaction/session prevents a successful migration ACK;
the caller can finish/rollback and close it, then reconcile. Failure failover uses
the same higher-epoch grant/acquire path but does not mistake missing heartbeats
for proof that old backend I/O stopped. A restored old worker still meets Rust's
durable fence, not merely a Go routing decision. Suspend survives controller
restart and only an explicit resume allocates another active lease.

## Registry and policy

Workers start unhealthy; authenticated actual state polling supplies identity,
load and heartbeat. Selection uses freshness, health, draining state, capacity,
region and deterministic load/capacity ranking. Re-registering an existing worker
preserves observed health/load/heartbeat/draining state and forbids address or
region rebinding under its identity. A replacement endpoint needs a new identity
and an explicit migration. Polling/RPC errors do not grant ownership directly.

Controller operations are serialized separately from the metadata lock. Network
I/O is never performed while holding the metadata mutex. This is intentionally a
simple policy implementation; long RPCs/global serialization, whole-state JSON
copies and per-cycle polling are candidates for architecture-first pressure
testing, not claimed high-scale scheduler performance.

## Failure and security limits

One controller owns one private directory under a nonblocking lifetime lock.
Atomic checksummed metadata and separately fsynced epoch/revision/digest floor
detect single-file rollback and interrupted replacement. Uncertain persistence
poisons the store and refuses further allocation. Missing/corrupt existing state
must never be reset as a fresh deployment. Coordinated rollback of every file is
not prevented by a local filesystem; this is not an HA/consensus service.

Only pre-dispatch HTTP rejection or a typed precondition is a definitive RPC
rejection. A generic Rust negative response can occur after publication or an
unknown backend outcome, so it is conservatively ambiguous. Some expired nonce
or irrecoverable pending requests require explicit operator reconciliation;
the controller does not silently abandon them and claim success. An authority
namespace with unknown raw I/O remains unavailable under P4's rules.

Service credentials are distinct and kept in private regular configuration
files, not desired-state metadata, command arguments or logs. Go has authority
control credentials and worker control credentials, but no S3 credentials or raw
backend path. SQL credentials remain separate. Listeners are authenticated
loopback HTTP for this phase; external routing/TLS/RBAC belong to P6.

## Acceptance scope

See `P5-VERIFICATION.md` for frozen hashes, real test counts, failures/fixes and
commands. Process acceptance uses a real local object backend behind the Rust
authority. It does not mislabel controller mocks as native durability evidence,
nor claim a Go-through-S3 experiment before that experiment is executed. P4's
unchanged S3 implementation and the later whole-project S3 joint/pressure runs
are tracked separately.
