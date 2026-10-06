# P6 implementation design

## Components and boundaries

`prototypes/v1/platform` owns organization/project identity, RBAC, credential
issuance, routing, quotas, management, an embedded admin UI, audit, and the
private KMS broker. Its controller dependency is the already verified P5 Go
module. Native SQL, pager, transaction semantics, durable fencing, encrypted
pages, snapshots, PITR, and physical storage remain in Rust/Turso. Go does not
implement a pager or reinterpret SQL for authorization.

The platform runs the controller reconciliation loop, public TLS gateway and
management listener, and a separate private loopback broker listener. The
local launcher starts a separate authority and two workers; it exposes only
the public gateway, checks readiness through real authenticated APIs, and
keeps durable service identity across clean restarts. A deployment shares
one network namespace because private RPC requires literal loopback addresses.

## SQL admission and session safety

Opaque sessions bind the authenticated subject, project, database, upstream
worker, and ownership epoch. Every dispatch rechecks current identity and
placement. Read-only tokens require actual native read-only placement; Query
is not assumed read-only. Missing, stale, or migrated bindings fail before
native dispatch. There is no transparent SQL replay after transport failure.

Project quotas bound session counts, concurrent native dispatches, offered
request rates, SQL bytes, and returned bytes. Sessions have a non-blocking
single-dispatch lock. Native workers separately bound accepted HTTP requests,
SQL admission, sessions, cooperative VM steps, elapsed statement time, rows,
and result accounting. The native trusted compatibility CLI retains its old
larger SQL profile; public worker listeners establish the restrictive profile.

A typed terminal native SQL failure permits explicit rollback and later
session reuse. It does not guarantee no mutation or no committed batch prefix.
Unknown replies retain permits and poison sessions; gateway shutdown cannot
call a timeout or close acknowledgement a proof of cancellation. Native
physical-I/O uncertainty continues to block namespace takeover as required by P4.

## Identity and key hierarchy

API keys are high-entropy opaque credentials stored only as constant-time
verified digests. Global administration is separate from project SQL rights.
Signed short-lived database tokens contain issuer/project/role/root/branch
incarnation/domain and are checked against current durable state on every call.
Issuer revocation, delete, restore, and stale domain invalidate old tokens.
Organization/project creation derives an isolated tenant name and persists
quota reservations before contacting the KMS or ownership service.

New namespaces receive a private random DEK, initially wrapped as unbound.
The first fresh Rust grant binds it to the immutable native root and domain;
later broker requests validate authenticated worker, lease, role, and exact
binding on both sides of unwrap. Native AEGIS page encryption protects main,
WAL, and immutable segment cache images; every independent session supplies
the resolved key. Missing/wrong keys and broker uncertainty fail closed.
KEK rotation/rewrap preserves the DEK and retained native history.

The private local keyring provides development KMS behavior, not managed HSM
assurances. DEKs are wrapped with AES-GCM, but the KEK file itself contains
unencrypted key material under filesystem access controls. Compromise of the
key-management host is outside the stored-page/WAL confidentiality guarantee.
Metadata keeps namespace identities and historical DEKs rather
than claiming deletion is secure erasure. Anti-rollback local floors protect
individual-file rollback/corruption, not coordinated restoration of all files.

## Management and inspection

Management uses authenticated named commands for create/delete, lifecycle,
branch/clone/snapshot, restore/PITR, checkpoint, compact, refs, and GC. It does
not expose arbitrary native control capabilities to public clients. Restore
uses the existing Rust destructive root CAS/fence and waits for controller
convergence before recording the new branch incarnation.

Inspection combines durable refs and placement with explicitly diagnostic
native generation/WAL/cache snapshots. Diagnostics are not ownership proof.
Native metrics distinguish physical backend operations, end-to-end segment
RPCs, actual WAL append durability, new validated native commit records,
cooperative SQL calls, cold opens, and segment-cache hits. Missing observations
are unavailable/null, not fabricated zero or process RSS measurements.

The embedded console uses no browser credential storage and exposes real
SQL, schema, refs, generation/WAL status, recovery, and management APIs.
JSON parsers preserve signed/unsigned integer literals rather than rounding
64-bit SQL values, epochs, or PITR sequences through IEEE-754 doubles.

## Explicit prototype limitations

- Single local metadata authority/platform, no consensus, replicas, or multi-gateway quota guarantee.
- Global controller operation serialization remains a pressure-test candidate, not an assumed performance success.
- Cooperative budgets are not preemption or a hard RSS limit; slow transport readers remain a bounded-pool risk.
- Native results are materialized before bounded NDJSON network forwarding.
- Audit has a 64 MiB explicit cap and no automatic archival policy.
- Private page keys remain in process memory; hostile administrators, swap, and retroactive plaintext encryption are not covered.
- Deployment templates are not acceptance evidence until actually built/run; Helm availability is reported separately.

Independent verification and measured whole-project stress results are recorded
separately; this design document is not itself a PASS claim.
