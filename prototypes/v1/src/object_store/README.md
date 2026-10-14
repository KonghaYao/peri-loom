# ObjectStore contract

The synchronous `ObjectStore: Send + Sync` interface is shared by memory, local
filesystem and path-style S3-compatible backends. Keys are nonempty ASCII-safe
relative paths, at most 1024 bytes. Empty path components, dot components,
backslashes, percent encodings and query characters are rejected. Listing uses
literal prefix matching; namespace callers should pass `tenant/database/`.
No backend silently emulates remote conditional operations with GET-then-PUT.

`PutCondition::Absent` and `Match` are atomic preconditions. Failed preconditions
are downcastable `PreconditionFailed` errors. ETags are opaque: keep the exact
value returned by the store, including S3 quotes. Deletes are idempotent when
the backend permits deleting an absent object, and check an expected ETag on
existing objects. A transport or durability error can mean a mutation completed
but its acknowledgment was lost. Conditional retries preserve the original
precondition and never report a rejected retry as a successful mutation.

## Local backend

The local root contains hashed-key JSON envelopes rather than user-selected
directory paths. Every operation takes the same OS file lock, so separate store
instances and cooperating processes serialize CAS and deletes. Writes persist
a temporary envelope, atomically rename it, and fsync the directory. Deletes
fsync the directory. Read/list verify the embedded key and BLAKE3 payload digest;
list additionally checks the hashed filename. Interrupted temporary writes are
not listed. Memory/local ETags change even for identical replacement bytes.

The root and its parents must be trusted, and all writers must use this backend.
Symlink object/lock files and symlink roots are rejected, but a malicious local
process able to replace root directory entries is outside the locking contract.
Use a local filesystem with working OS locks, atomic rename and directory fsync,
not a filesystem that ignores these guarantees. One global lock trades
throughput for a simple, process-safe CAS boundary.

## S3 backend

Configure a bucket-specific path-style origin, e.g. a MinIO origin, AWS regional
S3 origin, or an R2 account origin, without endpoint credentials/query/base path.
SigV4 signs the payload digest, session token, conditional headers and SHA-256
metadata. Queries are individually percent encoded and sorted, including opaque
continuation tokens. URL-safe keys are not normalized or re-encoded.

Writes attach `x-amz-meta-peri-sha256` for interoperable payload verification on
GET. Full-object `x-amz-checksum-sha256` responses are also checked; composite
multipart digests are not incorrectly treated as full-object SHA-256. External
objects without these checksums are readable but lack application-level digest
verification. Error bodies are bounded and only allowlisted error codes reach
errors. Credentials/tokens and arbitrary server messages never reach logs.
Connections are direct, without implicit environment proxy configuration.

HTTP 429/5xx and network/body failures retry with capped exponential backoff;
`retries` counts additional attempts and is limited to 16. `timeout` applies to
each complete attempt, including its response body, not the total retry window.
Authentication, redirect and precondition errors do not retry. GET distinguishes
`NoSuchKey` from `NoSuchBucket`; listing validates prefixes and rejects token
cycles. No automatic redirects can move signed credentials to another origin.

The remote service must honor conditional PUT and DELETE. Mock conformance is
not proof of a provider's capabilities. AWS/MinIO/R2 real-service qualification
is separate. S3 ETags may repeat for equal contents: future fencing must put an
epoch/generation in the CAS object's contents, not assume ETag uniqueness or
advance to Go/P4 ownership behavior here.

## Validation

`cargo test --test object_store` covers memory/local contracts, process CAS,
corruption/symlink rejection and signed TCP HTTP contracts, including checksum,
pagination, retries, timeouts and safe errors. Library tests also include the
AWS published SigV4 GET example.

Real-service tests are ignored by default and additionally require explicit
opt-in:

```sh
PERI_MINIO_TEST=1 cargo test --test minio -- --ignored --nocapture
```

Use an isolated disposable MinIO service bound only to `127.0.0.1:19000`, with
test access key `peri-test-access`, test secret
`peri-test-secret-isolated-only`, and no user credentials. Tests create unique
test buckets and clean only those buckets. Coverage includes a 16-way network
CAS, conditional deletes, 1005-key pagination, injected checksum corruption,
and SQL/WAL recovery after deleting every local cache directory twice.

The attempted isolated Compose image was
`minio/minio:RELEASE.2025-09-07T16-13-09Z` (release confirmed in the official
MinIO GitHub repository). Registry manifest/pull requests failed with connection
reset/EOF; Quay requests did not find that manifest. Its image digest could not
be verified and no container was started. Real MinIO acceptance is therefore
**not passed** in this environment; neither are live AWS or R2 tests.
Explicitly running the two ignored tests with opt-in failed with connection
refused because the isolated service could not be started. The temporary Compose
configuration was removed and no test containers or networks remain.

### Native download fallback evidence

An additional attempt uses the official fixed release for the host's
`Darwin arm64` platform, with a fresh disposable temp directory. Requests run
under `env -i` with a fixed system PATH and `curl --noproxy '*'`; no user
credentials or environment proxies are read. Both GET requests failed:

```text
https://dl.min.io/server/minio/release/darwin-arm64/archive/minio.RELEASE.2025-09-07T16-13-09Z
HTTP/2 410
Date: Tue, 06 Oct 2026 16:32:54 GMT
x-request-id: 3e4255ae-7bc2-4107-bb55-e9925271d2e1
curl exit: 56

https://dl.min.io/server/minio/release/darwin-arm64/archive/minio.RELEASE.2025-09-07T16-13-09Z.sha256sum
HTTP/2 410
Date: Tue, 06 Oct 2026 16:32:55 GMT
x-request-id: 16a4262d-579a-4d1e-9d82-2c5bea949ec6
curl exit: 56
```

The archive index independently responds with a notice that these community
release files are no longer served. This is an unavailable official download,
not proof that a particular platform artifact never existed. Neither a binary
nor its official digest was available, so **no SHA verification, execution or
native MinIO acceptance was possible**. The empty temp directory was removed.
A pinned-source Go build was not attempted; it is an optional, more expensive
fallback. A real remote HTTP durable fixture is a separate next step, not an
equivalent claim that local filesystem acceptance proves remote acceptance.

Conditional PUT retries keep the original `If-Match` value. The explicit lost
ACK test drops the first response, returns 412 on retry, and verifies the
downcastable conflict reaches the caller; only an explicit GET can inspect the
committed receipt. RemoteWal's separate receipt/readback verification also passes
(`lost_chunk_and_index_acks_resolve_by_remote_readback_without_duplicate_lsn`).
That receipt test uses an in-memory fault-injection store; it verifies receipt
logic, not live S3 durability or remote SQL acceptance.

Primary references consulted: AWS S3 DeleteObject and conditional-deletes API
documentation, Cloudflare R2 S3 API compatibility documentation, and the official
MinIO GitHub release metadata. The former AWS SigV4 documentation URL currently
redirected to its API landing page; the signing implementation is additionally
checked against the known published example's exact signature.
