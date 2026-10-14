# P1/P2 remote WAL protocol

`wal/chunks/<blake3>` contains immutable serialized `WalRecord` objects. The
conditionally replaced `wal/index` is the only commit authority. Uploaded chunks
that are absent from the index are orphans, never replay inputs. Append returns an
LSN only after a successful index PUT or after GET confirms its unique append ID
in the durable index receipts. Receipts survive retention changes so an ambiguous
CAS acknowledgement cannot turn into a second application during an internal
retry. Separate calls with identical operations are distinct appends.

LSNs start at 1 and increase within a namespace. Epoch 0 is the P1 default. The
optional `fence` method raises index epoch monotonically; it is not a distributed
ownership/lease implementation and must be integrated with P4 authority later.
The database adapter fails closed after a WAL durability error: close it and
recover from the remote index before accepting more writes.

## Native commit boundaries (P2)

`WalRecord` and `WalMetadata` have a backward-compatible, serde-defaulted
`commit: Option<WalCommit>`, where `WalCommit` contains `lsn` and `position`.
`RemoteWal::append_commit(namespace, epoch, batch, position)` assigns the marker's
LSN and publishes it in the same conditional index update as the chunk reference
and append receipt. Existing `DurableWal::append` delegates with no marker. A
marker-free append clears the current index marker; it is not a sticky indication
that an earlier transaction committed. Historical record markers remain intact.

Native WAL sync examines the RAM WAL's header and complete final frame, checking
format version, magic, full page size, salts, frame page numbers and rolling
checksums in the byte order selected by the header. The final frame must have a
nonzero database-size field. Partial, invalid or uncommitted tails never fall back
to an earlier commit marker. Pending operations must cover the complete final
frame, and an unchanged acknowledged position is not emitted as a new commit.
This detection does not inspect SQL text or depend on `execute` returning.

A native full-header reset is guarded by the same checkpoint/durability rules as
truncation. Its batch includes an explicit zero-length truncation before the new
header write. RAM file length therefore tracks the new logical WAL, excluding
stale frames from the previous generation, without deleting old immutable chunks.
WAL headers store the full page size, including 65536; the database header's
16-bit page-size encoding is not used here.

`recovery::validate_index_commit(index_bytes, namespace, load_chunk)` receives a
callback returning chunk bytes for a full namespaced `wal/chunks/<hash>` key.
It validates index continuity, replay baseline, append receipts, namespace, epoch,
LSN and BLAKE3 identities, then replays each record. Every record marker must match
its validated replay image and frame-write proof. The final record marker must
equal the index marker at `last_lsn`. The callback must use the candidate root's
immutable metadata view, not a changing live branch view; validation never reads
database segments.

The authority adapter owns recovery-point time and commit sequence. Its durable
root CAS must atomically publish a new index, the marker's compatible HEAD and the
history recovery point. Record wall-clock timestamps are not PITR authority time.
An index marker alone does not implement ROOT publication or retention policy.

## Retention and replay

Append preserves all historical chunks and index entries until explicit
`truncate(namespace, before_lsn)`. This enables later PITR work; P1 does not expose
an arbitrary historical SQL snapshot API.

Truncation replaces the indexed prefix with a materialized WAL-byte baseline at
the last removed LSN. This baseline includes the complete WAL header, size and
all overwrites/resets in that prefix. Remaining records retain their original
LSNs. `read(namespace, 0)` always includes the baseline and is the supported full
reconstruction operation. Requests for an older nonzero LSN fail rather than
silently returning incomplete historical replay. `WalMetadata.replay_baseline_lsn`
exposes the boundary. Truncation does not physically delete chunks: object GC and
PITR retention policy remain separate future operations.

The native adapter reconstructs the complete RAM WAL from these records, not just
LSNs after the database manifest watermark. An applied checkpoint LSN alone is
insufficient to omit prior byte writes because later records may depend on their
header and frame prefix. Turso validates salts, frame checksums and commit markers
while recovering this file; torn or uncommitted tails are not committed SQL data.

## Native IO publication

Database pages are staged in `RemoteStorage`. WAL writes are RAM-only, accumulated
as ordered operations, and acknowledged durably at native WAL sync. Database sync
publishes the native `checkpoint_wal_position` hook and acknowledged remote WAL
LSN through storage HEAD CAS. Reset/truncate is denied for pending WAL writes,
unpublished pages/positions, failed durability/publication, or a durable tail
whose LSN has not been checkpointed. Successful WAL truncation is itself appended
durably. No database or WAL host files are created; PlatformIO handles temporary
files only. A cache-directory OS lock remains held by retained native file handles.

SQL batches use Turso's native statement runner, resetting FULL sync and disabling
automatic WAL actions before preparing each statement. A `PRAGMA synchronous=OFF`
cannot disable durability for a later statement in the same batch. Query and
explicit checkpoint entry points apply the same reset. Host IO rejects database
attachments and accepts only native `tursodb_temp_file` spill files in OS temporary
directories; attached persistent databases are not supported by this adapter.

Virtual file IDs are stable for canonical cache path, namespace and file role.
Including the canonical path prevents distinct object-store backends with equal
namespace names from aliasing in Turso's process-global WAL registry. Remote
ownership across independent cache directories is outside P1's local lock scope.
