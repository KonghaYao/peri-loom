# Unified completion journal v2

Issue #11 implementation, 2026-10-26. This is an opt-in durable format; it is
implemented in the service request path. The earlier
[`completion-envelope-design.md`](completion-envelope-design.md) records the
historical v1 proposal and its measurements, not the current v2 format.

## Enablement and compatibility

For a **new** launcher-owned run directory:

```sh
PERI_COMPLETION_JOURNAL=1 python3 prototypes/v1/platform/dev.py --run-dir /tmp/peri-completion-v2 --tls
```

The launcher records the format in its immutable launch profile and carries it
into recovery runs. An existing v2 state always opens as v2 even if the service
environment flag is absent. Setting the flag to zero does not downgrade a v2
volume. Previous binaries reject the new journal sidecars/state version.

The service opener can upgrade a fresh v1 store or a clean, checkpointed v1 store
with no unresolved operations. It first joins the legacy audit's actual confirmed
tail, writes `operations/completion.genesis`, and commits its seal plus
`operation_storage_version: 2` into the registry. The genesis binds the registry
and operation positions and the complete legacy audit tail. It records the old
audit pathname for diagnosis, but the prefix's sequence/hash establish identity,
so a consistent backup may be restored at another path.

The launcher does **not** silently change an existing run's format profile. This
change does not include an automatic migration command for existing launcher
directories. An interrupted service migration that has a genesis but no matching
registry identity fails closed; restore a consistent pre-migration copy under the
existing ownership procedure. Do not delete genesis, markers, proof files, or
individual journal files to force startup. An in-place downgrade is unsupported.

## Durable request path

Committing SQL still waits for its intent before dispatch. Its result and audit
event then share one journal envelope and one `Sync`; the authoritative operation
and isolation views reach the confirmed prefix before the response and permit
release. This reduces the isolated committing request's platform barriers from
three to two (33.3%). Worker/Authority durability remains a separate cost and no
end-to-end latency percentage follows from this count.

All v2 audit events use the same journal ordering, including known reads,
rejections, management events, and write completions. A known non-committing read
has only its audit envelope. A non-committing RPC with an Unknown outcome places
its quarantine record and audit in one reserved completion envelope. A failed
completion barrier returns Unknown, retains the relevant permit, and leaves the
store unavailable; it never causes SQL replay. A complete envelope surviving a
lost reply or failed barrier can be recovered as a completed operation with its
audit, while an intent without a surviving completion remains unresolved.

The existing coalescer still combines only already-waiting requests, up to 32
records / 1 MiB per batch; it adds no timer to a lone request. Completion audit
hashes are assigned under the journal ordering lock, not by asynchronous view
workers. There is no second authoritative terminal/audit write.

## Audit layout, bounds, and maintenance

`audit.jsonl` and its old segments form the immutable pre-v2 audit prefix. **New
v2 audit events are in `operations/journal` and `operations/completion.START.END`,
not appended to that legacy JSONL file.** Consumers and backups must include the
v2 journal and archives. This version provides no legacy JSONL export worker;
the immutable archives are the retained audit source.

The journal snapshot carries the current audit head together with the bounded
operation view. Compaction first durably writes an immutable archive of the full
prefix, then the snapshot, then truncates the active log. The operation view's
256 known-terminal retention does not remove audit from these archives. Recovery
validates every retained archive and its boundary, both hash chains, the snapshot
audit head, the active log, and the legacy prefix before serving traffic. A crash
between archive/snapshot/truncate is completed only after matching duplicate
active bytes to retained archive hashes. Complete corrupt records are refused
without erasing evidence; only an incomplete suffix beyond the proved head can
be removed.

The active admission threshold is 8 MiB. At most 64 accepted completions have an
additional one-record budget each (`1 MiB + 256` bytes including framing), also
reserved in queue and retained-byte checks. The retained-history soft ceiling is
1 GiB, with the same 64-completion emergency reserve. At the soft ceiling, new
SQL completion reservations and ordinary appends are explicitly refused; an
already-reserved completion can still settle. No audit segment is silently
deleted at a watermark. An operator must monitor capacity; a supported archival
retention/export-and-prune procedure is a follow-up before this format can serve
an indefinitely growing production history. Legacy audit-prefix bytes are
additional to the v2 retained-byte metric.

Maintenance is scheduled outside the request's synchronous call, with one
bounded task per store. It still holds the state lock while materializing the
snapshot and syncing files, so other state readers can wait behind it. This
implementation does not claim to eliminate maintenance p99 stalls. Recovery
memory holds one bounded archive and the bounded active duplicate proofs;
recovery work remains proportional to all retained audit bytes.

The existing 4096 unresolved-operation count checks are unchanged. The v2 byte
reserve is **not** a new reservation protocol for those operation slots. Parallel
non-committing requests near that count can still race other registrations; a
reserved audit slot alone does not prove an operation slot. Existing open-session
cleanup evidence and fail-closed restart rules remain in effect.

Shutdown drains the gateway and audit queue, materializes the operation
checkpoint, and closes state under the shared shutdown budget. The v2 checkpoint
also syncs the last journal witness. A blocked maintenance task cannot produce a
successful clean-stop result by timing out; the shutdown reports incomplete.

## Evidence and observability

`AuditStatus` identifies `durability_source: completion_journal_v2`. Its legacy
`fsync_*` counters are explicitly scoped to `immutable_legacy_audit_prefix`.
`completion_journal` supplies confirmed journal/audit positions, archived revision,
active/queued/retained bytes, soft/hard retention limits, and shared journal fsync
count/time. Shared counters include intent and non-audit operation records too;
they are not audit-only latency measurements.

The integrated `TestUnifiedCompletion*` tests cover the actual service/gateway
path, exact two-barrier committing behavior, concurrent mixed audit ordering,
600-event compaction/reopen, terminal retention independent of audit retention,
write/Sync failure, lost response, reserved Unknown under byte pressure, both
interrupted compaction windows, archive deletion, complete corrupt tails,
partial tails, maximum envelope framing, clean migration, and bounded shutdown.

```sh
cd prototypes/v1/platform
go test -buildvcs=false -race -run '^TestUnifiedCompletion' -count=1 .
```

These are correctness and failure-injection results. They are not evidence of
power-loss behavior on every filesystem, sustained capacity, SDK compatibility,
or a measured end-to-end performance gate; those need the full-stack paired runs.
