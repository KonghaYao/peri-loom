# Legacy audit checkpoint scheduling

The legacy audit keeps an append-only, synced record chain and independent durable
head/floor witnesses. The existing bound allows at most 32 committed records ahead
of the head. Previously the first batch that would exceed that bound advanced the
head to the preceding committed position before appending anything itself. A lone
request at that boundary paid two atomic replacements, each with file and directory
sync, before its own append barrier.

After a successful batch reaches exactly that existing bound, the single audit
drain now notifies its callers of their already-synced append and immediately runs
the same head/floor checkpoint before selecting another batch. The next batch still
waits behind the same writer. This moves work into a possible client/network/SQL
gap; it does not remove, combine or defer durability barriers.

The acknowledged batch needed only its append sync under the original protocol:
record 32 previously returned with head 0. It still does so. Record 33 cannot append
or acknowledge until the head checkpoint succeeds. The original pre-batch guard is
retained for reopened logs, non-full batches and direct recordBatch callers. There
is no timer, extra writer, larger accepted witness gap or new file format. A final
Close checkpoint does not duplicate a head already brought current.

The existing writer WaitGroup includes maintenance. Close stops admission before
joining it and reports checkpoint failures. A failed head/floor persistence poisons
the existing durable head, refusing every later append; it cannot retroactively
change the already-synced batch's result. The completion-journal v2 path is excluded.
Head and floor stay independent to preserve their rollback evidence.

AuditStatus adds atomic maintenance observations: `head_checkpoint_attempts_total`,
`head_checkpoint_ns_total`, `head_checkpoint_failures_total`, and
`head_checkpoint_in_progress`. Attempts include the pre-batch guard and Close;
failed availability checks are attempts too. `fsync_count` remains the count of
successful audit-file append syncs, not head/floor barriers.

Directed validation covers the frozen maintenance boundary, record 33 waiting
without appending, Close joining while refusing new records, real destination
rename failures before head replacement and after durable head/before floor
replacement, reopen of acknowledged prefixes, and completion-v2 exclusion.
Existing chain, crash-prefix, rotation, admission and completion tests remain
required. Performance acceptance depends on complete paired measurements; this
scheduling change alone is not evidence of a stable p99 reduction.
