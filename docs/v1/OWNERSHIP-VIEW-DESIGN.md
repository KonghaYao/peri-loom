# Fresh ownership proof versus full catalog health

## Contract

`Service::check_binding` validates a live lease, reads a fresh physical authority
root through `Authority::ownership_view`, matches durable ownership identity,
and validates the live lease again. This is an ownership proof, not a claim that
every retained historical object is healthy. Each call checks the existing Read
guard and performs a new root GET. There is no cache, fallback, or reused root
proof across calls.

The private view uses the existing strict root envelope decoder and checksum.
It requires schema version 2, a positive revision, a valid matching namespace,
and a valid matching root incarnation. It reuses `migration::validate` for the
entire inline ownership catalog, including labels, positive term, active owner
branch incarnations, and command receipt identifiers and digests. It does not
introduce a projection decoder. Parsing still consumes the full serialized root.

The returned private type contains only namespace/root identity, authority term,
owner records, and branch incarnations. It does not expose a catalog snapshot,
object entries, pins, uploads, history, or child metadata. Matching preserves the
previous `live_binding` rules for active state, role, epoch, worker, lease, grant,
term, and branch incarnation. The unchanged in-memory matcher permits an older
renewal sequence for the same lease but rejects a future sequence; renewal
commands still require the current sequence. Durable matching historically did
not compare renewal sequence, and this seam does not change that policy.

## Unchanged safety boundaries

Public snapshot, open, transactions, candidate validation, final publication
guards, pin updates, epoch floors, branch validation, fenced physical operations,
blob hash checks, and encryption authentication remain unchanged. Snapshot,
clone, garbage collection, and actual reads retain their full dependency checks.
The view does not replace the full root used for publication or grant decisions.
Monotonic deadline, uncertainty, and in-memory identity checks remain before and
after the fresh read. A failed/missing/corrupt root cannot authorize through a
previous successful read. Unsupported uncertain backends remain inadmissible.

A historical HEAD can be corrupted without changing the authority root. A pure
lease proof may consequently succeed while public snapshot/open, clone, GC,
and reading that HEAD fail. This distinction is intentional: root checksum and
root ETag never certify child integrity. No child-proof cache is introduced.

## Verification and limits

`tests/ownership_view.rs` uses counting/fault-injected in-memory storage and a
controlled monotonic clock. It checks one fresh root GET per pure proof, no
historical child GET, historical tamper detection at the full-tree boundaries,
zero GC deletion on corrupt history, inline catalog/identity rejection, malformed
envelopes, missing roots, backend failures, post-read expiry/regression, renewal
identity, and sticky uncertainty.

This candidate has no measured performance claim. Decoding/checksumming remain
proportional to serialized root size and inline catalog validation still scales
with that catalog. Acceptance of a performance improvement requires later
controlled before/after measurements using the same Go/SDK/harness and the
preserved old native binary. Focused regression tests are not a substitute for
the unified safety gate or that experiment.

The candidate's focused `cargo test --test ownership_view -- --test-threads=1`
run passed all seven tests. Scoped `rustfmt --check` passed for the four Rust
files in this change. No full Rust gate, release build, fixture RPC, load test,
or before/after performance measurement was run by this implementation task.

## Initial unified gate failure and stronger stage triggers

Fermat's first unified gate failed; it was not accepted as a safety or performance
PASS. Preserve `/tmp/peri-correlation-f-gates/rust-test.log` and
`/tmp/peri-correlation-f-gates/rust-clippy.log` as the original failure evidence.
Strict clippy rejected unused definitions from the shared test support module
and a complex closure-vector type in the new test. The scoped repair applies the
existing shared-fixture `dead_code` module convention and a `RootMutation` alias;
it does not suppress lints globally or alter production behavior.

The legacy expiry test used a global manifest-read `skip: 3`. Before this seam,
the source order included the pure binding proof's full-tree metadata read,
then the branch snapshot, reservation-current validation, and reservation-candidate
validation. Removing that first child read moved the fourth metadata read to
the post-upload publication-current validation. The old total-mutation assertion
therefore fired after an authorized intent CAS and physical blob PUT, not before
them. This old-order explanation is source-derived, not an old-binary measurement.

The instrumented new-order reproduction retained the original failing assertion:
reads 1/2/3 observed root revision 8, mutations 10, root PUTs 8; read 4 observed
revision 9, mutations 12, root PUTs 9. Its RED exit 101 is retained in
`/tmp/peri-ownership-view-legacy-trigger-red.log`. The original failed assertion
held the mutation mutex while panicking, poisoning it and causing a second panic
during pin-release Drop; the repaired assertions clone their evidence and release
the locks before asserting, rather than hiding either failure.

Only `final_root_guard_rechecks_expiry_after_slow_candidate_semantic_read` in
`tests/p4_authority_helpers/stages.rs` was changed. It now covers two stages:

1. Expire during the first pre-upload full semantic read. Assert zero mutations,
   unchanged complete root, serialized root bytes, ETag, objects, and branches.
2. Arm after the reserved upload reaches physical PUT, then at the next publication
   root GET. Within that transaction, skip its current-root metadata read and
   inject expiry during the candidate semantic read. Assert exactly one authorized
   reservation root CAS plus one blob PUT, but no final logical publication.
   Compare the entire root, bytes, and ETag against the captured reservation state;
   separately assert unchanged original objects/branches and an unresolved intent
   protecting the physically present orphan through `reachable`.

The second case does not claim the whole operation made zero root writes: its
reservation advances the revision once. It proves the final candidate cannot
publish after expiry. Production guard placement and deadline checks are unchanged.
The staged trigger is local to this transaction, not a global child-GET ordinal.

After repair, the focused guard passed ten consecutive runs; the stages subset
passed 11/11, OwnershipView passed 7/7, scoped strict clippy passed for both test
targets, and scoped formatting passed for all five Rust files. Evidence is in
`/tmp/peri-ownership-view-stage-repeat.log`,
`/tmp/peri-ownership-view-stages-regression.log`,
`/tmp/peri-ownership-view-focused.log`, and
`/tmp/peri-ownership-view-scoped-clippy.log`. These focused results do not replace
Fermat's unified rerun and do not establish any performance improvement.
