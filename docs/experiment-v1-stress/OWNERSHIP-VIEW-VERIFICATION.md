# Fresh OwnershipView safety verification

Independent verifier: Fermat. Date: 2026-10-07. Result: **safety/regression gate PASS for the approved layered contract; performance candidate UNMEASURED**.

## Approved seam and immutable boundaries

This is a fresh ownership proof, **not** a full catalog/child-integrity proof. The sole existing production behavioral change is `Service::check_binding`: unchanged live-lease checks surround a private fresh-root ownership read instead of a full public snapshot. `authority.rs` adds the private module; the new `authority/ownership_view.rs` implements it. The C SDK central-client change is disjoint.

The private view checks the existing Read guard, performs a fresh physical authority-root GET, uses the existing strict envelope/checksum decoder, requires schema 2, positive revision and exact valid namespace/root incarnation, and runs full inline `migration::validate`. It checks active ownership, authority term, epoch, worker/lease/grant identity, role and both branch incarnations against the supplied binding. It introduces no cache, projection decoder or fallback on a failed/missing root.

Unchanged live checks before and after the read preserve monotonic deadline, regression/uncertainty and same-lease fencing. Durable matching continues the pre-existing policy of not comparing renewal sequence; the unchanged live matcher permits older sequence values for the same renewed lease and rejects future ones. This is not a new grant or permission to reuse an unrelated binding.

Public snapshot/open/clone/GC/transact/pins/publication/epoch-floor checks, BranchStore validation, physical blob hash checks and AEAD remain full checks. In the approved counterexample, corrupt historical HEAD can coexist with a successful pure lease proof; public snapshot/open/clone/GC and the relevant read still fail, with zero GC deletion. A root ETag/checksum never proves child integrity.

## Exact freeze

| File | SHA256 |
| --- | --- |
| `prototypes/v1/src/authority.rs` | `160f0baea9348aee92856050a7aaeaec92044933b794170d6a2c5c49a01af8f2` |
| `prototypes/v1/src/authority/ownership.rs` | `9f7e5996e5a4ab0bf969025458e47760fedfc167941befefde32ef97aa39fe88` |
| `prototypes/v1/src/authority/ownership_view.rs` | `abaeaab9b66e94adbb457511aec757e5d74bbfc952b7650fc328f31de4d56122` |
| `prototypes/v1/tests/ownership_view.rs` | `f1808b2f1fdda65c6a742773b63fb16cae3fd1a0843a4f31e2d523cab15102c1` |
| `prototypes/v1/tests/p4_authority_helpers/stages.rs` | `5ec689b6446a2f9096c3a3a6ea45ead5e91ee85accf1e317f8821445bd9a9058` |
| `docs/v1/OWNERSHIP-VIEW-DESIGN.md` | `7883a747c654f41cdcda8cfc279745119a093de5d2e60e2dac0c39ca6b6f466e` |

The existing stages file changed only the explicitly authorized expiry test. No verifier production/test edits occurred. Comparison with `/tmp/peri-v1-joint-native/before.json` finds exactly five changed historical native paths: A's two existing production files, C's SDK client, and the two explicitly authorized old tests. The new view module/test are additions. Cargo files, other historical native files, P0's ten files and all 4,115 tracked Turso files are unchanged. Unified owned/source before-after identity proofs and binary hashes are shared with `CORRELATION-FIX-VERIFICATION.md`.

## Initial failure and stronger guard evidence

The first unified gate genuinely failed and remains in `/tmp/peri-correlation-f-gates/rust-{clippy,test,status}.log`. The legacy guard test asserted zero whole-operation mutations after a global child-read skip. Removing the pure-proof child read shifted that ordinal into publication-current validation, after an authorized upload reservation CAS and physical blob PUT. The old assertion observed 12 versus 10 mutations; its held mutex then poisoned Drop and aborted the target. This was not accepted as PASS, nor hidden by the successful release build.

The old-order explanation is source-derived, not an old-binary experiment. A retained RED reproduction, `/tmp/peri-ownership-view-legacy-trigger-red.log`, confirms the new-order fourth read observes revision 9/mutations 12/root PUTs 9, versus revision 8/mutations 10/root PUTs 8 at reads 1–3.

Independent static review of the authorized replacement confirms two explicit stages:

1. Pre-upload semantic expiry: zero mutations and identical full root/serialized bytes/ETag.
2. After the physical upload stage arms the next publication-root read, expire in that transaction's candidate semantic read. Exactly one prior reservation CAS and one blob PUT are allowed. Full root/bytes/ETag equal the captured reservation state: **zero final root publication**. Original object/branch maps remain unchanged; the sole staged orphan is present and protected by the unresolved upload intent/reachability.

This does not falsely claim zero root writes for the entire second operation; reservation advances the revision once. No production deadline or final-publication guard was weakened. Assertions copy evidence before panicking instead of holding the mutation mutex. Scoped test-support lint repair and a type alias do not suppress production/global checks.

## Unified independent gates

All commands used Rust 1.98.1, ROOT `prototypes/v1`, and `CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target`:

```text
cargo +1.98.1 fmt --all --check
cargo +1.98.1 clippy --locked --offline --all-targets --no-deps -- -D warnings
cargo +1.98.1 test --locked --offline --release --no-fail-fast
cargo +1.98.1 build --locked --offline --release --bin peri-loom-v1
```

All four exit 0. Full package: **298 unique tests PASS, 5 ignored**, no failed targets. Raw result-summary pass counts total 299 because a subprocess reruns one recovery helper; that is not an extra unique test. The seven OwnershipView tests, all 35 P4 authority tests including the repaired guard, and seven Rust SDK tests pass. Five ignored network tests are not counted as offline PASS. Logs and unique records: `/tmp/peri-correlation-f-rerun/rust-{fmt,clippy,test,release-build,status}.log` and `rust-counts.json`.

The OwnershipView tests actually cover root-only fresh GETs, historical tamper with full-boundary failure/zero deletion, invalid inline catalog/identities, malformed envelope/missing root/backend error without cached fallback, revocation during the read, post-read expiry/clock regression, renewed same-lease/future-sequence behavior, and sticky uncertainty/unsupported backends.

Go normal and race full platform runs additionally exercise all seven real native P6 integration tests using `/tmp/peri-stress-native-view-bin`, with `PERI_P6_VERIFY_RACE_CLI=1`. Both runs PASS; counts and remaining platform/helper skips are itemized in the protocol report. These are isolated test fixtures, not actual pressure runs.

## Additional real S3 gate: readonly evidence verification

Main independently executed, through a private PTY and environment-only credentials:

```text
CARGO_TARGET_DIR=/tmp/peri-loom-p6-dev/prototypes/v1/target
cargo +1.98.1 test --locked --offline --release --manifest-path prototypes/v1/Cargo.toml --test minio -- --ignored --test-threads=1
```

F did not obtain credentials, execute S3 requests or rerun this gate. Readonly inspection confirms `/private/tmp/peri-stress-s3-gate-Hd4o4IEm/exit-code` is 0 and `test.log` reports **5 PASS, 0 failed, 3 filtered**, 14.77 seconds. The directory is 0700; log and exit-code are 0600. Log SHA256: `f3eb2dc9a7503f2856d7dacdd43bada6a6a41661b10c779cd7060d9f56e7e82a`.

The raw log selects the same cached current release test executable as F's full gate, `/tmp/peri-loom-p6-dev/prototypes/v1/target/release/deps/minio-458fbff260aff428`, SHA256 `64de72e83ccd4ccc9804876a986b253f282330abd22defdbda0025b9fc431f27`. Its Cargo fingerprint JSON SHA256 is `b0247f033b87a53a3ccaf18d8a92f9fdfb6974f695b0b05779b1addaa964ebae`; release-profile completion is recorded as 0.29 seconds with no compilation lines. The command/endpoint/credential-injection procedure is executor-reported; the observed cached runner, test identities, results, file permissions and hashes were independently checked.

All five tests enforce explicit network enablement rather than succeeding through an inactive early return. Source creates PID/time/temp-salt unique buckets, verifies HEAD absence before creation, and scopes cleanup to its own bucket. Tests cover object-store conformance, fresh-cache SQL recovery, P2 authority/PITR/GC, P3 multi-database recovery and P4 remote-worker fencing/restored snapshots. Main reports endpoint `127.0.0.1:9000` and no existing-object/container modification. The aggregate log does not independently audit every bucket name, cleanup completion or HTTP attempt; the S3 test configuration has bounded store retries, so no claim of zero physical HTTP retries is made.

## Acceptance limits

The new arm64 release native SHA256 is `b3e6d22dd0a16c967c56141f235122d79b7ef7bf25a8438916314d2f7fa6128a`, now build-usable after these safety gates. The preserved old native remains `3c67f760ac260d70b6c1b42702a1d75051adbe1a9acef620a1cd7045bcd77836`. All verifier builds/tests completed before measurement handoff.

This candidate still decodes/checksums the full root and validates the inline catalog. Safety acceptance establishes neither saturation capacity nor a latency/QPS improvement. Controlled old/new native measurements with the same final Go/harness and fresh equivalent fixtures remain necessary. Earlier unknown operations remain unresolved, starvation is not promoted to PASS, and unrun point/mixed cases remain unrun. No actual pressure run or fixture provisioning was performed by this verifier.
