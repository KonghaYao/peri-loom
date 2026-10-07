# Linux baseline independent verification — HOLD

- Non-author read-only review: bounded build-baseline evidence accepted; deployment/latest-image acceptance HOLD, runtime NOT RUN.
- Frozen author document SHA256 `05b8aa48a71bce109f673ef440c8ec30ee45536527954a19274fcfdcaf269bdd` matches main authorization.
- Evidence `/private/tmp/peri-linux-final-author.X3Bnigq4`; all five evidence.sha256 entries independently match, including raw build/image/source/seed records.
- Snapshot HEAD `bd325e3c1e54c8237590a6ce79f8bb9ec175b45a`, Turso gitlink `ad892d4f113b2e62544bef48be6cebfd6e20da52`; not current/final-HEAD build proof.
- Context4116 independently byte-verified:4106 files and10 symlink targets, without dereferencing symlinks; manifest blockers0 and after-build changed inputs0.
- Seed534 recorded files hash-match; independently checked273 locked archive checksums and273 locked sparse entries against captured Cargo.lock; verification network calls0.
- Raw build log/exit evidence agrees: Linux arm64 build exit0,291s; Go/Rust build stages and own image export retained. Docker base/apt networking occurred; not fully offline.
- Own tag peri-loom:linux-final-bd325e3-x3bnigq4; captured image ID `sha256:b8041f1abecf497c0ca3f25aee22ebb1d7246fe24600cac847e2de49077269bc`.
- Captured image inspection says linux/arm64, configured UID10001:10001, Local/TLS/no-build defaults, writer admission OFF; no fresh daemon inspection performed.
- Author flags container_started=false/runtime_checks_executed=false; runtime script was not run. No runtime UID/TLS/SPKI/whole11/Stop/joined exit/OOM/clean-marker proof or live PID-absence proof.
- Source comparison records Root775bc62 after build and changed Go CLI main.go; current Root file independently differs from context. This is code drift, not merely docs-only HEAD change.
- Bundled legacy CLI unknown/failure auto-Close blocker and current Native FAIL remain deployment blockers; neither is washed by build exit0.
- Final Rust raw/count hashes match: measured775bc62,392 unique PASS+1 FAIL+5 ignored; fmt/clippy exit0, release test101, Native/SDK example builds NOT RUN. Historical390 passes are not current actual.
- P4 next-grant authority rejection remains opaque/unrootcaused; five opt-in S3 tests ignored; no full Turso workspace, universal cold-fault coverage or NativeRollback proof.
- DOC gate five items agree with16F bounded checkpoint/metrics acceptance, static Go/TS/Rust unsafe-close blockers, SDK0API at that blocked stage, and Linux runtime NOT RUN; not actual SDK UNKNOWN execution.
- R14 reported maps/HTTP200 are bounded observations, not complete instrumentation; performance NOT PASS. Remote Turso source publication remains unverified/unpublished as previously qualified.
- No Docker start/API/SQL/build/signals, secret configuration reads, old Unknown actions, source edits or commit; only this independent report added.
- Decision: baseline evidence PASS within captured scope; final joint/deployment gate HOLD pending separately authorized repaired context/runtime and independent checks.
