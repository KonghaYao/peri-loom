# Latest Linux deployment verification — HOLD

Status: **build baseline only; not latest-image acceptance or runtime PASS**. Main withdrew final-delivery eligibility during this build because the bundled legacy Go CLI has an unknown-outcome auto-close blocker and the latest Native gate remains HOLD.

## Actual author evidence
- Build snapshot HEAD: `bd325e3c1e54c8237590a6ce79f8bb9ec175b45a`; clean pinned Turso gitlink: `ad892d4f113b2e62544bef48be6cebfd6e20da52`.
- Dockerfile: `prototypes/v1/deploy/Dockerfile`; filtered tracked-only context contains 4116 inputs, zero blockers; excludes CLAUDE, git/env, keys, targets and node_modules.
- Context/source manifest: `/private/tmp/peri-final-linux-prep.ddZAp11C/source-peri-linux-final-author.x3bnigq4.json`, SHA256 `ead3744043615eb5900cd925acba940954e1b345a890d274663392b01a78785d`.
- Existing private seed was reverified against current Cargo.lock: 273 archive checksums and 273 sparse locked entries; verification network calls zero. Lock SHA256 `e070b8dc077f27f0d2d29d20de11e222fb0a6ee1d6531f622550d3f3a9de34dd`.
- Seed manifest SHA256: `b4c9fedcf4fe5ce27a1f8cc9b50d74709a57fa5a8e44c067b85a9fe6231d4124`.
- Actual build used `--platform linux/arm64`, named `local-cargo` seed context and `CARGO_OFFLINE=true`; normal registry/target cache mount IDs were retained. Docker base downloads and apt access occurred; this was not a fully network-offline Docker build.
- Build exit **0**, elapsed **291 seconds**; warnings remain in the raw log. No failed build was retried or hidden.
- Own tag: `peri-loom:linux-final-bd325e3-x3bnigq4`; despite its tag spelling, this image is only the superseded baseline.
- Actual image ID: `sha256:b8041f1abecf497c0ca3f25aee22ebb1d7246fe24600cac847e2de49077269bc`; inspected OS/architecture `linux/arm64`, configured user `10001:10001`.
- Image defaults are Local/TLS/no-build, **writer admission OFF**. The unexecuted own runtime script explicitly adds `--writer-admission`; configured UID is not proof of runtime UID.
- After-build byte inventory confirmed all 4116 context inputs unchanged. Root HEAD observed afterward: `775bc629a3e8cbc0148243bebe7cddb954c8a28b`; `prototypes/v1/platform/cmd/peri/main.go` differs from the snapshot. This cannot be treated as only a documentation change.
- Evidence directory: `/private/tmp/peri-linux-final-author.X3Bnigq4`; `evidence.sha256` records build, image identity and source/seed hashes.
- Raw `build.log` SHA256: `a89990605f6a897310c4b9c134f95c1b373a757b609978f0791bfc47d3f4153d`; source comparison SHA256: `97cadf624426eb72ec6dba42b10165cca1c359286de4d1a9dee7e9a3abce6696`.

## Not executed / required next
- No container or volume was created; no API, SQL, SDK, browser, TLS runtime smoke, whole11 or shutdown signal was executed. Runtime binary SHA extraction, actual UID and joined exit remain unverified.
- Wait for the CLI independent codegate and Native/P4 resolution, then obtain authorization for one new frozen context/tag; reuse cache only where actual inputs permit. Do not ship this baseline as latest.
- Required own-only runtime: fresh volume/loopback port, UID10001, explicit Local backend and writer profile, bounded readiness, proper-CA TLS and actual whole11.
- The current whole-smoke helper hardcodes `external_s3_fixture` in result metadata; any future Local run must disclose that inherited label rather than claim Linux S3 coverage.
- After known sessions close, require SIGTERM grace45, actual `docker wait` exit0, OOM false, joined service logs and exact lowercase `authority/authority.lock` bytes `clean\n`; stop-command success alone is insufficient.
- No push, prune, user-resource deletion, S3 credential retrieval or production-source edit occurred. Image/context/logs remain for later independent read-only verification; this document is author evidence, not independent acceptance.
