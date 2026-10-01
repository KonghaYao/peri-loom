# Simple / distributed isolated validation (2026-10-01)

Validation used a point-in-time copy of the repository in `/tmp/peri-loom-validation.k9F19U`, excluding `.git`, `target`, `web/node_modules`, `.run`, and secrets. It did not start or alter the existing `db-platform` Compose project. The first copied source predates later host, metadata schema, and Hrana streaming edits; the final Simple rebuild is recorded below.

| Check | Result | Evidence / scope |
| --- | --- | --- |
| `docker build -f Dockerfile.simple -t peri-loom-simple:validation .` | PASS | Built Node Web and Linux release `peri-loom`; runtime image contains one service binary. Default Rust base was corrected from unavailable `rust:1.98.1-slim-bookworm` to cached `rust:1.98-slim-bookworm`, whose `rustc --version` is 1.98.1. |
| Isolated `docker run` smoke | PASS | `--help` lists serve/export/import; `/healthz` and `/readyz` return 200, `/api/v1/deployment` returns simple/local_fsync, `/` and compiled JS return 200, unknown API path returns JSON 404. The container was removed afterward. |
| `docker build -f Dockerfile --target builder` | PASS | Compiled Linux release `db-server`, `db-worker`, `db-runtime`, `wal-service` from the copied source. |
| Linux `cargo check --locked --bins` | PASS | All workspace binary targets compile in Linux; includes Worker pidfd code that macOS cannot compile. |
| Linux WAL regression | PASS | Engine adapter 61 unit tests, WAL-only recovery integration test, local sync/power-loss model integration tests, wal-client 55 unit tests, wal-service 80 unit tests (1 ignored). Executed inside an isolated Docker build stage from the copied source. |
| Fresh PostgreSQL Catalog regression | PASS | `cargo test --locked -p catalog --lib -- --ignored --test-threads=1`: 32 passed. PostgreSQL 16 ran in a temporary container with a fresh database; it was removed afterward. |

These container checks do not claim a physical power-cut test or external multi-node failure test. Real-binary API/Hrana end-to-end coverage is recorded separately in [simple-deployment-progress.md](simple-deployment-progress.md).

## Final Simple source rebuild

After the SQLite backup-history helper, Simple job changes, schema/host fixes, and Hrana streaming changes, the source was copied again into the same isolated build context. `cargo test -p catalog --lib local_backup_idempotency_tests` passed, verifying the backup record is unique across repeated calls and a catalog reopen. `cargo check -p db-server` and `cargo clippy -p catalog -p db-server --all-targets -- -D warnings` passed. `docker build -f Dockerfile.simple -t peri-loom-simple:validation-final .` passed and rebuilt the Linux release binary plus Web assets. The final image was run in a fresh throwaway container: `/healthz` 200, `/readyz` 200, `/api/v1/deployment` 200 with `mode=simple`, `/` 200 HTML, unknown API path 404 JSON. The container was removed afterward.

The distributed Linux regression and fresh PostgreSQL suite above ran on an earlier source snapshot. Later edits were concentrated in Simple host, SQLite metadata, Web, and Hrana code; the final release image compiled those edits, while a full distributed integration rerun was not performed on the final snapshot.
