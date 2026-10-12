# turso_serverless 0.1.3 read-only compatibility probe

Run `cargo run --offline --manifest-path prototypes/v1/platform/tests/hrana-sdk/turso-rust-readonly/Cargo.toml -- /path/to/private/connection.json` from the repository root. Supply the existing private local fixture; do not copy credentials into this directory.

For an isolated, disposable gateway bound only to `127.0.0.1` without TLS, append `--loopback-http`. This tests the SDK protocol separately from certificate trust; do not expose an HTTP gateway on a network interface.

To verify writes and transactional batches against an isolated fixture under `/tmp/peri-v3-json.*` only, append `--loopback-http --disposable-write`. This creates and resets `__peri_v3_compat_smoke` in that disposable database. Never run this mode against a shared database.

For HTTPS the probe checks reqwest's default trust and validates the fixture CA with a separate reqwest client; HTTP mode is only for a loopback-only isolated fixture. It then uses the **unmodified** SDK for `SELECT 1` via `/v3/cursor` and `/v3/pipeline`, in that order. It never disables certificate or hostname validation and never prints tokens, SQL response bodies, or raw SDK errors. On failure the second SDK request is not run.

`turso_serverless` creates its own `reqwest::Client::new()` inside `Session::new`; adding the CA to a separate reqwest client does **not** configure the SDK. For application-scoped trust, upstream SDK support for an injected `reqwest::Client` (built using `reqwest::Certificate::from_pem` and `Client::builder().add_root_certificate(...)`) or a maintained fork providing that seam is required. On macOS the default reqwest 0.13 backend uses platform certificate verification; importing the CA into the OS trust store would affect other clients and is deliberately not performed here. Do not use `danger_accept_invalid_certs` or disable TLS verification.
