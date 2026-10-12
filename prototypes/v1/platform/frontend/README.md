# Platform console

This React + Vite + TypeScript project builds the browser console embedded by
the Go platform. Install dependencies and build **before** compiling Go:

```sh
cd prototypes/v1/platform/frontend
npm ci
npm run typecheck
npm test
npm run build
cd ..
go build ./cmd/peri-platform ./cmd/peri
```

`npm run build` emits `index.html`, `console.js`, and `console.css` into `../web`.
The output directory is not emptied, preserving the existing `rpc.js`,
`json.js`, and `refs.js` for the UI and protocol tests. Building replaces the
served `web/index.html`; it does not rewrite the legacy protocol modules.
`//go:embed web/*` captures the files **when Go compiles**: after changing the
frontend, rebuild the Go executable (or restart `go run`) to update its UI.
Production serves the built UI from Go, without a CDN or Node server.

The Databases page lets a global administrator issue a project SQL API key
(shown once, SQL role permits writes) and activate it without saving it in
browser storage. Only after project-key access is confirmed does the page load
actual table names from `/v1/databases/schema` and preview at most 25 unordered
rows per selected table using an explicitly opened and closed SQL session.
The global management key can inspect the catalog but cannot open SQL sessions.
The database connection panel reads `GET /v1/platform/capabilities` from the
current gateway. It appears above the table browser on the Databases page;
users can copy the SDK URI, WebSocket URI when available, or the native V1 API
base URL directly. With `PERI_HRANA_EXPERIMENTAL=1` on a newly started platform,
it displays and lets users copy the actual gateway URI
`https://<host>/db/<tenant>/<database>/<branch>/` for the official JS libSQL
client's HTTP v2 transport, plus a key-free SDK example. Users issue a separate
project SQL API key in the same database page; the master key cannot execute SQL
and neither the URI nor the example includes credentials. When the adapter is
off or discovery fails, the console does not present a usable libSQL URI. This
is still an experimental adapter: when `hrana_ws` is advertised, the console
also provides a copyable `wss://<host>/db/<tenant>/<database>/<branch>/` URI.
WebSocket v1/v2 JSON and v3 protobuf work with the pinned low-level JS SDK;
v3 protobuf cursors on HTTP and WebSocket fetch native pages, including
read-only and write-RETURNING statements beyond 10,000 rows and mixed
multi-step batches with successful, skipped, and failed conditions. An
independent HTTPS run verified that closing an unfinished native write cursor
rolls back its uncommitted rows. Continuous full-suite compatibility and
crash-restart recovery remain unverified. This is **not** complete Hrana or a
production-ready libSQL endpoint. Do not use the native V1 API URL as one.
When the adapter is off (the default), neither an HTTP nor a WebSocket libSQL
URI is available; creating a project key alone does not enable the adapter.
Only when the user checks **Remember master key** and a global-admin-only
`/v1/metrics` call succeeds does the console save that global management API
key in the browser's origin-scoped localStorage. It provides a clear button;
project keys and KMS keys are never stored there. This is an explicit
convenience/security tradeoff: same-origin JavaScript, browser extensions, and
other users of the browser profile can read localStorage. Never enable it on
a shared device; disconnecting alone does not clear a saved key.

For local iteration, start the Go gateway with TLS as described in
`../README.md`, then start Vite on loopback:

```sh
cd prototypes/v1/platform/frontend
PERI_GATEWAY_URL=https://127.0.0.1:9443 \
NODE_EXTRA_CA_CERTS=/private/run/keys/dev-ca.crt npm run dev
```

Use the actual `ca_file` in the private launcher `connection.json` for
`NODE_EXTRA_CA_CERTS`; for an existing trusted production CA, Node's normal
trust store suffices. Vite proxies `/v1/` and the legacy `/rpc.js`, `/json.js`,
`/refs.js` with TLS certificate verification enabled. Never set
`NODE_TLS_REJECT_UNAUTHORIZED=0` or bypass certificate validation in the proxy
or browser. For production, access the Go gateway over HTTPS with a trusted
certificate.
