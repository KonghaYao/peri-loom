# Platform console

Build the React + Vite + TypeScript console before compiling the Go gateway:

```sh
cd prototypes/v1/platform/frontend
bun install --frozen-lockfile
bun run typecheck
bun run test
bun run build
cd ..
go build -buildvcs=false ./cmd/peri-platform ./cmd/peri
```

Vite emits `index.html`, `console.js`, and `console.css` into `../web`. The
directory is not emptied, preserving the existing protocol modules. Go's
`//go:embed web/*` captures these files when the Go binary is compiled. The
deployed console uses no runtime CDN.

The console uses `@cloudflare/kumo@2.14.0` with standalone styles. Hash routes
include the project, database, and branch. The database list is the home page;
open a database for data, SQL, branches, recovery, activity, and settings.
Management and project SQL credentials are held separately. Project SQL keys
are never written to localStorage. Only a positively verified global admin can
opt to remember a master key. Disconnecting does not erase a saved master key;
clear that credential separately on the device.

Table names come from `/v1/databases/schema`. A selected table previews at most
25 unordered rows. SQL uses the native `describe` and `statement` routes and
displays real column names; results displayed here are capped at 200 rows,
not paginated via cursors. The connection dialog checks the live capabilities
endpoint and only exposes libSQL/WS addresses when enabled. The adapter is
experimental, and the native V1 URL is not a libSQL URL.

The current API lacks `/v1/me`, key metadata listing, global operation paging,
and region/organization discovery. The UI does not fabricate these records:
project settings are read-only, key revocation is by ID, and operation records
are scoped to a database. Protocol tests do not replace a live gateway check.

For local iteration, start the Go gateway with TLS, then start Vite on loopback:

```sh
cd prototypes/v1/platform/frontend
PERI_GATEWAY_URL=https://127.0.0.1:9443 \
NODE_EXTRA_CA_CERTS=/private/run/keys/dev-ca.crt bun run dev
```

Use the actual CA file from the private launcher configuration. Vite proxies
`/v1/` and the legacy protocol modules with TLS verification enabled. Do not
disable certificate verification in the proxy or browser.
