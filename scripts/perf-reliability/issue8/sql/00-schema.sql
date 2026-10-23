-- Benchmark fixtures only. Create in a fresh, isolated test database.
-- The bench table intentionally matches scripts/perf-reliability/runner.py.
CREATE TABLE bench(id INTEGER PRIMARY KEY, payload TEXT);
CREATE TABLE kv(
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  value INTEGER NOT NULL,
  payload TEXT NOT NULL
);
CREATE INDEX kv_tenant_id ON kv(tenant_id, id);
CREATE TABLE events(
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  revision INTEGER NOT NULL,
  payload TEXT NOT NULL
);
CREATE TABLE accounts(id INTEGER PRIMARY KEY, balance INTEGER NOT NULL);
CREATE TABLE transfer_receipts(
  op_id TEXT PRIMARY KEY,
  source_id INTEGER NOT NULL,
  target_id INTEGER NOT NULL,
  amount INTEGER NOT NULL
);
