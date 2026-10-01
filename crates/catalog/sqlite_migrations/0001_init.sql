CREATE TABLE catalog_version (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL);
INSERT INTO catalog_version VALUES (1,0);
CREATE TABLE databases (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, name TEXT NOT NULL, state TEXT NOT NULL, deleted_at TEXT, record TEXT NOT NULL);
CREATE UNIQUE INDEX databases_live_name ON databases(tenant_id,name) WHERE deleted_at IS NULL;
CREATE TABLE users (id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, status TEXT NOT NULL, record TEXT NOT NULL);
CREATE TABLE roles (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE role_permissions (role_id TEXT NOT NULL REFERENCES roles(id), permission TEXT NOT NULL, PRIMARY KEY(role_id,permission));
CREATE TABLE role_bindings (id TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id), role_id TEXT NOT NULL REFERENCES roles(id), tenant_id TEXT, database_id TEXT);
CREATE TABLE api_tokens (id TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id), token_hash TEXT NOT NULL UNIQUE, revoked_at TEXT, expires_at TEXT, created_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE INDEX api_tokens_user ON api_tokens(user_id);
CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id TEXT, action TEXT NOT NULL, result TEXT NOT NULL, created_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE TABLE operations (id TEXT PRIMARY KEY, database_id TEXT REFERENCES databases(id), kind TEXT NOT NULL, state TEXT NOT NULL, idempotency_key TEXT UNIQUE, created_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE TABLE jobs (id TEXT PRIMARY KEY, database_id TEXT REFERENCES databases(id), kind TEXT NOT NULL, state TEXT NOT NULL, priority INTEGER NOT NULL, run_after TEXT NOT NULL, lease_expires_at TEXT, idempotency_key TEXT UNIQUE, created_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE INDEX jobs_claim ON jobs(state,kind,priority,run_after);
CREATE INDEX jobs_pending_db ON jobs(database_id,state,kind);
CREATE TABLE idempotency_keys (key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, operation_id TEXT NOT NULL REFERENCES operations(id), job_id TEXT NOT NULL REFERENCES jobs(id), operation_record TEXT NOT NULL, job_record TEXT NOT NULL, database_record TEXT);
CREATE TABLE snapshots (id TEXT PRIMARY KEY, database_id TEXT NOT NULL REFERENCES databases(id), state TEXT NOT NULL, created_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE INDEX snapshots_db ON snapshots(database_id,state,created_at);
CREATE TABLE backup_jobs (id TEXT PRIMARY KEY, database_id TEXT NOT NULL REFERENCES databases(id), state TEXT NOT NULL, created_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE TABLE panel_preferences (user_id TEXT NOT NULL REFERENCES users(id), key TEXT NOT NULL, record TEXT NOT NULL, PRIMARY KEY(user_id,key));
CREATE TABLE saved_queries (id TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id), updated_at TEXT NOT NULL, record TEXT NOT NULL);
CREATE TABLE slow_queries (id INTEGER PRIMARY KEY AUTOINCREMENT, database_id TEXT NOT NULL REFERENCES databases(id), duration_micros INTEGER NOT NULL, created_at TEXT NOT NULL, record TEXT NOT NULL);
INSERT INTO roles VALUES
 ('00000000-0000-0000-0000-000000000010','platform-admin'),
 ('00000000-0000-0000-0000-000000000011','dba'),
 ('00000000-0000-0000-0000-000000000012','developer'),
 ('00000000-0000-0000-0000-000000000013','viewer');
INSERT INTO role_permissions VALUES
 ('00000000-0000-0000-0000-000000000010','db:read'),
 ('00000000-0000-0000-0000-000000000010','db:write'),
 ('00000000-0000-0000-0000-000000000010','db:admin'),
 ('00000000-0000-0000-0000-000000000010','audit:read'),
 ('00000000-0000-0000-0000-000000000010','token:admin'),
 ('00000000-0000-0000-0000-000000000011','db:read'),
 ('00000000-0000-0000-0000-000000000011','db:write'),
 ('00000000-0000-0000-0000-000000000011','db:admin'),
 ('00000000-0000-0000-0000-000000000011','audit:read'),
 ('00000000-0000-0000-0000-000000000012','db:read'),
 ('00000000-0000-0000-0000-000000000012','db:write'),
 ('00000000-0000-0000-0000-000000000013','db:read');
