-- ============================================================================
-- DB Platform Catalog —— PostgreSQL 权威事实源（架构 §17.5 / §17.5.1）
--
-- 本 schema 是下列数据的唯一权威来源，禁止把这些表迁移到 TursoDB：
--   database catalog / ownership+epoch / worker inventory / scheduler+control jobs
--   auth+RBAC metadata / snapshot+backup metadata / audit metadata
--   Panel UI preferences / Saved SQL / saved filters
--
-- bootstrap 边界：PostgreSQL Ready -> Server/Control Plane Ready -> Worker/DB Runtime -> 用户负载
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ------------------------------------------------------------------ 全局版本
-- 用于 Server Router Cache 的 correctness reconcile（LISTEN/NOTIFY 只是加速提示）
CREATE TABLE catalog_version (
    id          SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version     BIGINT   NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO catalog_version (id, version) VALUES (1, 1) ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------------ Tenant
CREATE TABLE tenants (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'ACTIVE'
                CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DELETED')),
    -- 平台级配额（NULL = 无限制）
    quota_max_databases   INTEGER,
    quota_max_cpu_milli   BIGINT,
    quota_max_memory_mib  BIGINT,
    quota_max_storage_mib BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------ Worker
-- Worker inventory。capacity 与 usage 均以“资源预算”模型表达（架构 §9）
CREATE TABLE workers (
    id                TEXT PRIMARY KEY,
    endpoint          TEXT NOT NULL,
    -- gRPC endpoint for control & data path
    control_endpoint  TEXT,
    data_endpoint     TEXT,
    state             TEXT NOT NULL DEFAULT 'ACTIVE'
                      CHECK (state IN ('ACTIVE', 'SUSPECT', 'DRAINING', 'EMPTY', 'UNAVAILABLE')),
    region            TEXT NOT NULL DEFAULT 'default',
    zone              TEXT NOT NULL DEFAULT 'default',
    version           TEXT NOT NULL DEFAULT '',
    -- 容量预算（milestone：milli-core / MiB / count）
    cpu_milli_total       BIGINT NOT NULL DEFAULT 0,
    memory_mib_total      BIGINT NOT NULL DEFAULT 0,
    fd_total              BIGINT NOT NULL DEFAULT 0,
    disk_mib_total        BIGINT NOT NULL DEFAULT 0,
    process_slots_total   BIGINT NOT NULL DEFAULT 0,
    iops_total            BIGINT NOT NULL DEFAULT 0,
    -- 最近一次上报的占用
    cpu_milli_used        BIGINT NOT NULL DEFAULT 0,
    memory_mib_used       BIGINT NOT NULL DEFAULT 0,
    fd_used               BIGINT NOT NULL DEFAULT 0,
    disk_mib_used         BIGINT NOT NULL DEFAULT 0,
    process_slots_used    BIGINT NOT NULL DEFAULT 0,
    iops_used             BIGINT NOT NULL DEFAULT 0,
    last_heartbeat_at     TIMESTAMPTZ,
    -- 连续 heartbeat miss 计数；>=3 判定 Suspect/Unavailable（架构 §16）
    missed_heartbeats     INTEGER NOT NULL DEFAULT 0,
    inventory_version     BIGINT NOT NULL DEFAULT 0,
    -- 是否保留该 Worker 不作普通 Placement（failover reserve 用）
    reserved_for_failover BOOLEAN NOT NULL DEFAULT FALSE,
    labels                JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX workers_state_idx ON workers (state);
CREATE INDEX workers_heartbeat_idx ON workers (last_heartbeat_at);

-- ------------------------------------------------------------------ Database
CREATE TABLE databases (
    id                UUID PRIMARY KEY,
    tenant_id         UUID NOT NULL REFERENCES tenants (id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    -- 生命周期：COLD / STARTING / WARM / HOT / DRAINING / STOPPING / FAILED
    state             TEXT NOT NULL DEFAULT 'COLD'
                      CHECK (state IN ('COLD', 'STARTING', 'WARM', 'HOT', 'DRAINING', 'STOPPING', 'FAILED')),
    -- 当前 Owner Worker；COLD 时为 NULL
    owner_worker_id   TEXT REFERENCES workers (id) ON DELETE SET NULL,
    -- 单调递增 Owner Epoch：所有 Start/Write/Ownership 动作都携带（架构 §10）
    owner_epoch       BIGINT NOT NULL DEFAULT 0,
    -- 租约：Owner 必须在 lease_expires_at 前续约
    lease_expires_at  TIMESTAMPTZ,
    -- 冷启动并发合并（Coalesce Wakeup）：同一 COLD DB 只允许一个 Start 动作
    wakeup_in_progress BOOLEAN NOT NULL DEFAULT FALSE,
    wakeup_started_at  TIMESTAMPTZ,
    -- Storage location（逻辑位置，实际数据在 Worker Local NVMe）
    storage_region    TEXT NOT NULL DEFAULT 'default',
    storage_prefix    TEXT NOT NULL DEFAULT '',
    -- 最近一次恢复基线
    last_snapshot_id  TEXT,
    last_snapshot_lsn BIGINT,
    -- 资源画像 / 策略
    cpu_milli         BIGINT NOT NULL DEFAULT 500,
    memory_mib        BIGINT NOT NULL DEFAULT 256,
    fd_limit          BIGINT NOT NULL DEFAULT 512,
    disk_mib          BIGINT NOT NULL DEFAULT 1024,
    iops_limit        BIGINT NOT NULL DEFAULT 2000,
    priority          INTEGER NOT NULL DEFAULT 100,
    -- HOT / WARM 回收策略
    evictable         BOOLEAN NOT NULL DEFAULT TRUE,
    -- DB engine version（TursoDB 集成版本）
    engine_version    TEXT NOT NULL DEFAULT '',
    -- schema/version metadata
    schema_version    INTEGER NOT NULL DEFAULT 0,
    -- 亲和性
    affinity_worker_id TEXT,
    anti_affinity_worker_id TEXT,
    labels            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at        TIMESTAMPTZ,
    UNIQUE (tenant_id, name)
);
CREATE INDEX databases_worker_idx ON databases (owner_worker_id);
CREATE INDEX databases_state_idx ON databases (state);
CREATE INDEX databases_tenant_idx ON databases (tenant_id);
-- Scheduler 需要快速找出可回收的 WARM/COLD DB
CREATE INDEX databases_evict_idx ON databases (state, priority) WHERE evictable;

-- Owner Epoch 变更审计（用于 Split Brain 事后校验）
CREATE TABLE ownership_events (
    id              BIGSERIAL PRIMARY KEY,
    database_id     UUID NOT NULL,
    from_worker_id  TEXT,
    to_worker_id    TEXT,
    from_epoch      BIGINT NOT NULL,
    to_epoch        BIGINT NOT NULL,
    reason          TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ownership_events_db_idx ON ownership_events (database_id, created_at DESC);

-- ------------------------------------------------------------------ Operations
-- 长操作统一走 202 + operation_id 语义（架构 §17.4）
CREATE TABLE operations (
    id                UUID PRIMARY KEY,
    kind              TEXT NOT NULL
                      CHECK (kind IN ('CREATE_DB', 'DELETE_DB', 'START_DB', 'STOP_DB', 'RESTART_DB',
                                      'MOVE_DB', 'BACKUP_DB', 'RESTORE_DB', 'SNAPSHOT_DB',
                                      'DRAIN_WORKER', 'CREATE_TOKEN')),
    state             TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    database_id       UUID REFERENCES databases (id) ON DELETE SET NULL,
    worker_id         TEXT,
    tenant_id         UUID,
    requested_by      UUID,
    idempotency_key   TEXT,
    progress          SMALLINT NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
    error_code        TEXT,
    error_message     TEXT,
    result            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);
CREATE INDEX operations_state_idx ON operations (state, created_at);
CREATE INDEX operations_db_idx ON operations (database_id, created_at DESC);
-- 相同 Idempotency-Key 的重复提交只能产生 1 个 Operation（架构 §16 Public HTTP Contract）
CREATE UNIQUE INDEX operations_idempotency_idx ON operations (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- ------------------------------------------------------------------ Job queue
-- PostgreSQL Job Table + FOR UPDATE SKIP LOCKED + lease/idempotency（架构 §17.5）
-- 不引入 RabbitMQ/Kafka/NATS。
CREATE TABLE jobs (
    id              UUID PRIMARY KEY,
    kind            TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    state           TEXT NOT NULL DEFAULT 'READY'
                    CHECK (state IN ('READY', 'LEASED', 'DONE', 'FAILED', 'CANCELLED')),
    priority        INTEGER NOT NULL DEFAULT 100,
    run_after       TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner     TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    last_error      TEXT,
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
CREATE INDEX jobs_ready_idx ON jobs (state, run_after, priority) WHERE state IN ('READY', 'LEASED');
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- ------------------------------------------------------------------ Idempotency
-- Management 副作用请求的幂等记录（Create/Move/Backup/Restore 等）
CREATE TABLE idempotency_keys (
    key             TEXT PRIMARY KEY,
    request_hash    TEXT NOT NULL,
    operation_id    UUID,
    response_status INTEGER NOT NULL DEFAULT 202,
    response_body   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '24 hours'
);
CREATE INDEX idempotency_expiry_idx ON idempotency_keys (expires_at);

-- ------------------------------------------------------------------ Snapshot
CREATE TABLE snapshots (
    id                TEXT PRIMARY KEY,
    database_id       UUID NOT NULL REFERENCES databases (id) ON DELETE CASCADE,
    base_lsn          BIGINT NOT NULL,
    checksum          TEXT NOT NULL,
    size_bytes        BIGINT NOT NULL DEFAULT 0,
    object_key        TEXT NOT NULL,
    compression       TEXT NOT NULL DEFAULT 'zstd',
    owner_epoch       BIGINT NOT NULL,
    engine_version    TEXT NOT NULL DEFAULT '',
    schema_version    INTEGER NOT NULL DEFAULT 0,
    state             TEXT NOT NULL DEFAULT 'AVAILABLE'
                      CHECK (state IN ('PENDING', 'AVAILABLE', 'CORRUPTED', 'DELETED')),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_at       TIMESTAMPTZ
);
CREATE INDEX snapshots_db_idx ON snapshots (database_id, created_at DESC);

-- 备份 / 恢复作业记录（Backup RPO <= 5min，Restore 成功率 >= 99.9%）
CREATE TABLE backup_jobs (
    id              UUID PRIMARY KEY,
    database_id     UUID NOT NULL REFERENCES databases (id) ON DELETE CASCADE,
    operation_id    UUID REFERENCES operations (id) ON DELETE SET NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('BACKUP', 'RESTORE', 'PITR')),
    state           TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    snapshot_id     TEXT,
    -- PITR 目标时间点
    target_time     TIMESTAMPTZ,
    -- 实际恢复点（用于校验偏差 <= 1min）
    actual_point    TIMESTAMPTZ,
    bytes_transferred BIGINT NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
CREATE INDEX backup_jobs_db_idx ON backup_jobs (database_id, created_at DESC);

-- ------------------------------------------------------------------ RBAC
CREATE TABLE users (
    id            UUID PRIMARY KEY,
    tenant_id     UUID REFERENCES tenants (id) ON DELETE CASCADE,
    username      TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL DEFAULT '',
    password_hash TEXT,
    -- OIDC subject（外部身份）
    oidc_subject  TEXT UNIQUE,
    email         TEXT,
    status        TEXT NOT NULL DEFAULT 'ACTIVE'
                  CHECK (status IN ('ACTIVE', 'DISABLED', 'DELETED')),
    -- 全局超管（跨 tenant）
    is_superuser  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE roles (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    -- 权限清单，如 ["db:read","db:write","db:admin","worker:admin","audit:read"]
    permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 绑定可以是全局（database_id 为空）、tenant 级或 DB 级
CREATE TABLE role_bindings (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role_id     UUID NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    tenant_id   UUID REFERENCES tenants (id) ON DELETE CASCADE,
    database_id UUID REFERENCES databases (id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, role_id, tenant_id, database_id)
);
CREATE INDEX role_bindings_user_idx ON role_bindings (user_id);

CREATE TABLE api_tokens (
    id            UUID PRIMARY KEY,
    user_id       UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name          TEXT NOT NULL DEFAULT '',
    -- 只存哈希，明文只在创建时返回一次
    token_hash    TEXT NOT NULL UNIQUE,
    -- 作用域（db 级 / tenant 级）
    tenant_id     UUID REFERENCES tenants (id) ON DELETE CASCADE,
    database_id   UUID REFERENCES databases (id) ON DELETE CASCADE,
    permissions   JSONB NOT NULL DEFAULT '[]'::jsonb,
    expires_at    TIMESTAMPTZ,
    last_used_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX api_tokens_user_idx ON api_tokens (user_id);

-- ------------------------------------------------------------------ Audit
CREATE TABLE audit_log (
    id            BIGSERIAL PRIMARY KEY,
    actor_id      UUID,
    actor_name    TEXT NOT NULL DEFAULT '',
    tenant_id     UUID,
    database_id   UUID,
    -- 操作类型：DB_CREATE / DB_STOP / TOKEN_CREATE / WORKER_DRAIN ...
    action        TEXT NOT NULL,
    target_type   TEXT NOT NULL DEFAULT '',
    target_id     TEXT NOT NULL DEFAULT '',
    result        TEXT NOT NULL DEFAULT 'SUCCESS' CHECK (result IN ('SUCCESS', 'FAILURE')),
    error_code    TEXT,
    source_ip     TEXT,
    request_id    TEXT,
    detail        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX audit_log_time_idx ON audit_log (created_at DESC);
CREATE INDEX audit_log_db_idx ON audit_log (database_id, created_at DESC);
CREATE INDEX audit_log_actor_idx ON audit_log (actor_id, created_at DESC);

-- ------------------------------------------------------------------ Panel 数据
CREATE TABLE panel_preferences (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    value       JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, key)
);

CREATE TABLE saved_queries (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    database_id UUID REFERENCES databases (id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    sql         TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    tags        TEXT[] NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX saved_queries_user_idx ON saved_queries (user_id, updated_at DESC);

CREATE TABLE saved_filters (
    id          UUID PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    scope       TEXT NOT NULL,
    name        TEXT NOT NULL,
    value       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, scope, name)
);

-- ------------------------------------------------------------------ Slow query
CREATE TABLE slow_queries (
    id            BIGSERIAL PRIMARY KEY,
    database_id   UUID NOT NULL,
    worker_id     TEXT,
    session_id    TEXT,
    fingerprint   TEXT NOT NULL DEFAULT '',
    sql_text      TEXT NOT NULL,
    duration_micros BIGINT NOT NULL,
    rows_returned BIGINT NOT NULL DEFAULT 0,
    error_code    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX slow_queries_db_idx ON slow_queries (database_id, created_at DESC);
CREATE INDEX slow_queries_duration_idx ON slow_queries (duration_micros DESC);

-- ============================================================================
-- Catalog 变更通知：LISTEN/NOTIFY 只作为 Router Cache 的快速失效提示，
-- 不作为可靠消息队列。Server 断线恢复后必须通过 catalog_version 重新 reconcile。
-- ============================================================================
CREATE OR REPLACE FUNCTION bump_catalog_version() RETURNS TRIGGER AS $$
DECLARE
    new_version BIGINT;
    table_name  TEXT := TG_TABLE_NAME;
    rec_id      TEXT;
BEGIN
    UPDATE catalog_version SET version = version + 1, updated_at = now()
    WHERE id = 1 RETURNING version INTO new_version;

    BEGIN
        IF TG_OP = 'DELETE' THEN
            rec_id := OLD.id::text;
        ELSE
            rec_id := NEW.id::text;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        rec_id := '';
    END;

    PERFORM pg_notify('catalog_changes', json_build_object(
        'version', new_version,
        'table', table_name,
        'op', TG_OP,
        'id', rec_id
    )::text);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER catalog_version_databases
    AFTER INSERT OR UPDATE OR DELETE ON databases
    FOR EACH ROW EXECUTE FUNCTION bump_catalog_version();

CREATE TRIGGER catalog_version_workers
    AFTER INSERT OR UPDATE OR DELETE ON workers
    FOR EACH ROW EXECUTE FUNCTION bump_catalog_version();

-- ============================================================================
-- 初始数据：默认 tenant 与内置角色
-- ============================================================================
INSERT INTO tenants (id, name, display_name)
VALUES ('00000000-0000-0000-0000-000000000001', 'default', 'Default Tenant')
ON CONFLICT DO NOTHING;

INSERT INTO roles (id, name, description, permissions) VALUES
    ('00000000-0000-0000-0000-000000000010', 'platform-admin', 'Platform administrator',
     '["db:read","db:write","db:admin","worker:admin","audit:read","token:admin"]'::jsonb),
    ('00000000-0000-0000-0000-000000000011', 'dba', 'Database administrator',
     '["db:read","db:write","db:admin","audit:read"]'::jsonb),
    ('00000000-0000-0000-0000-000000000012', 'developer', 'Developer',
     '["db:read","db:write"]'::jsonb),
    ('00000000-0000-0000-0000-000000000013', 'viewer', 'Read-only observer',
     '["db:read"]'::jsonb)
ON CONFLICT DO NOTHING;
