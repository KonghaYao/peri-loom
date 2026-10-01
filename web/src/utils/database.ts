/** 字段读取助手：字段名以后端 openapi.json 为准（主键统一是 `id`），避免各页面重复判断 */
import type { AuditLog, Database, Operation, SlowQuery, Snapshot } from '../api/types';

export function dbId(db?: Pick<Database, 'id'> | null): string {
  return db?.id ?? '-';
}

export function dbName(db?: Pick<Database, 'id' | 'name'> | null): string {
  if (!db) return '-';
  return db.name || db.id;
}

export function dbWorkerId(db?: Pick<Database, 'owner_worker_id'> | null): string {
  return db?.owner_worker_id ?? '-';
}

export function dbEpoch(db?: Pick<Database, 'owner_epoch'> | null): string {
  return db?.owner_epoch === undefined || db?.owner_epoch === null ? '-' : String(db.owner_epoch);
}

/** 操作类型（OperationView.kind，如 CREATE_DB） */
export function operationKind(op?: Pick<Operation, 'kind'> | null): string {
  return op?.kind ?? '-';
}

/** 操作主键（OperationView.id） */
export function operationId(op?: Pick<Operation, 'id'> | null): string | undefined {
  return op?.id ?? undefined;
}

/** 操作关联的数据库 ID */
export function operationDbId(op?: Pick<Operation, 'database_id'> | null): string | undefined {
  return op?.database_id ?? undefined;
}

/** 快照主键（SnapshotView.id） */
export function snapshotId(snapshot?: Pick<Snapshot, 'id'> | null): string {
  return snapshot?.id ?? '-';
}

export function snapshotDbId(snapshot?: Pick<Snapshot, 'database_id'> | null): string | undefined {
  return snapshot?.database_id ?? undefined;
}

/** 快照状态（SnapshotView.state） */
export function snapshotState(snapshot?: Pick<Snapshot, 'state'> | null): string {
  return snapshot?.state ?? '-';
}

/** 慢查询语句（SlowQueryView.sql_text） */
export function slowQuerySql(q?: Pick<SlowQuery, 'sql_text'> | null): string {
  return q?.sql_text || '-';
}

/** 慢查询耗时（后端给的是微秒：duration_micros），换算为毫秒 */
export function slowQueryMillis(q?: Pick<SlowQuery, 'duration_micros'> | null): number | null {
  return typeof q?.duration_micros === 'number' ? q.duration_micros / 1000 : null;
}

/** 审计主键（AuditView.id 是自增整数） */
export function auditId(log?: Pick<AuditLog, 'id'> | null): string {
  return log?.id === undefined || log?.id === null ? '-' : String(log.id);
}

/** 审计操作者（AuditView.actor_name） */
export function auditActor(log?: Pick<AuditLog, 'actor_name' | 'actor_id'> | null): string {
  return log?.actor_name || log?.actor_id || '-';
}
