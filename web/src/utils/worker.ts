/**
 * Worker 字段读取：后端 WorkerView 的用量/容量都是**绝对值**
 * （usage.cpu_milli_used、capacity.cpu_milli_total），百分比在这里换算，
 * 避免各页面重复判断。
 */
import type { Database, Worker } from '../api/types';
import { toPercent } from './format';
import { saturationColor, saturationLevel, type SaturationLevel } from './saturation';

function ratioPercent(used?: number | null, total?: number | null): number | null {
  if (typeof used !== 'number' || typeof total !== 'number' || total <= 0) return null;
  return toPercent((used / total) * 100);
}

export function workerCpuPercent(worker: Worker): number | null {
  return ratioPercent(worker.usage?.cpu_milli_used, worker.capacity?.cpu_milli_total);
}

export function workerMemoryPercent(worker: Worker): number | null {
  return ratioPercent(worker.usage?.memory_mib_used, worker.capacity?.memory_mib_total);
}

export function workerProcessCount(worker: Worker): number | null {
  const v = worker.usage?.process_slots_used;
  return typeof v === 'number' ? v : null;
}

export function workerMaxProcesses(worker: Worker): number | null {
  const v = worker.capacity?.process_slots_total;
  return typeof v === 'number' ? v : null;
}

/** 进程数占用率 */
export function workerProcessPercent(worker: Worker): number | null {
  return ratioPercent(worker.usage?.process_slots_used, worker.capacity?.process_slots_total);
}

/** Worker 上的 DB 列表（WorkerView.running_databases） */
export function workerDatabaseRefs(worker: Worker): Array<string | Database> {
  return worker.running_databases ?? [];
}

export function workerDbCount(worker: Worker): number {
  return workerDatabaseRefs(worker).length;
}

export function workerState(worker: Worker): string {
  return worker.state ?? 'UNKNOWN';
}

export function workerAddress(worker: Worker): string {
  return worker.endpoint ?? '-';
}

/** Worker 名（WorkerView.id） */
export function workerName(worker: Worker): string {
  return worker.id;
}

/**
 * Worker 饱和度：max(CPU, 内存, 进程数)。
 * 无任何用量数据时返回 null（界面显示「无数据」而非 0）。
 */
export function workerSaturation(worker: Worker): number | null {
  const candidates = [workerCpuPercent(worker), workerMemoryPercent(worker), workerProcessPercent(worker)].filter(
    (v): v is number => v !== null,
  );
  if (candidates.length === 0) return null;
  return Math.max(...candidates);
}

export function workerSaturationLevel(worker: Worker): SaturationLevel | null {
  const sat = workerSaturation(worker);
  return sat === null ? null : saturationLevel(sat);
}

export function workerSaturationColor(worker: Worker): string {
  const sat = workerSaturation(worker);
  return sat === null ? '#d9d9d9' : saturationColor(sat);
}
