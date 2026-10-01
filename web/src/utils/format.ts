/** 通用格式化工具 */
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import 'dayjs/locale/zh-cn';

dayjs.extend(relativeTime);
dayjs.locale('zh-cn');

export function formatTime(value?: string | number | null): string {
  if (value === undefined || value === null || value === '') return '-';
  const d = typeof value === 'number' ? (value > 1e12 ? dayjs(value) : dayjs.unix(value)) : dayjs(value);
  return d.isValid() ? d.format('YYYY-MM-DD HH:mm:ss') : String(value);
}

export function formatRelative(value?: string | number | null): string {
  if (value === undefined || value === null || value === '') return '-';
  const d = dayjs(value);
  return d.isValid() ? d.fromNow() : String(value);
}

export function formatBytes(bytes?: number | null): string {
  if (bytes === undefined || bytes === null || Number.isNaN(bytes)) return '-';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB', 'PB'];
  let v = bytes / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`;
}

/** 微秒 -> 人类可读耗时 */
export function formatMicros(micros?: number | null): string {
  if (micros === undefined || micros === null || Number.isNaN(micros)) return '-';
  if (micros < 1000) return `${micros} µs`;
  return formatMillis(micros / 1000);
}

export function formatMillis(ms?: number | null): string {
  if (ms === undefined || ms === null || Number.isNaN(ms)) return '-';
  if (ms < 1000) return `${ms.toFixed(ms < 10 ? 2 : 0)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)} s`;
  const m = Math.floor(s / 60);
  return `${m} 分 ${(s - m * 60).toFixed(1)} 秒`;
}

/** 长 ID 中间省略，保留可辨识前后缀 */
export function shortId(id?: string | null, head = 8, tail = 4): string {
  if (!id) return '-';
  if (id.length <= head + tail + 1) return id;
  return `${id.slice(0, head)}…${id.slice(-tail)}`;
}

/**
 * 归一化为百分比（0-100）。
 * 后端可能返回 0-1 的比例或 0-100 的百分比，这里按值域推断。
 */
export function toPercent(value?: number | null): number | null {
  if (value === undefined || value === null || Number.isNaN(value)) return null;
  const v = value <= 1.0001 && value >= 0 ? value * 100 : value;
  return Math.max(0, Math.min(100, v));
}

export function clampPercent(value: number): number {
  return Math.max(0, Math.min(100, value));
}

/** 复制到剪贴板（不可用时降级为 execCommand） */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* 降级 */
  }
  try {
    const el = document.createElement('textarea');
    el.value = text;
    el.style.position = 'fixed';
    el.style.opacity = '0';
    document.body.appendChild(el);
    el.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(el);
    return ok;
  } catch {
    return false;
  }
}
