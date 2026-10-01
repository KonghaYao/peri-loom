/**
 * 饱和度模型（架构 §5/§15：Worker 目标水位 70%~75%，80% 预警，90% 危险）。
 * 饱和度 = max(CPU, 内存, 进程数) 三个维度中最高者 —— 最紧张的维度决定可调度性。
 */

/** 目标区间下界 */
export const SAT_TARGET_MIN = 70;
/** 目标区间上界 */
export const SAT_TARGET_MAX = 75;
/** 预警水位线 */
export const SAT_WARN = 80;
/** 危险水位线 */
export const SAT_DANGER = 90;

export type SaturationLevel = 'cold' | 'target' | 'warm' | 'warn' | 'danger';

export function saturationLevel(value: number): SaturationLevel {
  if (value >= SAT_DANGER) return 'danger';
  if (value >= SAT_WARN) return 'warn';
  if (value >= SAT_TARGET_MAX) return 'warm';
  if (value >= SAT_TARGET_MIN) return 'target';
  return 'cold';
}

export const SATURATION_COLORS: Record<SaturationLevel, string> = {
  cold: '#1677ff', // 利用率偏低，可继续承载
  target: '#52c41a', // 目标区间
  warm: '#13c2c2', // 目标之上但未预警
  warn: '#faad14', // 80% 预警
  danger: '#ff4d4f', // 90% 危险
};

export const SATURATION_LABELS: Record<SaturationLevel, string> = {
  cold: '偏低',
  target: '目标区间',
  warm: '偏高',
  warn: '预警',
  danger: '危险',
};

export function saturationColor(value: number): string {
  return SATURATION_COLORS[saturationLevel(value)];
}
