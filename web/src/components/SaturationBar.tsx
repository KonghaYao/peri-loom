/**
 * 饱和度条：标出 70%~75% 目标区间与 80% / 90% 水位线（架构 §15 容量水位）。
 * 数值为 null 时显示「无数据」，避免把缺失数据误读为 0%。
 */
import { Tooltip } from 'antd';
import { clampPercent } from '../utils/format';
import {
  SAT_DANGER,
  SAT_TARGET_MAX,
  SAT_TARGET_MIN,
  SAT_WARN,
  SATURATION_LABELS,
  saturationColor,
  saturationLevel,
} from '../utils/saturation';

interface SaturationBarProps {
  value: number | null;
  height?: number;
  /** 是否显示水位线标注（表格内可关闭以节省空间） */
  showMarks?: boolean;
  /** 悬停说明，默认显示水位线规则 */
  title?: string;
}

export function SaturationBar({
  value,
  height = 12,
  showMarks = true,
  title,
}: SaturationBarProps): JSX.Element {
  if (value === null) {
    return <span className="sat-empty">无数据</span>;
  }
  const pct = clampPercent(value);
  const level = saturationLevel(pct);
  const color = saturationColor(pct);
  const tip =
    title ??
    `饱和度 ${pct.toFixed(1)}%（${SATURATION_LABELS[level]}）｜目标区间 ${SAT_TARGET_MIN}%~${SAT_TARGET_MAX}%｜预警线 ${SAT_WARN}%｜危险线 ${SAT_DANGER}%`;

  return (
    <Tooltip title={tip}>
      <div className="sat-wrap">
        <div className="sat-bar" style={{ height }}>
          {/* 目标区间高亮带 */}
          <div
            className="sat-band"
            style={{ left: `${SAT_TARGET_MIN}%`, width: `${SAT_TARGET_MAX - SAT_TARGET_MIN}%` }}
          />
          {/* 水位线 */}
          {showMarks && (
            <>
              <div className="sat-mark" style={{ left: `${SAT_WARN}%` }} />
              <div className="sat-mark sat-mark--danger" style={{ left: `${SAT_DANGER}%` }} />
            </>
          )}
          <div className="sat-fill" style={{ width: `${pct}%`, background: color }} />
        </div>
        <span className="sat-value" style={{ color }}>
          {pct.toFixed(1)}%
        </span>
      </div>
    </Tooltip>
  );
}
