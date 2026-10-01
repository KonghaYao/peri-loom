/** 状态标签：DB 生命周期 / Worker / 操作 三类状态的统一渲染 */
import { Tag, Tooltip } from 'antd';
import {
  DATABASE_STATE_META,
  OPERATION_STATE_META,
  WORKER_STATE_META,
} from '../api/types';

interface Meta {
  label: string;
  color: string;
}

function resolve(map: Record<string, Meta>, state?: string | null, fallbackLabel = '未知'): Meta {
  if (!state) return { label: fallbackLabel, color: 'default' };
  return map[state] ?? { label: state, color: 'default' };
}

export function DatabaseStateTag({ state }: { state?: string | null }): JSX.Element {
  const meta = resolve(DATABASE_STATE_META, state);
  return (
    <Tooltip title={state ?? undefined}>
      <Tag color={meta.color}>{meta.label}</Tag>
    </Tooltip>
  );
}

export function WorkerStateTag({ state }: { state?: string | null }): JSX.Element {
  const meta = resolve(WORKER_STATE_META, state);
  return (
    <Tooltip title={state ?? undefined}>
      <Tag color={meta.color}>{meta.label}</Tag>
    </Tooltip>
  );
}

export function OperationStateTag({ state }: { state?: string | null }): JSX.Element {
  const meta = resolve(OPERATION_STATE_META, state);
  return (
    <Tooltip title={state ?? undefined}>
      <Tag color={meta.color}>{meta.label}</Tag>
    </Tooltip>
  );
}
