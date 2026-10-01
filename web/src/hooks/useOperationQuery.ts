/**
 * 长操作轮询：提交返回 202 + operation_id 后，轮询 /operations/{id} 直到终态。
 * 终态（SUCCEEDED / FAILED / CANCELLED）自动停止轮询。
 */
import { useQuery, type UseQueryResult } from '@tanstack/react-query';
import { api } from '../api/client';
import { isTerminalOperation, type Operation, type Page } from '../api/types';

/** 轮询间隔（毫秒） */
export const OPERATION_POLL_INTERVAL = 1000;

export function useOperationQuery(
  operationId: string | null | undefined,
  enabled = true,
): UseQueryResult<Operation, Error> {
  return useQuery({
    queryKey: ['operation', operationId],
    queryFn: () => api.operations.get(operationId as string),
    enabled: Boolean(operationId) && enabled,
    refetchInterval: (query) => {
      const data = query.state.data as Operation | undefined;
      if (data && isTerminalOperation(data.state)) return false;
      return OPERATION_POLL_INTERVAL;
    },
  });
}

/** 操作中心列表：存在未完成操作时自动轮询 */
export function useOperationsListQuery(limit: number, offset: number): UseQueryResult<Page<Operation>, Error> {
  return useQuery({
    queryKey: ['operations', limit, offset],
    queryFn: () => api.operations.list({ limit, offset }),
    refetchInterval: (query) => {
      const items = (query.state.data as { items?: Operation[] } | undefined)?.items ?? [];
      return items.some((op) => !isTerminalOperation(op.state)) ? 2000 : false;
    },
  });
}
