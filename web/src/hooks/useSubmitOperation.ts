/**
 * 提交长操作：把返回 202 的接口调用包成「pending / operationId / error」三态，
 * 页面据此渲染二次确认、进度弹窗与错误提示。
 */
import { useCallback, useState } from 'react';
import { ApiError, toApiError } from '../api/client';
import type { OperationAccepted } from '../api/types';

export interface SubmitOperationState {
  pending: boolean;
  operationId: string | null;
  error: ApiError | null;
  /** 执行提交；成功返回 202 响应，失败返回 null（错误在 error 中） */
  run: (fn: () => Promise<OperationAccepted>) => Promise<OperationAccepted | null>;
  /** 直接跟踪一个已提交的 operation_id（弹窗内提交的场景） */
  track: (operationId: string) => void;
  /** 关闭进度弹窗 */
  clearOperation: () => void;
  clearError: () => void;
}

export function useSubmitOperation(): SubmitOperationState {
  const [pending, setPending] = useState(false);
  const [operationId, setOperationId] = useState<string | null>(null);
  const [error, setError] = useState<ApiError | null>(null);

  const run = useCallback(async (fn: () => Promise<OperationAccepted>) => {
    setPending(true);
    setError(null);
    setOperationId(null);
    try {
      const accepted = await fn();
      setOperationId(accepted?.operation_id ?? null);
      return accepted;
    } catch (err) {
      setError(toApiError(err));
      return null;
    } finally {
      setPending(false);
    }
  }, []);

  const track = useCallback((id: string) => {
    setError(null);
    setOperationId(id);
  }, []);

  const clearOperation = useCallback(() => setOperationId(null), []);
  const clearError = useCallback(() => setError(null), []);

  return { pending, operationId, error, run, track, clearOperation, clearError };
}
