/**
 * 认证上下文：管理台只接受管理员 JWT（账号密码登录换取或手动粘贴）。
 * dbp_ API Token 是数据库 SDK 凭据，不能进入管理台认证状态。
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { api, ApiError, toApiError, tokenStore, UNAUTHORIZED_EVENT } from '../api/client';
import { useQueryClient } from '@tanstack/react-query';

interface AuthContextValue {
  token: string | null;
  user: string | null;
  /** 校验并保存管理员 JWT；返回校验过程中的告警（后端不可达等），校验失败则抛错 */
  signInWithToken: (token: string, user?: string) => Promise<string | null>;
  signOut: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

/**
 * 校验当前 Token 是否可用（token 已写入 localStorage，client 会自动带上）：
 *   - 200            -> 有效；
 *   - 401/403        -> 无效，抛错；
 *   - 其他（404/501/网络）-> 后端未实现或不可达，放行并给出告警。
 */
async function verifyToken(): Promise<string | null> {
  try {
    await api.me();
    return null;
  } catch (err) {
    const e = toApiError(err);
    if (e.status === 401 || e.status === 403 || e.code === 'TOKEN_INVALID' || e.code === 'TOKEN_EXPIRED') {
      throw new ApiError({
        code: e.code,
        status: e.status,
        message: e.message,
        requestId: e.requestId,
        retryable: false,
      });
    }
    return `无法在线校验 Token（${e.status ? `HTTP ${e.status}` : e.code}），已直接使用；若后续请求返回 401，请重新登录。`;
  }
}

/** 清掉旧版本可能留在浏览器里的 dbp_ 数据库凭据，避免它被当作管理台会话恢复。 */
function readManagementToken(): string | null {
  const stored = tokenStore.get();
  if (stored?.startsWith('dbp_')) {
    tokenStore.clear();
    return null;
  }
  return stored;
}

export function AuthProvider({ children }: { children: ReactNode }): JSX.Element {
  const queryClient = useQueryClient();
  const [token, setToken] = useState<string | null>(readManagementToken);
  const [user, setUser] = useState<string | null>(() => tokenStore.getUser());

  const signInWithToken = useCallback(async (next: string, userName?: string) => {
    const value = next.trim();
    if (!value) throw new Error('Token 不能为空');
    if (value.startsWith('dbp_')) {
      throw new Error('dbp_ API Token 只供数据库 SDK 使用，不能登录管理台。请使用账号密码或管理员 JWT。');
    }
    tokenStore.set(value);
    let warning: string | null = null;
    try {
      warning = await verifyToken();
    } catch (err) {
      tokenStore.clear();
      throw err;
    }
    if (userName) {
      tokenStore.setUser(userName);
      setUser(userName);
    }
    queryClient.clear();
    setToken(value);
    return warning;
  }, [queryClient]);

  const signOut = useCallback(() => {
    queryClient.clear();
    tokenStore.clear();
    setToken(null);
    setUser(null);
  }, [queryClient]);

  // 任意请求返回 401 时，client 会广播事件，这里同步清理本地状态
  useEffect(() => {
    const onUnauthorized = () => {
      queryClient.clear();
      setToken(null);
      setUser(null);
    };
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  }, [queryClient]);

  const value = useMemo<AuthContextValue>(
    () => ({ token, user, signInWithToken, signOut }),
    [token, user, signInWithToken, signOut],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth 必须在 AuthProvider 内使用');
  return ctx;
}
