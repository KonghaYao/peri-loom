/**
 * Panel 偏好：每个 key 独立存 /api/v1/panel/preferences/{key}（404 表示未设置）。
 * 写入采用「乐观更新 + 失败回滚」，不阻塞界面。
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { api, toApiError } from '../api/client';

/** 偏好键（避免散落在各页面的魔法字符串） */
export const PREF_KEYS = {
  theme: 'panel.theme',
  pageSize: 'panel.page_size',
  sqlStream: 'panel.sql_stream',
  tableDensity: 'panel.table_density',
  defaultDatabaseId: 'panel.default_database_id',
  sqlEditorHeight: 'panel.sql_editor_height',
} as const;

export type ThemeMode = 'light' | 'dark' | 'system';

interface PreferencesContextValue {
  /** 后端返回的原始偏好值 */
  values: Record<string, unknown>;
  loading: boolean;
  error: string | null;
  get: <T>(key: string, fallback: T) => T;
  set: (key: string, value: unknown) => Promise<void>;
  reload: () => void;
}

const PreferencesContext = createContext<PreferencesContextValue | null>(null);

const ALL_KEYS = Object.values(PREF_KEYS);

export function PreferencesProvider({ children }: { children: ReactNode }): JSX.Element {
  const [values, setValues] = useState<Record<string, unknown>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);
  const valuesRef = useRef<Record<string, unknown>>({});
  valuesRef.current = values;

  useEffect(() => {
    let alive = true;
    setLoading(true);
    // 偏好是「尽力而为」：单个 key 失败不影响其它 key
    Promise.all(
      ALL_KEYS.map(async (key) => {
        try {
          const value = await api.preferences.get(key);
          return [key, value] as const;
        } catch (err) {
          return [key, { __error: toApiError(err).friendlyMessage }] as const;
        }
      }),
    )
      .then((entries) => {
        if (!alive) return;
        const next: Record<string, unknown> = {};
        let firstError: string | null = null;
        for (const [key, value] of entries) {
          if (value && typeof value === 'object' && '__error' in (value as object)) {
            firstError = firstError ?? String((value as { __error: unknown }).__error);
            continue;
          }
          if (value !== null && value !== undefined) next[key] = value;
        }
        setValues(next);
        setError(firstError);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [nonce]);

  const set = useCallback(async (key: string, value: unknown) => {
    const previous = valuesRef.current[key];
    setValues((prev) => ({ ...prev, [key]: value })); // 乐观更新
    try {
      await api.preferences.put(key, value);
    } catch (err) {
      // 失败回滚
      setValues((prev) => {
        const next = { ...prev };
        if (previous === undefined) delete next[key];
        else next[key] = previous;
        return next;
      });
      throw err;
    }
  }, []);

  const get = useCallback(
    <T,>(key: string, fallback: T): T => {
      const value = values[key];
      return value === undefined || value === null ? fallback : (value as T);
    },
    [values],
  );

  const reload = useCallback(() => setNonce((n) => n + 1), []);

  const value = useMemo<PreferencesContextValue>(
    () => ({ values, loading, error, get, set, reload }),
    [values, loading, error, get, set, reload],
  );

  return <PreferencesContext.Provider value={value}>{children}</PreferencesContext.Provider>;
}

export function usePreferences(): PreferencesContextValue {
  const ctx = useContext(PreferencesContext);
  if (!ctx) throw new Error('usePreferences 必须在 PreferencesProvider 内使用');
  return ctx;
}
