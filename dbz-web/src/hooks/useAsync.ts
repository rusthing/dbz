/**
 * # 一次性异步请求 Hook
 *
 * 用于「全量列表」「下拉选项」这类不需要分页的请求。`deps` 变化或调用 `reload` 时重查。
 */

import { useCallback, useEffect, useState } from 'react';
import type { ApiError } from '../api/client';
import { toApiError } from './usePagedQuery';

/** # 异步状态 */
export interface AsyncState<T> {
  data: T;
  loading: boolean;
  error: ApiError | null;
  reload: () => void;
}

/**
 * # 执行一次性请求
 *
 * ## 参数
 * * `fetcher` - 请求函数，`deps` 变化时会被重新调用
 * * `deps` - 依赖项
 * * `initial` - 初始值，请求未返回前使用
 */
export function useAsync<T>(fetcher: () => Promise<T>, deps: unknown[], initial: T): AsyncState<T> {
  const [data, setData] = useState<T>(initial);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ApiError | null>(null);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError(null);

    fetcher()
      .then((result) => {
        if (alive) {
          setData(result);
        }
      })
      .catch((reason: unknown) => {
        if (alive) {
          setError(toApiError(reason));
        }
      })
      .finally(() => {
        if (alive) {
          setLoading(false);
        }
      });

    return () => {
      alive = false;
    };
    // fetcher 每次渲染都是新引用，刻意不放进依赖
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);

  const reload = useCallback(() => {
    setNonce((value) => value + 1);
  }, []);

  return { data, loading, error, reload };
}
