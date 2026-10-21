/**
 * # 分页查询 Hook
 *
 * 把「筛选条件 + 页码 + 页大小 + 手动刷新」收敛成一份状态，页面只负责渲染。
 *
 * 约定：`fetchPage` 必须是**稳定引用**（模块级函数或 `useCallback`），否则每次渲染都会重新请求。
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import type { PageRx } from '../api/types';
import { ApiError, type QueryParams } from '../api/client';

/** # 分页状态 */
export interface PagedQueryState<T> {
  /** 当前页数据 */
  data: PageRx<T>;
  /** 请求中 */
  loading: boolean;
  /** 最近一次错误 */
  error: ApiError | null;
  /** 当前筛选条件 */
  filters: QueryParams;
  /** 当前页码，从 1 开始 */
  pageNum: number;
  /** 当前页大小 */
  pageSize: number;
  /** 用新条件重查（自动回到第 1 页） */
  search: (filters: QueryParams) => void;
  /** 保持条件不变，重新拉当前页 */
  reload: () => void;
  /** 切页 */
  setPageNum: (pageNum: number) => void;
  /** 改页大小（回到第 1 页） */
  setPageSize: (pageSize: number) => void;
}

/** # 把任意异常收敛成 ApiError */
export function toApiError(error: unknown): ApiError {
  if (error instanceof ApiError) {
    return error;
  }
  return new ApiError(error instanceof Error ? error.message : String(error));
}

/**
 * # 分页查询
 *
 * ## 参数
 * * `fetchPage` - 拉取一页数据，入参已包含 `_page` / `_size` 与业务筛选条件
 * * `defaultFilters` - 初始筛选条件
 * * `defaultPageSize` - 初始页大小
 */
export function usePagedQuery<T>(
  fetchPage: (params: QueryParams) => Promise<PageRx<T>>,
  defaultFilters: QueryParams = {},
  defaultPageSize = 10,
): PagedQueryState<T> {
  const [filters, setFilters] = useState<QueryParams>(defaultFilters);
  const [pageNum, setPageNum] = useState(1);
  const [pageSize, setPageSize] = useState(defaultPageSize);
  const [nonce, setNonce] = useState(0);
  const [data, setData] = useState<PageRx<T>>({ pageNum: 1, total: 0, list: [] });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ApiError | null>(null);

  // fetchPage 变化不应触发重查（调用方可能传内联函数），只保留最新引用
  const fetchRef = useRef(fetchPage);
  fetchRef.current = fetchPage;

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setError(null);

    fetchRef
      .current({ ...filters, _page: pageNum, _size: pageSize })
      .then((result) => {
        if (alive) {
          setData(result);
        }
      })
      .catch((reason: unknown) => {
        if (alive) {
          setError(toApiError(reason));
          setData({ pageNum, total: 0, list: [] });
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
  }, [filters, pageNum, pageSize, nonce]);

  const search = useCallback((next: QueryParams) => {
    setFilters(next);
    setPageNum(1);
    // 条件与页码都没变时也要能重查，靠 nonce 兜底
    setNonce((value) => value + 1);
  }, []);

  const reload = useCallback(() => {
    setNonce((value) => value + 1);
  }, []);

  const changePageSize = useCallback((next: number) => {
    setPageSize(next);
    setPageNum(1);
  }, []);

  return {
    data,
    loading,
    error,
    filters,
    pageNum,
    pageSize,
    search,
    reload,
    setPageNum,
    setPageSize: changePageSize,
  };
}
