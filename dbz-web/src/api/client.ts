/**
 * # HTTP 客户端
 *
 * 把服务端的 `Ro<T>` 信封统一拆开：业务成功时直接返回 `extra`，任何失败都抛 {@link ApiError}。
 * 这样调用方只需要处理「拿到数据」和「捕获异常」两种情况。
 *
 * ## 登录态
 *
 * 所有业务请求都带 `Authorization: Bearer <token>`，身份由服务端按令牌识别；
 * `X-User-Id` 仍保留一位真实用户ID（来自登录结果），因为 `#[ctrl]` 生成的
 * add / modify / save 与删除审计直接读这个头写 `creater_id` / `updater_id`。
 * 两者都从 {@link readSession} 取，页面无需关心。
 *
 * 令牌失效（HTTP 401 或业务码 `ADM00005`）时广播会话失效，由 AuthProvider
 * 清登录态并跳登录页，不会让用户卡在「看着像登录、点什么都报错」的界面。
 */

import { authHeaders, handleUnauthorized } from '../lib/auth-core';
import type { PageRx, Ro } from './types';

/** 与 `RoResult::Success` 对应的结果码 */
const RO_SUCCESS = 1;

/** 接口前缀，生产态留空表示同源 */
const API_BASE = (import.meta.env.VITE_API_BASE ?? '').replace(/\/+$/, '');

/** # 查询参数 */
export type QueryParams = Record<string, string | number | boolean | null | undefined>;

/** # 接口异常 */
export class ApiError extends Error {
  /** 服务端返回的结果码 */
  readonly result?: number;
  /** 业务编码，如 `RTW00001` */
  readonly code?: string;
  /** 详细错误信息 */
  readonly detail?: string;
  /** HTTP 状态码 */
  readonly status?: number;

  constructor(
    message: string,
    options: { result?: number; code?: string; detail?: string; status?: number } = {},
  ) {
    super(message);
    this.name = 'ApiError';
    this.result = options.result;
    this.code = options.code;
    this.detail = options.detail;
    this.status = options.status;
  }
}

/**
 * # 拼查询串
 *
 * 空字符串、`null`、`undefined` 一律跳过——服务端 QueryDto 用 `Option` 表达「不筛选」，
 * 传空串反而会变成一个真实的等值条件。
 */
function buildQuery(params?: QueryParams): string {
  if (!params) {
    return '';
  }
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') {
      continue;
    }
    search.append(key, String(value));
  }
  const query = search.toString();
  return query ? `?${query}` : '';
}

/** # 发送请求并拆信封 */
async function request<T>(
  method: string,
  path: string,
  options: { params?: QueryParams; body?: unknown } = {},
): Promise<Ro<T>> {
  const url = `${API_BASE}${path}${buildQuery(options.params)}`;
  const headers: Record<string, string> = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...authHeaders(),
  };

  let response: Response;
  try {
    response = await fetch(url, {
      method,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new ApiError(`请求 ${method} ${path} 失败：${reason}`);
  }

  const text = await response.text();
  let payload: unknown;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = undefined;
    }
  }
  const ro = payload as Ro<T> | undefined;

  if (!response.ok) {
    if (response.status === 401) {
      // 令牌失效：清本地会话并通知 AuthProvider 回登录页。
      handleUnauthorized();
    }
    throw new ApiError(ro?.msg || `HTTP ${response.status} ${response.statusText}`, {
      status: response.status,
      result: ro?.result,
      code: ro?.code,
      detail: ro?.detail,
    });
  }
  if (!ro || typeof ro.result !== 'number') {
    throw new ApiError('响应格式不符合 Ro 约定', { status: response.status });
  }
  if (ro.result !== RO_SUCCESS) {
    // 少数认证链路在 HTTP 200 里用 ADM00005 表达令牌失效，语义与 401 一致
    if (ro.code === 'ADM00005') {
      handleUnauthorized();
    }
    throw new ApiError(ro.msg || '请求失败', {
      result: ro.result,
      code: ro.code,
      detail: ro.detail,
    });
  }
  return ro;
}

/** # GET，返回业务数据 */
export async function apiGet<T>(path: string, params?: QueryParams): Promise<T> {
  const ro = await request<T>('GET', path, { params });
  return ro.extra as T;
}

/** # GET 列表，`extra` 缺失时返回空数组 */
export async function apiGetList<T>(path: string, params?: QueryParams): Promise<T[]> {
  const ro = await request<T[]>('GET', path, { params });
  return ro.extra ?? [];
}

/** # GET 分页，`extra` 缺失时返回空页 */
export async function apiGetPage<T>(path: string, params?: QueryParams): Promise<PageRx<T>> {
  const ro = await request<PageRx<T>>('GET', path, { params });
  return ro.extra ?? { pageNum: 1, total: 0, list: [] };
}

/** # POST，返回业务数据 */
export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  const ro = await request<T>('POST', path, { body });
  return ro.extra as T;
}

/**
 * # PUT，返回业务数据
 *
 * `params` 是可选的：Kafka Connect 的 stop / resume 是「PUT 但没请求体」，
 * 目标连接器只能放在查询串里。
 */
export async function apiPut<T>(path: string, body?: unknown, params?: QueryParams): Promise<T> {
  const ro = await request<T>('PUT', path, { body, params });
  return ro.extra as T;
}

/** # DELETE，返回业务数据 */
export async function apiDelete<T>(path: string, params?: QueryParams): Promise<T> {
  const ro = await request<T>('DELETE', path, { params });
  return ro.extra as T;
}
