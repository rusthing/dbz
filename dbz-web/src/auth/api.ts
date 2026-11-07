/**
 * # 认证接口
 *
 * dbz 复用 adm-rs 的用户体系：登录、登出都直接调 adm 的认证接口，
 * 路径前缀由 `VITE_AUTH_BASE` 决定（生产是网关的 `/admrs-svr`，开发态走 Vite 代理）。
 *
 * 登录请求体 `{ account, loginPswd }`，`loginPswd` 是前端做过一次 SM3 的
 * 64 位小写十六进制；服务端返回的令牌放在 `extra.token`。
 */

import { ApiError } from '../api/client';
import { readSession } from './session';

/** adm 认证接口前缀；生产由网关剥掉 `/admrs-svr` */
const AUTH_BASE = (import.meta.env.VITE_AUTH_BASE ?? '').replace(/\/+$/, '');

const RO_SUCCESS = 1;

/** # adm 登录结果（`AdmLoginVo`） */
export interface AdmLoginVo {
  token: string;
  tokenType: string;
  expireMs: string;
  userId: string;
  name: string;
  nickName?: string;
  loginPswdDefault: boolean;
  loginLogId: string;
}

/** # 登录失败的业务编码→可读提示 */
export const LOGIN_CODE_TEXT: Record<string, string> = {
  ADM00001: '账号或口令不正确',
  ADM00002: '账号已被锁定，请稍后再试或联系管理员',
  ADM00003: '账号已停用',
  ADM00004: '账号已过期',
  ADM00005: '令牌无效，请重新登录',
  ADM00006: '该登录标识命中多个账号，请改用登录名',
};

interface Ro<T> {
  result: number;
  msg?: string;
  code?: string;
  detail?: string;
  extra?: T;
}

/** 发送认证请求并拆 Ro 信封 */
async function authRequest<T>(path: string, init: RequestInit): Promise<T | undefined> {
  let response: Response;
  try {
    response = await fetch(`${AUTH_BASE}${path}`, init);
  } catch (error) {
    const reason = error instanceof Error ? error.message : '网络错误';
    throw new ApiError(`无法连接认证服务：${reason}`, { status: 0 });
  }

  const text = await response.text();
  let payload: Ro<T> | undefined;
  if (text) {
    try {
      payload = JSON.parse(text) as Ro<T>;
    } catch {
      throw new ApiError(`认证服务返回的不是 JSON（HTTP ${response.status}）`, {
        status: response.status,
        detail: text.slice(0, 200),
      });
    }
  }

  if (!response.ok) {
    throw new ApiError(payload?.msg || `认证服务请求失败（HTTP ${response.status}）`, {
      status: response.status,
      result: payload?.result,
      code: payload?.code,
      detail: payload?.detail,
    });
  }
  if (!payload) {
    throw new ApiError('认证服务返回空响应', { status: response.status });
  }
  if (payload.result !== RO_SUCCESS) {
    throw new ApiError(payload.msg || '认证失败', {
      status: response.status,
      result: payload.result,
      code: payload.code,
      detail: payload.detail,
    });
  }
  return payload.extra;
}

/** 登录：口令已由调用方做过一次 SM3 */
export async function loginApi(input: {
  account: string;
  loginPswd: string;
}): Promise<AdmLoginVo> {
  const extra = await authRequest<AdmLoginVo>('/adm/auth/login', {
    method: 'POST',
    headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  if (!extra?.token) {
    throw new ApiError('登录接口未返回令牌');
  }
  return extra;
}

/** 登出：令牌已在服务端作废也无妨，本地会话由调用方清理 */
export async function logoutApi(): Promise<void> {
  const session = readSession();
  await authRequest('/adm/auth/logout', {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      ...(session ? { Authorization: `Bearer ${session.token}` } : {}),
    },
  });
}
