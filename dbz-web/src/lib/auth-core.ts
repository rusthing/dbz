/*!
 * rusthing-auth-core v1.0.0
 *
 * 六端共用的无 UI 认证内核。各仓库按原样复制本文件与 `sm3.ts`，不要各自改逻辑；
 * 升级时整体替换，并同步修改上面的版本号。
 *
 * 约定：
 * 1. 是否需要登录由**构建期**的 `VITE_AUTH_MODE` 决定，缺省按 `required` 处理；
 *    `disabled` 只在开发环境使用，请求固定带 `X-User-Id: 0`（系统账号），不带令牌。
 * 2. 六端使用同一个 localStorage key，同一浏览器下登录态可以互相复用。
 * 3. 登录/登出/过期广播的行为集中在这里，各端只负责登录页外观与路由承接。
 */

import { sm3 } from "./sm3";

/** 认证模式：缺省按 required（生产环境）处理，避免漏配就变成免登录 */
export type AuthMode = "required" | "disabled";

const rawMode = String(import.meta.env.VITE_AUTH_MODE ?? "required").trim().toLowerCase();

/** 当前构建的认证模式 */
export const AUTH_MODE: AuthMode = rawMode === "disabled" ? "disabled" : "required";

/** 是否必须登录 */
export const AUTH_REQUIRED = AUTH_MODE === "required";

/** 统一会话 key */
export const SESSION_KEY = "adm.auth.session";

/** 会话失效广播事件名 */
export const SESSION_EXPIRED_EVENT = "rusthing:session-expired";

/** 开发免登录模式下的固定系统账号 */
export const DEV_USER_ID = "0";

/** 认证接口前缀；由各端 `.env.*` 的 `VITE_AUTH_BASE` 指定 */
export const AUTH_BASE = String(import.meta.env.VITE_AUTH_BASE ?? "")
  .trim()
  .replace(/\/+$/, "");

/** 登录会话（与服务端 `POST /adm/auth/login` 返回字段一致） */
export interface AuthSession {
  token: string;
  tokenType: string;
  expireMs: string;
  userId: string;
  name: string;
  nickName?: string;
  loginPswdDefault: boolean;
  loginLogId: string;
}

/** 开发免登录会话：不落盘，只用于让界面按「已登录」渲染 */
const DEV_SESSION: AuthSession = {
  token: "",
  tokenType: "",
  expireMs: "9999999999999",
  userId: DEV_USER_ID,
  name: "system",
  nickName: "系统账号",
  loginPswdDefault: false,
  loginLogId: "0",
};

/** 登录失败 */
export class AuthError extends Error {
  /** 服务端业务编码 */
  readonly code?: string;

  constructor(message: string, code?: string) {
    super(message);
    this.name = "AuthError";
    this.code = code;
  }
}

function readJson<T>(key: string): T | null {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}

function writeJson(key: string, value: unknown): void {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* localStorage 被禁用时不影响本次会话（刷新后会要求重新登录） */
  }
}

function remove(key: string): void {
  try {
    localStorage.removeItem(key);
  } catch {
    /* ignore */
  }
}

/** 本地判定会话是否过期；免登录模式下永远视为有效 */
export function isExpired(session: AuthSession | null): boolean {
  if (!AUTH_REQUIRED) return false;
  if (!session) return true;
  const expire = Number(session.expireMs);
  return !Number.isFinite(expire) || expire <= Date.now();
}

/** 读取会话；免登录模式返回固定的系统账号会话 */
export function readSession(): AuthSession | null {
  if (!AUTH_REQUIRED) return DEV_SESSION;
  const session = readJson<AuthSession>(SESSION_KEY);
  if (!session?.token) return null;
  if (isExpired(session)) {
    clearSession();
    notifySessionExpired();
    return null;
  }
  return session;
}

/** 保存会话；免登录模式不落盘 */
export function saveSession(session: AuthSession): void {
  if (!AUTH_REQUIRED) return;
  writeJson(SESSION_KEY, session);
}

/** 清除会话；免登录模式无需清除（始终是系统账号） */
export function clearSession(): void {
  if (!AUTH_REQUIRED) return;
  remove(SESSION_KEY);
}

/**
 * 后端返回 401 / `ADM00005` 时统一调用。
 *
 * 与 {@link clearSession} 的区别是**不区分认证模式**：即使构建成 disabled，
 * 后端仍然可能拒绝请求；此时必须真的把本地残留会话清掉并通知界面回登录页，
 * 不能让页面停在「看起来已登录、点什么都报错」的状态。
 */
export function handleUnauthorized(): void {
  remove(SESSION_KEY);
  notifySessionExpired();
}

/**
 * 请求头：登录后带 Bearer + 当前用户；免登录模式固定 `X-User-Id: 0`。
 * 各端请求层统一从这里取，避免出现「有的接口带 token、有的不带」。
 */
export function authHeaders(): Record<string, string> {
  if (!AUTH_REQUIRED) {
    return { "X-User-Id": DEV_USER_ID };
  }
  const session = readSession();
  if (!session?.token) return {};
  return {
    Authorization: `Bearer ${session.token}`,
    "X-User-Id": String(session.userId),
  };
}

/** 订阅会话失效（HTTP 401 或本地过期）；返回取消订阅函数 */
export function subscribeSessionExpired(handler: () => void): () => void {
  if (typeof window === "undefined") return () => {};
  window.addEventListener(SESSION_EXPIRED_EVENT, handler);
  return () => window.removeEventListener(SESSION_EXPIRED_EVENT, handler);
}

/** 广播会话失效；disabled 也要广播，交给界面清状态并弹登录页 */
export function notifySessionExpired(): void {
  if (typeof window === "undefined") return;
  window.dispatchEvent(new Event(SESSION_EXPIRED_EVENT));
}

/**
 * 读取当前用户与权限（`GET /adm/auth/me`）。
 *
 * 各端登录后可以用它校验会话，或交给上层缓存菜单/权限；失败时按 401 处理。
 */
export async function fetchMe<T>(): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${AUTH_BASE}/adm/auth/me`, {
      method: "GET",
      headers: { Accept: "application/json", ...authHeaders() },
    });
  } catch (error) {
    throw new AuthError(`读取用户信息失败：${error instanceof Error ? error.message : String(error)}`);
  }

  const text = await response.text();
  let payload: Ro<T> | undefined;
  try {
    payload = text ? (JSON.parse(text) as Ro<T>) : undefined;
  } catch {
    payload = undefined;
  }
  if (response.status === 401 || payload?.code === "ADM00005") {
    handleUnauthorized();
    throw new AuthError("登录状态已失效，请重新登录", payload?.code);
  }
  if (!response.ok || !payload || payload.result !== 1) {
    throw new AuthError(payload?.msg || `HTTP ${response.status}`, payload?.code);
  }
  return payload.extra as T;
}

/**
 * 过期定时检查：长时间停留在页面时，令牌到期也能及时回登录页。
 *
 * disabled 模式没有真实令牌，直接返回空停止函数。
 */
export function startSessionWatch(intervalMs = 60_000): () => void {
  if (!AUTH_REQUIRED || typeof window === "undefined") return () => {};
  const timer = window.setInterval(() => {
    const session = readJson<AuthSession>(SESSION_KEY);
    if (session && isExpired(session)) {
      remove(SESSION_KEY);
      notifySessionExpired();
    }
  }, intervalMs);
  return () => window.clearInterval(timer);
}

interface Ro<T> {
  result?: number;
  msg?: string;
  code?: string;
  detail?: string;
  extra?: T;
}

interface LoginExtra {
  token: string;
  tokenType: string;
  expireMs: number | string;
  userId: number | string;
  name: string;
  nickName: string;
  loginPswdDefault: boolean;
  loginLogId: string;
}

/** 登录失败时按业务码给出可读文案；未知码回落到服务端 msg */
export function loginErrorText(error: Ro<unknown>): string {
  switch (error.code) {
    case "ADM00001":
      return "账号或密码错误";
    case "ADM00002":
      return "账号已被锁定，请稍后再试";
    case "ADM00003":
      return "账号已被禁用";
    case "ADM00004":
      return "账号已过期";
    default:
      return error.msg || "登录失败";
  }
}

/**
 * 账号密码登录（SM3 两段式：前端只做一次 SM3，服务端再套盐）。
 * 成功后落盘会话并返回；失败抛 {@link AuthError}。
 */
export async function loginWithPassword(account: string, password: string): Promise<AuthSession> {
  if (!AUTH_REQUIRED) {
    return DEV_SESSION;
  }
  let response: Response;
  try {
    response = await fetch(`${AUTH_BASE}/adm/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ account: account.trim(), loginPswd: sm3(password) }),
    });
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new AuthError(`登录请求失败：${reason}`);
  }

  const text = await response.text();
  let payload: Ro<LoginExtra> | undefined;
  try {
    payload = text ? (JSON.parse(text) as Ro<LoginExtra>) : undefined;
  } catch {
    payload = undefined;
  }
  if (!response.ok) {
    throw new AuthError(payload?.msg || `HTTP ${response.status}`, payload?.code);
  }
  if (!payload || payload.result !== 1 || !payload.extra) {
    throw new AuthError(loginErrorText(payload ?? {}), payload?.code);
  }

  const session: AuthSession = {
    token: payload.extra.token,
    tokenType: payload.extra.tokenType,
    expireMs: String(payload.extra.expireMs),
    userId: String(payload.extra.userId),
    name: payload.extra.name,
    nickName: payload.extra.nickName,
    loginPswdDefault: payload.extra.loginPswdDefault,
    loginLogId: payload.extra.loginLogId,
  };
  saveSession(session);
  return session;
}

/** 登出：先通知服务端作废令牌，再清本地；服务端失败也保证本地已登出 */
export async function logoutRemote(): Promise<void> {
  if (!AUTH_REQUIRED) {
    clearSession();
    return;
  }
  const session = readJson<AuthSession>(SESSION_KEY);
  try {
    if (session?.token) {
      await fetch(`${AUTH_BASE}/adm/auth/logout`, {
        method: "POST",
        headers: { ...authHeaders(), "Content-Type": "application/json" },
      });
    }
  } catch {
    /* 网络或服务端异常不阻塞本地登出 */
  } finally {
    clearSession();
  }
}
