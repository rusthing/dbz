/**
 * # 登录态存储
 *
 * 令牌与用户信息放在 localStorage 的 `dbz.auth.session`，与 adm-web 的
 * `adm.auth.session` 结构一致（只换了 key，避免两个产品共用同一份会话时
 * 互相踩踏）。服务端下发 `expireMs`（毫秒时间戳，字符串形态避免丢精度），
 * 本地读取时先判过期；过期即清会话并广播事件，由 AuthProvider 统一跳登录页。
 */

/** 会话失效广播（HTTP 401 / `ADM00005` / 本地令牌过期） */
export const SESSION_EXPIRED_EVENT = 'dbz:session-expired';

export const SESSION_KEY = 'dbz.auth.session';

/** # 登录会话 */
export interface AuthSession {
  token: string;
  tokenType: string;
  /** 令牌到期时间戳（毫秒，服务端以字符串下发） */
  expireMs: string;
  userId: string;
  /** 登录名 */
  name: string;
  nickName?: string;
  loginPswdDefault: boolean;
  loginLogId: string;
}

/**
 * 广播会话失效。
 *
 * `api/client.ts` 是纯请求层，不该直接认识 React / Router；用 DOM 事件把
 * 「令牌已作废」传给 AuthProvider，由它统一清状态并跳登录。
 */
export function notifySessionExpired(): void {
  if (typeof window !== 'undefined') {
    window.dispatchEvent(new CustomEvent(SESSION_EXPIRED_EVENT));
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

export function isExpired(session: AuthSession | null): boolean {
  if (!session) return true;
  const expire = Number(session.expireMs);
  return !Number.isFinite(expire) || expire <= Date.now();
}

export function readSession(): AuthSession | null {
  const session = readJson<AuthSession>(SESSION_KEY);
  if (!session?.token) return null;
  if (isExpired(session)) {
    clearSession();
    // 本地已经判定过期，但 React 里的 session 状态还是旧的；广播一次让界面同步登出。
    notifySessionExpired();
    return null;
  }
  return session;
}

export function saveSession(session: AuthSession): void {
  writeJson(SESSION_KEY, session);
}

export function clearSession(): void {
  remove(SESSION_KEY);
}
