/**
 * 会话存储统一交给 `lib/auth-core.ts`（六端共用的认证内核）。
 * 本文件只保留对外导出名，并负责把 dbz-web 的旧会话 key 迁移到统一 key。
 */
import {
  SESSION_EXPIRED_EVENT,
  SESSION_KEY,
  clearSession,
  isExpired,
  notifySessionExpired,
  readSession,
  saveSession,
  type AuthSession,
} from "../lib/auth-core";

/** dbz-web 旧版本使用的会话 key，只在迁移时读一次 */
const LEGACY_SESSION_KEY = "dbz.auth.session";

/**
 * 一次性迁移：旧 key 有值而统一 key 无值时复制过去，然后删掉旧 key。
 * 统一 key 已有值时以新值为准，避免旧会话覆盖本轮登录后的状态。
 */
function migrateLegacySession(): void {
  if (typeof window === "undefined") return;
  try {
    const legacy = localStorage.getItem(LEGACY_SESSION_KEY);
    if (!legacy) return;
    if (!localStorage.getItem(SESSION_KEY)) {
      localStorage.setItem(SESSION_KEY, legacy);
    }
    localStorage.removeItem(LEGACY_SESSION_KEY);
  } catch {
    /* localStorage 不可用时忽略 */
  }
}

migrateLegacySession();

export { SESSION_EXPIRED_EVENT, clearSession, isExpired, notifySessionExpired, readSession, saveSession };
export type { AuthSession };
