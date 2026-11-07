/**
 * # 登录态上下文
 *
 * 负责三件事：
 * - 从 localStorage 恢复会话；令牌过期时清掉并广播；
 * - 登录（前端先做 SM3，再调 adm 登录接口）与登出；
 * - 监听会话失效事件，统一清状态并带原地址跳登录页，避免业务页停在空白页。
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router-dom';
import { loginApi, logoutApi } from './api';
import {
  SESSION_EXPIRED_EVENT,
  clearSession,
  readSession,
  saveSession,
  type AuthSession,
} from './session';
import { sm3 } from '../lib/sm3';

interface AuthContextValue {
  session: AuthSession | null;
  login: (account: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  const location = useLocation();
  const [session, setSession] = useState<AuthSession | null>(() => readSession());

  /** 401 / `ADM00005` / 本地令牌过期：清会话并回登录页，原地址带上便于登录后返回 */
  useEffect(() => {
    const onExpired = () => {
      clearSession();
      setSession(null);
      if (location.pathname !== '/login') {
        const returnUrl = `${location.pathname}${location.search}`;
        navigate(`/login?returnUrl=${encodeURIComponent(returnUrl)}`, { replace: true });
      }
    };
    window.addEventListener(SESSION_EXPIRED_EVENT, onExpired);
    return () => window.removeEventListener(SESSION_EXPIRED_EVENT, onExpired);
  }, [navigate, location.pathname, location.search]);

  const login = useCallback(async (account: string, password: string) => {
    const result = await loginApi({ account: account.trim(), loginPswd: sm3(password) });
    const next: AuthSession = {
      token: result.token,
      tokenType: result.tokenType,
      expireMs: result.expireMs,
      userId: result.userId,
      name: result.name,
      nickName: result.nickName,
      loginPswdDefault: result.loginPswdDefault,
      loginLogId: result.loginLogId,
    };
    saveSession(next);
    setSession(next);
  }, []);

  const logout = useCallback(async () => {
    try {
      await logoutApi();
    } catch {
      /* 令牌已失效时登出接口可能返回业务错误；本地会话仍需清掉 */
    }
    clearSession();
    setSession(null);
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({ session, login, logout }),
    [session, login, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth 必须在 AuthProvider 内使用');
  return context;
}

/** 未登录 → 登录页，并把当前地址带回去 */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { session } = useAuth();
  const location = useLocation();
  if (!session) {
    const returnUrl = `${location.pathname}${location.search}`;
    return <Navigate to={`/login?returnUrl=${encodeURIComponent(returnUrl)}`} replace />;
  }
  return <>{children}</>;
}
