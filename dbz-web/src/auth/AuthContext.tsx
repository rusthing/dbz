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
import { loginWithPassword, logoutRemote, startSessionWatch } from '../lib/auth-core';
import {
  SESSION_EXPIRED_EVENT,
  clearSession,
  readSession,
  type AuthSession,
} from './session';

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

  /** 空闲时按分钟检查一次本地令牌是否过期，到期统一回登录页。 */
  useEffect(() => startSessionWatch(), []);

  const login = useCallback(async (account: string, password: string) => {
    // auth-core 内部完成 SM3 摘要、登录调用与会话写入。
    const next = await loginWithPassword(account, password);
    setSession(next);
  }, []);

  const logout = useCallback(async () => {
    // 退出接口尽力而为：adm 不可用时也必须清掉本地会话。
    await logoutRemote().catch(() => undefined);
    // 免登录模式下仍保持系统账号会话；required 模式 readSession() 返回 null。
    setSession(readSession());
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
