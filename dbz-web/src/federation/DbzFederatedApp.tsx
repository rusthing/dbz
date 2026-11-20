/**
 * 联邦入口：给 uiam 桌面用的页面级模块。
 *
 * MemoryRouter 把路由关在窗口内部；页面清单复用 App.tsx 的 AppRoutes，标题映射复用
 * AppLayout 的 NAV_ITEMS/HIDDEN_PAGES，主题通过 props 传入。AuthProvider 在这里
 * 单独包一层（独立入口在 main.tsx 里包），两者共用同一套会话上下文。
 */
import { useEffect } from 'react';
import { App as AntdApp, ConfigProvider } from 'antd';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { AppRoutes } from '../App';
import { AuthProvider } from '../auth/AuthContext';
import { HIDDEN_PAGES, NAV_ITEMS } from '../layouts/AppLayout';
import { antdConfig } from '../lib/theme-config';
import '../styles/global.css';
import './federation.css';

export interface DbzFederatedProps {
  theme?: 'light' | 'dark';
  initialPath?: string;
  onRoute?: (path: string, title: string) => void;
}

function titleOf(pathname: string): string {
  const hit =
    NAV_ITEMS.find((item) => pathname.startsWith(item.path)) ??
    HIDDEN_PAGES.find((item) => pathname.startsWith(item.path));
  return hit ? hit.title : '控制面板';
}

function RouteReporter({ onRoute }: { onRoute?: DbzFederatedProps['onRoute'] }) {
  const { pathname } = useLocation();
  useEffect(() => {
    onRoute?.(pathname, titleOf(pathname));
  }, [pathname, onRoute]);
  return null;
}

export default function DbzFederatedApp({
  theme = 'light',
  initialPath = '/control-panel',
  onRoute,
}: DbzFederatedProps) {
  const entry = initialPath.startsWith('/') ? initialPath : '/control-panel';
  const config = antdConfig({ dark: theme === 'dark' });
  return (
    <div className="dbz-federated" data-theme={theme}>
      <ConfigProvider {...config}>
        <AntdApp>
          <MemoryRouter initialEntries={[entry]}>
            <AuthProvider>
              <RouteReporter onRoute={onRoute} />
              <AppRoutes />
            </AuthProvider>
          </MemoryRouter>
        </AntdApp>
      </ConfigProvider>
    </div>
  );
}
