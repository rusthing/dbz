/**
 * # 应用入口（独立访问）
 *
 * 联邦入口在 src/federation/DbzFederatedApp.tsx，两者共用 AppRoutes 与主题配置。
 */

import { App as AntdApp, ConfigProvider } from 'antd';
import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import 'dayjs/locale/zh-cn';
import App from './App';
import { AuthProvider } from './auth/AuthContext';
import { initEmbed, useEmbedTheme } from './lib/embed-bridge';
import { antdConfig } from './lib/theme-config';
import './styles/global.css';

const container = document.getElementById('root');
if (!container) {
  throw new Error('找不到挂载节点 #root');
}

/**
 * 路由前缀（basename）
 *
 * 直接取 Vite 的 `BASE_URL`（`vite.config.ts` 的 `base`，形如 `/dbz-web/`），
 * 去掉末尾斜杠给 `BrowserRouter`；根路径部署时为空串。
 */
const BASENAME = import.meta.env.BASE_URL.replace(/\/+$/, '') || '/';

/** 嵌入态由宿主下发主题；独立访问时保持原有视觉 */
function Root() {
  const embeddedTheme = useEmbedTheme();
  const config = antdConfig({ dark: embeddedTheme === 'dark' });
  return (
    <React.StrictMode>
      <ConfigProvider {...config}>
        <AntdApp>
          <BrowserRouter basename={BASENAME}>
            <AuthProvider>
              <App />
            </AuthProvider>
          </BrowserRouter>
        </AntdApp>
      </ConfigProvider>
    </React.StrictMode>
  );
}

initEmbed('dbz');

ReactDOM.createRoot(container).render(<Root />);
