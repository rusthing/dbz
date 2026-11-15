/**
 * # 应用入口
 */

import { App as AntdApp, ConfigProvider } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import 'dayjs/locale/zh-cn';
import App from './App';
import { AuthProvider } from './auth/AuthContext';
import './styles/global.css';

const container = document.getElementById('root');
if (!container) {
  throw new Error('找不到挂载节点 #root');
}

/**
 * 路由前缀（basename）
 *
 * 直接取 Vite 的 `BASE_URL`（就是 `vite.config.ts` 里的 `base`，形如 `/dbz-web/`），
 * 去掉末尾斜杠给 `BrowserRouter`；根路径部署时为空串，按 `/` 处理。
 * 这样「页面挂在哪个子路径」只需要改一处 `VITE_PUBLIC_BASE`。
 */
const BASENAME = import.meta.env.BASE_URL.replace(/\/+$/, '') || '/';

ReactDOM.createRoot(container).render(
  <React.StrictMode>
    <ConfigProvider
      locale={zhCN}
      button={{ autoInsertSpace: false }}
      theme={{
        token: {
          colorPrimary: '#1677ff',
          borderRadius: 6,
          fontSize: 14,
          // antd 默认的 colorTextTertiary 是 rgba(0,0,0,.45)，压在白底上只有 3.4:1，
          // 而本项目大量用它承载 11–12px 的说明文字（地址、时间、计数），读起来发虚。
          // 抬到 .56（约 4.9:1），仍是「次要」但达到正文对比度要求。
          colorTextTertiary: 'rgba(0, 0, 0, 0.56)',
        },
        components: {
          Layout: {
            headerBg: '#ffffff',
            bodyBg: '#f4f6fa',
            siderBg: '#ffffff',
          },
          Table: {
            headerBg: '#fafbfd',
          },
        },
      }}
    >
      <AntdApp>
        <BrowserRouter basename={BASENAME}>
          <AuthProvider>
            <App />
          </AuthProvider>
        </BrowserRouter>
      </AntdApp>
    </ConfigProvider>
  </React.StrictMode>,
);
