/**
 * # 路由表
 *
 * 登录页在布局外；业务页面统一包在 `RequireAuth` 里，未登录直接跳登录页并带回原地址。
 */

import { Navigate, Route, Routes } from 'react-router-dom';
import { RequireAuth } from './auth/AuthContext';
import AppLayout from './layouts/AppLayout';
import ControlPanelPage from './pages/control-panel';
import DeadLetterPage from './pages/dead-letter';
import KafkaConnectorPage from './pages/kafka-connector';
import LoginPage from './pages/login';

export default function App() {
  return (
    <Routes>
      <Route path="login" element={<LoginPage />} />

      <Route
        element={
          <RequireAuth>
            <AppLayout />
          </RequireAuth>
        }
      >
        {/* 控制面板承接原先的列表页；旧地址保留重定向，避免收藏夹直接 404 */}
        <Route index element={<Navigate to="/control-panel" replace />} />
        <Route path="control-panel" element={<ControlPanelPage />} />
        <Route path="connector-topology" element={<Navigate to="/control-panel" replace />} />
        <Route path="control-plane" element={<Navigate to="/control-panel" replace />} />
        <Route path="kafka-connector" element={<KafkaConnectorPage />} />
        <Route path="kafka-broker" element={<Navigate to="/control-panel" replace />} />
        <Route path="kafka-connect" element={<Navigate to="/control-panel" replace />} />
        <Route path="source-connector" element={<Navigate to="/control-panel" replace />} />
        <Route path="sink-connector" element={<Navigate to="/control-panel" replace />} />
        <Route path="dead-letter" element={<DeadLetterPage />} />
        <Route path="*" element={<Navigate to="/control-panel" replace />} />
      </Route>
    </Routes>
  );
}
