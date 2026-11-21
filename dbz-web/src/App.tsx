/**
 * # 路由表
 *
 * 登录页在布局外；业务页面统一包在 `RequireAuth` 里，未登录直接跳登录页并带回原地址。
 */

import { Navigate, Route, Routes } from 'react-router-dom';
import { RequireAuth } from './auth/AuthContext';
import { EmbedNavigation } from './lib/embed-bridge';
import AppLayout from './layouts/AppLayout';
import ControlPanelPage from './pages/control-panel';
import DeadLetterPage from './pages/dead-letter';
import KafkaBrokerPage from './pages/kafka-broker';
import KafkaConnectPage from './pages/kafka-connect';
import KafkaConnectorPage from './pages/kafka-connector';
import LoginPage from './pages/login';
import SinkConnectorPage from './pages/sink-connector';
import SourceConnectorPage from './pages/source-connector';

/**
 * 页面路由表：独立访问与联邦入口（MemoryRouter）共用这一份，避免两套路由定义漂移。
 */
export function AppRoutes() {
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
        {/* 控制面板是总视图；四类对象的维护页保留独立入口，旧地址仍重定向避免收藏夹 404 */}
        <Route index element={<Navigate to="/control-panel" replace />} />
        <Route path="control-panel" element={<ControlPanelPage />} />
        <Route path="connector-topology" element={<Navigate to="/control-panel" replace />} />
        <Route path="control-plane" element={<Navigate to="/control-panel" replace />} />
        <Route path="kafka-broker" element={<KafkaBrokerPage />} />
        <Route path="kafka-connect" element={<KafkaConnectPage />} />
        <Route path="source-connector" element={<SourceConnectorPage />} />
        <Route path="sink-connector" element={<SinkConnectorPage />} />
        <Route path="kafka-connector" element={<KafkaConnectorPage />} />
        <Route path="remote-connector" element={<Navigate to="/kafka-connector" replace />} />
        <Route path="dead-letter" element={<DeadLetterPage />} />
        <Route path="*" element={<Navigate to="/control-panel" replace />} />
      </Route>
    </Routes>
  );
}

export default function App() {
  return (
    <>
      <EmbedNavigation />
      <AppRoutes />
    </>
  );
}
