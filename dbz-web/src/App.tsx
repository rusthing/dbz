/**
 * # 路由表
 */

import { Navigate, Route, Routes } from 'react-router-dom';
import AppLayout from './layouts/AppLayout';
import Dashboard from './pages/Dashboard';
import KafkaConnectPage from './pages/kafka-connect';
import RemoteConnectorPage from './pages/remote-connector';
import SinkConnectorPage from './pages/sink-connector';
import SourceConnectorPage from './pages/source-connector';

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<Dashboard />} />
        <Route path="kafka-connect" element={<KafkaConnectPage />} />
        <Route path="source-connector" element={<SourceConnectorPage />} />
        <Route path="sink-connector" element={<SinkConnectorPage />} />
        <Route path="remote-connector" element={<RemoteConnectorPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
