/**
 * # 应用骨架
 *
 * 左侧固定导航 + 顶部说明栏 + 内容区。标题与说明由路由表推导，避免每个页面各写一遍。
 */

import {
  ApiOutlined,
  ClusterOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  ImportOutlined,
  ExportOutlined,
} from '@ant-design/icons';
import { Layout, Menu, Typography } from 'antd';
import { Link, Outlet, useLocation } from 'react-router-dom';

const { Sider, Header, Content } = Layout;

/** 导航项：path 与标题、说明、图标一一对应 */
const NAV_ITEMS = [
  {
    path: '/',
    label: '总览',
    title: '总览',
    desc: '集群与连接器的整体健康度',
    icon: <DashboardOutlined />,
  },
  {
    path: '/kafka-broker',
    label: 'Kafka 集群',
    title: 'Kafka 集群（broker）',
    desc: '消息往哪个集群投：地址、认证与连通状态',
    icon: <DatabaseOutlined />,
  },
  {
    path: '/kafka-connect',
    label: 'Kafka Connect',
    title: 'Kafka Connect 集群',
    desc: '连接器实际运行的集群，地址与启停都收敛在这里',
    icon: <ClusterOutlined />,
  },
  {
    path: '/source-connector',
    label: '上游连接器',
    title: '上游连接器',
    desc: 'Debezium 从源库读取变更并写入 Kafka',
    icon: <ImportOutlined />,
  },
  {
    path: '/sink-connector',
    label: '下游连接器',
    title: '下游连接器',
    desc: '把 Kafka 中的变更写入目标存储',
    icon: <ExportOutlined />,
  },
  {
    path: '/remote-connector',
    label: '远端连接器',
    title: '远端连接器',
    desc: '直接查看与操作集群上真实运行的连接器',
    icon: <ApiOutlined />,
  },
];

export default function AppLayout() {
  const location = useLocation();
  const current =
    NAV_ITEMS.find((item) => item.path !== '/' && location.pathname.startsWith(item.path)) ??
    NAV_ITEMS[0];

  return (
    <Layout style={{ height: '100vh' }}>
      <Sider width={216} theme="light" className="dbz-sider">
        <div className="dbz-brand">
          <div className="dbz-brand-logo">dbz</div>
          <div className="dbz-brand-text">
            <span className="dbz-brand-title">Debezium 控制台</span>
            <span className="dbz-brand-sub">dbz-svr</span>
          </div>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[current.path]}
          style={{ borderInlineEnd: 'none', paddingTop: 8 }}
          items={NAV_ITEMS.map((item) => ({
            key: item.path,
            icon: item.icon,
            label: <Link to={item.path}>{item.label}</Link>,
          }))}
        />
      </Sider>

      <Layout>
        <Header className="dbz-header">
          <div>
            <div className="dbz-header-title">{current.title}</div>
            <div className="dbz-header-desc">{current.desc}</div>
          </div>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            健康检查由 jobx 定时任务触发，状态约每 60 秒刷新一次
          </Typography.Text>
        </Header>
        <Content className="dbz-content">
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
