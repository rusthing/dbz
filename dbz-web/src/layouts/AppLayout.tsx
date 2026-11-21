/**
 * # 应用骨架
 *
 * 左侧固定导航 + 顶部说明栏 + 内容区。标题与说明由路由表推导，避免每个页面各写一遍。
 * 顶栏右侧展示当前登录用户，退出登录会清会话并回登录页。
 */

import {
  AlertOutlined,
  ApartmentOutlined,
  ClusterOutlined,
  DatabaseOutlined,
  DownOutlined,
  ExportOutlined,
  ImportOutlined,
  LogoutOutlined,
  UserOutlined,
} from '@ant-design/icons';
import { Avatar, Dropdown, Layout, Menu, Typography } from 'antd';
import { useEffect } from 'react';
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../auth/AuthContext';
import { reportView } from '../lib/embed-bridge';

const { Sider, Header, Content } = Layout;

/** 导航项：控制面板看全局，四类对象各有独立维护页，死信独立成页（联邦入口复用它做窗口标题回报） */
export const NAV_ITEMS = [
  {
    path: '/control-panel',
    label: '控制面板',
    title: '控制面板',
    desc: 'Kafka 集群 → Connect → 连接器的泳道图，实时状态、搜索定位与常用操作',
    icon: <ApartmentOutlined />,
  },
  {
    path: '/kafka-broker',
    label: 'Kafka 集群',
    title: 'Kafka 集群（broker）',
    desc: '消息投到哪个集群：地址、认证与连通状态',
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
    path: '/dead-letter',
    label: '死信',
    title: '死信与重试',
    desc: '下游连接器失败的报文：看原因、重投、标记处理或废弃',
    icon: <AlertOutlined />,
  },
];

/**
 * # 不在导航里的页面
 *
 * 集群连接器页已从左侧导航移除，但页面与接口保留、直接 URL 仍可访问；
 * 单独维护标题，避免顶部回退成「控制面板」。
 */
export const HIDDEN_PAGES = [
  {
    path: '/kafka-connector',
    title: '集群连接器',
    desc: '查看 Kafka Connect 集群上实际运行的连接器（含未纳管）与已安装插件',
  },
];

export default function AppLayout() {
  const location = useLocation();
  const navigate = useNavigate();
  const { session, logout } = useAuth();
  const navItem = NAV_ITEMS.find((item) => location.pathname.startsWith(item.path));
  const hiddenPage = HIDDEN_PAGES.find((item) => location.pathname.startsWith(item.path));
  const current = navItem ?? hiddenPage ?? NAV_ITEMS[0];

  useEffect(() => {
    reportView(current.title, location.pathname);
  }, [current.title, location.pathname]);

  const handleLogout = async () => {
    await logout();
    navigate('/login', { replace: true });
  };

  return (
    <Layout style={{ height: '100vh' }}>
      {/* 窄屏（< lg）自动收起，给内容区留出可读宽度；trigger 保留，随时可展开 */}
      <Sider
        width={216}
        theme="light"
        className="dbz-sider"
        breakpoint="lg"
        collapsedWidth={0}
      >
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
          <div className="dbz-header-right">
            <Typography.Text type="secondary" className="dbz-header-note" style={{ fontSize: 12.5 }}>
              健康检查由 jobx 定时任务触发，状态约每 60 秒刷新一次
            </Typography.Text>
            {session && (
              <Dropdown
                menu={{
                  items: [
                    { key: 'logout', icon: <LogoutOutlined />, label: '退出登录' },
                  ],
                  onClick: ({ key }) => {
                    if (key === 'logout') {
                      void handleLogout();
                    }
                  },
                }}
                placement="bottomRight"
                trigger={['click']}
              >
                <span className="dbz-header-user" data-header-user>
                  <Avatar size={24} icon={<UserOutlined />} />
                  <span>{session.nickName || session.name}</span>
                  <DownOutlined style={{ fontSize: 10 }} />
                </span>
              </Dropdown>
            )}
          </div>
        </Header>
        <Content className="dbz-content">
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
