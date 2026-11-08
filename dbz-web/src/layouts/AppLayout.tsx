/**
 * # 应用骨架
 *
 * 左侧固定导航 + 顶部说明栏 + 内容区。标题与说明由路由表推导，避免每个页面各写一遍。
 * 顶栏右侧展示当前登录用户，退出登录会清会话并回登录页。
 */

import {
  AlertOutlined,
  ApartmentOutlined,
  DownOutlined,
  LogoutOutlined,
  UnorderedListOutlined,
  UserOutlined,
} from '@ant-design/icons';
import { Avatar, Dropdown, Layout, Menu, Typography } from 'antd';
import { Link, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../auth/AuthContext';

const { Sider, Header, Content } = Layout;

/** 导航项：连接器拓扑负责管理，集群连接器负责查询集群现状，死信独立成页 */
const NAV_ITEMS = [
  {
    path: '/connector-topology',
    label: '连接器拓扑',
    title: '连接器拓扑',
    desc: 'Kafka 集群 → Connect → 连接器的拓扑图，实时状态、搜索定位与常用操作',
    icon: <ApartmentOutlined />,
  },
  {
    path: '/kafka-connector',
    label: '集群连接器',
    title: '集群连接器',
    desc: '查看 Kafka Connect 集群上实际运行的连接器（含未纳管）与已安装插件',
    icon: <UnorderedListOutlined />,
  },
  {
    path: '/dead-letter',
    label: '死信',
    title: '死信与重试',
    desc: '下游连接器失败的报文：看原因、重投、标记处理或废弃',
    icon: <AlertOutlined />,
  },
];

export default function AppLayout() {
  const location = useLocation();
  const navigate = useNavigate();
  const { session, logout } = useAuth();
  const current =
    NAV_ITEMS.find((item) => location.pathname.startsWith(item.path)) ?? NAV_ITEMS[0];

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
            <Typography.Text type="secondary" className="dbz-header-note" style={{ fontSize: 12 }}>
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
