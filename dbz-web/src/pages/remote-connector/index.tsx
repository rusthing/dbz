/**
 * # 远端连接器
 *
 * 这一页操作的是**集群上真实运行的连接器**（Kafka Connect REST API），不是库里的记录：
 * 因此能看到未经 dbz 纳管的连接器（有人在集群上手工建的），也能直接重启它们。
 *
 * 定位目标集群用 `?kafkaConnectId=`：从「Kafka Connect」页点「连接器」跳过来时自动带上。
 */

import { App as AntdApp, Alert, Button, Card, Empty, Popconfirm, Select, Table, Tabs, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ReloadOutlined } from '@ant-design/icons';
import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import type {
  KafkaConnectorPluginVo,
  KafkaConnectorStatus,
  KafkaConnectorTaskVo,
  KafkaConnectorVo,
} from '../../api/types';
import StatusTag from '../../components/StatusTag';
import { kafkaConnectorStatusDic } from '../../dic';
import { useAsync } from '../../hooks/useAsync';
import { useClusterOptions } from '../../hooks/useOptions';
import { errorMessage, orDash } from '../../utils/format';

export default function RemoteConnectorPage() {
  const cluster = useClusterOptions();
  const [searchParams, setSearchParams] = useSearchParams();
  const raw = searchParams.get('kafkaConnectId');
  const selectedId = raw ? Number(raw) : undefined;

  return (
    <Card variant="borderless">
      <div className="dbz-toolbar">
        <Select
          showSearch
          optionFilterProp="label"
          placeholder="选择 Kafka Connect 集群"
          style={{ width: 320 }}
          loading={cluster.loading}
          options={cluster.options}
          value={selectedId}
          onChange={(value) => setSearchParams({ kafkaConnectId: String(value) })}
        />
        <Button icon={<ReloadOutlined />} onClick={cluster.reload}>
          刷新集群
        </Button>
        <span className="dbz-toolbar-spacer" />
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          这里的操作直接发往 Kafka Connect，不落库；重启是异步的，状态请稍后重新查询
        </Typography.Text>
      </div>

      {!selectedId ? (
        <Empty
          description="请选择一台 Kafka Connect 集群，查看其上真实运行的连接器与插件"
          style={{ padding: '48px 0' }}
        />
      ) : (
        <Tabs
          items={[
            {
              key: 'connectors',
              label: '连接器',
              children: <ConnectorsPanel kafkaConnectId={selectedId} />,
            },
            {
              key: 'plugins',
              label: '插件',
              children: <PluginsPanel kafkaConnectId={selectedId} />,
            },
          ]}
        />
      )}
    </Card>
  );
}

/** # 连接器列表 */
function ConnectorsPanel({ kafkaConnectId }: { kafkaConnectId: number }) {
  const { message } = AntdApp.useApp();
  const { data, loading, error, reload } = useAsync<KafkaConnectorVo[]>(
    () => kafkaConnectorApi.list(kafkaConnectId),
    [kafkaConnectId],
    [],
  );
  const [restarting, setRestarting] = useState<string | null>(null);

  const handleRestart = async (connectorName: string) => {
    setRestarting(connectorName);
    try {
      await kafkaConnectorApi.restart({
        kafkaConnectId,
        connectorName,
        includeTasks: true,
        onlyFailed: false,
      });
      message.success(`已提交重启：${connectorName}`);
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setRestarting(null);
    }
  };

  const columns: ColumnsType<KafkaConnectorVo> = [
    {
      title: '连接器名称',
      dataIndex: 'name',
      width: 240,
      render: (name: string) => <span className="dbz-mono">{name}</span>,
    },
    {
      title: '类型',
      dataIndex: 'type',
      width: 90,
      render: (type: string) =>
        type === 'source' ? (
          <Tag color="blue">source</Tag>
        ) : type === 'sink' ? (
          <Tag color="purple">sink</Tag>
        ) : (
          <Tag>{orDash(type)}</Tag>
        ),
    },
    {
      title: '状态',
      dataIndex: 'state',
      width: 110,
      render: (state: KafkaConnectorStatus) => (
        <StatusTag dic={kafkaConnectorStatusDic} value={state} />
      ),
    },
    {
      title: '类名',
      dataIndex: 'connectorClass',
      ellipsis: true,
      render: (value: string) => <span className="dbz-mono">{orDash(value)}</span>,
    },
    {
      title: '操作',
      key: 'actions',
      width: 120,
      render: (_, record) => (
        <Popconfirm
          title={`重启连接器「${record.name}」？`}
          description="将连同其任务一起重启，异步生效。"
          okText="重启"
          cancelText="取消"
          onConfirm={() => handleRestart(record.name)}
        >
          <Button type="link" size="small" loading={restarting === record.name}>
            重启
          </Button>
        </Popconfirm>
      ),
    },
  ];

  if (error) {
    return (
      <Alert
        type="error"
        showIcon
        message="查询集群上的连接器失败"
        description={error.message}
        action={
          <Button size="small" onClick={reload}>
            重试
          </Button>
        }
      />
    );
  }

  return (
    <>
      <div className="dbz-toolbar">
        <Typography.Text type="secondary">
          共 {data.length} 个连接器，展开行可查看任务并单独重启失败任务
        </Typography.Text>
        <span className="dbz-toolbar-spacer" />
        <Button size="small" icon={<ReloadOutlined />} onClick={reload}>
          刷新
        </Button>
      </div>
      <Table<KafkaConnectorVo>
        rowKey="name"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data}
        pagination={false}
        expandable={{
          rowExpandable: (record) => record.tasks.length > 0,
          expandedRowRender: (record) => (
            <TaskTable kafkaConnectId={kafkaConnectId} connectorName={record.name} tasks={record.tasks} />
          ),
        }}
      />
    </>
  );
}

/** # 连接器任务表 */
function TaskTable({
  kafkaConnectId,
  connectorName,
  tasks,
}: {
  kafkaConnectId: number;
  connectorName: string;
  tasks: KafkaConnectorTaskVo[];
}) {
  const { message } = AntdApp.useApp();
  const [restarting, setRestarting] = useState<number | null>(null);

  const handleRestart = async (taskId: number) => {
    setRestarting(taskId);
    try {
      await kafkaConnectorApi.restartTask({ kafkaConnectId, connectorName, taskId });
      message.success(`已提交重启任务：${connectorName}#${taskId}`);
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setRestarting(null);
    }
  };

  const columns: ColumnsType<KafkaConnectorTaskVo> = [
    { title: '任务ID', dataIndex: 'id', width: 90 },
    {
      title: '状态',
      dataIndex: 'state',
      width: 110,
      render: (state: KafkaConnectorStatus) => (
        <StatusTag dic={kafkaConnectorStatusDic} value={state} />
      ),
    },
    {
      title: 'Worker',
      dataIndex: 'workerId',
      width: 260,
      render: (workerId?: string | null) => <span className="dbz-mono">{orDash(workerId)}</span>,
    },
    {
      title: '异常',
      dataIndex: 'trace',
      render: (trace?: string | null) =>
        trace ? (
          <Typography.Text type="danger" ellipsis={{ tooltip: trace }} style={{ maxWidth: 420 }}>
            {trace.split('\n')[0]}
          </Typography.Text>
        ) : (
          '-'
        ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 100,
      render: (_, record) => (
        <Popconfirm
          title={`重启任务 #${record.id}？`}
          okText="重启"
          cancelText="取消"
          onConfirm={() => handleRestart(record.id)}
        >
          <Button type="link" size="small" loading={restarting === record.id}>
            重启
          </Button>
        </Popconfirm>
      ),
    },
  ];

  return (
    <Table<KafkaConnectorTaskVo>
      rowKey="id"
      size="small"
      columns={columns}
      dataSource={tasks}
      pagination={false}
    />
  );
}

/** # 插件列表 */
function PluginsPanel({ kafkaConnectId }: { kafkaConnectId: number }) {
  const { data, loading, error, reload } = useAsync<KafkaConnectorPluginVo[]>(
    () => kafkaConnectorApi.plugins(kafkaConnectId),
    [kafkaConnectId],
    [],
  );

  const columns: ColumnsType<KafkaConnectorPluginVo> = [
    {
      title: '类名',
      dataIndex: 'class',
      render: (value: string) => <span className="dbz-mono">{value}</span>,
    },
    {
      title: '类型',
      dataIndex: 'type',
      width: 100,
      render: (type: string) =>
        type === 'source' ? (
          <Tag color="blue">source</Tag>
        ) : type === 'sink' ? (
          <Tag color="purple">sink</Tag>
        ) : (
          <Tag>{orDash(type)}</Tag>
        ),
    },
    {
      title: '版本',
      dataIndex: 'version',
      width: 160,
      render: (version?: string | null) => <span className="dbz-mono">{orDash(version)}</span>,
    },
  ];

  if (error) {
    return (
      <Alert
        type="error"
        showIcon
        message="查询连接器插件失败"
        description={error.message}
        action={
          <Button size="small" onClick={reload}>
            重试
          </Button>
        }
      />
    );
  }

  return (
    <>
      <div className="dbz-toolbar">
        <Typography.Text type="secondary">
          新建连接器前可用它确认这台集群装了对应方向的 Debezium 插件
        </Typography.Text>
        <span className="dbz-toolbar-spacer" />
        <Button size="small" icon={<ReloadOutlined />} onClick={reload}>
          刷新
        </Button>
      </div>
      <Table<KafkaConnectorPluginVo>
        rowKey="class"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data}
        pagination={false}
      />
    </>
  );
}
