/**
 * # 集群连接器
 *
 * 查询一台 Kafka Connect 集群上**实际运行**的连接器与已安装插件。这一页只读：
 * 数据直接来自 Kafka Connect REST API，不读写 dbz 库。
 *
 * 与上游/下游连接器页的区别：那两页维护的是 dbz 纳管的记录；这一页看到的是集群现状，
 * 因此能发现有人在集群上手工创建、dbz 库里没有记录的连接器。
 *
 * 连接器与插件按 Tab 分开：插件列表用来确认集群是否装了对应方向的 Debezium 插件，
 * 新建连接器前可以先在这里核对。
 */

import { ReloadOutlined } from '@ant-design/icons';
import {
  Alert,
  Button,
  Card,
  Empty,
  Select,
  Table,
  Tabs,
  Tag,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useSearchParams } from 'react-router-dom';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import type {
  Id,
  KafkaConnectorPluginVo,
  KafkaConnectorStatus,
  KafkaConnectorTaskVo,
  KafkaConnectorVo,
} from '../../api/types';
import StatusTag, { type StatusDetail } from '../../components/StatusTag';
import { kafkaConnectorStatusDic } from '../../dic';
import { useAsync } from '../../hooks/useAsync';
import { useClusterOptions } from '../../hooks/useOptions';
import { EMPTY_TEXT, errorMessage, orDash } from '../../utils/format';

/** 任务的异常状态：FAILED 与 UNASSIGNED（与后端 `is_task_abnormal` 同一套判定） */
const ABNORMAL_TASK_STATES: KafkaConnectorStatus[] = [1, 3];

/**
 * # 连接器的实时运行详情
 *
 * 一次 `GET /connectors?expand=status` 就把连接器与任务的 trace 都带回来了，
 * 因此悬停详情不需要额外请求。失败原因优先取连接器自己的 trace（它没起来），
 * 其次取第一个失败任务的（它起来了但挂了）。
 */
function connectorDetail(record: KafkaConnectorVo): StatusDetail {
  const abnormal = record.tasks.filter((task) => ABNORMAL_TASK_STATES.includes(task.state));
  const taskText =
    record.tasks.length === 0
      ? '无任务（连接器没分配任务）'
      : abnormal.length === 0
        ? `${record.tasks.length} 个，全部正常`
        : `${record.tasks.length} 个，异常 ${abnormal.length} 个（#${abnormal
            .map((task) => task.id)
            .join(', #')}）`;

  const taskTrace = abnormal
    .map((task) => task.trace?.trim())
    .find((trace): trace is string => Boolean(trace));

  return {
    summary: `运行详情 · ${record.name}`,
    rows: [
      {
        label: '类名',
        value: <span className="dbz-mono">{orDash(record.connectorClass)}</span>,
      },
      { label: 'Worker', value: <span className="dbz-mono">{orDash(record.workerId)}</span> },
      { label: '任务', value: taskText },
      ...(record.state === 5
        ? [{ label: '说明', value: '已停止：配置与位点保留，恢复后从原处继续' }]
        : []),
      ...(record.state === 2
        ? [{ label: '说明', value: '已暂停：人工或集群侧暂停，dbz 不会自动重启它' }]
        : []),
    ],
    reason: record.trace?.trim() || taskTrace || null,
  };
}

export default function KafkaConnectorPage() {
  const cluster = useClusterOptions();
  const [searchParams, setSearchParams] = useSearchParams();
  const raw = searchParams.get('kafkaConnectId');
  // ID 是 u64 字符串，不能 Number()：19 位雪花 ID 转数字会丢精度，查到的就是别的集群
  const selectedId = raw ?? undefined;

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
          notFoundContent={
            cluster.error
              ? '集群列表加载失败，请点「刷新集群」重试'
              : cluster.loading
                ? '加载中…'
                : '暂无集群'
          }
          value={selectedId}
          onChange={(value) => setSearchParams({ kafkaConnectId: String(value) })}
        />
        <Button icon={<ReloadOutlined />} onClick={cluster.reload}>
          刷新集群
        </Button>
        <span className="dbz-toolbar-spacer" />
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          这里只查询集群现状，不改集群、也不改库；连接器状态是当前时刻的实时结果。
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

/** # 连接器列表（集群上全部连接器，含未纳管） */
function ConnectorsPanel({ kafkaConnectId }: { kafkaConnectId: Id }) {
  const { data, loading, error, reload } = useAsync<KafkaConnectorVo[]>(
    () => kafkaConnectorApi.list(kafkaConnectId),
    [kafkaConnectId],
    [],
  );

  const columns: ColumnsType<KafkaConnectorVo> = [
    {
      title: '连接器名称',
      dataIndex: 'name',
      width: 240,
      render: (name: string) => (
        <span className="dbz-mono dbz-ellipsis" title={orDash(name)}>
          {orDash(name)}
        </span>
      ),
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
          <Tag>{EMPTY_TEXT}</Tag>
        ),
    },
    {
      title: '状态',
      dataIndex: 'state',
      width: 110,
      render: (state: KafkaConnectorStatus, record) => (
        <StatusTag dic={kafkaConnectorStatusDic} value={state} detail={connectorDetail(record)} />
      ),
    },
    {
      title: '类名',
      dataIndex: 'connectorClass',
      ellipsis: true,
      render: (value: string) => (
        <span className="dbz-mono dbz-ellipsis" title={orDash(value)}>
          {orDash(value)}
        </span>
      ),
    },
  ];

  if (error) {
    return (
      <Alert
        type="error"
        showIcon
        message="查询集群上的连接器失败"
        description={errorMessage(error)}
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
          共 {data.length} 个连接器（含集群上未纳管的），展开行可查看任务与异常
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
        scroll={{ x: 'max-content' }}
        locale={{ emptyText: loading ? '加载中…' : '集群上没有连接器' }}
        expandable={{
          rowExpandable: (record) => record.tasks.length > 0,
          expandedRowRender: (record) => <TaskTable tasks={record.tasks} />,
        }}
      />
    </>
  );
}

/** # 连接器任务表（只读） */
function TaskTable({ tasks }: { tasks: KafkaConnectorTaskVo[] }) {
  const columns: ColumnsType<KafkaConnectorTaskVo> = [
    { title: '任务ID', dataIndex: 'id', width: 90, align: 'center', className: 'dbz-num' },
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
          orDash(null)
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
      scroll={{ x: 'max-content' }}
      locale={{ emptyText: '该连接器没有任务' }}
    />
  );
}

/** # 插件列表 */
function PluginsPanel({ kafkaConnectId }: { kafkaConnectId: Id }) {
  const { data, loading, error, reload } = useAsync<KafkaConnectorPluginVo[]>(
    () => kafkaConnectorApi.plugins(kafkaConnectId),
    [kafkaConnectId],
    [],
  );

  const columns: ColumnsType<KafkaConnectorPluginVo> = [
    {
      title: '类名',
      dataIndex: 'class',
      render: (value: string) => (
        <span className="dbz-mono dbz-ellipsis" title={orDash(value)}>
          {orDash(value)}
        </span>
      ),
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
          <Tag>{EMPTY_TEXT}</Tag>
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
        description={errorMessage(error)}
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
        scroll={{ x: 'max-content' }}
        locale={{ emptyText: loading ? '加载中…' : '该集群没有已安装的插件' }}
      />
    </>
  );
}
