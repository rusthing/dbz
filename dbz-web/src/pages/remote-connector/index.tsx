/**
 * # 远端连接器
 *
 * 这一页操作的是**集群上真实运行的连接器**（Kafka Connect REST API），不是库里的记录：
 * 因此能看到未经 dbz 纳管的连接器（有人在集群上手工建的），也能直接重启、停止、重置位点
 * 或删除它们。
 *
 * 删除只动集群、不动库：删掉的连接器若仍被 dbz 纳管，下一轮健康检查会按记录重新部署
 * 回来；要彻底停掉请去对应的上游/下游连接器列表把启用开关关掉。
 *
 * ## 停止与重置位点的区别
 *
 * - **停止**（`/stop`，Kafka 3.5+）：保留配置与位点，恢复后从原处继续。
 * - **重置位点**（`/offsets`）：清掉位点，连接器就是「从没跑过」，`snapshot.mode=initial`
 *   时会整表快照一遍。服务端会先停再清，**不可撤销**。
 *
 * 停在「已停止」的连接器在 dbz 健康检查里会被记为异常并告警，但不会被自动重启 ——
 * 自动修复只处理 `FAILED`，人工动作不该被机器覆盖。
 *
 * 定位目标集群用 `?kafkaConnectId=`：从「Kafka Connect」页点「连接器」跳过来时自动带上。
 */

import {
  App as AntdApp,
  Alert,
  Button,
  Card,
  Checkbox,
  Empty,
  Modal,
  Popconfirm,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ReloadOutlined } from '@ant-design/icons';
import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import type {
  Id,
  KafkaConnectorPluginVo,
  KafkaConnectorStatus,
  KafkaConnectorTaskVo,
  KafkaConnectorVo,
} from '../../api/types';
import StatusTag from '../../components/StatusTag';
import type { StatusDetail } from '../../components/StatusTag';
import { kafkaConnectorStatusDic } from '../../dic';
import { useAsync } from '../../hooks/useAsync';
import { useClusterOptions } from '../../hooks/useOptions';
import { errorMessage, orDash } from '../../utils/format';

/** 任务的异常状态：FAILED 与 UNASSIGNED（与后端 `is_task_abnormal` 同一套判定） */
const ABNORMAL_TASK_STATES: KafkaConnectorStatus[] = [1, 3];

/**
 * 远端连接器的运行详情
 *
 * 这一页的状态是集群上**此刻**的真实状态，`/connectors?expand=status` 一次就把
 * 连接器与任务的 trace 都带回来了，所以不用再发请求。失败原因优先取连接器自己的
 * trace（它没起来），其次取第一个失败任务的（它起来了但挂了）。
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
      {
        label: 'Worker',
        value: <span className="dbz-mono">{orDash(record.workerId)}</span>,
      },
      { label: '任务', value: taskText },
      ...(record.state === 5
        ? [{ label: '说明', value: '已停止：配置与位点保留，恢复后从原处继续' }]
        : []),
      ...(record.state === 2
        ? [{ label: '说明', value: '已暂停：人工或集群侧暂停，dbz 不会自动重启它' }]
        : []),
    ],
    // 连接器自身的 trace 优先；任务那份额外可以在展开行里逐个重启
    reason: record.trace?.trim() || taskTrace || null,
  };
}

export default function RemoteConnectorPage() {
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
          value={selectedId}
          onChange={(value) => setSearchParams({ kafkaConnectId: String(value) })}
        />
        <Button icon={<ReloadOutlined />} onClick={cluster.reload}>
          刷新集群
        </Button>
        <span className="dbz-toolbar-spacer" />
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          这里的操作直接发往 Kafka Connect，不落库；重启是异步的，状态请稍后重新查询。删除只注销集群上的连接器，库里的记录需另行处理
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
function ConnectorsPanel({ kafkaConnectId }: { kafkaConnectId: Id }) {
  const { message } = AntdApp.useApp();
  const { data, loading, error, reload } = useAsync<KafkaConnectorVo[]>(
    () => kafkaConnectorApi.list(kafkaConnectId),
    [kafkaConnectId],
    [],
  );
  const [restarting, setRestarting] = useState<string | null>(null);
  const [removing, setRemoving] = useState<string | null>(null);
  const [switching, setSwitching] = useState<string | null>(null);
  /** 重置位点的目标连接器；非空时弹出确认框 */
  const [resetTarget, setResetTarget] = useState<KafkaConnectorVo | null>(null);
  const [resetResume, setResetResume] = useState(false);
  const [resetting, setResetting] = useState(false);

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

  /** 停止 / 恢复：同一个按钮按当前状态切换语义 */
  const handleSwitch = async (connectorName: string, stopped: boolean) => {
    setSwitching(connectorName);
    try {
      if (stopped) {
        await kafkaConnectorApi.resume({ kafkaConnectId, connectorName });
        message.success(`已提交恢复：${connectorName}`);
      } else {
        await kafkaConnectorApi.stop({ kafkaConnectId, connectorName });
        message.success(`已提交停止：${connectorName}（配置与位点保留）`);
      }
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setSwitching(null);
    }
  };

  const handleResetOffsets = async () => {
    if (!resetTarget) {
      return;
    }
    setResetting(true);
    try {
      await kafkaConnectorApi.resetOffsets({
        kafkaConnectId,
        connectorName: resetTarget.name,
        resume: resetResume,
      });
      message.success(
        resetResume
          ? `已清空位点并恢复运行：${resetTarget.name}`
          : `已清空位点：${resetTarget.name}（仍处于停止状态）`,
      );
      setResetTarget(null);
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setResetting(false);
    }
  };

  const handleRemove = async (connectorName: string) => {
    setRemoving(connectorName);
    try {
      await kafkaConnectorApi.remove({ kafkaConnectId, connectorName });
      message.success(`已删除：${connectorName}`);
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setRemoving(null);
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
      render: (state: KafkaConnectorStatus, record) => (
        <StatusTag
          dic={kafkaConnectorStatusDic}
          value={state}
          detail={connectorDetail(record)}
        />
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
      width: 290,
      render: (_, record) => {
        // 已停止的连接器给「恢复」，其余给「停止」
        const stopped = record.state === 5;
        return (
          <Space size={4}>
            <Popconfirm
              title={
                stopped
                  ? `恢复连接器「${record.name}」？`
                  : `停止连接器「${record.name}」？`
              }
              description={
                stopped
                  ? '恢复后从原位点继续消费。'
                  : '配置与位点都会保留，恢复后从原处继续；停止期间每轮健康检查都会把它记为异常并告警（不会自动重启）。需要 Kafka 3.5+。'
              }
              okText={stopped ? '恢复' : '停止'}
              cancelText="取消"
              onConfirm={() => handleSwitch(record.name, stopped)}
            >
              <Button type="link" size="small" loading={switching === record.name}>
                {stopped ? '恢复' : '停止'}
              </Button>
            </Popconfirm>
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
            <Button
              type="link"
              size="small"
              danger
              onClick={() => {
                setResetResume(false);
                setResetTarget(record);
              }}
            >
              重置位点
            </Button>
            <Popconfirm
              title={`从集群上删除「${record.name}」？`}
              description="连接器与其任务会从集群上注销，库里的记录不受影响；若它仍被 dbz 纳管，下一轮检查会重新部署回来。"
              okText="删除"
              okButtonProps={{ danger: true }}
              cancelText="取消"
              onConfirm={() => handleRemove(record.name)}
            >
              <Button type="link" size="small" danger loading={removing === record.name}>
                删除
              </Button>
            </Popconfirm>
          </Space>
        );
      },
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

      <Modal
        open={Boolean(resetTarget)}
        title={`重置位点 · ${resetTarget?.name ?? ''}`}
        width={580}
        okText="清空位点"
        okButtonProps={{ danger: true }}
        cancelText="取消"
        confirmLoading={resetting}
        onOk={handleResetOffsets}
        onCancel={() => setResetTarget(null)}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Alert
            type="warning"
            showIcon
            message="位点删了就没有了，这一步不可撤销"
            description="服务端会先停止连接器再清空位点。清空后连接器就是「从没跑过」：snapshot.mode=initial 时会做一次整表快照，数据以 op=r 追加到原 topic，下游需按主键幂等处理。"
          />
          <Typography.Text type="secondary">
            与「再同步」的区别：再同步走信号，只重读指定表、不清位点、不中断流式读取；
            这条路清掉全部位点，影响整条连接器。
          </Typography.Text>
          <Checkbox
            checked={resetResume}
            onChange={(event) => setResetResume(event.target.checked)}
          >
            清空后立即恢复运行（等于立刻重做一次全量快照）
          </Checkbox>
          {!resetResume && (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              不勾选时连接器保持「已停止」，需要再点一次「恢复」；期间健康检查会把它记为异常并告警。
            </Typography.Text>
          )}
        </Space>
      </Modal>
    </>
  );
}

/** # 连接器任务表 */
function TaskTable({
  kafkaConnectId,
  connectorName,
  tasks,
}: {
  kafkaConnectId: Id;
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
