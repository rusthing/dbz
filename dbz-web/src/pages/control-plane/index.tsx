/**
 * # 控制面视图
 *
 * 按 **Kafka 集群 → Connect → 上游/下游连接器** 的层级展示整条链路，节点上的状态
 * 优先取 Kafka Connect REST 的实时结果；库内 `run_status` 只作为「最近一次检查」的
 * 对照，两者不一致时标注出来。
 *
 * 可视化只是入口，真正的增删改仍在三个明细维护页；图上只做高频动作：查看详情/配置、
 * 启用/停用、重启。
 */

import {
  Alert,
  App as AntdApp,
  Badge,
  Button,
  Card,
  Empty,
  Popconfirm,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  ExportOutlined,
  ImportOutlined,
  ReloadOutlined,
  SyncOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { deadLetterApi } from '../../api/dead-letter';
import { kafkaBrokerApi } from '../../api/kafka-broker';
import { kafkaConnectApi } from '../../api/kafka-connect';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import { sinkConnectorApi } from '../../api/sink-connector';
import { sourceConnectorApi } from '../../api/source-connector';
import type {
  DbzDeadLetterCountVo,
  DbzKafkaBrokerVo,
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  DeadLetterConnectorType,
  Id,
  KafkaConnectorVo,
} from '../../api/types';
import EnabledSwitch from '../../components/EnabledSwitch';
import StatusTag from '../../components/StatusTag';
import {
  connectorRunStatusDic,
  kafkaBrokerStatusDic,
  kafkaConnectStatusDic,
  kafkaConnectorStatusDic,
} from '../../dic';
import { useAsync, type AsyncState } from '../../hooks/useAsync';
import { errorMessage, formatMs, orDash, truncate } from '../../utils/format';
import ConnectorDetailDrawer from './ConnectorDetailDrawer';
import {
  authLabelOf,
  brokerDetailOf,
  connectDbDetailOf,
  connectorDbDetailOf,
  liveDetailOf,
  topicInfoOf,
} from './details';
import {
  buildControlPlane,
  type BrokerNode,
  type ConnectHealth,
  type ConnectNode,
  type ConnectorDetailTarget,
  type ConnectorKind,
  type LiveResult,
  type ManagedConnectorNode,
  type ManagedConnectorRecord,
} from './model';

/** 实时状态查询的最长等待时间：单台 Connect 卡住不能拖死整个视图 */
const LIVE_TIMEOUT_MS = 15_000;

/** 禁用连接器时统一的影响说明 */
const TOGGLE_CONFIRM =
  '禁用后会先从这个 Kafka Connect 注销连接器，再改库；重新启用会当场把连接器部署回去。';

type BadgeStatus = 'success' | 'processing' | 'default' | 'error' | 'warning';

interface ControlPlaneData {
  brokers: DbzKafkaBrokerVo[];
  connects: DbzKafkaConnectVo[];
  sources: DbzSourceConnectorVo[];
  sinks: DbzSinkConnectorVo[];
  /** 每台 Connect 的实时连接器状态；失败时带 `error` */
  live: Map<Id, LiveResult>;
  /** 每台 Connect 自身的健康探测结果 */
  health: Map<Id, ConnectHealth>;
  /** 连接器的待处理死信数量，键见 {@link deadLetterKey} */
  deadLetter: Map<string, number>;
  /** 本次实时采集时间 */
  loadedAt: number;
}

const EMPTY_DATA: ControlPlaneData = {
  brokers: [],
  connects: [],
  sources: [],
  sinks: [],
  live: new Map(),
  health: new Map(),
  deadLetter: new Map(),
  loadedAt: 0,
};

/** 死信计数的键：连接器 id 在不同表里会重复，必须带方向 */
function deadLetterKey(connectorType: DeadLetterConnectorType, connectorId: Id): string {
  return `${connectorType}:${connectorId}`;
}

/** 连接器方向转死信里的枚举 */
function deadLetterTypeOf(kind: ConnectorKind): DeadLetterConnectorType {
  return kind === 'source' ? 1 : 2;
}

/** 带超时的 Promise：超时只影响展示，底层请求仍会自然结束 */
function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = window.setTimeout(() => {
      reject(new Error(`实时状态查询超过 ${Math.round(timeoutMs / 1000)} 秒未返回`));
    }, timeoutMs);
    promise.then(
      (value) => {
        window.clearTimeout(timer);
        resolve(value);
      },
      (reason: unknown) => {
        window.clearTimeout(timer);
        reject(reason);
      },
    );
  });
}

/** 一次加载：四张表 + 每台 Connect 的实时连接器 + 死信待处理计数 */
async function loadControlPlane(): Promise<ControlPlaneData> {
  const [brokers, connects, sources, sinks, deadLetterCounts] = await Promise.all([
    kafkaBrokerApi.list(),
    kafkaConnectApi.list(),
    sourceConnectorApi.list(),
    sinkConnectorApi.list(),
    // 死信计数只影响角标，接口失败时降级为空，不拖垮整页
    deadLetterApi.pendingCount().catch(() => [] as DbzDeadLetterCountVo[]),
  ]);

  const live = new Map<Id, LiveResult>();
  const health = new Map<Id, ConnectHealth>();
  await Promise.all(
    connects.map(async (cluster) => {
      // 已停用的 Connect 不参与健康检查，也没必要发实时请求
      if (!cluster.enabled) {
        live.set(cluster.id, { connectors: [], skipped: true });
        health.set(cluster.id, { skipped: true });
        return;
      }
      // 连接器列表与 Connect 自身健康并行取，任一失败都不影响另一项
      await Promise.all([
        (async () => {
          try {
            const connectors = await withTimeout(
              kafkaConnectorApi.list(cluster.id),
              LIVE_TIMEOUT_MS,
            );
            live.set(cluster.id, { connectors });
          } catch (reason) {
            live.set(cluster.id, { connectors: [], error: errorMessage(reason) });
          }
        })(),
        (async () => {
          try {
            const result = await withTimeout(
              kafkaConnectApi.health(cluster.id),
              LIVE_TIMEOUT_MS,
            );
            health.set(cluster.id, { health: result });
          } catch (reason) {
            health.set(cluster.id, { error: errorMessage(reason) });
          }
        })(),
      ]);
    }),
  );

  const deadLetter = new Map(
    deadLetterCounts.map((item) => [deadLetterKey(item.connectorType, item.connectorId), item.count]),
  );

  return { brokers, connects, sources, sinks, live, health, deadLetter, loadedAt: Date.now() };
}

/** 连接器在页面上的唯一键（重启 loading 用） */
function connectorKey(record: ManagedConnectorRecord): string {
  return `${record.kafkaConnectId}:${record.connectorName}`;
}

/** 明细维护页路径 */
function detailPageOf(kind: ConnectorKind): string {
  return kind === 'source' ? '/source-connector' : '/sink-connector';
}

/** broker 的节点色 */
function brokerBadgeStatus(broker: DbzKafkaBrokerVo): BadgeStatus {
  if (!broker.enabled) {
    return 'default';
  }
  switch (broker.runStatus) {
    case 1:
      return 'success';
    case 3:
      return 'error';
    default:
      return 'processing';
  }
}

/** Connect 的节点色：以实时可达性为准 */
function connectBadgeStatus(node: ConnectNode): BadgeStatus {
  if (!node.cluster.enabled) {
    return 'default';
  }
  return node.liveIssue ? 'error' : 'success';
}

/** 连接器的节点色：以实时状态为准 */
function connectorBadgeStatus(node: ManagedConnectorNode): BadgeStatus {
  if (!node.record.enabled || node.connectDisabled) {
    return 'default';
  }
  if (node.liveIssue) {
    return 'error';
  }
  if (!node.live) {
    return 'warning';
  }
  switch (node.live.state) {
    case 0:
      return 'success';
    case 1:
      return 'error';
    case 4:
      return 'default';
    default:
      return 'warning';
  }
}

/** 未纳管连接器名称，超过 4 个时省略 */
function unmanagedText(items: KafkaConnectorVo[]): string {
  const names = items.slice(0, 4).map((item) => item.name);
  return items.length > 4 ? `${names.join('、')} 等 ${items.length} 个` : names.join('、');
}

/**
 * # Connect 自身健康标签
 *
 * 以 `GET /dbz/kafka-connect/health` 的结果为准：可达时带上版本、worker 与连接器数，
 * 不可达或探测失败时给出原因。停用的 Connect 不探测。
 */
function connectHealthTag(node: ConnectNode) {
  if (!node.cluster.enabled) {
    return <Tag>已停用</Tag>;
  }
  const probe = node.health;
  if (probe?.error) {
    return (
      <Tooltip title={probe.error}>
        <Tag color="error">健康探测失败</Tag>
      </Tooltip>
    );
  }
  if (!probe?.health) {
    return <Tag>未探测</Tag>;
  }
  const health = probe.health;
  if (!health.reachable) {
    return (
      <Tooltip title={health.message ?? 'Kafka Connect 不可达'}>
        <Tag color="error">不可达</Tag>
      </Tooltip>
    );
  }
  return (
    <Tooltip
      title={`Commit ${health.commit ?? '--'} · Kafka 集群 ${health.kafkaClusterId ?? '--'} · 耗时 ${health.elapsedMs} ms`}
    >
      <Tag color="success">
        {health.version ? `v${health.version}` : '可达'} · {health.workerCount} worker ·{' '}
        {health.connectorCount} 连接器
      </Tag>
    </Tooltip>
  );
}

interface NodeActions {
  checkingBrokerId: Id | null;
  restartingKey: string | null;
  onCheckBroker: (broker: DbzKafkaBrokerVo) => void;
  onOpenDetail: (kind: ConnectorKind, record: ManagedConnectorRecord) => void;
  onRestart: (record: ManagedConnectorRecord) => void;
  onToggle: (kind: ConnectorKind, record: ManagedConnectorRecord, enabled: boolean) => Promise<void>;
  /** 某个连接器的待处理死信数 */
  pendingOf: (kind: ConnectorKind, connectorId: Id) => number;
  navigateTo: (path: string) => void;
}

function BrokerSection({ node, actions }: { node: BrokerNode; actions: NodeActions }) {
  const { broker, connects } = node;
  const checking = actions.checkingBrokerId === broker.id;

  return (
    <Card
      className="dbz-topology-broker"
      title={
        <div className="dbz-topology-broker-head">
          <div className="dbz-topology-title-line">
            <Badge status={brokerBadgeStatus(broker)} />
            <span className="dbz-topology-name">{broker.name}</span>
            <StatusTag
              dic={kafkaBrokerStatusDic}
              value={broker.runStatus}
              detail={brokerDetailOf(broker)}
            />
            {!broker.enabled && <Tag>已停用</Tag>}
            <span className="dbz-topology-count">
              {connects.length} 个 Connect · {node.sourceCount} 个上游 · {node.sinkCount} 个下游
            </span>
          </div>
          <div className="dbz-topology-meta dbz-mono">
            {orDash(broker.bootstrapServers)} · {authLabelOf(broker)}
          </div>
        </div>
      }
      extra={
        <Space size={4} wrap>
          <Button
            size="small"
            loading={checking}
            onClick={() => actions.onCheckBroker(broker)}
          >
            检测
          </Button>
          <Button size="small" type="link" onClick={() => actions.navigateTo('/kafka-broker')}>
            明细
          </Button>
        </Space>
      }
    >
      {connects.length === 0 ? (
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description="这个 Kafka 集群下还没有 Connect 实例"
        >
          <Button size="small" onClick={() => actions.navigateTo('/kafka-connect')}>
            去明细维护新增
          </Button>
        </Empty>
      ) : (
        <div className="dbz-topology-grid">
          {connects.map((connectNode) => (
            <ConnectCard key={connectNode.cluster.id} node={connectNode} actions={actions} />
          ))}
        </div>
      )}
    </Card>
  );
}

function ConnectCard({ node, actions }: { node: ConnectNode; actions: NodeActions }) {
  const { cluster } = node;
  const remotePath = `/remote-connector?kafkaConnectId=${cluster.id}`;

  return (
    <Card
      size="small"
      type="inner"
      className="dbz-topology-connect"
      title={
        <div className="dbz-topology-connect-head">
          <div className="dbz-topology-title-line">
            <Badge status={connectBadgeStatus(node)} />
            <span className="dbz-topology-name">{cluster.name}</span>
            {connectHealthTag(node)}
            <span className="dbz-topology-db">
              库:
              <StatusTag
                dic={kafkaConnectStatusDic}
                value={cluster.runStatus}
                detail={connectDbDetailOf(cluster)}
              />
            </span>
            {node.mismatch && (
              <Tooltip title={node.mismatch}>
                <WarningOutlined className="dbz-topology-mismatch" aria-label={node.mismatch} />
              </Tooltip>
            )}
          </div>
          <div className="dbz-topology-meta dbz-mono">{orDash(cluster.url)}</div>
        </div>
      }
      extra={
        <Space size={0} wrap>
          <Button size="small" type="link" onClick={() => actions.navigateTo('/kafka-connect')}>
            明细
          </Button>
          <Button size="small" type="link" onClick={() => actions.navigateTo(remotePath)}>
            集群连接器
          </Button>
        </Space>
      }
    >
      {node.liveIssue && (
        <div className="dbz-topology-issue">
          <WarningOutlined /> 实时状态查询失败：{truncate(node.liveIssue, 100)}
        </div>
      )}

      <div className="dbz-topology-group">
        <div className="dbz-topology-group-label">
          <ImportOutlined /> 上游 source <Tag>{node.sources.length}</Tag>
        </div>
        {node.sources.length === 0 ? (
          <span className="dbz-topology-empty">暂无上游连接器</span>
        ) : (
          node.sources.map((item) => (
            <ConnectorRow key={item.record.id} node={item} actions={actions} />
          ))
        )}
      </div>

      <div className="dbz-topology-group" style={{ marginTop: 10 }}>
        <div className="dbz-topology-group-label">
          <ExportOutlined /> 下游 sink <Tag>{node.sinks.length}</Tag>
        </div>
        {node.sinks.length === 0 ? (
          <span className="dbz-topology-empty">暂无下游连接器</span>
        ) : (
          node.sinks.map((item) => (
            <ConnectorRow key={item.record.id} node={item} actions={actions} />
          ))
        )}
      </div>

      {node.unmanaged.length > 0 && (
        <div className="dbz-topology-unmanaged">
          集群上另有未纳管连接器：
          <Tooltip title={node.unmanaged.map((item) => item.name).join('、')}>
            <span>{unmanagedText(node.unmanaged)}</span>
          </Tooltip>
          <Button size="small" type="link" onClick={() => actions.navigateTo(remotePath)}>
            查看
          </Button>
        </div>
      )}
    </Card>
  );
}

function ConnectorRow({
  node,
  actions,
}: {
  node: ManagedConnectorNode;
  actions: NodeActions;
}) {
  const { kind, record, live, liveIssue, mismatch } = node;
  const restarting = actions.restartingKey === connectorKey(record);
  const topic = topicInfoOf(kind, record);
  const pending = actions.pendingOf(kind, record.id);

  const liveTag = node.connectDisabled ? (
    <Tag>所属 Connect 已停用</Tag>
  ) : !record.enabled ? (
    <Tag>已停用</Tag>
  ) : liveIssue ? (
    <Tooltip title={liveIssue}>
      <Tag color="error">实时不可达</Tag>
    </Tooltip>
  ) : live ? (
    <StatusTag dic={kafkaConnectorStatusDic} value={live.state} detail={liveDetailOf(live)} />
  ) : (
    <Tag color="warning">集群上未部署</Tag>
  );

  return (
    <div className="dbz-topology-connector">
      <div className="dbz-topology-connector-main">
        <div className="dbz-topology-connector-line">
          <Badge status={connectorBadgeStatus(node)} />
          <Tag color={kind === 'source' ? 'blue' : 'purple'}>
            {kind === 'source' ? 'source' : 'sink'}
          </Tag>
          <span className="dbz-topology-connector-name">{record.name}</span>
          <span
            className="dbz-mono dbz-topology-connector-alias"
            title={record.connectorName}
          >
            {record.connectorName}
          </span>
        </div>
        <div className="dbz-topology-connector-line">
          {liveTag}
          <span className="dbz-topology-db">
            库:
            <StatusTag
              dic={connectorRunStatusDic}
              value={record.runStatus}
              detail={connectorDbDetailOf(record)}
            />
          </span>
          {mismatch && (
            <Tooltip title={mismatch}>
              <WarningOutlined className="dbz-topology-mismatch" aria-label={mismatch} />
            </Tooltip>
          )}
          {pending > 0 && (
            <Tooltip title="这个连接器有待处理死信，点「死信」查看报文并重投或废弃">
              <Tag
                color="error"
                style={{ cursor: 'pointer' }}
                onClick={() =>
                  actions.navigateTo(
                    `/dead-letter?connectorType=${deadLetterTypeOf(kind)}&connectorId=${record.id}`,
                  )
                }
              >
                死信 {pending}
              </Tag>
            </Tooltip>
          )}
        </div>
        {topic && (
          <div className="dbz-topology-connector-line dbz-topology-topic">
            <span>{topic.label}:</span>
            <Tooltip title={topic.value}>
              <span className="dbz-mono dbz-topology-topic-value">
                {truncate(topic.value, 40)}
              </span>
            </Tooltip>
          </div>
        )}
      </div>
      <div className="dbz-topology-connector-actions">
        <Button size="small" type="link" onClick={() => actions.onOpenDetail(kind, record)}>
          详情
        </Button>
        <Button
          size="small"
          type="link"
          onClick={() => actions.navigateTo(detailPageOf(kind))}
        >
          明细
        </Button>
        {record.enabled && live && !liveIssue && !node.connectDisabled && (
          <Popconfirm
            title="重启连接器？"
            description="会连同任务一起重启，Kafka Connect 异步执行。"
            okText="重启"
            cancelText="取消"
            onConfirm={() => actions.onRestart(record)}
          >
            <Button size="small" type="link" icon={<SyncOutlined />} loading={restarting}>
              重启
            </Button>
          </Popconfirm>
        )}
        <EnabledSwitch
          enabled={record.enabled}
          name={record.name}
          confirmContent={TOGGLE_CONFIRM}
          onToggle={(enabled) => actions.onToggle(kind, record, enabled)}
        />
      </div>
    </div>
  );
}

export default function ControlPlanePage() {
  const { message, modal } = AntdApp.useApp();
  const navigate = useNavigate();
  const state: AsyncState<ControlPlaneData> = useAsync(loadControlPlane, [], EMPTY_DATA);
  const [detail, setDetail] = useState<ConnectorDetailTarget | null>(null);
  const [checkingBrokerId, setCheckingBrokerId] = useState<Id | null>(null);
  const [restartingKey, setRestartingKey] = useState<string | null>(null);

  const nodes = useMemo(
    () =>
      buildControlPlane(
        state.data.brokers,
        state.data.connects,
        state.data.sources,
        state.data.sinks,
        state.data.live,
        state.data.health,
      ),
    [state.data],
  );

  const mismatchTotal = useMemo(
    () => nodes.reduce((sum, node) => sum + node.mismatchCount, 0),
    [nodes],
  );

  /** 顶部 Connect 自身健康汇总：只统计启用中的记录 */
  const healthSummary = useMemo(() => {
    const enabled = nodes.flatMap((node) => node.connects).filter((node) => node.cluster.enabled);
    const reachable = enabled.filter((node) => node.health?.health?.reachable).length;
    const unreachable = enabled.filter(
      (node) =>
        node.health?.error || (node.health?.health && !node.health.health.reachable),
    ).length;
    return { total: enabled.length, reachable, unreachable };
  }, [nodes]);

  /** 待处理死信总数：连接器卡片上的角标与顶栏共用一份计数 */
  const deadLetterTotal = useMemo(
    () => [...state.data.deadLetter.values()].reduce((sum, count) => sum + count, 0),
    [state.data.deadLetter],
  );

  const checkBroker = async (broker: DbzKafkaBrokerVo) => {
    setCheckingBrokerId(broker.id);
    try {
      const result = await kafkaBrokerApi.check(broker.id);
      if (result.reachable) {
        message.success(
          `检测通过：topic ${result.topicCount} 个，耗时 ${Math.round(result.elapsedMs)} ms`,
        );
      } else {
        modal.warning({
          title: `Kafka 集群「${broker.name}」检测未通过`,
          content: result.message || '无法连接到 Kafka 集群。',
        });
      }
      // 检测结果会回写 run_status / run_msg，整页刷新才能看到最新结论
      state.reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setCheckingBrokerId(null);
    }
  };

  const restartConnector = async (record: ManagedConnectorRecord) => {
    const key = connectorKey(record);
    setRestartingKey(key);
    try {
      await kafkaConnectorApi.restart({
        kafkaConnectId: record.kafkaConnectId,
        connectorName: record.connectorName,
        includeTasks: true,
        onlyFailed: false,
      });
      message.success(`已提交重启「${record.connectorName}」，状态稍后刷新`);
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setRestartingKey(null);
    }
  };

  const toggleConnector = async (
    kind: ConnectorKind,
    record: ManagedConnectorRecord,
    enabled: boolean,
  ) => {
    if (kind === 'source') {
      await sourceConnectorApi.modify({ id: record.id, enabled });
    } else {
      await sinkConnectorApi.modify({ id: record.id, enabled });
    }
    // 启用/停用会动到集群，刷新后实时状态才能反映出来
    state.reload();
  };

  const actions: NodeActions = {
    checkingBrokerId,
    restartingKey,
    onCheckBroker: (broker) => {
      void checkBroker(broker);
    },
    onOpenDetail: (kind, record) => setDetail({ kind, record }),
    onRestart: (record) => {
      void restartConnector(record);
    },
    onToggle: (kind, record, enabled) => toggleConnector(kind, record, enabled),
    pendingOf: (kind, connectorId) =>
      state.data.deadLetter.get(deadLetterKey(deadLetterTypeOf(kind), connectorId)) ?? 0,
    navigateTo: (path) => navigate(path),
  };

  /** 抽屉需要的上下文，随刷新一起更新 */
  const detailContext = useMemo(() => {
    if (!detail) {
      return {} as {
        cluster?: DbzKafkaConnectVo;
        broker?: DbzKafkaBrokerVo;
        upstream?: DbzSourceConnectorVo;
        live?: KafkaConnectorVo;
        liveIssue?: string;
      };
    }
    const record = detail.record;
    const cluster = state.data.connects.find((item) => item.id === record.kafkaConnectId);
    const broker = cluster
      ? state.data.brokers.find((item) => item.id === cluster.kafkaBrokerId)
      : undefined;
    const upstream =
      'sourceConnectorId' in record
        ? state.data.sources.find((item) => item.id === record.sourceConnectorId)
        : undefined;
    const liveResult = state.data.live.get(record.kafkaConnectId);
    const live = liveResult?.connectors.find((item) => item.name === record.connectorName);
    return { cluster, broker, upstream, live, liveIssue: liveResult?.error };
  }, [detail, state.data]);

  return (
    <>
      <div className="dbz-toolbar">
        <Typography.Text type="secondary">
          实时状态来自 Kafka Connect REST；库内状态是健康检查任务最近一次回写的结论，两者不一致会标黄提示
        </Typography.Text>
        <span className="dbz-toolbar-spacer" />
        {healthSummary.total > 0 && (
          <Tooltip title="Connect 自身可达性与版本信息来自 GET /dbz/kafka-connect/health">
            <Tag color={healthSummary.unreachable > 0 ? 'error' : 'success'}>
              Connect 健康 {healthSummary.reachable}/{healthSummary.total} 可达
              {healthSummary.unreachable > 0 ? `，${healthSummary.unreachable} 台异常` : ''}
            </Tag>
          </Tooltip>
        )}
        {deadLetterTotal > 0 && (
          <Tooltip title="magic 脚本上报的待处理死信；点开可看报文并重投或废弃">
            <Tag
              color="error"
              style={{ cursor: 'pointer' }}
              onClick={() => navigate('/dead-letter?status=0')}
            >
              待处理死信 {deadLetterTotal}
            </Tag>
          </Tooltip>
        )}
        {state.data.loadedAt > 0 && (
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            实时采集于 {formatMs(state.data.loadedAt)}
          </Typography.Text>
        )}
        <Button icon={<ReloadOutlined />} loading={state.loading} onClick={state.reload}>
          刷新实时状态
        </Button>
      </div>

      <Space size={16} wrap className="dbz-topology-legend">
        <Badge status="success" text="运行中 / 在线" />
        <Badge status="error" text="失败 / 不可达" />
        <Badge status="warning" text="未部署 / 未分配 / 已停止" />
        <Badge status="default" text="已停用 / 已销毁" />
        <Tag color="gold" icon={<WarningOutlined />}>
          库内与实时不一致
        </Tag>
      </Space>

      {state.error && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="控制面数据加载失败"
          description={errorMessage(state.error)}
          action={
            <Button size="small" onClick={state.reload}>
              重试
            </Button>
          }
        />
      )}

      {mismatchTotal > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message={`有 ${mismatchTotal} 处库内状态与集群实时状态不一致`}
          description="图上已用黄色标记。库内状态等待健康检查任务回写，本次比对以 Kafka Connect REST 的实时结果为准。"
        />
      )}

      <Spin spinning={state.loading}>
        {nodes.map((node) => (
          <BrokerSection key={node.broker.id} node={node} actions={actions} />
        ))}

        {nodes.length === 0 && !state.loading && !state.error && (
          <Card>
            <Empty description="还没有 Kafka 集群记录，先到明细维护新增">
              <Button type="primary" onClick={() => navigate('/kafka-broker')}>
                去 Kafka 集群
              </Button>
            </Empty>
          </Card>
        )}
      </Spin>

      <ConnectorDetailDrawer
        target={detail}
        cluster={detailContext.cluster}
        broker={detailContext.broker}
        upstream={detailContext.upstream}
        live={detailContext.live}
        liveIssue={detailContext.liveIssue}
        restarting={Boolean(detail && restartingKey === connectorKey(detail.record))}
        onClose={() => setDetail(null)}
        onRestart={() => {
          if (detail) {
            void restartConnector(detail.record);
          }
        }}
        onToggle={(enabled) =>
          detail ? toggleConnector(detail.kind, detail.record, enabled) : Promise.resolve()
        }
      />
    </>
  );
}
