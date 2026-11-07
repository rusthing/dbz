/**
 * # 控制面视图（自由画布）
 *
 * 以方案 A 的交互为准：左侧组件调色板拖到画布新增节点，节点可自由摆放，空白平移、
 * 以指针为中心缩放、右下角缩略图；右侧是属性检查器。
 *
 * 连线规则是「下游锁定」：
 * - 连线上游端可以拖到另一台同类节点，改挂上游；
 * - 下游端是方形锁标，不响应拖拽，检查器里也不提供改下游的入口；
 * - 连线不能删除，删除快捷键对连线无效；要断开链路只能删除下游节点，
 *   删除前会把受影响的数据流和写入目标列出来。
 *
 * 节点位置按「节点类型 + 记录 ID」存 `dbz_control_plane_layout`，拖动松手即保存，
 * 多人打开同一套连接器看到一致布局。
 */

import {
  DeleteOutlined,
  EditOutlined,
  LockOutlined,
  ReloadOutlined,
  SyncOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import {
  Alert,
  App as AntdApp,
  Button,
  Descriptions,
  Empty,
  Popconfirm,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { controlPlaneApi } from '../../api/control-plane';
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
} from '../../api/types';
import EnabledSwitch from '../../components/EnabledSwitch';
import StatusTag from '../../components/StatusTag';
import {
  connectorRunStatusDic,
  kafkaBrokerStatusDic,
  kafkaConnectStatusDic,
} from '../../dic';
import { useAsync, type AsyncState } from '../../hooks/useAsync';
import { errorMessage, orDash } from '../../utils/format';
import ConnectorDetailDrawer from './ConnectorDetailDrawer';
import ControlPlaneCanvas from './ControlPlaneCanvas';
import {
  NODE_TYPE_OF,
  affectedEdgesOf,
  buildEdges,
  defaultPosition,
  flattenNodes,
  nodeKeyOf,
  type CanvasEdge,
  type CanvasNode,
  type CanvasNodeKind,
} from './canvas-model';
import {
  brokerDetailOf,
  connectorDbDetailOf,
  connectDbDetailOf,
  topicInfoOf,
} from './details';
import {
  buildControlPlane,
  liveFailureReason,
  liveStatusLabel,
  type ConnectorDetailTarget,
  type ConnectorKind,
  type ConnectHealth,
  type LiveResult,
  type ManagedConnectorRecord,
} from './model';
import KafkaBrokerForm from '../kafka-broker/KafkaBrokerForm';
import KafkaConnectForm from '../kafka-connect/KafkaConnectForm';
import SinkConnectorForm from '../sink-connector/SinkConnectorForm';
import SourceConnectorForm from '../source-connector/SourceConnectorForm';

/** 实时状态查询的最长等待时间：单台 Connect 卡住不能拖死整个视图 */
const LIVE_TIMEOUT_MS = 15_000;

/** 禁用连接器时统一的影响说明 */
const TOGGLE_CONFIRM =
  '禁用后会先从这个 Kafka Connect 注销连接器，再改库；重新启用会当场把连接器部署回去。';

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
  /** 画布上已保存的节点位置 */
  layout: { nodeType: number; nodeId: Id; x: number; y: number }[];
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
  layout: [],
  loadedAt: 0,
};

interface Point {
  x: number;
  y: number;
}

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

/** 一次加载：四张表 + 每台 Connect 的实时连接器 + 死信计数 + 画布位置 */
async function loadControlPlane(): Promise<ControlPlaneData> {
  const [brokers, connects, sources, sinks, deadLetterCounts, layout] = await Promise.all([
    kafkaBrokerApi.list(),
    kafkaConnectApi.list(),
    sourceConnectorApi.list(),
    sinkConnectorApi.list(),
    // 死信计数只影响角标，接口失败时降级为空，不拖垮整页
    deadLetterApi.pendingCount().catch(() => [] as DbzDeadLetterCountVo[]),
    // 布局接口失败时按默认网格摆放，不阻塞节点数据
    controlPlaneApi.listLayout().catch(() => []),
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

  return { brokers, connects, sources, sinks, live, health, deadLetter, layout, loadedAt: Date.now() };
}

/** 连接器在页面上的唯一键（重启 loading 用） */
function connectorKey(record: ManagedConnectorRecord): string {
  return `${record.kafkaConnectId}:${record.connectorName}`;
}

/** 超时/错误提示里统一把未知异常转成字符串 */
function reasonText(reason: unknown, fallback = '请求失败'): string {
  if (reason instanceof Error && reason.message) {
    return reason.message;
  }
  return fallback;
}

/** 取某种节点在数据里的记录 */
function recordsOfKind(
  data: ControlPlaneData,
  kind: CanvasNodeKind,
  fallback: { id: Id }[] = [],
): { id: Id }[] {
  if (!data) {
    return fallback;
  }
  switch (kind) {
    case 'broker':
      return data.brokers;
    case 'connect':
      return data.connects;
    case 'source':
      return data.sources;
    case 'sink':
      return data.sinks;
  }
}

/** 节点类型中文名 */
function kindLabel(kind: CanvasNodeKind): string {
  switch (kind) {
    case 'broker':
      return 'Kafka 集群';
    case 'connect':
      return 'Kafka Connect';
    case 'source':
      return '上游连接器';
    case 'sink':
      return '下游连接器';
  }
}

/** 连接器明细页 */
function detailPageOf(kind: ConnectorKind): string {
  return kind === 'source' ? '/source-connector' : '/sink-connector';
}

/** 节点明细页 */
function nodeDetailPageOf(kind: CanvasNodeKind): string {
  switch (kind) {
    case 'broker':
      return '/kafka-broker';
    case 'connect':
      return '/kafka-connect';
    case 'source':
      return '/source-connector';
    case 'sink':
      return '/sink-connector';
  }
}

export default function ControlPlanePage() {
  const { message, modal } = AntdApp.useApp();
  const navigate = useNavigate();
  const state: AsyncState<ControlPlaneData> = useAsync(loadControlPlane, [], EMPTY_DATA);

  const [positions, setPositions] = useState<Record<string, Point>>({});
  const [selectedNodeKey, setSelectedNodeKey] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [detail, setDetail] = useState<ConnectorDetailTarget | null>(null);
  const [checkingBrokerId, setCheckingBrokerId] = useState<Id | null>(null);
  const [restartingKey, setRestartingKey] = useState<string | null>(null);
  const [busyNodeKey, setBusyNodeKey] = useState<string | null>(null);

  /** 新增：调色板落位后先记下位置，表单成功后从新记录里找出 ID 再落库 */
  const [createKind, setCreateKind] = useState<CanvasNodeKind | null>(null);
  const [pendingDrop, setPendingDrop] = useState<{
    kind: CanvasNodeKind;
    x: number;
    y: number;
    existingIds: Set<Id>;
  } | null>(null);
  const [editTarget, setEditTarget] = useState<CanvasNode | null>(null);

  const tree = useMemo(
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

  const deadCountOf = (kind: ConnectorKind, id: Id): number =>
    state.data.deadLetter.get(deadLetterKey(deadLetterTypeOf(kind), id)) ?? 0;

  const nodes = useMemo(() => flattenNodes(tree, deadCountOf), [tree, state.data.deadLetter]);

  const edges = useMemo(
    () => buildEdges(state.data.brokers, state.data.connects, state.data.sources, state.data.sinks),
    [state.data.brokers, state.data.connects, state.data.sinks, state.data.sources],
  );

  /** 服务端保存的位置；没有位置的节点按默认网格补 */
  useEffect(() => {
    const next: Record<string, Point> = {};
    for (const item of state.data.layout) {
      next[`${item.nodeType}:${item.nodeId}`] = { x: item.x, y: item.y };
    }
    setPositions(next);
  }, [state.data.layout]);

  /** 有持久化位置就用，没有就按同类节点的序号摆 */
  const resolvedPositions = useMemo(() => {
    const result: Record<string, Point> = { ...positions };
    const counters: Record<CanvasNodeKind, number> = { broker: 0, connect: 0, source: 0, sink: 0 };
    for (const node of nodes) {
      const index = counters[node.kind];
      counters[node.kind] += 1;
      if (!result[node.key]) {
        result[node.key] = defaultPosition(node.kind, index);
      }
    }
    return result;
  }, [nodes, positions]);

  const selectedNode = useMemo(
    () => nodes.find((node) => node.key === selectedNodeKey) ?? null,
    [nodes, selectedNodeKey],
  );
  const selectedEdge = useMemo(
    () => edges.find((edge) => edge.id === selectedEdgeId) ?? null,
    [edges, selectedEdgeId],
  );

  /** 节点被删除后清掉选中态，避免检查器挂在空记录上 */
  useEffect(() => {
    if (selectedNodeKey && !nodes.some((node) => node.key === selectedNodeKey)) {
      setSelectedNodeKey(null);
    }
  }, [nodes, selectedNodeKey]);
  useEffect(() => {
    if (selectedEdgeId && !edges.some((edge) => edge.id === selectedEdgeId)) {
      setSelectedEdgeId(null);
    }
  }, [edges, selectedEdgeId]);

  /** 新增表单成功后，从数据里找出新记录，补上落位位置并落库 */
  useEffect(() => {
    if (!pendingDrop) {
      return;
    }
    const created = recordsOfKind(state.data, pendingDrop.kind).find(
      (record) => !pendingDrop.existingIds.has(record.id),
    );
    if (!created) {
      return;
    }
    const key = nodeKeyOf(pendingDrop.kind, created.id);
    const point = { x: pendingDrop.x, y: pendingDrop.y };
    setPositions((previous) => ({ ...previous, [key]: point }));
    setSelectedNodeKey(key);
    setSelectedEdgeId(null);
    setPendingDrop(null);
    void controlPlaneApi
      .saveLayout({
        nodeType: NODE_TYPE_OF[pendingDrop.kind],
        nodeId: created.id,
        x: point.x,
        y: point.y,
      })
      .catch((reason: unknown) => {
        message.warning(`节点已创建，但布局位置保存失败：${errorMessage(reason)}`);
      });
  }, [message, pendingDrop, state.data]);

  const mismatchTotal = useMemo(
    () => nodes.reduce((sum, node) => sum + (node.mismatch ? 1 : 0), 0),
    [nodes],
  );

  /** 顶部 Connect 自身健康汇总：只统计启用中的记录 */
  const healthSummary = useMemo(() => {
    const enabled = nodes.filter((node) => node.kind === 'connect' && node.cluster?.enabled);
    const reachable = enabled.filter((node) => node.health?.health?.reachable).length;
    const unreachable = enabled.filter(
      (node) =>
        node.health?.error || (node.health?.health && !node.health.health.reachable),
    ).length;
    return { total: enabled.length, reachable, unreachable };
  }, [nodes]);

  /** 待处理死信总数：节点角标与顶栏共用一份计数 */
  const deadLetterTotal = useMemo(
    () => nodes.reduce((sum, node) => sum + node.dead, 0),
    [nodes],
  );

  const clusterOptions = useMemo(
    () => state.data.connects.map((cluster) => ({ value: cluster.id, label: cluster.name })),
    [state.data.connects],
  );
  const sourceOptions = useMemo(
    () => state.data.sources.map((source) => ({ value: source.id, label: source.name })),
    [state.data.sources],
  );

  /** 拖动结束后保存位置 */
  const handleNodeMove = (key: string, x: number, y: number, commit: boolean) => {
    setPositions((previous) => ({ ...previous, [key]: { x, y } }));
    if (!commit) {
      return;
    }
    const node = nodes.find((item) => item.key === key);
    if (!node) {
      return;
    }
    void controlPlaneApi
      .saveLayout({ nodeType: node.nodeType, nodeId: node.id, x, y })
      .catch((reason: unknown) => {
        message.error(`布局保存失败：${errorMessage(reason)}`);
      });
  };

  /** 调色板拖到画布：记下已有 ID，表单成功后用它识别新记录 */
  const handleCreateAt = (kind: CanvasNodeKind, x: number, y: number) => {
    const existingIds = new Set(recordsOfKind(state.data, kind).map((record) => record.id));
    setPendingDrop({ kind, x, y, existingIds });
    setEditTarget(null);
    setCreateKind(kind);
  };

  const cancelForm = () => {
    setCreateKind(null);
    setEditTarget(null);
    setPendingDrop(null);
  };

  const formSuccess = () => {
    setCreateKind(null);
    setEditTarget(null);
    state.reload();
  };

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
      state.reload();
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
    message.success(`已${enabled ? '启用' : '停用'}「${record.name}」`);
    state.reload();
  };

  const toggleBroker = async (broker: DbzKafkaBrokerVo, enabled: boolean) => {
    await kafkaBrokerApi.modify({ id: broker.id, enabled });
    message.success(`已${enabled ? '启用' : '停用'}「${broker.name}」`);
    state.reload();
  };

  const toggleConnect = async (cluster: DbzKafkaConnectVo, enabled: boolean) => {
    await kafkaConnectApi.modify({ id: cluster.id, enabled });
    message.success(`已${enabled ? '启用' : '停用'}「${cluster.name}」`);
    state.reload();
  };

  /** 把上游端改挂到新节点；连线类型不同写的字段不同 */
  const setUpstream = async (edge: CanvasEdge, target: CanvasNode) => {
    if (target.key === edge.from) {
      message.info('上游没有变化');
      return;
    }
    try {
      if (edge.relation.type === 'broker-connect') {
        await kafkaConnectApi.modify({
          id: edge.relation.connectId,
          kafkaBrokerId: target.id,
        });
      } else if (edge.relation.type === 'connect-connector') {
        if (edge.relation.connectorKind === 'source') {
          await sourceConnectorApi.modify({
            id: edge.relation.connectorId,
            kafkaConnectId: target.id,
          });
        } else {
          await sinkConnectorApi.modify({
            id: edge.relation.connectorId,
            kafkaConnectId: target.id,
          });
        }
      } else {
        await sinkConnectorApi.modify({ id: edge.relation.sinkId, sourceConnectorId: target.id });
      }
      message.success(`已把上游改为「${target.name}」；下游目标锁定不变`);
      state.reload();
    } catch (reason) {
      message.error(reasonText(reason, '更换上游失败'));
    }
  };

  /** 从节点锚点新建连线：归属改外键，数据流改 sink 的上游 */
  const createEdge = async (from: CanvasNode, to: CanvasNode) => {
    try {
      if (from.kind === 'broker' && to.kind === 'connect') {
        await kafkaConnectApi.modify({ id: to.id, kafkaBrokerId: from.id });
        message.success(`已把「${to.name}」挂到「${from.name}」`);
      } else if (from.kind === 'connect' && (to.kind === 'source' || to.kind === 'sink')) {
        if (to.kind === 'source') {
          await sourceConnectorApi.modify({ id: to.id, kafkaConnectId: from.id });
        } else {
          await sinkConnectorApi.modify({ id: to.id, kafkaConnectId: from.id });
        }
        message.success(`已把「${to.name}」挂到「${from.name}」`);
      } else if (from.kind === 'source' && to.kind === 'sink') {
        await sinkConnectorApi.modify({ id: to.id, sourceConnectorId: from.id });
        message.success(`已建立数据流「${from.name} → ${to.name}」`);
      } else {
        message.warning('这两种节点之间不支持连线');
        return;
      }
      state.reload();
    } catch (reason) {
      message.error(reasonText(reason, '建立连线失败'));
    }
  };

  const edgeDeleteRejected = () => {
    message.warning('连线不能删除。上游端可以拖到其他节点更换上游；要断开链路，请删除下游节点。');
  };

  /** 删除节点前的依赖说明：数据流与写入目标必须写清楚 */
  const deleteImpactOf = (node: CanvasNode) => {
    const direct = affectedEdgesOf(node, edges);
    const all = new Set(direct);
    if (node.kind === 'connect' || node.kind === 'broker') {
      const connectIds =
        node.kind === 'connect'
          ? new Set([(node.record as DbzKafkaConnectVo).id])
          : new Set(
              state.data.connects
                .filter((cluster) => cluster.kafkaBrokerId === (node.record as DbzKafkaBrokerVo).id)
                .map((cluster) => cluster.id),
            );
      const sourceKeys = new Set(
        state.data.sources
          .filter((source) => connectIds.has(source.kafkaConnectId))
          .map((source) => nodeKeyOf('source', source.id)),
      );
      const sinkKeys = new Set(
        state.data.sinks
          .filter((sink) => connectIds.has(sink.kafkaConnectId))
          .map((sink) => nodeKeyOf('sink', sink.id)),
      );
      for (const edge of edges) {
        if (edge.kind === 'flow' && (sourceKeys.has(edge.from) || sinkKeys.has(edge.to))) {
          all.add(edge);
        }
      }
    }
    return [...all];
  };

  const confirmDeleteNode = (node: CanvasNode) => {
    const impact = deleteImpactOf(node);
    const nodeMap = new Map(nodes.map((item) => [item.key, item]));
    const flows = impact
      .filter((edge) => edge.kind === 'flow')
      .map((edge) => ({
        source: nodeMap.get(edge.from)?.name ?? '未知上游',
        sink: nodeMap.get(edge.to)?.name ?? '未知下游',
        sinkRecord: nodeMap.get(edge.to)?.record as DbzSinkConnectorVo | undefined,
      }));
    const ownerships = impact.filter((edge) => edge.kind === 'ownership');
    const dependentChildren = impact.some((edge) => edge.kind === 'ownership' && edge.from === node.key);

    modal.confirm({
      title: `删除${kindLabel(node.kind)}「${node.name}」`,
      width: 560,
      okText: '删除节点',
      okButtonProps: { danger: true },
      cancelText: '取消',
      content: (
        <div className="dbz-delete-impact">
          {flows.length > 0 ? (
            <>
              <p>删除后会断开以下数据流，并影响对应的写入目标：</p>
              <ul>
                {flows.map((flow, index) => (
                  <li key={`${flow.source}-${flow.sink}-${index}`}>
                    <span className="dbz-mono">
                      {flow.source} → {flow.sink}
                    </span>
                    {flow.sinkRecord && (
                      <span>
                        ：写入目标 <strong>{flow.sink}</strong>（
                        {flow.sinkRecord.connectorName || '未命名连接器'}）不再接收数据
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            </>
          ) : (
            <p>这条记录没有下游数据流，删除不会断掉写入链路。</p>
          )}
          {ownerships.length > 0 && (
            <p>
              同时会移除 {ownerships.length} 条归属关系
              {dependentChildren ? '；服务端检测到仍有子记录时可能拒绝删除，请先处理依赖。' : '。'}
            </p>
          )}
          <Alert
            type="warning"
            showIcon
            message="删除不可恢复"
            description="连接器会同时从 Kafka Connect 注销；需要保留写入目标，请先改挂上游或另建下游。"
          />
        </div>
      ),
      onOk: async () => {
        setBusyNodeKey(node.key);
        try {
          if (node.kind === 'broker') {
            await kafkaBrokerApi.remove(node.id);
          } else if (node.kind === 'connect') {
            await kafkaConnectApi.remove(node.id);
          } else if (node.kind === 'source') {
            await sourceConnectorApi.remove(node.id);
          } else {
            await sinkConnectorApi.remove(node.id);
          }
          await controlPlaneApi.deleteLayout(node.nodeType, node.id).catch(() => undefined);
          setSelectedNodeKey(null);
          setSelectedEdgeId(null);
          message.success(`已删除「${node.name}」`);
          state.reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        } finally {
          setBusyNodeKey(null);
        }
      },
    });
  };

  const renderInspector = () => {
    if (selectedEdge) {
      const fromNode = nodes.find((node) => node.key === selectedEdge.from);
      const toNode = nodes.find((node) => node.key === selectedEdge.to);
      return (
        <div className="dbz-inspector-panel">
          <div className="dbz-inspector-head">
            <Typography.Title level={5}>连线属性</Typography.Title>
            <Tag color={selectedEdge.kind === 'flow' ? 'blue' : 'default'}>
              {selectedEdge.kind === 'flow' ? '数据流' : '归属'}
            </Tag>
          </div>
          <Descriptions
            column={1}
            size="small"
            items={[
              { key: 'from', label: '上游（可更换）', children: fromNode?.name ?? '未知' },
              { key: 'to', label: '下游（锁定）', children: toNode?.name ?? '未知' },
            ]}
          />
          <Alert
            type="info"
            showIcon
            message="下游目标不可更改"
            description={
              selectedEdge.kind === 'flow'
                ? '这条数据流由下游连接器的「上游连接器」决定。拖动连线上游端的圆形把手可以换到另一个 source；下游端不响应拖拽，连线也没有删除入口。'
                : '这条归属关系由下游记录的所属 Connect / 集群决定。拖动连线上游端的圆形把手可以换到另一台 Connect 或集群；下游端锁定，连线不能删除。'
            }
          />
          <div className="dbz-inspector-actions">
            <Button onClick={() => setSelectedEdgeId(null)}>取消选中</Button>
          </div>
          {toNode && (
            <Button type="link" onClick={() => setSelectedNodeKey(toNode.key)}>
              定位下游节点「{toNode.name}」
            </Button>
          )}
        </div>
      );
    }

    if (!selectedNode) {
      return (
        <div className="dbz-inspector-panel">
          <Typography.Title level={5}>属性检查器</Typography.Title>
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description="选中节点查看配置与操作；选中连线查看上下游与锁定说明。"
          />
          <div className="dbz-inspector-tip">
            <p>拖动连线上游端的圆形把手可以更换上游；方形锁标表示下游锁定。</p>
            <p>连线不能删除。要断开数据流，请删除下游节点，删除前会列出受影响的写入目标。</p>
          </div>
        </div>
      );
    }

    const node = selectedNode;
    const mismatch = node.mismatch;
    const detailButton = node.kind === 'source' || node.kind === 'sink';

    return (
      <div className="dbz-inspector-panel">
        <div className="dbz-inspector-head">
          <div>
            <Typography.Title level={5}>{node.name}</Typography.Title>
            <Tag>{kindLabel(node.kind)}</Tag>
          </div>
          <Button size="small" type="text" onClick={() => setSelectedNodeKey(null)}>
            关闭
          </Button>
        </div>

        {mismatch && (
          <Alert
            type="warning"
            showIcon
            icon={<WarningOutlined />}
            message="库内记录与实时状态不一致"
            description={mismatch}
          />
        )}

        {node.kind === 'broker' && (
          <>
            <Descriptions
              column={1}
              size="small"
              items={[
                {
                  key: 'status',
                  label: '状态',
                  children: (
                    <StatusTag
                      dic={kafkaBrokerStatusDic}
                      value={(node.record as DbzKafkaBrokerVo).runStatus}
                      detail={brokerDetailOf(node.record as DbzKafkaBrokerVo)}
                    />
                  ),
                },
                {
                  key: 'address',
                  label: '地址',
                  children: <span className="dbz-mono">{orDash((node.record as DbzKafkaBrokerVo).bootstrapServers)}</span>,
                },
                {
                  key: 'enabled',
                  label: '启用',
                  children: (node.record as DbzKafkaBrokerVo).enabled ? '是' : '否',
                },
              ]}
            />
            <div className="dbz-inspector-actions">
              <Button
                size="small"
                loading={checkingBrokerId === node.id}
                onClick={() => void checkBroker(node.record as DbzKafkaBrokerVo)}
              >
                检测
              </Button>
              <Button size="small" icon={<EditOutlined />} onClick={() => setEditTarget(node)}>
                编辑配置
              </Button>
              <Button size="small" type="link" onClick={() => navigate(nodeDetailPageOf(node.kind))}>
                明细
              </Button>
              <EnabledSwitch
                enabled={(node.record as DbzKafkaBrokerVo).enabled}
                name={node.name}
                confirmContent="禁用后这台集群不参与健康检查，挂在它上面的连接器发不了 Kafka 信号；重新启用后由下一轮检查重新判定状态。"
                onToggle={(enabled) => toggleBroker(node.record as DbzKafkaBrokerVo, enabled)}
              />
            </div>
          </>
        )}

        {node.kind === 'connect' && (
          <>
            <Descriptions
              column={1}
              size="small"
              items={[
                {
                  key: 'status',
                  label: '库内状态',
                  children: (
                    <StatusTag
                      dic={kafkaConnectStatusDic}
                      value={(node.record as DbzKafkaConnectVo).runStatus}
                      detail={connectDbDetailOf(node.record as DbzKafkaConnectVo)}
                    />
                  ),
                },
                {
                  key: 'health',
                  label: '实时健康',
                  children: node.health?.error ? (
                    <Tooltip title={node.health.error}>
                      <Tag color="error">探测失败</Tag>
                    </Tooltip>
                  ) : node.health?.health ? (
                    <Tag color={node.health.health.reachable ? 'success' : 'error'}>
                      {node.health.health.reachable
                        ? `v${node.health.health.version ?? '?'} · ${node.health.health.workerCount} worker · ${node.health.health.connectorCount} 连接器`
                        : '不可达'}
                    </Tag>
                  ) : (
                    <Tag>未探测</Tag>
                  ),
                },
                {
                  key: 'broker',
                  label: 'Kafka 集群',
                  children: node.broker?.name ?? '未知',
                },
                {
                  key: 'url',
                  label: '地址',
                  children: <span className="dbz-mono">{orDash((node.record as DbzKafkaConnectVo).url)}</span>,
                },
              ]}
            />
            <div className="dbz-inspector-actions">
              <Button size="small" icon={<EditOutlined />} onClick={() => setEditTarget(node)}>
                编辑配置
              </Button>
              <Button size="small" type="link" onClick={() => navigate('/kafka-connect')}>
                集群连接器
              </Button>
              <Button size="small" type="link" onClick={() => navigate(nodeDetailPageOf(node.kind))}>
                明细
              </Button>
              <EnabledSwitch
                enabled={(node.record as DbzKafkaConnectVo).enabled}
                name={node.name}
                confirmContent="禁用后这台集群不参与健康检查，名下连接器会先从集群上注销；重新启用后由下一轮检查重新部署。"
                onToggle={(enabled) => toggleConnect(node.record as DbzKafkaConnectVo, enabled)}
              />
            </div>
          </>
        )}

        {(node.kind === 'source' || node.kind === 'sink') && (() => {
          const record = node.record as ManagedConnectorRecord;
          const kind = node.kind;
          const topic = topicInfoOf(kind, record);
          const upstream =
            node.kind === 'sink'
              ? state.data.sources.find(
                  (source) => source.id === (node.record as DbzSinkConnectorVo).sourceConnectorId,
                )
              : undefined;
          return (
            <>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'status',
                    label: '实时状态',
                    children: node.liveIssue ? (
                      <Tooltip title={node.liveIssue}>
                        <Tag color="error">实时查询失败</Tag>
                      </Tooltip>
                    ) : node.live ? (
                      <Tooltip title={liveFailureReason(node.live) ?? ''}>
                        <Tag
                          color={
                            node.live.state === 0
                              ? 'success'
                              : node.live.state === 1
                                ? 'error'
                                : 'warning'
                          }
                        >
                          {liveStatusLabel(node.live.state)}
                        </Tag>
                      </Tooltip>
                    ) : (
                      <Tag color="warning">集群未部署</Tag>
                    ),
                  },
                  {
                    key: 'dbStatus',
                    label: '库内状态',
                    children: (
                      <StatusTag
                        dic={connectorRunStatusDic}
                        value={record.runStatus}
                        detail={connectorDbDetailOf(record, node.cluster)}
                      />
                    ),
                  },
                  ...(topic
                    ? [
                        {
                          key: 'topic',
                          label: topic.label,
                          children: <span className="dbz-mono">{topic.value}</span>,
                        },
                      ]
                    : []),
                  ...(node.kind === 'sink'
                    ? [
                        {
                          key: 'upstream',
                          label: '上游（可更换）',
                          children: upstream?.name ?? '未归属',
                        },
                      ]
                    : []),
                  {
                    key: 'cluster',
                    label: '所属 Connect',
                    children: node.cluster?.enabled ? node.cluster.name : `${node.cluster?.name ?? '未知'}（已停用）`,
                  },
                  {
                    key: 'dead',
                    label: '待处理死信',
                    children:
                      node.dead > 0 ? (
                        <Tag color="error" className="dbz-clickable" onClick={() => navigate('/dead-letter')}>
                          死信 {node.dead}
                        </Tag>
                      ) : (
                        '0'
                      ),
                  },
                ]}
              />
              {node.live && (
                <div className="dbz-inspector-live dbz-mono">
                  {liveFailureReason(node.live) ??
                    `运行正常 · worker ${node.live.workerId ?? '未知'} · 任务 ${node.live.tasks.length} 个`}
                </div>
              )}
              <div className="dbz-inspector-actions">
                {detailButton && (
                  <Button
                    size="small"
                    onClick={() =>
                      setDetail({ kind: kind as ConnectorKind, record })
                    }
                  >
                    详情
                  </Button>
                )}
                <Button size="small" icon={<EditOutlined />} onClick={() => setEditTarget(node)}>
                  编辑配置
                </Button>
                <Button size="small" type="link" onClick={() => navigate(detailPageOf(kind))}>
                  明细
                </Button>
                {record.enabled && node.live && !node.liveIssue && !node.connectDisabled && (
                  <Popconfirm
                    title="重启连接器？"
                    description="会连同任务一起重启，Kafka Connect 异步执行。"
                    okText="重启"
                    cancelText="取消"
                    onConfirm={() => void restartConnector(record)}
                  >
                    <Button
                      size="small"
                      type="link"
                      icon={<SyncOutlined />}
                      loading={restartingKey === connectorKey(record)}
                    >
                      重启
                    </Button>
                  </Popconfirm>
                )}
                <EnabledSwitch
                  enabled={record.enabled}
                  name={record.name}
                  confirmContent={TOGGLE_CONFIRM}
                  onToggle={(enabled) => toggleConnector(kind, record, enabled)}
                />
              </div>
            </>
          );
        })()}

        <div className="dbz-inspector-footer">
          <Button
            danger
            icon={<DeleteOutlined />}
            loading={busyNodeKey === node.key}
            onClick={() => confirmDeleteNode(node)}
          >
            删除节点
          </Button>
        </div>
        <div className="dbz-inspector-tip">
          <p>
            <LockHint /> 下游目标不可更改；连线不能删除。
          </p>
        </div>
      </div>
    );
  };

  return (
    <div className="dbz-canvas-page">
      <div className="dbz-canvas-topbar">
        <div className="dbz-canvas-title">
          <Typography.Title level={4}>控制面视图</Typography.Title>
          <span className="dbz-canvas-caption">
            实时状态以 Connect REST 为准；库内记录只做对照
          </span>
        </div>
        <Space size={12} wrap>
          <Tag>{nodes.length} 个节点</Tag>
          {mismatchTotal > 0 && <Tag color="warning">{mismatchTotal} 处状态不一致</Tag>}
          {deadLetterTotal > 0 && (
            <Tag color="error" className="dbz-clickable" onClick={() => navigate('/dead-letter')}>
              死信 {deadLetterTotal}
            </Tag>
          )}
          <Tag color={healthSummary.unreachable > 0 ? 'error' : 'success'}>
            Connect 可达 {healthSummary.reachable}/{healthSummary.total}
          </Tag>
          <span className="dbz-canvas-loaded">
            采集于 {state.data.loadedAt ? new Date(state.data.loadedAt).toLocaleTimeString() : '--'}
          </span>
          <Button
            icon={<ReloadOutlined />}
            loading={state.loading}
            onClick={() => state.reload()}
          >
            刷新
          </Button>
        </Space>
      </div>

      {state.error && (
        <Alert
          type="error"
          showIcon
          closable
          message={`加载控制面数据失败：${state.error.message}`}
          className="dbz-canvas-error"
        />
      )}

      {state.loading && !state.data.loadedAt ? (
        <div className="dbz-canvas-loading">
          {/* 裸用 Spin 的 tip 会被 antd 忽略并告警，文案单独放 */}
          <Spin />
          <Typography.Text type="secondary">正在读取节点与实时状态…</Typography.Text>
        </div>
      ) : (
        <ControlPlaneCanvas
          nodes={nodes}
          edges={edges}
          positions={resolvedPositions}
          selectedNodeKey={selectedNodeKey}
          selectedEdgeId={selectedEdgeId}
          onNodeMove={handleNodeMove}
          onSelectNode={(key) => {
            setSelectedNodeKey(key);
            setSelectedEdgeId(null);
          }}
          onSelectEdge={(id) => {
            setSelectedEdgeId(id);
            setSelectedNodeKey(null);
          }}
          onCreateEdge={(from, to) => void createEdge(from, to)}
          onReconnectEdge={(edge, target) => void setUpstream(edge, target)}
          onCreateAt={handleCreateAt}
          onEdgeDeleteRejected={edgeDeleteRejected}
          renderInspector={renderInspector}
        />
      )}

      <KafkaBrokerForm
        open={createKind === 'broker' || editTarget?.kind === 'broker'}
        record={editTarget?.kind === 'broker' ? (editTarget.record as DbzKafkaBrokerVo) : null}
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />
      <KafkaConnectForm
        open={createKind === 'connect' || editTarget?.kind === 'connect'}
        record={editTarget?.kind === 'connect' ? (editTarget.record as DbzKafkaConnectVo) : null}
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />
      <SourceConnectorForm
        open={createKind === 'source' || editTarget?.kind === 'source'}
        record={
          editTarget?.kind === 'source' ? (editTarget.record as DbzSourceConnectorVo) : null
        }
        clusterOptions={clusterOptions}
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />
      <SinkConnectorForm
        open={createKind === 'sink' || editTarget?.kind === 'sink'}
        record={editTarget?.kind === 'sink' ? (editTarget.record as DbzSinkConnectorVo) : null}
        clusterOptions={clusterOptions}
        sourceOptions={sourceOptions}
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />

      <ConnectorDetailDrawer
        target={detail}
        cluster={
          detail
            ? state.data.connects.find((cluster) => cluster.id === detail.record.kafkaConnectId)
            : undefined
        }
        broker={
          detail
            ? state.data.brokers.find(
                (broker) =>
                  broker.id ===
                  state.data.connects.find(
                    (cluster) => cluster.id === detail.record.kafkaConnectId,
                  )?.kafkaBrokerId,
              )
            : undefined
        }
        upstream={
          detail && detail.kind === 'sink'
            ? state.data.sources.find(
                (source) => source.id === (detail.record as DbzSinkConnectorVo).sourceConnectorId,
              )
            : undefined
        }
        live={
          detail
            ? state.data.live
                .get(detail.record.kafkaConnectId)
                ?.connectors.find((connector) => connector.name === detail.record.connectorName)
            : undefined
        }
        liveIssue={detail ? state.data.live.get(detail.record.kafkaConnectId)?.error : undefined}
        restarting={detail ? restartingKey === connectorKey(detail.record) : false}
        onClose={() => setDetail(null)}
        onRestart={() => {
          if (detail) {
            void restartConnector(detail.record);
          }
        }}
        onToggle={async (enabled) => {
          if (detail) {
            await toggleConnector(detail.kind, detail.record, enabled);
          }
        }}
      />
    </div>
  );
}

/** 检查器底部的小锁提示图标 */
function LockHint() {
  return <LockOutlined className="dbz-inspector-lock" />;
}
