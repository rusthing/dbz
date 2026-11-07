/**
 * # 控制面视图
 *
 * 只选上游、其余自动推导：
 * - 新增下级只走节点底部的「+」，父级固定且表单里不出现；
 * - sink 表单只选上游连接器，所属 Connect / Kafka 集群由上游自动带出；
 * - 画布只画归属线（Kafka 集群 → Connect → 上游 → 下游），连线只作展示，
 *   没有拖拽改上游、也没有删除入口；
 * - 改 sink 上游统一在表单里选 source。
 *
 * 节点可自由摆放，空白平移、以指针为中心缩放、右下角缩略图；右侧是属性检查器。
 * 启用/停用、重启、编辑、删除、集群检测、查看死信都放在节点卡片上；属性检查器只负责
 * 展示库内配置、实时状态、任务与异常，不再另开详情抽屉。
 *
 * 节点位置按「节点类型 + 记录 ID」存 `dbz_control_plane_layout`，拖动松手即保存，
 * 多人打开同一套连接器看到一致布局。
 */

import {
  PlusOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import {
  Alert,
  App as AntdApp,
  Button,
  Checkbox,
  Descriptions,
  Empty,
  InputNumber,
  Modal,
  Space,
  Spin,
  Switch,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  controlPlaneApi,
  type DbzResetConnectorOffsetsVo,
} from '../../api/control-plane';
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
import StatusTag from '../../components/StatusTag';
import { connectorRunStatusDic, kafkaConnectorStatusDic } from '../../dic';
import { useAsync, type AsyncState } from '../../hooks/useAsync';
import { errorMessage, formatMs, orDash, prettyJson, truncate } from '../../utils/format';
import ControlPlaneCanvas from './ControlPlaneCanvas';
import {
  NODE_SIZE,
  NODE_TYPE_OF,
  affectedEdgesOf,
  buildEdges,
  defaultPosition,
  flattenNodes,
  nodeKeyOf,
  type CanvasNode,
  type CanvasNodeAction,
  type CanvasNodeKind,
} from './canvas-model';
import {
  authLabelOf,
  brokerDetailOf,
  topicInfoOf,
} from './details';
import {
  buildControlPlane,
  liveFailureReason,
  taskSummary,
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

/** Kafka 集群的库内状态折算成统一枚举 */
function brokerStatusOf(broker: DbzKafkaBrokerVo): number {
  if (!broker.enabled) return 2;
  if (broker.runStatus === 1) return 1;
  if (broker.runStatus === 3) return 5;
  return 0;
}

/** Connect 的库内状态与实时健康折算成统一枚举 */
function connectStatusOf(node: CanvasNode): number {
  if (!node.cluster?.enabled) return 2;
  if (node.health?.error || node.health?.health?.reachable === false || node.liveIssue) return 5;
  if (node.health?.health?.reachable) return 1;
  return 0;
}

/** 连接器统一状态：已停用优先，其余取后端回写的 run_status */
function connectorStatusOf(node: CanvasNode): number {
  const record = node.record as ManagedConnectorRecord;
  if (!record.enabled || node.connectDisabled) {
    return 2;
  }
  return record.runStatus;
}

/** 读取连接器配置里的一个字符串项（配置不是 JSON 时返回空串） */
function configValueOf(record: { connectorConfig: string }, key: string): string {
  try {
    const parsed: unknown = JSON.parse(record.connectorConfig);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return '';
    }
    const value = (parsed as Record<string, unknown>)[key];
    return value === null || value === undefined ? '' : String(value).trim();
  } catch {
    return '';
  }
}

/**
 * 下游清空位点后的起点由连接器自己的 `auto.offset.reset` 决定：
 * earliest 才会从头重放，latest 直接跳到末尾。提示必须按实际配置给出，
 * 不能统一按 earliest 吓人或按 latest 隐瞒。
 */
function sinkOffsetHint(record: DbzSinkConnectorVo): string {
  const value = configValueOf(record, 'auto.offset.reset').toLowerCase();
  if (value === 'earliest') {
    return '该连接器的 auto.offset.reset=earliest：清空位点后会从 topic 最早位置重新消费，历史消息会全部重投下游，可能造成重复写入与死信。';
  }
  if (value === 'latest') {
    return '该连接器的 auto.offset.reset=latest：清空位点后会直接跳到 topic 当前末尾，历史消息不会补投，等于放弃这段区间的数据。';
  }
  return `该连接器未显式配置 auto.offset.reset（值为「${value || '未配置'}」），按 Kafka 默认 latest 处理：清空位点后会跳到 topic 当前末尾，历史消息不会补投，等于放弃这段区间的数据。`;
}

/** 上游清空位点会触发的快照行为 */
function sourceSnapshotHint(record: DbzSourceConnectorVo): string {
  const mode = configValueOf(record, 'snapshot.mode') || '未配置';
  return `当前 snapshot.mode=${mode}：清空位点后会重新做一次全量快照，整库整表重读，下游会被重灌一遍。耗时与源库压力取决于数据量，请在低峰期执行。`;
}

/** 重置位点的步骤文案 */
function resetStepLabel(step?: string | null): string {
  switch (step) {
    case 'stop':
      return '停止连接器';
    case 'clear':
      return '清空位点';
    case 'resume':
      return '恢复运行';
    case 'confirm':
      return '执行前确认';
    default:
      return step || '未知步骤';
  }
}

/** 自动刷新配置：默认 15 秒，0 或关闭开关表示停用 */
interface RefreshSetting {
  enabled: boolean;
  seconds: number;
}

const REFRESH_STORAGE_KEY = 'dbz.control-plane.refresh';
const DEFAULT_REFRESH: RefreshSetting = { enabled: true, seconds: 15 };

function loadRefreshSetting(): RefreshSetting {
  try {
    const raw = window.localStorage.getItem(REFRESH_STORAGE_KEY);
    if (!raw) {
      return DEFAULT_REFRESH;
    }
    const parsed = JSON.parse(raw) as Partial<RefreshSetting>;
    return {
      enabled: typeof parsed.enabled === 'boolean' ? parsed.enabled : DEFAULT_REFRESH.enabled,
      seconds:
        typeof parsed.seconds === 'number' && Number.isFinite(parsed.seconds)
          ? Math.max(0, Math.min(600, parsed.seconds))
          : DEFAULT_REFRESH.seconds,
    };
  } catch {
    return DEFAULT_REFRESH;
  }
}

function saveRefreshSetting(setting: RefreshSetting): void {
  try {
    window.localStorage.setItem(REFRESH_STORAGE_KEY, JSON.stringify(setting));
  } catch {
    // 隐私模式下写不进去，只在本次会话生效
  }
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

/**
 * # 轻量刷新
 *
 * 自动刷新与手动刷新只做两件事：让后端把统一状态回写 `run_status`，再重取四张表与
 * 死信计数。不重新探测 Connect 实时列表、不重取布局，因此不会重排画布、不会重置
 * 选中节点与属性框，也不会打断正在编辑的表单；失败时保留上一份数据。
 */
async function loadLightStatus(): Promise<{
  brokers: DbzKafkaBrokerVo[];
  connects: DbzKafkaConnectVo[];
  sources: DbzSourceConnectorVo[];
  sinks: DbzSinkConnectorVo[];
  deadLetter: Map<string, number>;
}> {
  // 回写失败不影响展示：卡片沿用上一份 run_status，进入页面时也走这条降级路径
  await controlPlaneApi.refreshStatus().catch(() => undefined);

  const [brokers, connects, sources, sinks, deadLetterCounts] = await Promise.all([
    kafkaBrokerApi.list(),
    kafkaConnectApi.list(),
    sourceConnectorApi.list(),
    sinkConnectorApi.list(),
    deadLetterApi.pendingCount().catch(() => [] as DbzDeadLetterCountVo[]),
  ]);

  return {
    brokers,
    connects,
    sources,
    sinks,
    deadLetter: new Map(
      deadLetterCounts.map((item) => [
        deadLetterKey(item.connectorType, item.connectorId),
        item.count,
      ]),
    ),
  };
}

/** 连接器在页面上的唯一键（重启 loading 用） */
function connectorKey(record: ManagedConnectorRecord): string {
  return `${record.kafkaConnectId}:${record.connectorName}`;
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

/**
 * # 新增下级时的落位
 *
 * 从「+」新增没有指针落点：默认放在父节点正下方，和已有节点重叠时左右错开，
 * 保证新建的卡片不会盖住同排的其它卡片。
 */
function childPositionOf(
  kind: CanvasNodeKind,
  parent: CanvasNode | null,
  nodes: CanvasNode[],
  positions: Record<string, Point>,
): Point {
  if (!parent) {
    const index = nodes.filter((node) => node.kind === kind).length;
    return defaultPosition(kind, index);
  }
  const parentPos = positions[parent.key] ?? defaultPosition(parent.kind, 0);
  const parentSize = NODE_SIZE[parent.kind];
  const size = NODE_SIZE[kind];
  const baseX = parentPos.x + parentSize.w / 2 - size.w / 2;
  const baseY = parentPos.y + parentSize.h + 28;

  const overlaps = (point: Point) =>
    nodes.some((node) => {
      if (node.key === parent.key) {
        return false;
      }
      const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
      const other = NODE_SIZE[node.kind];
      return !(
        point.x + size.w <= pos.x ||
        point.x >= pos.x + other.w ||
        point.y + size.h <= pos.y ||
        point.y >= pos.y + other.h
      );
    });

  for (const offset of [0, -1, 1, -2, 2, -3, 3]) {
    const point = { x: baseX + offset * 280, y: baseY };
    if (!overlaps(point)) {
      return point;
    }
  }
  return { x: baseX, y: baseY };
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
  const [checkingBrokerId, setCheckingBrokerId] = useState<Id | null>(null);
  const [restartingKey, setRestartingKey] = useState<string | null>(null);
  const [busyNodeKey, setBusyNodeKey] = useState<string | null>(null);
  const [togglingNodeKey, setTogglingNodeKey] = useState<string | null>(null);
  /** 自动刷新：默认 15 秒，可调秒数或关闭，选择持久化 */
  const [refreshSetting, setRefreshSetting] = useState<RefreshSetting>(loadRefreshSetting);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const [pageVisible, setPageVisible] = useState(
    () => document.visibilityState === 'visible',
  );
  const entryRefreshed = useRef(false);

  /** 重置位点弹窗 */
  const [resetTarget, setResetTarget] = useState<CanvasNode | null>(null);
  const [resetResume, setResetResume] = useState(true);
  const [resetConfirmed, setResetConfirmed] = useState(false);
  const [resetRunning, setResetRunning] = useState(false);
  const [resetResult, setResetResult] = useState<DbzResetConnectorOffsetsVo | null>(null);

  /** 新增：父级固定（顶层 Kafka 集群没有父级），成功后从新记录里找出 ID 再落位 */
  const [createKind, setCreateKind] = useState<CanvasNodeKind | null>(null);
  const [createParent, setCreateParent] = useState<CanvasNode | null>(null);
  const [pendingDrop, setPendingDrop] = useState<{
    kind: CanvasNodeKind;
    x: number;
    y: number;
    existingIds: Set<Id>;
  } | null>(null);
  const [editTarget, setEditTarget] = useState<CanvasNode | null>(null);

  /** 轻量刷新：只合并状态与死信计数，保留实时明细、布局与选中态 */
  const refreshLight = useCallback(async () => {
    const next = await loadLightStatus();
    state.setData((previous) => ({ ...previous, ...next, loadedAt: Date.now() }));
  }, [state.setData]);

  /** 进入页面触发一次状态回写；失败不阻塞渲染，沿用库内最后已知状态 */
  useEffect(() => {
    if (entryRefreshed.current) {
      return;
    }
    entryRefreshed.current = true;
    void controlPlaneApi
      .refreshStatus()
      .then(() => state.reload())
      .catch(() => undefined);
  }, [state.reload]);

  /** 页面不可见时暂停轮询，回来再恢复；不在后台发无意义请求 */
  useEffect(() => {
    const onVisibilityChange = () => setPageVisible(document.visibilityState === 'visible');
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => document.removeEventListener('visibilitychange', onVisibilityChange);
  }, []);

  const refreshSeconds = refreshSetting.enabled ? refreshSetting.seconds : 0;

  useEffect(() => {
    if (!pageVisible || refreshSeconds <= 0) {
      return;
    }
    const timer = window.setInterval(() => {
      void refreshLight().catch(() => undefined);
    }, refreshSeconds * 1000);
    return () => window.clearInterval(timer);
  }, [pageVisible, refreshSeconds, refreshLight]);

  const updateRefreshSetting = (next: Partial<RefreshSetting>) => {
    const merged: RefreshSetting = {
      enabled: next.enabled ?? refreshSetting.enabled,
      seconds:
        next.seconds === undefined
          ? refreshSetting.seconds
          : Math.max(0, Math.min(600, Math.round(next.seconds))),
    };
    setRefreshSetting(merged);
    saveRefreshSetting(merged);
  };

  /** 手动刷新：立即回写一次状态，期间只禁用刷新按钮，不出现整页 loading */
  const handleManualRefresh = async () => {
    setManualRefreshing(true);
    try {
      await refreshLight();
      message.success('已刷新状态与死信数量');
    } catch (reason) {
      message.warning(`刷新失败，沿用最后已知状态：${errorMessage(reason)}`);
    } finally {
      setManualRefreshing(false);
    }
  };

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

  const nodes = useMemo(
    () => flattenNodes(tree, deadCountOf, state.data.sources),
    [tree, state.data.deadLetter, state.data.sources],
  );

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

  /**
   * 打开新增表单：父级固定，子类型按父级收敛。
   * 只有顶层 Kafka 集群没有父级（顶栏的「新增 Kafka 集群」）。
   */
  const startCreate = (kind: CanvasNodeKind, parent: CanvasNode | null) => {
    const existingIds = new Set(recordsOfKind(state.data, kind).map((record) => record.id));
    const point = childPositionOf(kind, parent, nodes, resolvedPositions);
    setPendingDrop({ kind, x: point.x, y: point.y, existingIds });
    setEditTarget(null);
    setCreateParent(parent);
    setCreateKind(kind);
  };

  /** 节点底部「+」：按当前节点决定只能新增哪类下级 */
  const handleAddChild = (node: CanvasNode) => {
    switch (node.kind) {
      case 'broker':
        startCreate('connect', node);
        break;
      case 'connect':
        startCreate('source', node);
        break;
      case 'source':
        startCreate('sink', node);
        break;
      default:
        break;
    }
  };

  const cancelForm = () => {
    setCreateKind(null);
    setCreateParent(null);
    setEditTarget(null);
    setPendingDrop(null);
  };

  const formSuccess = () => {
    setCreateKind(null);
    setCreateParent(null);
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

  /**
   * # 卡片开关：直接启用/停用
   *
   * 开关是受控的（`checked` 来自服务端记录）：请求期间显示 loading 并禁用重复点击；
   * 失败时不改本地状态，重新拉一次数据把开关回滚到服务端的真实值。
   */
  const applyToggle = async (node: CanvasNode, enabled: boolean) => {
    setTogglingNodeKey(node.key);
    try {
      if (node.kind === 'broker') {
        await kafkaBrokerApi.modify({ id: node.id, enabled });
      } else if (node.kind === 'connect') {
        await kafkaConnectApi.modify({ id: node.id, enabled });
      } else if (node.kind === 'source') {
        await sourceConnectorApi.modify({ id: node.id, enabled });
      } else {
        await sinkConnectorApi.modify({ id: node.id, enabled });
      }
      message.success(`已${enabled ? '启用' : '停用'}「${node.name}」`);
      state.reload();
    } catch (reason) {
      message.error(`${enabled ? '启用' : '停用'}失败：${errorMessage(reason)}`);
      // 开关保持服务端值；重新拉数据确保与服务端一致
      state.reload();
    } finally {
      setTogglingNodeKey(null);
    }
  };

  /** 删除节点前提示会移除的归属关系 */
  const deleteImpactOf = (node: CanvasNode) => affectedEdgesOf(node, edges);

  const confirmDeleteNode = (node: CanvasNode) => {
    const ownerships = deleteImpactOf(node);
    const dependentChildren = ownerships.some((edge) => edge.from === node.key);

    modal.confirm({
      title: `删除${kindLabel(node.kind)}「${node.name}」`,
      width: 520,
      okText: '删除节点',
      okButtonProps: { danger: true },
      cancelText: '取消',
      content: (
        <div className="dbz-delete-impact">
          {ownerships.length > 0 ? (
            <p>
              删除后会移除 {ownerships.length} 条归属关系
              {dependentChildren ? '；服务端检测到仍有子记录时可能拒绝删除，请先处理依赖。' : '。'}
            </p>
          ) : (
            <p>这个节点没有关联的归属关系。</p>
          )}
          <Alert
            type="warning"
            showIcon
            message="删除不可恢复"
            description="连接器会同时从 Kafka Connect 注销；需要保留下游连接器，请先改挂到其他 Connect。"
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

  /** 卡片上的重启：会连同任务一起重启 */
  const confirmRestartNode = (node: CanvasNode) => {
    const record = node.record as ManagedConnectorRecord;
    modal.confirm({
      title: `重启「${record.name}」？`,
      content: '会连同任务一起重启，Kafka Connect 异步执行，状态稍后刷新。',
      okText: '重启',
      cancelText: '取消',
      onOk: () => restartConnector(record),
    });
  };

  /** 打开重置位点弹窗：默认勾选「清空后立即恢复运行」 */
  const openResetOffsets = (node: CanvasNode) => {
    setResetTarget(node);
    setResetResume(true);
    setResetConfirmed(false);
    setResetResult(null);
  };

  const closeResetOffsets = () => {
    if (resetRunning) {
      return;
    }
    setResetTarget(null);
    setResetResult(null);
  };

  /**
   * # 执行重置位点
   *
   * 三步由后端串行完成（stop → delete offsets → 可选 resume），前端只提交意图。
   * 失败时展示停在哪一步、连接器当时处于什么状态，并刷新一次状态让卡片与实际一致。
   */
  const executeResetOffsets = async () => {
    if (!resetTarget) {
      return;
    }
    setResetRunning(true);
    try {
      const result = await controlPlaneApi.resetOffsets({
        connectorType: resetTarget.kind === 'source' ? 'source' : 'sink',
        connectorId: resetTarget.id,
        resume: resetResume,
        confirmReplay: resetTarget.kind === 'sink' ? resetConfirmed : true,
      });
      setResetResult(result);
      if (result.ok) {
        message.success(
          resetResume ? '已清空位点并恢复运行，状态稍后回写' : '已清空位点，连接器保持停止',
        );
      }
      await refreshLight().catch(() => undefined);
    } catch (reason) {
      setResetResult({
        ok: false,
        message: errorMessage(reason),
        connectorState: '未知',
        steps: [],
      });
    } finally {
      setResetRunning(false);
    }
  };

  /** 卡片操作统一入口：卡片只负责点击，确认与写库都在页面里 */
  const handleNodeAction = (node: CanvasNode, action: CanvasNodeAction) => {
    switch (action) {
      case 'restart':
        confirmRestartNode(node);
        break;
      case 'reset-offsets':
        openResetOffsets(node);
        break;
      case 'edit':
        setEditTarget(node);
        break;
      case 'delete':
        confirmDeleteNode(node);
        break;
      case 'check':
        void checkBroker(node.record as DbzKafkaBrokerVo);
        break;
      case 'dead-letter':
        // 复用死信页深链：按连接器类型 + id 预筛选
        navigate(
          `/dead-letter?connectorType=${deadLetterTypeOf(node.kind as ConnectorKind)}&connectorId=${node.id}`,
        );
        break;
    }
  };

  const renderInspector = () => {
    if (selectedEdge) {
      const fromNode = nodes.find((node) => node.key === selectedEdge.from);
      const toNode = nodes.find((node) => node.key === selectedEdge.to);
      return (
        <div className="dbz-inspector-panel">
          <div className="dbz-inspector-head">
            <Typography.Title level={5}>归属连线</Typography.Title>
            <Tag>归属</Tag>
          </div>
          <Descriptions
            column={1}
            size="small"
            items={[
              { key: 'from', label: '上游', children: fromNode?.name ?? '未知' },
              { key: 'to', label: '下游', children: toNode?.name ?? '未知' },
            ]}
          />
          <Alert
            type="info"
            showIcon
            message="归属线只作展示"
            description="连线由下游记录的上游字段决定：下游连接器的上游在 sink 表单里选择；其它节点由「+」新增时的父节点固定。连线不能拖动，也不能删除。"
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
            description="选中节点查看库内配置、统一状态、任务与异常；选中连线查看上下游归属说明。"
          />
          <div className="dbz-inspector-tip">
            <p>新增下级走节点底部的「+」按钮；下游连接器的上游在 sink 表单里选择。</p>
            <p>归属线只作展示，不能拖动，也不能删除。</p>
          </div>
        </div>
      );
    }

    const node = selectedNode;

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

        {node.kind === 'broker' && (() => {
          const broker = node.record as DbzKafkaBrokerVo;
          return (
            <>
              <Typography.Title level={5} className="dbz-inspector-section">
                库内配置
              </Typography.Title>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'address',
                    label: '地址',
                    children: <span className="dbz-mono">{orDash(broker.bootstrapServers)}</span>,
                  },
                  { key: 'auth', label: '认证', children: authLabelOf(broker) },
                  { key: 'enabled', label: '启用', children: broker.enabled ? '是' : '否' },
                  { key: 'update', label: '最近检查', children: formatMs(broker.updateMs) },
                  { key: 'remark', label: '备注', children: orDash(broker.remark) },
                ]}
              />
              <Typography.Title level={5} className="dbz-inspector-section">
                状态
              </Typography.Title>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'status',
                    label: '状态',
                    children: (
                      <StatusTag
                        dic={connectorRunStatusDic}
                        value={brokerStatusOf(broker)}
                        detail={brokerDetailOf(broker)}
                      />
                    ),
                  },
                  {
                    key: 'checked',
                    label: '最后检查时间',
                    children: formatMs(broker.updateMs),
                  },
                  {
                    key: 'reason',
                    label: '失败原因',
                    children: orDash(broker.runMsg),
                  },
                ]}
              />
              {broker.runMsg && (
                <Alert
                  type={broker.runStatus === 3 ? 'error' : 'warning'}
                  showIcon
                  message="最近一次检测结论"
                  description={broker.runMsg}
                />
              )}
            </>
          );
        })()}

        {node.kind === 'connect' && (() => {
          const cluster = node.record as DbzKafkaConnectVo;
          return (
            <>
              <Typography.Title level={5} className="dbz-inspector-section">
                库内配置
              </Typography.Title>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'url',
                    label: '地址',
                    children: <span className="dbz-mono">{orDash(cluster.url)}</span>,
                  },
                  { key: 'broker', label: 'Kafka 集群', children: node.broker?.name ?? '未知' },
                  { key: 'enabled', label: '启用', children: cluster.enabled ? '是' : '否' },
                  { key: 'update', label: '最近检查', children: formatMs(cluster.updateMs) },
                  { key: 'remark', label: '备注', children: orDash(cluster.remark) },
                ]}
              />
              <Typography.Title level={5} className="dbz-inspector-section">
                状态
              </Typography.Title>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'status',
                    label: '状态',
                    children: (
                      <StatusTag
                        dic={connectorRunStatusDic}
                        value={connectStatusOf(node)}
                      />
                    ),
                  },
                  {
                    key: 'checked',
                    label: '最后检查时间',
                    children: formatMs(cluster.updateMs),
                  },
                  {
                    key: 'reason',
                    label: '失败原因',
                    children: orDash(
                      cluster.runMsg || node.liveIssue || node.health?.error,
                    ),
                  },
                  {
                    key: 'health',
                    label: '健康探测',
                    children: node.health?.error ? (
                      <Tooltip title={node.health.error}>
                        <span>探测失败</span>
                      </Tooltip>
                    ) : node.health?.health ? (
                      node.health.health.reachable
                        ? `v${node.health.health.version ?? '?'} · ${node.health.health.workerCount} worker · ${node.health.health.connectorCount} 连接器`
                        : '不可达'
                    ) : (
                      '未探测'
                    ),
                  },
                ]}
              />
              {node.liveIssue && (
                <Alert
                  type="error"
                  showIcon
                  message="连接器查询失败"
                  description={node.liveIssue}
                />
              )}
            </>
          );
        })()}

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
          const { abnormal, total } = node.live
            ? taskSummary(node.live)
            : { abnormal: 0, total: 0 };
          const failure = node.live ? liveFailureReason(node.live) : null;
          return (
            <>
              <Typography.Title level={5} className="dbz-inspector-section">
                库内配置
              </Typography.Title>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'connectorName',
                    label: '连接器名称',
                    children: <span className="dbz-mono">{orDash(record.connectorName)}</span>,
                  },
                  {
                    key: 'cluster',
                    label: '所属 Connect',
                    children: node.cluster?.enabled
                      ? node.cluster.name
                      : `${node.cluster?.name ?? '未知'}（已停用）`,
                  },
                  { key: 'broker', label: 'Kafka 集群', children: node.broker?.name ?? '未知' },
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
                          label: '上游连接器',
                          children: upstream?.name ?? '未归属',
                        },
                      ]
                    : []),
                  {
                    key: 'status',
                    label: '状态',
                    children: (
                      <StatusTag dic={connectorRunStatusDic} value={connectorStatusOf(node)} />
                    ),
                  },
                  { key: 'enabled', label: '启用', children: record.enabled ? '是' : '否' },
                  {
                    key: 'checked',
                    label: '最后检查时间',
                    children: formatMs(record.updateMs),
                  },
                  {
                    key: 'reason',
                    label: '失败原因',
                    children: orDash(record.runMsg),
                  },
                  { key: 'remark', label: '备注', children: orDash(record.remark) },
                  {
                    key: 'dead',
                    label: '待处理死信',
                    children:
                      node.dead > 0 ? (
                        <Tag
                          color="error"
                          className="dbz-clickable"
                          onClick={() =>
                            navigate(
                              `/dead-letter?connectorType=${deadLetterTypeOf(node.kind as ConnectorKind)}&connectorId=${node.id}`,
                            )
                          }
                        >
                          死信 {node.dead}
                        </Tag>
                      ) : (
                        '0'
                      ),
                  },
                ]}
              />
              <pre className="dbz-json-view">{prettyJson(record.connectorConfig) || '--'}</pre>

              {record.runStatus === 5 && (
                <Alert
                  type="warning"
                  showIcon
                  message="Connect 当前不可达"
                  description={`上面展示的是最后一次回写的状态（${formatMs(
                    record.updateMs,
                  )}），不代表此刻的真实运行情况；Connect 恢复后会自动刷新。`}
                />
              )}
              {record.runStatus === 2 && (
                <Alert
                  type="info"
                  showIcon
                  message="连接器已停用"
                  description="已停用的连接器不发起实时请求，状态直接显示为已停用，也不会有新的任务与异常。"
                />
              )}

              <Typography.Title level={5} className="dbz-inspector-section">
                任务与异常
              </Typography.Title>
              <Descriptions
                column={1}
                size="small"
                items={[
                  {
                    key: 'worker',
                    label: 'Worker',
                    children: <span className="dbz-mono">{orDash(node.live?.workerId)}</span>,
                  },
                  {
                    key: 'class',
                    label: '连接器类',
                    children: (
                      <span className="dbz-mono">{orDash(node.live?.connectorClass)}</span>
                    ),
                  },
                  {
                    key: 'tasks',
                    label: '任务',
                    children:
                      total === 0
                        ? '无'
                        : `${total} 个${abnormal > 0 ? `（异常 ${abnormal} 个）` : ''}`,
                  },
                ]}
              />
              {node.liveIssue && (
                <Alert
                  type="error"
                  showIcon
                  message="实时查询失败"
                  description={node.liveIssue}
                />
              )}

              {node.live && node.live.tasks.length > 0 ? (
                <Table
                  size="small"
                  rowKey="id"
                  pagination={false}
                  dataSource={node.live.tasks}
                  columns={[
                    {
                      title: '任务',
                      dataIndex: 'id',
                      width: 58,
                      render: (id: number) => `#${id}`,
                    },
                    {
                      title: '状态',
                      dataIndex: 'state',
                      width: 84,
                      render: (state: number) => (
                        <StatusTag dic={kafkaConnectorStatusDic} value={state} />
                      ),
                    },
                    {
                      title: '异常',
                      dataIndex: 'trace',
                      ellipsis: true,
                      render: (trace?: string | null) =>
                        trace ? (
                          <Tooltip title={trace}>
                            <span className="dbz-ellipsis">{truncate(trace, 80)}</span>
                          </Tooltip>
                        ) : (
                          '--'
                        ),
                    },
                  ]}
                />
              ) : (
                <Typography.Text type="secondary">
                  {node.live ? '没有任务。' : '集群上没有该连接器，拿不到任务与异常。'}
                </Typography.Text>
              )}
              {failure && (
                <Alert type="error" showIcon message="异常原因" description={failure} />
              )}
            </>
          );
        })()}

        <div className="dbz-inspector-footer">
          <Button size="small" type="link" onClick={() => navigate(nodeDetailPageOf(node.kind))}>
            打开明细页
          </Button>
          {(node.kind === 'source' || node.kind === 'sink') && node.dead > 0 && (
            <Button
              size="small"
              type="link"
              onClick={() =>
                navigate(
                  `/dead-letter?connectorType=${deadLetterTypeOf(node.kind as ConnectorKind)}&connectorId=${node.id}`,
                )
              }
            >
              处理死信
            </Button>
          )}
        </div>
        <div className="dbz-inspector-tip">
          <p>归属线只作展示：新增下级走「+」，sink 上游在表单里选择。</p>
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
            状态由健康检查与进入页面刷新回写，卡片只展示一个统一状态
          </span>
        </div>
        <Space size={12} wrap>
          <Tag>{nodes.length} 个节点</Tag>
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
          <span className="dbz-canvas-refresh" data-refresh-setting>
            <Switch
              size="small"
              checked={refreshSetting.enabled}
              onChange={(checked) => updateRefreshSetting({ enabled: checked })}
            />
            <InputNumber
              size="small"
              min={0}
              max={600}
              value={refreshSetting.seconds}
              onChange={(value) => updateRefreshSetting({ seconds: value ?? 0 })}
              style={{ width: 72 }}
            />
            <span className="dbz-canvas-refresh-unit">秒</span>
          </span>
          <Button icon={<PlusOutlined />} onClick={() => startCreate('broker', null)}>
            新增 Kafka 集群
          </Button>
          <Button
            icon={<ReloadOutlined />}
            loading={manualRefreshing}
            onClick={() => void handleManualRefresh()}
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
          onAddChild={handleAddChild}
          onNodeAction={handleNodeAction}
          onToggleNode={(node, enabled) => void applyToggle(node, enabled)}
          busyNodeKey={busyNodeKey}
          togglingNodeKey={togglingNodeKey}
          checkingBrokerId={checkingBrokerId}
          restartingKey={restartingKey}
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
        fixedKafkaBrokerId={
          !editTarget && createParent?.kind === 'broker' ? createParent.id : null
        }
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />
      <SourceConnectorForm
        open={createKind === 'source' || editTarget?.kind === 'source'}
        record={
          editTarget?.kind === 'source' ? (editTarget.record as DbzSourceConnectorVo) : null
        }
        fixedKafkaConnectId={
          !editTarget && createParent?.kind === 'connect' ? createParent.id : null
        }
        clusterOptions={clusterOptions}
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />
      <SinkConnectorForm
        open={createKind === 'sink' || editTarget?.kind === 'sink'}
        record={editTarget?.kind === 'sink' ? (editTarget.record as DbzSinkConnectorVo) : null}
        fixedSource={
          !editTarget && createParent?.kind === 'source'
            ? {
                id: createParent.id,
                kafkaConnectId: (createParent.record as DbzSourceConnectorVo).kafkaConnectId,
              }
            : null
        }
        onCancel={cancelForm}
        onSuccess={formSuccess}
      />

      <Modal
        open={resetTarget !== null}
        title={resetTarget ? `重置位点「${resetTarget.name}」` : '重置位点'}
        onCancel={closeResetOffsets}
        maskClosable={!resetRunning}
        destroyOnHidden
        footer={
          resetResult
            ? [
                <Button key="close" onClick={closeResetOffsets}>
                  关闭
                </Button>,
              ]
            : [
                <Button key="cancel" onClick={closeResetOffsets} disabled={resetRunning}>
                  取消
                </Button>,
                <Button
                  key="ok"
                  type="primary"
                  danger
                  loading={resetRunning}
                  disabled={resetTarget?.kind === 'sink' && !resetConfirmed}
                  onClick={() => void executeResetOffsets()}
                >
                  {resetResume ? '清空并恢复运行' : '清空位点'}
                </Button>,
              ]
        }
      >
        {resetTarget && (
          <div className="dbz-reset-offsets">
            <Alert
              type="warning"
              showIcon
              message={
                resetTarget.kind === 'sink'
                  ? '清空后按 auto.offset.reset 决定起点，可能造成历史消息重放'
                  : '重置后会重新做全量快照，下游会被重灌一遍'
              }
              description={
                resetTarget.kind === 'sink'
                  ? sinkOffsetHint(resetTarget.record as DbzSinkConnectorVo)
                  : sourceSnapshotHint(resetTarget.record as DbzSourceConnectorVo)
              }
            />
            <p className="dbz-reset-order">
              执行顺序：停止连接器 → 删除位点 → （按勾选）恢复运行。任一步失败会停下并说明停在哪一步。
            </p>
            <Checkbox
              checked={resetResume}
              disabled={resetRunning}
              onChange={(event) => setResetResume(event.target.checked)}
            >
              清空后立即恢复运行
            </Checkbox>
            {resetTarget.kind === 'sink' && (
              <Checkbox
                checked={resetConfirmed}
                disabled={resetRunning}
                onChange={(event) => setResetConfirmed(event.target.checked)}
              >
                我知道会重放历史消息、可能产生重复写入与死信
              </Checkbox>
            )}

            {resetResult && (
              <Alert
                className="dbz-reset-result"
                type={resetResult.ok ? 'success' : 'error'}
                showIcon
                message={
                  resetResult.ok
                    ? `执行完成，当前状态：${resetResult.connectorState}`
                    : `第「${resetStepLabel(resetResult.failedStep)}」步失败：${resetResult.message}`
                }
                description={`当前连接器状态：${resetResult.connectorState}`}
              />
            )}
            {resetResult && resetResult.steps.length > 0 && (
              <div className="dbz-reset-steps">
                {resetResult.steps.map((step) => (
                  <div key={step.step} className="dbz-reset-step">
                    <Tag color={step.ok ? 'success' : 'error'}>
                      {resetStepLabel(step.step)}
                    </Tag>
                    <span>{step.message || (step.ok ? '完成' : '失败')}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </Modal>

    </div>
  );
}
