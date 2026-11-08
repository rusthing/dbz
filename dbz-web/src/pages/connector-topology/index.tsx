/**
 * # 连接器拓扑
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
  DownOutlined,
  PlusOutlined,
  ReloadOutlined,
  SearchOutlined,
  UpOutlined,
} from '@ant-design/icons';
import {
  Alert,
  App as AntdApp,
  Button,
  Checkbox,
  Descriptions,
  Empty,
  Input,
  InputNumber,
  Modal,
  Radio,
  Select,
  Space,
  Spin,
  Switch,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type HTMLAttributes,
} from 'react';
import { useNavigate } from 'react-router-dom';
import { notifySessionExpired } from '../../auth/session';
import { sm3 } from '../../lib/sm3';
import { backupApi } from '../../api/backup';
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
  BackupRestoreStrategy,
  DbzBackupRestoreItemVo,
  DbzBackupRestoreVo,
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
import ConnectorTopologyCanvas from './ConnectorTopologyCanvas';
import {
  backupFileName,
  buildImportRows,
  buildTransferFile,
  downloadJson,
  exportFileName,
  nextImportedName,
  parseTransferFile,
  type ImportDecision,
  type ImportOutcome,
  type ImportPreviewRow,
  type TransferConnector,
  type TransferFile,
} from './connect-transfer';
import {
  NODE_SIZE,
  NODE_TYPE_OF,
  affectedEdgesOf,
  buildEdges,
  defaultPosition,
  flattenNodes,
  nodeKeyOf,
  nodeSearchText,
  type CanvasNode,
  type CanvasNodeAction,
  type CanvasNodeKind,
} from './canvas-model';
import {
  authLabelOf,
  brokerDetailOf,
  connectorConfigValueOf,
  topicInfoOf,
} from './details';
import {
  buildConnectorTopology,
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
import SnapshotModal from '../source-connector/SnapshotModal';
import SourceConnectorForm from '../source-connector/SourceConnectorForm';

/** 实时状态查询的最长等待时间：单台 Connect 卡住不能拖死整个视图 */
const LIVE_TIMEOUT_MS = 15_000;

interface ConnectorTopologyData {
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
  /** 已保存的连线折点：edgeId → 中间水平段 Y */
  edgeBends: Record<string, number>;
  /** 本次实时采集时间 */
  loadedAt: number;
}

const EMPTY_DATA: ConnectorTopologyData = {
  brokers: [],
  connects: [],
  sources: [],
  sinks: [],
  live: new Map(),
  health: new Map(),
  deadLetter: new Map(),
  layout: [],
  edgeBends: {},
  loadedAt: 0,
};

/** 一次导入会话：文件、预览行与执行结果都放在同一份状态里 */
interface ImportSession {
  target: CanvasNode;
  file: TransferFile;
  fileName: string;
  phase: 'preview' | 'running' | 'result';
  /** 预览阶段正在读取目标 Connect 的实时连接器 */
  loadingPreview: boolean;
  rows: ImportPreviewRow[];
  liveNames: Set<string> | null;
  liveWarning?: string;
  outcomes: ImportOutcome[];
}

/** 导入结果分类文案：汇总里必须把「覆盖并纳管」与「覆盖已有纳管记录」分开 */
const IMPORT_ACTION_LABEL: Record<ImportOutcome['action'], string> = {
  created: '新增',
  'overwritten-managed': '覆盖已有纳管记录',
  'overwritten-adopted': '覆盖并纳管',
  skipped: '跳过',
  'kept-both': '改名保留',
  failed: '失败',
};

const IMPORT_DECISION_LABEL: Record<ImportDecision, string> = {
  overwrite: '覆盖',
  skip: '跳过',
  keep: '两者都保留',
};

const IMPORT_ACTION_COLOR: Record<ImportOutcome['action'], string> = {
  created: 'success',
  'overwritten-managed': 'geekblue',
  'overwritten-adopted': 'blue',
  skipped: 'default',
  'kept-both': 'purple',
  failed: 'error',
};

/**
 * 一次恢复会话
 *
 * 预览与执行都走同一个后端入口：文件内容原样回传，`dryRun` 决定是否写库。
 * 切换策略时重新取一次预览，保证界面上的计数与真正执行时的判定一致。
 */
interface RestoreSession {
  fileName: string;
  /** 原始 JSON（已通过格式校验，原样回传后端） */
  file: unknown;
  strategy: BackupRestoreStrategy;
  phase: 'preview' | 'running' | 'result';
  /** 切换策略后正在重新计算预览 */
  loadingPreview: boolean;
  preview: DbzBackupRestoreVo;
  result: DbzBackupRestoreVo | null;
}

/** 恢复明细的层级文案 */
const RESTORE_LEVEL_LABEL: Record<DbzBackupRestoreItemVo['level'], string> = {
  broker: 'Kafka 集群',
  connect: 'Connect',
  source: '上游连接器',
  sink: '下游连接器',
};

const RESTORE_LEVEL_COLOR: Record<DbzBackupRestoreItemVo['level'], string> = {
  broker: 'blue',
  connect: 'purple',
  source: 'cyan',
  sink: 'magenta',
};

const RESTORE_ACTION_LABEL: Record<DbzBackupRestoreItemVo['action'], string> = {
  create: '新增',
  overwrite: '覆盖',
  skip: '跳过',
  failed: '失败',
};

const RESTORE_ACTION_COLOR: Record<DbzBackupRestoreItemVo['action'], string> = {
  create: 'success',
  overwrite: 'geekblue',
  skip: 'default',
  failed: 'error',
};

/** 异常口径与卡片红色状态一致：失败与不可达，已停止/仅纳管不算异常 */
function connectorStatusAbnormal(runStatus: number): boolean {
  return runStatus === 3 || runStatus === 5;
}

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

/**
 * 下游清空位点后的起点由连接器自己的 `auto.offset.reset` 决定：
 * earliest 才会从头重放，latest 直接跳到末尾。提示必须按实际配置给出，
 * 不能统一按 earliest 吓人或按 latest 隐瞒。
 */
function sinkOffsetHint(record: DbzSinkConnectorVo): string {
  const value = connectorConfigValueOf(record, 'auto.offset.reset').toLowerCase();
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
  const mode = connectorConfigValueOf(record, 'snapshot.mode') || '未配置';
  return `当前 snapshot.mode=${mode}：清空位点后会重新做一次全量快照，整库整表重读，下游会被重灌一遍。耗时与源库压力取决于数据量，请在低峰期执行。`;
}

/**
 * # 一致性检测要比对的关键配置项
 *
 * 只比两侧都存在（或只有一侧存在）的键，不要求整套 Debezium 配置逐字段相同：
 * 版本不同会多出无关键，逐字段比会把正常差异误报成失败。这里覆盖用户真正会
 * 关心的连接信息、topic 范围与快照模式。
 */
const CONSISTENCY_CONFIG_KEYS = [
  'connector.class',
  'tasks.max',
  'database.hostname',
  'database.port',
  'database.user',
  'database.dbname',
  'database.url',
  'topic.prefix',
  'database.server.name',
  'table.include.list',
  'table.exclude.list',
  'topics',
  'topics.regex',
  'snapshot.mode',
];

/** 配置值归一化成可比较的字符串：数组/对象按 JSON，标量按字符串 */
function normalizeConfigValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'string') {
    return value.trim();
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

/** 解析库内 `connectorConfig`；非法 JSON 返回 null，由调用方给出明确原因 */
function parseConnectorConfig(raw: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return null;
    }
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** 比对库内配置与 Connect 实际配置，返回人类可读的差异列表 */
function configDiffOf(
  stored: Record<string, unknown>,
  actual: Record<string, unknown>,
): string[] {
  const diffs: string[] = [];
  const keys = new Set([...CONSISTENCY_CONFIG_KEYS, ...Object.keys(actual)]);
  for (const key of keys) {
    if (!CONSISTENCY_CONFIG_KEYS.includes(key)) {
      continue;
    }
    const left = normalizeConfigValue(stored[key]);
    const right = normalizeConfigValue(actual[key]);
    if (left !== right) {
      diffs.push(`${key}：库内「${left || '未配置'}」，Connect 实际「${right || '未配置'}」`);
    }
  }
  return diffs;
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

const REFRESH_STORAGE_KEY = 'dbz.connector-topology.refresh';
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
async function loadConnectorTopology(): Promise<ConnectorTopologyData> {
  const [brokers, connects, sources, sinks, deadLetterCounts, layout, edgeBends] =
    await Promise.all([
      kafkaBrokerApi.list(),
      kafkaConnectApi.list(),
      sourceConnectorApi.list(),
      sinkConnectorApi.list(),
      // 死信计数只影响角标，接口失败时降级为空，不拖垮整页
      deadLetterApi.pendingCount().catch(() => [] as DbzDeadLetterCountVo[]),
      // 布局接口失败时按默认网格摆放，不阻塞节点数据
      controlPlaneApi.listLayout().catch(() => []),
      // 折点接口失败时按默认居中走线，不影响节点
      controlPlaneApi.listEdgeBends().catch(() => []),
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

  return {
    brokers,
    connects,
    sources,
    sinks,
    live,
    health,
    deadLetter,
    layout,
    edgeBends: Object.fromEntries(edgeBends.map((item) => [item.edgeId, item.midY])),
    loadedAt: Date.now(),
  };
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
  data: ConnectorTopologyData,
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

export default function ConnectorTopologyPage() {
  const { message, modal } = AntdApp.useApp();
  const navigate = useNavigate();
  const state: AsyncState<ConnectorTopologyData> = useAsync(loadConnectorTopology, [], EMPTY_DATA);

  const [positions, setPositions] = useState<Record<string, Point>>({});
  /** 连线折点：本地即时更新，松手才落库 */
  const [edgeBends, setEdgeBends] = useState<Record<string, number>>({});
  const [selectedNodeKey, setSelectedNodeKey] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [checkingKey, setCheckingKey] = useState<string | null>(null);
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

  /** 上游「重新同步」：复用快照弹窗，默认增量、可多选表 */
  const [resyncTarget, setResyncTarget] = useState<DbzSourceConnectorVo | null>(null);

  /** 删除：当前登录用户审核口令弹窗；校验通过前不动任何数据 */
  const [deleteTarget, setDeleteTarget] = useState<CanvasNode | null>(null);
  const [deletePassword, setDeletePassword] = useState('');
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteRunning, setDeleteRunning] = useState(false);

  /** 画布搜索：只定位与高亮，不隐藏节点（搜索范围限定在当前集群 tab） */
  const [searchText, setSearchText] = useState('');
  const [searchCursor, setSearchCursor] = useState(0);
  const [searchFocusToken, setSearchFocusToken] = useState(0);

  /** 当前集群 tab；null 表示还没选过，按第一个集群兜底 */
  const [activeBrokerKey, setActiveBrokerKey] = useState<Id | null>(null);

  /** 导出弹窗：默认带凭据，脱敏由用户显式勾选 */
  const [exportTarget, setExportTarget] = useState<CanvasNode | null>(null);
  const [exportRedacted, setExportRedacted] = useState(false);

  /** 导入：文件选择与预览/执行结果 */
  const [importSession, setImportSession] = useState<ImportSession | null>(null);
  const importFileRef = useRef<HTMLInputElement>(null);
  const pendingImportTarget = useRef<CanvasNode | null>(null);

  /** 集群备份：确认导出整个 Kafka 集群 */
  const [backupTarget, setBackupTarget] = useState<CanvasNode | null>(null);
  const [backupRunning, setBackupRunning] = useState(false);

  /** 集群恢复：文件、策略与预览/执行结果 */
  const [restoreSession, setRestoreSession] = useState<RestoreSession | null>(null);
  const restoreFileRef = useRef<HTMLInputElement>(null);

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
      buildConnectorTopology(
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
    () => flattenNodes(tree, deadCountOf),
    [tree, state.data.deadLetter],
  );

  const edges = useMemo(
    () => buildEdges(state.data.brokers, state.data.connects, state.data.sources, state.data.sinks),
    [state.data.brokers, state.data.connects, state.data.sinks, state.data.sources],
  );

  /**
   * # 按 Kafka 集群分 tab
   *
   * tab 只统计当前集群下 Connect 的连接器数量与异常数（失败 / 不可达）。
   * 全局汇总（集群总数、死信总数）留在 tab 栏外；当前 tab 的节点数、Connect
   * 可达数与死信数在画布区顶部展示。
   */
  const brokerTabs = useMemo(() => {
    const connectsByBroker = new Map<Id, DbzKafkaConnectVo[]>();
    for (const cluster of state.data.connects) {
      const bucket = connectsByBroker.get(cluster.kafkaBrokerId) ?? [];
      bucket.push(cluster);
      connectsByBroker.set(cluster.kafkaBrokerId, bucket);
    }
    return state.data.brokers.map((broker) => {
      const connectIds = new Set((connectsByBroker.get(broker.id) ?? []).map((item) => item.id));
      const sourceRecords = state.data.sources.filter((item) =>
        connectIds.has(item.kafkaConnectId),
      );
      const sinkRecords = state.data.sinks.filter((item) =>
        connectIds.has(item.kafkaConnectId),
      );
      return {
        id: broker.id,
        name: broker.name,
        connectorCount: sourceRecords.length + sinkRecords.length,
        abnormalCount: [...sourceRecords, ...sinkRecords].filter((item) =>
          connectorStatusAbnormal(item.runStatus),
        ).length,
      };
    });
  }, [state.data.brokers, state.data.connects, state.data.sources, state.data.sinks]);

  /** 当前 tab：选中的集群被删掉或首次进入时回落到第一个集群 */
  const resolvedBrokerId = useMemo(() => {
    if (activeBrokerKey && brokerTabs.some((tab) => tab.id === activeBrokerKey)) {
      return activeBrokerKey;
    }
    return brokerTabs[0]?.id ?? null;
  }, [activeBrokerKey, brokerTabs]);

  /** 画布只拿当前集群的节点，切 tab 不串数据 */
  const scopeNodes = useMemo(
    () => nodes.filter((node) => node.broker?.id === resolvedBrokerId),
    [nodes, resolvedBrokerId],
  );

  const scopeEdges = useMemo(() => {
    const keys = new Set(scopeNodes.map((node) => node.key));
    return edges.filter((edge) => keys.has(edge.from) && keys.has(edge.to));
  }, [edges, scopeNodes]);

  /** 服务端保存的位置；没有位置的节点按默认网格补 */
  useEffect(() => {
    const next: Record<string, Point> = {};
    for (const item of state.data.layout) {
      next[`${item.nodeType}:${item.nodeId}`] = { x: item.x, y: item.y };
    }
    setPositions(next);
  }, [state.data.layout]);

  /** 服务端保存的连线折点；拖动中的本地值以最新一次为准 */
  useEffect(() => {
    setEdgeBends(state.data.edgeBends);
  }, [state.data.edgeBends]);

  /** 搜索命中：只作用于当前集群 tab，按名称 / topic / connectorName 匹配 */
  const searchHits = useMemo(() => {
    const query = searchText.trim().toLowerCase();
    if (!query) {
      return [];
    }
    return scopeNodes
      .filter((node) => nodeSearchText(node).includes(query))
      .map((node) => node.key);
  }, [scopeNodes, searchText]);

  const activeHitKey = searchHits.length
    ? searchHits[Math.min(searchCursor, searchHits.length - 1)]
    : null;

  /**
   * 有持久化位置就用，没有就按当前 tab 内同类节点的序号摆。
   * 序号只数当前集群，切 tab 时默认位置重新从 0 开始，不会沿用另一个集群的排布。
   */
  const resolvedPositions = useMemo(() => {
    const result: Record<string, Point> = { ...positions };
    const counters: Record<CanvasNodeKind, number> = { broker: 0, connect: 0, source: 0, sink: 0 };
    for (const node of scopeNodes) {
      const index = counters[node.kind];
      counters[node.kind] += 1;
      if (!result[node.key]) {
        result[node.key] = defaultPosition(node.kind, index);
      }
    }
    return result;
  }, [scopeNodes, positions]);

  const selectedNode = useMemo(
    () => scopeNodes.find((node) => node.key === selectedNodeKey) ?? null,
    [scopeNodes, selectedNodeKey],
  );
  const selectedEdge = useMemo(
    () => scopeEdges.find((edge) => edge.id === selectedEdgeId) ?? null,
    [scopeEdges, selectedEdgeId],
  );

  /** 节点被删除后清掉选中态，避免检查器挂在空记录上 */
  useEffect(() => {
    if (selectedNodeKey && !scopeNodes.some((node) => node.key === selectedNodeKey)) {
      setSelectedNodeKey(null);
    }
  }, [scopeNodes, selectedNodeKey]);
  useEffect(() => {
    if (selectedEdgeId && !scopeEdges.some((edge) => edge.id === selectedEdgeId)) {
      setSelectedEdgeId(null);
    }
  }, [scopeEdges, selectedEdgeId]);

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
    // 新建 Kafka 集群会多出一个 tab，直接切过去，否则新节点落在看不见的 tab 里
    if (pendingDrop.kind === 'broker') {
      setActiveBrokerKey(created.id);
    }
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

  /** 当前 tab 的 Connect 健康汇总：只统计启用中的记录 */
  const healthSummary = useMemo(() => {
    const enabled = scopeNodes.filter(
      (node) => node.kind === 'connect' && node.cluster?.enabled,
    );
    const reachable = enabled.filter((node) => node.health?.health?.reachable).length;
    const unreachable = enabled.filter(
      (node) =>
        node.health?.error || (node.health?.health && !node.health.health.reachable),
    ).length;
    return { total: enabled.length, reachable, unreachable };
  }, [scopeNodes]);

  /** 全局待处理死信总数：留在 tab 栏外 */
  const deadLetterTotal = useMemo(
    () => nodes.reduce((sum, node) => sum + node.dead, 0),
    [nodes],
  );

  /** 当前 tab 的待处理死信：画布区汇总 */
  const scopeDeadLetterTotal = useMemo(
    () => scopeNodes.reduce((sum, node) => sum + node.dead, 0),
    [scopeNodes],
  );

  const activeBrokerName =
    brokerTabs.find((tab) => tab.id === resolvedBrokerId)?.name ?? '--';

  /** 导入结果按分类汇总：覆盖两种口径必须分开统计 */
  const importStats = useMemo(() => {
    const counts: Record<ImportOutcome['action'], number> = {
      created: 0,
      'overwritten-managed': 0,
      'overwritten-adopted': 0,
      skipped: 0,
      'kept-both': 0,
      failed: 0,
    };
    for (const outcome of importSession?.outcomes ?? []) {
      counts[outcome.action] += 1;
    }
    return counts;
  }, [importSession?.outcomes]);

  const importConflictCount =
    importSession?.rows.filter((item) => item.conflict).length ?? 0;

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
    const node = scopeNodes.find((item) => item.key === key);
    if (!node) {
      return;
    }
    void controlPlaneApi
      .saveLayout({ nodeType: node.nodeType, nodeId: node.id, x, y })
      .catch((reason: unknown) => {
        message.error(`布局保存失败：${errorMessage(reason)}`);
      });
  };

  /** 拖动连线折点：过程只改本地，松手才落库，刷新后仍保持 */
  const handleEdgeBend = (edgeId: string, midY: number, commit: boolean) => {
    setEdgeBends((previous) => ({ ...previous, [edgeId]: midY }));
    if (!commit) {
      return;
    }
    void controlPlaneApi.saveEdgeBend({ edgeId, midY }).catch((reason: unknown) => {
      message.error(`折点保存失败：${errorMessage(reason)}`);
    });
  };

  /** 恢复默认居中走线：只删折点，连线本身保留 */
  const resetEdgeBend = (edgeId: string) => {
    setEdgeBends((previous) => {
      const next = { ...previous };
      delete next[edgeId];
      return next;
    });
    void controlPlaneApi.resetEdgeBend(edgeId).catch((reason: unknown) => {
      message.error(`恢复默认走线失败：${errorMessage(reason)}`);
    });
  };

  /** 搜索输入：立即定位第一个命中，清空时回到无命中状态 */
  const handleSearchChange = (value: string) => {
    setSearchText(value);
    setSearchCursor(0);
    setSearchFocusToken((token) => token + 1);
    const query = value.trim().toLowerCase();
    if (!query) {
      return;
    }
    const first = scopeNodes.find((node) => nodeSearchText(node).includes(query));
    if (first) {
      setSelectedNodeKey(first.key);
      setSelectedEdgeId(null);
    }
  };

  /** 上一个/下一个命中：循环切换并同步选中，画布负责居中 */
  const stepSearch = (delta: number) => {
    if (!searchHits.length) {
      return;
    }
    const next = (searchCursor + delta + searchHits.length) % searchHits.length;
    setSearchCursor(next);
    setSearchFocusToken((token) => token + 1);
    setSelectedNodeKey(searchHits[next]);
    setSelectedEdgeId(null);
  };

  const clearSearch = () => {
    setSearchText('');
    setSearchCursor(0);
  };

  /**
   * 打开新增表单：父级固定，子类型按父级收敛。
   * 只有顶层 Kafka 集群没有父级（顶栏的「新增 Kafka 集群」）。
   */
  const startCreate = (kind: CanvasNodeKind, parent: CanvasNode | null) => {
    const existingIds = new Set(recordsOfKind(state.data, kind).map((record) => record.id));
    const point = childPositionOf(kind, parent, scopeNodes, resolvedPositions);
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

  /** 集群检测：只测 Kafka 连通性与 topic 数 */
  const checkBroker = async (broker: DbzKafkaBrokerVo) => {
    const result = await kafkaBrokerApi.check(broker.id);
    if (result.reachable) {
      message.success(
        `集群检测通过：topic ${result.topicCount} 个，耗时 ${Math.round(result.elapsedMs)} ms`,
      );
    } else {
      modal.warning({
        title: `Kafka 集群「${broker.name}」检测未通过`,
        content: result.message || '无法连接到 Kafka 集群，请检查地址、认证与网络。',
      });
    }
    state.reload();
  };

  /** Connect 集群检测：可达性 / 版本 / worker / 插件，并同步纳管记录 */
  const checkConnect = async (node: CanvasNode) => {
    const cluster = node.record as DbzKafkaConnectVo;
    // 健康状态与插件清单一起取：插件缺失是新建连接器失败的常见原因。
    const [health, plugins] = await Promise.all([
      kafkaConnectApi.health(cluster.id),
      kafkaConnectorApi.plugins(cluster.id),
    ]);
    if (!health.reachable) {
      modal.warning({
        title: `Kafka Connect「${cluster.name}」检测未通过`,
        content: health.message || 'Connect 不可达，请检查地址、网络与服务状态。',
      });
      return;
    }

    // 可达才同步：以远端为准补录缺失、标记配置不一致、把已消失的置为禁用。
    const detect = await kafkaConnectApi.detect(cluster.id);
    const actionLabel: Record<string, string> = {
      added: '补录',
      marked: '标记不一致',
      disabled: '置禁用',
      cleared: '恢复一致',
      skipped: '跳过',
    };
    const notable = detect.items.filter((item) => item.action !== 'unchanged');

    modal.info({
      title: `Kafka Connect「${cluster.name}」检测通过`,
      width: 760,
      content: (
        <div className="dbz-connect-detect">
          <p>
            v{health.version ?? '?'}，{health.workerCount} 个 worker，{health.connectorCount}{' '}
            个连接器，已安装 {plugins.length} 个插件，耗时 {Math.round(health.elapsedMs)} ms
          </p>
          <p>
            同步结果：补录 {detect.added}，标记不一致 {detect.marked}，置禁用 {detect.disabled}
            ，恢复一致 {detect.cleared}，无变化 {detect.unchanged}，跳过 {detect.skipped}
          </p>
          {notable.length > 0 && (
            <div className="dbz-connect-detect-sync">
              {notable.map((item) => (
                <div
                  className="dbz-connect-detect-sync-row"
                  key={`${item.connectorType}:${item.connectorName}:${item.action}`}
                >
                  <Tag color={item.action === 'skipped' ? 'default' : 'warning'}>
                    {actionLabel[item.action] ?? item.action}
                  </Tag>
                  <span className="dbz-mono" title={item.connectorName}>
                    {item.connectorName}
                  </span>
                  <span className="dbz-connect-detect-sync-msg">{item.message || '—'}</span>
                </div>
              ))}
            </div>
          )}
          <div className="dbz-connect-detect-plugins">
            <div className="dbz-connect-detect-head">
              <span>插件类名</span>
              <span>方向</span>
              <span>版本</span>
            </div>
            {plugins.length === 0 ? (
              <div className="dbz-connect-detect-empty">未安装任何连接器插件</div>
            ) : (
              <div className="dbz-connect-detect-body">
                {plugins.map((plugin) => (
                  <div className="dbz-connect-detect-row" key={`${plugin.class}:${plugin.type}`}>
                    <span className="dbz-mono" title={plugin.class}>
                      {plugin.class}
                    </span>
                    <span>{plugin.type || '—'}</span>
                    <span className="dbz-mono">{plugin.version || '—'}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      ),
    });
    state.reload();
  };

  /**
   * 连接器一致性检测：库内配置 vs Connect 实际配置 + 启停状态
   *
   * 比 `connector.class`、连接信息、topic 范围等关键项；状态只比「启停是否一致」，
   * 不拿库内回写的 run_status 与远端逐状态比对，避免把刷新时序当成不一致。
   */
  const checkConnectorConsistency = async (node: CanvasNode) => {
    const record = node.record as ManagedConnectorRecord;
    const stored = parseConnectorConfig(record.connectorConfig);
    if (!stored) {
      modal.warning({
        title: `一致性检测未通过：${record.name}`,
        content: '库内 connectorConfig 不是合法的 JSON 对象，无法与 Connect 实际配置比对。请先在编辑表单里修正配置。',
      });
      return;
    }

    // 配置与实时列表并行取：任一失败都能给出具体原因
    const [actual, liveList] = await Promise.all([
      kafkaConnectorApi.config(record.kafkaConnectId, record.connectorName),
      kafkaConnectorApi.list(record.kafkaConnectId),
    ]);

    const diffs = configDiffOf(stored, actual.config);
    const live = liveList.find((item) => item.name === record.connectorName);
    if (!live) {
      diffs.unshift('Connect 上不存在该连接器，但库内仍在纳管');
    } else if (record.enabled && live.state !== 0) {
      const stateLabel =
        kafkaConnectorStatusDic.find((dic) => dic.value === live.state)?.label ?? `状态 ${live.state}`;
      diffs.unshift(`库内为启用，Connect 实际为「${stateLabel}」`);
    } else if (!record.enabled && live.state === 0) {
      diffs.unshift('库内为停用，但 Connect 上连接器仍在运行');
    }

    if (diffs.length === 0) {
      message.success(
        `一致性检测通过：${record.name} 的库内配置与 Connect 实际一致（连接器类、关键配置、启停状态）`,
      );
    } else {
      modal.warning({
        title: `一致性检测未通过：${record.name}`,
        width: 640,
        content: (
          <div className="dbz-consistency-diffs">
            <p>库内记录与 Connect 实际有 {diffs.length} 处不一致：</p>
            <ul>
              {diffs.map((diff) => (
                <li key={diff}>{diff}</li>
              ))}
            </ul>
          </div>
        ),
      });
    }
  };

  /** 四类卡片共用的「检测」入口 */
  const checkNode = async (node: CanvasNode) => {
    setCheckingKey(node.key);
    try {
      if (node.kind === 'broker') {
        await checkBroker(node.record as DbzKafkaBrokerVo);
      } else if (node.kind === 'connect') {
        await checkConnect(node);
      } else {
        await checkConnectorConsistency(node);
      }
    } catch (reason) {
      modal.error({
        title: `检测失败：${node.name}`,
        content: errorMessage(reason),
      });
    } finally {
      setCheckingKey(null);
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

  /** 打开删除口令弹窗：先确认影响，再要求输入当前登录用户审核口令 */
  const openDeleteNode = (node: CanvasNode) => {
    setDeleteTarget(node);
    setDeletePassword('');
    setDeleteError(null);
  };

  const closeDeleteNode = () => {
    if (deleteRunning) {
      return;
    }
    setDeleteTarget(null);
    setDeletePassword('');
    setDeleteError(null);
  };

  /**
   * # 执行删除
   *
   * 后端先带上当前用户身份转发 adm-rs 校验审核口令，通过后才删除并写审计；失败时
   * 一行数据都不动。前端按提示前缀区分「审核口令错误」「审核口令未设置」与
   * 「认证服务不可用」，后者不允许删除，也不能提示成口令错。
   */
  const executeDeleteNode = async () => {
    if (!deleteTarget) {
      return;
    }
    const password = deletePassword.trim();
    if (!password) {
      setDeleteError('审核口令错误：请输入当前登录用户的审核口令');
      return;
    }
    setDeleteRunning(true);
    setBusyNodeKey(deleteTarget.key);
    setDeleteError(null);
    try {
      // 口令明文不出浏览器：先做一次 SM3，后端只转发摘要给 adm-rs
      await controlPlaneApi.deleteNode({
        nodeType: deleteTarget.kind,
        nodeId: deleteTarget.id,
        auditPswd: sm3(password),
      });
      message.success(`已删除「${deleteTarget.name}」，删除人与时间已写入审计`);
      setDeleteTarget(null);
      setDeletePassword('');
      setSelectedNodeKey(null);
      setSelectedEdgeId(null);
      state.reload();
    } catch (reason) {
      const text = errorMessage(reason);
      if (text.includes('登录状态已失效')) {
        // 令牌失效：清会话并回登录页，删除未执行
        notifySessionExpired();
        setDeleteError(`登录状态已失效，删除未执行：${text}`);
      } else if (text.includes('审核口令未设置')) {
        // 用户还没设置审核口令：删除无法进行，需要先到用户管理里设置
        setDeleteError(
          `审核口令未设置，删除未执行：${text}。请先在用户管理中为当前用户设置审核口令。`,
        );
      } else if (text.includes('审核口令校验过于频繁')) {
        // adm 频控响应不返回剩余秒数，只能提示默认窗口，避免编造倒计时
        setDeleteError(
          `审核口令校验过于频繁，删除未执行：${text}。认证服务默认 10 分钟内最多允许 5 次错误，请等待窗口结束后重试。`,
        );
      } else if (text.includes('认证服务不可用')) {
        // 认证服务不可用与口令错误是两回事：前者重试也没用，不能说成口令错
        setDeleteError(`认证服务不可用，删除未执行：${text}`);
      } else {
        setDeleteError(text);
      }
    } finally {
      setDeleteRunning(false);
      setBusyNodeKey(null);
    }
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
      case 'resync':
        // 只给上游连接器：复用快照弹窗，写入增量 execute-snapshot 信号
        setResyncTarget(node.record as DbzSourceConnectorVo);
        break;
      case 'export-connectors':
        setExportTarget(node);
        setExportRedacted(false);
        break;
      case 'import-connectors':
        // 文件选择框是隐藏的，记下目标 Connect 再触发，避免用户选文件期间切换节点
        pendingImportTarget.current = node;
        if (importFileRef.current) {
          importFileRef.current.value = '';
          importFileRef.current.click();
        }
        break;
      case 'backup-cluster':
        setBackupTarget(node);
        break;
      case 'restore-cluster':
        // 恢复以文件里的集群为准，卡片只负责触发文件选择
        if (restoreFileRef.current) {
          restoreFileRef.current.value = '';
          restoreFileRef.current.click();
        }
        break;
      case 'edit':
        setEditTarget(node);
        break;
      case 'delete':
        openDeleteNode(node);
        break;
      case 'check':
        void checkNode(node);
        break;
      case 'dead-letter':
        // 复用死信页深链：按连接器类型 + id 预筛选
        navigate(
          `/dead-letter?connectorType=${deadLetterTypeOf(node.kind as ConnectorKind)}&connectorId=${node.id}`,
        );
        break;
    }
  };

  /** 切换集群 tab：搜索与选中都留在原 tab，切过去不串数据 */
  const handleClusterTabChange = (key: string) => {
    setActiveBrokerKey(key);
    setSelectedNodeKey(null);
    setSelectedEdgeId(null);
    setSearchText('');
    setSearchCursor(0);
  };

  /** 导出弹窗里的连接器数量：当前 Connect 下已纳管的上游 + 下游 */
  const exportConnectorCount = exportTarget
    ? state.data.sources.filter((item) => item.kafkaConnectId === exportTarget.id).length +
      state.data.sinks.filter((item) => item.kafkaConnectId === exportTarget.id).length
    : 0;

  /** 导出为 JSON：默认带凭据，脱敏由弹窗里的选项决定 */
  const executeExportConnectors = () => {
    if (!exportTarget) {
      return;
    }
    try {
      const connect = exportTarget.record as DbzKafkaConnectVo;
      const now = new Date();
      const file = buildTransferFile(
        state.data.brokers,
        state.data.sources,
        state.data.sinks,
        connect,
        exportRedacted,
        now,
      );
      downloadJson(exportFileName(connect.name, now), file);
      if (exportRedacted) {
        const masked = file.connectors.reduce(
          (sum, item) => sum + item.sensitiveConfigKeys.length,
          0,
        );
        message.success(
          `已脱敏导出 ${file.connectors.length} 个连接器，屏蔽 ${masked} 处凭据（值为 ******）`,
        );
      } else {
        message.success(
          `已导出 ${file.connectors.length} 个连接器（含凭据，敏感字段已在 sensitiveConfigKeys 中标注）`,
        );
      }
      setExportTarget(null);
    } catch (reason) {
      message.error(`导出失败：${errorMessage(reason)}`);
    }
  };

  /** 读文件 → 解析 → 打开预览，并异步补上目标 Connect 的实时连接器名称 */
  const handleImportFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const selected = event.target.files?.[0];
    const target = pendingImportTarget.current;
    event.target.value = '';
    pendingImportTarget.current = null;
    if (!selected || !target) {
      return;
    }

    let raw = '';
    try {
      raw = await selected.text();
    } catch (reason) {
      modal.error({
        title: '读取导入文件失败',
        content: errorMessage(reason),
      });
      return;
    }

    const parsed = parseTransferFile(raw);
    if (!parsed.file) {
      modal.error({
        title: '导入文件不合法',
        width: 640,
        content: (
          <ul className="dbz-transfer-errors">
            {parsed.errors.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ),
      });
      return;
    }

    const sessionBase: ImportSession = {
      target,
      file: parsed.file,
      fileName: selected.name,
      phase: 'preview',
      loadingPreview: true,
      rows: buildImportRows(
        parsed.file,
        target.id,
        state.data.sources,
        state.data.sinks,
        null,
      ),
      liveNames: null,
      outcomes: [],
    };
    setImportSession(sessionBase);

    try {
      const live = await kafkaConnectorApi.list(target.id);
      const liveNames = new Set(live.map((item) => item.name));
      setImportSession((previous) =>
        previous && previous.target.key === target.key && previous.fileName === selected.name
          ? {
              ...previous,
              loadingPreview: false,
              liveNames,
              rows: buildImportRows(
                previous.file,
                target.id,
                state.data.sources,
                state.data.sinks,
                liveNames,
              ),
            }
          : previous,
      );
    } catch (reason) {
      setImportSession((previous) =>
        previous && previous.target.key === target.key && previous.fileName === selected.name
          ? {
              ...previous,
              loadingPreview: false,
              liveWarning: `无法读取目标 Connect 上的实时连接器，同名冲突只能按库内记录判断：${errorMessage(reason)}`,
            }
          : previous,
      );
    }
  };

  /** 单条冲突处理方式 */
  const setImportDecision = (
    direction: TransferConnector['direction'],
    connectorName: string,
    decision: ImportDecision,
  ) => {
    setImportSession((previous) =>
      previous
        ? {
            ...previous,
            rows: previous.rows.map((item) =>
              item.row.direction === direction && item.row.connectorName === connectorName
                ? { ...item, decision }
                : item,
            ),
          }
        : previous,
    );
  };

  /** 顶部批量选择：只影响有冲突的行，无冲突行始终新增 */
  const setAllImportDecisions = (decision: ImportDecision) => {
    setImportSession((previous) =>
      previous
        ? {
            ...previous,
            rows: previous.rows.map((item) =>
              item.conflict ? { ...item, decision } : item,
            ),
          }
        : previous,
    );
  };

  /**
   * # 执行导入
   *
   * 顺序固定为先上游后下游：下游按文件里的 `upstream.connectorName` 在目标环境
   * 中查找已纳管的上游 id；找不到就把这条下游记为失败并说明缺的是哪个上游，
   * 不静默丢弃，也不拿另一个同名记录凑合。
   */
  const executeImport = async () => {
    if (!importSession) {
      return;
    }
    const session = importSession;
    const target = session.target;
    setImportSession((previous) => (previous ? { ...previous, phase: 'running' } : previous));

    const outcomes: ImportOutcome[] = [];
    const managedSources = state.data.sources.filter(
      (item) => item.kafkaConnectId === target.id,
    );
    /** 目标 Connect 上已纳管的上游：connectorName → 记录 ID */
    const sourceLookup = new Map<string, Id>(
      managedSources.map((item) => [item.connectorName, item.id]),
    );
    /** 已占用名称：库内 + 集群实时 + 文件，保证 `-imported-N` 不撞名 */
    const taken = new Set<string>([
      ...state.data.sources.map((item) => item.connectorName),
      ...state.data.sinks.map((item) => item.connectorName),
      ...(session.liveNames ? [...session.liveNames] : []),
      ...session.file.connectors.map((item) => item.connectorName),
    ]);

    const upstreamIdOf = (row: TransferConnector): Id => {
      const connectorName = row.upstream?.connectorName?.trim() ?? '';
      if (!connectorName) {
        throw new Error('缺少上游 connectorName，无法重新挂载下游连接器');
      }
      const id = sourceLookup.get(connectorName);
      if (!id) {
        throw new Error(
          `缺少上游连接器「${row.upstream?.name || connectorName}」（connectorName=${connectorName}），目标 Connect 上没有对应的纳管记录`,
        );
      }
      return id;
    };

    const addConnector = async (
      row: TransferConnector,
      connectorName: string,
    ): Promise<Id> => {
      if (row.direction === 'source') {
        const created = await sourceConnectorApi.add({
          kafkaConnectId: target.id,
          name: row.name,
          connectorName,
          connectorConfig: row.connectorConfig,
          remark: row.remark,
          enabled: row.enabled,
        });
        return created.id;
      }
      const created = await sinkConnectorApi.add({
        sourceConnectorId: upstreamIdOf(row),
        kafkaConnectId: target.id,
        name: row.name,
        connectorName,
        connectorConfig: row.connectorConfig,
        remark: row.remark,
        enabled: row.enabled,
      });
      return created.id;
    };

    const modifyConnector = async (
      row: TransferConnector,
      id: Id,
      connectorName: string,
    ): Promise<void> => {
      if (row.direction === 'source') {
        await sourceConnectorApi.modify({
          id,
          name: row.name,
          connectorName,
          connectorConfig: row.connectorConfig,
          remark: row.remark,
          enabled: row.enabled,
        });
        return;
      }
      await sinkConnectorApi.modify({
        id,
        name: row.name,
        connectorName,
        connectorConfig: row.connectorConfig,
        remark: row.remark,
        enabled: row.enabled,
        sourceConnectorId: upstreamIdOf(row),
      });
    };

    const runRow = async (preview: ImportPreviewRow): Promise<void> => {
      const row = preview.row;
      if (preview.conflict && preview.decision === 'skip') {
        outcomes.push({ row, action: 'skipped' });
        return;
      }
      try {
        if (preview.conflict && preview.decision === 'keep') {
          const renamed = nextImportedName(row.connectorName, taken);
          const createdId = await addConnector(row, renamed);
          taken.add(renamed);
          const hints = [`导入件已改名为 ${renamed}`];
          if (preview.conflict.unmanaged) {
            hints.push(
              `该 Connect 上另有一条未纳管的同名连接器「${row.connectorName}」，跳过或覆盖时需要单独处理`,
            );
          }
          outcomes.push({
            row,
            action: 'kept-both',
            renamedConnectorName: renamed,
            message: hints.join('；'),
          });
          // 「两者都保留」里被改名的上游不占用原名称：文件中的下游仍按原名称挂载
          if (row.direction === 'source' && preview.conflict.managed) {
            sourceLookup.set(row.connectorName, preview.conflict.managed.id);
          }
          if (row.direction === 'source') {
            sourceLookup.set(renamed, createdId);
          }
          return;
        }

        if (preview.conflict && preview.decision === 'overwrite') {
          if (preview.conflict.managed) {
            await modifyConnector(
              row,
              preview.conflict.managed.id,
              row.connectorName,
            );
            if (row.direction === 'source') {
              sourceLookup.set(row.connectorName, preview.conflict.managed.id);
            }
            outcomes.push({ row, action: 'overwritten-managed' });
            return;
          }
          const adoptedId = await addConnector(row, row.connectorName);
          taken.add(row.connectorName);
          if (row.direction === 'source') {
            sourceLookup.set(row.connectorName, adoptedId);
          }
          outcomes.push({ row, action: 'overwritten-adopted' });
          return;
        }

        // 无冲突：新增并纳管
        const createdId = await addConnector(row, row.connectorName);
        taken.add(row.connectorName);
        if (row.direction === 'source') {
          sourceLookup.set(row.connectorName, createdId);
        }
        outcomes.push({ row, action: 'created' });
      } catch (reason) {
        outcomes.push({ row, action: 'failed', message: errorMessage(reason) });
      }
    };

    // 先上游后下游：下游要用导入后的上游 id
    for (const preview of session.rows.filter((item) => item.row.direction === 'source')) {
      await runRow(preview);
    }
    for (const preview of session.rows.filter((item) => item.row.direction === 'sink')) {
      await runRow(preview);
    }

    setImportSession((previous) =>
      previous ? { ...previous, phase: 'result', outcomes } : previous,
    );
    state.reload();
  };

  /** 备份弹窗里的 Connect 数量：当前集群下已纳管的 Kafka Connect */
  const backupConnectCount = backupTarget
    ? state.data.connects.filter((item) => item.kafkaBrokerId === backupTarget.id).length
    : 0;

  /** 备份弹窗里的连接器数量：这些 Connect 下的上游 + 下游 */
  const backupConnectorCount = backupTarget
    ? (() => {
        const connectIds = new Set(
          state.data.connects
            .filter((item) => item.kafkaBrokerId === backupTarget.id)
            .map((item) => item.id),
        );
        return (
          state.data.sources.filter((item) => connectIds.has(item.kafkaConnectId)).length +
          state.data.sinks.filter((item) => connectIds.has(item.kafkaConnectId)).length
        );
      })()
    : 0;

  /**
   * # 导出整个 Kafka 集群
   *
   * 备份由后端按库内记录生成，前端只负责下载：`sasl_password` 照常导出，界面上必须
   * 明确提示文件含敏感信息。
   */
  const executeBackupCluster = async () => {
    if (!backupTarget) {
      return;
    }
    const broker = backupTarget.record as DbzKafkaBrokerVo;
    setBackupRunning(true);
    try {
      const file = await backupApi.exportCluster(broker.id);
      downloadJson(backupFileName(file.broker.name), file);
      const connectors = file.connects.reduce((sum, item) => sum + item.connectors.length, 0);
      message.success(
        `已导出 Kafka 集群「${file.broker.name}」：${file.connects.length} 台 Connect，${connectors} 个连接器（含凭据）`,
      );
      setBackupTarget(null);
    } catch (reason) {
      message.error(`导出失败：${errorMessage(reason)}`);
    } finally {
      setBackupRunning(false);
    }
  };

  /**
   * # 选择恢复文件
   *
   * 文件校验与冲突判定都交给后端的预览接口：`dryRun=true` 不写库。校验不通过时按
   * Connect 导入的方式逐条列出原因，不再本地维护一套格式校验。
   */
  const handleRestoreFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const selected = event.target.files?.[0];
    event.target.value = '';
    if (!selected) {
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(await selected.text());
    } catch (reason) {
      modal.error({
        title: '读取恢复文件失败',
        content:
          reason instanceof SyntaxError
            ? '文件不是合法的 JSON'
            : errorMessage(reason),
      });
      return;
    }

    try {
      const preview = await backupApi.restore(parsed, 'skip', true);
      if (!preview.valid) {
        modal.error({
          title: '备份文件不合法',
          width: 640,
          content: (
            <ul className="dbz-transfer-errors">
              {preview.errors.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          ),
        });
        return;
      }
      setRestoreSession({
        fileName: selected.name,
        file: parsed,
        strategy: 'skip',
        phase: 'preview',
        loadingPreview: false,
        preview,
        result: null,
      });
    } catch (reason) {
      modal.error({
        title: '读取备份文件失败',
        content: errorMessage(reason),
      });
    }
  };

  /** 切换恢复策略：重新取一次预览，保证计数与实际执行一致 */
  const changeRestoreStrategy = async (next: BackupRestoreStrategy) => {
    const session = restoreSession;
    if (
      !session ||
      session.loadingPreview ||
      session.phase === 'running' ||
      session.strategy === next
    ) {
      return;
    }
    setRestoreSession({ ...session, strategy: next, loadingPreview: true });
    try {
      const preview = await backupApi.restore(session.file, next, true);
      if (!preview.valid) {
        message.error(preview.errors.join('；') || '恢复预览失败');
        setRestoreSession((previous) =>
          previous && previous.fileName === session.fileName
            ? { ...previous, strategy: session.strategy, loadingPreview: false }
            : previous,
        );
        return;
      }
      setRestoreSession((previous) =>
        previous && previous.fileName === session.fileName
          ? { ...previous, preview, loadingPreview: false }
          : previous,
      );
    } catch (reason) {
      setRestoreSession((previous) =>
        previous && previous.fileName === session.fileName
          ? { ...previous, strategy: session.strategy, loadingPreview: false }
          : previous,
      );
      message.error(`恢复预览失败：${errorMessage(reason)}`);
    }
  };

  /**
   * # 执行恢复
   *
   * 保留二次确认、不做审核口令校验。恢复只写库内纳管记录，不向远端注册或删除
   * 连接器；执行成功与否都以后端返回的同一套判定为准。
   */
  const executeRestore = () => {
    const session = restoreSession;
    if (!session) {
      return;
    }
    const preview = session.preview;
    const overwrite = session.strategy === 'overwrite';
    modal.confirm({
      title: overwrite ? '覆盖已存在的纳管记录？' : '仅补齐缺失的纳管记录？',
      width: 560,
      content: [
        `将写库 ${preview.created + preview.overwritten} 条：新增 ${preview.created}，覆盖 ${preview.overwritten}，跳过 ${preview.skipped}。`,
        '恢复只写库内纳管记录，不会在远端注册或删除连接器；需要后续启用/检测才会下发。',
      ].join('\n'),
      okText: '执行恢复',
      cancelText: '取消',
      onOk: async () => {
        setRestoreSession((previous) =>
          previous ? { ...previous, phase: 'running' } : previous,
        );
        try {
          const result = await backupApi.restore(session.file, session.strategy, false);
          setRestoreSession((previous) =>
            previous && previous.fileName === session.fileName
              ? { ...previous, phase: 'result', result }
              : previous,
          );
          const summary = `新增 ${result.created}，覆盖 ${result.overwritten}，跳过 ${result.skipped}，失败 ${result.failed}`;
          if (result.failed > 0) {
            message.warning(`恢复完成：${summary}`);
          } else {
            message.success(`恢复完成：${summary}`);
          }
          await state.reload();
        } catch (reason) {
          setRestoreSession((previous) =>
            previous ? { ...previous, phase: 'preview' } : previous,
          );
          message.error(`恢复失败：${errorMessage(reason)}`);
        }
      },
    });
  };

  /** 关闭恢复弹窗：执行中不允许关闭 */
  const closeRestore = () => {
    if (restoreSession?.phase === 'running') {
      return;
    }
    setRestoreSession(null);
  };

  /** 恢复弹窗内容：预览阶段展示计划与策略，结果阶段展示同一套明细 */
  const renderRestoreBody = () => {
    const session = restoreSession;
    if (!session) {
      return null;
    }
    const view =
      session.phase === 'result' && session.result ? session.result : session.preview;
    const brokerItem = view.items.find((item) => item.level === 'broker');
    const connectCount = view.items.filter((item) => item.level === 'connect').length;
    const connectorCount = view.items.filter(
      (item) => item.level === 'source' || item.level === 'sink',
    ).length;
    const isResult = session.phase === 'result' && session.result !== null;

    return (
      <div className="dbz-transfer-import" data-restore-dialog>
        <div className="dbz-transfer-file">
          文件：<span className="dbz-mono">{session.fileName}</span>
          {brokerItem && (
            <>
              {' '}· 集群 <span className="dbz-mono">{brokerItem.name}</span>
            </>
          )}
          {' '}· Connect {connectCount} 台 · 连接器 {connectorCount} 条
        </div>

        <Alert
          type="warning"
          showIcon
          message="备份文件含 sasl_password 等敏感凭据，恢复后能直接连上原集群；请只在可信环境之间传递并妥善保管。"
        />
        <Alert
          type="info"
          showIcon
          message="恢复只写库内纳管记录，不会在远端注册或删除连接器；需要后续启用/检测才会下发到集群。"
        />

        {!isResult && (
          <div className="dbz-transfer-bulk" data-restore-strategy>
            <span>恢复策略：</span>
            <Radio.Group
              value={session.strategy}
              disabled={session.loadingPreview}
              onChange={(event) =>
                void changeRestoreStrategy(event.target.value as BackupRestoreStrategy)
              }
            >
              <Radio.Button value="skip">仅补齐缺失（默认）</Radio.Button>
              <Radio.Button value="overwrite">覆盖已存在</Radio.Button>
            </Radio.Group>
          </div>
        )}

        <div className="dbz-transfer-summary">
          {isResult ? (
            <>
              <Tag color="success">新增 {view.created}</Tag>
              <Tag color="geekblue">覆盖 {view.overwritten}</Tag>
              <Tag>跳过 {view.skipped}</Tag>
              <Tag color="error">失败 {view.failed}</Tag>
            </>
          ) : (
            <>
              <Tag color="success">将新增 {view.created}</Tag>
              <Tag color="geekblue">将覆盖 {view.overwritten}</Tag>
              <Tag>将跳过 {view.skipped}</Tag>
              <Tag color="error">失败 {view.failed}</Tag>
            </>
          )}
        </div>

        <Table<DbzBackupRestoreItemVo>
          size="small"
          rowKey={(item) => `${item.level}:${item.connectorName ?? item.name}`}
          loading={session.loadingPreview}
          pagination={false}
          dataSource={view.items}
          scroll={{ y: 360 }}
          columns={[
            {
              title: '层级',
              key: 'level',
              width: 110,
              render: (_value: unknown, item: DbzBackupRestoreItemVo) => (
                <Tag color={RESTORE_LEVEL_COLOR[item.level]}>
                  {RESTORE_LEVEL_LABEL[item.level]}
                </Tag>
              ),
            },
            {
              title: '名称 / connectorName',
              key: 'name',
              render: (_value: unknown, item: DbzBackupRestoreItemVo) => (
                <div>
                  <div>{item.name}</div>
                  {item.connectorName && (
                    <div className="dbz-mono">{item.connectorName}</div>
                  )}
                </div>
              ),
            },
            {
              title: '所属',
              key: 'parent',
              width: 160,
              render: (_value: unknown, item: DbzBackupRestoreItemVo) => item.parent || '--',
            },
            {
              title: '动作',
              key: 'action',
              width: 90,
              render: (_value: unknown, item: DbzBackupRestoreItemVo) => (
                <Tag color={RESTORE_ACTION_COLOR[item.action]}>
                  {RESTORE_ACTION_LABEL[item.action]}
                </Tag>
              ),
            },
            {
              title: '说明',
              key: 'message',
              render: (_value: unknown, item: DbzBackupRestoreItemVo) =>
                item.message ? (
                  <Typography.Text type={item.action === 'failed' ? 'danger' : 'secondary'}>
                    {item.message}
                  </Typography.Text>
                ) : (
                  '--'
                ),
            },
          ]}
        />
      </div>
    );
  };

  const renderInspector = () => {
    if (selectedEdge) {
      const fromNode = scopeNodes.find((node) => node.key === selectedEdge.from);
      const toNode = scopeNodes.find((node) => node.key === selectedEdge.to);
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
            message="端点由归属关系固定"
            description="连线由下游记录的上游字段决定：下游连接器的上游在 sink 表单里选择；其它节点由「+」新增时的父节点固定。两端不能拖动，也不能删除；选中后可拖中间折点避让卡片，松手即保存。"
          />
          <div className="dbz-inspector-actions">
            {edgeBends[selectedEdge.id] !== undefined && (
              <Button onClick={() => resetEdgeBend(selectedEdge.id)}>恢复默认走线</Button>
            )}
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
                    // 属性面板自成一体：这里只做展示，入口在卡片上的死信标签
                    children: node.dead > 0 ? `死信 ${node.dead}` : '0',
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

        <div className="dbz-inspector-tip">
          <p>属性面板只读展示；配置与操作都在卡片上，不跳转到其它页面。</p>
          <p>归属线端点固定，选中后可拖中间折点；连线不能删除。</p>
        </div>
      </div>
    );
  };

  return (
    <div className="dbz-canvas-page">
      <div className="dbz-canvas-topbar">
        <div className="dbz-canvas-title">
          <Typography.Title level={4}>连接器拓扑</Typography.Title>
          <span className="dbz-canvas-caption">
            状态由健康检查与进入页面刷新回写，卡片只展示一个统一状态
          </span>
        </div>
        <Space size={12} wrap>
          <span className="dbz-canvas-search">
            <Input
              allowClear
              size="small"
              prefix={<SearchOutlined />}
              placeholder="搜索名称 / topic / connectorName"
              value={searchText}
              onChange={(event) => handleSearchChange(event.target.value)}
              onPressEnter={() => stepSearch(1)}
              aria-label="搜索画布节点"
              data-canvas-search
            />
            <Button
              size="small"
              type="text"
              icon={<UpOutlined />}
              disabled={!searchHits.length}
              onClick={() => stepSearch(-1)}
              aria-label="上一个搜索结果"
              data-search-prev
            />
            <Button
              size="small"
              type="text"
              icon={<DownOutlined />}
              disabled={!searchHits.length}
              onClick={() => stepSearch(1)}
              aria-label="下一个搜索结果"
              data-search-next
            />
            {searchText.trim() && (
              <Button size="small" type="link" onClick={clearSearch} data-search-clear>
                清空
              </Button>
            )}
            <span className="dbz-canvas-search-count" data-search-count>
              {searchText.trim()
                ? `${searchHits.length ? Math.min(searchCursor, searchHits.length - 1) + 1 : 0}/${searchHits.length}`
                : ''}
            </span>
          </span>
          <Tag color="blue" data-global-clusters>
            {state.data.brokers.length} 个 Kafka 集群
          </Tag>
          {deadLetterTotal > 0 && (
            <Tag
              color="error"
              className="dbz-clickable"
              onClick={() => navigate('/dead-letter')}
              data-global-dead-letter
            >
              死信 {deadLetterTotal}
            </Tag>
          )}
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

      {brokerTabs.length > 1 && (
        <div className="dbz-cluster-tabs" data-cluster-tabs>
          <Tabs
            size="small"
            activeKey={resolvedBrokerId ?? undefined}
            onChange={handleClusterTabChange}
            items={brokerTabs.map((tab) => ({
              key: tab.id,
              label: (
                <span className="dbz-cluster-tab-label" data-cluster-tab={tab.id}>
                  {tab.name}
                  <Tag className="dbz-cluster-tab-count">连接器 {tab.connectorCount}</Tag>
                  <Tag
                    color={tab.abnormalCount > 0 ? 'error' : 'default'}
                    className="dbz-cluster-tab-abnormal"
                  >
                    异常 {tab.abnormalCount}
                  </Tag>
                </span>
              ),
            }))}
          />
        </div>
      )}

      {state.error && (
        <Alert
          type="error"
          showIcon
          closable
          message={`加载连接器拓扑数据失败：${state.error.message}`}
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
        <>
          <div className="dbz-canvas-scope-summary" data-canvas-summary>
            <Tag color="processing" data-active-cluster>
              {activeBrokerName}
            </Tag>
            <Tag>{scopeNodes.length} 个节点</Tag>
            <Tag color={healthSummary.unreachable > 0 ? 'error' : 'success'}>
              Connect 可达 {healthSummary.reachable}/{healthSummary.total}
            </Tag>
            {scopeDeadLetterTotal > 0 && (
              <Tag color="error">当前集群死信 {scopeDeadLetterTotal}</Tag>
            )}
            <span className="dbz-canvas-loaded">
              采集于{' '}
              {state.data.loadedAt
                ? new Date(state.data.loadedAt).toLocaleTimeString()
                : '--'}
            </span>
          </div>
          <ConnectorTopologyCanvas
            key={resolvedBrokerId ?? 'all'}
            nodes={scopeNodes}
            edges={scopeEdges}
            positions={resolvedPositions}
            edgeBends={edgeBends}
            selectedNodeKey={selectedNodeKey}
            selectedEdgeId={selectedEdgeId}
            onNodeMove={handleNodeMove}
            onEdgeBend={handleEdgeBend}
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
            checkingKey={checkingKey}
            restartingKey={restartingKey}
            searchHits={searchHits}
            activeHitKey={activeHitKey}
            focusToken={searchFocusToken}
            renderInspector={renderInspector}
          />
        </>
      )}

      {/* 隐藏文件输入：Connect 卡片上的导入按钮触发，目标 Connect 由 ref 记住 */}
      <input
        ref={importFileRef}
        type="file"
        accept=".json,application/json"
        style={{ display: 'none' }}
        data-import-file
        onChange={(event) => void handleImportFileChange(event)}
      />

      {/* 隐藏文件输入：Kafka 集群卡片上的恢复按钮触发，集群以文件内容为准 */}
      <input
        ref={restoreFileRef}
        type="file"
        accept=".json,application/json"
        style={{ display: 'none' }}
        data-restore-file
        onChange={(event) => void handleRestoreFileChange(event)}
      />

      {/* Connect 导出：默认带凭据，也可显式选择脱敏 */}
      <Modal
        open={exportTarget !== null}
        title={
          exportTarget
            ? `导出「${(exportTarget.record as DbzKafkaConnectVo).name}」的连接器`
            : '导出连接器'
        }
        onCancel={() => setExportTarget(null)}
        okText={exportRedacted ? '导出（脱敏）' : '导出（含凭据）'}
        onOk={executeExportConnectors}
        okButtonProps={{ disabled: exportConnectorCount === 0 }}
        destroyOnHidden
      >
        <div className="dbz-transfer-export" data-export-dialog>
          <p>
            将导出该 Connect 下 <strong>{exportConnectorCount}</strong> 个已纳管连接器，文件名含
            Connect 名称与导出时间。
          </p>
          <Radio.Group
            value={exportRedacted}
            onChange={(event) => setExportRedacted(event.target.value as boolean)}
          >
            <Space direction="vertical">
              <Radio value={false} data-export-mode="credential">
                含凭据（默认）：完整配置原样导出
              </Radio>
              <Radio value={true} data-export-mode="redacted">
                脱敏：口令/密钥类字段替换为 ******
              </Radio>
            </Space>
          </Radio.Group>
          {exportConnectorCount === 0 ? (
            <Alert
              type="info"
              showIcon
              message="该 Connect 下还没有已纳管连接器，没有可导出的内容。"
            />
          ) : exportRedacted ? (
            <Alert
              type="warning"
              showIcon
              message="脱敏导出的凭据字段不可用，导入后需要手动补齐才能正常运行。"
            />
          ) : (
            <Alert
              type="warning"
              showIcon
              message="含凭据导出会把口令/密钥原样写进 JSON 文件，请只在可信环境之间传递；每个连接器的 sensitiveConfigKeys 会标注敏感字段。"
            />
          )}
        </div>
      </Modal>

      {/* Connect 导入：先预览冲突，再逐条选择处理方式 */}
      <Modal
        open={importSession !== null}
        width={900}
        title={
          importSession
            ? `从 JSON 导入到「${(importSession.target.record as DbzKafkaConnectVo).name}」`
            : '导入连接器'
        }
        onCancel={() => {
          if (importSession?.phase !== 'running') {
            setImportSession(null);
          }
        }}
        maskClosable={importSession?.phase !== 'running'}
        destroyOnHidden
        footer={
          !importSession
            ? null
            : importSession.phase === 'result'
              ? [
                  <Button key="close" onClick={() => setImportSession(null)}>
                    关闭
                  </Button>,
                ]
              : importSession.phase === 'running'
                ? [
                    <Button key="running" type="primary" loading disabled>
                      正在导入…
                    </Button>,
                  ]
                : [
                    <Button key="cancel" onClick={() => setImportSession(null)}>
                      取消
                    </Button>,
                    <Button
                      key="ok"
                      type="primary"
                      loading={importSession.loadingPreview}
                      disabled={
                        importSession.loadingPreview || importSession.rows.length === 0
                      }
                      onClick={() => void executeImport()}
                      data-import-execute
                    >
                      执行导入
                    </Button>,
                  ]
        }
      >
        {importSession && (
          <div className="dbz-transfer-import">
            <div className="dbz-transfer-file">
              文件：<span className="dbz-mono">{importSession.fileName}</span> · 连接器{' '}
              {importSession.rows.length} 条 · 冲突 {importConflictCount} 条
            </div>

            {importSession.liveWarning && (
              <Alert type="warning" showIcon message={importSession.liveWarning} />
            )}
            {importSession.file.credentialMode === 'redacted' && (
              <Alert
                type="warning"
                showIcon
                message="该文件是脱敏导出，配置中的凭据为 ******；导入后需要手动补齐口令/密钥。"
              />
            )}
            {importSession.target.record &&
              !(importSession.target.record as DbzKafkaConnectVo).enabled && (
                <Alert
                  type="info"
                  showIcon
                  message="目标 Connect 已停用：导入只写库内记录，启用 Connect 后才会真正下发到集群。"
                />
              )}

            {importSession.phase === 'result' ? (
              <div data-import-result>
                <div className="dbz-transfer-summary">
                  <Tag color="success">新增 {importStats.created}</Tag>
                  <Tag color="blue">覆盖并纳管 {importStats['overwritten-adopted']}</Tag>
                  <Tag color="geekblue">
                    覆盖已有纳管记录 {importStats['overwritten-managed']}
                  </Tag>
                  <Tag>跳过 {importStats.skipped}</Tag>
                  <Tag color="purple">改名保留 {importStats['kept-both']}</Tag>
                  <Tag color="error">失败 {importStats.failed}</Tag>
                </div>
                <div className="dbz-transfer-outcomes">
                  {importSession.outcomes.map((outcome, index) => (
                    <div
                      className="dbz-transfer-outcome"
                      key={`${outcome.row.direction}:${outcome.row.connectorName}:${index}`}
                    >
                      <Tag color={IMPORT_ACTION_COLOR[outcome.action]}>
                        {IMPORT_ACTION_LABEL[outcome.action]}
                      </Tag>
                      <span className="dbz-transfer-outcome-name">
                        {outcome.row.name}
                      </span>
                      <span className="dbz-mono">
                        {outcome.renamedConnectorName ?? outcome.row.connectorName}
                      </span>
                      {outcome.message && (
                        <span className="dbz-transfer-outcome-message">{outcome.message}</span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <>
                {importConflictCount > 0 && (
                  <div className="dbz-transfer-bulk" data-import-bulk>
                    <span>冲突处理批量设置（无冲突行始终新增）：</span>
                    <Button
                      size="small"
                      onClick={() => setAllImportDecisions('overwrite')}
                      data-import-bulk-overwrite
                    >
                      全部覆盖
                    </Button>
                    <Button
                      size="small"
                      onClick={() => setAllImportDecisions('skip')}
                      data-import-bulk-skip
                    >
                      全部跳过
                    </Button>
                    <Button
                      size="small"
                      onClick={() => setAllImportDecisions('keep')}
                      data-import-bulk-keep
                    >
                      全部保留
                    </Button>
                  </div>
                )}
                <Table<ImportPreviewRow>
                  size="small"
                  rowKey={(item) => `${item.row.direction}:${item.row.connectorName}`}
                  loading={importSession.loadingPreview}
                  pagination={false}
                  dataSource={importSession.rows}
                  scroll={{ y: 360 }}
                  onRow={(item) => ({
                    'data-import-row': `${item.row.direction}:${item.row.connectorName}`,
                  } as HTMLAttributes<HTMLElement>)}
                  columns={[
                    {
                      title: '方向',
                      key: 'direction',
                      width: 70,
                      render: (_value: unknown, item: ImportPreviewRow) => (
                        <Tag color={item.row.direction === 'source' ? 'cyan' : 'magenta'}>
                          {item.row.direction === 'source' ? '上游' : '下游'}
                        </Tag>
                      ),
                    },
                    {
                      title: '名称 / connectorName',
                      key: 'name',
                      render: (_value: unknown, item: ImportPreviewRow) => (
                        <div>
                          <div>{item.row.name}</div>
                          <div className="dbz-mono">{item.row.connectorName}</div>
                        </div>
                      ),
                    },
                    {
                      title: '上游',
                      key: 'upstream',
                      render: (_value: unknown, item: ImportPreviewRow) =>
                        item.row.direction === 'sink' ? (
                          item.row.upstream ? (
                            <div>
                              <div>{item.row.upstream.name || '--'}</div>
                              <div className="dbz-mono">
                                {item.row.upstream.connectorName}
                              </div>
                            </div>
                          ) : (
                            <Tag color="error">缺少上游信息</Tag>
                          )
                        ) : (
                          '--'
                        ),
                    },
                    {
                      title: '同名冲突',
                      key: 'conflict',
                      width: 220,
                      render: (_value: unknown, item: ImportPreviewRow) =>
                        item.conflict ? (
                          <div className="dbz-transfer-conflict">
                            <Tag color="warning">同名冲突</Tag>
                            <span>
                              {item.conflict.managed
                                ? `已有纳管记录「${item.conflict.managed.name}」`
                                : '集群上未纳管'}
                            </span>
                          </div>
                        ) : (
                          <Tag color="success">无冲突，将新增</Tag>
                        ),
                    },
                    {
                      title: '处理方式',
                      key: 'decision',
                      width: 220,
                      render: (_value: unknown, item: ImportPreviewRow) =>
                        item.conflict ? (
                          <span
                            data-import-decision={`${item.row.direction}:${item.row.connectorName}`}
                          >
                            <Select<ImportDecision>
                              size="small"
                              value={item.decision}
                              style={{ width: 200 }}
                              onChange={(value) =>
                                setImportDecision(
                                  item.row.direction,
                                  item.row.connectorName,
                                  value,
                                )
                              }
                              options={[
                                {
                                  value: 'overwrite',
                                  label: item.conflict.managed
                                    ? IMPORT_ACTION_LABEL['overwritten-managed']
                                    : IMPORT_ACTION_LABEL['overwritten-adopted'],
                                },
                                { value: 'skip', label: IMPORT_DECISION_LABEL.skip },
                                {
                                  value: 'keep',
                                  label: `${IMPORT_DECISION_LABEL.keep}（改名 -imported-N）`,
                                },
                              ]}
                            />
                          </span>
                        ) : (
                          <Typography.Text type="secondary">新增</Typography.Text>
                        ),
                    },
                  ]}
                />
              </>
            )}
          </div>
        )}
      </Modal>

      {/* Kafka 集群备份：确认后由后端导出，前端只负责下载 */}
      <Modal
        open={backupTarget !== null}
        title={backupTarget ? `备份 Kafka 集群「${backupTarget.name}」` : '备份 Kafka 集群'}
        onCancel={() => {
          if (!backupRunning) {
            setBackupTarget(null);
          }
        }}
        maskClosable={!backupRunning}
        okText="下载备份文件"
        onOk={() => void executeBackupCluster()}
        okButtonProps={{ loading: backupRunning }}
        destroyOnHidden
      >
        <div className="dbz-transfer-export" data-backup-dialog>
          <p>
            将导出该集群的全部纳管记录：<strong>{backupConnectCount}</strong> 台 Kafka Connect、
            <strong>{backupConnectorCount}</strong> 个连接器（含上下游），以及集群自身的连接与
            认证配置。
          </p>
          <Alert
            type="warning"
            showIcon
            message="备份文件包含 sasl_password 等敏感凭据，恢复后可直接连上集群；请只在可信环境之间传递，并妥善保管文件。"
          />
          {backupConnectCount === 0 && (
            <Alert
              type="info"
              showIcon
              message="该集群下还没有 Kafka Connect，备份将只包含集群配置。"
            />
          )}
        </div>
      </Modal>

      {/* Kafka 集群恢复：预览计划 → 选择策略 → 二次确认 → 结果 */}
      <Modal
        open={restoreSession !== null}
        width={900}
        title="从 JSON 恢复 Kafka 集群"
        onCancel={closeRestore}
        maskClosable={restoreSession?.phase !== 'running'}
        destroyOnHidden
        footer={
          !restoreSession
            ? null
            : restoreSession.phase === 'result'
              ? [
                  <Button key="close" onClick={() => setRestoreSession(null)}>
                    关闭
                  </Button>,
                ]
              : restoreSession.phase === 'running'
                ? [
                    <Button key="running" type="primary" loading disabled>
                      正在恢复…
                    </Button>,
                  ]
                : [
                    <Button key="cancel" onClick={() => setRestoreSession(null)}>
                      取消
                    </Button>,
                    <Button
                      key="ok"
                      type="primary"
                      disabled={restoreSession.loadingPreview}
                      onClick={executeRestore}
                      data-restore-execute
                    >
                      执行恢复
                    </Button>,
                  ]
        }
      >
        {renderRestoreBody()}
      </Modal>

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

      {/* 上游「重新同步」：固定源库信号表 + 增量快照，可多选要重读的表 */}
      <SnapshotModal
        open={resyncTarget !== null}
        record={resyncTarget}
        fixedChannel="source"
        fixedSnapshotType="incremental"
        onClose={() => setResyncTarget(null)}
      />

      {/* 删除二次确认：口令校验通过前不删任何数据 */}
      <Modal
        open={deleteTarget !== null}
        title={
          deleteTarget
            ? `删除${kindLabel(deleteTarget.kind)}「${deleteTarget.name}」`
            : '删除节点'
        }
        width={560}
        okText="校验口令并删除"
        okButtonProps={{ danger: true, 'data-delete-confirm': 'true' }}
        cancelText="取消"
        confirmLoading={deleteRunning}
        maskClosable={!deleteRunning}
        onCancel={closeDeleteNode}
        onOk={() => void executeDeleteNode()}
        destroyOnHidden
      >
        {deleteTarget && (
          <div className="dbz-delete-impact" data-delete-dialog>
            {(() => {
              const ownerships = deleteImpactOf(deleteTarget);
              const dependentChildren = ownerships.some(
                (edge) => edge.from === deleteTarget.key,
              );
              return ownerships.length > 0 ? (
                <p>
                  删除后会移除 {ownerships.length} 条归属关系
                  {dependentChildren
                    ? '；服务端检测到仍有子记录时可能拒绝删除，请先处理依赖。'
                    : '。'}
                </p>
              ) : (
                <p>这个节点没有关联的归属关系。</p>
              );
            })()}
            <Alert
              type="warning"
              showIcon
              message="删除不可恢复"
              description="连接器会同时从 Kafka Connect 注销；需要保留下游连接器，请先改挂到其他 Connect。"
            />
            <div className="dbz-delete-password">
              <Typography.Text strong>审核口令</Typography.Text>
              <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 6 }}>
                审核口令在浏览器内先做 SM3 摘要，后端带登录令牌转给认证服务校验；未设置审核口令的用户需先在用户管理中设置。
              </Typography.Text>
              <Input.Password
                autoFocus
                value={deletePassword}
                placeholder="请输入当前登录用户的审核口令"
                data-delete-password
                onChange={(event) => {
                  setDeletePassword(event.target.value);
                  setDeleteError(null);
                }}
                onPressEnter={() => void executeDeleteNode()}
              />
            </div>
            {deleteError && (
              <Alert
                type="error"
                showIcon
                message={deleteError}
                data-delete-error
                className="dbz-delete-error"
              />
            )}
          </div>
        )}
      </Modal>
    </div>
  );
}
