/**
 * # 控制面板
 *
 * 只选上游、其余自动推导：
 * - 新增下级只走节点底部的「+」，父级固定且表单里不出现；
 * - sink 表单只选上游连接器，所属 Connect / Kafka 集群由上游自动带出；
 * - 泳道图只画归属线（Kafka 集群 → Connect → 上游 → 下游），连线只作展示，
 *   没有改上游、也没有删除入口；
 * - 改 sink 上游统一在表单里选 source。
 *
 * 布局是泳道图：四类节点各占一列泳道（从左到右），列头像表头一样固定在视口顶部，
 * 列内按父级分组自动排列，位置由数据推导、不持久化；空白平移、以指针为中心缩放、
 * 右下角缩放坞与缩略图；属性面板是画布右侧独立一列（可收起、可拖宽，点卡片不再自动打开），
 * 由顶栏「属性」按钮或画布右缘把手控制。
 * 启用/停用、重启、编辑、删除、集群检测、查看死信都放在节点卡片上；属性面板只负责
 * 展示库内配置、实时状态、任务与异常，不再另开详情抽屉。
 *
 * 顶层集群有三个入口：顶栏「新增 Kafka 集群」建一条空记录，「导出集群」把当前 tab
 * 的集群导出成备份 JSON，「导入集群」按备份 JSON 一次建出集群、Connect 与连接器。
 * 导出与集群卡片上的「备份」共用同一个确认弹窗（都走 `GET /dbz/backup/export`）；
 * 导入和集群卡片上的「恢复」是同一套预览/执行流程（都走 `POST /dbz/backup/restore`），
 * 只有入口与措辞不同；导入成功后自动切到新集群 tab。
 */

import {
  DownloadOutlined,
  DownOutlined,
  PlusOutlined,
  ProfileOutlined,
  ReloadOutlined,
  SearchOutlined,
  UpOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import {
  Alert,
  App as AntdApp,
  Button,
  Descriptions,
  Empty,
  Input,
  InputNumber,
  Modal,
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
} from 'react';
import { useNavigate } from 'react-router-dom';
import { sm3 } from '../../lib/sm3';
import { ApiError } from '../../api/client';
import { backupApi } from '../../api/backup';
import { controlPlaneApi } from '../../api/control-plane';
import ResetOffsetsModal from '../../components/connector-ops/ResetOffsetsModal';
import {
  detectBroker as detectBrokerOutcome,
  detectConnect as detectConnectOutcome,
  detectConnector as detectConnectorOutcome,
  detectFailure as detectFailureOutcome,
} from '../../components/connector-ops/detect';
import { restartManagedConnector } from '../../components/connector-ops/restart';
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
import ControlPanelCanvas from './ControlPanelCanvas';
import ExportConnectorsModal from '../../components/connector-ops/ExportConnectorsModal';
import RestoreClusterModal, {
  type RestoreClusterRequest,
} from '../../components/connector-ops/RestoreClusterModal';
import ImportConnectorsModal, {
  type ConnectorImportRequest,
} from '../../components/connector-ops/ImportConnectorsModal';
import DetectResultModal, {
  DetectResultBrief,
  type DetectOutcome,
} from './DetectResultModal';
import InconsistentDiffModal from './InconsistentDiffModal';
import { backupFileName, downloadJson } from './connect-transfer';
import {
  affectedEdgesOf,
  buildEdges,
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
  topicInfoOf,
} from './details';
import {
  buildControlPanel,
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

/** adm-rs 高危操作口令校验业务码 */
const CODE_PSWD_WRONG = 'ADM00007';
const CODE_PSWD_RATE_LIMITED = 'ADM00008';
const CODE_PSWD_USER_UNAVAILABLE = 'ADM00009';
const CODE_SESSION_INVALID = 'ADM00005';
/** dbz 自定义：认证服务不可用（网络、超时、5xx、未配置） */
const CODE_VERIFY_UNAVAILABLE = 'DBZ00001';

/** 一次集群检测：只有 Kafka 侧的 topic 清单（插件属于 Connect，不在这里查） */
interface ControlPanelData {
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

const EMPTY_DATA: ControlPanelData = {
  brokers: [],
  connects: [],
  sources: [],
  sinks: [],
  live: new Map(),
  health: new Map(),
  deadLetter: new Map(),
  loadedAt: 0,
};

/** 异常口径与卡片红色状态一致：失败与不可达，已停止/仅纳管不算异常 */
function connectorStatusAbnormal(runStatus: number): boolean {
  return runStatus === 3 || runStatus === 5;
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

/** 自动刷新配置：默认 15 秒，0 或关闭开关表示停用 */
interface RefreshSetting {
  enabled: boolean;
  seconds: number;
}

const REFRESH_STORAGE_KEY = 'dbz.control-panel.refresh';
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

/** 一次加载：四张表 + 每台 Connect 的实时连接器 + 死信计数 */
async function loadControlPanel(): Promise<ControlPanelData> {
  const [brokers, connects, sources, sinks, deadLetterCounts] =
    await Promise.all([
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

  return {
    brokers,
    connects,
    sources,
    sinks,
    live,
    health,
    deadLetter,
    loadedAt: Date.now(),
  };
}

/**
 * # 轻量刷新
 *
 * 自动刷新与手动刷新只做两件事：让后端把统一状态回写 `run_status`，再重取四张表与
 * 死信计数。不重新探测 Connect 实时列表，因此不会重置选中节点与属性框，也不会打断
 * 正在编辑的表单；失败时保留上一份数据。
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
  data: ControlPanelData,
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

export default function ControlPanelPage() {
  const { message, modal } = AntdApp.useApp();
  const navigate = useNavigate();
  const state: AsyncState<ControlPanelData> = useAsync(loadControlPanel, [], EMPTY_DATA);

  const [selectedNodeKey, setSelectedNodeKey] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  /**
   * 属性面板开关：画布右侧的独立一列，默认收起，选择记在 localStorage
   *
   * 点卡片只做选中高亮，不再自动弹面板；打开走顶栏「属性」按钮或画布右缘把手。
   */
  const [inspectorOpen, setInspectorOpen] = useState(
    () => localStorage.getItem('dbz-inspector-open') === '1',
  );
  const toggleInspector = useCallback(() => {
    setInspectorOpen((open) => {
      try {
        localStorage.setItem('dbz-inspector-open', open ? '0' : '1');
      } catch {
        /* 隐私模式存不了就算了 */
      }
      return !open;
    });
  }, []);
  /** 双击卡片只开不关，面板开着时保持不动 */
  const openInspector = useCallback(() => {
    try {
      localStorage.setItem('dbz-inspector-open', '1');
    } catch {
      /* 隐私模式存不了就算了 */
    }
    setInspectorOpen(true);
  }, []);
  const [checkingKey, setCheckingKey] = useState<string | null>(null);
  /**
   * 最近一次检测结果：按节点 key 保留，属性面板作为事后查看入口
   *
   * 四类卡片的「检测」都走同一份 `DetectOutcome`，弹窗与属性面板同源，不会出现
   * 弹窗说正常、面板说异常这种矛盾。
   */
  const [detectResults, setDetectResults] = useState<Record<string, DetectOutcome>>({});
  /** 当前打开的结果弹窗；一次检测只弹一个 */
  const [detectModal, setDetectModal] = useState<DetectOutcome | null>(null);
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

  /** 重置位点弹窗（内容组件与维护页共用） */
  const [resetTarget, setResetTarget] = useState<CanvasNode | null>(null);

  /** 新增：父级固定（顶层 Kafka 集群没有父级），成功后从新记录里找出 ID 再选中 */
  const [createKind, setCreateKind] = useState<CanvasNodeKind | null>(null);
  const [createParent, setCreateParent] = useState<CanvasNode | null>(null);
  const [pendingDrop, setPendingDrop] = useState<{
    kind: CanvasNodeKind;
    existingIds: Set<Id>;
  } | null>(null);
  const [editTarget, setEditTarget] = useState<CanvasNode | null>(null);

  /** 上游「重新同步」：复用快照弹窗，默认增量、可多选表 */
  const [resyncTarget, setResyncTarget] = useState<DbzSourceConnectorVo | null>(null);

  /** 删除：审核口令弹窗；未设置审核口令时由后端改用登录口令校验，校验通过前不动任何数据 */
  const [deleteTarget, setDeleteTarget] = useState<CanvasNode | null>(null);
  const [deletePassword, setDeletePassword] = useState('');
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteRunning, setDeleteRunning] = useState(false);

  /** 「不一致」对比弹窗：左边库内、右边远端，可编辑并双向同步 */
  const [inspectTarget, setInspectTarget] = useState<CanvasNode | null>(null);

  /** 画布搜索：只定位与高亮，不隐藏节点（搜索范围限定在当前集群 tab） */
  const [searchText, setSearchText] = useState('');
  const [searchCursor, setSearchCursor] = useState(0);
  const [searchFocusToken, setSearchFocusToken] = useState(0);

  /** 当前集群 tab；null 表示还没选过，按第一个集群兜底 */
  const [activeBrokerKey, setActiveBrokerKey] = useState<Id | null>(null);

  /** 导出弹窗：默认带凭据，脱敏由用户显式勾选 */
  const [exportTarget, setExportTarget] = useState<DbzKafkaConnectVo | null>(null);

  /** 导入：文件选择与预览/执行结果 */
  const [importRequest, setImportRequest] = useState<ConnectorImportRequest | null>(null);
  const importFileRef = useRef<HTMLInputElement>(null);
  const pendingImportTarget = useRef<CanvasNode | null>(null);

  /**
   * 集群导出：确认后整颗集群导出成备份 JSON
   *
   * 存 broker 记录而不是画布节点，顶栏「导出集群」（当前 tab）与集群卡片上的
   * 「备份」才能共用同一个弹窗 —— 节点只用于画布定位，导出只关心集群本身。
   */
  const [backupTarget, setBackupTarget] = useState<DbzKafkaBrokerVo | null>(null);
  const [backupRunning, setBackupRunning] = useState(false);

  /** 集群恢复 / 导入：文件、策略与预览/执行结果 */
  const [restoreRequest, setRestoreRequest] = useState<RestoreClusterRequest | null>(null);
  const restoreFileRef = useRef<HTMLInputElement>(null);
  /** 顶栏「导入集群」的文件选择框 */
  const clusterImportFileRef = useRef<HTMLInputElement>(null);
  /** 导入成功后要切过去的集群名；等刷新到这条记录再切 tab */
  const [pendingImportedBroker, setPendingImportedBroker] = useState<string | null>(null);

  /** 轻量刷新：只合并状态与死信计数，保留实时明细与选中态 */
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
      buildControlPanel(
        state.data.brokers,
        state.data.connects,
        state.data.sources,
        state.data.sinks,
        state.data.live,
        state.data.health,
      ),
    [state.data],
  );

  // 对比弹窗每次打开都按最新数据取记录：检测/同步后 state.reload 会替换掉旧对象
  const inspectRecord = useMemo<ManagedConnectorRecord | null>(() => {
    if (!inspectTarget) {
      return null;
    }
    if (inspectTarget.kind === 'source') {
      return state.data.sources.find((item) => item.id === inspectTarget.id) ?? null;
    }
    if (inspectTarget.kind === 'sink') {
      return state.data.sinks.find((item) => item.id === inspectTarget.id) ?? null;
    }
    return null;
  }, [inspectTarget, state.data.sinks, state.data.sources]);

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

  /**
   * 当前 tab 的集群记录：顶栏「导出集群」的目标
   *
   * 导出按集群走，所以不看画布节点，直接在 broker 列表里按当前 tab 取，集群被删掉
   * 或还没加载时为空，按钮随之禁用。
   */
  const currentBroker = useMemo(
    () => state.data.brokers.find((item) => item.id === resolvedBrokerId) ?? null,
    [state.data.brokers, resolvedBrokerId],
  );

  /** 画布只拿当前集群的节点，切 tab 不串数据 */
  const scopeNodes = useMemo(
    () => nodes.filter((node) => node.broker?.id === resolvedBrokerId),
    [nodes, resolvedBrokerId],
  );

  const scopeEdges = useMemo(() => {
    const keys = new Set(scopeNodes.map((node) => node.key));
    return edges.filter((edge) => keys.has(edge.from) && keys.has(edge.to));
  }, [edges, scopeNodes]);

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

  /** 新增表单成功后，从数据里找出新记录并选中；泳道位置由布局自动推导 */
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
    // 新建 Kafka 集群会多出一个 tab，直接切过去，否则新节点落在看不见的 tab 里
    if (pendingDrop.kind === 'broker') {
      setActiveBrokerKey(created.id);
    }
    setSelectedNodeKey(key);
    setSelectedEdgeId(null);
    setPendingDrop(null);
  }, [pendingDrop, state.data]);

  /**
   * 导入集群成功后切到新集群 tab
   *
   * 文件里的集群名就是恢复出来的名称，刷新到这条记录再切：新集群如果落在看不见的
   * tab 里，用户会以为导入没成功。选中新集群节点，属性检查器直接给出库内配置。
   */
  useEffect(() => {
    if (!pendingImportedBroker) {
      return;
    }
    const imported = state.data.brokers.find((item) => item.name === pendingImportedBroker);
    if (!imported) {
      // 数据还没刷新到，等下一轮
      return;
    }
    setActiveBrokerKey(imported.id);
    setSelectedNodeKey(nodeKeyOf('broker', imported.id));
    setSelectedEdgeId(null);
    setSearchText('');
    setSearchCursor(0);
    setPendingImportedBroker(null);
  }, [pendingImportedBroker, state.data.brokers]);

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

  const clusterOptions = useMemo(
    () => state.data.connects.map((cluster) => ({ value: cluster.id, label: cluster.name })),
    [state.data.connects],
  );

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
    setPendingDrop({ kind, existingIds });
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

  /** 记下最近一次检测结果，供属性面板事后查看；弹窗由调用方决定，避免叠加 */
  const recordDetect = (outcome: DetectOutcome) => {
    setDetectResults((previous) => ({ ...previous, [outcome.nodeKey]: outcome }));
  };

  /**
   * 集群检测：Kafka 连通性 + topic 清单（口径在共享模块，维护页也用同一份）
   */
  const checkBroker = async (node: CanvasNode): Promise<DetectOutcome> => {
    const outcome = await detectBrokerOutcome(
      node.record as DbzKafkaBrokerVo,
      node.key,
      kindLabel(node.kind),
    );
    recordDetect(outcome);
    state.reload();
    return outcome;
  };

  /**
   * Connect 检测：可达性 / 版本 / worker / 连接器 / 插件，并同步纳管记录
   *
   * 插件是 Connect worker 的能力，只在这里拉取与展示；同一台 Kafka 集群下多台
   * Connect 各自插件不同，不做跨 Connect 汇总。
   */
  const checkConnect = async (node: CanvasNode): Promise<DetectOutcome> => {
    const outcome = await detectConnectOutcome(
      node.record as DbzKafkaConnectVo,
      node.key,
      kindLabel(node.kind),
    );
    recordDetect(outcome);
    state.reload();
    return outcome;
  };

  /**
   * 连接器一致性检测：库内配置 vs Connect 实际配置 + 启停状态
   *
   * 判定交给后端 `POST /dbz/control-plane/connector-config/detect`，与整台 Connect
   * 的「检测」同源，但只作用于这一条记录；检测完刷新数据，卡片的「不一致」标签
   * 和对比弹窗立即能拿到本次结论。
   */
  const checkConnectorConsistency = async (node: CanvasNode): Promise<DetectOutcome> => {
    const outcome = await detectConnectorOutcome(
      node.kind as ConnectorKind,
      node.record as ManagedConnectorRecord,
      node.key,
      kindLabel(node.kind),
    );
    recordDetect(outcome);
    // 后端已落库标记与快照：刷新四张表让卡片标签、对比弹窗都用最新数据
    state.reload();
    return outcome;
  };

  /** 只跑检测、不弹窗（对比弹窗补检测时用） */
  const runCheck = async (node: CanvasNode): Promise<DetectOutcome> => {
    try {
      if (node.kind === 'broker') {
        return await checkBroker(node);
      }
      if (node.kind === 'connect') {
        return await checkConnect(node);
      }
      return await checkConnectorConsistency(node);
    } catch (reason) {
      const outcome = detectFailureOutcome(
        node.name,
        node.key,
        kindLabel(node.kind),
        reason,
      );
      recordDetect(outcome);
      return outcome;
    }
  };

  /** 四类卡片共用的「检测」入口：跑完统一弹结果弹窗，一次检测只弹一个 */
  const checkNode = async (node: CanvasNode) => {
    setCheckingKey(node.key);
    try {
      const outcome = await runCheck(node);
      setDetectModal(outcome);
    } finally {
      setCheckingKey(null);
    }
  };

  const restartConnector = async (record: ManagedConnectorRecord) => {
    const key = connectorKey(record);
    setRestartingKey(key);
    try {
      await restartManagedConnector(record);
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
   * 后端先带上当前用户身份转发 adm-rs 校验口令，通过后才删除并写审计；失败时
   * 一行数据都不动。是否回退到登录口令完全由后端判定，前端只提交一个口令字段，
   * 失败按 `ApiError.code` 分流，不再依赖提示文案。
   */
  const executeDeleteNode = async () => {
    if (!deleteTarget) {
      return;
    }
    const password = deletePassword.trim();
    if (!password) {
      setDeleteError('请输入审核口令；未设置审核口令时输入登录口令');
      return;
    }
    setDeleteRunning(true);
    setBusyNodeKey(deleteTarget.key);
    setDeleteError(null);
    try {
      // 口令明文不出浏览器：先做一次 SM3，后端只转发摘要给 adm-rs
      const result = await controlPlaneApi.deleteNode({
        nodeType: deleteTarget.kind,
        nodeId: deleteTarget.id,
        auditPswd: sm3(password),
      });
      message.success(`已删除「${deleteTarget.name}」，删除人与时间已写入审计`);
      if (result?.pswdKind === 'login') {
        message.info('当前账号未设置审核口令，已按登录口令校验通过');
      }
      setDeleteTarget(null);
      setDeletePassword('');
      setSelectedNodeKey(null);
      setSelectedEdgeId(null);
      state.reload();
    } catch (reason) {
      const code = reason instanceof ApiError ? reason.code : undefined;
      const text = errorMessage(reason);
      switch (code) {
        case CODE_SESSION_INVALID:
          // 令牌失效：请求层已清会话并回登录页，这里只说明删除未执行
          setDeleteError(`登录状态已失效，删除未执行：${text}`);
          break;
        case CODE_PSWD_RATE_LIMITED:
          // adm 频控响应不返回剩余秒数，只能提示默认窗口，避免编造倒计时
          setDeleteError(
            `${text}，删除未执行。认证服务默认 10 分钟内最多允许 5 次错误，请等待窗口结束后重试。`,
          );
          break;
        case CODE_PSWD_USER_UNAVAILABLE:
          setDeleteError(`${text}，删除未执行`);
          break;
        case CODE_VERIFY_UNAVAILABLE:
          // 认证服务不可用与口令错误是两回事：前者重试也没用，不能说成口令错
          setDeleteError(`${text}，删除未执行`);
          break;
        case CODE_PSWD_WRONG:
          setDeleteError(`${text}，删除未执行`);
          break;
        default:
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

  /** 打开重置位点弹窗：内容组件与维护页共用，执行与刷新都在组件里 */
  const openResetOffsets = (node: CanvasNode) => {
    setResetTarget(node);
  };

  /** 弹窗关闭：执行中由组件自己拦，这里只清目标 */
  const closeResetOffsets = () => {
    setResetTarget(null);
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
        setExportTarget(node.record as DbzKafkaConnectVo);
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
        // 与顶栏「导出集群」共用同一个确认弹窗，只是目标从节点里取
        setBackupTarget(node.record as DbzKafkaBrokerVo);
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
      case 'inspect-inconsistent':
        setInspectTarget(node);
        // 升级前的不一致标记没有快照：打开即补一次检测（后端会落库最新快照），
        // 但不弹检测结果弹窗，避免两个弹窗叠在一起
        if (!(node.record as ManagedConnectorRecord).inconsistentDetail) {
          setCheckingKey(node.key);
          void runCheck(node).finally(() => setCheckingKey(null));
        }
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

  /** 选好文件后交给共享导入弹窗：解析、冲突预览与执行都在组件里 */
  const handleImportFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const selected = event.target.files?.[0];
    const target = pendingImportTarget.current;
    event.target.value = '';
    pendingImportTarget.current = null;
    if (!selected || !target) {
      return;
    }
    setImportRequest({
      target: target.record as DbzKafkaConnectVo,
      file: selected,
      fileName: selected.name,
    });
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
   * 明确提示文件含敏感信息。顶栏「导出集群」与集群卡片上的「备份」都走这里。
   */
  const executeBackupCluster = async () => {
    if (!backupTarget) {
      return;
    }
    const broker = backupTarget;
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

  /** 选好备份文件后交给共享弹窗：预览、策略与执行都在组件里 */
  const handleRestoreFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const selected = event.target.files?.[0];
    event.target.value = '';
    if (!selected) {
      return;
    }
    setRestoreRequest({ file: selected, fileName: selected.name, mode: 'restore' });
  };

  /** 顶栏「导入集群」：目标环境还没有这个集群时，整颗集群一起导入 */
  const handleClusterImportFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const selected = event.target.files?.[0];
    event.target.value = '';
    if (!selected) {
      return;
    }
    setRestoreRequest({ file: selected, fileName: selected.name, mode: 'import' });
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
            message="上下游关系固定，走线自动路由"
            description="连线由下游记录的上游字段决定：下游连接器的上游在 sink 表单里选择；其它节点由「+」新增时的父节点固定。泳道图按归属自动排列节点与走线，不能拖动改接，也不能删除连线。"
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
            <p>归属线不能删除；泳道图按归属自动排列节点与走线。</p>
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
                  { key: 'remark', label: '备注', children: orDash(broker.remark) },
                ]}
              />
              <Typography.Title level={5} className="dbz-inspector-section">
                运行状态
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
              {(() => {
                const outcome = detectResults[node.key];
                if (!outcome) {
                  return (
                    <div className="dbz-inspector-check-hint">
                      点卡片上的「检测」可查看 Kafka 连通性与 topic 清单；连接器插件属于
                      Connect，请在各 Connect 卡片上检测查看。
                    </div>
                  );
                }
                return (
                  <>
                    <Typography.Title level={5} className="dbz-inspector-section">
                      检测结果
                    </Typography.Title>
                    <DetectResultBrief outcome={outcome} />
                  </>
                );
              })()}
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
                  { key: 'remark', label: '备注', children: orDash(cluster.remark) },
                ]}
              />
              <Typography.Title level={5} className="dbz-inspector-section">
                运行状态
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
              <Typography.Title level={5} className="dbz-inspector-section">
                检测结果
              </Typography.Title>
              {detectResults[node.key] ? (
                <DetectResultBrief outcome={detectResults[node.key]} />
              ) : (
                <div className="dbz-inspector-check-hint">
                  点卡片上的「检测」可查看可达性、版本、worker、连接器与插件。
                </div>
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

              <Typography.Title level={5} className="dbz-inspector-section">
                检测结果
              </Typography.Title>
              {detectResults[node.key] ? (
                <DetectResultBrief outcome={detectResults[node.key]} />
              ) : (
                <div className="dbz-inspector-check-hint">
                  点卡片上的「检测」可与 Connect 实际配置逐项比对，并检查启停是否一致。
                </div>
              )}
            </>
          );
        })()}

        <div className="dbz-inspector-tip">
          <p>属性面板只读展示；配置与操作都在卡片上，不跳转到其它页面。</p>
          <p>归属线端点固定，走线在泳道之间自动路由；连线不能删除。</p>
        </div>
      </div>
    );
  };

  return (
    <div className="dbz-canvas-page">
      <div className="dbz-canvas-topbar">
        <div className="dbz-canvas-toolbar-left">
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
        </div>
        <div className="dbz-canvas-toolbar-right">
          <Tag color="blue" data-global-clusters>
            {state.data.brokers.length} 个 Kafka 集群
          </Tag>
          <Button
            icon={<ProfileOutlined />}
            data-inspector-toggle
            aria-pressed={inspectorOpen}
            title="属性面板：库内配置、实时状态、任务与异常"
            onClick={toggleInspector}
          >
            属性
          </Button>
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
          {/* 全页唯一的主动作给「新增」；导出/导入是低频备份动作，普通描边即可 */}
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => startCreate('broker', null)}
          >
            新增 Kafka 集群
          </Button>
          <Button
            icon={<DownloadOutlined />}
            title={
              currentBroker
                ? `导出当前集群「${currentBroker.name}」（含 Connect 与连接器，文件含 sasl_password 等敏感信息）`
                : '先选中一个 Kafka 集群再导出'
            }
            data-cluster-export
            disabled={!currentBroker}
            onClick={() => {
              if (currentBroker) {
                setBackupTarget(currentBroker);
              }
            }}
          >
            导出集群
          </Button>
          <Button
            icon={<UploadOutlined />}
            title="从备份 JSON 导入整颗集群（含 Connect 与连接器），目标环境还没有该集群时用它"
            data-cluster-import
            onClick={() => {
              if (clusterImportFileRef.current) {
                clusterImportFileRef.current.value = '';
                clusterImportFileRef.current.click();
              }
            }}
          >
            导入集群
          </Button>
          <Button
            icon={<ReloadOutlined />}
            loading={manualRefreshing}
            onClick={() => void handleManualRefresh()}
          >
            刷新
          </Button>
        </div>
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
                  <span className="dbz-cluster-tab-name">{tab.name}</span>
                  <span className="dbz-cluster-tab-meta">连接器 {tab.connectorCount}</span>
                  <span
                    className={`dbz-cluster-tab-meta${
                      tab.abnormalCount > 0 ? ' is-abnormal' : ''
                    }`}
                  >
                    异常 {tab.abnormalCount}
                  </span>
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
          message={`加载控制面板数据失败：${state.error.message}`}
          className="dbz-canvas-error"
        />
      )}

      {state.loading && !state.data.loadedAt ? (
        <div className="dbz-canvas-loading">
          {/* 裸用 Spin 的 tip 会被 antd 忽略并告警，文案单独放 */}
          <Spin />
          <Typography.Text type="secondary">正在读取节点与实时状态…</Typography.Text>
        </div>
      ) : state.data.brokers.length === 0 ? (
        <div className="dbz-canvas-empty" data-canvas-empty>
          <div className="dbz-canvas-empty-title">还没有 Kafka 集群</div>
          <div className="dbz-canvas-empty-desc">
            控制面板按「Kafka 集群 → Connect → 连接器」三级展开，状态由健康检查定时回写。
            先新增一条集群记录，或从备份 JSON 一次导入整颗集群（含 Connect 与连接器）。
          </div>
          <div className="dbz-canvas-empty-actions">
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => startCreate('broker', null)}
            >
              新增 Kafka 集群
            </Button>
            <Button
              icon={<UploadOutlined />}
              onClick={() => {
                if (clusterImportFileRef.current) {
                  clusterImportFileRef.current.value = '';
                  clusterImportFileRef.current.click();
                }
              }}
            >
              导入集群
            </Button>
          </div>
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
                ? new Date(state.data.loadedAt).toLocaleTimeString('zh-CN', {
                    hour12: false,
                  })
                : '--'}
            </span>
          </div>
          <ControlPanelCanvas
            key={resolvedBrokerId ?? 'all'}
            nodes={scopeNodes}
            edges={scopeEdges}
            selectedNodeKey={selectedNodeKey}
            selectedEdgeId={selectedEdgeId}
            onSelectNode={(key) => {
              setSelectedNodeKey(key);
              setSelectedEdgeId(null);
            }}
            onSelectEdge={(id) => {
              setSelectedEdgeId(id);
              setSelectedNodeKey(null);
            }}
            onAddChild={handleAddChild}
            onAddRoot={() => startCreate('broker', null)}
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
            inspectorOpen={inspectorOpen}
            onToggleInspector={toggleInspector}
            onOpenInspector={openInspector}
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

      {/* 隐藏文件输入：顶栏「导入集群」触发，走同一套预览与执行流程 */}
      <input
        ref={clusterImportFileRef}
        type="file"
        accept=".json,application/json"
        style={{ display: 'none' }}
        data-cluster-import-file
        onChange={(event) => void handleClusterImportFileChange(event)}
      />

      <ExportConnectorsModal
        target={exportTarget}
        onClose={() => setExportTarget(null)}
      />

      <ImportConnectorsModal
        request={importRequest}
        onClose={() => setImportRequest(null)}
        onDone={() => state.reload()}
      />

      {/* Kafka 集群导出：确认后由后端生成备份，前端只负责下载 */}
      <Modal
        open={backupTarget !== null}
        title={backupTarget ? `导出 Kafka 集群「${backupTarget.name}」` : '导出 Kafka 集群'}
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

      <RestoreClusterModal
        request={restoreRequest}
        onClose={() => setRestoreRequest(null)}
        onDone={() => state.reload()}
        onImported={setPendingImportedBroker}
      />

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

      <ResetOffsetsModal
        target={
          resetTarget
            ? {
                kind: resetTarget.kind as 'source' | 'sink',
                id: resetTarget.id,
                name: resetTarget.name,
                record: resetTarget.record as ManagedConnectorRecord,
              }
            : null
        }
        onClose={closeResetOffsets}
        onDone={() => void refreshLight().catch(() => undefined)}
      />

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
              <Typography.Text strong>审核口令（未设置时使用登录口令）</Typography.Text>
              <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 6 }}>
                口令在浏览器内先做 SM3 摘要，后端带登录令牌转给认证服务校验；是否改用登录口令由认证服务判定，前端不做判断。
              </Typography.Text>
              <Input.Password
                autoFocus
                value={deletePassword}
                placeholder="请输入审核口令（未设置时输入登录口令）"
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

      {/* 四类卡片共用的检测结果弹窗 */}
      <DetectResultModal
        open={detectModal !== null}
        outcome={detectModal}
        onClose={() => setDetectModal(null)}
      />

      {/* 「不一致」对比：快照来自检测，敏感项已脱敏；可编辑并双向同步 */}
      <InconsistentDiffModal
        open={inspectTarget !== null}
        target={
          inspectTarget && (inspectTarget.kind === 'source' || inspectTarget.kind === 'sink')
            ? {
                kind: inspectTarget.kind,
                id: inspectTarget.id,
                name: inspectTarget.name,
              }
            : null
        }
        record={inspectRecord}
        detecting={inspectTarget !== null && checkingKey === inspectTarget.key}
        onClose={() => setInspectTarget(null)}
        onDetect={() => {
          if (inspectTarget) {
            void checkNode(inspectTarget);
          }
        }}
        onSynced={() => state.reload()}
      />
    </div>
  );
}
