/**
 * # 控制面板泳道图
 *
 * 四类节点各占一列泳道，从左到右按归属链排列（Kafka 集群 → Connect →
 * 上游连接器 → 下游连接器）；列头像表头一样固定在视口顶部，横向跟随泳道列、
 * 纵向不随平移缩放移动。列内按父级分组自动排列，位置由数据推导，不提供节点
 * 拖拽、也不持久化布局。
 *
 * 交互收敛为（与 uiam / syn 的控制面板同一套做法）：
 * - 空白可平移、滚轮以指针为中心缩放；列头像表头钉在视口顶部，列宽可拖、
 *   双击复位并记在本地；列头的「＋」新建本列节点，子列先挑父级；
 *   右下角一个缩放坞（−/百分比/＋/适应/定位选中）+ 缩略图；
 * - 连线只作展示：父右缘中点 → 子左缘中点画三次贝塞尔，带箭头与流动光点，
 *   不能改上下游，也不能删除；
 * - 新增下级只走节点底部的「+」按钮，点击后由页面打开父级已固定的表单；
 * - 四类卡片同一骨架，类型图标 + 类型文字 + 主色用于识别，状态只用标签表达。
 *
 * 组件只负责画与视口，真正写库的动作（打开表单、节点操作）由页面调用。
 */

import {
  AimOutlined,
  CloudServerOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  DownloadOutlined,
  EditOutlined,
  ExpandOutlined,
  ExportOutlined,
  CloseOutlined,
  HistoryOutlined,
  ImportOutlined,
  MinusOutlined,
  PlusOutlined,
  ProfileOutlined,
  ReloadOutlined,
  SyncOutlined,
  ThunderboltOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import { Button, Dropdown, Switch, Tag, Tooltip } from 'antd';
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from 'react';
import type { Id } from '../../api/types';
import { formatMs } from '../../utils/format';
import { connectorRunStatusDic } from '../../dic';
import { taskSummary, type ManagedConnectorRecord } from './model';
import {
  DEFAULT_LANE_COLUMNS,
  LANE_HEADER_H,
  NODE_KIND_COLOR,
  NODE_KIND_LABEL,
  buildSwimlaneLayout,
  clampLaneColumn,
  nodeTone,
  normalizeLaneColumns,
  type CanvasEdge,
  type CanvasNode,
  type CanvasNodeAction,
  type CanvasNodeKind,
} from './canvas-model';

/** 画布缩放范围 */
const MIN_SCALE = 0.35;
const MAX_SCALE = 2;

/** 属性面板的宽度边界（px）：拖左缘调整，双击恢复默认 */
const INSPECTOR_MIN_W = 280;
const INSPECTOR_MAX_W = 560;
const INSPECTOR_DEFAULT_W = 340;

/** 泳道列宽：四列各自记忆在本地，拖列头右缘调整、双击复位 */
const LANE_COLUMNS_KEY = 'dbz-lane-columns';

function readLaneColumns(): number[] {
  try {
    return normalizeLaneColumns(JSON.parse(localStorage.getItem(LANE_COLUMNS_KEY) ?? 'null'));
  } catch {
    return DEFAULT_LANE_COLUMNS.slice();
  }
}

function writeLaneColumns(columns: number[]): void {
  try {
    localStorage.setItem(LANE_COLUMNS_KEY, JSON.stringify(columns));
  } catch {
    /* 隐私模式等存不了就算了，下次打开回到默认列宽 */
  }
}

/** 「+」能新增的下级类型；下游连接器没有下一级，不显示按钮 */
const CHILD_KIND: Partial<Record<CanvasNodeKind, CanvasNodeKind>> = {
  broker: 'connect',
  connect: 'source',
  source: 'sink',
};

/** 列头「＋」新增本列节点时，要先挑一个上一级；Kafka 集群没有父级 */
const PARENT_KIND: Partial<Record<CanvasNodeKind, CanvasNodeKind>> = {
  connect: 'broker',
  source: 'connect',
  sink: 'source',
};

interface Point {
  x: number;
  y: number;
}

interface ViewState {
  x: number;
  y: number;
  scale: number;
}

interface EdgeGeometry {
  edge: CanvasEdge;
  path: string;
  color: string;
  marker: string;
}

interface ControlPanelCanvasProps {
  nodes: CanvasNode[];
  edges: CanvasEdge[];
  selectedNodeKey: string | null;
  selectedEdgeId: string | null;
  onSelectNode: (key: string | null) => void;
  onSelectEdge: (id: string | null) => void;
  /** 点击节点底部「+」：父级固定，页面按子类型打开新增表单 */
  onAddChild: (node: CanvasNode) => void;
  /** 点击列头「+」：新增一条本级记录（Kafka 集群没有父级，直接开新建表单） */
  onAddRoot: () => void;
  /** 卡片上的重启/编辑/删除/检测/重新同步 */
  onNodeAction: (node: CanvasNode, action: CanvasNodeAction) => void;
  /** 卡片上的启用开关：选中即启用，关闭即停用 */
  onToggleNode: (node: CanvasNode, enabled: boolean) => void;
  /** 各操作的 loading 状态 */
  busyNodeKey: string | null;
  togglingNodeKey: string | null;
  checkingKey: string | null;
  restartingKey: string | null;
  /** 命中的节点 key（结果顺序），用于高亮与自动定位；空数组表示没有搜索 */
  searchHits: string[];
  /** 当前定位到第几个命中 */
  activeHitKey: string | null;
  /** 每次点「上一个/下一个」自增，保证同一个命中也会重新居中 */
  focusToken: number;
  /** 右侧检查器内容 */
  renderInspector: () => ReactNode;
  /** 属性面板开关：画布右侧独立一列，点卡片只选中，由顶栏按钮/边缘把手/双击卡片控制 */
  inspectorOpen: boolean;
  onToggleInspector: () => void;
  /** 双击卡片：选中并展开属性面板 */
  onOpenInspector: () => void;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

/** 停用态：整卡降饱和/降透明度，与「状态异常」的红色标签区分开 */
function nodeDisabledOf(node: CanvasNode): boolean {
  if (node.kind === 'connect') {
    return !node.cluster?.enabled;
  }
  return !(node.record as { enabled: boolean }).enabled;
}

/**
 * # 统一状态值
 *
 * 连接器直接用后端回写的 `run_status`；broker/connect 没有统一列，按启停 + 最近
 * 检查结果折算成同一枚举，避免卡片各写一套字符串。
 */
function unifiedStatusOf(node: CanvasNode): number {
  if (node.kind === 'source' || node.kind === 'sink') {
    const record = node.record as ManagedConnectorRecord;
    // 已停用（或所属 Connect 已停用）直接是「已停用」，不再看库内最后一次结论
    if (!record.enabled || node.connectDisabled) {
      return 2;
    }
    return record.runStatus;
  }
  if (node.kind === 'broker') {
    const broker = node.record as { enabled: boolean; runStatus: number };
    if (!broker.enabled) return 2;
    if (broker.runStatus === 1) return 1;
    if (broker.runStatus === 3) return 5;
    return 0;
  }
  if (!node.cluster?.enabled) return 2;
  if (node.health?.error || node.health?.health?.reachable === false || node.liveIssue) return 5;
  if (node.health?.health?.reachable) return 1;
  return 0;
}

/** 状态标签：只展示一个统一枚举，状态色不占用类型主色 */
function StatusTag({ node }: { node: CanvasNode }) {
  const value = unifiedStatusOf(node);
  const item = connectorRunStatusDic.find((dic) => dic.value === value);
  return <Tag color={item?.color ?? 'default'}>{item?.label ?? '未知'}</Tag>;
}

/** 状态悬停详情：统一状态 + 最后检查时间 + 失败原因（不可达时带上最后已知） */
function StatusHint({ node }: { node: CanvasNode }) {
  const statusValue = unifiedStatusOf(node);
  const statusLabel =
    connectorRunStatusDic.find((dic) => dic.value === statusValue)?.label ?? '未知';
  const record = node.record as {
    runStatus: number;
    runMsg?: string | null;
    updateMs?: Id;
    enabled: boolean;
  };
  const tasks = node.live ? taskSummary(node.live) : null;
  const disabled = statusValue === 2;
  const rows: { label: string; value: string }[] = [
    {
      label: '状态',
      value:
        statusValue === 5
          ? '不可达（Connect 连不上，显示最后已知状态）'
          : statusLabel,
    },
    { label: '最后检查时间', value: formatMs(record.updateMs) },
    {
      label: '失败原因',
      value: disabled ? '已停用，不发起实时检查' : record.runMsg?.trim() || '无异常',
    },
  ];
  if (node.kind === 'source' || node.kind === 'sink') {
    rows.push({
      label: '任务',
      value: disabled
        ? '已停用，不发起实时请求'
        : tasks
          ? `共 ${tasks.total} 个，失败 ${tasks.abnormal} 个`
          : '无实时任务数据',
    });
  }

  return (
    <div className="dbz-status-hint">
      {rows.map((row) => (
        <div className="dbz-status-hint-row" key={row.label}>
          <span className="dbz-status-hint-label">{row.label}</span>
          <span className="dbz-status-hint-value">{row.value}</span>
        </div>
      ))}
    </div>
  );
}

/** 状态标签：悬停给出运行详情 */
function NodeStatus({ node }: { node: CanvasNode }) {
  return (
    <Tooltip
      title={<StatusHint node={node} />}
      styles={{ root: { maxWidth: 380 } }}
      placement="topLeft"
    >
      <span className="dbz-graph-status-tag" data-no-drag>
        <StatusTag node={node} />
      </span>
    </Tooltip>
  );
}

/** 四类各用一个语义明确的图标：集群 / 服务节点 / 入方向 / 出方向 */
function NodeIcon({ kind }: { kind: CanvasNodeKind }) {
  switch (kind) {
    case 'broker':
      return <DatabaseOutlined />;
    case 'connect':
      return <CloudServerOutlined />;
    case 'source':
      return <ImportOutlined />;
    case 'sink':
      return <ExportOutlined />;
  }
}

interface NodeActionsProps {
  node: CanvasNode;
  busyNodeKey: string | null;
  checkingKey: string | null;
  restartingKey: string | null;
  onSelect: (key: string) => void;
  onAction: (node: CanvasNode, action: CanvasNodeAction) => void;
}

/** 卡片操作行：编辑在最左，右侧依次检测/重启/重置/重新同步，删除隔一道分隔线放最右 */
function NodeActions({
  node,
  busyNodeKey,
  checkingKey,
  restartingKey,
  onSelect,
  onAction,
}: NodeActionsProps) {
  const rightItems: {
    action: CanvasNodeAction;
    title: string;
    icon: ReactNode;
    disabled?: boolean;
    loading?: boolean;
  }[] = [];

  // 检测排在最前：只读诊断，放在所有写操作之前
  rightItems.push({
    action: 'check',
    title:
      node.kind === 'broker'
        ? '集群检测：Kafka 连通性与 topic 清单'
        : node.kind === 'connect'
          ? 'Connect 检测：可达性 / 版本 / worker / 插件'
          : '一致性检测：库内配置与 Connect 实际配置是否一致，并刷新「不一致」标记与对比快照',
    icon: <ThunderboltOutlined />,
    loading: checkingKey === node.key,
  });

  if (node.kind === 'source' || node.kind === 'sink') {
    const record = node.record as ManagedConnectorRecord;
    let title = '重启';
    let disabled = false;
    if (node.connectDisabled) {
      title = '所属 Kafka Connect 已停用，先启用 Connect 后再重启';
      disabled = true;
    } else if (!record.enabled) {
      title = '连接器已停用，先启用后再重启';
      disabled = true;
    } else if (node.liveIssue) {
      title = '实时查询失败，拿不到连接器状态';
      disabled = true;
    } else if (!node.live) {
      title = '集群上没有该连接器，无法重启';
      disabled = true;
    }
    rightItems.push({
      action: 'restart',
      title,
      icon: <ReloadOutlined />,
      disabled,
      loading: restartingKey === `${record.kafkaConnectId}:${record.connectorName}`,
    });
    rightItems.push({
      action: 'reset-offsets',
      title: '重置位点（停止 → 清空位点 → 可选恢复）',
      icon: <HistoryOutlined />,
    });
  }

  // Connect：导入导出按整台 Connect 处理，避免在连接器卡片上重复入口
  if (node.kind === 'connect') {
    rightItems.push({
      action: 'export-connectors',
      title: '导出该 Connect 下全部连接器（JSON，默认含凭据）',
      icon: <DownloadOutlined />,
    });
    rightItems.push({
      action: 'import-connectors',
      title: '从 JSON 导入连接器，同名可覆盖、跳过或两者都保留',
      icon: <UploadOutlined />,
    });
  }

  // Kafka 集群：导出/恢复按整个集群处理，包含其下全部 Connect 与连接器
  if (node.kind === 'broker') {
    rightItems.push({
      action: 'backup-cluster',
      title: '导出整个 Kafka 集群（含 Connect 与连接器，文件含 sasl_password 等敏感信息）',
      icon: <DownloadOutlined />,
    });
    rightItems.push({
      action: 'restore-cluster',
      title: '从备份 JSON 恢复库内纳管记录（不向远端注册或删除连接器）',
      icon: <UploadOutlined />,
    });
  }

  // 重新同步只给上游连接器：向下游发增量快照没有「重读源表」这回事
  if (node.kind === 'source') {
    rightItems.push({
      action: 'resync',
      title: '重新同步：选表后写 execute-snapshot（增量），不重置位点、不停止连接器',
      icon: <SyncOutlined />,
    });
  }

  const renderAction = (item: {
    action: CanvasNodeAction;
    title: string;
    icon: ReactNode;
    danger?: boolean;
    disabled?: boolean;
    loading?: boolean;
  }) => (
    <Tooltip key={item.action} title={item.title}>
      <span className="dbz-graph-action">
        <Button
          size="small"
          type="text"
          danger={item.danger}
          icon={item.icon}
          disabled={item.disabled}
          loading={item.loading}
          aria-label={item.title}
          data-node-action={item.action}
          onClick={() => {
            onSelect(node.key);
            onAction(node, item.action);
          }}
        />
      </span>
    </Tooltip>
  );

  return (
    <div
      className="dbz-graph-node-actions"
      data-no-drag
      onPointerDown={(event) => event.stopPropagation()}
    >
      {/* 编辑靠最左；开关已经移到卡片右上角，这里不再重复 */}
      {renderAction({ action: 'edit', title: '编辑配置', icon: <EditOutlined /> })}
      <span className="dbz-graph-actions-spacer" />
      {rightItems.map(renderAction)}
      <span className="dbz-graph-actions-divider" aria-hidden />
      {renderAction({
        action: 'delete',
        title: '删除',
        icon: <DeleteOutlined />,
        danger: true,
        loading: busyNodeKey === node.key,
      })}
    </div>
  );
}

/** 卡片头部：类型标识在前，启用开关统一贴右上角 */
function NodeHead({
  node,
  togglingNodeKey,
  onSelect,
  onToggle,
}: {
  node: CanvasNode;
  togglingNodeKey: string | null;
  onSelect: (key: string) => void;
  onToggle: (node: CanvasNode, enabled: boolean) => void;
}) {
  const enabled = (node.record as { enabled: boolean }).enabled;
  const toggling = togglingNodeKey === node.key;
  return (
    <div className="dbz-graph-node-head">
      <span
        className="dbz-graph-node-icon"
        style={{ backgroundColor: NODE_KIND_COLOR[node.kind] }}
      >
        <NodeIcon kind={node.kind} />
      </span>
      <span
        className="dbz-graph-node-type-label"
      >
        {NODE_KIND_LABEL[node.kind]}
      </span>
      <Tooltip title={enabled ? '已启用，关闭即停用' : '已停用，打开即启用'}>
        <span className="dbz-graph-switch" data-no-drag>
          <Switch
            size="small"
            checked={enabled}
            loading={toggling}
            disabled={toggling}
            aria-label={enabled ? '停用' : '启用'}
            aria-checked={enabled}
            data-node-toggle={node.kind}
            onChange={(checked) => {
              onSelect(node.key);
              onToggle(node, checked);
            }}
          />
        </span>
      </Tooltip>
    </div>
  );
}

export default function ControlPanelCanvas({
  nodes,
  edges,
  selectedNodeKey,
  selectedEdgeId,
  onSelectNode,
  onSelectEdge,
  onAddChild,
  onAddRoot,
  onNodeAction,
  onToggleNode,
  busyNodeKey,
  togglingNodeKey,
  checkingKey,
  restartingKey,
  searchHits,
  activeHitKey,
  focusToken,
  renderInspector,
  inspectorOpen,
  onToggleInspector,
  onOpenInspector,
}: ControlPanelCanvasProps) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<ViewState>({ x: 0, y: 0, scale: 1 });
  const [viewportSize, setViewportSize] = useState({ w: 0, h: 0 });
  /** 平移进行中：临时隐藏缩略图，避免盖住右下角 */
  const [interacting, setInteracting] = useState(false);
  /** 属性面板宽度：拖左缘调整，双击恢复默认，localStorage 记住 */
  const [inspectorWidth, setInspectorWidth] = useState(() => {
    try {
      const raw = Number(localStorage.getItem('dbz-inspector-width'));
      return Number.isFinite(raw) && raw >= INSPECTOR_MIN_W && raw <= INSPECTOR_MAX_W
        ? raw
        : INSPECTOR_DEFAULT_W;
    } catch {
      return INSPECTOR_DEFAULT_W;
    }
  });
  /** 泳道列宽：四列各自可拖，localStorage 记住 */
  const [columns, setColumns] = useState<number[]>(readLaneColumns);
  const fittedRef = useRef(false);

  /** 泳道布局：位置完全由数据推导，节点增删后自动重排；列宽变了卡片宽度跟着变 */
  const layout = useMemo(
    () => buildSwimlaneLayout(nodes, edges, columns),
    [nodes, edges, columns],
  );
  const positions = layout.positions;
  const sizes = layout.sizes;

  const nodeMap = useMemo(() => new Map(nodes.map((node) => [node.key, node])), [nodes]);

  /** 连线几何：归属关系用三次贝塞尔横着连过去，父右缘中点 → 子左缘中点 */
  const geometries = useMemo(() => {
    const result: EdgeGeometry[] = [];
    for (const edge of edges) {
      const from = nodeMap.get(edge.from);
      const to = nodeMap.get(edge.to);
      if (!from || !to) {
        continue;
      }
      const fromPos = positions[from.key];
      const toPos = positions[to.key];
      const fromSize = sizes[from.key];
      const toSize = sizes[to.key];
      if (!fromPos || !toPos || !fromSize || !toSize) {
        continue;
      }
      const start: Point = { x: fromPos.x + fromSize.w, y: fromPos.y + fromSize.h / 2 };
      const end: Point = { x: toPos.x, y: toPos.y + toSize.h / 2 };
      // 控制点按水平距离取一半，短连线也留一点弯，长连线中途不鼓包
      const dx = Math.max(24, Math.abs(end.x - start.x) / 2);
      result.push({
        edge,
        path: `M${start.x},${start.y} C${start.x + dx},${start.y} ${end.x - dx},${end.y} ${end.x},${end.y}`,
        color: '#b7c2d1',
        marker: 'dbz-arrow-edge',
      });
    }
    return result;
  }, [edges, nodeMap, positions, sizes]);

  /** 搜索高亮：命中节点只高亮，不隐藏其它节点，整体拓扑始终保留 */
  const hitSet = useMemo(() => new Set(searchHits), [searchHits]);

  /** 定位到当前命中：把节点中心移到可见区域中心（列头下方），缩放保持用户当前值 */
  useEffect(() => {
    if (!activeHitKey || !viewportSize.w || !viewportSize.h) {
      return;
    }
    const node = nodeMap.get(activeHitKey);
    const pos = positions[activeHitKey];
    const size = sizes[activeHitKey];
    if (!node || !pos || !size) {
      return;
    }
    setView((previous) => ({
      ...previous,
      x: viewportSize.w / 2 - (pos.x + size.w / 2) * previous.scale,
      y:
        LANE_HEADER_H +
        (viewportSize.h - LANE_HEADER_H) / 2 -
        (pos.y + size.h / 2) * previous.scale,
    }));
    // focusToken 让同一个命中在反复点「下一个」时也能重新居中
  }, [activeHitKey, focusToken, nodeMap, positions, sizes, viewportSize]);

  /** 画布尺寸跟着容器变化，缩略图与适应画布要用 */
  useEffect(() => {
    const element = viewportRef.current;
    if (!element) {
      return;
    }
    const observer = new ResizeObserver(([entry]) => {
      setViewportSize({ w: entry.contentRect.width, h: entry.contentRect.height });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const fitView = useCallback(
    (widthOverride?: number) => {
      if (!nodes.length || !viewportSize.w || !viewportSize.h) {
        return;
      }
    // 属性面板是画布右侧的一列，视口本身就少掉那块宽度，不再另留空间；
    // 顶部让出固定列头，图从表头下面往下排（与 syn / uiam 一样贴顶，不垂直居中），
    // 横向居中只是在窄图时让左右留白均匀
      const pad = 16;
      // 面板开合后视口宽度会先于 ResizeObserver 回调变化，这里允许直接传入新宽度
      const width = widthOverride ?? viewportSize.w;
      const availableWidth = Math.max(1, width - pad * 2);
      const availableHeight = Math.max(1, viewportSize.h - LANE_HEADER_H - pad * 2);
      const scale = clamp(
        Math.min(availableWidth / layout.width, availableHeight / layout.height),
        MIN_SCALE,
        1.1,
      );
      setView({
        scale,
        x: pad + (availableWidth - layout.width * scale) / 2,
        y: LANE_HEADER_H + pad,
      });
    },
    [nodes.length, layout.width, layout.height, viewportSize],
  );

  /** 第一次拿到节点时自动适应一次；之后由用户自己控制视口 */
  useEffect(() => {
    if (!fittedRef.current && nodes.length > 0 && viewportSize.w > 0) {
      fittedRef.current = true;
      fitView();
    }
  }, [fitView, nodes.length, viewportSize.w]);

  /**
   * 属性面板开合会改变视口宽度：立刻按新的容器宽度适应一次，
   * 否则右侧两列会被新开的面板挤到看不见（画布不滚动，躲不出去）。
   * 这里直接量 DOM，不等 ResizeObserver，避免它晚一步把用户刚点的缩放盖掉。
   */
  const inspectorOpenRef = useRef(inspectorOpen);
  useEffect(() => {
    if (inspectorOpenRef.current === inspectorOpen) {
      return;
    }
    inspectorOpenRef.current = inspectorOpen;
    const width = viewportRef.current?.clientWidth;
    if (width) {
      fitView(width);
    }
  }, [inspectorOpen, fitView]);

  /** 滚轮缩放：以指针为中心，缩放前后指针下的世界坐标不移动 */
  useEffect(() => {
    const element = viewportRef.current;
    if (!element) {
      return;
    }
    const handleWheel = (event: WheelEvent) => {
      event.preventDefault();
      const rect = element.getBoundingClientRect();
      const pointerX = event.clientX - rect.left;
      const pointerY = event.clientY - rect.top;
      setView((previous) => {
        const factor = event.deltaY < 0 ? 1.12 : 1 / 1.12;
        const nextScale = clamp(previous.scale * factor, MIN_SCALE, MAX_SCALE);
        const worldX = (pointerX - previous.x) / previous.scale;
        const worldY = (pointerY - previous.y) / previous.scale;
        return {
          scale: nextScale,
          x: pointerX - worldX * nextScale,
          y: pointerY - worldY * nextScale,
        };
      });
    };
    element.addEventListener('wheel', handleWheel, { passive: false });
    return () => element.removeEventListener('wheel', handleWheel);
  }, []);

  /** 空白处拖动画布 */
  const handleViewportPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) {
      return;
    }
    const target = event.target as HTMLElement;
    if (
      target.closest('.dbz-graph-node') ||
      target.closest('.dbz-graph-edge-hit') ||
      target.closest('.dbz-graph-dock') ||
      target.closest('.dbz-graph-lane-grip') ||
      target.closest('.dbz-graph-lane-add')
    ) {
      return;
    }
    const startX = event.clientX;
    const startY = event.clientY;
    const origin = view;
    let moved = false;
    const move = (moveEvent: PointerEvent) => {
      if (
        !moved &&
        (Math.abs(moveEvent.clientX - startX) > 2 || Math.abs(moveEvent.clientY - startY) > 2)
      ) {
        moved = true;
        setInteracting(true);
      }
      setView({
        ...origin,
        x: origin.x + moveEvent.clientX - startX,
        y: origin.y + moveEvent.clientY - startY,
      });
    };
    const up = () => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      setInteracting(false);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /**
   * # 拖动属性面板左缘调宽
   *
   * 向左拖 = 变宽；松手落 localStorage，双击把手恢复默认宽度。
   */
  const handleInspectorResizeDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = inspectorWidth;
    const maxWidth = Math.min(INSPECTOR_MAX_W, Math.max(INSPECTOR_MIN_W, viewportSize.w - 48));
    const nextOf = (clientX: number) =>
      Math.round(clamp(startWidth + (startX - clientX), INSPECTOR_MIN_W, maxWidth));
    const move = (moveEvent: PointerEvent) => {
      setInspectorWidth(nextOf(moveEvent.clientX));
    };
    const up = (upEvent: PointerEvent) => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      setInspectorWidth(nextOf(upEvent.clientX));
      try {
        localStorage.setItem('dbz-inspector-width', String(nextOf(upEvent.clientX)));
      } catch {
        /* 隐私模式等存不了就算了 */
      }
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  const resetInspectorWidth = () => {
    setInspectorWidth(INSPECTOR_DEFAULT_W);
    try {
      localStorage.setItem('dbz-inspector-width', String(INSPECTOR_DEFAULT_W));
    } catch {
      /* 同上 */
    }
  };

  /**
   * # 拖动列头右缘调列宽
   *
   * 只改自己这一列，不动相邻列；拖动距离要除以画布缩放比，否则缩放后手感不对。
   * 拖动期间锁住文字选择与光标，Esc 整份回退，松手才写 localStorage。
   * 双击同一根拖手：该列回到默认宽度。
   */
  const startColumnDrag = (index: number) => (event: ReactPointerEvent<HTMLElement>) => {
    if (event.button !== 0) {
      return;
    }
    event.stopPropagation();
    const startX = event.clientX;
    const startWidth = columns[index];
    const origin = columns.slice();
    const nextWidthOf = (clientX: number) =>
      clampLaneColumn(startWidth + (clientX - startX) / view.scale);

    const apply = (width: number) => {
      setColumns((previous) => {
        const next = previous.slice();
        next[index] = width;
        return next;
      });
    };
    const body = document.body;
    const bodyCursor = body.style.cursor;
    const bodySelect = body.style.userSelect;
    body.style.cursor = 'col-resize';
    body.style.userSelect = 'none';

    let latest = startWidth;
    const finish = () => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      window.removeEventListener('keydown', onKeyDown);
      body.style.cursor = bodyCursor;
      body.style.userSelect = bodySelect;
    };
    const move = (moveEvent: PointerEvent) => {
      latest = nextWidthOf(moveEvent.clientX);
      apply(latest);
    };
    const up = () => {
      finish();
      writeLaneColumns(
        columns.map((width, columnIndex) => (columnIndex === index ? latest : width)),
      );
    };
    const onKeyDown = (keyEvent: KeyboardEvent) => {
      if (keyEvent.key !== 'Escape') {
        return;
      }
      keyEvent.preventDefault();
      finish();
      setColumns(origin);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
    window.addEventListener('keydown', onKeyDown);
  };

  const resetColumnWidth = (index: number) => {
    const next = columns.slice();
    next[index] = DEFAULT_LANE_COLUMNS[index];
    setColumns(next);
    writeLaneColumns(next);
  };

  /** 百分比点击回到 100%，与参考实现的缩放控制一致 */
  const resetZoom = () => {
    setView((previous) => ({ ...previous, scale: 1 }));
  };

  /** 定位当前选中节点：缩放到可读比例后把节点居中 */
  const focusSelectedNode = () => {
    if (!selectedNodeKey) {
      return;
    }
    const pos = positions[selectedNodeKey];
    const size = sizes[selectedNodeKey];
    if (!pos || !size || !viewportSize.w || !viewportSize.h) {
      return;
    }
    setView((previous) => {
      const scale = clamp(previous.scale, 0.7, 1.2);
      return {
        scale,
        x: viewportSize.w / 2 - (pos.x + size.w / 2) * scale,
        y:
          LANE_HEADER_H +
          (viewportSize.h - LANE_HEADER_H) / 2 -
          (pos.y + size.h / 2) * scale,
      };
    });
  };

  /** 缩略图范围：整张泳道图（含标题列与走线通道） */
  const bounds = useMemo(
    () => ({ minX: 0, minY: 0, width: layout.width, height: layout.height }),
    [layout.width, layout.height],
  );

  const minimapViewport = useMemo(() => {
    if (!viewportSize.w || !viewportSize.h) {
      return null;
    }
    return {
      x: (-view.x / view.scale - bounds.minX) / bounds.width,
      y: (-view.y / view.scale - bounds.minY) / bounds.height,
      w: viewportSize.w / view.scale / bounds.width,
      h: viewportSize.h / view.scale / bounds.height,
    };
  }, [bounds, view, viewportSize]);

  /** 整图都已可见时隐藏覆盖层，避免缩略图盖住被适应放大的卡片 */
  const overviewScale = Math.min(
    viewportSize.w / bounds.width,
    viewportSize.h / bounds.height,
  );
  const hideOverlays = !nodes.length || !viewportSize.w || view.scale <= overviewScale * 1.03;

  return (
    <div className="dbz-canvas-shell">
      <div className="dbz-canvas-main">
        <div
          className="dbz-graph-viewport"
          ref={viewportRef}
          onPointerDown={handleViewportPointerDown}
        >
          <div
            className="dbz-graph-world"
            style={{
              width: layout.width,
              height: layout.height,
              transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})`,
            }}
          >
            {/* 泳道列：纵向带，列头单独固定在视口顶部；空列保留可见带并给出提示 */}
            {layout.lanes.map((lane) => (
              <div
                key={lane.kind}
                className={`dbz-graph-lane dbz-graph-lane--${lane.kind}`}
                data-lane={lane.kind}
                style={{ left: lane.x, width: lane.w, top: 0, height: layout.height }}
              >
                {lane.count === 0 && (
                  <div className="dbz-graph-lane-empty">
                    暂无{NODE_KIND_LABEL[lane.kind]}，从上一级卡片的「+」新增
                  </div>
                )}
              </div>
            ))}

            <svg
              className="dbz-graph-edges"
              width={layout.width}
              height={layout.height}
              aria-hidden
            >
              <defs>
                <marker id="dbz-arrow-edge" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
                  <path d="M0,0 L10,5 L0,10 z" fill="#b7c2d1" />
                </marker>
              </defs>
              {geometries.map((geometry) => (
                <g key={geometry.edge.id}>
                  <path
                    className={`dbz-graph-edge-line${selectedEdgeId === geometry.edge.id ? ' is-selected' : ''}`}
                    d={geometry.path}
                    stroke={geometry.color}
                    markerEnd={`url(#${geometry.marker})`}
                  />
                  {/* 流向动画：小圆点从上游流向下游；选中后加快。reduced-motion 下静止隐藏 */}
                  <path className="dbz-graph-edge-flow" d={geometry.path} stroke={geometry.color} />
                  {/* 上下游归属固定，走线自动路由；点选只查看归属说明，不能删除 */}
                  <path
                    className="dbz-graph-edge-hit"
                    data-edge-id={geometry.edge.id}
                    d={geometry.path}
                    onPointerDown={(event) => {
                      event.stopPropagation();
                      onSelectEdge(geometry.edge.id);
                    }}
                  >
                    <title>归属连线：上下游关系固定，走线自动路由，不能删除</title>
                  </path>
                </g>
              ))}
            </svg>

            {nodes.map((node) => {
              const pos = positions[node.key];
              const size = sizes[node.key];
              if (!pos || !size) {
                return null;
              }
              const selected = selectedNodeKey === node.key;
              const childKind = CHILD_KIND[node.kind];
              return (
                <div
                  key={node.key}
                  className={`dbz-graph-node dbz-graph-node--${node.kind}${
                    nodeDisabledOf(node) ? ' is-disabled' : ''
                  }${selected ? ' is-selected' : ''}${
                    hitSet.has(node.key) ? ' is-hit' : ''
                  }${activeHitKey === node.key ? ' is-active-hit' : ''}`}
                  data-node-key={node.key}
                  data-node-type={node.kind}
                  // 卡片是图上唯一的可选单元：键盘也要能停上去选中，不能只有鼠标
                  tabIndex={0}
                  role="group"
                  aria-label={`${NODE_KIND_LABEL[node.kind]} ${node.name}`}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault();
                      onSelectNode(node.key);
                    }
                  }}
                  style={{ left: pos.x, top: pos.y, width: size.w, height: size.h }}
                  onPointerDown={(event) => {
                    if (event.button !== 0) {
                      return;
                    }
                    if ((event.target as HTMLElement).closest('[data-no-drag]')) {
                      return;
                    }
                    event.stopPropagation();
                    onSelectNode(node.key);
                  }}
                  // 双击卡片 = 选中并展开属性面板（单击只选中，不自动弹面板）
                  onDoubleClick={(event) => {
                    event.stopPropagation();
                    onSelectNode(node.key);
                    onOpenInspector();
                  }}
                >
                  <NodeHead
                    node={node}
                    togglingNodeKey={togglingNodeKey}
                    onSelect={onSelectNode}
                    onToggle={onToggleNode}
                  />

                  <div className="dbz-graph-node-name" title={node.name}>
                    {node.name}
                  </div>

                  <Tooltip
                    title={node.sub}
                    placement="topLeft"
                    styles={{ root: { maxWidth: 480 } }}
                  >
                    <div
                      className="dbz-graph-node-sub dbz-mono"
                      style={
                        node.kind === 'sink'
                          ? {
                              whiteSpace: 'pre-line',
                              wordBreak: 'break-all',
                              overflow: 'visible',
                              textOverflow: 'clip',
                            }
                          : undefined
                      }
                    >
                      {node.sub}
                    </div>
                  </Tooltip>

                  <div className="dbz-graph-node-status" data-no-drag>
                    <NodeStatus node={node} />
                    {(node.kind === 'source' || node.kind === 'sink') &&
                      (node.record as ManagedConnectorRecord).inconsistent && (
                        <Tooltip title="库内配置与 Connect 实际不一致，或远端已不存在；点击查看两边配置并同步">
                          <Tag
                            color="warning"
                            className="dbz-graph-inconsistent-tag"
                            data-no-drag
                            onClick={() => onNodeAction(node, 'inspect-inconsistent')}
                          >
                            不一致
                          </Tag>
                        </Tooltip>
                      )}
                    {node.dead > 0 && (
                      <Tooltip title={`待处理死信 ${node.dead} 条，点击进入死信页并按该连接器筛选`}>
                        <Tag
                          color="error"
                          className="dbz-graph-dead-tag"
                          data-no-drag
                          onClick={() => onNodeAction(node, 'dead-letter')}
                        >
                          死信 {node.dead}
                        </Tag>
                      </Tooltip>
                    )}
                  </div>

                  <NodeActions
                    node={node}
                    busyNodeKey={busyNodeKey}
                    checkingKey={checkingKey}
                    restartingKey={restartingKey}
                    onSelect={onSelectNode}
                    onAction={onNodeAction}
                  />

                  {/* 有下级的节点底部给一个「+」，新增只从这里走；sink 没有下级 */}
                  {childKind && (
                    <button
                      type="button"
                      className="dbz-graph-add-child"
                      data-no-drag
                      data-add-child={childKind}
                      title={`新增${NODE_KIND_LABEL[childKind]}`}
                      aria-label={`新增${NODE_KIND_LABEL[childKind]}`}
                      onPointerDown={(event) => event.stopPropagation()}
                      onClick={(event) => {
                        event.stopPropagation();
                        onAddChild(node);
                      }}
                    >
                      <PlusOutlined />
                    </button>
                  )}
                </div>
              );
            })}
          </div>

          {/* 列头：像表头一样固定在视口顶部，横向跟随泳道列平移缩放，纵向不动 */}
          <div className="dbz-graph-lane-headers">
            {layout.lanes.map((lane, index) => {
              const parentKind = PARENT_KIND[lane.kind];
              const parents = parentKind
                ? nodes.filter((node) => node.kind === parentKind)
                : [];
              return (
                <div
                  key={lane.kind}
                  className={`dbz-graph-lane-header dbz-graph-lane-header--${lane.kind}`}
                  data-lane-header={lane.kind}
                  style={{
                    left: view.x + lane.x * view.scale,
                    width: lane.w * view.scale,
                  }}
                >
                  <span className="dbz-graph-lane-name">{NODE_KIND_LABEL[lane.kind]}</span>
                  <span className="dbz-graph-lane-count">{lane.count}</span>
                  {/* 列头「＋」：新建本列节点；子列先选出上一级，父级仍由用户挑 */}
                  {lane.kind === 'broker' ? (
                    <button
                      type="button"
                      className="dbz-graph-lane-add"
                      data-lane-add="broker"
                      title="新增 Kafka 集群"
                      aria-label="新增 Kafka 集群"
                      onPointerDown={(event) => event.stopPropagation()}
                      onClick={() => onAddRoot()}
                    >
                      <PlusOutlined />
                    </button>
                  ) : parents.length ? (
                    <Dropdown
                      trigger={['click']}
                      menu={{
                        items: parents.map((node) => ({
                          key: node.key,
                          label: node.name,
                          onClick: () => onAddChild(node),
                        })),
                      }}
                    >
                      <button
                        type="button"
                        className="dbz-graph-lane-add"
                        data-lane-add={lane.kind}
                        title={`新增${NODE_KIND_LABEL[lane.kind]}：先选所属${NODE_KIND_LABEL[parentKind!]}`}
                        aria-label={`新增${NODE_KIND_LABEL[lane.kind]}`}
                        onPointerDown={(event) => event.stopPropagation()}
                      >
                        <PlusOutlined />
                      </button>
                    </Dropdown>
                  ) : (
                    <button
                      type="button"
                      className="dbz-graph-lane-add"
                      data-lane-add={lane.kind}
                      disabled
                      title={`先新增${NODE_KIND_LABEL[parentKind!]}，再从这里新增`}
                      aria-label={`新增${NODE_KIND_LABEL[lane.kind]}`}
                    >
                      <PlusOutlined />
                    </button>
                  )}
                  {/* 列宽拖手：落在两列之间的缝里，只改自己这一列；双击复位 */}
                  <button
                    type="button"
                    className="dbz-graph-lane-grip"
                    aria-label={`调整${NODE_KIND_LABEL[lane.kind]}列宽`}
                    title="拖动调整列宽，双击恢复默认"
                    onPointerDown={startColumnDrag(index)}
                    onDoubleClick={(event) => {
                      event.stopPropagation();
                      resetColumnWidth(index);
                    }}
                  />
                </div>
              );
            })}
          </div>

          {/* 右下角缩放坞：− / 百分比（点击回 100%）/ ＋ / 适应 / 定位选中；缩略图叠在它上面 */}
          <div className="dbz-graph-dock">
            {!hideOverlays && !interacting && (
              <div className="dbz-graph-minimap" title="缩略图：点击可移动视口">
                <svg
                  viewBox={`${bounds.minX - 20} ${bounds.minY - 20} ${bounds.width + 40} ${bounds.height + 40}`}
                  preserveAspectRatio="xMidYMid meet"
                  onClick={(event) => {
                    const rect = event.currentTarget.getBoundingClientRect();
                    const worldX =
                      bounds.minX + ((event.clientX - rect.left) / rect.width) * bounds.width;
                    const worldY =
                      bounds.minY + ((event.clientY - rect.top) / rect.height) * bounds.height;
                    setView((prev) => ({
                      ...prev,
                      x: viewportSize.w / 2 - worldX * prev.scale,
                      y: viewportSize.h / 2 - worldY * prev.scale,
                    }));
                  }}
                >
                  {layout.lanes.map((lane) => (
                    <rect
                      key={lane.kind}
                      x={lane.x}
                      y={bounds.minY}
                      width={lane.w}
                      height={bounds.height}
                      rx={6}
                      fill="rgba(22,119,255,0.05)"
                    />
                  ))}
                  {nodes.map((node) => {
                    const pos = positions[node.key];
                    const size = sizes[node.key];
                    if (!pos || !size) {
                      return null;
                    }
                    return (
                      <rect
                        key={node.key}
                        x={pos.x}
                        y={pos.y}
                        width={size.w}
                        height={size.h}
                        rx={8}
                        fill={
                          nodeTone(node) === 'success'
                            ? '#b7eb8f'
                            : nodeTone(node) === 'error'
                              ? '#ffa39e'
                              : nodeTone(node) === 'warning'
                                ? '#ffe58f'
                                : '#d9d9d9'
                        }
                      />
                    );
                  })}
                  {minimapViewport && (
                    <rect
                      x={bounds.minX + minimapViewport.x * bounds.width}
                      y={bounds.minY + minimapViewport.y * bounds.height}
                      width={minimapViewport.w * bounds.width}
                      height={minimapViewport.h * bounds.height}
                      fill="rgba(22,119,255,0.12)"
                      stroke="#1677ff"
                      strokeWidth={2}
                    />
                  )}
                </svg>
              </div>
            )}

            <div className="dbz-graph-controls">
              <button
                type="button"
                onClick={() =>
                  setView((prev) => ({
                    ...prev,
                    scale: clamp(prev.scale / 1.2, MIN_SCALE, MAX_SCALE),
                  }))
                }
                title="缩小"
              >
                <MinusOutlined />
              </button>
              <button
                type="button"
                className="dbz-graph-scale"
                title="回到 100%"
                onClick={resetZoom}
              >
                {Math.round(view.scale * 100)}%
              </button>
              <button
                type="button"
                onClick={() =>
                  setView((prev) => ({
                    ...prev,
                    scale: clamp(prev.scale * 1.2, MIN_SCALE, MAX_SCALE),
                  }))
                }
                title="放大"
              >
                <PlusOutlined />
              </button>
              <span className="dbz-graph-sep" aria-hidden />
              <button type="button" onClick={() => fitView()} title="适应画布">
                <ExpandOutlined />
              </button>
              {selectedNodeKey && (
                <button type="button" onClick={focusSelectedNode} title="定位选中">
                  <AimOutlined />
                </button>
              )}
            </div>
          </div>
        </div>

        {/* 属性面板：画布右侧独立一列（不是悬浮层），不遮卡片；可拖左缘调宽 */}
        {inspectorOpen ? (
          <div className="dbz-graph-inspector" style={{ width: inspectorWidth }}>
            <div
              className="dbz-graph-inspector-resize"
              title="拖动调宽，双击恢复默认"
              onPointerDown={handleInspectorResizeDown}
              onDoubleClick={resetInspectorWidth}
            />
            <div className="dbz-graph-inspector-chrome">
              <span className="dbz-graph-inspector-chrome-title">属性面板</span>
              <button
                type="button"
                className="dbz-graph-inspector-collapse"
                title="收起"
                onClick={onToggleInspector}
              >
                <CloseOutlined />
              </button>
            </div>
            <div className="dbz-graph-inspector-body">{renderInspector()}</div>
          </div>
        ) : (
          <button
            type="button"
            className="dbz-graph-inspector-rail"
            title="打开属性面板"
            onClick={onToggleInspector}
          >
            <ProfileOutlined />
            <span>属性</span>
          </button>
        )}
      </div>
    </div>
  );
}