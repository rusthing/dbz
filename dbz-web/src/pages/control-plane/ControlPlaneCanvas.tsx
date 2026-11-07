/**
 * # 控制面画布
 *
 * 交互按「只选上游、其余自动推导」收敛：
 * - 节点可拖、空白可平移、滚轮以指针为中心缩放、缩略图、正交折线箭头；
 * - 连线只作展示：两端都没有把手，不能拖动改上下游，也不能删除；
 * - 新增下级只走节点底部的「+」按钮，点击后由页面打开父级已固定的表单；
 * - 四类卡片同一骨架，类型图标 + 类型文字 + 主色用于识别，状态只用标签表达。
 *
 * 组件只负责画与拖，真正写库的动作（保存位置、打开表单、节点操作）由页面调用。
 */

import {
  CloudServerOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  EditOutlined,
  ExportOutlined,
  HistoryOutlined,
  ImportOutlined,
  MinusOutlined,
  PlusOutlined,
  ReloadOutlined,
  SyncOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Button, Switch, Tag, Tooltip } from 'antd';
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
  NODE_KIND_COLOR,
  NODE_KIND_LABEL,
  NODE_SIZE,
  defaultPosition,
  nodeTone,
  type CanvasEdge,
  type CanvasNode,
  type CanvasNodeAction,
  type CanvasNodeKind,
} from './canvas-model';

/** 画布缩放范围 */
const MIN_SCALE = 0.35;
const MAX_SCALE = 2;
/** 世界坐标系的逻辑尺寸，节点被拖到边缘也不会裁掉 */
const WORLD_WIDTH = 2400;
const WORLD_HEIGHT = 1600;

/** 「+」能新增的下级类型；下游连接器没有下一级，不显示按钮 */
const CHILD_KIND: Partial<Record<CanvasNodeKind, CanvasNodeKind>> = {
  broker: 'connect',
  connect: 'source',
  source: 'sink',
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
  start: Point;
  end: Point;
  /** 正交走线的中间水平段 Y；拖动折点即改这个值 */
  midY: number;
  handle: Point;
  color: string;
  marker: string;
}

interface ControlPlaneCanvasProps {
  nodes: CanvasNode[];
  edges: CanvasEdge[];
  positions: Record<string, Point>;
  /** 已持久化的连线折点：edgeId → 中间水平段 Y */
  edgeBends: Record<string, number>;
  selectedNodeKey: string | null;
  selectedEdgeId: string | null;
  /** 拖动过程中持续回调，`commit=false`；松手时 `commit=true` 由页面落库 */
  onNodeMove: (key: string, x: number, y: number, commit: boolean) => void;
  /** 拖动折点：commit=false 只更新本地，松手才落库 */
  onEdgeBend: (edgeId: string, midY: number, commit: boolean) => void;
  onSelectNode: (key: string | null) => void;
  onSelectEdge: (id: string | null) => void;
  /** 点击节点底部「+」：父级固定，页面按子类型打开新增表单 */
  onAddChild: (node: CanvasNode) => void;
  /** 组件面板点击「Kafka 集群」：在空白处建根节点 */
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

/** 连线折点的取值范围：留出画布内边距，拖出卡片重叠区也不会跑到世界外 */
const BEND_MIN_Y = 16;
const BEND_MAX_Y = WORLD_HEIGHT - 16;

/** 正交走线的默认中间水平段 Y：两端上下居中的位置 */
function defaultMidY(start: Point, end: Point): number {
  return (start.y + end.y) / 2;
}

/** 连线几何：归属关系统一走竖向折线；同源多条线在底部错开，避免重叠 */
function geometryOf(
  edge: CanvasEdge,
  nodeMap: Map<string, CanvasNode>,
  positions: Record<string, Point>,
  offset: number,
  bend?: number,
): EdgeGeometry | null {
  const from = nodeMap.get(edge.from);
  const to = nodeMap.get(edge.to);
  if (!from || !to) {
    return null;
  }
  const fromPos = positions[from.key] ?? defaultPosition(from.kind, 0);
  const toPos = positions[to.key] ?? defaultPosition(to.kind, 0);
  const fromSize = NODE_SIZE[from.kind];
  const toSize = NODE_SIZE[to.kind];

  const startX = clamp(
    fromPos.x + fromSize.w / 2 + offset,
    fromPos.x + 18,
    fromPos.x + fromSize.w - 18,
  );
  const start = { x: startX, y: fromPos.y + fromSize.h };
  const end = { x: toPos.x + toSize.w / 2, y: toPos.y };
  // 端点由归属关系固定，只有中间水平段在竖直方向可拖；拖过的值优先
  const midY = clamp(
    typeof bend === 'number' && Number.isFinite(bend) ? bend : defaultMidY(start, end),
    BEND_MIN_Y,
    BEND_MAX_Y,
  );
  return {
    edge,
    start,
    end,
    midY,
    handle: { x: (start.x + end.x) / 2, y: midY },
    path: `M${start.x},${start.y} L${start.x},${midY} L${end.x},${midY} L${end.x},${end.y}`,
    color: '#b7c2d1',
    marker: 'dbz-arrow-edge',
  };
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

/** 状态标签：悬停给出运行详情；标签本身不参与卡片拖动 */
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

/** 卡片操作行：编辑在最左，右侧依次重启/重置/重新同步/检测，删除隔一道分隔线放最右 */
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

  // 重新同步只给上游连接器：向下游发增量快照没有「重读源表」这回事
  if (node.kind === 'source') {
    rightItems.push({
      action: 'resync',
      title: '重新同步：选表后写 execute-snapshot（增量），不重置位点、不停止连接器',
      icon: <SyncOutlined />,
    });
  }

  rightItems.push({
    action: 'check',
    title:
      node.kind === 'broker'
        ? '集群检测：检测 Kafka 连通性'
        : node.kind === 'connect'
          ? '健康检测：可达性 / 版本 / worker'
          : '一致性检测：库内配置与 Connect 实际状态是否一致',
    icon: <ThunderboltOutlined />,
    loading: checkingKey === node.key,
  });

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
      {/* 编辑靠最左；开关已经移到卡片左上角，这里不再重复 */}
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

/** 卡片头部：启用开关在左上角，后面跟类型标识；开关不参与卡片拖动 */
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
      <span
        className="dbz-graph-node-icon"
        style={{ backgroundColor: NODE_KIND_COLOR[node.kind] }}
      >
        <NodeIcon kind={node.kind} />
      </span>
      <span
        className="dbz-graph-node-type-label"
        style={{ color: NODE_KIND_COLOR[node.kind] }}
      >
        {NODE_KIND_LABEL[node.kind]}
      </span>
    </div>
  );
}

export default function ControlPlaneCanvas({
  nodes,
  edges,
  positions,
  edgeBends,
  selectedNodeKey,
  selectedEdgeId,
  onNodeMove,
  onEdgeBend,
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
}: ControlPlaneCanvasProps) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<ViewState>({ x: 0, y: 0, scale: 1 });
  const [viewportSize, setViewportSize] = useState({ w: 0, h: 0 });
  const fittedRef = useRef(false);

  const nodeMap = useMemo(() => new Map(nodes.map((node) => [node.key, node])), [nodes]);

  /** 同一个上游节点引出的多条归属线在底部错开，避免叠在同一个点上 */
  const edgeOffsets = useMemo(() => {
    const groups = new Map<string, CanvasEdge[]>();
    for (const edge of edges) {
      const list = groups.get(edge.from);
      if (list) {
        list.push(edge);
      } else {
        groups.set(edge.from, [edge]);
      }
    }
    const result = new Map<string, number>();
    for (const list of groups.values()) {
      list.forEach((edge, index) => {
        result.set(edge.id, (index - (list.length - 1) / 2) * 26);
      });
    }
    return result;
  }, [edges]);

  const geometries = useMemo(
    () =>
      edges
        .map((edge) =>
          geometryOf(
            edge,
            nodeMap,
            positions,
            edgeOffsets.get(edge.id) ?? 0,
            edgeBends[edge.id],
          ),
        )
        .filter((item): item is EdgeGeometry => Boolean(item)),
    [edges, nodeMap, positions, edgeOffsets, edgeBends],
  );

  /** 搜索高亮：命中节点只高亮，不隐藏其它节点，整体拓扑始终保留 */
  const hitSet = useMemo(() => new Set(searchHits), [searchHits]);

  /** 定位到当前命中：把节点中心移到视口中心，缩放保持用户当前值 */
  useEffect(() => {
    if (!activeHitKey || !viewportSize.w || !viewportSize.h) {
      return;
    }
    const node = nodeMap.get(activeHitKey);
    if (!node) {
      return;
    }
    const pos = positions[activeHitKey] ?? defaultPosition(node.kind, 0);
    const size = NODE_SIZE[node.kind];
    setView((previous) => ({
      ...previous,
      x: viewportSize.w / 2 - (pos.x + size.w / 2) * previous.scale,
      y: viewportSize.h / 2 - (pos.y + size.h / 2) * previous.scale,
    }));
    // focusToken 让同一个命中在反复点「下一个」时也能重新居中
  }, [activeHitKey, focusToken, nodeMap, positions, viewportSize]);

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

  const fitView = useCallback(() => {
    if (!nodes.length || !viewportSize.w || !viewportSize.h) {
      return;
    }
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const node of nodes) {
      const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
      const size = NODE_SIZE[node.kind];
      minX = Math.min(minX, pos.x);
      minY = Math.min(minY, pos.y);
      maxX = Math.max(maxX, pos.x + size.w);
      maxY = Math.max(maxY, pos.y + size.h);
    }
    const width = Math.max(1, maxX - minX);
    const height = Math.max(1, maxY - minY);
    const scale = clamp(
      Math.min((viewportSize.w - 80) / width, (viewportSize.h - 80) / height),
      MIN_SCALE,
      1.1,
    );
    setView({
      scale,
      x: (viewportSize.w - width * scale) / 2 - minX * scale,
      y: (viewportSize.h - height * scale) / 2 - minY * scale,
    });
  }, [nodes, positions, viewportSize]);

  /** 第一次拿到节点时自动适应一次；之后由用户自己控制视口 */
  useEffect(() => {
    if (!fittedRef.current && nodes.length > 0 && viewportSize.w > 0) {
      fittedRef.current = true;
      fitView();
    }
  }, [fitView, nodes.length, viewportSize.w]);

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
      target.closest('.dbz-graph-bend-handle') ||
      target.closest('.dbz-graph-controls') ||
      target.closest('.dbz-graph-minimap')
    ) {
      return;
    }
    const startX = event.clientX;
    const startY = event.clientY;
    const origin = view;
    const move = (moveEvent: PointerEvent) => {
      setView({
        ...origin,
        x: origin.x + moveEvent.clientX - startX,
        y: origin.y + moveEvent.clientY - startY,
      });
    };
    const up = () => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /** 节点拖拽：pointerdown 记录起点，move 更新本地位置，up 落库 */
  const handleNodePointerDown = (event: ReactPointerEvent<HTMLDivElement>, node: CanvasNode) => {
    if (event.button !== 0) {
      return;
    }
    if ((event.target as HTMLElement).closest('[data-no-drag]')) {
      return;
    }
    event.stopPropagation();
    onSelectNode(node.key);
    const origin = positions[node.key] ?? defaultPosition(node.kind, 0);
    const startClientX = event.clientX;
    const startClientY = event.clientY;
    const scale = view.scale;
    let moved = false;

    const move = (moveEvent: PointerEvent) => {
      const dx = (moveEvent.clientX - startClientX) / scale;
      const dy = (moveEvent.clientY - startClientY) / scale;
      if (Math.abs(dx) > 2 || Math.abs(dy) > 2) {
        moved = true;
      }
      if (moved) {
        onNodeMove(node.key, origin.x + dx, origin.y + dy, false);
      }
    };
    const up = (upEvent: PointerEvent) => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      if (moved) {
        const dx = (upEvent.clientX - startClientX) / scale;
        const dy = (upEvent.clientY - startClientY) / scale;
        onNodeMove(node.key, origin.x + dx, origin.y + dy, true);
      }
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /**
   * # 拖动连线折点
   *
   * 只改正交走线中间水平段的 Y：两端仍锚在卡片边上，上游端不可拖、下游端锁定，
   * 连线本身也不提供删除。拖动过程只更新本地（`commit=false`），松手才落库。
   */
  const handleBendPointerDown = (
    event: ReactPointerEvent<SVGElement>,
    geometry: EdgeGeometry,
  ) => {
    if (event.button !== 0) {
      return;
    }
    event.stopPropagation();
    onSelectEdge(geometry.edge.id);
    const startClientY = event.clientY;
    const originY = geometry.midY;
    const scale = view.scale;
    let moved = false;

    const move = (moveEvent: PointerEvent) => {
      const nextY = originY + (moveEvent.clientY - startClientY) / scale;
      if (Math.abs(moveEvent.clientY - startClientY) > 2) {
        moved = true;
      }
      onEdgeBend(geometry.edge.id, clamp(nextY, BEND_MIN_Y, BEND_MAX_Y), false);
    };
    const up = (upEvent: PointerEvent) => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      if (moved) {
        const nextY = originY + (upEvent.clientY - startClientY) / scale;
        onEdgeBend(geometry.edge.id, clamp(nextY, BEND_MIN_Y, BEND_MAX_Y), true);
      }
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /** 缩略图范围 */
  const bounds = useMemo(() => {
    if (!nodes.length) {
      return { minX: 0, minY: 0, width: 1200, height: 700 };
    }
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const node of nodes) {
      const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
      const size = NODE_SIZE[node.kind];
      minX = Math.min(minX, pos.x);
      minY = Math.min(minY, pos.y);
      maxX = Math.max(maxX, pos.x + size.w);
      maxY = Math.max(maxY, pos.y + size.h);
    }
    return { minX, minY, width: Math.max(1, maxX - minX), height: Math.max(1, maxY - minY) };
  }, [nodes, positions]);

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

  return (
    <div className="dbz-canvas-shell">
      <aside className="dbz-component-panel" aria-label="组件面板">
        <div className="dbz-component-panel-title">组件</div>
        <p className="dbz-component-panel-tip">
          点击「Kafka 集群」在画布空白处新建根节点；其余组件只能从父节点的「+」创建。
        </p>
        <button
          type="button"
          className="dbz-component-item"
          data-component="broker"
          onClick={onAddRoot}
        >
          <span
            className="dbz-component-icon"
            style={{ backgroundColor: NODE_KIND_COLOR.broker }}
          >
            <DatabaseOutlined />
          </span>
          <span className="dbz-component-text">
            <span className="dbz-component-name">{NODE_KIND_LABEL.broker}</span>
            <span className="dbz-component-desc">点击新建根节点</span>
          </span>
        </button>
        {(
          [
            ['connect', CloudServerOutlined],
            ['source', ImportOutlined],
            ['sink', ExportOutlined],
          ] as const
        ).map(([kind, Icon]) => (
          <div
            key={kind}
            className="dbz-component-item is-disabled"
            data-component={kind}
            aria-disabled
            title={`${NODE_KIND_LABEL[kind]}只能通过父节点的「+」创建`}
          >
            <span
              className="dbz-component-icon"
              style={{ backgroundColor: NODE_KIND_COLOR[kind] }}
            >
              <Icon />
            </span>
            <span className="dbz-component-text">
              <span className="dbz-component-name">{NODE_KIND_LABEL[kind]}</span>
              <span className="dbz-component-desc">从父节点的「+」创建</span>
            </span>
          </div>
        ))}
      </aside>

      <div className="dbz-canvas-main">
        <div
          className="dbz-graph-viewport"
          ref={viewportRef}
          onPointerDown={handleViewportPointerDown}
        >
          <div
            className="dbz-graph-world"
            style={{
              width: WORLD_WIDTH,
              height: WORLD_HEIGHT,
              transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})`,
            }}
          >
            <svg
              className="dbz-graph-edges"
              width={WORLD_WIDTH}
              height={WORLD_HEIGHT}
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
                  {/* 端点由归属关系固定；选中后可拖中间水平段避让卡片，连线不能删除 */}
                  <path
                    className="dbz-graph-edge-hit"
                    data-edge-id={geometry.edge.id}
                    d={geometry.path}
                    onPointerDown={(event) => {
                      event.stopPropagation();
                      onSelectEdge(geometry.edge.id);
                    }}
                  >
                    <title>端点固定，选中后可拖动中间折点避让卡片；不能删除</title>
                  </path>
                  {selectedEdgeId === geometry.edge.id && (
                    <g
                      className="dbz-graph-bend-handle"
                      data-edge-bend={geometry.edge.id}
                      transform={`translate(${geometry.handle.x}, ${geometry.handle.y})`}
                      onPointerDown={(event) => handleBendPointerDown(event, geometry)}
                    >
                      <circle r={10} className="dbz-graph-bend-hit" />
                      <rect
                        x={-14}
                        y={-4}
                        width={28}
                        height={8}
                        rx={4}
                        className="dbz-graph-bend-shape"
                      />
                      <title>拖动这个折点调整走线，避开卡片重叠</title>
                    </g>
                  )}
                </g>
              ))}
            </svg>

            {nodes.map((node) => {
              const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
              const size = NODE_SIZE[node.kind];
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
                  style={{ left: pos.x, top: pos.y, width: size.w, height: size.h }}
                  onPointerDown={(event) => handleNodePointerDown(event, node)}
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

                  <div className="dbz-graph-node-sub dbz-mono" title={node.sub}>
                    {node.sub}
                  </div>

                  <div className="dbz-graph-node-status" data-no-drag>
                    <NodeStatus node={node} />
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

          <div className="dbz-graph-controls">
            <button type="button" onClick={() => setView((prev) => ({ ...prev, scale: clamp(prev.scale * 1.2, MIN_SCALE, MAX_SCALE) }))} title="放大">
              <PlusOutlined />
            </button>
            <button type="button" onClick={() => setView((prev) => ({ ...prev, scale: clamp(prev.scale / 1.2, MIN_SCALE, MAX_SCALE) }))} title="缩小">
              <MinusOutlined />
            </button>
            <button type="button" className="dbz-graph-fit" onClick={fitView} title="适应画布">
              适应
            </button>
            <span className="dbz-graph-scale">{Math.round(view.scale * 100)}%</span>
          </div>

          <div className="dbz-graph-minimap" title="缩略图：点击可移动视口">
            <svg
              viewBox={`${bounds.minX - 20} ${bounds.minY - 20} ${bounds.width + 40} ${bounds.height + 40}`}
              preserveAspectRatio="xMidYMid meet"
              onClick={(event) => {
                const rect = event.currentTarget.getBoundingClientRect();
                const worldX = bounds.minX + ((event.clientX - rect.left) / rect.width) * bounds.width;
                const worldY = bounds.minY + ((event.clientY - rect.top) / rect.height) * bounds.height;
                setView((prev) => ({
                  ...prev,
                  x: viewportSize.w / 2 - worldX * prev.scale,
                  y: viewportSize.h / 2 - worldY * prev.scale,
                }));
              }}
            >
              {nodes.map((node) => {
                const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
                const size = NODE_SIZE[node.kind];
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

          <div className="dbz-graph-legend">
            <span><i className="dbz-legend-line" />归属线（端点固定，选中后可拖折点）</span>
          </div>
        </div>
      </div>

      <aside className="dbz-graph-inspector">{renderInspector()}</aside>
    </div>
  );
}
