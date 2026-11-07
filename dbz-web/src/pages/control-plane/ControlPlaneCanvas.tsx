/**
 * # 控制面自由画布
 *
 * 交互按方案 A：节点拖拽、空白平移、以指针为中心缩放、缩略图、正交折线箭头。
 *
 * 连线的规则是「下游锁定」：
 * - 上游端带圆形把手，可以拖到同类节点上更换上游；
 * - 下游端是方形锁标，不响应拖拽，悬停提示「下游目标不可更改」；
 * - 连线本身没有删除入口，要断开链路只能删除下游节点。
 *
 * 组件只负责画与拖，真正写库的动作（改外键、删节点、保存位置）由页面调用。
 */

import {
  CloudServerOutlined,
  DatabaseOutlined,
  ExportOutlined,
  ImportOutlined,
  MinusOutlined,
  PlusOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { Tag, Tooltip } from 'antd';
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from 'react';
import { liveFailureReason, liveStatusLabel, taskSummary } from './model';
import {
  NODE_SIZE,
  defaultPosition,
  nodeTone,
  type CanvasEdge,
  type CanvasNode,
  type CanvasNodeKind,
} from './canvas-model';

/** 画布缩放范围 */
const MIN_SCALE = 0.35;
const MAX_SCALE = 2;
/** 世界坐标系的逻辑尺寸，节点被拖到边缘也不会裁掉 */
const WORLD_WIDTH = 2400;
const WORLD_HEIGHT = 1600;

const PALETTE_ITEMS: { kind: CanvasNodeKind; label: string; hint: string; icon: ReactNode }[] = [
  { kind: 'broker', label: 'Kafka 集群', hint: '消息集群的地址与认证', icon: <DatabaseOutlined /> },
  { kind: 'connect', label: 'Kafka Connect', hint: '连接器跑在哪台 worker', icon: <CloudServerOutlined /> },
  { kind: 'source', label: '上游连接器', hint: 'Debezium source', icon: <ImportOutlined /> },
  { kind: 'sink', label: '下游连接器', hint: '写入目标 sink', icon: <ExportOutlined /> },
];

/** 连线的上游端可以换成哪类节点 */
const UPSTREAM_KIND: Record<CanvasEdge['relation']['type'], CanvasNodeKind> = {
  'broker-connect': 'broker',
  'connect-connector': 'connect',
  flow: 'source',
};

/** 从某个节点拉出连线时，目标节点必须是哪类 */
const LINK_TARGET_KINDS: Partial<Record<CanvasNodeKind, CanvasNodeKind[]>> = {
  broker: ['connect'],
  connect: ['source', 'sink'],
  source: ['sink'],
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
  handle: Point;
  lock: Point;
  color: string;
  marker: string;
}

interface ControlPlaneCanvasProps {
  nodes: CanvasNode[];
  edges: CanvasEdge[];
  positions: Record<string, Point>;
  selectedNodeKey: string | null;
  selectedEdgeId: string | null;
  /** 拖动过程中持续回调，`commit=false`；松手时 `commit=true` 由页面落库 */
  onNodeMove: (key: string, x: number, y: number, commit: boolean) => void;
  onSelectNode: (key: string | null) => void;
  onSelectEdge: (id: string | null) => void;
  /** 从节点锚点拉出连线 */
  onCreateEdge: (from: CanvasNode, to: CanvasNode) => void;
  /** 拖动连线上游端，改挂到新的上游节点 */
  onReconnectEdge: (edge: CanvasEdge, target: CanvasNode) => void;
  /** 从调色板拖到画布，落位后由页面打开新增表单 */
  onCreateAt: (kind: CanvasNodeKind, x: number, y: number) => void;
  /** 尝试删除选中连线时的提示 */
  onEdgeDeleteRejected: (edge: CanvasEdge) => void;
  /** 右侧检查器内容 */
  renderInspector: () => ReactNode;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function pointInNode(test: Point, node: CanvasNode, point: Point): boolean {
  const size = NODE_SIZE[node.kind];
  return (
    test.x >= point.x &&
    test.x <= point.x + size.w &&
    test.y >= point.y &&
    test.y <= point.y + size.h
  );
}

/** 连线几何：归属走竖向折线，数据流走横向折线 */
function geometryOf(
  edge: CanvasEdge,
  nodeMap: Map<string, CanvasNode>,
  positions: Record<string, Point>,
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

  if (edge.kind === 'flow') {
    const start = { x: fromPos.x + fromSize.w, y: fromPos.y + fromSize.h / 2 };
    const end = { x: toPos.x, y: toPos.y + toSize.h / 2 };
    const midX = (start.x + end.x) / 2;
    return {
      edge,
      start,
      end,
      handle: { x: start.x + 14, y: start.y },
      lock: { x: end.x - 14, y: end.y },
      path: `M${start.x},${start.y} L${midX},${start.y} L${midX},${end.y} L${end.x},${end.y}`,
      color: '#1677ff',
      marker: 'dbz-arrow-flow',
    };
  }

  const start = { x: fromPos.x + fromSize.w / 2, y: fromPos.y + fromSize.h };
  const end = { x: toPos.x + toSize.w / 2, y: toPos.y };
  const midY = (start.y + end.y) / 2;
  return {
    edge,
    start,
    end,
    handle: { x: start.x, y: start.y + 14 },
    lock: { x: end.x, y: end.y - 14 },
    path: `M${start.x},${start.y} L${start.x},${midY} L${end.x},${midY} L${end.x},${end.y}`,
    color: '#8c8c8c',
    marker: 'dbz-arrow-ownership',
  };
}

/** 节点状态标签：实时优先，库内只做对照 */
function NodeStatus({ node }: { node: CanvasNode }) {
  if (node.kind === 'broker') {
    const broker = node.record as { runStatus: number; runMsg?: string | null; enabled: boolean };
    const tone = !broker.enabled ? 'default' : broker.runStatus === 1 ? 'success' : broker.runStatus === 3 ? 'error' : 'warning';
    return (
      <Tag color={tone} title={broker.runMsg ?? undefined}>
        {!broker.enabled ? '已停用' : broker.runStatus === 1 ? '正常' : broker.runStatus === 3 ? '连接失败' : '无状态'}
      </Tag>
    );
  }

  if (node.kind === 'connect') {
    if (!node.cluster?.enabled) {
      return <Tag>已停用</Tag>;
    }
    if (node.health?.error) {
      return <Tag color="error">健康探测失败</Tag>;
    }
    if (node.health?.health && !node.health.health.reachable) {
      return <Tag color="error">不可达</Tag>;
    }
    if (node.liveIssue) {
      return <Tag color="error">连接器查询失败</Tag>;
    }
    if (node.health?.health?.reachable) {
      const health = node.health.health;
      return (
        <Tag color="success" title={`Commit ${health.commit ?? '--'} · ${health.workerCount} worker · ${health.connectorCount} 连接器`}>
          {health.version ? `v${health.version}` : '可达'} · {health.workerCount} worker
        </Tag>
      );
    }
    return <Tag>未探测</Tag>;
  }

  if (!node.record.enabled) {
    return <Tag>已停用</Tag>;
  }
  if (node.connectDisabled) {
    return <Tag color="warning">所属 Connect 已停用</Tag>;
  }
  if (node.liveIssue) {
    return <Tag color="error">实时查询失败</Tag>;
  }
  if (!node.live) {
    return <Tag color="warning">集群未部署</Tag>;
  }
  const { abnormal, total } = taskSummary(node.live);
  return (
    <Tag
      color={node.live.state === 0 ? 'success' : node.live.state === 1 ? 'error' : 'warning'}
      title={`${total} 个任务${abnormal > 0 ? `，异常 ${abnormal} 个` : ''}${
        liveFailureReason(node.live) ? `\n${liveFailureReason(node.live)}` : ''
      }`}
    >
      {liveStatusLabel(node.live.state)}
    </Tag>
  );
}

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

export default function ControlPlaneCanvas({
  nodes,
  edges,
  positions,
  selectedNodeKey,
  selectedEdgeId,
  onNodeMove,
  onSelectNode,
  onSelectEdge,
  onCreateEdge,
  onReconnectEdge,
  onCreateAt,
  onEdgeDeleteRejected,
  renderInspector,
}: ControlPlaneCanvasProps) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [view, setView] = useState<ViewState>({ x: 0, y: 0, scale: 1 });
  const [viewportSize, setViewportSize] = useState({ w: 0, h: 0 });
  const fittedRef = useRef(false);

  const nodeMap = useMemo(() => new Map(nodes.map((node) => [node.key, node])), [nodes]);
  const geometries = useMemo(
    () =>
      edges
        .map((edge) => geometryOf(edge, nodeMap, positions))
        .filter((item): item is EdgeGeometry => Boolean(item)),
    [edges, nodeMap, positions],
  );
  const geometryByEdge = useMemo(
    () => new Map(geometries.map((item) => [item.edge.id, item])),
    [geometries],
  );

  /** 连线换上游、节点拉连线两种拖拽共用一套指针坐标 */
  const [reconnectEdge, setReconnectEdge] = useState<CanvasEdge | null>(null);
  const [reconnectPoint, setReconnectPoint] = useState<Point | null>(null);
  const [reconnectHover, setReconnectHover] = useState<string | null>(null);
  const [linkFrom, setLinkFrom] = useState<CanvasNode | null>(null);
  const [linkPoint, setLinkPoint] = useState<Point | null>(null);
  const [linkHover, setLinkHover] = useState<string | null>(null);

  /** 调色板拖拽时的浮层 */
  const [paletteDrag, setPaletteDrag] = useState<{ kind: CanvasNodeKind; x: number; y: number } | null>(null);

  const screenToWorld = useCallback(
    (clientX: number, clientY: number): Point => {
      const rect = viewportRef.current?.getBoundingClientRect();
      if (!rect) {
        return { x: 0, y: 0 };
      }
      return {
        x: (clientX - rect.left - view.x) / view.scale,
        y: (clientY - rect.top - view.y) / view.scale,
      };
    },
    [view],
  );

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
    const scale = clamp(Math.min((viewportSize.w - 80) / width, (viewportSize.h - 80) / height), MIN_SCALE, 1.1);
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
      target.closest('.dbz-graph-edge-handle') ||
      target.closest('.dbz-graph-edge-lock') ||
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

  /** 从节点锚点拉出连线 */
  const handleAnchorPointerDown = (event: ReactPointerEvent<HTMLSpanElement>, node: CanvasNode) => {
    if (event.button !== 0) {
      return;
    }
    event.stopPropagation();
    event.preventDefault();
    const targets = LINK_TARGET_KINDS[node.kind];
    if (!targets) {
      return;
    }
    setLinkFrom(node);
    setLinkPoint(null);
    const move = (moveEvent: PointerEvent) => {
      const point = screenToWorld(moveEvent.clientX, moveEvent.clientY);
      setLinkPoint(point);
      const hit = findNodeAt(point, targets);
      setLinkHover(hit?.key ?? null);
    };
    const up = (upEvent: PointerEvent) => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      const point = screenToWorld(upEvent.clientX, upEvent.clientY);
      const hit = findNodeAt(point, targets);
      setLinkFrom(null);
      setLinkPoint(null);
      setLinkHover(null);
      if (hit) {
        onCreateEdge(node, hit);
      }
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /** 拖动宿线上游端，换到另一台同类节点 */
  const handleEdgeHandlePointerDown = (
    event: ReactPointerEvent<SVGGElement>,
    edge: CanvasEdge,
  ) => {
    if (event.button !== 0) {
      return;
    }
    event.stopPropagation();
    event.preventDefault();
    const targetKind = UPSTREAM_KIND[edge.relation.type];
    setReconnectEdge(edge);
    setReconnectPoint(null);
    const move = (moveEvent: PointerEvent) => {
      const point = screenToWorld(moveEvent.clientX, moveEvent.clientY);
      setReconnectPoint(point);
      const hit = findNodeAt(point, [targetKind])?.key ?? null;
      setReconnectHover(hit);
    };
    const up = (upEvent: PointerEvent) => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      const point = screenToWorld(upEvent.clientX, upEvent.clientY);
      const hit = findNodeAt(point, [targetKind]);
      setReconnectEdge(null);
      setReconnectPoint(null);
      setReconnectHover(null);
      if (hit) {
        onReconnectEdge(edge, hit);
      }
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /** 按几何位置命中节点，避免与 DOM 事件顺序纠缠 */
  const findNodeAt = (point: Point, kinds: CanvasNodeKind[]): CanvasNode | null => {
    for (let index = nodes.length - 1; index >= 0; index -= 1) {
      const node = nodes[index];
      if (!kinds.includes(node.kind)) {
        continue;
      }
      const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
      if (pointInNode(point, node, pos)) {
        return node;
      }
    }
    return null;
  };

  /** 调色板拖到画布：松手位置换算成世界坐标后交给页面开表单 */
  const handlePalettePointerDown = (event: ReactPointerEvent<HTMLDivElement>, kind: CanvasNodeKind) => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    setPaletteDrag({ kind, x: event.clientX, y: event.clientY });
    const move = (moveEvent: PointerEvent) => {
      setPaletteDrag({ kind, x: moveEvent.clientX, y: moveEvent.clientY });
    };
    const up = (upEvent: PointerEvent) => {
      window.removeEventListener('pointermove', move);
      window.removeEventListener('pointerup', up);
      setPaletteDrag(null);
      const rect = viewportRef.current?.getBoundingClientRect();
      if (!rect) {
        return;
      }
      if (
        upEvent.clientX < rect.left ||
        upEvent.clientX > rect.right ||
        upEvent.clientY < rect.top ||
        upEvent.clientY > rect.bottom
      ) {
        return;
      }
      const point = screenToWorld(upEvent.clientX, upEvent.clientY);
      onCreateAt(kind, point.x, point.y);
    };
    window.addEventListener('pointermove', move);
    window.addEventListener('pointerup', up);
  };

  /** 删除快捷键对连线无效：页面对选中连线给出提示 */
  useEffect(() => {
    if (!selectedEdgeId) {
      return;
    }
    const handleKey = (event: KeyboardEvent) => {
      if (event.key !== 'Delete' && event.key !== 'Backspace') {
        return;
      }
      const edge = edges.find((item) => item.id === selectedEdgeId);
      if (edge) {
        event.preventDefault();
        onEdgeDeleteRejected(edge);
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [edges, onEdgeDeleteRejected, selectedEdgeId]);

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

  const linkStart = linkFrom
    ? (() => {
        const pos = positions[linkFrom.key] ?? defaultPosition(linkFrom.kind, 0);
        const size = NODE_SIZE[linkFrom.kind];
        if (linkFrom.kind === 'source') {
          return { x: pos.x + size.w, y: pos.y + size.h / 2 };
        }
        return { x: pos.x + size.w / 2, y: pos.y + size.h };
      })()
    : null;

  const reconnectGeometry = reconnectEdge ? geometryByEdge.get(reconnectEdge.id) : null;

  /** 高亮可换连/可接收的节点 */
  const nodeDropState = (node: CanvasNode): string => {
    if (reconnectEdge) {
      if (node.key === reconnectHover) {
        return ' is-link-hover';
      }
      if (node.kind === UPSTREAM_KIND[reconnectEdge.relation.type]) {
        return ' is-link-target';
      }
    }
    if (linkFrom) {
      if (node.key === linkHover) {
        return ' is-link-hover';
      }
      if (LINK_TARGET_KINDS[linkFrom.kind]?.includes(node.kind)) {
        return ' is-link-target';
      }
    }
    return '';
  };

  return (
    <div className="dbz-canvas-shell">
      <aside className="dbz-palette">
        <div className="dbz-palette-title">组件</div>
        {PALETTE_ITEMS.map((item) => (
          <div
            key={item.kind}
            className="dbz-palette-item"
            data-kind={item.kind}
            role="button"
            tabIndex={0}
            title={item.hint}
            onPointerDown={(event) => handlePalettePointerDown(event, item.kind)}
          >
            <span className="dbz-palette-icon">{item.icon}</span>
            <span>
              <strong>{item.label}</strong>
              <small>{item.hint}</small>
            </span>
          </div>
        ))}
        <div className="dbz-palette-tip">
          拖到画布落位后填配置。节点位置会保存到 dbz 库，其他人打开看到同一套布局。
        </div>
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
                <marker id="dbz-arrow-ownership" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
                  <path d="M0,0 L10,5 L0,10 z" fill="#8c8c8c" />
                </marker>
                <marker id="dbz-arrow-flow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
                  <path d="M0,0 L10,5 L0,10 z" fill="#1677ff" />
                </marker>
              </defs>
              {geometries.map(({ edge, path, color, marker }) => (
                <g key={edge.id}>
                  <path
                    className={`dbz-graph-edge-line dbz-graph-edge--${edge.kind}${selectedEdgeId === edge.id ? ' is-selected' : ''}`}
                    d={path}
                    stroke={color}
                    markerEnd={`url(#${marker})`}
                    strokeDasharray={edge.kind === 'ownership' ? '6 4' : undefined}
                  />
                  <path
                    className="dbz-graph-edge-hit"
                    data-edge-id={edge.id}
                    d={path}
                    onPointerDown={(event) => {
                      event.stopPropagation();
                      onSelectEdge(edge.id);
                    }}
                  >
                    <title>{edge.kind === 'ownership' ? '归属关系：上游可更换，下游锁定，不可删除' : '数据流：上游可更换，下游锁定，不可删除'}</title>
                  </path>
                </g>
              ))}
            </svg>

            {nodes.map((node) => {
              const pos = positions[node.key] ?? defaultPosition(node.kind, 0);
              const size = NODE_SIZE[node.kind];
              const selected = selectedNodeKey === node.key;
              return (
                <div
                  key={node.key}
                  className={`dbz-graph-node dbz-graph-node--${node.kind} dbz-graph-node--${nodeTone(node)}${selected ? ' is-selected' : ''}${nodeDropState(node)}`}
                  data-node-key={node.key}
                  data-node-type={node.kind}
                  style={{ left: pos.x, top: pos.y, width: size.w, height: size.h }}
                  onPointerDown={(event) => handleNodePointerDown(event, node)}
                >
                  <div className="dbz-graph-node-head">
                    <span className="dbz-graph-node-icon">
                      <NodeIcon kind={node.kind} />
                    </span>
                    <span className="dbz-graph-node-name" title={node.name}>
                      {node.name}
                    </span>
                    {node.mismatch && (
                      <Tooltip title={node.mismatch}>
                        <WarningOutlined className="dbz-graph-mismatch" data-no-drag aria-label={node.mismatch} />
                      </Tooltip>
                    )}
                  </div>
                  <div className="dbz-graph-node-status">
                    <NodeStatus node={node} />
                    {node.dead > 0 && (
                      <Tag color="error" title="待处理死信，点开检查器可进入死信页">
                        死信 {node.dead}
                      </Tag>
                    )}
                  </div>
                  <div className="dbz-graph-node-sub dbz-mono" title={node.sub}>
                    {node.sub}
                  </div>

                  {(node.kind === 'broker' || node.kind === 'connect') && (
                    <span
                      className="dbz-graph-pin dbz-graph-pin-out"
                      data-no-drag
                      title="从这里拖出连线"
                      onPointerDown={(event) => handleAnchorPointerDown(event, node)}
                    />
                  )}
                  {node.kind === 'connect' && <span className="dbz-graph-pin dbz-graph-pin-in dbz-graph-pin-top" data-no-drag title="归属入口（下游锁定）" />}
                  {node.kind === 'source' && (
                    <>
                      <span className="dbz-graph-pin dbz-graph-pin-out dbz-graph-pin-right" data-no-drag title="从这里拖出数据流" onPointerDown={(event) => handleAnchorPointerDown(event, node)} />
                      <span className="dbz-graph-pin dbz-graph-pin-in dbz-graph-pin-top" data-no-drag title="归属入口（下游锁定）" />
                    </>
                  )}
                  {node.kind === 'sink' && (
                    <>
                      <span className="dbz-graph-pin dbz-graph-pin-in dbz-graph-pin-top" data-no-drag title="归属入口（下游锁定）" />
                      <span className="dbz-graph-pin dbz-graph-pin-in dbz-graph-pin-left" data-no-drag title="数据流入口（下游锁定）" />
                    </>
                  )}
                </div>
              );
            })}

            <svg
              className="dbz-graph-overlay"
              width={WORLD_WIDTH}
              height={WORLD_HEIGHT}
              aria-hidden={false}
            >
              {geometries.map((geometry) => {
                const { edge, handle, lock, color } = geometry;
                return (
                  <g key={edge.id}>
                    <g
                      className="dbz-graph-edge-handle"
                      data-edge-id={edge.id}
                      transform={`translate(${handle.x},${handle.y})`}
                      onPointerDown={(event) => handleEdgeHandlePointerDown(event, edge)}
                    >
                      <circle r={9} fill="#fff" stroke={color} strokeWidth={2} />
                      <path d="M-4,0 L4,0 M0,-4 L0,4" stroke={color} strokeWidth={1.6} />
                      <title>拖动此端更换上游（下游目标不可更改）</title>
                    </g>
                    <g
                      className="dbz-graph-edge-lock"
                      data-edge-id={edge.id}
                      transform={`translate(${lock.x},${lock.y})`}
                      data-no-drag
                      onPointerDown={(event) => {
                        event.stopPropagation();
                        event.preventDefault();
                      }}
                    >
                      <rect x={-8} y={-8} width={16} height={16} rx={3} fill="#595959" />
                      {/* 锁标用 SVG 画，避免把 HTML 的 antd 图标塞进 SVG 里 */}
                      <rect x={-4} y={-1} width={8} height={6} rx={1} fill="#fff" />
                      <path d="M-2.5,-1 A2.5,2.5 0 0 1 2.5,-1" fill="none" stroke="#fff" strokeWidth={1.2} />
                      <title>下游目标不可更改，连线不能删除</title>
                    </g>
                  </g>
                );
              })}

              {reconnectEdge && reconnectPoint && reconnectGeometry && (
                <path
                  className="dbz-graph-reconnect-preview"
                  d={`M${reconnectPoint.x},${reconnectPoint.y} L${reconnectGeometry.lock.x},${reconnectGeometry.lock.y}`}
                />
              )}

              {linkFrom && linkPoint && linkStart && (
                <path className="dbz-graph-reconnect-preview" d={`M${linkStart.x},${linkStart.y} L${linkPoint.x},${linkPoint.y}`} />
              )}
            </svg>
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
            <span><i className="dbz-legend-line dbz-legend-ownership" />归属</span>
            <span><i className="dbz-legend-line dbz-legend-flow" />数据流</span>
            <span><i className="dbz-legend-lock" />下游锁定</span>
          </div>
        </div>
      </div>

      <aside className="dbz-graph-inspector">{renderInspector()}</aside>

      {paletteDrag && (
        <div
          className="dbz-palette-ghost"
          style={{ left: paletteDrag.x + 12, top: paletteDrag.y + 12 }}
        >
          {PALETTE_ITEMS.find((item) => item.kind === paletteDrag.kind)?.label}
        </div>
      )}
    </div>
  );
}
