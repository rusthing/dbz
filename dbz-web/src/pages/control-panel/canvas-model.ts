/**
 * # 控制面板泳道图的数据整理
 *
 * 把 `buildControlPanel` 组装出的 broker → connect → connector 树压平成图节点，
 * 只按归属关系生成连线：
 *
 * - broker → connect（`kafka_broker_id`）；
 * - connect → source（`kafka_connect_id`）；
 * - source → sink（`source_connector_id`）。
 *
 * sink 只保留通向它上游 source 的一条归属线，不再单独连到 Connect；这样
 * broker → connect → source → sink 始终是一条完整的链路。数据流本身不在图上
 * 另画，归属线已经表达了上下游关系。
 *
 * 布局是泳道图：四类节点各占一列泳道，从左到右按归属链排列（Kafka 集群 →
 * Connect → 上游 → 下游），列头像表头一样固定在视口顶部；列内节点按父级分组
 * 自动排列，位置完全由数据推导，不提供拖拽、也不持久化。
 * 连线只作展示：上游一律由下游表单（sink 表单选 source）或「+」新增时的父节点
 * 决定，图上不提供改接、也不提供删除。
 */

import type {
  ControlPlaneNodeType,
  DbzKafkaConnectVo,
  DbzKafkaBrokerVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
} from '../../api/types';
import { authLabelOf, connectorConfigValueOf, topicInfoOf } from './details';
import type {
  BrokerNode,
  ConnectHealth,
  ManagedConnectorNode,
  ManagedConnectorRecord,
} from './model';

export type CanvasNodeKind = 'broker' | 'connect' | 'source' | 'sink';

/** 节点卡片操作行里除启用开关以外的图标操作 */
export type CanvasNodeAction =
  | 'restart'
  | 'reset-offsets'
  /** 上游连接器：向信号表写 execute-snapshot，重读选中表且不重置位点 */
  | 'resync'
  /** Connect：导出该 Connect 下全部连接器为 JSON */
  | 'export-connectors'
  /** Connect：从 JSON 导入连接器 */
  | 'import-connectors'
  /** Kafka 集群：导出整个集群（含 Connect 与连接器） */
  | 'backup-cluster'
  /** Kafka 集群：从备份 JSON 恢复库内纳管记录 */
  | 'restore-cluster'
  /** 连接器：打开「不一致」对比弹窗，查看两边配置并可双向同步 */
  | 'inspect-inconsistent'
  | 'edit'
  | 'delete'
  | 'check'
  | 'dead-letter';

/** 四类卡片统一的类型标识：图标旁的文字与主色，颜色只用于图标底色和左侧色条 */
export const NODE_KIND_LABEL: Record<CanvasNodeKind, string> = {
  broker: 'Kafka 集群',
  connect: 'Kafka Connect',
  source: '上游连接器',
  sink: '下游连接器',
};

export const NODE_KIND_COLOR: Record<CanvasNodeKind, string> = {
  broker: '#1677ff',
  connect: '#722ed1',
  source: '#13a8a8',
  sink: '#eb2f96',
};

export type NodeTone = 'success' | 'error' | 'warning' | 'default';

export interface CanvasNode {
  /** React key，也是布局表里的 `(nodeType, id)` */
  key: string;
  nodeType: ControlPlaneNodeType;
  kind: CanvasNodeKind;
  id: Id;
  name: string;
  /** 副标题：broker 地址 / Connect URL / 连接器 topic */
  sub: string;
  /** topic 字段名，用于悬停说明 */
  topicLabel?: string;
  record: DbzKafkaBrokerVo | DbzKafkaConnectVo | ManagedConnectorRecord;
  /** 连接器的实时记录（只用于属性框任务明细，不再作为状态来源） */
  live?: ManagedConnectorNode['live'];
  /** 实时查询失败原因（所属 Connect 不可达） */
  liveIssue?: string;
  /** 所属 Connect 已停用 */
  connectDisabled?: boolean;
  /** Connect 自身健康探测 */
  health?: ConnectHealth;
  /** 待处理死信数（仅连接器） */
  dead: number;
  /** 所属 Connect，用于属性检查器 */
  cluster?: DbzKafkaConnectVo;
  broker?: DbzKafkaBrokerVo;
}

/** 泳道内节点坐标（世界坐标系，左上角） */
export interface LanePoint {
  x: number;
  y: number;
}

/** 连线的真实业务关系，仅用于属性框说明；画布上不提供编辑入口 */
export type CanvasEdgeRelation =
  | { type: 'broker-connect'; connectId: Id }
  | { type: 'connect-source'; sourceId: Id }
  | { type: 'source-sink'; sinkId: Id };

export interface CanvasEdge {
  id: string;
  from: string;
  to: string;
  relation: CanvasEdgeRelation;
}

/**
 * 节点尺寸：四类卡片同一骨架（类型标识 → 名称 → 关键字段 → 状态 → 操作行）。
 * 高度按内容收紧，不留「状态行被 margin-top:auto 顶到底部」造成的中间空档：
 * 卡片越矮，整图在「适应画布」下的缩放比越大，卡片里的字才读得清。
 * 下游多一行 http.api.url，单独高 18px。
 */
export const NODE_SIZE: Record<CanvasNodeKind, { w: number; h: number }> = {
  broker: { w: 360, h: 158 },
  connect: { w: 360, h: 158 },
  source: { w: 360, h: 158 },
  sink: { w: 360, h: 176 },
};

export const NODE_TYPE_OF: Record<CanvasNodeKind, ControlPlaneNodeType> = {
  broker: 1,
  connect: 2,
  source: 3,
  sink: 4,
};

export function nodeKeyOf(kind: CanvasNodeKind, id: Id): string {
  return `${NODE_TYPE_OF[kind]}:${id}`;
}

/**
 * # 画布搜索的匹配文本
 *
 * 只按用户能认出来的三样东西匹配：节点名称、topic（含 topics / topics.regex）、
 * Connect 上的 connectorName。统一转小写，调用方负责 trim。
 */
export function nodeSearchText(node: CanvasNode): string {
  const name = node.name.toLowerCase();
  if (node.kind === 'source' || node.kind === 'sink') {
    const record = node.record as ManagedConnectorRecord;
    const topic = topicInfoOf(node.kind, record)?.value ?? '';
    return `${name} ${record.connectorName.toLowerCase()} ${topic.toLowerCase()}`;
  }
  return name;
}

/**
 * # 泳道布局
 *
 * 四类节点各占一列泳道，从左到右按归属链排列；列内节点按父级在上一列泳道里的
 * 次序分组排开，同一父级的子节点彼此相邻，连线尽量短、少交叉。
 * 位置完全由数据推导：新增/删除记录后重算即可，不需要持久化。
 */
export const LANE_ORDER: CanvasNodeKind[] = ['broker', 'connect', 'source', 'sink'];

/** 列头高度：固定在视口顶部，不随平移缩放移动 */
export const LANE_HEADER_H = 40;
/** 泳道列内左右留白：卡片撑满列宽减去这份留白 */
export const LANE_PAD_X = 16;
/** 泳道列内上下留白 */
const LANE_PAD_Y = 24;
/** 列内相邻卡片间距 */
const NODE_GAP_Y = 16;
/** 相邻泳道列之间的间距（列宽拖手就落在缝里） */
export const LANE_GAP = 40;
/** 世界四周留白 */
const WORLD_MARGIN = 24;
/** 空泳道列的最小内容高度：保留一条可见的带，提示该层级还没有记录 */
const EMPTY_LANE_H = 120;

/** 列宽：默认值、可拖范围；四列各自记忆，双击列宽拖手回到默认 */
export const LANE_DEFAULT_W = 400;
export const LANE_COL_MIN = 320;
export const LANE_COL_MAX = 560;
export const DEFAULT_LANE_COLUMNS: number[] = LANE_ORDER.map(() => LANE_DEFAULT_W);

export function clampLaneColumn(width: number): number {
  if (!Number.isFinite(width)) {
    return LANE_DEFAULT_W;
  }
  return Math.round(Math.min(LANE_COL_MAX, Math.max(LANE_COL_MIN, width)));
}

/** 把 localStorage / 旧版本残留的任意值收敛成四列的合法宽度 */
export function normalizeLaneColumns(input: unknown): number[] {
  const list = Array.isArray(input) ? input : [];
  return LANE_ORDER.map((_, index) => clampLaneColumn(Number(list[index])));
}

export interface Swimlane {
  kind: CanvasNodeKind;
  /** 泳道列左上角 X（世界坐标） */
  x: number;
  /** 泳道列宽度 */
  w: number;
  /** 泳道内节点数 */
  count: number;
}

export interface LaneSize {
  w: number;
  h: number;
}

export interface SwimlaneLayout {
  positions: Record<string, LanePoint>;
  /** 每个节点的实际尺寸：宽度跟着列宽走，高度按类型收紧 */
  sizes: Record<string, LaneSize>;
  lanes: Swimlane[];
  /** 世界尺寸：随节点数量增长，画布不再固定 */
  width: number;
  height: number;
}

export function buildSwimlaneLayout(
  nodes: CanvasNode[],
  edges: CanvasEdge[],
  columns: number[] = DEFAULT_LANE_COLUMNS,
): SwimlaneLayout {
  const colWidths = normalizeLaneColumns(columns);
  // 子节点 → 父节点 key；归属链上每个节点只有一个父级
  const parentOf = new Map<string, string>();
  for (const edge of edges) {
    parentOf.set(edge.to, edge.from);
  }

  const byKind = new Map<CanvasNodeKind, CanvasNode[]>(
    LANE_ORDER.map((kind) => [kind, nodes.filter((node) => node.kind === kind)]),
  );

  // 逐列定序：broker 按树序；其余按父级在上一列里的下标稳定排序，
  // 没有归属线的孤儿节点排在末尾，保持原有相对顺序
  const orderIndex = new Map<string, number>();
  for (const kind of LANE_ORDER) {
    const list = byKind.get(kind) ?? [];
    if (kind !== 'broker') {
      list.sort((a, b) => {
        const pa = orderIndex.get(parentOf.get(a.key) ?? '') ?? Number.MAX_SAFE_INTEGER;
        const pb = orderIndex.get(parentOf.get(b.key) ?? '') ?? Number.MAX_SAFE_INTEGER;
        return pa - pb;
      });
    }
    list.forEach((node, index) => orderIndex.set(node.key, index));
  }

  // 列宽可拖：卡片撑满列宽减去左右留白，列宽变化直接反映到卡片宽度上
  const lanes: Swimlane[] = [];
  const positions: Record<string, LanePoint> = {};
  const sizes: Record<string, LaneSize> = {};
  let maxColH = 0;
  let cursorX = WORLD_MARGIN;

  LANE_ORDER.forEach((kind, colIndex) => {
    const list = byKind.get(kind) ?? [];
    const colW = colWidths[colIndex];
    const nodeW = colW - LANE_PAD_X * 2;
    const x = cursorX;
    lanes.push({ kind, x, w: colW, count: list.length });
    cursorX += colW + LANE_GAP;

    let y = WORLD_MARGIN + LANE_PAD_Y;
    for (const node of list) {
      sizes[node.key] = { w: nodeW, h: NODE_SIZE[node.kind].h };
      positions[node.key] = { x, y };
      y += NODE_SIZE[node.kind].h + NODE_GAP_Y;
    }
    const colH = list.length
      ? y - NODE_GAP_Y + LANE_PAD_Y
      : WORLD_MARGIN + EMPTY_LANE_H;
    maxColH = Math.max(maxColH, colH);
  });

  const width = cursorX - LANE_GAP + WORLD_MARGIN;
  const height = Math.max(maxColH, 480);
  return { positions, sizes, lanes, width, height };
}

/** 节点颜色：优先实时状态，其次库内状态 */
export function nodeTone(node: CanvasNode): NodeTone {
  if (node.kind === 'broker') {
    const broker = node.record as DbzKafkaBrokerVo;
    if (!broker.enabled) {
      return 'default';
    }
    if (broker.runStatus === 1) {
      return 'success';
    }
    if (broker.runStatus === 3) {
      return 'error';
    }
    return 'warning';
  }

  if (node.kind === 'connect') {
    if (!node.cluster?.enabled) {
      return 'default';
    }
    if (node.health?.error) {
      return 'error';
    }
    if (node.health?.health && !node.health.health.reachable) {
      return 'error';
    }
    if (node.liveIssue) {
      return 'error';
    }
    if (node.health?.health?.reachable) {
      return 'success';
    }
    return 'warning';
  }

  if (!node.record.enabled || node.connectDisabled) {
    return 'default';
  }
  if (node.liveIssue || node.live?.state === 1) {
    return 'error';
  }
  if (!node.live) {
    return 'warning';
  }
  switch (node.live.state) {
    case 0:
      return 'success';
    case 2:
    case 3:
    case 5:
      return 'warning';
    default:
      return 'default';
  }
}

/** 上游连接器说明行：用户名@主机:端口号/topic 前缀 */
function sourceAddressOf(record: DbzSourceConnectorVo): string {
  const valueOf = (key: string) => connectorConfigValueOf(record, key) || '--';
  // 老版本 Debezium 没有 topic.prefix 时，用 database.server.name 作为前缀
  const prefix =
    connectorConfigValueOf(record, 'topic.prefix') ||
    connectorConfigValueOf(record, 'database.server.name') ||
    '--';
  return `${valueOf('database.user')}@${valueOf('database.hostname')}:${valueOf('database.port')}/${prefix}`;
}

/** 下游连接器说明行：topic 一行、http.api.url 一行，卡片上分成两行展示 */
function sinkAddressOf(record: DbzSinkConnectorVo): string {
  const topic = topicInfoOf('sink', record)?.value || '--';
  const url = connectorConfigValueOf(record, 'http.api.url') || '--';
  return `${topic}\n${url}`;
}

/** 扁平化节点：副标题直接展示地址值，归属信息只在属性框里呈现 */
export function flattenNodes(
  tree: BrokerNode[],
  deadLetterOf: (kind: 'source' | 'sink', id: Id) => number,
): CanvasNode[] {
  const list: CanvasNode[] = [];

  for (const brokerNode of tree) {
    const broker = brokerNode.broker;
    list.push({
      key: nodeKeyOf('broker', broker.id),
      nodeType: NODE_TYPE_OF.broker,
      kind: 'broker',
      id: broker.id,
      name: broker.name,
      sub: `${broker.bootstrapServers} · ${authLabelOf(broker)}`,
      record: broker,
      broker,
      dead: 0,
    });

    for (const connect of brokerNode.connects) {
      const cluster = connect.cluster;
      list.push({
        key: nodeKeyOf('connect', cluster.id),
        nodeType: NODE_TYPE_OF.connect,
        kind: 'connect',
        id: cluster.id,
        name: cluster.name,
        sub: `${broker.name} · ${cluster.url}`,
        record: cluster,
        liveIssue: connect.liveIssue,
        health: connect.health,
        cluster,
        broker,
        dead: 0,
      });

      for (const item of [...connect.sources, ...connect.sinks]) {
        const topic = topicInfoOf(item.kind, item.record);
        list.push({
          key: nodeKeyOf(item.kind, item.record.id),
          nodeType: NODE_TYPE_OF[item.kind],
          kind: item.kind,
          id: item.record.id,
          name: item.record.name,
          sub:
            item.kind === 'source'
              ? sourceAddressOf(item.record as DbzSourceConnectorVo)
              : sinkAddressOf(item.record as DbzSinkConnectorVo),
          topicLabel: topic?.label,
          record: item.record,
          live: item.live,
          liveIssue: item.liveIssue,
          connectDisabled: item.connectDisabled,
          cluster,
          broker,
          dead: deadLetterOf(item.kind, item.record.id),
        });
      }
    }
  }

  return list;
}

/** 由外键生成归属连线：broker → connect → connector */
export function buildEdges(
  brokers: DbzKafkaBrokerVo[],
  connects: DbzKafkaConnectVo[],
  sources: DbzSourceConnectorVo[],
  sinks: DbzSinkConnectorVo[],
): CanvasEdge[] {
  const edges: CanvasEdge[] = [];

  for (const cluster of connects) {
    edges.push({
      id: `own:broker:${cluster.kafkaBrokerId}:connect:${cluster.id}`,
      from: nodeKeyOf('broker', cluster.kafkaBrokerId),
      to: nodeKeyOf('connect', cluster.id),
      relation: { type: 'broker-connect', connectId: cluster.id },
    });
  }

  for (const source of sources) {
    edges.push({
      id: `own:connect:${source.kafkaConnectId}:source:${source.id}`,
      from: nodeKeyOf('connect', source.kafkaConnectId),
      to: nodeKeyOf('source', source.id),
      relation: { type: 'connect-source', sourceId: source.id },
    });
  }

  for (const sink of sinks) {
    // 下游只保留一条归属线：连到它的上游连接器（source），不另连 Connect
    edges.push({
      id: `own:source:${sink.sourceConnectorId}:sink:${sink.id}`,
      from: nodeKeyOf('source', sink.sourceConnectorId),
      to: nodeKeyOf('sink', sink.id),
      relation: { type: 'source-sink', sinkId: sink.id },
    });
  }

  // 外键对不上的连线（记录被外部删掉）直接丢弃，避免画出悬空的线
  const nodeKeys = new Set(
    [
      ...brokers.map((item) => nodeKeyOf('broker', item.id)),
      ...connects.map((item) => nodeKeyOf('connect', item.id)),
      ...sources.map((item) => nodeKeyOf('source', item.id)),
      ...sinks.map((item) => nodeKeyOf('sink', item.id)),
    ],
  );
  return edges.filter((edge) => nodeKeys.has(edge.from) && nodeKeys.has(edge.to));
}

/**
 * # 删除节点会波及的连线
 *
 * 删除 broker / connect 时服务端通常还会因为子记录拒绝，提示里要把可能断掉的
 * 归属关系先列出来，让运维知道为什么删不掉、删掉会失去什么。
 */
export function affectedEdgesOf(node: CanvasNode, edges: CanvasEdge[]): CanvasEdge[] {
  return edges.filter((edge) => edge.from === node.key || edge.to === node.key);
}
