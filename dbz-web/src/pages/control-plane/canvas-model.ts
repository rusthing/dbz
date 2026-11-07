/**
 * # 控制面画布的数据整理
 *
 * 把 `buildControlPlane` 组装出的 broker → connect → connector 树压平成图节点，
 * 只按归属关系生成连线：
 *
 * - broker → connect（`kafka_broker_id`）；
 * - connect → source（`kafka_connect_id`）；
 * - source → sink（`source_connector_id`）。
 *
 * sink 只保留通向它上游 source 的一条归属线，不再单独连到 Connect；这样
 * broker → connect → source → sink 始终是一条完整的链路。数据流本身不在画布上
 * 另画，归属线已经表达了上下游关系。
 *
 * 连线只作展示：上游一律由下游表单（sink 表单选 source）或「+」新增时的父节点
 * 决定，画布上不提供拖拽改接、也不提供删除。
 *
 * 位置不在这里算：有持久化位置就用持久化的，没有才按默认网格摆放。
 */

import type {
  ControlPlaneNodeType,
  DbzKafkaConnectVo,
  DbzKafkaBrokerVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
} from '../../api/types';
import { authLabelOf, topicInfoOf } from './details';
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
 * 节点尺寸：四类卡片同一骨架（类型标识 → 名称 → 关键字段 → 状态 → 操作行），
 * 固定宽高让同类卡片高度一致，操作行贴底对齐。
 */
export const NODE_SIZE: Record<CanvasNodeKind, { w: number; h: number }> = {
  broker: { w: 240, h: 154 },
  connect: { w: 240, h: 154 },
  source: { w: 240, h: 154 },
  sink: { w: 240, h: 154 },
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

/** 默认网格：集群/Connect 居上，连接器在下排 */
export function defaultPosition(
  kind: CanvasNodeKind,
  index: number,
): { x: number; y: number } {
  switch (kind) {
    case 'broker':
      return { x: 420 + index * 300, y: 24 };
    case 'connect':
      return { x: 420 + index * 300, y: 210 };
    case 'source':
      return { x: 40 + index * 300, y: 396 };
    case 'sink':
      return { x: 40 + (index % 2) * 320, y: 580 + Math.floor(index / 2) * 170 };
  }
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

/** 扁平化节点：副标题带出归属对象，归属信息只在卡片与属性框里呈现 */
export function flattenNodes(
  tree: BrokerNode[],
  deadLetterOf: (kind: 'source' | 'sink', id: Id) => number,
  sources: DbzSourceConnectorVo[],
): CanvasNode[] {
  const list: CanvasNode[] = [];
  const sourceById = new Map(sources.map((source) => [source.id, source]));

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
        // 连接器副标题先带归属对象，再带关键字段：sink 的上游是 source，source 的
        // 上游是当前 Connect。归属异常统一由后端的 run_status 表达，不再单独标黄。
        const upstreamName =
          item.kind === 'sink'
            ? sourceById.get((item.record as DbzSinkConnectorVo).sourceConnectorId)?.name
            : cluster.name;
        list.push({
          key: nodeKeyOf(item.kind, item.record.id),
          nodeType: NODE_TYPE_OF[item.kind],
          kind: item.kind,
          id: item.record.id,
          name: item.record.name,
          sub: [upstreamName ?? '上游缺失', topic ? `${topic.label}: ${topic.value}` : '未配置 topic']
            .filter(Boolean)
            .join(' · '),
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
