/**
 * # 控制面画布的数据整理
 *
 * 把 `buildControlPlane` 组装出的 broker → connect → connector 树压平成图节点，
 * 只按归属关系生成连线：
 *
 * - broker → connect（`kafka_broker_id`）；
 * - connect → source/sink（`kafka_connect_id`）。
 *
 * source → sink 的数据流不在画布上画：两组连接器同属一台 Connect，归属线已经
 * 表达了它们跑在哪，再画数据流只会让图变乱。
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

/** 节点卡片上的操作 */
export type CanvasNodeAction = 'toggle' | 'restart' | 'edit' | 'delete' | 'check';

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
  /** 库内与实时不一致的说明 */
  mismatch?: string;
  /** 连接器的实时记录 */
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

/** 连线的真实业务关系；换上游时按这个分支写回不同的外键 */
export type CanvasEdgeRelation =
  | { type: 'broker-connect'; connectId: Id }
  | { type: 'connect-connector'; connectorKind: 'source' | 'sink'; connectorId: Id };

export interface CanvasEdge {
  id: string;
  from: string;
  to: string;
  relation: CanvasEdgeRelation;
}

/** 节点尺寸：宽度放完整连接器名，高度给底部操作按钮留一行 */
export const NODE_SIZE: Record<CanvasNodeKind, { w: number; h: number }> = {
  broker: { w: 240, h: 136 },
  connect: { w: 240, h: 140 },
  source: { w: 240, h: 132 },
  sink: { w: 240, h: 132 },
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

/** 默认网格：集群/Connect 居上，连接器在下排 */
export function defaultPosition(
  kind: CanvasNodeKind,
  index: number,
): { x: number; y: number } {
  switch (kind) {
    case 'broker':
      return { x: 420 + index * 300, y: 24 };
    case 'connect':
      return { x: 420 + index * 300, y: 168 };
    case 'source':
      return { x: 40 + index * 300, y: 360 };
    case 'sink':
      return { x: 40 + (index % 2) * 320, y: 520 + Math.floor(index / 2) * 150 };
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

/** 扁平化节点 */
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
        sub: cluster.url,
        record: cluster,
        mismatch: connect.mismatch,
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
          sub: topic ? `${topic.label}: ${topic.value}` : '未配置 topic',
          topicLabel: topic?.label,
          record: item.record,
          mismatch: item.mismatch,
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
      relation: { type: 'connect-connector', connectorKind: 'source', connectorId: source.id },
    });
  }

  for (const sink of sinks) {
    edges.push({
      id: `own:connect:${sink.kafkaConnectId}:sink:${sink.id}`,
      from: nodeKeyOf('connect', sink.kafkaConnectId),
      to: nodeKeyOf('sink', sink.id),
      relation: { type: 'connect-connector', connectorKind: 'sink', connectorId: sink.id },
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
