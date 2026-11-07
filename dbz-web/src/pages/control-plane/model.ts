/**
 * # 控制面视图的数据整理
 *
 * 把四张表（broker / connect / source / sink）与每个 Connect 的实时记录
 * 拼成一棵 broker → connect → connector 的树。
 *
 * 状态口径只有一份：连接器的 `run_status` 由服务端统一回写（健康检查任务 +
 * 进入控制面时的刷新），前端按 {@link connectorRunStatusDic} 映射展示，
 * 不再并列「库内状态 + 实时状态」，也不再自行判断不一致。
 *
 * 实时记录只用于卡片悬停提示与属性框的任务明细，不参与状态判定。
 */

import type {
  DbzKafkaBrokerVo,
  DbzKafkaConnectHealthVo,
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
  KafkaConnectorVo,
} from '../../api/types';

/** 连接器方向 */
export type ConnectorKind = 'source' | 'sink';

/** 上游、下游在列表页是两条记录，这里只取两者共有的字段 */
export type ManagedConnectorRecord = DbzSourceConnectorVo | DbzSinkConnectorVo;

/** 一个 Connect 上实时采集的结果 */
export interface LiveResult {
  connectors: KafkaConnectorVo[];
  /** 实时查询失败原因；有值时 `connectors` 一定为空 */
  error?: string;
  /** 所属 Connect 已停用，未发起实时查询 */
  skipped?: boolean;
}

/**
 * # 一次 Connect 自身健康探测的结果
 *
 * 与连接器列表分开收集：健康探测看的是 Connect 进程（版本、worker），
 * 列表看的是其上的连接器；两者都失败时优先展示健康探测的结论。
 */
export interface ConnectHealth {
  /** 探测成功（`reachable` 仍可能为 false，属于业务结论） */
  health?: DbzKafkaConnectHealthVo;
  /** 探测接口本身失败（网络、服务端错误） */
  error?: string;
  /** 所属 Connect 已停用，未发起探测 */
  skipped?: boolean;
}

/** 详情弹层要展示的目标 */
export interface ConnectorDetailTarget {
  kind: ConnectorKind;
  record: ManagedConnectorRecord;
}

/** 一个已纳管连接器的节点 */
export interface ManagedConnectorNode {
  kind: ConnectorKind;
  record: ManagedConnectorRecord;
  /** 集群上对应的实时记录；未部署时为 `undefined` */
  live?: KafkaConnectorVo;
  /** 实时查询失败原因（整台 Connect 不可达） */
  liveIssue?: string;
  /** 所属 Connect 已停用，不参与实时比对 */
  connectDisabled: boolean;
}

/** 一台 Connect 的节点 */
export interface ConnectNode {
  cluster: DbzKafkaConnectVo;
  /** 库内纳管的上游连接器 */
  sources: ManagedConnectorNode[];
  /** 库内纳管的下游连接器 */
  sinks: ManagedConnectorNode[];
  /** 集群上存在、但库里没有记录的连接器 */
  unmanaged: KafkaConnectorVo[];
  /** 实时查询失败原因 */
  liveIssue?: string;
  /** Connect 自身健康探测结果 */
  health?: ConnectHealth;
}

/** 一颗 Kafka 集群及其下全部 Connect 的节点 */
export interface BrokerNode {
  broker: DbzKafkaBrokerVo;
  connects: ConnectNode[];
  sourceCount: number;
  sinkCount: number;
}

/** 任务总数与异常任务数（FAILED / UNASSIGNED，与后端 `is_task_abnormal` 同一套判定） */
export function taskSummary(live: KafkaConnectorVo): { total: number; abnormal: number } {
  const abnormal = live.tasks.filter((task) => task.state === 1 || task.state === 3).length;
  return { total: live.tasks.length, abnormal };
}

/** 实时失败原因：连接器自身 trace 优先，其次第一个失败任务的 trace */
export function liveFailureReason(live: KafkaConnectorVo): string | null {
  const taskTrace = live.tasks
    .filter((task) => task.state === 1 || task.state === 3)
    .map((task) => task.trace?.trim())
    .find((trace): trace is string => Boolean(trace));
  return live.trace?.trim() || taskTrace || null;
}

/**
 * # 组装控制面树
 *
 * 顺序：broker 按名称、Connect 按名称、连接器按名称。数量级只有几十台，不额外做
 * 缓存或虚拟化。
 */
export function buildControlPlane(
  brokers: DbzKafkaBrokerVo[],
  connects: DbzKafkaConnectVo[],
  sources: DbzSourceConnectorVo[],
  sinks: DbzSinkConnectorVo[],
  liveByConnect: Map<Id, LiveResult>,
  healthByConnect: Map<Id, ConnectHealth> = new Map(),
): BrokerNode[] {
  const sourcesByConnect = new Map<Id, DbzSourceConnectorVo[]>();
  for (const source of sources) {
    const bucket = sourcesByConnect.get(source.kafkaConnectId) ?? [];
    bucket.push(source);
    sourcesByConnect.set(source.kafkaConnectId, bucket);
  }

  const sinksByConnect = new Map<Id, DbzSinkConnectorVo[]>();
  for (const sink of sinks) {
    const bucket = sinksByConnect.get(sink.kafkaConnectId) ?? [];
    bucket.push(sink);
    sinksByConnect.set(sink.kafkaConnectId, bucket);
  }

  const connectsByBroker = new Map<Id, DbzKafkaConnectVo[]>();
  for (const cluster of connects) {
    const bucket = connectsByBroker.get(cluster.kafkaBrokerId) ?? [];
    bucket.push(cluster);
    connectsByBroker.set(cluster.kafkaBrokerId, bucket);
  }

  return [...brokers]
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((broker) => {
      const clusterList = [...(connectsByBroker.get(broker.id) ?? [])].sort((left, right) =>
        left.name.localeCompare(right.name),
      );

      const connectNodes = clusterList.map<ConnectNode>((cluster) => {
        const live = liveByConnect.get(cluster.id);
        const liveByName = new Map((live?.connectors ?? []).map((item) => [item.name, item]));
        /** 已纳管的连接器名，用来挑出集群上的「未纳管」部分 */
        const managedNames = new Set<string>();

        const toNode = (
          kind: ConnectorKind,
          record: ManagedConnectorRecord,
        ): ManagedConnectorNode => {
          managedNames.add(record.connectorName);
          const liveConnector = liveByName.get(record.connectorName);
          return {
            kind,
            record,
            live: liveConnector,
            liveIssue: live?.error,
            connectDisabled: !cluster.enabled,
          };
        };

        const sourceNodes = (sourcesByConnect.get(cluster.id) ?? [])
          .slice()
          .sort((left, right) => left.name.localeCompare(right.name))
          .map((record) => toNode('source', record));

        const sinkNodes = (sinksByConnect.get(cluster.id) ?? [])
          .slice()
          .sort((left, right) => left.name.localeCompare(right.name))
          .map((record) => toNode('sink', record));

        const unmanaged = (live?.connectors ?? [])
          .filter((item) => !managedNames.has(item.name))
          .sort((left, right) => left.name.localeCompare(right.name));

        return {
          cluster,
          sources: sourceNodes,
          sinks: sinkNodes,
          unmanaged,
          liveIssue: live?.error,
          health: healthByConnect.get(cluster.id),
        };
      });

      return {
        broker,
        connects: connectNodes,
        sourceCount: connectNodes.reduce((sum, node) => sum + node.sources.length, 0),
        sinkCount: connectNodes.reduce((sum, node) => sum + node.sinks.length, 0),
      };
    });
}
