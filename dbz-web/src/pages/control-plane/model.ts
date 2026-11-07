/**
 * # 控制面视图的数据整理
 *
 * 把四张表（broker / connect / source / sink）与每个 Connect 的实时连接器状态
 * 拼成一棵 broker → connect → connector 的树，并给出「库内记录 vs 集群实时状态」
 * 的差异结论。
 *
 * 状态口径：
 * - **实时**：来自 Kafka Connect REST（`GET /connectors?expand=info&expand=status`），
 *   不落库，最新一次采集时间由页面记录；
 * - **库内**：`run_status` 是健康检查任务上一次回写的结论，只代表「最近一次检查」。
 *
 * 两者不是同一时刻的数据，所以只按「是否在运行」这一层做比对：库内运行中而实时
 * 不是运行中（或反过来）才标为不一致；同为异常时不追究具体错误类型。
 */

import type {
  ConnectorRunStatus,
  DbzKafkaBrokerVo,
  DbzKafkaConnectHealthVo,
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
  KafkaConnectorStatus,
  KafkaConnectorVo,
} from '../../api/types';
import { connectorRunStatusDic, dicLabel, kafkaConnectorStatusDic } from '../../dic';

/** 库内「运行中」的判别式 */
const DB_RUNNING: ConnectorRunStatus = 1;
/** 实时「运行中」的判别式 */
const LIVE_RUNNING: KafkaConnectorStatus = 0;

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
  /** 库内记录与实时状态不一致的说明 */
  mismatch?: string;
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
  /** 名下连接器与实时状态不一致的条数 */
  mismatchCount: number;
  /** 库内记录与实时可达性不一致的说明 */
  mismatch?: string;
}

/** 一颗 Kafka 集群及其下全部 Connect 的节点 */
export interface BrokerNode {
  broker: DbzKafkaBrokerVo;
  connects: ConnectNode[];
  sourceCount: number;
  sinkCount: number;
  mismatchCount: number;
}

/** 库内状态文案 */
export function connectorRunStatusLabel(value: ConnectorRunStatus): string {
  return dicLabel(connectorRunStatusDic, value);
}

/** 实时状态文案 */
export function liveStatusLabel(value: KafkaConnectorStatus): string {
  return dicLabel(kafkaConnectorStatusDic, value);
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
 * # 库内记录与实时状态是否对不上
 *
 * 返回说明文案，`undefined` 表示一致或无法比对（实时查询失败）。
 */
export function deriveConnectorMismatch(
  record: ManagedConnectorRecord,
  live: KafkaConnectorVo | undefined,
): string | undefined {
  if (!record.enabled) {
    // 禁用时服务端会注销集群上的连接器；还留着并在跑就是真的不一致
    if (live && live.state === LIVE_RUNNING) {
      return '库内已禁用，但集群上的连接器仍在运行';
    }
    return undefined;
  }

  if (!live) {
    return `库内已启用，但集群上没有「${record.connectorName}」`;
  }

  const dbRunning = record.runStatus === DB_RUNNING;
  const liveRunning = live.state === LIVE_RUNNING;
  if (dbRunning !== liveRunning) {
    return `库内状态「${connectorRunStatusLabel(record.runStatus)}」与集群实时状态「${liveStatusLabel(
      live.state,
    )}」不一致`;
  }
  return undefined;
}

/** Connect 的库内结论与实时可达性是否对不上 */
export function deriveConnectMismatch(
  cluster: DbzKafkaConnectVo,
  live: LiveResult | undefined,
): string | undefined {
  // 禁用的 Connect 不参与健康检查，可达与否不构成状态差异
  if (!cluster.enabled) {
    return undefined;
  }
  if (!live) {
    return undefined;
  }
  if (live.error) {
    return cluster.runStatus === 1 ? '库内记录为「正常」，但集群实时不可达' : undefined;
  }
  if (cluster.runStatus === 3) {
    return '库内记录为「连接失败」，实时已可访问';
  }
  if (cluster.runStatus === 0) {
    return '库内尚无检查结论，实时已可访问';
  }
  return undefined;
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
            // 实时查询失败或所属 Connect 已停用时都不判定差异，免得把「查不到」误报成「不一致」
            mismatch:
              live?.error || live?.skipped ? undefined : deriveConnectorMismatch(record, liveConnector),
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

        const mismatchCount =
          sourceNodes.filter((node) => node.mismatch).length +
          sinkNodes.filter((node) => node.mismatch).length;

        return {
          cluster,
          sources: sourceNodes,
          sinks: sinkNodes,
          unmanaged,
          liveIssue: live?.error,
          health: healthByConnect.get(cluster.id),
          mismatchCount,
          mismatch: deriveConnectMismatch(cluster, live),
        };
      });

      return {
        broker,
        connects: connectNodes,
        sourceCount: connectNodes.reduce((sum, node) => sum + node.sources.length, 0),
        sinkCount: connectNodes.reduce((sum, node) => sum + node.sinks.length, 0),
        mismatchCount:
          connectNodes.reduce((sum, node) => sum + node.mismatchCount, 0) +
          connectNodes.filter((node) => node.mismatch).length,
      };
    });
}
