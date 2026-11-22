/**
 * # 四类对象的检测口径（共享）
 *
 * 控制面板卡片与四类维护页共用这一份构造器：同样的接口、同样的判定、同样的文案。
 * 维护页只有一个记录（没有画布节点），所以入参是记录本身，`nodeKey` / `kindLabel`
 * 由调用方传进来，仅用于结果弹窗的标题与属性面板归档。
 *
 * 检测只读或按后端既定口径回写（Connect 检测会补录/标记纳管记录，连接器检测会
 * 回写 `inconsistent`），因此调用方拿到结果后刷新列表即可看到最新标记。
 */

import { controlPlaneApi } from '../../api/control-plane';
import { kafkaBrokerApi } from '../../api/kafka-broker';
import { kafkaConnectApi } from '../../api/kafka-connect';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import type { DbzKafkaBrokerVo, DbzKafkaConnectVo } from '../../api/types';
import { kafkaConnectorStatusDic } from '../../dic';
import { errorMessage } from '../../utils/format';
import type {
  DetectItemStatus,
  DetectOutcome,
} from '../../pages/control-panel/DetectResultModal';
import type { ConnectorKind, ManagedConnectorRecord } from '../../pages/control-panel/model';

/** 四类对象的检测（与画布节点类型同名，便于调用方直接映射） */
export type DetectKind = ConnectorKind | 'broker' | 'connect';

/** 节点类型中文名：维护页与画布共用同一套文案 */
export const DETECT_KIND_LABEL: Record<DetectKind, string> = {
  broker: 'Kafka 集群',
  connect: 'Kafka Connect',
  source: '上游连接器',
  sink: '下游连接器',
};

/**
 * 集群检测：Kafka 连通性 + topic 清单
 *
 * 只看 Kafka 侧（可达性、topic）。连接器插件是 Connect worker 的能力，归属在
 * Connect 检测里，这里不遍历名下的 Connect。
 */
export async function detectBroker(
  record: DbzKafkaBrokerVo,
  nodeKey: string,
  kindLabel: string,
): Promise<DetectOutcome> {
  const result = await kafkaBrokerApi.check(record.id);
  const reachable = result.reachable;
  return {
    nodeKey,
    kindLabel,
    name: record.name,
    elapsedMs: result.elapsedMs,
    items: [
      {
        key: 'reachable',
        label: 'Kafka 连通性',
        status: reachable ? 'ok' : 'error',
        detail: reachable
          ? `已连上 ${record.bootstrapServers}`
          : result.message || '无法连接，请检查地址、认证与网络',
      },
      {
        key: 'topics',
        label: 'Topic 清单',
        status: reachable ? 'ok' : 'error',
        detail: reachable ? `${result.topicCount} 个 topic` : '未获取到 topic 列表',
      },
    ],
    reason: reachable ? undefined : result.message || '无法连接到 Kafka 集群',
    topics: reachable ? (result.topics ?? []) : [],
  };
}

/**
 * Connect 检测：可达性 / 版本 / worker / 连接器 / 插件，并同步纳管记录
 *
 * 插件是 Connect worker 的能力，只在这里拉取与展示；同一台 Kafka 集群下多台
 * Connect 各自插件不同，不做跨 Connect 汇总。
 */
export async function detectConnect(
  cluster: DbzKafkaConnectVo,
  nodeKey: string,
  kindLabel: string,
): Promise<DetectOutcome> {
  const [healthResult, pluginResult] = await Promise.allSettled([
    kafkaConnectApi.health(cluster.id),
    kafkaConnectorApi.plugins(cluster.id),
  ]);
  if (healthResult.status === 'rejected') {
    throw healthResult.reason;
  }
  const health = healthResult.value;
  const plugins = pluginResult.status === 'fulfilled' ? pluginResult.value : [];
  const pluginIssue =
    pluginResult.status === 'rejected'
      ? `插件查询失败：${errorMessage(pluginResult.reason)}`
      : null;

  if (!health.reachable) {
    return {
      nodeKey,
      kindLabel,
      name: cluster.name,
      elapsedMs: health.elapsedMs,
      items: [
        {
          key: 'reachable',
          label: 'Connect 可达性',
          status: 'error',
          detail: health.message || 'Connect 不可达，请检查地址、网络与服务状态',
        },
        {
          key: 'sync',
          label: '纳管记录同步',
          status: 'warning',
          detail: 'Connect 不可达，未执行同步',
        },
      ],
      reason: health.message || 'Connect 不可达',
    };
  }

  // 可达才同步：以远端为准补录缺失、标记配置不一致、把已消失的置为禁用。
  const detect = await kafkaConnectApi.detect(cluster.id);
  return {
    nodeKey,
    kindLabel,
    name: cluster.name,
    elapsedMs: health.elapsedMs,
    items: [
      {
        key: 'reachable',
        label: 'Connect 可达性',
        status: 'ok',
        detail: `已连上 ${cluster.url}`,
      },
      {
        key: 'version',
        label: 'Connect 版本',
        status: health.version ? 'ok' : 'warning',
        detail: health.version ? `v${health.version}` : '版本未知（根路径未返回）',
      },
      {
        key: 'worker',
        label: 'Worker',
        status: health.workerCount > 0 ? 'ok' : 'warning',
        detail: `${health.workerCount} 个 worker`,
      },
      {
        key: 'connectors',
        label: '连接器',
        status: 'ok',
        detail: `${health.connectorCount} 个连接器`,
      },
      {
        key: 'plugins',
        label: '连接器插件',
        status: pluginIssue ? 'warning' : 'ok',
        detail: pluginIssue ?? `已安装 ${plugins.length} 个插件`,
      },
      {
        key: 'sync',
        label: '纳管记录同步',
        status: detect.skipped > 0 ? 'warning' : 'ok',
        detail: `补录 ${detect.added}，标记不一致 ${detect.marked}，置禁用 ${detect.disabled}，恢复一致 ${detect.cleared}，无变化 ${detect.unchanged}，跳过 ${detect.skipped}`,
      },
    ],
    plugins,
    sync: detect,
  };
}

/**
 * 连接器一致性检测：库内配置 vs Connect 实际配置 + 启停状态
 *
 * 判定交给后端 `POST /dbz/control-plane/connector-config/detect`：与整台 Connect
 * 的「检测」同源（同一套差异口径、同一份脱敏快照），但只作用于这一条记录，
 * 并回写 `inconsistent` / `inconsistent_detail`。
 */
export async function detectConnector(
  kind: ConnectorKind,
  record: ManagedConnectorRecord,
  nodeKey: string,
  kindLabel: string,
): Promise<DetectOutcome> {
  const startedAt = performance.now();
  const result = await controlPlaneApi.detectConnectorConfig({
    nodeType: kind,
    nodeId: record.id,
  });

  // 启停核对是补充诊断：实时列表取不到也不该把一致性结论带偏
  let runDetail = '未取到 Connect 实时状态，跳过启停核对';
  let runStatus: DetectItemStatus = 'warning';
  if (!result.remoteExists) {
    runDetail = '远端已不存在该连接器，未比对启停';
  } else {
    try {
      const liveList = await kafkaConnectorApi.list(record.kafkaConnectId);
      const live = liveList.find((item) => item.name === record.connectorName);
      if (!live) {
        runDetail = '实时列表里没有该连接器，未比对启停';
      } else if (record.enabled && live.state !== 0) {
        const stateLabel =
          kafkaConnectorStatusDic.find((dic) => dic.value === live.state)?.label ??
          `状态 ${live.state}`;
        runDetail = `库内为启用，Connect 实际为「${stateLabel}」`;
      } else if (!record.enabled && live.state === 0) {
        runDetail = '库内为停用，但 Connect 上连接器仍在运行';
      } else {
        runDetail = '库内启停与 Connect 实际一致';
        runStatus = 'ok';
      }
    } catch (reason) {
      runDetail = `实时状态查询失败：${errorMessage(reason)}`;
    }
  }

  const changedKeys = result.changedKeys ?? [];
  const configDetail = !result.configValid
    ? result.reason ?? '库内 connectorConfig 不是合法的 JSON 对象，无法逐项比对'
    : !result.remoteExists
      ? result.reason ?? '远端已不存在，无法逐项比对'
      : result.changed > 0
        ? `${result.changed} 处不一致：${changedKeys.join('；')}`
        : result.inconsistent
          ? result.reason ?? '配置无法逐项比对，已标记不一致'
          : '库内配置与 Connect 实际一致（连接信息、topic 范围等全部关键项）';

  return {
    nodeKey,
    kindLabel,
    name: record.name,
    elapsedMs: performance.now() - startedAt,
    items: [
      {
        key: 'stored',
        label: '库内配置格式',
        status: result.configValid ? 'ok' : 'error',
        detail: result.configValid
          ? '库内 connectorConfig 可正常解析'
          : '库内 connectorConfig 不是合法的 JSON 对象；请先在编辑表单里修正配置',
      },
      {
        key: 'exists',
        label: '远端连接器',
        status: result.remoteExists ? 'ok' : 'error',
        detail: result.remoteExists
          ? `Connect 上存在连接器 ${result.connectorName}`
          : 'Connect 上不存在该连接器，库内记录已置为禁用并标记不一致',
      },
      {
        key: 'config',
        label: '关键配置比对',
        status: result.inconsistent ? 'error' : 'ok',
        detail: configDetail,
      },
      {
        key: 'run',
        label: '启停状态',
        status: runStatus,
        detail: runDetail,
      },
      {
        key: 'flag',
        label: '检测标记',
        status: 'ok',
        detail: result.inconsistent
          ? result.updated
            ? '已刷新「不一致」标记与对比快照，可逐项对比并双向同步'
            : '「不一致」标记与快照已是本次结论'
          : result.updated
            ? '库内与远端一致，已清除不一致标记与快照'
            : '标记原本就是一致，无需改动',
      },
    ],
    reason: !result.configValid
      ? '库内配置不是合法 JSON 对象'
      : !result.remoteExists
        ? 'Connect 上已不存在该连接器'
        : result.changed > 0
          ? `库内记录与 Connect 实际有 ${result.changed} 处配置不一致`
          : undefined,
  };
}

/** 检测请求本身失败时的兜底结果：接口报错与被检对象异常分开表达 */
export function detectFailure(
  name: string,
  nodeKey: string,
  kindLabel: string,
  reason: unknown,
): DetectOutcome {
  return {
    nodeKey,
    kindLabel,
    name,
    elapsedMs: 0,
    items: [
      {
        key: 'request',
        label: '检测请求',
        status: 'error',
        detail: errorMessage(reason),
      },
    ],
    reason: errorMessage(reason),
  };
}

/** 记录级 nodeKey：维护页没有画布节点，用「类型:id」保证同页内唯一 */
export function detectKeyOf(kind: DetectKind, id: string): string {
  return `${kind}:${id}`;
}
