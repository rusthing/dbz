/**
 * # 远端连接器运维接口
 *
 * 对应 `DbzKafkaConnectorRouter` 手写的五个接口，路径前缀 `/dbz/kafka-connector`。
 *
 * 这些接口操作的是**集群上真实运行的连接器**，不是库里的记录：调用即向 Kafka Connect
 * 发 HTTP 请求，因此不需要也不应该落库。
 */

import { apiDelete, apiGet, apiPost, apiPut } from './client';
import type {
  Id,
  KafkaConnectorControlInput,
  KafkaConnectorDeleteInput,
  KafkaConnectorPluginVo,
  KafkaConnectorResetOffsetInput,
  KafkaConnectorRestartInput,
  KafkaConnectorTaskRestartInput,
  KafkaConnectorVo,
} from './types';

const BASE = '/dbz/kafka-connector';

export const kafkaConnectorApi = {
  /** 查看目标集群已安装的连接器插件（`kafkaConnectId` 是 u64 字符串，原样透传） */
  plugins: (kafkaConnectId: Id): Promise<KafkaConnectorPluginVo[]> =>
    apiGet(BASE + '/plugins', { kafkaConnectId }),

  /** 查看目标集群上的全部连接器（含任务状态） */
  list: (kafkaConnectId: Id): Promise<KafkaConnectorVo[]> =>
    apiGet(BASE + '/list', { kafkaConnectId }),

  /** 重启连接器，默认连同任务一起重启 */
  restart: (input: KafkaConnectorRestartInput): Promise<void> => apiPost(BASE + '/restart', input),

  /** 重启单个任务 */
  restartTask: (input: KafkaConnectorTaskRestartInput): Promise<void> =>
    apiPost(BASE + '/task/restart', input),

  /** 从集群上删除连接器（不删库里的记录；连接器本就不存在也算成功） */
  remove: (input: KafkaConnectorDeleteInput): Promise<void> =>
    apiDelete(BASE, {
      kafkaConnectId: input.kafkaConnectId,
      connectorName: input.connectorName,
    }),

  /**
   * 停止连接器（`PUT /connectors/{name}/stop`）
   *
   * 保留配置与位点，恢复后从原处继续。需要 Kafka 3.5+；停下来的连接器在健康检查里
   * 会被记为「已停止」并告警。
   */
  stop: (input: KafkaConnectorControlInput): Promise<void> =>
    apiPut(BASE + '/stop', undefined, {
      kafkaConnectId: input.kafkaConnectId,
      connectorName: input.connectorName,
    }),

  /** 恢复运行（`PUT /connectors/{name}/resume`），停止与暂停两种状态都能恢复 */
  resume: (input: KafkaConnectorControlInput): Promise<void> =>
    apiPut(BASE + '/resume', undefined, {
      kafkaConnectId: input.kafkaConnectId,
      connectorName: input.connectorName,
    }),

  /**
   * 重置位点（服务端先停再删位点）
   *
   * 位点清空后连接器就是「从没跑过」，`snapshot.mode=initial` 会做一次整表快照。
   * `resume` 决定清完要不要立刻拉起来。
   */
  resetOffsets: (input: KafkaConnectorResetOffsetInput): Promise<void> =>
    apiDelete(BASE + '/offsets', {
      kafkaConnectId: input.kafkaConnectId,
      connectorName: input.connectorName,
      resume: input.resume ?? false,
    }),
};
