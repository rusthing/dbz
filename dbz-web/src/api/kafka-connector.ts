/**
 * # 远端连接器运维接口
 *
 * 对应 `DbzKafkaConnectorRouter` 手写的四个接口，路径前缀 `/dbz/kafka-connector`。
 *
 * 这些接口操作的是**集群上真实运行的连接器**，不是库里的记录：调用即向 Kafka Connect
 * 发 HTTP 请求，因此不需要也不应该落库。
 */

import { apiGet, apiPost } from './client';
import type {
  Id,
  KafkaConnectorPluginVo,
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
};
