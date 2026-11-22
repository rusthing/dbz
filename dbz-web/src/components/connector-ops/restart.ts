/**
 * # 连接器重启（共享）
 *
 * 控制面板卡片与上游/下游维护页都走这里：重启的是集群上真实运行的连接器，
 * 连同任务一起重启，不写库；状态由后台检查任务稍后回写。
 */

import { kafkaConnectorApi } from '../../api/kafka-connector';
import type { ManagedConnectorRecord } from '../../pages/control-panel/model';

/** 重启连接器（含任务） */
export async function restartManagedConnector(record: ManagedConnectorRecord): Promise<void> {
  await kafkaConnectorApi.restart({
    kafkaConnectId: record.kafkaConnectId,
    connectorName: record.connectorName,
    includeTasks: true,
    onlyFailed: false,
  });
}
