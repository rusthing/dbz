/**
 * # Kafka Connect 集群接口
 *
 * 对应 `#[router(crud)]` 的 `DbzKafkaConnectRouter`，路径前缀 `/dbz/kafka-connect`。
 */

import { apiDelete, apiGet, apiGetPage, apiGetList, apiPost, apiPut, type QueryParams } from './client';
import type {
  DbzKafkaConnectAddInput,
  DbzKafkaConnectHealthVo,
  DbzKafkaConnectModifyInput,
  DbzKafkaConnectVo,
  Id,
  PageRx,
} from './types';

const BASE = '/dbz/kafka-connect';

export const kafkaConnectApi = {
  /** 分页查询（QueryDto：`name` / `url` / `runStatus` / `enabled` + `_keyword` / `_page` / `_size`） */
  page: (params?: QueryParams): Promise<PageRx<DbzKafkaConnectVo>> => apiGetPage(BASE + '/page', params),

  /** 全量列表，用于下拉选项与总览统计 */
  list: (params?: QueryParams): Promise<DbzKafkaConnectVo[]> => apiGetList(BASE + '/list', params),

  /** 新增 */
  add: (input: DbzKafkaConnectAddInput): Promise<DbzKafkaConnectVo> => apiPost(BASE, input),

  /** 修改 */
  modify: (input: DbzKafkaConnectModifyInput): Promise<DbzKafkaConnectVo> => apiPut(BASE, input),

  /** 删除（名下仍有连接器时服务端会拒绝） */
  remove: (id: Id): Promise<DbzKafkaConnectVo> => apiDelete(`${BASE}/${id}`),

  /**
   * 探测 Connect 自身健康状态（只读，不回写库内 run_status）
   *
   * 连不上也是 200，用 `reachable` 与 `message` 表达结论；版本、worker 数来自集群。
   */
  health: (kafkaConnectId: Id): Promise<DbzKafkaConnectHealthVo> =>
    apiGet(BASE + '/health', { kafkaConnectId }),
};
