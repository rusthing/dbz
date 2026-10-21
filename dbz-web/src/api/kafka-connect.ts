/**
 * # Kafka Connect 集群接口
 *
 * 对应 `#[router(crud)]` 的 `DbzKafkaConnectRouter`，路径前缀 `/dbz/kafka-connect`。
 */

import { apiDelete, apiGetPage, apiGetList, apiPost, apiPut, type QueryParams } from './client';
import type {
  DbzKafkaConnectAddInput,
  DbzKafkaConnectModifyInput,
  DbzKafkaConnectVo,
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
  remove: (id: number): Promise<DbzKafkaConnectVo> => apiDelete(`${BASE}/${id}`),
};
