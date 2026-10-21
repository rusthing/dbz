/**
 * # 下游（sink）连接器接口
 *
 * 对应 `DbzSinkConnectorRouter`，路径前缀 `/dbz/sink-connector`。
 */

import { apiDelete, apiGetPage, apiGetList, apiPost, apiPut, type QueryParams } from './client';
import type {
  DbzSinkConnectorAddInput,
  DbzSinkConnectorModifyInput,
  DbzSinkConnectorVo,
  Id,
  PageRx,
} from './types';

const BASE = '/dbz/sink-connector';

export const sinkConnectorApi = {
  /** 分页查询（QueryDto：`sourceConnectorId` / `kafkaConnectId` / `name` / `connectorName` / `runStatus` / `enabled`） */
  page: (params?: QueryParams): Promise<PageRx<DbzSinkConnectorVo>> => apiGetPage(BASE + '/page', params),

  /** 全量列表 */
  list: (params?: QueryParams): Promise<DbzSinkConnectorVo[]> => apiGetList(BASE + '/list', params),

  /** 新增 */
  add: (input: DbzSinkConnectorAddInput): Promise<DbzSinkConnectorVo> => apiPost(BASE, input),

  /** 修改 */
  modify: (input: DbzSinkConnectorModifyInput): Promise<DbzSinkConnectorVo> => apiPut(BASE, input),

  /** 删除 */
  remove: (id: Id): Promise<DbzSinkConnectorVo> => apiDelete(`${BASE}/${id}`),
};
