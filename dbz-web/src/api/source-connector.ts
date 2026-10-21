/**
 * # 上游（source）连接器接口
 *
 * 对应 `DbzSourceConnectorRouter`，路径前缀 `/dbz/source-connector`。
 */

import { apiDelete, apiGetPage, apiGetList, apiPost, apiPut, type QueryParams } from './client';
import type {
  DbzSourceConnectorAddInput,
  DbzSourceConnectorModifyInput,
  DbzSourceConnectorVo,
  PageRx,
} from './types';

const BASE = '/dbz/source-connector';

export const sourceConnectorApi = {
  /** 分页查询（QueryDto：`kafkaConnectId` / `name` / `connectorName` / `runStatus` / `enabled` + 分页参数） */
  page: (params?: QueryParams): Promise<PageRx<DbzSourceConnectorVo>> => apiGetPage(BASE + '/page', params),

  /** 全量列表 */
  list: (params?: QueryParams): Promise<DbzSourceConnectorVo[]> => apiGetList(BASE + '/list', params),

  /** 新增 */
  add: (input: DbzSourceConnectorAddInput): Promise<DbzSourceConnectorVo> => apiPost(BASE, input),

  /** 修改 */
  modify: (input: DbzSourceConnectorModifyInput): Promise<DbzSourceConnectorVo> => apiPut(BASE, input),

  /** 删除 */
  remove: (id: number): Promise<DbzSourceConnectorVo> => apiDelete(`${BASE}/${id}`),
};
