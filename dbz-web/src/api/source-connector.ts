/**
 * # 上游（source）连接器接口
 *
 * 对应 `DbzSourceConnectorRouter`，路径前缀 `/dbz/source-connector`。
 */

import { apiDelete, apiGetPage, apiGetList, apiGet, apiPost, apiPut, type QueryParams } from './client';
import type {
  DbzSourceConnectorAddInput,
  DbzSourceConnectorModifyInput,
  DbzSourceConnectorSnapshotInput,
  DbzSourceConnectorSnapshotVo,
  DbzSourceConnectorVo,
  Id,
  PageRx,
  SignalChannel,
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
  remove: (id: Id): Promise<DbzSourceConnectorVo> => apiDelete(`${BASE}/${id}`),

  /**
   * 预检「再同步」：列出这次会快照哪些表（只读，不写信号）
   *
   * `source` 通道要直连源库读 `information_schema`，所以这个请求比普通查询慢；
   * 连不上源库会在这里就报错，而不是等到真正触发时才发现。`kafka` 通道不连库，
   * 返回的是交给连接器展开的表范围正则。
   *
   * `channel` 留空表示让服务端按默认顺序挑一条可用的（source 优先）。
   */
  snapshotPreview: (id: Id, channel?: SignalChannel): Promise<DbzSourceConnectorSnapshotVo> =>
    apiGet(BASE + '/snapshot/preview', { id, channel }),

  /** 触发一次快照：`source` 通道写源库信号表，`kafka` 通道投信号 topic */
  snapshot: (input: DbzSourceConnectorSnapshotInput): Promise<void> =>
    apiPost(BASE + '/snapshot', input),
};
