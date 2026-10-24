/**
 * # Kafka 集群（broker 侧）接口
 *
 * 对应 `DbzKafkaBrokerRouter`（`#[router(crud)]`），路径前缀 `/dbz/kafka-broker`，
 * 外加手写的 `POST /dbz/kafka-broker/check`。
 *
 * 这些记录描述的是**消息集群**的地址与认证，和 `kafka-connect`（连接器跑在哪台
 * worker）是两件事；上游连接器通过 Connect 间接挂到某个集群上。
 */

import { apiDelete, apiGetList, apiGetPage, apiPost, apiPut, type QueryParams } from './client';
import type {
  DbzKafkaBrokerAddInput,
  DbzKafkaBrokerCheckVo,
  DbzKafkaBrokerModifyInput,
  DbzKafkaBrokerVo,
  Id,
  PageRx,
} from './types';

const BASE = '/dbz/kafka-broker';

export const kafkaBrokerApi = {
  /** 分页查询（QueryDto：`name` / `bootstrapServers` / `runStatus` / `enabled` + 分页参数） */
  page: (params?: QueryParams): Promise<PageRx<DbzKafkaBrokerVo>> => apiGetPage(BASE + '/page', params),

  /** 全量列表，用于下拉选项与总览统计 */
  list: (params?: QueryParams): Promise<DbzKafkaBrokerVo[]> => apiGetList(BASE + '/list', params),

  /** 新增 */
  add: (input: DbzKafkaBrokerAddInput): Promise<DbzKafkaBrokerVo> => apiPost(BASE, input),

  /** 修改（`saslPassword` 留空表示沿用原密码） */
  modify: (input: DbzKafkaBrokerModifyInput): Promise<DbzKafkaBrokerVo> => apiPut(BASE, input),

  /** 删除（名下有 Connect 记录时服务端会拒绝） */
  remove: (id: Id): Promise<DbzKafkaBrokerVo> => apiDelete(`${BASE}/${id}`),

  /**
   * 检测连通性
   *
   * 服务端会真去拉一次 topic 列表，并**回写状态**。连不上也是 200，
   * 看 `reachable` 判断结果——连接不通是业务结论，不是接口错误。
   */
  check: (id: Id): Promise<DbzKafkaBrokerCheckVo> => apiPost(BASE + '/check', { id }),
};
