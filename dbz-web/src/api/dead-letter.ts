/**
 * # 死信接口
 *
 * 对应 `DbzDeadLetterRouter`，路径前缀 `/dbz/dead-letter`。
 *
 * 死信由 Kafka Connect 写进 DLQ topic，服务端负责增量同步；页面只做查询、重投与
 * 处置。重投是把报文写回源 topic 让 Connect 重新消费，不再直接 POST 下游地址。
 */

import { apiDeleteWithBody, apiGet, apiGetPage, apiPost, type QueryParams } from './client';
import type {
  DbzDeadLetterCountVo,
  DbzDeadLetterDiscardInput,
  DbzDeadLetterEditPayloadInput,
  DbzDeadLetterMarkHandledInput,
  DbzDeadLetterPurgeInput,
  DbzDeadLetterRetryInput,
  DbzDeadLetterUnmarkHandledInput,
  DbzDeadLetterRetryResultVo,
  DbzDeadLetterVo,
  PageRx,
} from './types';

const BASE = '/dbz/dead-letter';

export const deadLetterApi = {
  /** 立即从 Kafka Connect 的 DLQ topic 同步一次，返回新落库条数 */
  sync: (): Promise<number> => apiPost(BASE + '/sync', {}),

  /** 分页查询（`connectorType` / `connectorId` / `status` / `topic` / `startMs` / `endMs`） */
  page: (params?: QueryParams): Promise<PageRx<DbzDeadLetterVo>> =>
    apiGetPage(BASE + '/page', params),

  /** 待处理计数：不传连接器时按连接器分组返回全部 */
  pendingCount: (params?: QueryParams): Promise<DbzDeadLetterCountVo[]> =>
    apiGet(BASE + '/pending-count', params),

  /** 单条或批量重投；`payload` 传修改后的 JSON 文本时仅支持单条 */
  retry: (input: DbzDeadLetterRetryInput): Promise<DbzDeadLetterRetryResultVo[]> =>
    apiPost(BASE + '/retry', input),

  /** 保存修改后的待投报文（不重投），原始报文不覆盖 */
  editPayload: (input: DbzDeadLetterEditPayloadInput): Promise<DbzDeadLetterVo> =>
    apiPost(BASE + '/edit-payload', input),

  /** 单条或批量废弃 */
  discard: (input: DbzDeadLetterDiscardInput): Promise<void> =>
    apiPost(BASE + '/discard', input),

  /** 标记「已人工处理」：问题已在别处修好，记录处理说明 */
  markHandled: (input: DbzDeadLetterMarkHandledInput): Promise<void> =>
    apiPost(BASE + '/mark-handled', input),

  /** 撤销「已人工处理」，恢复为「待处理」 */
  unmarkHandled: (input: DbzDeadLetterUnmarkHandledInput): Promise<void> =>
    apiPost(BASE + '/unmark-handled', input),

  /**
   * 清理死信：按连接器/状态/topic/分类/原因/时间区间物理删除页面索引（条件全空即
   * 清空整张表），被删记录的坐标会写成墓碑，同步重扫不会再插回来；
   * `clearTopic` 为真时同时清空 DLQ topic 的历史报文，不可恢复且不受筛选条件限制。
   */
  purge: (input: DbzDeadLetterPurgeInput): Promise<void> =>
    apiDeleteWithBody(BASE + '/purge', input),
};
