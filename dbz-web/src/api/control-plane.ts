/**
 * # 控制面画布接口
 *
 * 只有布局位置读写。节点本身仍由 broker / connect / source / sink 四套 CRUD 管理，
 * 这里不重复造节点接口。
 */

import { apiDelete, apiGetList, apiPut } from './client';
import type {
  ControlPlaneNodeType,
  DbzControlPlaneLayoutUpsertInput,
  DbzControlPlaneLayoutVo,
  Id,
} from './types';

const BASE = '/dbz/control-plane/layout';

export const controlPlaneApi = {
  /** 读取全部节点位置；返回空数组表示还没人整理过画布 */
  listLayout: (): Promise<DbzControlPlaneLayoutVo[]> => apiGetList(BASE),

  /** 保存一个节点的位置（拖动结束后调用） */
  saveLayout: (input: DbzControlPlaneLayoutUpsertInput): Promise<void> => apiPut(BASE, input),

  /** 节点被删除后清掉它的位置 */
  deleteLayout: (nodeType: ControlPlaneNodeType, nodeId: Id): Promise<void> =>
    apiDelete(BASE, { nodeType, nodeId }),
};
