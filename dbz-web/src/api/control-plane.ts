/**
 * # 控制面画布接口
 *
 * 只有布局位置读写。节点本身仍由 broker / connect / source / sink 四套 CRUD 管理，
 * 这里不重复造节点接口。
 */

import { apiDelete, apiGet, apiGetList, apiPost, apiPut } from './client';
import type {
  ControlPlaneNodeType,
  DbzControlPlaneLayoutUpsertInput,
  DbzControlPlaneLayoutVo,
  Id,
} from './types';

const BASE = '/dbz/control-plane/layout';

/** 重置位点输入：节点类型只接受 source / sink */
export interface DbzResetConnectorOffsetsInput {
  connectorType: 'source' | 'sink';
  connectorId: Id;
  /** 清空后立即恢复运行 */
  resume: boolean;
  /** 下游必需：确认会重放历史消息、可能重复写入 */
  confirmReplay: boolean;
}

/** 单步结果 */
export interface DbzResetOffsetsStepVo {
  step: string;
  ok: boolean;
  message?: string | null;
}

/** 重置位点整体结果 */
export interface DbzResetConnectorOffsetsVo {
  ok: boolean;
  failedStep?: string | null;
  message: string;
  connectorState: string;
  steps: DbzResetOffsetsStepVo[];
}

export const controlPlaneApi = {
  /** 进入页面/轮询时刷新统一状态；失败由调用方降级为最后已知展示 */
  refreshStatus: (): Promise<number> => apiGet<number>('/dbz/control-plane/status/refresh'),

  /** 重置位点：后端串行执行 stop → delete offsets → 可选 resume */
  resetOffsets: (input: DbzResetConnectorOffsetsInput): Promise<DbzResetConnectorOffsetsVo> =>
    apiPost<DbzResetConnectorOffsetsVo>('/dbz/control-plane/connector/reset-offsets', input),

  /** 读取全部节点位置；返回空数组表示还没人整理过画布 */
  listLayout: (): Promise<DbzControlPlaneLayoutVo[]> => apiGetList(BASE),

  /** 保存一个节点的位置（拖动结束后调用） */
  saveLayout: (input: DbzControlPlaneLayoutUpsertInput): Promise<void> => apiPut(BASE, input),

  /** 节点被删除后清掉它的位置 */
  deleteLayout: (nodeType: ControlPlaneNodeType, nodeId: Id): Promise<void> =>
    apiDelete(BASE, { nodeType, nodeId }),
};
