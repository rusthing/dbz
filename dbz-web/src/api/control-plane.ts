/**
 * # 控制面画布接口
 *
 * 只有布局位置读写。节点本身仍由 broker / connect / source / sink 四套 CRUD 管理，
 * 这里不重复造节点接口。
 */

import { apiDelete, apiGet, apiGetList, apiPost, apiPut } from './client';
import type {
  ControlPlaneNodeType,
  DbzConnectorConfigSyncInput,
  DbzConnectorConfigSyncVo,
  DbzControlPlaneEdgeBendUpsertInput,
  DbzControlPlaneEdgeBendVo,
  DbzControlPlaneLayoutUpsertInput,
  DbzControlPlaneLayoutVo,
  DbzControlPlaneNodeDeleteInput,
  DbzControlPlaneNodeDeleteVo,
  Id,
} from './types';

const BASE = '/dbz/control-plane/layout';

/**
 * 重置位点输入：节点类型用字典整数，3=source、4=sink。
 *
 * 后端 `DbzResetConnectorOffsetsDto.connectorType` 是 `#[dic]` 枚举，线格式是 i16
 * 数字；传 'source' / 'sink' 字符串会在 axum 的 Json 提取器里反序列化失败，直接
 * 返回 HTTP 422，根本进不到三步编排。
 */
export interface DbzResetConnectorOffsetsInput {
  connectorType: Extract<ControlPlaneNodeType, 3 | 4>;
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

  /** 节点被删除后清掉它的位置（统一删除接口已顺带清理，保留给手动整理场景） */
  deleteLayout: (nodeType: ControlPlaneNodeType, nodeId: Id): Promise<void> =>
    apiDelete(BASE, { nodeType, nodeId }),

  /** 读取全部连线折点；没有拖过的连线不返回 */
  listEdgeBends: (): Promise<DbzControlPlaneEdgeBendVo[]> =>
    apiGetList('/dbz/control-plane/edges'),

  /** 保存一条连线的折点（拖动结束后调用） */
  saveEdgeBend: (input: DbzControlPlaneEdgeBendUpsertInput): Promise<void> =>
    apiPut('/dbz/control-plane/edges', input),

  /** 恢复一条连线的默认居中走线 */
  resetEdgeBend: (edgeId: string): Promise<void> =>
    apiDelete('/dbz/control-plane/edges', { edgeId }),

  /**
   * 校验当前登录用户口令后删除节点
   *
   * 前端先对明文做 SM3，后端再带 `Authorization` 转发 adm-rs 的
   * `POST /adm/auth/verify-audit-password` 校验；通过后才删除并写审计。
   * 是否回退登录口令完全由后端判定，请求体始终只有一个口令摘要；成功时
   * `extra.pswdKind` 说明本次校验的是审核口令（`audit`）还是登录口令（`login`）。
   * 失败按 `ApiError.code` 分流：`ADM00007` 口令不正确、`ADM00008` 过于频繁、
   * `ADM00009` 账号不可用、`ADM00005` 登录状态已失效、`DBZ00001` 认证服务不可用。
   */
  deleteNode: (input: DbzControlPlaneNodeDeleteInput): Promise<DbzControlPlaneNodeDeleteVo> =>
    apiPost<DbzControlPlaneNodeDeleteVo>('/dbz/control-plane/node-delete', input),

  /**
   * 在「不一致」对比弹窗里双向同步配置
   *
   * `toRemote` 以库内为准下发远端 Connect（`PUT /connectors/{name}/config`，会更新并
   * 重载连接器），风险与删除相当，需要审核口令摘要；`toLocal` 以远端实际配置为准写回
   * 库内记录，不校验口令。成功后 `extra` 带重新计算的 `inconsistent` 与脱敏快照。
   */
  syncConnectorConfig: (
    input: DbzConnectorConfigSyncInput,
  ): Promise<DbzConnectorConfigSyncVo> =>
    apiPost<DbzConnectorConfigSyncVo>('/dbz/control-plane/connector-config/sync', input),
};
