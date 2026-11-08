/**
 * # Kafka 集群备份 / 恢复接口
 *
 * 对应手写路由 `/dbz/backup`：
 * - `GET  /dbz/backup/export`  导出整个集群（含 Connect 与连接器）
 * - `POST /dbz/backup/restore` 恢复库内纳管记录；`dryRun` 时只返回计划
 *
 * 恢复不触碰远端：不会注册或删除任何连接器，只写库。
 */

import { apiGet, apiPost } from './client';
import type {
  BackupRestoreStrategy,
  DbzBackupFileVo,
  DbzBackupRestoreVo,
  Id,
} from './types';

const BASE = '/dbz/backup';

export const backupApi = {
  /** 导出整个 Kafka 集群的备份文件内容（由前端下载保存） */
  exportCluster: (kafkaBrokerId: Id): Promise<DbzBackupFileVo> =>
    apiGet(BASE + '/export', { kafkaBrokerId }),

  /**
   * 恢复备份
   *
   * `dryRun=true` 只计算计划不写库，用于弹窗预览；执行时用同一套判定，预览与结果
   * 不会出现两套口径。
   */
  restore: (
    file: unknown,
    strategy: BackupRestoreStrategy,
    dryRun: boolean,
  ): Promise<DbzBackupRestoreVo> =>
    apiPost(BASE + '/restore', { file, strategy, dryRun }),
};
