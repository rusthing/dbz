/**
 * # 重置位点弹窗（共享）
 *
 * 控制面板卡片与上游/下游维护页共用：三步由后端串行完成（stop → delete offsets
 * → 可选 resume），前端只提交意图；失败时展示停在哪一步、连接器当时处于什么状态，
 * 成功后由调用方刷新列表让状态与集群一致。
 */

import { Alert, App as AntdApp, Button, Checkbox, Modal, Tag } from 'antd';
import { useEffect, useState } from 'react';
import {
  controlPlaneApi,
  type DbzResetConnectorOffsetsVo,
} from '../../api/control-plane';
import type { DbzSinkConnectorVo, DbzSourceConnectorVo, Id } from '../../api/types';
import { errorMessage } from '../../utils/format';
import { connectorConfigValueOf } from '../../pages/control-panel/details';
import type { ManagedConnectorRecord } from '../../pages/control-panel/model';

/** 重置位点的目标：上游/下游各自的 id 与库内配置 */
export interface ResetOffsetsTarget {
  kind: 'source' | 'sink';
  id: Id;
  name: string;
  record: ManagedConnectorRecord;
}

interface ResetOffsetsModalProps {
  target: ResetOffsetsTarget | null;
  onClose: () => void;
  /** 执行成功后调用，通常是刷新列表 */
  onDone?: () => void;
}

/**
 * 下游清空位点后的起点由连接器自己的 `auto.offset.reset` 决定：
 * earliest 才会从头重放，latest 直接跳到末尾。提示必须按实际配置给出。
 */
function sinkOffsetHint(record: DbzSinkConnectorVo): string {
  const value = connectorConfigValueOf(record, 'auto.offset.reset').toLowerCase();
  if (value === 'earliest') {
    return '该连接器的 auto.offset.reset=earliest：清空位点后会从 topic 最早位置重新消费，历史消息会全部重投下游，可能造成重复写入与死信。';
  }
  if (value === 'latest') {
    return '该连接器的 auto.offset.reset=latest：清空位点后会直接跳到 topic 当前末尾，历史消息不会补投，等于放弃这段区间的数据。';
  }
  return `该连接器未显式配置 auto.offset.reset（值为「${value || '未配置'}」），按 Kafka 默认 latest 处理：清空位点后会跳到 topic 当前末尾，历史消息不会补投，等于放弃这段区间的数据。`;
}

/** 上游清空位点会触发的快照行为 */
function sourceSnapshotHint(record: DbzSourceConnectorVo): string {
  const mode = connectorConfigValueOf(record, 'snapshot.mode') || '未配置';
  return `当前 snapshot.mode=${mode}：清空位点后会重新做一次全量快照，整库整表重读，下游会被重灌一遍。耗时与源库压力取决于数据量，请在低峰期执行。`;
}

/** 重置位点的步骤文案 */
export function resetStepLabel(step?: string | null): string {
  switch (step) {
    case 'stop':
      return '停止连接器';
    case 'clear':
      return '清空位点';
    case 'resume':
      return '恢复运行';
    case 'confirm':
      return '执行前确认';
    default:
      return step || '未知步骤';
  }
}

export default function ResetOffsetsModal({
  target,
  onClose,
  onDone,
}: ResetOffsetsModalProps) {
  const { message } = AntdApp.useApp();
  const [resume, setResume] = useState(true);
  const [confirmed, setConfirmed] = useState(false);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<DbzResetConnectorOffsetsVo | null>(null);

  // 换一条记录就回到默认：清空后立即恢复、下游需重新勾选重放确认
  useEffect(() => {
    setResume(true);
    setConfirmed(false);
    setResult(null);
  }, [target?.kind, target?.id]);

  const close = () => {
    if (running) {
      return;
    }
    onClose();
    setResult(null);
  };

  const execute = async () => {
    if (!target) {
      return;
    }
    setRunning(true);
    try {
      const outcome = await controlPlaneApi.resetOffsets({
        // 后端 ControlPlaneNodeType 是整数枚举：3=source、4=sink
        connectorType: target.kind === 'source' ? 3 : 4,
        connectorId: target.id,
        resume,
        confirmReplay: target.kind === 'sink' ? confirmed : true,
      });
      setResult(outcome);
      if (outcome.ok) {
        message.success(
          resume ? '已清空位点并恢复运行，状态稍后回写' : '已清空位点，连接器保持停止',
        );
        onDone?.();
      }
    } catch (reason) {
      setResult({
        ok: false,
        message: errorMessage(reason),
        connectorState: '未知',
        steps: [],
      });
    } finally {
      setRunning(false);
    }
  };

  return (
    <Modal
      open={target !== null}
      title={target ? `重置位点「${target.name}」` : '重置位点'}
      onCancel={close}
      maskClosable={!running}
      destroyOnHidden
      footer={
        result
          ? [
              <Button key="close" onClick={close}>
                关闭
              </Button>,
            ]
          : [
              <Button key="cancel" onClick={close} disabled={running}>
                取消
              </Button>,
              <Button
                key="ok"
                type="primary"
                danger
                loading={running}
                disabled={target?.kind === 'sink' && !confirmed}
                onClick={() => void execute()}
              >
                {resume ? '清空并恢复运行' : '清空位点'}
              </Button>,
            ]
      }
    >
      {target && (
        <div className="dbz-reset-offsets">
          <Alert
            type="warning"
            showIcon
            message={
              target.kind === 'sink'
                ? '清空后按 auto.offset.reset 决定起点，可能造成历史消息重放'
                : '重置后会重新做全量快照，下游会被重灌一遍'
            }
            description={
              target.kind === 'sink'
                ? sinkOffsetHint(target.record as DbzSinkConnectorVo)
                : sourceSnapshotHint(target.record as DbzSourceConnectorVo)
            }
          />
          <p className="dbz-reset-order">
            执行顺序：停止连接器 → 删除位点 → （按勾选）恢复运行。任一步失败会停下并说明停在哪一步。
          </p>
          <Checkbox
            checked={resume}
            disabled={running}
            onChange={(event) => setResume(event.target.checked)}
          >
            清空后立即恢复运行
          </Checkbox>
          {target.kind === 'sink' && (
            <Checkbox
              checked={confirmed}
              disabled={running}
              onChange={(event) => setConfirmed(event.target.checked)}
            >
              我知道会重放历史消息、可能产生重复写入与死信
            </Checkbox>
          )}

          {result && (
            <Alert
              className="dbz-reset-result"
              type={result.ok ? 'success' : 'error'}
              showIcon
              message={
                result.ok
                  ? `执行完成，当前状态：${result.connectorState}`
                  : `第「${resetStepLabel(result.failedStep)}」步失败：${result.message}`
              }
              description={`当前连接器状态：${result.connectorState}`}
            />
          )}
          {result && result.steps.length > 0 && (
            <div className="dbz-reset-steps">
              {result.steps.map((step) => (
                <div key={step.step} className="dbz-reset-step">
                  <Tag color={step.ok ? 'success' : 'error'}>
                    {resetStepLabel(step.step)}
                  </Tag>
                  <span>{step.message || (step.ok ? '完成' : '失败')}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </Modal>
  );
}
