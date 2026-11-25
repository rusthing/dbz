/**
 * # 导出连接器弹窗（共享）
 *
 * 控制面板卡片与 Kafka Connect 维护页共用：导出的是一台 Connect 下已纳管的上游 +
 * 下游连接器，格式与「导入连接器」一致。默认带凭据，脱敏只是把敏感值换成 ******
 * 并在 sensitiveConfigKeys 里标注，导入后需要人工补回。
 */

import { Alert, App as AntdApp, Radio, Space, Spin } from 'antd';
import { Modal } from '../../lib/federated-overlay';
import { useEffect, useState } from 'react';
import { kafkaBrokerApi } from '../../api/kafka-broker';
import { sinkConnectorApi } from '../../api/sink-connector';
import { sourceConnectorApi } from '../../api/source-connector';
import type {
  DbzKafkaBrokerVo,
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
} from '../../api/types';
import { errorMessage } from '../../utils/format';
import {
  buildTransferFile,
  downloadJson,
  exportFileName,
} from '../../pages/control-panel/connect-transfer';

interface ExportConnectorsModalProps {
  target: DbzKafkaConnectVo | null;
  onClose: () => void;
}

interface ExportData {
  brokers: DbzKafkaBrokerVo[];
  sources: DbzSourceConnectorVo[];
  sinks: DbzSinkConnectorVo[];
}

export default function ExportConnectorsModal({
  target,
  onClose,
}: ExportConnectorsModalProps) {
  const { message } = AntdApp.useApp();
  const [redacted, setRedacted] = useState(false);
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<ExportData>({ brokers: [], sources: [], sinks: [] });

  useEffect(() => {
    if (!target) {
      return;
    }
    setRedacted(false);
    setLoading(true);
    let cancelled = false;
    Promise.all([
      kafkaBrokerApi.list(),
      sourceConnectorApi.list({ kafkaConnectId: target.id }),
      sinkConnectorApi.list({ kafkaConnectId: target.id }),
    ])
      .then(([brokers, sources, sinks]) => {
        if (!cancelled) {
          setData({ brokers, sources, sinks });
        }
      })
      .catch((reason) => {
        if (!cancelled) {
          message.error(`读取连接器清单失败：${errorMessage(reason)}`);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [target, message]);

  const connectorCount = data.sources.length + data.sinks.length;

  const execute = () => {
    if (!target) {
      return;
    }
    try {
      const now = new Date();
      const file = buildTransferFile(
        data.brokers,
        data.sources,
        data.sinks,
        target,
        redacted,
        now,
      );
      downloadJson(exportFileName(target.name, now), file);
      if (redacted) {
        const masked = file.connectors.reduce(
          (sum, item) => sum + item.sensitiveConfigKeys.length,
          0,
        );
        message.success(
          `已脱敏导出 ${file.connectors.length} 个连接器，屏蔽 ${masked} 处凭据（值为 ******）`,
        );
      } else {
        message.success(
          `已导出 ${file.connectors.length} 个连接器（含凭据，敏感字段已在 sensitiveConfigKeys 中标注）`,
        );
      }
      onClose();
    } catch (reason) {
      message.error(`导出失败：${errorMessage(reason)}`);
    }
  };

  return (
    <Modal
      open={target !== null}
      title={target ? `导出「${target.name}」的连接器` : '导出连接器'}
      okText={redacted ? '导出（脱敏）' : '导出（含凭据）'}
      okButtonProps={{ disabled: loading || connectorCount === 0 }}
      onOk={execute}
      onCancel={onClose}
      destroyOnHidden
    >
      {loading ? (
        <Spin />
      ) : (
        <div className="dbz-transfer-export" data-export-dialog>
          <p>
            将导出该 Connect 下 <strong>{connectorCount}</strong> 个已纳管连接器，文件名含
            Connect 名称与导出时间。
          </p>
          <Radio.Group
            value={redacted}
            onChange={(event) => setRedacted(event.target.value as boolean)}
          >
            <Space direction="vertical">
              <Radio value={false} data-export-mode="credential">
                含凭据（默认）：完整配置原样导出
              </Radio>
              <Radio value={true} data-export-mode="redacted">
                脱敏：口令/密钥类字段替换为 ******
              </Radio>
            </Space>
          </Radio.Group>
          {connectorCount === 0 ? (
            <Alert
              type="info"
              showIcon
              message="该 Connect 下还没有已纳管连接器，没有可导出的内容。"
            />
          ) : redacted ? (
            <Alert
              type="warning"
              showIcon
              message="脱敏导出的凭据字段不可用，导入后需要手动补齐才能正常运行。"
            />
          ) : (
            <Alert
              type="warning"
              showIcon
              message="含凭据导出会把口令/密钥原样写进 JSON 文件，请只在可信环境之间传递；每个连接器的 sensitiveConfigKeys 会标注敏感字段。"
            />
          )}
        </div>
      )}
    </Modal>
  );
}
