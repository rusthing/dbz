/**
 * # 重新同步（触发快照）弹窗
 *
 * 打开时先调预检接口：服务端按连接器配置连一次源库，列出捕获范围内的表。真正的
 * 动作是往**源库的信号表**插一行 `execute-snapshot`，Debezium 读到后异步开始快照 ——
 * 所以提交成功只代表信号发出去了，这里不能等结果，也不该把按钮做成「加载中直到完成」。
 *
 * 预检放在打开时而不是提交时：连不上源库、没配 `signal.data.collection` 这类问题
 * 必须在选表之前暴露，否则用户选完一堆表才被告知「发不出去」。
 */

import { Alert, App as AntdApp, Descriptions, Modal, Radio, Select, Space, Spin, Typography } from 'antd';
import { useState } from 'react';
import { sourceConnectorApi } from '../../api/source-connector';
import type {
  DbzSourceConnectorSnapshotVo,
  DbzSourceConnectorVo,
  SnapshotType,
} from '../../api/types';
import { errorMessage } from '../../utils/format';

interface SnapshotModalProps {
  open: boolean;
  /** 目标上游连接器；关闭时为 null */
  record?: DbzSourceConnectorVo | null;
  onClose: () => void;
}

export default function SnapshotModal({ open, record, onClose }: SnapshotModalProps) {
  const { message } = AntdApp.useApp();
  const [preview, setPreview] = useState<DbzSourceConnectorSnapshotVo | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string[]>([]);
  const [snapshotType, setSnapshotType] = useState<SnapshotType>('incremental');
  const [submitting, setSubmitting] = useState(false);

  /** 拉一次预检；表清单默认全选，因为最常见的诉求就是「整库再同步一次」 */
  const loadPreview = async () => {
    if (!record) {
      return;
    }
    setLoading(true);
    setLoadError(null);
    setPreview(null);
    setSelected([]);
    try {
      const vo = await sourceConnectorApi.snapshotPreview(record.id);
      setPreview(vo);
      setSelected(vo.dataCollections);
    } catch (reason) {
      setLoadError(errorMessage(reason));
    } finally {
      setLoading(false);
    }
  };

  /**
   * 每次打开都重新预检
   *
   * 连接器配置可能刚改过，表清单不能沿用上一次的结果；放在 `afterOpenChange` 里
   * 与表单弹窗同一个理由 —— 弹窗子树是懒挂载的，`open` 变化的那次渲染还拿不到内部实例。
   */
  const handleOpenChange = (visible: boolean) => {
    if (!visible) {
      setPreview(null);
      setLoadError(null);
      setSelected([]);
      return;
    }
    void loadPreview();
  };

  const handleSubmit = async () => {
    if (!record || !preview) {
      return;
    }
    if (selected.length === 0) {
      message.warning('请至少选择一张表');
      return;
    }
    setSubmitting(true);
    try {
      await sourceConnectorApi.snapshot({
        id: record.id,
        dataCollections: selected,
        snapshotType,
      });
      message.success('快照信号已写入源库信号表，Debezium 读到后开始快照');
      onClose();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title={`重新同步 · ${record?.name ?? ''}`}
      width={720}
      okText="触发快照"
      cancelText="取消"
      confirmLoading={submitting}
      okButtonProps={{ disabled: loading || !preview || selected.length === 0 }}
      onOk={handleSubmit}
      onCancel={onClose}
      afterOpenChange={handleOpenChange}
    >
      {loading && (
        <Space style={{ padding: '40px 0', width: '100%', justifyContent: 'center' }}>
          {/* 不用 Spin 的 tip：antd 只在嵌套/全屏模式下才渲染它，裸用时会被忽略 */}
          <Spin />
          <Typography.Text type="secondary">正在读取源库表清单…</Typography.Text>
        </Space>
      )}

      {!loading && loadError && (
        <Alert type="error" showIcon message="预检失败" description={loadError} />
      )}

      {!loading && preview && (
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Alert
            type="info"
            showIcon
            message="这一步会往源库的信号表写一条 execute-snapshot 信号"
            description="Debezium 读到信号后对选中的表再跑一次快照，数据以 op=r 追加到原 topic。下游必须按主键幂等（覆盖写）；追加型消费者会看到重复数据。"
          />

          <Descriptions
            size="small"
            column={2}
            items={[
              { key: 'dialect', label: '源库', children: `${preview.dialect} · ${preview.database}` },
              { key: 'signal', label: '信号表', children: preview.signalCollection },
            ]}
          />

          {preview.warnings.length > 0 && (
            <Alert type="warning" showIcon message="配置提醒" description={preview.warnings.join('；')} />
          )}

          <div>
            <Typography.Text strong>要快照的表</Typography.Text>
            <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
              共 {preview.dataCollections.length} 张，已选 {selected.length} 张
            </Typography.Text>
            <Select
              mode="multiple"
              allowClear
              showSearch
              style={{ width: '100%', marginTop: 8 }}
              placeholder="选择要快照的表"
              maxTagCount="responsive"
              value={selected}
              onChange={(value: string[]) => setSelected(value)}
              options={preview.dataCollections.map((table) => ({ value: table, label: table }))}
            />
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              清单来自源库 information_schema，已按连接器的捕获范围过滤（信号表自身除外）；至少选一张才能提交。
            </Typography.Text>
          </div>

          <div>
            <Typography.Text strong>快照类型</Typography.Text>
            <Radio.Group
              style={{ display: 'block', marginTop: 8 }}
              value={snapshotType}
              onChange={(event) => setSnapshotType(event.target.value as SnapshotType)}
            >
              <Space direction="vertical">
                <Radio value="incremental">
                  增量快照（推荐）：分块读表，与流式读取并行，不影响连接器继续追增量
                </Radio>
                <Radio value="blocking">
                  阻塞快照：先暂停流式读取再整表重读，期间变更会延后；只在较新版本可用
                </Radio>
              </Space>
            </Radio.Group>
          </div>
        </Space>
      )}
    </Modal>
  );
}
