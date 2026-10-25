/**
 * # 重新同步（触发快照）弹窗
 *
 * 打开时先调预检接口：`source` 通道会连一次源库列出捕获范围内的表，`kafka` 通道不连库、
 * 直接给出连接器自己的表范围。真正的动作是发一条 `execute-snapshot` 信号 ——
 * Debezium 读到后异步开始快照，所以提交成功只代表信号送出去了，这里不能等结果，
 * 也不该把按钮做成「加载中直到完成」。
 *
 * 预检放在打开时而不是提交时：连不上源库、没配信号通道这类问题必须在选表之前暴露，
 * 否则用户选完一堆表才被告知「发不出去」。
 *
 * ## 两条通道怎么选
 *
 * 连接器上能用的通道由服务端算出来（看 `signal.enabled.channels` 与 kafka 是否挂得上
 * broker）。只有一个就直接用；两个都能用时给一组单选，切换会重新预检 —— 两条通道能读
 * 到的表范围不一样，不能让用户看着 A 的清单去发 B 的信号。清单本身与「本次用哪条」
 * 无关，所以切过去还能切回来。
 *
 * 预检失败时给一个「重新预检」按钮：连不上源库多半是暂时的，没必要关掉弹窗重开。
 */

import {
  Alert,
  App as AntdApp,
  Button,
  Descriptions,
  Modal,
  Radio,
  Select,
  Space,
  Spin,
  Tag,
  Typography,
} from 'antd';
import { useState } from 'react';
import { sourceConnectorApi } from '../../api/source-connector';
import type {
  DbzSourceConnectorSnapshotVo,
  DbzSourceConnectorVo,
  SignalChannel,
  SnapshotType,
} from '../../api/types';
import { errorMessage } from '../../utils/format';

interface SnapshotModalProps {
  open: boolean;
  /** 目标上游连接器；关闭时为 null */
  record?: DbzSourceConnectorVo | null;
  onClose: () => void;
}

/** 通道的展示信息 */
const CHANNEL_LABEL: Record<string, { label: string; desc: string }> = {
  source: { label: '源库信号表', desc: '往源库的信号表插一行，需要 dbz 能直连业务库' },
  kafka: { label: 'Kafka 信号 topic', desc: '往信号 topic 投一条消息，不碰业务库' },
};

export default function SnapshotModal({ open, record, onClose }: SnapshotModalProps) {
  const { message } = AntdApp.useApp();
  const [preview, setPreview] = useState<DbzSourceConnectorSnapshotVo | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string[]>([]);
  const [snapshotType, setSnapshotType] = useState<SnapshotType>('incremental');
  const [channel, setChannel] = useState<SignalChannel | undefined>(undefined);
  const [submitting, setSubmitting] = useState(false);

  /** 拉一次预检；表清单默认全选，因为最常见的诉求就是「整库再同步一次」 */
  const loadPreview = async (nextChannel?: SignalChannel) => {
    if (!record) {
      return;
    }
    setLoading(true);
    setLoadError(null);
    setPreview(null);
    setSelected([]);
    setChannel(nextChannel);
    try {
      const vo = await sourceConnectorApi.snapshotPreview(record.id, nextChannel);
      setPreview(vo);
      setSelected(vo.dataCollections);
      setChannel(vo.channel as SignalChannel);
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
      setChannel(undefined);
      return;
    }
    void loadPreview(undefined);
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
        // 带上预检看到的通道：服务端不会在提交时重新挑一条，避免「看着 A 发出 B」
        channel,
      });
      message.success(
        channel === 'kafka'
          ? '快照信号已投递到 Kafka 信号 topic，Debezium 读到后开始快照'
          : '快照信号已写入源库信号表，Debezium 读到后开始快照',
      );
      onClose();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setSubmitting(false);
    }
  };

  /** 两条通道都能用时才给选项 */
  const switchable = (preview?.channels.length ?? 0) > 1;
  const isKafka = preview?.channel === 'kafka';

  return (
    <Modal
      open={open}
      title={`重新同步 · ${record?.name ?? ''}`}
      width={760}
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
          <Typography.Text type="secondary">
            {channel === 'kafka' ? '正在读取连接器配置…' : '正在读取源库表清单…'}
          </Typography.Text>
        </Space>
      )}

      {!loading && loadError && (
        <Alert
          type="error"
          showIcon
          message="预检失败"
          description={loadError}
          // 预检失败的多半是「源库暂时连不上」这类可重试的事，给个按钮，
          // 别让人只能关掉弹窗再点一次「重新同步」
          action={
            <Button size="small" onClick={() => void loadPreview(channel)}>
              重新预检
            </Button>
          }
        />
      )}

      {!loading && preview && (
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          {switchable && (
            <div>
              <Typography.Text strong>信号通道</Typography.Text>
              <Typography.Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                两条都能用，切换会重新预检
              </Typography.Text>
              <Radio.Group
                style={{ display: 'block', marginTop: 8 }}
                value={preview.channel}
                onChange={(event) => void loadPreview(event.target.value as SignalChannel)}
              >
                <Space direction="vertical">
                  {preview.channels.map((item) => (
                    <Radio key={item} value={item}>
                      {CHANNEL_LABEL[item]?.label ?? item}
                      <Typography.Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                        {CHANNEL_LABEL[item]?.desc}
                      </Typography.Text>
                    </Radio>
                  ))}
                </Space>
              </Radio.Group>
            </div>
          )}

          <Alert
            type="info"
            showIcon
            message={
              isKafka
                ? '这一步会往 Kafka 信号 topic 投一条 execute-snapshot 信号'
                : '这一步会往源库的信号表写一条 execute-snapshot 信号'
            }
            description="Debezium 读到信号后对选中的表再跑一次快照，数据以 op=r 追加到原 topic。下游必须按主键幂等（覆盖写）；追加型消费者会看到重复数据。"
          />

          <Descriptions
            size="small"
            column={2}
            items={[
              { key: 'dialect', label: '源库', children: `${preview.dialect} · ${preview.database}` },
              isKafka
                ? {
                    key: 'signal',
                    label: '信号落点',
                    children: (
                      <Space size={4}>
                        <Tag color="processing">{preview.kafkaBroker}</Tag>
                        <span className="dbz-mono">{preview.kafkaTopic}</span>
                      </Space>
                    ),
                  }
                : { key: 'signal', label: '信号表', children: preview.signalCollection },
            ]}
          />

          {preview.warnings.length > 0 && (
            <Alert type="warning" showIcon message="配置提醒" description={preview.warnings.join('；')} />
          )}

          <div>
            <Typography.Text strong>{isKafka ? '要快照的表范围' : '要快照的表'}</Typography.Text>
            <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
              共 {preview.dataCollections.length} 项，已选 {selected.length} 项
            </Typography.Text>
            <Select
              mode="multiple"
              allowClear
              showSearch
              style={{ width: '100%', marginTop: 8 }}
              placeholder={isKafka ? '选择表范围' : '选择要快照的表'}
              maxTagCount="responsive"
              value={selected}
              onChange={(value: string[]) => setSelected(value)}
              options={preview.dataCollections.map((table) => ({ value: table, label: table }))}
            />
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {isKafka
                ? '范围取自连接器的 table.include.list（或库/模式范围），按正则原样交给 Debezium 展开；Kafka 通道不连源库，因此无法核对表是否真的存在。'
                : '清单来自源库 information_schema，已按连接器的捕获范围过滤（信号表自身除外）；至少选一张才能提交。'}
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
