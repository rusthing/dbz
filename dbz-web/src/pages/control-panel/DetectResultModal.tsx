/**
 * # 统一的「检测结果」弹窗
 *
 * 四类卡片（Kafka 集群 / Connect / 上游连接器 / 下游连接器）点「检测」后都弹这一个
 * 弹窗：结论（正常 / 部分失败 / 异常）、检测项清单与明细、失败原因、耗时。
 *
 * 弹窗只展示该卡片自身检测到的内容：集群带 topic，Connect 带插件与纳管同步结果，
 * 连接器带逐项差异。不跨层聚合。
 *
 * 属性面板里的「检测结果」用同一份 `DetectOutcome` 渲染摘要，弹窗关闭后仍能看到
 * 最近一次结论，且与弹窗说法一致。
 */

import { Alert, Badge, Descriptions, Modal, Space, Table, Tag, Typography } from 'antd';
import type {
  DbzConnectorDetectItemVo,
  DbzKafkaConnectDetectVo,
  KafkaConnectorPluginVo,
} from '../../api/types';

/** 单项检测的结论 */
export type DetectItemStatus = 'ok' | 'warning' | 'error';

/** 一条检测项：检测了什么、结论如何、明细是什么 */
export interface DetectCheckItem {
  key: string;
  label: string;
  status: DetectItemStatus;
  detail: string;
}

/** 一次检测的完整结果，弹窗与属性面板共用 */
export interface DetectOutcome {
  /** 节点 React key，用于挂到属性面板 */
  nodeKey: string;
  /** 卡片类型（Kafka 集群 / Kafka Connect / 上游连接器 / 下游连接器） */
  kindLabel: string;
  name: string;
  /** 本次检测耗时（毫秒） */
  elapsedMs: number;
  items: DetectCheckItem[];
  /** 整体失败原因（请求异常等） */
  reason?: string;
  /** 所属内容：集群的 topic */
  topics?: string[];
  /** 所属内容：Connect 上安装的连接器插件 */
  plugins?: KafkaConnectorPluginVo[];
  /** 所属内容：Connect 检测时对纳管记录的同步结果 */
  sync?: DbzKafkaConnectDetectVo;
}

const STATUS_META: Record<
  DetectItemStatus,
  { badge: 'success' | 'warning' | 'error'; text: string; tag: string; color: string }
> = {
  ok: { badge: 'success', text: '正常', tag: '通过', color: 'success' },
  warning: { badge: 'warning', text: '部分失败', tag: '注意', color: 'warning' },
  error: { badge: 'error', text: '异常', tag: '未通过', color: 'error' },
};

/** 由检测项推出整体结论：有 error 即异常，有 warning 即部分失败 */
export function outcomeStatus(outcome: DetectOutcome): DetectItemStatus {
  if (outcome.items.some((item) => item.status === 'error')) {
    return 'error';
  }
  if (outcome.items.some((item) => item.status === 'warning')) {
    return 'warning';
  }
  return 'ok';
}

/** 结论一句话：检测项数 + 异常/注意项数 */
export function outcomeSummary(outcome: DetectOutcome): string {
  const status = outcomeStatus(outcome);
  const bad = outcome.items.filter((item) => item.status !== 'ok');
  if (status === 'ok') {
    return `全部 ${outcome.items.length} 项检测通过`;
  }
  const label = STATUS_META[status].text;
  if (bad.length === 0) {
    return label;
  }
  return `${label}：${bad.map((item) => item.label).join('、')} 未通过`;
}

const ACTION_LABEL: Record<string, string> = {
  added: '补录',
  marked: '标记不一致',
  disabled: '置禁用',
  cleared: '恢复一致',
  skipped: '跳过',
  unchanged: '无变化',
};

const ACTION_COLOR: Record<string, string> = {
  added: 'blue',
  marked: 'warning',
  disabled: 'error',
  cleared: 'success',
  skipped: 'default',
  unchanged: 'default',
};

function SyncRows({ items }: { items: DbzConnectorDetectItemVo[] }) {
  return (
    <div className="dbz-connect-detect-sync">
      {items.map((item) => (
        <div
          className="dbz-connect-detect-sync-row"
          key={`${item.connectorType}:${item.connectorName}:${item.action}`}
        >
          <Tag color={ACTION_COLOR[item.action] ?? 'default'}>
            {ACTION_LABEL[item.action] ?? item.action}
          </Tag>
          <span className="dbz-mono" title={item.connectorName}>
            {item.connectorName}
          </span>
          <span className="dbz-connect-detect-sync-msg">{item.message || '—'}</span>
        </div>
      ))}
    </div>
  );
}

function TopicList({ topics }: { topics: string[] }) {
  if (topics.length === 0) {
    return <div className="dbz-connect-detect-empty">该集群上还没有任何 topic</div>;
  }
  return (
    <div className="dbz-inspector-check-list">
      {topics.map((topic) => (
        <Tag key={topic} className="dbz-mono">
          {topic}
        </Tag>
      ))}
    </div>
  );
}

function PluginList({ plugins }: { plugins: KafkaConnectorPluginVo[] }) {
  if (plugins.length === 0) {
    return <div className="dbz-connect-detect-empty">未安装任何连接器插件</div>;
  }
  return (
    <div className="dbz-connect-detect-plugins">
      <div className="dbz-connect-detect-head">
        <span>插件类名</span>
        <span>方向</span>
        <span>版本</span>
      </div>
      <div className="dbz-connect-detect-body">
        {plugins.map((plugin) => (
          <div className="dbz-connect-detect-row" key={`${plugin.class}:${plugin.type}`}>
            <span className="dbz-mono" title={plugin.class}>
              {plugin.class}
            </span>
            <span>{plugin.type || '—'}</span>
            <span className="dbz-mono">{plugin.version || '—'}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

/** 弹窗正文：结论 + 检测项 + 归属内容 + 同步结果 */
function OutcomeBody({ outcome }: { outcome: DetectOutcome }) {
  const status = outcomeStatus(outcome);
  const meta = STATUS_META[status];
  return (
    <div className="dbz-detect-body">
      <Alert
        type={status === 'ok' ? 'success' : status === 'warning' ? 'warning' : 'error'}
        showIcon
        message={<Badge status={meta.badge} text={`结论：${meta.text}`} />}
        description={
          <Space direction="vertical" size={2}>
            <span>{outcomeSummary(outcome)}</span>
            {outcome.reason && <span className="dbz-mono">{outcome.reason}</span>}
            <span>耗时 {Math.round(outcome.elapsedMs)} ms</span>
          </Space>
        }
      />

      {outcome.items.length > 0 && (
        <Table<DetectCheckItem>
          style={{ marginTop: 12 }}
          size="small"
          rowKey="key"
          pagination={false}
          dataSource={outcome.items}
          columns={[
            { title: '检测项', dataIndex: 'label', width: 170 },
            {
              title: '结论',
              dataIndex: 'status',
              width: 90,
              render: (value: DetectItemStatus) => (
                <Tag color={STATUS_META[value].color}>{STATUS_META[value].tag}</Tag>
              ),
            },
            {
              title: '明细',
              dataIndex: 'detail',
              render: (detail: string) => (
                <span className="dbz-detect-detail">{detail || '—'}</span>
              ),
            },
          ]}
        />
      )}

      {outcome.topics && (
        <>
          <Typography.Title level={5} className="dbz-inspector-section">
            Topic（{outcome.topics.length} 个）
          </Typography.Title>
          <TopicList topics={outcome.topics} />
        </>
      )}

      {outcome.sync && (
        <>
          <Typography.Title level={5} className="dbz-inspector-section">
            纳管记录同步结果
          </Typography.Title>
          <Descriptions
            column={3}
            size="small"
            items={[
              { key: 'added', label: '补录', children: outcome.sync.added },
              { key: 'marked', label: '标记不一致', children: outcome.sync.marked },
              { key: 'disabled', label: '置禁用', children: outcome.sync.disabled },
              { key: 'cleared', label: '恢复一致', children: outcome.sync.cleared },
              { key: 'unchanged', label: '无变化', children: outcome.sync.unchanged },
              { key: 'skipped', label: '跳过', children: outcome.sync.skipped },
            ]}
          />
          {outcome.sync.items.filter((item) => item.action !== 'unchanged').length > 0 && (
            <SyncRows
              items={outcome.sync.items.filter((item) => item.action !== 'unchanged')}
            />
          )}
        </>
      )}

      {outcome.plugins && (
        <>
          <Typography.Title level={5} className="dbz-inspector-section">
            连接器插件（{outcome.plugins.length} 个）
          </Typography.Title>
          <PluginList plugins={outcome.plugins} />
        </>
      )}
    </div>
  );
}

/** 属性面板里的摘要：结论 + 检测项 + 归属内容，与弹窗同源 */
export function DetectResultBrief({ outcome }: { outcome: DetectOutcome }) {
  const status = outcomeStatus(outcome);
  const meta = STATUS_META[status];
  return (
    <div className="dbz-detect-brief">
      <div className="dbz-inspector-check-head">
        <Space size={6}>
          <Tag color={meta.color}>{meta.text}</Tag>
          <span>{outcomeSummary(outcome)}</span>
        </Space>
        <span className="dbz-inspector-check-count">
          耗时 {Math.round(outcome.elapsedMs)} ms
        </span>
      </div>
      {outcome.items.map((item) => (
        <div className="dbz-inspector-check-group" key={item.key}>
          <div className="dbz-inspector-check-head">
            <Space size={6}>
              <Tag color={STATUS_META[item.status].color}>
                {STATUS_META[item.status].tag}
              </Tag>
              <span>{item.label}</span>
            </Space>
          </div>
          {item.detail && (
            <div className="dbz-inspector-check-empty dbz-detect-brief-detail">
              {item.detail}
            </div>
          )}
        </div>
      ))}
      {outcome.reason && (
        <Alert type="error" showIcon message="失败原因" description={outcome.reason} />
      )}
      {outcome.topics && (
        <div className="dbz-inspector-check-group">
          <div className="dbz-inspector-check-head">
            <span>Topic</span>
            <span className="dbz-inspector-check-count">
              {outcome.topics.length > 0 ? `${outcome.topics.length} 个` : '未获取'}
            </span>
          </div>
          <TopicList topics={outcome.topics} />
        </div>
      )}
      {outcome.sync && (
        <div className="dbz-inspector-check-group">
          <div className="dbz-inspector-check-head">
            <span>纳管同步</span>
            <span className="dbz-inspector-check-count">
              补录 {outcome.sync.added} · 标记 {outcome.sync.marked} · 置禁用{' '}
              {outcome.sync.disabled} · 恢复 {outcome.sync.cleared} · 跳过{' '}
              {outcome.sync.skipped}
            </span>
          </div>
        </div>
      )}
      {outcome.plugins && (
        <div className="dbz-inspector-check-group">
          <div className="dbz-inspector-check-head">
            <span>连接器插件</span>
            <span className="dbz-inspector-check-count">
              {outcome.plugins.length > 0 ? `${outcome.plugins.length} 个` : '未获取'}
            </span>
          </div>
          {outcome.plugins.length > 0 && <PluginList plugins={outcome.plugins} />}
        </div>
      )}
    </div>
  );
}

interface DetectResultModalProps {
  open: boolean;
  outcome: DetectOutcome | null;
  onClose: () => void;
}

/** 四类卡片共用的检测结果弹窗 */
export default function DetectResultModal({ open, outcome, onClose }: DetectResultModalProps) {
  const status = outcome ? outcomeStatus(outcome) : 'ok';
  const meta = STATUS_META[status];
  return (
    <Modal
      open={open && outcome !== null}
      title={
        outcome ? (
          <Space size={8} wrap>
            <span>检测结果</span>
            <Tag>{outcome.kindLabel}</Tag>
            <span className="dbz-mono">{outcome.name}</span>
            <Tag color={meta.color}>{meta.text}</Tag>
          </Space>
        ) : (
          '检测结果'
        )
      }
      width={880}
      okText="知道了"
      cancelButtonProps={{ style: { display: 'none' } }}
      maskClosable
      onCancel={onClose}
      onOk={onClose}
      destroyOnHidden
    >
      {outcome && (
        <div data-detect-dialog data-detect-status={status}>
          <OutcomeBody outcome={outcome} />
        </div>
      )}
    </Modal>
  );
}
