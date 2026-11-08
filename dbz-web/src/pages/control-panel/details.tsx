/**
 * # 控制面板的状态悬停详情
 *
 * 页面和详情抽屉共用这一组构造器，保证同一状态在两处的解释口径一致。
 */

import { Tag } from 'antd';
import type {
  DbzKafkaBrokerVo,
  DbzKafkaConnectVo,
  KafkaConnectorVo,
} from '../../api/types';
import type { StatusDetail } from '../../components/StatusTag';
import {
  dicLabel,
  kafkaSaslMechanismDic,
  kafkaSecurityProtocolDic,
} from '../../dic';
import { formatMs, orDash } from '../../utils/format';
import { liveFailureReason, taskSummary, type ManagedConnectorRecord } from './model';

/**
 * 读取连接器配置里的一个字符串项。
 *
 * 配置不是 JSON、顶层不是对象或键不存在时统一返回空串，调用方按空值展示占位。
 */
export function connectorConfigValueOf(
  record: { connectorConfig: string },
  key: string,
): string {
  try {
    const parsed: unknown = JSON.parse(record.connectorConfig);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return '';
    }
    const value = (parsed as Record<string, unknown>)[key];
    return value === null || value === undefined ? '' : String(value).trim();
  } catch {
    return '';
  }
}

/** 实时连接器状态详情 */
export function liveDetailOf(live: KafkaConnectorVo): StatusDetail {
  const { total, abnormal } = taskSummary(live);
  return {
    summary: '集群实时状态（Kafka Connect REST）',
    rows: [
      { label: 'Worker', value: <span className="dbz-mono">{orDash(live.workerId)}</span> },
      { label: '任务', value: `${total} 个${abnormal > 0 ? `，异常 ${abnormal} 个` : ''}` },
      {
        label: '连接器类',
        value: <span className="dbz-mono">{orDash(live.connectorClass)}</span>,
      },
    ],
    reason: liveFailureReason(live),
  };
}

/** 连接器库内结论详情 */
export function connectorDbDetailOf(
  record: ManagedConnectorRecord,
  cluster?: DbzKafkaConnectVo,
): StatusDetail {
  return {
    summary: '最近一次检查结论（库内记录）',
    rows: [
      { label: '所属 Connect', value: orDash(cluster?.name) },
      { label: '结论时间', value: formatMs(record.updateMs) },
    ],
    reason: record.runMsg,
  };
}

/** Kafka 集群（broker）运行详情 */
export function brokerDetailOf(broker: DbzKafkaBrokerVo): StatusDetail {
  return {
    summary: 'Kafka 集群运行详情',
    rows: [
      {
        label: '地址',
        value: <span className="dbz-mono">{orDash(broker.bootstrapServers)}</span>,
      },
      { label: '认证', value: authLabelOf(broker) },
      { label: '结论时间', value: formatMs(broker.updateMs) },
      { label: '启用', value: broker.enabled ? <Tag color="success">启用</Tag> : <Tag>禁用</Tag> },
    ],
    reason: broker.runMsg,
  };
}

/** Kafka Connect 库内结论详情 */
export function connectDbDetailOf(
  cluster: DbzKafkaConnectVo,
  broker?: DbzKafkaBrokerVo,
): StatusDetail {
  return {
    summary: 'Kafka Connect 最近一次检查结论（库内记录）',
    rows: [
      { label: '地址', value: <span className="dbz-mono">{orDash(cluster.url)}</span> },
      { label: 'Kafka 集群', value: orDash(broker?.name) },
      { label: '结论时间', value: formatMs(cluster.updateMs) },
    ],
    reason: cluster.runMsg,
  };
}

/** broker 的认证方式展示文案 */
export function authLabelOf(broker: DbzKafkaBrokerVo): string {
  const protocol = dicLabel(kafkaSecurityProtocolDic, broker.securityProtocol);
  const sasl = dicLabel(kafkaSaslMechanismDic, broker.saslMechanism);
  return sasl === '不使用' ? protocol : `${protocol} / ${sasl}`;
}

/**
 * # 连接器的 topic 信息
 *
 * 上游取 Debezium 的 `topic.prefix`（老配置兼容 `database.server.name`），
 * 下游取 `topics` 或 `topics.regex`。配置不是合法 JSON 时静默返回空串，
 * 列表页不因此报错。
 */
export function topicInfoOf(
  kind: 'source' | 'sink',
  record: ManagedConnectorRecord,
): { label: string; value: string } | null {
  const raw = record.connectorConfig?.trim();
  if (!raw) {
    return null;
  }

  let config: Record<string, unknown>;
  try {
    config = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return null;
  }

  const keys = kind === 'source' ? ['topic.prefix', 'database.server.name'] : ['topics', 'topics.regex'];
  for (const key of keys) {
    const value = config[key];
    if (typeof value === 'string' && value.trim()) {
      return {
        label: kind === 'source' ? 'topic 前缀' : 'topics',
        value: value.trim(),
      };
    }
    if (Array.isArray(value) && value.length > 0) {
      return { label: 'topics', value: value.join('、') };
    }
  }
  return null;
}

