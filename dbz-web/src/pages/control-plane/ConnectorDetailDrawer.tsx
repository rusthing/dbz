/**
 * # 连接器详情抽屉
 *
 * 图上每个连接器节点的「详情」入口：把库内记录、集群实时状态、任务和配置放在一处，
 * 同时提供启用/停用、重启、跳转明细维护页面。
 *
 * 配置优先展示**集群侧实时配置**（`GET /connectors/{name}/config`）；查询失败时
 * 回落到库内保存的那份，并明确说明原因（连接器不存在 404 / Connect 不可达 500）。
 */

import { Alert, Button, Descriptions, Drawer, Popconfirm, Space, Spin, Table, Tag, Tooltip, Typography } from 'antd';
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ApiError } from '../../api/client';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import type {
  DbzKafkaBrokerVo,
  DbzKafkaConnectVo,
  DbzSourceConnectorVo,
  Id,
  KafkaConnectorVo,
} from '../../api/types';
import EnabledSwitch from '../../components/EnabledSwitch';
import StatusTag from '../../components/StatusTag';
import { connectorRunStatusDic, kafkaConnectorStatusDic } from '../../dic';
import { errorMessage, formatMs, orDash, prettyJson, truncate } from '../../utils/format';
import {
  type ConnectorDetailTarget,
  type ConnectorKind,
  deriveConnectorMismatch,
  taskSummary,
} from './model';
import { connectorDbDetailOf, liveDetailOf } from './details';

/** 集群侧配置查询状态 */
interface ConfigState {
  loading: boolean;
  /** 集群返回的配置对象；有值时优先展示 */
  config?: Record<string, unknown>;
  /** 查询失败原因，失败时回落到库内配置 */
  error?: string;
  /** HTTP 状态，用于区分「连接器不存在」与「Connect 不可达」 */
  status?: number;
}

interface ConnectorDetailDrawerProps {
  target: ConnectorDetailTarget | null;
  cluster?: DbzKafkaConnectVo;
  broker?: DbzKafkaBrokerVo;
  /** 下游连接器才有 */
  upstream?: DbzSourceConnectorVo;
  /** 集群上的实时记录 */
  live?: KafkaConnectorVo;
  /** 实时查询失败原因 */
  liveIssue?: string;
  restarting: boolean;
  onClose: () => void;
  onRestart: () => void;
  onToggle: (enabled: boolean) => Promise<void>;
}

/** 明细维护页面路径 */
function detailPath(kind: ConnectorKind): string {
  return kind === 'source' ? '/source-connector' : '/sink-connector';
}

export default function ConnectorDetailDrawer({
  target,
  cluster,
  broker,
  upstream,
  live,
  liveIssue,
  restarting,
  onClose,
  onRestart,
  onToggle,
}: ConnectorDetailDrawerProps) {
  const navigate = useNavigate();
  const connectId: Id | undefined = target?.record.kafkaConnectId;
  const connectorName: string | undefined = target?.record.connectorName;
  const [config, setConfig] = useState<ConfigState>({ loading: false });

  // 集群侧配置随抽屉打开加载；关闭后丢弃结果，避免串到下一个连接器
  useEffect(() => {
    if (!connectId || !connectorName) {
      setConfig({ loading: false });
      return;
    }
    let cancelled = false;
    setConfig({ loading: true });
    kafkaConnectorApi
      .config(connectId, connectorName)
      .then((vo) => {
        if (!cancelled) {
          setConfig({ loading: false, config: vo.config });
        }
      })
      .catch((reason: unknown) => {
        if (cancelled) {
          return;
        }
        setConfig({
          loading: false,
          error: errorMessage(reason),
          status: reason instanceof ApiError ? reason.status : undefined,
        });
      });
    return () => {
      cancelled = true;
    };
  }, [connectId, connectorName]);

  if (!target) {
    return null;
  }

  const { kind, record } = target;
  const connectDisabled = Boolean(cluster && !cluster.enabled);
  const mismatch =
    connectDisabled || liveIssue ? undefined : deriveConnectorMismatch(record, live);
  const { total, abnormal } = live ? taskSummary(live) : { total: 0, abnormal: 0 };
  const recordConnectId: Id = record.kafkaConnectId;

  const dbDetail = connectorDbDetailOf(record, cluster);
  const configFromCluster = config.config !== undefined;

  return (
    <Drawer
      open
      width={760}
      title={`连接器详情 · ${record.name}`}
      onClose={onClose}
      extra={
        <Space size={8} wrap>
          <EnabledSwitch
            enabled={record.enabled}
            name={record.name}
            confirmContent="禁用后会先从这个 Kafka Connect 注销连接器，再改库；重新启用会当场把连接器部署回去。"
            onToggle={onToggle}
          />
          <Popconfirm
            title="重启连接器？"
            description="会连同任务一起重启。Kafka Connect 异步执行，状态稍后刷新。"
            okText="重启"
            cancelText="取消"
            onConfirm={onRestart}
          >
            <Tooltip
              title={
                connectDisabled
                  ? '所属 Kafka Connect 已停用，先启用 Connect 后再重启'
                  : !record.enabled
                    ? '连接器已禁用，先在集群上启用后再重启'
                    : !live
                      ? '集群上没有该连接器，无法重启'
                      : undefined
              }
            >
              <Button
                size="small"
                loading={restarting}
                disabled={connectDisabled || !record.enabled || !live}
              >
                重启
              </Button>
            </Tooltip>
          </Popconfirm>
          <Button size="small" type="link" onClick={() => navigate(detailPath(kind))}>
            明细维护
          </Button>
          <Button
            size="small"
            type="link"
            onClick={() => navigate(`/remote-connector?kafkaConnectId=${recordConnectId}`)}
          >
            集群侧
          </Button>
        </Space>
      }
    >
      {mismatch && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="库内记录与集群实时状态不一致"
          description={mismatch}
        />
      )}
      {liveIssue && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="实时状态查询失败"
          description={`${liveIssue}。下方库内状态仍为最近一次检查结论，不参与本次比对。`}
        />
      )}

      <Descriptions bordered size="small" column={2} styles={{ label: { width: 110 } }}>
        <Descriptions.Item label="方向">
          <Tag color={kind === 'source' ? 'blue' : 'purple'}>
            {kind === 'source' ? '上游 source' : '下游 sink'}
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label="连接器名称">
          <span className="dbz-mono">{orDash(record.connectorName)}</span>
        </Descriptions.Item>
        <Descriptions.Item label="所属 Connect">{orDash(cluster?.name)}</Descriptions.Item>
        <Descriptions.Item label="Kafka 集群">{orDash(broker?.name)}</Descriptions.Item>
        {kind === 'sink' && (
          <Descriptions.Item label="上游连接器">{orDash(upstream?.name)}</Descriptions.Item>
        )}
        <Descriptions.Item label="库内状态">
          <StatusTag dic={connectorRunStatusDic} value={record.runStatus} detail={dbDetail} />
        </Descriptions.Item>
        <Descriptions.Item label="集群实时状态">
          {connectDisabled ? (
            <Tag>所属 Connect 已停用</Tag>
          ) : !record.enabled ? (
            <Tag>已停用</Tag>
          ) : liveIssue ? (
            <Tooltip title={liveIssue}>
              <Tag color="error">实时不可达</Tag>
            </Tooltip>
          ) : live ? (
            <StatusTag
              dic={kafkaConnectorStatusDic}
              value={live.state}
              detail={liveDetailOf(live)}
            />
          ) : (
            <Tag color="warning">集群上未部署</Tag>
          )}
        </Descriptions.Item>
        <Descriptions.Item label="Worker">
          <span className="dbz-mono">{orDash(live?.workerId)}</span>
        </Descriptions.Item>
        <Descriptions.Item label="任务">
          {total === 0 ? '无' : `${total} 个${abnormal > 0 ? `（异常 ${abnormal} 个）` : ''}`}
        </Descriptions.Item>
        <Descriptions.Item label="连接器类">
          <span className="dbz-mono">{orDash(live?.connectorClass)}</span>
        </Descriptions.Item>
        <Descriptions.Item label="最近检查">
          {formatMs(record.updateMs)}
        </Descriptions.Item>
        <Descriptions.Item label="备注" span={2}>
          {orDash(record.remark)}
        </Descriptions.Item>
        {kind === 'sink' && (
          <Descriptions.Item label="上游 ID" span={2}>
            <span className="dbz-mono">
              {orDash('sourceConnectorId' in record ? record.sourceConnectorId : undefined)}
            </span>
          </Descriptions.Item>
        )}
      </Descriptions>

      {live && live.tasks.length > 0 && (
        <>
          <Typography.Title level={5} style={{ marginTop: 20 }}>
            任务
          </Typography.Title>
          <Table
            size="small"
            rowKey="id"
            pagination={false}
            dataSource={live.tasks}
            columns={[
              { title: '任务', dataIndex: 'id', width: 72, render: (id: number) => `#${id}` },
              {
                title: '状态',
                dataIndex: 'state',
                width: 100,
                render: (state: number) => (
                  <StatusTag dic={kafkaConnectorStatusDic} value={state} />
                ),
              },
              {
                title: 'Worker',
                dataIndex: 'workerId',
                width: 180,
                ellipsis: true,
                render: (workerId?: string | null) => (
                  <span className="dbz-mono">{orDash(workerId)}</span>
                ),
              },
              {
                title: '异常',
                dataIndex: 'trace',
                ellipsis: true,
                render: (trace?: string | null) =>
                  trace ? (
                    <Tooltip title={trace}>
                      <span className="dbz-ellipsis">{truncate(trace, 80)}</span>
                    </Tooltip>
                  ) : (
                    '--'
                  ),
              },
            ]}
          />
        </>
      )}

      <Typography.Title level={5} style={{ marginTop: 20 }}>
        连接器配置
        <Tag
          color={configFromCluster ? 'green' : undefined}
          style={{ marginLeft: 8, fontWeight: 'normal' }}
        >
          {configFromCluster ? '集群实时' : '库内保存'}
        </Tag>
        {config.loading && <Spin size="small" style={{ marginLeft: 8 }} />}
      </Typography.Title>
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        集群实时配置来自
        <span className="dbz-mono"> GET /connectors/&#123;name&#125;/config</span>
        ，与库内保存的配置可能不一致（有人绕过 dbz 直接改过集群）。
      </Typography.Text>
      {config.error && (
        <Alert
          type="warning"
          showIcon
          style={{ marginTop: 10 }}
          message={
            config.status === 404 ? '集群上不存在该连接器' : '集群配置查询失败'
          }
          description={`${config.error}。下方展示库内保存的配置。`}
        />
      )}
      <pre className="dbz-json-view" style={{ marginTop: 10 }}>
        {configFromCluster
          ? JSON.stringify(config.config, null, 2)
          : prettyJson(record.connectorConfig) || '--'}
      </pre>
    </Drawer>
  );
}
