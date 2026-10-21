/**
 * # 总览
 *
 * 三张表各自全量拉一次，在浏览器里算统计与异常清单。数量级是「几十台集群 + 几百条连接器」，
 * 一次拉完比让后端再开三个统计接口更划算，也避免统计口径在两处各写一遍。
 *
 * ## 口径
 *
 * - **正常**：已启用且状态为「正常 / 运行中」；
 * - **异常**：已启用且状态是失败类（连接失败、注册失败、检测异常……）；
 * - **待检测**：已启用但状态还是「无状态」——通常是刚新增或刚改过地址，等下一轮检查；
 * - **已禁用**：`enabled = false`，不参与健康检查，因此也不算异常。
 */

import { Alert, Button, Card, Col, Row, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ReloadOutlined } from '@ant-design/icons';
import { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { kafkaConnectApi } from '../api/kafka-connect';
import { sinkConnectorApi } from '../api/sink-connector';
import { sourceConnectorApi } from '../api/source-connector';
import type { DbzKafkaConnectVo, DbzSinkConnectorVo, DbzSourceConnectorVo } from '../api/types';
import StatusTag from '../components/StatusTag';
import type { DicItem } from '../dic';
import { connectorRunStatusDic, kafkaConnectStatusDic } from '../dic';
import { useAsync } from '../hooks/useAsync';
import { formatMs } from '../utils/format';

/** 一条异常记录，三个来源统一成同一行结构 */
interface AnomalyRow {
  key: string;
  kind: '集群' | '上游连接器' | '下游连接器';
  name: string;
  cluster: string;
  status: number;
  dic: DicItem[];
  updateMs: number;
}

/** 一组统计数字 */
interface StatGroup {
  total: number;
  healthy: number;
  abnormal: number;
  pending: number;
  disabled: number;
}

/** 计算连接器统计（上游/下游口径一致） */
function countConnectors(
  items: { enabled: boolean; runStatus: number }[],
): StatGroup {
  return items.reduce<StatGroup>(
    (acc, item) => {
      acc.total += 1;
      if (!item.enabled) {
        acc.disabled += 1;
      } else if (item.runStatus === 1) {
        acc.healthy += 1;
      } else if (item.runStatus === 0) {
        acc.pending += 1;
      } else {
        acc.abnormal += 1;
      }
      return acc;
    },
    { total: 0, healthy: 0, abnormal: 0, pending: 0, disabled: 0 },
  );
}

/** 计算集群统计 */
function countClusters(items: DbzKafkaConnectVo[]): StatGroup {
  return items.reduce<StatGroup>(
    (acc, item) => {
      acc.total += 1;
      if (!item.enabled) {
        acc.disabled += 1;
      } else if (item.runStatus === 1) {
        acc.healthy += 1;
      } else if (item.runStatus === 0) {
        acc.pending += 1;
      } else {
        acc.abnormal += 1;
      }
      return acc;
    },
    { total: 0, healthy: 0, abnormal: 0, pending: 0, disabled: 0 },
  );
}

/** 统计卡片 */
function StatCard({ title, group, unit }: { title: string; group: StatGroup; unit: string }) {
  return (
    <Card variant="borderless" className="dbz-stat-card">
      <div className="dbz-stat-label">{title}</div>
      <div className="dbz-stat-value">{group.total}</div>
      <Space size={6} wrap style={{ marginTop: 8 }}>
        <Tag color="success">正常 {group.healthy}</Tag>
        <Tag color={group.abnormal > 0 ? 'error' : 'default'}>异常 {group.abnormal}</Tag>
        <Tag color={group.pending > 0 ? 'processing' : 'default'}>待检测 {group.pending}</Tag>
        <Tag>禁用 {group.disabled}</Tag>
      </Space>
      <div className="dbz-stat-hint" style={{ marginTop: 8 }}>
        单位：{unit}
      </div>
    </Card>
  );
}

export default function Dashboard() {
  const clusters = useAsync<DbzKafkaConnectVo[]>(() => kafkaConnectApi.list(), [], []);
  const sources = useAsync<DbzSourceConnectorVo[]>(() => sourceConnectorApi.list(), [], []);
  const sinks = useAsync<DbzSinkConnectorVo[]>(() => sinkConnectorApi.list(), [], []);

  const loading = clusters.loading || sources.loading || sinks.loading;

  const clusterById = useMemo(
    () => new Map(clusters.data.map((item) => [item.id, item.name])),
    [clusters.data],
  );
  const sourceById = useMemo(
    () => new Map(sources.data.map((item) => [item.id, item])),
    [sources.data],
  );

  /** 与上游不在同一台集群上的下游数量，用于「链路速查」 */
  const crossClusterSinkCount = useMemo(
    () =>
      sinks.data.filter((sink) => {
        const source = sourceById.get(sink.sourceConnectorId);
        return source !== undefined && source.kafkaConnectId !== sink.kafkaConnectId;
      }).length,
    [sinks.data, sourceById],
  );

  /** 被下游挂载过的上游数量 */
  const mountedSourceCount = useMemo(
    () => new Set(sinks.data.map((item) => item.sourceConnectorId)).size,
    [sinks.data],
  );

  const clusterStat = useMemo(() => countClusters(clusters.data), [clusters.data]);
  const sourceStat = useMemo(() => countConnectors(sources.data), [sources.data]);
  const sinkStat = useMemo(() => countConnectors(sinks.data), [sinks.data]);

  const anomalies = useMemo<AnomalyRow[]>(() => {
    const rows: AnomalyRow[] = [];

    clusters.data.forEach((item) => {
      // 3 = 连接失败；0 是「无状态」，等下一轮检查，不算异常
      if (item.enabled && item.runStatus === 3) {
        rows.push({
          key: `cluster-${item.id}`,
          kind: '集群',
          name: item.name,
          cluster: item.url,
          status: item.runStatus,
          dic: kafkaConnectStatusDic,
          updateMs: item.updateMs,
        });
      }
    });

    sources.data.forEach((item) => {
      if (item.enabled && item.runStatus >= 3) {
        rows.push({
          key: `source-${item.id}`,
          kind: '上游连接器',
          name: item.name,
          cluster: clusterById.get(item.kafkaConnectId) ?? `#${item.kafkaConnectId}`,
          status: item.runStatus,
          dic: connectorRunStatusDic,
          updateMs: item.updateMs,
        });
      }
    });

    sinks.data.forEach((item) => {
      if (item.enabled && item.runStatus >= 3) {
        rows.push({
          key: `sink-${item.id}`,
          kind: '下游连接器',
          name: item.name,
          cluster: clusterById.get(item.kafkaConnectId) ?? `#${item.kafkaConnectId}`,
          status: item.runStatus,
          dic: connectorRunStatusDic,
          updateMs: item.updateMs,
        });
      }
    });

    return rows.sort((left, right) => right.updateMs - left.updateMs);
  }, [clusters.data, sources.data, sinks.data, clusterById]);

  const error = clusters.error ?? sources.error ?? sinks.error;

  const columns: ColumnsType<AnomalyRow> = [
    {
      title: '对象',
      dataIndex: 'kind',
      width: 130,
      render: (kind: AnomalyRow['kind']) => {
        const color = kind === '集群' ? 'geekblue' : kind === '上游连接器' ? 'blue' : 'purple';
        return <Tag color={color}>{kind}</Tag>;
      },
    },
    { title: '名称', dataIndex: 'name', width: 220 },
    {
      title: '所属集群 / 地址',
      dataIndex: 'cluster',
      ellipsis: true,
      render: (cluster: string) => <span className="dbz-mono">{cluster}</span>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 160,
      render: (status: number, record) => <StatusTag dic={record.dic} value={status} />,
    },
    {
      title: '进入该状态时间',
      dataIndex: 'updateMs',
      width: 180,
      render: (ms: number) => formatMs(ms),
    },
  ];

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <div className="dbz-toolbar" style={{ marginBottom: 0 }}>
        <Typography.Text type="secondary">
          异常来自最近一轮健康检查的结论，自动修复（重启）只是发请求，生效后需要下一轮确认
        </Typography.Text>
        <span className="dbz-toolbar-spacer" />
        <Button
          icon={<ReloadOutlined />}
          loading={loading}
          onClick={() => {
            clusters.reload();
            sources.reload();
            sinks.reload();
          }}
        >
          刷新
        </Button>
      </div>

      {error && (
        <Alert
          type="error"
          showIcon
          message="加载统计数据失败"
          description={error.message}
        />
      )}

      <Row gutter={16}>
        <Col xs={24} md={12} xl={6}>
          <StatCard title="Kafka Connect 集群" group={clusterStat} unit="台" />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <StatCard title="上游连接器" group={sourceStat} unit="条" />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <StatCard title="下游连接器" group={sinkStat} unit="条" />
        </Col>
        <Col xs={24} md={12} xl={6}>
          <Card variant="borderless" className="dbz-stat-card">
            <div className="dbz-stat-label">异常对象</div>
            <div
              className="dbz-stat-value"
              style={{ color: anomalies.length > 0 ? '#cf1322' : undefined }}
            >
              {anomalies.length}
            </div>
            <div className="dbz-stat-hint" style={{ marginTop: 8 }}>
              已启用的集群与连接器中，状态为失败类或异常的数量
            </div>
          </Card>
        </Col>
      </Row>

      <Card
        variant="borderless"
        title="异常清单"
        extra={
          <Space size={12}>
            <Link to="/kafka-connect">集群管理</Link>
            <Link to="/source-connector">上游连接器</Link>
            <Link to="/sink-connector">下游连接器</Link>
          </Space>
        }
      >
        <Table<AnomalyRow>
          rowKey="key"
          size="middle"
          loading={loading}
          columns={columns}
          dataSource={anomalies}
          pagination={false}
          locale={{ emptyText: '当前没有异常对象' }}
        />
      </Card>

      <Card variant="borderless" title="链路速查">
        <Space direction="vertical" size={6}>
          <Typography.Text type="secondary">
            上游连接器 {sources.data.length} 条，其中 {mountedSourceCount} 条已被下游挂载；
            未被挂载的上游不会出现在异常清单的「下游」一栏里。
          </Typography.Text>
          <Typography.Text type="secondary">
            上下游可以分处不同集群（只要对接同一个 Kafka）：当前有 {crossClusterSinkCount} 条下游
            与其上游不在同一台集群上。
          </Typography.Text>
        </Space>
      </Card>
    </Space>
  );
}
