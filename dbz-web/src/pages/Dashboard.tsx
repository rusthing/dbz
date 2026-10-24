/**
 * # 总览
 *
 * 四张表各自全量拉一次，在浏览器里算统计与异常清单。数量级是「几十台集群 + 几百条连接器」，
 * 一次拉完比让后端再开四个统计接口更划算，也避免统计口径在两处各写一遍。
 *
 * ## 口径
 *
 * - **正常**：已启用且状态为「正常 / 运行中」；
 * - **异常**：已启用且状态是失败类（连接失败、注册失败、检测异常……）；
 * - **待检测**：已启用但状态还是「无状态」——通常是刚新增或刚改过地址，等下一轮检查；
 * - **已禁用**：`enabled = false`，不参与健康检查，因此也不算异常。
 *
 * Kafka 集群多一种「已禁用」的写法：禁用后检查任务会把 `runStatus` 写成 Disabled(2)，
 * 所以这里按 `enabled` 与状态两处一起判定，避免禁用中的集群被算进异常。
 */

import { Alert, Button, Card, Col, Row, Space, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ReloadOutlined } from '@ant-design/icons';
import { useMemo } from 'react';
import { Link } from 'react-router-dom';
import { kafkaBrokerApi } from '../api/kafka-broker';
import { kafkaConnectApi } from '../api/kafka-connect';
import { sinkConnectorApi } from '../api/sink-connector';
import { sourceConnectorApi } from '../api/source-connector';
import type {
  DbzKafkaBrokerVo,
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
} from '../api/types';
import StatusTag from '../components/StatusTag';
import type { DicItem } from '../dic';
import { connectorRunStatusDic, kafkaBrokerStatusDic, kafkaConnectStatusDic } from '../dic';
import { useAsync } from '../hooks/useAsync';
import { formatMs } from '../utils/format';

/** 一条异常记录，四个来源统一成同一行结构 */
interface AnomalyRow {
  key: string;
  kind: '集群' | '消息集群' | '上游连接器' | '下游连接器';
  name: string;
  cluster: string;
  status: number;
  dic: DicItem[];
  updateMs: Id;
}

/** 一组统计数字 */
interface StatGroup {
  total: number;
  healthy: number;
  abnormal: number;
  pending: number;
  disabled: number;
}

/** 全零的统计，每次新建：reduce 会就地累加，不能共用同一个对象 */
const emptyStat = (): StatGroup => ({ total: 0, healthy: 0, abnormal: 0, pending: 0, disabled: 0 });

/** 计算统计（集群 / 上游 / 下游口径一致） */
function countByStatus(items: { enabled: boolean; runStatus: number }[]): StatGroup {
  return items.reduce<StatGroup>((acc, item) => {
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
  }, emptyStat());
}

/**
 * Kafka 集群的统计
 *
 * 与另外三张表不同：禁用一台集群后，检查任务会把 `runStatus` 落成 Disabled(2)，
 * 所以只看 `enabled` 不够，状态 2 也要归到「禁用」，否则禁用中的集群会被算进异常。
 */
function countBrokers(items: DbzKafkaBrokerVo[]): StatGroup {
  return items.reduce<StatGroup>((acc, item) => {
    acc.total += 1;
    if (!item.enabled || item.runStatus === 2) {
      acc.disabled += 1;
    } else if (item.runStatus === 1) {
      acc.healthy += 1;
    } else if (item.runStatus === 0) {
      acc.pending += 1;
    } else {
      acc.abnormal += 1;
    }
    return acc;
  }, emptyStat());
}

/** 统计卡片 */
function StatCard({
  title,
  group,
  unit,
  hint,
}: {
  title: string;
  group: StatGroup;
  unit: string;
  hint?: string;
}) {
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
        {hint ? ` · ${hint}` : ''}
      </div>
    </Card>
  );
}

export default function Dashboard() {
  const brokers = useAsync<DbzKafkaBrokerVo[]>(() => kafkaBrokerApi.list(), [], []);
  const clusters = useAsync<DbzKafkaConnectVo[]>(() => kafkaConnectApi.list(), [], []);
  const sources = useAsync<DbzSourceConnectorVo[]>(() => sourceConnectorApi.list(), [], []);
  const sinks = useAsync<DbzSinkConnectorVo[]>(() => sinkConnectorApi.list(), [], []);

  const loading = brokers.loading || clusters.loading || sources.loading || sinks.loading;

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

  /** 没有任何 worker 指向的 Kafka 集群：连接器投不进消息，信号也发不出去 */
  const unusedBrokerCount = useMemo(() => {
    const used = new Set(clusters.data.map((item) => item.kafkaBrokerId));
    return brokers.data.filter((item) => !used.has(item.id)).length;
  }, [brokers.data, clusters.data]);

  const brokerStat = useMemo(() => countBrokers(brokers.data), [brokers.data]);
  const clusterStat = useMemo(() => countByStatus(clusters.data), [clusters.data]);
  const sourceStat = useMemo(() => countByStatus(sources.data), [sources.data]);
  const sinkStat = useMemo(() => countByStatus(sinks.data), [sinks.data]);

  const anomalies = useMemo<AnomalyRow[]>(() => {
    const rows: AnomalyRow[] = [];

    brokers.data.forEach((item) => {
      // 3 = 连接失败；0 是「无状态」，2 是禁用后的落值，都不算异常
      if (item.enabled && item.runStatus === 3) {
        rows.push({
          key: `broker-${item.id}`,
          kind: '消息集群',
          name: item.name,
          cluster: item.bootstrapServers,
          status: item.runStatus,
          dic: kafkaBrokerStatusDic,
          updateMs: item.updateMs,
        });
      }
    });

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

    // updateMs 是 u64 字符串，减号会隐式转数字；显式写出来避免被当成字符串比较
    return rows.sort((left, right) => Number(right.updateMs) - Number(left.updateMs));
  }, [brokers.data, clusters.data, sources.data, sinks.data, clusterById]);

  const error = brokers.error ?? clusters.error ?? sources.error ?? sinks.error;

  const columns: ColumnsType<AnomalyRow> = [
    {
      title: '对象',
      dataIndex: 'kind',
      width: 130,
      render: (kind: AnomalyRow['kind']) => {
        const color =
          kind === '集群' ? 'geekblue' : kind === '消息集群' ? 'gold' : kind === '上游连接器' ? 'blue' : 'purple';
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
      render: (ms: Id) => formatMs(ms),
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
            brokers.reload();
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

      <Row gutter={[16, 16]}>
        {/* 五张卡片用 flex 而不是 xl={6}：24 栅格除不尽 5 份，硬分会留一条空档 */}
        <Col flex="1 1 200px">
          <StatCard title="Kafka 集群" group={brokerStat} unit="台" hint="连接器投递的目标" />
        </Col>
        <Col flex="1 1 200px">
          <StatCard title="Kafka Connect 集群" group={clusterStat} unit="台" hint="运行 worker 的集群" />
        </Col>
        <Col flex="1 1 200px">
          <StatCard title="上游连接器" group={sourceStat} unit="条" />
        </Col>
        <Col flex="1 1 200px">
          <StatCard title="下游连接器" group={sinkStat} unit="条" />
        </Col>
        <Col flex="1 1 200px">
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
            <Link to="/kafka-broker">Kafka 集群</Link>
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
          <Typography.Text type="secondary">
            Kafka 集群 {brokers.data.length} 个，其中 {unusedBrokerCount} 个还没有 worker 指向它
            （worker 的 Kafka 集群在集群管理里选）：没有 worker 的集群收不到变更，也不会收到
            Kafka 信号。
          </Typography.Text>
        </Space>
      </Card>
    </Space>
  );
}
