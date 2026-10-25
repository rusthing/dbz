/**
 * # Kafka 集群（broker 侧）管理
 *
 * 这一页管的是**消息集群**：上游连接器的信号往哪个集群投、认证怎么配、集群通不通。
 * 与「Kafka Connect」页的区别是那边管连接器跑在哪台 worker，两页的记录通过
 * Connect 表单上的「Kafka 集群」下拉关联起来。
 *
 * 状态只由「检测」改写：手动点按钮，或 jobx 每轮健康检查先跑一遍集群检测。改地址、
 * 改认证、重新启用都会把状态置回「无状态」，等下一轮检查给结论。
 *
 * 列里没有「更新时间」：这一页的判据是「地址/认证配得对不对、通不通」，加上它就挤掉
 * 备注的位置了（见 `scroll` 的宽度约束）。
 */

import { App as AntdApp, Alert, Button, Card, Input, Select, Space, Table, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { PlusOutlined, ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { useCallback, useState } from 'react';
import { kafkaBrokerApi } from '../../api/kafka-broker';
import type { DbzKafkaBrokerVo, KafkaBrokerStatus } from '../../api/types';
import EnabledSwitch from '../../components/EnabledSwitch';
import StatusTag from '../../components/StatusTag';
import type { StatusDetail } from '../../components/StatusTag';
import {
  dicLabel,
  dicOptions,
  kafkaBrokerStatusDic,
  kafkaSaslMechanismDic,
  kafkaSecurityProtocolDic,
} from '../../dic';
import { usePagedQuery } from '../../hooks/usePagedQuery';
import { errorMessage, formatMs, orDash } from '../../utils/format';
import KafkaBrokerForm from './KafkaBrokerForm';

/** 筛选表单的草稿状态 */
interface FilterDraft {
  keyword?: string;
  runStatus?: KafkaBrokerStatus;
  enabled?: boolean;
}

/**
 * 状态悬停详情
 *
 * 列表里的状态是「最近一次检测」的结论，悬停要回答的是：怎么连、什么时候得出的、
 * 失败原因是什么。原因由服务端在检测时写进 `run_msg`，不看这份就得去翻日志。
 */
function statusDetailOf(record: DbzKafkaBrokerVo): StatusDetail {
  const auth =
    record.securityProtocol === 0
      ? 'PLAINTEXT（无认证）'
      : `SASL_PLAINTEXT · ${dicLabel(kafkaSaslMechanismDic, record.saslMechanism)}${
          record.saslUsername ? ` · ${record.saslUsername}` : ''
        }`;
  return {
    summary: `运行详情 · ${record.name}`,
    rows: [
      { label: '地址', value: <span className="dbz-mono">{record.bootstrapServers}</span> },
      { label: '认证', value: auth },
      { label: '结论时间', value: formatMs(record.updateMs) },
      ...(record.runStatus === 0
        ? [{ label: '说明', value: '刚新增或改过地址/认证，等下一轮检测给结论' }]
        : []),
    ],
    reason: record.runMsg,
  };
}

export default function KafkaBrokerPage() {
  const { message, modal } = AntdApp.useApp();
  const [draft, setDraft] = useState<FilterDraft>({});
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<DbzKafkaBrokerVo | null>(null);
  /** 正在检测的记录ID，用来给按钮转圈 */
  const [checking, setChecking] = useState<string | null>(null);

  const { data, loading, error, pageNum, pageSize, search, reload, setPageNum, setPageSize } =
    usePagedQuery<DbzKafkaBrokerVo>((params) => kafkaBrokerApi.page(params));

  /** 把草稿翻译成 QueryDto 的查询参数；空值不传，避免变成等值条件 */
  const applyFilters = useCallback(
    (next: FilterDraft) => {
      search({
        _keyword: next.keyword?.trim() || undefined,
        runStatus: next.runStatus,
        enabled: next.enabled,
      });
    },
    [search],
  );

  /**
   * # 检测连通性
   *
   * 连不上也是 200（`reachable = false`），所以这里不能只看有没有抛错：
   * 抛错说明记录不存在之类的调用问题，`reachable = false` 才是集群不通。
   */
  const handleCheck = async (record: DbzKafkaBrokerVo) => {
    setChecking(record.id);
    try {
      const result = await kafkaBrokerApi.check(record.id);
      if (result.reachable) {
        message.success(`「${record.name}」连接正常，${result.topicCount} 个 topic，耗时 ${result.elapsedMs}ms`);
      } else {
        modal.warning({
          title: `「${record.name}」连接失败`,
          content: result.message ?? '未返回失败原因',
          okText: '知道了',
        });
      }
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setChecking(null);
    }
  };

  const handleDelete = (record: DbzKafkaBrokerVo) => {
    modal.confirm({
      title: `删除 Kafka 集群「${record.name}」？`,
      content: '还有 Kafka Connect 记录挂在它上面时服务端会拒绝删除，请先删除或改挂这些记录。',
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await kafkaBrokerApi.remove(record.id);
          message.success('删除成功');
          reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        }
      },
    });
  };

  const columns: ColumnsType<DbzKafkaBrokerVo> = [
    {
      title: '名称',
      dataIndex: 'name',
      width: 150,
      render: (name: string, record) => (
        <Space direction="vertical" size={0}>
          <span style={{ fontWeight: 500 }}>{name}</span>
          <span className="dbz-mono" style={{ color: 'rgba(0,0,0,0.45)' }}>
            #{record.id}
          </span>
        </Space>
      ),
    },
    {
      title: 'Broker 地址',
      dataIndex: 'bootstrapServers',
      width: 200,
      render: (servers: string) => (
        <Tooltip title={servers}>
          <span className="dbz-mono">{orDash(servers)}</span>
        </Tooltip>
      ),
      ellipsis: true,
    },
    {
      title: '安全协议',
      dataIndex: 'securityProtocol',
      width: 150,
      render: (value: number) => (
        <StatusTag dic={kafkaSecurityProtocolDic} value={value} />
      ),
    },
    {
      title: '认证账号',
      dataIndex: 'saslUsername',
      width: 110,
      ellipsis: true,
      render: (username?: string | null) => orDash(username),
    },
    {
      title: '状态',
      dataIndex: 'runStatus',
      width: 100,
      render: (value: KafkaBrokerStatus, record) => (
        <StatusTag
          dic={kafkaBrokerStatusDic}
          value={value}
          detail={statusDetailOf(record)}
        />
      ),
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      width: 80,
      render: (enabled: boolean, record) => (
        <EnabledSwitch
          enabled={enabled}
          name={record.name}
          confirmContent="禁用后这台集群不参与健康检查，挂在它上面的连接器发不了 Kafka 信号；重新启用后由下一轮检查重新判定状态。"
          onToggle={async (next) => {
            await kafkaBrokerApi.modify({ id: record.id, enabled: next });
            reload();
          }}
        />
      ),
    },
    {
      title: '备注',
      dataIndex: 'remark',
      ellipsis: true,
      render: (remark?: string | null) => orDash(remark),
    },
    {
      title: '操作',
      key: 'actions',
      width: 180,
      fixed: 'right',
      render: (_, record) => (
        <Space size={4}>
          <Button
            type="link"
            size="small"
            icon={<ThunderboltOutlined />}
            loading={checking === record.id}
            onClick={() => handleCheck(record)}
          >
            检测
          </Button>
          <Button
            type="link"
            size="small"
            onClick={() => {
              setEditing(record);
              setFormOpen(true);
            }}
          >
            编辑
          </Button>
          <Button type="link" size="small" danger onClick={() => handleDelete(record)}>
            删除
          </Button>
        </Space>
      ),
    },
  ];

  return (
    <Card variant="borderless">
      <div className="dbz-toolbar">
        <Input.Search
          allowClear
          placeholder="搜索名称 / 地址 / 备注"
          style={{ width: 260 }}
          value={draft.keyword ?? ''}
          onChange={(event) => setDraft((prev) => ({ ...prev, keyword: event.target.value }))}
          onSearch={() => applyFilters(draft)}
        />
        <Select
          allowClear
          placeholder="状态"
          style={{ width: 130 }}
          options={dicOptions(kafkaBrokerStatusDic)}
          value={draft.runStatus}
          onChange={(value) => {
            const next = { ...draft, runStatus: value };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Select
          allowClear
          placeholder="启用"
          style={{ width: 110 }}
          options={[
            { value: true, label: '启用' },
            { value: false, label: '禁用' },
          ]}
          value={draft.enabled}
          onChange={(value) => {
            const next = { ...draft, enabled: value };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Button
          onClick={() => {
            const next: FilterDraft = {};
            setDraft(next);
            applyFilters(next);
          }}
        >
          重置
        </Button>

        <span className="dbz-toolbar-spacer" />

        <Button icon={<ReloadOutlined />} onClick={reload}>
          刷新
        </Button>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => {
            setEditing(null);
            setFormOpen(true);
          }}
        >
          新增集群
        </Button>
      </div>

      {error && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="加载 Kafka 集群列表失败"
          description={error.message}
        />
      )}

      <Table<DbzKafkaBrokerVo>
        rowKey="id"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data.list}
        scroll={{ x: 1120 }}
        pagination={{
          current: pageNum,
          pageSize,
          total: data.total,
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 条`,
          onChange: (page, size) => {
            if (size !== pageSize) {
              setPageSize(size);
            } else {
              setPageNum(page);
            }
          },
        }}
      />

      <KafkaBrokerForm
        open={formOpen}
        record={editing}
        onCancel={() => setFormOpen(false)}
        onSuccess={() => {
          setFormOpen(false);
          reload();
        }}
      />
    </Card>
  );
}
