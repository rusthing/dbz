/**
 * # Kafka Connect 集群管理
 *
 * 集群是连接器实际运行的地方，也是健康检查的入口：每轮检查逐台探测连通性，再检查名下
 * 已启用的上游/下游连接器。因此「地址」与「启用」这两个字段的改动会牵动集群上的部署，
 * 相关提示写在表单里。
 */

import { App as AntdApp, Alert, Button, Card, Input, Select, Space, Table, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { useCallback, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { kafkaConnectApi } from '../../api/kafka-connect';
import type { DbzKafkaConnectVo, KafkaConnectStatus } from '../../api/types';
import EnabledTag from '../../components/EnabledTag';
import StatusTag from '../../components/StatusTag';
import { dicOptions, kafkaConnectStatusDic } from '../../dic';
import { usePagedQuery } from '../../hooks/usePagedQuery';
import { errorMessage, formatMs, orDash } from '../../utils/format';
import KafkaConnectForm from './KafkaConnectForm';

/** 筛选表单的草稿状态 */
interface FilterDraft {
  keyword?: string;
  runStatus?: KafkaConnectStatus;
  enabled?: boolean;
}

export default function KafkaConnectPage() {
  const { message, modal } = AntdApp.useApp();
  const navigate = useNavigate();
  const [draft, setDraft] = useState<FilterDraft>({});
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<DbzKafkaConnectVo | null>(null);

  const { data, loading, error, pageNum, pageSize, search, reload, setPageNum, setPageSize } =
    usePagedQuery<DbzKafkaConnectVo>((params) => kafkaConnectApi.page(params));

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

  const handleDelete = (record: DbzKafkaConnectVo) => {
    modal.confirm({
      title: `删除集群「${record.name}」？`,
      content: '名下仍有连接器时服务端会拒绝删除，请先删除或改挂这些连接器。',
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await kafkaConnectApi.remove(record.id);
          message.success('删除成功');
          reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        }
      },
    });
  };

  const columns: ColumnsType<DbzKafkaConnectVo> = [
    {
      title: '名称',
      dataIndex: 'name',
      width: 170,
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
      title: '地址',
      dataIndex: 'url',
      width: 230,
      ellipsis: true,
      render: (url: string) => (
        <Tooltip title={url}>
          <span className="dbz-mono">{orDash(url)}</span>
        </Tooltip>
      ),
    },
    {
      title: '状态',
      dataIndex: 'runStatus',
      width: 110,
      render: (value: KafkaConnectStatus) => <StatusTag dic={kafkaConnectStatusDic} value={value} />,
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      width: 70,
      render: (enabled: boolean) => <EnabledTag enabled={enabled} />,
    },
    {
      title: '备注',
      dataIndex: 'remark',
      ellipsis: true,
      render: (remark?: string | null) => orDash(remark),
    },
    {
      title: '更新时间',
      dataIndex: 'updateMs',
      width: 160,
      render: (ms: number) => formatMs(ms),
    },
    {
      title: '操作',
      key: 'actions',
      width: 190,
      fixed: 'right',
      render: (_, record) => (
        <Space size={4}>
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
          <Button
            type="link"
            size="small"
            onClick={() => navigate(`/remote-connector?kafkaConnectId=${record.id}`)}
          >
            连接器
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
          options={dicOptions(kafkaConnectStatusDic)}
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
          message="加载集群列表失败"
          description={error.message}
        />
      )}

      <Table<DbzKafkaConnectVo>
        rowKey="id"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data.list}
        scroll={{ x: 1080 }}
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

      <KafkaConnectForm
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
