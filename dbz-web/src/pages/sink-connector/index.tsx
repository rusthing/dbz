/**
 * # 下游连接器管理
 */

import { App as AntdApp, Alert, Button, Card, Drawer, Input, Select, Space, Table, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { useCallback, useState } from 'react';
import { sinkConnectorApi } from '../../api/sink-connector';
import type { ConnectorRunStatus, DbzSinkConnectorVo, Id } from '../../api/types';
import EnabledTag from '../../components/EnabledTag';
import StatusTag from '../../components/StatusTag';
import { connectorRunStatusDic, dicOptions } from '../../dic';
import { useClusterOptions, useSourceOptions } from '../../hooks/useOptions';
import { usePagedQuery } from '../../hooks/usePagedQuery';
import { errorMessage, formatMs, orDash, prettyJson } from '../../utils/format';
import SinkConnectorForm from './SinkConnectorForm';

/** 筛选草稿 */
interface FilterDraft {
  keyword?: string;
  sourceConnectorId?: Id;
  kafkaConnectId?: Id;
  runStatus?: ConnectorRunStatus;
  enabled?: boolean;
}

export default function SinkConnectorPage() {
  const { message, modal } = AntdApp.useApp();
  const cluster = useClusterOptions();
  const source = useSourceOptions();
  const [draft, setDraft] = useState<FilterDraft>({});
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<DbzSinkConnectorVo | null>(null);
  const [preview, setPreview] = useState<DbzSinkConnectorVo | null>(null);

  const { data, loading, error, pageNum, pageSize, search, reload, setPageNum, setPageSize } =
    usePagedQuery<DbzSinkConnectorVo>((params) => sinkConnectorApi.page(params));

  const applyFilters = useCallback(
    (next: FilterDraft) => {
      search({
        _keyword: next.keyword?.trim() || undefined,
        sourceConnectorId: next.sourceConnectorId,
        kafkaConnectId: next.kafkaConnectId,
        runStatus: next.runStatus,
        enabled: next.enabled,
      });
    },
    [search],
  );

  const handleDelete = (record: DbzSinkConnectorVo) => {
    modal.confirm({
      title: `删除下游连接器「${record.name}」？`,
      content: '删除记录不会自动注销集群上的连接器；若不再需要，请在「远端连接器」里一并处理。',
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await sinkConnectorApi.remove(record.id);
          message.success('删除成功');
          reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        }
      },
    });
  };

  const columns: ColumnsType<DbzSinkConnectorVo> = [
    {
      title: '名称',
      dataIndex: 'name',
      width: 160,
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
      title: '连接器名称',
      dataIndex: 'connectorName',
      width: 180,
      ellipsis: true,
      render: (connectorName: string) => (
        <Tooltip title={connectorName}>
          <span className="dbz-mono">{orDash(connectorName)}</span>
        </Tooltip>
      ),
    },
    {
      title: '上游连接器',
      dataIndex: 'sourceConnectorId',
      width: 130,
      ellipsis: true,
      render: (id: Id) => source.byId.get(id)?.name ?? `#${id}`,
    },
    {
      title: '运行集群',
      dataIndex: 'kafkaConnectId',
      width: 120,
      ellipsis: true,
      render: (id: Id) => cluster.byId.get(id)?.name ?? `#${id}`,
    },
    {
      title: '状态',
      dataIndex: 'runStatus',
      width: 120,
      render: (value: ConnectorRunStatus) => (
        <StatusTag dic={connectorRunStatusDic} value={value} />
      ),
    },
    {
      title: '启用',
      dataIndex: 'enabled',
      width: 70,
      render: (enabled: boolean) => <EnabledTag enabled={enabled} />,
    },
    {
      title: '更新时间',
      dataIndex: 'updateMs',
      width: 160,
      render: (ms: string) => formatMs(ms),
    },
    {
      title: '操作',
      key: 'actions',
      width: 170,
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
          <Button type="link" size="small" onClick={() => setPreview(record)}>
            配置
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
          placeholder="搜索名称 / 连接器名称 / 备注"
          style={{ width: 200 }}
          value={draft.keyword ?? ''}
          onChange={(event) => setDraft((prev) => ({ ...prev, keyword: event.target.value }))}
          onSearch={() => applyFilters(draft)}
        />
        <Select
          allowClear
          showSearch
          optionFilterProp="label"
          placeholder="上游连接器"
          style={{ width: 150 }}
          options={source.options}
          value={draft.sourceConnectorId}
          onChange={(value) => {
            const next = { ...draft, sourceConnectorId: value };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Select
          allowClear
          showSearch
          optionFilterProp="label"
          placeholder="运行集群"
          style={{ width: 140 }}
          options={cluster.options}
          value={draft.kafkaConnectId}
          onChange={(value) => {
            const next = { ...draft, kafkaConnectId: value };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Select
          allowClear
          placeholder="状态"
          style={{ width: 130 }}
          options={dicOptions(connectorRunStatusDic)}
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
          style={{ width: 100 }}
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
          新增下游连接器
        </Button>
      </div>

      {error && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="加载下游连接器失败"
          description={error.message}
        />
      )}

      <Table<DbzSinkConnectorVo>
        rowKey="id"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data.list}
        scroll={{ x: 1110 }}
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

      <SinkConnectorForm
        open={formOpen}
        record={editing}
        clusterOptions={cluster.options}
        sourceOptions={source.options}
        onCancel={() => setFormOpen(false)}
        onSuccess={() => {
          setFormOpen(false);
          reload();
        }}
      />

      <Drawer
        open={Boolean(preview)}
        title={preview ? `连接器配置 · ${preview.connectorName}` : '连接器配置'}
        width={640}
        onClose={() => setPreview(null)}
      >
        <pre className="dbz-json-view">{prettyJson(preview?.connectorConfig)}</pre>
      </Drawer>
    </Card>
  );
}
