/**
 * # 上游连接器管理
 *
 * 上游连接器 = Debezium source connector。列表里的「状态」是**检查任务的结论**（回写库），
 * 不是集群上的实时状态；要看待实时状态请到「远端连接器」页面。
 */

import { App as AntdApp, Alert, Button, Card, Drawer, Input, Select, Space, Table, Tooltip } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { PlusOutlined, ReloadOutlined } from '@ant-design/icons';
import { useCallback, useState } from 'react';
import { sourceConnectorApi } from '../../api/source-connector';
import type { ConnectorRunStatus, DbzSourceConnectorVo, Id } from '../../api/types';
import EnabledSwitch from '../../components/EnabledSwitch';
import StatusTag from '../../components/StatusTag';
import { connectorRunStatusDic, dicOptions } from '../../dic';
import { useClusterOptions } from '../../hooks/useOptions';
import { usePagedQuery } from '../../hooks/usePagedQuery';
import { errorMessage, formatMs, orDash, prettyJson, shortId } from '../../utils/format';
import SnapshotModal from './SnapshotModal';
import SourceConnectorForm from './SourceConnectorForm';

/** 筛选草稿 */
interface FilterDraft {
  keyword?: string;
  kafkaConnectId?: Id;
  runStatus?: ConnectorRunStatus;
  enabled?: boolean;
}

export default function SourceConnectorPage() {
  const { message, modal } = AntdApp.useApp();
  const cluster = useClusterOptions();
  const [draft, setDraft] = useState<FilterDraft>({});
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<DbzSourceConnectorVo | null>(null);
  const [preview, setPreview] = useState<DbzSourceConnectorVo | null>(null);
  // 不为空表示「重新同步」弹窗的目标记录
  const [snapshotTarget, setSnapshotTarget] = useState<DbzSourceConnectorVo | null>(null);
  // 开关与记录分开存：收起弹窗时只翻开关、记录留着，退出动画期间标题还是这条连接器；
  // 下次打开时整体换掉
  const [snapshotOpen, setSnapshotOpen] = useState(false);

  const { data, loading, error, pageNum, pageSize, search, reload, setPageNum, setPageSize } =
    usePagedQuery<DbzSourceConnectorVo>((params) => sourceConnectorApi.page(params));

  const applyFilters = useCallback(
    (next: FilterDraft) => {
      search({
        _keyword: next.keyword?.trim() || undefined,
        kafkaConnectId: next.kafkaConnectId,
        runStatus: next.runStatus,
        enabled: next.enabled,
      });
    },
    [search],
  );

  const handleDelete = (record: DbzSourceConnectorVo) => {
    modal.confirm({
      title: `删除上游连接器「${record.name}」？`,
      // 与 README「删除时先注销、再删记录」以及服务端 before_del 的行为保持一致
      content:
        '删除会先从集群注销该连接器，成功后再删除记录；注销失败则本次删除失败。若仍有下游挂载在本连接器上，服务端会拒绝删除，请先处理下游。',
      okText: '删除',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        try {
          await sourceConnectorApi.remove(record.id);
          message.success('删除成功');
          reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        }
      },
    });
  };

  const columns: ColumnsType<DbzSourceConnectorVo> = [
    {
      title: '名称',
      dataIndex: 'name',
      width: 150,
      render: (name: string, record) => (
        <Space direction="vertical" size={0}>
          <span className="dbz-ellipsis" style={{ fontWeight: 500 }} title={orDash(name)}>
            {orDash(name)}
          </span>
          <Tooltip title={`完整 ID：${record.id}`}>
            <span className="dbz-mono dbz-id" tabIndex={0}>
              #{shortId(record.id)}
            </span>
          </Tooltip>
        </Space>
      ),
    },
    {
      title: '连接器名称',
      dataIndex: 'connectorName',
      width: 170,
      ellipsis: true,
      render: (connectorName: string) => (
        <Tooltip title={connectorName}>
          <span className="dbz-mono">{orDash(connectorName)}</span>
        </Tooltip>
      ),
    },
    {
      title: '所属集群',
      dataIndex: 'kafkaConnectId',
      width: 120,
      ellipsis: true,
      render: (id: Id) =>
        cluster.byId.get(id)?.name ?? (id ? <span className="dbz-mono">#{shortId(id)}</span> : orDash(null)),
    },
    {
      title: '状态',
      dataIndex: 'runStatus',
      width: 110,
      render: (value: ConnectorRunStatus, record) => (
        <StatusTag
          dic={connectorRunStatusDic}
          value={value}
          detail={{
            summary: `运行详情 · ${record.name}`,
            rows: [
              { label: '连接器', value: <span className="dbz-mono">{orDash(record.connectorName)}</span> },
              {
                label: '所属集群',
                value: cluster.byId.get(record.kafkaConnectId)?.name ?? `#${shortId(record.kafkaConnectId)}`,
              },
              { label: '结论时间', value: formatMs(record.updateMs) },
            ],
            // 失败原因由健康检查写回：部署被拒的响应体、异常任务的 trace 首行都在这里
            reason: record.runMsg,
          }}
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
          confirmContent="禁用后该连接器不参与健康检查，并会先从集群上注销；重新启用时再当场部署回去。"
          onToggle={async (next) => {
            await sourceConnectorApi.modify({ id: record.id, enabled: next });
            reload();
          }}
        />
      ),
    },
    {
      title: '备注',
      dataIndex: 'remark',
      ellipsis: true,
      render: (remark?: string | null) => (
        <span className="dbz-ellipsis" title={orDash(remark)}>
          {orDash(remark)}
        </span>
      ),
    },
    {
      title: '更新时间',
      dataIndex: 'updateMs',
      width: 150,
      className: 'dbz-num',
      render: (ms: string) => formatMs(ms),
    },
    {
      title: '操作',
      key: 'actions',
      width: 230,
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
          <Button
            type="link"
            size="small"
            onClick={() => {
              setSnapshotTarget(record);
              setSnapshotOpen(true);
            }}
          >
            重新同步
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
          style={{ width: 220 }}
          value={draft.keyword ?? ''}
          onChange={(event) => setDraft((prev) => ({ ...prev, keyword: event.target.value }))}
          onSearch={() => applyFilters(draft)}
        />
        <Select
          allowClear
          showSearch
          optionFilterProp="label"
          placeholder="集群"
          style={{ width: 160 }}
          options={cluster.options}
          loading={cluster.loading}
          notFoundContent={
            cluster.error ? '集群列表加载失败，请刷新重试' : cluster.loading ? '加载中…' : '暂无集群'
          }
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
          style={{ width: 140 }}
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

        <Button
          icon={<ReloadOutlined />}
          onClick={() => {
            reload();
            cluster.reload();
          }}
        >
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
          新增上游连接器
        </Button>
      </div>

      {error && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="加载上游连接器失败"
          description={errorMessage(error)}
          action={
            <Button size="small" onClick={reload}>
              重试
            </Button>
          }
        />
      )}

      <Table<DbzSourceConnectorVo>
        rowKey="id"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data.list}
        locale={{ emptyText: loading ? '加载中…' : '暂无上游连接器' }}
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

      <SourceConnectorForm
        open={formOpen}
        record={editing}
        clusterOptions={cluster.options}
        onCancel={() => setFormOpen(false)}
        onSuccess={() => {
          setFormOpen(false);
          reload();
        }}
      />

      <SnapshotModal
        open={snapshotOpen}
        record={snapshotTarget}
        onClose={() => setSnapshotOpen(false)}
      />

      <Drawer
        open={Boolean(preview)}
        title={preview ? `连接器配置 · ${preview.connectorName}` : '连接器配置'}
        width="min(640px, 92vw)"
        onClose={() => setPreview(null)}
      >
        <pre className="dbz-json-view">{prettyJson(preview?.connectorConfig)}</pre>
      </Drawer>
    </Card>
  );
}
