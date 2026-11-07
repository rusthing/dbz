/**
 * # 死信列表
 *
 * 死信由 magic 脚本在失败分支显式上报，不由 Kafka Connect 的 DLQ 产生：
 * HttpSink 对 HTTP 4xx/5xx 只记日志并照常提交偏移量，Connect 层兜不住任何东西。
 * 页面只做四件事：看报文与失败原因、改报文后重投（仅确定性字段不合规类）、
 * 按原报文重投、废弃不再处理。
 *
 * 从控制面连接器卡片进来时带 `connectorType` / `connectorId`，筛选器会预先选中。
 */

import {
  Alert,
  App as AntdApp,
  Button,
  Card,
  Col,
  DatePicker,
  Drawer,
  Empty,
  Input,
  Row,
  Select,
  Space,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import {
  CheckCircleOutlined,
  DeleteOutlined,
  EditOutlined,
  RedoOutlined,
  ReloadOutlined,
  SaveOutlined,
  UndoOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import type { Dayjs } from 'dayjs';
import { useCallback, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { deadLetterApi } from '../../api/dead-letter';
import { sinkConnectorApi } from '../../api/sink-connector';
import { sourceConnectorApi } from '../../api/source-connector';
import type {
  DbzDeadLetterRetryResultVo,
  DbzDeadLetterVo,
  DeadLetterConnectorType,
  DeadLetterFailureCategory,
  DeadLetterStatus,
  Id,
} from '../../api/types';
import StatusTag from '../../components/StatusTag';
import {
  deadLetterConnectorTypeDic,
  deadLetterFailureCategoryDic,
  deadLetterFailureSubtypeDic,
  deadLetterStatusDic,
  dicOptions,
} from '../../dic';
import { useAsync } from '../../hooks/useAsync';
import { usePagedQuery } from '../../hooks/usePagedQuery';
import { errorMessage, formatMs, orDash, shortId } from '../../utils/format';

/** 待处理状态：角标与默认筛选都用它 */
const STATUS_PENDING: DeadLetterStatus = 0;

/** 筛选草稿 */
interface FilterDraft {
  connectorType?: DeadLetterConnectorType;
  connectorId?: Id;
  status?: DeadLetterStatus;
  topic?: string;
  range?: [Dayjs, Dayjs] | null;
}

/** 连接器下拉项 */
interface ConnectorOption {
  value: Id;
  label: string;
  connectorType: DeadLetterConnectorType;
  connectorId: Id;
}

/** 解析地址栏里的初始筛选 */
function initialDraft(params: URLSearchParams): FilterDraft {
  const type = Number(params.get('connectorType'));
  const connectorType =
    type === 1 || type === 2 ? (type as DeadLetterConnectorType) : undefined;
  const connectorId = params.get('connectorId') ?? undefined;
  const status = params.get('status');
  const parsedStatus = status === null ? undefined : Number(status);
  return {
    connectorType,
    connectorId,
    status:
      parsedStatus === 0 || parsedStatus === 1 || parsedStatus === 2 || parsedStatus === 3
        ? (parsedStatus as DeadLetterStatus)
        : undefined,
  };
}

/** 把报文对象格式化成编辑框文本 */
function payloadText(value: unknown): string {
  return JSON.stringify(value ?? null, null, 2) ?? '';
}

/** 编辑框里的 JSON：只做前端可读校验，字段检查由服务端按连接器配置做 */
function parsePayloadDraft(text: string): { value?: unknown; error?: string } {
  if (!text.trim()) {
    return { error: '待投报文不能为空' };
  }
  try {
    return { value: JSON.parse(text) };
  } catch (reason) {
    return {
      error: `不是合法 JSON：${reason instanceof Error ? reason.message : String(reason)}`,
    };
  }
}

export default function DeadLetterPage() {
  const { message, modal } = AntdApp.useApp();
  const [searchParams] = useSearchParams();
  const [draft, setDraft] = useState<FilterDraft>(() => initialDraft(searchParams));
  const [detail, setDetail] = useState<DbzDeadLetterVo | null>(null);
  const [payloadDraft, setPayloadDraft] = useState('');
  const [payloadError, setPayloadError] = useState<string | undefined>(undefined);
  const [savingPayload, setSavingPayload] = useState(false);
  const [selectedIds, setSelectedIds] = useState<Id[]>([]);
  const [acting, setActing] = useState(false);

  /** 打开详情时把「待投报文（优先）或原始报文」放进编辑框 */
  const openDetail = useCallback((record: DbzDeadLetterVo) => {
    setDetail(record);
    setPayloadDraft(payloadText(record.pendingPayload ?? record.payload));
    setPayloadError(undefined);
  }, []);

  /** 上游/下游连接器都取一次，id → 名称与筛选下拉共用 */
  const connectors = useAsync<ConnectorOption[]>(async () => {
    const [sources, sinks] = await Promise.all([
      sourceConnectorApi.list(),
      sinkConnectorApi.list(),
    ]);
    const sourceOptions: ConnectorOption[] = sources.map((item) => ({
      value: item.id,
      label: `上游 · ${item.name}`,
      connectorType: 1,
      connectorId: item.id,
    }));
    const sinkOptions: ConnectorOption[] = sinks.map((item) => ({
      value: item.id,
      label: `下游 · ${item.name}`,
      connectorType: 2,
      connectorId: item.id,
    }));
    return [...sourceOptions, ...sinkOptions];
  }, [], []);

  const { data, loading, error, pageNum, pageSize, search, reload, setPageNum, setPageSize } =
    usePagedQuery<DbzDeadLetterVo>((params) => deadLetterApi.page(params));

  /** 名称查找：连接器 id 在不同表里可能重复，必须带方向 */
  const nameOf = useCallback(
    (connectorType: DeadLetterConnectorType, connectorId: Id): string => {
      const option = connectors.data.find(
        (item) => item.connectorType === connectorType && item.connectorId === connectorId,
      );
      return option?.label ?? `${connectorType === 1 ? '上游' : '下游'} #${shortId(connectorId)}`;
    },
    [connectors.data],
  );

  const applyFilters = useCallback(
    (next: FilterDraft) => {
      setSelectedIds([]);
      search({
        connectorType: next.connectorType,
        connectorId: next.connectorId,
        status: next.status,
        topic: next.topic?.trim() || undefined,
        startMs: next.range?.[0] ? next.range[0].valueOf() : undefined,
        endMs: next.range?.[1] ? next.range[1].valueOf() : undefined,
      });
    },
    [search],
  );

  /** 按方向过滤下拉项：没选方向时两边的连接器都能选 */
  const connectorOptions = useMemo(
    () =>
      connectors.data
        .filter((item) => !draft.connectorType || item.connectorType === draft.connectorType)
        .map((item) => ({ value: item.value, label: item.label })),
    [connectors.data, draft.connectorType],
  );

  /** 重投：不传 payload 按原始报文；传 payload 表示用修改后的待投报文（仅单条） */
  const retryRecords = async (records: DbzDeadLetterVo[], payload?: string) => {
    if (records.length === 0) {
      return;
    }
    setActing(true);
    try {
      const results = await deadLetterApi.retry({
        ids: records.map((item) => item.id),
        ...(payload ? { payload } : {}),
      });
      applyRetryResults(results);
      setSelectedIds([]);
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setActing(false);
    }
  };

  /** 汇总逐条结果，并把单条详情上的最近重投结果同步刷新 */
  const applyRetryResults = (results: DbzDeadLetterRetryResultVo[]) => {
    const ok = results.filter((item) => item.success).length;
    const failed = results.filter((item) => !item.success);
    if (ok > 0) {
      message.success(`${ok} 条重投成功，已转为「已重投」`);
    }
    if (failed.length > 0) {
      modal.warning({
        title: `${failed.length} 条重投失败`,
        content: (
          <div className="dbz-status-detail">
            {failed.slice(0, 5).map((item) => (
              <div key={item.id} className="dbz-status-row">
                <span className="dbz-mono">#{shortId(item.id)}</span>
                <span className="dbz-status-row-value">{item.message}</span>
              </div>
            ))}
            {failed.length > 5 && <div>……其余 {failed.length - 5} 条见服务端日志</div>}
          </div>
        ),
      });
    }

    const result = results.length === 1 ? results[0] : undefined;
    if (result) {
      setDetail((prev) =>
        prev && prev.id === result.id
          ? {
              ...prev,
              status: result.success ? 1 : prev.status,
              lastRetrySuccess: result.success,
              lastRetryMs: String(Date.now()),
              lastRetryMessage: result.message,
            }
          : prev,
      );
    }
  };

  /** 保存待投报文：只写 pending_payload 与编辑审计，不覆盖原始报文 */
  const savePendingPayload = async () => {
    if (!detail) {
      return;
    }
    const parsed = parsePayloadDraft(payloadDraft);
    if (parsed.error) {
      setPayloadError(parsed.error);
      return;
    }
    setPayloadError(undefined);
    setSavingPayload(true);
    try {
      const updated = await deadLetterApi.editPayload({
        id: detail.id,
        payload: payloadDraft,
      });
      setDetail(updated);
      setPayloadDraft(payloadText(updated.pendingPayload ?? updated.payload));
      message.success('待投报文已保存，原始报文未被覆盖');
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setSavingPayload(false);
    }
  };

  /** 用编辑框里的报文直接重投 */
  const confirmRetryEdited = () => {
    if (!detail) {
      return;
    }
    const parsed = parsePayloadDraft(payloadDraft);
    if (parsed.error) {
      setPayloadError(parsed.error);
      return;
    }
    setPayloadError(undefined);
    modal.confirm({
      title: '用修改后的报文重投 1 条死信？',
      content:
        '待投报文会另外保存，原始报文不会被覆盖；对端返回 HTTP 2xx 且 result=1 才转为「已重投」，失败原因会回写到本条死信。',
      okText: '改后重投',
      cancelText: '取消',
      onOk: () => retryRecords([detail], payloadDraft),
    });
  };

  const discardRecords = (records: DbzDeadLetterVo[]) => {
    if (records.length === 0) {
      return;
    }
    modal.confirm({
      title: `废弃 ${records.length} 条死信？`,
      content: '废弃只改状态并记录处理时间，不删除报文；之后仍可在筛选「已废弃」时查看。',
      okText: '废弃',
      okButtonProps: { danger: true },
      cancelText: '取消',
      onOk: async () => {
        setActing(true);
        try {
          await deadLetterApi.discard({ ids: records.map((item) => item.id) });
          message.success(`已废弃 ${records.length} 条死信`);
          setSelectedIds([]);
          reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        } finally {
          setActing(false);
        }
      },
    });
  };

  const confirmRetry = (records: DbzDeadLetterVo[]) => {
    if (records.length === 0) {
      return;
    }
    modal.confirm({
      title: `重投 ${records.length} 条死信？`,
      content:
        '会把原始报文重新 POST 到下游连接器配置里的 magic 地址；对端返回 HTTP 2xx 且 result=1 才转为「已重投」，否则保持「待处理」。',
      okText: '重投',
      cancelText: '取消',
      onOk: () => retryRecords(records),
    });
  };

  const columns: ColumnsType<DbzDeadLetterVo> = [
    {
      title: '状态',
      dataIndex: 'status',
      width: 100,
      render: (value: DeadLetterStatus) => <StatusTag dic={deadLetterStatusDic} value={value} />,
    },
    {
      title: '连接器',
      key: 'connector',
      width: 190,
      ellipsis: true,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <span className="dbz-ellipsis" title={nameOf(record.connectorType, record.connectorId)}>
            {nameOf(record.connectorType, record.connectorId)}
          </span>
          <span className="dbz-mono dbz-id">#{shortId(record.connectorId)}</span>
        </Space>
      ),
    },
    {
      title: '失败分类',
      dataIndex: 'failureCategory',
      width: 150,
      render: (value: DeadLetterFailureCategory, record) => (
        <Space size={4} wrap>
          <StatusTag dic={deadLetterFailureCategoryDic} value={value} />
          {record.failureSubtype === 1 && (
            <Tag color="orange" bordered={false}>
              字段不合规
            </Tag>
          )}
        </Space>
      ),
    },
    {
      title: 'Topic',
      dataIndex: 'topic',
      width: 170,
      ellipsis: true,
      render: (topic: string) => (
        <Tooltip title={topic}>
          <span className="dbz-mono">{orDash(topic)}</span>
        </Tooltip>
      ),
    },
    {
      title: '记录主键',
      dataIndex: 'recordKey',
      width: 150,
      ellipsis: true,
      render: (recordKey: string) => (
        <Tooltip title={recordKey}>
          <span className="dbz-mono">{orDash(recordKey)}</span>
        </Tooltip>
      ),
    },
    {
      title: '失败原因',
      dataIndex: 'reason',
      ellipsis: true,
      render: (reason: string) => (
        <Tooltip title={reason}>
          <span>{orDash(reason)}</span>
        </Tooltip>
      ),
    },
    {
      title: '次数',
      dataIndex: 'retryCount',
      width: 76,
      className: 'dbz-num',
      render: (value: number) => value,
    },
    {
      title: '末次失败',
      dataIndex: 'lastSeenMs',
      width: 160,
      className: 'dbz-num',
      render: (ms: string) => formatMs(ms),
    },
    {
      title: '最近重投',
      key: 'lastRetry',
      width: 160,
      render: (_, record) =>
        record.lastRetryMs ? (
          <Tooltip title={record.lastRetryMessage ?? ''}>
            <Tag color={record.lastRetrySuccess ? 'success' : 'error'} bordered={false}>
              {record.lastRetrySuccess ? '成功' : '失败'} · {formatMs(record.lastRetryMs)}
            </Tag>
          </Tooltip>
        ) : (
          <Typography.Text type="secondary">未重投</Typography.Text>
        ),
    },
    {
      title: '操作',
      key: 'actions',
      width: 150,
      fixed: 'right',
      render: (_, record) => (
        <Space size={4}>
          <Button type="link" size="small" onClick={() => openDetail(record)}>
            详情
          </Button>
          {record.status !== 1 && record.status !== 2 && (
            <Button
              type="link"
              size="small"
              icon={<RedoOutlined />}
              onClick={() => confirmRetry([record])}
            >
              重投
            </Button>
          )}
          {record.status === STATUS_PENDING && (
            <Button type="link" size="small" danger onClick={() => discardRecords([record])}>
              废弃
            </Button>
          )}
        </Space>
      ),
    },
  ];

  const selectedRecords = data.list.filter((item) => selectedIds.includes(item.id));

  return (
    <Card variant="borderless">
      <div className="dbz-toolbar">
        <Select
          allowClear
          placeholder="方向"
          style={{ width: 100 }}
          options={dicOptions(deadLetterConnectorTypeDic)}
          value={draft.connectorType}
          onChange={(value: DeadLetterConnectorType | undefined) => {
            const next = { ...draft, connectorType: value, connectorId: undefined };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Select
          allowClear
          showSearch
          optionFilterProp="label"
          placeholder="连接器"
          style={{ width: 200 }}
          options={connectorOptions}
          loading={connectors.loading}
          notFoundContent={
            connectors.error
              ? '连接器列表加载失败，请刷新重试'
              : connectors.loading
                ? '加载中…'
                : '暂无连接器'
          }
          value={draft.connectorId}
          onChange={(value: Id | undefined) => {
            const option = connectors.data.find((item) => item.value === value);
            const next: FilterDraft = {
              ...draft,
              connectorId: value,
              connectorType: option?.connectorType ?? draft.connectorType,
            };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Select
          allowClear
          placeholder="状态"
          style={{ width: 120 }}
          options={dicOptions(deadLetterStatusDic)}
          value={draft.status}
          onChange={(value: DeadLetterStatus | undefined) => {
            const next = { ...draft, status: value };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <Input.Search
          allowClear
          placeholder="topic 精确匹配"
          style={{ width: 180 }}
          value={draft.topic ?? ''}
          onChange={(event) => setDraft((prev) => ({ ...prev, topic: event.target.value }))}
          onSearch={() => applyFilters(draft)}
        />
        <DatePicker.RangePicker
          showTime
          style={{ width: 330 }}
          value={draft.range ?? null}
          onChange={(value) => {
            const next = { ...draft, range: value as [Dayjs, Dayjs] | null };
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
          icon={<RedoOutlined />}
          disabled={selectedRecords.length === 0}
          loading={acting}
          onClick={() => confirmRetry(selectedRecords)}
        >
          重投选中
        </Button>
        <Button
          danger
          icon={<DeleteOutlined />}
          disabled={selectedRecords.length === 0}
          loading={acting}
          onClick={() => discardRecords(selectedRecords)}
        >
          废弃选中
        </Button>
        <Button icon={<ReloadOutlined />} onClick={reload}>
          刷新
        </Button>
      </div>

      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="死信由 magic 脚本显式上报，不依赖 Kafka Connect 的 DLQ"
        description="HttpSink 对 HTTP 4xx/5xx 只记日志并照常提交偏移量，Connect 层不会重投；传输类失败的重试由 dbz 后台 worker 按 1s→5s→30s→2m→10m 执行，超过 10 次才进死信。"
      />

      {error && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          message="加载死信失败"
          description={errorMessage(error)}
          action={
            <Button size="small" onClick={reload}>
              重试
            </Button>
          }
        />
      )}

      <Table<DbzDeadLetterVo>
        rowKey="id"
        size="middle"
        loading={loading}
        columns={columns}
        dataSource={data.list}
        rowSelection={{
          selectedRowKeys: selectedIds,
          onChange: (keys) => setSelectedIds(keys as Id[]),
          getCheckboxProps: (record) => ({ disabled: record.status === 1 || record.status === 2 }),
        }}
        locale={{ emptyText: loading ? '加载中…' : '暂无死信记录' }}
        scroll={{ x: 1400 }}
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

      <Drawer
        open={Boolean(detail)}
        title={detail ? `死信详情 · #${shortId(detail.id)}` : '死信详情'}
        width="min(760px, 94vw)"
        onClose={() => setDetail(null)}
        extra={
          detail && (
            <Space>
              {detail.status !== 1 && detail.status !== 2 && (
                <Button
                  icon={<RedoOutlined />}
                  loading={acting}
                  onClick={() => confirmRetry([detail])}
                >
                  按原报文重投
                </Button>
              )}
              {detail.status === STATUS_PENDING && (
                <Button danger icon={<DeleteOutlined />} loading={acting} onClick={() => discardRecords([detail])}>
                  废弃
                </Button>
              )}
            </Space>
          )
        }
      >
        {detail && (
          <div className="dbz-status-detail">
            {detail.lastRetryMs && (
              <Alert
                showIcon
                style={{ marginBottom: 8 }}
                type={detail.lastRetrySuccess ? 'success' : 'error'}
                message={detail.lastRetrySuccess ? '最近一次重投成功' : '最近一次重投仍失败'}
                description={`${detail.lastRetryMessage ?? ''}（${formatMs(detail.lastRetryMs)}）`}
              />
            )}
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">状态</span>
              <span className="dbz-status-row-value">
                <StatusTag dic={deadLetterStatusDic} value={detail.status} />
              </span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">连接器</span>
              <span className="dbz-status-row-value">
                {nameOf(detail.connectorType, detail.connectorId)}
                <span className="dbz-mono dbz-id"> #{shortId(detail.connectorId)}</span>
              </span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">方向</span>
              <span className="dbz-status-row-value">
                <StatusTag dic={deadLetterConnectorTypeDic} value={detail.connectorType} />
              </span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">失败分类</span>
              <span className="dbz-status-row-value">
                <Space size={6} wrap>
                  <StatusTag dic={deadLetterFailureCategoryDic} value={detail.failureCategory} />
                  <StatusTag dic={deadLetterFailureSubtypeDic} value={detail.failureSubtype} />
                </Space>
              </span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">Topic</span>
              <span className="dbz-status-row-value dbz-mono">{orDash(detail.topic)}</span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">记录主键</span>
              <span className="dbz-status-row-value dbz-mono">{orDash(detail.recordKey)}</span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">失败次数</span>
              <span className="dbz-status-row-value">{detail.retryCount}</span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">首次失败</span>
              <span className="dbz-status-row-value">{formatMs(detail.firstSeenMs)}</span>
            </div>
            <div className="dbz-status-row">
              <span className="dbz-status-row-label">末次失败</span>
              <span className="dbz-status-row-value">{formatMs(detail.lastSeenMs)}</span>
            </div>
            {detail.handledMs && (
              <div className="dbz-status-row">
                <span className="dbz-status-row-label">处理时间</span>
                <span className="dbz-status-row-value">{formatMs(detail.handledMs)}</span>
              </div>
            )}
            {detail.remark && (
              <div className="dbz-status-row">
                <span className="dbz-status-row-label">备注</span>
                <span className="dbz-status-row-value">{detail.remark}</span>
              </div>
            )}

            <Typography.Title level={5} style={{ marginTop: 16 }}>
              失败原因
            </Typography.Title>
            <Typography.Text strong>{orDash(detail.reason)}</Typography.Text>

            <Typography.Title level={5} style={{ marginTop: 16 }}>
              失败详情
            </Typography.Title>
            {detail.failureDetail ? (
              <Typography.Paragraph
                className="dbz-status-reason"
                style={{ marginBottom: 0, whiteSpace: 'pre-wrap' }}
                copyable={{
                  text: detail.failureDetail,
                  tooltips: ['复制失败详情', '已复制'],
                }}
                ellipsis={{ rows: 6, expandable: true, symbol: '展开完整详情' }}
              >
                {detail.failureDetail}
              </Typography.Paragraph>
            ) : (
              <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="没有失败详情" />
            )}

            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={detail.canEditPayload ? 12 : 24}>
                <Typography.Title level={5}>原始报文（永不覆盖）</Typography.Title>
                <pre className="dbz-json-view">{payloadText(detail.payload)}</pre>
              </Col>
              {detail.canEditPayload && (
                <Col span={12}>
                  <Typography.Title level={5}>已保存的待投报文</Typography.Title>
                  {detail.pendingPayload ? (
                    <>
                      <div className="dbz-payload-meta">
                        <span>
                          编辑人
                          {detail.payloadEditedBy
                            ? ` #${shortId(detail.payloadEditedBy)}`
                            : '未知'}
                        </span>
                        <span>
                          {detail.payloadEditedMs ? formatMs(detail.payloadEditedMs) : ''}
                        </span>
                      </div>
                      <pre className="dbz-json-view">
                        {payloadText(detail.pendingPayload)}
                      </pre>
                    </>
                  ) : (
                    <Empty
                      image={Empty.PRESENTED_IMAGE_SIMPLE}
                      description="还没有保存过待投报文，下面是按当前编辑框提交"
                    />
                  )}
                </Col>
              )}
            </Row>

            {detail.canEditPayload ? (
              <>
                <Alert
                  type="info"
                  showIcon
                  style={{ marginTop: 16 }}
                  message="修改报文与校验说明"
                  description="提交前会校验 JSON 合法性与结构；若下游连接器配置声明 pk.fields，服务端还会检查主键字段必填。目标表的列类型没有写在连接器配置里，无法离线做完整类型校验。"
                />
                <Typography.Title level={5} style={{ marginTop: 16 }}>
                  <EditOutlined /> 编辑待投报文
                </Typography.Title>
                <Input.TextArea
                  className="dbz-json-editor"
                  value={payloadDraft}
                  status={payloadError ? 'error' : undefined}
                  autoSize={{ minRows: 10, maxRows: 18 }}
                  onChange={(event) => {
                    setPayloadDraft(event.target.value);
                    setPayloadError(undefined);
                  }}
                />
                {payloadError && (
                  <Alert
                    type="error"
                    showIcon
                    style={{ marginTop: 8 }}
                    message="报文校验未通过"
                    description={payloadError}
                  />
                )}
                <Space style={{ marginTop: 12 }} wrap>
                  <Button
                    icon={<SaveOutlined />}
                    loading={savingPayload}
                    onClick={savePendingPayload}
                  >
                    保存待投
                  </Button>
                  <Button
                    type="primary"
                    icon={<RedoOutlined />}
                    loading={acting}
                    disabled={detail.status === 1 || detail.status === 2}
                    onClick={confirmRetryEdited}
                  >
                    改后重投
                  </Button>
                  <Button
                    icon={<UndoOutlined />}
                    onClick={() => {
                      setPayloadDraft(payloadText(detail.pendingPayload ?? detail.payload));
                      setPayloadError(undefined);
                    }}
                  >
                    恢复
                  </Button>
                </Space>
              </>
            ) : (
              <Alert
                type="warning"
                showIcon
                icon={<WarningOutlined />}
                style={{ marginTop: 16 }}
                message="该记录不允许修改报文"
                description="只有「确定性失败 + 字段不合规」的死信可以改报文重投；传输类、响应异常类以及唯一键冲突等其他确定性失败只能按原报文重投或废弃。"
              />
            )}

            {(detail.status === 1 || detail.status === 2) && (
              <Tag
                color={detail.status === 1 ? 'success' : 'default'}
                icon={<CheckCircleOutlined />}
                style={{ marginTop: 12 }}
              >
                {detail.status === 1 ? '已重投，无需再处理' : '已废弃，仅保留追溯'}
              </Tag>
            )}
          </div>
        )}
      </Drawer>
    </Card>
  );
}
