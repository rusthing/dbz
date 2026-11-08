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
  Modal,
  Popover,
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
  CheckOutlined,
  CopyOutlined,
  DeleteOutlined,
  EditOutlined,
  ExportOutlined,
  InfoCircleOutlined,
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
import type { QueryParams } from '../../api/client';
import type {
  DbzDeadLetterRetryResultVo,
  DbzDeadLetterVo,
  DeadLetterConnectorType,
  DeadLetterFailureCategory,
  DeadLetterFailureSubtype,
  DeadLetterStatus,
  Id,
} from '../../api/types';
import OptionButtons from '../../components/OptionButtons';
import StatusTag from '../../components/StatusTag';
import {
  deadLetterFailureCategoryDic,
  deadLetterFailureSubtypeDic,
  deadLetterStatusDic,
} from '../../dic';
import { useAsync } from '../../hooks/useAsync';
import { usePagedQuery } from '../../hooks/usePagedQuery';
import { errorMessage, formatMs, orDash, shortId } from '../../utils/format';

/** 待处理状态：角标与默认筛选都用它 */
const STATUS_PENDING: DeadLetterStatus = 0;
/** 已人工处理：终态，可撤销，但不再重投或废弃 */
const STATUS_MANUAL_HANDLED: DeadLetterStatus = 3;
/** 死信只针对下游连接器 */
const CONNECTOR_TYPE_SINK: DeadLetterConnectorType = 2;

/** 筛选草稿（方向固定为下游，不再提供上游选项） */
interface FilterDraft {
  connectorId?: Id;
  status?: DeadLetterStatus;
  topic?: string;
  failureCategory?: DeadLetterFailureCategory;
  failureSubtype?: DeadLetterFailureSubtype;
  reason?: string;
  range?: [Dayjs, Dayjs] | null;
}

/** 连接器下拉项 */
interface ConnectorOption {
  value: Id;
  label: string;
  connectorId: Id;
}

/** 解析地址栏里的初始筛选；旧的 `connectorType=1` 深链一律按下游处理 */
function initialDraft(params: URLSearchParams): FilterDraft {
  const connectorId = params.get('connectorId') ?? undefined;
  const status = params.get('status');
  const parsedStatus = status === null ? undefined : Number(status);
  return {
    connectorId,
    status:
      parsedStatus === 0 || parsedStatus === 1 || parsedStatus === 2 || parsedStatus === 3
        ? (parsedStatus as DeadLetterStatus)
        : undefined,
  };
}

/** 把筛选草稿转成接口参数，导出与查询共用 */
function filterParams(draft: FilterDraft): QueryParams {
  return {
    connectorType: CONNECTOR_TYPE_SINK,
    connectorId: draft.connectorId,
    status: draft.status,
    topic: draft.topic?.trim() || undefined,
    failureCategory: draft.failureCategory,
    failureSubtype: draft.failureSubtype,
    reason: draft.reason?.trim() || undefined,
    startMs: draft.range?.[0] ? draft.range[0].valueOf() : undefined,
    endMs: draft.range?.[1] ? draft.range[1].valueOf() : undefined,
  };
}

/** CSV 单元格转义 */
function csvCell(value: unknown): string {
  const text = value === null || value === undefined ? '' : String(value);
  return `"${text.replace(/"/g, '""')}"`;
}

/** 把报文对象格式化成编辑框文本 */
function payloadText(value: unknown): string {
  return JSON.stringify(value ?? null, null, 2) ?? '';
}

/** 列表里的原始报文只做单行预览，完整内容由悬停提示给出，避免超长报文撑开表格 */
function payloadInline(value: unknown): string {
  return JSON.stringify(value ?? null) ?? '';
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
  const [applied, setApplied] = useState<FilterDraft>(() => initialDraft(searchParams));
  const [exporting, setExporting] = useState(false);
  const [handledOpen, setHandledOpen] = useState(false);
  const [handledTargets, setHandledTargets] = useState<DbzDeadLetterVo[]>([]);
  const [handledRemark, setHandledRemark] = useState('');

  /** 打开详情时把「待投报文（优先）或原始报文」放进编辑框 */
  const openDetail = useCallback((record: DbzDeadLetterVo) => {
    setDetail(record);
    setPayloadDraft(payloadText(record.pendingPayload ?? record.payload));
    setPayloadError(undefined);
  }, []);

  /** 死信只属于下游连接器，只取下游列表作为名称与筛选下拉 */
  const connectors = useAsync<ConnectorOption[]>(async () => {
    const sinks = await sinkConnectorApi.list();
    return sinks.map((item) => ({
      value: item.id,
      label: item.name,
      connectorId: item.id,
    }));
  }, [], []);

  const { data, loading, error, pageNum, pageSize, search, reload, setPageNum, setPageSize } =
    usePagedQuery<DbzDeadLetterVo>((params) => deadLetterApi.page(params));

  /** 名称查找：死信只有下游，id 直接对应下游连接器 */
  const nameOf = useCallback(
    (connectorId: Id): string => {
      const option = connectors.data.find((item) => item.connectorId === connectorId);
      return option?.label ?? `下游 #${shortId(connectorId)}`;
    },
    [connectors.data],
  );

  const applyFilters = useCallback(
    (next: FilterDraft) => {
      setSelectedIds([]);
      setApplied(next);
      search(filterParams(next));
    },
    [search],
  );

  const connectorOptions = useMemo(
    () => connectors.data.map((item) => ({ value: item.value, label: item.label })),
    [connectors.data],
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

  /** 打开「标记已人工处理」弹窗；处理说明必填，便于之后追溯 */
  const openMarkHandled = (records: DbzDeadLetterVo[]) => {
    if (records.length === 0) {
      return;
    }
    setHandledTargets(records);
    setHandledRemark('');
    setHandledOpen(true);
  };

  const submitMarkHandled = async () => {
    if (!handledRemark.trim()) {
      message.warning('请填写处理说明（在哪修的、怎么处理的）');
      return;
    }
    setActing(true);
    try {
      await deadLetterApi.markHandled({
        ids: handledTargets.map((item) => item.id),
        remark: handledRemark.trim(),
      });
      message.success(
        `已标记 ${handledTargets.length} 条为「已人工处理」；与「已废弃」的区别是问题已解决`,
      );
      setHandledOpen(false);
      setSelectedIds([]);
      setDetail(null);
      reload();
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setActing(false);
    }
  };

  /** 撤销人工处理：恢复为待处理，并清空处理人与说明 */
  const undoHandled = (records: DbzDeadLetterVo[]) => {
    if (records.length === 0) {
      return;
    }
    modal.confirm({
      title: `撤销 ${records.length} 条「已人工处理」？`,
      content: '撤销后恢复为「待处理」，处理人与处理说明会被清空，可重新重投或标记。',
      okText: '撤销',
      cancelText: '取消',
      onOk: async () => {
        setActing(true);
        try {
          await deadLetterApi.unmarkHandled({ ids: records.map((item) => item.id) });
          message.success(`已撤销 ${records.length} 条的人工处理结论`);
          setSelectedIds([]);
          setDetail(null);
          reload();
        } catch (reason) {
          message.error(errorMessage(reason));
        } finally {
          setActing(false);
        }
      },
    });
  };

  /** 导出当前筛选结果为 CSV：按页循环取全量，不导出未筛选的其它记录 */
  const exportCsv = async () => {
    setExporting(true);
    try {
      const list: DbzDeadLetterVo[] = [];
      let page = 1;
      for (;;) {
        const result = await deadLetterApi.page({
          ...filterParams(applied),
          _page: page,
          _size: 200,
        });
        list.push(...result.list);
        if (result.list.length === 0 || list.length >= result.total) {
          break;
        }
        page += 1;
      }

      const header = [
        '死信ID',
        '状态',
        '下游连接器',
        '连接器ID',
        'Topic',
        'Partition',
        'Offset',
        '记录主键',
        '失败分类',
        '失败子类',
        '失败原因',
        '失败详情',
        '失败次数',
        '首次失败',
        '末次失败',
        '最近重投',
        '处理人',
        '处理时间',
        '处理说明',
        '创建时间',
      ];
      const rows = list.map((item) => [
        item.id,
        deadLetterStatusDic.find((entry) => entry.value === item.status)?.label ?? item.status,
        nameOf(item.connectorId),
        item.connectorId,
        item.topic,
        item.partition ?? '',
        item.offset ?? '',
        item.recordKey,
        deadLetterFailureCategoryDic.find((entry) => entry.value === item.failureCategory)?.label ??
          item.failureCategory,
        deadLetterFailureSubtypeDic.find((entry) => entry.value === item.failureSubtype)?.label ??
          item.failureSubtype,
        item.reason,
        item.failureDetail,
        item.retryCount,
        formatMs(item.firstSeenMs),
        formatMs(item.lastSeenMs),
        item.lastRetryMs ? `${item.lastRetrySuccess ? '成功' : '失败'} ${formatMs(item.lastRetryMs)}` : '',
        item.handledBy ?? '',
        item.handledMs ? formatMs(item.handledMs) : '',
        item.remark ?? '',
        formatMs(item.createMs),
      ]);
      const csv = [header, ...rows].map((row) => row.map(csvCell).join(',')).join('\r\n');

      const blob = new Blob([`\ufeff${csv}`], { type: 'text/csv;charset=utf-8' });
      const link = document.createElement('a');
      link.href = URL.createObjectURL(blob);
      link.download = `dead-letter-${Date.now()}.csv`;
      link.click();
      URL.revokeObjectURL(link.href);
      message.success(`已导出 ${list.length} 条（当前筛选范围）`);
    } catch (reason) {
      message.error(errorMessage(reason));
    } finally {
      setExporting(false);
    }
  };

  /** 人工处理是终态：不再重投、不再废弃，只能撤销 */
  const canRetry = (record: DbzDeadLetterVo) =>
    record.status === STATUS_PENDING || record.status === 2;
  const isManualHandled = (record: DbzDeadLetterVo) =>
    record.status === STATUS_MANUAL_HANDLED;

  /** 复制完整原文：不做任何截断，失败时提示手动选择 */
  const copyFullText = async (text: string, label: string) => {
    try {
      await navigator.clipboard.writeText(text);
      message.success(`${label}已复制`);
    } catch {
      message.error('复制失败，请手动选择文本');
    }
  };

  /** 失败详情全文：列表弹层与详情页共用同一套展示与复制 */
  const failureDetailBlock = (text: string) => (
    <div className="dbz-failure-detail-popover">
      <Button size="small" icon={<CopyOutlined />} onClick={() => void copyFullText(text, '失败详情')}>
        复制
      </Button>
      <pre className="dbz-json-view">{text}</pre>
    </div>
  );

  const columns: ColumnsType<DbzDeadLetterVo> = [
    {
      title: '状态',
      dataIndex: 'status',
      width: 70,
      render: (value: DeadLetterStatus) => <StatusTag dic={deadLetterStatusDic} value={value} />,
    },
    {
      title: '连接器',
      key: 'connector',
      width: 215,
      ellipsis: true,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <span className="dbz-ellipsis" title={nameOf(record.connectorId)}>
            {nameOf(record.connectorId)}
          </span>
          <span className="dbz-mono dbz-id">#{shortId(record.connectorId)}</span>
        </Space>
      ),
    },
    {
      title: '失败分类',
      dataIndex: 'failureCategory',
      width: 84,
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
      title: '失败原因',
      dataIndex: 'reason',
      width: 125,
      ellipsis: true,
      render: (reason: string, record) => (
        <div className="dbz-reason-cell">
          {record.failureDetail && (
            <Popover
              trigger="click"
              placement="topLeft"
              title="失败详情（全文）"
              content={failureDetailBlock(record.failureDetail)}
            >
              <Button
                type="text"
                size="small"
                icon={<InfoCircleOutlined />}
                aria-label="查看失败详情"
              />
            </Popover>
          )}
          <Tooltip title={reason}>
            <span className="dbz-ellipsis">{orDash(reason)}</span>
          </Tooltip>
        </div>
      ),
    },
    {
      title: '原始报文',
      dataIndex: 'payload',
      width: 84,
      ellipsis: true,
      render: (payload: unknown) => (
        <Tooltip
          placement="topLeft"
          title={<span className="dbz-payload-hint">{payloadText(payload)}</span>}
        >
          <span className="dbz-mono dbz-ellipsis">{payloadInline(payload)}</span>
        </Tooltip>
      ),
    },
    {
      title: '次数',
      dataIndex: 'retryCount',
      width: 40,
      className: 'dbz-num',
      render: (value: number) => value,
    },
    {
      title: '末次失败',
      dataIndex: 'lastSeenMs',
      width: 130,
      className: 'dbz-num',
      render: (ms: string) => formatMs(ms),
    },
    {
      title: '最近重投',
      key: 'lastRetry',
      width: 120,
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
      width: 210,
      fixed: 'right',
      render: (_, record) => (
        <Space size={4}>
          <Button type="link" size="small" onClick={() => openDetail(record)}>
            详情
          </Button>
          {canRetry(record) && (
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
            <>
              <Button
                type="link"
                size="small"
                icon={<CheckOutlined />}
                onClick={() => openMarkHandled([record])}
              >
                已处理
              </Button>
              <Button type="link" size="small" danger onClick={() => discardRecords([record])}>
                废弃
              </Button>
            </>
          )}
          {isManualHandled(record) && (
            <Button
              type="link"
              size="small"
              icon={<UndoOutlined />}
              loading={acting}
              onClick={() => undoHandled([record])}
            >
              撤销处理
            </Button>
          )}
        </Space>
      ),
    },
  ];

  const selectedRecords = data.list.filter((item) => selectedIds.includes(item.id));
  const retryTargets = selectedRecords.filter(canRetry);
  const pendingTargets = selectedRecords.filter((item) => item.status === STATUS_PENDING);
  const manualTargets = selectedRecords.filter(isManualHandled);

  return (
    <Card variant="borderless">
      <div className="dbz-toolbar">
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
            const next: FilterDraft = { ...draft, connectorId: value };
            setDraft(next);
            applyFilters(next);
          }}
        />
        <div className="dbz-filter-group">
          <span className="dbz-filter-label">状态</span>
          <OptionButtons
            allowClear
            ariaLabel="死信状态"
            options={deadLetterStatusDic}
            value={draft.status}
            onChange={(value) => {
              const next = { ...draft, status: value };
              setDraft(next);
              applyFilters(next);
            }}
          />
        </div>
        <div className="dbz-filter-group">
          <span className="dbz-filter-label">失败分类</span>
          <OptionButtons
            allowClear
            ariaLabel="失败分类"
            options={deadLetterFailureCategoryDic}
            value={draft.failureCategory}
            onChange={(value) => {
              const next = { ...draft, failureCategory: value };
              setDraft(next);
              applyFilters(next);
            }}
          />
        </div>
        <div className="dbz-filter-group">
          <span className="dbz-filter-label">失败子类</span>
          <OptionButtons
            allowClear
            ariaLabel="失败子类"
            options={deadLetterFailureSubtypeDic}
            value={draft.failureSubtype}
            onChange={(value) => {
              const next = { ...draft, failureSubtype: value };
              setDraft(next);
              applyFilters(next);
            }}
          />
        </div>
        <Input.Search
          allowClear
          placeholder="失败原因关键字"
          style={{ width: 170 }}
          value={draft.reason ?? ''}
          onChange={(event) => setDraft((prev) => ({ ...prev, reason: event.target.value }))}
          onSearch={() => applyFilters(draft)}
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
          icon={<CheckOutlined />}
          disabled={pendingTargets.length === 0}
          loading={acting}
          onClick={() => openMarkHandled(pendingTargets)}
        >
          标记已处理
        </Button>
        <Button
          icon={<RedoOutlined />}
          disabled={retryTargets.length === 0}
          loading={acting}
          onClick={() => confirmRetry(retryTargets)}
        >
          重投选中
        </Button>
        <Button
          danger
          icon={<DeleteOutlined />}
          disabled={pendingTargets.length === 0}
          loading={acting}
          onClick={() => discardRecords(pendingTargets)}
        >
          废弃选中
        </Button>
        <Button
          icon={<UndoOutlined />}
          disabled={manualTargets.length === 0}
          loading={acting}
          onClick={() => undoHandled(manualTargets)}
        >
          撤销处理
        </Button>
        <Button icon={<ExportOutlined />} loading={exporting} onClick={exportCsv}>
          导出 CSV
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
        scroll={{ x: 1078 }}
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

      <Modal
        open={handledOpen}
        title={`标记 ${handledTargets.length} 条为「已人工处理」`}
        okText="确认标记"
        cancelText="取消"
        confirmLoading={acting}
        onOk={submitMarkHandled}
        onCancel={() => setHandledOpen(false)}
        destroyOnHidden
      >
        <Typography.Paragraph type="secondary" style={{ marginTop: 8 }}>
          表示问题已在别处修好，不再需要重投；与「已废弃」不同，后者表示不再处理。处理人与时间由服务端记录，之后可撤销。
        </Typography.Paragraph>
        <Input.TextArea
          rows={4}
          maxLength={500}
          showCount
          placeholder="请填写处理说明，例如：在目标库补录后已人工处理"
          value={handledRemark}
          onChange={(event) => setHandledRemark(event.target.value)}
        />
      </Modal>

      <Drawer
        open={Boolean(detail)}
        title={detail ? `死信详情 · #${shortId(detail.id)}` : '死信详情'}
        width="min(760px, 94vw)"
        onClose={() => setDetail(null)}
        extra={
          detail && (
            <Space>
              {canRetry(detail) && (
                <Button
                  icon={<RedoOutlined />}
                  loading={acting}
                  onClick={() => confirmRetry([detail])}
                >
                  按原报文重投
                </Button>
              )}
              {detail.status === STATUS_PENDING && (
                <Button
                  icon={<CheckOutlined />}
                  loading={acting}
                  onClick={() => openMarkHandled([detail])}
                >
                  标记已处理
                </Button>
              )}
              {detail.status === STATUS_PENDING && (
                <Button danger icon={<DeleteOutlined />} loading={acting} onClick={() => discardRecords([detail])}>
                  废弃
                </Button>
              )}
              {isManualHandled(detail) && (
                <Button
                  icon={<UndoOutlined />}
                  loading={acting}
                  onClick={() => undoHandled([detail])}
                >
                  撤销处理
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
                {nameOf(detail.connectorId)}
                <span className="dbz-mono dbz-id"> #{shortId(detail.connectorId)}</span>
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
              <span className="dbz-status-row-label">分区 / Offset</span>
              <span className="dbz-status-row-value dbz-mono">
                {detail.partition ?? '—'} / {detail.offset ?? '—'}
              </span>
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
            {detail.handledBy && (
              <div className="dbz-status-row">
                <span className="dbz-status-row-label">处理人</span>
                <span className="dbz-status-row-value">#{shortId(detail.handledBy)}</span>
              </div>
            )}
            {detail.remark && (
              <div className="dbz-status-row">
                <span className="dbz-status-row-label">处理说明</span>
                <span className="dbz-status-row-value">{detail.remark}</span>
              </div>
            )}

            <Typography.Title level={5} style={{ marginTop: 16 }}>
              失败原因
            </Typography.Title>
            <Typography.Text strong>{orDash(detail.reason)}</Typography.Text>

            <div className="dbz-detail-section-head">
              <Typography.Title level={5} style={{ margin: 0 }}>
                失败详情
              </Typography.Title>
              {detail.failureDetail && (
                <Button
                  type="text"
                  size="small"
                  icon={<CopyOutlined />}
                  aria-label="复制失败详情"
                  onClick={() => void copyFullText(detail.failureDetail, '失败详情')}
                />
              )}
            </div>
            {detail.failureDetail ? (
              <pre className="dbz-json-view">{detail.failureDetail}</pre>
            ) : (
              <Typography.Text type="secondary">没有失败详情</Typography.Text>
            )}

            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={detail.canEditPayload ? 12 : 24}>
                <div className="dbz-detail-section-head">
                  <Typography.Title level={5} style={{ margin: 0 }}>
                    原始报文（永不覆盖）
                  </Typography.Title>
                  <Button
                    type="text"
                    size="small"
                    icon={<CopyOutlined />}
                    aria-label="复制原始报文"
                    onClick={() => void copyFullText(payloadText(detail.payload), '原始报文')}
                  />
                </div>
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
                    disabled={detail.status !== STATUS_PENDING}
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

            {detail.status === 1 && (
              <Tag color="success" icon={<CheckCircleOutlined />} style={{ marginTop: 12 }}>
                已重投，无需再处理
              </Tag>
            )}
            {detail.status === 2 && (
              <Tag color="default" icon={<CheckCircleOutlined />} style={{ marginTop: 12 }}>
                已废弃，仅保留追溯
              </Tag>
            )}
            {detail.status === STATUS_MANUAL_HANDLED && (
              <Tag color="processing" icon={<CheckOutlined />} style={{ marginTop: 12 }}>
                已人工处理：问题已在别处解决，可撤销
              </Tag>
            )}
          </div>
        )}
      </Drawer>
    </Card>
  );
}
