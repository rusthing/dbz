/**
 * # 「不一致」对比弹窗
 *
 * 点连接器卡片上的「不一致」标签打开，左边是库内纳管配置，右边是远端 Connect 的
 * 实际配置，差异项高亮；两边都来自检测时持久化的脱敏快照，判定口径与检测同源，
 * 保证「标签说不一致」时这里一定能看出是哪个字段。
 *
 * 两侧的键集合都展示：只在远端出现的项（比如插件升级把 `http.api.url` 换成了
 * `http.url`）排在库内键之后、带「仅远端」标记，否则用户只会看到库内旧键「远端
 * 缺少该配置项」，把「远端已有等价的新键」误读成「远端没配地址」。
 *
 * 弹窗内可以：
 * - 按方向就地编辑要作为基准的那份配置（库内 / 远端；「仅远端」项不可编辑）；
 * - 以库内为准下发远端（`PUT /connectors/{name}/config`，需审核口令）；
 * - 以远端为准写回库内记录（不校验口令）。
 *
 * 敏感字段在快照里已脱敏，编辑框留空表示保持原值，不会被占位符覆盖。
 */

import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  App as AntdApp,
  Button,
  Empty,
  Input,
  Modal,
  Radio,
  Space,
  Spin,
  Table,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { ApiError } from '../../api/client';
import { controlPlaneApi } from '../../api/control-plane';
import type {
  DbzConnectorCompareDetail,
  DbzConnectorCompareRow,
  DbzConnectorSyncDirection,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
} from '../../api/types';
import { sm3 } from '../../lib/sm3';

type ConnectorRecord = DbzSourceConnectorVo | DbzSinkConnectorVo;

export interface InconsistentDiffTarget {
  kind: 'source' | 'sink';
  id: Id;
  name: string;
}

interface Props {
  open: boolean;
  target: InconsistentDiffTarget | null;
  record: ConnectorRecord | null;
  /** 正在重新检测（无快照时自动触发） */
  detecting?: boolean;
  onClose: () => void;
  /** 重新检测，生成最新快照 */
  onDetect: () => void;
  /** 同步成功：刷新卡片与标记 */
  onSynced: () => void;
}

const CODE_PSWD_WRONG = 'ADM00007';
const CODE_PSWD_RATE_LIMITED = 'ADM00008';
const CODE_PSWD_USER_UNAVAILABLE = 'ADM00009';
const CODE_SESSION_INVALID = 'ADM00005';
const CODE_VERIFY_UNAVAILABLE = 'DBZ00001';

function isPasswordCode(code?: string): boolean {
  return (
    code === CODE_PSWD_WRONG ||
    code === CODE_PSWD_RATE_LIMITED ||
    code === CODE_PSWD_USER_UNAVAILABLE ||
    code === CODE_SESSION_INVALID ||
    code === CODE_VERIFY_UNAVAILABLE
  );
}

function errorText(reason: unknown): string {
  if (reason instanceof Error && reason.message) {
    return reason.message;
  }
  return typeof reason === 'string' ? reason : '未知错误';
}

/** 口令类失败按错误码分流，不再依赖提示文案 */
function passwordErrorText(reason: unknown): string {
  if (!(reason instanceof ApiError)) {
    return errorText(reason);
  }
  switch (reason.code) {
    case CODE_SESSION_INVALID:
      return `登录状态已失效，同步未执行：${reason.message}`;
    case CODE_PSWD_RATE_LIMITED:
      return `${reason.message}，同步未执行。认证服务默认 10 分钟内最多允许 5 次错误，请等待窗口结束后重试。`;
    case CODE_PSWD_USER_UNAVAILABLE:
      return `${reason.message}，同步未执行`;
    case CODE_VERIFY_UNAVAILABLE:
      return `${reason.message}，同步未执行`;
    default:
      return reason.message;
  }
}

function parseDetail(text?: string | null): DbzConnectorCompareDetail | null {
  if (!text) {
    return null;
  }
  try {
    const value = JSON.parse(text) as DbzConnectorCompareDetail;
    if (!value || typeof value !== 'object' || !Array.isArray(value.rows)) {
      return null;
    }
    return value;
  } catch {
    return null;
  }
}

/** 只对对象/数组尝试解析，其余按字符串提交（Connect 配置值基本是字符串） */
function parseEditedValue(text: string): unknown {
  const trimmed = text.trim();
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    try {
      return JSON.parse(trimmed);
    } catch {
      return text;
    }
  }
  return text;
}

export default function InconsistentDiffModal({
  open,
  target,
  record,
  detecting = false,
  onClose,
  onDetect,
  onSynced,
}: Props) {
  const { message, modal } = AntdApp.useApp();
  const [direction, setDirection] = useState<DbzConnectorSyncDirection>('toRemote');
  const [edits, setEdits] = useState<Record<string, string>>({});
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const detail = useMemo(
    () => parseDetail(record?.inconsistentDetail),
    [record?.inconsistentDetail],
  );
  const remoteMissing = detail !== null && !detail.remoteExists;
  const rows = detail?.rows ?? [];

  // 切换节点或重新打开时回到默认方向，不保留上一个连接器的编辑
  useEffect(() => {
    if (!open) {
      return;
    }
    setDirection('toRemote');
    setEdits({});
    setPassword('');
    setError(null);
    setSubmitting(false);
  }, [open, target?.kind, target?.id]);

  // 远端已不存在时只能以库内为准重新下发
  useEffect(() => {
    if (open && remoteMissing) {
      setDirection('toRemote');
    }
  }, [open, remoteMissing]);

  const editableValueOf = (row: DbzConnectorCompareRow): string =>
    direction === 'toRemote' ? (row.local ?? '') : (row.remote ?? '');

  const dirty = rows.some((row) => {
    if (row.remoteOnly) {
      // 「仅远端」项不可编辑，随同步整体收敛
      return false;
    }
    const edit = edits[row.key];
    if (edit === undefined) {
      return false;
    }
    if (row.sensitive) {
      // 敏感项留空表示保持原值，不算修改
      return edit.trim() !== '';
    }
    return edit !== editableValueOf(row);
  });

  const setEdit = (key: string, value: string) => {
    setEdits((prev) => ({ ...prev, [key]: value }));
    setError(null);
  };

  /** 只提交真正改过的字段；敏感项未重新输入就不传，由后端保留原值 */
  const buildPatch = (): Record<string, unknown> => {
    const patch: Record<string, unknown> = {};
    for (const row of rows) {
      // 「仅远端」项不在基准配置里，后端也会拒绝这类键
      if (row.remoteOnly) {
        continue;
      }
      const edit = edits[row.key];
      if (edit === undefined) {
        continue;
      }
      if (row.sensitive) {
        const trimmed = edit.trim();
        if (!trimmed) {
          continue;
        }
        patch[row.key] = trimmed;
        continue;
      }
      if (edit === editableValueOf(row)) {
        continue;
      }
      patch[row.key] = parseEditedValue(edit);
    }
    return patch;
  };

  const handleDirectionChange = (next: DbzConnectorSyncDirection) => {
    if (next === direction) {
      return;
    }
    if (!dirty) {
      setDirection(next);
      setEdits({});
      setError(null);
      return;
    }
    modal.confirm({
      title: '切换同步方向会丢弃未保存的修改',
      content: '当前方向下改过的字段不会保留，确定切换吗？',
      okText: '切换并丢弃',
      cancelText: '继续编辑',
      onOk: () => {
        setDirection(next);
        setEdits({});
        setError(null);
      },
    });
  };

  const requestClose = () => {
    if (submitting) {
      return;
    }
    if (!dirty) {
      onClose();
      return;
    }
    modal.confirm({
      title: '关闭会丢弃未保存的修改',
      content: '改过的字段还没有同步，关闭后不会保留。确定关闭吗？',
      okText: '关闭并丢弃',
      cancelText: '继续编辑',
      onOk: onClose,
    });
  };

  const doSync = async () => {
    if (!target) {
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const result = await controlPlaneApi.syncConnectorConfig({
        nodeType: target.kind,
        nodeId: target.id,
        direction,
        patch: buildPatch(),
        auditPswd: direction === 'toRemote' ? sm3(password.trim()) : undefined,
      });
      if (direction === 'toRemote' && result?.pswdKind === 'login') {
        message.info('当前账号未设置审核口令，已按登录口令校验通过');
      }
      if (result?.inconsistent) {
        message.warning('同步已完成，但重新检测后仍存在差异，标记保留');
      } else if (direction === 'toRemote') {
        message.success('已下发到远端，检测标记已重新计算');
      } else {
        message.success('已写回库内，检测标记已清除');
      }
      setEdits({});
      setPassword('');
      onSynced();
      onClose();
    } catch (reason) {
      if (
        direction === 'toRemote' &&
        reason instanceof ApiError &&
        isPasswordCode(reason.code)
      ) {
        setError(passwordErrorText(reason));
      } else {
        setError(errorText(reason));
      }
    } finally {
      setSubmitting(false);
    }
  };

  const runSync = () => {
    if (direction === 'toRemote') {
      if (!password.trim()) {
        setError('请输入审核口令；未设置审核口令时输入登录口令');
        return;
      }
      void doSync();
      return;
    }
    // 写回库内会覆盖现有配置，执行前再确认一次影响
    modal.confirm({
      title: `用远端配置覆盖库内「${target?.name ?? ''}」？`,
      content: '库内当前连接器配置会被替换，远端不变；保存后标记会按新配置重新计算。',
      okText: '覆盖并保存',
      cancelText: '取消',
      onOk: () => doSync(),
    });
  };

  const renderValue = (row: DbzConnectorCompareRow, side: 'local' | 'remote') => {
    const editable =
      !row.remoteOnly &&
      ((direction === 'toRemote' && side === 'local') ||
        (direction === 'toLocal' && side === 'remote'));
    // 只看「有没有这一项」，空字符串是合法的配置值
    const remoteMissing =
      side === 'remote' && (row.remote === null || row.remote === undefined);
    const localMissing =
      side === 'local' &&
      (row.remoteOnly === true || row.local === null || row.local === undefined);

    if (remoteMissing) {
      return (
        <Typography.Text type="secondary">（远端缺少该配置项）</Typography.Text>
      );
    }

    if (localMissing) {
      return (
        <Typography.Text type="secondary">
          {direction === 'toRemote'
            ? '（库内没有这一项，下发会移除）'
            : '（库内没有这一项，写回后补齐）'}
        </Typography.Text>
      );
    }

    if (!editable) {
      return <pre className="dbz-diff-value">{side === 'local' ? row.local : row.remote}</pre>;
    }

    const current = edits[row.key];
    if (row.sensitive) {
      return (
        <Input.Password
          size="small"
          value={current ?? ''}
          placeholder={
            side === 'local' ? '已脱敏，留空保持库内原值' : '已脱敏，留空保持远端原值'
          }
          onChange={(event) => setEdit(row.key, event.target.value)}
        />
      );
    }
    return (
      <Input
        size="small"
        value={current ?? editableValueOf(row)}
        onChange={(event) => setEdit(row.key, event.target.value)}
        placeholder={side === 'local' ? '库内配置' : '远端配置'}
      />
    );
  };

  const columns: ColumnsType<DbzConnectorCompareRow> = [
    {
      title: '配置项',
      dataIndex: 'key',
      width: 250,
      render: (key: string, row) => (
        <Space size={4} wrap>
          <Typography.Text className="dbz-mono" strong={row.changed}>
            {key}
          </Typography.Text>
          {row.changed && !row.remoteOnly && <Tag color="warning">差异</Tag>}
          {row.remoteOnly && (
            <Tooltip title="该项只在远端 Connect 上存在，库内没有纳管：下发会以库内为准整体覆盖（这一项被移除），写回则把它补进库内">
              <Tag color="geekblue">仅远端</Tag>
            </Tooltip>
          )}
          {row.sensitive && (
            <Tooltip title="含凭据类字段，值已脱敏，不在页面与快照中回显明文">
              <Tag>敏感</Tag>
            </Tooltip>
          )}
        </Space>
      ),
    },
    {
      title: '库内配置',
      dataIndex: 'local',
      width: 340,
      render: (_: string, row) => renderValue(row, 'local'),
    },
    {
      title: '远端 Connect 实际配置',
      dataIndex: 'remote',
      width: 340,
      render: (_: string | null | undefined, row) => renderValue(row, 'remote'),
    },
  ];

  const impact =
    direction === 'toRemote'
      ? '以库内配置为基准，把编辑后的配置下发到远端 Connect：远端连接器会被更新并重新加载，可能有短暂中断或任务重平衡；远端独有的配置项（标记「仅远端」）不在库内配置里，会被一并移除。此操作需要审核口令。'
      : '以远端 Connect 的实际配置为基准，覆盖库内纳管记录：库内当前配置会被替换，远端不做任何修改；远端独有的配置项会一起写进库内。';

  return (
    <Modal
      open={open}
      title={target ? `「${target.name}」配置对比` : '配置对比'}
      width={1060}
      okText={direction === 'toRemote' ? '下发到远端' : '写回库内'}
      okButtonProps={{
        danger: direction === 'toRemote',
        'data-diff-sync': 'true',
      }}
      cancelText="关闭"
      confirmLoading={submitting}
      maskClosable={!submitting}
      onCancel={requestClose}
      onOk={runSync}
      destroyOnHidden
    >
      <div data-diff-dialog>
        {remoteMissing ? (
          <Alert
            type="error"
            showIcon
            message="远端已不存在"
            description={
              detail?.reason ||
              '检测时 Connect 上已不存在该连接器，记录已被自动禁用。可切换为「以库内为准」在远端重新下发。'
            }
          />
        ) : detail === null ? (
          <Alert
            type={detecting ? 'info' : 'warning'}
            showIcon
            message={detecting ? '正在重新检测，生成对比快照…' : '还没有对比快照'}
            description={
              detecting ? (
                <Spin size="small" />
              ) : (
                <Space>
                  <span>该标记来自升级前的检测，缺少快照；重新检测后即可逐项对比。</span>
                  <Button size="small" onClick={onDetect}>
                    立即检测
                  </Button>
                </Space>
              )
            }
          />
        ) : detail.reason ? (
          <Alert type="warning" showIcon message="无法逐项对比" description={detail.reason} />
        ) : null}

        {!remoteMissing && (
          <Space direction="vertical" size={8} style={{ marginTop: 12 }}>
            <Space size={12} wrap>
              <Typography.Text strong>同步方向</Typography.Text>
              <Radio.Group
                value={direction}
                onChange={(event) => handleDirectionChange(event.target.value)}
              >
                <Radio.Button value="toRemote" data-diff-direction="toRemote">
                  以库内为准 → 下发远端
                </Radio.Button>
                <Radio.Button value="toLocal" data-diff-direction="toLocal">
                  以远端为准 → 写回库内
                </Radio.Button>
              </Radio.Group>
              <Typography.Text type="secondary">
                检测时间：{detail ? new Date(detail.checkedAt).toLocaleString() : '--'}
              </Typography.Text>
            </Space>
            <Alert type="info" showIcon message={impact} />
          </Space>
        )}

        {!remoteMissing && rows.length === 0 && detail !== null && (
          <Empty
            style={{ marginTop: 16 }}
            description={detail.reason || '没有可展示的差异项（请重新检测）'}
          />
        )}

        {!remoteMissing && rows.length > 0 && (
          <Table<DbzConnectorCompareRow>
            style={{ marginTop: 12 }}
            size="small"
            rowKey="key"
            columns={columns}
            dataSource={rows}
            pagination={false}
            scroll={{ y: 380 }}
            rowClassName={(row) => (row.changed ? 'dbz-diff-row-changed' : '')}
          />
        )}

        {direction === 'toRemote' && (
          <div className="dbz-delete-password" style={{ marginTop: 16 }}>
            <Typography.Text strong>审核口令（未设置时使用登录口令）</Typography.Text>
            <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 6 }}>
              下发会更新并重新加载远端连接器，风险与删除相当；口令在浏览器内先做 SM3
              摘要，后端带登录令牌转给认证服务校验，是否改用登录口令由认证服务判定。
            </Typography.Text>
            <Input.Password
              value={password}
              placeholder="请输入审核口令（未设置时输入登录口令）"
              data-diff-password
              onChange={(event) => {
                setPassword(event.target.value);
                setError(null);
              }}
              onPressEnter={runSync}
            />
          </div>
        )}

        {error && (
          <Alert
            type="error"
            showIcon
            message={error}
            data-diff-error
            style={{ marginTop: 12 }}
          />
        )}
      </div>
    </Modal>
  );
}
