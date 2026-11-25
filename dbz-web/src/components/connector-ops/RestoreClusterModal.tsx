/**
 * # 集群备份恢复 / 导入弹窗（共享）
 *
 * 控制面板的「恢复」「导入集群」与 Kafka 集群维护页的「恢复」共用：文件内容原样
 * 回传后端，`dryRun` 决定是否写库；切换策略重新取预览，保证界面计数与真正执行的
 * 判定一致。只写库内纳管记录，不向远端注册或删除连接器。
 */

import {
  Alert,
  App as AntdApp,
  Button,
  Radio,
  Table,
  Tag,
  Typography,
} from 'antd';
import { Modal } from '../../lib/federated-overlay';
import { useEffect, useState } from 'react';
import { backupApi } from '../../api/backup';
import type {
  BackupRestoreStrategy,
  DbzBackupRestoreItemVo,
  DbzBackupRestoreVo,
} from '../../api/types';
import { errorMessage } from '../../utils/format';

export interface RestoreClusterRequest {
  file: File;
  fileName: string;
  mode: RestoreMode;
}

/** 备份文件的两种用法：卡片上「恢复」与顶栏「导入集群」 */
export type RestoreMode = 'restore' | 'import';

interface Session {
  mode: RestoreMode;
  fileName: string;
  /** 原始 JSON（已通过格式校验，原样回传后端） */
  file: unknown;
  strategy: BackupRestoreStrategy;
  phase: 'preview' | 'running' | 'result';
  loadingPreview: boolean;
  preview: DbzBackupRestoreVo;
  result: DbzBackupRestoreVo | null;
}

interface RestoreClusterModalProps {
  request: RestoreClusterRequest | null;
  onClose: () => void;
  /** 执行成功后调用，通常是刷新列表 */
  onDone?: () => void;
  /** 导入新建集群时回报集群名，便于调用方切到对应视图 */
  onImported?: (brokerName: string) => void;
}

const RESTORE_TEXT: Record<
  RestoreMode,
  {
    title: string;
    noun: string;
    okText: string;
    runningText: string;
    scope: string;
    submit: string;
  }
> = {
  restore: {
    title: '从 JSON 恢复 Kafka 集群',
    noun: '恢复',
    okText: '执行恢复',
    runningText: '正在恢复…',
    scope:
      '恢复只写库内纳管记录，不会在远端注册或删除连接器；需要后续启用/检测才会下发到集群。',
    submit: '恢复完成',
  },
  import: {
    title: '从 JSON 导入 Kafka 集群',
    noun: '导入',
    okText: '执行导入',
    runningText: '正在导入…',
    scope:
      '导入按文件内容建库内记录：集群、Connect 与连接器都会新建或按策略覆盖，不向远端注册或删除任何连接器；导入后在卡片上启用/检测才会下发到集群。',
    submit: '导入完成',
  },
};

const RESTORE_LEVEL_LABEL: Record<DbzBackupRestoreItemVo['level'], string> = {
  broker: 'Kafka 集群',
  connect: 'Connect',
  source: '上游连接器',
  sink: '下游连接器',
};

const RESTORE_LEVEL_COLOR: Record<DbzBackupRestoreItemVo['level'], string> = {
  broker: 'blue',
  connect: 'purple',
  source: 'cyan',
  sink: 'magenta',
};

const RESTORE_ACTION_LABEL: Record<DbzBackupRestoreItemVo['action'], string> = {
  create: '新增',
  overwrite: '覆盖',
  skip: '跳过',
  failed: '失败',
};

const RESTORE_ACTION_COLOR: Record<DbzBackupRestoreItemVo['action'], string> = {
  create: 'success',
  overwrite: 'geekblue',
  skip: 'default',
  failed: 'error',
};

export default function RestoreClusterModal({
  request,
  onClose,
  onDone,
  onImported,
}: RestoreClusterModalProps) {
  const { message, modal } = AntdApp.useApp();
  const [session, setSession] = useState<Session | null>(null);

  // 新请求到达：解析 + 取预览（dryRun，不写库），失败原因逐条列出
  useEffect(() => {
    if (!request) {
      setSession(null);
      return;
    }
    let cancelled = false;
    void (async () => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(await request.file.text());
      } catch (reason) {
        if (cancelled) {
          return;
        }
        modal.error({
          title: '读取备份文件失败',
          width: 640,
          content: reason instanceof SyntaxError ? '文件不是合法的 JSON' : errorMessage(reason),
        });
        onClose();
        return;
      }
      try {
        const preview = await backupApi.restore(parsed, 'skip', true);
        if (cancelled) {
          return;
        }
        if (!preview.valid) {
          modal.error({
            title: '备份文件不合法',
            width: 640,
            content: (
              <ul className="dbz-transfer-errors">
                {preview.errors.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            ),
          });
          onClose();
          return;
        }
        setSession({
          mode: request.mode,
          fileName: request.fileName,
          file: parsed,
          strategy: 'skip',
          phase: 'preview',
          loadingPreview: false,
          preview,
          result: null,
        });
      } catch (reason) {
        if (cancelled) {
          return;
        }
        modal.error({ title: '读取备份文件失败', content: errorMessage(reason) });
        onClose();
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [request]);

  const close = () => {
    if (session?.phase === 'running') {
      return;
    }
    setSession(null);
    onClose();
  };

  const changeStrategy = async (next: BackupRestoreStrategy) => {
    const current = session;
    if (
      !current ||
      current.loadingPreview ||
      current.phase === 'running' ||
      current.strategy === next
    ) {
      return;
    }
    const noun = RESTORE_TEXT[current.mode].noun;
    setSession({ ...current, strategy: next, loadingPreview: true });
    try {
      const preview = await backupApi.restore(current.file, next, true);
      if (!preview.valid) {
        message.error(preview.errors.join('；') || `${noun}预览失败`);
        setSession((previous) =>
          previous
            ? { ...previous, strategy: current.strategy, loadingPreview: false }
            : previous,
        );
        return;
      }
      setSession((previous) =>
        previous ? { ...previous, preview, loadingPreview: false } : previous,
      );
    } catch (reason) {
      setSession((previous) =>
        previous
          ? { ...previous, strategy: current.strategy, loadingPreview: false }
          : previous,
      );
      message.error(`${noun}预览失败：${errorMessage(reason)}`);
    }
  };

  const execute = () => {
    const current = session;
    if (!current) {
      return;
    }
    const text = RESTORE_TEXT[current.mode];
    const preview = current.preview;
    const overwrite = current.strategy === 'overwrite';
    const brokerName = preview.items.find((item) => item.level === 'broker')?.name ?? '';
    modal.confirm({
      title: overwrite ? '覆盖已存在的纳管记录？' : '仅补齐缺失的纳管记录？',
      width: 560,
      content: [
        `将写库 ${preview.created + preview.overwritten} 条：新增 ${preview.created}，覆盖 ${preview.overwritten}，跳过 ${preview.skipped}。`,
        text.scope,
      ].join('\n'),
      okText: text.okText,
      cancelText: '取消',
      onOk: async () => {
        setSession((previous) => (previous ? { ...previous, phase: 'running' } : previous));
        try {
          const result = await backupApi.restore(current.file, current.strategy, false);
          setSession((previous) =>
            previous ? { ...previous, phase: 'result', result } : previous,
          );
          const summary = `新增 ${result.created}，覆盖 ${result.overwritten}，跳过 ${result.skipped}，失败 ${result.failed}`;
          if (current.mode === 'import' && brokerName && result.created > 0) {
            onImported?.(brokerName);
          }
          if (result.failed > 0) {
            message.warning(`${text.submit}：${summary}`);
          } else {
            message.success(`${text.submit}：${summary}`);
          }
          onDone?.();
        } catch (reason) {
          setSession((previous) =>
            previous ? { ...previous, phase: 'preview' } : previous,
          );
          message.error(`${text.noun}失败：${errorMessage(reason)}`);
        }
      },
    });
  };

  const text = session ? RESTORE_TEXT[session.mode] : RESTORE_TEXT.restore;
  const view =
    session?.phase === 'result' && session.result ? session.result : session?.preview ?? null;

  return (
    <Modal
      open={session !== null}
      width={900}
      title={text.title}
      onCancel={close}
      maskClosable={session?.phase !== 'running'}
      destroyOnHidden
      footer={
        !session
          ? null
          : session.phase === 'result'
            ? [
                <Button key="close" onClick={close}>
                  关闭
                </Button>,
              ]
            : session.phase === 'running'
              ? [
                  <Button key="running" type="primary" loading disabled>
                    {text.runningText}
                  </Button>,
                ]
              : [
                  <Button key="cancel" onClick={close}>
                    取消
                  </Button>,
                  <Button
                    key="ok"
                    type="primary"
                    loading={session.loadingPreview}
                    disabled={session.loadingPreview}
                    onClick={execute}
                  >
                    {text.okText}
                  </Button>,
                ]
      }
    >
      {session && view && (
        <div className="dbz-transfer-import" data-restore-dialog data-restore-mode={session.mode}>
          <div className="dbz-transfer-file">
            文件：<span className="dbz-mono">{session.fileName}</span>
            {view.items.find((item) => item.level === 'broker') && (
              <>
                {' '}
                · 集群{' '}
                <span className="dbz-mono">
                  {view.items.find((item) => item.level === 'broker')?.name}
                </span>
              </>
            )}
            {' '}
            · Connect {view.items.filter((item) => item.level === 'connect').length} 台 ·
            连接器{' '}
            {view.items.filter((item) => item.level === 'source' || item.level === 'sink').length} 条
          </div>

          <Alert
            type="warning"
            showIcon
            message={`备份文件含 sasl_password 等敏感凭据，${text.noun}后能直接连上原集群；请只在可信环境之间传递并妥善保管。`}
          />
          <Alert type="info" showIcon message={text.scope} />

          {session.phase !== 'result' && (
            <div className="dbz-transfer-bulk" data-restore-strategy>
              <span>{text.noun}策略：</span>
              <Radio.Group
                value={session.strategy}
                disabled={session.loadingPreview}
                onChange={(event) =>
                  void changeStrategy(event.target.value as BackupRestoreStrategy)
                }
              >
                <Radio.Button value="skip">仅补齐缺失（默认）</Radio.Button>
                <Radio.Button value="overwrite">覆盖已存在</Radio.Button>
              </Radio.Group>
            </div>
          )}

          <div className="dbz-transfer-summary">
            {session.phase === 'result' ? (
              <>
                <Tag color="success">新增 {view.created}</Tag>
                <Tag color="geekblue">覆盖 {view.overwritten}</Tag>
                <Tag>跳过 {view.skipped}</Tag>
                <Tag color="error">失败 {view.failed}</Tag>
              </>
            ) : (
              <>
                <Tag color="success">将新增 {view.created}</Tag>
                <Tag color="geekblue">将覆盖 {view.overwritten}</Tag>
                <Tag>将跳过 {view.skipped}</Tag>
                <Tag color="error">失败 {view.failed}</Tag>
              </>
            )}
          </div>

          <Table<DbzBackupRestoreItemVo>
            size="small"
            rowKey={(item) => `${item.level}:${item.connectorName ?? item.name}`}
            loading={session.loadingPreview}
            pagination={false}
            dataSource={view.items}
            scroll={{ y: 360 }}
            columns={[
              {
                title: '层级',
                key: 'level',
                width: 110,
                render: (_value: unknown, item: DbzBackupRestoreItemVo) => (
                  <Tag color={RESTORE_LEVEL_COLOR[item.level]}>
                    {RESTORE_LEVEL_LABEL[item.level]}
                  </Tag>
                ),
              },
              {
                title: '名称 / connectorName',
                key: 'name',
                render: (_value: unknown, item: DbzBackupRestoreItemVo) => (
                  <div>
                    <div>{item.name}</div>
                    {item.connectorName && (
                      <div className="dbz-mono">{item.connectorName}</div>
                    )}
                  </div>
                ),
              },
              {
                title: '所属',
                key: 'parent',
                width: 160,
                render: (_value: unknown, item: DbzBackupRestoreItemVo) => item.parent || '--',
              },
              {
                title: '动作',
                key: 'action',
                width: 90,
                render: (_value: unknown, item: DbzBackupRestoreItemVo) => (
                  <Tag color={RESTORE_ACTION_COLOR[item.action]}>
                    {RESTORE_ACTION_LABEL[item.action]}
                  </Tag>
                ),
              },
              {
                title: '说明',
                key: 'message',
                render: (_value: unknown, item: DbzBackupRestoreItemVo) =>
                  item.message ? (
                    <Typography.Text type={item.action === 'failed' ? 'danger' : 'secondary'}>
                      {item.message}
                    </Typography.Text>
                  ) : (
                    '--'
                  ),
              },
            ]}
          />
        </div>
      )}
    </Modal>
  );
}
