/**
 * # 导入连接器弹窗（共享）
 *
 * 控制面板 Connect 卡片与 Kafka Connect 维护页共用：读一份「导出连接器」JSON，
 * 预览同名冲突并逐条选择处理方式，执行时只写库内记录，不向远端注册连接器。
 *
 * 冲突判定按整库：`connector_name` 在上游表、下游表内各自唯一，与 Connect 无关。
 * 因此预览与执行都先取全量上游/下游记录，不能只看目标 Connect 名下的那几条。
 */

import {
  Alert,
  App as AntdApp,
  Button,
  Modal,
  Select,
  Table,
  Tag,
  Typography,
} from 'antd';
import { useEffect, useMemo, useState, type HTMLAttributes } from 'react';
import { kafkaConnectorApi } from '../../api/kafka-connector';
import { sinkConnectorApi } from '../../api/sink-connector';
import { sourceConnectorApi } from '../../api/source-connector';
import type {
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
} from '../../api/types';
import { errorMessage } from '../../utils/format';
import {
  buildImportRows,
  canOverwriteImport,
  nextImportedName,
  parseTransferFile,
  type ImportDecision,
  type ImportOutcome,
  type ImportPreviewRow,
  type TransferConnector,
  type TransferFile,
} from '../../pages/control-panel/connect-transfer';

/** 父级选好文件后交给弹窗：文件本身已读完，弹窗负责解析与预览 */
export interface ConnectorImportRequest {
  target: DbzKafkaConnectVo;
  file: File;
  fileName: string;
}

interface ImportSession {
  target: DbzKafkaConnectVo;
  file: TransferFile;
  fileName: string;
  phase: 'preview' | 'running' | 'result';
  /** 预览阶段正在读取目标 Connect 的实时连接器 */
  loadingPreview: boolean;
  rows: ImportPreviewRow[];
  liveNames: Set<string> | null;
  liveWarning?: string;
  outcomes: ImportOutcome[];
}

interface ImportConnectorsModalProps {
  request: ConnectorImportRequest | null;
  onClose: () => void;
  /** 执行结束后调用，通常是刷新列表 */
  onDone?: () => void;
}

/** 导入结果分类文案：汇总里必须把「覆盖并纳管」与「覆盖已有纳管记录」分开 */
const IMPORT_ACTION_LABEL: Record<ImportOutcome['action'], string> = {
  created: '新增',
  'overwritten-managed': '覆盖已有纳管记录',
  'overwritten-adopted': '覆盖并纳管',
  skipped: '跳过',
  'kept-both': '改名保留',
  failed: '失败',
};

const IMPORT_DECISION_LABEL: Record<ImportDecision, string> = {
  overwrite: '覆盖',
  skip: '跳过',
  keep: '两者都保留',
};

const IMPORT_ACTION_COLOR: Record<ImportOutcome['action'], string> = {
  created: 'success',
  'overwritten-managed': 'geekblue',
  'overwritten-adopted': 'blue',
  skipped: 'default',
  'kept-both': 'purple',
  failed: 'error',
};

export default function ImportConnectorsModal({
  request,
  onClose,
  onDone,
}: ImportConnectorsModalProps) {
  const { message } = AntdApp.useApp();
  const [session, setSession] = useState<ImportSession | null>(null);
  const [fileErrors, setFileErrors] = useState<string[] | null>(null);
  const [allSources, setAllSources] = useState<DbzSourceConnectorVo[]>([]);
  const [allSinks, setAllSinks] = useState<DbzSinkConnectorVo[]>([]);

  useEffect(() => {
    if (!request) {
      setSession(null);
      setFileErrors(null);
      return;
    }
    let cancelled = false;
    setFileErrors(null);
    setSession(null);

    void (async () => {
      let parsed: ReturnType<typeof parseTransferFile>;
      try {
        parsed = parseTransferFile(await request.file.text());
      } catch (reason) {
        if (!cancelled) {
          setFileErrors([`无法读取文件：${errorMessage(reason)}`]);
        }
        return;
      }
      if (!parsed.file) {
        if (!cancelled) {
          setFileErrors(parsed.errors);
        }
        return;
      }
      const file = parsed.file;

      try {
        const [sources, sinks, live] = await Promise.all([
          sourceConnectorApi.list(),
          sinkConnectorApi.list(),
          kafkaConnectorApi.list(request.target.id).catch(() => null),
        ]);
        if (cancelled) {
          return;
        }
        setAllSources(sources);
        setAllSinks(sinks);
        const liveNames = live ? new Set(live.map((item) => item.name)) : null;
        setSession({
          target: request.target,
          file,
          fileName: request.fileName,
          phase: 'preview',
          loadingPreview: false,
          rows: buildImportRows(file, request.target.id, sources, sinks, liveNames),
          liveNames,
          liveWarning: live
            ? undefined
            : '无法读取目标 Connect 上的实时连接器，同名冲突只能按库内记录判断',
          outcomes: [],
        });
      } catch (reason) {
        if (!cancelled) {
          setFileErrors([`读取连接器清单失败：${errorMessage(reason)}`]);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [request]);

  const importConflictCount = useMemo(
    () => (session ? session.rows.filter((item) => item.conflict).length : 0),
    [session],
  );

  const importStats = useMemo(() => {
    const base: Record<ImportOutcome['action'], number> = {
      created: 0,
      'overwritten-managed': 0,
      'overwritten-adopted': 0,
      skipped: 0,
      'kept-both': 0,
      failed: 0,
    };
    session?.outcomes.forEach((item) => {
      base[item.action] += 1;
    });
    return base;
  }, [session]);

  /** 单条冲突处理方式 */
  const setDecision = (
    direction: TransferConnector['direction'],
    connectorName: string,
    decision: ImportDecision,
  ) => {
    setSession((previous) =>
      previous
        ? {
            ...previous,
            rows: previous.rows.map((item) =>
              item.row.direction === direction && item.row.connectorName === connectorName
                ? { ...item, decision }
                : item,
            ),
          }
        : previous,
    );
  };

  /** 顶部批量选择：只影响有冲突的行；「覆盖」跳过名字被别的 Connect 占用的行 */
  const setAllDecisions = (decision: ImportDecision) => {
    setSession((previous) =>
      previous
        ? {
            ...previous,
            rows: previous.rows.map((item) => {
              if (!item.conflict) {
                return item;
              }
              if (decision === 'overwrite' && !canOverwriteImport(item.conflict)) {
                return item;
              }
              return { ...item, decision };
            }),
          }
        : previous,
    );
  };

  /** 执行导入：先上游后下游，下游按文件里的上游 connectorName 找回挂载点 */
  const executeImport = async () => {
    if (!session) {
      return;
    }
    const target = session.target;
    setSession((previous) => (previous ? { ...previous, phase: 'running' } : previous));

    const outcomes: ImportOutcome[] = [];
    const sourceLookup = new Map<string, Id>(
      allSources
        .filter((item) => item.kafkaConnectId === target.id)
        .map((item) => [item.connectorName, item.id]),
    );
    const taken = new Set<string>([
      ...allSources.map((item) => item.connectorName),
      ...allSinks.map((item) => item.connectorName),
      ...(session.liveNames ? [...session.liveNames] : []),
      ...session.file.connectors.map((item) => item.connectorName),
    ]);

    const upstreamIdOf = (row: TransferConnector): Id => {
      const connectorName = row.upstream?.connectorName?.trim() ?? '';
      if (!connectorName) {
        throw new Error('缺少上游 connectorName，无法重新挂载下游连接器');
      }
      const id = sourceLookup.get(connectorName);
      if (!id) {
        throw new Error(
          `缺少上游连接器「${row.upstream?.name || connectorName}」（connectorName=${connectorName}），目标 Connect 上没有对应的纳管记录`,
        );
      }
      return id;
    };

    const addConnector = async (
      row: TransferConnector,
      connectorName: string,
    ): Promise<Id> => {
      if (row.direction === 'source') {
        const created = await sourceConnectorApi.add({
          kafkaConnectId: target.id,
          name: row.name,
          connectorName,
          connectorConfig: row.connectorConfig,
          remark: row.remark,
          enabled: row.enabled,
        });
        return created.id;
      }
      const created = await sinkConnectorApi.add({
        sourceConnectorId: upstreamIdOf(row),
        kafkaConnectId: target.id,
        name: row.name,
        connectorName,
        connectorConfig: row.connectorConfig,
        remark: row.remark,
        enabled: row.enabled,
      });
      return created.id;
    };

    const modifyConnector = async (
      row: TransferConnector,
      id: Id,
      connectorName: string,
    ): Promise<void> => {
      if (row.direction === 'source') {
        await sourceConnectorApi.modify({
          id,
          name: row.name,
          connectorName,
          connectorConfig: row.connectorConfig,
          remark: row.remark,
          enabled: row.enabled,
        });
        return;
      }
      await sinkConnectorApi.modify({
        id,
        name: row.name,
        connectorName,
        connectorConfig: row.connectorConfig,
        remark: row.remark,
        enabled: row.enabled,
        sourceConnectorId: upstreamIdOf(row),
      });
    };

    const runRow = async (preview: ImportPreviewRow): Promise<void> => {
      const row = preview.row;
      if (preview.conflict && preview.decision === 'skip') {
        outcomes.push({ row, action: 'skipped' });
        return;
      }
      try {
        if (preview.conflict && preview.decision === 'keep') {
          const renamed = nextImportedName(row.connectorName, taken);
          const createdId = await addConnector(row, renamed);
          taken.add(renamed);
          const hints = [`导入件已改名为 ${renamed}`];
          if (preview.conflict.unmanaged) {
            hints.push(
              `该 Connect 上另有一条未纳管的同名连接器「${row.connectorName}」，跳过或覆盖时需要单独处理`,
            );
          }
          outcomes.push({
            row,
            action: 'kept-both',
            renamedConnectorName: renamed,
            message: hints.join('；'),
          });
          if (row.direction === 'source' && preview.conflict.managed) {
            sourceLookup.set(row.connectorName, preview.conflict.managed.id);
          }
          if (row.direction === 'source') {
            sourceLookup.set(renamed, createdId);
          }
          return;
        }

        if (preview.conflict && preview.decision === 'overwrite') {
          if (!canOverwriteImport(preview.conflict)) {
            outcomes.push({
              row,
              action: 'failed',
              message:
                `connectorName「${row.connectorName}」在另一台 Connect 已有纳管记录` +
                `（id=${preview.conflict.elsewhere?.id ?? '--'}），库内唯一，只能改名保留或跳过`,
            });
            return;
          }
          if (preview.conflict.managed) {
            await modifyConnector(row, preview.conflict.managed.id, row.connectorName);
            if (row.direction === 'source') {
              sourceLookup.set(row.connectorName, preview.conflict.managed.id);
            }
            outcomes.push({ row, action: 'overwritten-managed' });
            return;
          }
          const adoptedId = await addConnector(row, row.connectorName);
          taken.add(row.connectorName);
          if (row.direction === 'source') {
            sourceLookup.set(row.connectorName, adoptedId);
          }
          outcomes.push({ row, action: 'overwritten-adopted' });
          return;
        }

        const createdId = await addConnector(row, row.connectorName);
        taken.add(row.connectorName);
        if (row.direction === 'source') {
          sourceLookup.set(row.connectorName, createdId);
        }
        outcomes.push({ row, action: 'created' });
      } catch (reason) {
        outcomes.push({ row, action: 'failed', message: errorMessage(reason) });
      }
    };

    for (const preview of session.rows.filter((item) => item.row.direction === 'source')) {
      await runRow(preview);
    }
    for (const preview of session.rows.filter((item) => item.row.direction === 'sink')) {
      await runRow(preview);
    }

    setSession((previous) =>
      previous ? { ...previous, phase: 'result', outcomes } : previous,
    );
    message.success('导入结束，结果已按连接器列出');
    onDone?.();
  };

  const close = () => {
    if (session?.phase === 'running') {
      return;
    }
    onClose();
  };

  return (
    <Modal
      open={request !== null}
      width={900}
      title={
        request ? `从 JSON 导入到「${request.target.name}」` : '导入连接器'
      }
      onCancel={close}
      maskClosable={session?.phase !== 'running'}
      destroyOnHidden
      footer={
        !session
          ? [
              <Button key="close" onClick={onClose}>
                关闭
              </Button>,
            ]
          : session.phase === 'result'
            ? [
                <Button key="close" onClick={onClose}>
                  关闭
                </Button>,
              ]
            : session.phase === 'running'
              ? [
                  <Button key="running" type="primary" loading disabled>
                    正在导入…
                  </Button>,
                ]
              : [
                  <Button key="cancel" onClick={onClose}>
                    取消
                  </Button>,
                  <Button
                    key="ok"
                    type="primary"
                    disabled={session.rows.length === 0}
                    onClick={() => void executeImport()}
                    data-import-execute
                  >
                    执行导入
                  </Button>,
                ]
      }
    >
      {fileErrors && (
        <Alert
          type="error"
          showIcon
          message="导入文件不合法"
          description={
            <ul className="dbz-transfer-errors">
              {fileErrors.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          }
        />
      )}

      {session && (
        <div className="dbz-transfer-import">
          <div className="dbz-transfer-file">
            文件：<span className="dbz-mono">{session.fileName}</span> · 连接器{' '}
            {session.rows.length} 条 · 冲突 {importConflictCount} 条
          </div>

          {session.liveWarning && (
            <Alert type="warning" showIcon message={session.liveWarning} />
          )}
          {session.file.credentialMode === 'redacted' && (
            <Alert
              type="warning"
              showIcon
              message="该文件是脱敏导出，配置中的凭据为 ******；导入后需要手动补齐口令/密钥。"
            />
          )}
          {!session.target.enabled && (
            <Alert
              type="info"
              showIcon
              message="目标 Connect 已停用：导入只写库内记录，启用 Connect 后才会真正下发到集群。"
            />
          )}

          {session.phase === 'result' ? (
            <div data-import-result>
              <div className="dbz-transfer-summary">
                <Tag color="success">新增 {importStats.created}</Tag>
                <Tag color="blue">覆盖并纳管 {importStats['overwritten-adopted']}</Tag>
                <Tag color="geekblue">
                  覆盖已有纳管记录 {importStats['overwritten-managed']}
                </Tag>
                <Tag>跳过 {importStats.skipped}</Tag>
                <Tag color="purple">改名保留 {importStats['kept-both']}</Tag>
                <Tag color="error">失败 {importStats.failed}</Tag>
              </div>
              <div className="dbz-transfer-outcomes">
                {session.outcomes.map((outcome, index) => (
                  <div
                    className="dbz-transfer-outcome"
                    key={`${outcome.row.direction}:${outcome.row.connectorName}:${index}`}
                  >
                    <Tag color={IMPORT_ACTION_COLOR[outcome.action]}>
                      {IMPORT_ACTION_LABEL[outcome.action]}
                    </Tag>
                    <span className="dbz-transfer-outcome-name">{outcome.row.name}</span>
                    <span className="dbz-mono">
                      {outcome.renamedConnectorName ?? outcome.row.connectorName}
                    </span>
                    {outcome.message && (
                      <span className="dbz-transfer-outcome-message">{outcome.message}</span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <>
              {importConflictCount > 0 && (
                <div className="dbz-transfer-bulk" data-import-bulk>
                  <span>冲突处理批量设置（无冲突行始终新增）：</span>
                  <Button
                    size="small"
                    onClick={() => setAllDecisions('overwrite')}
                    data-import-bulk-overwrite
                  >
                    全部覆盖
                  </Button>
                  <Button
                    size="small"
                    onClick={() => setAllDecisions('skip')}
                    data-import-bulk-skip
                  >
                    全部跳过
                  </Button>
                  <Button
                    size="small"
                    onClick={() => setAllDecisions('keep')}
                    data-import-bulk-keep
                  >
                    全部保留
                  </Button>
                </div>
              )}
              <Table<ImportPreviewRow>
                size="small"
                rowKey={(item) => `${item.row.direction}:${item.row.connectorName}`}
                pagination={false}
                dataSource={session.rows}
                scroll={{ y: 360 }}
                onRow={(item) => ({
                  'data-import-row': `${item.row.direction}:${item.row.connectorName}`,
                } as HTMLAttributes<HTMLElement>)}
                columns={[
                  {
                    title: '方向',
                    key: 'direction',
                    width: 70,
                    render: (_value: unknown, item: ImportPreviewRow) => (
                      <Tag color={item.row.direction === 'source' ? 'cyan' : 'magenta'}>
                        {item.row.direction === 'source' ? '上游' : '下游'}
                      </Tag>
                    ),
                  },
                  {
                    title: '名称 / connectorName',
                    key: 'name',
                    render: (_value: unknown, item: ImportPreviewRow) => (
                      <div>
                        <div>{item.row.name}</div>
                        <div className="dbz-mono">{item.row.connectorName}</div>
                      </div>
                    ),
                  },
                  {
                    title: '上游',
                    key: 'upstream',
                    render: (_value: unknown, item: ImportPreviewRow) =>
                      item.row.direction === 'sink' ? (
                        item.row.upstream ? (
                          <div>
                            <div>{item.row.upstream.name || '--'}</div>
                            <div className="dbz-mono">
                              {item.row.upstream.connectorName}
                            </div>
                          </div>
                        ) : (
                          <Tag color="error">缺少上游信息</Tag>
                        )
                      ) : (
                        '--'
                      ),
                  },
                  {
                    title: '同名冲突',
                    key: 'conflict',
                    width: 220,
                    render: (_value: unknown, item: ImportPreviewRow) =>
                      item.conflict ? (
                        <div className="dbz-transfer-conflict">
                          <Tag color="warning">同名冲突</Tag>
                          <span>
                            {item.conflict.elsewhere
                              ? `另一台 Connect 已有记录「${item.conflict.elsewhere.name}」，需改名或跳过`
                              : item.conflict.managed
                                ? `已有纳管记录「${item.conflict.managed.name}」`
                                : '集群上未纳管'}
                          </span>
                        </div>
                      ) : (
                        <Tag color="success">无冲突，将新增</Tag>
                      ),
                  },
                  {
                    title: '处理方式',
                    key: 'decision',
                    width: 220,
                    render: (_value: unknown, item: ImportPreviewRow) =>
                      item.conflict ? (
                        <span
                          data-import-decision={`${item.row.direction}:${item.row.connectorName}`}
                        >
                          <Select<ImportDecision>
                            size="small"
                            value={item.decision}
                            style={{ width: 200 }}
                            onChange={(value) =>
                              setDecision(item.row.direction, item.row.connectorName, value)
                            }
                            options={[
                              ...(canOverwriteImport(item.conflict)
                                ? [
                                    {
                                      value: 'overwrite' as const,
                                      label: item.conflict.managed
                                        ? IMPORT_ACTION_LABEL['overwritten-managed']
                                        : IMPORT_ACTION_LABEL['overwritten-adopted'],
                                    },
                                  ]
                                : []),
                              { value: 'skip', label: IMPORT_DECISION_LABEL.skip },
                              {
                                value: 'keep',
                                label: `${IMPORT_DECISION_LABEL.keep}（改名 -imported-N）`,
                              },
                            ]}
                          />
                        </span>
                      ) : (
                        <Typography.Text type="secondary">新增</Typography.Text>
                      ),
                  },
                ]}
              />
            </>
          )}
        </div>
      )}
    </Modal>
  );
}
