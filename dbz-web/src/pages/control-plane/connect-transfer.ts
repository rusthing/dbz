/**
 * # Connect 连接器导入导出
 *
 * 导出把一台 Connect 下所有**已纳管**的上游/下游连接器压成一个 JSON：
 * 方向、名称、connectorName、库内备注、完整配置与启停状态；下游额外带上所属
 * 上游的库内名称与 connectorName，导入到另一套环境时按这个名称重新挂载。
 *
 * 导入只做纯数据整理，不发请求：先把文件解析成可预览的行，再由页面按用户对
 * 同名冲突的选择（覆盖 / 跳过 / 两者都保留）逐条调用现有 CRUD。
 *
 * ## 同名冲突
 *
 * 判定只看目标 Connect 上的 connectorName：库内记录与集群上真实存在的连接器
 * 都算，和「库里有没有记录」无关。这样绕过 dbz 手工建的连接器也不会被漏掉。
 *
 * ## 凭据
 *
 * 默认导出带凭据；脱敏导出把口令/密钥类字段替换成掩码，并在每个连接器的
 * `sensitiveConfigKeys` 里标注哪些字段属于凭据，避免文件被误传后才发现。
 */

import type {
  DbzKafkaBrokerVo,
  DbzKafkaConnectVo,
  DbzSinkConnectorVo,
  DbzSourceConnectorVo,
  Id,
} from '../../api/types';

/** 导出文件格式标识，导入时先认它 */
export const TRANSFER_FORMAT = 'dbz-connect-transfer';
export const TRANSFER_VERSION = 1;

/** 凭据类配置键：键名命中即视为敏感，`sasl.jaas.config` 整段值也含口令 */
const SENSITIVE_KEY_PATTERN = /(password|secret|token|credential)/i;

export type TransferDirection = 'source' | 'sink';
export type CredentialMode = 'included' | 'redacted';
export type ImportDecision = 'overwrite' | 'skip' | 'keep';

/** 下游在文件里记录的上游信息：id 仅用于追溯来源，导入时按 connectorName 重挂 */
export interface TransferUpstream {
  id: Id | null;
  name: string;
  connectorName: string;
}

/** 文件里的一个连接器 */
export interface TransferConnector {
  direction: TransferDirection;
  /** 库内展示名称 */
  name: string;
  /** Kafka Connect 上的连接器名称，同名冲突按它判定 */
  connectorName: string;
  /** 原始 JSON 字符串；导入时直接交给现有接口 */
  connectorConfig: string;
  remark: string;
  enabled: boolean;
  /** 下游才有：所属上游 */
  upstream?: TransferUpstream | null;
  /** 本次导出的凭据字段（脱敏导出时值是 `******`） */
  sensitiveConfigKeys: string[];
}

/** 导出文件整体结构 */
export interface TransferFile {
  format: typeof TRANSFER_FORMAT;
  version: typeof TRANSFER_VERSION;
  exportedAt: string;
  credentialMode: CredentialMode;
  connect: {
    id: Id;
    name: string;
    url: string;
    brokerName: string;
  };
  connectors: TransferConnector[];
}

/** 一条冲突的库内记录摘要 */
export interface ImportManagedRecord {
  id: Id;
  name: string;
  connectorName: string;
}

/** 预览行上的冲突结论 */
export interface ImportConflict {
  /** 目标 Connect 上已有的同向库内记录；没有则为未纳管 */
  managed?: ImportManagedRecord;
  /** 集群上已有同名连接器但库里没有记录 */
  unmanaged: boolean;
}

/** 预览行 */
export interface ImportPreviewRow {
  row: TransferConnector;
  conflict: ImportConflict | null;
  decision: ImportDecision;
}

/** 单条执行结果 */
export type ImportOutcomeAction =
  | 'created'
  | 'overwritten-managed'
  | 'overwritten-adopted'
  | 'skipped'
  | 'kept-both'
  | 'failed';

export interface ImportOutcome {
  row: TransferConnector;
  action: ImportOutcomeAction;
  message?: string;
  /** 「两者都保留」实际使用的连接器名称 */
  renamedConnectorName?: string;
}

/** 解析结果：成功时带文件，失败时带逐条原因 */
export interface TransferParseResult {
  file?: TransferFile;
  errors: string[];
}

/** 凭据字段判断：键名不区分大小写，`sasl.jaas.config` 整段视为敏感 */
export function isSensitiveConfigKey(key: string): boolean {
  const normalized = key.trim().toLowerCase();
  return normalized === 'sasl.jaas.config' || SENSITIVE_KEY_PATTERN.test(normalized);
}

/** 解析配置对象；不是合法 JSON 对象时返回 null */
function parseConfigObject(raw: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return null;
    }
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

/** 列出配置里的凭据字段（保持配置里的原始键顺序） */
export function sensitiveKeysOf(raw: string): string[] {
  const config = parseConfigObject(raw);
  if (!config) {
    return [];
  }
  return Object.keys(config).filter(isSensitiveConfigKey);
}

/** 脱敏导出：凭据字段值替换成掩码；配置不是合法 JSON 时拒绝脱敏 */
export function redactConnectorConfig(raw: string): {
  connectorConfig: string;
  sensitiveConfigKeys: string[];
} {
  const config = parseConfigObject(raw);
  if (!config) {
    throw new Error('配置不是合法的 JSON 对象，无法安全脱敏；请修正配置或选择含凭据导出');
  }
  const keys = Object.keys(config);
  const sensitive = keys.filter(isSensitiveConfigKey);
  const next: Record<string, unknown> = {};
  for (const key of keys) {
    next[key] = sensitive.includes(key) ? '******' : config[key];
  }
  return {
    connectorConfig: JSON.stringify(next),
    sensitiveConfigKeys: sensitive,
  };
}

/** 从四张表里收集一台 Connect 的导出内容：上游在前、下游在后 */
export function collectConnectExport(
  sources: DbzSourceConnectorVo[],
  sinks: DbzSinkConnectorVo[],
  kafkaConnectId: Id,
  redacted: boolean,
): TransferConnector[] {
  const sourceById = new Map(sources.map((source) => [source.id, source]));
  const connectorRows: TransferConnector[] = [];

  const connectSources = sources
    .filter((source) => source.kafkaConnectId === kafkaConnectId)
    .slice()
    .sort((left, right) => left.name.localeCompare(right.name));
  for (const source of connectSources) {
    const prepared = redacted
      ? redactConnectorConfig(source.connectorConfig)
      : {
          connectorConfig: source.connectorConfig,
          sensitiveConfigKeys: sensitiveKeysOf(source.connectorConfig),
        };
    connectorRows.push({
      direction: 'source',
      name: source.name,
      connectorName: source.connectorName,
      connectorConfig: prepared.connectorConfig,
      remark: source.remark ?? '',
      enabled: source.enabled,
      sensitiveConfigKeys: prepared.sensitiveConfigKeys,
    });
  }

  const connectSinks = sinks
    .filter((sink) => sink.kafkaConnectId === kafkaConnectId)
    .slice()
    .sort((left, right) => left.name.localeCompare(right.name));
  for (const sink of connectSinks) {
    const prepared = redacted
      ? redactConnectorConfig(sink.connectorConfig)
      : {
          connectorConfig: sink.connectorConfig,
          sensitiveConfigKeys: sensitiveKeysOf(sink.connectorConfig),
        };
    const upstream = sourceById.get(sink.sourceConnectorId);
    connectorRows.push({
      direction: 'sink',
      name: sink.name,
      connectorName: sink.connectorName,
      connectorConfig: prepared.connectorConfig,
      remark: sink.remark ?? '',
      enabled: sink.enabled,
      upstream: upstream
        ? { id: upstream.id, name: upstream.name, connectorName: upstream.connectorName }
        : null,
      sensitiveConfigKeys: prepared.sensitiveConfigKeys,
    });
  }

  return connectorRows;
}

/** 组装导出文件 */
export function buildTransferFile(
  brokers: DbzKafkaBrokerVo[],
  sources: DbzSourceConnectorVo[],
  sinks: DbzSinkConnectorVo[],
  connect: DbzKafkaConnectVo,
  redacted: boolean,
  exportedAt: Date = new Date(),
): TransferFile {
  const broker = brokers.find((item) => item.id === connect.kafkaBrokerId);
  return {
    format: TRANSFER_FORMAT,
    version: TRANSFER_VERSION,
    exportedAt: exportedAt.toISOString(),
    credentialMode: redacted ? 'redacted' : 'included',
    connect: {
      id: connect.id,
      name: connect.name,
      url: connect.url,
      brokerName: broker?.name ?? '',
    },
    connectors: collectConnectExport(sources, sinks, connect.id, redacted),
  };
}

/** 文件名：Connect 名称 + 时间戳，非法字符替换成下划线 */
export function exportFileName(connectName: string, date: Date = new Date()): string {
  const stamp = [
    date.getFullYear(),
    String(date.getMonth() + 1).padStart(2, '0'),
    String(date.getDate()).padStart(2, '0'),
    '-',
    String(date.getHours()).padStart(2, '0'),
    String(date.getMinutes()).padStart(2, '0'),
    String(date.getSeconds()).padStart(2, '0'),
  ].join('');
  const safeName = connectName.trim().replace(/[\\/:*?"<>|\s]+/g, '_') || 'connect';
  return `dbz-connect-${safeName}-${stamp}.json`;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

/** 字符串字段统一 trim */
function trimText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

/** 规范化一条连接器记录 */
function normalizeConnector(value: unknown, index: number, errors: string[]): TransferConnector | null {
  const label = `第 ${index + 1} 条连接器`;
  if (!isObject(value)) {
    errors.push(`${label}不是对象`);
    return null;
  }
  const direction = value.direction;
  if (direction !== 'source' && direction !== 'sink') {
    errors.push(`${label}的方向必须是 source 或 sink`);
    return null;
  }
  const name = trimText(value.name);
  const connectorName = trimText(value.connectorName);
  if (!name) {
    errors.push(`${label}缺少展示名称 name`);
  }
  if (!connectorName) {
    errors.push(`${label}缺少 connectorName`);
  }
  let connectorConfig: string;
  if (typeof value.connectorConfig === 'string') {
    connectorConfig = value.connectorConfig;
  } else if (isObject(value.connectorConfig)) {
    connectorConfig = JSON.stringify(value.connectorConfig);
  } else {
    errors.push(`${label}的 connectorConfig 必须是 JSON 字符串或对象`);
    connectorConfig = '';
  }
  if (connectorConfig && !parseConfigObject(connectorConfig)) {
    errors.push(`${label}「${name || connectorName}」的 connectorConfig 不是合法的 JSON 对象`);
  }

  let upstream: TransferUpstream | null = null;
  if (direction === 'sink') {
    const rawUpstream = value.upstream;
    if (!isObject(rawUpstream) || !trimText(rawUpstream.connectorName)) {
      errors.push(
        `${label}「${name || connectorName}」是下游连接器，但缺少上游信息（upstream.connectorName）`,
      );
    } else {
      upstream = {
        id: trimText(rawUpstream.id) || null,
        name: trimText(rawUpstream.name),
        connectorName: trimText(rawUpstream.connectorName),
      };
    }
  }

  if (!name || !connectorName || !connectorConfig) {
    return null;
  }

  const sensitive = Array.isArray(value.sensitiveConfigKeys)
    ? value.sensitiveConfigKeys.filter((key): key is string => typeof key === 'string')
    : sensitiveKeysOf(connectorConfig);

  return {
    direction,
    name,
    connectorName,
    connectorConfig,
    remark: trimText(value.remark),
    enabled: typeof value.enabled === 'boolean' ? value.enabled : true,
    upstream,
    sensitiveConfigKeys: sensitive.length ? sensitive : sensitiveKeysOf(connectorConfig),
  };
}

/** 解析导入文件：先认格式，再逐条校验；同名 connectorName 在文件内也视为错误 */
export function parseTransferFile(raw: string): TransferParseResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { errors: ['文件不是合法的 JSON'] };
  }
  if (!isObject(parsed)) {
    return { errors: ['文件内容不是一个 JSON 对象'] };
  }
  if (parsed.format !== TRANSFER_FORMAT) {
    return { errors: [`不是 dbz Connect 导入文件（format 应为 ${TRANSFER_FORMAT}）`] };
  }
  if (parsed.version !== TRANSFER_VERSION) {
    return { errors: [`不支持的导出版本 ${String(parsed.version)}，当前只支持 ${TRANSFER_VERSION}`] };
  }
  const rawConnect = isObject(parsed.connect) ? parsed.connect : {};
  const connect = {
    id: trimText(rawConnect.id),
    name: trimText(rawConnect.name),
    url: trimText(rawConnect.url),
    brokerName: trimText(rawConnect.brokerName),
  };
  const credentialMode: CredentialMode =
    parsed.credentialMode === 'redacted' ? 'redacted' : 'included';
  if (!Array.isArray(parsed.connectors)) {
    return { errors: ['文件缺少 connectors 数组'] };
  }

  const errors: string[] = [];
  const connectors: TransferConnector[] = [];
  const seen = new Set<string>();
  parsed.connectors.forEach((value, index) => {
    const connector = normalizeConnector(value, index, errors);
    if (!connector) {
      return;
    }
    if (seen.has(connector.connectorName)) {
      errors.push(`文件内 connectorName「${connector.connectorName}」重复，无法确定导入顺序`);
      return;
    }
    seen.add(connector.connectorName);
    connectors.push(connector);
  });

  if (errors.length) {
    return { errors };
  }
  return {
    file: {
      format: TRANSFER_FORMAT,
      version: TRANSFER_VERSION,
      exportedAt: trimText(parsed.exportedAt),
      credentialMode,
      connect,
      connectors,
    },
    errors: [],
  };
}

/**
 * # 逐条构建预览
 *
 * 冲突判定与库里有没有记录无关：目标 Connect 上已有同向库内记录，或集群上已有
 * 同名连接器（未纳管），都算冲突。`liveNames` 为 null 表示实时列表没取到，
 * 只能按库内记录判断，页面要明确提示这一点。
 */
export function buildImportRows(
  file: TransferFile,
  kafkaConnectId: Id,
  sources: DbzSourceConnectorVo[],
  sinks: DbzSinkConnectorVo[],
  liveNames: Set<string> | null,
): ImportPreviewRow[] {
  const managedSources = sources.filter((item) => item.kafkaConnectId === kafkaConnectId);
  const managedSinks = sinks.filter((item) => item.kafkaConnectId === kafkaConnectId);
  const allManaged = [...managedSources, ...managedSinks];

  return file.connectors.map((row) => {
    const sameDirection =
      row.direction === 'source' ? managedSources : managedSinks;
    const managedRecord = sameDirection.find(
      (item) => item.connectorName === row.connectorName,
    );
    const managedAnyDirection = allManaged.some(
      (item) => item.connectorName === row.connectorName,
    );
    const unmanaged = Boolean(liveNames?.has(row.connectorName)) && !managedAnyDirection;
    const conflict: ImportConflict | null =
      managedRecord || unmanaged
        ? {
            managed: managedRecord
              ? {
                  id: managedRecord.id,
                  name: managedRecord.name,
                  connectorName: managedRecord.connectorName,
                }
              : undefined,
            unmanaged,
          }
        : null;
    return {
      row,
      conflict,
      // 冲突默认跳过：覆盖会改库内记录并重新下发，必须先让人确认
      decision: conflict ? 'skip' : 'overwrite',
    };
  });
}

/** 生成不撞名的 `-imported-N`；已用名称集合由调用方维护 */
export function nextImportedName(base: string, taken: Set<string>): string {
  let index = 1;
  while (taken.has(`${base}-imported-${index}`)) {
    index += 1;
  }
  return `${base}-imported-${index}`;
}

/** 下载 JSON 文件 */
export function downloadJson(fileName: string, content: unknown): void {
  const blob = new Blob([JSON.stringify(content, null, 2)], {
    type: 'application/json;charset=utf-8',
  });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = fileName;
  link.click();
  URL.revokeObjectURL(link.href);
}
