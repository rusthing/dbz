/**
 * # 接口数据类型
 *
 * 与 `dbz-api` 中的 DTO / VO 一一对应。字段命名遵循服务端 `#[serde(rename_all = "camelCase")]`，
 * 因此 Rust 的 `run_status` 在这里是 `runStatus`。
 *
 * ## 两个容易踩的点
 *
 * 1. `#[vo]` 生成的 VO 带 `#[skip_serializing_none]`，`Option` 字段为 `None` 时**整个字段不出现**，
 *    所以 `remark` 必须按「可能不存在」处理。
 * 2. 字典枚举（`#[dic]`）序列化为**整数判别式**而不是字符串，取值见 `src/dic`。
 */

/** # 框架统一响应包装 */
export interface Ro<T> {
  /** 结果码：1 成功 / -1 参数错误 / -2 警告 / -3 失败 */
  result: number;
  /** 结果说明 */
  msg: string;
  /** 服务端生成响应时的毫秒时间戳 */
  timestamp: number;
  /** 业务数据 */
  extra?: T;
  /** 详细错误信息 */
  detail?: string;
  /** 业务编码，如 `RTW00001` 表示唯一键重复 */
  code?: string;
}

/** # 分页响应 */
export interface PageRx<T> {
  /** 当前页码（从 1 开始） */
  pageNum: number;
  /** 记录总数 */
  total: number;
  /** 当前页记录 */
  list: T[];
}

/** # 审计字段 */
export interface AuditFields {
  createrId: number;
  createMs: number;
  updaterId: number;
  updateMs: number;
}

/** # Kafka Connect 集群状态（`KafkaConnectStatus`） */
export type KafkaConnectStatus = 0 | 1 | 2 | 3;

/** # 连接器运行状态（`ConnectorRunStatus`），上游/下游共用 */
export type ConnectorRunStatus = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;

/** # 远端连接器实时状态（`KafkaConnectorStatus`） */
export type KafkaConnectorStatus = 0 | 1 | 2 | 3 | 4;

/** # Kafka Connect 集群 */
export interface DbzKafkaConnectVo extends AuditFields {
  id: number;
  name: string;
  url: string;
  runStatus: KafkaConnectStatus;
  remark?: string | null;
  enabled: boolean;
}

/** # 上游（source）连接器 */
export interface DbzSourceConnectorVo extends AuditFields {
  id: number;
  kafkaConnectId: number;
  name: string;
  connectorName: string;
  connectorConfig: string;
  runStatus: ConnectorRunStatus;
  remark?: string | null;
  enabled: boolean;
}

/** # 下游（sink）连接器 */
export interface DbzSinkConnectorVo extends AuditFields {
  id: number;
  sourceConnectorId: number;
  kafkaConnectId: number;
  name: string;
  connectorName: string;
  connectorConfig: string;
  runStatus: ConnectorRunStatus;
  remark?: string | null;
  enabled: boolean;
}

/** # 新增集群的请求体 */
export interface DbzKafkaConnectAddInput {
  name: string;
  url: string;
  remark?: string;
  enabled: boolean;
}

/** # 修改集群的请求体 */
export interface DbzKafkaConnectModifyInput extends DbzKafkaConnectAddInput {
  id: number;
}

/** # 新增上游连接器的请求体 */
export interface DbzSourceConnectorAddInput {
  kafkaConnectId: number;
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string;
  enabled: boolean;
}

/** # 修改上游连接器的请求体 */
export interface DbzSourceConnectorModifyInput extends DbzSourceConnectorAddInput {
  id: number;
}

/** # 新增下游连接器的请求体 */
export interface DbzSinkConnectorAddInput extends DbzSourceConnectorAddInput {
  sourceConnectorId: number;
}

/** # 修改下游连接器的请求体 */
export interface DbzSinkConnectorModifyInput extends DbzSinkConnectorAddInput {
  id: number;
}

/** # 连接器插件（`GET /connector-plugins` 的单项） */
export interface KafkaConnectorPluginVo {
  /** 插件类名 */
  class: string;
  /** 插件方向：`source` / `sink` */
  type: string;
  /** 插件版本 */
  version?: string | null;
}

/** # 连接器任务 */
export interface KafkaConnectorTaskVo {
  id: number;
  state: KafkaConnectorStatus;
  workerId?: string | null;
  /** 任务失败时的异常堆栈 */
  trace?: string | null;
}

/** # 远端连接器（`GET /connectors?expand=info&expand=status` 的单项） */
export interface KafkaConnectorVo {
  name: string;
  connectorClass: string;
  /** 集群自己判定的方向：`source` / `sink`，低版本集群可能为空串 */
  type: string;
  state: KafkaConnectorStatus;
  tasks: KafkaConnectorTaskVo[];
}

/** # 重启连接器的请求体 */
export interface KafkaConnectorRestartInput {
  kafkaConnectId: number;
  connectorName: string;
  /** 是否连同任务一起重启，默认 true */
  includeTasks?: boolean;
  /** 是否只重启失败的任务，默认 false */
  onlyFailed?: boolean;
}

/** # 重启连接器任务的请求体 */
export interface KafkaConnectorTaskRestartInput {
  kafkaConnectId: number;
  connectorName: string;
  taskId: number;
}
