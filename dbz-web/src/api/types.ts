/**
 * # 接口数据类型
 *
 * 与 `dbz-api` 中的 DTO / VO 一一对应。字段命名遵循服务端 `#[serde(rename_all = "camelCase")]`，
 * 因此 Rust 的 `run_status` 在这里是 `runStatus`。
 *
 * ## 三个容易踩的点
 *
 * 1. `#[vo]` 生成的 VO 带 `#[skip_serializing_none]`，`Option` 字段为 `None` 时**整个字段不出现**，
 *    所以 `remark` 必须按「可能不存在」处理。
 * 2. 字典枚举（`#[dic]`）序列化为**整数判别式**而不是字符串，取值见 `src/dic`。
 * 3. 所有 `u64` 字段（ID、审计 ID、毫秒时间戳）在 JSON 里是**字符串**，见 {@link Id}。
 */

/**
 * # 服务端 `u64` 字段的线上形态
 *
 * `#[vo]` / DTO 里的 `u64` 会被框架替换成 `robotech::api::U64`，序列化时输出字符串
 * （`"id": "1928374650123456789"`），为的是让 19 位雪花 ID 躲开 `JSON.parse` 的 `2^53` 精度上限。
 *
 * 由此带来两条硬约束：
 *
 * - **不要 `Number()`、不要算术**：`Number("1928374650123456789")` 直接变成 `...789000`，
 *   拿它去查/删会查不到任何记录；需要展示或传给 `dayjs` 时，毫秒时间戳（约 `1.7e12`）转数字是安全的。
 * - **反序列化只认字符串**：`U64` 的 visitor 只实现了 `visit_str`，把 ID 当 JSON 数字发回去会被
 *   后端判成参数错误，所以请求体里的 ID 也必须是 `string`。
 */
export type Id = string;

/** # 框架统一响应包装 */
export interface Ro<T> {
  /** 结果码：1 成功 / -1 参数错误 / -2 警告 / -3 失败 */
  result: number;
  /** 结果说明 */
  msg: string;
  /** 服务端生成响应时的毫秒时间戳（`u64_serde`，序列化为字符串） */
  timestamp: Id;
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

/** # 审计字段（`u64`，线上是字符串） */
export interface AuditFields {
  createrId: Id;
  /** 毫秒时间戳字符串，展示前用 `formatMs` 转数字 */
  createMs: Id;
  updaterId: Id;
  updateMs: Id;
}

/** # Kafka Connect 集群状态（`KafkaConnectStatus`） */
export type KafkaConnectStatus = 0 | 1 | 2 | 3;

/** # 连接器运行状态（`ConnectorRunStatus`），上游/下游共用 */
export type ConnectorRunStatus = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;

/** # 远端连接器实时状态（`KafkaConnectorStatus`） */
export type KafkaConnectorStatus = 0 | 1 | 2 | 3 | 4;

/** # Kafka Connect 集群 */
export interface DbzKafkaConnectVo extends AuditFields {
  id: Id;
  name: string;
  url: string;
  runStatus: KafkaConnectStatus;
  remark?: string | null;
  enabled: boolean;
}

/** # 上游（source）连接器 */
export interface DbzSourceConnectorVo extends AuditFields {
  id: Id;
  kafkaConnectId: Id;
  name: string;
  connectorName: string;
  connectorConfig: string;
  runStatus: ConnectorRunStatus;
  remark?: string | null;
  enabled: boolean;
}

/** # 下游（sink）连接器 */
export interface DbzSinkConnectorVo extends AuditFields {
  id: Id;
  sourceConnectorId: Id;
  kafkaConnectId: Id;
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

/**
 * # 修改集群的请求体
 *
 * 字段全部可选：服务端 ModifyDto 的每个字段都是 `Option`，**没传的字段不会写回**。
 * 列表上的启用开关就靠这一点，只提交 `{ id, enabled }`，不必把整条记录带回来。
 */
export interface DbzKafkaConnectModifyInput extends Partial<DbzKafkaConnectAddInput> {
  id: Id;
}

/** # 新增上游连接器的请求体 */
export interface DbzSourceConnectorAddInput {
  kafkaConnectId: Id;
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string;
  enabled: boolean;
}

/** # 修改上游连接器的请求体（字段可选，只提交要改的） */
export interface DbzSourceConnectorModifyInput extends Partial<DbzSourceConnectorAddInput> {
  id: Id;
}

/** # 新增下游连接器的请求体 */
export interface DbzSinkConnectorAddInput extends DbzSourceConnectorAddInput {
  sourceConnectorId: Id;
}

/** # 修改下游连接器的请求体（字段可选，只提交要改的） */
export interface DbzSinkConnectorModifyInput extends Partial<DbzSinkConnectorAddInput> {
  id: Id;
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
  kafkaConnectId: Id;
  connectorName: string;
  /** 是否连同任务一起重启，默认 true */
  includeTasks?: boolean;
  /** 是否只重启失败的任务，默认 false */
  onlyFailed?: boolean;
}

/** # 重启连接器任务的请求体 */
export interface KafkaConnectorTaskRestartInput {
  kafkaConnectId: Id;
  connectorName: string;
  taskId: number;
}

/**
 * # 删除远端连接器的参数
 *
 * 走查询参数（DELETE 不带请求体）：只把连接器从集群上注销，不删库里的记录。
 */
export interface KafkaConnectorDeleteInput {
  kafkaConnectId: Id;
  connectorName: string;
}
