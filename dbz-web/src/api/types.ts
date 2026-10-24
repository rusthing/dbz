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
export type KafkaConnectorStatus = 0 | 1 | 2 | 3 | 4 | 5;

/** # Kafka 集群（broker）状态（`KafkaBrokerStatus`） */
export type KafkaBrokerStatus = 0 | 1 | 2 | 3;

/** # Kafka 安全协议（`KafkaSecurityProtocol`） */
export type KafkaSecurityProtocol = 0 | 1;

/** # Kafka SASL 机制（`KafkaSaslMechanism`） */
export type KafkaSaslMechanism = 0 | 1 | 2 | 3;

/**
 * # Kafka 集群（broker 侧）
 *
 * 与 {@link DbzKafkaConnectVo} 是同一套 Kafka 的两面：Connect 记的是 REST 地址
 * （连接器跑在哪台 worker），这条记的是消息集群的地址与认证（信号投到哪）。
 *
 * 没有 `saslPassword` —— 密码只进不出，服务端不返回。
 */
export interface DbzKafkaBrokerVo extends AuditFields {
  id: Id;
  name: string;
  /** broker 地址，`host:port` 逗号分隔 */
  bootstrapServers: string;
  securityProtocol: KafkaSecurityProtocol;
  saslMechanism: KafkaSaslMechanism;
  saslUsername?: string | null;
  runStatus: KafkaBrokerStatus;
  remark?: string | null;
  enabled: boolean;
}

/** # 新增 Kafka 集群的请求体 */
export interface DbzKafkaBrokerAddInput {
  name: string;
  bootstrapServers: string;
  securityProtocol: KafkaSecurityProtocol;
  saslMechanism: KafkaSaslMechanism;
  saslUsername?: string;
  /** 认证密码；修改时留空表示沿用原密码 */
  saslPassword?: string;
  remark?: string;
  enabled: boolean;
}

/** # 修改 Kafka 集群的请求体（字段可选，只提交要改的） */
export interface DbzKafkaBrokerModifyInput extends Partial<DbzKafkaBrokerAddInput> {
  id: Id;
}

/** # 检测 Kafka 集群连通性的结果 */
export interface DbzKafkaBrokerCheckVo {
  /** 是否连通（连不上也是 200，这是业务结论） */
  reachable: boolean;
  /** 集群上的 topic 数量 */
  topicCount: number;
  /** 本次检测耗时（毫秒） */
  elapsedMs: number;
  /** 失败原因 */
  message?: string | null;
}

/** # Kafka Connect 集群 */
export interface DbzKafkaConnectVo extends AuditFields {
  id: Id;
  name: string;
  url: string;
  /** 关联的 Kafka 集群（broker）；老记录可能为空 */
  kafkaBrokerId?: Id | null;
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
  /** 关联的 Kafka 集群（broker），新增必填：信号要投到某个集群上 */
  kafkaBrokerId: Id;
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

/**
 * # 快照类型（`SnapshotType`）
 *
 * 对应 Debezium 信号里的 `data.type`。`incremental` 与流式读取并行、不阻塞；
 * `blocking` 会暂停流式读取，只在较新版本可用。
 */
export type SnapshotType = 'incremental' | 'blocking';

/**
 * # 信号通道（`SignalChannel`）
 *
 * 再同步的信号怎么送到 Debezium：
 * - `source` —— 往源库信号表插一行，需要 dbz 直连业务库；
 * - `kafka` —— 往信号 topic 投一条消息，不碰业务库，但要有 broker 记录。
 */
export type SignalChannel = 'source' | 'kafka';

/**
 * # 快照预检结果（`DbzSourceConnectorSnapshotVo`）
 *
 * 服务端按连接器配置解析出来的「这次会快照什么」：源库方言、连的库、信号落点，
 * 以及可快照的表清单。`source` 通道的表清单来自源库的 `information_schema`；
 * `kafka` 通道不连库，返回的是交给连接器展开的正则。
 */
export interface DbzSourceConnectorSnapshotVo {
  /** 源库方言：`mysql` / `postgresql` */
  dialect: string;
  /** 连接器连的库 */
  database: string;
  /** 这台连接器上可用的通道，按推荐顺序排列 */
  channels: string[];
  /** 本次预检实际用的通道 */
  channel: string;
  /** 信号表全限定名（`source` 通道） */
  signalCollection?: string | null;
  /** 信号 topic（`kafka` 通道） */
  kafkaTopic?: string | null;
  /** 投递信号用的 Kafka 集群名（`kafka` 通道） */
  kafkaBroker?: string | null;
  /** 本次可快照的表（`source` 是表名，`kafka` 是正则） */
  dataCollections: string[];
  /** 解析过程中的提醒（如 `table.include.list` 有条目没匹配到表） */
  warnings: string[];
}

/** # 触发快照的请求体 */
export interface DbzSourceConnectorSnapshotInput {
  id: Id;
  /** 要快照的表；空数组表示捕获范围内的全部表 */
  dataCollections: string[];
  /** 快照类型 */
  snapshotType: SnapshotType;
  /** 用哪条通道；不传由服务端按默认顺序挑 */
  channel?: SignalChannel;
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

/** # 停止 / 恢复远端连接器的参数（两者入参同形，都不带请求体） */
export type KafkaConnectorControlInput = KafkaConnectorDeleteInput;

/** # 重置远端连接器位点的参数 */
export interface KafkaConnectorResetOffsetInput extends KafkaConnectorDeleteInput {
  /** 清空位点后是否立即恢复运行；`true` 等于「重做一次全量」 */
  resume?: boolean;
}
