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

/**
 * # 连接器运行状态（`ConnectorRunStatus`），上游/下游共用
 *
 * 0 未知 / 1 运行中 / 2 已停用 / 3 失败 / 4 仅纳管未部署 / 5 不可达 / 6 已停止。
 * 后端只回写这一个枚举，前端不再自行拼「库内 + 实时」两套状态。
 */
export type ConnectorRunStatus = 0 | 1 | 2 | 3 | 4 | 5 | 6;

/** # 集群连接器实时状态（`KafkaConnectorStatus`） */
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
  /** 检查结论：最近一次检测的失败原因，正常时为空 */
  runMsg?: string | null;
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
  /** 状态字段：新增不传由数据库默认值兜底，修改不传保持原值 */
  enabled?: boolean;
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
  /** 集群上的 topic 名称（已排序；不通时为空） */
  topics: string[];
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
  /** 关联的 Kafka 集群（broker） */
  kafkaBrokerId: Id;
  runStatus: KafkaConnectStatus;
  /** 检查结论：最近一次探测的失败原因，正常时为空 */
  runMsg?: string | null;
  remark?: string | null;
  enabled: boolean;
}

/** # 一次配置比对的单行结果 */
export interface DbzConnectorCompareRow {
  /** 配置项名（服务端已按字典序排列） */
  key: string;
  /** 库内配置的值（敏感项已脱敏） */
  local: string;
  /** 远端配置的值；`null` 表示远端缺少该配置项（敏感项已脱敏） */
  remote?: string | null;
  /** 是否差异：远端缺键或值不同 */
  changed: boolean;
  /** 是否敏感项（值已脱敏，不落明文） */
  sensitive: boolean;
}

/** # 检测不一致时的对比快照 */
export interface DbzConnectorCompareDetail {
  /** 本次检测时间（毫秒） */
  checkedAt: number;
  /** 检测时远端是否还存在该连接器 */
  remoteExists: boolean;
  /** 无法逐项展示时的说明（远端不存在、配置不是合法 JSON 等） */
  reason?: string | null;
  /** 逐项对比结果；远端不存在或无法解析时为空 */
  rows: DbzConnectorCompareRow[];
}

/** # 同步方向：库内为准下发远端 / 远端为准写回库内 */
export type DbzConnectorSyncDirection = 'toRemote' | 'toLocal';

/** # 同步连接器配置的请求体 */
export interface DbzConnectorConfigSyncInput {
  nodeType: 'source' | 'sink';
  nodeId: Id;
  direction: DbzConnectorSyncDirection;
  /** 只提交用户改过的字段；敏感项留空表示保持原值 */
  patch?: Record<string, unknown>;
  /** 审核口令的 SM3 摘要；仅 toRemote 需要 */
  auditPswd?: string;
}

/** # 同步连接器配置的结果 */
export interface DbzConnectorConfigSyncVo {
  /** 本次实际校验的口令类型；toLocal 为空 */
  pswdKind?: DbzDeletePswdKind | null;
  /** 同步后重新计算的检测标记 */
  inconsistent: boolean;
  /** 同步后重新计算的对比快照（JSON 文本）；一致时为空 */
  inconsistentDetail?: string | null;
}

/** # 上游（source）连接器 */
export interface DbzSourceConnectorVo extends AuditFields {
  id: Id;
  kafkaConnectId: Id;
  name: string;
  connectorName: string;
  connectorConfig: string;
  runStatus: ConnectorRunStatus;
  /** 检查结论：最近一次检查的失败原因，正常时为空 */
  runMsg?: string | null;
  remark?: string | null;
  enabled: boolean;
  /** 检测标记：库内配置与 Connect 实际不一致，或远端已不存在（由 Connect 卡片「检测」写入） */
  inconsistent: boolean;
  /** 检测不一致时的脱敏对比快照（JSON 文本，由检测/同步写入） */
  inconsistentDetail?: string | null;
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
  /** 检查结论：最近一次检查的失败原因，正常时为空 */
  runMsg?: string | null;
  remark?: string | null;
  enabled: boolean;
  /** 检测标记：库内配置与 Connect 实际不一致，或远端已不存在（由 Connect 卡片「检测」写入） */
  inconsistent: boolean;
  /** 检测不一致时的脱敏对比快照（JSON 文本，由检测/同步写入） */
  inconsistentDetail?: string | null;
}

/** # 新增集群的请求体 */
export interface DbzKafkaConnectAddInput {
  name: string;
  url: string;
  /** 关联的 Kafka 集群（broker），新增必填：信号要投到某个集群上 */
  kafkaBrokerId: Id;
  remark?: string;
  /** 状态字段：新增不传由数据库默认值兜底，修改不传保持原值 */
  enabled?: boolean;
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
  /** 状态字段：新增不传由数据库默认值兜底，修改不传保持原值 */
  enabled?: boolean;
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

/** # 集群连接器（`GET /connectors?expand=info&expand=status` 的单项） */
export interface KafkaConnectorVo {
  name: string;
  connectorClass: string;
  /** 集群自己判定的方向：`source` / `sink`，低版本集群可能为空串 */
  type: string;
  state: KafkaConnectorStatus;
  /** 执行该连接器的 worker（任务可能分布在别的 worker 上） */
  workerId?: string | null;
  /** 连接器级异常栈（自身 FAILED 时才有，任务那份额外挂在任务上） */
  trace?: string | null;
  tasks: KafkaConnectorTaskVo[];
}

/**
 * # 连接器配置（`GET /connectors/{name}/config` 的响应）
 *
 * `config` 是 Kafka Connect 原样返回的 JSON 对象，字段随连接器类型变化，
 * 前端只按需读取，不做强类型约束。
 */
export interface KafkaConnectorConfigVo {
  connectorName: string;
  config: Record<string, unknown>;
}

/**
 * # Kafka Connect 自身健康状态
 *
 * 只读探测结果：可达性以 `GET /connectors` 为准，版本等信息来自 Connect 根路径。
 * 连不上也是 200，`reachable=false` 与 `message` 一起表达业务结论。
 */
export interface DbzKafkaConnectHealthVo {
  reachable: boolean;
  /** Connect 版本，根路径取不到时为空 */
  version?: string | null;
  commit?: string | null;
  kafkaClusterId?: string | null;
  /** 集群上的连接器数量 */
  connectorCount: number;
  /** worker 数量（按连接器与任务上报的 worker id 去重） */
  workerCount: number;
  workerIds: string[];
  /** 本次探测耗时（毫秒） */
  elapsedMs: number;
  /** 失败原因或提示 */
  message?: string | null;
}

/**
 * # Connect 检测：单条连接器的处理结果
 *
 * `action` 与后端一致：`added` 补录 / `marked` 标记不一致 / `disabled` 远端已不存在
 * 置禁用 / `cleared` 恢复一致 / `unchanged` 无变化 / `skipped` 跳过。
 */
export interface DbzConnectorDetectItemVo {
  connectorName: string;
  /** source / sink / unknown */
  connectorType: string;
  /** added / marked / disabled / cleared / unchanged / skipped */
  action: string;
  /** 跳过或失败时的原因 */
  message?: string | null;
}

/** # Connect 检测：同步结果汇总 */
export interface DbzKafkaConnectDetectVo {
  /** 远端有、库内没有，已补录 */
  added: number;
  /** 配置不一致，已打标记 */
  marked: number;
  /** 远端已不存在，已置禁用并标记 */
  disabled: number;
  /** 之前标记过，这次恢复一致 */
  cleared: number;
  /** 无变化 */
  unchanged: number;
  /** 读取配置失败 / 类型未知 / 无法确定上游等原因跳过 */
  skipped: number;
  items: DbzConnectorDetectItemVo[];
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
 * # 删除集群连接器的参数
 *
 * 走查询参数（DELETE 不带请求体）：只把连接器从集群上注销，不删库里的记录。
 */
export interface KafkaConnectorDeleteInput {
  kafkaConnectId: Id;
  connectorName: string;
}

/** # 停止 / 恢复集群连接器的参数（两者入参同形，都不带请求体） */
export type KafkaConnectorControlInput = KafkaConnectorDeleteInput;

/** # 重置集群连接器位点的参数 */
export interface KafkaConnectorResetOffsetInput extends KafkaConnectorDeleteInput {
  /** 清空位点后是否立即恢复运行；`true` 等于「重做一次全量」 */
  resume?: boolean;
}

/** # 控制面画布节点类型（`ControlPlaneNodeType`） */
export type ControlPlaneNodeType = 1 | 2 | 3 | 4;

/** # 控制面画布节点位置（`DbzControlPlaneLayoutVo`） */
export interface DbzControlPlaneLayoutVo {
  nodeType: ControlPlaneNodeType;
  nodeId: Id;
  /** 画布世界坐标 X */
  x: number;
  /** 画布世界坐标 Y */
  y: number;
  updateMs: string;
}

/** # 保存画布节点位置（`DbzControlPlaneLayoutUpsertDto`） */
export interface DbzControlPlaneLayoutUpsertInput {
  nodeType: ControlPlaneNodeType;
  nodeId: Id;
  x: number;
  y: number;
}

/** # 画布连线折点与连接点（`DbzControlPlaneEdgeBendVo`） */
export interface DbzControlPlaneEdgeBendVo {
  edgeId: string;
  /** 正交走线中间水平段的世界坐标 Y */
  midY: number;
  /** 上游连接点相对卡片中心的横向偏移 */
  startDx: number;
  /** 下游连接点相对卡片中心的横向偏移 */
  endDx: number;
  updateMs: string;
}

/** # 保存连线折点（`DbzControlPlaneEdgeBendUpsertDto`） */
export interface DbzControlPlaneEdgeBendUpsertInput {
  edgeId: string;
  midY: number;
  /** 不传则保留库内已有值，避免只拖折点时把连接点重置为 0 */
  startDx?: number;
  endDx?: number;
}

/** # 删除画布节点（`DbzControlPlaneNodeDeleteDto`） */
export interface DbzControlPlaneNodeDeleteInput {
  nodeType: 'broker' | 'connect' | 'source' | 'sink';
  nodeId: Id;
  /**
   * 输入的审核口令的 SM3 摘要（64 位小写十六进制），后端转发给 adm-rs 二次确认。
   * 未设置审核口令时由认证服务回退到登录口令，前端始终只提交这一个字段。
   */
  auditPswd: string;
}

/** # 本次删除确认实际校验的口令类型 */
export type DbzDeletePswdKind = 'audit' | 'login';

/** # 控制面节点删除结果 */
export interface DbzControlPlaneNodeDeleteVo {
  /** `audit` = 审核口令；`login` = 未设置审核口令，后端已回退校验登录口令 */
  pswdKind: DbzDeletePswdKind;
}

/** # 死信连接器类型（`DeadLetterConnectorType`） */
export type DeadLetterConnectorType = 1 | 2;

/** # 死信失败分类（`DeadLetterFailureCategory`） */
export type DeadLetterFailureCategory = 1 | 2 | 3;

/** # 死信失败子类（`DeadLetterFailureSubtype`）：1=字段不合规，允许改报文 */
export type DeadLetterFailureSubtype = 0 | 1;

/** # 死信处理状态（`DeadLetterStatus`） */
export type DeadLetterStatus = 0 | 1 | 2 | 3;

/**
 * # 死信记录（`DbzDeadLetterVo`）
 *
 * 幂等键是 `(connectorType, connectorId, topic, recordKey)`：同一条记录重复失败只累加
 * `retryCount` 并刷新 `lastSeenMs`，不新增行。
 */
export interface DbzDeadLetterVo {
  id: Id;
  connectorType: DeadLetterConnectorType;
  connectorId: Id;
  topic: string;
  recordKey: string;
  /** 原始消息所在分区；脚本未上报时为空 */
  partition?: number | null;
  /** 原始消息的 offset；脚本未上报时为空 */
  offset?: number | null;
  /** 原始报文（JSONB），字段随来源表变化 */
  payload: unknown;
  failureCategory: DeadLetterFailureCategory;
  /** 确定性失败细分；1=字段不合规才允许改报文 */
  failureSubtype: DeadLetterFailureSubtype;
  /** 服务端判定的「是否允许修改报文后重投」 */
  canEditPayload: boolean;
  /** 修改后的待投报文；为空表示没改过，原始 payload 永不覆盖 */
  pendingPayload?: unknown | null;
  /** 待投报文的最后编辑人 */
  payloadEditedBy?: string | null;
  /** 待投报文的最后编辑时间（毫秒） */
  payloadEditedMs?: string | null;
  /** 最近一次人工重投是否成功；为空表示还没人工重投过 */
  lastRetrySuccess?: boolean | null;
  /** 最近一次人工重投时间（毫秒） */
  lastRetryMs?: string | null;
  /** 最近一次人工重投的结果说明（失败时为对端原因） */
  lastRetryMessage?: string | null;
  /** 失败原因：后端按分类/子类生成的一句清晰说明 */
  reason: string;
  /** 失败详情：对端返回的原始技术内容，原样保存 */
  failureDetail: string;
  /** 累计失败次数（含进入死信前的重试） */
  retryCount: number;
  firstSeenMs: string;
  lastSeenMs: string;
  status: DeadLetterStatus;
  handledBy?: string | null;
  handledMs?: string | null;
  remark?: string | null;
  createMs: string;
  updateMs: string;
}

/** # 待处理死信计数（`DbzDeadLetterCountVo`） */
export interface DbzDeadLetterCountVo {
  connectorType: DeadLetterConnectorType;
  connectorId: Id;
  count: number;
}

/** # 单条重投结果（`DbzDeadLetterRetryResultVo`） */
export interface DbzDeadLetterRetryResultVo {
  id: Id;
  success: boolean;
  message: string;
}

/** # 重投请求体 */
export interface DbzDeadLetterRetryInput {
  ids: Id[];
  /** 修改后的报文（JSON 文本）；不传按原始报文重投，仅单条可用 */
  payload?: string;
  remark?: string;
}

/** # 保存待投报文请求体 */
export interface DbzDeadLetterEditPayloadInput {
  id: Id;
  /** 修改后的报文（JSON 文本） */
  payload: string;
}

/** # 废弃请求体 */
export interface DbzDeadLetterDiscardInput {
  ids: Id[];
  remark?: string;
}

/** # 标记「已人工处理」请求体 */
export interface DbzDeadLetterMarkHandledInput {
  ids: Id[];
  remark?: string;
}

/** # 撤销「已人工处理」请求体 */
export interface DbzDeadLetterUnmarkHandledInput {
  ids: Id[];
}

/** # 恢复策略：仅补齐缺失 / 覆盖已存在 */
export type BackupRestoreStrategy = 'skip' | 'overwrite';

/** # 备份文件里的下游所属上游（`id` 仅用于追溯，恢复按 connectorName 挂载） */
export interface DbzBackupUpstreamVo {
  id?: Id | null;
  name: string;
  connectorName: string;
}

/** # 备份文件里的一个连接器（与 Connect 导入导出的连接器条目同构） */
export interface DbzBackupConnectorVo {
  direction: 'source' | 'sink';
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string | null;
  enabled: boolean;
  upstream?: DbzBackupUpstreamVo | null;
  sensitiveConfigKeys: string[];
}

/** # 备份文件里的一台 Kafka Connect */
export interface DbzBackupConnectVo {
  name: string;
  url: string;
  remark?: string | null;
  enabled: boolean;
  connectors: DbzBackupConnectorVo[];
}

/** # 备份文件里的 Kafka 集群（含 sasl_password） */
export interface DbzBackupBrokerVo {
  name: string;
  bootstrapServers: string;
  securityProtocol: KafkaSecurityProtocol;
  saslMechanism: KafkaSaslMechanism;
  saslUsername?: string | null;
  saslPassword?: string | null;
  remark?: string | null;
  enabled: boolean;
}

/** # 导出的备份文件（`GET /dbz/backup/export`） */
export interface DbzBackupFileVo {
  format: string;
  version: number;
  exportedAt: string;
  credentialMode: 'included';
  broker: DbzBackupBrokerVo;
  connects: DbzBackupConnectVo[];
}

/** # 恢复计划 / 结果里的一条记录 */
export interface DbzBackupRestoreItemVo {
  level: 'broker' | 'connect' | 'source' | 'sink';
  parent?: string | null;
  name: string;
  connectorName?: string | null;
  action: 'create' | 'overwrite' | 'skip' | 'failed';
  message?: string | null;
}

/** # 恢复结果（`POST /dbz/backup/restore`） */
export interface DbzBackupRestoreVo {
  valid: boolean;
  errors: string[];
  strategy: string;
  dryRun: boolean;
  created: number;
  overwritten: number;
  skipped: number;
  failed: number;
  items: DbzBackupRestoreItemVo[];
}
