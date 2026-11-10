/*==============================================================*/
/* DBMS name:      PostgreSQL 9.x                               */
/* Created on:     2026/11/10 22:36:56                          */
/*==============================================================*/


/*==============================================================*/
/* Table: dbz_control_plane_edge_layout                         */
/*==============================================================*/
create table dbz_control_plane_edge_layout (
   id                   INT8                 not null,
   edge_id              VARCHAR(255)         not null,
   mid_y                FLOAT8               not null,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   start_dx             FLOAT8               not null default 0,
   end_dx               FLOAT8               not null default 0,
   constraint PK_DBZ_CONTROL_PLANE_EDGE_LAYO primary key (id),
   constraint AK_EDGE_ID_DBZ_CONTROL_PLANE_EDGE_LAYOUT unique (edge_id)
);

comment on table dbz_control_plane_edge_layout is
'控制面画布连线折点';

comment on column dbz_control_plane_edge_layout.id is
'ID';

comment on column dbz_control_plane_edge_layout.edge_id is
'连线 ID（由两端节点 key 拼成，如 own:broker:1:connect:2）';

comment on column dbz_control_plane_edge_layout.mid_y is
'正交走线中间水平段的世界坐标 Y；拖动折点只改这一个值';

comment on column dbz_control_plane_edge_layout.creater_id is
'创建者ID';

comment on column dbz_control_plane_edge_layout.create_ms is
'创建时间戳';

comment on column dbz_control_plane_edge_layout.updater_id is
'更新者ID';

comment on column dbz_control_plane_edge_layout.update_ms is
'更新时间戳';

comment on column dbz_control_plane_edge_layout.start_dx is
'上游连接点相对卡片中心的横向偏移，0 表示默认位置';

comment on column dbz_control_plane_edge_layout.end_dx is
'下游连接点相对卡片中心的横向偏移，0 表示默认位置';

/*==============================================================*/
/* Index: dbz_control_plane_edge_layout_P                       */
/*==============================================================*/
create unique index dbz_control_plane_edge_layout_P on dbz_control_plane_edge_layout (
id
);

/*==============================================================*/
/* Table: dbz_control_plane_layout                              */
/*==============================================================*/
create table dbz_control_plane_layout (
   id                   INT8                 not null,
   node_type            INT2                 not null,
   node_id              INT8                 not null,
   x                    FLOAT8               not null,
   y                    FLOAT8               not null,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   constraint PK_DBZ_CONTROL_PLANE_LAYOUT primary key (id),
   constraint AK_NODE_AND_TYPE_DBZ_CONTROL_PLANE_LAYOUT unique (node_type, node_id)
);

comment on table dbz_control_plane_layout is
'控制面画布节点位置';

comment on column dbz_control_plane_layout.id is
'ID';

comment on column dbz_control_plane_layout.node_type is
'节点类型
@enum ControlPlaneNodeType default=Broker
  1=Broker   Kafka 集群
  2=Connect  Kafka Connect
  3=Source   上游连接器
  4=Sink     下游连接器';

comment on column dbz_control_plane_layout.node_id is
'对应业务表的记录 ID';

comment on column dbz_control_plane_layout.x is
'画布世界坐标 X';

comment on column dbz_control_plane_layout.y is
'画布世界坐标 Y';

comment on column dbz_control_plane_layout.creater_id is
'创建者ID';

comment on column dbz_control_plane_layout.create_ms is
'创建时间戳';

comment on column dbz_control_plane_layout.updater_id is
'更新者ID';

comment on column dbz_control_plane_layout.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_control_plane_layout_PK                           */
/*==============================================================*/
create unique index dbz_control_plane_layout_PK on dbz_control_plane_layout (
id
);

/*==============================================================*/
/* Table: dbz_dead_letter                                       */
/*==============================================================*/
create table dbz_dead_letter (
   id                   INT8                 not null,
   connector_type       INT2                 not null,
   connector_id         INT8                 not null,
   topic                VARCHAR(255)         not null,
   record_key           VARCHAR(512)         not null default '',
   payload              JSONB                not null,
   failure_category     INT2                 not null,
   failure_detail       TEXT                 not null default '',
   retry_count          INT4                 not null default 0,
   first_seen_ms        INT8                 not null,
   last_seen_ms         INT8                 not null,
   status               INT2                 not null default 0,
   handled_by           INT8                 null,
   handled_ms           INT8                 null,
   remark               VARCHAR(500)         null,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   failure_subtype      INT2                 not null default 0,
   pending_payload      JSONB                null,
   payload_edited_by    INT8                 null,
   payload_edited_ms    INT8                 null,
   last_retry_success   BOOL                 null,
   last_retry_ms        INT8                 null,
   last_retry_message   TEXT                 null,
   reason               TEXT                 not null default '',
   partition            INT4                 null,
   "offset"             INT8                 null,
   constraint PK_DBZ_DEAD_LETTER primary key (id)
);

comment on table dbz_dead_letter is
'死信记录（Kafka Connect DLQ 镜像 + 人工处置）；事实来源是 DLQ topic，本表只做页面索引与人工结论';

comment on column dbz_dead_letter.id is
'ID';

comment on column dbz_dead_letter.connector_type is
'连接器类型；Connect 的 DLQ 只适用于 sink，故恒为 2
@enum DeadLetterConnectorType default=Sink
  1=Source  上游连接器（历史取值，不再产生死信）
  2=Sink    下游连接器';

comment on column dbz_dead_letter.connector_id is
'下游连接器记录 ID（由 DLQ 上下文头里的连接器名反查，未知为 0）；与 topic/partition/offset 组成幂等键';

comment on column dbz_dead_letter.topic is
'原始消息所在的源 topic（来自 __connect.errors.topic）；与 connector_id/partition/offset 组成幂等键';

comment on column dbz_dead_letter.record_key is
'原始消息的 key（来自 DLQ 记录 key，无 key 时空串）';

comment on column dbz_dead_letter.payload is
'DLQ 记录的原始报文（JSONB）；非 JSON 文本包成 {"__raw_utf8": ...} 保留';

comment on column dbz_dead_letter.failure_category is
'异常分类，由 DLQ 记录头里的异常类名推导，只用于页面展示，不参与重试决策
@enum DeadLetterFailureCategory default=Deterministic
  1=Deterministic  数据/格式/配置类异常（DataException、序列化失败等）
  2=Transient      投递类异常（RetriableException、超时、IO、HTTP 错误）
  3=Unknown        其它无法归类的异常';

comment on column dbz_dead_letter.failure_detail is
'DLQ 上下文原文：异常类名、消息、堆栈与 DLQ 坐标，原样保存不截断';

comment on column dbz_dead_letter.retry_count is
'保留列：Connect 的重试次数不写进 DLQ，故恒为 0';

comment on column dbz_dead_letter.first_seen_ms is
'Kafka 记录时间戳（毫秒）';

comment on column dbz_dead_letter.last_seen_ms is
'Kafka 记录时间戳（毫秒），与 first_seen_ms 相同';

comment on column dbz_dead_letter.status is
'处理状态
@enum DeadLetterStatus default=Pending
  0=Pending        待处理
  1=Retried        已重投（报文写回源 topic）
  2=Discarded      已废弃
  3=ManualHandled  已人工处理';

comment on column dbz_dead_letter.handled_by is
'处理人 ID';

comment on column dbz_dead_letter.handled_ms is
'处理时间戳（毫秒）';

comment on column dbz_dead_letter.remark is
'备注';

comment on column dbz_dead_letter.creater_id is
'创建者ID';

comment on column dbz_dead_letter.create_ms is
'创建时间戳';

comment on column dbz_dead_letter.updater_id is
'更新者ID';

comment on column dbz_dead_letter.update_ms is
'更新时间戳';

comment on column dbz_dead_letter.failure_subtype is
'异常细分子类，由 DLQ 异常消息推导
@enum DeadLetterFailureSubtype default=Other
  0=Other            未细分
  1=FieldValidation  字段不合规，允许修改报文后重投';

comment on column dbz_dead_letter.pending_payload is
'修改后的待投报文（JSONB）；原始报文 payload 永不覆盖，为空表示没有修改过';

comment on column dbz_dead_letter.payload_edited_by is
'待投报文的最后编辑人 ID';

comment on column dbz_dead_letter.payload_edited_ms is
'待投报文的最后编辑时间戳（毫秒）';

comment on column dbz_dead_letter.last_retry_success is
'最近一次人工重投是否成功；为空表示还没有人工重投过';

comment on column dbz_dead_letter.last_retry_ms is
'最近一次人工重投的时间戳（毫秒）';

comment on column dbz_dead_letter.last_retry_message is
'最近一次人工重投的结果说明（写回源 topic 的结果）';

comment on column dbz_dead_letter.reason is
'给人看的一句失败原因，由后端按异常分类统一生成；原始技术内容在 failure_detail';

comment on column dbz_dead_letter.partition is
'原始消息所在分区（来自 __connect.errors.partition）；与 connector_id/topic/offset 组成幂等键';

comment on column dbz_dead_letter."offset" is
'原始消息 offset（来自 __connect.errors.offset）；与 connector_id/topic/partition 组成幂等键';

/*==============================================================*/
/* Index: dbz_dead_letter_PK                                    */
/*==============================================================*/
create unique index dbz_dead_letter_PK on dbz_dead_letter (
id
);

/*==============================================================*/
/* Index: ux_dbz_dead_letter_key                                */
/*==============================================================*/
create unique index ux_dbz_dead_letter_key on dbz_dead_letter (
connector_id,
topic,
partition,
"offset"
);

/*==============================================================*/
/* Table: dbz_delete_audit                                      */
/*==============================================================*/
create table dbz_delete_audit (
   id                   INT8                 not null,
   node_type            VARCHAR(32)          not null,
   node_id              INT8                 not null,
   node_name            VARCHAR(255)         not null,
   operator_id          INT8                 not null,
   delete_ms            INT8                 not null,
   constraint PK_DBZ_DELETE_AUDIT primary key (id)
);

comment on table dbz_delete_audit is
'控制面删除审计';

comment on column dbz_delete_audit.id is
'ID';

comment on column dbz_delete_audit.node_type is
'节点类型：broker / connect / source / sink';

comment on column dbz_delete_audit.node_id is
'节点记录 ID';

comment on column dbz_delete_audit.node_name is
'删除前的节点名称';

comment on column dbz_delete_audit.operator_id is
'删除人（当前登录用户）';

comment on column dbz_delete_audit.delete_ms is
'删除时间戳（毫秒）';

/*==============================================================*/
/* Index: dbz_delete_audit_PK                                   */
/*==============================================================*/
create unique index dbz_delete_audit_PK on dbz_delete_audit (
id
);

/*==============================================================*/
/* Table: dbz_kafka_broker                                      */
/*==============================================================*/
create table dbz_kafka_broker (
   id                   INT8                 not null,
   name                 VARCHAR(50)          not null,
   bootstrap_servers    VARCHAR(500)         not null,
   security_protocol    INT2                 not null default 0,
   sasl_mechanism       INT2                 not null default 0,
   sasl_username        VARCHAR(100)         null,
   sasl_password        VARCHAR(200)         null,
   run_status           INT2                 not null default 0,
   run_msg              VARCHAR(500)         null,
   remark               VARCHAR(50)          null,
   enabled              BOOL                 not null default true,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   constraint PK_DBZ_KAFKA_BROKER primary key (id),
   constraint AK_NAME_DBZ_KAFKA_BROKER unique (name),
   constraint AK_BOOTSTRAP_SERVERS_DBZ_KAFKA_BROKER unique (bootstrap_servers)
);

comment on table dbz_kafka_broker is
'Kafka Broker';

comment on column dbz_kafka_broker.id is
'ID';

comment on column dbz_kafka_broker.name is
'名称
@len 1..=50';

comment on column dbz_kafka_broker.bootstrap_servers is
'Broker 地址(host:port，多个用逗号分隔)
@len 1..=500';

comment on column dbz_kafka_broker.security_protocol is
'安全协议
@enum KafkaSecurityProtocol default=Plaintext
  0=Plaintext      明文
  1=SaslPlaintext  SASL 明文（用户名/密码认证）';

comment on column dbz_kafka_broker.sasl_mechanism is
'SASL 机制
@enum KafkaSaslMechanism default=None
  0=None         不使用（安全协议为明文时）
  1=Plain        SASL/PLAIN
  2=ScramSha256  SCRAM-SHA-256
  3=ScramSha512  SCRAM-SHA-512';

comment on column dbz_kafka_broker.sasl_username is
'认证用户名
@len 0..=100';

comment on column dbz_kafka_broker.sasl_password is
'认证密码(明文存储，读写窗口只对运维开放)
@len 0..=200
@secret';

comment on column dbz_kafka_broker.run_status is
'状态
@enum KafkaBrokerStatus default=None
  0=None           无状态（刚添加记录，或刚改过地址/认证/重新启用，等待下一轮检查）
  1=Normal         正常
  2=Disabled       已禁用
  3=ConnectFailed  连接失败';

comment on column dbz_kafka_broker.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

comment on column dbz_kafka_broker.remark is
'备注
@len 0..=50';

comment on column dbz_kafka_broker.enabled is
'启用';

comment on column dbz_kafka_broker.creater_id is
'创建者ID';

comment on column dbz_kafka_broker.create_ms is
'创建时间戳';

comment on column dbz_kafka_broker.updater_id is
'更新者ID';

comment on column dbz_kafka_broker.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_kafka_broker_PK                                   */
/*==============================================================*/
create unique index dbz_kafka_broker_PK on dbz_kafka_broker (
id
);

/*==============================================================*/
/* Table: dbz_kafka_connect                                     */
/*==============================================================*/
create table dbz_kafka_connect (
   id                   INT8                 not null,
   kafka_broker_id      INT8                 not null,
   name                 VARCHAR(50)          not null,
   url                  VARCHAR(100)         not null,
   run_status           INT2                 not null default 0,
   run_msg              VARCHAR(500)         null,
   remark               VARCHAR(50)          null,
   enabled              BOOL                 not null default true,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   constraint PK_DBZ_KAFKA_CONNECT primary key (id),
   constraint AK_NAME_DBZ_KAFKA_CONNECT unique (name),
   constraint AK_URL_DBZ_KAFKA_CONNECT unique (url)
);

comment on table dbz_kafka_connect is
'Kafka Connect';

comment on column dbz_kafka_connect.id is
'ID';

comment on column dbz_kafka_connect.kafka_broker_id is
'Kafka Broker ID';

comment on column dbz_kafka_connect.name is
'名称
@len 1..=50';

comment on column dbz_kafka_connect.url is
'地址
@len 1..=100';

comment on column dbz_kafka_connect.run_status is
'状态
@enum KafkaConnectStatus default=None
  0=None           无状态（刚添加记录，或刚改过地址/重新启用，等待下一轮检查）
  1=Normal         正常
  2=Disabled       已禁用
  3=ConnectFailed  连接失败';

comment on column dbz_kafka_connect.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

comment on column dbz_kafka_connect.remark is
'备注
@len 0..=50';

comment on column dbz_kafka_connect.enabled is
'启用';

comment on column dbz_kafka_connect.creater_id is
'创建者ID';

comment on column dbz_kafka_connect.create_ms is
'创建时间戳';

comment on column dbz_kafka_connect.updater_id is
'更新者ID';

comment on column dbz_kafka_connect.update_ms is
'更新时间戳';

/*==============================================================*/
/* Index: dbz_kafka_connect_PK                                  */
/*==============================================================*/
create unique index dbz_kafka_connect_PK on dbz_kafka_connect (
id
);

/*==============================================================*/
/* Index: Relationship_4_FK                                     */
/*==============================================================*/
create  index Relationship_4_FK on dbz_kafka_connect (
kafka_broker_id
);

/*==============================================================*/
/* Table: dbz_sink_connector                                    */
/*==============================================================*/
create table dbz_sink_connector (
   id                   INT8                 not null,
   kafka_connect_id     INT8                 not null,
   source_connector_id  INT8                 not null,
   name                 VARCHAR(50)          not null,
   connector_name       VARCHAR(50)          not null,
   connector_config     TEXT                 not null,
   run_status           INT2                 not null default 0,
   run_msg              VARCHAR(500)         null,
   remark               VARCHAR(50)          null,
   enabled              BOOL                 not null default true,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   inconsistent         BOOL                 not null default false,
   inconsistent_detail  TEXT                 null,
   constraint PK_DBZ_SINK_CONNECTOR primary key (id),
   constraint AK_NAME_DBZ_SINK_CONNECTOR unique (name),
   constraint AK_CONNECTOR_NAME_DBZ_SINK_CONNECTOR unique (connector_name)
);

comment on table dbz_sink_connector is
'下游连接器';

comment on column dbz_sink_connector.id is
'ID';

comment on column dbz_sink_connector.kafka_connect_id is
'Kafka Connect ID';

comment on column dbz_sink_connector.source_connector_id is
'上游连接器ID';

comment on column dbz_sink_connector.name is
'名称
@len 1..=50';

comment on column dbz_sink_connector.connector_name is
'连接器名称
@len 1..=50';

comment on column dbz_sink_connector.connector_config is
'连接器配置(JSON)
@len 1..';

comment on column dbz_sink_connector.run_status is
'连接器统一状态
@enum ConnectorRunStatus default=Unknown
  0=Unknown       未知
  1=Running       运行中
  2=Disabled      已停用
  3=Failed        失败
  4=NotDeployed   仅纳管未部署
  5=Unreachable   不可达（Connect 连不上，保留最后已知状态）
  6=Stopped       已停止（PAUSED / STOPPED）';

comment on column dbz_sink_connector.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

comment on column dbz_sink_connector.remark is
'备注
@len 0..=50';

comment on column dbz_sink_connector.enabled is
'启用';

comment on column dbz_sink_connector.creater_id is
'创建者ID';

comment on column dbz_sink_connector.create_ms is
'创建时间戳';

comment on column dbz_sink_connector.updater_id is
'更新者ID';

comment on column dbz_sink_connector.update_ms is
'更新时间戳';

comment on column dbz_sink_connector.inconsistent is
'检测不一致标记：库内配置与 Connect 实际配置不一致，或远端已不存在';

comment on column dbz_sink_connector.inconsistent_detail is
'检测不一致时的对比快照（脱敏 JSON）：库内/远端逐项配置、差异标记与检测时间；一致时为 NULL';

/*==============================================================*/
/* Index: dbz_sink_connector_PK                                 */
/*==============================================================*/
create unique index dbz_sink_connector_PK on dbz_sink_connector (
id
);

/*==============================================================*/
/* Index: Relationship_1_FK                                     */
/*==============================================================*/
create  index Relationship_1_FK on dbz_sink_connector (
source_connector_id
);

/*==============================================================*/
/* Index: Relationship_3_FK                                     */
/*==============================================================*/
create  index Relationship_3_FK on dbz_sink_connector (
kafka_connect_id
);

/*==============================================================*/
/* Table: dbz_source_connector                                  */
/*==============================================================*/
create table dbz_source_connector (
   id                   INT8                 not null,
   kafka_connect_id     INT8                 not null,
   name                 VARCHAR(50)          not null,
   connector_name       VARCHAR(50)          not null,
   connector_config     TEXT                 not null,
   run_status           INT2                 not null default 0,
   run_msg              VARCHAR(500)         null,
   remark               VARCHAR(50)          null,
   enabled              BOOL                 not null default true,
   creater_id           INT8                 not null,
   create_ms            INT8                 not null,
   updater_id           INT8                 not null,
   update_ms            INT8                 not null,
   inconsistent         BOOL                 not null default false,
   inconsistent_detail  TEXT                 null,
   constraint PK_DBZ_SOURCE_CONNECTOR primary key (id),
   constraint AK_NAME_DBZ_SOURCE_CONNECTOR unique (name),
   constraint AK_CONNECTOR_NAME_DBZ_SOURCE_CONNECTOR unique (connector_name)
);

comment on table dbz_source_connector is
'上游连接器';

comment on column dbz_source_connector.id is
'ID';

comment on column dbz_source_connector.kafka_connect_id is
'Kafka Connect ID';

comment on column dbz_source_connector.name is
'名称
@len 1..=50';

comment on column dbz_source_connector.connector_name is
'连接器名称
@len 1..=50';

comment on column dbz_source_connector.connector_config is
'连接器配置(JSON)
@len 1..';

comment on column dbz_source_connector.run_status is
'连接器统一状态
@enum ConnectorRunStatus default=Unknown
  0=Unknown       未知
  1=Running       运行中
  2=Disabled      已停用
  3=Failed        失败
  4=NotDeployed   仅纳管未部署
  5=Unreachable   不可达（Connect 连不上，保留最后已知状态）
  6=Stopped       已停止（PAUSED / STOPPED）';

comment on column dbz_source_connector.run_msg is
'检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

comment on column dbz_source_connector.remark is
'备注
@len 0..=50';

comment on column dbz_source_connector.enabled is
'启用';

comment on column dbz_source_connector.creater_id is
'创建者ID';

comment on column dbz_source_connector.create_ms is
'创建时间戳';

comment on column dbz_source_connector.updater_id is
'更新者ID';

comment on column dbz_source_connector.update_ms is
'更新时间戳';

comment on column dbz_source_connector.inconsistent is
'检测不一致标记：库内配置与 Connect 实际配置不一致，或远端已不存在';

comment on column dbz_source_connector.inconsistent_detail is
'检测不一致时的对比快照（脱敏 JSON）：库内/远端逐项配置、差异标记与检测时间；一致时为 NULL';

/*==============================================================*/
/* Index: dbz_source_connector_PK                               */
/*==============================================================*/
create unique index dbz_source_connector_PK on dbz_source_connector (
id
);

/*==============================================================*/
/* Index: Relationship_2_FK                                     */
/*==============================================================*/
create  index Relationship_2_FK on dbz_source_connector (
kafka_connect_id
);

alter table dbz_kafka_connect
   add constraint fk_kafka_broker_id__from__dbz_kafka_broker foreign key (kafka_broker_id)
      references dbz_kafka_broker (id)
      on delete restrict on update restrict;

alter table dbz_sink_connector
   add constraint fk_source_connector_id__from__dbz_source_connector foreign key (source_connector_id)
      references dbz_source_connector (id)
      on delete restrict on update restrict;

alter table dbz_sink_connector
   add constraint fk_kafka_connect_id__from__dbz_kafka_connect foreign key (kafka_connect_id)
      references dbz_kafka_connect (id)
      on delete restrict on update restrict;

alter table dbz_source_connector
   add constraint fk_kafka_connect_id__from__dbz_kafka_connect foreign key (kafka_connect_id)
      references dbz_kafka_connect (id)
      on delete restrict on update restrict;

