create table dbz_kafka_broker (
                                   id                   INT8                 not null,
                                   name                 VARCHAR(50)          not null,
                                   bootstrap_servers    VARCHAR(500)         not null,
                                   security_protocol    INT2                 not null default 0,
                                   sasl_mechanism       INT2                 not null default 0,
                                   sasl_username        VARCHAR(100)         null,
                                   sasl_password        VARCHAR(200)         null,
                                   run_status           INT2                 not null default 0,
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
@len 0..=200';

comment on column dbz_kafka_broker.run_status is
'状态
@enum KafkaBrokerStatus default=None
  0=None           无状态（刚添加记录，或刚改过地址/认证/重新启用，等待下一轮检查）
  1=Normal         正常
  2=Disabled       已禁用
  3=ConnectFailed  连接失败';

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
create unique index dbz_kafka_broker_PK on dbz_kafka_broker (
id
);

alter table dbz_kafka_connect
   add column kafka_broker_id INT8 null;

comment on column dbz_kafka_connect.kafka_broker_id is
'Kafka Broker ID';

/*==============================================================*/
/* Index: Relationship_4_FK                                     */
create  index Relationship_4_FK on dbz_kafka_connect (
kafka_broker_id
);

alter table dbz_kafka_connect
   add constraint fk_kafka_broker_id__from__dbz_kafka_broker foreign key (kafka_broker_id)
      references dbz_kafka_broker (id)
      on delete restrict on update restrict;
