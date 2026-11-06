-- dbz 数据库迁移基线：init-create-table（最终态建表）
--
-- 本文件由 A 库（重放历史全部迁移 + 生产补种）导出，用于替代历史迁移。
-- 一旦发布并写入 _sqlx_migrations，禁止再修改本文件；后续变更必须新增迁移。
-- 生成来源：A 库 pg_dump --schema-only（逐字节还原列序、约束名、列注释）

CREATE TABLE public.dbz_kafka_broker (
    id bigint NOT NULL,
    name character varying(50) NOT NULL,
    bootstrap_servers character varying(500) NOT NULL,
    security_protocol smallint DEFAULT 0 NOT NULL,
    sasl_mechanism smallint DEFAULT 0 NOT NULL,
    sasl_username character varying(100),
    sasl_password character varying(200),
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    run_msg character varying(500)
);

COMMENT ON TABLE public.dbz_kafka_broker IS 'Kafka Broker';

COMMENT ON COLUMN public.dbz_kafka_broker.id IS 'ID';

COMMENT ON COLUMN public.dbz_kafka_broker.name IS '名称
@len 1..=50';

COMMENT ON COLUMN public.dbz_kafka_broker.bootstrap_servers IS 'Broker 地址(host:port，多个用逗号分隔)
@len 1..=500';

COMMENT ON COLUMN public.dbz_kafka_broker.security_protocol IS '安全协议
@enum KafkaSecurityProtocol default=Plaintext
  0=Plaintext      明文
  1=SaslPlaintext  SASL 明文（用户名/密码认证）';

COMMENT ON COLUMN public.dbz_kafka_broker.sasl_mechanism IS 'SASL 机制
@enum KafkaSaslMechanism default=None
  0=None         不使用（安全协议为明文时）
  1=Plain        SASL/PLAIN
  2=ScramSha256  SCRAM-SHA-256
  3=ScramSha512  SCRAM-SHA-512';

COMMENT ON COLUMN public.dbz_kafka_broker.sasl_username IS '认证用户名
@len 0..=100';

COMMENT ON COLUMN public.dbz_kafka_broker.sasl_password IS '认证密码(明文存储，读写窗口只对运维开放)
@len 0..=200';

COMMENT ON COLUMN public.dbz_kafka_broker.run_status IS '状态
@enum KafkaBrokerStatus default=None
  0=None           无状态（刚添加记录，或刚改过地址/认证/重新启用，等待下一轮检查）
  1=Normal         正常
  2=Disabled       已禁用
  3=ConnectFailed  连接失败';

COMMENT ON COLUMN public.dbz_kafka_broker.remark IS '备注
@len 0..=50';

COMMENT ON COLUMN public.dbz_kafka_broker.enabled IS '启用';

COMMENT ON COLUMN public.dbz_kafka_broker.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_kafka_broker.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_kafka_broker.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_kafka_broker.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_kafka_broker.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

CREATE TABLE public.dbz_kafka_connect (
    id bigint NOT NULL,
    name character varying(50) NOT NULL,
    url character varying(100) NOT NULL,
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    kafka_broker_id bigint,
    run_msg character varying(500)
);

COMMENT ON TABLE public.dbz_kafka_connect IS 'Kafka Connect';

COMMENT ON COLUMN public.dbz_kafka_connect.id IS 'ID';

COMMENT ON COLUMN public.dbz_kafka_connect.name IS '名称';

COMMENT ON COLUMN public.dbz_kafka_connect.url IS '地址';

COMMENT ON COLUMN public.dbz_kafka_connect.run_status IS '状态
0-无状态(刚添加记录)
1-正常
2-已禁用
3-连接失败';

COMMENT ON COLUMN public.dbz_kafka_connect.remark IS '备注';

COMMENT ON COLUMN public.dbz_kafka_connect.enabled IS '启用';

COMMENT ON COLUMN public.dbz_kafka_connect.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_kafka_connect.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_kafka_connect.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_kafka_connect.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_kafka_connect.kafka_broker_id IS 'Kafka Broker ID';

COMMENT ON COLUMN public.dbz_kafka_connect.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

CREATE TABLE public.dbz_sink_connector (
    id bigint NOT NULL,
    kafka_connect_id bigint NOT NULL,
    source_connector_id bigint NOT NULL,
    name character varying(50) NOT NULL,
    connector_name character varying(50) NOT NULL,
    connector_config text NOT NULL,
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    run_msg character varying(500)
);

COMMENT ON TABLE public.dbz_sink_connector IS '下游连接器';

COMMENT ON COLUMN public.dbz_sink_connector.id IS 'ID';

COMMENT ON COLUMN public.dbz_sink_connector.kafka_connect_id IS 'Kafka Connect ID';

COMMENT ON COLUMN public.dbz_sink_connector.source_connector_id IS '上游连接器ID';

COMMENT ON COLUMN public.dbz_sink_connector.name IS '名称';

COMMENT ON COLUMN public.dbz_sink_connector.connector_name IS '连接器名称';

COMMENT ON COLUMN public.dbz_sink_connector.connector_config IS '连接器配置(JSON)';

COMMENT ON COLUMN public.dbz_sink_connector.run_status IS '运行状态
0-无状态(刚添加记录)
1-运行中
2-已禁用
3-连接KafkaConnect失败
4-注册连接器失败
5-检测连接器失败
6-检测连接器异常
7-检测连接器状态失败
8-检测连接器状态异常';

COMMENT ON COLUMN public.dbz_sink_connector.remark IS '备注';

COMMENT ON COLUMN public.dbz_sink_connector.enabled IS '启用';

COMMENT ON COLUMN public.dbz_sink_connector.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_sink_connector.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_sink_connector.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_sink_connector.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_sink_connector.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

CREATE TABLE public.dbz_source_connector (
    id bigint NOT NULL,
    kafka_connect_id bigint NOT NULL,
    name character varying(50) NOT NULL,
    connector_name character varying(50) NOT NULL,
    connector_config text NOT NULL,
    run_status smallint DEFAULT 0 NOT NULL,
    remark character varying(50),
    enabled boolean DEFAULT true NOT NULL,
    creater_id bigint NOT NULL,
    create_ms bigint NOT NULL,
    updater_id bigint NOT NULL,
    update_ms bigint NOT NULL,
    run_msg character varying(500)
);

COMMENT ON TABLE public.dbz_source_connector IS '上游连接器';

COMMENT ON COLUMN public.dbz_source_connector.id IS 'ID';

COMMENT ON COLUMN public.dbz_source_connector.kafka_connect_id IS 'Kafka Connect ID';

COMMENT ON COLUMN public.dbz_source_connector.name IS '名称';

COMMENT ON COLUMN public.dbz_source_connector.connector_name IS '连接器名称';

COMMENT ON COLUMN public.dbz_source_connector.connector_config IS '连接器配置(JSON)';

COMMENT ON COLUMN public.dbz_source_connector.run_status IS '运行状态
0-无状态(刚添加记录)
1-运行中
2-已禁用
3-连接KafkaConnect失败
4-注册连接器失败
5-检测连接器失败
6-检测连接器异常
7-检测连接器状态失败
8-检测连接器状态异常';

COMMENT ON COLUMN public.dbz_source_connector.remark IS '备注';

COMMENT ON COLUMN public.dbz_source_connector.enabled IS '启用';

COMMENT ON COLUMN public.dbz_source_connector.creater_id IS '创建者ID';

COMMENT ON COLUMN public.dbz_source_connector.create_ms IS '创建时间戳';

COMMENT ON COLUMN public.dbz_source_connector.updater_id IS '更新者ID';

COMMENT ON COLUMN public.dbz_source_connector.update_ms IS '更新时间戳';

COMMENT ON COLUMN public.dbz_source_connector.run_msg IS '检查结论（最近一次检查的失败原因，成功时为空）
@len 0..=500';

ALTER TABLE ONLY public.dbz_kafka_broker
    ADD CONSTRAINT ak_bootstrap_servers_dbz_kafka_broker UNIQUE (bootstrap_servers);

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT ak_connector_name_dbz_sink_connector UNIQUE (connector_name);

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT ak_connector_name_dbz_source_connector UNIQUE (connector_name);

ALTER TABLE ONLY public.dbz_kafka_broker
    ADD CONSTRAINT ak_name_dbz_kafka_broker UNIQUE (name);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT ak_name_dbz_kafka_connect UNIQUE (name);

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT ak_name_dbz_sink_connector UNIQUE (name);

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT ak_name_dbz_source_connector UNIQUE (name);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT ak_url_dbz_kafka_connect UNIQUE (url);

ALTER TABLE ONLY public.dbz_kafka_broker
    ADD CONSTRAINT pk_dbz_kafka_broker PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT pk_dbz_kafka_connect PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT pk_dbz_sink_connector PRIMARY KEY (id);

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT pk_dbz_source_connector PRIMARY KEY (id);

CREATE UNIQUE INDEX dbz_kafka_broker_pk ON public.dbz_kafka_broker USING btree (id);

CREATE UNIQUE INDEX dbz_kafka_connect_pk ON public.dbz_kafka_connect USING btree (id);

CREATE UNIQUE INDEX dbz_sink_connector_pk ON public.dbz_sink_connector USING btree (id);

CREATE UNIQUE INDEX dbz_source_connector_pk ON public.dbz_source_connector USING btree (id);

CREATE INDEX relationship_1_fk ON public.dbz_sink_connector USING btree (source_connector_id);

CREATE INDEX relationship_2_fk ON public.dbz_source_connector USING btree (kafka_connect_id);

CREATE INDEX relationship_3_fk ON public.dbz_sink_connector USING btree (kafka_connect_id);

CREATE INDEX relationship_4_fk ON public.dbz_kafka_connect USING btree (kafka_broker_id);

ALTER TABLE ONLY public.dbz_kafka_connect
    ADD CONSTRAINT fk_kafka_broker_id__from__dbz_kafka_broker FOREIGN KEY (kafka_broker_id) REFERENCES public.dbz_kafka_broker(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT fk_kafka_connect_id__from__dbz_kafka_connect FOREIGN KEY (kafka_connect_id) REFERENCES public.dbz_kafka_connect(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ONLY public.dbz_source_connector
    ADD CONSTRAINT fk_kafka_connect_id__from__dbz_kafka_connect FOREIGN KEY (kafka_connect_id) REFERENCES public.dbz_kafka_connect(id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE ONLY public.dbz_sink_connector
    ADD CONSTRAINT fk_source_connector_id__from__dbz_source_connector FOREIGN KEY (source_connector_id) REFERENCES public.dbz_source_connector(id) ON UPDATE RESTRICT ON DELETE RESTRICT;
